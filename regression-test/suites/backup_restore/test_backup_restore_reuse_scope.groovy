// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

import groovy.json.JsonSlurper

// The scope of partition level reuse with the default configs: every reused partition is proved by the digest of each
// replica (the check level is full), and only duplicate tables are reused.
//   1. A duplicate table is kept whole, and the same INSERT and DELETE on the kept table and on the table of a whole
//      download leave the same rows (row count and row hash).
//   2. The counterexample of the merge-on-write table: the latest row of a key is a delete sign row on one side, the
//      key does not exist on the other side. The visible rows (and the digest) are the same, but the hidden state
//      differs, so the reuse is rejected (HIDDEN_STATE_MOW) and the partition is downloaded; then a partial update
//      with partial_update_new_key_behavior = ERROR behaves the same on both sides.
//   3. A merge-on-read table is rejected (HIDDEN_STATE_MOR) and downloaded.
//   4. The effective check level is full, whatever level the job asks for.
// Single replica, auto compaction is off, all the configs it changes are restored at the end.
suite("test_backup_restore_reuse_scope", "backup_restore") {
    String suiteName = "test_backup_restore_reuse_scope"
    String repoName = "${suiteName}_repo_" + UUID.randomUUID().toString().replace("-", "")
    String dbA = "${suiteName}_a"
    String dbB = "${suiteName}_b"
    String dbC = "${suiteName}_c"
    String dbD = "${suiteName}_d"
    String dupTable = "${suiteName}_dup"
    String mowTable = "${suiteName}_mow"
    String morTable = "${suiteName}_mor"
    int numPartitions = 3
    int numBatches = 4

    def syncer = getSyncer()
    syncer.createS3Repository(repoName)

    def setConfig = { String name, String value ->
        sql """ADMIN SET FRONTEND CONFIG ("${name}" = "${value}")"""
    }
    def getConfig = { String name ->
        def rows = sql_return_maparray """ADMIN SHOW FRONTEND CONFIG LIKE "${name}" """
        return rows.find { it.Key == name }.Value as String
    }

    // the defaults under test: not changed by this case
    assertEquals("full", getConfig("restore_reuse_default_check_level"))
    assertEquals("false", getConfig("restore_reuse_allow_unverified_keep"))
    assertEquals("false", getConfig("restore_reuse_allow_hidden_state_models"))

    setConfig("enable_restore_partition_reuse", "true")
    setConfig("restore_reuse_min_partition_bytes", "0")

    for (String db : [dbA, dbB, dbC, dbD]) {
        sql "DROP DATABASE IF EXISTS ${db} FORCE"
        sql "CREATE DATABASE ${db}"
    }

    def backupAndWait = { String db, String snapshot, List<String> tables ->
        sql """
            BACKUP SNAPSHOT ${db}.${snapshot}
            TO `${repoName}`
            ON (${tables.join(", ")})
            PROPERTIES ("logical_digest" = "true")
        """
        syncer.waitSnapshotFinish(db)
        def timestamp = syncer.getSnapshotTimestamp(repoName, snapshot)
        assertTrue(timestamp != null)
        return timestamp
    }

    // the ReuseEstimate and the DownloadStats of the last restore job
    def lastEstimate = null
    def lastStats = null
    def restoreAndWait = { String db, String snapshot, String timestamp, List<String> tables, String level ->
        String levelProp = level == null ? "" : ", \"reuse_check_level\" = \"${level}\""
        sql """
            RESTORE SNAPSHOT ${db}.${snapshot}
            FROM `${repoName}`
            ON (${tables.join(", ")})
            PROPERTIES ("backup_timestamp" = "${timestamp}", "reserve_replica" = "true"${levelProp})
        """
        syncer.waitAllRestoreFinish(db)
        def result = sql_return_maparray """ SHOW RESTORE FROM ${db} WHERE Label = "${snapshot}" """
        def job = result.last()
        logger.info("restore ${snapshot} into ${db}, level ${level}: ${job}")
        assertEquals("FINISHED", job.State)
        lastStats = new JsonSlurper().parseText(job.DownloadStats as String)
        lastEstimate = job.ReuseEstimate == null ? null : new JsonSlurper().parseText(job.ReuseEstimate as String)
        logger.info("download stats: ${lastStats}, reuse estimate: ${lastEstimate}")
        return lastStats
    }
    def keptPartitions = { (lastEstimate?.kept_partitions ?: 0) as int }

    // the row count and the hash of the rows
    def rowsOf = { String db, String tbl ->
        def r = sql """SELECT COUNT(*), SUM(CRC32(CONCAT(CAST(id AS STRING), '|', IFNULL(CAST(v1 AS STRING), 'NULL')))) FROM ${db}.${tbl}"""
        return "${r[0][0]}:${r[0][1]}"
    }
    def versionsOf = { String db, String tbl ->
        def parts = sql_return_maparray "SHOW PARTITIONS FROM ${db}.${tbl}"
        return parts.collectEntries { [(it.PartitionName): it.VisibleVersion as String] }
    }
    // the local data sizes are known by the report of the backends
    def waitDataSize = { String db, String tbl, long rows ->
        for (int i = 0; i < 30; ++i) {
            def tablets = sql_return_maparray "SHOW TABLETS FROM ${db}.${tbl}"
            if (tablets.sum { it.RowCount as long } == rows
                    && !tablets.any { (it.LocalDataSize as long) == 0 && (it.RowCount as long) > 0 }) {
                return
            }
            sleep(5000)
        }
    }

    // ------------------------------------------------------------------------------------------------
    // 1. duplicate table: kept whole, the same writes leave the same rows as a whole download
    // ------------------------------------------------------------------------------------------------
    sql """
        CREATE TABLE ${dbA}.${dupTable} (
            `id` INT NOT NULL,
            `v1` VARCHAR(64) NULL
        )
        DUPLICATE KEY(`id`)
        PARTITION BY RANGE(`id`)
        (
            PARTITION p1 VALUES LESS THAN ("1000"),
            PARTITION p2 VALUES LESS THAN ("2000"),
            PARTITION p3 VALUES LESS THAN ("3000")
        )
        DISTRIBUTED BY HASH(`id`) BUCKETS 2
        PROPERTIES ("replication_num" = "1", "disable_auto_compaction" = "true")
    """
    for (int b = 0; b < numBatches; ++b) {
        List<String> values = []
        for (int p = 0; p < numPartitions; ++p) {
            for (int i = 0; i < 20; ++i) {
                int id = p * 1000 + b * 20 + i
                values.add("(${id}, 'value of ${id} in batch ${b}')")
            }
        }
        sql "INSERT INTO ${dbA}.${dupTable} VALUES ${values.join(",")}"
    }
    sql "sync"
    long dupRows = numPartitions * numBatches * 20

    String snapDup = "${suiteName}_snap_dup"
    String tsDup = backupAndWait(dbA, snapDup, [dupTable])
    // B and C are both new: restored as a whole
    restoreAndWait(dbB, snapDup, tsDup, [dupTable], null)
    long dupBaseline = lastStats.downloaded_bytes as long
    assertTrue(dupBaseline > 0)
    restoreAndWait(dbC, snapDup, tsDup, [dupTable], null)
    assertEquals(rowsOf(dbA, dupTable), rowsOf(dbB, dupTable))
    assertEquals(rowsOf(dbA, dupTable), rowsOf(dbC, dupTable))
    waitDataSize(dbB, dupTable, dupRows)

    // restored again with the default level: every partition is kept, proved by the digest
    restoreAndWait(dbB, snapDup, tsDup, [dupTable], null)
    assertEquals(numPartitions, keptPartitions())
    assertEquals(0L, lastStats.downloaded_bytes as long)
    assertTrue((lastStats.kept_bytes as long) > 0)
    assertEquals(0, lastEstimate.l0_passed_but_hidden_state_mow as int)
    assertEquals(0, lastEstimate.l0_passed_but_hidden_state_mor as int)
    // 4. the default level is full, and the ones kept are all verified by the digest
    assertEquals("full", lastEstimate.check_level.requested)
    assertEquals("full", lastEstimate.check_level.effective)
    assertEquals(0, lastEstimate.check_level.kept_unverified as int)
    assertEquals("full", lastStats.check_level.effective)
    // the levels sample and off take effect as full (restore_reuse_allow_unverified_keep is false): the partitions
    // are verified by the digest, not kept on the sample or the lineage stamp
    for (String level : ["sample", "off"]) {
        restoreAndWait(dbB, snapDup, tsDup, [dupTable], level)
        assertEquals(numPartitions, keptPartitions())
        assertEquals(0L, lastStats.downloaded_bytes as long)
        assertEquals(level, lastEstimate.check_level.requested)
        assertEquals("full", lastEstimate.check_level.effective)
        assertEquals("full", lastStats.check_level.effective)
        assertEquals(0, lastEstimate.check_level.kept_unverified as int)
    }
    assertEquals(rowsOf(dbC, dupTable), rowsOf(dbB, dupTable))
    assertEquals(versionsOf(dbC, dupTable), versionsOf(dbB, dupTable))

    // the same writes on the kept table (B) and on the whole download (C): inserts (also of existing ids, a duplicate
    // table keeps both rows), deletes of rows of the backup, an insert of a deleted id again, in several partitions
    def writeBoth = { Closure write ->
        write(dbB)
        write(dbC)
    }
    writeBoth { String db ->
        sql "INSERT INTO ${db}.${dupTable} VALUES (5, 'duplicate of 5'), (1005, 'new in p2'), (2999, 'new in p3')"
        sql "DELETE FROM ${db}.${dupTable} WHERE id = 7"
        sql "DELETE FROM ${db}.${dupTable} PARTITION p2 WHERE id = 1010"
        sql "INSERT INTO ${db}.${dupTable} VALUES (7, 'inserted again after the delete'), (1010, 'again')"
        sql "DELETE FROM ${db}.${dupTable} WHERE id = 5"
        sql "INSERT INTO ${db}.${dupTable} VALUES (6, 'second 6')"
        sql "sync"
    }
    assertEquals(rowsOf(dbC, dupTable), rowsOf(dbB, dupTable))
    assertEquals(versionsOf(dbC, dupTable), versionsOf(dbB, dupTable))
    for (String q : ["WHERE id = 5", "WHERE id = 6", "WHERE id = 7", "WHERE id = 1010", "WHERE id = 2999"]) {
        assertEquals(sql("SELECT id, v1 FROM ${dbC}.${dupTable} ${q} ORDER BY v1"),
                sql("SELECT id, v1 FROM ${dbB}.${dupTable} ${q} ORDER BY v1"))
    }
    assertEquals(0, (sql "SELECT COUNT(*) FROM ${dbB}.${dupTable} WHERE id = 5")[0][0])
    assertEquals(2, (sql "SELECT COUNT(*) FROM ${dbB}.${dupTable} WHERE id = 6")[0][0])

    // ------------------------------------------------------------------------------------------------
    // 2. merge-on-write: a delete sign row on one side, no row of the key on the other, the visible rows are the same
    // ------------------------------------------------------------------------------------------------
    sql """
        CREATE TABLE ${dbA}.${mowTable} (
            `id` INT NOT NULL,
            `v1` VARCHAR(64) NULL,
            `v2` INT NULL
        )
        UNIQUE KEY(`id`)
        PARTITION BY RANGE(`id`)
        (
            PARTITION p1 VALUES LESS THAN ("1000")
        )
        DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES ("replication_num" = "1", "disable_auto_compaction" = "true",
                    "enable_unique_key_merge_on_write" = "true", "enable_mow_light_delete" = "false")
    """
    List<String> mowValues = []
    for (int id = 1; id <= 40; ++id) {
        if (id != 5) {
            mowValues.add("(${id}, 'value of ${id}', ${id})")
        }
    }
    sql "INSERT INTO ${dbA}.${mowTable} VALUES ${mowValues.join(",")}"
    sql "sync"
    String snapMow1 = "${suiteName}_snap_mow1"
    String tsMow1 = backupAndWait(dbA, snapMow1, [mowTable])
    // B is new: restored as a whole, and the table of B is stamped as a replica of the table of A
    restoreAndWait(dbB, snapMow1, tsMow1, [mowTable], null)
    assertEquals(rowsOf(dbA, mowTable), rowsOf(dbB, mowTable))
    // A: the key 5 is written and deleted, its latest row is a delete sign row. B: a key that A does not have is
    // written and deleted, the key 5 does not exist. The same number of versions, the same visible rows.
    sql "INSERT INTO ${dbA}.${mowTable} VALUES (5, 'transient', 1)"
    sql "DELETE FROM ${dbA}.${mowTable} WHERE id = 5"
    sql "INSERT INTO ${dbB}.${mowTable} VALUES (900, 'transient', 1)"
    sql "DELETE FROM ${dbB}.${mowTable} WHERE id = 900"
    sql "sync"
    assertEquals(rowsOf(dbA, mowTable), rowsOf(dbB, mowTable))
    assertEquals(versionsOf(dbA, mowTable), versionsOf(dbB, mowTable))
    assertEquals(0, (sql "SELECT COUNT(*) FROM ${dbA}.${mowTable} WHERE id = 5")[0][0])
    assertEquals(0, (sql "SELECT COUNT(*) FROM ${dbB}.${mowTable} WHERE id = 5")[0][0])

    String snapMow2 = "${suiteName}_snap_mow2"
    String tsMow2 = backupAndWait(dbA, snapMow2, [mowTable])
    // the default configs: the partition is a candidate (the versions are equal, the digests are equal because the
    // delete sign rows are skipped), but a unique table is not reused: rejected, downloaded as a whole
    def stats = restoreAndWait(dbB, snapMow2, tsMow2, [mowTable], null)
    assertEquals(0, keptPartitions())
    assertEquals(0, (lastEstimate?.incremental_partitions ?: 0) as int)
    assertEquals(1, lastEstimate.l0_passed_but_hidden_state_mow as int)
    assertEquals(0, lastEstimate.reusable as int)
    assertTrue((stats.downloaded_bytes as long) > 0)
    assertEquals(0L, stats.kept_bytes as long)
    assertEquals(rowsOf(dbA, mowTable), rowsOf(dbB, mowTable))
    assertEquals(versionsOf(dbA, mowTable), versionsOf(dbB, mowTable))

    // a partial update of an existing key and of a new key, with the new keys rejected: the same on both sides, as
    // both are the same data now (B was downloaded whole)
    def partialUpdate = { String db, int id ->
        String outcome
        sql "SET enable_unique_key_partial_update = true"
        sql "SET enable_insert_strict = false"
        sql "SET partial_update_new_key_behavior = \"ERROR\""
        try {
            sql "INSERT INTO ${db}.${mowTable}(id, v1) VALUES (${id}, 'partial update')"
            outcome = "OK"
        } catch (Exception e) {
            logger.info("partial update of ${id} in ${db}: ${e.getMessage()}")
            outcome = "ERROR"
        } finally {
            sql "SET partial_update_new_key_behavior = \"APPEND\""
            sql "SET enable_unique_key_partial_update = false"
            sql "SET enable_insert_strict = true"
        }
        return outcome
    }
    for (int id : [5, 3, 900, 777]) {
        assertEquals(partialUpdate(dbA, id), partialUpdate(dbB, id), "partial update of key ${id}")
    }
    // a new key is rejected on both sides
    assertEquals("ERROR", partialUpdate(dbA, 778))
    assertEquals("ERROR", partialUpdate(dbB, 778))
    sql "sync"
    assertEquals(rowsOf(dbA, mowTable), rowsOf(dbB, mowTable))
    assertEquals(versionsOf(dbA, mowTable), versionsOf(dbB, mowTable))

    // The control group: with the experiment switch on, the same partition IS reused, and then the same partial update
    // behaves differently on the two sides. This is why the reuse of a unique table is rejected by default.
    // C is built as B was before the rejected restore: restored from the first backup, then a key that A does not have
    // is written and deleted, so it has the same version and the same visible rows as A at the second backup.
    // D is a whole download of the second backup, the pristine copy of A (A itself was changed by the updates above).
    restoreAndWait(dbC, snapMow1, tsMow1, [mowTable], null)
    sql "INSERT INTO ${dbC}.${mowTable} VALUES (900, 'transient', 1)"
    sql "DELETE FROM ${dbC}.${mowTable} WHERE id = 900"
    sql "sync"
    restoreAndWait(dbD, snapMow2, tsMow2, [mowTable], "disable")
    assertEquals(rowsOf(dbD, mowTable), rowsOf(dbC, mowTable))
    assertEquals(versionsOf(dbD, mowTable), versionsOf(dbC, mowTable))
    setConfig("restore_reuse_allow_hidden_state_models", "true")
    restoreAndWait(dbC, snapMow2, tsMow2, [mowTable], null)
    setConfig("restore_reuse_allow_hidden_state_models", "false")
    assertEquals(1, keptPartitions())
    assertEquals(rowsOf(dbD, mowTable), rowsOf(dbC, mowTable))
    // key 5: a delete sign row in D, no row in C
    String outcomeD = partialUpdate(dbD, 5)
    String outcomeC = partialUpdate(dbC, 5)
    assertEquals("OK", outcomeD)
    assertEquals("ERROR", outcomeC)

    // ------------------------------------------------------------------------------------------------
    // 3. merge-on-read: rejected, downloaded as a whole
    // ------------------------------------------------------------------------------------------------
    sql """
        CREATE TABLE ${dbA}.${morTable} (
            `id` INT NOT NULL,
            `v1` VARCHAR(64) NULL
        )
        UNIQUE KEY(`id`)
        PARTITION BY RANGE(`id`)
        (
            PARTITION p1 VALUES LESS THAN ("1000")
        )
        DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES ("replication_num" = "1", "disable_auto_compaction" = "true",
                    "enable_unique_key_merge_on_write" = "false")
    """
    sql "INSERT INTO ${dbA}.${morTable} SELECT CAST(number AS INT), CONCAT('old ', CAST(number AS STRING)) FROM numbers('number' = '40')"
    // overwritten versions of some keys: not visible, but in the data
    sql "INSERT INTO ${dbA}.${morTable} VALUES (3, 'overwritten 3'), (4, 'overwritten 4')"
    sql "sync"
    String snapMor = "${suiteName}_snap_mor"
    String tsMor = backupAndWait(dbA, snapMor, [morTable])
    restoreAndWait(dbB, snapMor, tsMor, [morTable], null)
    assertEquals(rowsOf(dbA, morTable), rowsOf(dbB, morTable))
    stats = restoreAndWait(dbB, snapMor, tsMor, [morTable], "full")
    assertEquals(0, keptPartitions())
    assertEquals(1, lastEstimate.l0_passed_but_hidden_state_mor as int)
    assertEquals(0, lastEstimate.reusable as int)
    assertTrue((stats.downloaded_bytes as long) > 0)
    assertEquals(0L, stats.kept_bytes as long)
    assertEquals(rowsOf(dbA, morTable), rowsOf(dbB, morTable))
    // and the same DELETE on both sides leaves the same rows
    sql "DELETE FROM ${dbA}.${morTable} WHERE id = 3"
    sql "DELETE FROM ${dbB}.${morTable} WHERE id = 3"
    sql "sync"
    assertEquals(rowsOf(dbA, morTable), rowsOf(dbB, morTable))

    // ------------------------------------------------------------------------------------------------
    // clean up, and restore the configs
    // ------------------------------------------------------------------------------------------------
    setConfig("restore_reuse_min_partition_bytes", "0")
    setConfig("enable_restore_partition_reuse", "false")
    for (String db : [dbA, dbB, dbC, dbD]) {
        sql "DROP DATABASE ${db} FORCE"
    }
    sql "DROP REPOSITORY `${repoName}`"
}
