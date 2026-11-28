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

// Partition level reuse of the unique merge-on-write tables without a sequence column (the logical digest of the
// algorithm version 2, which covers the rows with a delete sign), with restore_reuse_mow on.
//   1. A is backed up and restored to B, A writes into p1 and is backed up again: p2 and p3 of B are kept, p1 is
//      appended from the backup. Then the same writes (upsert, DELETE, partial update of an existing key, of a deleted
//      key and of a new key with partial_update_new_key_behavior = ERROR) are run on B and on C, a whole download of
//      the same backup: the outcome of every statement and the rows are the same.
//      With restore_reuse_mow = false nothing of the table is reused (HIDDEN_STATE_MOW).
//   2. The counterexample of the version 1 digest: the latest row of a key is a delete sign row on one side, the key
//      does not exist on the other side. The visible rows are the same, the digests are not: the partition is
//      downloaded as a whole (DOWNLOAD_ROOT_MISMATCH), and a partial update behaves the same on both sides.
//   3. A table with a sequence column is never reused (HIDDEN_STATE_MOW_SEQUENCE), whatever the switch says.
// Single replica, auto compaction is off, all the configs it changes are restored at the end.
suite("test_backup_restore_mow_reuse", "backup_restore") {
    // enable_backup_manifest is off by default in branch-3.1: the case depends on the manifest of the backup,
    // turn it on for the whole case (restored when the case ends). The body is not indented, to keep the diff small.
    setFeConfigTemporary([enable_backup_manifest: true, enable_restore_partition_reuse: false,
            enable_restore_incremental_append: false, restore_reuse_min_partition_bytes: 0,
            restore_reuse_default_check_level: "full", restore_reuse_allow_hidden_state_models: false,
            restore_reuse_mow: false]) {
    String suiteName = "test_backup_restore_mow_reuse"
    String repoName = "${suiteName}_repo_" + UUID.randomUUID().toString().replace("-", "")
    String dbA = "${suiteName}_a"
    String dbB = "${suiteName}_b"
    String dbC = "${suiteName}_c"
    String mowTable = "${suiteName}_mow"
    String delTable = "${suiteName}_del"
    String seqTable = "${suiteName}_seq"
    int numPartitions = 3

    def syncer = getSyncer()
    syncer.createS3Repository(repoName)

    def setConfig = { String name, String value ->
        sql """ADMIN SET FRONTEND CONFIG ("${name}" = "${value}")"""
    }
    def getConfig = { String name ->
        def rows = sql_return_maparray """ADMIN SHOW FRONTEND CONFIG LIKE "${name}" """
        return rows.find { it.Key == name }.Value as String
    }
    // the configs this case changes; a run that failed half way may have left them changed, so read them back
    String origMow = getConfig("restore_reuse_mow")
    String origHidden = getConfig("restore_reuse_allow_hidden_state_models")
    setConfig("enable_restore_partition_reuse", "true")
    setConfig("enable_restore_incremental_append", "true")
    setConfig("restore_reuse_min_partition_bytes", "0")
    setConfig("restore_reuse_default_check_level", "full")
    // the version 1 digest of a merge-on-write table is never trusted, the switch of the hidden state models does not
    // reuse a merge-on-write table
    setConfig("restore_reuse_allow_hidden_state_models", "false")
    setConfig("restore_reuse_mow", "true")

    for (String db : [dbA, dbB, dbC]) {
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
    def lastEstimate = null
    // a field the estimate leaves out (a level that judges nothing, e.g. disable) counts as 0
    def est = { String name -> (lastEstimate?.get(name) ?: 0) as int }
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
        lastEstimate = new JsonSlurper().parseText(job.ReuseEstimate as String)
        logger.info("download stats: ${lastStats}, reuse estimate: ${lastEstimate}")
        return lastStats
    }
    // the row count and the hash of all the columns
    def rowsOf = { String db, String tbl ->
        def r = sql """SELECT COUNT(*), SUM(CRC32(CONCAT(CAST(id AS STRING), '|', IFNULL(v1, 'NULL'), '|',
                       IFNULL(CAST(v2 AS STRING), 'NULL')))) FROM ${db}.${tbl}"""
        return "${r[0][0]}:${r[0][1]}"
    }
    def versionsOf = { String db, String tbl ->
        def parts = sql_return_maparray "SHOW PARTITIONS FROM ${db}.${tbl}"
        return parts.collectEntries { [(it.PartitionName): it.VisibleVersion as String] }
    }
    // the local data sizes are known by the report of the backends
    // The kept bytes of SHOW RESTORE are the data sizes of the local replicas, which the tablet stat of the FE
    // sets every tablet_stat_update_interval_second: wait until every tablet has one (the replicas made by a restore
    // start with no size and no rows).
    def waitDataSize = { String db, String tbl ->
        for (int i = 0; i < 30; ++i) {
            def tablets = sql_return_maparray "SHOW TABLETS FROM ${db}.${tbl}"
            if (!tablets.any { (it.LocalDataSize as long) == 0 }) {
                return
            }
            sleep(5000)
        }
    }
    // a fixed partial update that must not insert a new key: the outcome is OK or ERROR
    def partialUpdate = { String db, String tbl, int id ->
        String outcome
        sql "SET enable_unique_key_partial_update = true"
        sql "SET enable_insert_strict = false"
        sql "SET partial_update_new_key_behavior = \"ERROR\""
        try {
            sql "INSERT INTO ${db}.${tbl}(id, v1) VALUES (${id}, 'partial update')"
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

    // ------------------------------------------------------------------------------------------------
    // 1. reuse and the incremental append, then the same writes on the reused table and on a whole download
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
            PARTITION p1 VALUES LESS THAN ("1000"),
            PARTITION p2 VALUES LESS THAN ("2000"),
            PARTITION p3 VALUES LESS THAN ("3000")
        )
        DISTRIBUTED BY HASH(`id`) BUCKETS 2
        PROPERTIES ("replication_num" = "1", "disable_auto_compaction" = "true",
                    "enable_unique_key_merge_on_write" = "true")
    """
    for (int b = 0; b < 3; ++b) {
        List<String> values = []
        for (int p = 0; p < numPartitions; ++p) {
            for (int i = 0; i < 200; ++i) {
                int id = p * 1000 + b * 200 + i
                values.add("(${id}, 'value of ${id} in batch ${b} with some padding to make it larger', ${id})")
            }
        }
        sql "INSERT INTO ${dbA}.${mowTable} VALUES ${values.join(",")}"
    }
    // a delete sign row in every partition, which stays in the data (the key is gone from the visible rows)
    sql "INSERT INTO ${dbA}.${mowTable} VALUES (700, 'transient', 1), (1700, 'transient', 1), (2700, 'transient', 1)"
    sql "DELETE FROM ${dbA}.${mowTable} WHERE id IN (700, 1700, 2700)"
    sql "sync"

    String snap1 = "${suiteName}_snap1"
    String ts1 = backupAndWait(dbA, snap1, [mowTable])
    // B is new: everything is downloaded
    restoreAndWait(dbB, snap1, ts1, [mowTable], null)
    assertEquals(rowsOf(dbA, mowTable), rowsOf(dbB, mowTable))
    assertEquals(versionsOf(dbA, mowTable), versionsOf(dbB, mowTable))
    long baselineBytes = lastStats.downloaded_bytes as long
    assertTrue(baselineBytes > 0)
    waitDataSize(dbB, mowTable)

    // A writes into p1: an update, a new key, a DELETE, the key of a delete sign row written again
    sql "INSERT INTO ${dbA}.${mowTable} VALUES (5, 'updated in p1', 5), (900, 'new row of p1', 900), (700, 'again', 7)"
    sql "DELETE FROM ${dbA}.${mowTable} WHERE id = 7"
    sql "sync"
    String snap2 = "${suiteName}_snap2"
    String ts2 = backupAndWait(dbA, snap2, [mowTable])
    waitDataSize(dbB, mowTable)
    def stats = restoreAndWait(dbB, snap2, ts2, [mowTable], null)
    // p2 and p3 are kept, p1 is appended with the rowsets after its version
    assertEquals(numPartitions - 1, est("kept_partitions"))
    assertEquals(1, est("incremental_partitions"))
    assertEquals(0, est("l0_passed_but_hidden_state_mow"))
    assertTrue((stats.incremental_bytes as long) > 0)
    assertTrue((stats.downloaded_bytes as long) * 3 < baselineBytes)
    assertEquals(rowsOf(dbA, mowTable), rowsOf(dbB, mowTable))
    assertEquals(versionsOf(dbA, mowTable), versionsOf(dbB, mowTable))
    // C: a whole download of the same backup, the control group
    restoreAndWait(dbC, snap2, ts2, [mowTable], "disable")
    assertEquals(0, est("kept_partitions"))
    assertEquals(rowsOf(dbC, mowTable), rowsOf(dbB, mowTable))
    assertEquals(versionsOf(dbC, mowTable), versionsOf(dbB, mowTable))

    // the switch off: B is the same as the backup now, but nothing of the table is reused
    setConfig("restore_reuse_mow", "false")
    try {
        stats = restoreAndWait(dbB, snap2, ts2, [mowTable], null)
    } finally {
        setConfig("restore_reuse_mow", "true")
    }
    assertEquals(0, est("kept_partitions"))
    assertEquals(0, (lastEstimate.incremental_partitions ?: 0) as int)
    assertEquals(numPartitions, est("l0_passed_but_hidden_state_mow"))
    assertEquals(0, est("reusable"))
    assertEquals(0L, stats.kept_bytes as long)
    assertTrue((stats.downloaded_bytes as long) > 0)
    assertEquals(rowsOf(dbA, mowTable), rowsOf(dbB, mowTable))
    // and with the switch on again, the same restore keeps all the partitions
    waitDataSize(dbB, mowTable)
    stats = restoreAndWait(dbB, snap2, ts2, [mowTable], null)
    assertEquals(numPartitions, est("kept_partitions"))
    assertTrue((stats.kept_bytes as long) > 0)
    assertEquals(rowsOf(dbA, mowTable), rowsOf(dbB, mowTable))

    // the same writes on the reused table and on the whole download: every statement has the same outcome, and the
    // rows are the same row by row
    def writeBoth = { Closure write ->
        def outcomes = []
        for (String db : [dbB, dbC]) {
            outcomes.add(write(db))
        }
        assertEquals(outcomes[0], outcomes[1])
    }
    writeBoth { String db ->
        sql "INSERT INTO ${db}.${mowTable} VALUES (11, 'upsert of an existing key', 11), (2901, 'upsert of a new key', 1)"
        sql "DELETE FROM ${db}.${mowTable} WHERE id IN (12, 1012)"
        sql "INSERT INTO ${db}.${mowTable} VALUES (12, 'written again after the delete', 12)"
        sql "sync"
        return "done"
    }
    // keys: an existing one (10), one that was deleted by the writes of A (7), one whose delete sign row is in the
    // data (700, 1700, written again at 700), the deleted ones of the writes above (1012), and new ones
    writeBoth { String db ->
        return [10, 7, 700, 1700, 2700, 1012, 12, 778, 2998].collect { int id -> partialUpdate(db, mowTable, id) }
    }
    writeBoth { String db ->
        sql "INSERT INTO ${db}.${mowTable} VALUES (7, 'upsert after the partial updates', 7), (1700, 'back', 2)"
        sql "DELETE FROM ${db}.${mowTable} WHERE id = 10"
        sql "sync"
        return "done"
    }
    assertEquals(rowsOf(dbC, mowTable), rowsOf(dbB, mowTable))
    assertEquals(versionsOf(dbC, mowTable), versionsOf(dbB, mowTable))
    // a key with a delete sign row is treated the same on both sides, a new key is rejected
    assertEquals(partialUpdate(dbB, mowTable, 2700), partialUpdate(dbC, mowTable, 2700))
    assertEquals("ERROR", partialUpdate(dbB, mowTable, 779))
    assertEquals("ERROR", partialUpdate(dbC, mowTable, 779))

    // ------------------------------------------------------------------------------------------------
    // 2. a delete sign row on one side, the key does not exist on the other side
    // ------------------------------------------------------------------------------------------------
    sql """
        CREATE TABLE ${dbA}.${delTable} (
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
                    "enable_unique_key_merge_on_write" = "true")
    """
    List<String> delValues = []
    for (int id = 1; id <= 40; ++id) {
        if (id != 5) {
            delValues.add("(${id}, 'value of ${id}', ${id})")
        }
    }
    sql "INSERT INTO ${dbA}.${delTable} VALUES ${delValues.join(",")}"
    sql "sync"
    String snapDel1 = "${suiteName}_snap_del1"
    String tsDel1 = backupAndWait(dbA, snapDel1, [delTable])
    restoreAndWait(dbB, snapDel1, tsDel1, [delTable], null)
    assertEquals(rowsOf(dbA, delTable), rowsOf(dbB, delTable))
    // A: the key 5 is written and deleted, its latest row is a delete sign row. B: a key that A does not have is
    // written and deleted, the key 5 does not exist. The same number of versions, the same visible rows.
    sql "INSERT INTO ${dbA}.${delTable} VALUES (5, 'transient', 1)"
    sql "DELETE FROM ${dbA}.${delTable} WHERE id = 5"
    sql "INSERT INTO ${dbB}.${delTable} VALUES (900, 'transient', 1)"
    sql "DELETE FROM ${dbB}.${delTable} WHERE id = 900"
    sql "sync"
    assertEquals(rowsOf(dbA, delTable), rowsOf(dbB, delTable))
    assertEquals(versionsOf(dbA, delTable), versionsOf(dbB, delTable))
    String snapDel2 = "${suiteName}_snap_del2"
    String tsDel2 = backupAndWait(dbA, snapDel2, [delTable])
    waitDataSize(dbB, delTable)
    stats = restoreAndWait(dbB, snapDel2, tsDel2, [delTable], null)
    // the visible rows are the same, the digests of the version 2 are not: downloaded as a whole
    assertEquals(0, est("kept_partitions"))
    assertEquals(0, (lastEstimate.incremental_partitions ?: 0) as int)
    assertEquals(1, est("download_digest_mismatch"))
    assertEquals(0, est("l0_passed_but_hidden_state_mow"))
    assertTrue((stats.downloaded_bytes as long) > 0)
    assertEquals(0L, stats.kept_bytes as long)
    assertEquals(rowsOf(dbA, delTable), rowsOf(dbB, delTable))
    // B is now what A is: the key 5 has a delete sign row on both sides, the same partial update gives the same outcome
    // (the key exists, the transient key of B is gone)
    for (int id : [5, 3, 900, 777]) {
        assertEquals(partialUpdate(dbA, delTable, id), partialUpdate(dbB, delTable, id), "partial update of ${id}")
    }
    assertEquals("ERROR", partialUpdate(dbB, delTable, 778))
    sql "sync"
    assertEquals(rowsOf(dbA, delTable), rowsOf(dbB, delTable))

    // ------------------------------------------------------------------------------------------------
    // 3. a sequence column: never reused
    // ------------------------------------------------------------------------------------------------
    sql """
        CREATE TABLE ${dbA}.${seqTable} (
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
                    "enable_unique_key_merge_on_write" = "true", "function_column.sequence_col" = "v2")
    """
    sql "INSERT INTO ${dbA}.${seqTable} VALUES (1, 'a', 20), (2, 'b', 20), (3, 'c', 20)"
    sql "INSERT INTO ${dbA}.${seqTable} VALUES (1, 'a2', 21)"
    sql "DELETE FROM ${dbA}.${seqTable} WHERE id = 2"
    // a late write which loses against the delete sign row by the sequence value
    sql "INSERT INTO ${dbA}.${seqTable} VALUES (2, 'late', 15)"
    sql "sync"
    String snapSeq = "${suiteName}_snap_seq"
    String tsSeq = backupAndWait(dbA, snapSeq, [seqTable])
    restoreAndWait(dbB, snapSeq, tsSeq, [seqTable], null)
    assertEquals(rowsOf(dbA, seqTable), rowsOf(dbB, seqTable))
    waitDataSize(dbB, seqTable)
    // B restores the same backup again: the partition is the same as the backup, but is not reused
    stats = restoreAndWait(dbB, snapSeq, tsSeq, [seqTable], null)
    assertEquals(0, est("kept_partitions"))
    assertEquals(1, est("l0_passed_but_hidden_state_mow_sequence"))
    assertEquals(0, est("reusable"))
    assertEquals(0L, stats.kept_bytes as long)
    assertTrue((stats.downloaded_bytes as long) > 0)
    assertEquals(rowsOf(dbA, seqTable), rowsOf(dbB, seqTable))
    // the later write of the sequence value 12 is rejected the same way on both sides
    for (String db : [dbA, dbB]) {
        sql "INSERT INTO ${db}.${seqTable} VALUES (2, 'seq 12', 12)"
    }
    sql "sync"
    assertEquals(rowsOf(dbA, seqTable), rowsOf(dbB, seqTable))

    // ------------------------------------------------------------------------------------------------
    // clean up, and restore the configs
    // ------------------------------------------------------------------------------------------------
    setConfig("restore_reuse_mow", origMow)
    setConfig("restore_reuse_allow_hidden_state_models", origHidden)
    setConfig("restore_reuse_min_partition_bytes", "0")
    setConfig("restore_reuse_default_check_level", "full")
    for (String db : [dbA, dbB, dbC]) {
        sql "DROP DATABASE ${db} FORCE"
    }
    sql "DROP REPOSITORY `${repoName}`"
    }
}
