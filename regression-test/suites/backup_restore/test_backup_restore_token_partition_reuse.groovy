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

// A non-atomic restore onto the table restored from the same backup keeps the partitions by their state token
// (restore_state_token_track, restore_reuse_trust_state_token), with the default backup (no logical digest, no
// manifest). The result is compared with the source (rows, partition versions).
//   1. The tables do not exist: restored as new tables, the partitions adopt the tokens of the backup.
//   2. The same backup again: every partition is kept by its token.
//   3. enable_restore_partition_reuse off, or restore_reuse_trust_state_token off: nothing is kept; on again: kept.
//   4. A full compaction on the target does not change the token: the partitions are still kept.
//   5. A write into p1 of the source: p1 has another token in the backup and is downloaded, the others are kept.
// The ReuseEstimate column of SHOW RESTORE has the counts. Single replica.
suite("test_backup_restore_token_partition_reuse", "backup_restore") {
    setFeConfigTemporary([enable_restore_partition_reuse: true, restore_reuse_min_partition_bytes: 0,
            restore_reuse_allow_hidden_state_models: false, restore_reuse_mow: true,
            restore_state_token_track: true, restore_reuse_trust_state_token: true,
            restore_reset_index_id: false]) {
    String suiteName = "test_backup_restore_token_partition_reuse"
    String repoName = "${suiteName}_repo_" + UUID.randomUUID().toString().replace("-", "")
    String srcDbName = "${suiteName}_src_db"
    String dbName = "${suiteName}_db"
    String dupTable = "${suiteName}_dup"
    String mowTable = "${suiteName}_mow"
    int numPartitions = 3
    int numBatches = 4

    def syncer = getSyncer()
    syncer.createS3Repository(repoName)

    def setConfig = { String name, String value ->
        sql """ADMIN SET FRONTEND CONFIG ("${name}" = "${value}")"""
    }

    for (String db : [srcDbName, dbName]) {
        sql "DROP DATABASE IF EXISTS ${db} FORCE"
        sql "CREATE DATABASE ${db}"
    }
    for (String tbl : [dupTable, mowTable]) {
        String keys = tbl == dupTable ? "DUPLICATE KEY(`id`)" : "UNIQUE KEY(`id`)"
        String props = tbl == dupTable ? "" : ", \"enable_unique_key_merge_on_write\" = \"true\""
        sql """
            CREATE TABLE ${srcDbName}.${tbl} (
                `id` INT NOT NULL,
                `value` VARCHAR(32) NULL
            )
            ${keys}
            PARTITION BY RANGE(`id`)
            (
                PARTITION p1 VALUES LESS THAN ("1000"),
                PARTITION p2 VALUES LESS THAN ("2000"),
                PARTITION p3 VALUES LESS THAN ("3000")
            )
            DISTRIBUTED BY HASH(`id`) BUCKETS 2
            PROPERTIES ("replication_num" = "1"${props})
            """
    }
    // several batches per partition, so a partition has several rowsets for the compaction to merge
    for (int b = 0; b < numBatches; ++b) {
        for (String tbl : [dupTable, mowTable]) {
            List<String> values = []
            for (int p = 0; p < numPartitions; ++p) {
                for (int i = 0; i < 20; ++i) {
                    int id = p * 1000 + b * 20 + i
                    values.add("(${id}, 'v${id}_b${b}')")
                }
            }
            sql "INSERT INTO ${srcDbName}.${tbl} VALUES ${values.join(",")}"
        }
    }
    sql "sync"

    // the default backup: no logical digest, no manifest
    def backupAndWait = { String snapshot ->
        sql """
            BACKUP SNAPSHOT ${srcDbName}.${snapshot}
            TO `${repoName}`
            ON (${dupTable}, ${mowTable})
        """
        syncer.waitSnapshotFinish(srcDbName)
        def timestamp = syncer.getSnapshotTimestamp(repoName, snapshot)
        assertTrue(timestamp != null)
        return timestamp
    }
    // returns the ReuseEstimate of the restore job, which must be FINISHED
    def restoreAndWait = { String snapshot, String timestamp ->
        sql """
            RESTORE SNAPSHOT ${dbName}.${snapshot}
            FROM `${repoName}`
            ON (${dupTable}, ${mowTable})
            PROPERTIES ("backup_timestamp" = "${timestamp}", "reserve_replica" = "true")
        """
        syncer.waitAllRestoreFinish(dbName)
        def result = sql_return_maparray """ SHOW RESTORE FROM ${dbName} WHERE Label = "${snapshot}" """
        def job = result.last()
        logger.info("restore ${snapshot} into ${dbName}: ${job}")
        assertEquals("FINISHED", job.State)
        def estimate = new JsonSlurper().parseText(job.ReuseEstimate as String)
        logger.info("reuse estimate: ${estimate}")
        return estimate
    }
    def rowsOf = { String db, String tbl ->
        def r = sql """SELECT COUNT(*), SUM(CRC32(CONCAT(CAST(id AS STRING), '|', value))) FROM ${db}.${tbl}"""
        return "${r[0][0]}:${r[0][1]}".toString()
    }
    def versionsOf = { String db, String tbl ->
        def parts = sql_return_maparray "SHOW PARTITIONS FROM ${db}.${tbl}"
        return parts.collectEntries { [(it.PartitionName): it.VisibleVersion as String] }
    }
    def assertSameAsSource = { ->
        for (String tbl : [dupTable, mowTable]) {
            assertEquals(rowsOf(srcDbName, tbl), rowsOf(dbName, tbl))
            assertEquals(versionsOf(srcDbName, tbl), versionsOf(dbName, tbl))
        }
    }
    def keptByToken = { estimate -> (estimate.kept_token ?: 0) as int }
    def keptPartitions = { estimate -> (estimate.kept_partitions ?: 0) as int }

    String snap1 = "${suiteName}_snap1"
    String ts1 = backupAndWait(snap1)

    // 1. the tables do not exist: restored as new tables, everything is downloaded
    def estimate = restoreAndWait(snap1, ts1)
    assertEquals(0, keptPartitions(estimate))
    assertSameAsSource()

    // 2. the same backup again: every partition is kept by its token
    estimate = restoreAndWait(snap1, ts1)
    assertEquals(2 * numPartitions, keptByToken(estimate))
    assertEquals(2 * numPartitions, keptPartitions(estimate))
    assertEquals(2 * numPartitions, (estimate.token_equal ?: 0) as int)
    assertTrue((estimate.kept_bytes_single_replica as long) >= 0)
    assertSameAsSource()
    order_qt_kept_dup """SELECT id DIV 1000, COUNT(*), SUM(CRC32(CONCAT(CAST(id AS STRING), '|', value)))
            FROM ${dbName}.${dupTable} GROUP BY id DIV 1000"""
    order_qt_kept_mow """SELECT id DIV 1000, COUNT(*), SUM(CRC32(CONCAT(CAST(id AS STRING), '|', value)))
            FROM ${dbName}.${mowTable} GROUP BY id DIV 1000"""

    // 3. the switch off, or the trust off: nothing is kept, everything is downloaded
    setConfig("enable_restore_partition_reuse", "false")
    estimate = restoreAndWait(snap1, ts1)
    setConfig("enable_restore_partition_reuse", "true")
    assertEquals(0, keptPartitions(estimate))
    assertSameAsSource()
    setConfig("restore_reuse_trust_state_token", "false")
    estimate = restoreAndWait(snap1, ts1)
    setConfig("restore_reuse_trust_state_token", "true")
    assertEquals(0, keptByToken(estimate))
    assertEquals(0, keptPartitions(estimate))
    assertSameAsSource()
    order_qt_not_trusted """SELECT id DIV 1000, COUNT(*), SUM(CRC32(CONCAT(CAST(id AS STRING), '|', value)))
            FROM ${dbName}.${dupTable} GROUP BY id DIV 1000"""
    // the downloaded partitions adopted the tokens again: kept
    estimate = restoreAndWait(snap1, ts1)
    assertEquals(2 * numPartitions, keptByToken(estimate))
    assertSameAsSource()

    // 4. a full compaction on the target changes the files, not the version or the token: still kept
    def versionCount = { String db, String tbl ->
        def tablets = sql_return_maparray "SHOW TABLETS FROM ${db}.${tbl}"
        return tablets.collect { it.VersionCount as int }.sum()
    }
    // the version count of a replica is -1 until the first tablet report of its backend after the restore
    def reported = { String db, String tbl ->
        def tablets = sql_return_maparray "SHOW TABLETS FROM ${db}.${tbl}"
        return tablets.every { (it.VersionCount as int) >= 0 }
    }
    for (int i = 0; i < 20 && !reported(dbName, dupTable); ++i) {
        sleep(10000)
    }
    assertTrue(reported(dbName, dupTable))
    int before = versionCount(dbName, dupTable)
    for (String tbl : [dupTable, mowTable]) {
        def tablets = sql_return_maparray "SHOW TABLETS FROM ${dbName}.${tbl}"
        for (def tablet : tablets) {
            String url = (tablet.CompactionStatus as String).replace("compaction/show", "compaction/run") +
                    "&compact_type=full"
            def (code, out, err) = curl("POST", url)
            logger.info("full compaction of tablet ${tablet.TabletId}: ${code} ${out}")
        }
    }
    // the version count is refreshed by the tablet report of the backends
    int after = before
    for (int i = 0; i < 20 && after >= before; ++i) {
        sleep(10000)
        after = versionCount(dbName, dupTable)
    }
    logger.info("version count of ${dupTable}: ${before} -> ${after}")
    assertTrue(after < before)
    estimate = restoreAndWait(snap1, ts1)
    assertEquals(2 * numPartitions, keptByToken(estimate))
    assertSameAsSource()
    order_qt_after_compaction """SELECT id DIV 1000, COUNT(*), SUM(CRC32(CONCAT(CAST(id AS STRING), '|', value)))
            FROM ${dbName}.${dupTable} GROUP BY id DIV 1000"""

    // 5. a write to p1 of the source, then back up again: p1 is downloaded, the others are kept
    sql "INSERT INTO ${srcDbName}.${dupTable} VALUES (5, 'new')"
    sql "INSERT INTO ${srcDbName}.${mowTable} VALUES (5, 'new')"
    sql "sync"
    String snap2 = "${suiteName}_snap2"
    String ts2 = backupAndWait(snap2)
    estimate = restoreAndWait(snap2, ts2)
    assertEquals(2 * (numPartitions - 1), keptByToken(estimate))
    assertEquals(2 * (numPartitions - 1), keptPartitions(estimate))
    assertSameAsSource()
    order_qt_p1_written_dup """SELECT id DIV 1000, COUNT(*), SUM(CRC32(CONCAT(CAST(id AS STRING), '|', value)))
            FROM ${dbName}.${dupTable} GROUP BY id DIV 1000"""
    order_qt_p1_written_mow """SELECT id DIV 1000, COUNT(*), SUM(CRC32(CONCAT(CAST(id AS STRING), '|', value)))
            FROM ${dbName}.${mowTable} GROUP BY id DIV 1000"""
    // the same backup again: everything is kept
    estimate = restoreAndWait(snap2, ts2)
    assertEquals(2 * numPartitions, keptByToken(estimate))
    assertSameAsSource()

    sql "DROP REPOSITORY `${repoName}`"
    }
}
