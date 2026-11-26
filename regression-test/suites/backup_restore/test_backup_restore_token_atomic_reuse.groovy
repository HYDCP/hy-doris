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

// An atomic restore that keeps the partitions by their state token (restore_state_token_track,
// restore_reuse_trust_state_token), with a backup that has neither a logical digest nor a manifest (the default
// backup). The kept partitions are loaded into the staging tablets by the snapshot task
// (restore_reuse_kept_load_at_snapshot) from the local tablets of the table being replaced, nothing is downloaded for
// them, and the table is replaced in the end: the rows and the versions equal the source, the tablets are new.
//   1. A -> B: B does not have the tables, everything is downloaded, the partitions adopt the tokens of the backup.
//   2. The same backup again: every partition is kept by its token; and again, the new tables have the tokens.
//   3. A writes into p1: p1 has another token in the backup, it is downloaded, the others are kept.
//   4. enable_restore_atomic_reuse off: everything is downloaded, as before. On again: everything is kept.
//   5. A write into the table being replaced before it is replaced (the job is paused by the FE debug point): the
//      job fails, the table is not replaced and keeps the write; the next restore downloads that partition only.
//      Needs enable_debug_points=true on the FE, skipped otherwise.
// The ReuseEstimate column of SHOW RESTORE has the counts, the per partition reasons are in the fe.log
// ("restore reuse: partition ..."). Single replica, auto compaction off.
suite("test_backup_restore_token_atomic_reuse", "backup_restore,nonConcurrent") {
    setFeConfigTemporary([enable_restore_partition_reuse: true, enable_restore_atomic_reuse: true,
            restore_reuse_min_partition_bytes: 0, restore_reuse_allow_hidden_state_models: false,
            restore_reuse_mow: true, restore_reuse_kept_load_at_snapshot: true, restore_state_token_track: true,
            restore_reuse_trust_state_token: true, restore_reset_index_id: false]) {
    String suiteName = "test_backup_restore_token_atomic_reuse"
    String repoName = "${suiteName}_repo_" + UUID.randomUUID().toString().replace("-", "")
    String dbA = "${suiteName}_a"
    String dbB = "${suiteName}_b"
    String dupTable = "${suiteName}_dup"
    String mowTable = "${suiteName}_mow"
    String stagingPrefix = "__doris_atomic_restore_prefix__"
    int numPartitions = 3
    boolean debugPoints = getFeConfig("enable_debug_points").equals("true")

    def syncer = getSyncer()
    syncer.createS3Repository(repoName)

    def setConfig = { String name, String value ->
        sql """ADMIN SET FRONTEND CONFIG ("${name}" = "${value}")"""
    }

    for (String db : [dbA, dbB]) {
        sql "DROP DATABASE IF EXISTS ${db} FORCE"
        sql "CREATE DATABASE ${db}"
    }
    for (String tbl : [dupTable, mowTable]) {
        String keys = tbl == dupTable ? "DUPLICATE KEY(`id`)" : "UNIQUE KEY(`id`)"
        String props = tbl == dupTable ? "" : ", \"enable_unique_key_merge_on_write\" = \"true\""
        sql """
            CREATE TABLE ${dbA}.${tbl} (
                `id` INT NOT NULL,
                `value` VARCHAR(64) NULL
            )
            ${keys}
            PARTITION BY RANGE(`id`)
            (
                PARTITION p1 VALUES LESS THAN ("1000"),
                PARTITION p2 VALUES LESS THAN ("2000"),
                PARTITION p3 VALUES LESS THAN ("3000")
            )
            DISTRIBUTED BY HASH(`id`) BUCKETS 2
            PROPERTIES ("replication_num" = "1", "disable_auto_compaction" = "true"${props})
            """
        for (int b = 0; b < 3; ++b) {
            List<String> values = []
            for (int p = 0; p < numPartitions; ++p) {
                for (int i = 0; i < 100; ++i) {
                    int id = p * 1000 + b * 100 + i
                    values.add("(${id}, 'value of ${id} in batch ${b}')")
                }
            }
            sql "INSERT INTO ${dbA}.${tbl} VALUES ${values.join(",")}"
        }
    }
    sql "sync"

    // ---- helpers ----
    // the default backup: no logical digest, no manifest
    def backupAndWait = { String snapshot ->
        sql """
            BACKUP SNAPSHOT ${dbA}.${snapshot}
            TO `${repoName}`
            ON (${dupTable}, ${mowTable})
        """
        syncer.waitSnapshotFinish(dbA)
        def timestamp = syncer.getSnapshotTimestamp(repoName, snapshot)
        assertTrue(timestamp != null)
        return timestamp
    }
    def startRestore = { String snapshot, String timestamp ->
        sql """
            RESTORE SNAPSHOT ${dbB}.${snapshot}
            FROM `${repoName}`
            ON (${dupTable}, ${mowTable})
            PROPERTIES ("backup_timestamp" = "${timestamp}", "reserve_replica" = "true", "atomic_restore" = "true")
        """
    }
    def jobOf = { String snapshot ->
        def result = sql_return_maparray """ SHOW RESTORE FROM ${dbB} WHERE Label = "${snapshot}" """
        return result.isEmpty() ? null : result.last()
    }
    // waits for the job to be FINISHED or CANCELLED, returns its row of SHOW RESTORE
    def waitRestoreDone = { String snapshot, long timeoutSec ->
        def job = null
        for (long waited = 0; waited < timeoutSec; waited += 3) {
            job = jobOf(snapshot)
            if (job != null && (job.State == "FINISHED" || job.State == "CANCELLED")) {
                logger.info("restore ${snapshot}: ${job}")
                return job
            }
            sleep(3000)
        }
        assertTrue(false, "the restore job ${snapshot} is not done after ${timeoutSec}s, state: ${job?.State}")
    }
    // the ReuseEstimate of the restore, which must be FINISHED
    def restoreAndWait = { String snapshot, String timestamp ->
        startRestore(snapshot, timestamp)
        def job = waitRestoreDone(snapshot, 600)
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
            assertEquals(rowsOf(dbA, tbl), rowsOf(dbB, tbl))
            assertEquals(versionsOf(dbA, tbl), versionsOf(dbB, tbl))
        }
    }
    def tabletIdsOf = { String tbl ->
        def tablets = sql_return_maparray "SHOW TABLETS FROM ${dbB}.${tbl}"
        return tablets.collect { it.TabletId as long }.toSet()
    }
    def keptAtomic = { estimate -> (estimate.kept_atomic ?: 0) as int }
    def keptByToken = { estimate -> (estimate.kept_token ?: 0) as int }
    def keptPartitions = { estimate -> (estimate.kept_partitions ?: 0) as int }

    // ---- 1. A -> B: B does not have the tables, everything is downloaded ----
    String snap1 = "${suiteName}_1"
    String ts1 = backupAndWait(snap1)
    def estimate = restoreAndWait(snap1, ts1)
    assertEquals(0, keptPartitions(estimate))
    assertSameAsSource()
    order_qt_new_dup """SELECT id DIV 1000, COUNT(*), SUM(CRC32(CONCAT(CAST(id AS STRING), '|', value)))
            FROM ${dbB}.${dupTable} GROUP BY id DIV 1000"""
    order_qt_new_mow """SELECT id DIV 1000, COUNT(*), SUM(CRC32(CONCAT(CAST(id AS STRING), '|', value)))
            FROM ${dbB}.${mowTable} GROUP BY id DIV 1000"""

    // the local data size of the replicas is known by the report of the backends, kept_atomic_bytes counts it
    for (int i = 0; i < 30; ++i) {
        boolean ready = [dupTable, mowTable].every { String tbl ->
            !(sql_return_maparray("SHOW TABLETS FROM ${dbB}.${tbl}")).any { (it.LocalDataSize as long) == 0 }
        }
        if (ready) {
            break
        }
        sleep(5000)
    }

    // ---- 2. the same backup again: every partition is kept by its token, the tables are replaced ----
    def tabletsBefore = [dupTable, mowTable].collectEntries { [(it): tabletIdsOf(it)] }
    estimate = restoreAndWait(snap1, ts1)
    assertEquals(2 * numPartitions, keptAtomic(estimate))
    assertEquals(2 * numPartitions, keptByToken(estimate))
    assertEquals(2 * numPartitions, keptPartitions(estimate))
    assertEquals(2 * numPartitions, (estimate.token_equal ?: 0) as int)
    assertTrue((estimate.kept_atomic_bytes_all_replicas as long) > 0)
    assertSameAsSource()
    for (String tbl : [dupTable, mowTable]) {
        def tabletsAfter = tabletIdsOf(tbl)
        assertEquals(tabletsBefore[tbl].size(), tabletsAfter.size())
        // replaced by the staging table: the tablets are all new ones
        assertTrue(tabletsBefore[tbl].intersect(tabletsAfter).isEmpty())
    }
    assertTrue((sql "SHOW TABLES FROM ${dbB} LIKE '${stagingPrefix}%'").isEmpty())
    order_qt_kept_dup """SELECT id DIV 1000, COUNT(*), SUM(CRC32(CONCAT(CAST(id AS STRING), '|', value)))
            FROM ${dbB}.${dupTable} GROUP BY id DIV 1000"""
    order_qt_kept_mow """SELECT id DIV 1000, COUNT(*), SUM(CRC32(CONCAT(CAST(id AS STRING), '|', value)))
            FROM ${dbB}.${mowTable} GROUP BY id DIV 1000"""
    // and again: the replaced tables adopted the tokens of the backup
    estimate = restoreAndWait(snap1, ts1)
    assertEquals(2 * numPartitions, keptAtomic(estimate))
    assertEquals(2 * numPartitions, keptByToken(estimate))
    assertSameAsSource()

    // ---- 3. A writes into p1, the others are not changed: p1 is downloaded, p2 and p3 are kept ----
    sql "INSERT INTO ${dbA}.${dupTable} VALUES (5, 'new row of p1'), (5, 'another row of p1')"
    sql "INSERT INTO ${dbA}.${mowTable} VALUES (5, 'updated in p1'), (6, 'new row of p1')"
    sql "DELETE FROM ${dbA}.${mowTable} WHERE id = 7"
    sql "sync"
    String snap2 = "${suiteName}_2"
    String ts2 = backupAndWait(snap2)
    estimate = restoreAndWait(snap2, ts2)
    assertEquals(2 * (numPartitions - 1), keptAtomic(estimate))
    assertEquals(2 * (numPartitions - 1), keptByToken(estimate))
    assertSameAsSource()
    order_qt_p1_written_dup """SELECT id DIV 1000, COUNT(*), SUM(CRC32(CONCAT(CAST(id AS STRING), '|', value)))
            FROM ${dbB}.${dupTable} GROUP BY id DIV 1000"""
    order_qt_p1_written_mow """SELECT id DIV 1000, COUNT(*), SUM(CRC32(CONCAT(CAST(id AS STRING), '|', value)))
            FROM ${dbB}.${mowTable} GROUP BY id DIV 1000"""
    order_qt_p1_rows """SELECT id, value FROM ${dbB}.${mowTable} WHERE id IN (5, 6, 7)"""
    // the downloaded p1 adopted the new token: the same backup again keeps every partition
    estimate = restoreAndWait(snap2, ts2)
    assertEquals(2 * numPartitions, keptByToken(estimate))
    assertSameAsSource()

    // ---- 4. the switch off: the atomic restore downloads everything, as before; on again: everything is kept ----
    setConfig("enable_restore_atomic_reuse", "false")
    estimate = restoreAndWait(snap2, ts2)
    assertEquals(0, keptAtomic(estimate))
    assertEquals(0, keptPartitions(estimate))
    assertTrue(!estimate.containsKey("kept_atomic"))
    assertSameAsSource()
    setConfig("enable_restore_atomic_reuse", "true")
    estimate = restoreAndWait(snap2, ts2)
    assertEquals(2 * numPartitions, keptAtomic(estimate))
    assertEquals(2 * numPartitions, keptByToken(estimate))
    assertSameAsSource()
    order_qt_switch_on_dup """SELECT id DIV 1000, COUNT(*), SUM(CRC32(CONCAT(CAST(id AS STRING), '|', value)))
            FROM ${dbB}.${dupTable} GROUP BY id DIV 1000"""

    // ---- 5. a write into the table being replaced before it is replaced: the job fails, the table is kept ----
    if (debugPoints) {
        // the label of a restore is the name of the backup snapshot
        try {
            GetDebugPoint().enableDebugPointForAllFEs("FE.PAUSE_NON_PENDING_RESTORE_JOB", [value: snap2])
            startRestore(snap2, ts2)
            def paused = null
            for (int i = 0; i < 120; ++i) {
                paused = jobOf(snap2)
                if (paused != null && paused.State == "SNAPSHOTING") {
                    break
                }
                sleep(1000)
            }
            assertEquals("SNAPSHOTING", paused?.State)
            // the partitions were judged: written now, before the table is replaced
            sql "INSERT INTO ${dbB}.${dupTable} VALUES (1500, 'written into the table being replaced')"
            sql "sync"
        } finally {
            GetDebugPoint().disableDebugPointForAllFEs("FE.PAUSE_NON_PENDING_RESTORE_JOB")
        }
        def job = waitRestoreDone(snap2, 600)
        assertEquals("CANCELLED", job.State)
        logger.info("the restore of a written table: ${job.Status}")
        assertTrue(job.Status.toString().contains("p2"), job.Status.toString())
        // the table is not replaced: it has the write, the staging table is dropped
        order_qt_written_before_replace """SELECT id DIV 1000, COUNT(*),
                SUM(CRC32(CONCAT(CAST(id AS STRING), '|', value))) FROM ${dbB}.${dupTable} GROUP BY id DIV 1000"""
        assertEquals(1, (sql "SELECT COUNT(*) FROM ${dbB}.${dupTable} WHERE id = 1500")[0][0])
        assertEquals(rowsOf(dbA, mowTable), rowsOf(dbB, mowTable))
        for (int i = 0; i < 60 && !(sql "SHOW TABLES FROM ${dbB} LIKE '${stagingPrefix}%'").isEmpty(); ++i) {
            sleep(1000)
        }
        assertTrue((sql "SHOW TABLES FROM ${dbB} LIKE '${stagingPrefix}%'").isEmpty())
        // the next restore: the written p2 has another token, it is downloaded, the others are kept
        estimate = restoreAndWait(snap2, ts2)
        assertEquals(2 * numPartitions - 1, keptAtomic(estimate))
        assertEquals(2 * numPartitions - 1, keptByToken(estimate))
        assertSameAsSource()
        order_qt_after_written """SELECT id DIV 1000, COUNT(*), SUM(CRC32(CONCAT(CAST(id AS STRING), '|', value)))
                FROM ${dbB}.${dupTable} GROUP BY id DIV 1000"""
    } else {
        logger.info("Config.enable_debug_points=true is required for the write before the replace, skip it")
    }

    sql "DROP REPOSITORY `${repoName}`"
    }
}
