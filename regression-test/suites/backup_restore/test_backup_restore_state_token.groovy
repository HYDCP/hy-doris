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

// A partition kept by its state token in a restore (restore_state_token_track, restore_reuse_trust_state_token):
//   1. Backed up with the tracking on, restored onto a database that has the same data: every partition is kept by
//      its state token (KEPT_STATE_TOKEN), nothing is downloaded. With the trust off the tokens are equal, but the
//      partitions are proved by the digest.
//   2. ADMIN SET REPLICA VERSION taints the partition, on the source (the backup records no token for it) and on the
//      target (the local partition has none): that partition is not kept by its token, the data is still right.
//   3. A finished BUILD INDEX of a partition invalidates its token: the backup gives the partition a new name, the
//      restore does not keep it by the old one, and the restored partition adopts the new name.
//   4. Backed up with the tracking off: the backup records no token, no partition is kept by its token.
// The ReuseEstimate column of SHOW RESTORE has the counts (kept_token, token_equal); the per partition reasons are in
// the fe.log ("restore reuse: partition ..."). Single replica, auto compaction off.
suite("test_backup_restore_state_token", "backup_restore") {
    setFeConfigTemporary([enable_backup_manifest: true, enable_restore_partition_reuse: false,
            restore_reuse_min_partition_bytes: 0, restore_state_token_track: false,
            restore_reuse_trust_state_token: false, restore_reset_index_id: false]) {
    String suiteName = "test_backup_restore_state_token"
    String repoName = "${suiteName}_repo_" + UUID.randomUUID().toString().replace("-", "")
    String dbA = "${suiteName}_a"
    String dbB = "${suiteName}_b"

    def syncer = getSyncer()
    syncer.createS3Repository(repoName)

    def setConfig = { String name, String value ->
        sql """ADMIN SET FRONTEND CONFIG ("${name}" = "${value}")"""
    }
    setConfig("enable_restore_partition_reuse", "true")
    setConfig("restore_reuse_min_partition_bytes", "0")
    setConfig("restore_state_token_track", "true")
    setConfig("restore_reuse_trust_state_token", "true")

    for (String db : [dbA, dbB]) {
        sql "DROP DATABASE IF EXISTS ${db} FORCE"
        sql "CREATE DATABASE ${db}"
    }

    def backupAndWait = { String snapshot, String tbl ->
        sql """
            BACKUP SNAPSHOT ${dbA}.${snapshot}
            TO `${repoName}`
            ON (${tbl})
            PROPERTIES ("logical_digest" = "true")
        """
        syncer.waitSnapshotFinish(dbA)
        def timestamp = syncer.getSnapshotTimestamp(repoName, snapshot)
        assertTrue(timestamp != null)
        return timestamp
    }

    // the ReuseEstimate and the DownloadStats of the last restore job
    def lastEstimate = null
    def lastStats = null
    def restoreAndWait = { String snapshot, String timestamp, String tbl ->
        sql """
            RESTORE SNAPSHOT ${dbB}.${snapshot}
            FROM `${repoName}`
            ON (${tbl})
            PROPERTIES ("backup_timestamp" = "${timestamp}", "reserve_replica" = "true")
        """
        syncer.waitAllRestoreFinish(dbB)
        def result = sql_return_maparray """ SHOW RESTORE FROM ${dbB} WHERE Label = "${snapshot}" """
        def job = result.last()
        logger.info("restore ${snapshot} into ${dbB}: ${job}")
        assertEquals("FINISHED", job.State)
        lastStats = new JsonSlurper().parseText(job.DownloadStats as String)
        lastEstimate = job.ReuseEstimate == null ? null : new JsonSlurper().parseText(job.ReuseEstimate as String)
        logger.info("download stats: ${lastStats}, reuse estimate: ${lastEstimate}")
    }
    def keptPartitions = { (lastEstimate?.kept_partitions ?: 0) as int }
    def keptByToken = { (lastEstimate?.kept_token ?: 0) as int }
    def tokenEqual = { (lastEstimate?.token_equal ?: 0) as int }

    // the row count and the hash of the rows of each partition
    def rowsOf = { String db, String tbl ->
        return sql("""SELECT id DIV 1000, COUNT(*), SUM(CRC32(CONCAT(CAST(id AS STRING), '|', v1)))
                FROM ${db}.${tbl} GROUP BY id DIV 1000 ORDER BY id DIV 1000""")
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
    // an operation on a replica outside of the versions: the version of the replica is set to the one it has
    def setReplicaVersion = { String db, String tbl, String partition ->
        def tablet = sql_return_maparray("SHOW TABLETS FROM ${db}.${tbl} PARTITION (${partition})")[0]
        sql """ADMIN SET REPLICA VERSION PROPERTIES ("tablet_id" = "${tablet.TabletId}",
                "backend_id" = "${tablet.BackendId}", "version" = "${tablet.Version}")"""
    }
    def insertRows = { String tbl ->
        for (int b = 0; b < 2; ++b) {
            List<String> values = []
            for (int p = 0; p < 3; ++p) {
                for (int i = 0; i < 20; ++i) {
                    int id = p * 1000 + b * 20 + i
                    values.add("(${id}, 'value of ${id} in batch ${b}')")
                }
            }
            sql "INSERT INTO ${dbA}.${tbl} VALUES ${values.join(",")}"
        }
        sql "sync"
    }

    sql """
        CREATE TABLE ${dbA}.test_backup_restore_state_token_dup (
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
    insertRows("test_backup_restore_state_token_dup")

    // ------------------------------------------------------------------------------------------------
    // 1. the tracking on: the restore onto the same data keeps every partition by its state token
    // ------------------------------------------------------------------------------------------------
    String snap1 = "${suiteName}_snap1"
    String ts1 = backupAndWait(snap1, "test_backup_restore_state_token_dup")
    // B is new: downloaded as a whole, the partitions adopt the tokens of the backup
    restoreAndWait(snap1, ts1, "test_backup_restore_state_token_dup")
    assertTrue((lastStats.downloaded_bytes as long) > 0)
    waitDataSize(dbB, "test_backup_restore_state_token_dup", 120)

    restoreAndWait(snap1, ts1, "test_backup_restore_state_token_dup")
    assertEquals(3, keptByToken())
    assertEquals(3, tokenEqual())
    assertEquals(3, keptPartitions())
    assertEquals(0L, lastStats.downloaded_bytes as long)
    order_qt_kept_by_token """SELECT id DIV 1000, COUNT(*), SUM(CRC32(CONCAT(CAST(id AS STRING), '|', v1)))
            FROM ${dbB}.test_backup_restore_state_token_dup GROUP BY id DIV 1000"""
    assertEquals(rowsOf(dbA, "test_backup_restore_state_token_dup"), rowsOf(dbB, "test_backup_restore_state_token_dup"))

    // the trust off: the tokens are equal, the partitions are kept by the digest
    setConfig("restore_reuse_trust_state_token", "false")
    restoreAndWait(snap1, ts1, "test_backup_restore_state_token_dup")
    setConfig("restore_reuse_trust_state_token", "true")
    assertEquals(0, keptByToken())
    assertEquals(3, tokenEqual())
    assertEquals(3, keptPartitions())
    assertEquals(rowsOf(dbA, "test_backup_restore_state_token_dup"), rowsOf(dbB, "test_backup_restore_state_token_dup"))

    // ------------------------------------------------------------------------------------------------
    // 2. ADMIN SET REPLICA VERSION taints the partition
    // ------------------------------------------------------------------------------------------------
    // on the source: the backup records no token for p1
    setReplicaVersion(dbA, "test_backup_restore_state_token_dup", "p1")
    String snap2 = "${suiteName}_snap2"
    String ts2 = backupAndWait(snap2, "test_backup_restore_state_token_dup")
    restoreAndWait(snap2, ts2, "test_backup_restore_state_token_dup")
    assertEquals(2, keptByToken())
    assertEquals(2, tokenEqual())
    order_qt_source_tainted """SELECT id DIV 1000, COUNT(*), SUM(CRC32(CONCAT(CAST(id AS STRING), '|', v1)))
            FROM ${dbB}.test_backup_restore_state_token_dup GROUP BY id DIV 1000"""
    assertEquals(rowsOf(dbA, "test_backup_restore_state_token_dup"), rowsOf(dbB, "test_backup_restore_state_token_dup"))

    // on the target: the local p3 has no token, p1 still has none in the backup
    setReplicaVersion(dbB, "test_backup_restore_state_token_dup", "p3")
    restoreAndWait(snap2, ts2, "test_backup_restore_state_token_dup")
    assertEquals(1, keptByToken())
    assertEquals(1, tokenEqual())
    order_qt_target_tainted """SELECT id DIV 1000, COUNT(*), SUM(CRC32(CONCAT(CAST(id AS STRING), '|', v1)))
            FROM ${dbB}.test_backup_restore_state_token_dup GROUP BY id DIV 1000"""
    assertEquals(rowsOf(dbA, "test_backup_restore_state_token_dup"), rowsOf(dbB, "test_backup_restore_state_token_dup"))

    // ------------------------------------------------------------------------------------------------
    // 3. a finished BUILD INDEX of p2 invalidates its token, the backup names it again
    // ------------------------------------------------------------------------------------------------
    sql """
        CREATE TABLE ${dbA}.test_backup_restore_state_token_idx (
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
    insertRows("test_backup_restore_state_token_idx")
    // the index is in the schema of the backup, its files are built by BUILD INDEX later
    sql "CREATE INDEX idx_v1 ON ${dbA}.test_backup_restore_state_token_idx (v1) USING INVERTED"
    for (int i = 0; i < 60; ++i) {
        def jobs = sql_return_maparray """SHOW ALTER TABLE COLUMN FROM ${dbA}
                WHERE TableName = "test_backup_restore_state_token_idx" """
        if (jobs.every { it.State == "FINISHED" }) {
            break
        }
        sleep(1000)
    }
    String snapIdx1 = "${suiteName}_snap_idx1"
    String tsIdx1 = backupAndWait(snapIdx1, "test_backup_restore_state_token_idx")
    restoreAndWait(snapIdx1, tsIdx1, "test_backup_restore_state_token_idx")
    waitDataSize(dbB, "test_backup_restore_state_token_idx", 120)
    restoreAndWait(snapIdx1, tsIdx1, "test_backup_restore_state_token_idx")
    assertEquals(3, keptByToken())

    sql "BUILD INDEX idx_v1 ON ${dbA}.test_backup_restore_state_token_idx PARTITION (p2)"
    String buildState = null
    for (int i = 0; i < 120; ++i) {
        def jobs = sql_return_maparray """SHOW BUILD INDEX FROM ${dbA}
                WHERE TableName = "test_backup_restore_state_token_idx" """
        buildState = jobs.isEmpty() ? null : jobs.last().State
        if (buildState == "FINISHED" || buildState == "CANCELLED") {
            break
        }
        sleep(1000)
    }
    assertEquals("FINISHED", buildState)

    String snapIdx2 = "${suiteName}_snap_idx2"
    String tsIdx2 = backupAndWait(snapIdx2, "test_backup_restore_state_token_idx")
    // p2 has a new name in the backup: not kept by the old one
    restoreAndWait(snapIdx2, tsIdx2, "test_backup_restore_state_token_idx")
    assertEquals(2, keptByToken())
    assertEquals(2, tokenEqual())
    order_qt_build_index """SELECT id DIV 1000, COUNT(*), SUM(CRC32(CONCAT(CAST(id AS STRING), '|', v1)))
            FROM ${dbB}.test_backup_restore_state_token_idx GROUP BY id DIV 1000"""
    qt_build_index_point """SELECT id, v1 FROM ${dbB}.test_backup_restore_state_token_idx
            WHERE v1 = 'value of 1005 in batch 0'"""
    assertEquals(rowsOf(dbA, "test_backup_restore_state_token_idx"), rowsOf(dbB, "test_backup_restore_state_token_idx"))
    // the restored p2 adopted the new name: the same backup again keeps every partition by its token
    restoreAndWait(snapIdx2, tsIdx2, "test_backup_restore_state_token_idx")
    assertEquals(3, keptByToken())
    assertEquals(rowsOf(dbA, "test_backup_restore_state_token_idx"), rowsOf(dbB, "test_backup_restore_state_token_idx"))

    // ------------------------------------------------------------------------------------------------
    // 4. the tracking off: the backup records no token, nothing is kept by a token
    // ------------------------------------------------------------------------------------------------
    setConfig("restore_state_token_track", "false")
    String snap4 = "${suiteName}_snap4"
    String ts4 = backupAndWait(snap4, "test_backup_restore_state_token_dup")
    restoreAndWait(snap4, ts4, "test_backup_restore_state_token_dup")
    assertEquals(0, keptByToken())
    assertEquals(0, tokenEqual())
    order_qt_track_off """SELECT id DIV 1000, COUNT(*), SUM(CRC32(CONCAT(CAST(id AS STRING), '|', v1)))
            FROM ${dbB}.test_backup_restore_state_token_dup GROUP BY id DIV 1000"""
    assertEquals(rowsOf(dbA, "test_backup_restore_state_token_dup"), rowsOf(dbB, "test_backup_restore_state_token_dup"))

    sql "DROP REPOSITORY `${repoName}`"
    }
}
