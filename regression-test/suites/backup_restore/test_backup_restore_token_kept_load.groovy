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
import org.apache.doris.regression.util.DebugPoint
import org.apache.doris.regression.util.NodeType

// The partitions of an atomic restore kept by their state token and loaded into the staging tablets by the snapshot
// task (restore_reuse_kept_load_at_snapshot): the move of the commit only checks the evidence of the load. The backup
// is the default one (no logical digest, no manifest). The BEs and the FE need enable_debug_points=true. A table of
// 3 replicas needs 3 backends, with fewer the case is skipped.
// Debug points used:
//   SnapshotLoader.make_local_source.delay             params: sleep_ms, tablet_id; on one backend or on all
//   WorkPool.finish_task.lost                          params: task_type, times: the reports of the tasks are lost
//   SnapshotLoader.confirm_local_source.clear_stage    the evidence of the load is dropped before the move checks it
//   SnapshotManager.restore_local_source.force_copy    the local source is copied as if it were on another disk
//   1. A late load of one backend: the tablet scheduler does not clone over the loaded staging replicas.
//   2. The report of a snapshot task is lost: the FE resends it, the resent load is judged as done.
//   3. The evidence of a load is gone when the move checks it: the job is cancelled at once, the table being replaced
//      is not touched, the next restore keeps everything.
//   4. restore_reuse_kept_load_at_snapshot off: the plain local snapshot, the move loads it.
//   5. The local source on another disk: copied, and confirmed the same way.
//   6. The job is cancelled while the loads are going on: the staging table is dropped, the backends are fine.
// The restart of the FE or of a BE in the middle of the load needs a docker cluster, see
// test_backup_restore_token_kept_load_docker.
suite("test_backup_restore_token_kept_load", "backup_restore,nonConcurrent") {
    setFeConfigTemporary([enable_restore_partition_reuse: true, enable_restore_atomic_reuse: true,
            restore_reuse_min_partition_bytes: 0, restore_reuse_allow_hidden_state_models: false,
            restore_reuse_mow: false, restore_local_source_task_max_retry: 10,
            restore_reuse_kept_load_at_snapshot: true, restore_state_token_track: true,
            restore_reuse_trust_state_token: true, restore_reset_index_id: false]) {
    if (!getFeConfig("enable_debug_points").equals("true")) {
        logger.info("Config.enable_debug_points=true is required")
        return
    }
    def backends = sql_return_maparray "SHOW BACKENDS"
    def aliveBackends = backends.findAll { it.Alive.toString() == "true" }
    if (aliveBackends.size() < 3) {
        logger.info("3 backends are required for 3 replicas, found ${aliveBackends.size()}, skip")
        return
    }

    String suiteName = "test_backup_restore_token_kept_load"
    String repoName = "${suiteName}_repo_" + UUID.randomUUID().toString().replace("-", "")
    String dbA = "${suiteName}_a"
    String dbB = "${suiteName}_b"
    String dupTable = "${suiteName}_dup"
    String stagingPrefix = "__doris_atomic_restore_prefix__"
    int numPartitions = 3
    int numBuckets = 3
    int numReplicas = 3
    // MAKE_SNAPSHOT of TTaskType
    int taskMakeSnapshot = 8
    // longer than the interval of the tablet report plus the one of the tablet checker
    long lateMs = 110000
    // the late backend, picked by its http port (the backends of a test cluster may share one host)
    def lateBe = aliveBackends[0]

    def syncer = getSyncer()
    syncer.createS3Repository(repoName)

    def setConfig = { String name, String value ->
        sql """ADMIN SET FRONTEND CONFIG ("${name}" = "${value}")"""
    }

    for (String db : [dbA, dbB]) {
        sql "DROP DATABASE IF EXISTS ${db} FORCE"
        sql "CREATE DATABASE ${db}"
    }
    sql """
        CREATE TABLE ${dbA}.${dupTable} (
            `id` INT NOT NULL,
            `value` VARCHAR(64) NULL
        )
        DUPLICATE KEY(`id`)
        PARTITION BY RANGE(`id`)
        (
            PARTITION p1 VALUES LESS THAN ("1000"),
            PARTITION p2 VALUES LESS THAN ("2000"),
            PARTITION p3 VALUES LESS THAN ("3000")
        )
        DISTRIBUTED BY HASH(`id`) BUCKETS ${numBuckets}
        PROPERTIES ("replication_num" = "${numReplicas}", "disable_auto_compaction" = "true")
        """
    for (int b = 0; b < 3; ++b) {
        List<String> values = []
        for (int p = 0; p < numPartitions; ++p) {
            for (int i = 0; i < 100; ++i) {
                int id = p * 1000 + b * 100 + i
                values.add("(${id}, 'value of ${id} in batch ${b}')")
            }
        }
        sql "INSERT INTO ${dbA}.${dupTable} VALUES ${values.join(",")}"
    }
    sql "sync"

    // ---- helpers ----
    def backupAndWait = { String snapshot ->
        sql """
            BACKUP SNAPSHOT ${dbA}.${snapshot}
            TO `${repoName}`
            ON (${dupTable})
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
            ON (${dupTable})
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
    def restoreAndWait = { String snapshot, String timestamp, long timeoutSec = 600 ->
        startRestore(snapshot, timestamp)
        return waitRestoreDone(snapshot, timeoutSec)
    }
    def rowsOf = { String db ->
        def r = sql """SELECT COUNT(*), SUM(CRC32(CONCAT(CAST(id AS STRING), '|', value))) FROM ${db}.${dupTable}"""
        return "${r[0][0]}:${r[0][1]}".toString()
    }
    def versionsOf = { String db ->
        def parts = sql_return_maparray "SHOW PARTITIONS FROM ${db}.${dupTable}"
        return parts.collectEntries { [(it.PartitionName): it.VisibleVersion as String] }
    }
    def assertSameAsSource = { ->
        assertEquals(rowsOf(dbA), rowsOf(dbB))
        assertEquals(versionsOf(dbA), versionsOf(dbB))
    }
    def tabletIdsOf = { ->
        def tablets = sql_return_maparray "SHOW TABLETS FROM ${dbB}.${dupTable}"
        return tablets.collect { it.TabletId as long }.toSet()
    }
    // every partition is kept by its token
    def assertAllKept = { job ->
        assertEquals("FINISHED", job.State)
        def estimate = new JsonSlurper().parseText(job.ReuseEstimate as String)
        logger.info("reuse estimate: ${estimate}")
        assertEquals(numPartitions, estimate.kept_atomic as int)
        assertEquals(numPartitions, estimate.kept_token as int)
    }
    // The tablet scheduler history (SHOW PROC '/cluster_balance/history_tablets'): a finished clone has a
    // destination backend. Times are FE times, compared with the FE now() as strings of the same format.
    def feNow = { -> sql("SELECT DATE_FORMAT(NOW(), '%Y-%m-%d %H:%i:%s')")[0][0].toString() }
    def clonesSince = { String since ->
        def rows = sql_return_maparray "SHOW PROC '/cluster_balance/history_tablets'"
        return rows.count {
            it.Create.toString() >= since && it.State.toString() == "FINISHED" && it.DestBe.toString() != "-1"
        }
    }
    def enableDebug = { String name, Map params ->
        GetDebugPoint().enableDebugPointForAllBEs(name, params.collectEntries { k, v -> [(k): v.toString()] })
    }
    def enableDebugOnLateBe = { String name, Map params ->
        DebugPoint.enableDebugPoint(lateBe.Host.toString(), lateBe.HttpPort as int, NodeType.BE, name,
                params.collectEntries { k, v -> [(k): v.toString()] })
    }
    def disableDebug = { String name ->
        GetDebugPoint().disableDebugPointForAllBEs(name)
    }
    def stagingTableExists = { ->
        return !(sql "SHOW TABLES FROM ${dbB} LIKE '${stagingPrefix}${dupTable}'").isEmpty()
    }
    def waitUntil = { long timeoutSec, Closure condition ->
        for (long waited = 0; waited < timeoutSec; ++waited) {
            if (condition()) {
                return true
            }
            sleep(1000)
        }
        return false
    }
    def allBackendsAlive = { ->
        def rows = sql_return_maparray "SHOW BACKENDS"
        return rows.findAll { it.Alive.toString() == "true" }.size() == aliveBackends.size()
    }

    // B = A by a whole download, the partitions adopt the tokens of the backup
    String snap0 = "${suiteName}_0"
    String ts0 = backupAndWait(snap0)
    def job = restoreAndWait(snap0, ts0)
    assertEquals("FINISHED", job.State)
    assertSameAsSource()
    sleep(5000)
    // the same backup again: every partition is kept, the tables are replaced
    def tabletsBefore = tabletIdsOf()
    job = restoreAndWait(snap0, ts0)
    assertAllKept(job)
    assertSameAsSource()
    assertTrue(tabletsBefore.intersect(tabletIdsOf()).isEmpty())
    order_qt_kept """SELECT id DIV 1000, COUNT(*), SUM(CRC32(CONCAT(CAST(id AS STRING), '|', value)))
            FROM ${dbB}.${dupTable} GROUP BY id DIV 1000"""

    // ---- 1. a late load of one backend: the scheduler must not clone over the loaded staging replicas, which hold
    // the data from SNAPSHOTING on ----
    try {
        String since1 = feNow()
        enableDebugOnLateBe("SnapshotLoader.make_local_source.delay", [sleep_ms: lateMs, execute: 1])
        job = restoreAndWait(snap0, ts0)
        assertAllKept(job)
        assertSameAsSource()
        assertEquals(0, clonesSince(since1))
    } finally {
        disableDebug("SnapshotLoader.make_local_source.delay")
    }

    // ---- 2. the report of a snapshot task is lost: the FE resends it, the resent load is judged as done ----
    try {
        enableDebug("WorkPool.finish_task.lost", [task_type: taskMakeSnapshot, times: 2])
        job = restoreAndWait(snap0, ts0)
        assertAllKept(job)
        assertSameAsSource()
    } finally {
        disableDebug("WorkPool.finish_task.lost")
    }

    // ---- 3. the evidence of a load is gone when the move checks it (as a staging tablet overwritten by a clone):
    // the job is cancelled at once, the table being replaced is not touched, and the next restore works ----
    try {
        String rowsBefore = rowsOf(dbB)
        def versionsBefore = versionsOf(dbB)
        def tabletsOfB = tabletIdsOf()
        enableDebug("SnapshotLoader.confirm_local_source.clear_stage", [execute: 1])
        job = restoreAndWait(snap0, ts0)
        assertEquals("CANCELLED", job.State)
        assertTrue(job.Status.toString().contains("[RESTORE_NOT_RETRYABLE]"), job.Status.toString())
        assertEquals(rowsBefore, rowsOf(dbB))
        assertEquals(versionsBefore, versionsOf(dbB))
        assertEquals(tabletsOfB, tabletIdsOf())
        assertTrue(waitUntil(60, { !stagingTableExists() }))
    } finally {
        disableDebug("SnapshotLoader.confirm_local_source.clear_stage")
    }
    job = restoreAndWait(snap0, ts0)
    assertAllKept(job)
    assertSameAsSource()

    // ---- 4. the switch off: the plain local snapshot, the move loads it ----
    try {
        setConfig("restore_reuse_kept_load_at_snapshot", "false")
        job = restoreAndWait(snap0, ts0)
        assertAllKept(job)
        assertSameAsSource()
    } finally {
        setConfig("restore_reuse_kept_load_at_snapshot", "true")
    }

    // ---- 5. the local source on another disk: copied, and confirmed the same way ----
    try {
        enableDebug("SnapshotManager.restore_local_source.force_copy", [:])
        job = restoreAndWait(snap0, ts0)
        assertAllKept(job)
        assertSameAsSource()
    } finally {
        disableDebug("SnapshotManager.restore_local_source.force_copy")
    }
    order_qt_copied """SELECT id DIV 1000, COUNT(*), SUM(CRC32(CONCAT(CAST(id AS STRING), '|', value)))
            FROM ${dbB}.${dupTable} GROUP BY id DIV 1000"""

    // ---- 6. the job is cancelled by the user while the loads are going on: the loads end on their own, the staging
    // table is dropped, the backends are fine, and the next restore works ----
    try {
        enableDebug("SnapshotLoader.make_local_source.delay", [sleep_ms: 20000])
        startRestore(snap0, ts0)
        assertTrue(waitUntil(120, { jobOf(snap0)?.State == "SNAPSHOTING" }), "state: ${jobOf(snap0)?.State}")
        sleep(5000)
        sql "CANCEL RESTORE FROM ${dbB}"
        job = waitRestoreDone(snap0, 300)
        assertEquals("CANCELLED", job.State)
        disableDebug("SnapshotLoader.make_local_source.delay")
        assertTrue(waitUntil(120, { !stagingTableExists() }))
        assertSameAsSource()
        // the loads which were still running have ended with the tablets dropped meanwhile
        sleep(60000)
        assertTrue(allBackendsAlive())
        job = restoreAndWait(snap0, ts0)
        assertAllKept(job)
        assertSameAsSource()
        assertTrue(allBackendsAlive())
    } finally {
        disableDebug("SnapshotLoader.make_local_source.delay")
    }
    order_qt_after_cancel """SELECT id DIV 1000, COUNT(*), SUM(CRC32(CONCAT(CAST(id AS STRING), '|', value)))
            FROM ${dbB}.${dupTable} GROUP BY id DIV 1000"""

    sql "DROP REPOSITORY `${repoName}`"
    }
}
