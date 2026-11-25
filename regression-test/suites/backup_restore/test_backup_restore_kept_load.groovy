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

// The kept partitions of an atomic restore loaded into the staging tablets by the snapshot task
// (restore_reuse_kept_load_at_snapshot, default true): the move of the commit only checks the evidence of the load.
// The BEs need enable_debug_points=true. A table of 3 replicas needs 3 backends, with fewer the case is skipped.
// Debug points used:
//   SnapshotLoader.make_local_source.delay             params: sleep_ms, backend (ip, any if unset), tablet_id
//   WorkPool.finish_task.lost                          params: task_type, times: the reports of the tasks are lost
//   SnapshotLoader.confirm_local_source.clear_stage    the evidence of the load is dropped before the move checks it
//   SnapshotManager.restore_local_source.force_copy    the local source is copied as if it were on another disk
// The restart of the FE or of a BE in the middle of the load needs a docker cluster, see
// test_backup_restore_atomic_local_source_docker.
suite("test_backup_restore_kept_load", "backup_restore,nonConcurrent") {
    // enable_backup_manifest is off by default in branch-3.1: the case depends on the manifest of the backup,
    // turn it on for the whole case (restored when the case ends). The body is not indented, to keep the diff small.
    setFeConfigTemporary([enable_backup_manifest: true, enable_restore_partition_reuse: false,
            enable_restore_incremental_append: false, enable_restore_atomic_reuse: false,
            restore_reuse_min_partition_bytes: 0, restore_reuse_default_check_level: "full",
            restore_reuse_allow_hidden_state_models: false, restore_reuse_mow: false,
            restore_local_source_task_max_retry: 10, restore_reuse_kept_load_at_snapshot: true]) {
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

    String suiteName = "test_backup_restore_kept_load"
    String repoName = "${suiteName}_repo_" + UUID.randomUUID().toString().replace("-", "")
    String dbA = "${suiteName}_a"
    String dbB = "${suiteName}_b"
    String dupTable = "${suiteName}_dup"
    String mowTable = "${suiteName}_mow"
    String stagingPrefix = "__doris_atomic_restore_prefix__"
    int numPartitions = 3
    int numBuckets = 3
    int numReplicas = 3
    // one staging tablet per replica, every partition is kept
    int keptReplicas = numPartitions * numBuckets * numReplicas
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
    List<String> configNames = ["enable_restore_partition_reuse", "enable_restore_incremental_append",
            "enable_restore_atomic_reuse", "restore_reuse_min_partition_bytes", "restore_reuse_default_check_level",
            "restore_reuse_allow_hidden_state_models", "restore_reuse_mow", "restore_local_source_task_max_retry",
            "restore_reuse_kept_load_at_snapshot"]
    def origConfigs = configNames.collectEntries { [(it): getFeConfig(it)] }
    setConfig("enable_restore_partition_reuse", "true")
    setConfig("enable_restore_incremental_append", "true")
    setConfig("enable_restore_atomic_reuse", "true")
    setConfig("restore_reuse_min_partition_bytes", "0")
    setConfig("restore_reuse_default_check_level", "full")
    setConfig("restore_reuse_allow_hidden_state_models", "false")
    setConfig("restore_reuse_mow", "false")
    setConfig("restore_local_source_task_max_retry", "10")
    setConfig("restore_reuse_kept_load_at_snapshot", "true")

    for (String db : [dbA, dbB]) {
        sql "DROP DATABASE IF EXISTS ${db} FORCE"
        sql "CREATE DATABASE ${db}"
    }
    def createTable = { String db, String tbl ->
        String keys = tbl == dupTable ? "DUPLICATE KEY(`id`)" : "UNIQUE KEY(`id`)"
        String props = tbl == dupTable ? "" : ", \"enable_unique_key_merge_on_write\" = \"true\""
        sql """
            CREATE TABLE ${db}.${tbl} (
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
            DISTRIBUTED BY HASH(`id`) BUCKETS ${numBuckets}
            PROPERTIES ("replication_num" = "${numReplicas}", "disable_auto_compaction" = "true"${props})
            """
        for (int b = 0; b < 3; ++b) {
            List<String> values = []
            for (int p = 0; p < numPartitions; ++p) {
                for (int i = 0; i < 100; ++i) {
                    int id = p * 1000 + b * 100 + i
                    values.add("(${id}, 'value of ${id} in batch ${b}')")
                }
            }
            sql "INSERT INTO ${db}.${tbl} VALUES ${values.join(",")}"
        }
    }
    createTable(dbA, dupTable)
    createTable(dbA, mowTable)
    sql "sync"

    // ---- helpers ----
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
    def startRestore = { String snapshot, String timestamp, String tbl ->
        sql """
            RESTORE SNAPSHOT ${dbB}.${snapshot}
            FROM `${repoName}`
            ON (${tbl})
            PROPERTIES ("backup_timestamp" = "${timestamp}", "reserve_replica" = "true",
                        "atomic_restore" = "true", "reuse_check_level" = "full")
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
                return job
            }
            sleep(3000)
        }
        assertTrue(false, "the restore job ${snapshot} is not done after ${timeoutSec}s, state: ${job?.State}")
    }
    def restoreAndWait = { String snapshot, String timestamp, String tbl, long timeoutSec = 600 ->
        startRestore(snapshot, timestamp, tbl)
        return waitRestoreDone(snapshot, timeoutSec)
    }
    def rowsOf = { String db, String tbl ->
        def r = sql """SELECT COUNT(*), SUM(CRC32(CONCAT(CAST(id AS STRING), '|', value))) FROM ${db}.${tbl}"""
        return "${r[0][0]}:${r[0][1]}".toString()
    }
    def versionsOf = { String db, String tbl ->
        def parts = sql_return_maparray "SHOW PARTITIONS FROM ${db}.${tbl}"
        return parts.collectEntries { [(it.PartitionName): it.VisibleVersion as String] }
    }
    def assertSameAsSource = { String tbl ->
        assertEquals(rowsOf(dbA, tbl), rowsOf(dbB, tbl))
        assertEquals(versionsOf(dbA, tbl), versionsOf(dbB, tbl))
    }
    def tabletIdsOf = { String tbl ->
        def tablets = sql_return_maparray "SHOW TABLETS FROM ${dbB}.${tbl}"
        return tablets.collect { it.TabletId as long }.toSet()
    }
    def estimateOf = { job -> new JsonSlurper().parseText(job.ReuseEstimate as String) }
    def statsOf = { job -> new JsonSlurper().parseText(job.DownloadStats as String) }
    // every replica of every partition is kept, by a local snapshot
    def assertAllKept = { job ->
        assertEquals("FINISHED", job.State)
        def estimate = estimateOf(job)
        def stats = statsOf(job)
        logger.info("reuse estimate: ${estimate}, download stats: ${stats}")
        assertEquals(numPartitions, estimate.kept_atomic as int)
        assertEquals(0L, stats.downloaded_bytes as long)
        assertEquals(keptReplicas, stats.atomic_local.local_tablets as int)
        return stats
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
    def stagingTableExists = { String tbl ->
        return !(sql "SHOW TABLES FROM ${dbB} LIKE '${stagingPrefix}${tbl}'").isEmpty()
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

    // B = A by a whole download: nothing is kept, nothing is loaded by the snapshot tasks
    String snap0 = "${suiteName}_0"
    String ts0 = backupAndWait(snap0, dupTable)
    def job = restoreAndWait(snap0, ts0, dupTable)
    assertEquals("FINISHED", job.State)
    assertSameAsSource(dupTable)
    assertEquals(0, (statsOf(job).atomic_local?.loaded_tablets ?: 0) as int)
    sleep(5000)

    // ---- 1. the same backup again: every partition is kept, the snapshot tasks load the staging tablets, the moves
    // only confirm them, and the tables are replaced ----
    String since1 = feNow()
    def tabletsBefore = tabletIdsOf(dupTable)
    job = restoreAndWait(snap0, ts0, dupTable)
    def stats = assertAllKept(job)
    assertEquals(keptReplicas, stats.atomic_local.loaded_tablets as int)
    assertTrue((stats.atomic_local.linked_bytes as long) > 0)
    assertEquals(0L, stats.atomic_local.copied_bytes as long)
    assertSameAsSource(dupTable)
    assertTrue(tabletsBefore.intersect(tabletIdsOf(dupTable)).isEmpty())
    assertEquals(0, clonesSince(since1))

    // ---- 2. a late load of one backend: the scheduler must not clone over the loaded staging replicas, which hold
    // the data from SNAPSHOTING on ----
    try {
        String since2 = feNow()
        enableDebugOnLateBe("SnapshotLoader.make_local_source.delay", [sleep_ms: lateMs, execute: 1])
        job = restoreAndWait(snap0, ts0, dupTable)
        stats = assertAllKept(job)
        assertEquals(keptReplicas, stats.atomic_local.loaded_tablets as int)
        assertSameAsSource(dupTable)
        assertEquals(0, clonesSince(since2))
    } finally {
        disableDebug("SnapshotLoader.make_local_source.delay")
    }

    // ---- 3. the report of a snapshot task is lost: the FE resends it, the resent load is judged as done ----
    try {
        enableDebug("WorkPool.finish_task.lost", [task_type: taskMakeSnapshot, times: 2])
        job = restoreAndWait(snap0, ts0, dupTable)
        stats = assertAllKept(job)
        // the resent tasks find the tablets loaded and count them, so the sums still add up to every replica
        assertEquals(keptReplicas, stats.atomic_local.loaded_tablets as int)
        assertSameAsSource(dupTable)
    } finally {
        disableDebug("WorkPool.finish_task.lost")
    }

    // ---- 4. the evidence of a load is gone when the move checks it (as a staging tablet overwritten by a clone):
    // the job is cancelled at once, the table being replaced is not touched, and the next restore works ----
    try {
        String rowsBefore = rowsOf(dbB, dupTable)
        def versionsBefore = versionsOf(dbB, dupTable)
        enableDebug("SnapshotLoader.confirm_local_source.clear_stage", [execute: 1])
        job = restoreAndWait(snap0, ts0, dupTable)
        assertEquals("CANCELLED", job.State)
        assertTrue(job.Status.toString().contains("[RESTORE_NOT_RETRYABLE]"), job.Status.toString())
        assertEquals(rowsBefore, rowsOf(dbB, dupTable))
        assertEquals(versionsBefore, versionsOf(dbB, dupTable))
        assertTrue(waitUntil(60, { !stagingTableExists(dupTable) }))
    } finally {
        disableDebug("SnapshotLoader.confirm_local_source.clear_stage")
    }
    job = restoreAndWait(snap0, ts0, dupTable)
    assertAllKept(job)
    assertSameAsSource(dupTable)

    // ---- 5. the switch off: the plain local snapshot, the move loads it, nothing is loaded by the snapshot task ----
    try {
        setConfig("restore_reuse_kept_load_at_snapshot", "false")
        job = restoreAndWait(snap0, ts0, dupTable)
        stats = assertAllKept(job)
        assertEquals(0, stats.atomic_local.loaded_tablets as int)
        assertSameAsSource(dupTable)
    } finally {
        setConfig("restore_reuse_kept_load_at_snapshot", "true")
    }

    // ---- 6. the local source on another disk: copied, and confirmed the same way ----
    try {
        enableDebug("SnapshotManager.restore_local_source.force_copy", [:])
        job = restoreAndWait(snap0, ts0, dupTable)
        stats = assertAllKept(job)
        assertEquals(keptReplicas, stats.atomic_local.loaded_tablets as int)
        assertTrue((stats.atomic_local.copied_bytes as long) > 0)
        assertEquals(0L, stats.atomic_local.linked_bytes as long)
        assertSameAsSource(dupTable)
    } finally {
        disableDebug("SnapshotManager.restore_local_source.force_copy")
    }

    // ---- 7. the job is cancelled by the user while the loads are going on: the loads end on their own, the staging
    // table is dropped, the backends are fine, and the next restore works ----
    try {
        enableDebug("SnapshotLoader.make_local_source.delay", [sleep_ms: 20000])
        startRestore(snap0, ts0, dupTable)
        assertTrue(waitUntil(120, { jobOf(snap0)?.State == "SNAPSHOTING" }), "state: ${jobOf(snap0)?.State}")
        sleep(5000)
        sql "CANCEL RESTORE FROM ${dbB}"
        job = waitRestoreDone(snap0, 300)
        assertEquals("CANCELLED", job.State)
        disableDebug("SnapshotLoader.make_local_source.delay")
        assertTrue(waitUntil(120, { !stagingTableExists(dupTable) }))
        assertSameAsSource(dupTable)
        // the loads which were still running have ended with the tablets dropped meanwhile
        sleep(60000)
        assertTrue(allBackendsAlive())
        job = restoreAndWait(snap0, ts0, dupTable)
        assertAllKept(job)
        assertSameAsSource(dupTable)
        assertTrue(allBackendsAlive())
    } finally {
        disableDebug("SnapshotLoader.make_local_source.delay")
    }

    // ---- 8. a merge-on-write table (no sequence column), with its switch: loaded by the snapshot task as well ----
    setConfig("restore_reuse_mow", "true")
    try {
        String snapM = "${suiteName}_m"
        String tsM = backupAndWait(snapM, mowTable)
        job = restoreAndWait(snapM, tsM, mowTable)
        assertEquals("FINISHED", job.State)
        assertSameAsSource(mowTable)
        sleep(5000)
        job = restoreAndWait(snapM, tsM, mowTable)
        stats = assertAllKept(job)
        assertEquals(keptReplicas, stats.atomic_local.loaded_tablets as int)
        assertSameAsSource(mowTable)
        // a write into p1 of the source: p2 and p3 are kept and loaded by the snapshot task, p1 is appended
        sql "INSERT INTO ${dbA}.${mowTable} VALUES (5, 'updated in p1'), (6, 'new row of p1')"
        sql "sync"
        String snapM2 = "${suiteName}_m2"
        String tsM2 = backupAndWait(snapM2, mowTable)
        job = restoreAndWait(snapM2, tsM2, mowTable)
        assertEquals("FINISHED", job.State)
        assertEquals(numPartitions - 1, estimateOf(job).kept_atomic as int)
        assertEquals(1, estimateOf(job).incremental_atomic as int)
        // the local base of the incremental partition is loaded by the snapshot task too
        assertEquals(keptReplicas, statsOf(job).atomic_local.loaded_tablets as int)
        assertSameAsSource(mowTable)
        assertEquals("updated in p1", (sql "SELECT value FROM ${dbB}.${mowTable} WHERE id = 5")[0][0])
    } finally {
        setConfig("restore_reuse_mow", "false")
    }

    origConfigs.each { name, value -> setConfig(name, value) }
    for (String db : [dbA, dbB]) {
        sql "DROP DATABASE IF EXISTS ${db} FORCE"
    }
    sql "DROP REPOSITORY `${repoName}`"
    }
}
