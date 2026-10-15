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

// The staging partitions of an atomic restore which are loaded from a local source (kept or incrementally appended
// partitions), and the tasks which load them: the original problem was an incremental append stuck in COMMITTING
// until the job timed out, because the tablet scheduler cloned over a staging replica whose append was late, and the
// resent move then found the tablet at the backup version and failed forever.
//
// The BEs need enable_debug_points=true. A table of 3 replicas needs 3 backends, with fewer the case is skipped.
// The cases use the debug points
//   SnapshotLoader.append_increment.delay              params: sleep_ms, backend (ip, any if unset), tablet_id
//   SnapshotLoader.make_local_source.delay             the same
//   SnapshotLoader.append_increment.fail_before_stage  the append is done without its evidence, then fails
//   WorkPool.finish_task.lost                          params: task_type, times (default 1): the reports of the
//                                                      first `times` such tasks of a backend are not sent
//   SnapshotLoader.move.fail                           the whole move of a tablet fails
//   Tablet.revise_tablet_meta_fail                     params: tablet_id; the append of the tablet fails
// "execute" limits how many times a debug point takes effect on a BE.
//
// The restart of the FE or of a BE in the middle of the commit needs a docker cluster, see
// test_backup_restore_atomic_local_source_docker.
suite("test_backup_restore_atomic_local_source", "backup_restore,nonConcurrent") {
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

    String suiteName = "test_backup_restore_atomic_local_source"
    String repoName = "${suiteName}_repo_" + UUID.randomUUID().toString().replace("-", "")
    String dbA = "${suiteName}_a"
    String dbB = "${suiteName}_b"
    String dbC = "${suiteName}_c"
    String dupTable = "${suiteName}_dup"
    String mowTable = "${suiteName}_mow"
    String stagingPrefix = "__doris_atomic_restore_prefix__"
    int numPartitions = 3
    // MAKE_SNAPSHOT and MOVE of TTaskType
    int taskMakeSnapshot = 8
    int taskMove = 14
    // longer than the interval of the tablet report plus the one of the tablet checker, so that the FE sees the
    // late replica as behind and the scheduler gets a chance to repair it by clone
    long lateMs = 110000
    // The late backend. All backends of a test cluster may share one host, so the late one is picked by its http
    // port: the debug point is enabled on it only.
    def lateBe = aliveBackends[0]

    def syncer = getSyncer()
    syncer.createS3Repository(repoName)

    def setConfig = { String name, String value ->
        sql """ADMIN SET FRONTEND CONFIG ("${name}" = "${value}")"""
    }
    setConfig("enable_restore_partition_reuse", "true")
    setConfig("enable_restore_incremental_append", "true")
    setConfig("enable_restore_atomic_reuse", "true")
    setConfig("restore_reuse_min_partition_bytes", "0")
    setConfig("restore_reuse_default_check_level", "full")
    setConfig("restore_reuse_allow_hidden_state_models", "false")
    setConfig("restore_local_source_task_max_retry", "10")

    for (String db : [dbA, dbB, dbC]) {
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
            DISTRIBUTED BY HASH(`id`) BUCKETS 3
            PROPERTIES ("replication_num" = "3", "disable_auto_compaction" = "true"${props})
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
    int writes = 0
    // a write into p1 of the source: B is then behind the next backup by an increment
    def writeIntoP1 = { String tbl ->
        ++writes
        sql """INSERT INTO ${dbA}.${tbl} VALUES (${500 + writes}, 'increment ${writes}'),
                (${600 + writes}, 'increment ${writes}')"""
        sql "sync"
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
    def startRestore = { String db, String snapshot, String timestamp, String tbl ->
        sql """
            RESTORE SNAPSHOT ${db}.${snapshot}
            FROM `${repoName}`
            ON (${tbl})
            PROPERTIES ("backup_timestamp" = "${timestamp}", "reserve_replica" = "true",
                        "atomic_restore" = "true", "reuse_check_level" = "full")
        """
    }
    // waits for the job to be FINISHED or CANCELLED, returns its row of SHOW RESTORE
    def waitRestoreDone = { String db, String snapshot, long timeoutSec ->
        def job = null
        for (long waited = 0; waited < timeoutSec; waited += 3) {
            def result = sql_return_maparray """ SHOW RESTORE FROM ${db} WHERE Label = "${snapshot}" """
            if (!result.isEmpty()) {
                job = result.last()
                if (job.State == "FINISHED" || job.State == "CANCELLED") {
                    return job
                }
            }
            sleep(3000)
        }
        assertTrue(false, "the restore job ${snapshot} is not done after ${timeoutSec}s, state: ${job?.State}")
    }
    def restoreAndWait = { String db, String snapshot, String timestamp, String tbl, long timeoutSec = 600 ->
        startRestore(db, snapshot, timestamp, tbl)
        return waitRestoreDone(db, snapshot, timeoutSec)
    }
    def rowsOf = { String db, String tbl ->
        def r = sql """SELECT COUNT(*), SUM(CRC32(CONCAT(CAST(id AS STRING), '|', value))) FROM ${db}.${tbl}"""
        return "${r[0][0]}:${r[0][1]}".toString()
    }
    def versionsOf = { String db, String tbl ->
        def parts = sql_return_maparray "SHOW PARTITIONS FROM ${db}.${tbl}"
        return parts.collectEntries { [(it.PartitionName): it.VisibleVersion as String] }
    }
    def assertSameAsSource = { String db, String tbl ->
        assertEquals(rowsOf(dbA, tbl), rowsOf(db, tbl))
        assertEquals(versionsOf(dbA, tbl), versionsOf(db, tbl))
    }
    // The tablet scheduler history (SHOW PROC '/cluster_balance/history_tablets'): a finished clone has a
    // destination backend, a tablet the restore job protects is cancelled with the reason below. Times are FE
    // times, so they compare with the FE now() as strings of the same format.
    def dbIdOf = { String db ->
        return (sql_return_maparray("SHOW PROC '/dbs'")).find { it.DbName.toString() == db }.DbId.toString()
    }
    def feNow = { -> sql("SELECT DATE_FORMAT(NOW(), '%Y-%m-%d %H:%i:%s')")[0][0].toString() }
    def schedHistorySince = { String since ->
        def rows = sql_return_maparray "SHOW PROC '/cluster_balance/history_tablets'"
        return rows.findAll { it.Create.toString() >= since }
    }
    def clonesSince = { String since, Set tabletIds = null ->
        return schedHistorySince(since).count {
            it.State.toString() == "FINISHED" && it.DestBe.toString() != "-1"
                    && (tabletIds == null || tabletIds.contains(it.TabletId.toString()))
        }
    }
    def protectedSince = { String since ->
        return schedHistorySince(since).count { it.ErrMsg.toString().contains("loaded from a local source") }
    }
    def enableDebug = { String name, Map params ->
        GetDebugPoint().enableDebugPointForAllBEs(name, params)
    }
    def enableDebugOnLateBe = { String name, Map params ->
        DebugPoint.enableDebugPoint(lateBe.Host.toString(), lateBe.HttpPort as int, NodeType.BE, name,
                params.collectEntries { k, v -> [(k): v.toString()] })
    }
    def enableDebugOnOtherBes = { String name, Map params ->
        aliveBackends.findAll { it.BackendId.toString() != lateBe.BackendId.toString() }.each { be ->
            DebugPoint.enableDebugPoint(be.Host.toString(), be.HttpPort as int, NodeType.BE, name,
                    params.collectEntries { k, v -> [(k): v.toString()] })
        }
    }
    def disableDebug = { String name ->
        GetDebugPoint().disableDebugPointForAllBEs(name)
    }
    def stagingTable = { String tbl -> "${stagingPrefix}${tbl}" }
    def stagingTableExists = { String db, String tbl ->
        return !(sql "SHOW TABLES FROM ${db} LIKE '${stagingTable(tbl)}'").isEmpty()
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
    def incrementalPartitions = { job ->
        def estimate = new JsonSlurper().parseText(job.ReuseEstimate as String)
        return estimate.incremental_atomic as int
    }

    // B = A by a whole download, then A is one write ahead of B in every case below
    String snap0 = "${suiteName}_0"
    String ts0 = backupAndWait(snap0, dupTable)
    def job = restoreAndWait(dbB, snap0, ts0, dupTable)
    assertEquals("FINISHED", job.State)
    assertSameAsSource(dbB, dupTable)
    sleep(5000)

    // ---- 1. a late append (a MOVE) of one replica: the scheduler must not clone over the staging replica ----
    // Before the fix the job was stuck in COMMITTING until its timeout.
    try {
        writeIntoP1(dupTable)
        String snap1 = "${suiteName}_1"
        String ts1 = backupAndWait(snap1, dupTable)
        String since1 = feNow()
        // As in the original incident: every replica is loaded to V_l and reported (the FE takes them all as
        // behind, no healthy source yet), then the other replicas append and report V_b while the late one is
        // still waiting, so the scheduler has a source to clone the late replica from. The waits are longer than
        // a tablet report (60 s) plus a tablet check (20 s).
        enableDebugOnOtherBes("SnapshotLoader.append_increment.delay", [sleep_ms: 80000, execute: 1])
        enableDebugOnLateBe("SnapshotLoader.append_increment.delay", [sleep_ms: 220000, execute: 1])
        job = restoreAndWait(dbB, snap1, ts1, dupTable)
        assertEquals("FINISHED", job.State)
        assertTrue(incrementalPartitions(job) > 0)
        assertSameAsSource(dbB, dupTable)
        // no clone was made for the staging replicas (nor anything else: the cluster is otherwise idle)
        assertEquals(0, clonesSince(since1))
        // Whether the scheduler meets the protection here depends on when the tablet reports and checks run, which
        // a small cluster does not make sure of; the protection itself is covered by TabletSchedulerLocalSourceTest
        // and the race at scale by the manual large restore. Only log it.
        logger.info("protected tablet schedules in case 1: ${protectedSince(since1)}")
    } finally {
        disableDebug("SnapshotLoader.append_increment.delay")
    }

    // ---- 2. a late load of the local source (a MAKE_SNAPSHOT) of one replica ----
    try {
        writeIntoP1(dupTable)
        String snap2 = "${suiteName}_2"
        String ts2 = backupAndWait(snap2, dupTable)
        String since2 = feNow()
        enableDebugOnLateBe("SnapshotLoader.make_local_source.delay", [sleep_ms: lateMs, execute: 1])
        job = restoreAndWait(dbB, snap2, ts2, dupTable)
        assertEquals("FINISHED", job.State)
        assertSameAsSource(dbB, dupTable)
        assertEquals(0, clonesSince(since2))
        logger.info("protected tablet schedules in case 2: ${protectedSince(since2)}")
    } finally {
        disableDebug("SnapshotLoader.make_local_source.delay")
    }

    // ---- 3. the report of the snapshot task and of the move task is lost: the FE resends the tasks, and the
    // resent load and append are judged as done ----
    for (int type : [taskMakeSnapshot, taskMove]) {
        try {
            writeIntoP1(dupTable)
            String snap3 = "${suiteName}_3_${type}"
            String ts3 = backupAndWait(snap3, dupTable)
            enableDebug("WorkPool.finish_task.lost", [task_type: type, times: 2])
            job = restoreAndWait(dbB, snap3, ts3, dupTable)
            assertEquals("FINISHED", job.State)
            assertSameAsSource(dbB, dupTable)
        } finally {
            disableDebug("WorkPool.finish_task.lost")
        }
    }

    // ---- 4. the append is done but its evidence is not written (the task fails after the append): the resent task
    // finds the tablet at the backup version and checks it against the digest of the backup ----
    try {
        writeIntoP1(dupTable)
        String snap4 = "${suiteName}_4"
        String ts4 = backupAndWait(snap4, dupTable)
        enableDebug("SnapshotLoader.append_increment.fail_before_stage", [execute: 2])
        job = restoreAndWait(dbB, snap4, ts4, dupTable)
        assertEquals("FINISHED", job.State)
        assertSameAsSource(dbB, dupTable)
    } finally {
        disableDebug("SnapshotLoader.append_increment.fail_before_stage")
    }

    // ---- 5. the append of a tablet keeps failing: the job is cancelled after a few retries, the table in B is
    // not changed, and the same restore works the next time ----
    setConfig("restore_local_source_task_max_retry", "2")
    String rowsBefore = rowsOf(dbB, dupTable)
    Map versionsBefore = versionsOf(dbB, dupTable)
    try {
        writeIntoP1(dupTable)
        String snap5 = "${suiteName}_5"
        String ts5 = backupAndWait(snap5, dupTable)
        startRestore(dbB, snap5, ts5, dupTable)
        // the staging table appears when the job creates it; fail the append of one tablet of its p1
        assertTrue(waitUntil(120) { stagingTableExists(dbB, dupTable) })
        def tablets = sql_return_maparray "SHOW TABLETS FROM ${dbB}.${stagingTable(dupTable)} PARTITION (p1)"
        assertFalse(tablets.isEmpty())
        enableDebug("Tablet.revise_tablet_meta_fail", [tablet_id: tablets[0].TabletId as String])
        job = waitRestoreDone(dbB, snap5, 600)
        assertEquals("CANCELLED", job.State)
        assertEquals(rowsBefore, rowsOf(dbB, dupTable))
        assertEquals(versionsBefore, versionsOf(dbB, dupTable))
        assertTrue(waitUntil(60) { !stagingTableExists(dbB, dupTable) })
        // the failure is gone: restoring again works
        disableDebug("Tablet.revise_tablet_meta_fail")
        job = restoreAndWait(dbB, snap5, ts5, dupTable)
        assertEquals("FINISHED", job.State)
        assertSameAsSource(dbB, dupTable)
    } finally {
        disableDebug("Tablet.revise_tablet_meta_fail")
        setConfig("restore_local_source_task_max_retry", "10")
    }

    // ---- 8. the job is cancelled by the user in the middle of the commit: the staging table is dropped, B is not
    // changed, and the scheduler repairs the replicas of B again ----
    String snap8 = "${suiteName}_8"
    String ts8 = null
    try {
        writeIntoP1(dupTable)
        ts8 = backupAndWait(snap8, dupTable)
        rowsBefore = rowsOf(dbB, dupTable)
        versionsBefore = versionsOf(dbB, dupTable)
        // the first append of every backend waits
        enableDebug("SnapshotLoader.append_increment.delay", [sleep_ms: 60000, execute: 1])
        startRestore(dbB, snap8, ts8, dupTable)
        assertTrue(waitUntil(300) {
            def result = sql_return_maparray """ SHOW RESTORE FROM ${dbB} WHERE Label = "${snap8}" """
            return !result.isEmpty() && result.last().State == "COMMITTING"
        })
        sql "CANCEL RESTORE FROM ${dbB}"
        job = waitRestoreDone(dbB, snap8, 120)
        assertEquals("CANCELLED", job.State)
        assertEquals(rowsBefore, rowsOf(dbB, dupTable))
        assertEquals(versionsBefore, versionsOf(dbB, dupTable))
        assertTrue(waitUntil(60) { !stagingTableExists(dbB, dupTable) })
    } finally {
        disableDebug("SnapshotLoader.append_increment.delay")
    }
    // the waiting appends end, then a replica of B marked bad is repaired by a clone as in any cluster
    sleep(70000)
    def tabletsOfB = sql_return_maparray "SHOW TABLETS FROM ${dbB}.${dupTable}"
    def victim = tabletsOfB[0]
    String since8 = feNow()
    // the protection is released with the job: B and its partitions are NORMAL, and the scheduler does not reject
    // any tablet for the restore job any more
    def tableStates = sql_return_maparray "SHOW PROC '/dbs/${dbIdOf(dbB)}'"
    assertEquals("NORMAL", tableStates.find { it.TableName.toString() == dupTable }.State.toString())
    sql """ADMIN SET REPLICA STATUS PROPERTIES("tablet_id" = "${victim.TabletId}",
            "backend_id" = "${victim.BackendId}", "status" = "bad")"""
    sleep(60000)
    assertEquals(0, protectedSince(since8))
    // Whether the bad replica is repaired within the wait depends on the cluster (a report may set the replica
    // back to ok on a small cluster); only log it.
    logger.info("clones of the bad replica of tablet ${victim.TabletId}: "
            + "${clonesSince(since8, [victim.TabletId.toString()] as Set)}")
    // the same backup restores now
    job = restoreAndWait(dbB, snap8, ts8, dupTable)
    assertEquals("FINISHED", job.State)
    assertSameAsSource(dbB, dupTable)

    // ---- 9. what is not an incremental append or a local source snapshot is as before ----
    // a whole download (no reuse) whose moves fail more often than the limit of the retries of the local source
    // tasks: the move is resent, the job is not cancelled
    setConfig("enable_restore_partition_reuse", "false")
    setConfig("restore_local_source_task_max_retry", "2")
    try {
        String snap9 = "${suiteName}_9"
        String ts9 = backupAndWait(snap9, dupTable)
        enableDebug("SnapshotLoader.move.fail", [execute: 10])
        job = restoreAndWait(dbC, snap9, ts9, dupTable, 900)
        assertEquals("FINISHED", job.State)
        assertSameAsSource(dbC, dupTable)
    } finally {
        disableDebug("SnapshotLoader.move.fail")
        setConfig("enable_restore_partition_reuse", "true")
        setConfig("restore_local_source_task_max_retry", "10")
    }

    // ---- MoW with the experiment switch (hidden state models) ----
    setConfig("restore_reuse_allow_hidden_state_models", "true")
    try {
        String snapM0 = "${suiteName}_m0"
        String tsM0 = backupAndWait(snapM0, mowTable)
        job = restoreAndWait(dbB, snapM0, tsM0, mowTable)
        assertEquals("FINISHED", job.State)
        assertSameAsSource(dbB, mowTable)
        sleep(5000)

        // a late append of a MoW table, with the evidence: FINISHED
        writeIntoP1(mowTable)
        String snapM1 = "${suiteName}_m1"
        String tsM1 = backupAndWait(snapM1, mowTable)
        enableDebugOnLateBe("SnapshotLoader.append_increment.delay", [sleep_ms: lateMs, execute: 1])
        job = restoreAndWait(dbB, snapM1, tsM1, mowTable)
        disableDebug("SnapshotLoader.append_increment.delay")
        assertEquals("FINISHED", job.State)
        assertSameAsSource(dbB, mowTable)

        // the append without its evidence: the digest does not prove the hidden state of a unique table, the job
        // is cancelled at once instead of being retried, and B is not changed
        writeIntoP1(mowTable)
        String snapM2 = "${suiteName}_m2"
        String tsM2 = backupAndWait(snapM2, mowTable)
        String mowRowsBefore = rowsOf(dbB, mowTable)
        enableDebug("SnapshotLoader.append_increment.fail_before_stage", [execute: 1])
        job = restoreAndWait(dbB, snapM2, tsM2, mowTable)
        assertEquals("CANCELLED", job.State)
        assertTrue(job.Status.toString().contains("RESTORE_NOT_RETRYABLE"), job.Status.toString())
        assertEquals(mowRowsBefore, rowsOf(dbB, mowTable))
    } finally {
        disableDebug("SnapshotLoader.append_increment.delay")
        disableDebug("SnapshotLoader.append_increment.fail_before_stage")
        setConfig("restore_reuse_allow_hidden_state_models", "false")
    }

    for (String db : [dbA, dbB, dbC]) {
        sql "DROP DATABASE IF EXISTS ${db} FORCE"
    }
}
