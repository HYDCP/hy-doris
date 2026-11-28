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

// backup_restore_merge_idle_ticks: the states of a backup or restore job which wait for nothing are run in the same
// round of the backup handler, instead of one per round (backup_handler_update_interval_millis). Observed by the
// times of SHOW BACKUP / SHOW RESTORE: the backup goes from the end of its upload to FINISHED (SAVE_META,
// UPLOAD_INFO) at once, the atomic restore which keeps every partition goes from the end of its snapshots to the end
// of its (empty) download at once. The pause debug point of the restore still pauses a merged round, a cancel in the
// middle of a merged round works, and backup_restore_run_on_task_finish runs the jobs the same way.
// The FE needs enable_debug_points=true for the pause case, which is skipped otherwise.
suite("test_backup_restore_merge_idle_ticks", "backup_restore,nonConcurrent") {
    // enable_backup_manifest is off by default in branch-3.1: the case depends on the manifest of the backup,
    // turn it on for the whole case (restored when the case ends). The body is not indented, to keep the diff small.
    setFeConfigTemporary([enable_backup_manifest: true, enable_restore_partition_reuse: false,
            enable_restore_incremental_append: false, enable_restore_atomic_reuse: false,
            restore_reuse_min_partition_bytes: 0, restore_reuse_default_check_level: "full",
            backup_restore_merge_idle_ticks: false, backup_restore_run_on_task_finish: false]) {
    String suiteName = "test_backup_restore_merge_idle_ticks"
    String repoName = "${suiteName}_repo_" + UUID.randomUUID().toString().replace("-", "")
    String dbA = "${suiteName}_a"
    String dbB = "${suiteName}_b"
    String tbl = "${suiteName}_dup"
    int numPartitions = 3
    long tickMs = (getFeConfig("backup_handler_update_interval_millis") as long)
    boolean debugPoints = getFeConfig("enable_debug_points").equals("true")

    def syncer = getSyncer()
    syncer.createS3Repository(repoName)

    def setConfig = { String name, String value ->
        sql """ADMIN SET FRONTEND CONFIG ("${name}" = "${value}")"""
    }
    List<String> configNames = ["enable_restore_partition_reuse", "enable_restore_incremental_append",
            "enable_restore_atomic_reuse", "restore_reuse_min_partition_bytes", "restore_reuse_default_check_level",
            "backup_restore_merge_idle_ticks", "backup_restore_run_on_task_finish"]
    def origConfigs = configNames.collectEntries { [(it): getFeConfig(it)] }
    setConfig("enable_restore_partition_reuse", "true")
    setConfig("enable_restore_incremental_append", "true")
    setConfig("enable_restore_atomic_reuse", "true")
    setConfig("restore_reuse_min_partition_bytes", "0")
    setConfig("restore_reuse_default_check_level", "full")
    setConfig("backup_restore_merge_idle_ticks", "false")
    setConfig("backup_restore_run_on_task_finish", "false")

    for (String db : [dbA, dbB]) {
        sql "DROP DATABASE IF EXISTS ${db} FORCE"
        sql "CREATE DATABASE ${db}"
    }
    sql """
        CREATE TABLE ${dbA}.${tbl} (
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
        DISTRIBUTED BY HASH(`id`) BUCKETS 2
        PROPERTIES ("replication_num" = "1", "disable_auto_compaction" = "true")
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
    sql "sync"

    // ---- helpers ----
    def timeOf = { String s -> java.sql.Timestamp.valueOf(s.toString()).time }
    def backupAndWait = { String snapshot ->
        sql """
            BACKUP SNAPSHOT ${dbA}.${snapshot}
            TO `${repoName}`
            ON (${tbl})
            PROPERTIES ("logical_digest" = "true")
        """
        syncer.waitSnapshotFinish(dbA)
        def timestamp = syncer.getSnapshotTimestamp(repoName, snapshot)
        assertTrue(timestamp != null)
        def job = (sql_return_maparray """ SHOW BACKUP FROM ${dbA} WHERE SnapshotName = "${snapshot}" """).last()
        assertEquals("FINISHED", job.State)
        logger.info("backup ${snapshot}: snapshots ${job.SnapshotFinishedTime}, upload ${job.UploadFinishedTime}, "
                + "finished ${job.FinishedTime}")
        return [timestamp: timestamp, job: job]
    }
    def startRestore = { String snapshot, String timestamp ->
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
    def waitRestoreDone = { String snapshot, long timeoutSec ->
        def job = null
        for (long waited = 0; waited < timeoutSec; waited += 1) {
            job = jobOf(snapshot)
            if (job != null && (job.State == "FINISHED" || job.State == "CANCELLED")) {
                logger.info("restore ${snapshot}: ${job.State}, meta ${job.MetaPreparedTime}, snapshots "
                        + "${job.SnapshotFinishedTime}, download ${job.DownloadFinishedTime}, finished "
                        + "${job.FinishedTime}")
                return job
            }
            sleep(1000)
        }
        assertTrue(false, "the restore job ${snapshot} is not done after ${timeoutSec}s, state: ${job?.State}")
    }
    def restoreAndWait = { String snapshot, String timestamp, long timeoutSec = 600 ->
        startRestore(snapshot, timestamp)
        return waitRestoreDone(snapshot, timeoutSec)
    }
    def rowsOf = { String db ->
        def r = sql """SELECT COUNT(*), SUM(CRC32(CONCAT(CAST(id AS STRING), '|', value))) FROM ${db}.${tbl}"""
        return "${r[0][0]}:${r[0][1]}".toString()
    }
    def assertKeptAll = { job ->
        assertEquals("FINISHED", job.State)
        def estimate = new JsonSlurper().parseText(job.ReuseEstimate as String)
        assertEquals(numPartitions, estimate.kept_atomic as int)
        assertEquals(rowsOf(dbA), rowsOf(dbB))
    }
    // the gap between the end of the download and the end of the snapshots of a restore which downloads nothing: the
    // DOWNLOAD state (sends nothing) and the DOWNLOADING state (waits for nothing), two rounds when not merged
    def downloadGapMs = { job -> timeOf(job.DownloadFinishedTime) - timeOf(job.SnapshotFinishedTime) }
    // the gap between FINISHED and the end of the upload of a backup: SAVE_META and UPLOAD_INFO, two rounds when not
    // merged
    def finishGapMs = { job -> timeOf(job.FinishedTime) - timeOf(job.UploadFinishedTime) }
    // The clock of the host may step (the time stamps of the jobs are read from it, in the order of the states): a
    // gap which is negative, or longer than many rounds, is a step of the clock, not the job. Such a measurement is
    // made again, a few times, with a new backup or restore each time.
    int backups = 0
    def lastBackup = null
    def lastJob = null
    def nextBackup = { ->
        ++backups
        lastBackup = backupAndWait("${suiteName}_b${backups}")
        return finishGapMs(lastBackup.job)
    }
    // restores the last backup once more: every partition is kept, nothing is downloaded
    def nextRestore = { ->
        lastJob = restoreAndWait("${suiteName}_b${backups}", lastBackup.timestamp)
        assertKeptAll(lastJob)
        return downloadGapMs(lastJob)
    }
    def measure = { String what, Closure once, Closure ok ->
        long gap = 0
        for (int attempt = 1; attempt <= 3; ++attempt) {
            gap = once() as long
            if (gap < 0 || gap > 20 * tickMs) {
                logger.warn("${what}: gap ${gap} ms looks like a step of the clock, measured again")
                continue
            }
            if (ok(gap)) {
                return gap
            }
            logger.warn("${what}: gap ${gap} ms, tick ${tickMs} ms, attempt ${attempt}")
        }
        assertTrue(false, "${what}: gap ${gap} ms, tick ${tickMs} ms")
    }

    // B = A, nothing kept yet
    String snap0 = "${suiteName}_0"
    def backup0 = backupAndWait(snap0)
    def job = restoreAndWait(snap0, backup0.timestamp)
    assertEquals("FINISHED", job.State)
    assertEquals(rowsOf(dbA), rowsOf(dbB))
    sleep(5000)

    // the time stamps are in seconds, and the gaps of one state per round are two rounds at least: the merged
    // rounds are told by a gap below one round
    assertTrue(tickMs >= 2000, "backup_handler_update_interval_millis ${tickMs} is too short for this test")

    // ---- 1. the switch off: one state per round ----
    nextBackup()
    measure("restore, one state per round", nextRestore, { long gap -> gap >= tickMs })
    measure("backup, one state per round", nextBackup, { long gap -> gap >= tickMs })

    // ---- 2. the switch on: the idle states in the same round, the result is the same ----
    setConfig("backup_restore_merge_idle_ticks", "true")
    sleep(5000)
    measure("restore, merged rounds", nextRestore, { long gap -> gap < tickMs })
    measure("backup, merged rounds", nextBackup, { long gap -> gap < tickMs })
    sleep(5000)
    String snap2 = "${suiteName}_b${backups}"
    def backup2 = lastBackup
    job = restoreAndWait(snap2, backup2.timestamp)
    assertKeptAll(job)

    // ---- 3. the pause debug point still stops a merged round, set before the job is created ----
    if (debugPoints) {
        // the label of a restore is the name of the backup snapshot: the same backup is restored once more
        try {
            GetDebugPoint().enableDebugPointForAllFEs("FE.PAUSE_NON_PENDING_RESTORE_JOB", [value: snap2])
            startRestore(snap2, backup2.timestamp)
            // the job is created and its replicas are made, then it is paused at the first state after that
            sleep(Math.max(3 * tickMs, 20000))
            def paused = jobOf(snap2)
            assertTrue(paused != null && paused.State != "FINISHED" && paused.State != "CANCELLED"
                    && paused.State != "PENDING", "state: ${paused?.State}")
            String state = paused.State
            sleep(Math.max(3 * tickMs, 10000))
            assertEquals(state, jobOf(snap2).State)
        } finally {
            GetDebugPoint().disableDebugPointForAllFEs("FE.PAUSE_NON_PENDING_RESTORE_JOB")
        }
        job = waitRestoreDone(snap2, 600)
        assertKeptAll(job)
    } else {
        logger.info("Config.enable_debug_points=true is required for the pause case, skip it")
    }

    // ---- 4. a cancel by the user: the job ends cancelled, B is not changed, the next restore works ----
    String rowsBefore = rowsOf(dbB)
    startRestore(snap2, backup2.timestamp)
    sleep(1000)
    try {
        sql "CANCEL RESTORE FROM ${dbB}"
    } catch (Exception e) {
        // the job may be done already on a fast cluster
        logger.info("cancel restore: ${e}")
    }
    job = waitRestoreDone(snap2, 600)
    assertTrue(job.State == "CANCELLED" || job.State == "FINISHED", job.State)
    assertEquals(rowsBefore, rowsOf(dbB))
    sleep(5000)
    job = restoreAndWait(snap2, backup2.timestamp)
    assertKeptAll(job)

    // ---- 5. the jobs run as soon as their last task is reported, too ----
    setConfig("backup_restore_run_on_task_finish", "true")
    sleep(5000)
    measure("backup, with the nudge", nextBackup, { long gap -> gap < tickMs })
    measure("restore, with the nudge", nextRestore, { long gap -> gap < tickMs })
    // a write into p1: p1 is appended, the others are kept
    sql "INSERT INTO ${dbA}.${tbl} VALUES (5, 'new row of p1')"
    sql "sync"
    String snap6 = "${suiteName}_6"
    def backup6 = backupAndWait(snap6)
    job = restoreAndWait(snap6, backup6.timestamp)
    assertEquals("FINISHED", job.State)
    assertEquals(rowsOf(dbA), rowsOf(dbB))

    origConfigs.each { name, value -> setConfig(name, value) }
    for (String db : [dbA, dbB]) {
        sql "DROP DATABASE IF EXISTS ${db} FORCE"
    }
    sql "DROP REPOSITORY `${repoName}`"
    }
}
