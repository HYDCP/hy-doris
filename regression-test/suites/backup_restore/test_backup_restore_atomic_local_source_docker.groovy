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

import org.apache.doris.regression.suite.ClusterOptions

// The restart of the FE and of a BE in the middle of the commit of an atomic restore with an incremental append:
//  - FE restart (a single FE here, it stands for a failover): the state COMMITTING is not written to the edit log, so
//    the restarted FE runs the job from COMMIT again and sends the move tasks again; the tablets which are already
//    appended are judged by their evidence (APPENDED), the late one is appended. The job is FINISHED and the data
//    equals the source.
//  - BE restart before the append: the evidence of the load (LOCAL_LOADED) is in the tablet meta, so it survives;
//    the move is sent again and appends. After the job, all BEs restart again (after the appends): the data and the
//    evidence are still right, restoring the same backup again keeps every partition.
// The cluster needs the docker framework (docker image), and the S3 settings of the other backup_restore suites.
suite("test_backup_restore_atomic_local_source_docker", "backup_restore,docker") {
    def options = new ClusterOptions()
    options.feConfigs += [
        "enable_restore_partition_reuse=true",
        "enable_restore_incremental_append=true",
        "enable_restore_atomic_reuse=true",
        "restore_reuse_min_partition_bytes=0",
    ]
    options.beConfigs += ["enable_java_support=false"]
    options.enableDebugPoints()
    options.feNum = 1
    options.beNum = 3

    docker(options) {
        String suiteName = "test_backup_restore_atomic_local_source_docker"
        String repoName = "${suiteName}_repo_" + UUID.randomUUID().toString().replace("-", "")
        String dbA = "${suiteName}_a"
        String dbB = "${suiteName}_b"
        String tbl = "${suiteName}_dup"

        def syncer = getSyncer()
        syncer.createS3Repository(repoName)

        for (String db : [dbA, dbB]) {
            sql "CREATE DATABASE IF NOT EXISTS ${db}"
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
                PARTITION p2 VALUES LESS THAN ("2000")
            )
            DISTRIBUTED BY HASH(`id`) BUCKETS 3
            PROPERTIES ("replication_num" = "3", "disable_auto_compaction" = "true")
            """
        List<String> values = []
        for (int i = 0; i < 1900; i += 7) {
            values.add("(${i}, 'value of ${i}')")
        }
        sql "INSERT INTO ${dbA}.${tbl} VALUES ${values.join(",")}"
        sql "sync"

        int writes = 0
        def backupAndWait = { String snapshot ->
            sql """
                BACKUP SNAPSHOT ${dbA}.${snapshot}
                TO `${repoName}`
                ON (${tbl})
                PROPERTIES ("logical_digest" = "true")
            """
            syncer.waitSnapshotFinish(dbA)
            return syncer.getSnapshotTimestamp(repoName, snapshot)
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
        def waitRestoreDone = { String snapshot, long timeoutSec ->
            def job = null
            for (long waited = 0; waited < timeoutSec; waited += 3) {
                try {
                    def result = sql_return_maparray """ SHOW RESTORE FROM ${dbB} WHERE Label = "${snapshot}" """
                    if (!result.isEmpty()) {
                        job = result.last()
                        if (job.State == "FINISHED" || job.State == "CANCELLED") {
                            return job
                        }
                    }
                } catch (Exception e) {
                    // the FE is restarting
                    logger.info("show restore failed: ${e}")
                }
                sleep(3000)
            }
            assertTrue(false, "the restore job ${snapshot} is not done after ${timeoutSec}s, state: ${job?.State}")
        }
        def jobState = { String snapshot ->
            try {
                def result = sql_return_maparray """ SHOW RESTORE FROM ${dbB} WHERE Label = "${snapshot}" """
                return result.isEmpty() ? null : result.last().State
            } catch (Exception e) {
                return null
            }
        }
        def rowsOf = { String db ->
            def r = sql """SELECT COUNT(*), SUM(CRC32(CONCAT(CAST(id AS STRING), '|', value))) FROM ${db}.${tbl}"""
            return "${r[0][0]}:${r[0][1]}".toString()
        }
        def writeIntoP1 = {
            ++writes
            sql "INSERT INTO ${dbA}.${tbl} VALUES (${500 + writes}, 'increment ${writes}')"
            sql "sync"
        }
        def delayAppends = { long ms ->
            GetDebugPoint().enableDebugPointForAllBEs("SnapshotLoader.append_increment.delay",
                    [sleep_ms: ms, execute: 1])
        }

        // B = A
        String snap0 = "${suiteName}_0"
        startRestore(snap0, backupAndWait(snap0))
        assertEquals("FINISHED", waitRestoreDone(snap0, 600).State)
        assertEquals(rowsOf(dbA), rowsOf(dbB))
        sleep(5000)

        // ---- 6. FE restart while the appends are going on ----
        writeIntoP1()
        String snap1 = "${suiteName}_1"
        String ts1 = backupAndWait(snap1)
        delayAppends(40000)
        startRestore(snap1, ts1)
        for (long waited = 0; waited < 300 && jobState(snap1) != "COMMITTING"; waited += 1) {
            sleep(1000)
        }
        assertEquals("COMMITTING", jobState(snap1))
        cluster.restartFrontends()
        sleep(15000)
        context.reconnectFe()
        def job = waitRestoreDone(snap1, 900)
        assertEquals("FINISHED", job.State)
        assertEquals(rowsOf(dbA), rowsOf(dbB))
        sleep(5000)

        // ---- 7. BE restart before the appends, and after them ----
        writeIntoP1()
        String snap2 = "${suiteName}_2"
        String ts2 = backupAndWait(snap2)
        delayAppends(60000)
        startRestore(snap2, ts2)
        for (long waited = 0; waited < 300 && jobState(snap2) != "COMMITTING"; waited += 1) {
            sleep(1000)
        }
        assertEquals("COMMITTING", jobState(snap2))
        // the appends wait: restart one BE, its waiting task is lost with the process and the FE sends it again
        cluster.restartBackends(1)
        job = waitRestoreDone(snap2, 900)
        assertEquals("FINISHED", job.State)
        assertEquals(rowsOf(dbA), rowsOf(dbB))
        // after the appends: restart all the BEs, the data is the same, and a restore of the same backup keeps all
        sleep(5000)
        cluster.restartBackends(1, 2, 3)
        sleep(30000)
        assertEquals(rowsOf(dbA), rowsOf(dbB))
        startRestore(snap2, ts2)
        job = waitRestoreDone(snap2, 900)
        assertEquals("FINISHED", job.State)
        assertEquals(rowsOf(dbA), rowsOf(dbB))
    }
}
