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

// A node of an old version does not report the capability of the restore reuse in the heartbeat, the restore job then
// downloads all the data, with the reason MIXED_VERSIONS. The FE debug point makes the check find such a node.
//   1. All the nodes are capable: a restore onto the same data keeps every partition.
//   2. A node is not capable (the debug point): the same restore downloads everything, the rows are the same, and
//      the estimate says MIXED_VERSIONS.
//   3. With restore_reuse_require_capability = false the debug point is not looked at, the partitions are kept again.
suite("test_backup_restore_mixed_versions", "backup_restore,nonConcurrent") {
    // enable_backup_manifest is off by default in branch-3.1: the case depends on the manifest of the backup,
    // turn it on for the whole case (restored when the case ends). The body is not indented, to keep the diff small.
    setFeConfigTemporary([enable_backup_manifest: true]) {
    String suiteName = "test_backup_restore_mixed_versions"
    String repoName = "${suiteName}_repo_" + UUID.randomUUID().toString().replace("-", "")
    String dbA = "${suiteName}_a"
    String dbB = "${suiteName}_b"
    String tableName = "${suiteName}_dup"
    String debugPoint = "RestoreJob.restoreReuseCapability.mixedVersions"
    int numPartitions = 2

    def syncer = getSyncer()
    syncer.createS3Repository(repoName)

    def setConfig = { String name, String value ->
        sql """ADMIN SET FRONTEND CONFIG ("${name}" = "${value}")"""
    }
    def getConfig = { String name ->
        def rows = sql_return_maparray """ADMIN SHOW FRONTEND CONFIG LIKE "${name}" """
        return rows.find { it.Key == name }.Value as String
    }
    assertEquals("true", getConfig("restore_reuse_require_capability"))
    String origEnable = getConfig("enable_restore_partition_reuse")
    String origMinBytes = getConfig("restore_reuse_min_partition_bytes")
    setConfig("enable_restore_partition_reuse", "true")
    setConfig("restore_reuse_min_partition_bytes", "0")

    for (String db : [dbA, dbB]) {
        sql "DROP DATABASE IF EXISTS ${db} FORCE"
        sql "CREATE DATABASE ${db}"
    }

    try {
        sql """
            CREATE TABLE ${dbA}.${tableName} (
                `id` INT NOT NULL,
                `v1` VARCHAR(64) NULL
            )
            DUPLICATE KEY(`id`)
            PARTITION BY RANGE(`id`)
            (
                PARTITION p1 VALUES LESS THAN ("1000"),
                PARTITION p2 VALUES LESS THAN ("2000")
            )
            DISTRIBUTED BY HASH(`id`) BUCKETS 2
            PROPERTIES ("replication_num" = "1", "disable_auto_compaction" = "true")
        """
        for (int b = 0; b < 3; ++b) {
            List<String> values = []
            for (int p = 0; p < numPartitions; ++p) {
                for (int i = 0; i < 20; ++i) {
                    int id = p * 1000 + b * 20 + i
                    values.add("(${id}, 'value of ${id} in batch ${b}')")
                }
            }
            sql "INSERT INTO ${dbA}.${tableName} VALUES ${values.join(",")}"
        }
        sql "sync"
        long rows = numPartitions * 3 * 20

        String snapshot = "${suiteName}_snap"
        sql """
            BACKUP SNAPSHOT ${dbA}.${snapshot}
            TO `${repoName}`
            ON (${tableName})
            PROPERTIES ("logical_digest" = "true")
        """
        syncer.waitSnapshotFinish(dbA)
        def timestamp = syncer.getSnapshotTimestamp(repoName, snapshot)
        assertTrue(timestamp != null)

        def lastEstimate = null
        def lastStats = null
        def restoreAndWait = {
            sql """
                RESTORE SNAPSHOT ${dbB}.${snapshot}
                FROM `${repoName}`
                ON (${tableName})
                PROPERTIES ("backup_timestamp" = "${timestamp}", "reserve_replica" = "true")
            """
            syncer.waitAllRestoreFinish(dbB)
            def result = sql_return_maparray """ SHOW RESTORE FROM ${dbB} WHERE Label = "${snapshot}" """
            def job = result.last()
            assertEquals("FINISHED", job.State)
            lastStats = new JsonSlurper().parseText(job.DownloadStats as String)
            lastEstimate = job.ReuseEstimate == null ? null : new JsonSlurper().parseText(job.ReuseEstimate as String)
            logger.info("download stats: ${lastStats}, reuse estimate: ${lastEstimate}")
        }
        def waitDataSize = {
            for (int i = 0; i < 30; ++i) {
                def tablets = sql_return_maparray "SHOW TABLETS FROM ${dbB}.${tableName}"
                if (tablets.sum { it.RowCount as long } == rows
                        && !tablets.any { (it.LocalDataSize as long) == 0 && (it.RowCount as long) > 0 }) {
                    return
                }
                sleep(5000)
            }
        }
        def rowsOf = { String db ->
            def r = sql """SELECT COUNT(*), SUM(CRC32(CONCAT(CAST(id AS STRING), '|', IFNULL(CAST(v1 AS STRING), 'NULL')))) FROM ${db}.${tableName}"""
            return "${r[0][0]}:${r[0][1]}"
        }

        // the first restore onto a new table downloads everything
        restoreAndWait()
        assertTrue((lastStats.downloaded_bytes as long) > 0)
        assertEquals(rowsOf(dbA), rowsOf(dbB))
        waitDataSize()

        // 1. all the nodes are capable: every partition is kept
        restoreAndWait()
        assertEquals(numPartitions, (lastEstimate?.kept_partitions ?: 0) as int)
        assertEquals(0L, lastStats.downloaded_bytes as long)
        assertEquals(null, lastEstimate?.mixed_versions)

        // 2. a node is not capable: nothing is kept, everything is downloaded, with the reason
        GetDebugPoint().enableDebugPointForAllFEs(debugPoint)
        try {
            restoreAndWait()
            assertEquals(0, (lastEstimate?.kept_partitions ?: 0) as int)
            assertTrue((lastStats.downloaded_bytes as long) > 0)
            assertEquals(numPartitions, lastEstimate.mixed_versions as int)
            assertEquals(numPartitions, lastEstimate.l0_passed_but_mixed_versions as int)
            assertEquals(rowsOf(dbA), rowsOf(dbB))
            waitDataSize()

            // 3. the check is turned off: the debug point is not looked at
            setConfig("restore_reuse_require_capability", "false")
            restoreAndWait()
            assertEquals(numPartitions, (lastEstimate?.kept_partitions ?: 0) as int)
            assertEquals(null, lastEstimate?.mixed_versions)
        } finally {
            setConfig("restore_reuse_require_capability", "true")
            GetDebugPoint().disableDebugPointForAllFEs(debugPoint)
        }
        assertEquals(rowsOf(dbA), rowsOf(dbB))
    } finally {
        setConfig("enable_restore_partition_reuse", origEnable)
        setConfig("restore_reuse_min_partition_bytes", origMinBytes)
        sql "DROP DATABASE IF EXISTS ${dbA} FORCE"
        sql "DROP DATABASE IF EXISTS ${dbB} FORCE"
        sql "DROP REPOSITORY `${repoName}`"
    }
    }
}
