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

// A node of an old version does not report the capability of the restore reuse in the heartbeat: a restore job then
// downloads all the data with the reason MIXED_VERSIONS, the partitions that would be kept by their state token too.
// The FE debug point makes the check find such a node. The backup is the default one (no logical digest, no
// manifest). The FE needs enable_debug_points=true.
//   1. All the nodes are capable: a restore onto the same data keeps every partition by its token.
//   2. A node is not capable: the same restore keeps nothing, the rows are the same, the estimate says MIXED_VERSIONS.
//      The same for an atomic restore.
//   3. With restore_reuse_require_capability = false the debug point is not looked at, the partitions are kept again.
suite("test_backup_restore_token_mixed_versions", "backup_restore,nonConcurrent") {
    setFeConfigTemporary([enable_restore_partition_reuse: true, enable_restore_atomic_reuse: true,
            restore_reuse_min_partition_bytes: 0, restore_reuse_require_capability: true,
            restore_state_token_track: true, restore_reuse_trust_state_token: true,
            restore_reset_index_id: false]) {
    if (!getFeConfig("enable_debug_points").equals("true")) {
        logger.info("Config.enable_debug_points=true is required")
        return
    }
    String suiteName = "test_backup_restore_token_mixed_versions"
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

    for (String db : [dbA, dbB]) {
        sql "DROP DATABASE IF EXISTS ${db} FORCE"
        sql "CREATE DATABASE ${db}"
    }
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

    String snapshot = "${suiteName}_snap"
    sql """
        BACKUP SNAPSHOT ${dbA}.${snapshot}
        TO `${repoName}`
        ON (${tableName})
    """
    syncer.waitSnapshotFinish(dbA)
    def timestamp = syncer.getSnapshotTimestamp(repoName, snapshot)
    assertTrue(timestamp != null)

    // returns the ReuseEstimate of the restore job, which must be FINISHED
    def restoreAndWait = { boolean atomic ->
        String atomicProp = atomic ? ", \"atomic_restore\" = \"true\"" : ""
        sql """
            RESTORE SNAPSHOT ${dbB}.${snapshot}
            FROM `${repoName}`
            ON (${tableName})
            PROPERTIES ("backup_timestamp" = "${timestamp}", "reserve_replica" = "true"${atomicProp})
        """
        syncer.waitAllRestoreFinish(dbB)
        def result = sql_return_maparray """ SHOW RESTORE FROM ${dbB} WHERE Label = "${snapshot}" """
        def job = result.last()
        assertEquals("FINISHED", job.State)
        def estimate = new JsonSlurper().parseText(job.ReuseEstimate as String)
        logger.info("reuse estimate: ${estimate}")
        return estimate
    }
    def rowsOf = { String db ->
        def r = sql """SELECT COUNT(*), SUM(CRC32(CONCAT(CAST(id AS STRING), '|', v1))) FROM ${db}.${tableName}"""
        return "${r[0][0]}:${r[0][1]}".toString()
    }
    def keptByToken = { estimate -> (estimate.kept_token ?: 0) as int }
    def keptPartitions = { estimate -> (estimate.kept_partitions ?: 0) as int }

    // the first restore onto a new table downloads everything
    def estimate = restoreAndWait(false)
    assertEquals(0, keptPartitions(estimate))
    assertEquals(rowsOf(dbA), rowsOf(dbB))

    // 1. all the nodes are capable: every partition is kept by its token
    estimate = restoreAndWait(false)
    assertEquals(numPartitions, keptByToken(estimate))
    assertEquals(numPartitions, keptPartitions(estimate))
    assertEquals(null, estimate.mixed_versions)
    assertEquals(rowsOf(dbA), rowsOf(dbB))

    try {
        // 2. a node is not capable: nothing is kept, everything is downloaded, with the reason
        GetDebugPoint().enableDebugPointForAllFEs(debugPoint)
        estimate = restoreAndWait(false)
        assertEquals(0, keptByToken(estimate))
        assertEquals(0, keptPartitions(estimate))
        assertEquals(numPartitions, estimate.mixed_versions as int)
        assertEquals(rowsOf(dbA), rowsOf(dbB))
        order_qt_mixed_versions """SELECT id DIV 1000, COUNT(*), SUM(CRC32(CONCAT(CAST(id AS STRING), '|', v1)))
                FROM ${dbB}.${tableName} GROUP BY id DIV 1000"""
        // the atomic restore too: the local source would be ignored by an old backend
        estimate = restoreAndWait(true)
        assertEquals(0, (estimate.kept_atomic ?: 0) as int)
        assertEquals(0, keptPartitions(estimate))
        assertEquals(numPartitions, estimate.mixed_versions as int)
        assertEquals(rowsOf(dbA), rowsOf(dbB))

        // 3. the check is turned off: the debug point is not looked at, the downloaded partitions adopted the tokens
        setConfig("restore_reuse_require_capability", "false")
        estimate = restoreAndWait(false)
        assertEquals(numPartitions, keptByToken(estimate))
        assertEquals(null, estimate.mixed_versions)
        estimate = restoreAndWait(true)
        assertEquals(numPartitions, (estimate.kept_atomic ?: 0) as int)
        assertEquals(numPartitions, keptByToken(estimate))
    } finally {
        setConfig("restore_reuse_require_capability", "true")
        GetDebugPoint().disableDebugPointForAllFEs(debugPoint)
    }
    assertEquals(rowsOf(dbA), rowsOf(dbB))
    order_qt_capability_not_required """SELECT id DIV 1000, COUNT(*),
            SUM(CRC32(CONCAT(CAST(id AS STRING), '|', v1))) FROM ${dbB}.${tableName} GROUP BY id DIV 1000"""

    sql "DROP REPOSITORY `${repoName}`"
    }
}
