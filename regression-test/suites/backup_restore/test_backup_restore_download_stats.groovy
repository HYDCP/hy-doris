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

// The DownloadStats column of SHOW RESTORE. A restore from a repository has no lineage reuse: every file is
// downloaded, linked_* is 0, and the reuse ratio is 0 (or near 0), also when the same snapshot is restored
// again. The reuse of the http snapshot path (CCR full sync) needs an upstream cluster and is checked
// outside of the regression tests.
suite("test_backup_restore_download_stats", "backup_restore") {
    String suiteName = "test_backup_restore_download_stats"
    String repoName = "${suiteName}_repo_" + UUID.randomUUID().toString().replace("-", "")
    String srcDbName = "${suiteName}_src_db"
    String dbName = "${suiteName}_db"
    String tableName = "${suiteName}_table"
    String snapshotName = "${suiteName}_snapshot"
    int numPartitions = 3
    int numBuckets = 2

    def syncer = getSyncer()
    syncer.createS3Repository(repoName)

    for (String db : [srcDbName, dbName]) {
        sql "DROP DATABASE IF EXISTS ${db} FORCE"
        sql "CREATE DATABASE ${db}"
    }
    sql """
        CREATE TABLE ${srcDbName}.${tableName} (
            `id` INT NOT NULL,
            `value` VARCHAR(32) NULL
        )
        DUPLICATE KEY(`id`)
        PARTITION BY RANGE(`id`)
        (
            PARTITION p1 VALUES LESS THAN ("100"),
            PARTITION p2 VALUES LESS THAN ("200"),
            PARTITION p3 VALUES LESS THAN ("300")
        )
        DISTRIBUTED BY HASH(`id`) BUCKETS ${numBuckets}
        PROPERTIES
        (
            "replication_num" = "1"
        )
        """
    List<String> values = []
    for (int i = 0; i < 300; i += 3) {
        values.add("(${i}, 'value_${i}')")
    }
    sql "INSERT INTO ${srcDbName}.${tableName} VALUES ${values.join(",")}"
    sql "sync"

    sql """
        BACKUP SNAPSHOT ${srcDbName}.${snapshotName}
        TO `${repoName}`
        ON (${tableName})
    """
    syncer.waitSnapshotFinish(srcDbName)
    def timestamp = syncer.getSnapshotTimestamp(repoName, snapshotName)
    assertTrue(timestamp != null)

    // Restore into the db and return the DownloadStats of the restore job as a map.
    def restoreAndWait = {
        sql """
            RESTORE SNAPSHOT ${dbName}.${snapshotName}
            FROM `${repoName}`
            ON (${tableName})
            PROPERTIES
            (
                "backup_timestamp" = "${timestamp}",
                "reserve_replica" = "true"
            )
        """
        syncer.waitAllRestoreFinish(dbName)
        def restoreResult = sql_return_maparray """ SHOW RESTORE FROM ${dbName} WHERE Label = "${snapshotName}" """
        def restoreJob = restoreResult.last()
        logger.info("restore job: ${restoreJob}")
        assertEquals("FINISHED", restoreJob.State)
        assertTrue(restoreJob.DownloadStats != null)
        def stats = new JsonSlurper().parseText(restoreJob.DownloadStats as String)
        logger.info("download stats: ${stats}")
        return stats
    }

    def checkStats = { stats ->
        // all the backends report the stats, and nothing is linked from the lineage in a repository restore
        assertEquals(0, stats.replicas.not_reported)
        assertEquals(0, stats.linked_files)
        assertEquals(0, stats.linked_bytes)
        assertEquals(0, stats.unmatched_rowsets)
        assertEquals(0, stats.unmatched_reason.no_source_rowset_id)
        assertEquals(0, stats.unmatched_reason.source_not_in_snapshot)
        assertEquals(0, stats.unmatched_reason.version_mismatch)
        // the tablets are counted by replica, an empty tablet is not counted
        int replicas = stats.replicas.full_reuse + stats.replicas.partial_reuse + stats.replicas.no_reuse
        assertTrue(replicas > 0 && replicas <= numPartitions * numBuckets)
        assertTrue(stats.downloaded_files + stats.skipped_files > 0)
        // no partition is kept (partition level reuse is off), so kept_bytes is 0
        assertEquals(0, stats.kept_bytes)
        double total = stats.skipped_bytes + stats.downloaded_bytes
        assertEquals(stats.skipped_bytes / total, stats.reuse_ratio as double, 0.001)
    }

    // 1. New tables: everything is downloaded.
    def stats = restoreAndWait()
    checkStats(stats)
    assertTrue(stats.downloaded_files > 0)
    assertTrue(stats.downloaded_bytes > 0)
    assertEquals(0, stats.skipped_files)
    assertEquals(0, stats.reuse_ratio as double, 0.0)
    assertEquals(0, stats.replicas.full_reuse)
    assertEquals(0, stats.replicas.partial_reuse)

    // 2. The same snapshot again: no lineage reuse in the repository path, the ratio stays near 0.
    stats = restoreAndWait()
    checkStats(stats)
    assertTrue(stats.reuse_ratio as double < 0.1)

    def result = sql "SELECT COUNT(*) FROM ${dbName}.${tableName}"
    assertEquals(100, result[0][0] as int)

    sql "DROP DATABASE IF EXISTS ${srcDbName} FORCE"
    sql "DROP DATABASE IF EXISTS ${dbName} FORCE"
    sql "DROP REPOSITORY `${repoName}`"
}
