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

// A restore of a backup of the metadata only creates empty partitions at the version of the backup. They must not be
// named after the data of the source: a later full restore of the same partitions at the same version downloads them
// (a kept empty partition would lose the rows and report success). With restore_state_token_track and
// restore_reuse_trust_state_token on. Single replica.
//   1. Back up the table twice from the same versions: the data, and the metadata only.
//   2. Restore the metadata only into a new table: the partitions are empty.
//   3. Restore the data onto it: nothing is kept by a token, the rows equal the source.
//   4. The data again: now every partition is kept by its token.
suite("test_backup_restore_token_metadata_only", "backup_restore") {
    setFeConfigTemporary([enable_restore_partition_reuse: true, restore_reuse_min_partition_bytes: 0,
            restore_state_token_track: true, restore_reuse_trust_state_token: true,
            restore_reset_index_id: false]) {
    String suiteName = "test_backup_restore_token_metadata_only"
    String repoName = "${suiteName}_repo_" + UUID.randomUUID().toString().replace("-", "")
    String srcDbName = "${suiteName}_src_db"
    String dbName = "${suiteName}_db"
    String tableName = "${suiteName}_dup"
    int numPartitions = 3

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
            PARTITION p1 VALUES LESS THAN ("1000"),
            PARTITION p2 VALUES LESS THAN ("2000"),
            PARTITION p3 VALUES LESS THAN ("3000")
        )
        DISTRIBUTED BY HASH(`id`) BUCKETS 2
        PROPERTIES ("replication_num" = "1")
        """
    List<String> values = []
    for (int p = 0; p < numPartitions; ++p) {
        for (int i = 0; i < 20; ++i) {
            int id = p * 1000 + i
            values.add("(${id}, 'v${id}')")
        }
    }
    sql "INSERT INTO ${srcDbName}.${tableName} VALUES ${values.join(",")}"
    sql "sync"

    def backupAndWait = { String snapshot, String content ->
        sql """
            BACKUP SNAPSHOT ${srcDbName}.${snapshot}
            TO `${repoName}`
            ON (${tableName})
            PROPERTIES ("content" = "${content}")
        """
        syncer.waitSnapshotFinish(srcDbName)
        def timestamp = syncer.getSnapshotTimestamp(repoName, snapshot)
        assertTrue(timestamp != null)
        return timestamp
    }
    // returns the ReuseEstimate of the restore job, which must be FINISHED
    def restoreAndWait = { String snapshot, String timestamp ->
        sql """
            RESTORE SNAPSHOT ${dbName}.${snapshot}
            FROM `${repoName}`
            ON (${tableName})
            PROPERTIES ("backup_timestamp" = "${timestamp}", "reserve_replica" = "true")
        """
        syncer.waitAllRestoreFinish(dbName)
        def result = sql_return_maparray """ SHOW RESTORE FROM ${dbName} WHERE Label = "${snapshot}" """
        def job = result.last()
        logger.info("restore ${snapshot} into ${dbName}: ${job}")
        assertEquals("FINISHED", job.State)
        def estimate = new JsonSlurper().parseText(job.ReuseEstimate as String)
        logger.info("reuse estimate: ${estimate}")
        return estimate
    }
    def keptByToken = { estimate -> (estimate.kept_token ?: 0) as int }

    // 1. the same versions, twice
    String snapAll = "${suiteName}_all"
    String tsAll = backupAndWait(snapAll, "all")
    String snapMeta = "${suiteName}_meta"
    String tsMeta = backupAndWait(snapMeta, "metadata_only")

    // 2. the metadata only: empty partitions at the version of the backup
    restoreAndWait(snapMeta, tsMeta)
    order_qt_meta_only """SELECT COUNT(*) FROM ${dbName}.${tableName}"""

    // 3. the data at the same versions: downloaded, not kept by a token
    def estimate = restoreAndWait(snapAll, tsAll)
    assertEquals(0, keptByToken(estimate))
    order_qt_after_full """SELECT id DIV 1000, COUNT(*), SUM(CRC32(CONCAT(CAST(id AS STRING), '|', value)))
            FROM ${dbName}.${tableName} GROUP BY id DIV 1000"""

    // 4. the data again: kept by the tokens the full restore gave
    estimate = restoreAndWait(snapAll, tsAll)
    assertEquals(numPartitions, keptByToken(estimate))
    order_qt_kept """SELECT id DIV 1000, COUNT(*), SUM(CRC32(CONCAT(CAST(id AS STRING), '|', value)))
            FROM ${dbName}.${tableName} GROUP BY id DIV 1000"""

    sql "DROP REPOSITORY `${repoName}`"
    }
}
