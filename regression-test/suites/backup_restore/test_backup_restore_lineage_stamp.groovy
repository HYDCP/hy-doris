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

// The restore lineage of a partition is not shown anywhere, so it is checked through the ReuseEstimate
// column of SHOW RESTORE: restoring the same backup again without any write is only counted as reusable
// if the previous restore wrote the lineage of that backup to the partitions.
suite("test_backup_restore_lineage_stamp", "backup_restore") {
    String suiteName = "test_backup_restore_lineage_stamp"
    String repoName = "${suiteName}_repo_" + UUID.randomUUID().toString().replace("-", "")
    // The source db, the db restored from the source, and the db restored from the restored db.
    String srcDbName = "${suiteName}_src_db"
    String dbName = "${suiteName}_db"
    String chainDbName = "${suiteName}_chain_db"
    String tableName = "${suiteName}_table"
    String aggTableName = "${suiteName}_agg_table"
    String snapshotName = "${suiteName}_snapshot"
    String chainSnapshotName = "${suiteName}_chain_snapshot"
    int numPartitions = 3
    int numRowsPerPartition = 5
    int numRows = numPartitions * numRowsPerPartition

    def syncer = getSyncer()
    syncer.createS3Repository(repoName)

    for (String db : [srcDbName, dbName, chainDbName]) {
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
            PARTITION p1 VALUES LESS THAN ("10"),
            PARTITION p2 VALUES LESS THAN ("20"),
            PARTITION p3 VALUES LESS THAN ("30")
        )
        DISTRIBUTED BY HASH(`id`) BUCKETS 2
        PROPERTIES
        (
            "replication_num" = "1"
        )
        """
    sql """
        CREATE TABLE ${srcDbName}.${aggTableName} (
            `id` INT NOT NULL,
            `count` BIGINT SUM DEFAULT "0"
        )
        AGGREGATE KEY(`id`)
        DISTRIBUTED BY HASH(`id`) BUCKETS 2
        PROPERTIES
        (
            "replication_num" = "1"
        )
        """

    List<String> values = []
    for (int p = 0; p < numPartitions; ++p) {
        for (int i = 0; i < numRowsPerPartition; ++i) {
            int id = p * 10 + i
            values.add("(${id}, 'v${id}')")
        }
    }
    sql "INSERT INTO ${srcDbName}.${tableName} VALUES ${values.join(",")}"
    sql "INSERT INTO ${srcDbName}.${aggTableName} VALUES (1, 1), (2, 2)"
    sql "sync"

    def backupAndWait = { String db, String snapshot, String onClause ->
        sql """
            BACKUP SNAPSHOT ${db}.${snapshot}
            TO `${repoName}`
            ON (${onClause})
        """
        syncer.waitSnapshotFinish(db)
        def timestamp = syncer.getSnapshotTimestamp(repoName, snapshot)
        assertTrue(timestamp != null)
        return timestamp
    }

    // Restore into the db and return the ReuseEstimate of the restore job as a map.
    def restoreAndWait = { String db, String snapshot, String timestamp, String onClause, boolean atomic ->
        sql """
            RESTORE SNAPSHOT ${db}.${snapshot}
            FROM `${repoName}`
            ON (${onClause})
            PROPERTIES
            (
                "backup_timestamp" = "${timestamp}",
                "reserve_replica" = "true",
                "atomic_restore" = "${atomic}"
            )
        """
        syncer.waitAllRestoreFinish(db)
        def restoreResult = sql_return_maparray """ SHOW RESTORE FROM ${db} WHERE Label = "${snapshot}" """
        def restoreJob = restoreResult.last()
        logger.info("restore ${snapshot} into ${db} on (${onClause}), atomic: ${atomic}, result: ${restoreJob}")
        assertEquals("FINISHED", restoreJob.State)
        def estimate = new JsonSlurper().parseText(restoreJob.ReuseEstimate as String)
        logger.info("reuse estimate: ${estimate}")
        return estimate
    }

    def assertRowCount = { String db, String tbl, int expected ->
        def result = sql "SELECT COUNT(*) FROM ${db}.${tbl}"
        assertEquals(expected, result[0][0] as int)
    }

    String snapshot = backupAndWait(srcDbName, snapshotName, "${tableName}, ${aggTableName}")

    // 1. New table path: the tables do not exist locally, nothing to reuse.
    def estimate = restoreAndWait(dbName, snapshotName, snapshot, "${tableName}, ${aggTableName}", false)
    assertEquals(numPartitions + 1, estimate.partitions)
    assertEquals(numPartitions + 1, estimate.no_local)
    assertEquals(0, estimate.reusable)
    assertRowCount(dbName, tableName, numRows)

    // 2. Restore the same backup again without any write: the new table path wrote the lineage, so all
    // partitions pass L0. The aggregate table is not covered and counted separately.
    estimate = restoreAndWait(dbName, snapshotName, snapshot, "${tableName}, ${aggTableName}", false)
    assertEquals(numPartitions + 1, estimate.partitions)
    assertEquals(numPartitions, estimate.reusable)
    assertEquals(1, estimate.l0_passed_but_aggregate_table)
    assertEquals(0, estimate.no_lineage)
    // The data size is reported by BE asynchronously, it may still be 0 here.
    assertTrue((estimate.reusable_bytes_single_replica as long) >= 0)
    assertRowCount(dbName, tableName, numRows)

    // 3. A write to p1 after the restore: p1 is not reusable, the others are.
    sql "INSERT INTO ${dbName}.${tableName} VALUES (1, 'new')"
    sql "sync"
    assertRowCount(dbName, tableName, numRows + 1)
    estimate = restoreAndWait(dbName, snapshotName, snapshot, "${tableName}", false)
    assertEquals(numPartitions, estimate.partitions)
    assertEquals(numPartitions - 1, estimate.reusable)
    assertEquals(1, estimate.local_version_changed)
    assertRowCount(dbName, tableName, numRows)

    // 4. Existing partition path (overwritten in place): it wrote the lineage again, p1 is reusable again.
    estimate = restoreAndWait(dbName, snapshotName, snapshot, "${tableName}", false)
    assertEquals(numPartitions, estimate.reusable)

    // 5. New partition path: p3 does not exist locally and is added to the existing table.
    sql "ALTER TABLE ${dbName}.${tableName} DROP PARTITION p3 FORCE"
    estimate = restoreAndWait(dbName, snapshotName, snapshot, "${tableName}", false)
    assertEquals(numPartitions - 1, estimate.reusable)
    assertEquals(1, estimate.no_local)
    assertRowCount(dbName, tableName, numRows)
    estimate = restoreAndWait(dbName, snapshotName, snapshot, "${tableName}", false)
    assertEquals(numPartitions, estimate.reusable)

    // 6. Atomic restore: all partitions pass L0, but atomic restore is not covered.
    estimate = restoreAndWait(dbName, snapshotName, snapshot, "${tableName}", true)
    assertEquals(numPartitions, estimate.partitions)
    assertEquals(numPartitions, estimate.l0_passed_but_atomic_restore)
    assertEquals(0, estimate.reusable)
    assertRowCount(dbName, tableName, numRows)
    // The table created by the atomic restore has the lineage.
    estimate = restoreAndWait(dbName, snapshotName, snapshot, "${tableName}", false)
    assertEquals(numPartitions, estimate.reusable)

    // 7. The partitions restored into dbName carry the lineage pointing to srcDbName, so does the backup of
    // them. Restoring that backup must write the lineage pointing to dbName, not pass the old one through.
    String chainSnapshot = backupAndWait(dbName, chainSnapshotName, "${tableName}")
    estimate = restoreAndWait(chainDbName, chainSnapshotName, chainSnapshot, "${tableName}", false)
    assertEquals(numPartitions, estimate.no_local)
    assertRowCount(chainDbName, tableName, numRows)
    // The lineage points to dbName.
    estimate = restoreAndWait(chainDbName, chainSnapshotName, chainSnapshot, "${tableName}", false)
    assertEquals(numPartitions, estimate.reusable)
    // It does not point to srcDbName, although the data and the versions are the same. If the lineage were
    // passed through from the backup meta, these partitions would be counted as reusable.
    estimate = restoreAndWait(chainDbName, snapshotName, snapshot, "${tableName}", false)
    assertEquals(numPartitions, estimate.lineage_mismatch)
    assertEquals(0, estimate.reusable)
    assertRowCount(chainDbName, tableName, numRows)

    for (String db : [srcDbName, dbName, chainDbName]) {
        sql "DROP DATABASE ${db} FORCE"
    }
    sql "DROP REPOSITORY `${repoName}`"
}
