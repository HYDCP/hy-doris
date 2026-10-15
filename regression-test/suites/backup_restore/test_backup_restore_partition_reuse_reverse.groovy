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

// Partition level reuse of restore (R2), the reverse path (L0 b): A is backed up and restored to B (the partitions
// of B carry the lineage that points to A), then B is backed up and restored back onto A. The partitions of A have no
// lineage, but the source partitions in the backup of B carry one that points to them, so the partitions of A that B
// did not change are kept and not downloaded, and the one that B changed is downloaded.
suite("test_backup_restore_partition_reuse_reverse", "backup_restore") {
    String suiteName = "test_backup_restore_partition_reuse_reverse"
    String repoName = "${suiteName}_repo_" + UUID.randomUUID().toString().replace("-", "")
    String dbA = "${suiteName}_a"
    String dbB = "${suiteName}_b"
    String dupTable = "${suiteName}_dup"
    String uniqTable = "${suiteName}_uniq"
    int numPartitions = 3

    def syncer = getSyncer()
    syncer.createS3Repository(repoName)

    def setConfig = { String name, String value ->
        sql """ADMIN SET FRONTEND CONFIG ("${name}" = "${value}")"""
    }
    setConfig("enable_restore_partition_reuse", "true")
    setConfig("restore_reuse_min_partition_bytes", "0")
    setConfig("restore_reuse_default_check_level", "full")
    // The tables of this case include unique tables (hidden state: delete sign rows, overwritten versions), which
    // partition level reuse only reuses with the experiment switch below (default false: only duplicate tables are
    // reused). Turn it on to cover the mechanics of the unique tables, and restore it at the end.
    setConfig("restore_reuse_allow_hidden_state_models", "true")

    for (String db : [dbA, dbB]) {
        sql "DROP DATABASE IF EXISTS ${db} FORCE"
        sql "CREATE DATABASE ${db}"
    }
    for (String tbl : [dupTable, uniqTable]) {
        String keys = tbl == dupTable ? "DUPLICATE KEY(`id`)" : "UNIQUE KEY(`id`)"
        String props = tbl == dupTable ? "" : ", \"enable_unique_key_merge_on_write\" = \"true\""
        sql """
            CREATE TABLE ${dbA}.${tbl} (
                `id` INT NOT NULL,
                `value` VARCHAR(32) NULL
            )
            ${keys}
            PARTITION BY RANGE(`id`)
            (
                PARTITION p1 VALUES LESS THAN ("1000"),
                PARTITION p2 VALUES LESS THAN ("2000"),
                PARTITION p3 VALUES LESS THAN ("3000")
            )
            DISTRIBUTED BY HASH(`id`) BUCKETS 2
            PROPERTIES ("replication_num" = "1"${props})
            """
        for (int b = 0; b < 3; ++b) {
            List<String> values = []
            for (int p = 0; p < numPartitions; ++p) {
                for (int i = 0; i < 20; ++i) {
                    int id = p * 1000 + b * 20 + i
                    values.add("(${id}, 'v${id}_b${b}')")
                }
            }
            sql "INSERT INTO ${dbA}.${tbl} VALUES ${values.join(",")}"
        }
    }
    sql "sync"

    def backupAndWait = { String db, String snapshot ->
        sql """
            BACKUP SNAPSHOT ${db}.${snapshot}
            TO `${repoName}`
            ON (${dupTable}, ${uniqTable})
            PROPERTIES ("logical_digest" = "true")
        """
        syncer.waitSnapshotFinish(db)
        def timestamp = syncer.getSnapshotTimestamp(repoName, snapshot)
        assertTrue(timestamp != null)
        return timestamp
    }
    def lastEstimate = null
    def restoreAndWait = { String db, String snapshot, String timestamp ->
        sql """
            RESTORE SNAPSHOT ${db}.${snapshot}
            FROM `${repoName}`
            ON (${dupTable}, ${uniqTable})
            PROPERTIES ("backup_timestamp" = "${timestamp}", "reserve_replica" = "true",
                        "reuse_check_level" = "full")
        """
        syncer.waitAllRestoreFinish(db)
        def result = sql_return_maparray """ SHOW RESTORE FROM ${db} WHERE Label = "${snapshot}" """
        def job = result.last()
        assertEquals("FINISHED", job.State)
        lastEstimate = new JsonSlurper().parseText(job.ReuseEstimate as String)
        logger.info("reuse estimate: ${lastEstimate}")
        def stats = new JsonSlurper().parseText(job.DownloadStats as String)
        logger.info("download stats: ${stats}")
        return stats
    }
    def rowsOf = { String db, String tbl ->
        def r = sql """SELECT COUNT(*), SUM(CRC32(CONCAT(CAST(id AS STRING), '|', value))) FROM ${db}.${tbl}"""
        return "${r[0][0]}:${r[0][1]}"
    }
    def versionsOf = { String db, String tbl ->
        def parts = sql_return_maparray "SHOW PARTITIONS FROM ${db}.${tbl}"
        return parts.collectEntries { [(it.PartitionName): it.VisibleVersion as String] }
    }
    def assertSame = { String left, String right ->
        for (String tbl : [dupTable, uniqTable]) {
            assertEquals(rowsOf(left, tbl), rowsOf(right, tbl))
            assertEquals(versionsOf(left, tbl), versionsOf(right, tbl))
        }
    }

    // 1. A -> B: B does not have the tables, everything is downloaded.
    String snapA = "${suiteName}_from_a"
    String tsA = backupAndWait(dbA, snapA)
    def stats = restoreAndWait(dbB, snapA, tsA)
    assertTrue((stats.downloaded_files as long) > 0)
    long baseline = stats.downloaded_files as long
    assertSame(dbA, dbB)

    // the data size of the replicas is known by the report of the backends, kept_bytes counts it
    def waitDataSize = { String db ->
        for (int i = 0; i < 30; ++i) {
            boolean ready = true
            for (String tbl : [dupTable, uniqTable]) {
                def tablets = sql_return_maparray "SHOW TABLETS FROM ${db}.${tbl}"
                if (tablets.sum { it.RowCount as long } != numPartitions * 3 * 20
                        || tablets.any { (it.LocalDataSize as long) == 0 && (it.RowCount as long) > 0 }) {
                    ready = false
                }
            }
            if (ready) {
                return
            }
            sleep(5000)
        }
    }
    waitDataSize(dbA)
    waitDataSize(dbB)

    // 2. B -> A, nothing changed: every partition of A is kept through the lineage in the backup of B (b).
    String snapB1 = "${suiteName}_from_b1"
    String tsB1 = backupAndWait(dbB, snapB1)
    stats = restoreAndWait(dbA, snapB1, tsB1)
    assertEquals(0L, stats.downloaded_files as long)
    assertEquals(0L, stats.downloaded_bytes as long)
    assertTrue((stats.kept_bytes as long) > 0)
    assertEquals(1.0, stats.reuse_ratio as double, 0.001)
    assertEquals(2 * numPartitions, lastEstimate.kept_partitions as int)
    assertEquals(2 * numPartitions, lastEstimate.kept_b as int)
    assertEquals(0, lastEstimate.kept_a as int)
    assertEquals(2 * numPartitions, lastEstimate.reusable_b as int)
    assertSame(dbA, dbB)

    // 3. B changes p1, B -> A again: p1 is downloaded, the other partitions are kept.
    sql "INSERT INTO ${dbB}.${dupTable} VALUES (5, 'changed in b')"
    sql "INSERT INTO ${dbB}.${uniqTable} VALUES (5, 'changed in b')"
    sql "sync"
    String snapB2 = "${suiteName}_from_b2"
    String tsB2 = backupAndWait(dbB, snapB2)
    stats = restoreAndWait(dbA, snapB2, tsB2)
    assertTrue((stats.downloaded_files as long) > 0)
    assertTrue((stats.downloaded_files as long) < baseline)
    assertTrue((stats.kept_bytes as long) > 0)
    assertTrue((stats.reuse_ratio as double) < 1.0)
    // the partitions of A now carry the lineage of the restore in step 2 (forward, a), p1 of A changed in B
    assertEquals(2 * (numPartitions - 1), lastEstimate.kept_partitions as int)
    assertEquals(2 * (numPartitions - 1), (lastEstimate.kept_a as int) + (lastEstimate.kept_b as int))
    assertSame(dbA, dbB)

    setConfig("restore_reuse_min_partition_bytes", "0")
    setConfig("restore_reuse_allow_hidden_state_models", "false")
    setConfig("restore_reuse_default_check_level", "full")
    for (String db : [dbA, dbB]) {
        sql "DROP DATABASE ${db} FORCE"
    }
    sql "DROP REPOSITORY `${repoName}`"
}
