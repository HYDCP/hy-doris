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

// Incremental append of restore (R2 phase 2): A is backed up and restored to B, then A writes into one partition and
// is backed up again. Restoring that onto B keeps the partitions that did not change, and for the partition that is
// behind only the rowsets after its version are downloaded and appended to the local tablets. The data of B equals
// the data of A row by row, the downloaded bytes are far less than a whole download, and with the switch off the
// partition is downloaded as a whole.
// Auto compaction is off: a compaction of A could merge the rowsets across the version of B, and the partition would
// be downloaded whole, which is correct but not what this test is about.
suite("test_backup_restore_incremental_append", "backup_restore") {
    String suiteName = "test_backup_restore_incremental_append"
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
    setConfig("enable_restore_incremental_append", "true")
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
            PROPERTIES ("replication_num" = "1", "disable_auto_compaction" = "true"${props})
            """
        for (int b = 0; b < 3; ++b) {
            List<String> values = []
            for (int p = 0; p < numPartitions; ++p) {
                for (int i = 0; i < 200; ++i) {
                    int id = p * 1000 + b * 200 + i
                    values.add("(${id}, 'value of ${id} in batch ${b} with some padding to make it larger')")
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

    // the data size of the replicas is known by the report of the backends, incremental_local_bytes and kept_bytes
    // count it
    def waitDataSize = { String db ->
        for (int i = 0; i < 30; ++i) {
            boolean ready = true
            for (String tbl : [dupTable, uniqTable]) {
                def tablets = sql_return_maparray "SHOW TABLETS FROM ${db}.${tbl}"
                if (tablets.sum { it.RowCount as long } != numPartitions * 3 * 200
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

    // 1. A -> B: B does not have the tables, everything is downloaded.
    String tsA1 = backupAndWait(dbA, "${suiteName}_1")
    def stats = restoreAndWait(dbB, "${suiteName}_1", tsA1)
    long baselineBytes = stats.downloaded_bytes as long
    assertTrue(baselineBytes > 0)
    assertSame(dbA, dbB)

    // 2. A writes into p1: new rows, an update and a delete in the unique table
    sql "INSERT INTO ${dbA}.${dupTable} VALUES (5, 'new row of p1'), (5, 'another row of p1')"
    sql "INSERT INTO ${dbA}.${uniqTable} VALUES (5, 'updated in p1'), (6, 'new row of p1')"
    sql "DELETE FROM ${dbA}.${uniqTable} WHERE id = 7"
    sql "sync"
    String tsA2 = backupAndWait(dbA, "${suiteName}_2")
    waitDataSize(dbB)
    stats = restoreAndWait(dbB, "${suiteName}_2", tsA2)
    // p2 and p3 are kept, p1 downloads the increment only
    assertEquals(2 * (numPartitions - 1), lastEstimate.kept_partitions as int)
    assertEquals(2, lastEstimate.incremental_partitions as int)
    assertEquals(0, lastEstimate.download_not_boundary as int)
    assertTrue((stats.incremental_tablets as long) > 0)
    assertTrue((stats.incremental_bytes as long) > 0)
    assertEquals(stats.incremental_bytes as long, stats.downloaded_bytes as long)
    assertTrue((stats.downloaded_bytes as long) * 5 < baselineBytes)
    assertSame(dbA, dbB)
    // the local data of p1 (0, V_l] that is kept and only appended to counts in the reuse ratio, which is about
    // (kept + local) / (kept + local + increment) now
    long incLocal = stats.incremental_local_bytes as long
    assertTrue(incLocal > 0)
    assertEquals(lastEstimate.incremental_local_bytes_all_replicas as long, incLocal)
    long reused = (stats.linked_bytes as long) + (stats.skipped_bytes as long) + (stats.kept_bytes as long) + incLocal
    double expectRatio = (double) reused / (reused + (stats.downloaded_bytes as long))
    assertTrue((stats.reuse_ratio as double) > 0)
    assertEquals(expectRatio, stats.reuse_ratio as double, 0.002)
    // and the partition itself: the local data against the local data and the increment
    double incRatio = (double) incLocal / (incLocal + (stats.incremental_bytes as long))
    assertTrue(incRatio > 0.5)
    assertTrue((stats.reuse_ratio as double) >= incRatio - 0.001)
    // the rows of the increment are there
    assertEquals(0, (sql "SELECT COUNT(*) FROM ${dbB}.${uniqTable} WHERE id = 7")[0][0])
    assertEquals("updated in p1", (sql "SELECT value FROM ${dbB}.${uniqTable} WHERE id = 5")[0][0])
    assertEquals(2, (sql "SELECT COUNT(*) FROM ${dbB}.${dupTable} WHERE id = 5 AND value LIKE '%row of p1'")[0][0])

    // 3. the switch off: the partition behind the backup is downloaded as a whole
    setConfig("enable_restore_incremental_append", "false")
    sql "INSERT INTO ${dbA}.${dupTable} VALUES (8, 'written again in p1')"
    sql "sync"
    String tsA3 = backupAndWait(dbA, "${suiteName}_3")
    stats = restoreAndWait(dbB, "${suiteName}_3", tsA3)
    assertEquals(0L, (stats.incremental_tablets ?: 0) as long)
    assertTrue(!lastEstimate.containsKey("incremental_partitions"))
    assertTrue((stats.downloaded_bytes as long) > 0)
    assertSame(dbA, dbB)

    setConfig("restore_reuse_min_partition_bytes", "0")
    setConfig("restore_reuse_allow_hidden_state_models", "false")
    setConfig("restore_reuse_default_check_level", "full")
    for (String db : [dbA, dbB]) {
        sql "DROP DATABASE ${db} FORCE"
    }
    sql "DROP REPOSITORY `${repoName}`"
}
