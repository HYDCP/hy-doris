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

// Partition level reuse and the incremental append of an atomic restore (R2 phase 2): the staging table is made from
// the local tablets of the table being replaced (hard links), the partitions that are not changed are not downloaded,
// the partition that is behind the backup downloads only the increment. The table is replaced in the end: the data,
// the versions equal the source, and the tablets are the new ones. With the switch off the atomic restore downloads
// everything as before.
// Auto compaction is off: a compaction of A could merge the rowsets across the version of B, and the partition would
// be downloaded whole, which is correct but not what this test is about.
suite("test_backup_restore_atomic_reuse", "backup_restore") {
    String suiteName = "test_backup_restore_atomic_reuse"
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
    setConfig("enable_restore_atomic_reuse", "true")
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
                `value` VARCHAR(64) NULL
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
                        "atomic_restore" = "true", "reuse_check_level" = "full")
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
    def tabletIdsOf = { String db, String tbl ->
        def tablets = sql_return_maparray "SHOW TABLETS FROM ${db}.${tbl}"
        return tablets.collect { it.TabletId as long }.toSet()
    }
    def waitDataSize = { String db ->
        for (int i = 0; i < 30; ++i) {
            boolean ready = true
            for (String tbl : [dupTable, uniqTable]) {
                def tablets = sql_return_maparray "SHOW TABLETS FROM ${db}.${tbl}"
                // every tablet of the tables has rows, and its size is reported by the backend
                if (tablets.any { (it.LocalDataSize as long) == 0 }) {
                    ready = false
                }
            }
            if (ready) {
                return
            }
            sleep(5000)
        }
    }

    // 1. A -> B: B does not have the tables, an atomic restore downloads everything.
    String snap1 = "${suiteName}_1"
    String tsA1 = backupAndWait(dbA, snap1)
    def stats = restoreAndWait(dbB, snap1, tsA1)
    long baselineBytes = stats.downloaded_bytes as long
    assertTrue(baselineBytes > 0)
    assertSame(dbA, dbB)
    waitDataSize(dbB)

    // 2. the same backup again: every partition keeps the local data, nothing is downloaded, the tables are replaced
    def tabletsBefore = [dupTable, uniqTable].collectEntries { [(it): tabletIdsOf(dbB, it)] }
    stats = restoreAndWait(dbB, snap1, tsA1)
    assertEquals(0L, stats.downloaded_bytes as long)
    assertEquals(0L, stats.downloaded_files as long)
    assertEquals(2 * numPartitions, lastEstimate.kept_partitions as int)
    assertEquals(2 * numPartitions, lastEstimate.kept_atomic as int)
    assertEquals(0, lastEstimate.incremental_atomic as int)
    assertTrue((stats.kept_atomic_bytes as long) > 0)
    assertEquals(stats.kept_bytes as long, stats.kept_atomic_bytes as long)
    assertEquals(1.0, stats.reuse_ratio as double, 0.001)
    // the local snapshots: hard links, nothing copied (one disk per tablet)
    assertEquals(2 * numPartitions * 2, stats.atomic_local.local_tablets as int)
    assertTrue((stats.atomic_local.linked_bytes as long) > 0)
    assertEquals(0L, stats.atomic_local.copied_bytes as long)
    assertSame(dbA, dbB)
    for (String tbl : [dupTable, uniqTable]) {
        def tabletsAfter = tabletIdsOf(dbB, tbl)
        assertEquals(tabletsBefore[tbl].size(), tabletsAfter.size())
        // replaced by the staging table: the tablets are all new ones
        assertTrue(tabletsBefore[tbl].intersect(tabletsAfter).isEmpty())
    }
    // and again: the lineage was written to the new table
    waitDataSize(dbB)
    stats = restoreAndWait(dbB, snap1, tsA1)
    assertEquals(0L, stats.downloaded_bytes as long)
    assertEquals(2 * numPartitions, lastEstimate.kept_atomic as int)
    assertSame(dbA, dbB)

    // 3. A writes into p1, the others are not changed: p2 and p3 are kept, p1 downloads the increment only
    sql "INSERT INTO ${dbA}.${dupTable} VALUES (5, 'new row of p1'), (5, 'another row of p1')"
    sql "INSERT INTO ${dbA}.${uniqTable} VALUES (5, 'updated in p1'), (6, 'new row of p1')"
    sql "DELETE FROM ${dbA}.${uniqTable} WHERE id = 7"
    sql "sync"
    String snap2 = "${suiteName}_2"
    String tsA2 = backupAndWait(dbA, snap2)
    waitDataSize(dbB)
    stats = restoreAndWait(dbB, snap2, tsA2)
    assertEquals(2 * (numPartitions - 1), lastEstimate.kept_atomic as int)
    assertEquals(2, lastEstimate.incremental_atomic as int)
    assertEquals(2, lastEstimate.incremental_partitions as int)
    assertTrue((stats.incremental_tablets as long) > 0)
    assertTrue((stats.incremental_bytes as long) > 0)
    assertEquals(stats.incremental_bytes as long, stats.downloaded_bytes as long)
    assertTrue((stats.downloaded_bytes as long) * 5 < baselineBytes)
    // the local data of the incremental partitions counts in the reuse ratio
    assertTrue((stats.incremental_local_bytes as long) > 0)
    assertTrue((stats.reuse_ratio as double) > 0)
    // the local base of the incremental partitions is a local snapshot too
    assertEquals(2 * numPartitions * 2, stats.atomic_local.local_tablets as int)
    assertSame(dbA, dbB)
    assertEquals(0, (sql "SELECT COUNT(*) FROM ${dbB}.${uniqTable} WHERE id = 7")[0][0])
    assertEquals("updated in p1", (sql "SELECT value FROM ${dbB}.${uniqTable} WHERE id = 5")[0][0])
    assertEquals(2, (sql "SELECT COUNT(*) FROM ${dbB}.${dupTable} WHERE id = 5 AND value LIKE '%row of p1'")[0][0])

    // 4. the switch off: the atomic restore downloads everything, as before
    setConfig("enable_restore_atomic_reuse", "false")
    waitDataSize(dbB)
    stats = restoreAndWait(dbB, snap2, tsA2)
    assertTrue((stats.downloaded_bytes as long) > 0)
    assertTrue(!lastEstimate.containsKey("kept_atomic"))
    assertTrue(!stats.containsKey("atomic_local"))
    assertTrue(!stats.containsKey("kept_atomic_bytes"))
    assertSame(dbA, dbB)

    // 5. the switch on again, the next restore of the same backup keeps everything
    setConfig("enable_restore_atomic_reuse", "true")
    waitDataSize(dbB)
    stats = restoreAndWait(dbB, snap2, tsA2)
    assertEquals(0L, stats.downloaded_bytes as long)
    assertEquals(2 * numPartitions, lastEstimate.kept_atomic as int)
    assertSame(dbA, dbB)

    setConfig("restore_reuse_min_partition_bytes", "0")
    setConfig("restore_reuse_allow_hidden_state_models", "false")
    setConfig("restore_reuse_default_check_level", "full")
    setConfig("enable_restore_atomic_reuse", "false")
    for (String db : [dbA, dbB]) {
        sql "DROP DATABASE ${db} FORCE"
    }
    sql "DROP REPOSITORY `${repoName}`"
}
