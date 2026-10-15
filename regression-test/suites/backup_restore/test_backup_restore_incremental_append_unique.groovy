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

// Incremental append for unique merge-on-read tables (with a sequence column) and merge-on-write tables with DELETE
// conditions (R2 design section 15), by the non atomic and by the atomic restore. A is backed up and restored to B,
// then A writes into p1 (updates, a stale update that loses by the sequence column, a DELETE condition) and is backed
// up again. Restoring that onto B keeps p2 and p3 and only appends the rowsets after the version of p1 to the local
// tablets. For merge-on-read the backup holds whole digests at its most recent boundaries only: when B stands at an
// older version, that partition is downloaded as a whole. The data of B equals the data of A row by row. Auto
// compaction is off: a compaction of A could merge the rowsets across the version of B, and the partition would be
// downloaded whole, which is correct but not what this test is about.
suite("test_backup_restore_incremental_append_unique", "backup_restore") {
    String suiteName = "test_backup_restore_incremental_append_unique"
    String repoName = "${suiteName}_repo_" + UUID.randomUUID().toString().replace("-", "")
    String morTable = "${suiteName}_mor"
    String mowTable = "${suiteName}_mow"
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

    def runCase = { boolean atomic ->
        String tag = atomic ? "atomic" : "plain"
        String dbA = "${suiteName}_${tag}_a"
        String dbB = "${suiteName}_${tag}_b"
        setConfig("enable_restore_atomic_reuse", atomic ? "true" : "false")
        for (String db : [dbA, dbB]) {
            sql "DROP DATABASE IF EXISTS ${db} FORCE"
            sql "CREATE DATABASE ${db}"
        }
        for (String tbl : [morTable, mowTable]) {
            String props = tbl == morTable
                    ? ", \"enable_unique_key_merge_on_write\" = \"false\", \"function_column.sequence_col\" = \"seq\""
                    : ", \"enable_unique_key_merge_on_write\" = \"true\", \"enable_mow_light_delete\" = \"true\", \"function_column.sequence_col\" = \"seq\""
            sql """
                CREATE TABLE ${dbA}.${tbl} (
                    `id` INT NOT NULL,
                    `value` VARCHAR(64) NULL,
                    `seq` INT NOT NULL
                )
                UNIQUE KEY(`id`)
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
                        values.add("(${id}, 'value of ${id} in batch ${b} with some padding to make it larger', 1)")
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
                ON (${morTable}, ${mowTable})
                PROPERTIES ("logical_digest" = "true")
            """
            syncer.waitSnapshotFinish(db)
            def timestamp = syncer.getSnapshotTimestamp(repoName, snapshot)
            assertTrue(timestamp != null)
            return timestamp
        }
        def lastEstimate = null
        def restoreAndWait = { String db, String snapshot, String timestamp ->
            String atomicProp = atomic ? ", \"atomic_restore\" = \"true\"" : ""
            sql """
                RESTORE SNAPSHOT ${db}.${snapshot}
                FROM `${repoName}`
                ON (${morTable}, ${mowTable})
                PROPERTIES ("backup_timestamp" = "${timestamp}", "reserve_replica" = "true",
                            "reuse_check_level" = "full"${atomicProp})
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
            def r = sql """SELECT COUNT(*), SUM(CRC32(CONCAT(CAST(id AS STRING), '|', value, '|', CAST(seq AS STRING)))) FROM ${db}.${tbl}"""
            return "${r[0][0]}:${r[0][1]}"
        }
        def versionsOf = { String db, String tbl ->
            def parts = sql_return_maparray "SHOW PARTITIONS FROM ${db}.${tbl}"
            return parts.collectEntries { [(it.PartitionName): it.VisibleVersion as String] }
        }
        def assertSame = { String left, String right ->
            for (String tbl : [morTable, mowTable]) {
                assertEquals(rowsOf(left, tbl), rowsOf(right, tbl))
                assertEquals(versionsOf(left, tbl), versionsOf(right, tbl))
            }
        }
        // the local data sizes are known by the report of the backends
        def waitDataSize = { String db ->
            for (int i = 0; i < 30; ++i) {
                boolean ready = true
                for (String tbl : [morTable, mowTable]) {
                    def tablets = sql_return_maparray "SHOW TABLETS FROM ${db}.${tbl}"
                    // every tablet has rows, and its size and row count are reported by the backend
                    if (tablets.any { (it.LocalDataSize as long) == 0 || (it.RowCount as long) == 0 }) {
                        ready = false
                    }
                }
                if (ready) {
                    return
                }
                sleep(5000)
            }
        }
        // the writes of A into p1: three versions in each table, one of them a DELETE condition
        def writeP1 = { int round ->
            for (String tbl : [morTable, mowTable]) {
                sql "INSERT INTO ${dbA}.${tbl} VALUES (5, 'updated in p1 round ${round}', ${10 + round}), (${900 + round}, 'new row of p1 round ${round}', 1)"
                // a stale update: the sequence column of both tables rejects it
                sql "INSERT INTO ${dbA}.${tbl} VALUES (6, 'stale update round ${round}', 0)"
                sql "DELETE FROM ${dbA}.${tbl} WHERE id = ${7 + round}"
            }
            sql "sync"
        }

        // 1. A -> B: B does not have the tables, everything is downloaded.
        String tsA1 = backupAndWait(dbA, "${suiteName}_${tag}_1")
        def stats = restoreAndWait(dbB, "${suiteName}_${tag}_1", tsA1)
        long baselineBytes = stats.downloaded_bytes as long
        assertTrue(baselineBytes > 0)
        assertSame(dbA, dbB)
        waitDataSize(dbB)

        // 2. A writes into p1 (3 versions, the version of B is a boundary of the backup): p2 and p3 are kept,
        // p1 of both tables downloads the rowsets after its version
        writeP1(1)
        assertTrue((sql_return_maparray("SHOW DELETE FROM ${dbA}")).size() >= 2)
        String tsA2 = backupAndWait(dbA, "${suiteName}_${tag}_2")
        stats = restoreAndWait(dbB, "${suiteName}_${tag}_2", tsA2)
        assertEquals(2 * (numPartitions - 1), lastEstimate.kept_partitions as int)
        assertEquals(2, lastEstimate.incremental_partitions as int)
        assertEquals(0, lastEstimate.download_not_boundary as int)
        if (atomic) {
            assertEquals(2, lastEstimate.incremental_atomic as int)
        }
        assertTrue((stats.incremental_tablets as long) > 0)
        assertTrue((stats.incremental_bytes as long) > 0)
        assertEquals(stats.incremental_bytes as long, stats.downloaded_bytes as long)
        assertTrue((stats.downloaded_bytes as long) * 5 < baselineBytes)
        assertTrue((stats.incremental_local_bytes as long) > 0)
        assertSame(dbA, dbB)
        for (String tbl : [morTable, mowTable]) {
            assertEquals("updated in p1 round 1", (sql "SELECT value FROM ${dbB}.${tbl} WHERE id = 5")[0][0])
            // the stale update lost by the sequence column
            assertTrue(((sql "SELECT value FROM ${dbB}.${tbl} WHERE id = 6")[0][0] as String).startsWith("value of 6 "))
            // the DELETE condition
            assertEquals(0, (sql "SELECT COUNT(*) FROM ${dbB}.${tbl} WHERE id = 8")[0][0])
            assertEquals(1, (sql "SELECT COUNT(*) FROM ${dbB}.${tbl} WHERE id = 901")[0][0])
        }
        waitDataSize(dbB)

        // 3. six more versions in p1: the version of B is no longer among the 3 recent boundaries below the
        // version of the backup. Merge-on-read downloads p1 whole, merge-on-write still appends the increment.
        for (int r = 2; r <= 3; ++r) {
            writeP1(r)
        }
        String tsA3 = backupAndWait(dbA, "${suiteName}_${tag}_3")
        stats = restoreAndWait(dbB, "${suiteName}_${tag}_3", tsA3)
        assertEquals(1, lastEstimate.incremental_partitions as int)
        assertEquals(1, lastEstimate.download_not_boundary as int)
        assertEquals(2 * (numPartitions - 1), lastEstimate.kept_partitions as int)
        assertTrue((stats.incremental_bytes as long) > 0)
        assertTrue((stats.downloaded_bytes as long) > (stats.incremental_bytes as long))
        assertSame(dbA, dbB)
        waitDataSize(dbB)

        // 4. one more version: B stands at the version of the previous backup, which is one of the 3 boundaries below
        // the version of the next one, for both tables
        sql "INSERT INTO ${dbA}.${morTable} VALUES (11, 'one more', 5)"
        sql "INSERT INTO ${dbA}.${mowTable} VALUES (11, 'one more', 5)"
        sql "sync"
        String tsA4 = backupAndWait(dbA, "${suiteName}_${tag}_4")
        stats = restoreAndWait(dbB, "${suiteName}_${tag}_4", tsA4)
        assertEquals(2, lastEstimate.incremental_partitions as int)
        assertEquals(0, lastEstimate.download_not_boundary as int)
        assertTrue((stats.downloaded_bytes as long) * 5 < baselineBytes)
        assertSame(dbA, dbB)

        // 5. the switch off: the partition behind the backup is downloaded as a whole
        setConfig("enable_restore_incremental_append", "false")
        sql "INSERT INTO ${dbA}.${morTable} VALUES (12, 'written with the switch off', 5)"
        sql "INSERT INTO ${dbA}.${mowTable} VALUES (12, 'written with the switch off', 5)"
        sql "sync"
        waitDataSize(dbB)
        String tsA5 = backupAndWait(dbA, "${suiteName}_${tag}_5")
        stats = restoreAndWait(dbB, "${suiteName}_${tag}_5", tsA5)
        assertEquals(0L, (stats.incremental_tablets ?: 0) as long)
        assertTrue(!lastEstimate.containsKey("incremental_partitions"))
        assertTrue((stats.downloaded_bytes as long) > 0)
        assertSame(dbA, dbB)
        setConfig("enable_restore_incremental_append", "true")

        for (String db : [dbA, dbB]) {
            sql "DROP DATABASE ${db} FORCE"
        }
    }

    runCase(false)
    runCase(true)

    setConfig("restore_reuse_min_partition_bytes", "0")
    setConfig("restore_reuse_allow_hidden_state_models", "false")
    setConfig("restore_reuse_default_check_level", "full")
    setConfig("enable_restore_atomic_reuse", "false")
    sql "DROP REPOSITORY `${repoName}`"
}
