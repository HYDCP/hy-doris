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

// Partition level reuse of restore (R2): a backup taken with logical_digest=true restored onto the table that was
// restored from it keeps the local partitions that are logically equal to the backup, and does not download them.
// What is downloaded is read from the DownloadStats column of SHOW RESTORE; the result is compared with a full
// download of the same backup (rows, partition versions).
suite("test_backup_restore_partition_reuse", "backup_restore") {
    String suiteName = "test_backup_restore_partition_reuse"
    String repoName = "${suiteName}_repo_" + UUID.randomUUID().toString().replace("-", "")
    String srcDbName = "${suiteName}_src_db"
    String dbName = "${suiteName}_db"
    String fullDbName = "${suiteName}_full_db"
    String dupTable = "${suiteName}_dup"
    String uniqTable = "${suiteName}_uniq"
    int numPartitions = 3
    int numBatches = 4

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

    for (String db : [srcDbName, dbName, fullDbName]) {
        sql "DROP DATABASE IF EXISTS ${db} FORCE"
        sql "CREATE DATABASE ${db}"
    }
    sql """
        CREATE TABLE ${srcDbName}.${dupTable} (
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
    sql """
        CREATE TABLE ${srcDbName}.${uniqTable} (
            `id` INT NOT NULL,
            `value` VARCHAR(32) NULL
        )
        UNIQUE KEY(`id`)
        PARTITION BY RANGE(`id`)
        (
            PARTITION p1 VALUES LESS THAN ("1000"),
            PARTITION p2 VALUES LESS THAN ("2000"),
            PARTITION p3 VALUES LESS THAN ("3000")
        )
        DISTRIBUTED BY HASH(`id`) BUCKETS 2
        PROPERTIES ("replication_num" = "1", "enable_unique_key_merge_on_write" = "true")
        """
    // several batches per partition, so a partition has several rowsets for the compaction to merge
    for (int b = 0; b < numBatches; ++b) {
        for (String tbl : [dupTable, uniqTable]) {
            List<String> values = []
            for (int p = 0; p < numPartitions; ++p) {
                for (int i = 0; i < 20; ++i) {
                    int id = p * 1000 + b * 20 + i
                    values.add("(${id}, 'v${id}_b${b}')")
                }
            }
            sql "INSERT INTO ${srcDbName}.${tbl} VALUES ${values.join(",")}"
        }
    }
    sql "sync"

    def backupAndWait = { String snapshot ->
        sql """
            BACKUP SNAPSHOT ${srcDbName}.${snapshot}
            TO `${repoName}`
            ON (${dupTable}, ${uniqTable})
            PROPERTIES ("logical_digest" = "true")
        """
        syncer.waitSnapshotFinish(srcDbName)
        def timestamp = syncer.getSnapshotTimestamp(repoName, snapshot)
        assertTrue(timestamp != null)
        return timestamp
    }

    // the ReuseEstimate of the last restore job
    def lastEstimate = null

    // returns the DownloadStats of the restore job
    def restoreAndWait = { String db, String snapshot, String timestamp, String level ->
        String levelProp = level == null ? "" : ", \"reuse_check_level\" = \"${level}\""
        sql """
            RESTORE SNAPSHOT ${db}.${snapshot}
            FROM `${repoName}`
            ON (${dupTable}, ${uniqTable})
            PROPERTIES ("backup_timestamp" = "${timestamp}", "reserve_replica" = "true"${levelProp})
        """
        syncer.waitAllRestoreFinish(db)
        def result = sql_return_maparray """ SHOW RESTORE FROM ${db} WHERE Label = "${snapshot}" """
        def job = result.last()
        logger.info("restore ${snapshot} into ${db}, level ${level}: ${job}")
        assertEquals("FINISHED", job.State)
        def stats = new JsonSlurper().parseText(job.DownloadStats as String)
        logger.info("download stats: ${stats}")
        lastEstimate = job.ReuseEstimate == null ? null : new JsonSlurper().parseText(job.ReuseEstimate as String)
        logger.info("reuse estimate: ${lastEstimate}")
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
    def assertSameAsSource = { String db ->
        for (String tbl : [dupTable, uniqTable]) {
            assertEquals(rowsOf(srcDbName, tbl), rowsOf(db, tbl))
            assertEquals(versionsOf(srcDbName, tbl), versionsOf(db, tbl))
        }
    }
    def downloaded = { stats -> (stats.downloaded_files as long) + (stats.skipped_files as long) + (stats.linked_files as long) }

    String snap1 = "${suiteName}_snap1"
    String ts1 = backupAndWait(snap1)

    // 1. the tables do not exist: restored as new tables, everything is downloaded.
    def stats = restoreAndWait(dbName, snap1, ts1, null)
    long baseline = stats.downloaded_files as long
    assertTrue(baseline > 0)
    assertSameAsSource(dbName)

    // the data size of the replicas is known by the report of the backends, kept_bytes counts it
    def waitDataSize = { String db ->
        for (int i = 0; i < 30; ++i) {
            boolean ready = true
            for (String tbl : [dupTable, uniqTable]) {
                def tablets = sql_return_maparray "SHOW TABLETS FROM ${db}.${tbl}"
                // the rows of the table are all reported, and every tablet with rows has a size
                if (tablets.sum { it.RowCount as long } != numPartitions * numBatches * 20
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
    waitDataSize(dbName)

    // 2. the same backup again: every partition is kept, nothing is downloaded, and the data is the same as the
    // source and as a full download.
    stats = restoreAndWait(dbName, snap1, ts1, "full")
    assertEquals(0L, downloaded(stats))
    assertEquals(0L, stats.downloaded_bytes as long)
    assertSameAsSource(dbName)
    // the kept data counts in kept_bytes and the reuse ratio, and the estimate shows the actual result
    assertTrue((stats.kept_bytes as long) > 0)
    assertEquals(1.0, stats.reuse_ratio as double, 0.001)
    assertEquals(2 * numPartitions, lastEstimate.kept_partitions as int)
    assertEquals(2 * numPartitions, lastEstimate.kept_a as int)
    assertEquals(0, lastEstimate.kept_b as int)
    assertEquals(0, lastEstimate.kept_c as int)
    assertTrue((lastEstimate.kept_bytes_single_replica as long) > 0)
    assertEquals(0, lastEstimate.download_digest_mismatch as int)
    stats = restoreAndWait(dbName, snap1, ts1, null)
    assertEquals(0L, downloaded(stats))
    assertSameAsSource(dbName)
    // a full download of the same backup into another db gives the same rows and versions
    restoreAndWait(fullDbName, snap1, ts1, "disable")
    for (String tbl : [dupTable, uniqTable]) {
        assertEquals(rowsOf(fullDbName, tbl), rowsOf(dbName, tbl))
        assertEquals(versionsOf(fullDbName, tbl), versionsOf(dbName, tbl))
    }

    // 3. reuse_check_level = disable downloads everything, also when the switch is on.
    stats = restoreAndWait(dbName, snap1, ts1, "disable")
    assertEquals(baseline, stats.downloaded_files as long)
    assertSameAsSource(dbName)
    // and the lineage was written again, the next restore keeps all partitions
    stats = restoreAndWait(dbName, snap1, ts1, "full")
    assertEquals(0L, downloaded(stats))

    // 4. the switch off: the behavior of before, everything is downloaded.
    setConfig("enable_restore_partition_reuse", "false")
    stats = restoreAndWait(dbName, snap1, ts1, "full")
    assertEquals(baseline, stats.downloaded_files as long)
    assertSameAsSource(dbName)
    setConfig("enable_restore_partition_reuse", "true")

    // 5. compaction on the target does not change the logical data: the partitions are still kept.
    def versionCount = { String db, String tbl ->
        def tablets = sql_return_maparray "SHOW TABLETS FROM ${db}.${tbl}"
        return tablets.collect { it.VersionCount as int }.sum()
    }
    stats = restoreAndWait(dbName, snap1, ts1, "full")
    assertEquals(0L, downloaded(stats))
    int before = versionCount(dbName, dupTable)
    for (String tbl : [dupTable, uniqTable]) {
        def tablets = sql_return_maparray "SHOW TABLETS FROM ${dbName}.${tbl}"
        for (def tablet : tablets) {
            String url = (tablet.CompactionStatus as String).replace("compaction/show", "compaction/run") +
                    "&compact_type=full"
            def (code, out, err) = curl("POST", url)
            logger.info("full compaction of tablet ${tablet.TabletId}: ${code} ${out}")
        }
    }
    // the version count is refreshed by the tablet report of the backends
    int after = before
    for (int i = 0; i < 20 && after >= before; ++i) {
        sleep(10000)
        after = versionCount(dbName, dupTable)
    }
    logger.info("version count of ${dupTable}: ${before} -> ${after}")
    assertTrue(after < before)
    stats = restoreAndWait(dbName, snap1, ts1, "full")
    assertEquals(0L, downloaded(stats))
    assertSameAsSource(dbName)

    // 6. a write to p1 of the source, then back up again: p1 is downloaded, the others are kept.
    sql "INSERT INTO ${srcDbName}.${dupTable} VALUES (5, 'new')"
    sql "INSERT INTO ${srcDbName}.${uniqTable} VALUES (5, 'new')"
    sql "sync"
    String snap2 = "${suiteName}_snap2"
    String ts2 = backupAndWait(snap2)
    stats = restoreAndWait(dbName, snap2, ts2, "full")
    assertTrue((stats.downloaded_files as long) > 0)
    assertTrue((stats.downloaded_files as long) < baseline)
    assertSameAsSource(dbName)
    assertTrue((stats.kept_bytes as long) > 0)
    assertTrue((stats.reuse_ratio as double) < 1.0)
    // the default level (full) gives the same result
    sql "INSERT INTO ${srcDbName}.${dupTable} VALUES (1005, 'new2')"
    sql "sync"
    String snap3 = "${suiteName}_snap3"
    String ts3 = backupAndWait(snap3)
    stats = restoreAndWait(dbName, snap3, ts3, null)
    assertTrue((stats.downloaded_files as long) > 0)
    assertTrue((stats.downloaded_files as long) < baseline)
    assertSameAsSource(dbName)
    stats = restoreAndWait(dbName, snap3, ts3, null)
    assertEquals(0L, downloaded(stats))

    // 7. an invalid level is rejected
    test {
        sql """
            RESTORE SNAPSHOT ${dbName}.${snap3}
            FROM `${repoName}`
            ON (${dupTable})
            PROPERTIES ("backup_timestamp" = "${ts3}", "reuse_check_level" = "bad")
        """
        exception "reuse_check_level"
    }

    setConfig("restore_reuse_min_partition_bytes", "0")
    setConfig("restore_reuse_allow_hidden_state_models", "false")
    setConfig("restore_reuse_default_check_level", "full")
    for (String db : [srcDbName, dbName, fullDbName]) {
        sql "DROP DATABASE ${db} FORCE"
    }
    sql "DROP REPOSITORY `${repoName}`"
}
