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

// The logical digest cache of the backend must not return the digest of data that was replaced (R2).
// The cache of the digests the backend computes for a backup is keyed by the tablet id, the version and the schema.
// The content of a tablet can be replaced under that key: here a NON-ATOMIC restore overwrites the existing
// partitions of the table T with another snapshot, which keeps the tablets (same tablet id, same replica, same
// schema) and gives the partition the version of the snapshot, equal to the version T had. The digest of T which
// was cached for the data X must not be used for the data Y which is there now, otherwise a backup of T records the
// digest of X for the files of Y, and a restore of that backup with reuse_check_level = full would keep the old
// partitions with X on a table which was restored from T before, and not download Y.
//
// Steps:
//   1. T (dbT) holds X, U (dbY) holds Y: the same schema, partitions and number of loads, so the same versions, and
//      the content is different (same ids and row counts, other values; only the digest and the row hash tell).
//   2. back up T with the logical digest (S1): the backend of T computes the digest of X and caches it.
//   3. restore S1 as B (dbB, new tables), and once more with reuse_check_level = full: all partitions are kept (the
//      control: the candidates are proved by the digest, and the digest of the unchanged data matches).
//   4. non-atomic restore of the snapshot of U onto T, without reuse: the tablet ids, the versions and the schema of
//      T are the same as before, and the data of T is Y now.
//   5. back up T with the logical digest (S2): the digest of the files of S2 must be the one of Y.
//   6. restore S2 onto B with reuse_check_level = full: no partition is kept, they fail the digest check
//      (download_digest_mismatch), and B holds Y, row by row. Restoring S2 once more keeps everything.
// Auto compaction is off, so the rowsets of T are what the loads made.
suite("test_backup_restore_digest_cache_replace", "backup_restore") {
    String suiteName = "test_backup_restore_digest_cache_replace"
    String repoName = "${suiteName}_repo_" + UUID.randomUUID().toString().replace("-", "")
    String dbT = "${suiteName}_t"
    String dbY = "${suiteName}_y"
    String dbB = "${suiteName}_b"
    String dupTable = "${suiteName}_dup"
    String uniqTable = "${suiteName}_uniq"
    int numPartitions = 2
    int numBatches = 3
    int rowsPerBatch = 50

    def syncer = getSyncer()
    syncer.createS3Repository(repoName)

    def setConfig = { String name, String value ->
        sql """ADMIN SET FRONTEND CONFIG ("${name}" = "${value}")"""
    }
    setConfig("enable_restore_partition_reuse", "true")
    setConfig("restore_reuse_min_partition_bytes", "0")
    setConfig("restore_reuse_default_check_level", "sample")
    // The unique table of this case has hidden state (delete sign rows), which partition level reuse only reuses
    // with the experiment switch below (default false: only duplicate tables are reused). The case checks the BE
    // digest cache of both models, so turn it on, and restore it at the end.
    setConfig("restore_reuse_allow_hidden_state_models", "true")

    for (String db : [dbT, dbY, dbB]) {
        sql "DROP DATABASE IF EXISTS ${db} FORCE"
        sql "CREATE DATABASE ${db}"
    }
    // The same DDL and the same loads in dbT and dbY: the same versions. Only the values differ: x_ or y_.
    for (def entry : [[dbT, "x"], [dbY, "y"]]) {
        String db = entry[0]
        String tag = entry[1]
        for (String tbl : [dupTable, uniqTable]) {
            String keys = tbl == dupTable ? "DUPLICATE KEY(`id`)" : "UNIQUE KEY(`id`)"
            String props = tbl == dupTable ? "" : ", \"enable_unique_key_merge_on_write\" = \"true\""
            sql """
                CREATE TABLE ${db}.${tbl} (
                    `id` INT NOT NULL,
                    `value` VARCHAR(64) NULL
                )
                ${keys}
                PARTITION BY RANGE(`id`)
                (
                    PARTITION p1 VALUES LESS THAN ("1000"),
                    PARTITION p2 VALUES LESS THAN ("2000")
                )
                DISTRIBUTED BY HASH(`id`) BUCKETS 2
                PROPERTIES ("replication_num" = "1", "disable_auto_compaction" = "true"${props})
                """
            for (int b = 0; b < numBatches; ++b) {
                List<String> values = []
                for (int p = 0; p < numPartitions; ++p) {
                    for (int i = 0; i < rowsPerBatch; ++i) {
                        int id = p * 1000 + b * rowsPerBatch + i
                        values.add("(${id}, '${tag}_value of ${id} in batch ${b} with some padding')")
                    }
                }
                sql "INSERT INTO ${db}.${tbl} VALUES ${values.join(",")}"
            }
        }
    }
    sql "sync"

    def backupAndWait = { String db, String snapshot, boolean digest ->
        sql """
            BACKUP SNAPSHOT ${db}.${snapshot}
            TO `${repoName}`
            ON (${dupTable}, ${uniqTable})
            PROPERTIES ("logical_digest" = "${digest}")
        """
        syncer.waitSnapshotFinish(db)
        def timestamp = syncer.getSnapshotTimestamp(repoName, snapshot)
        assertTrue(timestamp != null)
        return timestamp
    }
    def lastEstimate = null
    def restoreAndWait = { String db, String snapshot, String timestamp, String level, boolean atomic ->
        sql """
            RESTORE SNAPSHOT ${db}.${snapshot}
            FROM `${repoName}`
            ON (${dupTable}, ${uniqTable})
            PROPERTIES ("backup_timestamp" = "${timestamp}", "reserve_replica" = "true",
                        "atomic_restore" = "${atomic}", "reuse_check_level" = "${level}")
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
    def tabletIdsOf = { String db, String tbl ->
        def tablets = sql_return_maparray "SHOW TABLETS FROM ${db}.${tbl}"
        return tablets.collect { it.TabletId as String }.sort()
    }
    // the replicas have reported the data they hold: the versions and the row counts
    def waitReported = { String db ->
        for (int i = 0; i < 30; ++i) {
            boolean ready = true
            for (String tbl : [dupTable, uniqTable]) {
                def tablets = sql_return_maparray "SHOW TABLETS FROM ${db}.${tbl}"
                if (tablets.sum { it.RowCount as long } != numPartitions * numBatches * rowsPerBatch) {
                    ready = false
                }
            }
            if (ready) {
                return
            }
            sleep(5000)
        }
    }
    def downloaded = { stats -> (stats.downloaded_files as long) + (stats.skipped_files as long) + (stats.linked_files as long) }
    int allPartitions = 2 * numPartitions

    // 1. dbT and dbY: the same versions and row counts, different data
    Map<String, String> dataX = [:]
    Map<String, String> dataY = [:]
    for (String tbl : [dupTable, uniqTable]) {
        assertEquals(versionsOf(dbT, tbl), versionsOf(dbY, tbl))
        dataX[tbl] = rowsOf(dbT, tbl)
        dataY[tbl] = rowsOf(dbY, tbl)
        assertTrue(dataX[tbl] != dataY[tbl])
        assertTrue(dataX[tbl].startsWith("${numPartitions * numBatches * rowsPerBatch}:"))
        assertTrue(dataY[tbl].startsWith("${numPartitions * numBatches * rowsPerBatch}:"))
    }
    Map<String, Object> versionsBefore = [:]
    Map<String, Object> tabletsBefore = [:]
    for (String tbl : [dupTable, uniqTable]) {
        versionsBefore[tbl] = versionsOf(dbT, tbl)
        tabletsBefore[tbl] = tabletIdsOf(dbT, tbl)
    }

    // 2. back up T with the digest: the digest of X is computed and cached by the backends of T
    String tsS1 = backupAndWait(dbT, "${suiteName}_s1", true)
    // the snapshot of U, to be restored onto T in step 4
    String tsY = backupAndWait(dbY, "${suiteName}_y", false)

    // 3. B is restored from S1, and restoring S1 again keeps everything (the control)
    def stats = restoreAndWait(dbB, "${suiteName}_s1", tsS1, "disable", false)
    assertTrue((stats.downloaded_files as long) > 0)
    for (String tbl : [dupTable, uniqTable]) {
        assertEquals(dataX[tbl], rowsOf(dbB, tbl))
    }
    waitReported(dbB)
    stats = restoreAndWait(dbB, "${suiteName}_s1", tsS1, "full", false)
    assertEquals(0L, downloaded(stats))
    assertEquals(allPartitions, lastEstimate.kept_partitions as int)
    assertEquals(0, lastEstimate.download_digest_mismatch as int)
    for (String tbl : [dupTable, uniqTable]) {
        assertEquals(dataX[tbl], rowsOf(dbB, tbl))
    }

    // 4. the data of T is replaced by a non-atomic restore: the same tablets, versions, schema, other content
    stats = restoreAndWait(dbT, "${suiteName}_y", tsY, "disable", false)
    assertTrue((stats.downloaded_files as long) > 0)
    for (String tbl : [dupTable, uniqTable]) {
        assertEquals(tabletsBefore[tbl], tabletIdsOf(dbT, tbl))
        assertEquals(versionsBefore[tbl], versionsOf(dbT, tbl))
        assertEquals(dataY[tbl], rowsOf(dbT, tbl))
    }
    waitReported(dbT)

    // 5. back up T again with the digest, the digest must be the one of Y
    String tsS2 = backupAndWait(dbT, "${suiteName}_s2", true)

    // 6. B holds X, S2 holds Y: the digests differ, no partition may be kept, and B gets Y
    stats = restoreAndWait(dbB, "${suiteName}_s2", tsS2, "full", false)
    assertEquals(0, lastEstimate.kept_partitions as int)
    assertEquals(allPartitions, lastEstimate.download_digest_mismatch as int)
    assertTrue((stats.downloaded_files as long) > 0)
    for (String tbl : [dupTable, uniqTable]) {
        assertEquals(dataY[tbl], rowsOf(dbB, tbl))
        assertEquals(rowsOf(dbT, tbl), rowsOf(dbB, tbl))
        assertEquals(versionsOf(dbT, tbl), versionsOf(dbB, tbl))
    }
    // B holds Y now, S2 once more keeps all partitions
    waitReported(dbB)
    stats = restoreAndWait(dbB, "${suiteName}_s2", tsS2, "full", false)
    assertEquals(0L, downloaded(stats))
    assertEquals(allPartitions, lastEstimate.kept_partitions as int)
    for (String tbl : [dupTable, uniqTable]) {
        assertEquals(dataY[tbl], rowsOf(dbB, tbl))
    }

    setConfig("restore_reuse_min_partition_bytes", "0")
    setConfig("restore_reuse_default_check_level", "full")
    setConfig("restore_reuse_allow_hidden_state_models", "false")
    setConfig("enable_restore_partition_reuse", "false")
    for (String db : [dbT, dbY, dbB]) {
        sql "DROP DATABASE ${db} FORCE"
    }
    sql "DROP REPOSITORY `${repoName}`"
}
