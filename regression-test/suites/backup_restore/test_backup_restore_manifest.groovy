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

import com.amazonaws.services.s3.model.ListObjectsV2Request
import com.amazonaws.services.s3.model.ListObjectsV2Result
import com.google.gson.Gson
import groovy.json.JsonSlurper

// A backup writes the manifest of each tablet snapshot (file names, sizes and SHA-256) next to the tablet data, and
// records its SHA-256 (the root) in the job info. The restore fetches the manifest, checks it against the root,
// downloads the files in it and checks the downloaded snapshot against it on BE. The result is shown in the
// ManifestCheck column of SHOW RESTORE; a mismatch cancels the restore at once.
suite("test_backup_restore_manifest", "backup_restore") {
    String suiteName = "test_backup_restore_manifest"
    String repoName = "${suiteName}_repo_" + UUID.randomUUID().toString().replace("-", "")
    String srcDbName = "${suiteName}_src_db"
    String dbName = "${suiteName}_db"
    String tableName = "${suiteName}_table"
    String snapshotName = "${suiteName}_snapshot"
    int numPartitions = 3
    int numBuckets = 2
    // replication_num is 1 and reserve_replica is set, each tablet is downloaded once
    int numTablets = numPartitions * numBuckets
    int numRowsPerPartition = 5
    int numRows = numPartitions * numRowsPerPartition

    def syncer = getSyncer()
    syncer.createS3Repository(repoName)

    def createTable = { String db ->
        sql "DROP TABLE IF EXISTS ${db}.${tableName} FORCE"
        sql """
            CREATE TABLE ${db}.${tableName} (
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
            DISTRIBUTED BY HASH(`id`) BUCKETS ${numBuckets}
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
        sql "INSERT INTO ${db}.${tableName} VALUES ${values.join(",")}"
        // a second rowset in p1
        sql "INSERT INTO ${db}.${tableName} VALUES (1, 'again')"
        sql "sync"
    }

    def assertRowCount = { String db, int expected ->
        def result = sql "SELECT COUNT(*) FROM ${db}.${tableName}"
        assertEquals(expected, result[0][0] as int)
    }

    def lastRestoreJob = { String db ->
        def restoreResult = sql_return_maparray """ SHOW RESTORE FROM ${db} """
        def restoreJob = restoreResult.last()
        logger.info("restore job of ${db}: ${restoreJob}")
        assertEquals("FINISHED", restoreJob.State)
        return restoreJob
    }

    def lastCancelledRestoreJob = { String db ->
        def restoreJob = (sql_return_maparray """ SHOW RESTORE FROM ${db} """).last()
        logger.info("restore job of ${db}: ${restoreJob}")
        assertEquals("CANCELLED", restoreJob.State)
        return restoreJob
    }

    def manifestCheck = { restoreJob ->
        String shown = restoreJob.ManifestCheck as String
        assertTrue(shown != null && shown != "\\N", "ManifestCheck: ${shown}")
        return new JsonSlurper().parseText(shown)
    }

    for (String db : [srcDbName, dbName]) {
        sql "DROP DATABASE IF EXISTS ${db} FORCE"
        sql "CREATE DATABASE ${db}"
    }
    createTable(srcDbName)

    // 1. Repository path: the digests are computed when uploading.
    sql """
        BACKUP SNAPSHOT ${srcDbName}.${snapshotName}
        TO `${repoName}`
        ON (${tableName})
    """
    syncer.waitSnapshotFinish(srcDbName)
    String timestamp = syncer.getSnapshotTimestamp(repoName, snapshotName)
    assertTrue(timestamp != null)

    def restoreFromRepo = {
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
        def restoreJob = lastRestoreJob(dbName)
        assertRowCount(dbName, numRows + 1)
        return manifestCheck(restoreJob)
    }

    // 1.1 default: the set of files and the sizes are checked for all tablets, the digests are not.
    def check = restoreFromRepo()
    assertEquals(1, check.version)
    assertEquals("sha256", check.algo)
    assertEquals(numTablets, check.verified_tablets)
    assertEquals(0, check.unverified_tablets)
    assertEquals(false, check.digest_checked)

    // 1.2 with the digest check.
    setBeConfigTemporary([restore_manifest_digest_check: true]) {
        check = restoreFromRepo()
        assertEquals(numTablets, check.verified_tablets)
        assertEquals(0, check.unverified_tablets)
        assertEquals(true, check.digest_checked)
    }

    // 1.3 the check disabled on BE: nothing is verified, and the restore still succeeds as before.
    setBeConfigTemporary([restore_manifest_check: false]) {
        check = restoreFromRepo()
        assertEquals(0, check.verified_tablets)
        assertEquals(numTablets, check.unverified_tablets)
        assertEquals(false, check.digest_checked)
    }

    // 1.4 the manifests are next to the tablet dirs in the repository, one per tablet.
    def s3 = getS3Client()
    String bucket = getS3BucketName()
    def listRepoKeys = {
        List<String> keys = []
        def request = new ListObjectsV2Request().withBucketName(bucket)
                .withPrefix("${syncer.externalStoragePrefix()}/${repoName}/")
        ListObjectsV2Result result
        do {
            result = s3.listObjectsV2(request)
            keys.addAll(result.getObjectSummaries().collect { it.key })
            request.setContinuationToken(result.getNextContinuationToken())
        } while (result.isTruncated())
        return keys.findAll { it.contains("/__ss_${snapshotName}/") }
    }
    def repoKeys = listRepoKeys()
    def manifestKeys = repoKeys.findAll { it.contains("/__manifest__") }
    logger.info("manifests in the repository: ${manifestKeys}")
    assertEquals(numTablets, manifestKeys.size())
    for (String key : manifestKeys) {
        // .../__idx_<id>/__manifest__<tablet id>.<checksum>, not inside the tablet dir __<tablet id>/
        def parts = key.split("/")
        assertTrue(parts[parts.length - 2].startsWith("__idx_"), key)
    }

    // 1.5 an orphan file in a tablet dir of the repository, e.g. left by a retried upload: ignored, the restore
    // downloads the files in the manifest only. Without the manifest it would be downloaded, and fail the md5 check
    // as its name says md5 0.
    String dataKey = repoKeys.find { !it.contains("/__manifest__") && it.contains(".dat.") }
    assertTrue(dataKey != null, "no data file in ${repoKeys}")
    String orphanKey = dataKey.substring(0, dataKey.lastIndexOf("/") + 1) +
            "0200000000000000ffffffffffffffffffffffffffffffff_0.dat." + "0" * 32
    s3.putObject(bucket, orphanKey, "orphan")
    check = restoreFromRepo()
    assertEquals(numTablets, check.verified_tablets)
    assertEquals(0, check.unverified_tablets)
    s3.deleteObject(bucket, orphanKey)

    // 1.6 a tampered manifest in the repository: its SHA-256 does not match the root in the job info. BE downloads
    // the tablet again, then reports the mismatch, and FE cancels the restore at once instead of waiting until the
    // timeout (1 day by default).
    String tamperedKey = manifestKeys[0]
    String originalManifest = s3.getObjectAsString(bucket, tamperedKey)
    s3.putObject(bucket, tamperedKey, originalManifest.replaceFirst('"s":', '"s":1'))
    long start = System.currentTimeMillis()
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
    logger.info("the restore with a tampered manifest ends in ${System.currentTimeMillis() - start} ms")
    def cancelled = lastCancelledRestoreJob(dbName)
    assertTrue((cancelled.Status as String).contains("restore manifest check failed"), cancelled.Status as String)
    assertTrue((cancelled.Status as String).contains("does not match the root"), cancelled.Status as String)
    s3.putObject(bucket, tamperedKey, originalManifest)
    // the data restored before is intact
    assertRowCount(dbName, numRows + 1)

    // 2. Http path (a backup kept on local, as CCR does), restored into the suite db.
    String localDb = context.dbName
    createTable(localDb)
    def restoreLocalSnapshot = { String label, String properties ->
        sql """
            BACKUP SNAPSHOT ${localDb}.${label}
            TO `__keep_on_local__`
            ON (${tableName})
            ${properties}
        """
        syncer.waitSnapshotFinish(localDb)
        assertTrue(syncer.getSnapshot(label, tableName))
        assertTrue(syncer.restoreSnapshot())
        syncer.waitAllRestoreFinish(localDb)
        def restoreJob = lastRestoreJob(localDb)
        assertRowCount(localDb, numRows + 1)
        return manifestCheck(restoreJob)
    }

    // 2.1 default: sizes only.
    check = restoreLocalSnapshot("${suiteName}_local_1", "")
    assertEquals(1, check.version)
    assertEquals("none", check.algo)
    assertEquals(numTablets, check.verified_tablets)
    assertEquals(0, check.unverified_tablets)

    // 2.2 manifest_digest: the snapshot computes the digests, and the files linked from the local tablet instead of
    // downloading are checked too.
    setBeConfigTemporary([restore_manifest_digest_check: true]) {
        check = restoreLocalSnapshot("${suiteName}_local_2", """PROPERTIES ("manifest_digest" = "true")""")
        assertEquals("sha256", check.algo)
        assertEquals(numTablets, check.verified_tablets)
        assertEquals(0, check.unverified_tablets)
        assertEquals(true, check.digest_checked)
    }

    // 2.3 the job info from the source says another root for a tablet than its manifest: cancelled at once.
    String localLabel = "${suiteName}_local_3"
    sql """
        BACKUP SNAPSHOT ${localDb}.${localLabel}
        TO `__keep_on_local__`
        ON (${tableName})
    """
    syncer.waitSnapshotFinish(localDb)
    assertTrue(syncer.getSnapshot(localLabel, tableName))
    Gson gson = new Gson()
    Map jobInfo = gson.fromJson(new String(syncer.context.getSnapshotResult.getJobInfo()), Map.class)
    boolean replaced = false
    jobInfo.backup_objects.each { tbl, tblInfo ->
        tblInfo.partitions.each { part, partInfo ->
            partInfo.indexes.each { idx, idxInfo ->
                if (!replaced && idxInfo.manifest_roots) {
                    def tabletId = idxInfo.manifest_roots.keySet().iterator().next()
                    idxInfo.manifest_roots[tabletId] = "0" * 64
                    replaced = true
                }
            }
        }
    }
    assertTrue(replaced, "no manifest_roots in ${jobInfo}")
    syncer.context.getSnapshotResult.setJobInfo(gson.toJson(jobInfo).getBytes())
    assertTrue(syncer.restoreSnapshot())
    syncer.waitAllRestoreFinish(localDb)
    cancelled = lastCancelledRestoreJob(localDb)
    assertTrue((cancelled.Status as String).contains("does not match the root"), cancelled.Status as String)
    assertRowCount(localDb, numRows + 1)

    // 3. SHOW BRIEF RESTORE has no ManifestCheck.
    def brief = sql_return_maparray """ SHOW BRIEF RESTORE FROM ${dbName} """
    assertFalse(brief.last().containsKey("ManifestCheck"))

    sql "DROP TABLE ${localDb}.${tableName} FORCE"
    for (String db : [srcDbName, dbName]) {
        sql "DROP DATABASE ${db} FORCE"
    }
    sql "DROP REPOSITORY `${repoName}`"
}
