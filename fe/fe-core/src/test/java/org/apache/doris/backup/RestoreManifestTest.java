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

package org.apache.doris.backup;

import org.apache.doris.analysis.StorageBackend;
import org.apache.doris.backup.BackupJobInfo.BackupIndexInfo;
import org.apache.doris.backup.BackupJobInfo.BackupOlapTableInfo;
import org.apache.doris.backup.BackupJobInfo.BackupPartitionInfo;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.FsBroker;
import org.apache.doris.catalog.ReplicaAllocation;
import org.apache.doris.common.Config;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.nereids.trees.plans.commands.BackupCommand.BackupContent;
import org.apache.doris.nereids.trees.plans.commands.ShowRestoreCommand;
import org.apache.doris.persist.EditLog;
import org.apache.doris.task.DownloadTask;
import org.apache.doris.thrift.TBackend;
import org.apache.doris.thrift.TDownloadReq;
import org.apache.doris.thrift.TFinishTaskRequest;
import org.apache.doris.thrift.TRemoteTabletSnapshot;
import org.apache.doris.thrift.TStatus;
import org.apache.doris.thrift.TStatusCode;
import org.apache.doris.thrift.TTaskType;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

public class RestoreManifestTest {
    private static final long TABLET_1 = 101;
    private static final long TABLET_2 = 102;
    private static final String MD5 = "4f158689243a3d6030352fec3cfd3798";

    private final Env env = Mockito.mock(Env.class);
    private final InternalCatalog catalog = Mockito.mock(InternalCatalog.class);
    private final EditLog editLog = Mockito.mock(EditLog.class);
    private MockedStatic<Env> mockedEnvStatic;
    private Database db;

    @BeforeEach
    public void setUp() throws Exception {
        db = CatalogMocker.mockDb();
        mockedEnvStatic = Mockito.mockStatic(Env.class);
        mockedEnvStatic.when(Env::getCurrentEnvJournalVersion).thenReturn(FeConstants.meta_version);
        mockedEnvStatic.when(Env::getCurrentEnv).thenReturn(env);
        Mockito.when(env.getInternalCatalog()).thenReturn(catalog);
        Mockito.when(env.getEditLog()).thenReturn(editLog);
        Mockito.when(catalog.getDbNullable(Mockito.anyLong())).thenReturn(db);
    }

    @AfterEach
    public void tearDown() {
        if (mockedEnvStatic != null) {
            mockedEnvStatic.close();
        }
    }

    private static String sha(char c) {
        return String.valueOf(c).repeat(64);
    }

    // A job info with one table, one partition, one index and the given tablets, each with files 1.dat, 1.idx and
    // <tablet>.hdr.
    private static BackupJobInfo newJobInfo(long... tabletIds) {
        BackupJobInfo jobInfo = new BackupJobInfo();
        jobInfo.name = "snapshot_1";
        jobInfo.dbName = "src_db";
        jobInfo.dbId = 1;
        jobInfo.backupTime = 1700000000000L;
        jobInfo.success = true;
        jobInfo.content = BackupContent.ALL;
        jobInfo.metaVersion = FeConstants.meta_version;
        BackupOlapTableInfo tblInfo = new BackupOlapTableInfo();
        tblInfo.id = 2;
        BackupPartitionInfo partInfo = new BackupPartitionInfo();
        partInfo.id = 3;
        partInfo.version = 10;
        BackupIndexInfo idxInfo = new BackupIndexInfo();
        idxInfo.id = 4;
        idxInfo.schemaHash = 5;
        for (long tabletId : tabletIds) {
            idxInfo.tablets.put(tabletId, Lists.newArrayList("1.dat." + MD5, "1.idx." + MD5,
                    tabletId + ".hdr." + MD5));
            idxInfo.tabletsOrder.add(tabletId);
        }
        partInfo.indexes.put("tbl", idxInfo);
        tblInfo.partitions.put("p1", partInfo);
        jobInfo.backupOlapTableObjects.put("tbl", tblInfo);
        return jobInfo;
    }

    private static String root(long tabletId) {
        return String.format("%064x", tabletId);
    }

    private static SnapshotInfo snapshotInfo(long tabletId, String manifestRoot) {
        SnapshotInfo info = new SnapshotInfo(1, 2, 3, 4, tabletId, 10001, 5, "/path",
                Lists.newArrayList("1.dat." + MD5, "1.idx." + MD5, tabletId + ".hdr." + MD5));
        info.setManifestRoot(manifestRoot);
        return info;
    }

    private static Map<Long, SnapshotInfo> snapshotInfos(SnapshotInfo... infos) {
        Map<Long, SnapshotInfo> map = Maps.newHashMap();
        for (SnapshotInfo info : infos) {
            map.put(info.getTabletId(), info);
        }
        return map;
    }

    private static BackupJobInfo jobInfoWithManifest(long... tabletIds) {
        BackupJobInfo jobInfo = newJobInfo(tabletIds);
        SnapshotInfo[] infos = new SnapshotInfo[tabletIds.length];
        for (int i = 0; i < tabletIds.length; i++) {
            infos[i] = snapshotInfo(tabletIds[i], root(tabletIds[i]));
        }
        Assertions.assertTrue(jobInfo.buildManifest(snapshotInfos(infos), BackupJobInfo.DIGEST_SHA256));
        return jobInfo;
    }

    // ---- generating the manifest ----

    @Test
    public void testBuildManifest() {
        BackupJobInfo jobInfo = jobInfoWithManifest(TABLET_1, TABLET_2);
        Assertions.assertEquals(1, (int) jobInfo.manifestVersion);
        Assertions.assertEquals("sha256", jobInfo.digestAlgorithm);
        Assertions.assertEquals(root(TABLET_1), jobInfo.getManifestRoot(TABLET_1));
        Assertions.assertEquals(root(TABLET_2), jobInfo.getManifestRoot(TABLET_2));
        Assertions.assertNull(jobInfo.getManifestRoot(999));

        // a backup kept on local without manifest_digest; upper case roots are normalized
        Assertions.assertTrue(jobInfo.buildManifest(snapshotInfos(
                snapshotInfo(TABLET_1, root(TABLET_1).toUpperCase().replace('0', 'A')),
                snapshotInfo(TABLET_2, root(TABLET_2))), BackupJobInfo.DIGEST_NONE));
        Assertions.assertEquals("none", jobInfo.digestAlgorithm);
        Assertions.assertEquals(root(TABLET_1).replace('0', 'a'), jobInfo.getManifestRoot(TABLET_1));
    }

    @Test
    public void testNoManifestIfAnyTabletLacksManifestRoot() {
        BackupJobInfo jobInfo = newJobInfo(TABLET_1, TABLET_2);
        // tablet 2 is on an old backend
        Assertions.assertFalse(jobInfo.buildManifest(snapshotInfos(snapshotInfo(TABLET_1, root(TABLET_1)),
                snapshotInfo(TABLET_2, null)), BackupJobInfo.DIGEST_SHA256));
        Assertions.assertFalse(jobInfo.hasManifest());
        Assertions.assertNull(jobInfo.manifestVersion);
        Assertions.assertNull(jobInfo.digestAlgorithm);
        Assertions.assertNull(jobInfo.getManifestRoot(TABLET_1));
        Assertions.assertFalse(jobInfo.toJson(false).contains("manifest"));

        // not a SHA-256
        for (String bad : new String[] {"", "abc", root(TABLET_2).substring(1) + "g"}) {
            Assertions.assertFalse(jobInfo.buildManifest(snapshotInfos(snapshotInfo(TABLET_1, root(TABLET_1)),
                    snapshotInfo(TABLET_2, bad)), BackupJobInfo.DIGEST_SHA256), bad);
        }
        // a snapshot info is missing
        Assertions.assertFalse(jobInfo.buildManifest(snapshotInfos(snapshotInfo(TABLET_1, root(TABLET_1))),
                BackupJobInfo.DIGEST_SHA256));

        // a successful build after a failed one
        Assertions.assertTrue(jobInfo.buildManifest(snapshotInfos(snapshotInfo(TABLET_1, root(TABLET_1)),
                snapshotInfo(TABLET_2, root(TABLET_2))), BackupJobInfo.DIGEST_SHA256));
        Assertions.assertTrue(jobInfo.hasManifest());

        // metadata only backup has no files
        jobInfo.content = BackupContent.METADATA_ONLY;
        Assertions.assertFalse(jobInfo.buildManifest(snapshotInfos(snapshotInfo(TABLET_1, root(TABLET_1)),
                snapshotInfo(TABLET_2, root(TABLET_2))), BackupJobInfo.DIGEST_SHA256));
        Assertions.assertFalse(jobInfo.hasManifest());
        Assertions.assertNull(jobInfo.getManifestRoot(TABLET_1));
    }

    // ---- persisting ----

    @Test
    public void testGsonRoundTrip() {
        BackupJobInfo jobInfo = jobInfoWithManifest(TABLET_1, TABLET_2);
        String json = jobInfo.toJson(false);
        Assertions.assertTrue(json.contains("\"manifest_version\":1"), json);
        Assertions.assertTrue(json.contains("\"digest_algorithm\":\"sha256\""), json);
        Assertions.assertTrue(json.contains("\"manifest_roots\":{"), json);
        Assertions.assertTrue(json.contains("\"101\":\"" + root(TABLET_1) + "\""), json);

        BackupJobInfo read = BackupJobInfo.genFromJson(json);
        Assertions.assertTrue(read.hasManifest());
        Assertions.assertEquals("sha256", read.digestAlgorithm);
        Assertions.assertEquals(root(TABLET_1), read.getManifestRoot(TABLET_1));
        Assertions.assertEquals(root(TABLET_2), read.getManifestRoot(TABLET_2));
        // the BackupTabletInfo view
        BackupIndexInfo idxInfo = read.backupOlapTableObjects.get("tbl").partitions.get("p1").indexes.get("tbl");
        Assertions.assertEquals(2, idxInfo.sortedTabletInfoList.size());
        Assertions.assertEquals(root(TABLET_1), idxInfo.sortedTabletInfoList.get(0).manifestRoot);
        // written again, the same
        Assertions.assertEquals(json, read.toJson(false));

        // released after the restore: the roots are gone, the version and algorithm are kept for SHOW RESTORE.
        read.releaseSnapshotInfo();
        Assertions.assertTrue(read.hasManifest());
        Assertions.assertNull(read.getManifestRoot(TABLET_1));
        Assertions.assertNull(idxInfo.sortedTabletInfoList.get(0).manifestRoot);
        Assertions.assertFalse(read.toJson(false).contains("manifest_roots"));
    }

    @Test
    public void testReadOldJobInfo() {
        // a job info written by an old version, without any manifest field.
        String json = "{\"name\":\"snapshot_1\",\"database\":\"src_db\",\"id\":1,\"backup_time\":1700000000000,"
                + "\"content\":\"ALL\",\"backup_objects\":{\"tbl\":{\"id\":2,\"partitions\":{\"p1\":{\"id\":3,"
                + "\"version\":10,\"indexes\":{\"tbl\":{\"id\":4,\"schema_hash\":5,\"tablets\":{\"101\":"
                + "[\"1.dat." + MD5 + "\",\"101.hdr." + MD5 + "\"]},\"tablets_order\":[101]}}}}}},"
                + "\"new_backup_objects\":{},\"backup_result\":\"succeed\",\"meta_version\":0,"
                + "\"tablet_be_map\":{},\"tablet_snapshot_path_map\":{}}";
        BackupJobInfo jobInfo = BackupJobInfo.genFromJson(json);
        Assertions.assertFalse(jobInfo.hasManifest());
        Assertions.assertNull(jobInfo.manifestVersion);
        Assertions.assertNull(jobInfo.digestAlgorithm);
        Assertions.assertNull(jobInfo.getManifestRoot(101));
        BackupIndexInfo idxInfo = jobInfo.backupOlapTableObjects.get("tbl").partitions.get("p1").indexes.get("tbl");
        Assertions.assertNull(idxInfo.manifestRoots);
        Assertions.assertNull(idxInfo.sortedTabletInfoList.get(0).manifestRoot);
        Assertions.assertEquals(2, idxInfo.sortedTabletInfoList.get(0).files.size());
        Assertions.assertFalse(jobInfo.toJson(false).contains("manifest"));
    }

    @Test
    public void testUnknownManifestVersionIsNotChecked() {
        String json = jobInfoWithManifest(TABLET_1).toJson(false)
                .replace("\"manifest_version\":1", "\"manifest_version\":2");
        BackupJobInfo read = BackupJobInfo.genFromJson(json);
        Assertions.assertEquals(2, (int) read.manifestVersion);
        Assertions.assertFalse(read.hasManifest());
        Assertions.assertNull(read.getManifestRoot(TABLET_1));

        RestoreJob job = newRestoreJob(read);
        Assertions.assertNull(job.getExpectedManifestRoot(TABLET_1));
        Assertions.assertEquals(FeConstants.null_string, manifestCheckColumn(job));
    }

    // ---- restore ----

    private RestoreJob newRestoreJob(BackupJobInfo jobInfo) {
        return new RestoreJob("restore_label", "2024-01-01 00:00:00", db.getId(), db.getFullName(), jobInfo, false,
                new ReplicaAllocation((short) 3), 100000, -1, false, false, false, false, false, false,
                false, false, env, 20000);
    }

    private static String manifestCheckColumn(RestoreJob job) {
        List<String> fullInfo = job.getFullInfo();
        Assertions.assertEquals(ShowRestoreCommand.TITLE_NAMES.size(), fullInfo.size());
        return fullInfo.get(ShowRestoreCommand.TITLE_NAMES.indexOf("ManifestCheck"));
    }

    @Test
    public void testShowRestoreColumn() {
        // appended after ReuseEstimate, at the end; SHOW BRIEF RESTORE is unchanged.
        int column = ShowRestoreCommand.TITLE_NAMES.indexOf("ManifestCheck");
        // DownloadStats is appended after it.
        Assertions.assertEquals(ShowRestoreCommand.TITLE_NAMES.size() - 2, column);
        Assertions.assertEquals(column - 1, ShowRestoreCommand.TITLE_NAMES.indexOf("ReuseEstimate"));
        Assertions.assertFalse(ShowRestoreCommand.BRIEF_TITLE_NAMES.contains("ManifestCheck"));
        Assertions.assertFalse(ShowRestoreCommand.BRIEF_TITLE_NAMES.contains("ReuseEstimate"));

        // an old backup has no manifest
        RestoreJob job = newRestoreJob(newJobInfo(TABLET_1));
        Assertions.assertEquals(FeConstants.null_string, manifestCheckColumn(job));
        Assertions.assertNull(job.getExpectedManifestRoot(TABLET_1));
        Assertions.assertEquals(ShowRestoreCommand.BRIEF_TITLE_NAMES.size(), job.getBriefInfo().size());
    }

    @Test
    public void testManifestRootInDownloadTask() {
        RestoreJob job = newRestoreJob(jobInfoWithManifest(TABLET_1));
        String manifestRoot = job.getExpectedManifestRoot(TABLET_1);
        Assertions.assertEquals(root(TABLET_1), manifestRoot);
        Assertions.assertNull(job.getExpectedManifestRoot(999));

        // repository path: keyed by the src path, the same as src_dest_map
        Map<String, String> srcToDest = Maps.newHashMap();
        srcToDest.put("bos://repo/__101", "/local/snapshot/201/5");
        DownloadTask task = new DownloadTask(null, 10001, 1, 2, 3, srcToDest, new FsBroker("127.0.0.1", 8000),
                Maps.newHashMap(), StorageBackend.StorageType.BROKER, "bos://repo", "");
        TDownloadReq req = task.toThrift();
        Assertions.assertFalse(req.isSetManifestRoots());
        Map<String, String> roots = Maps.newHashMap();
        roots.put("bos://repo/__101", manifestRoot);
        task.setManifestRoots(roots);
        req = task.toThrift();
        Assertions.assertTrue(req.isSetManifestRoots());
        Assertions.assertEquals(manifestRoot, req.getManifestRoots().get("bos://repo/__101"));
        Assertions.assertEquals(req.getSrcDestMap().keySet(), req.getManifestRoots().keySet());

        // http path: in the remote tablet snapshot
        TRemoteTabletSnapshot snapshot = new TRemoteTabletSnapshot();
        snapshot.setLocalTabletId(201);
        snapshot.setRemoteTabletId(TABLET_1);
        snapshot.setManifestRoot(manifestRoot);
        req = new DownloadTask(null, 10001, 1, 2, 3, Lists.newArrayList(snapshot)).toThrift();
        Assertions.assertFalse(req.isSetManifestRoots());
        Assertions.assertEquals(manifestRoot, req.getRemoteTabletSnapshots().get(0).getManifestRoot());

        // not in cloud mode
        String origDeployMode = Config.deploy_mode;
        try {
            Config.deploy_mode = "cloud";
            Assertions.assertNull(job.getExpectedManifestRoot(TABLET_1));
        } finally {
            Config.deploy_mode = origDeployMode;
        }
    }

    private static TFinishTaskRequest downloadFinished(Boolean digestChecked, List<Long> verifiedTablets) {
        TFinishTaskRequest request = new TFinishTaskRequest(new TBackend("", 0, 1), TTaskType.DOWNLOAD, 1,
                new TStatus(TStatusCode.OK));
        if (verifiedTablets != null) {
            request.setManifestVerifiedTablets(verifiedTablets);
        }
        if (digestChecked != null) {
            request.setManifestDigestChecked(digestChecked);
        }
        return request;
    }

    @Test
    public void testManifestCheckResult() throws Exception {
        RestoreJob job = newRestoreJob(jobInfoWithManifest(TABLET_1, TABLET_2));
        // 4 replicas to download: tablet 201 and 202 on backend 1 and 2.
        for (long beId : new long[] {1, 2}) {
            for (long tabletId : new long[] {201, 202}) {
                job.snapshotInfos.put(tabletId, beId, new SnapshotInfo(1, 2, 3, 4, tabletId, beId, 5, "/p",
                        Lists.newArrayList()));
            }
        }
        Assertions.assertEquals("{\"version\":1,\"algo\":\"sha256\",\"verified_tablets\":0,\"unverified_tablets\":4,"
                + "\"digest_checked\":false}", manifestCheckColumn(job));

        // backend 1 is new and checked both tablets, with digest
        job.recordManifestCheck(1, downloadFinished(true, Lists.newArrayList(201L, 202L)));
        // reported again by a retried task, counted once
        job.recordManifestCheck(1, downloadFinished(true, Lists.newArrayList(201L)));
        RestoreManifestCheck check = job.computeManifestCheck();
        Assertions.assertEquals(2, check.getVerifiedTablets());
        Assertions.assertEquals(2, check.getUnverifiedTablets());
        Assertions.assertTrue(check.isDigestChecked());

        // backend 2 is old, reports nothing: unverified, never failed
        job.recordManifestCheck(2, downloadFinished(null, null));
        Assertions.assertEquals("{\"version\":1,\"algo\":\"sha256\",\"verified_tablets\":2,\"unverified_tablets\":2,"
                + "\"digest_checked\":true}", manifestCheckColumn(job));

        // backend 2 upgraded, checks without digest (restore_manifest_digest_check=false)
        job.recordManifestCheck(2, downloadFinished(false, Lists.newArrayList(201L, 202L)));
        String shown = "{\"version\":1,\"algo\":\"sha256\",\"verified_tablets\":4,\"unverified_tablets\":0,"
                + "\"digest_checked\":false}";
        Assertions.assertEquals(shown, manifestCheckColumn(job));

        // fixed when the download finishes, persisted with the job, then shown on followers and after restart.
        Deencapsulation.setField(job, "manifestCheck", job.computeManifestCheck());
        job.jobInfo.releaseSnapshotInfo();
        job.snapshotInfos.clear();
        Assertions.assertEquals(shown, manifestCheckColumn(job));
        RestoreJob readJob = writeAndRead(job);
        Assertions.assertEquals(shown, manifestCheckColumn(readJob));
    }

    @Test
    public void testManifestCheckOfRestoreJobWithoutManifest() throws Exception {
        // as persisted by an old version: no "mck", and the job info has no manifest.
        RestoreJob job = newRestoreJob(newJobInfo(TABLET_1));
        RestoreJob readJob = writeAndRead(job);
        Assertions.assertNull(readJob.getManifestCheck());
        Assertions.assertEquals(FeConstants.null_string, manifestCheckColumn(readJob));
    }

    private static TFinishTaskRequest downloadFailed(TStatusCode code, String errMsg) {
        TStatus status = new TStatus(code);
        status.setErrorMsgs(Lists.newArrayList(errMsg));
        return new TFinishTaskRequest(new TBackend("", 0, 1), TTaskType.DOWNLOAD, 1, status);
    }

    @Test
    public void testManifestMismatchCancelsTheJobAtOnce() {
        String errMsg = "restore manifest check failed, tablet 201, path /p: size mismatch of file 1.dat, "
                + "expected 100, actual 99";
        RestoreJob job = newRestoreJob(jobInfoWithManifest(TABLET_1));
        Deencapsulation.setField(job, "state", RestoreJob.RestoreJobState.DOWNLOADING);
        DownloadTask task = new DownloadTask(null, 10001, 1, job.getJobId(), db.getId(), Lists.newArrayList());
        Assertions.assertFalse(job.finishTabletDownloadTask(task,
                downloadFailed(TStatusCode.RESTORE_MANIFEST_MISMATCH, errMsg)));
        Assertions.assertEquals(RestoreJob.RestoreJobState.CANCELLED, job.getState());
        Assertions.assertEquals(Status.ErrCode.COMMON_ERROR, job.getStatus().getErrCode());
        Assertions.assertTrue(job.getStatus().getErrMsg().contains("restore manifest check failed on backend 10001"),
                job.getStatus().getErrMsg());
        Assertions.assertTrue(job.getStatus().getErrMsg().contains(errMsg), job.getStatus().getErrMsg());
        // the partial result of the check is kept
        Assertions.assertNotNull(job.getManifestCheck());

        // other download failures keep the current behavior: the task is retried until the job times out.
        RestoreJob other = newRestoreJob(jobInfoWithManifest(TABLET_1));
        Deencapsulation.setField(other, "state", RestoreJob.RestoreJobState.DOWNLOADING);
        DownloadTask otherTask = new DownloadTask(null, 10001, 2, other.getJobId(), db.getId(),
                Lists.newArrayList());
        Assertions.assertFalse(other.finishTabletDownloadTask(otherTask,
                downloadFailed(TStatusCode.INTERNAL_ERROR, "failed to download")));
        Assertions.assertEquals(RestoreJob.RestoreJobState.DOWNLOADING, other.getState());
        Assertions.assertTrue(other.getStatus().ok());

        // a late report after the job is done changes nothing
        Assertions.assertFalse(job.finishTabletDownloadTask(task,
                downloadFailed(TStatusCode.RESTORE_MANIFEST_MISMATCH, errMsg)));
        Assertions.assertEquals(RestoreJob.RestoreJobState.CANCELLED, job.getState());
    }

    private static RestoreJob writeAndRead(RestoreJob job) throws Exception {
        Path path = Files.createTempFile("restoreManifest", "tmp");
        try {
            try (DataOutputStream out = new DataOutputStream(Files.newOutputStream(path))) {
                job.write(out);
            }
            try (DataInputStream in = new DataInputStream(Files.newInputStream(path))) {
                return RestoreJob.read(in);
            }
        } finally {
            Files.delete(path);
        }
    }

    // ---- the size of the manifest ----

    /**
     * Measure how much the manifest adds to the job info, which is kept in FE memory and written to the edit log with
     * the restore job: only the root of each tablet, the manifest itself is a file next to the tablet data. Also
     * estimate the size of a manifest file of a tablet with 30 files, in the format written by BE.
     */
    @Test
    public void testManifestSize() {
        int tablets = 10000;
        long[] r = measureManifestSize(tablets);
        long[] half = measureManifestSize(tablets / 2);
        long delta = r[1] - r[0];
        long perTablet = delta / tablets;
        // grows linearly with the number of tablets, about 80 bytes per tablet
        Assertions.assertTrue(Math.abs(perTablet - (half[1] - half[0]) / (tablets / 2)) <= 1);
        Assertions.assertTrue(perTablet <= 100, String.valueOf(perTablet));
        System.out.printf("manifest in job info: %d tablets: %d -> %d bytes, +%d bytes (+%d per tablet, x%.3f); "
                        + "estimated +%.2f MB for 10k tablets, +%.2f MB for 100k tablets%n",
                tablets, r[0], r[1], delta, perTablet, (double) r[1] / r[0],
                delta * 10000.0 / tablets / 1024 / 1024, delta * 100000.0 / tablets / 1024 / 1024);
        System.out.printf("manifest file of a tablet with 30 files: repository (md5 + sha256) %d bytes, "
                        + "local with sha256 %d bytes, local sizes only %d bytes%n",
                manifestFileSize(30, true, true), manifestFileSize(30, false, true),
                manifestFileSize(30, false, false));
    }

    // The job info of a table with 30 files per tablet, returns {bytes without manifest, bytes with manifest}.
    private static long[] measureManifestSize(int tablets) {
        int filesPerTablet = 30;
        BackupJobInfo jobInfo = new BackupJobInfo();
        jobInfo.name = "snapshot_1";
        jobInfo.dbName = "src_db";
        jobInfo.content = BackupContent.ALL;
        BackupOlapTableInfo tblInfo = new BackupOlapTableInfo();
        BackupPartitionInfo partInfo = new BackupPartitionInfo();
        BackupIndexInfo idxInfo = new BackupIndexInfo();
        partInfo.indexes.put("tbl", idxInfo);
        tblInfo.partitions.put("p1", partInfo);
        jobInfo.backupOlapTableObjects.put("tbl", tblInfo);
        Map<Long, SnapshotInfo> infos = Maps.newHashMap();
        long baseTabletId = 1_700_000_000L;
        for (int t = 0; t < tablets; t++) {
            long tabletId = baseTabletId + t;
            List<String> files = Lists.newArrayListWithCapacity(filesPerTablet);
            files.add(tabletId + ".hdr." + MD5);
            for (int f = 1; f < filesPerTablet; f++) {
                files.add(fileName(tabletId, t * filesPerTablet + f) + "." + MD5);
            }
            idxInfo.tablets.put(tabletId, files);
            idxInfo.tabletsOrder.add(tabletId);
            SnapshotInfo info = new SnapshotInfo(1, 2, 3, 4, tabletId, 10001, 5, "/path", files);
            info.setManifestRoot(String.format("%064x", tabletId * 1000003L).replace('0', 'e'));
            infos.put(tabletId, info);
        }
        long without = jobInfo.toJson(false).getBytes(StandardCharsets.UTF_8).length;
        Assertions.assertTrue(jobInfo.buildManifest(infos, BackupJobInfo.DIGEST_SHA256));
        long with = jobInfo.toJson(false).getBytes(StandardCharsets.UTF_8).length;
        return new long[] {without, with};
    }

    // a segment or index file name with a rowset id v2 (48 hex chars)
    private static String fileName(long tabletId, int seq) {
        return String.format("02%014x%032x", tabletId, (long) seq) + "_" + (seq % 4) + (seq % 2 == 0 ? ".dat" : ".idx");
    }

    // The size of a manifest file in the format written by BE, see SnapshotManifest::serialize.
    private static int manifestFileSize(int files, boolean withMd5, boolean withDigest) {
        long tabletId = 1_700_000_000L;
        StringBuilder sb = new StringBuilder("{\"version\":1,\"tablet_id\":" + tabletId + ",\"files\":[");
        for (int f = 0; f < files; f++) {
            String name = f == 0 ? tabletId + ".hdr" : fileName(tabletId, f);
            sb.append(f == 0 ? "" : ",").append("{\"n\":\"").append(name).append("\",\"s\":").append(1_234_567_890L);
            if (withMd5) {
                sb.append(",\"m\":\"").append(MD5).append('"');
            }
            if (withDigest && f > 0) {
                sb.append(",\"d\":\"").append("e".repeat(64)).append('"');
            }
            sb.append('}');
        }
        return sb.append("]}").toString().getBytes(StandardCharsets.UTF_8).length;
    }
}
