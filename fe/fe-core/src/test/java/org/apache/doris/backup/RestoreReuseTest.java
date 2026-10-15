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

import org.apache.doris.backup.BackupJobInfo.BackupIndexInfo;
import org.apache.doris.backup.BackupJobInfo.BackupOlapTableInfo;
import org.apache.doris.backup.BackupJobInfo.BackupPartitionInfo;
import org.apache.doris.backup.BackupJobInfo.BackupTabletInfo;
import org.apache.doris.backup.RestoreJob.RestoreJobState;
import org.apache.doris.backup.RestoreReuseJudge.CheckLevel;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.MaterializedIndex.IndexExtState;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.OlapTable.OlapTableState;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.PartitionInfo;
import org.apache.doris.catalog.PartitionItem;
import org.apache.doris.catalog.Replica;
import org.apache.doris.catalog.Replica.ReplicaState;
import org.apache.doris.catalog.ReplicaAllocation;
import org.apache.doris.catalog.Resource;
import org.apache.doris.catalog.RestoreLineage;
import org.apache.doris.catalog.RestoreSource;
import org.apache.doris.catalog.Tablet;
import org.apache.doris.catalog.TabletInvertedIndex;
import org.apache.doris.common.Config;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.Pair;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.datasource.CatalogMgr;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.persist.EditLog;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.resource.Tag;
import org.apache.doris.system.Backend;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.task.AgentBatchTask;
import org.apache.doris.task.AgentTask;
import org.apache.doris.task.AgentTaskExecutor;
import org.apache.doris.task.DirMoveTask;
import org.apache.doris.task.DownloadTask;
import org.apache.doris.task.ReleaseSnapshotTask;
import org.apache.doris.task.RestoreDigestTask;
import org.apache.doris.task.SnapshotTask;
import org.apache.doris.thrift.TDownloadStats;
import org.apache.doris.thrift.TFinishTaskRequest;
import org.apache.doris.thrift.TLogicalDigest;
import org.apache.doris.thrift.TRemoteTabletSnapshot;
import org.apache.doris.thrift.TStatus;
import org.apache.doris.thrift.TStatusCode;
import org.apache.doris.thrift.TStorageMedium;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * Partition level reuse of restore: the candidate conditions, the check levels, the VERIFYING state, the partitions
 * that keep their local data, persistence and the recheck before commit.
 */
public class RestoreReuseTest {
    private static final long V1 = 10;
    private static final long BACKUP_TIME = 1700000000000L;
    private static final long RESTORE_TIME = 1800000000000L;
    private static final long COMMIT_SEQ = 500;
    private static final String ROOT_P1 = "aa11";
    private static final String ROOT_P2 = "bb22";
    private static final String SIG = "sig-1";
    // the cluster id of the source of the backup, and the one of this cluster
    private static final long SRC_CLUSTER = 777;
    private static final long LOCAL_CLUSTER = 555;

    private final Env env = Mockito.mock(Env.class);
    private final InternalCatalog catalog = Mockito.mock(InternalCatalog.class);
    private final EditLog editLog = Mockito.mock(EditLog.class);
    private final SystemInfoService systemInfo = Mockito.mock(SystemInfoService.class);
    private MockedStatic<Env> mockedEnvStatic;
    private MockedStatic<AgentTaskExecutor> mockedExecutor;
    // the tasks submitted to the backends
    private final List<AgentTask> submitted = Lists.newArrayList();

    private Database db;
    private OlapTable tbl2;

    private boolean origEnable;
    private boolean origIncremental;
    private boolean origAtomic;
    private String origLevel;
    private double origRatio;
    private long origMinBytes;
    private boolean origForceFull;
    private int origTimeout;
    private boolean origUnverified;
    private boolean origHidden;

    @BeforeEach
    public void setUp() throws Exception {
        origEnable = Config.enable_restore_partition_reuse;
        origIncremental = Config.enable_restore_incremental_append;
        origAtomic = Config.enable_restore_atomic_reuse;
        origLevel = Config.restore_reuse_default_check_level;
        origRatio = Config.restore_reuse_sample_ratio;
        origMinBytes = Config.restore_reuse_min_partition_bytes;
        origTimeout = Config.restore_digest_timeout_s;
        origForceFull = Config.restore_reuse_force_full_for_relation;
        origUnverified = Config.restore_reuse_allow_unverified_keep;
        origHidden = Config.restore_reuse_allow_hidden_state_models;
        Config.enable_restore_partition_reuse = true;
        Config.restore_reuse_default_check_level = "full";
        // The tests of the mechanics (the sample, the level off, the unique tables) run with the narrowing lifted,
        // the tests of the narrowing set the configs back.
        Config.restore_reuse_allow_unverified_keep = true;
        Config.restore_reuse_allow_hidden_state_models = true;
        Config.restore_reuse_min_partition_bytes = 100;

        db = CatalogMocker.mockDb();
        tbl2 = (OlapTable) db.getTableNullable(CatalogMocker.TEST_TBL2_ID);
        Deencapsulation.setField(tbl2, "keysType", KeysType.DUP_KEYS);

        mockedEnvStatic = Mockito.mockStatic(Env.class);
        mockedEnvStatic.when(Env::getCurrentEnvJournalVersion).thenReturn(FeConstants.meta_version);
        mockedEnvStatic.when(Env::getCurrentEnv).thenReturn(env);
        mockedEnvStatic.when(Env::getCurrentSystemInfo).thenReturn(systemInfo);
        Mockito.when(env.getInternalCatalog()).thenReturn(catalog);
        Mockito.when(env.getEditLog()).thenReturn(editLog);
        Mockito.when(env.getClusterId()).thenReturn((int) LOCAL_CLUSTER);
        Mockito.when(catalog.getDbNullable(Mockito.anyLong())).thenReturn(db);
        Mockito.when(catalog.getDbOrMetaException(Mockito.anyLong())).thenReturn(db);
        CatalogMgr catalogMgr = Mockito.mock(CatalogMgr.class);
        Mockito.when(env.getCatalogMgr()).thenReturn(catalogMgr);
        Mockito.when(catalogMgr.getCatalog(Mockito.anyString())).thenReturn(catalog);
        AtomicLong nextId = new AtomicLong(90000);
        Mockito.when(env.getNextId()).thenAnswer(inv -> nextId.getAndIncrement());
        Backend backend = Mockito.mock(Backend.class);
        Mockito.when(backend.isAlive()).thenReturn(true);
        Mockito.when(systemInfo.getBackend(Mockito.anyLong())).thenReturn(backend);
        Mockito.when(systemInfo.checkExceedDiskCapacityLimit(Mockito.any(), Mockito.anyBoolean()))
                .thenReturn(org.apache.doris.common.Status.OK);

        // the new replicas of the staging table of an atomic restore, they are bound to the local ones
        Mockito.when(systemInfo.selectBackendIdsForReplicaCreation(Mockito.any(), Mockito.any(), Mockito.any(),
                Mockito.anyBoolean(), Mockito.anyBoolean())).thenAnswer(inv -> {
                    Map<Tag, List<Long>> beIds = Maps.newHashMap();
                    beIds.put(Tag.DEFAULT_BACKEND_TAG, Lists.newArrayList(CatalogMocker.BACKEND1_ID,
                            CatalogMocker.BACKEND2_ID, CatalogMocker.BACKEND3_ID));
                    return Pair.of(beIds, TStorageMedium.HDD);
                });
        mockedEnvStatic.when(Env::getCurrentInvertedIndex).thenReturn(Mockito.mock(TabletInvertedIndex.class));

        mockedExecutor = Mockito.mockStatic(AgentTaskExecutor.class);
        mockedExecutor.when(() -> AgentTaskExecutor.submit(Mockito.any(AgentBatchTask.class)))
                .thenAnswer(inv -> {
                    submitted.addAll(((AgentBatchTask) inv.getArgument(0)).getAllTasks());
                    return null;
                });

        // CatalogMocker does not set the range of p2, the restore job compares it with the backup meta.
        PartitionInfo partitionInfo = tbl2.getPartitionInfo();
        if (partitionInfo.getItem(CatalogMocker.TEST_PARTITION2_ID) == null) {
            partitionInfo.setItem(CatalogMocker.TEST_PARTITION2_ID, false,
                    partitionInfo.getItem(CatalogMocker.TEST_PARTITION1_ID));
        }
        // p1 and p2 were restored from the backup at V1, are not written since then, and have a size.
        BackupJobInfo infoV1 = selfBackupJobInfo();
        for (Partition part : Lists.newArrayList(p1(), p2())) {
            part.updateVersionForRestore(V1);
            part.setRestoreLineage(lineage(infoV1, part));
            for (MaterializedIndex index : part.getMaterializedIndices(IndexExtState.VISIBLE)) {
                for (Tablet tablet : index.getTablets()) {
                    for (Replica replica : tablet.getReplicas()) {
                        replica.updateVersionForRestore(V1);
                        replica.setDataSize(1000);
                    }
                }
            }
        }
    }

    @AfterEach
    public void tearDown() {
        Config.enable_restore_partition_reuse = origEnable;
        Config.enable_restore_incremental_append = origIncremental;
        Config.enable_restore_atomic_reuse = origAtomic;
        Config.restore_reuse_default_check_level = origLevel;
        Config.restore_reuse_sample_ratio = origRatio;
        Config.restore_reuse_min_partition_bytes = origMinBytes;
        Config.restore_digest_timeout_s = origTimeout;
        Config.restore_reuse_force_full_for_relation = origForceFull;
        Config.restore_reuse_allow_unverified_keep = origUnverified;
        Config.restore_reuse_allow_hidden_state_models = origHidden;
        if (mockedExecutor != null) {
            mockedExecutor.close();
        }
        if (mockedEnvStatic != null) {
            mockedEnvStatic.close();
        }
    }

    // ---------------------------------------------------------------------------------------------
    // helpers
    // ---------------------------------------------------------------------------------------------

    private Partition p1() {
        return tbl2.getPartition(CatalogMocker.TEST_PARTITION1_NAME);
    }

    private Partition p2() {
        return tbl2.getPartition(CatalogMocker.TEST_PARTITION2_NAME);
    }

    // the number of replicas of all tablets of all visible indexes of the partition (one task for each)
    private static int replicasOf(Partition part) {
        int num = 0;
        for (MaterializedIndex index : part.getMaterializedIndices(IndexExtState.VISIBLE)) {
            for (Tablet tablet : index.getTablets()) {
                num += tablet.getReplicas().size();
            }
        }
        return num;
    }

    private static long tabletOf(Partition part) {
        return part.getBaseIndex().getTablets().get(0).getId();
    }

    private static String rootOf(Partition part) {
        return part.getName().equals(CatalogMocker.TEST_PARTITION1_NAME) ? ROOT_P1 : ROOT_P2;
    }

    // A backup of test_tbl2 taken in this cluster at V1 with the logical digest of every tablet.
    private BackupJobInfo selfBackupJobInfo() {
        BackupJobInfo info = new BackupJobInfo();
        info.name = "snapshot_v1";
        info.backupTime = BACKUP_TIME;
        info.dbId = db.getId();
        info.dbName = db.getFullName();
        info.success = true;
        info.srcClusterId = SRC_CLUSTER;
        info.tableCommitSeqMap = Maps.newHashMap();
        info.tableCommitSeqMap.put(tbl2.getId(), COMMIT_SEQ);
        BackupOlapTableInfo tblInfo = new BackupOlapTableInfo();
        tblInfo.id = tbl2.getId();
        for (Partition part : tbl2.getPartitions()) {
            BackupPartitionInfo partInfo = new BackupPartitionInfo();
            partInfo.id = part.getId();
            partInfo.version = V1;
            for (MaterializedIndex index : part.getMaterializedIndices(IndexExtState.VISIBLE)) {
                BackupIndexInfo idxInfo = new BackupIndexInfo();
                idxInfo.id = index.getId();
                idxInfo.schemaHash = tbl2.getSchemaHashByIndexId(index.getId());
                idxInfo.logicalDigests = Maps.newHashMap();
                partInfo.indexes.put(tbl2.getIndexNameById(index.getId()), idxInfo);
                for (Tablet tablet : index.getTablets()) {
                    idxInfo.sortedTabletInfoList.add(new BackupTabletInfo(tablet.getId(), Lists.newArrayList()));
                    idxInfo.logicalDigests.put(tablet.getId(), LogicalDigestInfo.of(1, SIG, rootOf(part)));
                }
            }
            tblInfo.partitions.put(part.getName(), partInfo);
        }
        info.backupOlapTableObjects.put(tbl2.getName(), tblInfo);
        return info;
    }

    private static RestoreLineage lineage(BackupJobInfo info, Partition part) {
        BackupOlapTableInfo tblInfo = info.getOlapTableInfo(CatalogMocker.TEST_TBL2_NAME);
        return RestoreJob.buildRestoreLineage(info, tblInfo, tblInfo.getPartInfo(part.getName()), RESTORE_TIME);
    }

    private BackupMeta backupMeta() {
        OlapTable remoteTbl = tbl2.selectiveCopy(null, IndexExtState.VISIBLE, true);
        return new BackupMeta(Lists.newArrayList(remoteTbl), Lists.<Resource>newArrayList());
    }

    private RestoreJob newJob(BackupJobInfo info, String level) {
        RestoreJob job = new RestoreJob("restore_label", "2024-01-01 00:00:00", db.getId(), db.getFullName(), info,
                false, new ReplicaAllocation((short) 3), 100000, -1, false, false, false, false, false, false,
                false, false, env, Repository.KEEP_ON_LOCAL_REPO_ID, backupMeta());
        job.setReuseCheckLevel(level);
        return job;
    }

    // A non-atomic restore of the backup onto the existing test_tbl2, after checkAndPrepareMeta.
    private RestoreJob prepareJob(String level) {
        RestoreJob job = newJob(selfBackupJobInfo(), level);
        Deencapsulation.invoke(job, "checkAndPrepareMeta");
        Assertions.assertTrue(job.getStatus().ok(), job.getStatus().toString());
        Assertions.assertEquals(RestoreJobState.CREATING, job.getState());
        return job;
    }

    private static com.google.common.collect.Table<Long, Long, Long> versionInfo(RestoreJob job) {
        return Deencapsulation.getField(job, "restoredVersionInfo");
    }

    private static RestoreFileMapping fileMapping(RestoreJob job) {
        return Deencapsulation.getField(job, "fileMapping");
    }

    private static com.google.common.collect.Table<Long, Long, SnapshotInfo> snapshotInfos(RestoreJob job) {
        return Deencapsulation.getField(job, "snapshotInfos");
    }

    private static List<RestoreDigestTask> digestTasks(List<AgentTask> tasks) {
        return tasks.stream().filter(t -> t instanceof RestoreDigestTask).map(t -> (RestoreDigestTask) t)
                .collect(Collectors.toList());
    }

    private static TFinishTaskRequest okReport(String root, String sig, int algo) {
        TFinishTaskRequest request = new TFinishTaskRequest();
        request.setTaskStatus(new TStatus(TStatusCode.OK));
        TLogicalDigest digest = new TLogicalDigest();
        digest.setStatusCode("OK");
        digest.setAlgoVersion(algo);
        digest.setSchemaSig(sig);
        digest.setRoot(root);
        request.setLogicalDigest(digest);
        return request;
    }

    private static TFinishTaskRequest failedReport() {
        TFinishTaskRequest request = new TFinishTaskRequest();
        TStatus status = new TStatus(TStatusCode.INTERNAL_ERROR);
        status.setErrorMsgs(Lists.newArrayList("boom"));
        request.setTaskStatus(status);
        return request;
    }

    // Report every digest task: the digest of a replica is the digest of the backup, unless the partition (and
    // optionally the replica of the backend) is in the map of the faulty ones.
    private void reportAll(RestoreJob job, List<RestoreDigestTask> tasks, Map<Long, TFinishTaskRequest> faulty) {
        for (RestoreDigestTask task : tasks) {
            TFinishTaskRequest report = faulty.get(task.getSignature());
            if (report == null) {
                String root = task.getPartitionId() == p1().getId() ? ROOT_P1 : ROOT_P2;
                report = okReport(root, SIG, 1);
            }
            Assertions.assertTrue(job.finishRestoreDigestTask(task, report));
        }
    }

    private List<RestoreDigestTask> newDigestTasks() {
        List<RestoreDigestTask> tasks = Lists.newArrayList();
        for (AgentTask task : submitted) {
            if (task instanceof RestoreDigestTask) {
                tasks.add((RestoreDigestTask) task);
            }
        }
        submitted.clear();
        return tasks;
    }

    private void toVerifying(RestoreJob job) {
        Deencapsulation.invoke(job, "allReplicasCreated");
        Assertions.assertEquals(RestoreJobState.VERIFYING, job.getState());
    }

    private void waitDigests(RestoreJob job) {
        Deencapsulation.invoke(job, "waitingAllDigestsFinished");
    }

    private static RestoreJob writeAndRead(RestoreJob restoreJob) throws Exception {
        Path path = Files.createTempFile("restoreJob", "tmp");
        try {
            try (DataOutputStream out = new DataOutputStream(Files.newOutputStream(path))) {
                restoreJob.write(out);
            }
            try (DataInputStream in = new DataInputStream(Files.newInputStream(path))) {
                return RestoreJob.read(in);
            }
        } finally {
            Files.delete(path);
        }
    }

    // ---------------------------------------------------------------------------------------------
    // the chain of candidate conditions
    // ---------------------------------------------------------------------------------------------

    private RestoreReuseJudge.Input input(CheckLevel level) {
        BackupJobInfo info = selfBackupJobInfo();
        RestoreReuseJudge.Input in = new RestoreReuseJudge.Input();
        in.atomicRestore = false;
        in.allowLoad = false;
        in.cloudMode = false;
        in.minPartitionBytes = 100;
        in.level = level;
        in.jobInfo = info;
        in.backupTable = info.getOlapTableInfo(CatalogMocker.TEST_TBL2_NAME);
        in.backupPartition = in.backupTable.getPartInfo(CatalogMocker.TEST_PARTITION1_NAME);
        in.srcCommitSeq = COMMIT_SEQ;
        in.localTable = tbl2;
        in.localPartition = p1();
        tbl2.setState(OlapTableState.RESTORE);
        return in;
    }

    @Test
    public void testCandidateConditions() {
        Assertions.assertNull(RestoreReuseJudge.firstReject(input(CheckLevel.FULL)));

        RestoreReuseJudge.Input in = input(CheckLevel.FULL);
        in.atomicRestore = true;
        Assertions.assertEquals("ATOMIC_RESTORE", RestoreReuseJudge.firstReject(in));

        in = input(CheckLevel.FULL);
        in.allowLoad = true;
        Assertions.assertEquals("ALLOW_LOAD", RestoreReuseJudge.firstReject(in));

        in = input(CheckLevel.FULL);
        tbl2.setState(OlapTableState.NORMAL);
        Assertions.assertEquals("TABLE_STATE_NORMAL", RestoreReuseJudge.firstReject(in));
        tbl2.setState(OlapTableState.RESTORE_WITH_LOAD);
        Assertions.assertEquals("TABLE_STATE_RESTORE_WITH_LOAD", RestoreReuseJudge.firstReject(in));

        // L0: no lineage, or a lineage of another source.
        in = input(CheckLevel.FULL);
        p1().setRestoreLineage(null);
        Assertions.assertEquals("L0_NO_LINEAGE", RestoreReuseJudge.firstReject(in));
        p1().setRestoreLineage(new RestoreLineage(7, 7, 7, V1, COMMIT_SEQ, 1, 1));
        Assertions.assertEquals("L0_LINEAGE_MISMATCH", RestoreReuseJudge.firstReject(in));
        // the local partition was written
        p1().setRestoreLineage(lineage(in.jobInfo, p1()));
        p1().updateVersionForRestore(V1 + 1);
        Assertions.assertEquals("L0_LOCAL_VERSION_CHANGED", RestoreReuseJudge.firstReject(in));
        p1().updateVersionForRestore(V1);

        // unsupported cases
        in = input(CheckLevel.FULL);
        in.cloudMode = true;
        Assertions.assertEquals("CLOUD_MODE", RestoreReuseJudge.firstReject(in));
        Deencapsulation.setField(tbl2, "keysType", KeysType.AGG_KEYS);
        in = input(CheckLevel.FULL);
        Assertions.assertEquals("AGGREGATE_TABLE", RestoreReuseJudge.firstReject(in));
        Deencapsulation.setField(tbl2, "keysType", KeysType.DUP_KEYS);
        Tablet tablet = p1().getBaseIndex().getTablets().get(0);
        Replica replica = tablet.getReplicas().get(0);
        tablet.setCooldownConf(replica.getId(), 1);
        replica.setRemoteDataSize(100);
        Assertions.assertEquals("REMOTE_STORAGE", RestoreReuseJudge.firstReject(input(CheckLevel.FULL)));
        replica.setRemoteDataSize(0);

        // a tablet of the backup has no digest, except for the check level off
        in = input(CheckLevel.FULL);
        BackupIndexInfo idx = in.backupPartition.indexes.values().iterator().next();
        idx.logicalDigests.put(tabletOf(p1()), LogicalDigestInfo.none(LogicalDigestInfo.REASON_NOT_SUPPORTED));
        Assertions.assertEquals("NO_DIGEST", RestoreReuseJudge.firstReject(in));
        in.level = CheckLevel.OFF;
        Assertions.assertNull(RestoreReuseJudge.firstReject(in));
        in.level = CheckLevel.SAMPLE;
        idx.logicalDigests = null;
        Assertions.assertEquals("NO_DIGEST", RestoreReuseJudge.firstReject(in));

        // too small
        in = input(CheckLevel.FULL);
        long size = in.getSingleReplicaBytes();
        Assertions.assertTrue(size > 0);
        in.minPartitionBytes = size + 1;
        Assertions.assertEquals("TOO_SMALL", RestoreReuseJudge.firstReject(in));
        in.minPartitionBytes = size;
        Assertions.assertNull(RestoreReuseJudge.firstReject(in));
    }


    // ---------------------------------------------------------------------------------------------
    // the extended L0: reverse (b) and table level (c)
    // ---------------------------------------------------------------------------------------------

    // The version of the partition and of all its replicas, as after the increments of a synchronization.
    private static void setVersion(Partition part, long version) {
        part.updateVersionForRestore(version);
        for (MaterializedIndex index : part.getMaterializedIndices(IndexExtState.VISIBLE)) {
            for (Tablet tablet : index.getTablets()) {
                for (Replica replica : tablet.getReplicas()) {
                    replica.updateVersionForRestore(version);
                }
            }
        }
    }

    private RestoreLineage lineageToLocal(Partition part, long version) {
        return new RestoreLineage(db.getId(), tbl2.getId(), part.getId(), version, COMMIT_SEQ, BACKUP_TIME, 1,
                LOCAL_CLUSTER);
    }

    // The input of a partition that fails the forward check: the local partition has no lineage.
    private RestoreReuseJudge.Input inputWithoutForward(CheckLevel level) {
        RestoreReuseJudge.Input in = input(level);
        p1().setRestoreLineage(null);
        in.backupOlapTable = tbl2.selectiveCopy(null, IndexExtState.VISIBLE, true);
        return in;
    }

    @Test
    public void testReverseL0() {
        // forward only: no lineage on the local partition
        RestoreReuseJudge.Input in = inputWithoutForward(CheckLevel.FULL);
        in.localDbId = db.getId();
        Assertions.assertEquals("L0_NO_LINEAGE", RestoreReuseJudge.firstReject(in));
        Assertions.assertEquals(RestoreReuseShadowStats.L0Verdict.NO_BACKUP_LINEAGE,
                RestoreReuseJudge.REVERSE_L0.check(in));

        // the source partition in the backup was restored from this local partition, at the same version
        in.backupLineage = lineageToLocal(p1(), 2);
        Assertions.assertNull(RestoreReuseJudge.firstReject(in));
        Assertions.assertEquals(RestoreReuseJudge.L0_REVERSE, in.l0Path);
        in.level = CheckLevel.SAMPLE;
        Assertions.assertNull(RestoreReuseJudge.firstReject(in));
        Assertions.assertEquals(RestoreReuseJudge.L0_REVERSE, in.l0Path);

        // the ids must all be the local ones
        for (RestoreLineage other : Lists.newArrayList(
                new RestoreLineage(db.getId() + 1, tbl2.getId(), p1().getId(), V1, COMMIT_SEQ, 1, 1),
                new RestoreLineage(db.getId(), tbl2.getId() + 1, p1().getId(), V1, COMMIT_SEQ, 1, 1),
                new RestoreLineage(db.getId(), tbl2.getId(), p2().getId(), V1, COMMIT_SEQ, 1, 1))) {
            in.backupLineage = other;
            Assertions.assertEquals(RestoreReuseShadowStats.L0Verdict.LINEAGE_MISMATCH,
                    RestoreReuseJudge.REVERSE_L0.check(in));
            Assertions.assertEquals("L0_NO_LINEAGE", RestoreReuseJudge.firstReject(in));
            Assertions.assertNull(in.l0Path);
        }

        // the versions must be equal: the local partition was written (the lineage version of the backup is not
        // looked at, the digest is the proof)
        in.backupLineage = lineageToLocal(p1(), V1);
        p1().updateVersionForRestore(V1 + 1);
        Assertions.assertEquals(RestoreReuseShadowStats.L0Verdict.VERSION_MISMATCH,
                RestoreReuseJudge.REVERSE_L0.check(in));
        Assertions.assertNotNull(RestoreReuseJudge.firstReject(in));
        in.backupPartition.version = V1 + 1;
        Assertions.assertEquals(RestoreReuseShadowStats.L0Verdict.REUSABLE, RestoreReuseJudge.REVERSE_L0.check(in));
        p1().updateVersionForRestore(V1);

        // not allowed without a digest
        in.backupPartition.version = V1;
        in.level = CheckLevel.OFF;
        Assertions.assertEquals(RestoreReuseShadowStats.L0Verdict.LEVEL_NOT_ALLOWED,
                RestoreReuseJudge.REVERSE_L0.check(in));
        Assertions.assertEquals("L0_NO_LINEAGE", RestoreReuseJudge.firstReject(in));
        in.level = CheckLevel.DISABLE;
        Assertions.assertNotNull(RestoreReuseJudge.firstReject(in));
    }

    @Test
    public void testTableLevelL0() {
        RestoreReuseJudge.Input in = inputWithoutForward(CheckLevel.FULL);
        in.localDbId = db.getId();
        // no relation
        Assertions.assertEquals(RestoreReuseShadowStats.L0Verdict.NO_TABLE_RELATION,
                RestoreReuseJudge.TABLE_L0.check(in));
        Assertions.assertEquals("L0_NO_LINEAGE", RestoreReuseJudge.firstReject(in));

        // forward: the local table was restored from the source table of the backup (the local lineage is stale,
        // the increments of a synchronization moved the versions of both)
        tbl2.setRestoreSource(new RestoreSource(in.jobInfo.dbId, in.backupTable.id));
        p1().setRestoreLineage(lineage(in.jobInfo, p1()));
        setVersion(p1(), V1 + 3);
        in.backupPartition.version = V1 + 3;
        Assertions.assertEquals(RestoreReuseShadowStats.L0Verdict.LOCAL_VERSION_CHANGED,
                RestoreReuseJudge.FORWARD_L0.check(in));
        Assertions.assertNull(RestoreReuseJudge.firstReject(in));
        Assertions.assertEquals(RestoreReuseJudge.L0_TABLE, in.l0Path);
        // another source table
        tbl2.setRestoreSource(new RestoreSource(in.jobInfo.dbId, in.backupTable.id + 1));
        Assertions.assertEquals(RestoreReuseShadowStats.L0Verdict.NO_TABLE_RELATION,
                RestoreReuseJudge.TABLE_L0.check(in));
        tbl2.setRestoreSource(new RestoreSource(in.jobInfo.dbId + 1, in.backupTable.id));
        Assertions.assertEquals(RestoreReuseShadowStats.L0Verdict.NO_TABLE_RELATION,
                RestoreReuseJudge.TABLE_L0.check(in));
        tbl2.setRestoreSource(new RestoreSource(in.jobInfo.dbId, in.backupTable.id));

        // reverse: the source table in the backup was restored from the local table
        tbl2.setRestoreSource(null);
        in.backupTableSource = new RestoreSource(db.getId(), tbl2.getId());
        Assertions.assertNull(RestoreReuseJudge.firstReject(in));
        Assertions.assertEquals(RestoreReuseJudge.L0_TABLE, in.l0Path);
        in.backupTableSource = new RestoreSource(db.getId(), tbl2.getId() + 1);
        Assertions.assertEquals(RestoreReuseShadowStats.L0Verdict.NO_TABLE_RELATION,
                RestoreReuseJudge.TABLE_L0.check(in));
        in.backupTableSource = new RestoreSource(db.getId() + 1, tbl2.getId());
        Assertions.assertEquals(RestoreReuseShadowStats.L0Verdict.NO_TABLE_RELATION,
                RestoreReuseJudge.TABLE_L0.check(in));
        in.backupTableSource = new RestoreSource(db.getId(), tbl2.getId());

        // the versions differ
        in.backupPartition.version = V1 + 2;
        Assertions.assertEquals(RestoreReuseShadowStats.L0Verdict.VERSION_MISMATCH,
                RestoreReuseJudge.TABLE_L0.check(in));
        Assertions.assertNotNull(RestoreReuseJudge.firstReject(in));
        in.backupPartition.version = V1 + 3;

        // the range differs
        PartitionInfo backupInfo = in.backupOlapTable.getPartitionInfo();
        long backupPartId = in.backupOlapTable.getPartition(p1().getName()).getId();
        backupInfo.setItem(backupPartId, false, backupInfo.getItem(CatalogMocker.TEST_PARTITION2_ID));
        PartitionInfo localInfo = tbl2.getPartitionInfo();
        PartitionItem localItem = localInfo.getItem(p1().getId());
        Assertions.assertEquals(RestoreReuseShadowStats.L0Verdict.REUSABLE, RestoreReuseJudge.TABLE_L0.check(in));
        backupInfo.setItem(backupPartId, false, org.apache.doris.catalog.RangePartitionItem.DUMMY_ITEM);
        Assertions.assertNotEquals(localItem, backupInfo.getItem(backupPartId));
        Assertions.assertEquals(RestoreReuseShadowStats.L0Verdict.PARTITION_MISMATCH,
                RestoreReuseJudge.TABLE_L0.check(in));
        backupInfo.setItem(backupPartId, false, localItem);
        Assertions.assertEquals(RestoreReuseShadowStats.L0Verdict.REUSABLE, RestoreReuseJudge.TABLE_L0.check(in));
        // the range of another partition type
        PartitionInfo other = Mockito.mock(PartitionInfo.class);
        Mockito.when(other.getType()).thenReturn(org.apache.doris.catalog.PartitionType.LIST);
        Deencapsulation.setField(in.backupOlapTable, "partitionInfo", other);
        Assertions.assertEquals(RestoreReuseShadowStats.L0Verdict.PARTITION_MISMATCH,
                RestoreReuseJudge.TABLE_L0.check(in));
        Deencapsulation.setField(in.backupOlapTable, "partitionInfo", backupInfo);
        // the partition name differs: no such partition in the backup table
        Map<String, Partition> nameToPartition = Deencapsulation.getField(in.backupOlapTable, "nameToPartition");
        Partition removed = nameToPartition.remove(p1().getName());
        Assertions.assertEquals(RestoreReuseShadowStats.L0Verdict.PARTITION_MISMATCH,
                RestoreReuseJudge.TABLE_L0.check(in));
        nameToPartition.put(p1().getName(), removed);
        // unknown backup table
        in.backupOlapTable = null;
        Assertions.assertEquals(RestoreReuseShadowStats.L0Verdict.PARTITION_MISMATCH,
                RestoreReuseJudge.TABLE_L0.check(in));
    }

    @Test
    public void testOffLevelOnlyAcceptsForward() {
        RestoreReuseJudge.Input in = inputWithoutForward(CheckLevel.OFF);
        in.localDbId = db.getId();
        in.backupLineage = lineageToLocal(p1(), V1);
        tbl2.setRestoreSource(new RestoreSource(in.jobInfo.dbId, in.backupTable.id));
        // b and c both hold, but off judges by a only
        Assertions.assertEquals("L0_NO_LINEAGE", RestoreReuseJudge.firstReject(in));
        for (CheckLevel level : new CheckLevel[] {CheckLevel.SAMPLE, CheckLevel.FULL}) {
            in.level = level;
            Assertions.assertNull(RestoreReuseJudge.firstReject(in));
            // the first check that passes: a does not, b does
            Assertions.assertEquals(RestoreReuseJudge.L0_REVERSE, in.l0Path);
        }
        // forward is accepted at off
        p1().setRestoreLineage(lineage(in.jobInfo, p1()));
        in = input(CheckLevel.OFF);
        Assertions.assertNull(RestoreReuseJudge.firstReject(in));
        Assertions.assertEquals(RestoreReuseJudge.L0_FORWARD, in.l0Path);
    }

    // A restore of the backup onto the existing test_tbl2, where the local partitions have no lineage, and the
    // partitions in the backup meta carry the lineage that is set by the given function.
    private RestoreJob prepareJobWithBackupStamps(String level,
            java.util.function.Consumer<OlapTable> backupTableStamps) {
        return prepareJobWithBackupStamps(level, backupTableStamps, false);
    }

    private RestoreJob prepareJobWithBackupStamps(String level,
            java.util.function.Consumer<OlapTable> backupTableStamps, boolean keepP1Forward) {
        BackupJobInfo info = selfBackupJobInfo();
        OlapTable remoteTbl = tbl2.selectiveCopy(null, IndexExtState.VISIBLE, true);
        backupTableStamps.accept(remoteTbl);
        BackupMeta meta = new BackupMeta(Lists.newArrayList(remoteTbl), Lists.<Resource>newArrayList());
        RestoreJob job = new RestoreJob("restore_label", "2024-01-01 00:00:00", db.getId(), db.getFullName(), info,
                false, new ReplicaAllocation((short) 3), 100000, -1, false, false, false, false, false, false,
                false, false, env, Repository.KEEP_ON_LOCAL_REPO_ID, meta);
        job.setReuseCheckLevel(level);
        if (!keepP1Forward) {
            p1().setRestoreLineage(null);
        }
        p2().setRestoreLineage(null);
        Deencapsulation.invoke(job, "checkAndPrepareMeta");
        Assertions.assertTrue(job.getStatus().ok(), job.getStatus().toString());
        return job;
    }

    @Test
    public void testReverseKeepsAndIsObservable() {
        // the backup comes from a table that was restored from this local table
        RestoreJob job = prepareJobWithBackupStamps("full", remote -> {
            for (Partition part : remote.getAllPartitions()) {
                part.setRestoreLineage(lineageToLocal(part, V1));
            }
        });
        // the estimate counts the reverse path
        RestoreReuseShadowStats estimate = job.getReuseShadowStats();
        Assertions.assertEquals(2, estimate.getReusable());
        Assertions.assertEquals(0, estimate.getReusableForward());
        Assertions.assertEquals(2, estimate.getReusableReverse());
        Assertions.assertEquals(0, estimate.getReusableTable());
        Assertions.assertEquals(0, estimate.getNoLineage());

        toVerifying(job);
        reportAll(job, newDigestTasks(), Maps.newHashMap());
        waitDigests(job);
        RestoreReuseResult result = job.getReuseResult();
        Assertions.assertEquals(2, result.getKeptPartitions());
        Assertions.assertEquals(2, result.getKeptByPath(RestoreReuseJudge.L0_REVERSE));
        Assertions.assertEquals(0, result.getKeptByPath(RestoreReuseJudge.L0_FORWARD));
        Assertions.assertEquals(0, result.getKeptByPath(RestoreReuseJudge.L0_TABLE));
        Assertions.assertTrue(versionInfo(job).isEmpty());
        // all replicas, by the size of each
        long all = RestoreReuseShadowStats.getAllReplicasLocalDataSize(p1())
                + RestoreReuseShadowStats.getAllReplicasLocalDataSize(p2());
        Assertions.assertEquals(all, result.getKeptBytesAllReplicas());
        Assertions.assertTrue(all > result.getKeptBytesSingleReplica());

        // the ReuseEstimate and DownloadStats columns
        List<String> info = job.getInfo(false);
        com.google.gson.JsonObject estimateJson = com.google.gson.JsonParser.parseString(
                info.get(info.size() - 3)).getAsJsonObject();
        Assertions.assertEquals(2, estimateJson.get("kept_partitions").getAsLong());
        Assertions.assertEquals(result.getKeptBytesSingleReplica(),
                estimateJson.get("kept_bytes_single_replica").getAsLong());
        Assertions.assertEquals(2, estimateJson.get("kept_b").getAsLong());
        Assertions.assertEquals(0, estimateJson.get("kept_a").getAsLong());
        Assertions.assertEquals(2, estimateJson.get("reusable_b").getAsLong());
        Assertions.assertEquals(0, estimateJson.get("download_digest_mismatch").getAsLong());
        com.google.gson.JsonObject downloadJson = com.google.gson.JsonParser.parseString(
                info.get(info.size() - 1)).getAsJsonObject();
        Assertions.assertEquals(all, downloadJson.get("kept_bytes").getAsLong());
        Assertions.assertEquals(1.0, downloadJson.get("reuse_ratio").getAsDouble(), 0.0001);
    }

    @Test
    public void testTableLevelKeepsAfterIncrementsAndCountsDownloads() {
        // the table is a replica of the backup's source table, and both moved on by the same increments
        tbl2.setRestoreSource(new RestoreSource(db.getId(), tbl2.getId()));
        RestoreJob job = prepareJobWithBackupStamps("full", remote -> {
            for (Partition part : remote.getAllPartitions()) {
                part.setRestoreLineage(null);
            }
        });
        // both partitions are candidates by c, with the lineage that a does not accept
        Assertions.assertEquals(2, job.getReuseShadowStats().getReusableTable());
        toVerifying(job);
        List<RestoreDigestTask> tasks = newDigestTasks();
        // p1 has a different digest
        Map<Long, TFinishTaskRequest> faulty = Maps.newHashMap();
        faulty.put(tasks.stream().filter(t -> t.getPartitionId() == p1().getId()).findFirst().get().getSignature(),
                okReport("ffff", SIG, 1));
        reportAll(job, tasks, faulty);
        waitDigests(job);
        RestoreReuseResult result = job.getReuseResult();
        Assertions.assertEquals(1, result.getKeptPartitions());
        Assertions.assertEquals(1, result.getKeptByPath(RestoreReuseJudge.L0_TABLE));
        List<String> row = job.getInfo(false);
        com.google.gson.JsonObject estimateJson = com.google.gson.JsonParser.parseString(
                row.get(row.size() - 3)).getAsJsonObject();
        Assertions.assertEquals(1, estimateJson.get("kept_partitions").getAsLong());
        Assertions.assertEquals(1, estimateJson.get("kept_c").getAsLong());
        Assertions.assertEquals(1, estimateJson.get("download_digest_mismatch").getAsLong());
        Assertions.assertEquals(0, estimateJson.get("download_timeout").getAsLong());
        Assertions.assertEquals(0, estimateJson.get("download_not_supported").getAsLong());
        Assertions.assertEquals(0, estimateJson.get("download_sample_failed").getAsLong());
        // the partition with the mismatch is downloaded, and its lineage is gone
        Assertions.assertTrue(versionInfo(job).contains(tbl2.getId(), p1().getId()));
        Assertions.assertNull(p1().getRestoreLineage());
    }

    // p1 is a candidate by the forward path (a), p2 by the reverse path (b)
    private RestoreJob prepareMixedJob(String level) {
        return prepareJobWithBackupStamps(level, remote -> {
            remote.getPartition(p2().getName()).setRestoreLineage(lineageToLocal(p2(), V1));
        }, true);
    }

    @Test
    public void testReversePathIsDigestedInSampleLevel() {
        Config.restore_reuse_force_full_for_relation = true;
        Config.restore_reuse_sample_ratio = 0.1;
        RestoreJob job = prepareMixedJob("sample");
        Assertions.assertEquals(1, job.getReuseShadowStats().getReusableForward());
        Assertions.assertEquals(1, job.getReuseShadowStats().getReusableReverse());
        toVerifying(job);
        // the b partition is digested although it is not sampled, and the a partition is the one sampled
        List<RestoreDigestTask> tasks = newDigestTasks();
        Assertions.assertEquals(replicasOf(p1()) + replicasOf(p2()), tasks.size());
        reportAll(job, tasks, Maps.newHashMap());
        waitDigests(job);
        Assertions.assertEquals(RestoreJobState.SNAPSHOTING, job.getState());
        for (RestoreReuseResult.Decision decision : job.getReuseResult().getDecisions()) {
            Assertions.assertTrue(decision.kept);
            Assertions.assertEquals(RestoreReuseResult.KEPT_DIGEST_VERIFIED, decision.reason);
        }
    }

    @Test
    public void testTableLevelPathsAreNotSampled() {
        Config.restore_reuse_force_full_for_relation = true;
        Config.restore_reuse_sample_ratio = 0.1;
        // both partitions are candidates by c, none is sampled
        tbl2.setRestoreSource(new RestoreSource(db.getId(), tbl2.getId()));
        RestoreJob job = prepareJobWithBackupStamps("sample", remote -> {
            for (Partition part : remote.getAllPartitions()) {
                part.setRestoreLineage(null);
            }
        });
        Assertions.assertEquals(2, job.getReuseShadowStats().getReusableTable());
        toVerifying(job);
        List<RestoreDigestTask> tasks = newDigestTasks();
        Assertions.assertEquals(replicasOf(p1()) + replicasOf(p2()), tasks.size());
        reportAll(job, tasks, Maps.newHashMap());
        waitDigests(job);
        for (RestoreReuseResult.Decision decision : job.getReuseResult().getDecisions()) {
            Assertions.assertEquals(RestoreReuseResult.KEPT_DIGEST_VERIFIED, decision.reason);
        }
    }

    @Test
    public void testForwardIsStillSampledWhenRelationIsForced() {
        Config.restore_reuse_force_full_for_relation = true;
        Config.restore_reuse_sample_ratio = 0.1;
        // both are forward: a is sampled, one partition only
        RestoreJob job = prepareJob("sample");
        toVerifying(job);
        Assertions.assertEquals(replicasOf(p1()), newDigestTasks().size());
    }

    @Test
    public void testRelationDigestMismatchDownloads() {
        Config.restore_reuse_force_full_for_relation = true;
        Config.restore_reuse_sample_ratio = 0.1;
        RestoreJob job = prepareMixedJob("sample");
        toVerifying(job);
        List<RestoreDigestTask> tasks = newDigestTasks();
        // the b partition p2 has another digest, p1 (a) is consistent
        Map<Long, TFinishTaskRequest> faulty = Maps.newHashMap();
        faulty.put(tasks.stream().filter(t -> t.getPartitionId() == p2().getId()).findFirst().get().getSignature(),
                okReport("ffff", SIG, 1));
        reportAll(job, tasks, faulty);
        waitDigests(job);
        Assertions.assertEquals(RestoreJobState.SNAPSHOTING, job.getState());
        RestoreReuseResult result = job.getReuseResult();
        Assertions.assertTrue(result.isKept(tbl2.getId(), p1().getId()));
        Assertions.assertFalse(result.isKept(tbl2.getId(), p2().getId()));
        Assertions.assertEquals(RestoreReuseResult.DOWNLOAD_ROOT_MISMATCH, result.getDecisions().stream()
                .filter(d -> d.partitionId == p2().getId()).findFirst().get().reason);
        Assertions.assertTrue(versionInfo(job).contains(tbl2.getId(), p2().getId()));
    }

    @Test
    public void testRelationIsSampledIfNotForced() {
        Config.restore_reuse_force_full_for_relation = false;
        Config.restore_reuse_sample_ratio = 0.1;
        RestoreJob job = prepareMixedJob("sample");
        toVerifying(job);
        // one of the two is sampled, the other is kept by the sample
        List<RestoreDigestTask> tasks = newDigestTasks();
        Assertions.assertEquals(replicasOf(p1()), tasks.size());
        long sampledPart = tasks.get(0).getPartitionId();
        reportAll(job, tasks, Maps.newHashMap());
        waitDigests(job);
        RestoreReuseResult result = job.getReuseResult();
        Assertions.assertEquals(2, result.getKeptPartitions());
        for (RestoreReuseResult.Decision decision : result.getDecisions()) {
            Assertions.assertEquals(decision.partitionId == sampledPart ? RestoreReuseResult.KEPT_DIGEST_VERIFIED
                    : RestoreReuseResult.KEPT_SAMPLE_PASSED, decision.reason);
        }
    }

    @Test
    public void testOffLevelStillRejectsRelationWhenForced() {
        Config.restore_reuse_force_full_for_relation = true;
        RestoreJob job = prepareMixedJob("off");
        // only the forward partition is a candidate at the level off, and it is kept without any digest
        Deencapsulation.invoke(job, "allReplicasCreated");
        Assertions.assertTrue(newDigestTasks().isEmpty());
        Assertions.assertEquals(RestoreJobState.SNAPSHOTING, job.getState());
        Assertions.assertEquals(1, job.getReuseResult().getKeptByPath(RestoreReuseJudge.L0_FORWARD));
        Assertions.assertEquals(1, job.getReuseResult().getDecisions().size());
    }

    @Test
    public void testNeedsFullDigest() {
        Assertions.assertFalse(RestoreReuseJudge.needsFullDigest(RestoreReuseJudge.L0_FORWARD, true));
        Assertions.assertTrue(RestoreReuseJudge.needsFullDigest(RestoreReuseJudge.L0_REVERSE, true));
        Assertions.assertTrue(RestoreReuseJudge.needsFullDigest(RestoreReuseJudge.L0_TABLE, true));
        Assertions.assertFalse(RestoreReuseJudge.needsFullDigest(RestoreReuseJudge.L0_REVERSE, false));
        Assertions.assertFalse(RestoreReuseJudge.needsFullDigest(null, true));
    }

    @Test
    public void testEstimateAtOffLevelCountsForwardOnly() {
        RestoreJob job = prepareJobWithBackupStamps("off", remote -> {
            for (Partition part : remote.getAllPartitions()) {
                part.setRestoreLineage(lineageToLocal(part, V1));
            }
        });
        Assertions.assertEquals(0, job.getReuseShadowStats().getReusable());
    }

    @Test
    public void testRestoreSourceIsClearedThenWritten() {
        // the backup meta carries the restore source and the lineage of the source cluster's own upstream
        RestoreSource upstream = new RestoreSource(7001, 7002);
        RestoreJob job = prepareJobWithBackupStamps("full", remote -> {
            remote.setRestoreSource(upstream);
            for (Partition part : remote.getAllPartitions()) {
                part.setRestoreLineage(new RestoreLineage(7001, 7002, 7003, V1, COMMIT_SEQ, 1, 1));
            }
        });
        // captured before the clear, which the job did in checkAndPrepareMeta
        OlapTable remote = (OlapTable) ((BackupMeta) Deencapsulation.getField(job, "backupMeta"))
                .getTable(CatalogMocker.TEST_TBL2_NAME);
        Assertions.assertNull(remote.getRestoreSource());
        Assertions.assertTrue(remote.getAllPartitions().stream().allMatch(p -> p.getRestoreLineage() == null));
        Assertions.assertNull(tbl2.getRestoreSource());

        // commit writes the source of this job, never the one of the backup meta
        BackupJobInfo info = Deencapsulation.getField(job, "jobInfo");
        job.stampRestoreLineage(db, 1);
        Assertions.assertEquals(new RestoreSource(info.dbId, tbl2.getId(), SRC_CLUSTER), tbl2.getRestoreSource());
        Assertions.assertNotEquals(upstream, tbl2.getRestoreSource());
        // a second restore overwrites it
        BackupJobInfo other = selfBackupJobInfo();
        other.dbId = 8001;
        Deencapsulation.setField(job, "jobInfo", other);
        job.stampRestoreLineage(db, 2);
        Assertions.assertEquals(new RestoreSource(8001, tbl2.getId(), SRC_CLUSTER), tbl2.getRestoreSource());
    }

    @Test
    public void testRestoreSourcePersists() throws Exception {
        tbl2.setRestoreSource(new RestoreSource(11, 22));
        OlapTable copy = GsonUtils.GSON.fromJson(
                GsonUtils.GSON.toJson(tbl2), OlapTable.class);
        Assertions.assertEquals(new RestoreSource(11, 22), copy.getRestoreSource());
        // the deep copy of a backup carries it too, which is why it is cleared in the backup meta
        OlapTable backupCopy = tbl2.selectiveCopy(null, IndexExtState.VISIBLE, true);
        Assertions.assertEquals(new RestoreSource(11, 22), backupCopy.getRestoreSource());
        // an old table without it
        tbl2.setRestoreSource(null);
        String json = GsonUtils.GSON.toJson(tbl2);
        Assertions.assertFalse(json.contains("\"rsrc\""));
        Assertions.assertNull(GsonUtils.GSON.fromJson(json, OlapTable.class)
                .getRestoreSource());
    }

    @Test
    public void testDownloadStatsReuseRatioWithKeptBytes() {
        RestoreDownloadStats stats = new RestoreDownloadStats();
        Assertions.assertEquals(0.0, stats.getReuseRatio(0), 0.0001);
        Assertions.assertEquals(1.0, stats.getReuseRatio(500), 0.0001);
        org.apache.doris.thrift.TDownloadStats reported = new org.apache.doris.thrift.TDownloadStats();
        reported.setLinkedBytes(100);
        reported.setSkippedBytes(100);
        reported.setDownloadedBytes(300);
        stats.add(reported, 3);
        Assertions.assertEquals(0.4, stats.getReuseRatio(), 0.0001);
        // (100 + 100 + 500) / (100 + 100 + 500 + 300)
        Assertions.assertEquals(0.7, stats.getReuseRatio(500), 0.0001);
        com.google.gson.JsonObject json = com.google.gson.JsonParser.parseString(stats.toJson(3, 500))
                .getAsJsonObject();
        Assertions.assertEquals(500, json.get("kept_bytes").getAsLong());
        Assertions.assertEquals(0.7, json.get("reuse_ratio").getAsDouble(), 0.0001);
        // without partition level reuse nothing changes
        json = com.google.gson.JsonParser.parseString(stats.toJson(3)).getAsJsonObject();
        Assertions.assertEquals(0, json.get("kept_bytes").getAsLong());
        Assertions.assertEquals(0.4, json.get("reuse_ratio").getAsDouble(), 0.0001);
    }

    // The incremental partitions keep their local data (0, V_l]: DownloadStats counts it in incremental_local_bytes
    // and the reuse ratio, by replica.
    private void assertIncrementalLocalBytes(RestoreJob job, RestoreReuseResult result) {
        long local = RestoreReuseShadowStats.getAllReplicasLocalDataSize(p1())
                + RestoreReuseShadowStats.getAllReplicasLocalDataSize(p2());
        Assertions.assertTrue(local > 0);
        Assertions.assertEquals(local, result.getIncrementalLocalBytesAllReplicas());
        Assertions.assertEquals(0, result.getKeptBytesAllReplicas());
        List<String> info = job.getInfo(false);
        com.google.gson.JsonObject json = com.google.gson.JsonParser.parseString(info.get(info.size() - 1))
                .getAsJsonObject();
        Assertions.assertEquals(local, json.get("incremental_local_bytes").getAsLong());
        Assertions.assertEquals(0, json.get("kept_bytes").getAsLong());
        // nothing is downloaded yet: the whole of the data is the local one
        Assertions.assertEquals(1.0, json.get("reuse_ratio").getAsDouble(), 0.0001);
    }

    @Test
    public void testDownloadStatsReuseRatioWithIncrementalLocalBytes() {
        RestoreDownloadStats stats = new RestoreDownloadStats();
        org.apache.doris.thrift.TDownloadStats reported = new org.apache.doris.thrift.TDownloadStats();
        reported.setLinkedBytes(100);
        reported.setSkippedBytes(100);
        reported.setDownloadedBytes(300);
        stats.add(reported, 3);
        // (100 + 100 + 500 + 400) / (100 + 100 + 500 + 400 + 300)
        Assertions.assertEquals(0.786, stats.getReuseRatio(500, 400), 0.0001);
        Assertions.assertEquals(0.7, stats.getReuseRatio(500), 0.0001);
        com.google.gson.JsonObject json = com.google.gson.JsonParser.parseString(stats.toJson(3, 500, 0, 400))
                .getAsJsonObject();
        Assertions.assertEquals(500, json.get("kept_bytes").getAsLong());
        Assertions.assertEquals(400, json.get("incremental_local_bytes").getAsLong());
        Assertions.assertEquals(0.786, json.get("reuse_ratio").getAsDouble(), 0.0001);
        // only incremental local data and the increment: 400 / (400 + 300)
        stats = new RestoreDownloadStats();
        stats.add(new org.apache.doris.thrift.TDownloadStats().setDownloadedBytes(300), 3);
        json = com.google.gson.JsonParser.parseString(stats.toJson(3, 0, 0, 400)).getAsJsonObject();
        Assertions.assertEquals(0.571, json.get("reuse_ratio").getAsDouble(), 0.0001);
    }

    @Test
    public void testReplicaMustBeHealthyAtTheBackupVersion() {
        Replica replica = p1().getBaseIndex().getTablets().get(0).getReplicas().get(0);
        RestoreReuseJudge.Input in = input(CheckLevel.FULL);
        replica.updateVersionForRestore(V1 - 1);
        Assertions.assertEquals("REPLICA_UNHEALTHY", RestoreReuseJudge.firstReject(in));
        replica.updateVersionForRestore(V1);
        replica.setState(ReplicaState.DECOMMISSION);
        Assertions.assertEquals("REPLICA_UNHEALTHY", RestoreReuseJudge.firstReject(in));
        replica.setState(ReplicaState.NORMAL);
        replica.updateLastFailedVersion(V1 + 1);
        Assertions.assertEquals("REPLICA_UNHEALTHY", RestoreReuseJudge.firstReject(in));
    }

    @Test
    public void testConditionChainIsExtensible() {
        // A new way to pass L0 only needs to be added to the list: a forward, b reverse, c table level.
        Assertions.assertEquals(3, RestoreReuseJudge.L0_CHECKS.size());
        Assertions.assertEquals(Lists.newArrayList("a", "b", "c"), RestoreReuseJudge.L0_CHECKS.stream()
                .map(c -> c.name).collect(Collectors.toList()));
        Assertions.assertEquals(6, RestoreReuseJudge.CONDITIONS.size());
    }

    // ---------------------------------------------------------------------------------------------
    // levels, sampling, comparison
    // ---------------------------------------------------------------------------------------------

    @Test
    public void testConfigDefaults() {
        // as captured before setUp changed them
        Assertions.assertEquals("full", origLevel);
        Assertions.assertFalse(origUnverified);
        Assertions.assertFalse(origHidden);
    }

    @Test
    public void testEffectiveLevelWithoutUnverifiedKeep() {
        Config.restore_reuse_allow_unverified_keep = false;
        for (KeysType keysType : new KeysType[] {KeysType.DUP_KEYS, KeysType.UNIQUE_KEYS}) {
            Assertions.assertEquals(CheckLevel.FULL, RestoreReuseJudge.levelOfTable(CheckLevel.OFF, keysType));
            Assertions.assertEquals(CheckLevel.FULL, RestoreReuseJudge.levelOfTable(CheckLevel.SAMPLE, keysType));
            Assertions.assertEquals(CheckLevel.FULL, RestoreReuseJudge.levelOfTable(CheckLevel.FULL, keysType));
            Assertions.assertEquals(CheckLevel.DISABLE, RestoreReuseJudge.levelOfTable(CheckLevel.DISABLE, keysType));
        }
        // lifted: as requested, except that a unique table does not allow off
        Config.restore_reuse_allow_unverified_keep = true;
        Assertions.assertEquals(CheckLevel.OFF, RestoreReuseJudge.levelOfTable(CheckLevel.OFF, KeysType.DUP_KEYS));
        Assertions.assertEquals(CheckLevel.SAMPLE,
                RestoreReuseJudge.levelOfTable(CheckLevel.SAMPLE, KeysType.DUP_KEYS));
        Assertions.assertEquals(CheckLevel.DISABLE,
                RestoreReuseJudge.levelOfTable(CheckLevel.DISABLE, KeysType.DUP_KEYS));
    }

    private void assertTakenAsFull(String requested) {
        Config.restore_reuse_allow_unverified_keep = false;
        Config.restore_reuse_sample_ratio = 0.1;
        RestoreJob job = prepareJob(requested);
        // every partition is digested, none is kept on the lineage or the sample
        toVerifying(job);
        List<RestoreDigestTask> tasks = newDigestTasks();
        Assertions.assertEquals(replicasOf(p1()) + replicasOf(p2()), tasks.size(), requested);
        reportAll(job, tasks, Maps.newHashMap());
        waitDigests(job);
        RestoreReuseResult result = job.getReuseResult();
        Assertions.assertEquals(2, result.getKeptPartitions(), requested);
        for (RestoreReuseResult.Decision decision : result.getDecisions()) {
            Assertions.assertEquals(RestoreReuseResult.KEPT_DIGEST_VERIFIED, decision.reason);
            Assertions.assertEquals("full", decision.level);
            Assertions.assertEquals(requested, decision.requestedLevel);
            Assertions.assertFalse(decision.isUnverifiedKeep());
        }
        // SHOW RESTORE: the requested and the effective level
        List<String> info = job.getInfo(false);
        com.google.gson.JsonObject estimate = com.google.gson.JsonParser.parseString(
                info.get(info.size() - 3)).getAsJsonObject();
        Assertions.assertEquals(requested, estimate.getAsJsonObject("check_level").get("requested").getAsString());
        Assertions.assertEquals("full", estimate.getAsJsonObject("check_level").get("effective").getAsString());
        Assertions.assertEquals(0, estimate.getAsJsonObject("check_level").get("kept_unverified").getAsLong());
        com.google.gson.JsonObject download = com.google.gson.JsonParser.parseString(
                info.get(info.size() - 1)).getAsJsonObject();
        Assertions.assertEquals("full", download.getAsJsonObject("check_level").get("effective").getAsString());
        Assertions.assertEquals(requested, download.getAsJsonObject("check_level").get("requested").getAsString());
        // a decision persisted by the older version has no requested level: same as the level
        result.getDecisions().forEach(d -> d.requestedLevel = null);
        Assertions.assertEquals("full", result.levelSummary().get("requested"));
    }

    private void assertAtomicTakenAsFull(String requested) {
        // the two new switches are false: the partitions of an atomic restore are all digested, whatever is requested
        Config.restore_reuse_allow_unverified_keep = false;
        Config.restore_reuse_allow_hidden_state_models = false;
        Config.restore_reuse_sample_ratio = 0.1;
        RestoreJob job = prepareAtomicJob(selfBackupJobInfo(), requested);
        toVerifying(job);
        List<RestoreDigestTask> tasks = newDigestTasks();
        Assertions.assertEquals(replicasOf(p1()) + replicasOf(p2()), tasks.size(), requested);
        reportAll(job, tasks, Maps.newHashMap());
        waitDigests(job);
        RestoreReuseResult result = job.getReuseResult();
        Assertions.assertEquals(2, result.getKeptAtomicPartitions(), requested);
        for (RestoreReuseResult.Decision decision : result.getDecisions()) {
            Assertions.assertTrue(decision.atomic);
            Assertions.assertEquals(RestoreReuseResult.KEPT_DIGEST_VERIFIED, decision.reason);
            Assertions.assertEquals("full", decision.level);
            Assertions.assertEquals(requested, decision.requestedLevel);
        }
    }

    @Test
    public void testAtomicJobOffIsTakenAsFullByDefault() {
        assertAtomicTakenAsFull("off");
    }

    @Test
    public void testAtomicJobSampleIsTakenAsFullByDefault() {
        assertAtomicTakenAsFull("sample");
    }

    @Test
    public void testIncrementalCandidatesAreDigestedByDefault() {
        Config.restore_reuse_allow_unverified_keep = false;
        Config.restore_reuse_allow_hidden_state_models = false;
        Config.restore_reuse_sample_ratio = 0.1;
        RestoreJob job = prepareIncrementalJob("off");
        toVerifying(job);
        List<RestoreDigestTask> tasks = newDigestTasks();
        // both partitions are incremental candidates, none is skipped by the level
        Assertions.assertEquals(replicasOf(p1()) + replicasOf(p2()), tasks.size());
        reportPrefix(job, tasks, Maps.newHashMap());
        waitDigests(job);
        RestoreReuseResult result = job.getReuseResult();
        Assertions.assertEquals(2, result.getIncrementalPartitions());
        for (RestoreReuseResult.Decision decision : result.getDecisions()) {
            Assertions.assertTrue(decision.incremental);
            Assertions.assertEquals("full", decision.level);
            Assertions.assertEquals("off", decision.requestedLevel);
        }
    }

    @Test
    public void testSampleTakenAsFullByTheJob() {
        assertTakenAsFull("sample");
    }

    @Test
    public void testOffTakenAsFullByTheJob() {
        assertTakenAsFull("off");
    }

    @Test
    public void testFullStaysFullByTheJob() {
        assertTakenAsFull("full");
    }

    @Test
    public void testDefaultLevelIsFullByTheJob() {
        // no property: the default level of the config, full
        Config.restore_reuse_allow_unverified_keep = false;
        Config.restore_reuse_default_check_level = origLevel;
        RestoreJob job = prepareJob(null);
        toVerifying(job);
        Assertions.assertEquals(replicasOf(p1()) + replicasOf(p2()), newDigestTasks().size());
    }

    @Test
    public void testUnverifiedKeepWhenAllowed() {
        Config.restore_reuse_allow_unverified_keep = true;
        // off: kept on the lineage stamp, the cluster id of the source is equal on both sides
        RestoreJob job = prepareJob("off");
        Deencapsulation.invoke(job, "allReplicasCreated");
        RestoreReuseResult result = job.getReuseResult();
        Assertions.assertEquals(2, result.getKeptPartitions());
        for (RestoreReuseResult.Decision decision : result.getDecisions()) {
            Assertions.assertEquals(RestoreReuseResult.KEPT_L0_ONLY, decision.reason);
            Assertions.assertEquals("off", decision.level);
            Assertions.assertEquals("off", decision.requestedLevel);
            Assertions.assertTrue(decision.isUnverifiedKeep());
            Assertions.assertTrue(decision.toString().contains("UNPROVEN"), decision.toString());
        }
        Assertions.assertEquals(2, result.getKeptUnverifiedPartitions());
        Assertions.assertEquals("2", String.valueOf(result.levelSummary().get("kept_unverified")));
    }

    @Test
    public void testSampleKeepsTheRestUnprovenWhenAllowed() {
        Config.restore_reuse_allow_unverified_keep = true;
        Config.restore_reuse_force_full_for_relation = true;
        Config.restore_reuse_sample_ratio = 0.1;
        RestoreJob job = prepareJob("sample");
        toVerifying(job);
        reportAll(job, newDigestTasks(), Maps.newHashMap());
        waitDigests(job);
        RestoreReuseResult result = job.getReuseResult();
        Assertions.assertEquals(2, result.getKeptPartitions());
        Assertions.assertEquals(1, result.getKeptUnverifiedPartitions());
        Assertions.assertEquals(1L, result.getDecisions().stream()
                .filter(d -> RestoreReuseResult.KEPT_SAMPLE_PASSED.equals(d.reason)).count());
    }

    @Test
    public void testOffWithoutClusterIdIsVerifiedByTheDigest() {
        Config.restore_reuse_allow_unverified_keep = true;
        // the local stamps were written before the cluster id was recorded
        for (Partition part : Lists.newArrayList(p1(), p2())) {
            RestoreLineage l = part.getRestoreLineage();
            part.setRestoreLineage(new RestoreLineage(l.getSrcDbId(), l.getSrcTableId(), l.getSrcPartitionId(),
                    l.getSrcVersion(), l.getSrcCommitSeq(), l.getBackupTime(), l.getRestoreTime()));
        }
        RestoreJob job = prepareJob("off");
        // not kept on the stamp: candidates for the digest, whose level is raised to full
        Assertions.assertTrue(job.getReuseResult().getDecisions().isEmpty());
        toVerifying(job);
        List<RestoreDigestTask> tasks = newDigestTasks();
        Assertions.assertEquals(replicasOf(p1()) + replicasOf(p2()), tasks.size());
        reportAll(job, tasks, Maps.newHashMap());
        waitDigests(job);
        for (RestoreReuseResult.Decision decision : job.getReuseResult().getDecisions()) {
            Assertions.assertEquals(RestoreReuseResult.KEPT_DIGEST_VERIFIED, decision.reason);
            Assertions.assertEquals("full", decision.level);
            Assertions.assertEquals("off", decision.requestedLevel);
        }
    }

    @Test
    public void testOffWithOldBackupIsVerifiedByTheDigest() {
        Config.restore_reuse_allow_unverified_keep = true;
        BackupJobInfo info = selfBackupJobInfo();
        info.srcClusterId = null;
        RestoreJob job = newJob(info, "off");
        Deencapsulation.invoke(job, "checkAndPrepareMeta");
        Assertions.assertTrue(job.getStatus().ok(), job.getStatus().toString());
        Assertions.assertTrue(job.getReuseResult().getDecisions().isEmpty());
        toVerifying(job);
        Assertions.assertEquals(replicasOf(p1()) + replicasOf(p2()), newDigestTasks().size());
    }

    @Test
    public void testClusterIdForward() {
        RestoreReuseJudge.Input in = input(CheckLevel.FULL);
        Assertions.assertNull(RestoreReuseJudge.firstReject(in));
        Assertions.assertTrue(in.clusterProven);
        // another cluster: the ids of the db, table and partition may coincide
        in.jobInfo.srcClusterId = SRC_CLUSTER + 1;
        Assertions.assertEquals(RestoreReuseShadowStats.L0Verdict.CLUSTER_MISMATCH,
                RestoreReuseJudge.FORWARD_L0.check(in));
        Assertions.assertEquals("L0_CLUSTER_MISMATCH", RestoreReuseJudge.firstReject(in));
        in.relationOnly = true;
        Assertions.assertEquals(RestoreReuseShadowStats.L0Verdict.CLUSTER_MISMATCH,
                RestoreReuseJudge.FORWARD_L0.check(in));
        in.relationOnly = false;
        // an old backup: still a candidate, but the cluster is not proved
        in.jobInfo.srcClusterId = null;
        Assertions.assertNull(RestoreReuseJudge.firstReject(in));
        Assertions.assertFalse(in.clusterProven);
        // an old stamp
        in = input(CheckLevel.FULL);
        RestoreLineage l = p1().getRestoreLineage();
        p1().setRestoreLineage(new RestoreLineage(l.getSrcDbId(), l.getSrcTableId(), l.getSrcPartitionId(),
                l.getSrcVersion(), l.getSrcCommitSeq(), l.getBackupTime(), l.getRestoreTime()));
        Assertions.assertNull(RestoreReuseJudge.firstReject(in));
        Assertions.assertFalse(in.clusterProven);
        // both are old
        in.jobInfo.srcClusterId = null;
        Assertions.assertNull(RestoreReuseJudge.firstReject(in));
        Assertions.assertFalse(in.clusterProven);
        // the estimate counts the mismatch
        RestoreReuseShadowStats stats = new RestoreReuseShadowStats();
        stats.add(RestoreReuseShadowStats.L0Verdict.CLUSTER_MISMATCH, RestoreReuseShadowStats.Unsupported.NONE, 1);
        Assertions.assertEquals(1, stats.getClusterMismatch());
        Assertions.assertEquals(0, stats.getReusable());
    }

    @Test
    public void testClusterIdReverse() {
        RestoreReuseJudge.Input in = inputWithoutForward(CheckLevel.FULL);
        in.localDbId = db.getId();
        in.localClusterId = LOCAL_CLUSTER;
        in.backupLineage = lineageToLocal(p1(), V1);
        Assertions.assertEquals(RestoreReuseShadowStats.L0Verdict.REUSABLE, RestoreReuseJudge.REVERSE_L0.check(in));
        Assertions.assertTrue(RestoreReuseJudge.clusterProven(in, RestoreReuseJudge.L0_REVERSE));
        // the backup table was restored from a table of another cluster with the same ids
        in.backupLineage = new RestoreLineage(db.getId(), tbl2.getId(), p1().getId(), V1, COMMIT_SEQ, BACKUP_TIME, 1,
                LOCAL_CLUSTER + 1);
        Assertions.assertEquals(RestoreReuseShadowStats.L0Verdict.CLUSTER_MISMATCH,
                RestoreReuseJudge.REVERSE_L0.check(in));
        Assertions.assertNotNull(RestoreReuseJudge.firstReject(in));
        // an old stamp in the backup: a candidate, not proved
        in.backupLineage = new RestoreLineage(db.getId(), tbl2.getId(), p1().getId(), V1, COMMIT_SEQ, BACKUP_TIME, 1);
        Assertions.assertEquals(RestoreReuseShadowStats.L0Verdict.REUSABLE, RestoreReuseJudge.REVERSE_L0.check(in));
        Assertions.assertFalse(RestoreReuseJudge.clusterProven(in, RestoreReuseJudge.L0_REVERSE));
        Assertions.assertNull(RestoreReuseJudge.firstReject(in));
        Assertions.assertEquals(RestoreReuseJudge.L0_REVERSE, in.l0Path);
        Assertions.assertFalse(in.clusterProven);
        // the local cluster id is unknown
        in.backupLineage = lineageToLocal(p1(), V1);
        in.localClusterId = RestoreLineage.UNKNOWN_CLUSTER_ID;
        Assertions.assertEquals(RestoreReuseShadowStats.L0Verdict.REUSABLE, RestoreReuseJudge.REVERSE_L0.check(in));
        Assertions.assertFalse(RestoreReuseJudge.clusterProven(in, RestoreReuseJudge.L0_REVERSE));
    }

    @Test
    public void testClusterIdTableLevel() {
        RestoreReuseJudge.Input in = inputWithoutForward(CheckLevel.FULL);
        in.localDbId = db.getId();
        in.localClusterId = LOCAL_CLUSTER;
        // forward: the local table was restored from the source table of the backup
        tbl2.setRestoreSource(new RestoreSource(in.jobInfo.dbId, in.backupTable.id, SRC_CLUSTER));
        Assertions.assertEquals(RestoreReuseShadowStats.L0Verdict.REUSABLE, RestoreReuseJudge.TABLE_L0.check(in));
        Assertions.assertTrue(RestoreReuseJudge.clusterProven(in, RestoreReuseJudge.L0_TABLE));
        tbl2.setRestoreSource(new RestoreSource(in.jobInfo.dbId, in.backupTable.id, SRC_CLUSTER + 1));
        Assertions.assertEquals(RestoreReuseShadowStats.L0Verdict.CLUSTER_MISMATCH,
                RestoreReuseJudge.TABLE_L0.check(in));
        // an old stamp
        tbl2.setRestoreSource(new RestoreSource(in.jobInfo.dbId, in.backupTable.id));
        Assertions.assertEquals(RestoreReuseShadowStats.L0Verdict.REUSABLE, RestoreReuseJudge.TABLE_L0.check(in));
        Assertions.assertFalse(RestoreReuseJudge.clusterProven(in, RestoreReuseJudge.L0_TABLE));
        // an old backup
        tbl2.setRestoreSource(new RestoreSource(in.jobInfo.dbId, in.backupTable.id, SRC_CLUSTER));
        in.jobInfo.srcClusterId = null;
        Assertions.assertEquals(RestoreReuseShadowStats.L0Verdict.REUSABLE, RestoreReuseJudge.TABLE_L0.check(in));
        Assertions.assertFalse(RestoreReuseJudge.clusterProven(in, RestoreReuseJudge.L0_TABLE));
        in.jobInfo.srcClusterId = SRC_CLUSTER;

        // reverse: the source table in the backup was restored from the local table
        tbl2.setRestoreSource(null);
        in.backupTableSource = new RestoreSource(db.getId(), tbl2.getId(), LOCAL_CLUSTER);
        Assertions.assertEquals(RestoreReuseShadowStats.L0Verdict.REUSABLE, RestoreReuseJudge.TABLE_L0.check(in));
        Assertions.assertTrue(RestoreReuseJudge.clusterProven(in, RestoreReuseJudge.L0_TABLE));
        in.backupTableSource = new RestoreSource(db.getId(), tbl2.getId(), LOCAL_CLUSTER + 1);
        Assertions.assertEquals(RestoreReuseShadowStats.L0Verdict.CLUSTER_MISMATCH,
                RestoreReuseJudge.TABLE_L0.check(in));
        in.backupTableSource = new RestoreSource(db.getId(), tbl2.getId());
        Assertions.assertEquals(RestoreReuseShadowStats.L0Verdict.REUSABLE, RestoreReuseJudge.TABLE_L0.check(in));
        Assertions.assertFalse(RestoreReuseJudge.clusterProven(in, RestoreReuseJudge.L0_TABLE));
        // the forward direction with a wrong cluster does not make the reverse one wrong, and vice versa
        tbl2.setRestoreSource(new RestoreSource(in.jobInfo.dbId, in.backupTable.id, SRC_CLUSTER + 1));
        in.backupTableSource = new RestoreSource(db.getId(), tbl2.getId(), LOCAL_CLUSTER);
        Assertions.assertEquals(RestoreReuseShadowStats.L0Verdict.REUSABLE, RestoreReuseJudge.TABLE_L0.check(in));
        Assertions.assertTrue(RestoreReuseJudge.clusterProven(in, RestoreReuseJudge.L0_TABLE));
        // no relation at all stays no relation
        tbl2.setRestoreSource(null);
        in.backupTableSource = null;
        Assertions.assertEquals(RestoreReuseShadowStats.L0Verdict.NO_TABLE_RELATION,
                RestoreReuseJudge.TABLE_L0.check(in));
    }

    @Test
    public void testClusterIdStampedFromTheBackup() {
        BackupJobInfo info = selfBackupJobInfo();
        BackupOlapTableInfo tblInfo = info.getOlapTableInfo(CatalogMocker.TEST_TBL2_NAME);
        RestoreLineage stamp = RestoreJob.buildRestoreLineage(info, tblInfo, tblInfo.getPartInfo(p1().getName()), 1);
        Assertions.assertEquals(SRC_CLUSTER, stamp.getSrcClusterId());
        info.srcClusterId = null;
        stamp = RestoreJob.buildRestoreLineage(info, tblInfo, tblInfo.getPartInfo(p1().getName()), 1);
        Assertions.assertEquals(RestoreLineage.UNKNOWN_CLUSTER_ID, stamp.getSrcClusterId());
        info.srcClusterId = SRC_CLUSTER;
        RestoreJob job = newJob(info, "full");
        job.stampRestoreLineage(db, 1);
        Assertions.assertEquals(SRC_CLUSTER, p1().getRestoreLineage().getSrcClusterId());
        Assertions.assertEquals(SRC_CLUSTER, tbl2.getRestoreSource().getSrcClusterId());
    }

    @Test
    public void testClusterIdGsonCompatibility() {
        // the new field is absent in the json of an old version
        RestoreLineage oldLineage = GsonUtils.GSON.fromJson(
                "{\"db\":1,\"tbl\":2,\"part\":3,\"ver\":4,\"seq\":5,\"bt\":6,\"rt\":7}", RestoreLineage.class);
        Assertions.assertEquals(RestoreLineage.UNKNOWN_CLUSTER_ID, oldLineage.getSrcClusterId());
        Assertions.assertTrue(oldLineage.isSameSource(1, 2, 3));
        // and an unknown cluster id is not written, so the format of the old stamps is unchanged
        Assertions.assertFalse(GsonUtils.GSON.toJson(oldLineage).contains("cid"));
        RestoreSource oldSource = GsonUtils.GSON.fromJson("{\"db\":1,\"tbl\":2}",
                RestoreSource.class);
        Assertions.assertEquals(RestoreLineage.UNKNOWN_CLUSTER_ID, oldSource.getSrcClusterId());
        // the new one round trips, and an old reader ignores it (gson skips unknown fields)
        RestoreLineage lineage = new RestoreLineage(1, 2, 3, 4, 5, 6, 7, 99);
        Assertions.assertEquals(lineage,
                GsonUtils.GSON.fromJson(GsonUtils.GSON.toJson(lineage), RestoreLineage.class));
        Assertions.assertTrue(GsonUtils.GSON.toJson(lineage).contains("\"cid\":99"));
        Assertions.assertEquals(new RestoreSource(1, 2, 99),
                GsonUtils.GSON.fromJson(GsonUtils.GSON.toJson(new RestoreSource(1, 2, 99)),
                        RestoreSource.class));
        // the job info of an old backup has no scid, and does not write it
        BackupJobInfo info = new BackupJobInfo();
        Assertions.assertFalse(GsonUtils.GSON.toJson(info).contains("scid"));
        Assertions.assertEquals(RestoreLineage.UNKNOWN_CLUSTER_ID, info.getSrcClusterIdOrUnknown());
        info.srcClusterId = 12L;
        BackupJobInfo read = GsonUtils.GSON.fromJson(GsonUtils.GSON.toJson(info),
                BackupJobInfo.class);
        Assertions.assertEquals(12L, read.getSrcClusterIdOrUnknown());
        // a decision of an old version has no requested level
        RestoreReuseResult.Decision decision = GsonUtils.GSON.fromJson(
                "{\"l\":\"sample\",\"k\":true}", RestoreReuseResult.Decision.class);
        Assertions.assertNull(decision.requestedLevel);
    }

    @Test
    public void testOnlyDuplicateTablesAreReused() {
        Config.restore_reuse_allow_hidden_state_models = false;
        Assertions.assertNull(RestoreReuseJudge.firstReject(input(CheckLevel.FULL)));
        Assertions.assertNull(RestoreReuseJudge.checkIncrementalModel(input(CheckLevel.FULL)));

        // unique merge-on-write
        Deencapsulation.setField(tbl2, "keysType", KeysType.UNIQUE_KEYS);
        tbl2.setEnableUniqueKeyMergeOnWrite(true);
        Assertions.assertEquals("HIDDEN_STATE_MOW", RestoreReuseJudge.firstReject(input(CheckLevel.FULL)));
        Assertions.assertEquals("HIDDEN_STATE_MOW", RestoreReuseJudge.checkIncrementalModel(input(CheckLevel.FULL)));
        Assertions.assertEquals(RestoreReuseShadowStats.Unsupported.HIDDEN_STATE_MOW,
                RestoreReuseShadowStats.checkUnsupported(false, tbl2, p1()));
        // unique merge-on-read
        tbl2.setEnableUniqueKeyMergeOnWrite(false);
        Assertions.assertEquals("HIDDEN_STATE_MOR", RestoreReuseJudge.firstReject(input(CheckLevel.FULL)));
        Assertions.assertEquals("HIDDEN_STATE_MOR", RestoreReuseJudge.checkIncrementalModel(input(CheckLevel.FULL)));
        Assertions.assertEquals(RestoreReuseShadowStats.Unsupported.HIDDEN_STATE_MOR,
                RestoreReuseShadowStats.checkUnsupported(false, tbl2, p1()));
        // the incremental chain rejects them too
        Assertions.assertEquals("HIDDEN_STATE_MOR",
                RestoreReuseJudge.firstRejectIncremental(incrementalInput(CheckLevel.FULL)));
        // the experiment switch restores the behavior before the narrowing
        Config.restore_reuse_allow_hidden_state_models = true;
        Assertions.assertNull(RestoreReuseJudge.firstReject(input(CheckLevel.FULL)));
        tbl2.setEnableUniqueKeyMergeOnWrite(true);
        Assertions.assertNull(RestoreReuseJudge.firstReject(input(CheckLevel.FULL)));
        Assertions.assertNull(RestoreReuseJudge.checkIncrementalModel(input(CheckLevel.FULL)));
        // back to duplicate
        Config.restore_reuse_allow_hidden_state_models = false;
        Deencapsulation.setField(tbl2, "keysType", KeysType.DUP_KEYS);
        Assertions.assertNull(RestoreReuseJudge.firstReject(input(CheckLevel.FULL)));
    }

    @Test
    public void testHiddenStateTablesAreDownloadedByTheJob() {
        Config.restore_reuse_allow_hidden_state_models = false;
        Deencapsulation.setField(tbl2, "keysType", KeysType.UNIQUE_KEYS);
        tbl2.setEnableUniqueKeyMergeOnWrite(true);
        RestoreJob job = prepareJob("full");
        // no candidate, everything is downloaded
        Assertions.assertTrue(job.getReuseResult().getDecisions().isEmpty());
        Assertions.assertEquals(2L, (long) job.getReuseResult().getRejected().get("HIDDEN_STATE_MOW"));
        Assertions.assertEquals(2, versionInfo(job).size());
        // the estimate does not count them as reusable
        RestoreReuseShadowStats estimate = job.getReuseShadowStats();
        Assertions.assertEquals(0, estimate.getReusable());
        Assertions.assertEquals(2, estimate.getL0PassedButHiddenStateMow());
        Assertions.assertEquals(0, estimate.getL0PassedButHiddenStateMor());
    }

    @Test
    public void testShadowStatsDoNotCountHiddenStateAsReusable() {
        RestoreReuseShadowStats stats = new RestoreReuseShadowStats();
        stats.add(RestoreReuseShadowStats.L0Verdict.REUSABLE, RestoreReuseShadowStats.Unsupported.HIDDEN_STATE_MOW,
                100);
        stats.add(RestoreReuseShadowStats.L0Verdict.REUSABLE, RestoreReuseShadowStats.Unsupported.HIDDEN_STATE_MOR,
                100);
        stats.add(RestoreReuseShadowStats.L0Verdict.REUSABLE, RestoreReuseShadowStats.Unsupported.HIDDEN_STATE_MOR,
                100, RestoreReuseJudge.L0_REVERSE);
        Assertions.assertEquals(3, stats.getPartitions());
        Assertions.assertEquals(0, stats.getReusable());
        Assertions.assertEquals(0, stats.getReusableBytesSingleReplica());
        Assertions.assertEquals(0, stats.getReusableForward() + stats.getReusableReverse() + stats.getReusableTable());
        Assertions.assertEquals(1, stats.getL0PassedButHiddenStateMow());
        Assertions.assertEquals(2, stats.getL0PassedButHiddenStateMor());
        stats.add(RestoreReuseShadowStats.L0Verdict.REUSABLE, RestoreReuseShadowStats.Unsupported.NONE, 100);
        Assertions.assertEquals(1, stats.getReusable());
    }

    @Test
    public void testCheckLevel() {
        Assertions.assertEquals(CheckLevel.OFF, CheckLevel.parse("OFF", "sample"));
        Assertions.assertEquals(CheckLevel.FULL, CheckLevel.parse(" full ", "sample"));
        Assertions.assertEquals(CheckLevel.DISABLE, CheckLevel.parse("disable", "sample"));
        // absent or invalid: the default, and sample if the default is invalid too
        Assertions.assertEquals(CheckLevel.FULL, CheckLevel.parse(null, "full"));
        Assertions.assertEquals(CheckLevel.FULL, CheckLevel.parse("bad", "full"));
        Assertions.assertEquals(CheckLevel.SAMPLE, CheckLevel.parse(null, "bad"));
        Assertions.assertEquals(CheckLevel.SAMPLE, CheckLevel.parse(null, null));

        // unique tables do not allow off
        Assertions.assertEquals(CheckLevel.SAMPLE, RestoreReuseJudge.levelOfTable(CheckLevel.OFF, KeysType.UNIQUE_KEYS));
        Assertions.assertEquals(CheckLevel.OFF, RestoreReuseJudge.levelOfTable(CheckLevel.OFF, KeysType.DUP_KEYS));
        Assertions.assertEquals(CheckLevel.FULL, RestoreReuseJudge.levelOfTable(CheckLevel.FULL, KeysType.UNIQUE_KEYS));
    }

    @Test
    public void testPickSample() {
        List<Integer> pool = Lists.newArrayList();
        for (int i = 0; i < 20; i++) {
            pool.add(i);
        }
        Random random = new Random(1);
        Assertions.assertEquals(2, RestoreReuseJudge.pickSample(pool, 0.1, random).size());
        // at least one
        Assertions.assertEquals(1, RestoreReuseJudge.pickSample(pool, 0.0, random).size());
        Assertions.assertEquals(1, RestoreReuseJudge.pickSample(Lists.newArrayList(5), 0.1, random).size());
        // at most all, no duplicates
        Assertions.assertEquals(20, Sets.newHashSet(RestoreReuseJudge.pickSample(pool, 5, random)).size());
        Assertions.assertTrue(RestoreReuseJudge.pickSample(Lists.<Integer>newArrayList(), 0.5, random).isEmpty());
    }

    @Test
    public void testCompareTablet() {
        LogicalDigestInfo expected = LogicalDigestInfo.of(1, SIG, "abcd");
        Assertions.assertNull(RestoreReuseJudge.compareTablet(expected, Lists.newArrayList(
                LogicalDigestInfo.of(1, SIG, "abcd"), LogicalDigestInfo.of(1, SIG, "ABCD"))));
        // one replica is different
        Assertions.assertEquals(RestoreReuseResult.DOWNLOAD_ROOT_MISMATCH, RestoreReuseJudge.compareTablet(expected,
                Lists.newArrayList(LogicalDigestInfo.of(1, SIG, "abcd"), LogicalDigestInfo.of(1, SIG, "abce"),
                        LogicalDigestInfo.of(1, SIG, "abcd"))));
        // the schema signature or the algorithm version is different
        Assertions.assertEquals(RestoreReuseResult.DOWNLOAD_SCHEMA_MISMATCH, RestoreReuseJudge.compareTablet(expected,
                Lists.newArrayList(LogicalDigestInfo.of(1, "other", "abcd"))));
        Assertions.assertEquals(RestoreReuseResult.DOWNLOAD_SCHEMA_MISMATCH, RestoreReuseJudge.compareTablet(expected,
                Lists.newArrayList(LogicalDigestInfo.of(2, SIG, "abcd"))));
        // no digest of a replica, or of the backup
        Assertions.assertEquals(RestoreReuseResult.DOWNLOAD_DIGEST_ERROR, RestoreReuseJudge.compareTablet(expected,
                Lists.newArrayList(LogicalDigestInfo.of(1, SIG, "abcd"),
                        LogicalDigestInfo.none(LogicalDigestInfo.REASON_ERROR))));
        Assertions.assertEquals(RestoreReuseResult.DOWNLOAD_DIGEST_ERROR, RestoreReuseJudge.compareTablet(expected,
                Lists.newArrayList((LogicalDigestInfo) null)));
        Assertions.assertEquals(RestoreReuseResult.DOWNLOAD_DIGEST_ERROR, RestoreReuseJudge.compareTablet(null,
                Lists.newArrayList(LogicalDigestInfo.of(1, SIG, "abcd"))));
        Assertions.assertEquals(RestoreReuseResult.DOWNLOAD_DIGEST_ERROR,
                RestoreReuseJudge.compareTablet(expected, Lists.<LogicalDigestInfo>newArrayList()));
    }

    // ---------------------------------------------------------------------------------------------
    // selecting the candidates in the job
    // ---------------------------------------------------------------------------------------------

    @Test
    public void testSwitchOffKeepsTheBehavior() {
        Config.enable_restore_partition_reuse = false;
        RestoreJob job = prepareJob(null);
        Assertions.assertNull(job.getReuseResult());
        Assertions.assertEquals(2, versionInfo(job).size());
        // the lineage of the overwritten partitions is invalidated at once, as before
        Assertions.assertNull(p1().getRestoreLineage());
        Assertions.assertNull(p2().getRestoreLineage());

        // allReplicasCreated goes to SNAPSHOTING directly, with snapshot tasks of all partitions.
        Deencapsulation.invoke(job, "allReplicasCreated");
        Assertions.assertEquals(RestoreJobState.SNAPSHOTING, job.getState());
        Assertions.assertEquals(replicasOf(p1()) + replicasOf(p2()),
                submitted.stream().filter(t -> t instanceof SnapshotTask).count());
        Assertions.assertTrue(digestTasks(submitted).isEmpty());
    }

    @Test
    public void testPropertyDisable() {
        RestoreJob job = prepareJob("disable");
        Assertions.assertNull(job.getReuseResult());
        Assertions.assertEquals(2, versionInfo(job).size());
        Assertions.assertNull(p1().getRestoreLineage());
        Deencapsulation.invoke(job, "allReplicasCreated");
        Assertions.assertEquals(RestoreJobState.SNAPSHOTING, job.getState());
        Assertions.assertEquals(replicasOf(p1()) + replicasOf(p2()),
                submitted.stream().filter(t -> t instanceof SnapshotTask).count());
    }

    @Test
    public void testNoCandidateSkipsVerifying() {
        // p1 was written after the restore, p2 is too small.
        p1().updateVersionForRestore(V1 + 1);
        Config.restore_reuse_min_partition_bytes = 3001;
        RestoreJob job = prepareJob(null);
        Assertions.assertEquals(0, job.getReuseResult().getDecisions().size());
        Assertions.assertEquals(1L, (long) job.getReuseResult().getRejected().get("L0_LOCAL_VERSION_CHANGED"));
        Assertions.assertEquals(1L, (long) job.getReuseResult().getRejected().get("TOO_SMALL"));
        Deencapsulation.invoke(job, "allReplicasCreated");
        Assertions.assertEquals(RestoreJobState.SNAPSHOTING, job.getState());
        Assertions.assertTrue(digestTasks(submitted).isEmpty());
    }

    @Test
    public void testCandidatesKeepTheirLineageUntilDecided() {
        RestoreJob job = prepareJob(null);
        Assertions.assertNotNull(job.getReuseResult());
        Assertions.assertNotNull(p1().getRestoreLineage());
        Assertions.assertNotNull(p2().getRestoreLineage());
        Assertions.assertEquals(2, versionInfo(job).size());
    }

    @Test
    public void testAtomicRestoreAndAllowLoadAreNotCandidates() {
        RestoreJob job = new RestoreJob("l", "2024-01-01 00:00:00", db.getId(), db.getFullName(), selfBackupJobInfo(),
                true /* allow load */, new ReplicaAllocation((short) 3), 100000, -1, false, false, false, false,
                false, false, false, false, env, Repository.KEEP_ON_LOCAL_REPO_ID, backupMeta());
        Deencapsulation.invoke(job, "checkAndPrepareMeta");
        Assertions.assertTrue(job.getStatus().ok(), job.getStatus().toString());
        Assertions.assertEquals(2L, (long) job.getReuseResult().getRejected().get("ALLOW_LOAD"));
        Assertions.assertTrue(job.getReuseResult().getDecisions().isEmpty());
    }

    // ---------------------------------------------------------------------------------------------
    // VERIFYING
    // ---------------------------------------------------------------------------------------------

    @Test
    public void testAllKeptSkipsSnapshotDownloadAndMove() {
        RestoreJob job = prepareJob("full");
        toVerifying(job);
        List<RestoreDigestTask> tasks = newDigestTasks();
        // one task for each replica of the 2 partitions, at the local visible version
        Assertions.assertEquals(replicasOf(p1()) + replicasOf(p2()), tasks.size());
        for (RestoreDigestTask task : tasks) {
            Assertions.assertEquals(V1, task.getVersion());
            Assertions.assertEquals(job.getJobId(), task.getJobId());
        }
        Assertions.assertEquals(tasks.size(), tasks.stream().map(AgentTask::getSignature).distinct().count());
        Assertions.assertTrue(submitted.stream().noneMatch(t -> t instanceof SnapshotTask));

        // still waiting
        reportAll(job, tasks.subList(0, tasks.size() - 1), Maps.newHashMap());
        waitDigests(job);
        Assertions.assertEquals(RestoreJobState.VERIFYING, job.getState());

        reportAll(job, tasks.subList(tasks.size() - 1, tasks.size()), Maps.newHashMap());
        waitDigests(job);
        Assertions.assertEquals(RestoreJobState.SNAPSHOTING, job.getState());

        // both are kept: nothing to snapshot, download or move, and they are not in restoredVersionInfo
        RestoreReuseResult result = job.getReuseResult();
        Assertions.assertEquals(2, result.getKeptPartitions());
        Assertions.assertEquals(0, result.getDownloadedPartitions());
        Assertions.assertEquals(RestoreReuseShadowStats.getSingleReplicaLocalDataSize(p1())
                + RestoreReuseShadowStats.getSingleReplicaLocalDataSize(p2()), result.getKeptBytesSingleReplica());
        for (RestoreReuseResult.Decision decision : result.getDecisions()) {
            Assertions.assertEquals(RestoreReuseResult.KEPT_DIGEST_VERIFIED, decision.reason);
        }
        Assertions.assertTrue(versionInfo(job).isEmpty());
        Assertions.assertTrue(fileMapping(job).getMapping().isEmpty());
        Assertions.assertTrue(submitted.isEmpty());
        // the lineage is not invalidated
        Assertions.assertNotNull(p1().getRestoreLineage());
        Assertions.assertNotNull(p2().getRestoreLineage());

        // no move task
        Deencapsulation.setField(job, "state", RestoreJobState.COMMIT);
        job.commit();
        Assertions.assertTrue(submitted.stream().noneMatch(t -> t instanceof DirMoveTask));
        Assertions.assertEquals(RestoreJobState.COMMITTING, job.getState());

        // commit: versions are untouched, the lineage is written again with the time of this restore
        Status st = job.allTabletCommitted(false);
        Assertions.assertTrue(st.ok(), st.toString());
        Assertions.assertEquals(V1, p1().getVisibleVersion());
        Assertions.assertEquals(V1, p2().getVisibleVersion());
        for (Partition part : Lists.newArrayList(p1(), p2())) {
            Assertions.assertEquals(lineage(selfBackupJobInfo(), part).getSrcVersion(),
                    part.getRestoreLineage().getSrcVersion());
            Assertions.assertEquals(job.getFinishedTime(), part.getRestoreLineage().getRestoreTime());
            Assertions.assertEquals(org.apache.doris.catalog.Partition.PartitionState.NORMAL, part.getState());
            for (Replica replica : part.getBaseIndex().getTablets().get(0).getReplicas()) {
                Assertions.assertEquals(V1, replica.getVersion());
            }
        }
        Assertions.assertEquals(OlapTableState.NORMAL, tbl2.getState());
        Assertions.assertEquals(2, job.getReuseResult().getKeptPartitions());
    }

    @Test
    public void testOneReplicaDiffersDownloadsOnlyThatPartition() {
        RestoreJob job = prepareJob("full");
        toVerifying(job);
        List<RestoreDigestTask> tasks = newDigestTasks();
        // one replica of p1 has a different root
        Map<Long, TFinishTaskRequest> faulty = Maps.newHashMap();
        RestoreDigestTask bad = tasks.stream().filter(t -> t.getPartitionId() == p1().getId()).skip(1).findFirst().get();
        faulty.put(bad.getSignature(), okReport("ffff", SIG, 1));
        reportAll(job, tasks, faulty);
        waitDigests(job);

        Assertions.assertEquals(RestoreJobState.SNAPSHOTING, job.getState());
        RestoreReuseResult result = job.getReuseResult();
        Assertions.assertEquals(1, result.getKeptPartitions());
        Assertions.assertTrue(result.isKept(tbl2.getId(), p2().getId()));
        Assertions.assertFalse(result.isKept(tbl2.getId(), p1().getId()));
        for (RestoreReuseResult.Decision decision : result.getDecisions()) {
            if (decision.partitionId == p1().getId()) {
                Assertions.assertEquals(RestoreReuseResult.DOWNLOAD_ROOT_MISMATCH, decision.reason);
            }
        }
        // p1 is restored as usual, p2 is not
        Assertions.assertTrue(versionInfo(job).contains(tbl2.getId(), p1().getId()));
        Assertions.assertFalse(versionInfo(job).contains(tbl2.getId(), p2().getId()));
        Assertions.assertTrue(fileMapping(job).getMapping().keySet().stream()
                .allMatch(chain -> chain.getPartId() == p1().getId()));
        Assertions.assertEquals(replicasOf(p1()), fileMapping(job).getMapping().size());
        // only p1 is snapshotted
        List<SnapshotTask> snapshotTasks = submitted.stream().filter(t -> t instanceof SnapshotTask)
                .map(t -> (SnapshotTask) t).collect(Collectors.toList());
        Assertions.assertEquals(replicasOf(p1()), snapshotTasks.size());
        Assertions.assertTrue(snapshotTasks.stream().allMatch(t -> t.getPartitionId() == p1().getId()));
        // the lineage of p1 is invalidated, p2 keeps it
        Assertions.assertNull(p1().getRestoreLineage());
        Assertions.assertNotNull(p2().getRestoreLineage());

        // download and move follow the snapshots, so only p1 has them
        for (SnapshotTask task : snapshotTasks) {
            TFinishTaskRequest report = new TFinishTaskRequest();
            report.setTaskStatus(new TStatus(TStatusCode.OK));
            report.setSnapshotPath("/path/snapshot");
            Assertions.assertTrue(job.finishTabletSnapshotTask(task, report));
        }
        submitted.clear();
        Deencapsulation.setField(job, "state", RestoreJobState.COMMIT);
        job.commit();
        List<DirMoveTask> moves = submitted.stream().filter(t -> t instanceof DirMoveTask)
                .map(t -> (DirMoveTask) t).collect(Collectors.toList());
        Assertions.assertEquals(replicasOf(p1()), moves.size());
        Assertions.assertTrue(moves.stream().allMatch(t -> t.getPartitionId() == p1().getId()));
        Assertions.assertTrue(snapshotInfos(job).values().stream().allMatch(i -> i.getPartitionId() == p1().getId()));

        // commit: p1 gets the backup version, p2 is kept as it is
        Status st = job.allTabletCommitted(false);
        Assertions.assertTrue(st.ok(), st.toString());
        Assertions.assertEquals(V1, p1().getVisibleVersion());
        Assertions.assertEquals(V1, p2().getVisibleVersion());
        Assertions.assertNotNull(p1().getRestoreLineage());
        Assertions.assertNotNull(p2().getRestoreLineage());
    }

    @Test
    public void testSchemaSigDifferenceDownloads() {
        RestoreJob job = prepareJob("full");
        toVerifying(job);
        List<RestoreDigestTask> tasks = newDigestTasks();
        Map<Long, TFinishTaskRequest> faulty = Maps.newHashMap();
        for (RestoreDigestTask task : tasks) {
            if (task.getPartitionId() == p2().getId()) {
                faulty.put(task.getSignature(), okReport(ROOT_P2, "another-schema", 1));
            }
        }
        reportAll(job, tasks, faulty);
        waitDigests(job);
        Assertions.assertEquals(1, job.getReuseResult().getKeptPartitions());
        Assertions.assertTrue(job.getReuseResult().isKept(tbl2.getId(), p1().getId()));
        Assertions.assertEquals(RestoreReuseResult.DOWNLOAD_SCHEMA_MISMATCH, job.getReuseResult().getDecisions()
                .stream().filter(d -> !d.kept).findFirst().get().reason);
    }

    @Test
    public void testFailedTaskAndUnsupportedDownload() {
        RestoreJob job = prepareJob("full");
        toVerifying(job);
        List<RestoreDigestTask> tasks = newDigestTasks();
        Map<Long, TFinishTaskRequest> faulty = Maps.newHashMap();
        // a failed task in p1, a report without digest (an old backend) in p2
        faulty.put(tasks.stream().filter(t -> t.getPartitionId() == p1().getId()).findFirst().get().getSignature(),
                failedReport());
        TFinishTaskRequest noDigest = new TFinishTaskRequest();
        noDigest.setTaskStatus(new TStatus(TStatusCode.OK));
        faulty.put(tasks.stream().filter(t -> t.getPartitionId() == p2().getId()).findFirst().get().getSignature(),
                noDigest);
        reportAll(job, tasks, faulty);
        waitDigests(job);
        Assertions.assertEquals(0, job.getReuseResult().getKeptPartitions());
        Assertions.assertTrue(job.getReuseResult().getDecisions().stream()
                .allMatch(d -> RestoreReuseResult.DOWNLOAD_DIGEST_ERROR.equals(d.reason)));
        Assertions.assertEquals(2, versionInfo(job).size());
        Assertions.assertEquals(replicasOf(p1()) + replicasOf(p2()),
                submitted.stream().filter(t -> t instanceof SnapshotTask).count());
        Assertions.assertNull(p1().getRestoreLineage());
        Assertions.assertNull(p2().getRestoreLineage());
    }

    @Test
    public void testTimeoutDownloadsTheUnfinished() {
        RestoreJob job = prepareJob("full");
        toVerifying(job);
        List<RestoreDigestTask> tasks = newDigestTasks();
        // p2 is done, one task of p1 is not
        reportAll(job, tasks.stream().filter(t -> t.getPartitionId() == p2().getId()).collect(Collectors.toList()),
                Maps.newHashMap());
        reportAll(job, tasks.stream().filter(t -> t.getPartitionId() == p1().getId()).skip(1)
                .collect(Collectors.toList()), Maps.newHashMap());
        waitDigests(job);
        Assertions.assertEquals(RestoreJobState.VERIFYING, job.getState());

        Config.restore_digest_timeout_s = 1;
        Deencapsulation.setField(job, "verifyRoundStartMs", System.currentTimeMillis() - 5000);
        waitDigests(job);
        Assertions.assertEquals(RestoreJobState.SNAPSHOTING, job.getState());
        RestoreReuseResult result = job.getReuseResult();
        Assertions.assertTrue(result.isKept(tbl2.getId(), p2().getId()));
        Assertions.assertFalse(result.isKept(tbl2.getId(), p1().getId()));
        Assertions.assertEquals(RestoreReuseResult.DOWNLOAD_TIMEOUT, result.getDecisions().stream()
                .filter(d -> d.partitionId == p1().getId()).findFirst().get().reason);
        // a report that comes after the timeout is ignored
        Assertions.assertTrue(job.finishRestoreDigestTask(tasks.get(0), okReport(ROOT_P1, SIG, 1)));
    }

    @Test
    public void testUnavailableBackendDownloads() {
        Mockito.when(systemInfo.getBackend(CatalogMocker.BACKEND1_ID)).thenReturn(null);
        RestoreJob job = prepareJob("full");
        toVerifying(job);
        List<RestoreDigestTask> tasks = newDigestTasks();
        // no task for the replicas on the missing backend
        Assertions.assertEquals(replicasOf(p1()) + replicasOf(p2()) - 4, tasks.size());
        reportAll(job, tasks, Maps.newHashMap());
        waitDigests(job);
        Assertions.assertEquals(RestoreJobState.SNAPSHOTING, job.getState());
        Assertions.assertEquals(0, job.getReuseResult().getKeptPartitions());
    }

    @Test
    public void testLevelOffKeepsWithoutDigest() {
        RestoreJob job = prepareJob("off");
        // the lineage of the candidates is kept until they are decided
        Assertions.assertNotNull(p1().getRestoreLineage());
        Deencapsulation.invoke(job, "allReplicasCreated");
        // no digest to compute: VERIFYING is skipped
        Assertions.assertEquals(RestoreJobState.SNAPSHOTING, job.getState());
        Assertions.assertTrue(digestTasks(submitted).isEmpty());
        Assertions.assertEquals(0, submitted.size());
        Assertions.assertEquals(2, job.getReuseResult().getKeptPartitions());
        Assertions.assertEquals(2, job.getReuseResult().getDecisions().size());
        for (RestoreReuseResult.Decision decision : job.getReuseResult().getDecisions()) {
            Assertions.assertEquals(RestoreReuseResult.KEPT_L0_ONLY, decision.reason);
            Assertions.assertEquals("off", decision.level);
        }
        Assertions.assertTrue(versionInfo(job).isEmpty());
        Assertions.assertTrue(fileMapping(job).getMapping().isEmpty());
    }

    @Test
    public void testUniqueTableUpgradesOffToSample() {
        Deencapsulation.setField(tbl2, "keysType", KeysType.UNIQUE_KEYS);
        Config.restore_reuse_sample_ratio = 0.1;
        RestoreJob job = prepareJob("off");
        // not kept on the lineage alone, they need a digest
        Assertions.assertTrue(job.getReuseResult().getDecisions().isEmpty());
        Deencapsulation.invoke(job, "allReplicasCreated");
        Assertions.assertEquals(RestoreJobState.VERIFYING, job.getState());
        // sampled: one of the two partitions
        List<RestoreDigestTask> tasks = newDigestTasks();
        Assertions.assertEquals(replicasOf(p1()), tasks.size());
    }

    @Test
    public void testSampleMismatchUpgradesTheRestToFull() {
        Config.restore_reuse_sample_ratio = 0.1;
        RestoreJob job = prepareJob("sample");
        toVerifying(job);
        List<RestoreDigestTask> tasks = newDigestTasks();
        // only the sampled partition (at least one) is computed first
        Assertions.assertEquals(replicasOf(p1()), tasks.size());
        long sampledPart = tasks.get(0).getPartitionId();
        Map<Long, TFinishTaskRequest> faulty = Maps.newHashMap();
        faulty.put(tasks.get(1).getSignature(), okReport("ffff", SIG, 1));
        reportAll(job, tasks, faulty);
        waitDigests(job);

        // the other partition is verified too, in a second round
        Assertions.assertEquals(RestoreJobState.VERIFYING, job.getState());
        List<RestoreDigestTask> second = newDigestTasks();
        Assertions.assertEquals(replicasOf(p2()), second.size());
        Assertions.assertTrue(second.stream().noneMatch(t -> t.getPartitionId() == sampledPart));
        Assertions.assertEquals(2, (int) Deencapsulation.getField(job, "verifyRound"));
        reportAll(job, second, Maps.newHashMap());
        waitDigests(job);
        Assertions.assertEquals(RestoreJobState.SNAPSHOTING, job.getState());
        RestoreReuseResult result = job.getReuseResult();
        Assertions.assertEquals(1, result.getKeptPartitions());
        Assertions.assertFalse(result.isKept(tbl2.getId(), sampledPart));
        RestoreReuseResult.Decision other = result.getDecisions().stream()
                .filter(d -> d.partitionId != sampledPart).findFirst().get();
        Assertions.assertTrue(other.kept);
        Assertions.assertEquals(RestoreReuseResult.KEPT_DIGEST_VERIFIED, other.reason);
        Assertions.assertEquals("full", other.level);
    }

    @Test
    public void testSamplePassedKeepsTheRestWithoutDigest() {
        Config.restore_reuse_sample_ratio = 0.1;
        RestoreJob job = prepareJob("sample");
        toVerifying(job);
        List<RestoreDigestTask> tasks = newDigestTasks();
        Assertions.assertEquals(replicasOf(p1()), tasks.size());
        long sampledPart = tasks.get(0).getPartitionId();
        reportAll(job, tasks, Maps.newHashMap());
        waitDigests(job);
        Assertions.assertEquals(RestoreJobState.SNAPSHOTING, job.getState());
        RestoreReuseResult result = job.getReuseResult();
        Assertions.assertEquals(2, result.getKeptPartitions());
        for (RestoreReuseResult.Decision decision : result.getDecisions()) {
            Assertions.assertEquals(decision.partitionId == sampledPart ? RestoreReuseResult.KEPT_DIGEST_VERIFIED
                    : RestoreReuseResult.KEPT_SAMPLE_PASSED, decision.reason);
        }
    }

    @Test
    public void testSampleFailureDownloadsTheRest() {
        Config.restore_reuse_sample_ratio = 0.1;
        RestoreJob job = prepareJob("sample");
        toVerifying(job);
        List<RestoreDigestTask> tasks = newDigestTasks();
        long sampledPart = tasks.get(0).getPartitionId();
        Map<Long, TFinishTaskRequest> faulty = Maps.newHashMap();
        faulty.put(tasks.get(0).getSignature(), failedReport());
        reportAll(job, tasks, faulty);
        waitDigests(job);
        // an uncertain sample proves nothing about the others, no second round
        Assertions.assertEquals(RestoreJobState.SNAPSHOTING, job.getState());
        Assertions.assertEquals(0, job.getReuseResult().getKeptPartitions());
        Assertions.assertEquals(RestoreReuseResult.DOWNLOAD_SAMPLE_FAILED, job.getReuseResult().getDecisions()
                .stream().filter(d -> d.partitionId != sampledPart).findFirst().get().reason);
        Assertions.assertEquals(replicasOf(p1()) + replicasOf(p2()),
                submitted.stream().filter(t -> t instanceof SnapshotTask).count());
    }

    @Test
    public void testVersionChangedWhileVerifyingDownloads() {
        RestoreJob job = prepareJob("full");
        toVerifying(job);
        List<RestoreDigestTask> tasks = newDigestTasks();
        reportAll(job, tasks, Maps.newHashMap());
        // a write sneaks in after the digests are computed
        p1().updateVersionForRestore(V1 + 1);
        waitDigests(job);
        Assertions.assertEquals(1, job.getReuseResult().getKeptPartitions());
        Assertions.assertEquals(RestoreReuseResult.DOWNLOAD_CHANGED, job.getReuseResult().getDecisions().stream()
                .filter(d -> d.partitionId == p1().getId()).findFirst().get().reason);
        Assertions.assertTrue(versionInfo(job).contains(tbl2.getId(), p1().getId()));
    }

    // ---------------------------------------------------------------------------------------------
    // cancel, persistence and replay, the recheck before commit
    // ---------------------------------------------------------------------------------------------

    @Test
    public void testCancelWhileVerifyingChangesNothingLocal() {
        RestoreJob job = prepareJob("full");
        toVerifying(job);
        newDigestTasks();
        Assertions.assertTrue(job.cancel().ok());
        Assertions.assertEquals(RestoreJobState.CANCELLED, job.getState());
        Assertions.assertEquals(OlapTableState.NORMAL, tbl2.getState());
        Assertions.assertEquals(V1, p1().getVisibleVersion());
        Assertions.assertEquals(V1, p2().getVisibleVersion());
        // the lineage was never touched, the next restore can still reuse the partitions
        Assertions.assertNotNull(p1().getRestoreLineage());
        Assertions.assertNotNull(p2().getRestoreLineage());
        Assertions.assertEquals(Partition.PartitionState.NORMAL, p1().getState());
        // a late report is ignored
        Assertions.assertTrue(job.finishRestoreDigestTask(new RestoreDigestTask(CatalogMocker.BACKEND1_ID, 1,
                job.getJobId(), db.getId(), tbl2.getId(), p1().getId(), tbl2.getId(), tabletOf(p1()), 1, V1, 0),
                okReport(ROOT_P1, SIG, 1)));
    }

    @Test
    public void testPersistAndReplay() throws Exception {
        RestoreJob job = prepareJob("full");
        toVerifying(job);
        List<RestoreDigestTask> tasks = newDigestTasks();
        Map<Long, TFinishTaskRequest> faulty = Maps.newHashMap();
        faulty.put(tasks.stream().filter(t -> t.getPartitionId() == p1().getId()).findFirst().get().getSignature(),
                okReport("ffff", SIG, 1));
        reportAll(job, tasks, faulty);
        waitDigests(job);
        // the DOWNLOAD edit log
        Deencapsulation.setField(job, "state", RestoreJobState.DOWNLOAD);
        RestoreJob replayed = writeAndRead(job);

        RestoreReuseResult result = replayed.getReuseResult();
        Assertions.assertNotNull(result);
        Assertions.assertEquals(2, result.getDecisions().size());
        Assertions.assertEquals(1, result.getKeptPartitions());
        Assertions.assertTrue(result.isKept(tbl2.getId(), p2().getId()));
        Assertions.assertEquals(job.getReuseResult().toString(), result.toString());
        Assertions.assertEquals(RestoreReuseResult.DOWNLOAD_ROOT_MISMATCH, result.getDecisions().stream()
                .filter(d -> !d.kept).findFirst().get().reason);
        Assertions.assertEquals(RestoreReuseResult.KEPT_DIGEST_VERIFIED, result.getDecisions().stream()
                .filter(d -> d.kept).findFirst().get().reason);
        Assertions.assertTrue(versionInfo(replayed).contains(tbl2.getId(), p1().getId()));
        Assertions.assertFalse(versionInfo(replayed).contains(tbl2.getId(), p2().getId()));
        Assertions.assertEquals(replicasOf(p1()), fileMapping(replayed).getMapping().size());

        // the follower replays it: the lineage of the partition to download is invalidated, the kept one stays.
        p1().setRestoreLineage(lineage(selfBackupJobInfo(), p1()));
        p2().setRestoreLineage(lineage(selfBackupJobInfo(), p2()));
        p1().setState(Partition.PartitionState.NORMAL);
        replayed.setEnv(env);
        replayed.replayRun();
        Assertions.assertNull(p1().getRestoreLineage());
        Assertions.assertNotNull(p2().getRestoreLineage());
        Assertions.assertEquals(Partition.PartitionState.RESTORE, p2().getState());

        // the finished job (the log written at the end) keeps the result, and the follower commits the same
        Deencapsulation.setField(job, "state", RestoreJobState.COMMITTING);
        Status st = job.allTabletCommitted(false);
        Assertions.assertTrue(st.ok(), st.toString());
        RestoreJob finished = writeAndRead(job);
        Assertions.assertEquals(1, finished.getReuseResult().getKeptPartitions());
        Assertions.assertEquals(RestoreJobState.FINISHED, finished.getState());
    }

    @Test
    public void testGsonWithoutReuseResult() throws Exception {
        // A job written by an FE without the field (or with the switch off).
        Config.enable_restore_partition_reuse = false;
        RestoreJob job = prepareJob(null);
        RestoreJob replayed = writeAndRead(job);
        Assertions.assertNull(replayed.getReuseResult());
        String json = GsonUtils.GSON.toJson(job);
        Assertions.assertFalse(json.contains("\"rru\""));
        // the working state is not persisted
        Assertions.assertFalse(json.contains("digestResults"));
        Assertions.assertFalse(json.contains("reuseCandidates"));
    }

    private RestoreJob keptJobAtCommit() {
        RestoreJob job = prepareJob("full");
        toVerifying(job);
        reportAll(job, newDigestTasks(), Maps.newHashMap());
        waitDigests(job);
        Assertions.assertEquals(2, job.getReuseResult().getKeptPartitions());
        return job;
    }

    @Test
    public void testRecheckBeforeCommitFailsTheJob() {
        RestoreJob job = keptJobAtCommit();
        // a write that should never happen
        p2().updateVersionForRestore(V1 + 3);
        Status st = job.allTabletCommitted(false);
        Assertions.assertFalse(st.ok());
        Assertions.assertTrue(st.getErrMsg().contains("changed from " + V1 + " to " + (V1 + 3)), st.getErrMsg());
        Assertions.assertTrue(st.getErrMsg().contains("p2"), st.getErrMsg());
        // nothing is committed: still RESTORE state and the version is not rewritten
        Assertions.assertEquals(OlapTableState.RESTORE, tbl2.getState());
        Assertions.assertEquals(V1 + 3, p2().getVisibleVersion());

        // before any tablet is moved
        Deencapsulation.setField(job, "state", RestoreJobState.COMMIT);
        job.commit();
        Assertions.assertFalse(job.getStatus().ok());
        Assertions.assertEquals(RestoreJobState.COMMIT, job.getState());
        Assertions.assertTrue(submitted.stream().noneMatch(t -> t instanceof DirMoveTask));
    }

    @Test
    public void testRecheckFailsWhenTheKeptPartitionIsDropped() {
        RestoreJob job = keptJobAtCommit();
        Map<Long, Partition> idToPartition = Deencapsulation.getField(tbl2, "idToPartition");
        Map<String, Partition> nameToPartition = Deencapsulation.getField(tbl2, "nameToPartition");
        Partition p1 = p1();
        idToPartition.remove(p1.getId());
        nameToPartition.remove(p1.getName());
        Status st = job.allTabletCommitted(false);
        Assertions.assertFalse(st.ok());
        Assertions.assertTrue(st.getErrMsg().contains("has been dropped"), st.getErrMsg());
    }

    @Test
    public void testReplayDoesNotRecheck() {
        RestoreJob job = keptJobAtCommit();
        p2().updateVersionForRestore(V1 + 3);
        // the follower trusts the master
        Status st = job.allTabletCommitted(true);
        Assertions.assertTrue(st.ok(), st.toString());
    }

    // ---------------------------------------------------------------------------------------------
    // incremental append
    // ---------------------------------------------------------------------------------------------

    private static final long V2 = V1 + 4;

    // The backup of test_tbl2 taken at V2 (the local partitions are at V1), kept on local, with the manifest and the
    // decomposed digest of every tablet.
    private BackupJobInfo incrementalJobInfo() {
        BackupJobInfo info = selfBackupJobInfo();
        info.manifestVersion = BackupJobInfo.MANIFEST_VERSION;
        info.extraInfo = new BackupJobInfo.ExtraInfo();
        info.extraInfo.token = "token";
        BackupJobInfo.ExtraInfo.NetworkAddrss addr = new BackupJobInfo.ExtraInfo.NetworkAddrss();
        addr.ip = "127.0.0.1";
        addr.port = 8040;
        info.extraInfo.beNetworkMap.put(1L, addr);
        BackupOlapTableInfo tblInfo = info.getOlapTableInfo(CatalogMocker.TEST_TBL2_NAME);
        for (BackupPartitionInfo partInfo : tblInfo.partitions.values()) {
            partInfo.version = V2;
            for (BackupIndexInfo idxInfo : partInfo.indexes.values()) {
                idxInfo.manifestRoots = Maps.newHashMap();
                idxInfo.prefixDigestRoots = Maps.newHashMap();
                for (BackupTabletInfo tablet : idxInfo.sortedTabletInfoList) {
                    tablet.manifestRoot = "m" + tablet.id;
                    tablet.prefixDigestRoot = "p" + tablet.id;
                    idxInfo.manifestRoots.put(tablet.id, tablet.manifestRoot);
                    idxInfo.prefixDigestRoots.put(tablet.id, tablet.prefixDigestRoot);
                    info.tabletBeMap.put(tablet.id, 1L);
                    info.tabletSnapshotPathMap.put(tablet.id, "/snapshot/ss");
                }
            }
        }
        return info;
    }

    private RestoreJob prepareIncrementalJob(String level) {
        Config.enable_restore_incremental_append = true;
        RestoreJob job = newJob(incrementalJobInfo(), level);
        Deencapsulation.invoke(job, "checkAndPrepareMeta");
        Assertions.assertTrue(job.getStatus().ok(), job.getStatus().toString());
        return job;
    }

    private static TFinishTaskRequest prefixReport(String verdict) {
        TFinishTaskRequest request = okReport("cc33", SIG, 1);
        request.getLogicalDigest().setPrefixVerdict(verdict);
        return request;
    }

    private void reportPrefix(RestoreJob job, List<RestoreDigestTask> tasks, Map<Long, String> verdictOfPartition) {
        for (RestoreDigestTask task : tasks) {
            String verdict = verdictOfPartition.getOrDefault(task.getPartitionId(), "OK");
            Assertions.assertTrue(job.finishRestoreDigestTask(task, prefixReport(verdict)));
        }
    }

    private RestoreReuseJudge.Input incrementalInput(CheckLevel level) {
        RestoreReuseJudge.Input in = input(level);
        in.backupPartition.version = V2;
        in.jobInfo = incrementalJobInfo();
        in.backupTable = in.jobInfo.getOlapTableInfo(CatalogMocker.TEST_TBL2_NAME);
        in.backupPartition = in.backupTable.getPartInfo(CatalogMocker.TEST_PARTITION1_NAME);
        in.incrementalEnabled = true;
        return in;
    }

    @Test
    public void testIncrementalConditions() {
        Assertions.assertNull(RestoreReuseJudge.firstRejectIncremental(incrementalInput(CheckLevel.FULL)));
        Assertions.assertNull(RestoreReuseJudge.firstRejectIncremental(incrementalInput(CheckLevel.SAMPLE)));
        // the plain chain rejects it: the versions differ
        Assertions.assertNotNull(RestoreReuseJudge.firstReject(incrementalInput(CheckLevel.FULL)));

        RestoreReuseJudge.Input in = incrementalInput(CheckLevel.FULL);
        in.incrementalEnabled = false;
        Assertions.assertEquals("INCREMENTAL_DISABLED", RestoreReuseJudge.firstRejectIncremental(in));

        in = incrementalInput(CheckLevel.OFF);
        Assertions.assertEquals("INCREMENTAL_LEVEL_OFF", RestoreReuseJudge.firstRejectIncremental(in));
        in = incrementalInput(CheckLevel.DISABLE);
        Assertions.assertEquals("INCREMENTAL_LEVEL_DISABLE", RestoreReuseJudge.firstRejectIncremental(in));

        in = incrementalInput(CheckLevel.FULL);
        in.atomicRestore = true;
        Assertions.assertEquals("ATOMIC_RESTORE", RestoreReuseJudge.firstRejectIncremental(in));
        in = incrementalInput(CheckLevel.FULL);
        in.allowLoad = true;
        Assertions.assertEquals("ALLOW_LOAD", RestoreReuseJudge.firstRejectIncremental(in));

        // the same version, or the local partition is ahead
        in = incrementalInput(CheckLevel.FULL);
        in.backupPartition.version = V1;
        Assertions.assertEquals("INCREMENTAL_SAME_VERSION", RestoreReuseJudge.firstRejectIncremental(in));
        in = incrementalInput(CheckLevel.FULL);
        in.backupPartition.version = V1 - 1;
        Assertions.assertEquals("INCREMENTAL_LOCAL_AHEAD", RestoreReuseJudge.firstRejectIncremental(in));

        // no relation at all
        in = incrementalInput(CheckLevel.FULL);
        p1().setRestoreLineage(null);
        Assertions.assertEquals("L0_NO_LINEAGE", RestoreReuseJudge.firstRejectIncremental(in));
        p1().setRestoreLineage(lineage(selfBackupJobInfo(), p1()));
        // another source partition
        in = incrementalInput(CheckLevel.FULL);
        in.jobInfo.dbId = in.jobInfo.dbId + 1;
        Assertions.assertEquals("L0_LINEAGE_MISMATCH", RestoreReuseJudge.firstRejectIncremental(in));
        // the commit seq goes backwards
        in = incrementalInput(CheckLevel.FULL);
        in.srcCommitSeq = COMMIT_SEQ - 1;
        Assertions.assertEquals("L0_COMMIT_SEQ_MISMATCH", RestoreReuseJudge.firstRejectIncremental(in));

        // the model: duplicate and unique (merge-on-read and merge-on-write) are candidates, aggregate is not
        in = incrementalInput(CheckLevel.FULL);
        Deencapsulation.setField(tbl2, "keysType", KeysType.AGG_KEYS);
        Assertions.assertEquals("AGGREGATE_TABLE", RestoreReuseJudge.firstRejectIncremental(in));
        Deencapsulation.setField(tbl2, "keysType", KeysType.UNIQUE_KEYS);
        tbl2.setEnableUniqueKeyMergeOnWrite(false);
        Assertions.assertFalse(tbl2.getEnableUniqueKeyMergeOnWrite());
        Assertions.assertNull(RestoreReuseJudge.firstRejectIncremental(in));
        Assertions.assertNull(RestoreReuseJudge.checkIncrementalModel(in));
        tbl2.setEnableUniqueKeyMergeOnWrite(true);
        Assertions.assertNull(RestoreReuseJudge.firstRejectIncremental(in));
        Assertions.assertNull(RestoreReuseJudge.checkIncrementalModel(in));
        // the other conditions still apply to them
        in.incrementalEnabled = false;
        Assertions.assertEquals("INCREMENTAL_DISABLED", RestoreReuseJudge.firstRejectIncremental(in));
        in.incrementalEnabled = true;
        tbl2.setEnableUniqueKeyMergeOnWrite(false);
        in.jobInfo.manifestVersion = null;
        Assertions.assertEquals("NO_MANIFEST", RestoreReuseJudge.firstRejectIncremental(in));
        in = incrementalInput(CheckLevel.FULL);
        Deencapsulation.setField(tbl2, "keysType", KeysType.DUP_KEYS);

        // the backup must have the manifest and the decomposed digest of every tablet
        in = incrementalInput(CheckLevel.FULL);
        in.jobInfo.manifestVersion = null;
        Assertions.assertEquals("NO_MANIFEST", RestoreReuseJudge.firstRejectIncremental(in));
        in = incrementalInput(CheckLevel.FULL);
        in.backupPartition.getIdx(tbl2.getIndexNameById(p1().getBaseIndex().getId()))
                .sortedTabletInfoList.get(0).prefixDigestRoot = null;
        Assertions.assertEquals("NO_PREFIX_DIGEST", RestoreReuseJudge.firstRejectIncremental(in));
        in = incrementalInput(CheckLevel.FULL);
        in.backupPartition.getIdx(tbl2.getIndexNameById(p1().getBaseIndex().getId()))
                .sortedTabletInfoList.get(0).manifestRoot = null;
        Assertions.assertEquals("NO_MANIFEST", RestoreReuseJudge.firstRejectIncremental(in));
        in = incrementalInput(CheckLevel.FULL);
        in.backupPartition.getIdx(tbl2.getIndexNameById(p1().getBaseIndex().getId())).logicalDigests.clear();
        Assertions.assertEquals("NO_DIGEST", RestoreReuseJudge.firstRejectIncremental(in));

        // small partition, unhealthy replica
        in = incrementalInput(CheckLevel.FULL);
        in.minPartitionBytes = 1000000;
        Assertions.assertEquals("TOO_SMALL", RestoreReuseJudge.firstRejectIncremental(in));
        in = incrementalInput(CheckLevel.FULL);
        Replica replica = p1().getBaseIndex().getTablets().get(0).getReplicas().get(0);
        replica.updateVersionForRestore(V1 - 1);
        Assertions.assertEquals("REPLICA_UNHEALTHY", RestoreReuseJudge.firstRejectIncremental(in));
        replica.updateVersionForRestore(V1);

        // the relation flag is restored, the plain chain still looks at the versions
        in = incrementalInput(CheckLevel.FULL);
        RestoreReuseJudge.firstRejectIncremental(in);
        Assertions.assertFalse(in.relationOnly);
    }

    @Test
    public void testCompareIncrementalTablet() {
        LogicalDigestInfo ok = LogicalDigestInfo.of(1, SIG, "aa");
        ok.prefixVerdict = "OK";
        Assertions.assertNull(RestoreReuseJudge.compareIncrementalTablet(Lists.newArrayList(ok, ok)));
        LogicalDigestInfo bad = LogicalDigestInfo.of(1, SIG, "aa");
        bad.prefixVerdict = "NOT_BOUNDARY";
        Assertions.assertEquals(RestoreReuseResult.DOWNLOAD_NOT_BOUNDARY,
                RestoreReuseJudge.compareIncrementalTablet(Lists.newArrayList(ok, bad)));
        bad.prefixVerdict = "MISMATCH";
        Assertions.assertEquals(RestoreReuseResult.DOWNLOAD_ROOT_MISMATCH,
                RestoreReuseJudge.compareIncrementalTablet(Lists.newArrayList(ok, bad)));
        bad.prefixVerdict = "SCHEMA_MISMATCH";
        Assertions.assertEquals(RestoreReuseResult.DOWNLOAD_SCHEMA_MISMATCH,
                RestoreReuseJudge.compareIncrementalTablet(Lists.newArrayList(bad, ok)));
        bad.prefixVerdict = "ERROR";
        Assertions.assertEquals(RestoreReuseResult.DOWNLOAD_PREFIX_ERROR,
                RestoreReuseJudge.compareIncrementalTablet(Lists.newArrayList(ok, bad)));
        // an old backend: no verdict
        Assertions.assertEquals(RestoreReuseResult.DOWNLOAD_DIGEST_ERROR,
                RestoreReuseJudge.compareIncrementalTablet(Lists.newArrayList(LogicalDigestInfo.of(1, SIG, "aa"))));
        Assertions.assertEquals(RestoreReuseResult.DOWNLOAD_DIGEST_ERROR,
                RestoreReuseJudge.compareIncrementalTablet(Lists.newArrayList(
                        LogicalDigestInfo.none(LogicalDigestInfo.REASON_ERROR))));
        Assertions.assertEquals(RestoreReuseResult.DOWNLOAD_DIGEST_ERROR,
                RestoreReuseJudge.compareIncrementalTablet(Lists.newArrayList()));
    }

    @Test
    public void testSwitchOffKeepsTheBehaviorOfALaggingPartition() {
        // the backup is ahead, but the switch is off: no digest task, everything is downloaded as before
        Config.enable_restore_incremental_append = false;
        RestoreJob job = newJob(incrementalJobInfo(), "full");
        Deencapsulation.invoke(job, "checkAndPrepareMeta");
        Assertions.assertTrue(job.getStatus().ok());
        Deencapsulation.invoke(job, "allReplicasCreated");
        Assertions.assertEquals(RestoreJobState.SNAPSHOTING, job.getState());
        Assertions.assertTrue(digestTasks(submitted).isEmpty());
        Assertions.assertTrue(submitted.stream().noneMatch(t -> t instanceof SnapshotTask
                && ((SnapshotTask) t).isRestoreIncremental()));
        Assertions.assertTrue(job.getReuseResult() == null || job.getReuseResult().getDecisions().isEmpty());
        Assertions.assertEquals(2, versionInfo(job).size());
        // reuse is off too: also nothing
        Config.enable_restore_incremental_append = true;
        Config.enable_restore_partition_reuse = false;
        submitted.clear();
        job = newJob(incrementalJobInfo(), "full");
        Deencapsulation.invoke(job, "checkAndPrepareMeta");
        Deencapsulation.invoke(job, "allReplicasCreated");
        Assertions.assertEquals(RestoreJobState.SNAPSHOTING, job.getState());
        Assertions.assertTrue(digestTasks(submitted).isEmpty());
    }

    @Test
    public void testIncrementalFlowToCommit() throws Exception {
        RestoreJob job = prepareIncrementalJob("full");
        toVerifying(job);
        List<RestoreDigestTask> tasks = newDigestTasks();
        Assertions.assertEquals(replicasOf(p1()) + replicasOf(p2()), tasks.size());
        for (RestoreDigestTask task : tasks) {
            // the digest is compared at the local version, with the decomposed digest of the backup
            Assertions.assertEquals(V1, task.getVersion());
            Assertions.assertNotNull(task.getPrefixSource());
            Assertions.assertEquals(V2, task.getPrefixSource().getEndVersion());
            Assertions.assertEquals("p" + task.getTabletId(), task.getPrefixSource().getRoot());
            Assertions.assertTrue(task.getPrefixSource().isSetRemoteTabletSnapshot());
            Assertions.assertTrue(task.toThrift().isSetPrefixSource());
        }
        reportPrefix(job, tasks, Maps.newHashMap());
        waitDigests(job);
        Assertions.assertEquals(RestoreJobState.SNAPSHOTING, job.getState());

        RestoreReuseResult result = job.getReuseResult();
        Assertions.assertEquals(0, result.getKeptPartitions());
        Assertions.assertEquals(2, result.getIncrementalPartitions());
        Assertions.assertEquals(0, result.getDownloadedPartitions());
        for (RestoreReuseResult.Decision decision : result.getDecisions()) {
            Assertions.assertTrue(decision.incremental);
            Assertions.assertFalse(decision.kept);
            Assertions.assertEquals(V1, decision.version);
            Assertions.assertEquals(V2, decision.targetVersion);
            Assertions.assertEquals(RestoreReuseResult.INCREMENTAL_VERIFIED, decision.reason);
        }
        assertIncrementalLocalBytes(job, result);
        // the partitions are still restored: version info, file mapping, and the lineage is invalidated
        Assertions.assertEquals(2, versionInfo(job).size());
        Assertions.assertEquals(V2, versionInfo(job).get(tbl2.getId(), p1().getId()));
        Assertions.assertEquals(replicasOf(p1()) + replicasOf(p2()), fileMapping(job).getMapping().size());
        Assertions.assertNull(p1().getRestoreLineage());
        // the snapshot is an empty dir to download the increment into
        List<SnapshotTask> snapshotTasks = submitted.stream().filter(t -> t instanceof SnapshotTask)
                .map(t -> (SnapshotTask) t).collect(Collectors.toList());
        Assertions.assertEquals(replicasOf(p1()) + replicasOf(p2()), snapshotTasks.size());
        Assertions.assertTrue(snapshotTasks.stream().allMatch(SnapshotTask::isRestoreIncremental));
        Assertions.assertTrue(snapshotTasks.get(0).toThrift().isRestoreIncremental());
        for (SnapshotTask task : snapshotTasks) {
            TFinishTaskRequest report = new TFinishTaskRequest();
            report.setTaskStatus(new TStatus(TStatusCode.OK));
            report.setSnapshotPath("/path/snapshot");
            Assertions.assertTrue(job.finishTabletSnapshotTask(task, report));
        }

        // the download asks for the increment (V1, V2] of every tablet
        submitted.clear();
        Deencapsulation.invoke(job, "downloadSnapshots");
        List<DownloadTask> downloads = submitted.stream().filter(t -> t instanceof DownloadTask)
                .map(t -> (DownloadTask) t).collect(Collectors.toList());
        int remotes = 0;
        for (DownloadTask download : downloads) {
            for (TRemoteTabletSnapshot remote : download.getRemoteTabletSnapshots()) {
                Assertions.assertTrue(remote.isSetIncremental());
                Assertions.assertEquals(V1, remote.getIncremental().getBaseVersion());
                Assertions.assertEquals(V2, remote.getIncremental().getEndVersion());
                Assertions.assertTrue(remote.isSetManifestRoot());
                remotes++;
            }
        }
        Assertions.assertEquals(replicasOf(p1()) + replicasOf(p2()), remotes);

        // the move appends instead of replacing
        submitted.clear();
        Deencapsulation.setField(job, "state", RestoreJobState.COMMIT);
        job.commit();
        List<DirMoveTask> moves = submitted.stream().filter(t -> t instanceof DirMoveTask)
                .map(t -> (DirMoveTask) t).collect(Collectors.toList());
        Assertions.assertEquals(replicasOf(p1()) + replicasOf(p2()), moves.size());
        for (DirMoveTask move : moves) {
            Assertions.assertEquals(V1, move.getIncrementalRange().getBaseVersion());
            Assertions.assertEquals(V2, move.getIncrementalRange().getEndVersion());
            Assertions.assertTrue(move.toThrift().isSetIncremental());
            // the non atomic restore does not load from a local source: no evidence is judged by the backend
            Assertions.assertFalse(move.isLocalSourceLoaded());
            Assertions.assertFalse(move.toThrift().isSetLocalSourceLoaded());
        }

        // persisted and replayed
        RestoreJob replayed = writeAndRead(job);
        Assertions.assertEquals(2, replayed.getReuseResult().getIncrementalPartitions());
        Assertions.assertEquals(V2, replayed.getReuseResult().getIncrementalDecisions().get(0).targetVersion);

        // commit: the backup version, as a whole download, and the lineage of this restore
        Status st = job.allTabletCommitted(false);
        Assertions.assertTrue(st.ok(), st.toString());
        for (Partition part : Lists.newArrayList(p1(), p2())) {
            Assertions.assertEquals(V2, part.getVisibleVersion());
            Assertions.assertEquals(V2, part.getRestoreLineage().getSrcVersion());
            for (Replica replica : part.getBaseIndex().getTablets().get(0).getReplicas()) {
                Assertions.assertEquals(V2, replica.getVersion());
            }
        }
        Assertions.assertEquals(OlapTableState.NORMAL, tbl2.getState());
        // observable
        String estimate = job.getFullInfo().stream().filter(x -> x.contains("incremental_partitions"))
                .findFirst().orElse("");
        Assertions.assertTrue(estimate.contains("\"incremental_partitions\":2"), estimate);
        Assertions.assertTrue(job.getReuseResult().toString().contains("INCREMENTAL_VERIFIED"));
    }

    @Test
    public void testIncrementalFailuresDownloadThePartition() {
        RestoreJob job = prepareIncrementalJob("full");
        toVerifying(job);
        List<RestoreDigestTask> tasks = newDigestTasks();
        Map<Long, String> verdicts = Maps.newHashMap();
        verdicts.put(p2().getId(), "NOT_BOUNDARY");
        reportPrefix(job, tasks, verdicts);
        waitDigests(job);
        RestoreReuseResult result = job.getReuseResult();
        Assertions.assertEquals(1, result.getIncrementalPartitions());
        Assertions.assertNotNull(result.getIncremental(tbl2.getId(), p1().getId()));
        Assertions.assertNull(result.getIncremental(tbl2.getId(), p2().getId()));
        Assertions.assertEquals(1, result.getDownloadedPartitions());
        for (RestoreReuseResult.Decision decision : result.getDecisions()) {
            if (decision.partitionId == p2().getId()) {
                Assertions.assertFalse(decision.incremental);
                Assertions.assertFalse(decision.kept);
                Assertions.assertEquals(RestoreReuseResult.DOWNLOAD_NOT_BOUNDARY, decision.reason);
                Assertions.assertEquals(V2, decision.version);
            }
        }
        // p2 is a plain download: its snapshot is a real snapshot
        for (AgentTask task : submitted) {
            if (task instanceof SnapshotTask) {
                Assertions.assertEquals(task.getPartitionId() == p1().getId(),
                        ((SnapshotTask) task).isRestoreIncremental());
            }
        }
    }

    // Merge-on-read and merge-on-write tables are candidates of the incremental append. For merge-on-read the
    // backup holds whole digests at its most recent boundaries only: a local version outside them is reported as
    // NOT_BOUNDARY by the BE, and that partition is downloaded whole.
    @Test
    public void testMergeOnReadTableIsAnIncrementalCandidate() {
        assertUniqueTableIsACandidate(false);
    }

    @Test
    public void testMergeOnWriteTableIsAnIncrementalCandidate() {
        assertUniqueTableIsACandidate(true);
    }

    private void assertUniqueTableIsACandidate(boolean mow) {
        Deencapsulation.setField(tbl2, "keysType", KeysType.UNIQUE_KEYS);
        tbl2.setEnableUniqueKeyMergeOnWrite(mow);
        RestoreJob job = prepareIncrementalJob("full");
        toVerifying(job);
        List<RestoreDigestTask> tasks = newDigestTasks();
        Assertions.assertEquals(2, tasks.stream().map(RestoreDigestTask::getPartitionId).distinct().count());
        Map<Long, String> verdicts = Maps.newHashMap();
        verdicts.put(p2().getId(), "NOT_BOUNDARY");
        reportPrefix(job, tasks, verdicts);
        waitDigests(job);
        RestoreReuseResult result = job.getReuseResult();
        String what = mow ? "mow" : "mor";
        Assertions.assertEquals(1, result.getIncrementalPartitions(), what);
        Assertions.assertNotNull(result.getIncremental(tbl2.getId(), p1().getId()), what);
        Assertions.assertNull(result.getIncremental(tbl2.getId(), p2().getId()), what);
        Assertions.assertEquals(1, result.getDownloadedPartitions(), what);
        for (RestoreReuseResult.Decision decision : result.getDecisions()) {
            if (decision.partitionId == p2().getId()) {
                Assertions.assertFalse(decision.incremental, what);
                Assertions.assertEquals(RestoreReuseResult.DOWNLOAD_NOT_BOUNDARY, decision.reason, what);
            }
        }
    }

    private void assertWholeDownloadFor(String verdict) {
        RestoreJob other = prepareIncrementalJob("full");
        toVerifying(other);
        for (RestoreDigestTask task : newDigestTasks()) {
            TFinishTaskRequest report = verdict == null ? failedReport() : prefixReport(verdict);
            other.finishRestoreDigestTask(task, report);
        }
        waitDigests(other);
        Assertions.assertEquals(0, other.getReuseResult().getIncrementalPartitions(), String.valueOf(verdict));
        Assertions.assertEquals(2, other.getReuseResult().getDownloadedPartitions(), String.valueOf(verdict));
        Assertions.assertTrue(submitted.stream().noneMatch(t -> t instanceof SnapshotTask
                && ((SnapshotTask) t).isRestoreIncremental()));
    }

    @Test
    public void testIncrementalMismatchDownloads() {
        assertWholeDownloadFor("MISMATCH");
    }

    @Test
    public void testIncrementalErrorDownloads() {
        assertWholeDownloadFor("ERROR");
    }

    @Test
    public void testIncrementalSchemaMismatchDownloads() {
        assertWholeDownloadFor("SCHEMA_MISMATCH");
    }

    @Test
    public void testIncrementalFailedTaskDownloads() {
        assertWholeDownloadFor(null);
    }

    @Test
    public void testIncrementalIsVerifiedEvenInTheSampleLevel() {
        Config.restore_reuse_sample_ratio = 0.0;
        RestoreJob job = prepareIncrementalJob("sample");
        toVerifying(job);
        // all partitions are digested, the sample does not apply to the increments
        Assertions.assertEquals(replicasOf(p1()) + replicasOf(p2()), newDigestTasks().size());
    }

    @Test
    public void testIncrementalRecheckBeforeCommit() {
        RestoreJob job = prepareIncrementalJob("full");
        toVerifying(job);
        reportPrefix(job, newDigestTasks(), Maps.newHashMap());
        waitDigests(job);
        Assertions.assertEquals(2, job.getReuseResult().getIncrementalPartitions());
        // a write that should never happen
        p1().updateVersionForRestore(V1 + 1);
        Status st = job.allTabletCommitted(false);
        Assertions.assertFalse(st.ok());
        Assertions.assertTrue(st.getErrMsg().contains("changed from " + V1 + " to " + (V1 + 1)), st.getErrMsg());
        Assertions.assertTrue(st.getErrMsg().contains("appending the increment"), st.getErrMsg());
        // before any tablet is moved
        Deencapsulation.setField(job, "state", RestoreJobState.COMMIT);
        job.commit();
        Assertions.assertFalse(job.getStatus().ok());
        Assertions.assertTrue(submitted.stream().noneMatch(t -> t instanceof DirMoveTask));
    }

    @Test
    public void testIncrementalCancelWhileVerifyingChangesNothingLocal() {
        RestoreJob job = prepareIncrementalJob("full");
        toVerifying(job);
        newDigestTasks();
        job.cancel();
        Assertions.assertEquals(RestoreJobState.CANCELLED, job.getState());
        Assertions.assertEquals(V1, p1().getVisibleVersion());
        Assertions.assertNotNull(p1().getRestoreLineage());
        Assertions.assertEquals(OlapTableState.NORMAL, tbl2.getState());
    }

    @Test
    public void testIncrementalDownloadStatsAreObservable() {
        RestoreDownloadStats stats = new RestoreDownloadStats();
        org.apache.doris.thrift.TDownloadStats reported = new org.apache.doris.thrift.TDownloadStats();
        reported.setDownloadedFiles(4);
        reported.setDownloadedBytes(400);
        reported.setTabletsIncremental(2);
        reported.setIncrementalFiles(4);
        reported.setIncrementalBytes(400);
        stats.add(reported, 2);
        Assertions.assertEquals(400, stats.getIncrementalBytes());
        Assertions.assertEquals(2, stats.getIncrementalTablets());
        String json = stats.toJson(2, 0);
        Assertions.assertTrue(json.contains("\"incremental_tablets\":2"), json);
        Assertions.assertTrue(json.contains("\"incremental_bytes\":400"), json);
        // off: unchanged output for the jobs without an increment
        Assertions.assertFalse(new RestoreDownloadStats().toJson(0, 0).contains("incremental"));
    }

    // ---------------------------------------------------------------------------------------------
    // atomic restore: the partitions of the table being replaced are the source of the data
    // ---------------------------------------------------------------------------------------------

    // The backup kept on local: where the snapshot of every tablet is, for the download.
    private BackupJobInfo localBackupJobInfo() {
        BackupJobInfo info = selfBackupJobInfo();
        info.extraInfo = new BackupJobInfo.ExtraInfo();
        info.extraInfo.token = "token";
        BackupJobInfo.ExtraInfo.NetworkAddrss addr = new BackupJobInfo.ExtraInfo.NetworkAddrss();
        addr.ip = "127.0.0.1";
        addr.port = 8040;
        info.extraInfo.beNetworkMap.put(1L, addr);
        for (BackupPartitionInfo partInfo : info.getOlapTableInfo(CatalogMocker.TEST_TBL2_NAME).partitions.values()) {
            for (BackupIndexInfo idxInfo : partInfo.indexes.values()) {
                for (BackupTabletInfo tablet : idxInfo.sortedTabletInfoList) {
                    info.tabletBeMap.put(tablet.id, 1L);
                    info.tabletSnapshotPathMap.put(tablet.id, "/snapshot/ss");
                }
            }
        }
        return info;
    }

    // the backup meta of test_tbl2 with the partitions at the version
    private BackupMeta backupMetaAt(long version) {
        OlapTable remoteTbl = tbl2.selectiveCopy(null, IndexExtState.VISIBLE, true);
        for (Partition part : remoteTbl.getPartitions()) {
            part.updateVersionForRestore(version);
        }
        return new BackupMeta(Lists.newArrayList(remoteTbl), Lists.<Resource>newArrayList());
    }

    private RestoreJob newAtomicJob(BackupJobInfo info, String level) {
        long version = info.getOlapTableInfo(CatalogMocker.TEST_TBL2_NAME).partitions.values().iterator().next()
                .version;
        RestoreJob job = new RestoreJob("restore_label", "2024-01-01 00:00:00", db.getId(), db.getFullName(), info,
                false, new ReplicaAllocation((short) 3), 100000, -1, false, false, false, false, false, false,
                true /* atomic */, false, env, Repository.KEEP_ON_LOCAL_REPO_ID, backupMetaAt(version));
        job.setReuseCheckLevel(level);
        return job;
    }

    private RestoreJob prepareAtomicJob(BackupJobInfo info, String level) {
        Config.enable_restore_atomic_reuse = true;
        RestoreJob job = newAtomicJob(info, level);
        Deencapsulation.invoke(job, "checkAndPrepareMeta");
        Assertions.assertTrue(job.getStatus().ok(), job.getStatus().toString());
        Assertions.assertEquals(RestoreJobState.CREATING, job.getState());
        return job;
    }

    // the staging table of an atomic restore, registered under the temp name
    private OlapTable staging(RestoreJob job) {
        List<org.apache.doris.catalog.Table> restored = Deencapsulation.getField(job, "restoredTbls");
        Assertions.assertEquals(1, restored.size());
        return (OlapTable) restored.get(0);
    }

    private static List<SnapshotTask> snapshotTasks(List<AgentTask> tasks) {
        return tasks.stream().filter(t -> t instanceof SnapshotTask).map(t -> (SnapshotTask) t)
                .collect(Collectors.toList());
    }

    private static List<Long> tabletIds(Partition part) {
        List<Long> ids = Lists.newArrayList();
        for (MaterializedIndex index : part.getMaterializedIndices(IndexExtState.VISIBLE)) {
            for (Tablet tablet : index.getTablets()) {
                ids.add(tablet.getId());
            }
        }
        return ids;
    }

    // Finish the snapshot tasks, each reporting what it made from the local tablet.
    private void finishSnapshots(RestoreJob job, List<SnapshotTask> tasks, long linkedBytes, long copiedBytes) {
        for (SnapshotTask task : tasks) {
            TFinishTaskRequest report = new TFinishTaskRequest();
            report.setTaskStatus(new TStatus(TStatusCode.OK));
            report.setSnapshotPath("/path/snapshot/" + task.getTabletId());
            if (task.isRestoreLocalSource()) {
                TDownloadStats stats = new TDownloadStats();
                stats.setLocalSourceTablets(1);
                stats.setLocalSourceLinkedFiles(linkedBytes > 0 ? 2 : 0);
                stats.setLocalSourceLinkedBytes(linkedBytes);
                stats.setLocalSourceCopiedFiles(copiedBytes > 0 ? 3 : 0);
                stats.setLocalSourceCopiedBytes(copiedBytes);
                report.setDownloadStats(stats);
            }
            Assertions.assertTrue(job.finishTabletSnapshotTask(task, report));
        }
    }

    @Test
    public void testAtomicRestoreIsNotReusedByDefault() {
        Config.enable_restore_atomic_reuse = false;
        RestoreJob job = newAtomicJob(selfBackupJobInfo(), "full");
        Deencapsulation.invoke(job, "checkAndPrepareMeta");
        Assertions.assertTrue(job.getStatus().ok(), job.getStatus().toString());
        Deencapsulation.invoke(job, "allReplicasCreated");
        // no candidate, no digest task: straight to the snapshots, which are as before
        Assertions.assertEquals(RestoreJobState.SNAPSHOTING, job.getState());
        Assertions.assertNull(job.getReuseResult());
        Assertions.assertTrue(digestTasks(submitted).isEmpty());
        List<SnapshotTask> snapshots = snapshotTasks(submitted);
        Assertions.assertFalse(snapshots.isEmpty());
        for (SnapshotTask task : snapshots) {
            Assertions.assertFalse(task.isRestoreLocalSource());
            Assertions.assertFalse(task.isRestoreIncremental());
            Assertions.assertFalse(task.toThrift().isSetVersion());
            // the base tablet is still used by the download, as before
            Assertions.assertTrue(task.toThrift().isSetRefTabletId());
        }
        // with the switch off the judge still says why
        RestoreReuseJudge.Input in = input(CheckLevel.FULL);
        in.atomicRestore = true;
        Assertions.assertEquals("ATOMIC_RESTORE", RestoreReuseJudge.firstReject(in));
        Assertions.assertEquals(RestoreReuseShadowStats.Unsupported.ATOMIC_RESTORE,
                RestoreReuseShadowStats.checkUnsupported(true, tbl2, p1()));
        Assertions.assertFalse(job.getFullInfo().toString().contains("kept_atomic"));
    }

    @Test
    public void testAtomicCandidatesAreThePartitionsOfTheTableBeingReplaced() {
        RestoreJob job = prepareAtomicJob(selfBackupJobInfo(), "full");
        OlapTable stagingTbl = staging(job);
        Assertions.assertNotEquals(tbl2.getId(), stagingTbl.getId());
        // the table being replaced is not touched: not in the RESTORE state, no lineage change
        Assertions.assertEquals(OlapTableState.NORMAL, tbl2.getState());
        Assertions.assertNotNull(p1().getRestoreLineage());
        Assertions.assertNotNull(p2().getRestoreLineage());
        // nothing is registered as an overwritten partition
        Assertions.assertTrue(versionInfo(job).isEmpty());
        RestoreReuseResult result = job.getReuseResult();
        Assertions.assertNotNull(result);
        Assertions.assertTrue(result.getRejected().isEmpty(), result.getRejected().toString());
        toVerifying(job);
        List<RestoreDigestTask> tasks = newDigestTasks();
        // one digest task for each replica of the table being replaced, at the version of the backup
        Assertions.assertEquals(replicasOf(p1()) + replicasOf(p2()), tasks.size());
        for (RestoreDigestTask task : tasks) {
            Assertions.assertEquals(tbl2.getId(), task.getTableId());
            Assertions.assertEquals(V1, task.getVersion());
            Assertions.assertTrue(tabletIds(p1()).contains(task.getTabletId())
                    || tabletIds(p2()).contains(task.getTabletId()));
        }
        Assertions.assertTrue(submitted.stream().noneMatch(t -> t instanceof SnapshotTask));
    }

    @Test
    public void testAtomicKeepFlowToCommit() throws Exception {
        RestoreJob job = prepareAtomicJob(selfBackupJobInfo(), "full");
        OlapTable stagingTbl = staging(job);
        Partition stagingP1 = stagingTbl.getPartition(p1().getName());
        Partition stagingP2 = stagingTbl.getPartition(p2().getName());
        toVerifying(job);
        reportAll(job, newDigestTasks(), Maps.newHashMap());
        waitDigests(job);
        Assertions.assertEquals(RestoreJobState.SNAPSHOTING, job.getState());

        RestoreReuseResult result = job.getReuseResult();
        Assertions.assertEquals(2, result.getKeptPartitions());
        Assertions.assertEquals(2, result.getKeptAtomicPartitions());
        Assertions.assertEquals(0, result.getIncrementalPartitions());
        for (RestoreReuseResult.Decision decision : result.getDecisions()) {
            Assertions.assertTrue(decision.kept);
            Assertions.assertTrue(decision.atomic);
            // the decision is about the table being replaced, the tasks work on the staging table
            Assertions.assertEquals(tbl2.getId(), decision.tableId);
            Assertions.assertEquals(stagingTbl.getId(), decision.stagingTableId);
            Partition local = tbl2.getPartition(decision.partitionId);
            Assertions.assertEquals(stagingTbl.getPartition(local.getName()).getId(), decision.stagingPartitionId);
            Assertions.assertEquals(V1, decision.version);
            Assertions.assertEquals(RestoreReuseResult.KEPT_DIGEST_VERIFIED, decision.reason);
        }
        Assertions.assertNotNull(result.getKept(stagingTbl.getId(), stagingP1.getId()));
        Assertions.assertNull(result.getKept(tbl2.getId(), p1().getId()));
        // the lineage of the table being replaced is not invalidated (the table is dropped when replaced)
        Assertions.assertNotNull(p1().getRestoreLineage());

        // the snapshot tasks of the staging tablets are made from the local tablets at the version of the backup
        List<SnapshotTask> snapshots = snapshotTasks(submitted);
        Assertions.assertEquals(replicasOf(stagingP1) + replicasOf(stagingP2), snapshots.size());
        List<Long> localTablets = Lists.newArrayList(tabletIds(p1()));
        localTablets.addAll(tabletIds(p2()));
        for (SnapshotTask task : snapshots) {
            Assertions.assertTrue(task.isRestoreLocalSource());
            Assertions.assertFalse(task.isRestoreIncremental());
            Assertions.assertEquals(V1, task.getVersion());
            Assertions.assertEquals(stagingTbl.getId(), task.getTableId());
            org.apache.doris.thrift.TSnapshotRequest request = task.toThrift();
            Assertions.assertTrue(request.isRestoreLocalSource());
            Assertions.assertEquals(V1, request.getVersion());
            Assertions.assertTrue(localTablets.contains(request.getRefTabletId()));
            Assertions.assertNotEquals(task.getTabletId(), request.getRefTabletId());
        }
        finishSnapshots(job, snapshots, 1000, 0);

        // nothing is downloaded
        Deencapsulation.invoke(job, "waitingAllSnapshotsFinished");
        Assertions.assertEquals(RestoreJobState.DOWNLOAD, job.getState());
        submitted.clear();
        Deencapsulation.invoke(job, "downloadSnapshots");
        Assertions.assertEquals(RestoreJobState.DOWNLOADING, job.getState());
        Assertions.assertTrue(submitted.stream().noneMatch(t -> t instanceof DownloadTask), submitted.toString());
        Deencapsulation.invoke(job, "waitingAllDownloadFinished");
        Assertions.assertEquals(RestoreJobState.COMMIT, job.getState());

        // the move of every staging tablet loads its snapshot as it is
        submitted.clear();
        job.commit();
        List<DirMoveTask> moves = submitted.stream().filter(t -> t instanceof DirMoveTask)
                .map(t -> (DirMoveTask) t).collect(Collectors.toList());
        Assertions.assertEquals(replicasOf(stagingP1) + replicasOf(stagingP2), moves.size());
        for (DirMoveTask move : moves) {
            Assertions.assertNull(move.getIncrementalRange());
        }

        // observable: the partitions kept, and the local bytes by the snapshots
        String download = job.getFullInfo().stream().filter(x -> x.contains("atomic_local")).findFirst().orElse("");
        long snapshotsMade = snapshots.size();
        Assertions.assertTrue(download.contains("\"local_tablets\":" + snapshotsMade), download);
        Assertions.assertTrue(download.contains("\"linked_bytes\":" + 1000 * snapshotsMade), download);
        Assertions.assertTrue(download.contains("\"copied_bytes\":0"), download);
        Assertions.assertTrue(download.contains("\"kept_atomic_bytes\":" + result.getKeptAtomicBytesAllReplicas()),
                download);
        Assertions.assertTrue(result.getKeptAtomicBytesAllReplicas() > 0);
        // the tablets are not counted as downloaded: every replica to download is reported
        Assertions.assertTrue(download.contains("\"not_reported\":0"), download);
        String estimate = job.getFullInfo().stream().filter(x -> x.contains("kept_atomic\"")).findFirst().orElse("");
        Assertions.assertTrue(estimate.contains("\"kept_atomic\":2"), estimate);
        Assertions.assertTrue(estimate.contains("\"incremental_atomic\":0"), estimate);
        Assertions.assertTrue(estimate.contains("\"kept_partitions\":2"), estimate);

        // persisted: the decisions with the staging ids, and the counts of the local snapshots
        RestoreJob replayed = writeAndRead(job);
        RestoreReuseResult.Decision kept = replayed.getReuseResult().getKept(stagingTbl.getId(), stagingP1.getId());
        Assertions.assertNotNull(kept);
        Assertions.assertTrue(kept.atomic);
        Assertions.assertEquals(tbl2.getId(), kept.tableId);
        Assertions.assertEquals(p1().getId(), kept.partitionId);
        Assertions.assertEquals(result.getKeptAtomicBytesAllReplicas(),
                replayed.getReuseResult().getKeptAtomicBytesAllReplicas());
        Assertions.assertEquals(snapshotsMade, replayed.getDownloadStats().getLocalSourceTablets());
        Assertions.assertEquals(1000 * snapshotsMade, replayed.getDownloadStats().getLocalSourceLinkedBytes());

        // the commit replaces the table, which is the staging one at the version of the backup
        for (DirMoveTask move : moves) {
            TFinishTaskRequest report = new TFinishTaskRequest();
            report.setTaskStatus(new TStatus(TStatusCode.OK));
            Assertions.assertTrue(job.finishDirMoveTask(move, report));
        }
        Status st = job.allTabletCommitted(false);
        Assertions.assertTrue(st.ok(), st.toString());
        OlapTable replaced = (OlapTable) db.getTableNullable(CatalogMocker.TEST_TBL2_NAME);
        Assertions.assertEquals(stagingTbl.getId(), replaced.getId());
        Assertions.assertEquals(OlapTableState.NORMAL, replaced.getState());
        for (Partition part : replaced.getPartitions()) {
            Assertions.assertEquals(V1, part.getVisibleVersion());
            Assertions.assertEquals(V1, part.getRestoreLineage().getSrcVersion());
            for (Replica replica : part.getBaseIndex().getTablets().get(0).getReplicas()) {
                Assertions.assertEquals(V1, replica.getVersion());
            }
        }
        Assertions.assertEquals(RestoreJobState.FINISHED, job.getState());
    }

    @Test
    public void testAtomicDigestMismatchDownloadsThePartition() {
        RestoreJob job = prepareAtomicJob(localBackupJobInfo(), "full");
        toVerifying(job);
        List<RestoreDigestTask> tasks = newDigestTasks();
        Map<Long, TFinishTaskRequest> faulty = Maps.newHashMap();
        // one replica of p2 differs
        for (RestoreDigestTask task : tasks) {
            if (task.getPartitionId() == p2().getId()) {
                faulty.put(task.getSignature(), okReport("ff99", SIG, 1));
                break;
            }
        }
        reportAll(job, tasks, faulty);
        waitDigests(job);
        RestoreReuseResult result = job.getReuseResult();
        Assertions.assertEquals(1, result.getKeptPartitions());
        Assertions.assertEquals(1, result.getDownloadedPartitions());
        OlapTable stagingTbl = staging(job);
        Assertions.assertNotNull(result.getKept(stagingTbl.getId(), stagingTbl.getPartition(p1().getName()).getId()));
        Assertions.assertNull(result.getKept(stagingTbl.getId(), stagingTbl.getPartition(p2().getName()).getId()));
        // p2 is a plain restore of the staging partition, as it is without partition level reuse
        for (SnapshotTask task : snapshotTasks(submitted)) {
            boolean ofP1 = task.getPartitionId() == stagingTbl.getPartition(p1().getName()).getId();
            Assertions.assertEquals(ofP1, task.isRestoreLocalSource());
            Assertions.assertEquals(ofP1, task.getVersion() == V1);
            Assertions.assertTrue(task.toThrift().isSetRefTabletId());
        }
        finishSnapshots(job, snapshotTasks(submitted), 0, 500);
        Deencapsulation.invoke(job, "waitingAllSnapshotsFinished");
        submitted.clear();
        Deencapsulation.invoke(job, "downloadSnapshots");
        // only p2 is downloaded
        List<DownloadTask> downloads = submitted.stream().filter(t -> t instanceof DownloadTask)
                .map(t -> (DownloadTask) t).collect(Collectors.toList());
        Assertions.assertFalse(downloads.isEmpty());
        int remotes = 0;
        for (DownloadTask download : downloads) {
            for (TRemoteTabletSnapshot remote : download.getRemoteTabletSnapshots()) {
                Assertions.assertTrue(tabletIds(stagingTbl.getPartition(p2().getName()))
                        .contains(remote.getLocalTabletId()));
                remotes++;
            }
        }
        Assertions.assertEquals(replicasOf(stagingTbl.getPartition(p2().getName())), remotes);
        // the copied bytes are counted apart
        String download = job.getFullInfo().stream().filter(x -> x.contains("atomic_local")).findFirst().orElse("");
        Assertions.assertTrue(download.contains("\"linked_bytes\":0"), download);
        Assertions.assertTrue(download.contains("\"copied_bytes\":" + 500 * replicasOf(p1())), download);
    }

    @Test
    public void testAtomicNotBoundIsDownloaded() {
        RestoreJob job = prepareAtomicJob(selfBackupJobInfo(), "full");
        OlapTable stagingTbl = staging(job);
        // the binding of the tablets as the job made it
        Map<Long, RestoreJob.TabletRef> bases = Maps.newHashMap();
        for (Partition part : stagingTbl.getPartitions()) {
            Partition local = tbl2.getPartition(part.getName());
            for (MaterializedIndex stagingIdx : part.getMaterializedIndices(IndexExtState.VISIBLE)) {
                MaterializedIndex localIdx = stagingIdx.getId() == stagingTbl.getBaseIndexId() ? local.getBaseIndex()
                        : local.getIndex(tbl2.getIndexIdByName(stagingTbl.getIndexNameById(stagingIdx.getId())));
                for (int i = 0; i < stagingIdx.getTablets().size(); i++) {
                    bases.put(stagingIdx.getTablets().get(i).getId(), new RestoreJob.TabletRef(
                            localIdx.getTablets().get(i).getId(), 1));
                }
            }
        }
        Assertions.assertNull(RestoreJob.checkAtomicBinding(tbl2, p1(), stagingTbl, bases));
        Assertions.assertNull(RestoreJob.checkAtomicBinding(tbl2, p2(), stagingTbl, bases));

        // a staging tablet without a base tablet
        Tablet stagingTablet = stagingTbl.getPartition(p1().getName()).getBaseIndex().getTablets().get(0);
        RestoreJob.TabletRef ref = bases.remove(stagingTablet.getId());
        Assertions.assertEquals("ATOMIC_NOT_BOUND_TABLET", RestoreJob.checkAtomicBinding(tbl2, p1(), stagingTbl, bases));
        Assertions.assertNull(RestoreJob.checkAtomicBinding(tbl2, p2(), stagingTbl, bases));
        // a base tablet that is another one
        bases.put(stagingTablet.getId(), new RestoreJob.TabletRef(ref.tabletId + 1, 1));
        Assertions.assertEquals("ATOMIC_NOT_BOUND_TABLET", RestoreJob.checkAtomicBinding(tbl2, p1(), stagingTbl, bases));
        bases.put(stagingTablet.getId(), ref);
        Assertions.assertNull(RestoreJob.checkAtomicBinding(tbl2, p1(), stagingTbl, bases));

        // the replicas are not on the same backends
        Replica stagingReplica = stagingTablet.getReplicas().get(0);
        long backend = stagingReplica.getBackendIdWithoutException();
        stagingReplica.setBackendId(backend + 100);
        Assertions.assertEquals("ATOMIC_NOT_BOUND_REPLICA", RestoreJob.checkAtomicBinding(tbl2, p1(), stagingTbl, bases));
        // the order matters: the replica of the same position
        stagingReplica.setBackendId(stagingTablet.getReplicas().get(1).getBackendIdWithoutException());
        Assertions.assertEquals("ATOMIC_NOT_BOUND_REPLICA", RestoreJob.checkAtomicBinding(tbl2, p1(), stagingTbl, bases));
        stagingReplica.setBackendId(backend);
        Assertions.assertNull(RestoreJob.checkAtomicBinding(tbl2, p1(), stagingTbl, bases));
        // a different number of replicas
        Replica removed = stagingTablet.getReplicas().get(2);
        stagingTablet.deleteReplica(removed);
        Assertions.assertEquals("ATOMIC_NOT_BOUND_REPLICA", RestoreJob.checkAtomicBinding(tbl2, p1(), stagingTbl, bases));
        stagingTablet.addReplica(removed, true);

        // no staging partition of the name
        String p2Name = p2().getName();
        p2().setName("p2_renamed");
        Assertions.assertEquals("ATOMIC_NOT_BOUND_PARTITION", RestoreJob.checkAtomicBinding(tbl2, p2(), stagingTbl, bases));
        p2().setName(p2Name);

        // the job: a table that is not bound (schema changed) is not a candidate, the judge says why
        job.selectReuseCandidates(db, Maps.newHashMap(), bases);
        Assertions.assertEquals(2L, (long) job.getReuseResult().getRejected().get("ATOMIC_NOT_BOUND_TABLE"));
        Assertions.assertTrue(job.getReuseResult().getDecisions().isEmpty());
        Map<String, OlapTable> bound = Maps.newHashMap();
        bound.put(CatalogMocker.TEST_TBL2_NAME, stagingTbl);
        bases.remove(stagingTablet.getId());
        job.selectReuseCandidates(db, bound, bases);
        Assertions.assertEquals(1L, (long) job.getReuseResult().getRejected().get("ATOMIC_NOT_BOUND_TABLET"));
    }

    @Test
    public void testAtomicConditions() {
        Config.enable_restore_atomic_reuse = true;
        RestoreReuseJudge.Input in = input(CheckLevel.FULL);
        tbl2.setState(OlapTableState.NORMAL);
        in.atomicRestore = true;
        // the table being replaced is not forbidden to write
        Assertions.assertNull(RestoreReuseJudge.firstReject(in));
        in.allowLoad = true;
        Assertions.assertEquals("ALLOW_LOAD", RestoreReuseJudge.firstReject(in));
        in.allowLoad = false;
        in.atomicReject = "ATOMIC_NOT_BOUND_REPLICA";
        Assertions.assertEquals("ATOMIC_NOT_BOUND_REPLICA", RestoreReuseJudge.firstReject(in));
        in.atomicReject = null;
        tbl2.setState(OlapTableState.SCHEMA_CHANGE);
        Assertions.assertEquals("TABLE_STATE_SCHEMA_CHANGE", RestoreReuseJudge.firstReject(in));
        tbl2.setState(OlapTableState.NORMAL);
        // the other conditions are the same
        p1().setRestoreLineage(null);
        Assertions.assertEquals("L0_NO_LINEAGE", RestoreReuseJudge.firstReject(in));
        p1().setRestoreLineage(lineage(in.jobInfo, p1()));
        Assertions.assertNull(RestoreReuseJudge.firstReject(in));
        Assertions.assertEquals(RestoreReuseShadowStats.Unsupported.NONE,
                RestoreReuseShadowStats.checkUnsupported(true, tbl2, p1()));
        Deencapsulation.setField(tbl2, "keysType", KeysType.AGG_KEYS);
        Assertions.assertEquals("AGGREGATE_TABLE", RestoreReuseJudge.firstReject(in));
    }

    @Test
    public void testAtomicIncrementalFlow() throws Exception {
        Config.enable_restore_incremental_append = true;
        RestoreJob job = prepareAtomicJob(incrementalJobInfo(), "full");
        OlapTable stagingTbl = staging(job);
        toVerifying(job);
        List<RestoreDigestTask> tasks = newDigestTasks();
        Assertions.assertEquals(replicasOf(p1()) + replicasOf(p2()), tasks.size());
        for (RestoreDigestTask task : tasks) {
            // at the local version, of the table being replaced, with the decomposed digest of the backup
            Assertions.assertEquals(tbl2.getId(), task.getTableId());
            Assertions.assertEquals(V1, task.getVersion());
            Assertions.assertEquals(V2, task.getPrefixSource().getEndVersion());
        }
        reportPrefix(job, tasks, Maps.newHashMap());
        waitDigests(job);
        RestoreReuseResult result = job.getReuseResult();
        Assertions.assertEquals(2, result.getIncrementalPartitions());
        Assertions.assertEquals(2, result.getIncrementalAtomicPartitions());
        Assertions.assertEquals(0, result.getKeptPartitions());
        for (RestoreReuseResult.Decision decision : result.getDecisions()) {
            Assertions.assertTrue(decision.incremental);
            Assertions.assertTrue(decision.atomic);
            Assertions.assertEquals(V1, decision.version);
            Assertions.assertEquals(V2, decision.targetVersion);
            Assertions.assertEquals(stagingTbl.getId(), decision.stagingTableId);
        }
        assertIncrementalLocalBytes(job, result);
        Partition stagingP1 = stagingTbl.getPartition(p1().getName());
        Assertions.assertNotNull(result.getIncremental(stagingTbl.getId(), stagingP1.getId()));
        Assertions.assertNull(result.getIncremental(tbl2.getId(), p1().getId()));

        // the snapshot loads the local data up to the local version, and leaves an empty dir for the increment
        List<SnapshotTask> snapshots = snapshotTasks(submitted);
        Assertions.assertEquals(replicasOf(p1()) + replicasOf(p2()), snapshots.size());
        for (SnapshotTask task : snapshots) {
            Assertions.assertTrue(task.isRestoreLocalSource());
            Assertions.assertTrue(task.isRestoreIncremental());
            Assertions.assertEquals(V1, task.getVersion());
            Assertions.assertTrue(task.toThrift().isSetRefTabletId());
        }
        finishSnapshots(job, snapshots, 700, 0);
        Deencapsulation.invoke(job, "waitingAllSnapshotsFinished");

        // the download is the increment, of the staging tablets
        submitted.clear();
        Deencapsulation.invoke(job, "downloadSnapshots");
        int remotes = 0;
        for (AgentTask task : submitted) {
            if (task instanceof DownloadTask) {
                for (TRemoteTabletSnapshot remote : ((DownloadTask) task).getRemoteTabletSnapshots()) {
                    Assertions.assertEquals(V1, remote.getIncremental().getBaseVersion());
                    Assertions.assertEquals(V2, remote.getIncremental().getEndVersion());
                    remotes++;
                }
            }
        }
        Assertions.assertEquals(snapshots.size(), remotes);
        // the download resets its stats, not what the local snapshots made
        Assertions.assertEquals(snapshots.size(), job.getDownloadStats().getLocalSourceTablets());
        Deencapsulation.setField(job, "state", RestoreJobState.COMMIT);
        submitted.clear();
        job.commit();
        List<DirMoveTask> moves = submitted.stream().filter(t -> t instanceof DirMoveTask)
                .map(t -> (DirMoveTask) t).collect(Collectors.toList());
        Assertions.assertEquals(snapshots.size(), moves.size());
        for (DirMoveTask move : moves) {
            Assertions.assertEquals(V1, move.getIncrementalRange().getBaseVersion());
            Assertions.assertEquals(V2, move.getIncrementalRange().getEndVersion());
        }
        String estimate = job.getFullInfo().stream().filter(x -> x.contains("kept_atomic\"")).findFirst().orElse("");
        Assertions.assertTrue(estimate.contains("\"incremental_atomic\":2"), estimate);
        Assertions.assertTrue(estimate.contains("\"kept_atomic\":0"), estimate);
        Assertions.assertTrue(estimate.contains("\"incremental_partitions\":2"), estimate);
        RestoreJob replayed = writeAndRead(job);
        Assertions.assertEquals(2, replayed.getReuseResult().getIncrementalAtomicPartitions());
        // the versions of the table being replaced are checked, with the version of the proof
        Status st = job.allTabletCommitted(false);
        Assertions.assertTrue(st.ok(), st.toString());
        OlapTable replaced = (OlapTable) db.getTableNullable(CatalogMocker.TEST_TBL2_NAME);
        Assertions.assertEquals(stagingTbl.getId(), replaced.getId());
        for (Partition part : replaced.getPartitions()) {
            Assertions.assertEquals(V2, part.getVisibleVersion());
            Assertions.assertEquals(V2, part.getRestoreLineage().getSrcVersion());
        }
    }

    @Test
    public void testAtomicWriteBeforeReplaceFailsAndTheTableIsNotReplaced() {
        RestoreJob job = prepareAtomicJob(selfBackupJobInfo(), "full");
        toVerifying(job);
        reportAll(job, newDigestTasks(), Maps.newHashMap());
        waitDigests(job);
        OlapTable stagingTbl = staging(job);
        Assertions.assertEquals(2, job.getReuseResult().getKeptAtomicPartitions());

        // the check under the lock of the table being replaced
        Assertions.assertTrue(job.checkAtomicLocalVersions(tbl2).ok());
        // a write: the version moved
        p1().updateVersionForRestore(V1 + 1);
        Status st = job.checkAtomicLocalVersions(tbl2);
        Assertions.assertFalse(st.ok());
        Assertions.assertTrue(st.getErrMsg().contains("changed from " + V1 + " to " + (V1 + 1)), st.getErrMsg());
        p1().updateVersionForRestore(V1);
        // a load committed and not published yet
        p2().setNextVersion(V1 + 2);
        st = job.checkAtomicLocalVersions(tbl2);
        Assertions.assertFalse(st.ok());
        Assertions.assertTrue(st.getErrMsg().contains(p2().getName()), st.getErrMsg());
        p2().setNextVersion(V1 + 1);
        Assertions.assertTrue(job.checkAtomicLocalVersions(tbl2).ok());

        // the commit: fails before any tablet is moved and before the tables are replaced
        p1().updateVersionForRestore(V1 + 1);
        submitted.clear();
        Deencapsulation.setField(job, "state", RestoreJobState.COMMIT);
        job.commit();
        Assertions.assertFalse(job.getStatus().ok());
        Assertions.assertTrue(submitted.stream().noneMatch(t -> t instanceof DirMoveTask));
        st = job.allTabletCommitted(false);
        Assertions.assertFalse(st.ok());
        // the table being replaced is still the table of the name
        Assertions.assertEquals(tbl2.getId(), db.getTableNullable(CatalogMocker.TEST_TBL2_NAME).getId());
        Assertions.assertNotNull(db.getTableNullable(RestoreJob.tableAliasWithAtomicRestore(
                CatalogMocker.TEST_TBL2_NAME)));
        Assertions.assertEquals(stagingTbl.getId(), db.getTableNullable(RestoreJob.tableAliasWithAtomicRestore(
                CatalogMocker.TEST_TBL2_NAME)).getId());
    }

    @Test
    public void testAtomicCancelReleasesTheLocalSnapshotsAndKeepsTheTable() {
        RestoreJob job = prepareAtomicJob(selfBackupJobInfo(), "full");
        toVerifying(job);
        reportAll(job, newDigestTasks(), Maps.newHashMap());
        waitDigests(job);
        List<SnapshotTask> snapshots = snapshotTasks(submitted);
        finishSnapshots(job, snapshots, 1000, 0);
        Deencapsulation.invoke(job, "waitingAllSnapshotsFinished");
        submitted.clear();
        job.cancel();
        Assertions.assertEquals(RestoreJobState.CANCELLED, job.getState());
        // every snapshot, the ones made from the local tablets too, is released
        List<ReleaseSnapshotTask> releases = submitted.stream().filter(t -> t instanceof ReleaseSnapshotTask)
                .map(t -> (ReleaseSnapshotTask) t).collect(Collectors.toList());
        Assertions.assertEquals(snapshots.size(), releases.size());
        // the table being replaced is as before, the staging table is gone
        Assertions.assertEquals(V1, p1().getVisibleVersion());
        Assertions.assertNotNull(p1().getRestoreLineage());
        Assertions.assertEquals(OlapTableState.NORMAL, tbl2.getState());
        Assertions.assertFalse(tbl2.isInAtomicRestore());
        Assertions.assertNull(db.getTableNullable(RestoreJob.tableAliasWithAtomicRestore(
                CatalogMocker.TEST_TBL2_NAME)));
    }

    @Test
    public void testAtomicCancelWhileVerifyingChangesNothing() {
        RestoreJob job = prepareAtomicJob(selfBackupJobInfo(), "full");
        toVerifying(job);
        newDigestTasks();
        job.cancel();
        Assertions.assertEquals(RestoreJobState.CANCELLED, job.getState());
        Assertions.assertEquals(V1, p1().getVisibleVersion());
        Assertions.assertNotNull(p1().getRestoreLineage());
        Assertions.assertEquals(OlapTableState.NORMAL, tbl2.getState());
        Assertions.assertNull(db.getTableNullable(RestoreJob.tableAliasWithAtomicRestore(
                CatalogMocker.TEST_TBL2_NAME)));
    }

    @Test
    public void testLocalSourceStatsAreSummedAndJson() {
        RestoreDownloadStats stats = new RestoreDownloadStats();
        TDownloadStats a = new TDownloadStats();
        a.setLocalSourceTablets(2);
        a.setLocalSourceLinkedFiles(5);
        a.setLocalSourceLinkedBytes(500);
        a.setLocalSourceCopiedFiles(1);
        a.setLocalSourceCopiedBytes(70);
        stats.addLocalSource(a);
        stats.addLocalSource(a);
        Assertions.assertEquals(4, stats.getLocalSourceTablets());
        Assertions.assertEquals(1000, stats.getLocalSourceLinkedBytes());
        Assertions.assertEquals(140, stats.getLocalSourceCopiedBytes());
        RestoreDownloadStats next = new RestoreDownloadStats();
        next.carryLocalSource(stats);
        String json = next.toJson(0, 5000, 5000);
        Assertions.assertTrue(json.contains("\"kept_atomic_bytes\":5000"), json);
        Assertions.assertTrue(json.contains("\"copied_bytes\":140"), json);
        Assertions.assertTrue(json.contains("\"local_tablets\":4"), json);
        // untouched without an atomic restore
        Config.enable_restore_atomic_reuse = false;
        Assertions.assertFalse(new RestoreDownloadStats().toJson(0, 0).contains("atomic"));
    }

    // ---------------------------------------------------------------------------------------------
    // the protection of the staging partitions loaded from a local source, and the failure of the tasks
    // ---------------------------------------------------------------------------------------------

    private BackupHandler newHandler() {
        BackupHandler handler = new BackupHandler(env);
        Mockito.when(env.getBackupHandler()).thenReturn(handler);
        return handler;
    }

    // An atomic restore of the incremental backup, up to SNAPSHOTING: the staging partitions are loaded from the
    // local source and the snapshot tasks are sent.
    private RestoreJob atomicIncrementalJob(Map<Long, String> verdicts) {
        Config.enable_restore_incremental_append = true;
        RestoreJob job = prepareAtomicJob(incrementalJobInfo(), "full");
        toVerifying(job);
        reportPrefix(job, newDigestTasks(), verdicts);
        waitDigests(job);
        Assertions.assertEquals(RestoreJobState.SNAPSHOTING, job.getState());
        return job;
    }

    private Set<Long> stagingPartitionIds(RestoreJob job, Partition... locals) {
        OlapTable stagingTbl = staging(job);
        Set<Long> ids = Sets.newHashSet();
        for (Partition local : locals) {
            ids.add(stagingTbl.getPartition(local.getName()).getId());
        }
        return ids;
    }

    // To COMMITTING with the move tasks of the job (incremental appends), in AgentTaskQueue's place: `submitted`.
    private List<DirMoveTask> toCommitting(RestoreJob job) {
        finishSnapshots(job, snapshotTasks(submitted), 700, 0);
        Deencapsulation.invoke(job, "waitingAllSnapshotsFinished");
        submitted.clear();
        Deencapsulation.invoke(job, "downloadSnapshots");
        Deencapsulation.setField(job, "state", RestoreJobState.COMMIT);
        submitted.clear();
        job.commit();
        Assertions.assertEquals(RestoreJobState.COMMITTING, job.getState());
        return submitted.stream().filter(t -> t instanceof DirMoveTask).map(t -> (DirMoveTask) t)
                .collect(Collectors.toList());
    }

    private static TFinishTaskRequest failure(TStatusCode code, String msg) {
        TFinishTaskRequest request = new TFinishTaskRequest();
        TStatus status = new TStatus(code);
        status.setErrorMsgs(Lists.newArrayList(msg));
        request.setTaskStatus(status);
        return request;
    }

    // what MasterImpl does before it hands a failed task to the job
    private static void failed(AgentTask task, int times) {
        for (int i = 0; i < times; i++) {
            task.failed();
        }
    }

    @Test
    public void testLocalSourceStagingPartitionsAreBuiltBeforeTheSnapshotsAndClearedWhenFinished() {
        BackupHandler handler = newHandler();
        RestoreJob job = atomicIncrementalJob(Maps.newHashMap());
        // the staging partitions of the 2 incremental partitions, not the partitions of the table being replaced
        Set<Long> expected = stagingPartitionIds(job, p1(), p2());
        Assertions.assertEquals(2, expected.size());
        Assertions.assertEquals(expected, job.getLocalSourceStagingPartitions());
        for (Long id : expected) {
            Assertions.assertTrue(job.isLocalSourceStagingPartition(id));
        }
        Assertions.assertFalse(job.isLocalSourceStagingPartition(p1().getId()));
        Assertions.assertTrue(handler.hasJobsWithLocalSource());
        Assertions.assertEquals(Sets.newHashSet(job.getJobId()), handler.getJobsWithLocalSource());
        // the set is immutable: it is only replaced as a whole
        Assertions.assertThrows(UnsupportedOperationException.class,
                () -> job.getLocalSourceStagingPartitions().clear());

        toCommitting(job);
        Assertions.assertEquals(expected, job.getLocalSourceStagingPartitions());
        Status st = job.allTabletCommitted(false);
        Assertions.assertTrue(st.ok(), st.toString());
        Assertions.assertTrue(job.getLocalSourceStagingPartitions().isEmpty());
        Assertions.assertFalse(handler.hasJobsWithLocalSource());
    }

    @Test
    public void testOnlyThePartitionsLoadedFromTheLocalSourceAreProtected() {
        BackupHandler handler = newHandler();
        // p2 is downloaded as a whole: its staging partition is a normal one
        Map<Long, String> verdicts = Maps.newHashMap();
        verdicts.put(p2().getId(), "NOT_BOUNDARY");
        RestoreJob job = atomicIncrementalJob(verdicts);
        Assertions.assertEquals(stagingPartitionIds(job, p1()), job.getLocalSourceStagingPartitions());
        Assertions.assertFalse(job.isLocalSourceStagingPartition(stagingPartitionIds(job, p2()).iterator().next()));
        Assertions.assertTrue(handler.hasJobsWithLocalSource());
    }

    @Test
    public void testKeptAtomicPartitionsAreProtected() {
        BackupHandler handler = newHandler();
        RestoreJob job = prepareAtomicJob(selfBackupJobInfo(), "full");
        toVerifying(job);
        reportAll(job, newDigestTasks(), Maps.newHashMap());
        waitDigests(job);
        Assertions.assertEquals(2, job.getReuseResult().getKeptAtomicPartitions());
        Assertions.assertEquals(stagingPartitionIds(job, p1(), p2()), job.getLocalSourceStagingPartitions());
        Assertions.assertTrue(handler.hasJobsWithLocalSource());
    }

    @Test
    public void testNothingIsProtectedWithoutLocalSourcePartitions() {
        BackupHandler handler = newHandler();
        // a plain atomic restore (no reuse)
        Config.enable_restore_atomic_reuse = false;
        RestoreJob job = newAtomicJob(selfBackupJobInfo(), "full");
        Deencapsulation.invoke(job, "checkAndPrepareMeta");
        Deencapsulation.invoke(job, "allReplicasCreated");
        Assertions.assertEquals(RestoreJobState.SNAPSHOTING, job.getState());
        Assertions.assertTrue(job.getLocalSourceStagingPartitions().isEmpty());
        Assertions.assertFalse(handler.hasJobsWithLocalSource());
        // a non atomic restore which keeps a partition
        RestoreJob nonAtomic = prepareJob("full");
        toVerifying(nonAtomic);
        reportAll(nonAtomic, newDigestTasks(), Maps.newHashMap());
        waitDigests(nonAtomic);
        Assertions.assertEquals(2, nonAtomic.getReuseResult().getKeptPartitions());
        Assertions.assertTrue(nonAtomic.getLocalSourceStagingPartitions().isEmpty());
        Assertions.assertFalse(handler.hasJobsWithLocalSource());
    }

    @Test
    public void testCancelClearsTheProtection() {
        BackupHandler handler = newHandler();
        RestoreJob job = atomicIncrementalJob(Maps.newHashMap());
        Assertions.assertTrue(handler.hasJobsWithLocalSource());
        job.cancel();
        Assertions.assertEquals(RestoreJobState.CANCELLED, job.getState());
        Assertions.assertTrue(job.getLocalSourceStagingPartitions().isEmpty());
        Assertions.assertFalse(handler.hasJobsWithLocalSource());
    }

    @Test
    public void testReplayRebuildsTheProtectionOfARunningJobOnly() throws Exception {
        RestoreJob job = atomicIncrementalJob(Maps.newHashMap());
        Set<Long> expected = Sets.newHashSet(job.getLocalSourceStagingPartitions());
        // the edit log of the job entering DOWNLOAD / the image: the set is not persisted, it is rebuilt from the
        // decisions
        RestoreJob replayed = writeAndRead(job);
        Assertions.assertEquals(expected, replayed.getLocalSourceStagingPartitions());
        // the replayed object replaces the job in the handler: the handler follows the new object
        BackupHandler handler = newHandler();
        Deencapsulation.invoke(handler, "addBackupOrRestoreJob", db.getId(), job);
        handler.replayAddJob(replayed);
        Assertions.assertEquals(Sets.newHashSet(job.getJobId()), handler.getJobsWithLocalSource());
        Assertions.assertSame(replayed, handler.getJob(db.getId()));

        // finished: the same persisted decisions, but nothing is protected
        toCommitting(job);
        Assertions.assertTrue(job.allTabletCommitted(false).ok());
        RestoreJob finished = writeAndRead(job);
        Assertions.assertEquals(RestoreJobState.FINISHED, finished.getState());
        Assertions.assertFalse(finished.getReuseResult().getDecisions().isEmpty());
        Assertions.assertTrue(finished.getLocalSourceStagingPartitions().isEmpty());
        // and the replay of the finished state follows
        finished.setEnv(env);
        handler.replayAddJob(finished);
        Assertions.assertFalse(handler.hasJobsWithLocalSource());
    }

    @Test
    public void testHandlerKeepsTheJobSetWhenTheObjectIsReplacedOrEvicted() throws Exception {
        BackupHandler handler = newHandler();
        RestoreJob job = atomicIncrementalJob(Maps.newHashMap());
        Assertions.assertTrue(handler.getJobsWithLocalSource().contains(job.getJobId()));

        // the object is replaced by a deserialized one of the same job in a terminal state: no leak
        Deencapsulation.invoke(handler, "addBackupOrRestoreJob", db.getId(), job);
        RestoreJob copy = writeAndRead(job);
        Deencapsulation.setField(copy, "state", RestoreJobState.CANCELLED);
        Deencapsulation.setField(copy, "localSourceStagingPartitions", java.util.Collections.emptySet());
        Deencapsulation.invoke(handler, "addBackupOrRestoreJob", db.getId(), copy);
        Assertions.assertFalse(handler.hasJobsWithLocalSource());
        // replaced by one which is still running: kept
        RestoreJob running = writeAndRead(job);
        Deencapsulation.invoke(handler, "addBackupOrRestoreJob", db.getId(), running);
        Assertions.assertTrue(handler.getJobsWithLocalSource().contains(job.getJobId()));

        // evicted by the limit of the jobs of the database
        int origNum = Config.max_backup_restore_job_num_per_db;
        try {
            Config.max_backup_restore_job_num_per_db = 1;
            RestoreJob other = newJob(selfBackupJobInfo(), "full");
            Deencapsulation.invoke(handler, "addBackupOrRestoreJob", db.getId(), other);
            Assertions.assertFalse(handler.hasJobsWithLocalSource());
            Assertions.assertSame(other, handler.getJob(db.getId()));
        } finally {
            Config.max_backup_restore_job_num_per_db = origNum;
        }
    }

    @Test
    public void testTheMoveTasksCarryTheDigestOfTheBackup() {
        newHandler();
        RestoreJob job = atomicIncrementalJob(Maps.newHashMap());
        // the local source snapshot tasks carry the job id
        for (SnapshotTask task : snapshotTasks(submitted)) {
            Assertions.assertTrue(task.isRestoreLocalSource());
            Assertions.assertTrue(task.toThrift().isSetJobId());
            Assertions.assertEquals(job.getJobId(), task.toThrift().getJobId());
        }
        List<DirMoveTask> moves = toCommitting(job);
        Assertions.assertFalse(moves.isEmpty());
        for (DirMoveTask move : moves) {
            Assertions.assertNotNull(move.getBackupLogicalDigest());
            Assertions.assertTrue(move.toThrift().isSetBackupLogicalDigest());
            Assertions.assertEquals(SIG, move.toThrift().getBackupLogicalDigest().getSchemaSig());
            Assertions.assertEquals(1, move.toThrift().getBackupLogicalDigest().getAlgoVersion());
            Assertions.assertEquals("OK", move.toThrift().getBackupLogicalDigest().getStatusCode());
            Assertions.assertTrue(move.isLocalSourceLoaded());
            Assertions.assertTrue(move.toThrift().isLocalSourceLoaded());
            Assertions.assertEquals(job.getJobId(), move.toThrift().getJobId());
        }
    }

    @Test
    public void testNotRetryableErrorOfTheAppendCancelsAtOnce() {
        newHandler();
        RestoreJob job = atomicIncrementalJob(Maps.newHashMap());
        List<DirMoveTask> moves = toCommitting(job);
        DirMoveTask move = moves.get(0);
        failed(move, 1);
        // the status message is wrapped by the backend: (host)[INTERNAL_ERROR][RESTORE_NOT_RETRYABLE] ...
        Assertions.assertFalse(job.finishDirMoveTask(move, failure(TStatusCode.INTERNAL_ERROR,
                "(127.0.0.1)[INTERNAL_ERROR]" + RestoreJob.RESTORE_NOT_RETRYABLE + " no evidence")));
        Assertions.assertEquals(RestoreJobState.CANCELLED, job.getState());
        Assertions.assertTrue(job.getStatus().getErrMsg().contains(RestoreJob.RESTORE_NOT_RETRYABLE),
                job.getStatus().getErrMsg());
        Assertions.assertTrue(job.getLocalSourceStagingPartitions().isEmpty());
    }

    @Test
    public void testOtherErrorsOfTheAppendCancelAfterTheMaxRetry() {
        newHandler();
        int origMax = Config.restore_local_source_task_max_retry;
        Config.restore_local_source_task_max_retry = 3;
        try {
            RestoreJob job = atomicIncrementalJob(Maps.newHashMap());
            DirMoveTask move = toCommitting(job).get(0);
            for (int i = 1; i <= 3; i++) {
                failed(move, 1);
                Assertions.assertFalse(job.finishDirMoveTask(move, failure(TStatusCode.INTERNAL_ERROR, "boom")));
                Assertions.assertEquals(RestoreJobState.COMMITTING, job.getState(), "failure " + i);
            }
            // a failure to obtain the locks is not counted: it is taken back
            failed(move, 1);
            Assertions.assertEquals(4, move.getFailedTimes());
            Assertions.assertFalse(job.finishDirMoveTask(move, failure(TStatusCode.OBTAIN_LOCK_FAILED, "locks")));
            Assertions.assertEquals(3, move.getFailedTimes());
            Assertions.assertEquals(RestoreJobState.COMMITTING, job.getState());
            // the 4th failure which counts
            failed(move, 1);
            Assertions.assertFalse(job.finishDirMoveTask(move, failure(TStatusCode.INTERNAL_ERROR, "boom")));
            Assertions.assertEquals(RestoreJobState.CANCELLED, job.getState());
            Assertions.assertTrue(job.getStatus().getErrMsg().contains("boom"), job.getStatus().getErrMsg());
        } finally {
            Config.restore_local_source_task_max_retry = origMax;
        }
    }

    @Test
    public void testLocalSourceSnapshotFailuresCancelTheSameWay() {
        newHandler();
        int origMax = Config.restore_local_source_task_max_retry;
        Config.restore_local_source_task_max_retry = 2;
        try {
            RestoreJob job = atomicIncrementalJob(Maps.newHashMap());
            List<SnapshotTask> snapshots = snapshotTasks(submitted);
            SnapshotTask task = snapshots.get(0);
            for (int i = 1; i <= 2; i++) {
                failed(task, 1);
                Assertions.assertFalse(job.finishTabletSnapshotTask(task, failure(TStatusCode.INTERNAL_ERROR, "boom")));
                Assertions.assertEquals(RestoreJobState.SNAPSHOTING, job.getState());
            }
            failed(task, 1);
            Assertions.assertFalse(job.finishTabletSnapshotTask(task, failure(TStatusCode.INTERNAL_ERROR, "boom")));
            Assertions.assertEquals(RestoreJobState.CANCELLED, job.getState());

            // the not retryable error of another task of another job: at once
            RestoreJob other = atomicIncrementalJob(Maps.newHashMap());
            SnapshotTask otherTask = snapshotTasks(submitted).get(0);
            failed(otherTask, 1);
            Assertions.assertFalse(other.finishTabletSnapshotTask(otherTask, failure(TStatusCode.INTERNAL_ERROR,
                    "[INTERNAL_ERROR]" + RestoreJob.RESTORE_NOT_RETRYABLE + " another job")));
            Assertions.assertEquals(RestoreJobState.CANCELLED, other.getState());
        } finally {
            Config.restore_local_source_task_max_retry = origMax;
        }
    }

    @Test
    public void testTheFailureOfTheOtherTasksIsNotConverged() {
        newHandler();
        int origMax = Config.restore_local_source_task_max_retry;
        Config.restore_local_source_task_max_retry = 1;
        try {
            RestoreJob job = atomicIncrementalJob(Maps.newHashMap());
            toCommitting(job);
            // a whole move (no incremental range), way over the limit and with the not retryable prefix: only
            // recorded, the task is resent by the normal flow, the job goes on
            DirMoveTask whole = new DirMoveTask(null, CatalogMocker.BACKEND1_ID, 777001L, job.getJobId(), db.getId(),
                    tbl2.getId(), p1().getId(), p1().getBaseIndex().getId(), tabletOf(p1()), "/path/snapshot", 1, true);
            Assertions.assertNull(whole.getIncrementalRange());
            failed(whole, 100);
            Assertions.assertFalse(job.finishDirMoveTask(whole, failure(TStatusCode.INTERNAL_ERROR, "boom")));
            Assertions.assertFalse(job.finishDirMoveTask(whole, failure(TStatusCode.INTERNAL_ERROR,
                    RestoreJob.RESTORE_NOT_RETRYABLE + " not for this task")));
            Assertions.assertEquals(RestoreJobState.COMMITTING, job.getState());
            Assertions.assertEquals(100, whole.getFailedTimes());

        } finally {
            Config.restore_local_source_task_max_retry = origMax;
        }
    }

    @Test
    public void testAFailureAfterTheJobMovedOnIsIgnored() {
        newHandler();
        RestoreJob job = atomicIncrementalJob(Maps.newHashMap());
        DirMoveTask move = toCommitting(job).get(0);
        // the move task of a job which is no longer COMMITTING (the late report of a task): not cancelled twice
        Deencapsulation.setField(job, "state", RestoreJobState.DOWNLOADING);
        failed(move, 1);
        Assertions.assertFalse(job.finishDirMoveTask(move, failure(TStatusCode.INTERNAL_ERROR,
                RestoreJob.RESTORE_NOT_RETRYABLE + " late")));
        Assertions.assertEquals(RestoreJobState.DOWNLOADING, job.getState());
    }

    @Test
    public void testTheFailureOfAWholeSnapshotIsNotConverged() {
        newHandler();
        int origMax = Config.restore_local_source_task_max_retry;
        Config.restore_local_source_task_max_retry = 1;
        try {
            Config.enable_restore_atomic_reuse = false;
            RestoreJob plain = newAtomicJob(selfBackupJobInfo(), "full");
            Deencapsulation.invoke(plain, "checkAndPrepareMeta");
            Deencapsulation.invoke(plain, "allReplicasCreated");
            Assertions.assertEquals(RestoreJobState.SNAPSHOTING, plain.getState());
            SnapshotTask whole = snapshotTasks(submitted).get(0);
            Assertions.assertFalse(whole.isRestoreLocalSource());
            failed(whole, 100);
            Assertions.assertFalse(plain.finishTabletSnapshotTask(whole, failure(TStatusCode.INTERNAL_ERROR,
                    RestoreJob.RESTORE_NOT_RETRYABLE + " boom")));
            Assertions.assertEquals(RestoreJobState.SNAPSHOTING, plain.getState());
            Assertions.assertEquals(100, whole.getFailedTimes());
        } finally {
            Config.restore_local_source_task_max_retry = origMax;
        }
    }

    @Test
    public void testTheFailureOfANonAtomicIncrementalAppendIsNotConverged() {
        newHandler();
        int origMax = Config.restore_local_source_task_max_retry;
        Config.restore_local_source_task_max_retry = 1;
        try {
            RestoreJob job = atomicIncrementalJob(Maps.newHashMap());
            toCommitting(job);
            // an incremental append of a tablet which was not loaded from a local source (the non atomic restore)
            DirMoveTask append = new DirMoveTask(null, CatalogMocker.BACKEND1_ID, 777002L, job.getJobId(), db.getId(),
                    tbl2.getId(), p1().getId(), p1().getBaseIndex().getId(), tabletOf(p1()), "/path/snapshot", 1, true);
            append.setIncrementalRange(V1, V2);
            Assertions.assertFalse(append.isLocalSourceLoaded());
            failed(append, 100);
            Assertions.assertFalse(job.finishDirMoveTask(append, failure(TStatusCode.INTERNAL_ERROR,
                    RestoreJob.RESTORE_NOT_RETRYABLE + " not for this task")));
            Assertions.assertEquals(RestoreJobState.COMMITTING, job.getState());
        } finally {
            Config.restore_local_source_task_max_retry = origMax;
        }
    }
}
