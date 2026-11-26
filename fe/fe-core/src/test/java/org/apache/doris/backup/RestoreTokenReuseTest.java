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

import org.apache.doris.analysis.IndexDef.IndexType;
import org.apache.doris.analysis.PartitionValue;
import org.apache.doris.backup.BackupJobInfo.BackupIndexInfo;
import org.apache.doris.backup.BackupJobInfo.BackupOlapTableInfo;
import org.apache.doris.backup.BackupJobInfo.BackupPartitionInfo;
import org.apache.doris.backup.BackupJobInfo.BackupTabletInfo;
import org.apache.doris.backup.RestoreJob.RestoreJobState;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.Index;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.MaterializedIndex.IndexExtState;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.OlapTable.OlapTableState;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.PartitionInfo;
import org.apache.doris.catalog.PartitionKey;
import org.apache.doris.catalog.PartitionStateToken;
import org.apache.doris.catalog.RandomDistributionInfo;
import org.apache.doris.catalog.RangePartitionItem;
import org.apache.doris.catalog.Replica;
import org.apache.doris.catalog.ReplicaAllocation;
import org.apache.doris.catalog.Resource;
import org.apache.doris.catalog.SinglePartitionInfo;
import org.apache.doris.catalog.Tablet;
import org.apache.doris.catalog.TabletInvertedIndex;
import org.apache.doris.common.Config;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.Pair;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.datasource.CatalogMgr;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.ha.FrontendNodeType;
import org.apache.doris.persist.EditLog;
import org.apache.doris.qe.OriginStatement;
import org.apache.doris.resource.Tag;
import org.apache.doris.system.Backend;
import org.apache.doris.system.Frontend;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.task.AgentBatchTask;
import org.apache.doris.task.AgentTask;
import org.apache.doris.task.AgentTaskExecutor;
import org.apache.doris.task.DirMoveTask;
import org.apache.doris.task.DownloadTask;
import org.apache.doris.task.ReleaseSnapshotTask;
import org.apache.doris.task.SnapshotTask;
import org.apache.doris.thrift.TFinishTaskRequest;
import org.apache.doris.thrift.TRemoteTabletSnapshot;
import org.apache.doris.thrift.TSnapshotRequest;
import org.apache.doris.thrift.TStatus;
import org.apache.doris.thrift.TStatusCode;
import org.apache.doris.thrift.TStorageMedium;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Range;
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
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * Partition level reuse of a restore proved by the state token of the partition alone
 * (restore_reuse_trust_state_token): the backup carries the token of each partition and no logical digest, the local
 * partitions have the token and no restore lineage. Covers the non-atomic keep, the atomic keep loaded by the
 * snapshot task, the conditions of the token chain, the capability of the nodes, the recheck before the commit, the
 * persistence, the failure of the tasks of the local source, the token the commit leaves and the nudge of the job.
 * Only what holds both with and without the logical verification is asserted: a partition that is not kept by its
 * token is downloaded, whatever the reason recorded for it.
 */
public class RestoreTokenReuseTest {
    private static final long V1 = 10;
    private static final long BACKUP_TIME = 1700000000000L;
    private static final String TK1 = "token-of-p1";
    private static final String TK2 = "token-of-p2";

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
    private boolean origAtomic;
    private boolean origTrust;
    private boolean origTrack;
    private long origMinBytes;
    private boolean origHidden;
    private boolean origMow;
    private boolean origKeptLoad;
    private boolean origRequireCapability;
    private int origMaxRetry;
    private boolean origMergeIdleTicks;
    private boolean origRunOnTaskFinish;

    @BeforeEach
    public void setUp() throws Exception {
        origEnable = Config.enable_restore_partition_reuse;
        origAtomic = Config.enable_restore_atomic_reuse;
        origTrust = Config.restore_reuse_trust_state_token;
        origTrack = Config.restore_state_token_track;
        origMinBytes = Config.restore_reuse_min_partition_bytes;
        origHidden = Config.restore_reuse_allow_hidden_state_models;
        origMow = Config.restore_reuse_mow;
        origKeptLoad = Config.restore_reuse_kept_load_at_snapshot;
        origRequireCapability = Config.restore_reuse_require_capability;
        origMaxRetry = Config.restore_local_source_task_max_retry;
        origMergeIdleTicks = Config.backup_restore_merge_idle_ticks;
        origRunOnTaskFinish = Config.backup_restore_run_on_task_finish;
        Config.enable_restore_partition_reuse = true;
        Config.restore_reuse_trust_state_token = true;
        Config.restore_state_token_track = true;
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
        Mockito.when(systemInfo.getAllClusterBackendsNoException()).thenReturn(ImmutableMap.of());
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
        // p1 and p2 are at V1, have a size and the state token of the backup partition of the same name, and no
        // restore lineage.
        for (Partition part : Lists.newArrayList(p1(), p2())) {
            part.updateVersionForRestore(V1);
            for (MaterializedIndex index : part.getMaterializedIndices(IndexExtState.VISIBLE)) {
                for (Tablet tablet : index.getTablets()) {
                    for (Replica replica : tablet.getReplicas()) {
                        replica.updateVersionForRestore(V1);
                        replica.setDataSize(1000);
                    }
                }
            }
            setToken(part, tokenOf(part));
        }
    }

    @AfterEach
    public void tearDown() {
        Config.enable_restore_partition_reuse = origEnable;
        Config.enable_restore_atomic_reuse = origAtomic;
        Config.restore_reuse_trust_state_token = origTrust;
        Config.restore_state_token_track = origTrack;
        Config.restore_reuse_min_partition_bytes = origMinBytes;
        Config.restore_reuse_allow_hidden_state_models = origHidden;
        Config.restore_reuse_mow = origMow;
        Config.restore_reuse_kept_load_at_snapshot = origKeptLoad;
        Config.restore_reuse_require_capability = origRequireCapability;
        Config.restore_local_source_task_max_retry = origMaxRetry;
        Config.backup_restore_merge_idle_ticks = origMergeIdleTicks;
        Config.backup_restore_run_on_task_finish = origRunOnTaskFinish;
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

    private static String tokenOf(Partition part) {
        return part.getName().equals(CatalogMocker.TEST_PARTITION1_NAME) ? TK1 : TK2;
    }

    // the token bound to the current visible version of the partition
    private static void setToken(Partition part, String token) {
        part.setStateToken(new PartitionStateToken(token, part.getVisibleVersion(), part.getVisibleVersionTime()));
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

    private static List<Long> tabletIds(Partition part) {
        List<Long> ids = Lists.newArrayList();
        for (MaterializedIndex index : part.getMaterializedIndices(IndexExtState.VISIBLE)) {
            for (Tablet tablet : index.getTablets()) {
                ids.add(tablet.getId());
            }
        }
        return ids;
    }

    // A backup of test_tbl2 taken in this cluster at V1 with the state token of every partition and no logical digest.
    private BackupJobInfo tokenBackupJobInfo() {
        BackupJobInfo info = new BackupJobInfo();
        info.name = "snapshot_v1";
        info.backupTime = BACKUP_TIME;
        info.dbId = db.getId();
        info.dbName = db.getFullName();
        info.success = true;
        BackupOlapTableInfo tblInfo = new BackupOlapTableInfo();
        tblInfo.id = tbl2.getId();
        for (Partition part : tbl2.getPartitions()) {
            BackupPartitionInfo partInfo = new BackupPartitionInfo();
            partInfo.id = part.getId();
            partInfo.version = V1;
            partInfo.stateToken = tokenOf(part);
            for (MaterializedIndex index : part.getMaterializedIndices(IndexExtState.VISIBLE)) {
                BackupIndexInfo idxInfo = new BackupIndexInfo();
                idxInfo.id = index.getId();
                idxInfo.schemaHash = tbl2.getSchemaHashByIndexId(index.getId());
                partInfo.indexes.put(tbl2.getIndexNameById(index.getId()), idxInfo);
                for (Tablet tablet : index.getTablets()) {
                    idxInfo.sortedTabletInfoList.add(new BackupTabletInfo(tablet.getId(), Lists.newArrayList()));
                    idxInfo.tablets.put(tablet.getId(), Lists.newArrayList());
                    idxInfo.tabletsOrder.add(tablet.getId());
                }
            }
            tblInfo.partitions.put(part.getName(), partInfo);
        }
        info.backupOlapTableObjects.put(tbl2.getName(), tblInfo);
        return info;
    }

    // The backup kept on local: where the snapshot of every tablet is, for the download.
    private BackupJobInfo localTokenBackupJobInfo() {
        BackupJobInfo info = tokenBackupJobInfo();
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

    private static BackupPartitionInfo partInfoOf(BackupJobInfo info, String partitionName) {
        return info.getOlapTableInfo(CatalogMocker.TEST_TBL2_NAME).getPartInfo(partitionName);
    }

    // the backup meta of test_tbl2 with the partitions at V1
    private BackupMeta backupMeta() {
        OlapTable remoteTbl = tbl2.selectiveCopy(null, IndexExtState.VISIBLE, true);
        for (Partition part : remoteTbl.getPartitions()) {
            part.updateVersionForRestore(V1);
        }
        return new BackupMeta(Lists.newArrayList(remoteTbl), Lists.<Resource>newArrayList());
    }

    private RestoreJob newJob(BackupJobInfo info) {
        return new RestoreJob("restore_label", "2024-01-01 00:00:00", db.getId(), db.getFullName(), info,
                false, new ReplicaAllocation((short) 3), 100000, -1, false, false, false, false, false, false,
                false, env, Repository.KEEP_ON_LOCAL_REPO_ID, backupMeta());
    }

    private RestoreJob newAtomicJob(BackupJobInfo info) {
        return new RestoreJob("restore_label", "2024-01-01 00:00:00", db.getId(), db.getFullName(), info,
                false, new ReplicaAllocation((short) 3), 100000, -1, false, false, false, false, false,
                true /* atomic */, false, env, Repository.KEEP_ON_LOCAL_REPO_ID, backupMeta());
    }

    // after checkAndPrepareMeta: CREATING
    private RestoreJob prepare(RestoreJob job) {
        Deencapsulation.invoke(job, "checkAndPrepareMeta");
        Assertions.assertTrue(job.getStatus().ok(), job.getStatus().toString());
        Assertions.assertEquals(RestoreJobState.CREATING, job.getState());
        return job;
    }

    // A non-atomic restore of the backup onto the existing test_tbl2, up to SNAPSHOTING.
    private RestoreJob toSnapshoting(BackupJobInfo info) {
        RestoreJob job = prepare(newJob(info));
        Deencapsulation.invoke(job, "allReplicasCreated");
        Assertions.assertTrue(job.getStatus().ok(), job.getStatus().toString());
        // nothing to verify by a digest: the candidates are decided when they are selected
        Assertions.assertEquals(RestoreJobState.SNAPSHOTING, job.getState());
        return job;
    }

    // An atomic restore of the backup of the table itself, up to SNAPSHOTING.
    private RestoreJob atomicToSnapshoting(BackupJobInfo info) {
        Config.enable_restore_atomic_reuse = true;
        RestoreJob job = prepare(newAtomicJob(info));
        Deencapsulation.invoke(job, "allReplicasCreated");
        Assertions.assertTrue(job.getStatus().ok(), job.getStatus().toString());
        Assertions.assertEquals(RestoreJobState.SNAPSHOTING, job.getState());
        return job;
    }

    // An atomic restore whose partitions are all kept by their token, up to SNAPSHOTING.
    private RestoreJob atomicKeptJob() {
        RestoreJob job = atomicToSnapshoting(tokenBackupJobInfo());
        Assertions.assertEquals(2, job.getReuseResult().getKeptAtomicPartitions());
        return job;
    }

    private static com.google.common.collect.Table<Long, Long, Long> versionInfo(RestoreJob job) {
        return Deencapsulation.getField(job, "restoredVersionInfo");
    }

    private static RestoreFileMapping fileMapping(RestoreJob job) {
        return Deencapsulation.getField(job, "fileMapping");
    }

    // the staging table of an atomic restore, registered under the temp name
    private OlapTable staging(RestoreJob job) {
        List<org.apache.doris.catalog.Table> restored = Deencapsulation.getField(job, "restoredTbls");
        Assertions.assertEquals(1, restored.size());
        return (OlapTable) restored.get(0);
    }

    private Set<Long> stagingPartitionIds(RestoreJob job, Partition... locals) {
        OlapTable stagingTbl = staging(job);
        Set<Long> ids = Sets.newHashSet();
        for (Partition local : locals) {
            ids.add(stagingTbl.getPartition(local.getName()).getId());
        }
        return ids;
    }

    private static List<SnapshotTask> snapshotTasks(List<AgentTask> tasks) {
        return tasks.stream().filter(t -> t instanceof SnapshotTask).map(t -> (SnapshotTask) t)
                .collect(Collectors.toList());
    }

    private static List<DirMoveTask> moveTasks(List<AgentTask> tasks) {
        return tasks.stream().filter(t -> t instanceof DirMoveTask).map(t -> (DirMoveTask) t)
                .collect(Collectors.toList());
    }

    private static TFinishTaskRequest ok() {
        TFinishTaskRequest report = new TFinishTaskRequest();
        report.setTaskStatus(new TStatus(TStatusCode.OK));
        return report;
    }

    private static TFinishTaskRequest snapshotReport(SnapshotTask task) {
        TFinishTaskRequest report = ok();
        report.setSnapshotPath("/path/snapshot/" + task.getTabletId());
        return report;
    }

    private void finishSnapshots(RestoreJob job, List<SnapshotTask> tasks) {
        for (SnapshotTask task : tasks) {
            Assertions.assertTrue(job.finishTabletSnapshotTask(task, snapshotReport(task)));
        }
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

    // From SNAPSHOTING to COMMITTING (in AgentTaskQueue's place: `submitted`), with the move tasks of the job.
    private List<DirMoveTask> toCommitting(RestoreJob job) {
        finishSnapshots(job, snapshotTasks(submitted));
        Deencapsulation.invoke(job, "waitingAllSnapshotsFinished");
        Assertions.assertEquals(RestoreJobState.DOWNLOAD, job.getState());
        submitted.clear();
        Deencapsulation.invoke(job, "downloadSnapshots");
        Deencapsulation.setField(job, "state", RestoreJobState.COMMIT);
        submitted.clear();
        job.commit();
        Assertions.assertEquals(RestoreJobState.COMMITTING, job.getState());
        return moveTasks(submitted);
    }

    private BackupHandler newHandler() {
        BackupHandler handler = new BackupHandler(env);
        Mockito.when(env.getBackupHandler()).thenReturn(handler);
        return handler;
    }

    // the ReuseEstimate column of SHOW RESTORE
    private static String reuseEstimate(RestoreJob job) {
        return job.getFullInfo().stream().filter(x -> x.contains("\"kept_token\"")).findFirst().orElse("");
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
    // non-atomic restore
    // ---------------------------------------------------------------------------------------------

    @Test
    public void testKeepByTokenSkipsSnapshotDownloadAndMove() {
        RestoreJob job = prepare(newJob(tokenBackupJobInfo()));
        RestoreReuseResult selected = job.getReuseResult();
        Assertions.assertNotNull(selected);
        // decided when selected, the partitions are still overwritten partitions until the meta is prepared
        Assertions.assertEquals(2, versionInfo(job).size());
        Assertions.assertEquals(TK1, PartitionStateToken.effective(p1()));
        Assertions.assertEquals(TK2, PartitionStateToken.effective(p2()));

        Deencapsulation.invoke(job, "allReplicasCreated");
        Assertions.assertEquals(RestoreJobState.SNAPSHOTING, job.getState());
        RestoreReuseResult result = job.getReuseResult();
        Assertions.assertEquals(2, result.getKeptPartitions());
        Assertions.assertEquals(0, result.getDownloadedPartitions());
        Assertions.assertEquals(2, result.countKeptReason(RestoreReuseResult.KEPT_STATE_TOKEN));
        Assertions.assertEquals(2, result.getTokenEqualPartitions());
        Assertions.assertEquals(RestoreReuseShadowStats.getSingleReplicaLocalDataSize(p1())
                + RestoreReuseShadowStats.getSingleReplicaLocalDataSize(p2()), result.getKeptBytesSingleReplica());
        for (RestoreReuseResult.Decision decision : result.getDecisions()) {
            Assertions.assertTrue(decision.kept);
            Assertions.assertFalse(decision.atomic);
            Assertions.assertFalse(decision.incremental);
            Assertions.assertTrue(decision.tokenEqual);
            Assertions.assertEquals(V1, decision.version);
            Assertions.assertEquals(RestoreReuseResult.KEPT_STATE_TOKEN, decision.reason);
        }
        // nothing to snapshot, download or move
        Assertions.assertTrue(versionInfo(job).isEmpty());
        Assertions.assertTrue(fileMapping(job).getMapping().isEmpty());
        Assertions.assertTrue(submitted.isEmpty(), submitted.toString());
        // the kept partitions keep their token, nothing is protected (a non-atomic restore)
        Assertions.assertEquals(TK1, PartitionStateToken.effective(p1()));
        Assertions.assertEquals(TK2, PartitionStateToken.effective(p2()));
        Assertions.assertTrue(job.getLocalSourceStagingPartitions().isEmpty());
        // observable
        String estimate = reuseEstimate(job);
        Assertions.assertTrue(estimate.contains("\"kept_partitions\":2"), estimate);
        Assertions.assertTrue(estimate.contains("\"kept_token\":2"), estimate);
        Assertions.assertTrue(estimate.contains("\"token_equal\":2"), estimate);

        // no move task
        Deencapsulation.setField(job, "state", RestoreJobState.COMMIT);
        job.commit();
        Assertions.assertTrue(moveTasks(submitted).isEmpty());
        Assertions.assertEquals(RestoreJobState.COMMITTING, job.getState());

        // the commit: the versions are untouched, the partitions have the token of the backup
        Status st = job.allTabletCommitted(false);
        Assertions.assertTrue(st.ok(), st.toString());
        for (Partition part : Lists.newArrayList(p1(), p2())) {
            Assertions.assertEquals(V1, part.getVisibleVersion());
            Assertions.assertEquals(tokenOf(part), PartitionStateToken.effective(part));
            Assertions.assertEquals(Partition.PartitionState.NORMAL, part.getState());
            for (Replica replica : part.getBaseIndex().getTablets().get(0).getReplicas()) {
                Assertions.assertEquals(V1, replica.getVersion());
            }
        }
        Assertions.assertEquals(OlapTableState.NORMAL, tbl2.getState());
        Assertions.assertEquals(RestoreJobState.FINISHED, job.getState());
    }

    @Test
    public void testOnePartitionOfAnotherTokenDownloadsOnlyThatPartition() {
        // p1 was written since the backup: another token
        setToken(p1(), "another-token");
        RestoreJob job = toSnapshoting(tokenBackupJobInfo());
        RestoreReuseResult result = job.getReuseResult();
        Assertions.assertEquals(1, result.getKeptPartitions());
        Assertions.assertTrue(result.isKept(tbl2.getId(), p2().getId()));
        Assertions.assertFalse(result.isKept(tbl2.getId(), p1().getId()));
        // p1 is restored as usual, p2 is not
        Assertions.assertTrue(versionInfo(job).contains(tbl2.getId(), p1().getId()));
        Assertions.assertFalse(versionInfo(job).contains(tbl2.getId(), p2().getId()));
        Assertions.assertEquals(replicasOf(p1()), fileMapping(job).getMapping().size());
        Assertions.assertTrue(fileMapping(job).getMapping().keySet().stream()
                .allMatch(chain -> chain.getPartId() == p1().getId()));
        List<SnapshotTask> snapshots = snapshotTasks(submitted);
        Assertions.assertEquals(replicasOf(p1()), snapshots.size());
        for (SnapshotTask task : snapshots) {
            Assertions.assertEquals(p1().getId(), task.getPartitionId());
            Assertions.assertFalse(task.isRestoreLocalSource());
            Assertions.assertFalse(task.isRestoreKeptLoad());
        }
        // the data of p1 is about to be replaced: no state until the job stamps it; p2 keeps its token
        Assertions.assertNull(PartitionStateToken.effective(p1()));
        Assertions.assertEquals(TK2, PartitionStateToken.effective(p2()));

        // only p1 has the move tasks
        finishSnapshots(job, snapshots);
        submitted.clear();
        Deencapsulation.setField(job, "state", RestoreJobState.COMMIT);
        job.commit();
        List<DirMoveTask> moves = moveTasks(submitted);
        Assertions.assertEquals(replicasOf(p1()), moves.size());
        for (DirMoveTask move : moves) {
            Assertions.assertEquals(p1().getId(), move.getPartitionId());
            Assertions.assertFalse(move.isLocalSourceKept());
            Assertions.assertFalse(move.isLocalSourceLoaded());
            Assertions.assertNull(move.getIncrementalRange());
        }
        // the commit: p1 is the backup partition now, with its token
        Status st = job.allTabletCommitted(false);
        Assertions.assertTrue(st.ok(), st.toString());
        Assertions.assertEquals(V1, p1().getVisibleVersion());
        Assertions.assertEquals(TK1, PartitionStateToken.effective(p1()));
        Assertions.assertEquals(TK2, PartitionStateToken.effective(p2()));
    }

    // every partition is restored as without the reuse
    private void assertDownloadsAll(RestoreJob job) {
        RestoreReuseResult result = job.getReuseResult();
        Assertions.assertTrue(result == null || result.getKeptPartitions() == 0, String.valueOf(result));
        Assertions.assertEquals(2, versionInfo(job).size());
        Assertions.assertEquals(replicasOf(p1()) + replicasOf(p2()), fileMapping(job).getMapping().size());
        Assertions.assertEquals(replicasOf(p1()) + replicasOf(p2()), snapshotTasks(submitted).size());
        Assertions.assertNull(PartitionStateToken.effective(p1()));
        Assertions.assertNull(PartitionStateToken.effective(p2()));
    }

    @Test
    public void testTokenNotTrustedDownloadsAll() {
        Config.restore_reuse_trust_state_token = false;
        RestoreJob job = toSnapshoting(tokenBackupJobInfo());
        assertDownloadsAll(job);
        Assertions.assertFalse(reuseEstimate(job).contains("\"kept_token\":1"), reuseEstimate(job));
    }

    @Test
    public void testSwitchOffDownloadsAll() {
        Config.enable_restore_partition_reuse = false;
        RestoreJob job = toSnapshoting(tokenBackupJobInfo());
        Assertions.assertNull(job.getReuseResult());
        assertDownloadsAll(job);
    }

    @Test
    public void testBackupWithoutTokensDownloadsAll() {
        BackupJobInfo info = tokenBackupJobInfo();
        for (BackupPartitionInfo partInfo : info.getOlapTableInfo(CatalogMocker.TEST_TBL2_NAME).partitions.values()) {
            partInfo.stateToken = null;
        }
        assertDownloadsAll(toSnapshoting(info));
    }

    @Test
    public void testCancelBeforeTheDownloadKeepsTheKeptToken() {
        setToken(p1(), "another-token");
        RestoreJob job = toSnapshoting(tokenBackupJobInfo());
        Assertions.assertTrue(job.cancel().ok());
        Assertions.assertEquals(RestoreJobState.CANCELLED, job.getState());
        Assertions.assertEquals(OlapTableState.NORMAL, tbl2.getState());
        Assertions.assertEquals(V1, p1().getVisibleVersion());
        Assertions.assertEquals(V1, p2().getVisibleVersion());
        // the kept partition was never touched, the next restore can still keep it
        Assertions.assertEquals(TK2, PartitionStateToken.effective(p2()));
        Assertions.assertFalse(PartitionStateToken.isTainted(p1()));
        Assertions.assertFalse(PartitionStateToken.isTainted(p2()));
    }

    // ---------------------------------------------------------------------------------------------
    // the conditions of the token chain
    // ---------------------------------------------------------------------------------------------

    // the backup table: a copy of test_tbl2 as the backup meta has it
    private OlapTable backupCopy() {
        return tbl2.selectiveCopy(null, IndexExtState.VISIBLE, true);
    }

    // p1 restored onto itself, every condition satisfied
    private RestoreReuseJudge.Input tokenInput() {
        BackupJobInfo info = tokenBackupJobInfo();
        RestoreReuseJudge.Input in = new RestoreReuseJudge.Input();
        in.atomicRestore = false;
        in.allowLoad = false;
        in.cloudMode = false;
        in.minPartitionBytes = 100;
        in.backupPartition = partInfoOf(info, CatalogMocker.TEST_PARTITION1_NAME);
        in.localTable = tbl2;
        in.localPartition = p1();
        in.backupOlapTable = backupCopy();
        in.localStateToken = PartitionStateToken.effective(p1());
        in.backupStateToken = in.backupPartition.stateToken;
        tbl2.setState(OlapTableState.RESTORE);
        return in;
    }

    private String structure(OlapTable backupTbl) {
        RestoreReuseJudge.Input in = tokenInput();
        in.backupOlapTable = backupTbl;
        return RestoreReuseJudge.firstRejectByToken(in);
    }

    @Test
    public void testTokenConditions() {
        RestoreReuseJudge.Input in = tokenInput();
        Assertions.assertNull(RestoreReuseJudge.firstRejectByToken(in));
        Assertions.assertTrue(RestoreReuseJudge.tokenEqual(in));

        in = tokenInput();
        in.backupStateToken = null;
        Assertions.assertEquals("TOKEN_ABSENT_BACKUP", RestoreReuseJudge.firstRejectByToken(in));
        Assertions.assertFalse(RestoreReuseJudge.tokenEqual(in));
        in = tokenInput();
        in.localStateToken = null;
        Assertions.assertEquals("TOKEN_ABSENT_LOCAL", RestoreReuseJudge.firstRejectByToken(in));
        in = tokenInput();
        in.localStateToken = TK2;
        Assertions.assertEquals("TOKEN_MISMATCH", RestoreReuseJudge.firstRejectByToken(in));
        Assertions.assertFalse(RestoreReuseJudge.tokenEqual(in));
        // the same token of another version
        in = tokenInput();
        in.backupPartition.version = V1 + 1;
        Assertions.assertEquals("TOKEN_VERSION_MISMATCH", RestoreReuseJudge.firstRejectByToken(in));
        Assertions.assertTrue(RestoreReuseJudge.tokenEqual(in));

        // the effective token of a partition: tainted, invalidated, or bound to another version is none
        PartitionStateToken.taint(p1());
        Assertions.assertNull(PartitionStateToken.effective(p1()));
        setToken(p1(), TK1);
        tbl2.invalidatePartitionStateTokens();
        Assertions.assertNull(PartitionStateToken.effective(p1()));
        setToken(p1(), TK1);
        p1().updateVisibleVersionAndTime(V1 + 1, p1().getVisibleVersionTime() + 1);
        Assertions.assertNull(PartitionStateToken.effective(p1()));
    }

    @Test
    public void testTokenNeedsTheSameStructure() {
        Assertions.assertNull(structure(backupCopy()));
        Assertions.assertEquals("STRUCTURE_UNKNOWN", structure(null));
        // the model
        OlapTable backup = backupCopy();
        Deencapsulation.setField(backup, "keysType", KeysType.UNIQUE_KEYS);
        Assertions.assertEquals("STRUCTURE_MODEL", structure(backup));
        // the partition type and range
        backup = backupCopy();
        Deencapsulation.setField(backup, "partitionInfo", new SinglePartitionInfo());
        Assertions.assertEquals("STRUCTURE_RANGE", structure(backup));
        // the distribution of the partition
        backup = backupCopy();
        Partition backupPart = backup.getPartition(CatalogMocker.TEST_PARTITION1_NAME);
        Deencapsulation.setField(backupPart, "distributionInfo",
                new RandomDistributionInfo(backupPart.getDistributionInfo().getBucketNum() + 1));
        Assertions.assertEquals("STRUCTURE_DISTRIBUTION", structure(backup));
        // an index of the table only on one side
        backup = backupCopy();
        backup.setIndexes(Lists.newArrayList(new Index(70001L, "idx_k1", Lists.newArrayList("k1"),
                IndexType.INVERTED, Maps.newHashMap(), null)));
        Assertions.assertEquals("STRUCTURE_TABLE_INDEX", structure(backup));
        // the bloom filter columns
        backup = backupCopy();
        backup.setBloomFilterInfo(Sets.newHashSet("k1"), 0.05);
        Assertions.assertEquals("STRUCTURE_BLOOM_FILTER", structure(backup));
        // the definition of a materialized index: another WHERE
        backup = backupCopy();
        Deencapsulation.setField(backup.getIndexMetaByIndexId(CatalogMocker.TEST_ROLLUP_ID), "defineStmt",
                new OriginStatement("CREATE MATERIALIZED VIEW test_rollup AS SELECT k1, k2, v1 FROM test_tbl2 "
                        + "WHERE k1 > 1", 0));
        Assertions.assertEquals("STRUCTURE_INDEX_DEFINE", structure(backup));
        // the short key column count
        backup = backupCopy();
        Deencapsulation.setField(backup.getIndexMetaByIndexId(backup.getBaseIndexId()), "shortKeyColumnCount",
                (short) (tbl2.getIndexMetaByIndexId(tbl2.getBaseIndexId()).getShortKeyColumnCount() + 1));
        Assertions.assertEquals("STRUCTURE_INDEX_DEFINE", structure(backup));
        // a column: the default value, the unique id, nullable; a comment is metadata only
        backup = backupCopy();
        backup.getBaseSchema().get(0).setComment("another comment");
        Assertions.assertNull(structure(backup));
        backup = backupCopy();
        Deencapsulation.setField(backup.getBaseSchema().get(1), "defaultValue", "42");
        Assertions.assertEquals("STRUCTURE_COLUMN", structure(backup));
        backup = backupCopy();
        backup.getBaseSchema().get(0).setUniqueId(12345);
        Assertions.assertEquals("STRUCTURE_COLUMN", structure(backup));
        backup = backupCopy();
        Column col = backup.getBaseSchema().get(2);
        col.setIsAllowNull(!col.isAllowNull());
        Assertions.assertEquals("STRUCTURE_COLUMN", structure(backup));

        // the indexes and the tablets of the backup partition, as the job info has them
        RestoreReuseJudge.Input in = tokenInput();
        in.backupPartition.indexes.remove(CatalogMocker.TEST_ROLLUP_NAME);
        Assertions.assertEquals("STRUCTURE_INDEX_COUNT", RestoreReuseJudge.firstRejectByToken(in));
        in = tokenInput();
        in.backupPartition.indexes.put("another_rollup",
                in.backupPartition.indexes.remove(CatalogMocker.TEST_ROLLUP_NAME));
        Assertions.assertEquals("STRUCTURE_INDEX_NAME", RestoreReuseJudge.firstRejectByToken(in));
        in = tokenInput();
        in.backupPartition.getIdx(CatalogMocker.TEST_ROLLUP_NAME).sortedTabletInfoList
                .add(new BackupTabletInfo(777777L, Lists.newArrayList()));
        Assertions.assertEquals("STRUCTURE_INDEX_TABLETS", RestoreReuseJudge.firstRejectByToken(in));
    }

    @Test
    public void testTokenSharesTheModeSupportedSizeAndReplicaConditions() {
        // the mode: a non-atomic restore into a table forbidden to write
        RestoreReuseJudge.Input in = tokenInput();
        in.allowLoad = true;
        Assertions.assertEquals("ALLOW_LOAD", RestoreReuseJudge.firstRejectByToken(in));
        in = tokenInput();
        tbl2.setState(OlapTableState.NORMAL);
        Assertions.assertEquals("TABLE_STATE_NORMAL", RestoreReuseJudge.firstRejectByToken(in));
        // atomic: the switch, the binding, the state of the table being replaced
        in = tokenInput();
        in.atomicRestore = true;
        tbl2.setState(OlapTableState.NORMAL);
        Config.enable_restore_atomic_reuse = false;
        Assertions.assertEquals("ATOMIC_RESTORE", RestoreReuseJudge.firstRejectByToken(in));
        Config.enable_restore_atomic_reuse = true;
        Assertions.assertNull(RestoreReuseJudge.firstRejectByToken(in));
        in.allowLoad = true;
        Assertions.assertEquals("ALLOW_LOAD", RestoreReuseJudge.firstRejectByToken(in));
        in.allowLoad = false;
        in.atomicReject = "ATOMIC_NOT_BOUND_REPLICA";
        Assertions.assertEquals("ATOMIC_NOT_BOUND_REPLICA", RestoreReuseJudge.firstRejectByToken(in));
        in.atomicReject = null;
        tbl2.setState(OlapTableState.SCHEMA_CHANGE);
        Assertions.assertEquals("TABLE_STATE_SCHEMA_CHANGE", RestoreReuseJudge.firstRejectByToken(in));

        // supported
        in = tokenInput();
        in.cloudMode = true;
        Assertions.assertEquals("CLOUD_MODE", RestoreReuseJudge.firstRejectByToken(in));
        Deencapsulation.setField(tbl2, "keysType", KeysType.AGG_KEYS);
        Assertions.assertEquals("AGGREGATE_TABLE", RestoreReuseJudge.firstRejectByToken(tokenInput()));
        // unique merge-on-read: only with restore_reuse_allow_hidden_state_models
        Deencapsulation.setField(tbl2, "keysType", KeysType.UNIQUE_KEYS);
        tbl2.setEnableUniqueKeyMergeOnWrite(false);
        Config.restore_reuse_allow_hidden_state_models = false;
        Assertions.assertEquals("HIDDEN_STATE_MOR", RestoreReuseJudge.firstRejectByToken(tokenInput()));
        Config.restore_reuse_allow_hidden_state_models = true;
        Assertions.assertNull(RestoreReuseJudge.firstRejectByToken(tokenInput()));
        // unique merge-on-write: only with restore_reuse_mow, never with a sequence column
        tbl2.setEnableUniqueKeyMergeOnWrite(true);
        Config.restore_reuse_mow = false;
        Assertions.assertEquals("HIDDEN_STATE_MOW", RestoreReuseJudge.firstRejectByToken(tokenInput()));
        Config.restore_reuse_mow = true;
        Assertions.assertNull(RestoreReuseJudge.firstRejectByToken(tokenInput()));
        tbl2.setSequenceMapCol("v1");
        Assertions.assertEquals("HIDDEN_STATE_MOW_SEQUENCE", RestoreReuseJudge.firstRejectByToken(tokenInput()));
        tbl2.setSequenceMapCol(null);
        Deencapsulation.setField(tbl2, "keysType", KeysType.DUP_KEYS);
        Assertions.assertNull(RestoreReuseJudge.firstRejectByToken(tokenInput()));
        // cooled down to the remote storage
        Tablet tablet = p1().getBaseIndex().getTablets().get(0);
        Replica cooldown = tablet.getReplicas().get(0);
        tablet.setCooldownConf(cooldown.getId(), 1);
        cooldown.setRemoteDataSize(500);
        try {
            Assertions.assertEquals("REMOTE_STORAGE", RestoreReuseJudge.firstRejectByToken(tokenInput()));
        } finally {
            cooldown.setRemoteDataSize(0);
            tablet.setCooldownConf(-1, -1);
        }
        Assertions.assertNull(RestoreReuseJudge.firstRejectByToken(tokenInput()));

        // the size
        in = tokenInput();
        in.minPartitionBytes = RestoreReuseShadowStats.getSingleReplicaLocalDataSize(p1()) + 1;
        Assertions.assertEquals("TOO_SMALL", RestoreReuseJudge.firstRejectByToken(in));
        // every replica is healthy at the version of the backup
        Replica behind = p1().getBaseIndex().getTablets().get(0).getReplicas().get(1);
        behind.updateVersionForRestore(V1 - 1);
        Assertions.assertEquals("REPLICA_UNHEALTHY", RestoreReuseJudge.firstRejectByToken(tokenInput()));
        behind.updateVersionForRestore(V1);
        Assertions.assertNull(RestoreReuseJudge.firstRejectByToken(tokenInput()));
    }

    @Test
    public void testAPartitionNotKeptByItsTokenIsDownloadedByTheJob() {
        // too small: the token is the same, the partition is downloaded
        Config.restore_reuse_min_partition_bytes = RestoreReuseShadowStats.getSingleReplicaLocalDataSize(p1()) + 1;
        RestoreJob job = toSnapshoting(tokenBackupJobInfo());
        assertDownloadsAll(job);
    }

    // ---------------------------------------------------------------------------------------------
    // the capability of the nodes
    // ---------------------------------------------------------------------------------------------

    private final List<Frontend> frontends = Lists.newArrayList();
    private final List<Backend> aliveBackends = Lists.newArrayList();

    private void frontend(FrontendNodeType role, String host, int capability) {
        Frontend fe = new Frontend(role, host, host, 9010);
        fe.setRestoreReuseCapability(capability);
        frontends.add(fe);
    }

    private void backend(long id, int capability) {
        Backend be = new Backend(id, "be" + id, 9050);
        be.setRestoreReuseCapability(capability);
        be.setAlive(true);
        aliveBackends.add(be);
    }

    // the cluster as the master (fe1) sees it
    private void mockCluster() {
        Mockito.when(env.getSelfNode()).thenReturn(new SystemInfoService.HostInfo("fe1", 9010));
        Mockito.when(env.getFrontends(Mockito.any())).thenAnswer(inv -> {
            FrontendNodeType type = inv.getArgument(0);
            return frontends.stream().filter(fe -> type == null || fe.getRole() == type)
                    .collect(Collectors.toList());
        });
        Mockito.when(systemInfo.getAllClusterBackendsNoException()).thenAnswer(inv -> {
            Map<Long, Backend> map = Maps.newHashMap();
            for (Backend be : aliveBackends) {
                map.put(be.getId(), be);
            }
            return ImmutableMap.copyOf(map);
        });
    }

    private void assertMixedVersions(RestoreJob job) {
        RestoreReuseResult result = job.getReuseResult();
        Assertions.assertNotNull(result);
        Assertions.assertTrue(result.getDecisions().isEmpty());
        Assertions.assertEquals(2L, (long) result.getRejected().get(RestoreJob.MIXED_VERSIONS));
        Assertions.assertEquals(0, result.getKeptPartitions());
        String estimate = reuseEstimate(job);
        Assertions.assertTrue(estimate.contains("\"mixed_versions\":2"), estimate);
        Assertions.assertTrue(estimate.contains("\"kept_token\":0"), estimate);
        assertDownloadsAll(job);
    }

    @Test
    public void testAllNodesCapableKeepByToken() {
        mockCluster();
        // the master itself has not been heard from (0), it is this build
        frontend(FrontendNodeType.FOLLOWER, "fe1", 0);
        frontend(FrontendNodeType.FOLLOWER, "fe2", 1);
        frontend(FrontendNodeType.OBSERVER, "fe3", 0);
        backend(1, 1);
        backend(2, 1);
        Assertions.assertNull(RestoreJob.findNodeWithoutReuseCapability());
        Assertions.assertEquals(2, toSnapshoting(tokenBackupJobInfo()).getReuseResult().getKeptPartitions());
    }

    @Test
    public void testBackendWithoutCapabilityDownloadsAll() {
        mockCluster();
        frontend(FrontendNodeType.FOLLOWER, "fe1", 0);
        backend(1, 1);
        backend(2, 0);
        Assertions.assertTrue(RestoreJob.findNodeWithoutReuseCapability().contains("be2"));
        assertMixedVersions(toSnapshoting(tokenBackupJobInfo()));
    }

    @Test
    public void testFollowerWithoutCapabilityDownloadsAll() {
        mockCluster();
        frontend(FrontendNodeType.FOLLOWER, "fe1", 0);
        frontend(FrontendNodeType.FOLLOWER, "fe2", 0);
        backend(1, 1);
        Assertions.assertTrue(RestoreJob.findNodeWithoutReuseCapability().contains("fe2"));
        assertMixedVersions(toSnapshoting(tokenBackupJobInfo()));
    }

    @Test
    public void testAtomicWithAnOldBackendDownloadsAll() {
        mockCluster();
        frontend(FrontendNodeType.FOLLOWER, "fe1", 0);
        backend(1, 0);
        RestoreJob job = atomicToSnapshoting(tokenBackupJobInfo());
        Assertions.assertEquals(2L, (long) job.getReuseResult().getRejected().get(RestoreJob.MIXED_VERSIONS));
        Assertions.assertEquals(0, job.getReuseResult().getKeptAtomicPartitions());
        // an old backend would ignore the local source: none is asked for
        Assertions.assertFalse(snapshotTasks(submitted).isEmpty());
        for (SnapshotTask task : snapshotTasks(submitted)) {
            Assertions.assertFalse(task.isRestoreLocalSource());
            Assertions.assertFalse(task.isRestoreKeptLoad());
        }
        Assertions.assertTrue(job.getLocalSourceStagingPartitions().isEmpty());
    }

    @Test
    public void testCapabilityIsNotCheckedIfNotRequired() {
        mockCluster();
        frontend(FrontendNodeType.FOLLOWER, "fe1", 0);
        backend(1, 0);
        Config.restore_reuse_require_capability = false;
        Assertions.assertNull(RestoreJob.findNodeWithoutReuseCapability());
        RestoreJob job = toSnapshoting(tokenBackupJobInfo());
        Assertions.assertNull(job.getReuseResult().getRejected().get(RestoreJob.MIXED_VERSIONS));
        Assertions.assertEquals(2, job.getReuseResult().getKeptPartitions());
    }

    // ---------------------------------------------------------------------------------------------
    // the recheck before the commit, the persistence
    // ---------------------------------------------------------------------------------------------

    // a non-atomic restore whose partitions are all kept by their token, at the commit
    private RestoreJob keptJobAtCommit() {
        RestoreJob job = toSnapshoting(tokenBackupJobInfo());
        Assertions.assertEquals(2, job.getReuseResult().getKeptPartitions());
        Deencapsulation.setField(job, "state", RestoreJobState.COMMITTING);
        return job;
    }

    @Test
    public void testRecheckOfTheVersionBeforeTheCommitFailsTheJob() {
        RestoreJob job = keptJobAtCommit();
        // a write that should never happen
        p2().updateVersionForRestore(V1 + 3);
        Status st = job.allTabletCommitted(false);
        Assertions.assertFalse(st.ok());
        Assertions.assertTrue(st.getErrMsg().contains("changed from " + V1 + " to " + (V1 + 3)), st.getErrMsg());
        Assertions.assertTrue(st.getErrMsg().contains("p2"), st.getErrMsg());
        // nothing is committed
        Assertions.assertEquals(OlapTableState.RESTORE, tbl2.getState());
        Assertions.assertEquals(V1 + 3, p2().getVisibleVersion());

        // before any tablet is moved
        Deencapsulation.setField(job, "state", RestoreJobState.COMMIT);
        submitted.clear();
        job.commit();
        Assertions.assertFalse(job.getStatus().ok());
        Assertions.assertEquals(RestoreJobState.COMMIT, job.getState());
        Assertions.assertTrue(moveTasks(submitted).isEmpty());
    }

    @Test
    public void testRecheckOfTheTokenBeforeTheCommitFailsTheJob() {
        RestoreJob job = keptJobAtCommit();
        Assertions.assertTrue(job.checkKeptPartitions().ok());
        // a schema change job finished since the judgement: the same version, the token is invalidated
        tbl2.invalidatePartitionStateTokens();
        Status st = job.allTabletCommitted(false);
        Assertions.assertFalse(st.ok());
        Assertions.assertTrue(st.getErrMsg().contains("state token"), st.getErrMsg());
        Assertions.assertEquals(OlapTableState.RESTORE, tbl2.getState());
        // an admin operation on a replica taints it
        setToken(p1(), TK1);
        setToken(p2(), TK2);
        Assertions.assertTrue(job.checkKeptPartitions().ok());
        PartitionStateToken.taint(p1());
        Assertions.assertFalse(job.checkKeptPartitions().ok());
        // another token of the same version
        setToken(p1(), TK2);
        Assertions.assertFalse(job.checkKeptPartitions().ok());
        setToken(p1(), TK1);
        Assertions.assertTrue(job.allTabletCommitted(false).ok());
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
        tbl2.invalidatePartitionStateTokens();
        // the follower trusts the master
        Status st = job.allTabletCommitted(true);
        Assertions.assertTrue(st.ok(), st.toString());
    }

    @Test
    public void testPersistAndReplay() throws Exception {
        setToken(p1(), "another-token");
        RestoreJob job = toSnapshoting(localTokenBackupJobInfo());
        // the DOWNLOAD edit log
        Deencapsulation.setField(job, "state", RestoreJobState.DOWNLOAD);
        RestoreJob replayed = writeAndRead(job);

        RestoreReuseResult result = replayed.getReuseResult();
        Assertions.assertNotNull(result);
        Assertions.assertEquals(job.getReuseResult().toString(), result.toString());
        Assertions.assertEquals(1, result.getKeptPartitions());
        RestoreReuseResult.Decision kept = result.getKept(tbl2.getId(), p2().getId());
        Assertions.assertNotNull(kept);
        Assertions.assertEquals(RestoreReuseResult.KEPT_STATE_TOKEN, kept.reason);
        Assertions.assertTrue(kept.tokenEqual);
        Assertions.assertEquals(V1, kept.version);
        Assertions.assertTrue(versionInfo(replayed).contains(tbl2.getId(), p1().getId()));
        Assertions.assertFalse(versionInfo(replayed).contains(tbl2.getId(), p2().getId()));
        Assertions.assertEquals(replicasOf(p1()), fileMapping(replayed).getMapping().size());

        // the follower replays it: the token of the partition to download is invalidated, the kept one stays
        setToken(p1(), "another-token");
        p1().setState(Partition.PartitionState.NORMAL);
        replayed.setEnv(env);
        replayed.replayRun();
        Assertions.assertNull(PartitionStateToken.effective(p1()));
        Assertions.assertEquals(TK2, PartitionStateToken.effective(p2()));

        // the finished job (the log written at the end) keeps the result and whether the tokens are stamped
        Deencapsulation.setField(job, "state", RestoreJobState.COMMITTING);
        Status st = job.allTabletCommitted(false);
        Assertions.assertTrue(st.ok(), st.toString());
        RestoreJob finished = writeAndRead(job);
        Assertions.assertEquals(RestoreJobState.FINISHED, finished.getState());
        Assertions.assertEquals(1, finished.getReuseResult().getKeptPartitions());
        Assertions.assertTrue((boolean) Deencapsulation.getField(finished, "stampStateTokens"));
        Assertions.assertEquals(TK1, PartitionStateToken.effective(p1()));
        Assertions.assertEquals(TK2, PartitionStateToken.effective(p2()));
        // the follower commits the same: the tokens are those of the backup
        p1().setStateToken(null);
        finished.setEnv(env);
        Assertions.assertTrue(finished.allTabletCommitted(true).ok());
        Assertions.assertEquals(TK1, PartitionStateToken.effective(p1()));
        Assertions.assertEquals(TK2, PartitionStateToken.effective(p2()));
    }

    // ---------------------------------------------------------------------------------------------
    // the token the commit leaves: an overwritten partition, a new partition, a new table, an atomic restore
    // ---------------------------------------------------------------------------------------------

    @Test
    public void testTheCommitAdoptsTheTokenOfAnOverwrittenPartition() {
        Config.restore_reuse_trust_state_token = false;
        RestoreJob job = toSnapshoting(tokenBackupJobInfo());
        assertDownloadsAll(job);
        Deencapsulation.setField(job, "state", RestoreJobState.COMMITTING);
        Assertions.assertTrue(job.allTabletCommitted(false).ok());
        Assertions.assertEquals(TK1, PartitionStateToken.effective(p1()));
        Assertions.assertEquals(TK2, PartitionStateToken.effective(p2()));
    }

    @Test
    public void testTheCommitGivesNoTokenWithoutTheTracking() {
        RestoreJob job = keptJobAtCommit();
        // the master does not track the tokens when it commits the job: no name, the kept ones too
        Config.restore_state_token_track = false;
        Assertions.assertTrue(job.allTabletCommitted(false).ok());
        Assertions.assertNull(PartitionStateToken.effective(p1()));
        Assertions.assertNull(PartitionStateToken.effective(p2()));
        Assertions.assertTrue(p1().getStateToken().isBad());
        Assertions.assertTrue(p2().getStateToken().isBad());
    }

    @Test
    public void testTheCommitAdoptsTheTokenOfANewPartition() throws Exception {
        // p2 of the backup is [20, 30), not in the table: the restore creates it, at the range of the backup
        Column k1 = tbl2.getBaseSchema().get(0);
        tbl2.getPartitionInfo().setItem(CatalogMocker.TEST_PARTITION2_ID, false, new RangePartitionItem(
                Range.closedOpen(PartitionKey.createPartitionKey(Lists.newArrayList(new PartitionValue("20")),
                        Lists.newArrayList(k1)),
                        PartitionKey.createPartitionKey(Lists.newArrayList(new PartitionValue("30")),
                                Lists.newArrayList(k1)))));
        // CatalogMocker does not set them, the new partition is added with those of the backup
        tbl2.getPartitionInfo().setIsInMemory(CatalogMocker.TEST_PARTITION2_ID, false);
        tbl2.getPartitionInfo().setIsMutable(CatalogMocker.TEST_PARTITION2_ID, true);
        Map<Long, Partition> idToPartition = Deencapsulation.getField(tbl2, "idToPartition");
        Map<String, Partition> nameToPartition = Deencapsulation.getField(tbl2, "nameToPartition");
        BackupJobInfo info = tokenBackupJobInfo();
        RestoreJob job = newJob(info);
        Partition p2 = p2();
        idToPartition.remove(p2.getId());
        nameToPartition.remove(p2.getName());
        tbl2.getPartitionInfo().dropPartition(p2.getId());
        prepare(job);
        Deencapsulation.invoke(job, "allReplicasCreated");
        Assertions.assertTrue(job.getStatus().ok(), job.getStatus().toString());
        Partition restored = tbl2.getPartition(CatalogMocker.TEST_PARTITION2_NAME);
        Assertions.assertNotNull(restored);
        Assertions.assertNotEquals(p2.getId(), restored.getId());
        // p1 is kept by its token
        Assertions.assertEquals(1, job.getReuseResult().getKeptPartitions());
        Assertions.assertTrue(job.getReuseResult().isKept(tbl2.getId(), p1().getId()));
        Deencapsulation.setField(job, "state", RestoreJobState.COMMITTING);
        Status st = job.allTabletCommitted(false);
        Assertions.assertTrue(st.ok(), st.toString());
        Assertions.assertEquals(V1, restored.getVisibleVersion());
        Assertions.assertEquals(TK2, PartitionStateToken.effective(restored));
        Assertions.assertEquals(TK1, PartitionStateToken.effective(p1()));
    }

    @Test
    public void testTheCommitAdoptsTheTokenOfANewTable() {
        // test_tbl2 is not in the database: the restore creates it
        db.unregisterTable(CatalogMocker.TEST_TBL2_NAME);
        Assertions.assertNull(db.getTableNullable(CatalogMocker.TEST_TBL2_NAME));
        RestoreJob job = prepare(newJob(tokenBackupJobInfo()));
        // nothing to keep: no local partition
        Assertions.assertTrue(job.getReuseResult() == null || job.getReuseResult().getKeptPartitions() == 0);
        Deencapsulation.invoke(job, "allReplicasCreated");
        Assertions.assertTrue(job.getStatus().ok(), job.getStatus().toString());
        OlapTable created = (OlapTable) db.getTableNullable(CatalogMocker.TEST_TBL2_NAME);
        Assertions.assertNotNull(created);
        Assertions.assertNotEquals(tbl2.getId(), created.getId());
        Deencapsulation.setField(job, "state", RestoreJobState.COMMITTING);
        Status st = job.allTabletCommitted(false);
        Assertions.assertTrue(st.ok(), st.toString());
        Assertions.assertEquals(2, created.getPartitions().size());
        for (Partition part : created.getPartitions()) {
            Assertions.assertEquals(V1, part.getVisibleVersion());
            Assertions.assertEquals(tokenOf(part), PartitionStateToken.effective(part));
        }
    }

    // ---------------------------------------------------------------------------------------------
    // atomic restore: the partitions of the table being replaced are the source of the data
    // ---------------------------------------------------------------------------------------------

    @Test
    public void testAtomicKeepByTokenFlowToCommit() throws Exception {
        BackupHandler handler = newHandler();
        Config.restore_reuse_kept_load_at_snapshot = true;
        RestoreJob job = atomicToSnapshoting(tokenBackupJobInfo());
        OlapTable stagingTbl = staging(job);
        Partition stagingP1 = stagingTbl.getPartition(p1().getName());
        Partition stagingP2 = stagingTbl.getPartition(p2().getName());
        // the table being replaced is not touched
        Assertions.assertEquals(OlapTableState.NORMAL, tbl2.getState());
        Assertions.assertTrue(versionInfo(job).isEmpty());

        RestoreReuseResult result = job.getReuseResult();
        Assertions.assertEquals(2, result.getKeptPartitions());
        Assertions.assertEquals(2, result.getKeptAtomicPartitions());
        Assertions.assertEquals(2, result.countKeptReason(RestoreReuseResult.KEPT_STATE_TOKEN));
        for (RestoreReuseResult.Decision decision : result.getDecisions()) {
            Assertions.assertTrue(decision.kept);
            Assertions.assertTrue(decision.atomic);
            Assertions.assertTrue(decision.tokenEqual);
            // the decision is about the table being replaced, the tasks work on the staging table
            Assertions.assertEquals(tbl2.getId(), decision.tableId);
            Assertions.assertEquals(stagingTbl.getId(), decision.stagingTableId);
            Partition local = tbl2.getPartition(decision.partitionId);
            Assertions.assertEquals(stagingTbl.getPartition(local.getName()).getId(), decision.stagingPartitionId);
            Assertions.assertEquals(V1, decision.version);
        }
        Assertions.assertNotNull(result.getKept(stagingTbl.getId(), stagingP1.getId()));
        Assertions.assertNull(result.getKept(tbl2.getId(), p1().getId()));
        Assertions.assertTrue(result.getKeptAtomicBytesAllReplicas() > 0);
        // the staging partitions are protected from the tablet scheduler
        Assertions.assertEquals(stagingPartitionIds(job, p1(), p2()), job.getLocalSourceStagingPartitions());
        Assertions.assertTrue(job.isLocalSourceStagingPartition(stagingP1.getId()));
        Assertions.assertFalse(job.isLocalSourceStagingPartition(p1().getId()));
        Assertions.assertEquals(Sets.newHashSet(job.getJobId()), handler.getJobsWithLocalSource());

        // the snapshot tasks of the staging tablets load the local tablets at the version of the backup
        List<SnapshotTask> snapshots = snapshotTasks(submitted);
        Assertions.assertEquals(replicasOf(stagingP1) + replicasOf(stagingP2), snapshots.size());
        List<Long> localTablets = Lists.newArrayList(tabletIds(p1()));
        localTablets.addAll(tabletIds(p2()));
        for (SnapshotTask task : snapshots) {
            Assertions.assertTrue(task.isRestoreLocalSource());
            Assertions.assertTrue(task.isRestoreKeptLoad());
            Assertions.assertEquals(V1, task.getVersion());
            Assertions.assertEquals(stagingTbl.getId(), task.getTableId());
            TSnapshotRequest request = task.toThrift();
            Assertions.assertTrue(request.isRestoreLocalSource());
            Assertions.assertTrue(request.isRestoreKeptLoad());
            Assertions.assertEquals(job.getJobId(), request.getJobId());
            Assertions.assertEquals(V1, request.getVersion());
            Assertions.assertTrue(localTablets.contains(request.getRefTabletId()));
            Assertions.assertNotEquals(task.getTabletId(), request.getRefTabletId());
        }
        finishSnapshots(job, snapshots);

        // nothing is downloaded
        Deencapsulation.invoke(job, "waitingAllSnapshotsFinished");
        Assertions.assertEquals(RestoreJobState.DOWNLOAD, job.getState());
        submitted.clear();
        Deencapsulation.invoke(job, "downloadSnapshots");
        Assertions.assertEquals(RestoreJobState.DOWNLOADING, job.getState());
        Assertions.assertTrue(submitted.stream().noneMatch(t -> t instanceof DownloadTask), submitted.toString());
        Deencapsulation.invoke(job, "waitingAllDownloadFinished");
        Assertions.assertEquals(RestoreJobState.COMMIT, job.getState());

        // the move of every staging tablet confirms the load, the request is the plain move
        submitted.clear();
        job.commit();
        List<DirMoveTask> moves = moveTasks(submitted);
        Assertions.assertEquals(replicasOf(stagingP1) + replicasOf(stagingP2), moves.size());
        for (DirMoveTask move : moves) {
            Assertions.assertTrue(move.isLocalSourceKept());
            Assertions.assertTrue(move.isLocalSourceLoaded());
            Assertions.assertNull(move.getIncrementalRange());
            Assertions.assertEquals(job.getJobId(), move.toThrift().getJobId());
        }

        // observable
        String estimate = reuseEstimate(job);
        Assertions.assertTrue(estimate.contains("\"kept_atomic\":2"), estimate);
        Assertions.assertTrue(estimate.contains("\"kept_token\":2"), estimate);
        Assertions.assertTrue(estimate.contains("\"kept_partitions\":2"), estimate);
        Assertions.assertTrue(estimate.contains("\"kept_atomic_bytes_all_replicas\":"
                + result.getKeptAtomicBytesAllReplicas()), estimate);

        // persisted: the decisions with the staging ids, the protection is rebuilt from them
        RestoreJob replayed = writeAndRead(job);
        RestoreReuseResult.Decision kept = replayed.getReuseResult().getKept(stagingTbl.getId(), stagingP1.getId());
        Assertions.assertNotNull(kept);
        Assertions.assertTrue(kept.atomic);
        Assertions.assertEquals(RestoreReuseResult.KEPT_STATE_TOKEN, kept.reason);
        Assertions.assertEquals(tbl2.getId(), kept.tableId);
        Assertions.assertEquals(p1().getId(), kept.partitionId);
        Assertions.assertEquals(job.getLocalSourceStagingPartitions(), replayed.getLocalSourceStagingPartitions());

        // the commit replaces the table, which is the staging one at the version of the backup
        for (DirMoveTask move : moves) {
            Assertions.assertTrue(job.finishDirMoveTask(move, ok()));
        }
        // an admin operation on a replica of the local p1 after it was judged: the token is not the one it was kept by,
        // the table is not replaced
        PartitionStateToken.taint(p1());
        Status st = job.allTabletCommitted(false);
        Assertions.assertFalse(st.ok());
        Assertions.assertTrue(st.getErrMsg().contains("state token"), st.getErrMsg());
        Assertions.assertSame(tbl2, db.getTableNullable(CatalogMocker.TEST_TBL2_NAME));
        setToken(p1(), TK1);
        st = job.allTabletCommitted(false);
        Assertions.assertTrue(st.ok(), st.toString());
        OlapTable replaced = (OlapTable) db.getTableNullable(CatalogMocker.TEST_TBL2_NAME);
        Assertions.assertEquals(stagingTbl.getId(), replaced.getId());
        Assertions.assertEquals(OlapTableState.NORMAL, replaced.getState());
        for (Partition part : replaced.getPartitions()) {
            Assertions.assertEquals(V1, part.getVisibleVersion());
            for (Replica replica : part.getBaseIndex().getTablets().get(0).getReplicas()) {
                Assertions.assertEquals(V1, replica.getVersion());
            }
        }
        // the partitions of the replaced table adopted the tokens of the backup
        Assertions.assertEquals(TK1, PartitionStateToken.effective(replaced.getPartition(stagingP1.getId())));
        Assertions.assertEquals(TK2, PartitionStateToken.effective(replaced.getPartition(stagingP2.getId())));
        Assertions.assertEquals(RestoreJobState.FINISHED, job.getState());
        // done: nothing is protected any more
        Assertions.assertTrue(job.getLocalSourceStagingPartitions().isEmpty());
        Assertions.assertFalse(handler.hasJobsWithLocalSource());
    }

    @Test
    public void testAtomicKeptLoadSwitchOffMakesThePlainLocalSnapshot() {
        newHandler();
        Config.restore_reuse_kept_load_at_snapshot = false;
        RestoreJob job = atomicKeptJob();
        List<SnapshotTask> snapshots = snapshotTasks(submitted);
        Assertions.assertFalse(snapshots.isEmpty());
        for (SnapshotTask task : snapshots) {
            Assertions.assertTrue(task.isRestoreLocalSource());
            Assertions.assertFalse(task.isRestoreKeptLoad());
            Assertions.assertFalse(task.toThrift().isSetRestoreKeptLoad());
            Assertions.assertEquals(V1, task.getVersion());
        }
        // the move loads the snapshot: the same request, judged the same way
        for (DirMoveTask move : toCommitting(job)) {
            Assertions.assertTrue(move.isLocalSourceKept());
            Assertions.assertTrue(move.isLocalSourceLoaded());
            Assertions.assertNull(move.getIncrementalRange());
        }
        Assertions.assertTrue(job.cancel().ok());

        // the usual atomic restore has none of it
        Config.enable_restore_atomic_reuse = false;
        submitted.clear();
        RestoreJob plain = prepare(newAtomicJob(tokenBackupJobInfo()));
        Deencapsulation.invoke(plain, "allReplicasCreated");
        Assertions.assertEquals(RestoreJobState.SNAPSHOTING, plain.getState());
        Assertions.assertNull(plain.getReuseResult());
        for (SnapshotTask task : snapshotTasks(submitted)) {
            Assertions.assertFalse(task.isRestoreLocalSource());
            Assertions.assertFalse(task.isRestoreKeptLoad());
            Assertions.assertFalse(task.toThrift().isSetVersion());
            // the base tablet is still used by the download, as before
            Assertions.assertTrue(task.toThrift().isSetRefTabletId());
        }
        Assertions.assertTrue(plain.getLocalSourceStagingPartitions().isEmpty());
    }

    @Test
    public void testAtomicPartitionOfAnotherTokenIsDownloaded() {
        newHandler();
        setToken(p2(), "another-token");
        RestoreJob job = atomicToSnapshoting(localTokenBackupJobInfo());
        RestoreReuseResult result = job.getReuseResult();
        Assertions.assertEquals(1, result.getKeptPartitions());
        OlapTable stagingTbl = staging(job);
        Partition stagingP1 = stagingTbl.getPartition(p1().getName());
        Partition stagingP2 = stagingTbl.getPartition(p2().getName());
        Assertions.assertNotNull(result.getKept(stagingTbl.getId(), stagingP1.getId()));
        Assertions.assertNull(result.getKept(stagingTbl.getId(), stagingP2.getId()));
        // only the kept staging partition is protected
        Assertions.assertEquals(Sets.newHashSet(stagingP1.getId()), job.getLocalSourceStagingPartitions());
        // p2 is a plain restore of the staging partition, as it is without partition level reuse
        for (SnapshotTask task : snapshotTasks(submitted)) {
            boolean ofP1 = task.getPartitionId() == stagingP1.getId();
            Assertions.assertEquals(ofP1, task.isRestoreLocalSource());
            Assertions.assertEquals(ofP1, task.getVersion() == V1);
            Assertions.assertTrue(task.toThrift().isSetRefTabletId());
        }
        finishSnapshots(job, snapshotTasks(submitted));
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
                Assertions.assertTrue(tabletIds(stagingP2).contains(remote.getLocalTabletId()));
                remotes++;
            }
        }
        Assertions.assertEquals(replicasOf(stagingP2), remotes);
    }

    @Test
    public void testAtomicNotBoundIsNotKept() {
        RestoreJob job = prepare(newAtomicJobWithReuse());
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
        Map<String, OlapTable> bound = Maps.newHashMap();
        bound.put(CatalogMocker.TEST_TBL2_NAME, stagingTbl);
        // the candidates, every one of them kept by its token
        Pair<Long, Long> keyP1 = Pair.of(tbl2.getId(), p1().getId());
        Pair<Long, Long> keyP2 = Pair.of(tbl2.getId(), p2().getId());
        Assertions.assertEquals(Sets.newHashSet(keyP1, keyP2), job.selectReuseCandidates(db, bound, bases));
        // a table that is not bound (schema changed) keeps nothing
        Assertions.assertTrue(job.selectReuseCandidates(db, Maps.newHashMap(), bases).isEmpty());
        // a staging tablet without the base tablet: only that partition is not kept
        Tablet stagingTablet = stagingTbl.getPartition(p1().getName()).getBaseIndex().getTablets().get(0);
        bases.remove(stagingTablet.getId());
        Assertions.assertEquals(Sets.newHashSet(keyP2), job.selectReuseCandidates(db, bound, bases));
        // the job goes on with it: p1 is downloaded
        Deencapsulation.invoke(job, "allReplicasCreated");
        Assertions.assertEquals(1, job.getReuseResult().getKeptAtomicPartitions());
        Assertions.assertTrue(job.getReuseResult().isKept(tbl2.getId(), p2().getId()));
        Assertions.assertEquals(stagingPartitionIds(job, p2()), job.getLocalSourceStagingPartitions());
    }

    private RestoreJob newAtomicJobWithReuse() {
        Config.enable_restore_atomic_reuse = true;
        return newAtomicJob(tokenBackupJobInfo());
    }

    @Test
    public void testAtomicWriteOrTokenChangeBeforeReplaceFailsAndTheTableIsNotReplaced() {
        RestoreJob job = atomicKeptJob();
        OlapTable stagingTbl = staging(job);
        // the check under the lock of the table being replaced
        Assertions.assertTrue(job.checkAtomicLocalVersions(tbl2).ok());
        // a write: the version moved
        p1().updateVersionForRestore(V1 + 1);
        Status st = job.checkAtomicLocalVersions(tbl2);
        Assertions.assertFalse(st.ok());
        Assertions.assertTrue(st.getErrMsg().contains("changed from " + V1 + " to " + (V1 + 1)), st.getErrMsg());
        p1().updateVersionForRestore(V1);
        setToken(p1(), TK1);
        Assertions.assertTrue(job.checkAtomicLocalVersions(tbl2).ok());
        // a load committed and not published yet
        p2().setNextVersion(V1 + 2);
        Assertions.assertFalse(job.checkAtomicLocalVersions(tbl2).ok());
        p2().setNextVersion(V1 + 1);
        Assertions.assertTrue(job.checkAtomicLocalVersions(tbl2).ok());
        // a change that keeps the version: the token is invalidated, tainted, or another one
        tbl2.invalidatePartitionStateTokens();
        st = job.checkAtomicLocalVersions(tbl2);
        Assertions.assertFalse(st.ok());
        Assertions.assertTrue(st.getErrMsg().contains("The table is not replaced"), st.getErrMsg());
        setToken(p1(), TK1);
        setToken(p2(), TK2);
        Assertions.assertTrue(job.checkAtomicLocalVersions(tbl2).ok());
        PartitionStateToken.taint(p2());
        Assertions.assertFalse(job.checkAtomicLocalVersions(tbl2).ok());
        setToken(p2(), TK1);
        Assertions.assertFalse(job.checkAtomicLocalVersions(tbl2).ok());

        // the commit: fails before any tablet is moved and before the tables are replaced
        submitted.clear();
        Deencapsulation.setField(job, "state", RestoreJobState.COMMIT);
        job.commit();
        Assertions.assertFalse(job.getStatus().ok());
        Assertions.assertTrue(moveTasks(submitted).isEmpty());
        st = job.allTabletCommitted(false);
        Assertions.assertFalse(st.ok());
        // the table being replaced is still the table of the name
        Assertions.assertSame(tbl2, db.getTableNullable(CatalogMocker.TEST_TBL2_NAME));
        Assertions.assertSame(stagingTbl, db.getTableNullable(RestoreJob.tableAliasWithAtomicRestore(
                CatalogMocker.TEST_TBL2_NAME)));
    }

    // Adds a second table to the atomic job: the table of the db (test_tbl) is the origin table, a table registered
    // under the temp name is its staging table, both with a local decision at the version of their partition.
    private OlapTable addSecondAtomicTable(RestoreJob job) {
        OlapTable originB = (OlapTable) db.getTableNullable(CatalogMocker.TEST_TBL_NAME);
        Partition partB = originB.getPartitions().iterator().next();
        OlapTable stagingB = new OlapTable(originB.getId() + 7777,
                RestoreJob.tableAliasWithAtomicRestore(CatalogMocker.TEST_TBL_NAME), Lists.newArrayList(),
                KeysType.DUP_KEYS, originB.getPartitionInfo(), originB.getDefaultDistributionInfo());
        // the rename of the table looks up its base index by the name
        Map<String, Long> indexNameToId = Deencapsulation.getField(stagingB, "indexNameToId");
        indexNameToId.put(stagingB.getName(), stagingB.getId());
        db.registerTable(stagingB);
        BackupJobInfo info = Deencapsulation.getField(job, "jobInfo");
        info.backupOlapTableObjects.put(CatalogMocker.TEST_TBL_NAME, new BackupOlapTableInfo());
        RestoreReuseResult.Decision decision = new RestoreReuseResult.Decision();
        decision.tableId = originB.getId();
        decision.partitionId = partB.getId();
        decision.tableName = originB.getName();
        decision.partitionName = partB.getName();
        decision.version = partB.getVisibleVersion();
        decision.kept = true;
        decision.atomic = true;
        job.getReuseResult().addDecision(decision);
        return stagingB;
    }

    @Test
    public void testAtomicMultiTableCheckedBeforeAnyTableIsReplaced() {
        RestoreJob job = atomicKeptJob();
        OlapTable stagingA = staging(job);
        OlapTable stagingB = addSecondAtomicTable(job);
        OlapTable originB = (OlapTable) db.getTableNullable(CatalogMocker.TEST_TBL_NAME);
        Partition partB = originB.getPartitions().iterator().next();
        // a load of the second table is committed and not published yet
        partB.setNextVersion(partB.getVisibleVersion() + 2);

        Mockito.clearInvocations(env);
        Status st = Deencapsulation.invoke(job, "atomicReplaceOlapTables", db, false);
        Assertions.assertFalse(st.ok());
        Assertions.assertTrue(st.getErrMsg().contains(partB.getName()), st.getErrMsg());
        // whatever the order of the tables, none of them is replaced or erased
        Assertions.assertSame(tbl2, db.getTableNullable(CatalogMocker.TEST_TBL2_NAME));
        Assertions.assertSame(originB, db.getTableNullable(CatalogMocker.TEST_TBL_NAME));
        Assertions.assertSame(stagingA, db.getTableNullable(
                RestoreJob.tableAliasWithAtomicRestore(CatalogMocker.TEST_TBL2_NAME)));
        Assertions.assertSame(stagingB, db.getTableNullable(
                RestoreJob.tableAliasWithAtomicRestore(CatalogMocker.TEST_TBL_NAME)));
        Mockito.verify(env, Mockito.never()).onEraseOlapTable(Mockito.any(OlapTable.class), Mockito.anyBoolean());
        Assertions.assertFalse(tbl2.isWriteLockHeldByCurrentThread());
        Assertions.assertFalse(originB.isWriteLockHeldByCurrentThread());

        // the token of the first table changed instead: the second table is not replaced either
        partB.setNextVersion(partB.getVisibleVersion() + 1);
        tbl2.invalidatePartitionStateTokens();
        st = Deencapsulation.invoke(job, "atomicReplaceOlapTables", db, false);
        Assertions.assertFalse(st.ok());
        Assertions.assertSame(tbl2, db.getTableNullable(CatalogMocker.TEST_TBL2_NAME));
        Assertions.assertSame(originB, db.getTableNullable(CatalogMocker.TEST_TBL_NAME));
        Mockito.verify(env, Mockito.never()).onEraseOlapTable(Mockito.any(OlapTable.class), Mockito.anyBoolean());

        // all checked: all replaced
        setToken(p1(), TK1);
        setToken(p2(), TK2);
        st = Deencapsulation.invoke(job, "atomicReplaceOlapTables", db, false);
        Assertions.assertTrue(st.ok(), st.toString());
        Assertions.assertSame(stagingA, db.getTableNullable(CatalogMocker.TEST_TBL2_NAME));
        Assertions.assertSame(stagingB, db.getTableNullable(CatalogMocker.TEST_TBL_NAME));
        Mockito.verify(env).onEraseOlapTable(Mockito.same(tbl2), Mockito.eq(false));
        Mockito.verify(env).onEraseOlapTable(Mockito.same(originB), Mockito.eq(false));
    }

    @Test
    public void testAtomicCancelReleasesTheLocalSnapshotsAndKeepsTheTable() {
        BackupHandler handler = newHandler();
        RestoreJob job = atomicKeptJob();
        List<SnapshotTask> snapshots = snapshotTasks(submitted);
        finishSnapshots(job, snapshots);
        Deencapsulation.invoke(job, "waitingAllSnapshotsFinished");
        submitted.clear();
        job.cancel();
        Assertions.assertEquals(RestoreJobState.CANCELLED, job.getState());
        // every snapshot, the ones made from the local tablets too, is released
        Assertions.assertEquals(snapshots.size(),
                submitted.stream().filter(t -> t instanceof ReleaseSnapshotTask).count());
        // the table being replaced is as before, the staging table is gone, nothing is protected
        Assertions.assertEquals(V1, p1().getVisibleVersion());
        Assertions.assertEquals(TK1, PartitionStateToken.effective(p1()));
        Assertions.assertEquals(TK2, PartitionStateToken.effective(p2()));
        Assertions.assertEquals(OlapTableState.NORMAL, tbl2.getState());
        Assertions.assertFalse(tbl2.isInAtomicRestore());
        Assertions.assertNull(db.getTableNullable(RestoreJob.tableAliasWithAtomicRestore(
                CatalogMocker.TEST_TBL2_NAME)));
        Assertions.assertTrue(job.getLocalSourceStagingPartitions().isEmpty());
        Assertions.assertFalse(handler.hasJobsWithLocalSource());
    }

    // ---------------------------------------------------------------------------------------------
    // the failure of the tasks of the local source
    // ---------------------------------------------------------------------------------------------

    @Test
    public void testNotRetryableKeptLoadSnapshotCancelsAtOnce() {
        newHandler();
        RestoreJob job = atomicKeptJob();
        SnapshotTask task = snapshotTasks(submitted).get(0);
        failed(task, 1);
        Assertions.assertFalse(job.finishTabletSnapshotTask(task, failure(TStatusCode.INTERNAL_ERROR,
                "(127.0.0.1)[INTERNAL_ERROR]" + RestoreJob.RESTORE_NOT_RETRYABLE + " another job loaded it")));
        Assertions.assertEquals(RestoreJobState.CANCELLED, job.getState());
        Assertions.assertTrue(job.getStatus().getErrMsg().contains(RestoreJob.RESTORE_NOT_RETRYABLE),
                job.getStatus().getErrMsg());
        Assertions.assertTrue(job.getLocalSourceStagingPartitions().isEmpty());
        Assertions.assertSame(tbl2, db.getTableNullable(CatalogMocker.TEST_TBL2_NAME));
    }

    @Test
    public void testOtherKeptLoadSnapshotErrorsCancelAfterTheMaxRetry() {
        newHandler();
        Config.restore_local_source_task_max_retry = 2;
        RestoreJob job = atomicKeptJob();
        SnapshotTask task = snapshotTasks(submitted).get(0);
        for (int i = 1; i <= 2; i++) {
            failed(task, 1);
            Assertions.assertFalse(job.finishTabletSnapshotTask(task, failure(TStatusCode.INTERNAL_ERROR, "boom")));
            Assertions.assertEquals(RestoreJobState.SNAPSHOTING, job.getState(), "failure " + i);
        }
        // a failure to obtain the locks is not counted: it is taken back
        failed(task, 1);
        Assertions.assertEquals(3, task.getFailedTimes());
        Assertions.assertFalse(job.finishTabletSnapshotTask(task, failure(TStatusCode.OBTAIN_LOCK_FAILED, "locks")));
        Assertions.assertEquals(2, task.getFailedTimes());
        Assertions.assertEquals(RestoreJobState.SNAPSHOTING, job.getState());
        failed(task, 1);
        Assertions.assertFalse(job.finishTabletSnapshotTask(task, failure(TStatusCode.INTERNAL_ERROR, "boom")));
        Assertions.assertEquals(RestoreJobState.CANCELLED, job.getState());
        Assertions.assertTrue(job.getStatus().getErrMsg().contains("boom"), job.getStatus().getErrMsg());
    }

    @Test
    public void testNotRetryableKeptMoveCancelsAtOnce() {
        newHandler();
        RestoreJob job = atomicKeptJob();
        DirMoveTask move = toCommitting(job).get(0);
        failed(move, 1);
        Assertions.assertFalse(job.finishDirMoveTask(move, failure(TStatusCode.INTERNAL_ERROR,
                "(127.0.0.1)[INTERNAL_ERROR]" + RestoreJob.RESTORE_NOT_RETRYABLE + " no evidence of the load")));
        Assertions.assertEquals(RestoreJobState.CANCELLED, job.getState());
        Assertions.assertTrue(job.getStatus().getErrMsg().contains(RestoreJob.RESTORE_NOT_RETRYABLE),
                job.getStatus().getErrMsg());
        Assertions.assertTrue(job.getLocalSourceStagingPartitions().isEmpty());
        // the table being replaced is not touched
        Assertions.assertSame(tbl2, db.getTableNullable(CatalogMocker.TEST_TBL2_NAME));
        Assertions.assertEquals(TK1, PartitionStateToken.effective(p1()));
    }

    @Test
    public void testOtherKeptMoveErrorsCancelAfterTheMaxRetry() {
        newHandler();
        Config.restore_local_source_task_max_retry = 2;
        RestoreJob job = atomicKeptJob();
        DirMoveTask move = toCommitting(job).get(0);
        for (int i = 1; i <= 2; i++) {
            failed(move, 1);
            Assertions.assertFalse(job.finishDirMoveTask(move, failure(TStatusCode.INTERNAL_ERROR, "boom")));
            Assertions.assertEquals(RestoreJobState.COMMITTING, job.getState(), "failure " + i);
        }
        failed(move, 1);
        Assertions.assertFalse(job.finishDirMoveTask(move, failure(TStatusCode.INTERNAL_ERROR, "boom")));
        Assertions.assertEquals(RestoreJobState.CANCELLED, job.getState());
    }

    @Test
    public void testAFailureAfterTheJobMovedOnIsIgnored() {
        newHandler();
        RestoreJob job = atomicKeptJob();
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
        Config.restore_local_source_task_max_retry = 1;
        // a whole snapshot of an atomic restore without the reuse
        Config.enable_restore_atomic_reuse = false;
        RestoreJob plain = prepare(newAtomicJob(tokenBackupJobInfo()));
        Deencapsulation.invoke(plain, "allReplicasCreated");
        Assertions.assertEquals(RestoreJobState.SNAPSHOTING, plain.getState());
        SnapshotTask whole = snapshotTasks(submitted).get(0);
        Assertions.assertFalse(whole.isRestoreLocalSource());
        failed(whole, 100);
        Assertions.assertFalse(plain.finishTabletSnapshotTask(whole, failure(TStatusCode.INTERNAL_ERROR,
                RestoreJob.RESTORE_NOT_RETRYABLE + " boom")));
        Assertions.assertEquals(RestoreJobState.SNAPSHOTING, plain.getState());
        Assertions.assertEquals(100, whole.getFailedTimes());
    }

    @Test
    public void testTheFailureOfAWholeMoveIsNotConverged() {
        newHandler();
        Config.restore_local_source_task_max_retry = 1;
        // a whole move of a non-atomic restore that keeps p2 and downloads p1
        setToken(p1(), "another-token");
        RestoreJob job = toSnapshoting(localTokenBackupJobInfo());
        Assertions.assertEquals(1, job.getReuseResult().getKeptPartitions());
        DirMoveTask move = toCommitting(job).get(0);
        Assertions.assertFalse(move.isLocalSourceKept());
        failed(move, 100);
        Assertions.assertFalse(job.finishDirMoveTask(move, failure(TStatusCode.INTERNAL_ERROR,
                RestoreJob.RESTORE_NOT_RETRYABLE + " not for this task")));
        Assertions.assertEquals(RestoreJobState.COMMITTING, job.getState());
    }

    // ---------------------------------------------------------------------------------------------
    // backup_restore_merge_idle_ticks and backup_restore_run_on_task_finish on a restore kept by the token
    // ---------------------------------------------------------------------------------------------

    // the state of the job at every edit log written from now on
    private List<RestoreJobState> loggedStates() {
        List<RestoreJobState> states = Lists.newArrayList();
        Mockito.doAnswer(inv -> {
            states.add(((RestoreJob) inv.getArgument(0)).getState());
            return null;
        }).when(editLog).logRestoreJob(Mockito.any(RestoreJob.class));
        return states;
    }

    @Test
    public void testMergedTicksGoFromTheSnapshotsToTheMovesInOneRun() {
        newHandler();
        // the switch off: one state per run
        Config.backup_restore_merge_idle_ticks = false;
        RestoreJob job = atomicKeptJob();
        finishSnapshots(job, snapshotTasks(submitted));
        List<RestoreJobState> states = loggedStates();
        job.run();
        Assertions.assertEquals(RestoreJobState.DOWNLOAD, job.getState());
        job.run();
        Assertions.assertEquals(RestoreJobState.DOWNLOADING, job.getState());
        job.run();
        Assertions.assertEquals(RestoreJobState.COMMIT, job.getState());
        job.run();
        Assertions.assertEquals(RestoreJobState.COMMITTING, job.getState());
        Assertions.assertEquals(Lists.newArrayList(RestoreJobState.DOWNLOAD, RestoreJobState.COMMIT), states);
        Assertions.assertTrue(job.cancel().ok());

        // the switch on, nothing to download: from the snapshots to the moves in one run, the same logs
        Config.backup_restore_merge_idle_ticks = true;
        submitted.clear();
        job = atomicKeptJob();
        finishSnapshots(job, snapshotTasks(submitted));
        submitted.clear();
        states = loggedStates();
        job.run();
        Assertions.assertTrue(job.getStatus().ok(), job.getStatus().toString());
        Assertions.assertEquals(RestoreJobState.COMMITTING, job.getState());
        Assertions.assertEquals(Lists.newArrayList(RestoreJobState.DOWNLOAD, RestoreJobState.COMMIT), states);
        Assertions.assertTrue(submitted.stream().noneMatch(t -> t instanceof DownloadTask));
        Assertions.assertFalse(moveTasks(submitted).isEmpty());
        // waits for the moves
        job.run();
        Assertions.assertEquals(RestoreJobState.COMMITTING, job.getState());
        Assertions.assertEquals(2, states.size());
    }

    // runs everything the nudge thread of the handler has been given so far
    private static void drainNudges(BackupHandler handler) throws Exception {
        ExecutorService executor = Deencapsulation.getField(handler, "nudgeExecutor");
        executor.submit(() -> { }).get(10, TimeUnit.SECONDS);
    }

    private interface Report {
        void send(RestoreJob job);
    }

    // The reports of the tasks a job waits for, in the order they are sent, and a repeat of the last one.
    private static final class NudgeSite {
        private RestoreJob job;
        private final List<Report> reports = Lists.newArrayList();
        private Report duplicateOfLast;
    }

    // RestoreJob.finishTabletSnapshotTask: a kept restore in SNAPSHOTING
    private NudgeSite snapshotSite() {
        NudgeSite site = new NudgeSite();
        site.job = atomicKeptJob();
        List<SnapshotTask> tasks = snapshotTasks(submitted);
        for (SnapshotTask task : tasks) {
            site.reports.add(job -> Assertions.assertTrue(job.finishTabletSnapshotTask(task, snapshotReport(task))));
        }
        SnapshotTask last = tasks.get(tasks.size() - 1);
        site.duplicateOfLast = job -> Assertions.assertFalse(job.finishTabletSnapshotTask(last, snapshotReport(last)));
        return site;
    }

    // RestoreJob.finishTabletDownloadTask: 2 download tasks of 2 backends in DOWNLOADING. The job has no tablet to
    // download (all kept), so the tasks are put in by hand, in place of the ones downloadSnapshots makes.
    private NudgeSite downloadSite() {
        NudgeSite site = new NudgeSite();
        site.job = atomicKeptJob();
        Map<Long, Long> unfinished = Deencapsulation.getField(site.job, "unfinishedSignatureToId");
        unfinished.clear();
        Deencapsulation.setField(site.job, "state", RestoreJobState.DOWNLOADING);
        List<DownloadTask> tasks = Lists.newArrayList();
        for (int i = 0; i < 2; i++) {
            long beId = CatalogMocker.BACKEND1_ID + i;
            DownloadTask task = new DownloadTask(null, beId, 7001 + i, site.job.getJobId(), db.getId(),
                    Lists.newArrayList());
            unfinished.put(task.getSignature(), beId);
            tasks.add(task);
        }
        for (DownloadTask task : tasks) {
            site.reports.add(job -> Assertions.assertTrue(job.finishTabletDownloadTask(task, downloadReport())));
        }
        DownloadTask last = tasks.get(tasks.size() - 1);
        site.duplicateOfLast = job -> Assertions.assertFalse(job.finishTabletDownloadTask(last, downloadReport()));
        return site;
    }

    private static TFinishTaskRequest downloadReport() {
        TFinishTaskRequest report = ok();
        report.setDownloadedTabletIds(Lists.newArrayList());
        return report;
    }

    // RestoreJob.finishDirMoveTask: a kept restore in COMMITTING
    private NudgeSite dirMoveSite() {
        NudgeSite site = new NudgeSite();
        site.job = atomicKeptJob();
        List<DirMoveTask> moves = toCommitting(site.job);
        Assertions.assertTrue(moves.size() >= 2, moves.toString());
        for (DirMoveTask move : moves) {
            site.reports.add(job -> Assertions.assertTrue(job.finishDirMoveTask(move, ok())));
        }
        DirMoveTask last = moves.get(moves.size() - 1);
        site.duplicateOfLast = job -> Assertions.assertFalse(job.finishDirMoveTask(last, ok()));
        return site;
    }

    // Sends the reports of the site to a job of the real handler. The job is a spy whose run() does nothing, so what
    // the nudge does is counted: handler.nudge() (the call of the site) and job.run() (the nudge let through).
    private void sendReports(NudgeSite site, boolean switchOn) throws Exception {
        Config.backup_restore_run_on_task_finish = switchOn;
        Assertions.assertTrue(site.reports.size() >= 2, "the site must have other tasks outstanding");
        BackupHandler handler = Mockito.spy(new BackupHandler(env));
        Mockito.when(env.getBackupHandler()).thenReturn(handler);
        Mockito.when(env.isReady()).thenReturn(true);
        Mockito.when(env.isMaster()).thenReturn(true);
        Deencapsulation.setField(handler, "isInit", true);
        RestoreJob job = Mockito.spy(site.job);
        Mockito.doNothing().when(job).run();
        long dbId = job.getDbId();
        long jobId = job.getJobId();
        Deencapsulation.invoke(handler, "addBackupOrRestoreJob", dbId, job);

        // other tasks outstanding: no nudge
        for (int i = 0; i < site.reports.size() - 1; i++) {
            site.reports.get(i).send(job);
            drainNudges(handler);
            Mockito.verify(handler, Mockito.never()).nudge(Mockito.anyLong(), Mockito.anyLong());
            Mockito.verify(job, Mockito.never()).run();
        }
        // the last outstanding task: the site nudges the job once; it is run only with the switch on
        site.reports.get(site.reports.size() - 1).send(job);
        drainNudges(handler);
        Mockito.verify(handler, Mockito.times(1)).nudge(dbId, jobId);
        Mockito.verify(job, Mockito.times(switchOn ? 1 : 0)).run();
        // a repeat of that report: nothing is outstanding by it, no second nudge
        site.duplicateOfLast.send(job);
        drainNudges(handler);
        Mockito.verify(handler, Mockito.times(1)).nudge(Mockito.anyLong(), Mockito.anyLong());
        Mockito.verify(job, Mockito.times(switchOn ? 1 : 0)).run();
    }

    @Test
    public void testSnapshotReportNudgesOnlyOnTheLastTask() throws Exception {
        sendReports(snapshotSite(), true);
    }

    @Test
    public void testSnapshotReportDoesNotRunTheJobWhenTheSwitchIsOff() throws Exception {
        sendReports(snapshotSite(), false);
    }

    @Test
    public void testDownloadReportNudgesOnlyOnTheLastTask() throws Exception {
        sendReports(downloadSite(), true);
    }

    @Test
    public void testDownloadReportDoesNotRunTheJobWhenTheSwitchIsOff() throws Exception {
        sendReports(downloadSite(), false);
    }

    @Test
    public void testDirMoveReportNudgesOnlyOnTheLastTask() throws Exception {
        sendReports(dirMoveSite(), true);
    }

    @Test
    public void testDirMoveReportDoesNotRunTheJobWhenTheSwitchIsOff() throws Exception {
        sendReports(dirMoveSite(), false);
    }
}
