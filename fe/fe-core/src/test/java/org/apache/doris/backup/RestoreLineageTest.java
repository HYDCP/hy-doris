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
import org.apache.doris.backup.RestoreReuseShadowStats.L0Verdict;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.MaterializedIndex.IndexExtState;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.PartitionInfo;
import org.apache.doris.catalog.Replica;
import org.apache.doris.catalog.Replica.ReplicaState;
import org.apache.doris.catalog.ReplicaAllocation;
import org.apache.doris.catalog.Resource;
import org.apache.doris.catalog.RestoreLineage;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.Tablet;
import org.apache.doris.common.Config;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.Pair;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.datasource.CatalogMgr;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.nereids.trees.plans.commands.ShowRestoreCommand;
import org.apache.doris.persist.EditLog;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.resource.Tag;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.thrift.TStorageMedium;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
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
import java.util.concurrent.atomic.AtomicLong;

public class RestoreLineageTest {
    private static final long SRC_DB_ID = 1000;
    private static final long SRC_TBL_ID = 2000;
    private static final long SRC_P1_ID = 3001;
    private static final long SRC_P2_ID = 3002;
    private static final long SRC_P1_VERSION = 10;
    private static final long SRC_P2_VERSION = 20;
    private static final long SRC_COMMIT_SEQ = 500;
    private static final long BACKUP_TIME = 1700000000000L;
    private static final long RESTORE_TIME = 1800000000000L;
    private static final RestoreLineage STALE = new RestoreLineage(7, 7, 7, 7, 7, 7, 7);
    private static final int REUSE_ESTIMATE_COLUMN = ShowRestoreCommand.TITLE_NAMES.indexOf("ReuseEstimate");

    private final Env env = Mockito.mock(Env.class);
    private final InternalCatalog catalog = Mockito.mock(InternalCatalog.class);
    private final EditLog editLog = Mockito.mock(EditLog.class);
    private MockedStatic<Env> mockedEnvStatic;

    private Database db;
    private OlapTable tbl2;
    private BackupJobInfo jobInfo;
    private RestoreJob job;

    @BeforeEach
    public void setUp() throws Exception {
        db = CatalogMocker.mockDb();
        tbl2 = (OlapTable) db.getTableNullable(CatalogMocker.TEST_TBL2_ID);
        // The mocked table is an aggregate table, which partition level reuse does not cover.
        Deencapsulation.setField(tbl2, "keysType", KeysType.DUP_KEYS);

        mockedEnvStatic = Mockito.mockStatic(Env.class);
        mockedEnvStatic.when(Env::getCurrentEnvJournalVersion).thenReturn(FeConstants.meta_version);
        mockedEnvStatic.when(Env::getCurrentEnv).thenReturn(env);
        Mockito.when(env.getInternalCatalog()).thenReturn(catalog);
        Mockito.when(env.getEditLog()).thenReturn(editLog);
        Mockito.when(catalog.getDbNullable(Mockito.anyLong())).thenReturn(db);
        Mockito.when(catalog.getDbOrMetaException(Mockito.anyLong())).thenReturn(db);
        CatalogMgr catalogMgr = Mockito.mock(CatalogMgr.class);
        Mockito.when(env.getCatalogMgr()).thenReturn(catalogMgr);
        Mockito.when(catalogMgr.getCatalog(Mockito.anyString())).thenReturn(catalog);
        AtomicLong nextId = new AtomicLong(90000);
        Mockito.when(env.getNextId()).thenAnswer(inv -> nextId.getAndIncrement());

        // the backup of test_tbl2 (p1, p2), taken in the source cluster.
        jobInfo = new BackupJobInfo();
        jobInfo.name = "snapshot_1";
        jobInfo.backupTime = BACKUP_TIME;
        jobInfo.dbId = SRC_DB_ID;
        jobInfo.dbName = "src_db";
        jobInfo.success = true;
        jobInfo.tableCommitSeqMap = Maps.newHashMap();
        jobInfo.tableCommitSeqMap.put(SRC_TBL_ID, SRC_COMMIT_SEQ);
        BackupOlapTableInfo tblInfo = new BackupOlapTableInfo();
        tblInfo.id = SRC_TBL_ID;
        tblInfo.partitions.put(CatalogMocker.TEST_PARTITION1_NAME, partInfo(SRC_P1_ID, SRC_P1_VERSION));
        tblInfo.partitions.put(CatalogMocker.TEST_PARTITION2_NAME, partInfo(SRC_P2_ID, SRC_P2_VERSION));
        jobInfo.backupOlapTableObjects.put(CatalogMocker.TEST_TBL2_NAME, tblInfo);

        job = newJob(jobInfo, null, false);
    }

    @AfterEach
    public void tearDown() {
        if (mockedEnvStatic != null) {
            mockedEnvStatic.close();
        }
    }

    private static BackupPartitionInfo partInfo(long id, long version) {
        BackupPartitionInfo partInfo = new BackupPartitionInfo();
        partInfo.id = id;
        partInfo.version = version;
        return partInfo;
    }

    private RestoreJob newJob(BackupJobInfo info, BackupMeta backupMeta, boolean isAtomicRestore) {
        return new RestoreJob("restore_label", "2024-01-01 00:00:00", db.getId(), db.getFullName(), info, false,
                new ReplicaAllocation((short) 3), 100000, -1, false, false, false, false, false, false,
                isAtomicRestore, false, env, 20000, backupMeta);
    }

    private static RestoreLineage expectedLineage(long srcPartId, long srcVersion, long restoreTime) {
        return new RestoreLineage(SRC_DB_ID, SRC_TBL_ID, srcPartId, srcVersion, SRC_COMMIT_SEQ, BACKUP_TIME,
                restoreTime);
    }

    private static RestoreLineage expectedLineage(long srcPartId, long srcVersion) {
        return expectedLineage(srcPartId, srcVersion, RESTORE_TIME);
    }

    // The backup meta of test_tbl2: a deep copy of the source table, carrying the source's lineage.
    private BackupMeta backupMetaWithStaleLineage() {
        OlapTable remoteTbl = tbl2.selectiveCopy(null, IndexExtState.VISIBLE, true);
        for (Partition part : remoteTbl.getPartitions()) {
            part.setRestoreLineage(STALE);
        }
        List<Table> tbls = Lists.newArrayList(remoteTbl);
        List<Resource> resources = Lists.newArrayList();
        return new BackupMeta(tbls, resources);
    }

    private OlapTable currentTbl2() {
        return (OlapTable) db.getTableNullable(CatalogMocker.TEST_TBL2_NAME);
    }

    private Partition p1() {
        return tbl2.getPartition(CatalogMocker.TEST_PARTITION1_NAME);
    }

    private Partition p2() {
        return tbl2.getPartition(CatalogMocker.TEST_PARTITION2_NAME);
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
    // Persistence of the lineage
    // ---------------------------------------------------------------------------------------------

    @Test
    public void testLineageSerializedFormat() {
        // The serialized names are persisted in the image, they must never change.
        RestoreLineage lineage = new RestoreLineage(1, 2, 3, 4, 5, 6, 7);
        String json = GsonUtils.GSON.toJson(lineage);
        Assertions.assertEquals("{\"db\":1,\"tbl\":2,\"part\":3,\"ver\":4,\"seq\":5,\"bt\":6,\"rt\":7}", json);
        Assertions.assertEquals(lineage, GsonUtils.GSON.fromJson(json, RestoreLineage.class));
        // the cluster id of the source is written only if known
        RestoreLineage withCluster = new RestoreLineage(1, 2, 3, 4, 5, 6, 7, 8);
        String clusterJson = GsonUtils.GSON.toJson(withCluster);
        Assertions.assertEquals("{\"db\":1,\"tbl\":2,\"part\":3,\"ver\":4,\"seq\":5,\"bt\":6,\"rt\":7,"
                + "\"cid\":8}", clusterJson);
        Assertions.assertEquals(withCluster, GsonUtils.GSON.fromJson(clusterJson, RestoreLineage.class));
        Assertions.assertNotEquals(lineage, withCluster);
    }

    @Test
    public void testPartitionGsonRoundTrip() {
        Partition part = p1();
        RestoreLineage lineage = expectedLineage(SRC_P1_ID, SRC_P1_VERSION);
        part.setRestoreLineage(lineage);

        String json = GsonUtils.GSON.toJson(part, Partition.class);
        JsonObject obj = JsonParser.parseString(json).getAsJsonObject();
        Assertions.assertTrue(obj.has("rl"));

        Partition readPart = GsonUtils.GSON.fromJson(json, Partition.class);
        Assertions.assertEquals(lineage, readPart.getRestoreLineage());
        Assertions.assertEquals(part.getId(), readPart.getId());
        Assertions.assertEquals(part.getVisibleVersion(), readPart.getVisibleVersion());
    }

    @Test
    public void testPartitionGsonWithoutLineage() {
        // A partition that was never restored does not write the field, so the format is unchanged.
        Partition part = p1();
        Assertions.assertNull(part.getRestoreLineage());
        String json = GsonUtils.GSON.toJson(part, Partition.class);
        Assertions.assertFalse(JsonParser.parseString(json).getAsJsonObject().has("rl"));
        Assertions.assertNull(GsonUtils.GSON.fromJson(json, Partition.class).getRestoreLineage());

        // An image written by an old FE has no such field, the lineage is read as null.
        part.setRestoreLineage(expectedLineage(SRC_P1_ID, SRC_P1_VERSION));
        JsonObject obj = JsonParser.parseString(GsonUtils.GSON.toJson(part, Partition.class)).getAsJsonObject();
        obj.remove("rl");
        Partition oldPart = GsonUtils.GSON.fromJson(obj.toString(), Partition.class);
        Assertions.assertNull(oldPart.getRestoreLineage());
        Assertions.assertEquals(part.getId(), oldPart.getId());
    }

    @Test
    public void testPartitionGsonIgnoresUnknownFields() {
        // An old FE reading an image of a new FE ignores the fields it does not know, which is what the
        // lineage field relies on. Simulate it with fields unknown to this FE, both at the partition level
        // and inside the lineage.
        Partition part = p1();
        part.setRestoreLineage(expectedLineage(SRC_P1_ID, SRC_P1_VERSION));
        JsonObject obj = JsonParser.parseString(GsonUtils.GSON.toJson(part, Partition.class)).getAsJsonObject();
        JsonObject unknown = new JsonObject();
        unknown.addProperty("x", 1);
        obj.add("unknown_field", unknown);
        obj.getAsJsonObject("rl").addProperty("unknown_sub_field", "abc");

        Partition readPart = GsonUtils.GSON.fromJson(obj.toString(), Partition.class);
        Assertions.assertEquals(expectedLineage(SRC_P1_ID, SRC_P1_VERSION), readPart.getRestoreLineage());
    }

    // ---------------------------------------------------------------------------------------------
    // Writing the lineage
    // ---------------------------------------------------------------------------------------------

    @Test
    public void testBuildRestoreLineage() {
        BackupOlapTableInfo tblInfo = jobInfo.getOlapTableInfo(CatalogMocker.TEST_TBL2_NAME);
        BackupPartitionInfo partInfo = tblInfo.getPartInfo(CatalogMocker.TEST_PARTITION1_NAME);
        Assertions.assertEquals(expectedLineage(SRC_P1_ID, SRC_P1_VERSION),
                RestoreJob.buildRestoreLineage(jobInfo, tblInfo, partInfo, RESTORE_TIME));

        // The commit seq of the table is missing.
        jobInfo.tableCommitSeqMap.clear();
        Assertions.assertEquals(RestoreLineage.UNKNOWN_COMMIT_SEQ,
                RestoreJob.buildRestoreLineage(jobInfo, tblInfo, partInfo, RESTORE_TIME).getSrcCommitSeq());
        // A backup of an old version has no commit seq map.
        jobInfo.tableCommitSeqMap = null;
        Assertions.assertEquals(RestoreLineage.UNKNOWN_COMMIT_SEQ,
                RestoreJob.buildRestoreLineage(jobInfo, tblInfo, partInfo, RESTORE_TIME).getSrcCommitSeq());
    }

    @Test
    public void testStampRewritesInheritedLineage() {
        // p1 carries the lineage of another restore (e.g. the upstream's own lineage), p2 has none.
        RestoreLineage stale = STALE;
        p1().setRestoreLineage(stale);
        // test_tbl is not restored by this job, its lineage must be kept.
        Partition untouched = ((OlapTable) db.getTableNullable(CatalogMocker.TEST_TBL_ID))
                .getPartition(CatalogMocker.TEST_SINGLE_PARTITION_NAME);
        untouched.setRestoreLineage(stale);

        job.stampRestoreLineage(db, RESTORE_TIME);

        Assertions.assertEquals(expectedLineage(SRC_P1_ID, SRC_P1_VERSION), p1().getRestoreLineage());
        Assertions.assertEquals(expectedLineage(SRC_P2_ID, SRC_P2_VERSION), p2().getRestoreLineage());
        Assertions.assertEquals(stale, untouched.getRestoreLineage());
    }

    @Test
    public void testClearLineageInBackupMeta() {
        BackupMeta backupMeta = backupMetaWithStaleLineage();
        OlapTable remoteTbl = (OlapTable) backupMeta.getTable(CatalogMocker.TEST_TBL2_NAME);
        RestoreJob restoreJob = newJob(jobInfo, backupMeta, false);

        restoreJob.clearRestoreLineageInBackupMeta();

        for (Partition part : remoteTbl.getPartitions()) {
            Assertions.assertNull(part.getRestoreLineage());
        }
    }

    @Test
    public void testStampExistingPartitionOnCommitAndReplay() throws Exception {
        // Path 1: p1 exists locally and is overwritten (non-atomic restore), the restore job writes back its
        // version. It carries the lineage of an earlier restore.
        p1().setRestoreLineage(STALE);
        com.google.common.collect.Table<Long, Long, Long> restoredVersionInfo =
                Deencapsulation.getField(job, "restoredVersionInfo");
        restoredVersionInfo.put(tbl2.getId(), p1().getId(), SRC_P1_VERSION);
        restoredVersionInfo.put(tbl2.getId(), p2().getId(), SRC_P2_VERSION);

        Status st = job.allTabletCommitted(false /* not replay */);
        Assertions.assertTrue(st.ok(), st.toString());
        long restoreTime = job.getFinishedTime();
        Assertions.assertTrue(restoreTime > 0);
        Assertions.assertEquals(SRC_P1_VERSION, p1().getVisibleVersion());
        Assertions.assertEquals(expectedLineage(SRC_P1_ID, SRC_P1_VERSION, restoreTime), p1().getRestoreLineage());
        Assertions.assertEquals(expectedLineage(SRC_P2_ID, SRC_P2_VERSION, restoreTime), p2().getRestoreLineage());
        Mockito.verify(editLog).logRestoreJob(job);

        // The follower replays the FINISHED job from the edit log written above, and writes the same lineage.
        RestoreJob replayed = writeAndRead(job);
        replayed.setEnv(env);
        p1().setRestoreLineage(STALE);
        p2().setRestoreLineage(null);
        replayed.replayRun();
        Assertions.assertEquals(expectedLineage(SRC_P1_ID, SRC_P1_VERSION, restoreTime), p1().getRestoreLineage());
        Assertions.assertEquals(expectedLineage(SRC_P2_ID, SRC_P2_VERSION, restoreTime), p2().getRestoreLineage());
    }

    @Test
    public void testStampNewPartitionOnCommit() throws Exception {
        // Path 2: p2 does not exist locally, it is created from the backup meta (resetPartitionForRestore).
        BackupMeta backupMeta = backupMetaWithStaleLineage();
        OlapTable remoteTbl = (OlapTable) backupMeta.getTable(CatalogMocker.TEST_TBL2_NAME);
        RestoreJob restoreJob = newJob(jobInfo, backupMeta, false);
        // p2 does not exist locally.
        Partition localP2 = p2();
        Map<Long, Partition> idToPartition = Deencapsulation.getField(tbl2, "idToPartition");
        Map<String, Partition> nameToPartition = Deencapsulation.getField(tbl2, "nameToPartition");
        idToPartition.remove(localP2.getId());
        nameToPartition.remove(localP2.getName());
        Assertions.assertNull(p2());
        SystemInfoService systemInfoService = Mockito.mock(SystemInfoService.class);
        mockedEnvStatic.when(Env::getCurrentSystemInfo).thenReturn(systemInfoService);
        Map<Tag, List<Long>> beIds = Maps.newHashMap();
        beIds.put(Tag.DEFAULT_BACKEND_TAG, Lists.newArrayList(CatalogMocker.BACKEND1_ID,
                CatalogMocker.BACKEND2_ID, CatalogMocker.BACKEND3_ID));
        Mockito.when(systemInfoService.selectBackendIdsForReplicaCreation(Mockito.any(), Mockito.any(),
                Mockito.any(), Mockito.anyBoolean(), Mockito.anyBoolean()))
                .thenReturn(Pair.of(beIds, TStorageMedium.HDD));

        restoreJob.clearRestoreLineageInBackupMeta();
        Partition restorePart = restoreJob.resetPartitionForRestore(tbl2, remoteTbl,
                CatalogMocker.TEST_PARTITION2_NAME, new ReplicaAllocation((short) 3));
        Assertions.assertNull(restorePart.getRestoreLineage());
        tbl2.addPartition(restorePart);

        Status st = restoreJob.allTabletCommitted(false /* not replay */);
        Assertions.assertTrue(st.ok(), st.toString());
        Assertions.assertSame(restorePart, p2());
        Assertions.assertEquals(expectedLineage(SRC_P2_ID, SRC_P2_VERSION, restoreJob.getFinishedTime()),
                p2().getRestoreLineage());
    }

    @Test
    public void testStampNewTableOnCommit() throws Exception {
        // Path 3: the table does not exist locally, it is created from the backup meta (resetIdsForRestore).
        BackupMeta backupMeta = backupMetaWithStaleLineage();
        OlapTable remoteTbl = (OlapTable) backupMeta.getTable(CatalogMocker.TEST_TBL2_NAME);
        RestoreJob restoreJob = newJob(jobInfo, backupMeta, false);
        restoreJob.clearRestoreLineageInBackupMeta();
        db.unregisterTable(CatalogMocker.TEST_TBL2_NAME);
        remoteTbl.setId(tbl2.getId() + 100);
        db.registerTable(remoteTbl);

        Status st = restoreJob.allTabletCommitted(false /* not replay */);
        Assertions.assertTrue(st.ok(), st.toString());
        OlapTable restored = currentTbl2();
        Assertions.assertSame(remoteTbl, restored);
        long restoreTime = restoreJob.getFinishedTime();
        Assertions.assertEquals(expectedLineage(SRC_P1_ID, SRC_P1_VERSION, restoreTime),
                restored.getPartition(CatalogMocker.TEST_PARTITION1_NAME).getRestoreLineage());
        Assertions.assertEquals(expectedLineage(SRC_P2_ID, SRC_P2_VERSION, restoreTime),
                restored.getPartition(CatalogMocker.TEST_PARTITION2_NAME).getRestoreLineage());
    }

    @Test
    public void testStampAtomicRestoreOnCommit() throws Exception {
        // Path 4: atomic restore, the table is created from the backup meta under a temp name, and replaces
        // the local table at commit.
        BackupMeta backupMeta = backupMetaWithStaleLineage();
        OlapTable remoteTbl = (OlapTable) backupMeta.getTable(CatalogMocker.TEST_TBL2_NAME);
        RestoreJob restoreJob = newJob(jobInfo, backupMeta, true);
        restoreJob.clearRestoreLineageInBackupMeta();
        p1().setRestoreLineage(STALE);
        remoteTbl.setId(tbl2.getId() + 100);
        remoteTbl.setName(RestoreJob.tableAliasWithAtomicRestore(CatalogMocker.TEST_TBL2_NAME));
        db.registerTable(remoteTbl);

        Status st = restoreJob.allTabletCommitted(false /* not replay */);
        Assertions.assertTrue(st.ok(), st.toString());
        OlapTable restored = currentTbl2();
        Assertions.assertSame(remoteTbl, restored);
        long restoreTime = restoreJob.getFinishedTime();
        Assertions.assertEquals(expectedLineage(SRC_P1_ID, SRC_P1_VERSION, restoreTime),
                restored.getPartition(CatalogMocker.TEST_PARTITION1_NAME).getRestoreLineage());
        Assertions.assertEquals(expectedLineage(SRC_P2_ID, SRC_P2_VERSION, restoreTime),
                restored.getPartition(CatalogMocker.TEST_PARTITION2_NAME).getRestoreLineage());
    }

    // ---------------------------------------------------------------------------------------------
    // Shadow statistics
    // ---------------------------------------------------------------------------------------------

    @Test
    public void testCheckL0() {
        Partition part = p1();
        part.updateVersionForRestore(SRC_P1_VERSION);

        Assertions.assertEquals(L0Verdict.NO_LOCAL_PARTITION, RestoreReuseShadowStats.checkL0(null,
                SRC_DB_ID, SRC_TBL_ID, SRC_P1_ID, SRC_P1_VERSION, SRC_COMMIT_SEQ));
        Assertions.assertEquals(L0Verdict.NO_LINEAGE, RestoreReuseShadowStats.checkL0(part,
                SRC_DB_ID, SRC_TBL_ID, SRC_P1_ID, SRC_P1_VERSION, SRC_COMMIT_SEQ));

        part.setRestoreLineage(expectedLineage(SRC_P1_ID, SRC_P1_VERSION));
        // 1. same source partition.
        Assertions.assertEquals(L0Verdict.REUSABLE, RestoreReuseShadowStats.checkL0(part,
                SRC_DB_ID, SRC_TBL_ID, SRC_P1_ID, SRC_P1_VERSION, SRC_COMMIT_SEQ));
        Assertions.assertEquals(L0Verdict.LINEAGE_MISMATCH, RestoreReuseShadowStats.checkL0(part,
                SRC_DB_ID + 1, SRC_TBL_ID, SRC_P1_ID, SRC_P1_VERSION, SRC_COMMIT_SEQ));
        Assertions.assertEquals(L0Verdict.LINEAGE_MISMATCH, RestoreReuseShadowStats.checkL0(part,
                SRC_DB_ID, SRC_TBL_ID + 1, SRC_P1_ID, SRC_P1_VERSION, SRC_COMMIT_SEQ));
        Assertions.assertEquals(L0Verdict.LINEAGE_MISMATCH, RestoreReuseShadowStats.checkL0(part,
                SRC_DB_ID, SRC_TBL_ID, SRC_P2_ID, SRC_P1_VERSION, SRC_COMMIT_SEQ));
        // 3. the source partition was written after the last backup.
        Assertions.assertEquals(L0Verdict.SOURCE_VERSION_CHANGED, RestoreReuseShadowStats.checkL0(part,
                SRC_DB_ID, SRC_TBL_ID, SRC_P1_ID, SRC_P1_VERSION + 1, SRC_COMMIT_SEQ));
        // 4. commit seq.
        Assertions.assertEquals(L0Verdict.REUSABLE, RestoreReuseShadowStats.checkL0(part,
                SRC_DB_ID, SRC_TBL_ID, SRC_P1_ID, SRC_P1_VERSION, SRC_COMMIT_SEQ + 100));
        Assertions.assertEquals(L0Verdict.COMMIT_SEQ_MISMATCH, RestoreReuseShadowStats.checkL0(part,
                SRC_DB_ID, SRC_TBL_ID, SRC_P1_ID, SRC_P1_VERSION, SRC_COMMIT_SEQ - 1));
        Assertions.assertEquals(L0Verdict.COMMIT_SEQ_MISMATCH, RestoreReuseShadowStats.checkL0(part,
                SRC_DB_ID, SRC_TBL_ID, SRC_P1_ID, SRC_P1_VERSION, RestoreLineage.UNKNOWN_COMMIT_SEQ));
        part.setRestoreLineage(new RestoreLineage(SRC_DB_ID, SRC_TBL_ID, SRC_P1_ID, SRC_P1_VERSION,
                RestoreLineage.UNKNOWN_COMMIT_SEQ, BACKUP_TIME, RESTORE_TIME));
        Assertions.assertEquals(L0Verdict.COMMIT_SEQ_MISMATCH, RestoreReuseShadowStats.checkL0(part,
                SRC_DB_ID, SRC_TBL_ID, SRC_P1_ID, SRC_P1_VERSION, SRC_COMMIT_SEQ));

        // 2. the local partition was written after the restore.
        part.setRestoreLineage(expectedLineage(SRC_P1_ID, SRC_P1_VERSION));
        part.updateVisibleVersion(SRC_P1_VERSION + 1);
        Assertions.assertEquals(L0Verdict.LOCAL_VERSION_CHANGED, RestoreReuseShadowStats.checkL0(part,
                SRC_DB_ID, SRC_TBL_ID, SRC_P1_ID, SRC_P1_VERSION, SRC_COMMIT_SEQ));
    }

    @Test
    public void testComputeReuseShadowStats() throws Exception {
        // p1 was restored from the same source partition and nothing changed since then.
        p1().updateVersionForRestore(SRC_P1_VERSION);
        p1().setRestoreLineage(expectedLineage(SRC_P1_ID, SRC_P1_VERSION));
        long bytes = setReplicaDataSizes(p1());
        Assertions.assertTrue(bytes > 0);
        // p2 was restored from the same source, but has been loaded since then.
        p2().updateVersionForRestore(SRC_P2_VERSION);
        p2().setRestoreLineage(expectedLineage(SRC_P2_ID, SRC_P2_VERSION));
        p2().updateVisibleVersion(SRC_P2_VERSION + 1);
        // A table that does not exist locally.
        BackupOlapTableInfo absentTbl = new BackupOlapTableInfo();
        absentTbl.id = SRC_TBL_ID + 1;
        absentTbl.partitions.put("a1", partInfo(4001, 2));
        absentTbl.partitions.put("a2", partInfo(4002, 2));
        jobInfo.backupOlapTableObjects.put("absent_tbl", absentTbl);

        long p1Version = p1().getVisibleVersion();
        job.computeReuseShadowStats(db);

        RestoreReuseShadowStats stats = job.getReuseShadowStats();
        Assertions.assertNotNull(stats);
        Assertions.assertEquals(4, stats.getPartitions());
        Assertions.assertEquals(1, stats.getReusable());
        Assertions.assertEquals(bytes, stats.getReusableBytesSingleReplica());
        Assertions.assertEquals(1, stats.getLocalVersionChanged());
        Assertions.assertEquals(2, stats.getNoLocalPartition());
        Assertions.assertEquals(0, stats.getNoLineage() + stats.getLineageMismatch()
                + stats.getSourceVersionChanged() + stats.getCommitSeqMismatch()
                + stats.getL0PassedButAtomicRestore() + stats.getL0PassedButAggregateTable()
                + stats.getL0PassedButRemoteStorage());
        // Only observes, never modifies.
        Assertions.assertEquals(p1Version, p1().getVisibleVersion());
        Assertions.assertEquals(expectedLineage(SRC_P1_ID, SRC_P1_VERSION), p1().getRestoreLineage());

        // Shown in the ReuseEstimate column of SHOW RESTORE (after Timeout), and not in SHOW BRIEF RESTORE.
        List<String> fullInfo = job.getFullInfo();
        Assertions.assertEquals(ShowRestoreCommand.TITLE_NAMES.size(), fullInfo.size());
        Assertions.assertEquals("Timeout", ShowRestoreCommand.TITLE_NAMES.get(REUSE_ESTIMATE_COLUMN - 1));
        String shown = fullInfo.get(REUSE_ESTIMATE_COLUMN);
        Assertions.assertEquals("{\"partitions\":4,\"reusable\":1,\"reusable_bytes_single_replica\":" + bytes
                + ",\"reusable_a\":1,\"reusable_b\":0,\"reusable_c\":0,"
                + "\"l0_passed_but_atomic_restore\":0,\"l0_passed_but_aggregate_table\":0,"
                + "\"l0_passed_but_remote_storage\":0,\"l0_passed_but_hidden_state_mow\":0,"
                + "\"l0_passed_but_hidden_state_mor\":0,\"cluster_mismatch\":0,\"no_local\":2,\"no_lineage\":0,"
                + "\"lineage_mismatch\":0,"
                + "\"local_version_changed\":1,\"source_version_changed\":0,\"commit_seq_mismatch\":0}", shown);
        Assertions.assertEquals(ShowRestoreCommand.BRIEF_TITLE_NAMES.size(), job.getBriefInfo().size());

        // Persisted with the job, so that it can be shown on followers and after restart.
        RestoreJob readJob = writeAndRead(job);
        Assertions.assertEquals(shown, readJob.getFullInfo().get(REUSE_ESTIMATE_COLUMN));
    }

    @Test
    public void testComputeReuseShadowStatsNeverFailsTheJob() {
        // Not computed yet.
        List<String> fullInfo = job.getFullInfo();
        Assertions.assertEquals(FeConstants.null_string, fullInfo.get(REUSE_ESTIMATE_COLUMN));

        // A broken job info makes the computation fail, the error is swallowed.
        jobInfo.backupOlapTableObjects.put("broken_tbl", null);
        Assertions.assertDoesNotThrow(() -> job.computeReuseShadowStats(db));
        Assertions.assertNull(job.getReuseShadowStats());
        Assertions.assertTrue(job.getStatus().ok());
    }

    // Set different local data sizes to the replicas of each tablet, returns the expected single replica size.
    private static long setReplicaDataSizes(Partition part) {
        long expected = 0;
        for (MaterializedIndex index : part.getMaterializedIndices(IndexExtState.VISIBLE)) {
            for (Tablet tablet : index.getTablets()) {
                long size = 100;
                for (Replica replica : tablet.getReplicas()) {
                    replica.setDataSize(size);
                    size += 10;
                }
                expected += size - 10;
            }
        }
        return expected;
    }

    @Test
    public void testSingleReplicaLocalDataSize() {
        Partition part = p1();
        long expected = setReplicaDataSizes(part);
        Assertions.assertTrue(expected > 0);
        // The largest NORMAL replica of each tablet, not the sum or the average of all replicas.
        Assertions.assertEquals(expected, RestoreReuseShadowStats.getSingleReplicaLocalDataSize(part));
        Assertions.assertTrue(expected < part.getDataSize(false /* all replicas */));

        // A replica that is not NORMAL is ignored.
        Tablet tablet = part.getBaseIndex().getTablets().get(0);
        Replica largest = tablet.getReplicas().get(tablet.getReplicas().size() - 1);
        largest.setDataSize(1000000);
        largest.setState(ReplicaState.DECOMMISSION);
        Assertions.assertEquals(expected - 10, RestoreReuseShadowStats.getSingleReplicaLocalDataSize(part));
    }

    private void makeBothPartitionsPassL0() {
        p1().updateVersionForRestore(SRC_P1_VERSION);
        p1().setRestoreLineage(expectedLineage(SRC_P1_ID, SRC_P1_VERSION));
        p2().updateVersionForRestore(SRC_P2_VERSION);
        p2().setRestoreLineage(expectedLineage(SRC_P2_ID, SRC_P2_VERSION));
    }

    @Test
    public void testShadowStatsExcludeUnsupportedCases() {
        makeBothPartitionsPassL0();
        setReplicaDataSizes(p1());
        long p2Bytes = setReplicaDataSizes(p2());

        // Atomic restore.
        RestoreJob atomicJob = newJob(jobInfo, null, true);
        atomicJob.computeReuseShadowStats(db);
        RestoreReuseShadowStats stats = atomicJob.getReuseShadowStats();
        Assertions.assertEquals(2, stats.getPartitions());
        Assertions.assertEquals(2, stats.getL0PassedButAtomicRestore());
        Assertions.assertEquals(0, stats.getReusable());
        Assertions.assertEquals(0, stats.getReusableBytesSingleReplica());

        // Aggregate table.
        Deencapsulation.setField(tbl2, "keysType", KeysType.AGG_KEYS);
        job.computeReuseShadowStats(db);
        stats = job.getReuseShadowStats();
        Assertions.assertEquals(2, stats.getL0PassedButAggregateTable());
        Assertions.assertEquals(0, stats.getReusable());
        Assertions.assertEquals(0, stats.getReusableBytesSingleReplica());

        // Part of p1 is cooled down to remote storage.
        Deencapsulation.setField(tbl2, "keysType", KeysType.DUP_KEYS);
        Tablet tablet = p1().getBaseIndex().getTablets().get(0);
        Replica replica = tablet.getReplicas().get(0);
        tablet.setCooldownConf(replica.getId(), 1);
        replica.setRemoteDataSize(100);
        Assertions.assertTrue(p1().getRemoteDataSize() > 0);
        job.computeReuseShadowStats(db);
        stats = job.getReuseShadowStats();
        Assertions.assertEquals(1, stats.getL0PassedButRemoteStorage());
        Assertions.assertEquals(1, stats.getReusable());
        Assertions.assertEquals(p2Bytes, stats.getReusableBytesSingleReplica());
        Assertions.assertEquals(2, stats.getPartitions());
    }

    @Test
    public void testShadowStatsOnlyExcludeL0Passed() {
        // The unsupported cases are only counted for partitions that pass L0: a partition that fails L0 is
        // counted by the failed condition even in an atomic restore of an aggregate table.
        Deencapsulation.setField(tbl2, "keysType", KeysType.AGG_KEYS);
        RestoreJob atomicJob = newJob(jobInfo, null, true);
        atomicJob.computeReuseShadowStats(db);
        RestoreReuseShadowStats stats = atomicJob.getReuseShadowStats();
        Assertions.assertEquals(2, stats.getNoLineage());
        Assertions.assertEquals(0, stats.getL0PassedButAtomicRestore() + stats.getL0PassedButAggregateTable());
    }

    // ---------------------------------------------------------------------------------------------
    // Invalidating the lineage of the partitions overwritten in place
    // ---------------------------------------------------------------------------------------------

    private static final long V1 = 10;
    private static final long V2 = 20;

    // A backup of test_tbl2 taken in this cluster, with all partitions at the given version. The ids match
    // the backup meta made by backupMetaWithStaleLineage(), so the job can go through checkAndPrepareMeta.
    private BackupJobInfo selfBackupJobInfo(long version) {
        BackupJobInfo info = new BackupJobInfo();
        info.name = "snapshot_v" + version;
        info.backupTime = BACKUP_TIME + version;
        info.dbId = db.getId();
        info.dbName = db.getFullName();
        info.success = true;
        info.tableCommitSeqMap = Maps.newHashMap();
        info.tableCommitSeqMap.put(tbl2.getId(), SRC_COMMIT_SEQ + version);
        BackupOlapTableInfo tblInfo = new BackupOlapTableInfo();
        tblInfo.id = tbl2.getId();
        for (Partition part : tbl2.getPartitions()) {
            BackupPartitionInfo partInfo = partInfo(part.getId(), version);
            for (MaterializedIndex index : part.getMaterializedIndices(IndexExtState.VISIBLE)) {
                BackupIndexInfo idxInfo = new BackupIndexInfo();
                idxInfo.id = index.getId();
                idxInfo.schemaHash = tbl2.getSchemaHashByIndexId(index.getId());
                partInfo.indexes.put(tbl2.getIndexNameById(index.getId()), idxInfo);
                for (Tablet tablet : index.getTablets()) {
                    idxInfo.sortedTabletInfoList.add(new BackupTabletInfo(tablet.getId(), Lists.newArrayList()));
                }
            }
            tblInfo.partitions.put(part.getName(), partInfo);
        }
        info.backupOlapTableObjects.put(tbl2.getName(), tblInfo);
        return info;
    }

    private RestoreLineage selfLineage(BackupJobInfo info, Partition part, long restoreTime) {
        BackupOlapTableInfo tblInfo = info.getOlapTableInfo(tbl2.getName());
        return RestoreJob.buildRestoreLineage(info, tblInfo, tblInfo.getPartInfo(part.getName()), restoreTime);
    }

    // p1 and p2 were restored from the backup at V1 and not written since then.
    private BackupJobInfo restoredFromV1() {
        BackupJobInfo infoV1 = selfBackupJobInfo(V1);
        for (Partition part : Lists.newArrayList(p1(), p2())) {
            part.updateVersionForRestore(V1);
            part.setRestoreLineage(selfLineage(infoV1, part, RESTORE_TIME));
        }
        return infoV1;
    }

    // A non-atomic restore of the backup at V2 onto the existing test_tbl2, after checkAndPrepareMeta.
    private RestoreJob prepareOverwriteJob(BackupJobInfo info) {
        // CatalogMocker does not set the range of p2, the restore job compares it with the backup meta.
        PartitionInfo partitionInfo = tbl2.getPartitionInfo();
        if (partitionInfo.getItem(CatalogMocker.TEST_PARTITION2_ID) == null) {
            partitionInfo.setItem(CatalogMocker.TEST_PARTITION2_ID, false,
                    partitionInfo.getItem(CatalogMocker.TEST_PARTITION1_ID));
        }
        RestoreJob restoreJob = new RestoreJob("restore_label", "2024-01-01 00:00:00", db.getId(), db.getFullName(),
                info, false, new ReplicaAllocation((short) 3), 100000, -1, false, false, false, false, false, false,
                false, false, env, Repository.KEEP_ON_LOCAL_REPO_ID, backupMetaWithStaleLineage());
        Deencapsulation.invoke(restoreJob, "checkAndPrepareMeta");
        Assertions.assertTrue(restoreJob.getStatus().ok(), restoreJob.getStatus().toString());
        Assertions.assertEquals(RestoreJobState.CREATING, restoreJob.getState());
        com.google.common.collect.Table<Long, Long, Long> restoredVersionInfo =
                Deencapsulation.getField(restoreJob, "restoredVersionInfo");
        Assertions.assertEquals(2, restoredVersionInfo.size());
        return restoreJob;
    }

    @Test
    public void testPrepareInvalidatesLineageOfOverwrittenPartitions() {
        restoredFromV1();
        RestoreJob jobV2 = prepareOverwriteJob(selfBackupJobInfo(V2));

        // The shadow statistics were computed with the old lineage, before it was cleared.
        RestoreReuseShadowStats stats = jobV2.getReuseShadowStats();
        Assertions.assertEquals(2, stats.getSourceVersionChanged());
        // The lineage of the partitions to overwrite is cleared, the data (version) is not changed yet.
        Assertions.assertNull(p1().getRestoreLineage());
        Assertions.assertNull(p2().getRestoreLineage());
        Assertions.assertEquals(V1, p1().getVisibleVersion());
    }

    @Test
    public void testCancelInCommittingLeavesNoLineage() {
        BackupJobInfo infoV1 = restoredFromV1();
        RestoreJob jobV2 = prepareOverwriteJob(selfBackupJobInfo(V2));

        // Cancelled after some tablets may have been moved: the version is still V1, but the data may not be.
        Deencapsulation.setField(jobV2, "state", RestoreJobState.COMMITTING);
        Assertions.assertTrue(jobV2.cancel().ok());
        Assertions.assertEquals(RestoreJobState.CANCELLED, jobV2.getState());
        Assertions.assertEquals(V1, p1().getVisibleVersion());
        Assertions.assertNull(p1().getRestoreLineage());
        Assertions.assertNull(p2().getRestoreLineage());

        // Restoring the backup at V1 again must not reuse them.
        RestoreJob jobV1 = newJob(infoV1, null, false);
        jobV1.computeReuseShadowStats(db);
        Assertions.assertEquals(2, jobV1.getReuseShadowStats().getNoLineage());
        Assertions.assertEquals(0, jobV1.getReuseShadowStats().getReusable());
    }

    @Test
    public void testCommitAfterPrepareWritesLineageAgain() {
        restoredFromV1();
        BackupJobInfo infoV2 = selfBackupJobInfo(V2);
        RestoreJob jobV2 = prepareOverwriteJob(infoV2);

        Status st = jobV2.allTabletCommitted(false /* not replay */);
        Assertions.assertTrue(st.ok(), st.toString());
        for (Partition part : Lists.newArrayList(p1(), p2())) {
            Assertions.assertEquals(V2, part.getVisibleVersion());
            Assertions.assertEquals(selfLineage(infoV2, part, jobV2.getFinishedTime()), part.getRestoreLineage());
        }
    }

    @Test
    public void testReplayDownloadInvalidatesLineage() throws Exception {
        BackupJobInfo infoV1 = restoredFromV1();
        RestoreJob jobV2 = prepareOverwriteJob(selfBackupJobInfo(V2));
        // The DOWNLOAD edit log written by the master after the snapshots are made.
        Deencapsulation.setField(jobV2, "state", RestoreJobState.DOWNLOAD);
        RestoreJob replayed = writeAndRead(jobV2);

        // The follower still has the old lineage.
        for (Partition part : Lists.newArrayList(p1(), p2())) {
            part.setRestoreLineage(selfLineage(infoV1, part, RESTORE_TIME));
        }
        replayed.setEnv(env);
        replayed.replayRun();
        Assertions.assertNull(p1().getRestoreLineage());
        Assertions.assertNull(p2().getRestoreLineage());
    }

    @Test
    public void testShadowStatsSkippedInCloudMode() {
        makeBothPartitionsPassL0();
        String origDeployMode = Config.deploy_mode;
        try {
            Config.deploy_mode = "cloud";
            Assertions.assertTrue(Config.isCloudMode());
            job.computeReuseShadowStats(db);
            Assertions.assertNull(job.getReuseShadowStats());
            List<String> fullInfo = job.getFullInfo();
            Assertions.assertEquals(FeConstants.null_string, fullInfo.get(REUSE_ESTIMATE_COLUMN));
        } finally {
            Config.deploy_mode = origDeployMode;
        }
        job.computeReuseShadowStats(db);
        Assertions.assertEquals(2, job.getReuseShadowStats().getReusable());
    }
}
