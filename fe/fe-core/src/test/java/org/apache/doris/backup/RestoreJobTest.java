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
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.HashDistributionInfo;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.MaterializedIndex.IndexExtState;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.PartitionInfo;
import org.apache.doris.catalog.PartitionType;
import org.apache.doris.catalog.ReplicaAllocation;
import org.apache.doris.catalog.Resource;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.Tablet;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.MarkedCountDownLatch;
import org.apache.doris.common.Pair;
import org.apache.doris.common.UserException;
import org.apache.doris.common.io.DataOutputBuffer;
import org.apache.doris.common.io.Writable;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.datasource.property.storage.BrokerProperties;
import org.apache.doris.fs.FileSystemFactory;
import org.apache.doris.journal.JournalEntity;
import org.apache.doris.persist.EditLog;
import org.apache.doris.persist.OperationType;
import org.apache.doris.resource.Tag;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.thrift.TStorageMedium;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import mockit.Delegate;
import mockit.Expectations;
import mockit.Injectable;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.Adler32;

public class RestoreJobTest {

    private Database db;
    private BackupJobInfo jobInfo;
    private RestoreJob job;
    private String label = "test_label";

    private AtomicLong id = new AtomicLong(50000);

    private OlapTable expectedRestoreTbl;

    private long repoId = 20000;

    @Mocked
    private Env env;
    @Mocked
    private InternalCatalog catalog;

    private MockBackupHandler backupHandler;

    private MockRepositoryMgr repoMgr;

    public RestoreJobTest() throws UserException {
    }

    // Thread is not mockable in Jmockit, use subclass instead
    private final class MockBackupHandler extends BackupHandler {
        public MockBackupHandler(Env env) {
            super(env);
        }

        @Override
        public RepositoryMgr getRepoMgr() {
            return repoMgr;
        }
    }

    // Thread is not mockable in Jmockit, use subclass instead
    private final class MockRepositoryMgr extends RepositoryMgr {
        public MockRepositoryMgr() {
            super();
        }

        @Override
        public Repository getRepo(long repoId) {
            return repo;
        }
    }

    @Mocked
    private EditLog editLog;
    @Mocked
    private SystemInfoService systemInfoService;

    @Injectable
    private Repository repo = new Repository(repoId, "repo", false, "bos://my_repo",
            FileSystemFactory.get(BrokerProperties.of("broker", Maps.newHashMap())));

    private BackupMeta backupMeta;

    @Before
    public void setUp() throws Exception {
        db = CatalogMocker.mockDb();
        backupHandler = new MockBackupHandler(env);
        repoMgr = new MockRepositoryMgr();

        Deencapsulation.setField(env, "backupHandler", backupHandler);

        new Expectations() {
            {
                env.getInternalCatalog();
                minTimes = 0;
                result = catalog;

                catalog.getDbNullable(anyLong);
                minTimes = 0;
                result = db;

                Env.getCurrentEnvJournalVersion();
                minTimes = 0;
                result = FeConstants.meta_version;

                env.getNextId();
                minTimes = 0;
                result = id.getAndIncrement();

                env.getEditLog();
                minTimes = 0;
                result = editLog;

                Env.getCurrentSystemInfo();
                minTimes = 0;
                result = systemInfoService;
            }
        };

        new Expectations() {
            {
                systemInfoService.selectBackendIdsForReplicaCreation((ReplicaAllocation) any,
                        Maps.newHashMap(), (TStorageMedium) any, false, true);
                minTimes = 0;
                result = new Delegate() {
                    public synchronized List<Long> selectBackendIdsForReplicaCreation(
                            ReplicaAllocation replicaAlloc, Map<Tag, Integer> nextIndexs,
                            TStorageMedium medium, boolean isStorageMediumSpecified,
                            boolean isOnlyForCheck) {
                        List<Long> beIds = Lists.newArrayList();
                        beIds.add(CatalogMocker.BACKEND1_ID);
                        beIds.add(CatalogMocker.BACKEND2_ID);
                        beIds.add(CatalogMocker.BACKEND3_ID);
                        return beIds;
                    }
                };
            }
        };

        new Expectations() {
            {
                editLog.logBackupJob((BackupJob) any);
                minTimes = 0;
                result = new Delegate() {
                    public void logBackupJob(BackupJob job) {
                        System.out.println("log backup job: " + job);
                    }
                };
            }
        };

        new Expectations() {
            {
                repo.upload(anyString, anyString);
                result = Status.OK;
                minTimes = 0;

                List<BackupMeta> backupMetas = Lists.newArrayList();
                repo.getSnapshotMetaFile(label, backupMetas, -1);
                minTimes = 0;
                result = new Delegate() {
                    public Status getSnapshotMetaFile(String label, List<BackupMeta> backupMetas) {
                        backupMetas.add(backupMeta);
                        return Status.OK;
                    }
                };
            }
        };

        new MockUp<MarkedCountDownLatch>() {
            @Mock
            boolean await(long timeout, TimeUnit unit) {
                return true;
            }
        };

        // gen BackupJobInfo
        jobInfo = new BackupJobInfo();
        jobInfo.backupTime = System.currentTimeMillis();
        jobInfo.dbId = CatalogMocker.TEST_DB_ID;
        jobInfo.dbName = CatalogMocker.TEST_DB_NAME;
        jobInfo.name = label;
        jobInfo.success = true;

        expectedRestoreTbl = (OlapTable) db.getTableNullable(CatalogMocker.TEST_TBL2_ID);
        BackupOlapTableInfo tblInfo = new BackupOlapTableInfo();
        tblInfo.id = CatalogMocker.TEST_TBL2_ID;
        jobInfo.backupOlapTableObjects.put(CatalogMocker.TEST_TBL2_NAME, tblInfo);

        for (Partition partition : expectedRestoreTbl.getPartitions()) {
            BackupPartitionInfo partInfo = new BackupPartitionInfo();
            partInfo.id = partition.getId();
            tblInfo.partitions.put(partition.getName(), partInfo);

            for (MaterializedIndex index : partition.getMaterializedIndices(IndexExtState.VISIBLE)) {
                BackupIndexInfo idxInfo = new BackupIndexInfo();
                idxInfo.id = index.getId();
                idxInfo.schemaHash = expectedRestoreTbl.getSchemaHashByIndexId(index.getId());
                partInfo.indexes.put(expectedRestoreTbl.getIndexNameById(index.getId()), idxInfo);

                for (Tablet tablet : index.getTablets()) {
                    List<String> files = Lists.newArrayList(tablet.getId() + ".dat",
                            tablet.getId() + ".idx", tablet.getId() + ".hdr");
                    BackupTabletInfo tabletInfo = new BackupTabletInfo(tablet.getId(), files);
                    idxInfo.sortedTabletInfoList.add(tabletInfo);
                }
            }
        }

        // drop this table, cause we want to try restoring this table
        db.unregisterTable(expectedRestoreTbl.getName());

        job = new RestoreJob(label, "2018-01-01 01:01:01", db.getId(), db.getFullName(), jobInfo, false,
                new ReplicaAllocation((short) 3), 100000, -1, false, false,
                false, false, false, false, false,
                env, repo.getId());

        List<Table> tbls = Lists.newArrayList();
        List<Resource> resources = Lists.newArrayList();
        tbls.add(expectedRestoreTbl);
        backupMeta = new BackupMeta(tbls, resources);
    }

    @Test
    public void testSignature() throws AnalysisException {
        Adler32 sig1 = new Adler32();
        sig1.update("name1".getBytes());
        sig1.update("name2".getBytes());
        System.out.println("sig1: " + Math.abs((int) sig1.getValue()));

        Adler32 sig2 = new Adler32();
        sig2.update("name2".getBytes());
        sig2.update("name1".getBytes());
        System.out.println("sig2: " + Math.abs((int) sig2.getValue()));

        OlapTable tbl = db.getOlapTableOrAnalysisException(CatalogMocker.TEST_TBL_NAME);
        List<String> partNames = Lists.newArrayList(tbl.getPartitionNames());
        System.out.println(partNames);
        System.out.println("tbl signature: " + tbl.getSignature(BackupHandler.SIGNATURE_VERSION, partNames));
        tbl.setName("newName");
        partNames = Lists.newArrayList(tbl.getPartitionNames());
        System.out.println("tbl signature: " + tbl.getSignature(BackupHandler.SIGNATURE_VERSION, partNames));
    }

    @Test
    public void testSerialization() throws IOException, AnalysisException {
        // populate the Guava Table fields with non-trivial content, to cover the
        // streaming Table adapter with real metadata objects (SnapshotInfo values)
        com.google.common.collect.Table<Long, Long, SnapshotInfo> snapshotInfos
                = Deencapsulation.getField(job, "snapshotInfos");
        snapshotInfos.put(10001L, 20001L, new SnapshotInfo(db.getId(), CatalogMocker.TEST_TBL2_ID, 30001L, 40001L,
                10001L, 20001L, 1234, "/path/to/snapshot1", Lists.newArrayList("1.dat", "1.idx", "1.hdr")));
        snapshotInfos.put(10002L, 20001L, new SnapshotInfo(db.getId(), CatalogMocker.TEST_TBL2_ID, 30001L, 40001L,
                10002L, 20001L, 1234, "/path/to/snapshot2", Lists.newArrayList("2.dat")));
        com.google.common.collect.Table<Long, Long, Long> restoredVersionInfo
                = Deencapsulation.getField(job, "restoredVersionInfo");
        restoredVersionInfo.put(CatalogMocker.TEST_TBL2_ID, 30001L, 99L);

        // 1. Write objects to file
        final Path path = Files.createTempFile("restoreJob", "tmp");
        DataOutputStream out = new DataOutputStream(Files.newOutputStream(path));

        job.write(out);
        out.flush();
        out.close();

        // 2. Read objects from file
        DataInputStream in = new DataInputStream(Files.newInputStream(path));

        RestoreJob job2 = RestoreJob.read(in);

        Assert.assertEquals(job.getJobId(), job2.getJobId());
        Assert.assertEquals(job.getDbId(), job2.getDbId());
        Assert.assertEquals(job.getCreateTime(), job2.getCreateTime());
        Assert.assertEquals(job.getType(), job2.getType());

        com.google.common.collect.Table<Long, Long, SnapshotInfo> readSnapshotInfos
                = Deencapsulation.getField(job2, "snapshotInfos");
        Assert.assertEquals(2, readSnapshotInfos.size());
        SnapshotInfo info1 = readSnapshotInfos.get(10001L, 20001L);
        Assert.assertEquals(CatalogMocker.TEST_TBL2_ID, info1.getTblId());
        Assert.assertEquals("/path/to/snapshot1", info1.getPath());
        Assert.assertEquals(Lists.newArrayList("1.dat", "1.idx", "1.hdr"), info1.getFiles());
        Assert.assertEquals("2.dat", readSnapshotInfos.get(10002L, 20001L).getFiles().get(0));
        com.google.common.collect.Table<Long, Long, Long> readRestoredVersionInfo
                = Deencapsulation.getField(job2, "restoredVersionInfo");
        Assert.assertEquals(Long.valueOf(99L), readRestoredVersionInfo.get(CatalogMocker.TEST_TBL2_ID, 30001L));

        // 3. delete files
        in.close();
        Files.delete(path);
    }

    @Test
    public void testResetPartitionVisibleAndNextVersionForRestore() throws Exception {
        long visibleVersion = 1234;
        long remotePartId = 123;
        String partName = "p20240723";
        MaterializedIndex index = new MaterializedIndex();
        Partition remotePart = new Partition(remotePartId, partName, index, new HashDistributionInfo());
        remotePart.setVisibleVersionAndTime(visibleVersion, 0);
        remotePart.setNextVersion(visibleVersion + 10);

        OlapTable localTbl = new OlapTable();
        localTbl.setPartitionInfo(new PartitionInfo(PartitionType.RANGE));
        OlapTable remoteTbl = new OlapTable();
        remoteTbl.addPartition(remotePart);
        remoteTbl.setPartitionInfo(new PartitionInfo(PartitionType.RANGE));

        ReplicaAllocation alloc = new ReplicaAllocation();
        job.resetPartitionForRestore(localTbl, remoteTbl, partName, alloc);

        Partition localPart = remoteTbl.getPartition(partName);
        Assert.assertEquals(localPart.getVisibleVersion(), visibleVersion);
        Assert.assertEquals(localPart.getNextVersion(), visibleVersion + 1);
    }

    @Test
    public void testBackupMetaRetainTablesByNameInBackup() throws Exception {
        OlapTable keptTbl = (OlapTable) db.getTableNullable(CatalogMocker.TEST_TBL_ID);
        BackupMeta meta = new BackupMeta(Lists.newArrayList(keptTbl, expectedRestoreTbl), Lists.newArrayList());

        // a restored table gets renamed, but stays in the meta under the name it had in the backup
        keptTbl.setName("alias_of_test_tbl");
        meta.retainTables(Sets.newHashSet(CatalogMocker.TEST_TBL_NAME));

        Assert.assertSame(keptTbl, meta.getTable(CatalogMocker.TEST_TBL_NAME));
        Assert.assertSame(keptTbl, meta.getTable(CatalogMocker.TEST_TBL_ID));
        Assert.assertNull(meta.getTable(CatalogMocker.TEST_TBL2_NAME));
        Assert.assertNull(meta.getTable(CatalogMocker.TEST_TBL2_ID));
    }

    @Test
    public void testDownloadJournalKeepsOnlyTablesOfRestoredPartitions() throws Exception {
        // test_tbl exists locally and gets a restored partition, test_tbl2 is restored as a new table
        OlapTable localTbl = (OlapTable) db.getTableNullable(CatalogMocker.TEST_TBL_ID);
        OlapTable remoteTbl = (OlapTable) CatalogMocker.mockDb().getTableNullable(CatalogMocker.TEST_TBL_ID);
        Partition restoredPart = remoteTbl.getPartition(CatalogMocker.TEST_SINGLE_PARTITION_NAME);
        long restoredPartId = 40100L;
        restoredPart.setIdForRestore(restoredPartId);

        BackupPartitionInfo remotePartInfo = new BackupPartitionInfo();
        remotePartInfo.id = CatalogMocker.TEST_SINGLE_PARTITION_ID;
        BackupOlapTableInfo remoteTblInfo = new BackupOlapTableInfo();
        remoteTblInfo.id = CatalogMocker.TEST_TBL_ID;
        remoteTblInfo.partitions.put(restoredPart.getName(), remotePartInfo);
        jobInfo.backupOlapTableObjects.put(CatalogMocker.TEST_TBL_NAME, remoteTblInfo);

        Deencapsulation.setField(job, "backupMeta",
                new BackupMeta(Lists.newArrayList(remoteTbl, expectedRestoreTbl), Lists.newArrayList()));
        job.restoredPartitions.add(Pair.of(CatalogMocker.TEST_TBL_NAME, restoredPart));
        job.restoredTbls.add(expectedRestoreTbl);

        DataOutputBuffer journal = new DataOutputBuffer();
        new Expectations() {
            {
                editLog.logRestoreJob((RestoreJob) any);
                times = 1;
                result = new Delegate() {
                    public void logRestoreJob(RestoreJob restoreJob) throws IOException {
                        restoreJob.write(journal);
                    }
                };
            }
        };

        job.waitingAllSnapshotsFinished();
        Assert.assertEquals(RestoreJob.RestoreJobState.DOWNLOAD, job.getState());

        RestoreJob replayedJob = RestoreJob.read(
                new DataInputStream(new ByteArrayInputStream(journal.getData(), 0, journal.getLength())));

        BackupMeta replayedMeta = Deencapsulation.getField(replayedJob, "backupMeta");
        Assert.assertEquals(Sets.newHashSet(CatalogMocker.TEST_TBL_NAME), replayedMeta.getTables().keySet());
        Assert.assertEquals(1, replayedJob.restoredTbls.size());

        // replaying the journal still finds the remote table of the restored partition
        new Expectations() {
            {
                catalog.getDbOrMetaException(anyLong);
                minTimes = 0;
                result = db;
            }
        };
        replayedJob.setEnv(env);
        replayedJob.replayRun();

        Assert.assertNotNull(localTbl.getPartition(restoredPartId));
        Assert.assertNotNull(db.getTableNullable(CatalogMocker.TEST_TBL2_ID));
    }

    private static byte[] journalOf(Writable writable) throws IOException {
        DataOutputBuffer buffer = new DataOutputBuffer();
        writable.write(buffer);
        return Arrays.copyOf(buffer.getData(), buffer.getLength());
    }

    private static DataInputStream journalInput(byte[] journal) {
        return new DataInputStream(new ByteArrayInputStream(journal));
    }

    // The json of a job which is not done, as AbstractJob.write puts it into a journal or the image
    private static String jsonOf(AbstractJob restoreJob) throws IOException {
        byte[] journal = journalOf(restoreJob);
        return new String(journal, Integer.BYTES, journal.length - Integer.BYTES, StandardCharsets.UTF_8);
    }

    private static RestoreJobStateChange readStateChangeJournal(byte[] journal) throws IOException {
        DataOutputBuffer buffer = new DataOutputBuffer();
        buffer.writeShort(OperationType.OP_RESTORE_JOB_STATE);
        buffer.write(journal);
        JournalEntity entity = new JournalEntity();
        entity.readFields(new DataInputStream(new ByteArrayInputStream(buffer.getData(), 0, buffer.getLength())));
        Assert.assertEquals(OperationType.OP_RESTORE_JOB_STATE, entity.getOpCode());
        return (RestoreJobStateChange) entity.getData();
    }

    // The catalog of a follower before the restore job: test_tbl exists, test_tbl2 doesn't
    private Database mockFollowerDb() throws Exception {
        Database followerDb = CatalogMocker.mockDb();
        followerDb.unregisterTable(CatalogMocker.TEST_TBL2_NAME);
        new Expectations() {
            {
                catalog.getDbOrMetaException(anyLong);
                minTimes = 0;
                result = followerDb;
            }
        };
        return followerDb;
    }

    @Test
    public void testStateChangeJournalsReplayToSameJobAsWholeJobJournals() throws Exception {
        // The master. test_tbl exists locally and gets a restored partition, test_tbl2 is restored as a new table.
        OlapTable remoteTbl = (OlapTable) CatalogMocker.mockDb().getTableNullable(CatalogMocker.TEST_TBL_ID);
        Partition restoredPart = remoteTbl.getPartition(CatalogMocker.TEST_SINGLE_PARTITION_NAME);
        long restoredPartId = 40100L;
        restoredPart.setIdForRestore(restoredPartId);

        BackupPartitionInfo remotePartInfo = new BackupPartitionInfo();
        remotePartInfo.id = CatalogMocker.TEST_SINGLE_PARTITION_ID;
        BackupOlapTableInfo remoteTblInfo = new BackupOlapTableInfo();
        remoteTblInfo.id = CatalogMocker.TEST_TBL_ID;
        remoteTblInfo.partitions.put(restoredPart.getName(), remotePartInfo);
        jobInfo.backupOlapTableObjects.put(CatalogMocker.TEST_TBL_NAME, remoteTblInfo);
        Deencapsulation.setField(job, "backupMeta",
                new BackupMeta(Lists.newArrayList(remoteTbl, expectedRestoreTbl), Lists.newArrayList()));

        // 1. The creation journal is the whole job in both formats.
        byte[] creationJournal = journalOf(job);

        // 2. What checkAndPrepareMeta and the snapshot tasks produce before the DOWNLOAD state.
        Deencapsulation.setField(job, "jobId", 60001L);
        job.metaPreparedTime = 1700000000000L;
        job.restoredPartitions.add(Pair.of(CatalogMocker.TEST_TBL_NAME, restoredPart));
        job.restoredTbls.add(expectedRestoreTbl);
        com.google.common.collect.Table<Long, Long, Long> restoredVersionInfo
                = Deencapsulation.getField(job, "restoredVersionInfo");
        restoredVersionInfo.put(CatalogMocker.TEST_TBL_ID, restoredPartId, 99L);
        for (long tabletId = 10001L; tabletId <= 10003L; tabletId++) {
            for (long beId : Lists.newArrayList(CatalogMocker.BACKEND1_ID, CatalogMocker.BACKEND2_ID)) {
                job.snapshotInfos.put(tabletId, beId, new SnapshotInfo(db.getId(), CatalogMocker.TEST_TBL2_ID,
                        30001L, 40001L, tabletId, beId, 1234, "/path/to/snapshot/" + tabletId,
                        Lists.newArrayList(tabletId + ".dat", tabletId + ".hdr")));
            }
            job.fileMapping.putMapping(
                    new RestoreFileMapping.IdChain(CatalogMocker.TEST_TBL2_ID, 30001L, 40001L, tabletId, 1L, -1L),
                    new RestoreFileMapping.IdChain(CatalogMocker.TEST_TBL2_ID, 30002L, 40002L, tabletId + 100, 2L,
                            -1L), true);
        }
        Map<Long, String> taskErrMsg = Deencapsulation.getField(job, "taskErrMsg");
        taskErrMsg.put(70001L, "a failed and retried task");

        // 3. The master enters DOWNLOAD and COMMIT. Each time, also take the whole job as OP_RESTORE_JOB writes it.
        List<byte[]> stateChangeJournals = Lists.newArrayList();
        new Expectations() {
            {
                editLog.logRestoreJob((RestoreJob) any);
                times = 0;

                editLog.logRestoreJobStateChange((RestoreJobStateChange) any);
                times = 2;
                result = new Delegate() {
                    public void logRestoreJobStateChange(RestoreJobStateChange stateChange) throws IOException {
                        stateChangeJournals.add(journalOf(stateChange));
                    }
                };
            }
        };
        byte[] wholeDownloadJournal;
        byte[] wholeCommitJournal;
        Config.enable_restore_job_state_journal = true;
        try {
            job.waitingAllSnapshotsFinished();
            Assert.assertEquals(RestoreJob.RestoreJobState.DOWNLOAD, job.getState());
            wholeDownloadJournal = journalOf(job);

            Deencapsulation.invoke(job, "waitingAllDownloadFinished");
            Assert.assertEquals(RestoreJob.RestoreJobState.COMMIT, job.getState());
            wholeCommitJournal = journalOf(job);
        } finally {
            Config.enable_restore_job_state_journal = false;
        }
        System.out.println("journal bytes, whole job: creation " + creationJournal.length + ", DOWNLOAD "
                + wholeDownloadJournal.length + ", COMMIT " + wholeCommitJournal.length + "; state change: DOWNLOAD "
                + stateChangeJournals.get(0).length + ", COMMIT " + stateChangeJournals.get(1).length);
        Assert.assertTrue(stateChangeJournals.get(0).length < wholeDownloadJournal.length);
        // the COMMIT state change carries no metadata
        Assert.assertTrue(stateChangeJournals.get(1).length < 200);

        // 4. Follower A replays the whole job journals.
        Database dbA = mockFollowerDb();
        BackupHandler handlerA = new MockBackupHandler(env);
        List<String> jobJsonsA = Lists.newArrayList();
        for (byte[] journal : Lists.newArrayList(creationJournal, wholeDownloadJournal, wholeCommitJournal)) {
            RestoreJob replayedJob = RestoreJob.read(journalInput(journal));
            replayedJob.setEnv(env);
            handlerA.replayAddJob(replayedJob);
            jobJsonsA.add(jsonOf(handlerA.getJob(db.getId())));
        }
        RestoreJob jobA = (RestoreJob) handlerA.getJob(db.getId());

        // 5. Follower B replays the creation journal and the state change journals.
        Database dbB = mockFollowerDb();
        BackupHandler handlerB = new MockBackupHandler(env);
        List<String> jobJsonsB = Lists.newArrayList();
        RestoreJob jobB = RestoreJob.read(journalInput(creationJournal));
        jobB.setEnv(env);
        handlerB.replayAddJob(jobB);
        jobJsonsB.add(jsonOf(handlerB.getJob(db.getId())));
        for (byte[] journal : stateChangeJournals) {
            handlerB.replayRestoreJobStateChange(readStateChangeJournal(journal));
            jobJsonsB.add(jsonOf(handlerB.getJob(db.getId())));
        }

        // 6. Both followers hold the same job after every journal, so they write the same image,
        // and either of them continues the job the same way after becoming the master.
        Assert.assertEquals(jobJsonsA.get(0), jobJsonsB.get(0));
        Assert.assertEquals(jobJsonsA.get(1), jobJsonsB.get(1));
        Assert.assertEquals(jobJsonsA.get(2), jobJsonsB.get(2));
        Assert.assertTrue(jobJsonsB.get(1).contains("\"st\":\"DOWNLOAD\""));
        Assert.assertTrue(jobJsonsB.get(2).contains("\"st\":\"COMMIT\""));

        // 7. Both followers have the same catalog.
        for (Database followerDb : Lists.newArrayList(dbA, dbB)) {
            OlapTable localTbl = (OlapTable) followerDb.getTableNullable(CatalogMocker.TEST_TBL_ID);
            Assert.assertNotNull(localTbl.getPartition(restoredPartId));
            Assert.assertNotNull(followerDb.getTableNullable(CatalogMocker.TEST_TBL2_ID));
        }

        // 8. The state change journals are applied to the job created by the creation journal, so its
        // restored tables stay the catalog tables, as on the master. A whole job journal replaces the
        // job, so after the COMMIT journal follower A holds a second copy of the tables.
        Assert.assertSame(jobB, handlerB.getJob(db.getId()));
        Assert.assertSame(dbB.getTableNullable(CatalogMocker.TEST_TBL2_ID), jobB.restoredTbls.get(0));
        Assert.assertNotSame(dbA.getTableNullable(CatalogMocker.TEST_TBL2_ID), jobA.restoredTbls.get(0));
    }

    @Test
    public void testStateChangeJournalRequiresJobInPreviousState() throws Exception {
        Deencapsulation.setField(job, "backupMeta",
                new BackupMeta(Lists.newArrayList(expectedRestoreTbl), Lists.newArrayList()));
        byte[] creationJournal = journalOf(job);

        List<byte[]> stateChangeJournals = Lists.newArrayList();
        new Expectations() {
            {
                editLog.logRestoreJobStateChange((RestoreJobStateChange) any);
                times = 2;
                result = new Delegate() {
                    public void logRestoreJobStateChange(RestoreJobStateChange stateChange) throws IOException {
                        stateChangeJournals.add(journalOf(stateChange));
                    }
                };
            }
        };
        Config.enable_restore_job_state_journal = true;
        try {
            job.waitingAllSnapshotsFinished();
            Deencapsulation.invoke(job, "waitingAllDownloadFinished");
        } finally {
            Config.enable_restore_job_state_journal = false;
        }

        // the COMMIT state change can't be applied to a job which has not replayed the DOWNLOAD one
        RestoreJob pendingJob = RestoreJob.read(journalInput(creationJournal));
        pendingJob.setEnv(env);
        RestoreJobStateChange commit = readStateChangeJournal(stateChangeJournals.get(1));
        Assert.assertThrows(IllegalStateException.class, () -> pendingJob.replayStateChange(commit));
    }
}
