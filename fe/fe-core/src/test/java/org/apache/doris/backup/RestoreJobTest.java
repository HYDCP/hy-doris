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
import org.apache.doris.catalog.AutoIncrementGenerator;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.HashDistributionInfo;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.MaterializedIndex.IndexExtState;
import org.apache.doris.catalog.OdbcCatalogResource;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.PartitionInfo;
import org.apache.doris.catalog.PartitionType;
import org.apache.doris.catalog.ReplicaAllocation;
import org.apache.doris.catalog.Resource;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.Tablet;
import org.apache.doris.catalog.View;
import org.apache.doris.common.AnalysisException;
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
import org.apache.doris.journal.SerializedJournalEntity;
import org.apache.doris.persist.EditLog;
import org.apache.doris.resource.Tag;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.thrift.TStorageMedium;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.Uninterruptibles;
import mockit.Delegate;
import mockit.Expectations;
import mockit.Injectable;
import mockit.Invocation;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
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

    // Logs DOWNLOAD journals like the EditLog does, and keeps them: the journal has its place in the log when it is
    // queued, and the job releases its locks there.
    private List<Writable> logDownloadJournals() {
        List<Writable> journals = Lists.newArrayList();
        new Expectations() {
            {
                editLog.logRestoreJob((Writable) any, (Runnable) any);
                minTimes = 0;
                result = new Delegate() {
                    public void logRestoreJob(Writable journal, Runnable afterQueued) {
                        journals.add(journal);
                        afterQueued.run();
                    }
                };
            }
        };
        return journals;
    }

    // What an FE that replays the journal reads from it.
    private RestoreJob readDownloadJournal(Writable journal) throws IOException {
        DataOutputBuffer buffer = new DataOutputBuffer();
        journal.write(buffer);
        return RestoreJob.read(new DataInputStream(new ByteArrayInputStream(buffer.getData(), 0, buffer.getLength())));
    }

    // Runs after each serialization of the job, like a statement of another session that commits right then.
    private AtomicInteger afterEachSerialization(Consumer<Integer> statement) {
        AtomicInteger serializedNum = new AtomicInteger();
        new MockUp<AbstractJob>() {
            @Mock
            public void write(Invocation inv, DataOutput out) {
                inv.proceed();
                int num = serializedNum.incrementAndGet();
                Thread session = new Thread(() -> statement.accept(num));
                session.start();
                Uninterruptibles.joinUninterruptibly(session);
            }
        };
        return serializedNum;
    }

    private boolean canWriteLock(Database db, Table table) throws InterruptedException {
        AtomicBoolean locked = new AtomicBoolean(true);
        Thread session = new Thread(() -> {
            if (db.tryWriteLock(10, TimeUnit.MILLISECONDS)) {
                db.writeUnlock();
            } else {
                locked.set(false);
            }
            if (table.tryWriteLock(10, TimeUnit.MILLISECONDS)) {
                table.writeUnlock();
            } else {
                locked.set(false);
            }
        });
        session.start();
        session.join();
        return locked.get();
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

        List<Writable> journals = logDownloadJournals();

        // the restored table has been registered on the master since PENDING, but not on an FE that replays
        db.registerTable(expectedRestoreTbl);
        job.waitingAllSnapshotsFinished();
        Assert.assertEquals(RestoreJob.RestoreJobState.DOWNLOAD, job.getState());
        db.unregisterTable(expectedRestoreTbl.getName());

        Assert.assertEquals(1, journals.size());
        RestoreJob replayedJob = readDownloadJournal(journals.get(0));

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

    // The restored table test_tbl2 has been registered and its first replica has to be downloaded.
    private void prepareDownloadOfRestoredTable() {
        db.registerTable(expectedRestoreTbl);
        Partition part = expectedRestoreTbl.getPartitions().iterator().next();
        MaterializedIndex index = part.getBaseIndex();
        Tablet tablet = index.getTablets().get(0);
        long replicaId = tablet.getReplicas().get(0).getId();
        RestoreFileMapping.IdChain catalogIds = new RestoreFileMapping.IdChain(expectedRestoreTbl.getId(),
                part.getId(), index.getId(), tablet.getId(), replicaId, -1L);
        RestoreFileMapping.IdChain repoIds = new RestoreFileMapping.IdChain(CatalogMocker.TEST_TBL2_ID,
                part.getId(), index.getId(), tablet.getId(), -1L, -1L);
        job.fileMapping.putMapping(catalogIds, repoIds, true);
        job.restoredTbls.add(expectedRestoreTbl);
        Deencapsulation.setField(job, "backupMeta", backupMeta);
        Deencapsulation.setField(job, "state", RestoreJob.RestoreJobState.SNAPSHOTING);
    }

    private View prepareDownloadOfRestoredView() {
        View restoredView = new View(40200L, "test_restored_view", Lists.newArrayList());
        restoredView.setComment("before");
        db.registerTable(restoredView);
        job.restoredTbls.add(restoredView);
        return restoredView;
    }

    // The existing table test_tbl has got a restored partition.
    private Partition prepareDownloadOfRestoredPartition() throws Exception {
        OlapTable localTbl = (OlapTable) db.getTableNullable(CatalogMocker.TEST_TBL_ID);
        OlapTable remoteTbl = (OlapTable) CatalogMocker.mockDb().getTableNullable(CatalogMocker.TEST_TBL_ID);
        Partition restoredPart = remoteTbl.getPartition(CatalogMocker.TEST_SINGLE_PARTITION_NAME);
        restoredPart.setIdForRestore(40100L);
        restoredPart.setName("restored_partition");
        localTbl.addPartition(restoredPart);
        job.restoredPartitions.add(Pair.of(CatalogMocker.TEST_TBL_NAME, restoredPart));
        Deencapsulation.setField(job, "backupMeta",
                new BackupMeta(Lists.newArrayList(remoteTbl, expectedRestoreTbl), Lists.newArrayList()));
        return restoredPart;
    }

    @Test
    public void testDownloadJournalIsQueuedUnderLocksAndWrittenWithoutThem() {
        prepareDownloadOfRestoredTable();

        // a drop of the restored table needs the db write lock, any other change of it needs its write lock
        List<Boolean> couldWriteLock = Lists.newArrayList();
        new Expectations() {
            {
                editLog.logRestoreJob((Writable) any, (Runnable) any);
                times = 1;
                result = new Delegate() {
                    public void logRestoreJob(Writable journal, Runnable afterQueued) throws InterruptedException {
                        couldWriteLock.add(canWriteLock(db, expectedRestoreTbl));
                        afterQueued.run();
                        // the journal has its place in the log, and is written without the locks
                        couldWriteLock.add(canWriteLock(db, expectedRestoreTbl));
                    }
                };
            }
        };

        job.waitingAllSnapshotsFinished();
        Assert.assertTrue(job.getStatus().ok());
        Assert.assertEquals(RestoreJob.RestoreJobState.DOWNLOAD, job.getState());
        Assert.assertEquals(Lists.newArrayList(false, true), couldWriteLock);
    }

    @Test
    public void testDownloadJournalIsNotLoggedAfterRestoredTableIsDropped() {
        prepareDownloadOfRestoredTable();
        List<Writable> journals = logDownloadJournals();

        // DROP TABLE FORCE commits while the job is being serialized for the DOWNLOAD journal
        afterEachSerialization(num -> db.unregisterTable(expectedRestoreTbl.getName()));

        job.waitingAllSnapshotsFinished();
        Assert.assertEquals(Status.ErrCode.NOT_FOUND, job.getStatus().getErrCode());
        Assert.assertTrue(journals.isEmpty());
    }

    @Test
    public void testDownloadJournalIsNotLoggedAfterRestoredViewIsDropped() {
        prepareDownloadOfRestoredTable();
        View restoredView = prepareDownloadOfRestoredView();
        List<Writable> journals = logDownloadJournals();

        // DROP VIEW needs no FORCE, and the view is not in the file mapping
        afterEachSerialization(num -> db.unregisterTable(restoredView.getName()));

        job.waitingAllSnapshotsFinished();
        Assert.assertEquals(Status.ErrCode.NOT_FOUND, job.getStatus().getErrCode());
        Assert.assertTrue(journals.isEmpty());
    }

    @Test
    public void testDownloadJournalIsNotLoggedAfterTableOfRestoredPartitionIsDropped() throws Exception {
        prepareDownloadOfRestoredTable();
        prepareDownloadOfRestoredPartition();
        List<Writable> journals = logDownloadJournals();

        afterEachSerialization(num -> db.unregisterTable(CatalogMocker.TEST_TBL_NAME));

        job.waitingAllSnapshotsFinished();
        Assert.assertEquals(Status.ErrCode.NOT_FOUND, job.getStatus().getErrCode());
        Assert.assertTrue(journals.isEmpty());
    }

    // What Alter.modifyViewDef() does to a view, under the same locks.
    private void alterViewComment(View view, String comment) {
        db.writeLock();
        view.writeLock();
        try {
            view.setComment(comment);
            db.unregisterTable(view.getName());
            db.registerTable(view);
        } finally {
            view.writeUnlock();
            db.writeUnlock();
        }
    }

    @Test
    public void testDownloadJournalCarriesViewAlteredWhileSerialized() throws Exception {
        prepareDownloadOfRestoredTable();
        View restoredView = prepareDownloadOfRestoredView();
        List<Writable> journals = logDownloadJournals();

        // ALTER VIEW commits right after the job has been serialized for the first time. It changes the same View
        // object, and its journal is skipped by an FE that replays, which does not know the view yet.
        AtomicInteger serializedNum = afterEachSerialization(num -> {
            if (num == 1) {
                alterViewComment(restoredView, "after");
            }
        });

        job.waitingAllSnapshotsFinished();
        Assert.assertTrue(job.getStatus().ok());
        Assert.assertEquals(2, serializedNum.get());
        Assert.assertEquals(1, journals.size());
        Assert.assertTrue(journals.get(0) instanceof SerializedJournalEntity);

        RestoreJob replayedJob = readDownloadJournal(journals.get(0));
        Table replayedView = replayedJob.restoredTbls.stream()
                .filter(tbl -> tbl.getId() == restoredView.getId()).findFirst().get();
        Assert.assertEquals("after", replayedView.getComment());
    }

    @Test
    public void testDownloadJournalCarriesRestoredTableWrittenWhileSerialized() throws Exception {
        prepareDownloadOfRestoredTable();
        List<Writable> journals = logDownloadJournals();

        // any change of an OLAP table takes its write lock, like this ALTER TABLE MODIFY COMMENT
        AtomicInteger serializedNum = afterEachSerialization(num -> {
            if (num == 1) {
                expectedRestoreTbl.writeLock();
                try {
                    expectedRestoreTbl.setComment("after");
                } finally {
                    expectedRestoreTbl.writeUnlock();
                }
            }
        });

        job.waitingAllSnapshotsFinished();
        Assert.assertTrue(job.getStatus().ok());
        Assert.assertEquals(2, serializedNum.get());
        Assert.assertEquals(1, journals.size());
        Assert.assertEquals("after", readDownloadJournal(journals.get(0)).restoredTbls.get(0).getComment());
    }

    @Test
    public void testDownloadJournalCarriesTableOfRestoredPartitionWrittenWhileSerialized() throws Exception {
        prepareDownloadOfRestoredTable();
        Partition restoredPart = prepareDownloadOfRestoredPartition();
        OlapTable localTbl = (OlapTable) db.getTableNullable(CatalogMocker.TEST_TBL_ID);
        List<Writable> journals = logDownloadJournals();

        // the restored partition is inside a table that is not restored, and is changed under the lock of that table
        AtomicInteger serializedNum = afterEachSerialization(num -> {
            if (num == 1) {
                localTbl.writeLock();
                try {
                    restoredPart.setVisibleVersionAndTime(7L, 0L);
                } finally {
                    localTbl.writeUnlock();
                }
            }
        });

        job.waitingAllSnapshotsFinished();
        Assert.assertTrue(job.getStatus().ok());
        Assert.assertEquals(2, serializedNum.get());
        Assert.assertEquals(1, journals.size());
        Assert.assertEquals(7L, readDownloadJournal(journals.get(0)).restoredPartitions.get(0).second
                .getVisibleVersion());
    }

    @Test
    public void testDownloadJournalIsLeftToFlusherIfRestoredTableKeepsBeingWritten() {
        prepareDownloadOfRestoredTable();
        List<Writable> journals = logDownloadJournals();

        // like loads into a table restored with allow_load
        AtomicInteger serializedNum = afterEachSerialization(num -> {
            expectedRestoreTbl.writeLock();
            expectedRestoreTbl.writeUnlock();
        });

        job.waitingAllSnapshotsFinished();
        Assert.assertTrue(job.getStatus().ok());
        Assert.assertEquals(RestoreJob.RestoreJobState.DOWNLOAD, job.getState());
        Assert.assertEquals(2, serializedNum.get());
        // the job itself is queued, and the flusher serializes it once it has its place in the log
        Assert.assertEquals(1, journals.size());
        Assert.assertSame(job, journals.get(0));
    }

    // Not every logged change of the restored objects takes the write lock of a table, so the job can not prove
    // that a journal it has serialized itself is not outdated.
    private void checkDownloadJournalIsLeftToFlusherWithoutSerializing() {
        List<Writable> journals = logDownloadJournals();
        AtomicInteger serializedNum = afterEachSerialization(num -> { });

        job.waitingAllSnapshotsFinished();
        Assert.assertTrue(job.getStatus().ok());
        Assert.assertEquals(0, serializedNum.get());
        Assert.assertEquals(1, journals.size());
        Assert.assertSame(job, journals.get(0));
    }

    @Test
    public void testDownloadJournalWithRestoredResourceIsLeftToFlusher() {
        prepareDownloadOfRestoredTable();
        job.restoredResources.add(new OdbcCatalogResource("test_restored_resource"));
        checkDownloadJournalIsLeftToFlusherWithoutSerializing();
    }

    @Test
    public void testDownloadJournalWithAutoIncrementTableIsLeftToFlusher() {
        prepareDownloadOfRestoredTable();
        Deencapsulation.setField(expectedRestoreTbl, "autoIncrementGenerator",
                new AutoIncrementGenerator(db.getId(), expectedRestoreTbl.getId(), 1L, 1L));
        checkDownloadJournalIsLeftToFlusherWithoutSerializing();
    }

    @Test
    public void testDownloadJournalWithStoragePolicyIsLeftToFlusher() throws Exception {
        prepareDownloadOfRestoredTable();
        expectedRestoreTbl.setStoragePolicy("test_storage_policy");
        checkDownloadJournalIsLeftToFlusherWithoutSerializing();
    }

    @Test
    public void testDownloadJournalLeftToFlusherIsNotLoggedAfterRestoredViewIsDropped() {
        prepareDownloadOfRestoredTable();
        View restoredView = prepareDownloadOfRestoredView();
        job.restoredResources.add(new OdbcCatalogResource("test_restored_resource"));
        List<Writable> journals = logDownloadJournals();

        db.unregisterTable(restoredView.getName());
        job.waitingAllSnapshotsFinished();
        Assert.assertEquals(Status.ErrCode.NOT_FOUND, job.getStatus().getErrCode());
        Assert.assertTrue(journals.isEmpty());
    }

    private void logCancelledJob() {
        new Expectations() {
            {
                editLog.logRestoreJob((RestoreJob) any);
                minTimes = 0;
            }
        };
    }

    @Test
    public void testCancelRemovesRestoredTableAndView() {
        prepareDownloadOfRestoredTable();
        View restoredView = prepareDownloadOfRestoredView();
        logCancelledJob();

        Assert.assertTrue(job.cancel().ok());
        Assert.assertNull(db.getTableNullable(expectedRestoreTbl.getId()));
        Assert.assertNull(db.getTableNullable(restoredView.getId()));
    }

    @Test
    public void testReplayCancelRemovesRestoredTableAndView() throws Exception {
        prepareDownloadOfRestoredTable();
        View restoredView = prepareDownloadOfRestoredView();

        // an FE that replays has its own copy of the job, whose restored tables are not the objects in its catalog
        Deencapsulation.setField(job, "state", RestoreJob.RestoreJobState.DOWNLOAD);
        RestoreJob replayedJob = readDownloadJournal(job);
        Assert.assertNotSame(expectedRestoreTbl, replayedJob.restoredTbls.get(0));
        replayedJob.setEnv(env);
        replayedJob.replayCancel();

        Assert.assertNull(db.getTableNullable(expectedRestoreTbl.getId()));
        Assert.assertNull(db.getTableNullable(restoredView.getId()));
    }

    @Test
    public void testCancelKeepsViewCreatedWithNameOfDroppedRestoredView() {
        prepareDownloadOfRestoredTable();
        View restoredView = prepareDownloadOfRestoredView();
        logCancelledJob();

        // DROP VIEW, then CREATE VIEW with the same name. An FE that replays the cancellation has no restored
        // tables in its job if the DOWNLOAD journal has not been logged, and keeps the new view.
        db.unregisterTable(restoredView.getName());
        View newView = new View(60002L, restoredView.getName(), Lists.newArrayList());
        Assert.assertTrue(db.registerTable(newView));

        Assert.assertTrue(job.cancel().ok());
        Assert.assertSame(newView, db.getTableNullable(restoredView.getName()));
        Assert.assertNull(db.getTableNullable(expectedRestoreTbl.getId()));
    }

    @Test
    public void testCancelKeepsTableCreatedWithNameOfDroppedRestoredTable() throws Exception {
        prepareDownloadOfRestoredTable();
        logCancelledJob();

        // DROP TABLE FORCE, then CREATE TABLE with the same name
        db.unregisterTable(expectedRestoreTbl.getName());
        OlapTable newTbl = (OlapTable) CatalogMocker.mockDb().getTableNullable(CatalogMocker.TEST_TBL2_ID);
        Deencapsulation.setField(newTbl, "id", 60001L);
        Assert.assertTrue(db.registerTable(newTbl));

        Assert.assertTrue(job.cancel().ok());
        Assert.assertSame(newTbl, db.getTableNullable(expectedRestoreTbl.getName()));
    }

    @Test
    public void testCancelKeepsPartitionOfTableCreatedWithNameOfDroppedTable() throws Exception {
        prepareDownloadOfRestoredTable();
        Partition restoredPart = prepareDownloadOfRestoredPartition();
        OlapTable localTbl = (OlapTable) db.getTableNullable(CatalogMocker.TEST_TBL_ID);
        logCancelledJob();

        // DROP TABLE FORCE of the table that has got the restored partition, then CREATE TABLE with the same name
        // and a partition with the name of the restored one
        db.unregisterTable(localTbl.getName());
        OlapTable newTbl = (OlapTable) CatalogMocker.mockDb().getTableNullable(CatalogMocker.TEST_TBL_ID);
        Deencapsulation.setField(newTbl, "id", 60001L);
        Partition newPart = newTbl.getPartition(CatalogMocker.TEST_SINGLE_PARTITION_NAME);
        newTbl.renamePartition(newPart.getName(), restoredPart.getName());
        Assert.assertTrue(db.registerTable(newTbl));

        Assert.assertTrue(job.cancel().ok());
        Assert.assertSame(newPart, newTbl.getPartition(restoredPart.getName()));
    }

    @Test
    public void testCancelRemovesRestoredPartition() throws Exception {
        prepareDownloadOfRestoredTable();
        Partition restoredPart = prepareDownloadOfRestoredPartition();
        OlapTable localTbl = (OlapTable) db.getTableNullable(CatalogMocker.TEST_TBL_ID);
        logCancelledJob();

        Assert.assertTrue(job.cancel().ok());
        Assert.assertNull(localTbl.getPartition(restoredPart.getId()));
        Assert.assertNotNull(localTbl.getPartition(CatalogMocker.TEST_SINGLE_PARTITION_ID));
    }
}
