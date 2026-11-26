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
import org.apache.doris.common.UserException;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.datasource.property.storage.BrokerProperties;
import org.apache.doris.fs.FileSystemFactory;
import org.apache.doris.persist.EditLog;
import org.apache.doris.resource.Tag;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.task.DirMoveTask;
import org.apache.doris.task.DownloadTask;
import org.apache.doris.task.SnapshotTask;
import org.apache.doris.thrift.TFinishTaskRequest;
import org.apache.doris.thrift.TStatus;
import org.apache.doris.thrift.TStatusCode;
import org.apache.doris.thrift.TStorageMedium;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import mockit.Delegate;
import mockit.Expectations;
import mockit.Injectable;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
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

        // the jobs nudged, whether or not Config.backup_restore_run_on_task_finish lets the nudge through
        private final List<Long> nudgedJobIds = new CopyOnWriteArrayList<>();

        @Override
        public void nudge(long dbId, long jobId) {
            nudgedJobIds.add(jobId);
            super.nudge(dbId, jobId);
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

    // the state of the job at every restore edit log written
    private final List<RestoreJobState> loggedStates = new CopyOnWriteArrayList<>();
    private boolean origMergeIdleTicks;
    private boolean origRunOnTaskFinish;

    @Before
    public void setUp() throws Exception {
        origMergeIdleTicks = Config.backup_restore_merge_idle_ticks;
        origRunOnTaskFinish = Config.backup_restore_run_on_task_finish;
        Config.backup_restore_merge_idle_ticks = false;
        Config.backup_restore_run_on_task_finish = false;
        loggedStates.clear();
        db = CatalogMocker.mockDb();
        backupHandler = new MockBackupHandler(env);
        repoMgr = new MockRepositoryMgr();

        Deencapsulation.setField(env, "backupHandler", backupHandler);

        // partial on env, as in BackupJobTest: what is not recorded (getBackupHandler()) runs the real code, so that
        // the nudge of a job reaches the handler above (Thread is unmockable, it can not be cascaded)
        new Expectations(env) {
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

                editLog.logRestoreJob((RestoreJob) any);
                minTimes = 0;
                result = new Delegate() {
                    public void logRestoreJob(RestoreJob job) {
                        loggedStates.add(job.getState());
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

        job = newJob();

        List<Table> tbls = Lists.newArrayList();
        List<Resource> resources = Lists.newArrayList();
        tbls.add(expectedRestoreTbl);
        backupMeta = new BackupMeta(tbls, resources);
    }

    // the repo is set, as by the first run()
    private RestoreJob newJob() {
        RestoreJob restoreJob = new RestoreJob(label, "2018-01-01 01:01:01", db.getId(), db.getFullName(), jobInfo,
                false, new ReplicaAllocation((short) 3), 100000, -1, false, false,
                false, false, false, false, false,
                env, repo.getId());
        Deencapsulation.setField(restoreJob, "repo", repo);
        return restoreJob;
    }

    @After
    public void tearDown() {
        Config.backup_restore_merge_idle_ticks = origMergeIdleTicks;
        Config.backup_restore_run_on_task_finish = origRunOnTaskFinish;
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

    // ---------------------------------------------------------------------------------------------
    // backup_restore_merge_idle_ticks and backup_restore_run_on_task_finish on a restore job
    // ---------------------------------------------------------------------------------------------

    private static TFinishTaskRequest ok() {
        TFinishTaskRequest report = new TFinishTaskRequest();
        report.setTaskStatus(new TStatus(TStatusCode.OK));
        return report;
    }

    private static TFinishTaskRequest downloadReport() {
        TFinishTaskRequest report = ok();
        report.setDownloadedTabletIds(Lists.newArrayList());
        return report;
    }

    // the job in DOWNLOADING with the given download tasks outstanding, of different backends; the job has nothing
    // to move at the commit (no snapshot info)
    private List<DownloadTask> downloading(RestoreJob restoreJob, int taskNum) {
        Deencapsulation.setField(restoreJob, "state", RestoreJobState.DOWNLOADING);
        Map<Long, Long> unfinished = Deencapsulation.getField(restoreJob, "unfinishedSignatureToId");
        List<DownloadTask> tasks = Lists.newArrayList();
        for (int i = 0; i < taskNum; i++) {
            long beId = CatalogMocker.BACKEND1_ID + i;
            DownloadTask task = new DownloadTask(null, beId, 7001 + i, restoreJob.getJobId(), db.getId(),
                    Lists.newArrayList());
            unfinished.put(task.getSignature(), beId);
            tasks.add(task);
        }
        return tasks;
    }

    @Test
    public void testMergedTicksGoFromTheDownloadToTheEndInOneRun() {
        // the switch off: one state per run, as before
        job = newJob();
        downloading(job, 0);
        job.run();
        Assert.assertEquals(RestoreJobState.COMMIT, job.getState());
        job.run();
        Assert.assertEquals(RestoreJobState.COMMITTING, job.getState());
        job.run();
        Assert.assertEquals(RestoreJobState.FINISHED, job.getState());
        List<RestoreJobState> oneByOne = Lists.newArrayList(loggedStates);
        Assert.assertEquals(Lists.newArrayList(RestoreJobState.COMMIT, RestoreJobState.FINISHED), oneByOne);

        // the switch on: the job waits while a download task is outstanding, then goes to the end in one run with
        // the same logs in the same order
        Config.backup_restore_merge_idle_ticks = true;
        loggedStates.clear();
        job = newJob();
        DownloadTask task = downloading(job, 1).get(0);
        job.run();
        Assert.assertEquals(RestoreJobState.DOWNLOADING, job.getState());
        Assert.assertTrue(loggedStates.isEmpty());
        Assert.assertTrue(job.finishTabletDownloadTask(task, downloadReport()));
        job.run();
        Assert.assertTrue(job.getStatus().toString(), job.getStatus().ok());
        Assert.assertEquals(RestoreJobState.FINISHED, job.getState());
        Assert.assertEquals(oneByOne, loggedStates);
    }

    // The handler runs the nudged job as in the FE: this FE is a ready master and the job is the current one of its
    // database. env.getBackupHandler() is the handler of setUp (set by reflection, Thread is unmockable).
    private void armNudges(RestoreJob nudged) {
        Deencapsulation.setField(backupHandler, "isInit", true);
        new Expectations() {
            {
                env.isReady();
                minTimes = 0;
                result = true;

                env.isMaster();
                minTimes = 0;
                result = true;
            }
        };
        Deencapsulation.invoke(backupHandler, "addBackupOrRestoreJob", db.getId(), nudged);
        backupHandler.nudgedJobIds.clear();
    }

    // waits until everything the nudge thread has been given so far has run
    private void drainNudges() throws Exception {
        ExecutorService executor = Deencapsulation.getField(backupHandler, "nudgeExecutor");
        executor.submit(() -> { }).get(10, TimeUnit.SECONDS);
    }

    // RestoreJob.finishTabletDownloadTask
    @Test
    public void testNudgeOnTheReportOfTheLastDownloadTask() throws Exception {
        Config.backup_restore_run_on_task_finish = true;
        job = newJob();
        List<DownloadTask> tasks = downloading(job, 2);
        armNudges(job);

        // the other task is still outstanding: no nudge
        Assert.assertTrue(job.finishTabletDownloadTask(tasks.get(0), downloadReport()));
        drainNudges();
        Assert.assertTrue(backupHandler.nudgedJobIds.toString(), backupHandler.nudgedJobIds.isEmpty());
        Assert.assertEquals(RestoreJobState.DOWNLOADING, job.getState());

        // the last task: nudged once, and the nudge runs the job, which moves on without waiting for its round
        Assert.assertTrue(job.finishTabletDownloadTask(tasks.get(1), downloadReport()));
        drainNudges();
        Assert.assertEquals(Lists.newArrayList(job.getJobId()), backupHandler.nudgedJobIds);
        Assert.assertEquals(RestoreJobState.COMMIT, job.getState());

        // a duplicate report: not accepted, no nudge
        Assert.assertFalse(job.finishTabletDownloadTask(tasks.get(1), downloadReport()));
        drainNudges();
        Assert.assertEquals(1, backupHandler.nudgedJobIds.size());
    }

    @Test
    public void testNoNudgedRunOnTheDownloadReportWhenTheSwitchIsOff() throws Exception {
        Config.backup_restore_run_on_task_finish = false;
        job = newJob();
        List<DownloadTask> tasks = downloading(job, 2);
        armNudges(job);
        Assert.assertTrue(job.finishTabletDownloadTask(tasks.get(0), downloadReport()));
        Assert.assertTrue(job.finishTabletDownloadTask(tasks.get(1), downloadReport()));
        drainNudges();
        // the last report asks for the nudge, the handler does not run the job: it waits for its next round
        Assert.assertEquals(Lists.newArrayList(job.getJobId()), backupHandler.nudgedJobIds);
        Assert.assertEquals(RestoreJobState.DOWNLOADING, job.getState());
        job.run();
        Assert.assertEquals(RestoreJobState.COMMIT, job.getState());
    }

    // RestoreJob.finishDirMoveTask
    @Test
    public void testNudgeOnTheReportOfTheLastDirMoveTask() throws Exception {
        Config.backup_restore_run_on_task_finish = true;
        job = newJob();
        Deencapsulation.setField(job, "state", RestoreJobState.COMMITTING);
        Map<Long, Long> unfinished = Deencapsulation.getField(job, "unfinishedSignatureToId");
        List<DirMoveTask> moves = Lists.newArrayList();
        for (int i = 0; i < 2; i++) {
            long tabletId = 8001 + i;
            DirMoveTask move = new DirMoveTask(null, CatalogMocker.BACKEND1_ID, 7101 + i, job.getJobId(), db.getId(),
                    CatalogMocker.TEST_TBL2_ID, 1, 1, tabletId, "/path/to/snapshot", 0, true);
            unfinished.put(move.getSignature(), tabletId);
            moves.add(move);
        }
        armNudges(job);

        Assert.assertTrue(job.finishDirMoveTask(moves.get(0), ok()));
        drainNudges();
        Assert.assertTrue(backupHandler.nudgedJobIds.toString(), backupHandler.nudgedJobIds.isEmpty());
        Assert.assertEquals(RestoreJobState.COMMITTING, job.getState());

        Assert.assertTrue(job.finishDirMoveTask(moves.get(1), ok()));
        drainNudges();
        Assert.assertEquals(Lists.newArrayList(job.getJobId()), backupHandler.nudgedJobIds);
        Assert.assertEquals(RestoreJobState.FINISHED, job.getState());

        Assert.assertFalse(job.finishDirMoveTask(moves.get(1), ok()));
        drainNudges();
        Assert.assertEquals(1, backupHandler.nudgedJobIds.size());
    }

    // RestoreJob.finishTabletSnapshotTask: the site asks for the nudge on the last report only (the switch is off,
    // the job is not run by it)
    @Test
    public void testNudgeOnTheReportOfTheLastSnapshotTask() throws Exception {
        job = newJob();
        Deencapsulation.setField(job, "state", RestoreJobState.SNAPSHOTING);
        Map<Long, Long> unfinished = Deencapsulation.getField(job, "unfinishedSignatureToId");
        List<SnapshotTask> tasks = Lists.newArrayList();
        for (int i = 0; i < 2; i++) {
            long tabletId = 8001 + i;
            SnapshotTask task = new SnapshotTask(null, CatalogMocker.BACKEND1_ID, 7201 + i, job.getJobId(),
                    db.getId(), CatalogMocker.TEST_TBL2_ID, 1, 1, tabletId, 1, 0, 1000, true);
            unfinished.put(task.getSignature(), tabletId);
            tasks.add(task);
        }
        armNudges(job);

        TFinishTaskRequest report = ok();
        report.setSnapshotPath("/path/to/snapshot");
        Assert.assertTrue(job.finishTabletSnapshotTask(tasks.get(0), report));
        drainNudges();
        Assert.assertTrue(backupHandler.nudgedJobIds.toString(), backupHandler.nudgedJobIds.isEmpty());

        Assert.assertTrue(job.finishTabletSnapshotTask(tasks.get(1), report));
        drainNudges();
        Assert.assertEquals(Lists.newArrayList(job.getJobId()), backupHandler.nudgedJobIds);
        Assert.assertEquals(RestoreJobState.SNAPSHOTING, job.getState());

        Assert.assertFalse(job.finishTabletSnapshotTask(tasks.get(1), report));
        drainNudges();
        Assert.assertEquals(1, backupHandler.nudgedJobIds.size());
    }
}
