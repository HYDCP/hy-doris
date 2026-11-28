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

import org.apache.doris.analysis.BackupStmt;
import org.apache.doris.analysis.ShowBackupStmt;
import org.apache.doris.analysis.StorageBackend;
import org.apache.doris.analysis.TableName;
import org.apache.doris.analysis.TableRef;
import org.apache.doris.backup.BackupJob.BackupJobState;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.DynamicPartitionProperty;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.FsBroker;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.MaterializedIndex.IndexExtState;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.TableProperty;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.GZIPUtils;
import org.apache.doris.common.UserException;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.common.util.UnitTestUtil;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.datasource.property.storage.BrokerProperties;
import org.apache.doris.fs.FileSystemFactory;
import org.apache.doris.persist.EditLog;
import org.apache.doris.task.AgentBatchTask;
import org.apache.doris.task.AgentTask;
import org.apache.doris.task.AgentTaskExecutor;
import org.apache.doris.task.AgentTaskQueue;
import org.apache.doris.task.SnapshotTask;
import org.apache.doris.task.UploadTask;
import org.apache.doris.thrift.TBackend;
import org.apache.doris.thrift.TFinishTaskRequest;
import org.apache.doris.thrift.TLogicalDigest;
import org.apache.doris.thrift.TStatus;
import org.apache.doris.thrift.TStatusCode;
import org.apache.doris.thrift.TTaskType;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import mockit.Delegate;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Field;
import java.nio.file.FileVisitOption;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Deque;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

public class BackupJobTest {

    private BackupJob job;
    private Database db;
    private OlapTable table2;

    private long dbId = 1;
    private long tblId = 2;
    private long partId = 3;
    private long idxId = 4;
    private long tabletId = 5;
    private long backendId = 10000;
    private long version = 6;

    private long tblId2 = 3;
    private long partId2 = 4;
    private long idxId2 = 5;
    private long tabletId2 = 6;
    private String table2Name = "testTable2";

    private long repoId = 20000;
    private AtomicLong id = new AtomicLong(50000);

    @Mocked
    private Env env;
    @Mocked
    private InternalCatalog catalog;

    private MockBackupHandler backupHandler;

    private MockRepositoryMgr repoMgr;

    public BackupJobTest() throws UserException {
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

    private Repository repo = new Repository(repoId, "repo", false, "my_repo",
            FileSystemFactory.get(BrokerProperties.of("broker", Maps.newHashMap())));

    @BeforeClass
    public static void start() {
        Config.tmp_dir = "./";
        File backupDir = new File(BackupHandler.BACKUP_ROOT_DIR.toString());
        backupDir.mkdirs();
    }

    @AfterClass
    public static void end() throws IOException {
        Config.tmp_dir = "./";
        File backupDir = new File(BackupHandler.BACKUP_ROOT_DIR.toString());
        if (backupDir.exists()) {
            Files.walk(BackupHandler.BACKUP_ROOT_DIR,
                       FileVisitOption.FOLLOW_LINKS).sorted(Comparator.reverseOrder()).map(Path::toFile).forEach(File::delete);
        }
    }

    // the state of the job at every edit log written
    private final List<BackupJobState> loggedStates = new ArrayList<>();
    // what Repository.upload returns
    private Status uploadStatus = Status.OK;
    private boolean origMergeIdleTicks;
    private boolean origRunOnTaskFinish;
    private boolean origBackupManifest;

    @Before
    public void setUp() {
        // the tests step the job one state per run(); the merged rounds are tested with the switch set explicitly
        origMergeIdleTicks = Config.backup_restore_merge_idle_ticks;
        origRunOnTaskFinish = Config.backup_restore_run_on_task_finish;
        Config.backup_restore_merge_idle_ticks = false;
        Config.backup_restore_run_on_task_finish = false;
        // the manifest is off by default in branch-3.1, the tests of the backup job look at the tasks with it
        origBackupManifest = Config.enable_backup_manifest;
        Config.enable_backup_manifest = true;
        loggedStates.clear();
        uploadStatus = Status.OK;
        // the task queue is static, other cases leave their tasks in it
        AgentTaskQueue.clearAllTasks();

        repoMgr = new MockRepositoryMgr();
        backupHandler = new MockBackupHandler(env);

        // Thread is unmockable after Jmockit version 1.48, so use reflection to set field instead.
        Deencapsulation.setField(env, "backupHandler", backupHandler);

        db = UnitTestUtil.createDb(dbId, tblId, partId, idxId, tabletId, backendId, version);

        // Create second table in setUp to avoid Env initialization issues
        table2 = UnitTestUtil.createTable(db, tblId2, table2Name, partId2, idxId2, tabletId2, backendId, version);

        catalog = Deencapsulation.newInstance(InternalCatalog.class);
        new Expectations(env) {
            {
                env.getInternalCatalog();
                minTimes = 0;
                result = catalog;

                catalog.getDbNullable(anyLong);
                minTimes = 0;
                result = db;

                catalog.getTableByTableId(anyLong);
                minTimes = 0;
                result = new Delegate<Table>() {
                    public Table getTableByTableId(Long tableId) {
                        // Check if table exists in the database
                        return db.getTableNullable(tableId);
                    }
                };

                Env.getCurrentEnv();
                minTimes = 0;
                result = env;

                Env.getCurrentEnvJournalVersion();
                minTimes = 0;
                result = FeConstants.meta_version;

                env.getNextId();
                minTimes = 0;
                result = new Delegate<Long>() {
                    public Long getNextId() {
                        return id.getAndIncrement();
                    }
                };

                env.getEditLog();
                minTimes = 0;
                result = editLog;
            }
        };

        new Expectations() {
            {
                editLog.logBackupJob((BackupJob) any);
                minTimes = 0;
                result = new Delegate() {
                    public void logBackupJob(BackupJob job) {
                        loggedStates.add(job.getState());
                        System.out.println("log backup job: " + job);
                    }
                };
            }
        };

        new MockUp<AgentTaskExecutor>() {
            @Mock
            public void submit(AgentBatchTask task) {

            }
        };

        new MockUp<Repository>() {
            @Mock
            Status upload(String localFilePath, String remoteFilePath) {
                return uploadStatus;
            }

            @Mock
            Status getBrokerAddress(Long beId, Env env, List<FsBroker> brokerAddrs) {
                brokerAddrs.add(new FsBroker());
                return Status.OK;
            }
        };

        // Only include first table to ensure other tests are not affected
        List<TableRef> tableRefs = Lists.newArrayList();
        tableRefs.add(new TableRef(
                new TableName(InternalCatalog.INTERNAL_CATALOG_NAME, UnitTestUtil.DB_NAME, UnitTestUtil.TABLE_NAME),
                null));
        job = new BackupJob("label", dbId, UnitTestUtil.DB_NAME, tableRefs, 13600 * 1000, BackupStmt.BackupContent.ALL,
                env, repo.getId(), 0);
    }

    @After
    public void tearDown() {
        Config.backup_restore_merge_idle_ticks = origMergeIdleTicks;
        Config.backup_restore_run_on_task_finish = origRunOnTaskFinish;
        Config.enable_backup_manifest = origBackupManifest;
        AgentTaskQueue.clearAllTasks();
    }

    /**
     * Test normal backup job execution flow
     *
     * Scenario: Backup a single table with all content
     * Expected Results:
     * 1. Job should progress through all states: PENDING -> SNAPSHOTING -> UPLOAD_SNAPSHOT -> UPLOADING -> SAVE_META -> UPLOAD_INFO -> FINISHED
     * 2. Backup meta should contain the correct table information
     * 3. Snapshot and upload tasks should be created and executed successfully
     * 4. Meta files should be saved and uploaded correctly
     * 5. Job should complete successfully with OK status
     */
    @Test
    public void testRunNormal() {
        // 1. pending
        Assert.assertEquals(BackupJobState.PENDING, job.getState());
        job.run();
        Assert.assertEquals(Status.OK, job.getStatus());
        Assert.assertEquals(BackupJobState.SNAPSHOTING, job.getState());

        BackupMeta backupMeta = job.getBackupMeta();
        Assert.assertEquals(1, backupMeta.getTables().size());
        OlapTable backupTbl = (OlapTable) backupMeta.getTable(UnitTestUtil.TABLE_NAME);
        List<String> partNames = Lists.newArrayList(backupTbl.getPartitionNames());
        Assert.assertNotNull(backupTbl);
        Assert.assertEquals(backupTbl.getSignature(BackupHandler.SIGNATURE_VERSION, partNames),
                            ((OlapTable) db.getTableNullable(tblId)).getSignature(BackupHandler.SIGNATURE_VERSION, partNames));
        Assert.assertEquals(1, AgentTaskQueue.getTaskNum());
        AgentTask task = AgentTaskQueue.getTask(backendId, TTaskType.MAKE_SNAPSHOT, id.get() - 1);
        Assert.assertTrue(task instanceof SnapshotTask);
        SnapshotTask snapshotTask = (SnapshotTask) task;

        // 2. snapshoting
        job.run();
        Assert.assertEquals(Status.OK, job.getStatus());
        Assert.assertEquals(BackupJobState.SNAPSHOTING, job.getState());

        // 3. snapshot finished
        String snapshotPath = "/path/to/snapshot";
        List<String> snapshotFiles = Lists.newArrayList();
        snapshotFiles.add("1.dat");
        snapshotFiles.add("1.idx");
        snapshotFiles.add("1.hdr");
        TStatus taskStatus = new TStatus(TStatusCode.OK);
        TBackend tBackend = new TBackend("", 0, 1);
        TFinishTaskRequest request = new TFinishTaskRequest(tBackend, TTaskType.MAKE_SNAPSHOT,
                snapshotTask.getSignature(), taskStatus);
        request.setSnapshotFiles(snapshotFiles);
        request.setSnapshotPath(snapshotPath);
        Assert.assertTrue(job.finishTabletSnapshotTask(snapshotTask, request));
        job.run();
        Assert.assertEquals(Status.OK, job.getStatus());
        Assert.assertEquals(BackupJobState.UPLOAD_SNAPSHOT, job.getState());

        // 4. upload snapshots
        AgentTaskQueue.clearAllTasks();
        job.run();
        Assert.assertEquals(Status.OK, job.getStatus());
        Assert.assertEquals(BackupJobState.UPLOADING, job.getState());
        Assert.assertEquals(1, AgentTaskQueue.getTaskNum());
        task = AgentTaskQueue.getTask(backendId, TTaskType.UPLOAD, id.get() - 1);
        Assert.assertTrue(task instanceof UploadTask);
        UploadTask upTask = (UploadTask) task;

        Assert.assertEquals(job.getJobId(), upTask.getJobId());
        Map<String, String> srcToDest = upTask.getSrcToDestPath();
        Assert.assertEquals(1, srcToDest.size());
        String dest = srcToDest.get(snapshotPath + "/" + tabletId + "/" + 0);
        Assert.assertNotNull(dest);

        // 5. uploading
        job.run();
        Assert.assertEquals(Status.OK, job.getStatus());
        Assert.assertEquals(BackupJobState.UPLOADING, job.getState());
        Map<Long, List<String>> tabletFileMap = Maps.newHashMap();
        request = new TFinishTaskRequest(tBackend, TTaskType.UPLOAD,
                upTask.getSignature(), taskStatus);
        request.setTabletFiles(tabletFileMap);

        Assert.assertFalse(job.finishSnapshotUploadTask(upTask, request));
        List<String> tabletFiles = Lists.newArrayList();
        tabletFileMap.put(tabletId, tabletFiles);
        Assert.assertFalse(job.finishSnapshotUploadTask(upTask, request));
        tabletFiles.add("1.dat.4f158689243a3d6030352fec3cfd3798");
        tabletFiles.add("wrong_files.idx.4f158689243a3d6030352fec3cfd3798");
        tabletFiles.add("wrong_files.hdr.4f158689243a3d6030352fec3cfd3798");
        Assert.assertFalse(job.finishSnapshotUploadTask(upTask, request));
        tabletFiles.clear();
        tabletFiles.add("1.dat.4f158689243a3d6030352fec3cfd3798");
        tabletFiles.add("1.idx.4f158689243a3d6030352fec3cfd3798");
        tabletFiles.add("1.hdr.4f158689243a3d6030352fec3cfd3798");
        Assert.assertTrue(job.finishSnapshotUploadTask(upTask, request));
        job.run();
        Assert.assertEquals(Status.OK, job.getStatus());
        Assert.assertEquals(BackupJobState.SAVE_META, job.getState());

        // 6. save meta
        job.run();
        Assert.assertEquals(Status.OK, job.getStatus());
        Assert.assertEquals(BackupJobState.UPLOAD_INFO, job.getState());
        File metaInfo = new File(job.getLocalMetaInfoFilePath());
        Assert.assertTrue(metaInfo.exists());
        File jobInfo = new File(job.getLocalJobInfoFilePath());
        Assert.assertTrue(jobInfo.exists());
        File localJobDir = jobInfo.getParentFile();

        BackupMeta restoreMetaInfo = null;
        BackupJobInfo restoreJobInfo = null;
        try {
            restoreMetaInfo = BackupMeta.fromFile(job.getLocalMetaInfoFilePath(), FeConstants.meta_version);
            Assert.assertEquals(1, restoreMetaInfo.getTables().size());
            OlapTable olapTable = (OlapTable) restoreMetaInfo.getTable(tblId);
            Assert.assertNotNull(olapTable);
            Assert.assertNotNull(restoreMetaInfo.getTable(UnitTestUtil.TABLE_NAME));
            List<String> names = Lists.newArrayList(olapTable.getPartitionNames());
            Assert.assertEquals(((OlapTable) db.getTableNullable(tblId)).getSignature(BackupHandler.SIGNATURE_VERSION, names),
                                olapTable.getSignature(BackupHandler.SIGNATURE_VERSION, names));

            restoreJobInfo = BackupJobInfo.fromFile(job.getLocalJobInfoFilePath());
            Assert.assertEquals(UnitTestUtil.DB_NAME, restoreJobInfo.dbName);
            Assert.assertEquals(job.getLabel(), restoreJobInfo.name);
            Assert.assertEquals(1, restoreJobInfo.backupOlapTableObjects.values().size());
        } catch (IOException e) {
            e.printStackTrace();
            Assert.fail();
        }

        Assert.assertNull(job.getBackupMeta());
        Assert.assertNull(job.getJobInfo());

        // 7. upload_info
        job.run();
        Assert.assertEquals(Status.OK, job.getStatus());
        Assert.assertEquals(BackupJobState.FINISHED, job.getState());
        Assert.assertTrue(localJobDir.exists());

        // BackupHandler invokes cleanup after job.run() in the same daemon cycle.
        job.cleanupLocalJobDirIfNecessary(job.getCreateTime());
        Assert.assertFalse(localJobDir.exists());
    }

    @Test
    public void testBackupCopyTableWithDirtyDynamicPartitionStorageMedium() {
        Map<String, String> dirtyProperties = Maps.newHashMap();
        dirtyProperties.put(DynamicPartitionProperty.STORAGE_MEDIUM, "hdd");
        table2.setTableProperty(new TableProperty(dirtyProperties));

        Assert.assertFalse(table2.dynamicPartitionExists());
        OlapTable copied = table2.selectiveCopy(null, IndexExtState.VISIBLE, true);
        Assert.assertNotNull(copied);
        Assert.assertFalse(copied.dynamicPartitionExists());
        Assert.assertTrue(copied.getTableProperty().hasInvalidDynamicPartition());
    }

    @Test
    public void testBackupCopyTableWithDirtyDynamicPartitionStoragePolicy() {
        Map<String, String> dirtyProperties = Maps.newHashMap();
        dirtyProperties.put(DynamicPartitionProperty.STORAGE_POLICY, "test_policy");
        table2.setTableProperty(new TableProperty(dirtyProperties));

        Assert.assertFalse(table2.dynamicPartitionExists());
        OlapTable copied = table2.selectiveCopy(null, IndexExtState.VISIBLE, true);
        Assert.assertNotNull(copied);
        Assert.assertFalse(copied.dynamicPartitionExists());
        Assert.assertTrue(copied.getTableProperty().hasInvalidDynamicPartition());
    }

    @Test
    public void testCleanupFinishedRemoteBackupJobDirImmediately() throws IOException {
        File localJobDir = createLocalJobDir("remote_cleanup");
        File metaInfo = new File(localJobDir, Repository.FILE_META_INFO);
        File jobInfo = new File(localJobDir, Repository.PREFIX_JOB_INFO + "2026-07-09-10-00-00");
        Assert.assertTrue(metaInfo.createNewFile());
        Assert.assertTrue(jobInfo.createNewFile());

        Deencapsulation.setField(job, "state", BackupJobState.FINISHED);
        long createTime = System.currentTimeMillis();
        Deencapsulation.setField(job, "createTime", createTime);
        Deencapsulation.setField(job, "timeoutMs", 1000L);
        Deencapsulation.setField(job, "localJobDirPath", null);
        Deencapsulation.setField(job, "localMetaInfoFilePath", metaInfo.getAbsolutePath());
        Deencapsulation.setField(job, "localJobInfoFilePath", jobInfo.getAbsolutePath());

        job.cleanupLocalJobDirIfNecessary(createTime);
        Assert.assertFalse(localJobDir.exists());
    }

    @Test
    public void testCleanupLocalSnapshotJobDirOnlyAfterExpired() throws IOException {
        BackupJob localSnapshotJob = new BackupJob("local_label", dbId, UnitTestUtil.DB_NAME,
                Lists.newArrayList(), 1000, BackupStmt.BackupContent.ALL,
                env, Repository.KEEP_ON_LOCAL_REPO_ID, 0);
        File localJobDir = createLocalJobDir("local_snapshot_cleanup");
        File metaInfo = new File(localJobDir, Repository.FILE_META_INFO);
        File jobInfo = new File(localJobDir, Repository.PREFIX_JOB_INFO + "2026-07-09-11-00-00");
        Assert.assertTrue(metaInfo.createNewFile());
        Assert.assertTrue(jobInfo.createNewFile());

        long createTime = System.currentTimeMillis();
        Deencapsulation.setField(localSnapshotJob, "state", BackupJobState.FINISHED);
        Deencapsulation.setField(localSnapshotJob, "createTime", createTime);
        Deencapsulation.setField(localSnapshotJob, "localJobDirPath", null);
        Deencapsulation.setField(localSnapshotJob, "localMetaInfoFilePath", metaInfo.getAbsolutePath());
        Deencapsulation.setField(localSnapshotJob, "localJobInfoFilePath", jobInfo.getAbsolutePath());

        localSnapshotJob.cleanupLocalJobDirIfNecessary(createTime + 999);
        Assert.assertTrue(localJobDir.exists());

        localSnapshotJob.cleanupLocalJobDirIfNecessary(createTime + 1000);
        Assert.assertFalse(localJobDir.exists());
    }

    @Test
    public void testLocalSnapshotDataRemainsReadableAfterJobDirCleanup() throws IOException {
        BackupJob localSnapshotJob = new BackupJob("local_label", dbId, UnitTestUtil.DB_NAME,
                Lists.newArrayList(), 1000, BackupStmt.BackupContent.ALL,
                env, Repository.KEEP_ON_LOCAL_REPO_ID, 0);
        File localJobDir = createLocalJobDir("local_snapshot_read_cleanup");
        File metaInfo = new File(localJobDir, Repository.FILE_META_INFO);
        File jobInfo = new File(localJobDir, Repository.PREFIX_JOB_INFO + "2026-07-09-12-00-00");
        byte[] metaBytes = new byte[] {1, 2, 3};
        byte[] jobInfoBytes = new byte[] {4, 5, 6};
        Files.write(metaInfo.toPath(), metaBytes);
        Files.write(jobInfo.toPath(), jobInfoBytes);

        long createTime = System.currentTimeMillis();
        Deencapsulation.setField(localSnapshotJob, "state", BackupJobState.FINISHED);
        Deencapsulation.setField(localSnapshotJob, "createTime", createTime);
        Deencapsulation.setField(localSnapshotJob, "localJobDirPath", null);
        Deencapsulation.setField(localSnapshotJob, "localMetaInfoFilePath", metaInfo.getAbsolutePath());
        Deencapsulation.setField(localSnapshotJob, "localJobInfoFilePath", jobInfo.getAbsolutePath());

        Snapshot snapshot = localSnapshotJob.getSnapshot(false);
        localSnapshotJob.cleanupLocalJobDirAfterRemoved();

        Assert.assertFalse(localJobDir.exists());
        Assert.assertFalse(snapshot.isCompressed());
        Assert.assertEquals(metaBytes.length, snapshot.getMetaSize());
        Assert.assertEquals(jobInfoBytes.length, snapshot.getJobInfoSize());
        Assert.assertArrayEquals(metaBytes, snapshot.getMeta());
        Assert.assertArrayEquals(jobInfoBytes, snapshot.getJobInfo());
    }

    /**
     * Oversized local snapshot must not be fully read into FE heap when compression is off.
     * Sparse files provide a logical size &gt;= Integer.MAX_VALUE without allocating disk/RAM.
     */
    @Test
    public void testLocalSnapshotOversizedWithoutCompressDoesNotReadFiles() throws IOException {
        BackupJob localSnapshotJob = new BackupJob("local_label", dbId, UnitTestUtil.DB_NAME,
                Lists.newArrayList(), 3600 * 1000, BackupStmt.BackupContent.ALL,
                env, Repository.KEEP_ON_LOCAL_REPO_ID, 0);
        File localJobDir = createLocalJobDir("local_snapshot_oversized");
        File metaInfo = new File(localJobDir, Repository.FILE_META_INFO);
        File jobInfo = new File(localJobDir, Repository.PREFIX_JOB_INFO + "2026-07-09-14-00-00");

        long metaLogicalSize = (long) Integer.MAX_VALUE - 100L;
        long jobInfoLogicalSize = 100L;
        createSparseFile(metaInfo, metaLogicalSize);
        createSparseFile(jobInfo, jobInfoLogicalSize);
        Assert.assertEquals(metaLogicalSize, metaInfo.length());
        Assert.assertEquals(jobInfoLogicalSize, jobInfo.length());

        long createTime = System.currentTimeMillis();
        Deencapsulation.setField(localSnapshotJob, "state", BackupJobState.FINISHED);
        Deencapsulation.setField(localSnapshotJob, "createTime", createTime);
        Deencapsulation.setField(localSnapshotJob, "localJobDirPath", null);
        Deencapsulation.setField(localSnapshotJob, "localMetaInfoFilePath", metaInfo.getAbsolutePath());
        Deencapsulation.setField(localSnapshotJob, "localJobInfoFilePath", jobInfo.getAbsolutePath());

        Snapshot snapshot = localSnapshotJob.getSnapshot(false);

        Assert.assertNotNull(snapshot);
        Assert.assertFalse(snapshot.isExpired());
        Assert.assertFalse(snapshot.isCompressed());
        Assert.assertEquals(metaLogicalSize, snapshot.getMetaSize());
        Assert.assertEquals(jobInfoLogicalSize, snapshot.getJobInfoSize());
        Assert.assertTrue(snapshot.getMetaSize() + snapshot.getJobInfoSize() >= Integer.MAX_VALUE);
        // Content must not be materialized for oversized uncompressed responses.
        Assert.assertNull(snapshot.getMeta());
        Assert.assertNull(snapshot.getJobInfo());
        // Job dir must still exist; size guard rejected before any cleanup side-effect.
        Assert.assertTrue(localJobDir.exists());
    }

    @Test
    public void testLocalSnapshotCompressStreamsFilesAndSurvivesCleanup() throws IOException {
        BackupJob localSnapshotJob = new BackupJob("local_label", dbId, UnitTestUtil.DB_NAME,
                Lists.newArrayList(), 3600 * 1000, BackupStmt.BackupContent.ALL,
                env, Repository.KEEP_ON_LOCAL_REPO_ID, 0);
        File localJobDir = createLocalJobDir("local_snapshot_compress");
        File metaInfo = new File(localJobDir, Repository.FILE_META_INFO);
        File jobInfo = new File(localJobDir, Repository.PREFIX_JOB_INFO + "2026-07-09-15-00-00");
        // Slightly compressible payload; enough bytes to exercise streaming path.
        byte[] metaBytes = new byte[64 * 1024];
        byte[] jobInfoBytes = new byte[32 * 1024];
        for (int i = 0; i < metaBytes.length; i++) {
            metaBytes[i] = (byte) (i % 7);
        }
        for (int i = 0; i < jobInfoBytes.length; i++) {
            jobInfoBytes[i] = (byte) (i % 11);
        }
        Files.write(metaInfo.toPath(), metaBytes);
        Files.write(jobInfo.toPath(), jobInfoBytes);

        long createTime = System.currentTimeMillis();
        Deencapsulation.setField(localSnapshotJob, "state", BackupJobState.FINISHED);
        Deencapsulation.setField(localSnapshotJob, "createTime", createTime);
        Deencapsulation.setField(localSnapshotJob, "localJobDirPath", null);
        Deencapsulation.setField(localSnapshotJob, "localMetaInfoFilePath", metaInfo.getAbsolutePath());
        Deencapsulation.setField(localSnapshotJob, "localJobInfoFilePath", jobInfo.getAbsolutePath());

        Snapshot snapshot = localSnapshotJob.getSnapshot(true);
        Assert.assertNotNull(snapshot);
        Assert.assertTrue(snapshot.isCompressed());
        Assert.assertEquals(metaBytes.length, snapshot.getMetaSize());
        Assert.assertEquals(jobInfoBytes.length, snapshot.getJobInfoSize());
        Assert.assertTrue(GZIPUtils.isGZIPCompressed(snapshot.getMeta()));
        Assert.assertTrue(GZIPUtils.isGZIPCompressed(snapshot.getJobInfo()));
        Assert.assertArrayEquals(metaBytes, GZIPUtils.decompress(snapshot.getMeta()));
        Assert.assertArrayEquals(jobInfoBytes, GZIPUtils.decompress(snapshot.getJobInfo()));

        localSnapshotJob.cleanupLocalJobDirAfterRemoved();
        Assert.assertFalse(localJobDir.exists());
        // Materialized compressed payload remains usable after job dir is deleted.
        Assert.assertArrayEquals(metaBytes, GZIPUtils.decompress(snapshot.getMeta()));
        Assert.assertArrayEquals(jobInfoBytes, GZIPUtils.decompress(snapshot.getJobInfo()));
    }

    /**
     * Active getSnapshot readers pin the staging dir via refcount; cleanup is deferred
     * to a later handler cleanup cycle after the last reader releases, so neither
     * mid-read deletion nor directory IO on the RPC thread can occur.
     */
    @Test
    public void testLocalSnapshotCleanupDefersWhileGetSnapshotReaderPinned() throws Exception {
        BackupJob localSnapshotJob = new BackupJob("local_label", dbId, UnitTestUtil.DB_NAME,
                Lists.newArrayList(), 3600 * 1000, BackupStmt.BackupContent.ALL,
                env, Repository.KEEP_ON_LOCAL_REPO_ID, 0);
        File localJobDir = createLocalJobDir("local_snapshot_lock");
        File metaInfo = new File(localJobDir, Repository.FILE_META_INFO);
        File jobInfo = new File(localJobDir, Repository.PREFIX_JOB_INFO + "2026-07-09-16-00-00");
        byte[] metaBytes = new byte[] {10, 20, 30, 40};
        byte[] jobInfoBytes = new byte[] {50, 60, 70};
        Files.write(metaInfo.toPath(), metaBytes);
        Files.write(jobInfo.toPath(), jobInfoBytes);

        long createTime = System.currentTimeMillis();
        Deencapsulation.setField(localSnapshotJob, "state", BackupJobState.FINISHED);
        Deencapsulation.setField(localSnapshotJob, "createTime", createTime);
        Deencapsulation.setField(localSnapshotJob, "localJobDirPath", null);
        Deencapsulation.setField(localSnapshotJob, "localMetaInfoFilePath", metaInfo.getAbsolutePath());
        Deencapsulation.setField(localSnapshotJob, "localJobInfoFilePath", jobInfo.getAbsolutePath());

        // Simulate an in-flight getSnapshot reader pin.
        Deencapsulation.setField(localSnapshotJob, "localJobDirReaders", 1);
        localSnapshotJob.cleanupLocalJobDirAfterRemoved();
        Assert.assertTrue("cleanup must defer while a reader is pinned", localJobDir.exists());

        // Release the pin (readers 1 -> 0); the RPC reader must not run directory IO.
        Deencapsulation.invoke(localSnapshotJob, "releaseLocalJobDirRead");
        Assert.assertTrue(localJobDir.exists());
        Assert.assertFalse(Deencapsulation.getField(localSnapshotJob, "localJobDirCleaned"));

        // A later backup handler cleanup cycle performs the deferred deletion.
        Assert.assertTrue(localSnapshotJob.cleanupLocalJobDirAfterRemoved());
        Assert.assertFalse(localJobDir.exists());
        Assert.assertTrue(Deencapsulation.getField(localSnapshotJob, "localJobDirCleaned"));
    }

    @Test
    public void testCleanupRetriesAfterDeleteFailure() throws IOException {
        boolean[] failDelete = {true};
        BackupJob localSnapshotJob = new BackupJob("local_label", dbId, UnitTestUtil.DB_NAME,
                Lists.newArrayList(), 3600 * 1000, BackupStmt.BackupContent.ALL,
                env, Repository.KEEP_ON_LOCAL_REPO_ID, 0) {
            @Override
            void deleteLocalJobDir(Path jobDirPath) throws IOException {
                if (failDelete[0]) {
                    failDelete[0] = false;
                    throw new IOException("injected delete failure");
                }
                super.deleteLocalJobDir(jobDirPath);
            }
        };
        File localJobDir = createLocalJobDir("local_snapshot_delete_retry");
        File metaInfo = new File(localJobDir, Repository.FILE_META_INFO);
        File jobInfo = new File(localJobDir, Repository.PREFIX_JOB_INFO + "2026-07-10-10-00-00");
        Assert.assertTrue(metaInfo.createNewFile());
        Assert.assertTrue(jobInfo.createNewFile());

        Deencapsulation.setField(localSnapshotJob, "state", BackupJobState.CANCELLED);
        long createTime = System.currentTimeMillis();
        Deencapsulation.setField(localSnapshotJob, "createTime", createTime);
        Deencapsulation.setField(localSnapshotJob, "localJobDirPath", null);
        Deencapsulation.setField(localSnapshotJob, "localMetaInfoFilePath", metaInfo.getAbsolutePath());
        Deencapsulation.setField(localSnapshotJob, "localJobInfoFilePath", jobInfo.getAbsolutePath());

        localSnapshotJob.cleanupLocalJobDirIfNecessary(createTime + 3600 * 1000);
        Assert.assertTrue(localJobDir.exists());
        Assert.assertFalse(Deencapsulation.getField(localSnapshotJob, "localJobDirCleaned"));

        localSnapshotJob.cleanupLocalJobDirIfNecessary(createTime + 3600 * 1000);
        Assert.assertFalse(localJobDir.exists());
        Assert.assertTrue(Deencapsulation.getField(localSnapshotJob, "localJobDirCleaned"));
    }

    @Test
    public void testLocalSnapshotIoFailurePropagatesAsIOException() throws IOException {
        BackupJob localSnapshotJob = new BackupJob("local_label", dbId, UnitTestUtil.DB_NAME,
                Lists.newArrayList(), 3600 * 1000, BackupStmt.BackupContent.ALL,
                env, Repository.KEEP_ON_LOCAL_REPO_ID, 0);
        File localJobDir = createLocalJobDir("local_snapshot_io_fail");
        File metaInfo = new File(localJobDir, Repository.FILE_META_INFO);
        File jobInfo = new File(localJobDir, Repository.PREFIX_JOB_INFO + "2026-07-09-17-00-00");
        Assert.assertTrue(metaInfo.createNewFile());
        Assert.assertTrue(jobInfo.createNewFile());

        long createTime = System.currentTimeMillis();
        Deencapsulation.setField(localSnapshotJob, "state", BackupJobState.FINISHED);
        Deencapsulation.setField(localSnapshotJob, "createTime", createTime);
        Deencapsulation.setField(localSnapshotJob, "localJobDirPath", null);
        Deencapsulation.setField(localSnapshotJob, "localMetaInfoFilePath", metaInfo.getAbsolutePath());
        Deencapsulation.setField(localSnapshotJob, "localJobInfoFilePath", jobInfo.getAbsolutePath());

        Assert.assertTrue(metaInfo.delete());
        try {
            localSnapshotJob.getSnapshot(false);
            Assert.fail("expected IOException when meta file is missing");
        } catch (IOException e) {
            // Must not be swallowed as null / SNAPSHOT_NOT_EXIST.
            Assert.assertNotNull(e.getMessage());
        }
        // Failed materialization still releases the pin so later cleanup can proceed.
        localSnapshotJob.cleanupLocalJobDirAfterRemoved();
        Assert.assertFalse(localJobDir.exists());
    }

    @Test
    public void testLocalSnapshotCompressHonorsOutputSizeLimit() throws IOException {
        BackupJob localSnapshotJob = new BackupJob("local_label", dbId, UnitTestUtil.DB_NAME,
                Lists.newArrayList(), 3600 * 1000, BackupStmt.BackupContent.ALL,
                env, Repository.KEEP_ON_LOCAL_REPO_ID, 0);
        File localJobDir = createLocalJobDir("local_snapshot_compress_limit");
        File metaInfo = new File(localJobDir, Repository.FILE_META_INFO);
        File jobInfo = new File(localJobDir, Repository.PREFIX_JOB_INFO + "2026-07-09-18-00-00");
        byte[] lowCompress = new byte[128 * 1024];
        new java.util.Random(1).nextBytes(lowCompress);
        Files.write(metaInfo.toPath(), lowCompress);
        Files.write(jobInfo.toPath(), new byte[] {1, 2, 3});

        long createTime = System.currentTimeMillis();
        Deencapsulation.setField(localSnapshotJob, "state", BackupJobState.FINISHED);
        Deencapsulation.setField(localSnapshotJob, "createTime", createTime);
        Deencapsulation.setField(localSnapshotJob, "localJobDirPath", null);
        Deencapsulation.setField(localSnapshotJob, "localMetaInfoFilePath", metaInfo.getAbsolutePath());
        Deencapsulation.setField(localSnapshotJob, "localJobInfoFilePath", jobInfo.getAbsolutePath());

        // Directly exercise bounded compress used by getSnapshot(true).
        try {
            GZIPUtils.compress(metaInfo, 64);
            Assert.fail("expected compressed size limit to fail on low-compressibility data");
        } catch (IOException e) {
            Assert.assertTrue(e.getMessage().contains("compressed size exceeds limit"));
        }

        // Normal compress path still works with full limit.
        Snapshot snapshot = localSnapshotJob.getSnapshot(true);
        Assert.assertNotNull(snapshot);
        Assert.assertTrue(snapshot.isCompressed());
        Assert.assertArrayEquals(lowCompress, GZIPUtils.decompress(snapshot.getMeta()));
    }

    @Test
    public void testCleanupCancelledLocalSnapshotJobDirFromPersistedFilePaths() throws IOException {
        BackupJob localSnapshotJob = new BackupJob("local_label", dbId, UnitTestUtil.DB_NAME,
                Lists.newArrayList(), 1000, BackupStmt.BackupContent.ALL,
                env, Repository.KEEP_ON_LOCAL_REPO_ID, 0);
        File localJobDir = createLocalJobDir("local_cancel_cleanup");
        File metaInfo = new File(localJobDir, Repository.FILE_META_INFO);
        File jobInfo = new File(localJobDir, Repository.PREFIX_JOB_INFO + "2026-07-09-13-00-00");
        Assert.assertTrue(metaInfo.createNewFile());
        Assert.assertTrue(jobInfo.createNewFile());

        Deencapsulation.setField(localSnapshotJob, "state", BackupJobState.CANCELLED);
        long createTime = System.currentTimeMillis();
        Deencapsulation.setField(localSnapshotJob, "createTime", createTime);
        Deencapsulation.setField(localSnapshotJob, "localJobDirPath", null);
        Deencapsulation.setField(localSnapshotJob, "localMetaInfoFilePath", metaInfo.getAbsolutePath());
        Deencapsulation.setField(localSnapshotJob, "localJobInfoFilePath", jobInfo.getAbsolutePath());

        localSnapshotJob.cleanupLocalJobDirIfNecessary(createTime);
        Assert.assertFalse(localJobDir.exists());
    }

    @Test
    public void testExpiredGetSnapshotDoesNotDeleteJobDir() throws IOException {
        BackupJob localSnapshotJob = new BackupJob("local_label", dbId, UnitTestUtil.DB_NAME,
                Lists.newArrayList(), 1000, BackupStmt.BackupContent.ALL,
                env, Repository.KEEP_ON_LOCAL_REPO_ID, 0);
        File localJobDir = createLocalJobDir("expired_snapshot_rpc");
        File metaInfo = new File(localJobDir, Repository.FILE_META_INFO);
        File jobInfo = new File(localJobDir, Repository.PREFIX_JOB_INFO + "2026-07-15-10-00-00");
        Assert.assertTrue(metaInfo.createNewFile());
        Assert.assertTrue(jobInfo.createNewFile());

        long createTime = System.currentTimeMillis() - 1000;
        Deencapsulation.setField(localSnapshotJob, "state", BackupJobState.FINISHED);
        Deencapsulation.setField(localSnapshotJob, "createTime", createTime);
        Deencapsulation.setField(localSnapshotJob, "localJobDirPath", null);
        Deencapsulation.setField(localSnapshotJob, "localMetaInfoFilePath", metaInfo.getAbsolutePath());
        Deencapsulation.setField(localSnapshotJob, "localJobInfoFilePath", jobInfo.getAbsolutePath());

        Snapshot snapshot = localSnapshotJob.getSnapshot(false);
        Assert.assertTrue(snapshot.isExpired());
        Assert.assertTrue(localJobDir.exists());

        localSnapshotJob.cleanupLocalJobDirIfNecessary(createTime + 1000);
        Assert.assertFalse(localJobDir.exists());
    }

    @Test
    public void testEvictedJobCleanupWaitsForPinnedReader() throws Exception {
        int oldMaxJobNum = Config.max_backup_restore_job_num_per_db;
        Config.max_backup_restore_job_num_per_db = 1;
        try {
            BackupJob evictedJob = new BackupJob("evicted_label", dbId, UnitTestUtil.DB_NAME,
                    Lists.newArrayList(), 1000, BackupStmt.BackupContent.ALL, env, repoId, 0);
            File localJobDir = createLocalJobDir("evicted_snapshot");
            File metaInfo = new File(localJobDir, Repository.FILE_META_INFO);
            File jobInfo = new File(localJobDir, Repository.PREFIX_JOB_INFO + "2026-07-15-11-00-00");
            Assert.assertTrue(metaInfo.createNewFile());
            Assert.assertTrue(jobInfo.createNewFile());
            Deencapsulation.setField(evictedJob, "state", BackupJobState.FINISHED);
            Deencapsulation.setField(evictedJob, "localJobDirPath", null);
            Deencapsulation.setField(evictedJob, "localMetaInfoFilePath", metaInfo.getAbsolutePath());
            Deencapsulation.setField(evictedJob, "localJobInfoFilePath", jobInfo.getAbsolutePath());
            Deencapsulation.setField(evictedJob, "localJobDirReaders", 1);

            BackupJob replacementJob = new BackupJob("replacement_label", dbId, UnitTestUtil.DB_NAME,
                    Lists.newArrayList(), 1000, BackupStmt.BackupContent.ALL, env, repoId, 0);
            Deencapsulation.invoke(backupHandler, "addBackupOrRestoreJob", dbId, evictedJob);
            Deencapsulation.invoke(backupHandler, "addBackupOrRestoreJob", dbId, replacementJob);

            Deque<BackupJob> pendingCleanupJobs = Deencapsulation.getField(
                    backupHandler, "pendingCleanupJobs");
            Assert.assertEquals(1, pendingCleanupJobs.size());
            Deencapsulation.invoke(backupHandler, "cleanupPendingBackupJobLocalJobDirs");
            Assert.assertTrue(localJobDir.exists());
            Assert.assertEquals(1, pendingCleanupJobs.size());

            Deencapsulation.invoke(evictedJob, "releaseLocalJobDirRead");
            Assert.assertTrue(localJobDir.exists());
            Deencapsulation.invoke(backupHandler, "cleanupPendingBackupJobLocalJobDirs");
            Assert.assertFalse(localJobDir.exists());
            Assert.assertTrue(pendingCleanupJobs.isEmpty());
        } finally {
            Config.max_backup_restore_job_num_per_db = oldMaxJobNum;
        }
    }

    @Test
    public void testCheckpointReplayAndEvictionDoNotTouchJobDirs() throws Exception {
        Field checkpointThreadIdField = Env.class.getDeclaredField("checkpointThreadId");
        checkpointThreadIdField.setAccessible(true);
        long oldCheckpointThreadId = checkpointThreadIdField.getLong(null);
        int oldMaxJobNum = Config.max_backup_restore_job_num_per_db;
        Config.max_backup_restore_job_num_per_db = 1;
        checkpointThreadIdField.setLong(null, Thread.currentThread().getId());
        try {
            Deencapsulation.setField(job, "state", BackupJobState.SAVE_META);
            job.replayRun();
            Assert.assertNull(job.getLocalMetaInfoFilePath());
            Assert.assertNull(job.getLocalJobInfoFilePath());

            BackupJob finishedJob = new BackupJob("checkpoint_finished", dbId, UnitTestUtil.DB_NAME,
                    Lists.newArrayList(), 1000, BackupStmt.BackupContent.ALL, env, repoId, 0);
            File localJobDir = createLocalJobDir("checkpoint_eviction");
            Deencapsulation.setField(finishedJob, "state", BackupJobState.FINISHED);
            Deencapsulation.setField(finishedJob, "localJobDirPath", localJobDir.toPath());
            BackupJob replacementJob = new BackupJob("checkpoint_replacement", dbId, UnitTestUtil.DB_NAME,
                    Lists.newArrayList(), 1000, BackupStmt.BackupContent.ALL, env, repoId, 0);

            Deencapsulation.invoke(backupHandler, "addBackupOrRestoreJob", dbId, finishedJob);
            Deencapsulation.invoke(backupHandler, "addBackupOrRestoreJob", dbId, replacementJob);

            Deque<BackupJob> pendingCleanupJobs = Deencapsulation.getField(
                    backupHandler, "pendingCleanupJobs");
            Assert.assertTrue(pendingCleanupJobs.isEmpty());
            Assert.assertTrue(localJobDir.exists());
        } finally {
            checkpointThreadIdField.setLong(null, oldCheckpointThreadId);
            Config.max_backup_restore_job_num_per_db = oldMaxJobNum;
        }
    }

    @Test
    public void testOrphanJobDirCleanupUsesReferencesAndGracePeriod() throws Exception {
        int oldOrphanKeepSecond = Config.backup_orphan_dir_keep_max_second;
        Config.backup_orphan_dir_keep_max_second = 2 * 24 * 3600;
        Path externalDir = Files.createTempDirectory("backup_orphan_external");
        Path symlink = null;
        try {
            long nowMs = System.currentTimeMillis();
            File referencedDir = createRepoJobDir(repoId, "referenced");
            File oldOrphanDir = createRepoJobDir(repoId, "old_orphan");
            File newOrphanDir = createRepoJobDir(repoId, "new_orphan");
            Files.setLastModifiedTime(referencedDir.toPath(),
                    FileTime.fromMillis(nowMs - TimeUnit.DAYS.toMillis(3)));
            Files.setLastModifiedTime(oldOrphanDir.toPath(),
                    FileTime.fromMillis(nowMs - TimeUnit.DAYS.toMillis(3)));

            BackupJob referencedJob = new BackupJob("referenced_label", dbId, UnitTestUtil.DB_NAME,
                    Lists.newArrayList(), TimeUnit.DAYS.toMillis(7), BackupStmt.BackupContent.ALL,
                    env, repoId, 0);
            Deencapsulation.setField(referencedJob, "state", BackupJobState.FINISHED);
            Deencapsulation.setField(referencedJob, "localJobDirPath", referencedDir.toPath());
            Deencapsulation.invoke(backupHandler, "addBackupOrRestoreJob", dbId, referencedJob);

            Path repoDir = referencedDir.toPath().getParent();
            symlink = repoDir.resolve("orphan_symlink_" + id.getAndIncrement());
            Files.createSymbolicLink(symlink, externalDir);

            Deencapsulation.invoke(backupHandler, "cleanupOrphanBackupJobLocalJobDirs", nowMs);

            Assert.assertTrue(referencedDir.exists());
            Assert.assertFalse(oldOrphanDir.exists());
            Assert.assertTrue(newOrphanDir.exists());
            Assert.assertTrue(Files.exists(symlink, LinkOption.NOFOLLOW_LINKS));
            Assert.assertTrue(Files.exists(externalDir));

            File disabledCleanupOrphanDir = createRepoJobDir(repoId, "disabled_cleanup_orphan");
            Files.setLastModifiedTime(disabledCleanupOrphanDir.toPath(),
                    FileTime.fromMillis(nowMs - TimeUnit.DAYS.toMillis(3)));
            Config.backup_orphan_dir_keep_max_second = 0;
            Deencapsulation.invoke(backupHandler, "cleanupOrphanBackupJobLocalJobDirs", nowMs);
            Assert.assertTrue(disabledCleanupOrphanDir.exists());
        } finally {
            if (symlink != null) {
                Files.deleteIfExists(symlink);
            }
            Files.deleteIfExists(externalDir);
            Config.backup_orphan_dir_keep_max_second = oldOrphanKeepSecond;
        }
    }

    @Test
    public void testOrphanJobDirCleanupUsesIndependentInterval() throws Exception {
        long oldCleanupIntervalSecond = Config.backup_orphan_dir_cleanup_interval_second;
        Config.backup_orphan_dir_cleanup_interval_second = 3600;
        try {
            long nowMs = System.currentTimeMillis();

            File firstOldOrphanDir = createRepoJobDir(repoId, "first_interval_orphan");
            Files.setLastModifiedTime(firstOldOrphanDir.toPath(),
                    FileTime.fromMillis(nowMs - TimeUnit.DAYS.toMillis(3)));
            Deencapsulation.invoke(backupHandler, "cleanupOrphanBackupJobLocalJobDirsIfNecessary", nowMs);
            Assert.assertFalse(firstOldOrphanDir.exists());

            File secondOldOrphanDir = createRepoJobDir(repoId, "second_interval_orphan");
            Files.setLastModifiedTime(secondOldOrphanDir.toPath(),
                    FileTime.fromMillis(nowMs - TimeUnit.DAYS.toMillis(3)));
            Deencapsulation.invoke(backupHandler, "cleanupOrphanBackupJobLocalJobDirsIfNecessary",
                    nowMs + Config.backup_handler_update_interval_millis);
            Assert.assertTrue(secondOldOrphanDir.exists());

            Deencapsulation.invoke(backupHandler, "cleanupOrphanBackupJobLocalJobDirsIfNecessary",
                    nowMs + TimeUnit.HOURS.toMillis(1));
            Assert.assertFalse(secondOldOrphanDir.exists());
        } finally {
            Config.backup_orphan_dir_cleanup_interval_second = oldCleanupIntervalSecond;
        }
    }

    @Test
    public void testLocalStagingCleanerCleansTerminalAndOrphanDirs() throws Exception {
        Deencapsulation.setField(backupHandler, "isInit", true);
        long nowMs = System.currentTimeMillis();

        // terminal remote job whose staging dir also exists on a follower after journal replay
        BackupJob finishedJob = new BackupJob("cleaner_finished", dbId, UnitTestUtil.DB_NAME,
                Lists.newArrayList(), 1000, BackupStmt.BackupContent.ALL, env, repoId, 0);
        File finishedDir = createLocalJobDir("cleaner_finished");
        File metaInfo = new File(finishedDir, Repository.FILE_META_INFO);
        File jobInfo = new File(finishedDir, Repository.PREFIX_JOB_INFO + "2026-09-10-10-00-00");
        Assert.assertTrue(metaInfo.createNewFile());
        Assert.assertTrue(jobInfo.createNewFile());
        Deencapsulation.setField(finishedJob, "state", BackupJobState.FINISHED);
        Deencapsulation.setField(finishedJob, "localJobDirPath", null);
        Deencapsulation.setField(finishedJob, "localMetaInfoFilePath", metaInfo.getAbsolutePath());
        Deencapsulation.setField(finishedJob, "localJobInfoFilePath", jobInfo.getAbsolutePath());
        Deencapsulation.invoke(backupHandler, "addBackupOrRestoreJob", dbId, finishedJob);

        // evicted terminal job sitting in the pending cleanup queue
        BackupJob pendingJob = new BackupJob("cleaner_pending", dbId, UnitTestUtil.DB_NAME,
                Lists.newArrayList(), 1000, BackupStmt.BackupContent.ALL, env, repoId, 0);
        File pendingDir = createLocalJobDir("cleaner_pending");
        Deencapsulation.setField(pendingJob, "state", BackupJobState.FINISHED);
        Deencapsulation.setField(pendingJob, "localJobDirPath", pendingDir.toPath());
        Deque<BackupJob> pendingCleanupJobs = Deencapsulation.getField(backupHandler, "pendingCleanupJobs");
        pendingCleanupJobs.add(pendingJob);

        File orphanDir = createRepoJobDir(repoId, "cleaner_orphan");
        Files.setLastModifiedTime(orphanDir.toPath(), FileTime.fromMillis(nowMs - TimeUnit.DAYS.toMillis(3)));

        Object cleaner = Deencapsulation.getField(backupHandler, "localStagingCleaner");
        Deencapsulation.invoke(cleaner, "runAfterCatalogReady");

        Assert.assertFalse(finishedDir.exists());
        Assert.assertFalse(pendingDir.exists());
        Assert.assertFalse(orphanDir.exists());
        Assert.assertTrue(pendingCleanupJobs.isEmpty());
    }

    @Test
    public void testRunAfterCatalogReadyDoesNotCleanStagingDirs() throws Exception {
        Deencapsulation.setField(backupHandler, "isInit", true);

        BackupJob finishedJob = new BackupJob("master_no_cleanup", dbId, UnitTestUtil.DB_NAME,
                Lists.newArrayList(), 1000, BackupStmt.BackupContent.ALL, env, repoId, 0);
        File finishedDir = createLocalJobDir("master_no_cleanup");
        Deencapsulation.setField(finishedJob, "state", BackupJobState.FINISHED);
        Deencapsulation.setField(finishedJob, "localJobDirPath", finishedDir.toPath());
        Deencapsulation.invoke(backupHandler, "addBackupOrRestoreJob", dbId, finishedJob);

        // the backup handler daemon only advances jobs now; staging cleanup has moved to the
        // local staging cleaner daemon which runs on every serving FE
        backupHandler.runAfterCatalogReady();
        Assert.assertTrue(finishedDir.exists());

        Object cleaner = Deencapsulation.getField(backupHandler, "localStagingCleaner");
        Deencapsulation.invoke(cleaner, "runAfterCatalogReady");
        Assert.assertFalse(finishedDir.exists());
    }

    @Test
    public void testOrphanCleanupClampsTooSmallKeepMaxSecond() throws Exception {
        int oldOrphanKeepSecond = Config.backup_orphan_dir_keep_max_second;
        Config.backup_orphan_dir_keep_max_second = 1;
        try {
            long nowMs = System.currentTimeMillis();
            File recentOrphanDir = createRepoJobDir(repoId, "clamp_recent_orphan");
            Files.setLastModifiedTime(recentOrphanDir.toPath(),
                    FileTime.fromMillis(nowMs - TimeUnit.SECONDS.toMillis(5)));

            Deencapsulation.invoke(backupHandler, "cleanupOrphanBackupJobLocalJobDirs", nowMs);
            // without the clamp a 1s retention would delete a 5s-old directory that may still be
            // written by concurrent replay
            Assert.assertTrue(recentOrphanDir.exists());
            Assert.assertTrue(Deencapsulation.getField(backupHandler, "orphanDirKeepMaxClampWarned"));

            Files.setLastModifiedTime(recentOrphanDir.toPath(),
                    FileTime.fromMillis(nowMs - TimeUnit.SECONDS.toMillis(400)));
            Deencapsulation.invoke(backupHandler, "cleanupOrphanBackupJobLocalJobDirs", nowMs);
            Assert.assertFalse(recentOrphanDir.exists());
        } finally {
            Config.backup_orphan_dir_keep_max_second = oldOrphanKeepSecond;
        }
    }

    @Test
    public void testOrphanCleanupSkipsDirRevalidatedAsReferenced() throws Exception {
        long nowMs = System.currentTimeMillis();
        File writerDir = createRepoJobDir(repoId, "writer_pinned_orphan");
        Files.setLastModifiedTime(writerDir.toPath(), FileTime.fromMillis(nowMs - TimeUnit.DAYS.toMillis(3)));
        Path normalizedDir = writerDir.toPath().toAbsolutePath().normalize();

        // simulate a replay writer holding the dir write lock while building the staging dir
        ReentrantLock writeLock = BackupHandler.getJobDirWriteLock(normalizedDir);
        writeLock.lock();
        Thread cleanerThread = new Thread(
                () -> Deencapsulation.invoke(backupHandler, "cleanupOrphanBackupJobLocalJobDirs", nowMs));
        try {
            cleanerThread.start();
            waitForCleanerQueuedOn(writeLock);
            Assert.assertTrue(writerDir.exists());

            // the writer finishes and the replayed job is published, referencing the dir. The
            // orphan scan already snapshotted its references before this, so only the
            // revalidation under the write lock can save the dir.
            BackupJob replayedJob = new BackupJob("writer_pinned_label", dbId, UnitTestUtil.DB_NAME,
                    Lists.newArrayList(), 1000, BackupStmt.BackupContent.ALL, env, repoId, 0);
            Deencapsulation.setField(replayedJob, "state", BackupJobState.FINISHED);
            Deencapsulation.setField(replayedJob, "localJobDirPath", normalizedDir);
            Deencapsulation.invoke(backupHandler, "addBackupOrRestoreJob", dbId, replayedJob);
        } finally {
            writeLock.unlock();
        }
        cleanerThread.join(10000);
        Assert.assertFalse(cleanerThread.isAlive());
        Assert.assertTrue(writerDir.exists());
    }

    @Test
    public void testOrphanCleanupDeletesDirAfterWriterReleases() throws Exception {
        long nowMs = System.currentTimeMillis();
        File writerDir = createRepoJobDir(repoId, "writer_released_orphan");
        Files.setLastModifiedTime(writerDir.toPath(), FileTime.fromMillis(nowMs - TimeUnit.DAYS.toMillis(3)));
        Path normalizedDir = writerDir.toPath().toAbsolutePath().normalize();

        ReentrantLock writeLock = BackupHandler.getJobDirWriteLock(normalizedDir);
        writeLock.lock();
        Thread cleanerThread = new Thread(
                () -> Deencapsulation.invoke(backupHandler, "cleanupOrphanBackupJobLocalJobDirs", nowMs));
        try {
            cleanerThread.start();
            waitForCleanerQueuedOn(writeLock);
            Assert.assertTrue(writerDir.exists());
        } finally {
            writeLock.unlock();
        }
        // no job ever references the dir: once the writer releases, the revalidation under the
        // write lock still sees an unreferenced, expired dir and deletes it
        cleanerThread.join(10000);
        Assert.assertFalse(cleanerThread.isAlive());
        Assert.assertFalse(writerDir.exists());
    }

    @Test
    public void testReplayAddJobKeepsStagingDirUntilJobPublished() throws Exception {
        long nowMs = System.currentTimeMillis();

        // a replay is in progress on this FE: the current job of the db is not done yet
        BackupJob existingJob = new BackupJob("replay_gap_existing", dbId, UnitTestUtil.DB_NAME,
                Lists.newArrayList(), 1000, BackupStmt.BackupContent.ALL, env, repoId, 0);
        Deencapsulation.invoke(backupHandler, "addBackupOrRestoreJob", dbId, existingJob);

        BackupJob replayedJob = new BackupJob("replay_gap_label", dbId, UnitTestUtil.DB_NAME,
                Lists.newArrayList(), 1000, BackupStmt.BackupContent.ALL, env, repoId, 0);
        Deencapsulation.setField(replayedJob, "state", BackupJobState.FINISHED);
        Path stagingDir = replayedJob.getLocalStagingDirPath();
        Files.createDirectories(stagingDir);
        // a slow meta write leaves the dir mtime older than the (clamped) retention: creating the
        // last file refreshes the dir mtime, writing its content does not
        Files.setLastModifiedTime(stagingDir, FileTime.fromMillis(nowMs - TimeUnit.DAYS.toMillis(3)));

        new MockUp<BackupJob>() {
            @Mock
            public void replayRun() {
                // stands in for the real writer: the staging dir is fully written and its write
                // lock is released, but replayAddJob has not published the job yet. This is the
                // production order, replayRun() returns before addBackupOrRestoreJob() runs.
                Deencapsulation.invoke(backupHandler, "cleanupOrphanBackupJobLocalJobDirs", nowMs);
            }
        };

        backupHandler.replayAddJob(replayedJob);

        Assert.assertTrue("expired staging dir was deleted in the write-to-publish window",
                Files.exists(stagingDir));
        Map<?, ?> unpublishedStagingDirs = (Map<?, ?>) getBackupHandlerStaticField("UNPUBLISHED_STAGING_DIRS");
        // the reservation is released once the job is published, so the map stays bounded by the
        // number of replays in flight
        Assert.assertFalse(unpublishedStagingDirs.containsKey(stagingDir.toAbsolutePath().normalize()));
    }

    @Test
    public void testJobDirWriteLocksAreStriped() throws Exception {
        int stripeNum = (int) getBackupHandlerStaticField("JOB_DIR_WRITE_LOCK_STRIPE_NUM");
        IdentityHashMap<ReentrantLock, Boolean> distinctLocks = new IdentityHashMap<>();
        Path firstJobDir = null;
        for (int i = 0; i < 10000; i++) {
            Path jobDir = BackupHandler.BACKUP_ROOT_DIR.resolve("repo__" + repoId)
                    .resolve("striped_label_" + i + "__2026-09-11-10-00-00");
            if (firstJobDir == null) {
                firstJobDir = jobDir;
            }
            distinctLocks.put(BackupHandler.getJobDirWriteLock(jobDir), Boolean.TRUE);
        }
        // the lock table has a fixed size, it does not retain one lock per staging dir ever seen
        Assert.assertTrue(distinctLocks.size() <= stripeNum);
        // and writers and deleters still agree on one lock per directory, however the path is spelled
        Assert.assertSame(BackupHandler.getJobDirWriteLock(firstJobDir),
                BackupHandler.getJobDirWriteLock(firstJobDir.toAbsolutePath().normalize()));
    }

    // Deencapsulation.getField(Class, String) rejects a null target, so read static fields directly
    private static Object getBackupHandlerStaticField(String fieldName) throws Exception {
        Field field = BackupHandler.class.getDeclaredField(fieldName);
        field.setAccessible(true);
        return field.get(null);
    }

    private void waitForCleanerQueuedOn(ReentrantLock writeLock) throws InterruptedException {
        long deadlineMs = System.currentTimeMillis() + 10000;
        while (!writeLock.hasQueuedThreads() && System.currentTimeMillis() < deadlineMs) {
            Thread.sleep(10);
        }
        Assert.assertTrue("cleaner did not block on the dir write lock", writeLock.hasQueuedThreads());
    }

    /**
     * Test backup job execution with non-existent table
     *
     * Scenario: Attempt to backup a table that does not exist
     * Expected Results:
     * 1. Job should fail with NOT_FOUND error code
     * 2. Job state should be CANCELLED
     * 3. No backup tasks should be created
     */
    @Test
    public void testRunAbnormal() {
        // 1. pending
        AgentTaskQueue.clearAllTasks();

        List<TableRef> tableRefs = Lists.newArrayList();
        tableRefs.add(
                new TableRef(new TableName(InternalCatalog.INTERNAL_CATALOG_NAME, UnitTestUtil.DB_NAME, "unknown_tbl"),
                        null));
        job = new BackupJob("label", dbId, UnitTestUtil.DB_NAME, tableRefs, 13600 * 1000, BackupStmt.BackupContent.ALL,
                env, repo.getId(), 0);
        job.run();
        Assert.assertEquals(Status.ErrCode.NOT_FOUND, job.getStatus().getErrCode());
        Assert.assertEquals(BackupJobState.CANCELLED, job.getState());
    }

    /**
     * Test backup job execution with mixed existing and non-existent tables
     *
     * Scenario: Backup two tables - one existing table and one non-existent table
     * Expected Results:
     * 1. Job should succeed and proceed to SNAPSHOTING state
     * 2. Backup meta should only contain the existing table
     * 3. Only snapshot tasks for the existing table should be created
     * 4. Non-existent table should be skipped without causing job failure
     */
    @Test
    public void testRunAbnormalWithMixedTables() {
        // Test backup two tables: one normal table and one non-existent table
        // Verify backup succeeds, backs up the normal table, and skips the non-existent table
        AgentTaskQueue.clearAllTasks();

        List<TableRef> tableRefs = Lists.newArrayList();
        // Add normal table
        tableRefs.add(new TableRef(
                new TableName(InternalCatalog.INTERNAL_CATALOG_NAME, UnitTestUtil.DB_NAME, UnitTestUtil.TABLE_NAME),
                null));
        // Add non-existent table
        tableRefs.add(
                new TableRef(new TableName(InternalCatalog.INTERNAL_CATALOG_NAME, UnitTestUtil.DB_NAME, "unknown_tbl"),
                        null));

        job = new BackupJob("label", dbId, UnitTestUtil.DB_NAME, tableRefs, 13600 * 1000, BackupStmt.BackupContent.ALL,
                env, repo.getId(), 0);

        // 1. pending
        Assert.assertEquals(BackupJobState.PENDING, job.getState());
        job.run();
        Assert.assertEquals(Status.OK, job.getStatus());
        Assert.assertEquals(BackupJobState.SNAPSHOTING, job.getState());

        // Verify backup meta only contains the normal table
        BackupMeta backupMeta = job.getBackupMeta();
        Assert.assertEquals(1, backupMeta.getTables().size());
        OlapTable backupTbl = (OlapTable) backupMeta.getTable(UnitTestUtil.TABLE_NAME);
        Assert.assertNotNull(backupTbl);
        Assert.assertNull(backupMeta.getTable("unknown_tbl"));

        // Verify only snapshot tasks for the normal table are created
        Assert.assertEquals(1, AgentTaskQueue.getTaskNum());
        AgentTask task = AgentTaskQueue.getTask(backendId, TTaskType.MAKE_SNAPSHOT, id.get() - 1);
        Assert.assertTrue(task instanceof SnapshotTask);
        SnapshotTask snapshotTask = (SnapshotTask) task;
        Assert.assertEquals(tblId, snapshotTask.getTableId());
        Assert.assertEquals(dbId, snapshotTask.getDbId());
        Assert.assertEquals(partId, snapshotTask.getPartitionId());
        Assert.assertEquals(idxId, snapshotTask.getIndexId());
        Assert.assertEquals(tabletId, snapshotTask.getTabletId());
    }

    /**
     * Test backup job execution when a table is dropped during SNAPSHOTING phase
     *
     * Scenario: Start backup with two normal tables, then drop one table during SNAPSHOTING phase
     * Expected Results:
     * 1. Job should start with two tables and create snapshot tasks for both
     * 2. When one table is dropped during SNAPSHOTING, the dropped table should be marked as dropped
     * 3. Backup should continue successfully with only the remaining table
     * 4. Final backup meta should only contain the non-dropped table
     * 5. Job should complete successfully with FINISHED state
     */
    @Test
    public void testRunWithTableDroppedDuringSnapshoting() {
        try {
            AgentTaskQueue.clearAllTasks();

            List<TableRef> tableRefs = Lists.newArrayList();
            tableRefs.add(new TableRef(
                    new TableName(InternalCatalog.INTERNAL_CATALOG_NAME, UnitTestUtil.DB_NAME, UnitTestUtil.TABLE_NAME),
                    null));
            tableRefs.add(new TableRef(
                    new TableName(InternalCatalog.INTERNAL_CATALOG_NAME, UnitTestUtil.DB_NAME, table2Name),
                    null));

            job = new BackupJob("label", dbId, UnitTestUtil.DB_NAME, tableRefs, 13600 * 1000, BackupStmt.BackupContent.ALL,
                    env, repo.getId(), 0);

            // 1. pending - should create snapshot tasks for both tables
            Assert.assertEquals(BackupJobState.PENDING, job.getState());
            job.run();
            Assert.assertEquals(Status.OK, job.getStatus());
            Assert.assertEquals(BackupJobState.SNAPSHOTING, job.getState());

            // Verify backup meta contains both tables initially
            BackupMeta backupMeta = job.getBackupMeta();
            Assert.assertEquals(2, backupMeta.getTables().size());
            Assert.assertNotNull(backupMeta.getTable(UnitTestUtil.TABLE_NAME));
            Assert.assertNotNull(backupMeta.getTable(table2Name));

            // Verify snapshot tasks are created for both tables
            Assert.assertEquals(2, AgentTaskQueue.getTaskNum());

            // 2. Simulate dropping the second table during SNAPSHOTING phase
            db.unregisterTable(table2Name);

            // 3. Finish snapshot tasks
            SnapshotTask taskForDroppedTable = null;
            SnapshotTask taskForExistingTable = null;
            long taskTabletId1 = AgentTaskQueue.getTask(backendId, TTaskType.MAKE_SNAPSHOT, id.get() - 2).getTabletId();
            if (taskTabletId1 == tabletId) {
                taskForExistingTable = (SnapshotTask) AgentTaskQueue.getTask(backendId, TTaskType.MAKE_SNAPSHOT, id.get() - 2);
                taskForDroppedTable = (SnapshotTask) AgentTaskQueue.getTask(backendId, TTaskType.MAKE_SNAPSHOT, id.get() - 1);
            } else {
                taskForDroppedTable = (SnapshotTask) AgentTaskQueue.getTask(backendId, TTaskType.MAKE_SNAPSHOT, id.get() - 2);
                taskForExistingTable = (SnapshotTask) AgentTaskQueue.getTask(backendId, TTaskType.MAKE_SNAPSHOT, id.get() - 1);
            }

            TBackend tBackend = new TBackend("", 0, 1);

            // Finish task for dropped table
            TStatus taskStatusMissing = new TStatus(TStatusCode.TABLET_MISSING);
            taskStatusMissing.setErrorMsgs(Lists.newArrayList("Tablet missing"));
            TFinishTaskRequest requestMissing = new TFinishTaskRequest(tBackend, TTaskType.MAKE_SNAPSHOT,
                    taskForDroppedTable.getSignature(), taskStatusMissing);
            Assert.assertTrue(job.finishTabletSnapshotTask(taskForDroppedTable, requestMissing));

            // Finish task for existing table
            String snapshotPath = "/path/to/snapshot";
            List<String> snapshotFiles = Lists.newArrayList("1.dat", "1.idx", "1.hdr");
            TStatus taskStatusOK = new TStatus(TStatusCode.OK);
            TFinishTaskRequest requestOK = new TFinishTaskRequest(tBackend, TTaskType.MAKE_SNAPSHOT,
                    taskForExistingTable.getSignature(), taskStatusOK);
            requestOK.setSnapshotFiles(snapshotFiles);
            requestOK.setSnapshotPath(snapshotPath);
            Assert.assertTrue(job.finishTabletSnapshotTask(taskForExistingTable, requestOK));

            // 4. Continue the backup process
            job.run();
            Assert.assertEquals(Status.OK, job.getStatus());
            Assert.assertEquals(BackupJobState.UPLOAD_SNAPSHOT, job.getState());

            AgentTaskQueue.clearAllTasks();
            job.run(); // UPLOAD_SNAPSHOT -> UPLOADING
            Assert.assertEquals(1, AgentTaskQueue.getTaskNum());
            UploadTask upTask = (UploadTask) AgentTaskQueue.getTask(backendId, TTaskType.UPLOAD, id.get() - 1);

            // Finish upload task
            Map<Long, List<String>> tabletFileMap = Maps.newHashMap();
            List<String> tabletFiles = Lists.newArrayList();
            tabletFiles.add("1.dat.4f158689243a3d6030352fec3cfd3798");
            tabletFiles.add("1.idx.4f158689243a3d6030352fec3cfd3798");
            tabletFiles.add("1.hdr.4f158689243a3d6030352fec3cfd3798");
            tabletFileMap.put(taskForExistingTable.getTabletId(), tabletFiles);
            TFinishTaskRequest requestUpload = new TFinishTaskRequest(tBackend, TTaskType.UPLOAD,
                    upTask.getSignature(), taskStatusOK);
            requestUpload.setTabletFiles(tabletFileMap);
            Assert.assertTrue(job.finishSnapshotUploadTask(upTask, requestUpload));

            job.run(); // UPLOADING -> SAVE_META
            Assert.assertEquals(BackupJobState.SAVE_META, job.getState());

            job.run(); // SAVE_META -> UPLOAD_INFO
            Assert.assertEquals(BackupJobState.UPLOAD_INFO, job.getState());

            job.run(); // UPLOAD_INFO -> FINISHED
            Assert.assertEquals(BackupJobState.FINISHED, job.getState());

        } catch (Throwable e) {
            e.printStackTrace();
            Assert.fail(e.getMessage());
        } finally {
            // Clean up: re-register the second table if it was removed
            if (db.getTableNullable(table2Name) == null && table2 != null) {
                db.registerTable(table2);
            }
        }
    }

    /**
     * Test backup job serialization and deserialization
     *
     * Scenario: Write backup job to file and read it back
     * Expected Results:
     * 1. Backup job should be successfully written to file
     * 2. Backup job should be successfully read from file
     * 3. All job properties should be preserved during serialization/deserialization
     * 4. Temporary files should be cleaned up
     */
    @Test
    public void testSerialization() throws IOException, AnalysisException {
        // 1. Write objects to file
        final Path path = Files.createTempFile("backupJob", "tmp");
        DataOutputStream out = new DataOutputStream(Files.newOutputStream(path));

        List<TableRef> tableRefs = Lists.newArrayList();
        tableRefs.add(
                new TableRef(new TableName(InternalCatalog.INTERNAL_CATALOG_NAME, UnitTestUtil.DB_NAME, UnitTestUtil.TABLE_NAME),
                        null));
        job = new BackupJob("label", dbId, UnitTestUtil.DB_NAME, tableRefs, 13600 * 1000, BackupStmt.BackupContent.ALL,
            env, repo.getId(), 123);

        job.write(out);
        out.flush();
        out.close();

        // 2. Read objects from file
        DataInputStream in = new DataInputStream(Files.newInputStream(path));

        BackupJob job2 = BackupJob.read(in);

        Assert.assertEquals(job.getJobId(), job2.getJobId());
        Assert.assertEquals(job.getDbId(), job2.getDbId());
        Assert.assertEquals(job.getCreateTime(), job2.getCreateTime());
        Assert.assertEquals(job.getType(), job2.getType());
        Assert.assertEquals(job.getCommitSeq(), job2.getCommitSeq());

        // 3. delete files
        in.close();
        Files.delete(path);
    }

    private File createLocalJobDir(String name) throws IOException {
        Path jobDirPath = BackupHandler.BACKUP_ROOT_DIR.resolve(name + "_" + id.getAndIncrement());
        Files.createDirectories(jobDirPath);
        return jobDirPath.toFile();
    }

    private File createRepoJobDir(long jobRepoId, String name) throws IOException {
        Path jobDirPath = BackupHandler.BACKUP_ROOT_DIR.resolve("repo__" + jobRepoId)
                .resolve(name + "_" + id.getAndIncrement());
        Files.createDirectories(jobDirPath);
        return jobDirPath.toFile();
    }

    private void createSparseFile(File file, long logicalSize) throws IOException {
        try (RandomAccessFile raf = new RandomAccessFile(file, "rw")) {
            raf.setLength(logicalSize);
        }
    }

    private static final String MD5 = "4f158689243a3d6030352fec3cfd3798";

    // Whether the snapshot task asks the backend for the logical digest when the property is on, false for a unique
    // table with the default config.
    private boolean logicalDigestAsked = true;

    private static String sha(char c) {
        return Strings.repeat(String.valueOf(c), 64).toLowerCase();
    }

    // Run the job until the snapshot tasks are sent, returns them.
    private List<SnapshotTask> runToSnapshotTasks(BackupJob backupJob) {
        AgentTaskQueue.clearAllTasks();
        backupJob.run();
        Assert.assertEquals(BackupJobState.SNAPSHOTING, backupJob.getState());
        List<SnapshotTask> tasks = Lists.newArrayList();
        for (long signature = id.get() - 10; signature < id.get(); signature++) {
            AgentTask task = AgentTaskQueue.getTask(backendId, TTaskType.MAKE_SNAPSHOT, signature);
            if (task instanceof SnapshotTask) {
                tasks.add((SnapshotTask) task);
            }
        }
        return tasks;
    }

    // Finish the snapshot task of the job, as a backend which writes the manifest root if snapshotRoot.
    private void finishSnapshotTask(SnapshotTask snapshotTask, boolean snapshotRoot) {
        finishSnapshotTask(snapshotTask, snapshotRoot, null);
    }

    // With digest, the backend also reports the logical digest of the tablet.
    private void finishSnapshotTask(SnapshotTask snapshotTask, boolean snapshotRoot, TLogicalDigest digest) {
        TBackend tBackend = new TBackend("", 0, 1);
        TStatus ok = new TStatus(TStatusCode.OK);
        TFinishTaskRequest request = new TFinishTaskRequest(tBackend, TTaskType.MAKE_SNAPSHOT,
                snapshotTask.getSignature(), ok);
        request.setSnapshotFiles(Lists.newArrayList("1.dat", "1.idx", "1.hdr"));
        request.setSnapshotPath("/path/to/snapshot");
        if (snapshotRoot) {
            request.setManifestRoot(sha('1'));
        }
        if (digest != null) {
            request.setLogicalDigest(digest);
        }
        Assert.assertTrue(job.finishTabletSnapshotTask(snapshotTask, request));
    }

    // Run the backup to a remote repository until the job info file is saved, returns the job info read from it.
    // The snapshot task reports the root of the manifest written with the snapshot if snapshotRoot, the upload task
    // reports the root of the manifest uploaded to the repository if uploadRoot, as a new backend does.
    private BackupJobInfo runBackupToSavedJobInfo(boolean snapshotRoot, boolean uploadRoot) throws IOException {
        return runBackupToSavedJobInfo(snapshotRoot, uploadRoot, false, null);
    }

    // With logicalDigest, the logical_digest property is on and the snapshot task reports `digest` (null: not
    // reported, as an old backend).
    private BackupJobInfo runBackupToSavedJobInfo(boolean snapshotRoot, boolean uploadRoot, boolean logicalDigest,
            TLogicalDigest digest) throws IOException {
        AgentTaskQueue.clearAllTasks();
        if (logicalDigest) {
            job.setLogicalDigest(true);
        }
        job.run();
        Assert.assertEquals(BackupJobState.SNAPSHOTING, job.getState());
        SnapshotTask snapshotTask = (SnapshotTask) AgentTaskQueue.getTask(backendId, TTaskType.MAKE_SNAPSHOT,
                id.get() - 1);
        // a remote repository computes the digests when uploading, not in the snapshot.
        Assert.assertFalse(snapshotTask.toThrift().isSetComputeDigest());
        boolean asked = logicalDigest && logicalDigestAsked;
        Assert.assertEquals(asked, snapshotTask.isComputeLogicalDigest());
        Assert.assertEquals(asked, snapshotTask.toThrift().isSetComputeLogicalDigest());

        finishSnapshotTask(snapshotTask, snapshotRoot, digest);
        job.run();
        Assert.assertEquals(BackupJobState.UPLOAD_SNAPSHOT, job.getState());

        AgentTaskQueue.clearAllTasks();
        job.run();
        Assert.assertEquals(BackupJobState.UPLOADING, job.getState());
        UploadTask upTask = (UploadTask) AgentTaskQueue.getTask(backendId, TTaskType.UPLOAD, id.get() - 1);
        TFinishTaskRequest request = new TFinishTaskRequest(new TBackend("", 0, 1), TTaskType.UPLOAD,
                upTask.getSignature(), new TStatus(TStatusCode.OK));
        Map<Long, List<String>> tabletFileMap = Maps.newHashMap();
        tabletFileMap.put(tabletId, Lists.newArrayList("1.dat." + MD5, "1.idx." + MD5, "1.hdr." + MD5));
        request.setTabletFiles(tabletFileMap);
        if (uploadRoot) {
            Map<Long, String> roots = Maps.newHashMap();
            roots.put(tabletId, sha('A'));
            request.setManifestRoots(roots);
        }
        Assert.assertTrue(job.finishSnapshotUploadTask(upTask, request));
        job.run();
        Assert.assertEquals(BackupJobState.SAVE_META, job.getState());
        job.run();
        Assert.assertEquals(BackupJobState.UPLOAD_INFO, job.getState());
        return BackupJobInfo.fromFile(job.getLocalJobInfoFilePath());
    }

    @Test
    public void testBackupWritesManifest() throws IOException {
        BackupJobInfo jobInfo = runBackupToSavedJobInfo(true, true);
        Assert.assertEquals(BackupJobInfo.MANIFEST_VERSION, (int) jobInfo.manifestVersion);
        Assert.assertEquals(BackupJobInfo.DIGEST_SHA256, jobInfo.digestAlgorithm);
        Assert.assertTrue(jobInfo.hasManifest());

        // the root of the manifest uploaded to the repository, not the one written with the snapshot
        Assert.assertEquals(sha('a'), onlyIndex(jobInfo).getManifestRoot(tabletId));

        // read by position, there is no view object of the tablet
        BackupJobInfo.BackupIndexInfo idxInfo = jobInfo.backupOlapTableObjects.values().iterator().next()
                .partitions.values().iterator().next().indexes.values().iterator().next();
        BackupJobInfo.BackupTabletInfo tabletInfo = idxInfo.sortedTabletInfoList.get(0);
        Assert.assertEquals(sha('a'), idxInfo.getManifestRootAt(0));
        // the existing files are unchanged
        Assert.assertEquals(Lists.newArrayList("1.dat." + MD5, "1.idx." + MD5, "1.hdr." + MD5),
                tabletInfo.files);
    }

    @Test
    public void testBackupWithoutManifestIfUploadHasNoManifestRoot() throws IOException {
        // the snapshot wrote a manifest, but the upload reports no root (e.g. the backend is downgraded).
        BackupJobInfo jobInfo = runBackupToSavedJobInfo(true, false);
        Assert.assertNull(jobInfo.manifestVersion);
        Assert.assertNull(jobInfo.digestAlgorithm);
        Assert.assertFalse(jobInfo.hasManifest());
        Assert.assertNull(onlyIndex(jobInfo).getManifestRoot(tabletId));
        // same as a backup of an old version: no new key in the job info file.
        String json = new String(Files.readAllBytes(new File(job.getLocalJobInfoFilePath()).toPath()));
        Assert.assertFalse(json, json.contains("manifest"));
        Assert.assertFalse(json, json.contains("digest_algorithm"));
    }

    private static BackupJobInfo.BackupIndexInfo onlyIndex(BackupJobInfo jobInfo) {
        return jobInfo.backupOlapTableObjects.values().iterator().next()
                .partitions.values().iterator().next().indexes.values().iterator().next();
    }

    private static TLogicalDigest reportedDigest(String code) {
        TLogicalDigest digest = new TLogicalDigest();
        digest.setStatusCode(code);
        if ("OK".equals(code)) {
            digest.setAlgoVersion(1);
            digest.setSchemaSig(sha('5'));
            digest.setRoot(sha('B'));
            digest.setRows(42);
        } else {
            digest.setStatusMsg("restore digest: " + code);
        }
        return digest;
    }

    @Test
    public void testBackupRecordsLogicalDigest() throws IOException {
        BackupJobInfo jobInfo = runBackupToSavedJobInfo(true, true, true, reportedDigest("OK"));
        LogicalDigestInfo digest = onlyIndex(jobInfo).getLogicalDigest(tabletId);
        Assert.assertNotNull(digest);
        Assert.assertTrue(digest.hasDigest());
        Assert.assertEquals(1, (int) digest.algoVersion);
        Assert.assertEquals(sha('5'), digest.schemaSig);
        Assert.assertEquals(sha('b'), digest.root);
        Assert.assertNull(digest.reason);
        BackupJobInfo.BackupIndexInfo idxInfo = jobInfo.backupOlapTableObjects.values().iterator().next()
                .partitions.values().iterator().next().indexes.values().iterator().next();
        Assert.assertEquals(digest.root, idxInfo.getLogicalDigestAt(0).root);
        // the manifest is not affected
        Assert.assertEquals(sha('a'), onlyIndex(jobInfo).getManifestRoot(tabletId));
        String json = new String(Files.readAllBytes(new File(job.getLocalJobInfoFilePath()).toPath()));
        // the schema signature and the algorithm once for the index, the root for each tablet; no old "ld"
        Assert.assertTrue(json, json.contains("\"lra\":[\"" + sha('b') + "\"]"));
        Assert.assertFalse(json, json.contains("\"ldr\""));
        Assert.assertFalse(json, json.contains("\"manifest_roots\""));
        Assert.assertTrue(json, json.contains("\"mra\":[\"" + sha('a') + "\"]"));
        Assert.assertTrue(json, json.contains("\"lds\":\"" + sha('5') + "\""));
        Assert.assertTrue(json, json.contains("\"lda\":1"));
        Assert.assertFalse(json, json.contains("\"ld\""));
    }

    private void checkNoLogicalDigest(TLogicalDigest reported, String reason) throws IOException {
        // NOT_SUPPORTED, a failure and no report (an old backend) never fail the backup
        BackupJobInfo jobInfo = runBackupToSavedJobInfo(true, true, true, reported);
        LogicalDigestInfo digest = onlyIndex(jobInfo).getLogicalDigest(tabletId);
        Assert.assertNotNull(digest);
        Assert.assertFalse(digest.hasDigest());
        Assert.assertEquals("", digest.root);
        Assert.assertEquals(reason, digest.reason);
        Assert.assertNull(digest.schemaSig);
        // the files of the backup are there
        Assert.assertTrue(jobInfo.hasManifest());
    }

    @Test
    public void testBackupRecordsLogicalDigestNotSupported() throws IOException {
        checkNoLogicalDigest(reportedDigest("NOT_SUPPORTED"), LogicalDigestInfo.REASON_NOT_SUPPORTED);
    }

    @Test
    public void testBackupRecordsLogicalDigestError() throws IOException {
        checkNoLogicalDigest(reportedDigest("ERROR"), LogicalDigestInfo.REASON_ERROR);
    }

    @Test
    public void testBackupRecordsLogicalDigestNoReport() throws IOException {
        checkNoLogicalDigest(null, LogicalDigestInfo.REASON_NO_REPORT);
    }

    @Test
    public void testBackupWithoutLogicalDigestPropertyHasNone() throws IOException {
        // the backend reports one anyway (it does not, but the job must not record it)
        BackupJobInfo jobInfo = runBackupToSavedJobInfo(true, true, false, reportedDigest("OK"));
        Assert.assertNull(onlyIndex(jobInfo).getLogicalDigest(tabletId));
        String json = new String(Files.readAllBytes(new File(job.getLocalJobInfoFilePath()).toPath()));
        Assert.assertFalse(json, json.contains("\"ld\""));
    }

    private List<TableRef> tableRefsOf(String... tableNames) {
        List<TableRef> tableRefs = Lists.newArrayList();
        for (String name : tableNames) {
            tableRefs.add(new TableRef(
                    new TableName(InternalCatalog.INTERNAL_CATALOG_NAME, UnitTestUtil.DB_NAME, name), null));
        }
        return tableRefs;
    }

    @Test
    public void testLogicalDigestOfUniqueTableIsNotAskedFor() {
        boolean orig = Config.restore_reuse_allow_hidden_state_models;
        OlapTable tbl = (OlapTable) db.getTableNullable(tblId);
        try {
            job.setLogicalDigest(true);
            // a duplicate table is asked for it
            Deencapsulation.setField(tbl, "keysType", KeysType.DUP_KEYS);
            Config.restore_reuse_allow_hidden_state_models = false;
            List<SnapshotTask> tasks = runToSnapshotTasks(job);
            Assert.assertEquals(1, tasks.size());
            Assert.assertTrue(tasks.get(0).isComputeLogicalDigest());

            // a unique table is not, with the default config of the source cluster
            Deencapsulation.setField(tbl, "keysType", KeysType.UNIQUE_KEYS);
            job = new BackupJob("label2", dbId, UnitTestUtil.DB_NAME, tableRefsOf(UnitTestUtil.TABLE_NAME),
                    13600 * 1000, BackupStmt.BackupContent.ALL, env, repo.getId(), 0);
            job.setLogicalDigest(true);
            tasks = runToSnapshotTasks(job);
            Assert.assertEquals(1, tasks.size());
            Assert.assertFalse(tasks.get(0).isComputeLogicalDigest());
            Assert.assertFalse(tasks.get(0).toThrift().isSetComputeLogicalDigest());
            // the manifest is not affected
            Assert.assertTrue(tasks.get(0).toThrift().isSetListFiles());

            // unless the source cluster allows the hidden state models
            Config.restore_reuse_allow_hidden_state_models = true;
            job = new BackupJob("label3", dbId, UnitTestUtil.DB_NAME, tableRefsOf(UnitTestUtil.TABLE_NAME),
                    13600 * 1000, BackupStmt.BackupContent.ALL, env, repo.getId(), 0);
            job.setLogicalDigest(true);
            tasks = runToSnapshotTasks(job);
            Assert.assertTrue(tasks.get(0).isComputeLogicalDigest());
        } finally {
            Config.restore_reuse_allow_hidden_state_models = orig;
            Deencapsulation.setField(tbl, "keysType", KeysType.DUP_KEYS);
        }
    }

    // A unique merge-on-write table without a sequence column is asked for the digest (the version 2), whatever the
    // value of restore_reuse_mow: the backup must have it when the switch is turned on later. One with a sequence
    // column or a sequence mapping has none (the BE would say NOT_SUPPORTED), and a merge-on-read table only when the
    // hidden state models are allowed.
    @Test
    public void testLogicalDigestOfMowTableIsAskedForWithoutSequenceColumn() {
        boolean origHidden = Config.restore_reuse_allow_hidden_state_models;
        boolean origMow = Config.restore_reuse_mow;
        OlapTable tbl = (OlapTable) db.getTableNullable(tblId);
        try {
            Config.restore_reuse_allow_hidden_state_models = false;
            Deencapsulation.setField(tbl, "keysType", KeysType.UNIQUE_KEYS);
            tbl.setEnableUniqueKeyMergeOnWrite(true);
            int n = 0;
            for (boolean mowSwitch : new boolean[] {false, true}) {
                Config.restore_reuse_mow = mowSwitch;
                job = new BackupJob("mow" + n++, dbId, UnitTestUtil.DB_NAME, tableRefsOf(UnitTestUtil.TABLE_NAME),
                        13600 * 1000, BackupStmt.BackupContent.ALL, env, repo.getId(), 0);
                job.setLogicalDigest(true);
                List<SnapshotTask> tasks = runToSnapshotTasks(job);
                Assert.assertEquals(1, tasks.size());
                Assert.assertTrue("restore_reuse_mow " + mowSwitch, tasks.get(0).isComputeLogicalDigest());
            }
            // a sequence mapping column
            tbl.setSequenceMapCol("k1");
            job = new BackupJob("mow" + n++, dbId, UnitTestUtil.DB_NAME, tableRefsOf(UnitTestUtil.TABLE_NAME),
                    13600 * 1000, BackupStmt.BackupContent.ALL, env, repo.getId(), 0);
            job.setLogicalDigest(true);
            List<SnapshotTask> tasks = runToSnapshotTasks(job);
            Assert.assertFalse(tasks.get(0).isComputeLogicalDigest());
            tbl.setSequenceMapCol(null);
            // merge-on-read: not, unless the hidden state models are allowed
            tbl.setEnableUniqueKeyMergeOnWrite(false);
            job = new BackupJob("mow" + n++, dbId, UnitTestUtil.DB_NAME, tableRefsOf(UnitTestUtil.TABLE_NAME),
                    13600 * 1000, BackupStmt.BackupContent.ALL, env, repo.getId(), 0);
            job.setLogicalDigest(true);
            Assert.assertFalse(runToSnapshotTasks(job).get(0).isComputeLogicalDigest());
            Config.restore_reuse_allow_hidden_state_models = true;
            job = new BackupJob("mow" + n++, dbId, UnitTestUtil.DB_NAME, tableRefsOf(UnitTestUtil.TABLE_NAME),
                    13600 * 1000, BackupStmt.BackupContent.ALL, env, repo.getId(), 0);
            job.setLogicalDigest(true);
            Assert.assertTrue(runToSnapshotTasks(job).get(0).isComputeLogicalDigest());
        } finally {
            Config.restore_reuse_allow_hidden_state_models = origHidden;
            Config.restore_reuse_mow = origMow;
            tbl.setSequenceMapCol(null);
            tbl.setEnableUniqueKeyMergeOnWrite(false);
            Deencapsulation.setField(tbl, "keysType", KeysType.DUP_KEYS);
        }
    }

    @Test
    public void testUniqueTableHasNoLogicalDigestInJobInfo() throws IOException {
        boolean orig = Config.restore_reuse_allow_hidden_state_models;
        OlapTable tbl = (OlapTable) db.getTableNullable(tblId);
        try {
            Config.restore_reuse_allow_hidden_state_models = false;
            Deencapsulation.setField(tbl, "keysType", KeysType.UNIQUE_KEYS);
            logicalDigestAsked = false;
            // the backend reports none, as it was not asked
            BackupJobInfo jobInfo = runBackupToSavedJobInfo(true, true, true, null);
            Assert.assertNull(onlyIndex(jobInfo).getLogicalDigest(tabletId));
            String json = new String(Files.readAllBytes(new File(job.getLocalJobInfoFilePath()).toPath()));
            Assert.assertFalse(json, json.contains("\"lra\""));
            Assert.assertFalse(json, json.contains("\"lna\""));
            Assert.assertTrue(jobInfo.hasManifest());
        } finally {
            Config.restore_reuse_allow_hidden_state_models = orig;
            Deencapsulation.setField(tbl, "keysType", KeysType.DUP_KEYS);
        }
    }

    @Test
    public void testLogicalDigestMaxTablets() {
        long orig = Config.backup_logical_digest_max_tablets;
        try {
            // two tablets
            for (long limit : new long[] {0, 2, 1}) {
                Config.backup_logical_digest_max_tablets = limit;
                BackupJob twoTables = new BackupJob("label_" + limit, dbId, UnitTestUtil.DB_NAME,
                        tableRefsOf(UnitTestUtil.TABLE_NAME, table2Name), 13600 * 1000,
                        BackupStmt.BackupContent.ALL, env, repo.getId(), 0);
                twoTables.setLogicalDigest(true);
                List<SnapshotTask> tasks = runToSnapshotTasks(twoTables);
                Assert.assertEquals(2, tasks.size());
                boolean exceeded = limit == 1;
                for (SnapshotTask task : tasks) {
                    // no limit (0) and not exceeded: asked for the digest; exceeded: not asked
                    Assert.assertEquals("limit " + limit, !exceeded, task.isComputeLogicalDigest());
                }
                if (exceeded) {
                    Assert.assertEquals("TOO_MANY_TABLETS:2>1", twoTables.getLogicalDigestSkipReason());
                    List<String> info = twoTables.getInfo();
                    Assert.assertEquals("SKIPPED:TOO_MANY_TABLETS:2>1", info.get(info.size() - 1));
                } else {
                    Assert.assertNull(twoTables.getLogicalDigestSkipReason());
                    List<String> info = twoTables.getInfo();
                    Assert.assertEquals("ON", info.get(info.size() - 1));
                }
            }
            // the limit does nothing if the digest is not asked for
            Config.backup_logical_digest_max_tablets = 1;
            BackupJob noDigest = new BackupJob("label_off", dbId, UnitTestUtil.DB_NAME,
                    tableRefsOf(UnitTestUtil.TABLE_NAME, table2Name), 13600 * 1000,
                    BackupStmt.BackupContent.ALL, env, repo.getId(), 0);
            runToSnapshotTasks(noDigest);
            Assert.assertNull(noDigest.getLogicalDigestSkipReason());
            List<String> info = noDigest.getInfo();
            Assert.assertEquals("OFF", info.get(info.size() - 1));
        } finally {
            Config.backup_logical_digest_max_tablets = orig;
        }
    }

    @Test
    public void testBackupWithTooManyTabletsRecordsNoDigestAndSucceeds() throws IOException {
        long orig = Config.backup_logical_digest_max_tablets;
        try {
            // one tablet, the limit is exceeded only by a limit below it: use the skip decided by the job directly
            Config.backup_logical_digest_max_tablets = 1;
            List<TableRef> refs = tableRefsOf(UnitTestUtil.TABLE_NAME, table2Name);
            job = new BackupJob("label", dbId, UnitTestUtil.DB_NAME, refs, 13600 * 1000,
                    BackupStmt.BackupContent.ALL, env, repo.getId(), 0);
            job.setLogicalDigest(true);
            List<SnapshotTask> tasks = runToSnapshotTasks(job);
            Assert.assertEquals(2, tasks.size());
            TBackend tBackend = new TBackend("", 0, 1);
            TStatus ok = new TStatus(TStatusCode.OK);
            for (SnapshotTask task : tasks) {
                Assert.assertFalse(task.toThrift().isSetComputeLogicalDigest());
                TFinishTaskRequest request = new TFinishTaskRequest(tBackend, TTaskType.MAKE_SNAPSHOT,
                        task.getSignature(), ok);
                request.setSnapshotFiles(Lists.newArrayList("1.dat", "1.idx", "1.hdr"));
                request.setSnapshotPath("/path/to/snapshot");
                request.setManifestRoot(sha('1'));
                // even a backend which reports a digest anyway is not recorded
                request.setLogicalDigest(reportedDigest("OK"));
                Assert.assertTrue(job.finishTabletSnapshotTask(task, request));
            }
            job.run();
            Assert.assertEquals(BackupJobState.UPLOAD_SNAPSHOT, job.getState());
            // keep on local: no upload, go on to the job info
            job.run();
            Assert.assertEquals(BackupJobState.UPLOADING, job.getState());
        } finally {
            Config.backup_logical_digest_max_tablets = orig;
        }
    }

    @Test
    public void testShowBackupLogicalDigestColumn() throws IOException {
        Assert.assertEquals("LogicalDigest", ShowBackupStmt.TITLE_NAMES.get(
                ShowBackupStmt.TITLE_NAMES.size() - 1));
        // Status is not changed
        Assert.assertEquals("Status", ShowBackupStmt.TITLE_NAMES.get(
                ShowBackupStmt.TITLE_NAMES.size() - 3));
        List<String> info = job.getInfo();
        Assert.assertEquals(ShowBackupStmt.TITLE_NAMES.size(), info.size());
        Assert.assertEquals("OFF", info.get(info.size() - 1));
        Assert.assertEquals("[OK]", info.get(info.size() - 3));

        runBackupToSavedJobInfo(true, true, true, reportedDigest("OK"));
        info = job.getInfo();
        Assert.assertEquals(ShowBackupStmt.TITLE_NAMES.size(), info.size());
        Assert.assertEquals("RECORDED:digest=1,no_digest=0,not_asked=0", info.get(info.size() - 1));
    }

    @Test
    public void testNoManifestIfDisabled() throws IOException {
        boolean orig = Config.enable_backup_manifest;
        try {
            Config.enable_backup_manifest = false;
            AgentTaskQueue.clearAllTasks();
            job.setManifestDigest(true);
            List<SnapshotTask> tasks = runToSnapshotTasks(job);
            // the field is not set at all, as by a frontend which does not know the manifest
            Assert.assertFalse(tasks.get(0).isWriteManifest());
            Assert.assertFalse(tasks.get(0).toThrift().isSetWriteManifest());
            Assert.assertTrue(tasks.get(0).toThrift().isSetListFiles());
            Assert.assertFalse(tasks.get(0).toThrift().isSetComputeDigest());
        } finally {
            Config.enable_backup_manifest = orig;
        }
    }

    @Test
    public void testJobInfoHasNoManifestIfDisabled() throws IOException {
        boolean orig = Config.enable_backup_manifest;
        try {
            Config.enable_backup_manifest = false;
            // even if the backend reports the roots (a backend which does not know the request field)
            BackupJobInfo jobInfo = runBackupToSavedJobInfo(true, true);
            Assert.assertFalse(jobInfo.hasManifest());
            Assert.assertNull(jobInfo.manifestVersion);
            Assert.assertNull(onlyIndex(jobInfo).getManifestRoot(tabletId));
            String json = new String(Files.readAllBytes(new File(job.getLocalJobInfoFilePath()).toPath()));
            Assert.assertFalse(json, json.contains("manifest"));
            Assert.assertFalse(json, json.contains("digest_algorithm"));
        } finally {
            Config.enable_backup_manifest = orig;
        }
    }

    @Test
    public void testUploadTaskWritesManifestOnlyIfAsked() {
        UploadTask task = new UploadTask(null, backendId, 1, 2, dbId, Maps.newHashMap(),
                new FsBroker("127.0.0.1", 8000), Maps.newHashMap(), StorageBackend.StorageType.BROKER, "loc");
        Assert.assertFalse(task.toThrift().isSetWriteManifest());
        task.setWriteManifest(true);
        Assert.assertTrue(task.toThrift().isSetWriteManifest());
        Assert.assertTrue(task.toThrift().isWriteManifest());
    }

    @Test
    public void testUploadTasksOfJobWithoutManifest() {
        boolean orig = Config.enable_backup_manifest;
        try {
            Config.enable_backup_manifest = false;
            List<SnapshotTask> tasks = runToSnapshotTasks(job);
            TBackend tBackend = new TBackend("", 0, 1);
            TFinishTaskRequest request = new TFinishTaskRequest(tBackend, TTaskType.MAKE_SNAPSHOT,
                    tasks.get(0).getSignature(), new TStatus(TStatusCode.OK));
            request.setSnapshotFiles(Lists.newArrayList("1.dat", "1.idx", "1.hdr"));
            request.setSnapshotPath("/path/to/snapshot");
            Assert.assertTrue(job.finishTabletSnapshotTask(tasks.get(0), request));
            job.run();
            Assert.assertEquals(BackupJobState.UPLOAD_SNAPSHOT, job.getState());
            AgentTaskQueue.clearAllTasks();
            job.run();
            Assert.assertEquals(BackupJobState.UPLOADING, job.getState());
            UploadTask upTask = (UploadTask) AgentTaskQueue.getTask(backendId, TTaskType.UPLOAD, id.get() - 1);
            Assert.assertFalse(upTask.isWriteManifest());
            Assert.assertFalse(upTask.toThrift().isSetWriteManifest());
        } finally {
            Config.enable_backup_manifest = orig;
        }
    }

    @Test
    public void testUploadTasksOfJobWithManifest() {
        boolean orig = Config.enable_backup_manifest;
        try {
            Config.enable_backup_manifest = true;
            List<SnapshotTask> tasks = runToSnapshotTasks(job);
            Assert.assertTrue(tasks.get(0).toThrift().isWriteManifest());
            TBackend tBackend = new TBackend("", 0, 1);
            TFinishTaskRequest request = new TFinishTaskRequest(tBackend, TTaskType.MAKE_SNAPSHOT,
                    tasks.get(0).getSignature(), new TStatus(TStatusCode.OK));
            request.setSnapshotFiles(Lists.newArrayList("1.dat", "1.idx", "1.hdr"));
            request.setSnapshotPath("/path/to/snapshot");
            Assert.assertTrue(job.finishTabletSnapshotTask(tasks.get(0), request));
            job.run();
            Assert.assertEquals(BackupJobState.UPLOAD_SNAPSHOT, job.getState());
            AgentTaskQueue.clearAllTasks();
            job.run();
            Assert.assertEquals(BackupJobState.UPLOADING, job.getState());
            UploadTask upTask = (UploadTask) AgentTaskQueue.getTask(backendId, TTaskType.UPLOAD, id.get() - 1);
            Assert.assertTrue(upTask.isWriteManifest());
            Assert.assertTrue(upTask.toThrift().isWriteManifest());
        } finally {
            Config.enable_backup_manifest = orig;
        }
    }

    @Test
    public void testLocalSnapshotManifestDigest() {
        List<TableRef> tableRefs = Lists.newArrayList(new TableRef(
                new TableName(InternalCatalog.INTERNAL_CATALOG_NAME, UnitTestUtil.DB_NAME, UnitTestUtil.TABLE_NAME),
                null));
        for (boolean manifestDigest : new boolean[] {false, true}) {
            AgentTaskQueue.clearAllTasks();
            job = new BackupJob("label", dbId, UnitTestUtil.DB_NAME, tableRefs, 13600 * 1000,
                    BackupStmt.BackupContent.ALL, env, Repository.KEEP_ON_LOCAL_REPO_ID, 0);
            if (manifestDigest) {
                job.setManifestDigest(true);
            }
            Assert.assertEquals(manifestDigest, job.isManifestDigest());
            job.run();
            Assert.assertEquals(BackupJobState.SNAPSHOTING, job.getState());
            SnapshotTask snapshotTask = (SnapshotTask) AgentTaskQueue.getTask(backendId, TTaskType.MAKE_SNAPSHOT,
                    id.get() - 1);
            Assert.assertEquals(manifestDigest, snapshotTask.toThrift().isSetComputeDigest());
            Assert.assertEquals(manifestDigest, snapshotTask.isComputeDigest());
            // the manifest is asked for by every snapshot task of a job (enable_backup_manifest is true)
            Assert.assertTrue(snapshotTask.toThrift().isWriteManifest());
        }
    }

    // ---------------------------------------------------------------------------------------------
    // backup_restore_merge_idle_ticks: the states which wait for nothing are run in the same round
    // ---------------------------------------------------------------------------------------------

    // runs the job to SNAPSHOTING and reports its snapshot task
    private void finishTheSnapshot(BackupJob job) {
        job.run();
        Assert.assertEquals(BackupJobState.SNAPSHOTING, job.getState());
        SnapshotTask snapshotTask = (SnapshotTask) AgentTaskQueue.getTask(backendId, TTaskType.MAKE_SNAPSHOT,
                id.get() - 1);
        TFinishTaskRequest request = new TFinishTaskRequest(new TBackend("", 0, 1), TTaskType.MAKE_SNAPSHOT,
                snapshotTask.getSignature(), new TStatus(TStatusCode.OK));
        request.setSnapshotFiles(Lists.newArrayList("1.dat", "1.idx", "1.hdr"));
        request.setSnapshotPath("/path/to/snapshot");
        Assert.assertTrue(job.finishTabletSnapshotTask(snapshotTask, request));
    }

    private BackupJob keepOnLocalJob() {
        List<TableRef> tableRefs = Lists.newArrayList(new TableRef(
                new TableName(InternalCatalog.INTERNAL_CATALOG_NAME, UnitTestUtil.DB_NAME, UnitTestUtil.TABLE_NAME),
                null));
        return new BackupJob("label", dbId, UnitTestUtil.DB_NAME, tableRefs, 13600 * 1000,
                BackupStmt.BackupContent.ALL, env, Repository.KEEP_ON_LOCAL_REPO_ID, 0);
    }

    // reports the upload task of the job
    private void finishTheUpload(BackupJob job) {
        UploadTask upTask = (UploadTask) AgentTaskQueue.getTask(backendId, TTaskType.UPLOAD, id.get() - 1);
        Map<Long, List<String>> tabletFileMap = Maps.newHashMap();
        tabletFileMap.put(tabletId, Lists.newArrayList("1.dat.4f158689243a3d6030352fec3cfd3798",
                "1.idx.4f158689243a3d6030352fec3cfd3798", "1.hdr.4f158689243a3d6030352fec3cfd3798"));
        TFinishTaskRequest request = new TFinishTaskRequest(new TBackend("", 0, 1), TTaskType.UPLOAD,
                upTask.getSignature(), new TStatus(TStatusCode.OK));
        request.setTabletFiles(tabletFileMap);
        Assert.assertTrue(job.finishSnapshotUploadTask(upTask, request));
    }

    @Test
    public void testMergedTicksKeepOnLocalBackupFinishesInOneRunAfterSnapshots() {
        boolean orig = Config.backup_restore_merge_idle_ticks;
        try {
            // the switch off: one state per run, as before
            Config.backup_restore_merge_idle_ticks = false;
            job = keepOnLocalJob();
            finishTheSnapshot(job);
            loggedStates.clear();
            job.run();
            Assert.assertEquals(BackupJobState.UPLOAD_SNAPSHOT, job.getState());
            Assert.assertEquals(Lists.newArrayList(BackupJobState.UPLOAD_SNAPSHOT), loggedStates);
            job.run();
            Assert.assertEquals(BackupJobState.UPLOADING, job.getState());
            job.run();
            Assert.assertEquals(BackupJobState.SAVE_META, job.getState());
            job.run();
            Assert.assertEquals(BackupJobState.UPLOAD_INFO, job.getState());
            job.run();
            Assert.assertEquals(BackupJobState.FINISHED, job.getState());
            List<BackupJobState> oneByOne = Lists.newArrayList(loggedStates);

            // the switch on: the same logs in the same order, in one run
            Config.backup_restore_merge_idle_ticks = true;
            AgentTaskQueue.clearAllTasks();
            job = keepOnLocalJob();
            finishTheSnapshot(job);
            loggedStates.clear();
            job.run();
            Assert.assertEquals(Status.OK, job.getStatus());
            Assert.assertEquals(BackupJobState.FINISHED, job.getState());
            Assert.assertEquals(oneByOne, loggedStates);
            Assert.assertEquals(Lists.newArrayList(BackupJobState.UPLOAD_SNAPSHOT, BackupJobState.SAVE_META,
                    BackupJobState.UPLOAD_INFO, BackupJobState.FINISHED), loggedStates);
            Assert.assertTrue(new File(job.getLocalJobInfoFilePath()).exists());
        } finally {
            Config.backup_restore_merge_idle_ticks = orig;
        }
    }

    @Test
    public void testMergedTicksStopAtUploadingWhenUploadTasksExist() {
        boolean orig = Config.backup_restore_merge_idle_ticks;
        try {
            Config.backup_restore_merge_idle_ticks = true;
            finishTheSnapshot(job);
            loggedStates.clear();
            AgentTaskQueue.clearAllTasks();
            // the upload tasks are sent, then the job waits for them
            job.run();
            Assert.assertEquals(BackupJobState.UPLOADING, job.getState());
            Assert.assertEquals(Lists.newArrayList(BackupJobState.UPLOAD_SNAPSHOT), loggedStates);
            Assert.assertEquals(1, AgentTaskQueue.getTaskNum());
            job.run();
            Assert.assertEquals(BackupJobState.UPLOADING, job.getState());
            Assert.assertEquals(1, loggedStates.size());
            // the upload is reported: the rest in one run
            finishTheUpload(job);
            job.run();
            Assert.assertEquals(Status.OK, job.getStatus());
            Assert.assertEquals(BackupJobState.FINISHED, job.getState());
            Assert.assertEquals(Lists.newArrayList(BackupJobState.UPLOAD_SNAPSHOT, BackupJobState.SAVE_META,
                    BackupJobState.UPLOAD_INFO, BackupJobState.FINISHED), loggedStates);
        } finally {
            Config.backup_restore_merge_idle_ticks = orig;
        }
    }

    // a failure of UPLOAD_INFO ends the run after the logs of the states before it, the job is not cancelled and the
    // next run retries it, as before (the status is not reset by the merged run)
    @Test
    public void testUploadInfoFailureStillRetriesNextRunWithMergedTicks() {
        boolean orig = Config.backup_restore_merge_idle_ticks;
        try {
            Config.backup_restore_merge_idle_ticks = true;
            finishTheSnapshot(job);
            loggedStates.clear();
            AgentTaskQueue.clearAllTasks();
            job.run();
            Assert.assertEquals(BackupJobState.UPLOADING, job.getState());
            finishTheUpload(job);

            uploadStatus = new Status(Status.ErrCode.COMMON_ERROR, "upload failed");
            job.run();
            Assert.assertEquals(BackupJobState.UPLOAD_INFO, job.getState());
            Assert.assertFalse(job.getStatus().ok());
            Assert.assertEquals(Lists.newArrayList(BackupJobState.UPLOAD_SNAPSHOT, BackupJobState.SAVE_META,
                    BackupJobState.UPLOAD_INFO), loggedStates);

            uploadStatus = Status.OK;
            job.run();
            Assert.assertEquals(BackupJobState.FINISHED, job.getState());
            Assert.assertEquals(Status.OK, job.getStatus());
            Assert.assertEquals(BackupJobState.FINISHED, loggedStates.get(loggedStates.size() - 1));
        } finally {
            Config.backup_restore_merge_idle_ticks = orig;
        }
    }

    // ---------------------------------------------------------------------------------------------
    // backup_restore_run_on_task_finish: the call sites of nudgeIfIdle in the finish reports of the tasks
    // ---------------------------------------------------------------------------------------------

    // The handler is wired as in the FE (env.getBackupHandler() is the one of setUp): it runs the job which is nudged. The nudge goes through the real
    // BackupHandler, so the job advances by itself only when a nudge is let through.
    private void armNudges(BackupJob nudged) {
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
        Deencapsulation.invoke(backupHandler, "addBackupOrRestoreJob", dbId, nudged);
        backupHandler.nudgedJobIds.clear();
    }

    // waits until everything the nudge thread has been given so far has run
    private void drainNudges() throws Exception {
        ExecutorService executor = Deencapsulation.getField(backupHandler, "nudgeExecutor");
        executor.submit(() -> { }).get(10, TimeUnit.SECONDS);
    }

    private BackupJob twoTablesJob() {
        List<TableRef> tableRefs = Lists.newArrayList();
        tableRefs.add(new TableRef(
                new TableName(InternalCatalog.INTERNAL_CATALOG_NAME, UnitTestUtil.DB_NAME, UnitTestUtil.TABLE_NAME),
                null));
        tableRefs.add(new TableRef(
                new TableName(InternalCatalog.INTERNAL_CATALOG_NAME, UnitTestUtil.DB_NAME, table2Name), null));
        return new BackupJob("label", dbId, UnitTestUtil.DB_NAME, tableRefs, 13600 * 1000,
                BackupStmt.BackupContent.ALL, env, repo.getId(), 0);
    }

    private TFinishTaskRequest snapshotReport(SnapshotTask task) {
        TFinishTaskRequest request = new TFinishTaskRequest(new TBackend("", 0, 1), TTaskType.MAKE_SNAPSHOT,
                task.getSignature(), new TStatus(TStatusCode.OK));
        request.setSnapshotFiles(Lists.newArrayList("1.dat", "1.idx", "1.hdr"));
        request.setSnapshotPath("/path/to/snapshot");
        return request;
    }

    // a job with 2 snapshot tasks (SnapshotTask.getTabletId() tells them apart), armed for the nudges
    private List<SnapshotTask> snapshotingJobWithTwoTasks() {
        AgentTaskQueue.clearAllTasks();
        job = twoTablesJob();
        job.run();
        Assert.assertEquals(BackupJobState.SNAPSHOTING, job.getState());
        List<SnapshotTask> tasks = new ArrayList<>();
        for (AgentTask task : AgentTaskQueue.getTask(TTaskType.MAKE_SNAPSHOT)) {
            tasks.add((SnapshotTask) task);
        }
        Assert.assertEquals(2, tasks.size());
        armNudges(job);
        return tasks;
    }

    // BackupJob.finishTabletSnapshotTask
    @Test
    public void testNudgeOnTheReportOfTheLastSnapshotTask() throws Exception {
        Config.backup_restore_run_on_task_finish = true;
        List<SnapshotTask> tasks = snapshotingJobWithTwoTasks();

        // the first report: the other task is still outstanding
        Assert.assertTrue(job.finishTabletSnapshotTask(tasks.get(0), snapshotReport(tasks.get(0))));
        drainNudges();
        Assert.assertTrue(backupHandler.nudgedJobIds.toString(), backupHandler.nudgedJobIds.isEmpty());
        Assert.assertEquals(BackupJobState.SNAPSHOTING, job.getState());

        // the last report: nudged once, and the nudge runs the job, which moves on without waiting for its round
        Assert.assertTrue(job.finishTabletSnapshotTask(tasks.get(1), snapshotReport(tasks.get(1))));
        drainNudges();
        Assert.assertEquals(Lists.newArrayList(job.getJobId()), backupHandler.nudgedJobIds);
        Assert.assertEquals(BackupJobState.UPLOAD_SNAPSHOT, job.getState());

        // a duplicate report of either task: not accepted, no nudge
        Assert.assertFalse(job.finishTabletSnapshotTask(tasks.get(1), snapshotReport(tasks.get(1))));
        Assert.assertFalse(job.finishTabletSnapshotTask(tasks.get(0), snapshotReport(tasks.get(0))));
        drainNudges();
        Assert.assertEquals(1, backupHandler.nudgedJobIds.size());
    }

    // a TABLET_MISSING report handled as a drop (the table was dropped meanwhile) is the last report: nudged too
    @Test
    public void testNudgeOnTheTabletMissingReportOfTheLastSnapshotTask() throws Exception {
        Config.backup_restore_run_on_task_finish = true;
        List<SnapshotTask> tasks = snapshotingJobWithTwoTasks();
        SnapshotTask existing = tasks.get(0).getTabletId() == tabletId ? tasks.get(0) : tasks.get(1);
        SnapshotTask dropped = existing == tasks.get(0) ? tasks.get(1) : tasks.get(0);
        db.unregisterTable(table2Name);
        try {
            Assert.assertTrue(job.finishTabletSnapshotTask(existing, snapshotReport(existing)));
            drainNudges();
            Assert.assertTrue(backupHandler.nudgedJobIds.toString(), backupHandler.nudgedJobIds.isEmpty());

            TStatus missing = new TStatus(TStatusCode.TABLET_MISSING);
            missing.setErrorMsgs(Lists.newArrayList("Tablet missing"));
            Assert.assertTrue(job.finishTabletSnapshotTask(dropped,
                    new TFinishTaskRequest(new TBackend("", 0, 1), TTaskType.MAKE_SNAPSHOT, dropped.getSignature(),
                            missing)));
            drainNudges();
            Assert.assertEquals(Lists.newArrayList(job.getJobId()), backupHandler.nudgedJobIds);
            Assert.assertEquals(BackupJobState.UPLOAD_SNAPSHOT, job.getState());
        } finally {
            if (db.getTableNullable(table2Name) == null && table2 != null) {
                db.registerTable(table2);
            }
        }
    }

    @Test
    public void testNoNudgeOnTheSnapshotReportWhenTheSwitchIsOff() throws Exception {
        Config.backup_restore_run_on_task_finish = false;
        List<SnapshotTask> tasks = snapshotingJobWithTwoTasks();
        Assert.assertTrue(job.finishTabletSnapshotTask(tasks.get(0), snapshotReport(tasks.get(0))));
        Assert.assertTrue(job.finishTabletSnapshotTask(tasks.get(1), snapshotReport(tasks.get(1))));
        drainNudges();
        // all tasks are reported, but the job waits for its next round
        Assert.assertEquals(BackupJobState.SNAPSHOTING, job.getState());
        job.run();
        Assert.assertEquals(BackupJobState.UPLOAD_SNAPSHOT, job.getState());
    }

    // the job of setUp, to UPLOADING with its (single) upload task, armed for the nudges
    private UploadTask uploadingJob() {
        boolean orig = Config.backup_restore_merge_idle_ticks;
        try {
            Config.backup_restore_merge_idle_ticks = true;
            finishTheSnapshot(job);
            AgentTaskQueue.clearAllTasks();
            job.run();
        } finally {
            Config.backup_restore_merge_idle_ticks = orig;
        }
        Assert.assertEquals(BackupJobState.UPLOADING, job.getState());
        armNudges(job);
        return (UploadTask) AgentTaskQueue.getTask(backendId, TTaskType.UPLOAD, id.get() - 1);
    }

    private TFinishTaskRequest uploadReport(UploadTask task, List<String> files) {
        Map<Long, List<String>> tabletFileMap = Maps.newHashMap();
        tabletFileMap.put(tabletId, files);
        TFinishTaskRequest request = new TFinishTaskRequest(new TBackend("", 0, 1), TTaskType.UPLOAD,
                task.getSignature(), new TStatus(TStatusCode.OK));
        request.setTabletFiles(tabletFileMap);
        return request;
    }

    private static final List<String> UPLOADED_FILES = Lists.newArrayList("1.dat.4f158689243a3d6030352fec3cfd3798",
            "1.idx.4f158689243a3d6030352fec3cfd3798", "1.hdr.4f158689243a3d6030352fec3cfd3798");

    // BackupJob.finishSnapshotUploadTask
    @Test
    public void testNudgeOnTheReportOfTheLastUploadTask() throws Exception {
        Config.backup_restore_run_on_task_finish = true;
        UploadTask upTask = uploadingJob();

        // a report which is not accepted (the files of the tablet differ): the task stays, no nudge
        Assert.assertFalse(job.finishSnapshotUploadTask(upTask,
                uploadReport(upTask, Lists.newArrayList("1.dat.4f158689243a3d6030352fec3cfd3798"))));
        drainNudges();
        Assert.assertTrue(backupHandler.nudgedJobIds.isEmpty());
        Assert.assertEquals(BackupJobState.UPLOADING, job.getState());

        // the last (only) task: nudged once, and the nudge runs the job to the end
        Assert.assertTrue(job.finishSnapshotUploadTask(upTask, uploadReport(upTask, UPLOADED_FILES)));
        drainNudges();
        Assert.assertEquals(Lists.newArrayList(job.getJobId()), backupHandler.nudgedJobIds);
        Assert.assertNotEquals(BackupJobState.UPLOADING, job.getState());

        // a duplicate report: no nudge
        Assert.assertFalse(job.finishSnapshotUploadTask(upTask, uploadReport(upTask, UPLOADED_FILES)));
        drainNudges();
        Assert.assertEquals(1, backupHandler.nudgedJobIds.size());
    }

    @Test
    public void testNoNudgeOnTheUploadReportWhileAnotherTaskIsOutstanding() throws Exception {
        Config.backup_restore_run_on_task_finish = true;
        UploadTask upTask = uploadingJob();
        Map<Long, Long> unfinished = Deencapsulation.getField(job, "unfinishedTaskIds");
        long other = 987654321L;
        unfinished.put(other, backendId);

        Assert.assertTrue(job.finishSnapshotUploadTask(upTask, uploadReport(upTask, UPLOADED_FILES)));
        drainNudges();
        Assert.assertTrue(backupHandler.nudgedJobIds.isEmpty());
        Assert.assertEquals(BackupJobState.UPLOADING, job.getState());
        Assert.assertEquals(1, unfinished.size());
    }

    @Test
    public void testNoNudgeOnTheUploadReportWhenTheSwitchIsOff() throws Exception {
        Config.backup_restore_run_on_task_finish = false;
        UploadTask upTask = uploadingJob();
        Assert.assertTrue(job.finishSnapshotUploadTask(upTask, uploadReport(upTask, UPLOADED_FILES)));
        drainNudges();
        Assert.assertEquals(BackupJobState.UPLOADING, job.getState());
        job.run();
        Assert.assertNotEquals(BackupJobState.UPLOADING, job.getState());
    }
}
