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
import org.apache.doris.backup.BackupJob.BackupJobState;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.DynamicPartitionProperty;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.FsBroker;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.MaterializedIndex.IndexExtState;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.TableProperty;
import org.apache.doris.catalog.info.TableNameInfo;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.UserException;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.common.util.UnitTestUtil;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.datasource.storage.StorageAdapter;
import org.apache.doris.fs.FileSystemDescriptor;
import org.apache.doris.info.TableRefInfo;
import org.apache.doris.nereids.trees.plans.commands.BackupCommand;
import org.apache.doris.nereids.trees.plans.commands.ShowBackupCommand;
import org.apache.doris.persist.EditLog;
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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.file.FileVisitOption;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

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

    private Env env = Mockito.mock(Env.class);
    private InternalCatalog catalog = Mockito.mock(InternalCatalog.class);

    private BackupHandler backupHandler;

    private RepositoryMgr repoMgr;

    public BackupJobTest() throws UserException {
    }

    private EditLog editLog = Mockito.mock(EditLog.class);

    private Repository repo = Mockito.spy(new Repository(repoId, "repo", false, "my_repo",
            StorageAdapter.ofBroker("broker", Maps.newHashMap())));

    private MockedStatic<Env> mockedEnvStatic;
    private MockedStatic<AgentTaskExecutor> mockedAgentTaskExecutor;
    private MockedConstruction<FileSystemDescriptor> mockedFsDescriptor;

    @BeforeAll
    public static void start() {
        Config.tmp_dir = "./";
        File backupDir = new File(BackupHandler.BACKUP_ROOT_DIR.toString());
        backupDir.mkdirs();
    }

    @AfterAll
    public static void end() throws IOException {
        Config.tmp_dir = "./";
        File backupDir = new File(BackupHandler.BACKUP_ROOT_DIR.toString());
        if (backupDir.exists()) {
            Files.walk(BackupHandler.BACKUP_ROOT_DIR,
                       FileVisitOption.FOLLOW_LINKS).sorted(Comparator.reverseOrder()).map(Path::toFile).forEach(File::delete);
        }
    }

    private boolean origMergeIdleTicks;
    private boolean origRunOnTaskFinish;

    @BeforeEach
    public void setUp() {
        // the tests step the job one state per run(); the merged rounds are tested with the switch set explicitly
        origMergeIdleTicks = Config.backup_restore_merge_idle_ticks;
        origRunOnTaskFinish = Config.backup_restore_run_on_task_finish;
        Config.backup_restore_merge_idle_ticks = false;
        Config.backup_restore_run_on_task_finish = false;
        repoMgr = Mockito.mock(RepositoryMgr.class);
        backupHandler = Mockito.mock(BackupHandler.class);

        db = UnitTestUtil.createDb(dbId, tblId, partId, idxId, tabletId, backendId, version);

        // Create second table in setUp to avoid Env initialization issues
        table2 = UnitTestUtil.createTable(db, tblId2, table2Name, partId2, idxId2, tabletId2, backendId, version);

        // Mock Env static methods
        mockedEnvStatic = Mockito.mockStatic(Env.class);
        mockedEnvStatic.when(Env::getCurrentEnv).thenReturn(env);
        mockedEnvStatic.when(Env::getCurrentEnvJournalVersion).thenReturn(FeConstants.meta_version);

        // Mock instance methods on env
        Mockito.when(env.getInternalCatalog()).thenReturn(catalog);
        Mockito.when(env.getNextId()).thenAnswer(inv -> id.getAndIncrement());
        Mockito.when(env.getEditLog()).thenReturn(editLog);
        Mockito.when(env.getBackupHandler()).thenReturn(backupHandler);

        // Mock repository lookup through Env.getCurrentEnv().getBackupHandler().getRepoMgr()
        Mockito.when(backupHandler.getRepoMgr()).thenReturn(repoMgr);
        Mockito.when(repoMgr.getRepo(Mockito.anyLong())).thenReturn(repo);

        // Mock instance methods on catalog
        Mockito.when(catalog.getDbNullable(ArgumentMatchers.anyLong())).thenReturn(db);
        Mockito.when(catalog.getTableByTableId(ArgumentMatchers.anyLong())).thenAnswer(inv -> {
            Long tableId = inv.getArgument(0);
            return db.getTableNullable(tableId);
        });

        // Mock editLog
        Mockito.doAnswer(inv -> {
            BackupJob job = inv.getArgument(0);
            System.out.println("log backup job: " + job);
            return null;
        }).when(editLog).logBackupJob(ArgumentMatchers.any(BackupJob.class));

        // Mock AgentTaskExecutor static method
        mockedAgentTaskExecutor = Mockito.mockStatic(AgentTaskExecutor.class);

        // Mock Repository instance methods via spy
        Mockito.doReturn(Status.OK).when(repo).upload(ArgumentMatchers.anyString(), ArgumentMatchers.anyString());
        Mockito.doAnswer(inv -> {
            List<FsBroker> brokerAddrs = inv.getArgument(2);
            brokerAddrs.add(new FsBroker());
            return Status.OK;
        }).when(repo).getBrokerAddress(ArgumentMatchers.any(Long.class), ArgumentMatchers.any(Env.class), ArgumentMatchers.anyList());
        FileSystemDescriptor fileSystemDescriptor = Mockito.mock(FileSystemDescriptor.class);
        Mockito.when(fileSystemDescriptor.getBackendConfigProperties()).thenReturn(Maps.newHashMap());
        Mockito.when(fileSystemDescriptor.getThriftStorageType()).thenReturn(StorageBackend.StorageType.BROKER);
        Mockito.doReturn(fileSystemDescriptor).when(repo).getFileSystemDescriptor();

        // Mock FileSystemDescriptor construction
        mockedFsDescriptor = Mockito.mockConstruction(FileSystemDescriptor.class, (mock, ctx) -> {
            Mockito.when(mock.getBackendConfigProperties()).thenReturn(Maps.newHashMap());
        });

        // Only include first table to ensure other tests are not affected
        List<TableRefInfo> tableRefs = Lists.newArrayList();
        tableRefs.add(new TableRefInfo(
                new TableNameInfo(InternalCatalog.INTERNAL_CATALOG_NAME, UnitTestUtil.DB_NAME, UnitTestUtil.TABLE_NAME),
                null,
                null,
                null,
                new ArrayList<>(),
                null,
                null,
                new ArrayList<>()));
        job = new BackupJob("label", dbId, UnitTestUtil.DB_NAME, tableRefs, 13600 * 1000, BackupCommand.BackupContent.ALL,
                env, repo.getId(), 0);
    }

    @AfterEach
    public void tearDown() {
        Config.backup_restore_merge_idle_ticks = origMergeIdleTicks;
        Config.backup_restore_run_on_task_finish = origRunOnTaskFinish;
        AgentTaskQueue.clearAllTasks();
        if (mockedEnvStatic != null) {
            mockedEnvStatic.close();
        }
        if (mockedAgentTaskExecutor != null) {
            mockedAgentTaskExecutor.close();
        }
        if (mockedFsDescriptor != null) {
            mockedFsDescriptor.close();
        }
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
        Assertions.assertEquals(BackupJobState.PENDING, job.getState());
        job.run();
        Assertions.assertEquals(Status.OK, job.getStatus());
        Assertions.assertEquals(BackupJobState.SNAPSHOTING, job.getState());

        BackupMeta backupMeta = job.getBackupMeta();
        Assertions.assertEquals(1, backupMeta.getTables().size());
        OlapTable backupTbl = (OlapTable) backupMeta.getTable(UnitTestUtil.TABLE_NAME);
        List<String> partNames = Lists.newArrayList(backupTbl.getPartitionNames());
        Assertions.assertNotNull(backupTbl);
        Assertions.assertEquals(backupTbl.getSignature(BackupHandler.SIGNATURE_VERSION, partNames),
                            ((OlapTable) db.getTableNullable(tblId)).getSignature(BackupHandler.SIGNATURE_VERSION, partNames));
        Assertions.assertEquals(1, AgentTaskQueue.getTaskNum());
        AgentTask task = AgentTaskQueue.getTask(backendId, TTaskType.MAKE_SNAPSHOT, id.get() - 1);
        Assertions.assertTrue(task instanceof SnapshotTask);
        SnapshotTask snapshotTask = (SnapshotTask) task;

        // 2. snapshoting
        job.run();
        Assertions.assertEquals(Status.OK, job.getStatus());
        Assertions.assertEquals(BackupJobState.SNAPSHOTING, job.getState());

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
        Assertions.assertTrue(job.finishTabletSnapshotTask(snapshotTask, request));
        job.run();
        Assertions.assertEquals(Status.OK, job.getStatus());
        Assertions.assertEquals(BackupJobState.UPLOAD_SNAPSHOT, job.getState());

        // 4. upload snapshots
        AgentTaskQueue.clearAllTasks();
        job.run();
        Assertions.assertEquals(Status.OK, job.getStatus());
        Assertions.assertEquals(BackupJobState.UPLOADING, job.getState());
        Assertions.assertEquals(1, AgentTaskQueue.getTaskNum());
        task = AgentTaskQueue.getTask(backendId, TTaskType.UPLOAD, id.get() - 1);
        Assertions.assertTrue(task instanceof UploadTask);
        UploadTask upTask = (UploadTask) task;

        Assertions.assertEquals(job.getJobId(), upTask.getJobId());
        Map<String, String> srcToDest = upTask.getSrcToDestPath();
        Assertions.assertEquals(1, srcToDest.size());
        String dest = srcToDest.get(snapshotPath + "/" + tabletId + "/" + 0);
        Assertions.assertNotNull(dest);

        // 5. uploading
        job.run();
        Assertions.assertEquals(Status.OK, job.getStatus());
        Assertions.assertEquals(BackupJobState.UPLOADING, job.getState());
        Map<Long, List<String>> tabletFileMap = Maps.newHashMap();
        request = new TFinishTaskRequest(tBackend, TTaskType.UPLOAD,
                upTask.getSignature(), taskStatus);
        request.setTabletFiles(tabletFileMap);

        Assertions.assertFalse(job.finishSnapshotUploadTask(upTask, request));
        List<String> tabletFiles = Lists.newArrayList();
        tabletFileMap.put(tabletId, tabletFiles);
        Assertions.assertFalse(job.finishSnapshotUploadTask(upTask, request));
        tabletFiles.add("1.dat.4f158689243a3d6030352fec3cfd3798");
        tabletFiles.add("wrong_files.idx.4f158689243a3d6030352fec3cfd3798");
        tabletFiles.add("wrong_files.hdr.4f158689243a3d6030352fec3cfd3798");
        Assertions.assertFalse(job.finishSnapshotUploadTask(upTask, request));
        tabletFiles.clear();
        tabletFiles.add("1.dat.4f158689243a3d6030352fec3cfd3798");
        tabletFiles.add("1.idx.4f158689243a3d6030352fec3cfd3798");
        tabletFiles.add("1.hdr.4f158689243a3d6030352fec3cfd3798");
        Assertions.assertTrue(job.finishSnapshotUploadTask(upTask, request));
        job.run();
        Assertions.assertEquals(Status.OK, job.getStatus());
        Assertions.assertEquals(BackupJobState.SAVE_META, job.getState());

        // 6. save meta
        job.run();
        Assertions.assertEquals(Status.OK, job.getStatus());
        Assertions.assertEquals(BackupJobState.UPLOAD_INFO, job.getState());
        File metaInfo = new File(job.getLocalMetaInfoFilePath());
        Assertions.assertTrue(metaInfo.exists());
        File jobInfo = new File(job.getLocalJobInfoFilePath());
        Assertions.assertTrue(jobInfo.exists());

        BackupMeta restoreMetaInfo = null;
        BackupJobInfo restoreJobInfo = null;
        try {
            restoreMetaInfo = BackupMeta.fromFile(job.getLocalMetaInfoFilePath(), FeConstants.meta_version);
            Assertions.assertEquals(1, restoreMetaInfo.getTables().size());
            OlapTable olapTable = (OlapTable) restoreMetaInfo.getTable(tblId);
            Assertions.assertNotNull(olapTable);
            Assertions.assertNotNull(restoreMetaInfo.getTable(UnitTestUtil.TABLE_NAME));
            List<String> names = Lists.newArrayList(olapTable.getPartitionNames());
            Assertions.assertEquals(((OlapTable) db.getTableNullable(tblId)).getSignature(BackupHandler.SIGNATURE_VERSION, names),
                                olapTable.getSignature(BackupHandler.SIGNATURE_VERSION, names));

            restoreJobInfo = BackupJobInfo.fromFile(job.getLocalJobInfoFilePath());
            Assertions.assertEquals(UnitTestUtil.DB_NAME, restoreJobInfo.dbName);
            Assertions.assertEquals(job.getLabel(), restoreJobInfo.name);
            Assertions.assertEquals(1, restoreJobInfo.backupOlapTableObjects.values().size());
        } catch (IOException e) {
            e.printStackTrace();
            Assertions.fail();
        }

        Assertions.assertNull(job.getBackupMeta());
        Assertions.assertNull(job.getJobInfo());

        // 7. upload_info
        job.run();
        Assertions.assertEquals(Status.OK, job.getStatus());
        Assertions.assertEquals(BackupJobState.FINISHED, job.getState());
    }

    @Test
    public void testBackupCopyTableWithDirtyDynamicPartitionStorageMedium() {
        Map<String, String> dirtyProperties = Maps.newHashMap();
        dirtyProperties.put(DynamicPartitionProperty.STORAGE_MEDIUM, "hdd");
        table2.setTableProperty(new TableProperty(dirtyProperties));

        Assertions.assertFalse(table2.dynamicPartitionExists());
        OlapTable copied = table2.selectiveCopy(null, IndexExtState.VISIBLE, true);
        Assertions.assertNotNull(copied);
        Assertions.assertFalse(copied.dynamicPartitionExists());
        Assertions.assertTrue(copied.getTableProperty().hasInvalidDynamicPartition());
    }

    @Test
    public void testBackupCopyTableWithDirtyDynamicPartitionStoragePolicy() {
        Map<String, String> dirtyProperties = Maps.newHashMap();
        dirtyProperties.put(DynamicPartitionProperty.STORAGE_POLICY, "test_policy");
        table2.setTableProperty(new TableProperty(dirtyProperties));

        Assertions.assertFalse(table2.dynamicPartitionExists());
        OlapTable copied = table2.selectiveCopy(null, IndexExtState.VISIBLE, true);
        Assertions.assertNotNull(copied);
        Assertions.assertFalse(copied.dynamicPartitionExists());
        Assertions.assertTrue(copied.getTableProperty().hasInvalidDynamicPartition());
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

        List<TableRefInfo> tableRefs = Lists.newArrayList();
        tableRefs.add(new TableRefInfo(
                new TableNameInfo(InternalCatalog.INTERNAL_CATALOG_NAME, UnitTestUtil.DB_NAME, "unknown_tbl"),
                null,
                null,
                null,
                new ArrayList<>(),
                null,
                null,
                new ArrayList<>()));
        job = new BackupJob("label", dbId, UnitTestUtil.DB_NAME, tableRefs, 13600 * 1000, BackupCommand.BackupContent.ALL,
                env, repo.getId(), 0);
        job.run();
        Assertions.assertEquals(Status.ErrCode.NOT_FOUND, job.getStatus().getErrCode());
        Assertions.assertEquals(BackupJobState.CANCELLED, job.getState());
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

        List<TableRefInfo> tableRefs = Lists.newArrayList();
        // Add normal table
        tableRefs.add(new TableRefInfo(
                new TableNameInfo(InternalCatalog.INTERNAL_CATALOG_NAME, UnitTestUtil.DB_NAME, UnitTestUtil.TABLE_NAME),
                null,
                null,
                null,
                new ArrayList<>(),
                null,
                null,
                new ArrayList<>()));
        // Add non-existent table
        tableRefs.add(new TableRefInfo(
                new TableNameInfo(InternalCatalog.INTERNAL_CATALOG_NAME, UnitTestUtil.DB_NAME, "unknown_tbl"),
                null,
                null,
                null,
                new ArrayList<>(),
                null,
                null,
                new ArrayList<>()));

        job = new BackupJob("label", dbId, UnitTestUtil.DB_NAME, tableRefs, 13600 * 1000, BackupCommand.BackupContent.ALL,
                env, repo.getId(), 0);

        // 1. pending
        Assertions.assertEquals(BackupJobState.PENDING, job.getState());
        job.run();
        Assertions.assertEquals(Status.OK, job.getStatus());
        Assertions.assertEquals(BackupJobState.SNAPSHOTING, job.getState());

        // Verify backup meta only contains the normal table
        BackupMeta backupMeta = job.getBackupMeta();
        Assertions.assertEquals(1, backupMeta.getTables().size());
        OlapTable backupTbl = (OlapTable) backupMeta.getTable(UnitTestUtil.TABLE_NAME);
        Assertions.assertNotNull(backupTbl);
        Assertions.assertNull(backupMeta.getTable("unknown_tbl"));

        // Verify only snapshot tasks for the normal table are created
        Assertions.assertEquals(1, AgentTaskQueue.getTaskNum());
        AgentTask task = AgentTaskQueue.getTask(backendId, TTaskType.MAKE_SNAPSHOT, id.get() - 1);
        Assertions.assertTrue(task instanceof SnapshotTask);
        SnapshotTask snapshotTask = (SnapshotTask) task;
        Assertions.assertEquals(tblId, snapshotTask.getTableId());
        Assertions.assertEquals(dbId, snapshotTask.getDbId());
        Assertions.assertEquals(partId, snapshotTask.getPartitionId());
        Assertions.assertEquals(idxId, snapshotTask.getIndexId());
        Assertions.assertEquals(tabletId, snapshotTask.getTabletId());
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

            List<TableRefInfo> tableRefs = Lists.newArrayList();
            tableRefs.add(new TableRefInfo(
                    new TableNameInfo(InternalCatalog.INTERNAL_CATALOG_NAME, UnitTestUtil.DB_NAME, UnitTestUtil.TABLE_NAME),
                    null,
                    null,
                    null,
                    new ArrayList<>(),
                    null,
                    null,
                    new ArrayList<>()));
            tableRefs.add(new TableRefInfo(
                    new TableNameInfo(InternalCatalog.INTERNAL_CATALOG_NAME, UnitTestUtil.DB_NAME, table2Name),
                    null,
                    null,
                    null,
                    new ArrayList<>(),
                    null,
                    null,
                    new ArrayList<>()));

            job = new BackupJob("label", dbId, UnitTestUtil.DB_NAME, tableRefs, 13600 * 1000, BackupCommand.BackupContent.ALL,
                    env, repo.getId(), 0);

            // 1. pending - should create snapshot tasks for both tables
            Assertions.assertEquals(BackupJobState.PENDING, job.getState());
            job.run();
            Assertions.assertEquals(Status.OK, job.getStatus());
            Assertions.assertEquals(BackupJobState.SNAPSHOTING, job.getState());

            // Verify backup meta contains both tables initially
            BackupMeta backupMeta = job.getBackupMeta();
            Assertions.assertEquals(2, backupMeta.getTables().size());
            Assertions.assertNotNull(backupMeta.getTable(UnitTestUtil.TABLE_NAME));
            Assertions.assertNotNull(backupMeta.getTable(table2Name));

            // Verify snapshot tasks are created for both tables
            Assertions.assertEquals(2, AgentTaskQueue.getTaskNum());

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
            Assertions.assertTrue(job.finishTabletSnapshotTask(taskForDroppedTable, requestMissing));

            // Finish task for existing table
            String snapshotPath = "/path/to/snapshot";
            List<String> snapshotFiles = Lists.newArrayList("1.dat", "1.idx", "1.hdr");
            TStatus taskStatusOK = new TStatus(TStatusCode.OK);
            TFinishTaskRequest requestOK = new TFinishTaskRequest(tBackend, TTaskType.MAKE_SNAPSHOT,
                    taskForExistingTable.getSignature(), taskStatusOK);
            requestOK.setSnapshotFiles(snapshotFiles);
            requestOK.setSnapshotPath(snapshotPath);
            Assertions.assertTrue(job.finishTabletSnapshotTask(taskForExistingTable, requestOK));

            // 4. Continue the backup process
            job.run();
            Assertions.assertEquals(Status.OK, job.getStatus());
            Assertions.assertEquals(BackupJobState.UPLOAD_SNAPSHOT, job.getState());

            AgentTaskQueue.clearAllTasks();
            job.run(); // UPLOAD_SNAPSHOT -> UPLOADING
            Assertions.assertEquals(1, AgentTaskQueue.getTaskNum());
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
            Assertions.assertTrue(job.finishSnapshotUploadTask(upTask, requestUpload));

            job.run(); // UPLOADING -> SAVE_META
            Assertions.assertEquals(BackupJobState.SAVE_META, job.getState());

            job.run(); // SAVE_META -> UPLOAD_INFO
            Assertions.assertEquals(BackupJobState.UPLOAD_INFO, job.getState());

            job.run(); // UPLOAD_INFO -> FINISHED
            Assertions.assertEquals(BackupJobState.FINISHED, job.getState());

        } catch (Throwable e) {
            e.printStackTrace();
            Assertions.fail(e.getMessage());
        } finally {
            // Clean up: re-register the second table if it was removed
            if (db.getTableNullable(table2Name) == null && table2 != null) {
                db.registerTable(table2);
            }
        }
    }

    /**
     * The snapshot task of a dropped table is the last one the SNAPSHOTING state waits for: its TABLET_MISSING
     * report, handled as a drop, nudges the job like the report of a finished task (backup_restore_run_on_task_finish).
     */
    @Test
    public void testTabletMissingLastTaskNudges() {
        try {
            AgentTaskQueue.clearAllTasks();
            List<TableRefInfo> tableRefs = Lists.newArrayList();
            for (String name : new String[] {UnitTestUtil.TABLE_NAME, table2Name}) {
                tableRefs.add(new TableRefInfo(
                        new TableNameInfo(InternalCatalog.INTERNAL_CATALOG_NAME, UnitTestUtil.DB_NAME, name),
                        null, null, null, new ArrayList<>(), null, null, new ArrayList<>()));
            }
            job = new BackupJob("label", dbId, UnitTestUtil.DB_NAME, tableRefs, 13600 * 1000,
                    BackupCommand.BackupContent.ALL, env, repo.getId(), 0);
            job.run();
            Assertions.assertEquals(BackupJobState.SNAPSHOTING, job.getState());
            Assertions.assertEquals(2, AgentTaskQueue.getTaskNum());
            db.unregisterTable(table2Name);

            SnapshotTask first = (SnapshotTask) AgentTaskQueue.getTask(backendId, TTaskType.MAKE_SNAPSHOT, id.get() - 2);
            SnapshotTask second = (SnapshotTask) AgentTaskQueue.getTask(backendId, TTaskType.MAKE_SNAPSHOT, id.get() - 1);
            SnapshotTask existing = first.getTabletId() == tabletId ? first : second;
            SnapshotTask dropped = existing == first ? second : first;
            TBackend tBackend = new TBackend("", 0, 1);
            Mockito.clearInvocations(backupHandler);

            // the existing table first: one task is still unfinished, no nudge
            TFinishTaskRequest ok = new TFinishTaskRequest(tBackend, TTaskType.MAKE_SNAPSHOT,
                    existing.getSignature(), new TStatus(TStatusCode.OK));
            ok.setSnapshotFiles(Lists.newArrayList("1.dat", "1.idx", "1.hdr"));
            ok.setSnapshotPath("/path/to/snapshot");
            Assertions.assertTrue(job.finishTabletSnapshotTask(existing, ok));
            Mockito.verify(backupHandler, Mockito.never()).nudge(Mockito.anyLong(), Mockito.anyLong());

            // the dropped one last: handled as a drop, and it is the last task, so the job is nudged once
            TStatus missing = new TStatus(TStatusCode.TABLET_MISSING);
            missing.setErrorMsgs(Lists.newArrayList("Tablet missing"));
            Assertions.assertTrue(job.finishTabletSnapshotTask(dropped,
                    new TFinishTaskRequest(tBackend, TTaskType.MAKE_SNAPSHOT, dropped.getSignature(), missing)));
            Mockito.verify(backupHandler, Mockito.times(1)).nudge(dbId, job.getJobId());

            job.run();
            Assertions.assertEquals(Status.OK, job.getStatus());
            Assertions.assertEquals(BackupJobState.UPLOAD_SNAPSHOT, job.getState());
        } finally {
            AgentTaskQueue.clearAllTasks();
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

        List<TableRefInfo> tableRefs = Lists.newArrayList();
        tableRefs.add(
                new TableRefInfo(
                        new TableNameInfo(InternalCatalog.INTERNAL_CATALOG_NAME, UnitTestUtil.DB_NAME, UnitTestUtil.TABLE_NAME),
                        null,
                        null,
                        null,
                        new ArrayList<>(),
                        null,
                        null,
                        new ArrayList<>()));
        job = new BackupJob("label", dbId, UnitTestUtil.DB_NAME, tableRefs, 13600 * 1000, BackupCommand.BackupContent.ALL,
            env, repo.getId(), 123);

        job.write(out);
        out.flush();
        out.close();

        // 2. Read objects from file
        DataInputStream in = new DataInputStream(Files.newInputStream(path));

        BackupJob job2 = BackupJob.read(in);

        Assertions.assertEquals(job.getJobId(), job2.getJobId());
        Assertions.assertEquals(job.getDbId(), job2.getDbId());
        Assertions.assertEquals(job.getCreateTime(), job2.getCreateTime());
        Assertions.assertEquals(job.getType(), job2.getType());
        Assertions.assertEquals(job.getCommitSeq(), job2.getCommitSeq());

        // 3. delete files
        in.close();
        Files.delete(path);
    }

    private static final String MD5 = "4f158689243a3d6030352fec3cfd3798";

    // Whether the snapshot task asks the backend for the logical digest when the property is on, false for a unique
    // table with the default config.
    private boolean logicalDigestAsked = true;

    private static String sha(char c) {
        return String.valueOf(c).repeat(64).toLowerCase();
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
        Assertions.assertEquals(BackupJobState.SNAPSHOTING, job.getState());
        SnapshotTask snapshotTask = (SnapshotTask) AgentTaskQueue.getTask(backendId, TTaskType.MAKE_SNAPSHOT,
                id.get() - 1);
        // a remote repository computes the digests when uploading, not in the snapshot.
        Assertions.assertFalse(snapshotTask.toThrift().isSetComputeDigest());
        boolean asked = logicalDigest && logicalDigestAsked;
        Assertions.assertEquals(asked, snapshotTask.isComputeLogicalDigest());
        Assertions.assertEquals(asked, snapshotTask.toThrift().isSetComputeLogicalDigest());

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
        Assertions.assertTrue(job.finishTabletSnapshotTask(snapshotTask, request));
        job.run();
        Assertions.assertEquals(BackupJobState.UPLOAD_SNAPSHOT, job.getState());

        AgentTaskQueue.clearAllTasks();
        job.run();
        Assertions.assertEquals(BackupJobState.UPLOADING, job.getState());
        UploadTask upTask = (UploadTask) AgentTaskQueue.getTask(backendId, TTaskType.UPLOAD, id.get() - 1);
        request = new TFinishTaskRequest(tBackend, TTaskType.UPLOAD, upTask.getSignature(), ok);
        Map<Long, List<String>> tabletFileMap = Maps.newHashMap();
        tabletFileMap.put(tabletId, Lists.newArrayList("1.dat." + MD5, "1.idx." + MD5, "1.hdr." + MD5));
        request.setTabletFiles(tabletFileMap);
        if (uploadRoot) {
            Map<Long, String> roots = Maps.newHashMap();
            roots.put(tabletId, sha('A'));
            request.setManifestRoots(roots);
        }
        Assertions.assertTrue(job.finishSnapshotUploadTask(upTask, request));
        job.run();
        Assertions.assertEquals(BackupJobState.SAVE_META, job.getState());
        job.run();
        Assertions.assertEquals(BackupJobState.UPLOAD_INFO, job.getState());
        return BackupJobInfo.fromFile(job.getLocalJobInfoFilePath());
    }

    @Test
    public void testBackupWritesManifest() throws IOException {
        BackupJobInfo jobInfo = runBackupToSavedJobInfo(true, true);
        Assertions.assertEquals(BackupJobInfo.MANIFEST_VERSION, (int) jobInfo.manifestVersion);
        Assertions.assertEquals(BackupJobInfo.DIGEST_SHA256, jobInfo.digestAlgorithm);
        Assertions.assertTrue(jobInfo.hasManifest());

        // the root of the manifest uploaded to the repository, not the one written with the snapshot
        Assertions.assertEquals(sha('a'), onlyIndex(jobInfo).getManifestRoot(tabletId));

        // read by position, there is no view object of the tablet
        BackupJobInfo.BackupIndexInfo idxInfo = jobInfo.backupOlapTableObjects.values().iterator().next()
                .partitions.values().iterator().next().indexes.values().iterator().next();
        BackupJobInfo.BackupTabletInfo tabletInfo = idxInfo.sortedTabletInfoList.get(0);
        Assertions.assertEquals(sha('a'), idxInfo.getManifestRootAt(0));
        // the existing files are unchanged
        Assertions.assertEquals(Lists.newArrayList("1.dat." + MD5, "1.idx." + MD5, "1.hdr." + MD5),
                tabletInfo.files);
    }

    @Test
    public void testBackupWithoutManifestIfUploadHasNoManifestRoot() throws IOException {
        // the snapshot wrote a manifest, but the upload reports no root (e.g. the backend is downgraded).
        BackupJobInfo jobInfo = runBackupToSavedJobInfo(true, false);
        Assertions.assertNull(jobInfo.manifestVersion);
        Assertions.assertNull(jobInfo.digestAlgorithm);
        Assertions.assertFalse(jobInfo.hasManifest());
        Assertions.assertNull(onlyIndex(jobInfo).getManifestRoot(tabletId));
        // same as a backup of an old version: no new key in the job info file.
        String json = new String(Files.readAllBytes(new File(job.getLocalJobInfoFilePath()).toPath()));
        Assertions.assertFalse(json.contains("manifest"), json);
        Assertions.assertFalse(json.contains("digest_algorithm"), json);
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
        Assertions.assertNotNull(digest);
        Assertions.assertTrue(digest.hasDigest());
        Assertions.assertEquals(1, (int) digest.algoVersion);
        Assertions.assertEquals(sha('5'), digest.schemaSig);
        Assertions.assertEquals(sha('b'), digest.root);
        Assertions.assertNull(digest.reason);
        BackupJobInfo.BackupIndexInfo idxInfo = jobInfo.backupOlapTableObjects.values().iterator().next()
                .partitions.values().iterator().next().indexes.values().iterator().next();
        Assertions.assertEquals(digest.root, idxInfo.getLogicalDigestAt(0).root);
        // the manifest is not affected
        Assertions.assertEquals(sha('a'), onlyIndex(jobInfo).getManifestRoot(tabletId));
        String json = new String(Files.readAllBytes(new File(job.getLocalJobInfoFilePath()).toPath()));
        // the schema signature and the algorithm once for the index, the root for each tablet; no old "ld"
        Assertions.assertTrue(json.contains("\"lra\":[\"" + sha('b') + "\"]"), json);
        Assertions.assertFalse(json.contains("\"ldr\""), json);
        Assertions.assertFalse(json.contains("\"manifest_roots\""), json);
        Assertions.assertTrue(json.contains("\"mra\":[\"" + sha('a') + "\"]"), json);
        Assertions.assertTrue(json.contains("\"lds\":\"" + sha('5') + "\""), json);
        Assertions.assertTrue(json.contains("\"lda\":1"), json);
        Assertions.assertFalse(json.contains("\"ld\""), json);
    }

    private void checkNoLogicalDigest(TLogicalDigest reported, String reason) throws IOException {
        // NOT_SUPPORTED, a failure and no report (an old backend) never fail the backup
        BackupJobInfo jobInfo = runBackupToSavedJobInfo(true, true, true, reported);
        LogicalDigestInfo digest = onlyIndex(jobInfo).getLogicalDigest(tabletId);
        Assertions.assertNotNull(digest);
        Assertions.assertFalse(digest.hasDigest());
        Assertions.assertEquals("", digest.root);
        Assertions.assertEquals(reason, digest.reason);
        Assertions.assertNull(digest.schemaSig);
        // the files of the backup are there
        Assertions.assertTrue(jobInfo.hasManifest());
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
        Assertions.assertNull(onlyIndex(jobInfo).getLogicalDigest(tabletId));
        String json = new String(Files.readAllBytes(new File(job.getLocalJobInfoFilePath()).toPath()));
        Assertions.assertFalse(json.contains("\"ld\""), json);
    }

    private List<TableRefInfo> tableRefsOf(String... tableNames) {
        List<TableRefInfo> tableRefs = Lists.newArrayList();
        for (String name : tableNames) {
            tableRefs.add(new TableRefInfo(
                    new TableNameInfo(InternalCatalog.INTERNAL_CATALOG_NAME, UnitTestUtil.DB_NAME, name),
                    null, null, null, new ArrayList<>(), null, null, new ArrayList<>()));
        }
        return tableRefs;
    }

    // Run the job until the snapshot tasks are sent, returns them.
    private List<SnapshotTask> runToSnapshotTasks(BackupJob backupJob) {
        AgentTaskQueue.clearAllTasks();
        backupJob.run();
        Assertions.assertEquals(BackupJobState.SNAPSHOTING, backupJob.getState());
        List<SnapshotTask> tasks = Lists.newArrayList();
        for (long signature = id.get() - 10; signature < id.get(); signature++) {
            AgentTask task = AgentTaskQueue.getTask(backendId, TTaskType.MAKE_SNAPSHOT, signature);
            if (task instanceof SnapshotTask) {
                tasks.add((SnapshotTask) task);
            }
        }
        return tasks;
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
            Assertions.assertEquals(1, tasks.size());
            Assertions.assertTrue(tasks.get(0).isComputeLogicalDigest());

            // a unique table is not, with the default config of the source cluster
            Deencapsulation.setField(tbl, "keysType", KeysType.UNIQUE_KEYS);
            job = new BackupJob("label2", dbId, UnitTestUtil.DB_NAME, tableRefsOf(UnitTestUtil.TABLE_NAME),
                    13600 * 1000, BackupCommand.BackupContent.ALL, env, repo.getId(), 0);
            job.setLogicalDigest(true);
            tasks = runToSnapshotTasks(job);
            Assertions.assertEquals(1, tasks.size());
            Assertions.assertFalse(tasks.get(0).isComputeLogicalDigest());
            Assertions.assertFalse(tasks.get(0).toThrift().isSetComputeLogicalDigest());
            // the manifest is not affected
            Assertions.assertTrue(tasks.get(0).toThrift().isSetListFiles());

            // unless the source cluster allows the hidden state models
            Config.restore_reuse_allow_hidden_state_models = true;
            job = new BackupJob("label3", dbId, UnitTestUtil.DB_NAME, tableRefsOf(UnitTestUtil.TABLE_NAME),
                    13600 * 1000, BackupCommand.BackupContent.ALL, env, repo.getId(), 0);
            job.setLogicalDigest(true);
            tasks = runToSnapshotTasks(job);
            Assertions.assertTrue(tasks.get(0).isComputeLogicalDigest());
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
                        13600 * 1000, BackupCommand.BackupContent.ALL, env, repo.getId(), 0);
                job.setLogicalDigest(true);
                List<SnapshotTask> tasks = runToSnapshotTasks(job);
                Assertions.assertEquals(1, tasks.size());
                Assertions.assertTrue(tasks.get(0).isComputeLogicalDigest(), "restore_reuse_mow " + mowSwitch);
            }
            // a sequence mapping column
            tbl.setSequenceMapCol("k1");
            job = new BackupJob("mow" + n++, dbId, UnitTestUtil.DB_NAME, tableRefsOf(UnitTestUtil.TABLE_NAME),
                    13600 * 1000, BackupCommand.BackupContent.ALL, env, repo.getId(), 0);
            job.setLogicalDigest(true);
            List<SnapshotTask> tasks = runToSnapshotTasks(job);
            Assertions.assertFalse(tasks.get(0).isComputeLogicalDigest());
            tbl.setSequenceMapCol(null);
            // merge-on-read: not, unless the hidden state models are allowed
            tbl.setEnableUniqueKeyMergeOnWrite(false);
            job = new BackupJob("mow" + n++, dbId, UnitTestUtil.DB_NAME, tableRefsOf(UnitTestUtil.TABLE_NAME),
                    13600 * 1000, BackupCommand.BackupContent.ALL, env, repo.getId(), 0);
            job.setLogicalDigest(true);
            Assertions.assertFalse(runToSnapshotTasks(job).get(0).isComputeLogicalDigest());
            Config.restore_reuse_allow_hidden_state_models = true;
            job = new BackupJob("mow" + n++, dbId, UnitTestUtil.DB_NAME, tableRefsOf(UnitTestUtil.TABLE_NAME),
                    13600 * 1000, BackupCommand.BackupContent.ALL, env, repo.getId(), 0);
            job.setLogicalDigest(true);
            Assertions.assertTrue(runToSnapshotTasks(job).get(0).isComputeLogicalDigest());
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
            Assertions.assertNull(onlyIndex(jobInfo).getLogicalDigest(tabletId));
            String json = new String(Files.readAllBytes(new File(job.getLocalJobInfoFilePath()).toPath()));
            Assertions.assertFalse(json.contains("\"lra\""), json);
            Assertions.assertFalse(json.contains("\"lna\""), json);
            Assertions.assertTrue(jobInfo.hasManifest());
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
                        BackupCommand.BackupContent.ALL, env, repo.getId(), 0);
                twoTables.setLogicalDigest(true);
                List<SnapshotTask> tasks = runToSnapshotTasks(twoTables);
                Assertions.assertEquals(2, tasks.size());
                boolean exceeded = limit == 1;
                for (SnapshotTask task : tasks) {
                    // no limit (0) and not exceeded: asked for the digest; exceeded: not asked
                    Assertions.assertEquals(!exceeded, task.isComputeLogicalDigest(), "limit " + limit);
                }
                if (exceeded) {
                    Assertions.assertEquals("TOO_MANY_TABLETS:2>1", twoTables.getLogicalDigestSkipReason());
                    List<String> info = twoTables.getInfo();
                    Assertions.assertEquals("SKIPPED:TOO_MANY_TABLETS:2>1", info.get(info.size() - 1));
                } else {
                    Assertions.assertNull(twoTables.getLogicalDigestSkipReason());
                    List<String> info = twoTables.getInfo();
                    Assertions.assertEquals("ON", info.get(info.size() - 1));
                }
            }
            // the limit does nothing if the digest is not asked for
            Config.backup_logical_digest_max_tablets = 1;
            BackupJob noDigest = new BackupJob("label_off", dbId, UnitTestUtil.DB_NAME,
                    tableRefsOf(UnitTestUtil.TABLE_NAME, table2Name), 13600 * 1000,
                    BackupCommand.BackupContent.ALL, env, repo.getId(), 0);
            runToSnapshotTasks(noDigest);
            Assertions.assertNull(noDigest.getLogicalDigestSkipReason());
            List<String> info = noDigest.getInfo();
            Assertions.assertEquals("OFF", info.get(info.size() - 1));
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
            List<TableRefInfo> refs = tableRefsOf(UnitTestUtil.TABLE_NAME, table2Name);
            job = new BackupJob("label", dbId, UnitTestUtil.DB_NAME, refs, 13600 * 1000,
                    BackupCommand.BackupContent.ALL, env, repo.getId(), 0);
            job.setLogicalDigest(true);
            List<SnapshotTask> tasks = runToSnapshotTasks(job);
            Assertions.assertEquals(2, tasks.size());
            TBackend tBackend = new TBackend("", 0, 1);
            TStatus ok = new TStatus(TStatusCode.OK);
            for (SnapshotTask task : tasks) {
                Assertions.assertFalse(task.toThrift().isSetComputeLogicalDigest());
                TFinishTaskRequest request = new TFinishTaskRequest(tBackend, TTaskType.MAKE_SNAPSHOT,
                        task.getSignature(), ok);
                request.setSnapshotFiles(Lists.newArrayList("1.dat", "1.idx", "1.hdr"));
                request.setSnapshotPath("/path/to/snapshot");
                request.setManifestRoot(sha('1'));
                // even a backend which reports a digest anyway is not recorded
                request.setLogicalDigest(reportedDigest("OK"));
                Assertions.assertTrue(job.finishTabletSnapshotTask(task, request));
            }
            job.run();
            Assertions.assertEquals(BackupJobState.UPLOAD_SNAPSHOT, job.getState());
            // keep on local: no upload, go on to the job info
            job.run();
            Assertions.assertEquals(BackupJobState.UPLOADING, job.getState());
        } finally {
            Config.backup_logical_digest_max_tablets = orig;
        }
    }

    @Test
    public void testShowBackupLogicalDigestColumn() throws IOException {
        Assertions.assertEquals("LogicalDigest", ShowBackupCommand.TITLE_NAMES.get(
                ShowBackupCommand.TITLE_NAMES.size() - 1));
        // Status is not changed
        Assertions.assertEquals("Status", ShowBackupCommand.TITLE_NAMES.get(
                ShowBackupCommand.TITLE_NAMES.size() - 3));
        List<String> info = job.getInfo();
        Assertions.assertEquals(ShowBackupCommand.TITLE_NAMES.size(), info.size());
        Assertions.assertEquals("OFF", info.get(info.size() - 1));
        Assertions.assertEquals("[OK]", info.get(info.size() - 3));

        runBackupToSavedJobInfo(true, true, true, reportedDigest("OK"));
        info = job.getInfo();
        Assertions.assertEquals(ShowBackupCommand.TITLE_NAMES.size(), info.size());
        Assertions.assertEquals("RECORDED:digest=1,no_digest=0,not_asked=0", info.get(info.size() - 1));
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
            Assertions.assertFalse(tasks.get(0).isWriteManifest());
            Assertions.assertFalse(tasks.get(0).toThrift().isSetWriteManifest());
            Assertions.assertTrue(tasks.get(0).toThrift().isSetListFiles());
            Assertions.assertFalse(tasks.get(0).toThrift().isSetComputeDigest());
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
            Assertions.assertFalse(jobInfo.hasManifest());
            Assertions.assertNull(jobInfo.manifestVersion);
            Assertions.assertNull(onlyIndex(jobInfo).getManifestRoot(tabletId));
            String json = new String(Files.readAllBytes(new File(job.getLocalJobInfoFilePath()).toPath()));
            Assertions.assertFalse(json.contains("manifest"), json);
            Assertions.assertFalse(json.contains("digest_algorithm"), json);
        } finally {
            Config.enable_backup_manifest = orig;
        }
    }

    @Test
    public void testUploadTaskWritesManifestOnlyIfAsked() {
        UploadTask task = new UploadTask(null, backendId, 1, 2, dbId, Maps.newHashMap(),
                new FsBroker("127.0.0.1", 8000), Maps.newHashMap(), StorageBackend.StorageType.BROKER, "loc");
        Assertions.assertFalse(task.toThrift().isSetWriteManifest());
        task.setWriteManifest(true);
        Assertions.assertTrue(task.toThrift().isSetWriteManifest());
        Assertions.assertTrue(task.toThrift().isWriteManifest());
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
            Assertions.assertTrue(job.finishTabletSnapshotTask(tasks.get(0), request));
            job.run();
            Assertions.assertEquals(BackupJobState.UPLOAD_SNAPSHOT, job.getState());
            AgentTaskQueue.clearAllTasks();
            job.run();
            Assertions.assertEquals(BackupJobState.UPLOADING, job.getState());
            UploadTask upTask = (UploadTask) AgentTaskQueue.getTask(backendId, TTaskType.UPLOAD, id.get() - 1);
            Assertions.assertFalse(upTask.isWriteManifest());
            Assertions.assertFalse(upTask.toThrift().isSetWriteManifest());
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
            Assertions.assertTrue(tasks.get(0).toThrift().isWriteManifest());
            TBackend tBackend = new TBackend("", 0, 1);
            TFinishTaskRequest request = new TFinishTaskRequest(tBackend, TTaskType.MAKE_SNAPSHOT,
                    tasks.get(0).getSignature(), new TStatus(TStatusCode.OK));
            request.setSnapshotFiles(Lists.newArrayList("1.dat", "1.idx", "1.hdr"));
            request.setSnapshotPath("/path/to/snapshot");
            Assertions.assertTrue(job.finishTabletSnapshotTask(tasks.get(0), request));
            job.run();
            Assertions.assertEquals(BackupJobState.UPLOAD_SNAPSHOT, job.getState());
            AgentTaskQueue.clearAllTasks();
            job.run();
            Assertions.assertEquals(BackupJobState.UPLOADING, job.getState());
            UploadTask upTask = (UploadTask) AgentTaskQueue.getTask(backendId, TTaskType.UPLOAD, id.get() - 1);
            Assertions.assertTrue(upTask.isWriteManifest());
            Assertions.assertTrue(upTask.toThrift().isWriteManifest());
        } finally {
            Config.enable_backup_manifest = orig;
        }
    }

    @Test
    public void testLocalSnapshotManifestDigest() {
        List<TableRefInfo> tableRefs = Lists.newArrayList(new TableRefInfo(
                new TableNameInfo(InternalCatalog.INTERNAL_CATALOG_NAME, UnitTestUtil.DB_NAME, UnitTestUtil.TABLE_NAME),
                null, null, null, new ArrayList<>(), null, null, new ArrayList<>()));
        for (boolean manifestDigest : new boolean[] {false, true}) {
            AgentTaskQueue.clearAllTasks();
            job = new BackupJob("label", dbId, UnitTestUtil.DB_NAME, tableRefs, 13600 * 1000,
                    BackupCommand.BackupContent.ALL, env, Repository.KEEP_ON_LOCAL_REPO_ID, 0);
            if (manifestDigest) {
                job.setManifestDigest(true);
            }
            Assertions.assertEquals(manifestDigest, job.isManifestDigest());
            job.run();
            Assertions.assertEquals(BackupJobState.SNAPSHOTING, job.getState());
            SnapshotTask snapshotTask = (SnapshotTask) AgentTaskQueue.getTask(backendId, TTaskType.MAKE_SNAPSHOT,
                    id.get() - 1);
            Assertions.assertEquals(manifestDigest, snapshotTask.toThrift().isSetComputeDigest());
            Assertions.assertEquals(manifestDigest, snapshotTask.isComputeDigest());
        }
    }

    // ---------------------------------------------------------------------------------------------
    // backup_restore_merge_idle_ticks: the states which wait for nothing are run in the same round
    // ---------------------------------------------------------------------------------------------

    // the state of the job at every edit log written from now on
    private List<BackupJobState> loggedStates() {
        List<BackupJobState> states = new ArrayList<>();
        Mockito.doAnswer(inv -> {
            states.add(((BackupJob) inv.getArgument(0)).getState());
            return null;
        }).when(editLog).logBackupJob(ArgumentMatchers.any(BackupJob.class));
        return states;
    }

    // runs the job to SNAPSHOTING and reports its snapshot task
    private void finishTheSnapshot(BackupJob job) {
        job.run();
        Assertions.assertEquals(BackupJobState.SNAPSHOTING, job.getState());
        SnapshotTask snapshotTask = (SnapshotTask) AgentTaskQueue.getTask(backendId, TTaskType.MAKE_SNAPSHOT,
                id.get() - 1);
        TFinishTaskRequest request = new TFinishTaskRequest(new TBackend("", 0, 1), TTaskType.MAKE_SNAPSHOT,
                snapshotTask.getSignature(), new TStatus(TStatusCode.OK));
        request.setSnapshotFiles(Lists.newArrayList("1.dat", "1.idx", "1.hdr"));
        request.setSnapshotPath("/path/to/snapshot");
        Assertions.assertTrue(job.finishTabletSnapshotTask(snapshotTask, request));
    }

    private BackupJob keepOnLocalJob() {
        List<TableRefInfo> tableRefs = Lists.newArrayList(new TableRefInfo(
                new TableNameInfo(InternalCatalog.INTERNAL_CATALOG_NAME, UnitTestUtil.DB_NAME, UnitTestUtil.TABLE_NAME),
                null, null, null, new ArrayList<>(), null, null, new ArrayList<>()));
        return new BackupJob("label", dbId, UnitTestUtil.DB_NAME, tableRefs, 13600 * 1000,
                BackupCommand.BackupContent.ALL, env, Repository.KEEP_ON_LOCAL_REPO_ID, 0);
    }

    @Test
    public void testMergedTicksKeepOnLocalBackupFinishesInOneRunAfterSnapshots() {
        boolean orig = Config.backup_restore_merge_idle_ticks;
        try {
            // the switch off: one state per run, as before
            Config.backup_restore_merge_idle_ticks = false;
            job = keepOnLocalJob();
            finishTheSnapshot(job);
            List<BackupJobState> states = loggedStates();
            job.run();
            Assertions.assertEquals(BackupJobState.UPLOAD_SNAPSHOT, job.getState());
            Assertions.assertEquals(Lists.newArrayList(BackupJobState.UPLOAD_SNAPSHOT), states);
            job.run();
            Assertions.assertEquals(BackupJobState.UPLOADING, job.getState());
            job.run();
            Assertions.assertEquals(BackupJobState.SAVE_META, job.getState());
            job.run();
            Assertions.assertEquals(BackupJobState.UPLOAD_INFO, job.getState());
            job.run();
            Assertions.assertEquals(BackupJobState.FINISHED, job.getState());
            List<BackupJobState> oneByOne = Lists.newArrayList(states);

            // the switch on: the same logs in the same order, in one run
            Config.backup_restore_merge_idle_ticks = true;
            AgentTaskQueue.clearAllTasks();
            job = keepOnLocalJob();
            finishTheSnapshot(job);
            states = loggedStates();
            job.run();
            Assertions.assertEquals(Status.OK, job.getStatus());
            Assertions.assertEquals(BackupJobState.FINISHED, job.getState());
            Assertions.assertEquals(oneByOne, states);
            Assertions.assertEquals(Lists.newArrayList(BackupJobState.UPLOAD_SNAPSHOT, BackupJobState.SAVE_META,
                    BackupJobState.UPLOAD_INFO, BackupJobState.FINISHED), states);
            Assertions.assertTrue(new File(job.getLocalJobInfoFilePath()).exists());
            Mockito.verify(backupHandler).addSnapshot(ArgumentMatchers.eq("label"), ArgumentMatchers.same(job));
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
            List<BackupJobState> states = loggedStates();
            AgentTaskQueue.clearAllTasks();
            // the upload tasks are sent, then the job waits for them
            job.run();
            Assertions.assertEquals(BackupJobState.UPLOADING, job.getState());
            Assertions.assertEquals(Lists.newArrayList(BackupJobState.UPLOAD_SNAPSHOT), states);
            Assertions.assertEquals(1, AgentTaskQueue.getTaskNum());
            UploadTask upTask = (UploadTask) AgentTaskQueue.getTask(backendId, TTaskType.UPLOAD, id.get() - 1);
            job.run();
            Assertions.assertEquals(BackupJobState.UPLOADING, job.getState());
            Assertions.assertEquals(1, states.size());
            // the upload is reported: the rest in one run
            Map<Long, List<String>> tabletFileMap = Maps.newHashMap();
            tabletFileMap.put(tabletId, Lists.newArrayList("1.dat.4f158689243a3d6030352fec3cfd3798",
                    "1.idx.4f158689243a3d6030352fec3cfd3798", "1.hdr.4f158689243a3d6030352fec3cfd3798"));
            TFinishTaskRequest request = new TFinishTaskRequest(new TBackend("", 0, 1), TTaskType.UPLOAD,
                    upTask.getSignature(), new TStatus(TStatusCode.OK));
            request.setTabletFiles(tabletFileMap);
            Assertions.assertTrue(job.finishSnapshotUploadTask(upTask, request));
            job.run();
            Assertions.assertEquals(Status.OK, job.getStatus());
            Assertions.assertEquals(BackupJobState.FINISHED, job.getState());
            Assertions.assertEquals(Lists.newArrayList(BackupJobState.UPLOAD_SNAPSHOT, BackupJobState.SAVE_META,
                    BackupJobState.UPLOAD_INFO, BackupJobState.FINISHED), states);
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
            List<BackupJobState> states = loggedStates();
            AgentTaskQueue.clearAllTasks();
            job.run();
            Assertions.assertEquals(BackupJobState.UPLOADING, job.getState());
            UploadTask upTask = (UploadTask) AgentTaskQueue.getTask(backendId, TTaskType.UPLOAD, id.get() - 1);
            Map<Long, List<String>> tabletFileMap = Maps.newHashMap();
            tabletFileMap.put(tabletId, Lists.newArrayList("1.dat.4f158689243a3d6030352fec3cfd3798",
                    "1.idx.4f158689243a3d6030352fec3cfd3798", "1.hdr.4f158689243a3d6030352fec3cfd3798"));
            TFinishTaskRequest request = new TFinishTaskRequest(new TBackend("", 0, 1), TTaskType.UPLOAD,
                    upTask.getSignature(), new TStatus(TStatusCode.OK));
            request.setTabletFiles(tabletFileMap);
            Assertions.assertTrue(job.finishSnapshotUploadTask(upTask, request));

            Mockito.doReturn(new Status(Status.ErrCode.COMMON_ERROR, "upload failed")).when(repo)
                    .upload(ArgumentMatchers.anyString(), ArgumentMatchers.anyString());
            job.run();
            Assertions.assertEquals(BackupJobState.UPLOAD_INFO, job.getState());
            Assertions.assertFalse(job.getStatus().ok());
            Assertions.assertEquals(Lists.newArrayList(BackupJobState.UPLOAD_SNAPSHOT, BackupJobState.SAVE_META,
                    BackupJobState.UPLOAD_INFO), states);

            Mockito.doReturn(Status.OK).when(repo).upload(ArgumentMatchers.anyString(), ArgumentMatchers.anyString());
            job.run();
            Assertions.assertEquals(BackupJobState.FINISHED, job.getState());
            Assertions.assertEquals(Status.OK, job.getStatus());
            Assertions.assertEquals(BackupJobState.FINISHED, states.get(states.size() - 1));
        } finally {
            Config.backup_restore_merge_idle_ticks = orig;
        }
    }
}
