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

package org.apache.doris.nereids.trees.plans.commands.insert;

import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.EnvFactory;
import org.apache.doris.catalog.Table;
import org.apache.doris.common.Status;
import org.apache.doris.common.profile.ExecutionProfile;
import org.apache.doris.common.profile.Profile;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.datasource.hive.HiveTransactionMgr;
import org.apache.doris.load.loadv2.LoadManager;
import org.apache.doris.nereids.NereidsPlanner;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.Coordinator;
import org.apache.doris.qe.InsertResult;
import org.apache.doris.qe.QueryState.MysqlStateType;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.task.LoadEtlTask;
import org.apache.doris.thrift.TQueryOptions;
import org.apache.doris.thrift.TStatusCode;
import org.apache.doris.thrift.TUniqueId;
import org.apache.doris.transaction.GlobalTransactionMgrIface;
import org.apache.doris.transaction.TransactionStatus;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Optional;

/**
 * Tests for publish-timeout behaviors in {@link OlapInsertExecutor}.
 */
public class OlapInsertExecutorTest extends TestWithFeService {

    @Override
    protected void runBeforeAll() {
    }

    @Test
    public void testExecuteSingleInsertPublishTimeoutReturnErrorKeepsCommittedAccounting() throws Exception {
        ConnectContext ctx = createExecutorContext();
        ctx.getSessionVariable().setInsertVisibleTimeoutReturnMode(
                SessionVariable.INSERT_VISIBLE_TIMEOUT_RETURN_MODE_ERROR);

        Coordinator coordinator = createCoordinator();
        GlobalTransactionMgrIface txnMgr = Mockito.mock(GlobalTransactionMgrIface.class);
        LoadManager loadManager = Mockito.mock(LoadManager.class);
        StmtExecutor stmtExecutor = createStmtExecutor();
        boolean originEnableNereidsLoad = org.apache.doris.common.Config.enable_nereids_load;

        // Keep loadv2 recording enabled so the test validates the full committed accounting path.
        org.apache.doris.common.Config.enable_nereids_load = false;
        try (MockedStatic<EnvFactory> envFactoryMock = Mockito.mockStatic(EnvFactory.class);
                MockedStatic<Env> envMock = Mockito.mockStatic(Env.class)) {
            prepareFactoryMocks(envFactoryMock, envMock, coordinator, txnMgr);
            prepareContextEnv(ctx, loadManager);
            Mockito.when(txnMgr.commitAndPublishTransaction(
                    Mockito.any(), Mockito.anyList(), Mockito.anyLong(), Mockito.anyList(), Mockito.anyLong()))
                    .thenReturn(false);

            OlapInsertExecutor executor = createExecutor(ctx);
            executor.txnId = 10001L;
            executor.executeSingleInsert(stmtExecutor, 0L);

            Assertions.assertEquals(TransactionStatus.COMMITTED, executor.txnStatus);
            Assertions.assertEquals(MysqlStateType.ERR, ctx.getState().getStateType());
            Assertions.assertTrue(ctx.getState().getErrorMessage().contains(
                    "transaction commit successfully, BUT data did not become visible within "
                            + "insert_visible_timeout_ms and will be visible later."));

            InsertResult insertResult = ctx.getInsertResult();
            Assertions.assertNotNull(insertResult);
            Assertions.assertEquals(TransactionStatus.COMMITTED, insertResult.txnStatus);
            Assertions.assertEquals(12L, insertResult.loadedRows);
            Assertions.assertEquals(1L, insertResult.filteredRows);
            Assertions.assertEquals(12L, ctx.getReturnRows());
            // Verify the insert path propagates the profile-safe decision before coordinator execution.
            Mockito.verify(coordinator).setIsProfileSafeStmt(false);

            // The finished load job must still be recorded even when the client response becomes ERR.
            ArgumentCaptor<String> failMsgCaptor = ArgumentCaptor.forClass(String.class);
            Mockito.verify(loadManager).recordFinishedLoadJob(Mockito.eq("label_test"), Mockito.eq(10001L),
                    Mockito.eq("test_db"), Mockito.eq(2L), Mockito.any(), Mockito.anyLong(), failMsgCaptor.capture(),
                    Mockito.isNull(), Mockito.isNull(), Mockito.eq(0L));
            Assertions.assertEquals("", failMsgCaptor.getValue());
            Mockito.verify(txnMgr, Mockito.never()).abortTransaction(Mockito.anyLong(), Mockito.anyLong(),
                    Mockito.anyString());
        } finally {
            org.apache.doris.common.Config.enable_nereids_load = originEnableNereidsLoad;
        }
    }

    @Test
    public void testPublishTimeoutCommittedModeReturnsOk() throws Exception {
        ConnectContext ctx = createExecutorContext();
        Coordinator coordinator = createCoordinator();
        GlobalTransactionMgrIface txnMgr = Mockito.mock(GlobalTransactionMgrIface.class);
        StmtExecutor stmtExecutor = createStmtExecutor();
        boolean originEnableNereidsLoad = org.apache.doris.common.Config.enable_nereids_load;

        // Skip loadv2 recording in this case so the test stays focused on the committed-mode response path.
        org.apache.doris.common.Config.enable_nereids_load = true;
        try (MockedStatic<EnvFactory> envFactoryMock = Mockito.mockStatic(EnvFactory.class);
                MockedStatic<Env> envMock = Mockito.mockStatic(Env.class)) {
            prepareFactoryMocks(envFactoryMock, envMock, coordinator, txnMgr);
            Mockito.when(txnMgr.commitAndPublishTransaction(
                    Mockito.any(), Mockito.anyList(), Mockito.anyLong(), Mockito.anyList(), Mockito.anyLong()))
                    .thenReturn(false);

            OlapInsertExecutor executor = createExecutor(ctx);
            executor.txnId = 10002L;
            executor.executeSingleInsert(stmtExecutor, 0L);

            Assertions.assertEquals(TransactionStatus.COMMITTED, executor.txnStatus);
            Assertions.assertEquals(MysqlStateType.OK, ctx.getState().getStateType());
            Assertions.assertTrue(ctx.getState().getInfoMessage().contains("'status':'COMMITTED'"));

            InsertResult insertResult = ctx.getInsertResult();
            Assertions.assertNotNull(insertResult);
            Assertions.assertEquals(TransactionStatus.COMMITTED, insertResult.txnStatus);
            Assertions.assertEquals(12L, insertResult.loadedRows);
            Assertions.assertEquals(1L, insertResult.filteredRows);
            Assertions.assertEquals(12L, ctx.getReturnRows());

            Mockito.verify(txnMgr, Mockito.never()).abortTransaction(Mockito.anyLong(), Mockito.anyLong(),
                    Mockito.anyString());
        } finally {
            org.apache.doris.common.Config.enable_nereids_load = originEnableNereidsLoad;
        }
    }

    @Test
    public void testOnFailAbortsUncommittedTransaction() throws Exception {
        ConnectContext ctx = createExecutorContext();
        Coordinator coordinator = createCoordinator();
        GlobalTransactionMgrIface txnMgr = Mockito.mock(GlobalTransactionMgrIface.class);

        try (MockedStatic<EnvFactory> envFactoryMock = Mockito.mockStatic(EnvFactory.class);
                MockedStatic<Env> envMock = Mockito.mockStatic(Env.class)) {
            prepareFactoryMocks(envFactoryMock, envMock, coordinator, txnMgr);

            // Simulate a pre-commit failure so the executor must abort the transaction.
            OlapInsertExecutor executor = createExecutor(ctx);
            executor.txnId = 10003L;
            executor.txnStatus = TransactionStatus.ABORTED;

            executor.onFail(new RuntimeException("pre-commit failure"));

            Assertions.assertEquals(MysqlStateType.ERR, ctx.getState().getStateType());
            Assertions.assertTrue(ctx.getState().getErrorMessage().contains("pre-commit failure"));
            Assertions.assertNull(ctx.getInsertResult());
            Mockito.verify(txnMgr).abortTransaction(Mockito.eq(1L), Mockito.eq(10003L),
                    Mockito.eq("pre-commit failure"));
        }
    }

    // Build a fresh context per case so insertResult and QueryState do not leak between tests.
    private ConnectContext createExecutorContext() {
        ConnectContext ctx = new ConnectContext();
        ctx.setQueryId(new TUniqueId(1, 2));
        // Disable strict insert mode because this test intentionally keeps one filtered row in the mocked counters.
        ctx.getSessionVariable().setEnableInsertStrict(false);
        ctx.getState().reset();
        ctx.resetReturnRows();
        return ctx;
    }

    // Prepare the mocked coordinator so the executor can run its completion logic without real execution.
    private Coordinator createCoordinator() {
        Coordinator coordinator = Mockito.mock(Coordinator.class);
        Mockito.when(coordinator.join(Mockito.anyInt())).thenReturn(true);
        Mockito.when(coordinator.isDone()).thenReturn(true);
        Mockito.when(coordinator.getExecStatus()).thenReturn(new Status(TStatusCode.OK, ""));
        // Provide default query options so the real insert execution path can access profile flags safely.
        Mockito.when(coordinator.getQueryOptions()).thenReturn(new TQueryOptions());
        Mockito.when(coordinator.getCommitInfos()).thenReturn(Lists.newArrayList());
        Mockito.when(coordinator.getTrackingUrl()).thenReturn(null);
        Mockito.when(coordinator.getExecutionProfile()).thenReturn(Mockito.mock(ExecutionProfile.class));
        Mockito.when(coordinator.getLoadCounters()).thenReturn(ImmutableMap.of(
                LoadEtlTask.DPP_NORMAL_ALL, "12",
                LoadEtlTask.DPP_ABNORMAL_ALL, "1"));
        return coordinator;
    }

    // Use a mocked executor so executeSingleInsert can run the real control flow without a full query setup.
    private StmtExecutor createStmtExecutor() {
        StmtExecutor stmtExecutor = Mockito.mock(StmtExecutor.class);
        Mockito.when(stmtExecutor.getProfile()).thenReturn(Mockito.mock(Profile.class));
        Mockito.when(stmtExecutor.getOriginStmtInString()).thenReturn("insert into test_tbl select 1");
        // Force the executor to mark this mocked insert as profile-unsafe so the propagation can be asserted.
        Mockito.when(stmtExecutor.isProfileSafeStmt()).thenReturn(false);
        return stmtExecutor;
    }

    // Create an executor with mocked table metadata because this test only validates timeout result handling.
    private OlapInsertExecutor createExecutor(ConnectContext ctx) {
        Database database = Mockito.mock(Database.class);
        Mockito.when(database.getFullName()).thenReturn("test_db");
        Mockito.when(database.getId()).thenReturn(1L);

        Table table = Mockito.mock(Table.class);
        Mockito.when(table.getDatabase()).thenReturn(database);
        Mockito.when(table.getName()).thenReturn("test_tbl");
        Mockito.when(table.getId()).thenReturn(2L);

        return new OlapInsertExecutor(ctx, table, "label_test", Mockito.mock(NereidsPlanner.class),
                Optional.empty(), false);
    }

    // Attach a mocked env so afterExec can record the finished load job without depending on a real FE env.
    private void prepareContextEnv(ConnectContext ctx, LoadManager loadManager) {
        Env env = Mockito.mock(Env.class);
        InternalCatalog internalCatalog = Mockito.mock(InternalCatalog.class);
        Mockito.when(env.getInternalCatalog()).thenReturn(internalCatalog);
        Mockito.when(internalCatalog.getName()).thenReturn("internal");
        Mockito.when(env.getLoadManager()).thenReturn(loadManager);
        ctx.setEnv(env);
    }

    // Redirect coordinator creation and transaction access to mocks so the test stays deterministic.
    private void prepareFactoryMocks(MockedStatic<EnvFactory> envFactoryMock, MockedStatic<Env> envMock,
            Coordinator coordinator, GlobalTransactionMgrIface txnMgr) {
        EnvFactory envFactory = Mockito.mock(EnvFactory.class);
        HiveTransactionMgr hiveTransactionMgr = Mockito.mock(HiveTransactionMgr.class);
        envFactoryMock.when(EnvFactory::getInstance).thenReturn(envFactory);
        Mockito.when(envFactory.createCoordinator(Mockito.any(), Mockito.isNull(), Mockito.any(), Mockito.any()))
                .thenReturn(coordinator);
        envMock.when(Env::getCurrentGlobalTransactionMgr).thenReturn(txnMgr);
        // Provide a no-op hive transaction manager so unregisterQuery() can finish its cleanup path safely.
        envMock.when(Env::getCurrentHiveTransactionMgr).thenReturn(hiveTransactionMgr);
    }
}
