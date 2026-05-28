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
import org.apache.doris.common.profile.ExecutionProfile;
import org.apache.doris.nereids.NereidsPlanner;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.Coordinator;
import org.apache.doris.qe.InsertResult;
import org.apache.doris.qe.QueryState.MysqlStateType;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.thrift.TUniqueId;
import org.apache.doris.transaction.GlobalTransactionMgrIface;
import org.apache.doris.transaction.TransactionStatus;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
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
    public void testPublishTimeoutReturnErrorKeepsCommittedStatus() throws Exception {
        ConnectContext ctx = createExecutorContext();
        ctx.getSessionVariable().setInsertVisibleTimeoutReturnMode(
                SessionVariable.INSERT_VISIBLE_TIMEOUT_RETURN_MODE_ERROR);

        Coordinator coordinator = createCoordinator();
        GlobalTransactionMgrIface txnMgr = Mockito.mock(GlobalTransactionMgrIface.class);

        // Mock the transaction publish result so the executor enters the timeout branch.
        try (MockedStatic<EnvFactory> envFactoryMock = Mockito.mockStatic(EnvFactory.class);
                MockedStatic<Env> envMock = Mockito.mockStatic(Env.class)) {
            prepareFactoryMocks(envFactoryMock, envMock, coordinator, txnMgr);
            Mockito.when(txnMgr.commitAndPublishTransaction(
                    Mockito.any(), Mockito.anyList(), Mockito.anyLong(), Mockito.anyList(), Mockito.anyLong()))
                    .thenReturn(false);

            OlapInsertExecutor executor = createExecutor(ctx);
            executor.txnId = 10001L;
            executor.loadedRows = 12L;
            executor.filteredRows = 1;

            Exception exception = Assertions.assertThrows(Exception.class, executor::onComplete);
            executor.onFail(exception);

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

            Mockito.verify(txnMgr, Mockito.never()).abortTransaction(Mockito.anyLong(), Mockito.anyLong(),
                    Mockito.anyString());
        }
    }

    @Test
    public void testPublishTimeoutCommittedModeReturnsOk() throws Exception {
        ConnectContext ctx = createExecutorContext();
        Coordinator coordinator = createCoordinator();
        GlobalTransactionMgrIface txnMgr = Mockito.mock(GlobalTransactionMgrIface.class);
        boolean originEnableNereidsLoad = org.apache.doris.common.Config.enable_nereids_load;

        // Keep afterExec focused on client-visible return info so the test only covers timeout handling.
        org.apache.doris.common.Config.enable_nereids_load = true;
        try (MockedStatic<EnvFactory> envFactoryMock = Mockito.mockStatic(EnvFactory.class);
                MockedStatic<Env> envMock = Mockito.mockStatic(Env.class)) {
            prepareFactoryMocks(envFactoryMock, envMock, coordinator, txnMgr);
            Mockito.when(txnMgr.commitAndPublishTransaction(
                    Mockito.any(), Mockito.anyList(), Mockito.anyLong(), Mockito.anyList(), Mockito.anyLong()))
                    .thenReturn(false);

            OlapInsertExecutor executor = createExecutor(ctx);
            executor.txnId = 10002L;
            executor.loadedRows = 20L;
            executor.filteredRows = 2;

            executor.onComplete();
            executor.afterExec(Mockito.mock(StmtExecutor.class));

            Assertions.assertEquals(TransactionStatus.COMMITTED, executor.txnStatus);
            Assertions.assertEquals(MysqlStateType.OK, ctx.getState().getStateType());
            Assertions.assertTrue(ctx.getState().getInfoMessage().contains("'status':'COMMITTED'"));

            InsertResult insertResult = ctx.getInsertResult();
            Assertions.assertNotNull(insertResult);
            Assertions.assertEquals(TransactionStatus.COMMITTED, insertResult.txnStatus);
            Assertions.assertEquals(20L, insertResult.loadedRows);
            Assertions.assertEquals(2L, insertResult.filteredRows);
            Assertions.assertEquals(20L, ctx.getReturnRows());

            Mockito.verify(txnMgr, Mockito.never()).abortTransaction(Mockito.anyLong(), Mockito.anyLong(),
                    Mockito.anyString());
        } finally {
            org.apache.doris.common.Config.enable_nereids_load = originEnableNereidsLoad;
        }
    }

    // Build a fresh context per case so insertResult and QueryState do not leak between tests.
    private ConnectContext createExecutorContext() {
        ConnectContext ctx = new ConnectContext();
        ctx.setQueryId(new TUniqueId(1, 2));
        ctx.getState().reset();
        ctx.resetReturnRows();
        return ctx;
    }

    // Prepare the mocked coordinator so the executor can run its completion logic without real execution.
    private Coordinator createCoordinator() {
        Coordinator coordinator = Mockito.mock(Coordinator.class);
        Mockito.when(coordinator.getCommitInfos()).thenReturn(Lists.newArrayList());
        Mockito.when(coordinator.getTrackingUrl()).thenReturn(null);
        Mockito.when(coordinator.getExecutionProfile()).thenReturn(Mockito.mock(ExecutionProfile.class));
        Mockito.when(coordinator.getLoadCounters()).thenReturn(ImmutableMap.of());
        return coordinator;
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

    // Redirect coordinator creation and transaction access to mocks so the test stays deterministic.
    private void prepareFactoryMocks(MockedStatic<EnvFactory> envFactoryMock, MockedStatic<Env> envMock,
            Coordinator coordinator, GlobalTransactionMgrIface txnMgr) {
        EnvFactory envFactory = Mockito.mock(EnvFactory.class);
        envFactoryMock.when(EnvFactory::getInstance).thenReturn(envFactory);
        Mockito.when(envFactory.createCoordinator(Mockito.any(), Mockito.isNull(), Mockito.any(), Mockito.any()))
                .thenReturn(coordinator);
        envMock.when(Env::getCurrentGlobalTransactionMgr).thenReturn(txnMgr);
    }
}
