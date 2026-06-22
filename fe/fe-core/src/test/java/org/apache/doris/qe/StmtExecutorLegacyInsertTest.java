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

package org.apache.doris.qe;

import org.apache.doris.analysis.Analyzer;
import org.apache.doris.analysis.InsertSource;
import org.apache.doris.analysis.InsertTarget;
import org.apache.doris.analysis.NativeInsertStmt;
import org.apache.doris.analysis.QueryStmt;
import org.apache.doris.analysis.TableName;
import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.EnvFactory;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.TableIf.TableType;
import org.apache.doris.common.Status;
import org.apache.doris.common.profile.ExecutionProfile;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.datasource.hive.HiveTransactionMgr;
import org.apache.doris.load.loadv2.LoadManager;
import org.apache.doris.planner.Planner;
import org.apache.doris.task.LoadEtlTask;
import org.apache.doris.thrift.TQueryOptions;
import org.apache.doris.thrift.TStatusCode;
import org.apache.doris.thrift.TUniqueId;
import org.apache.doris.transaction.GlobalTransactionMgrIface;
import org.apache.doris.transaction.TransactionStatus;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

public class StmtExecutorLegacyInsertTest {

    @Test
    public void testLegacyInsertPublishTimeoutReturnErrorKeepsCommittedAccounting() throws Exception {
        LegacyInsertCase testCase = createLegacyInsertCase(10001L);
        testCase.context.getSessionVariable().setInsertVisibleTimeoutReturnMode(
                SessionVariable.INSERT_VISIBLE_TIMEOUT_RETURN_MODE_ERROR);

        runLegacyInsert(testCase);

        Assertions.assertEquals(QueryState.MysqlStateType.ERR, testCase.context.getState().getStateType());
        Assertions.assertTrue(testCase.context.getState().getErrorMessage().contains(
                "transaction commit successfully, BUT data did not become visible within "
                        + "insert_visible_timeout_ms and will be visible later."));
        assertCommittedAccounting(testCase, 10001L);
    }

    @Test
    public void testLegacyInsertPublishTimeoutCommittedModeReturnsOk() throws Exception {
        LegacyInsertCase testCase = createLegacyInsertCase(10002L);

        runLegacyInsert(testCase);

        Assertions.assertEquals(QueryState.MysqlStateType.OK, testCase.context.getState().getStateType());
        Assertions.assertTrue(testCase.context.getState().getInfoMessage().contains("'status':'COMMITTED'"));
        assertCommittedAccounting(testCase, 10002L);
    }

    private void runLegacyInsert(LegacyInsertCase testCase) throws Exception {
        try (MockedStatic<EnvFactory> envFactoryMock = Mockito.mockStatic(EnvFactory.class);
                MockedStatic<Env> envMock = Mockito.mockStatic(Env.class)) {
            prepareFactoryMocks(envFactoryMock, envMock, testCase.coordinator, testCase.txnMgr);
            invokeHandleInsertStmt(testCase.executor);
        } finally {
            ConnectContext.remove();
        }
    }

    private void assertCommittedAccounting(LegacyInsertCase testCase, long txnId) throws Exception {
        InsertResult insertResult = testCase.context.getInsertResult();
        Assertions.assertNotNull(insertResult);
        Assertions.assertEquals(TransactionStatus.COMMITTED, insertResult.txnStatus);
        Assertions.assertEquals(12L, insertResult.loadedRows);
        Assertions.assertEquals(0L, insertResult.filteredRows);
        Assertions.assertEquals(12L, testCase.context.getReturnRows());

        ArgumentCaptor<String> failMsgCaptor = ArgumentCaptor.forClass(String.class);
        Mockito.verify(testCase.loadManager).recordFinishedLoadJob(Mockito.eq("label_test"), Mockito.eq(txnId),
                Mockito.eq("test_db"), Mockito.eq(2L), Mockito.any(), Mockito.anyLong(), failMsgCaptor.capture(),
                Mockito.isNull(), Mockito.isNull(), Mockito.eq(0L));
        Assertions.assertEquals("", failMsgCaptor.getValue());
        Mockito.verify(testCase.txnMgr).commitAndPublishTransaction(Mockito.eq(testCase.database),
                Mockito.anyList(), Mockito.eq(txnId), Mockito.anyList(), Mockito.anyLong());
        Mockito.verify(testCase.txnMgr, Mockito.never()).abortTransaction(Mockito.anyLong(), Mockito.anyLong(),
                Mockito.anyString());
    }

    private LegacyInsertCase createLegacyInsertCase(long txnId) throws Exception {
        ConnectContext context = new ConnectContext();
        context.setQueryId(new TUniqueId(1, txnId));
        context.getSessionVariable().setEnableInsertStrict(false);
        context.getState().reset();
        context.resetReturnRows();
        context.setThreadLocalInfo();

        Coordinator coordinator = createCoordinator();
        GlobalTransactionMgrIface txnMgr = Mockito.mock(GlobalTransactionMgrIface.class);
        Mockito.when(txnMgr.commitAndPublishTransaction(
                Mockito.any(), Mockito.anyList(), Mockito.anyLong(), Mockito.anyList(), Mockito.anyLong()))
                .thenReturn(false);

        LoadManager loadManager = Mockito.mock(LoadManager.class);
        Env env = createEnv(loadManager);
        context.setEnv(env);

        DatabaseIf database = Mockito.mock(DatabaseIf.class);
        Mockito.when(database.getId()).thenReturn(1L);
        Mockito.when(database.getFullName()).thenReturn("test_db");

        Table table = Mockito.mock(Table.class);
        Mockito.when(table.getId()).thenReturn(2L);
        Mockito.when(table.getName()).thenReturn("test_tbl");
        Mockito.when(table.getType()).thenReturn(TableType.OLAP);

        NativeInsertStmt insertStmt = createInsertStmt(database, table, txnId);
        StmtExecutor executor = new StmtExecutor(context, insertStmt, false);
        return new LegacyInsertCase(context, executor, coordinator, txnMgr, loadManager, database);
    }

    private NativeInsertStmt createInsertStmt(DatabaseIf database, Table table, long txnId) throws Exception {
        QueryStmt queryStmt = Mockito.mock(QueryStmt.class);
        Mockito.when(queryStmt.hasOutFileClause()).thenReturn(false);
        Mockito.when(queryStmt.isExplain()).thenReturn(false);

        NativeInsertStmt insertStmt = new NativeInsertStmt(
                new InsertTarget(new TableName(null, "test_db", "test_tbl"), null),
                "label_test", null, new InsertSource(queryStmt), Lists.newArrayList());
        insertStmt.setOrigStmt(new OriginStatement("insert into test_tbl select 1", 0));
        insertStmt.setTargetTable(table);
        setPrivateField(insertStmt, "db", database);
        setPrivateField(insertStmt, "transactionId", txnId);
        return insertStmt;
    }

    private Coordinator createCoordinator() {
        Coordinator coordinator = Mockito.mock(Coordinator.class);
        Mockito.when(coordinator.join(Mockito.anyInt())).thenReturn(true);
        Mockito.when(coordinator.isDone()).thenReturn(true);
        Mockito.when(coordinator.getExecStatus()).thenReturn(new Status(TStatusCode.OK, ""));
        Mockito.when(coordinator.getQueryOptions()).thenReturn(new TQueryOptions());
        Mockito.when(coordinator.getCommitInfos()).thenReturn(Lists.newArrayList());
        Mockito.when(coordinator.getTrackingUrl()).thenReturn(null);
        Mockito.when(coordinator.getExecutionProfile()).thenReturn(Mockito.mock(ExecutionProfile.class));
        Mockito.when(coordinator.getLoadCounters()).thenReturn(ImmutableMap.of(
                LoadEtlTask.DPP_NORMAL_ALL, "12",
                LoadEtlTask.DPP_ABNORMAL_ALL, "0"));
        return coordinator;
    }

    private Env createEnv(LoadManager loadManager) {
        Env env = Mockito.mock(Env.class);
        InternalCatalog internalCatalog = Mockito.mock(InternalCatalog.class);
        Mockito.when(env.getInternalCatalog()).thenReturn(internalCatalog);
        Mockito.when(internalCatalog.getName()).thenReturn("internal");
        Mockito.when(env.getLoadManager()).thenReturn(loadManager);
        return env;
    }

    private void prepareFactoryMocks(MockedStatic<EnvFactory> envFactoryMock, MockedStatic<Env> envMock,
            Coordinator coordinator, GlobalTransactionMgrIface txnMgr) {
        EnvFactory envFactory = Mockito.mock(EnvFactory.class);
        HiveTransactionMgr hiveTransactionMgr = Mockito.mock(HiveTransactionMgr.class);
        envFactoryMock.when(EnvFactory::getInstance).thenReturn(envFactory);
        Mockito.when(envFactory.createCoordinator(Mockito.any(), Mockito.nullable(Analyzer.class),
                        Mockito.nullable(Planner.class), Mockito.any()))
                .thenReturn(coordinator);
        envMock.when(Env::getCurrentGlobalTransactionMgr).thenReturn(txnMgr);
        envMock.when(Env::getCurrentHiveTransactionMgr).thenReturn(hiveTransactionMgr);
    }

    private void invokeHandleInsertStmt(StmtExecutor executor) throws Exception {
        Method method = StmtExecutor.class.getDeclaredMethod("handleInsertStmt");
        method.setAccessible(true);
        try {
            method.invoke(executor);
        } catch (InvocationTargetException e) {
            Throwable cause = e.getCause();
            if (cause instanceof Exception) {
                throw (Exception) cause;
            }
            throw e;
        }
    }

    private void setPrivateField(Object target, String fieldName, Object value) throws Exception {
        Field field = target.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(target, value);
    }

    private static class LegacyInsertCase {
        private final ConnectContext context;
        private final StmtExecutor executor;
        private final Coordinator coordinator;
        private final GlobalTransactionMgrIface txnMgr;
        private final LoadManager loadManager;
        private final DatabaseIf database;

        private LegacyInsertCase(ConnectContext context, StmtExecutor executor, Coordinator coordinator,
                GlobalTransactionMgrIface txnMgr, LoadManager loadManager, DatabaseIf database) {
            this.context = context;
            this.executor = executor;
            this.coordinator = coordinator;
            this.txnMgr = txnMgr;
            this.loadManager = loadManager;
            this.database = database;
        }
    }
}
