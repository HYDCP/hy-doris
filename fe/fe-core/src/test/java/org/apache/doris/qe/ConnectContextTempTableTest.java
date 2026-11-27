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

import org.apache.doris.analysis.RedirectStatus;
import org.apache.doris.analysis.TableName;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.Table;
import org.apache.doris.common.ClientPool;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.FeNameFormat;
import org.apache.doris.common.GenericPool;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.nereids.glue.LogicalPlanAdapter;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.qe.QueryState.MysqlStateType;
import org.apache.doris.thrift.FrontendService;
import org.apache.doris.thrift.TDropSessionTemporaryTableRequest;
import org.apache.doris.thrift.TDropSessionTemporaryTableResult;
import org.apache.doris.thrift.TMasterOpRequest;
import org.apache.doris.thrift.TMasterOpResult;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TStatus;
import org.apache.doris.thrift.TStatusCode;
import org.apache.doris.thrift.TTxnParams;
import org.apache.doris.transaction.TransactionEntry;

import com.google.common.collect.ImmutableList;
import mockit.Expectations;
import mockit.Invocation;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import mockit.Verifications;
import org.apache.thrift.TApplicationException;
import org.apache.thrift.TException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

class ConnectContextTempTableTest {
    private static final GenericPool<FrontendService.Client> FRONTEND_POOL = ClientPool.frontendPool;

    @Mocked
    private Env env;
    @Mocked
    private InternalCatalog catalog;
    @Mocked
    private Database db;

    private void mockLocalCatalog(boolean leader) {
        new Expectations() {
            {
                Env.getCurrentEnv();
                result = env;
                minTimes = 0;
                env.isMaster();
                result = leader;
                minTimes = 0;
                env.getInternalCatalog();
                result = catalog;
                minTimes = 0;
                catalog.getDbNullable(10L);
                result = db;
                minTimes = 0;
            }
        };
    }

    @Test
    void retriesFailedResetWithoutRepeatingSuccessfulDrops() throws Exception {
        mockLocalCatalog(true);
        ConnectContext ctx = new ConnectContext();
        String owner = ctx.getSessionId();
        ctx.addTemporaryTable(10L, 11L);
        ctx.addTemporaryTable(10L, 12L);
        new Expectations() {
            {
                catalog.dropTemporaryTable(db, 11L, owner);
                result = new DdlException("injected DROP failure");
                result = null;
                times = 2;
            }
        };

        ConnectProcessor processor = new MysqlConnectProcessor(ctx);
        processor.handleResetConnection();
        Assertions.assertEquals(MysqlStateType.ERR, ctx.getState().getStateType());
        Assertions.assertEquals(owner, ctx.getSessionId());
        processor.handleResetConnection();
        Assertions.assertNotEquals(owner, ctx.getSessionId());
        Assertions.assertEquals(MysqlStateType.OK, ctx.getState().getStateType());

        new Verifications() {
            {
                catalog.dropTemporaryTable(db, 12L, owner);
                times = 1;
            }
        };
    }

    @Test
    void retainsTemporaryTablesWhenRollbackFails(@Mocked TransactionEntry transaction) throws Exception {
        mockLocalCatalog(true);
        ConnectContext ctx = new ConnectContext();
        ctx.setTxnEntry(transaction);
        ctx.addTemporaryTable(10L, 11L);
        new Expectations() {
            {
                transaction.isTxnModel();
                result = true;
                transaction.abortTransaction();
                result = new DdlException("injected rollback failure");
            }
        };

        new MysqlConnectProcessor(ctx).handleResetConnection();
        Assertions.assertEquals(MysqlStateType.ERR, ctx.getState().getStateType());
        Assertions.assertSame(transaction, ctx.getTxnEntry());
        new Verifications() {
            {
                catalog.dropTemporaryTable(db, anyLong, anyString);
                times = 0;
            }
        };
    }

    @Test
    void drainsInFlightStatementBeforeCleaningOnce() throws Exception {
        mockLocalCatalog(true);
        ConnectContext ctx = new ConnectContext();
        ctx.addTemporaryTable(10L, 11L);
        ctx.beginStatementExecution();
        ctx.onConnectionUnregistering();
        Assertions.assertTrue(ctx.isOpenForTemporaryTableCleanup());
        ctx.cleanup();
        new Verifications() {
            {
                catalog.dropTemporaryTable(db, anyLong, anyString);
                times = 0;
            }
        };

        ctx.addTemporaryTable(10L, 12L);
        ctx.endStatementExecution();
        Assertions.assertFalse(ctx.isOpenForTemporaryTableCleanup());
        ctx.cleanup();
        Assertions.assertThrows(DdlException.class, ctx::beginStatementExecution);

        new Verifications() {
            {
                catalog.dropTemporaryTable(db, 11L, ctx.getSessionId());
                times = 1;
                catalog.dropTemporaryTable(db, 12L, ctx.getSessionId());
                times = 1;
            }
        };
    }

    @Test
    void legacyMasterFallsBackToQualifiedDropAndChecksStatus(@Mocked Table table,
            @Mocked GenericPool<FrontendService.Client> pool, @Mocked FrontendService.Client client) throws Exception {
        mockLocalCatalog(false);
        ConnectContext ctx = new ConnectContext();
        ctx.setEnv(env);
        ctx.changeDefaultCatalog("external_catalog");
        ctx.addTemporaryTable(10L, 11L);
        String internalName = ctx.getSessionId() + FeNameFormat.TEMPORARY_TABLE_SIGN + "tmp";
        new Expectations() {
            {
                FRONTEND_POOL.borrowObject((TNetworkAddress) any, anyInt);
                result = client;
                client.dropSessionTemporaryTable((TDropSessionTemporaryTableRequest) any);
                result = new TApplicationException(TApplicationException.UNKNOWN_METHOD, "old FE");
                Env.isStoredTableNamesLowerCase();
                result = true;
                db.getFullName();
                result = "other_db";
                db.getTableNullable(11L);
                result = table;
                result = table;
                result = null;
                table.isTemporary();
                result = true;
                table.getName();
                result = internalName;
            }
        };
        List<String> sentSql = new ArrayList<>();
        new MockUp<MasterOpExecutor>() {
            @Mock
            public void syncJournal(Invocation invocation) throws DdlException {
                if (invocation.getInvocationCount() == 2) {
                    throw new DdlException("legacy follower replay is behind");
                }
            }

            @Mock
            public void executeTemporaryTableDrop(Invocation invocation) throws DdlException {
                MasterOpExecutor operation = invocation.getInvokedInstance();
                sentSql.add(operation.originStmt.originStmt);
                if (sentSql.size() == 1) {
                    throw new DdlException("injected forwarded error");
                }
            }
        };

        DdlException failure = Assertions.assertThrows(DdlException.class, ctx::resetTemporaryTables);
        Assertions.assertTrue(failure.getCause().getMessage().contains("injected forwarded error"));
        Assertions.assertEquals(1, sentSql.size());
        Assertions.assertEquals("DROP TEMPORARY TABLE IF EXISTS `internal`.`other_db`.`tmp` FORCE",
                sentSql.get(0));

        ctx.cleanup();
        Assertions.assertEquals(2, sentSql.size());
        Assertions.assertEquals(sentSql.get(0), sentSql.get(1));
        Assertions.assertTrue(ctx.snapshotTemporaryTables().isEmpty());
    }

    @Test
    void rejectsResetAfterUncertainCreate() {
        mockLocalCatalog(false);
        ConnectContext ctx = new ConnectContext();
        ctx.markTemporaryTableDdlUncertain(new TableName("internal", "stage", "tmp"), null);
        DdlException error = Assertions.assertThrows(DdlException.class, ctx::resetTemporaryTables);
        Assertions.assertTrue(error.getMessage().contains("unknown"));
    }

    @Test
    void followerRetainsTransactionAfterMasterRejectsRollback() {
        mockLocalCatalog(false);
        ConnectContext ctx = new ConnectContext();
        ctx.setEnv(env);
        ctx.setThreadLocalInfo();
        TransactionEntry txn = new TransactionEntry();
        txn.setTxnConf(new TTxnParams().setNeedTxn(true).setTxnId(-1));
        Deencapsulation.setField(txn, "isTransactionBegan", true);
        ctx.setTxnEntry(txn);
        ctx.addTemporaryTable(10L, 11L);
        new MockUp<FEOpExecutor>() {
            @Mock
            protected TMasterOpRequest buildStmtForwardParams() {
                return new TMasterOpRequest();
            }

            @Mock
            protected TMasterOpResult forward(TMasterOpRequest request) {
                return new TMasterOpResult().setStatusCode(1105).setErrMessage("injected remote rollback failure");
            }
        };
        try {
            new MysqlConnectProcessor(ctx).handleResetConnection();
            Assertions.assertEquals(MysqlStateType.ERR, ctx.getState().getStateType());
            Assertions.assertSame(txn, ctx.getTxnEntry());
            new Verifications() {
                {
                    catalog.getDbNullable(anyLong);
                    times = 0;
                }
            };
        } finally {
            ConnectContext.remove();
        }
    }

    @Test
    void followerDisconnectChecksMasterForCreateAbsentFromLocalReplay() {
        mockLocalCatalog(false);
        ConnectContext ctx = new ConnectContext();
        ctx.setEnv(env);
        ctx.markTemporaryTableDdlUncertain(new TableName("internal", "stage", "late"), null);
        List<String> sentSql = new ArrayList<>();
        new MockUp<MasterOpExecutor>() {
            @Mock
            public void executeTemporaryTableDrop(Invocation invocation) {
                MasterOpExecutor operation = invocation.getInvokedInstance();
                sentSql.add(operation.originStmt.originStmt);
            }
        };
        ctx.cleanup();
        Assertions.assertEquals(1, sentSql.size());
        Assertions.assertEquals("DROP TEMPORARY TABLE IF EXISTS `internal`.`stage`.`"
                + ctx.getSessionId() + FeNameFormat.TEMPORARY_TABLE_SIGN + "late` FORCE", sentSql.get(0));
    }

    @Test
    void disconnectChecksBothPossibleDatabasesAfterRenameAndLostReply() {
        mockLocalCatalog(false);
        new Expectations() {
            {
                db.getId();
                result = 10L;
                db.getFullName();
                result = "renamed";
            }
        };
        ConnectContext ctx = new ConnectContext();
        ctx.setEnv(env);
        ctx.markTemporaryTableDdlUncertain(new TableName("internal", "original", "tmp"), db);
        List<String> drops = new ArrayList<>();
        new MockUp<MasterOpExecutor>() {
            @Mock
            public void executeTemporaryTableDrop(Invocation invocation) {
                MasterOpExecutor operation = invocation.getInvokedInstance();
                drops.add(operation.originStmt.originStmt);
            }
        };
        ctx.cleanup();
        String target = "`.`" + ctx.getSessionId() + FeNameFormat.TEMPORARY_TABLE_SIGN + "tmp` FORCE";
        Assertions.assertEquals(ImmutableList.of("DROP TEMPORARY TABLE IF EXISTS `internal`.`renamed" + target,
                "DROP TEMPORARY TABLE IF EXISTS `internal`.`original" + target), drops);
    }

    private StmtExecutor forwardedCreate(ConnectContext ctx) {
        String sql = "CREATE TEMPORARY TABLE old_db.tmp (id INT)";
        StmtExecutor executor = new StmtExecutor(ctx, sql);
        Deencapsulation.setField(executor, "parsedStmt",
                new LogicalPlanAdapter(new NereidsParser().parseSingle(sql), ctx.getStatementContext()));
        Deencapsulation.setField(executor, "redirectStatus", RedirectStatus.FORWARD_WITH_SYNC);
        return executor;
    }

    @Test
    void resetCleansForwardedCreateByIdWithStaleDatabaseName(@Mocked GenericPool<FrontendService.Client> pool,
            @Mocked FrontendService.Client client) throws Exception {
        mockLocalCatalog(false);
        ConnectContext ctx = new ConnectContext();
        ctx.setEnv(env);
        ctx.setThreadLocalInfo();
        new Expectations() {
            {
                env.getClusterId();
                result = 77;
                env.getToken();
                result = "test-token";
                FRONTEND_POOL.borrowObject((TNetworkAddress) any, anyInt);
                result = client;
                client.dropSessionTemporaryTable(new TDropSessionTemporaryTableRequest(77, "test-token",
                        ctx.getSessionId(), 10L, 11L));
                result = new TDropSessionTemporaryTableResult(new TStatus(TStatusCode.OK), 43L);
                env.getJournalObservable().waitOn(43L, anyInt);
                times = 1;
            }
        };
        new MockUp<MasterOpExecutor>() {
            @Mock
            public void execute(Invocation invocation) {
                MasterOpExecutor op = invocation.getInvokedInstance();
                op.result = new TMasterOpResult().setStatusCode(0)
                        .setCreatedTemporaryTableDbIds(Collections.singletonMap(11L, 10L));
            }
        };
        try {
            Deencapsulation.invoke(forwardedCreate(ctx), "forwardToMaster");
            Assertions.assertEquals(Collections.singletonMap(11L, 10L), ctx.snapshotTemporaryTables());
            new MysqlConnectProcessor(ctx).handleResetConnection();
            Assertions.assertEquals(MysqlStateType.OK, ctx.getState().getStateType());
            Assertions.assertTrue(ctx.snapshotTemporaryTables().isEmpty());
            new Verifications() {
                {
                    db.getFullName();
                    times = 0;
                }
            };
        } finally {
            ConnectContext.remove();
        }
    }

    @Test
    void resetSucceedsAfterCreateFailedBeforeSendingRpc() throws Exception {
        mockLocalCatalog(false);
        ConnectContext ctx = new ConnectContext();
        ctx.setEnv(env);
        ctx.setThreadLocalInfo();
        new MockUp<FEOpExecutor>() {
            @Mock
            protected TMasterOpRequest buildStmtForwardParams() {
                return new TMasterOpRequest();
            }
        };
        new Expectations() {
            {
                env.checkReadyOrThrow();
                result = new DdlException("not ready before dispatch");
                times = 1;
            }
        };
        try {
            StmtExecutor create = forwardedCreate(ctx);
            Exception error = Assertions.assertThrows(Exception.class,
                    () -> Deencapsulation.invoke(create, "forwardToMaster"));
            Assertions.assertTrue(error.getMessage().contains("not ready before dispatch"));
            MysqlConnectProcessor processor = new MysqlConnectProcessor(ctx);
            processor.handleResetConnection();
            Assertions.assertEquals(MysqlStateType.OK, ctx.getState().getStateType());
        } finally {
            ConnectContext.remove();
        }
    }

    @Test
    void resetRejectsCreateWhenDispatchedRpcLosesReply(@Mocked GenericPool<FrontendService.Client> pool,
            @Mocked FrontendService.Client client) throws Exception {
        mockLocalCatalog(false);
        ConnectContext ctx = new ConnectContext();
        ctx.setEnv(env);
        ctx.setThreadLocalInfo();
        new MockUp<FEOpExecutor>() {
            @Mock
            protected TMasterOpRequest buildStmtForwardParams() {
                return new TMasterOpRequest();
            }
        };
        new Expectations() {
            {
                FRONTEND_POOL.borrowObject((TNetworkAddress) any, anyInt);
                result = client;
                client.forward((TMasterOpRequest) any);
                result = new TException("lost CREATE reply");
                result = new TMasterOpResult().setStatusCode(0);
            }
        };
        try {
            StmtExecutor create = forwardedCreate(ctx);
            Exception error = Assertions.assertThrows(Exception.class,
                    () -> Deencapsulation.invoke(create, "forwardToMaster"));
            Assertions.assertTrue(error.getMessage().contains("lost CREATE reply"));
            new MysqlConnectProcessor(ctx).handleResetConnection();
            Assertions.assertEquals(MysqlStateType.ERR, ctx.getState().getStateType());
            Assertions.assertTrue(ctx.getState().getErrorMessage().contains("unknown"));
        } finally {
            ConnectContext.remove();
        }
    }

    @Test
    void replayTimeoutKeepsConfirmedTableIdsForResetRetry(@Mocked GenericPool<FrontendService.Client> pool,
            @Mocked FrontendService.Client client) throws Exception {
        mockLocalCatalog(false);
        ConnectContext ctx = new ConnectContext();
        ctx.setEnv(env);
        ctx.setThreadLocalInfo();
        new MockUp<FEOpExecutor>() {
            @Mock
            protected TMasterOpRequest buildStmtForwardParams() {
                return new TMasterOpRequest();
            }
        };
        new Expectations() {
            {
                FRONTEND_POOL.borrowObject((TNetworkAddress) any, anyInt);
                result = client;
                client.forward((TMasterOpRequest) any);
                result = new TMasterOpResult().setStatusCode(0).setMaxJournalId(42L)
                        .setCreatedTemporaryTableDbIds(Collections.singletonMap(11L, 10L));
                client.dropSessionTemporaryTable((TDropSessionTemporaryTableRequest) any);
                result = new TDropSessionTemporaryTableResult(new TStatus(TStatusCode.OK), 43L);
                times = 2;
                env.getJournalObservable().waitOn(42L, anyInt);
                result = new DdlException("replay is behind");
                times = 1;
                env.getJournalObservable().waitOn(43L, anyInt);
                result = new DdlException("DROP replay is behind");
                result = null;
            }
        };
        try {
            StmtExecutor create = forwardedCreate(ctx);
            Exception error = Assertions.assertThrows(Exception.class,
                    () -> Deencapsulation.invoke(create, "forwardToMaster"));
            Assertions.assertTrue(error.getMessage().contains("replay is behind"));
            Assertions.assertEquals(Collections.singletonMap(11L, 10L), ctx.snapshotTemporaryTables());

            MysqlConnectProcessor processor = new MysqlConnectProcessor(ctx);
            processor.handleResetConnection();
            Assertions.assertEquals(MysqlStateType.ERR, ctx.getState().getStateType());
            Assertions.assertEquals(Collections.singletonMap(11L, 10L), ctx.snapshotTemporaryTables());
            processor.handleResetConnection();
            Assertions.assertEquals(MysqlStateType.OK, ctx.getState().getStateType());
            Assertions.assertTrue(ctx.snapshotTemporaryTables().isEmpty());
        } finally {
            ConnectContext.remove();
        }
    }

    @Test
    void disconnectDropsConfirmedIdWithoutWaitingForLocalReplay(@Mocked GenericPool<FrontendService.Client> pool,
            @Mocked FrontendService.Client client) throws Exception {
        mockLocalCatalog(false);
        ConnectContext ctx = new ConnectContext();
        ctx.setEnv(env);
        ctx.addForwardedTemporaryTables(Collections.singletonMap(11L, 10L), 42L,
                new TableName("internal", "stale_name", "tmp"));
        new Expectations() {
            {
                FRONTEND_POOL.borrowObject((TNetworkAddress) any, anyInt);
                result = client;
                client.dropSessionTemporaryTable((TDropSessionTemporaryTableRequest) any);
                result = new TDropSessionTemporaryTableResult(new TStatus(TStatusCode.OK), 43L);
                times = 1;
            }
        };
        ctx.cleanup();
        Assertions.assertTrue(ctx.snapshotTemporaryTables().isEmpty());
        new Verifications() {
            {
                env.getJournalObservable();
                times = 0;
                catalog.getDbNullable(10L);
                times = 0;
            }
        };
    }

    @Test
    void masterRejectionKeepsIdWithoutTryingLegacySql(@Mocked GenericPool<FrontendService.Client> pool,
            @Mocked FrontendService.Client client) throws Exception {
        mockLocalCatalog(false);
        ConnectContext ctx = new ConnectContext();
        ctx.setEnv(env);
        ctx.addTemporaryTable(10L, 11L);
        new Expectations() {
            {
                FRONTEND_POOL.borrowObject((TNetworkAddress) any, anyInt);
                result = client;
                client.dropSessionTemporaryTable((TDropSessionTemporaryTableRequest) any);
                result = new TDropSessionTemporaryTableResult(new TStatus(TStatusCode.NOT_MASTER), 0L);
            }
        };
        Assertions.assertThrows(DdlException.class, ctx::resetTemporaryTables);
        Assertions.assertEquals(Collections.singletonMap(11L, 10L), ctx.snapshotTemporaryTables());
        new Verifications() {
            {
                client.forward((TMasterOpRequest) any);
                times = 0;
            }
        };
    }

    @Test
    void proxyDoesNotCleanOriginatingSession() throws Exception {
        ConnectContext proxy = new ConnectContext(null, true, "test-session");
        proxy.addTemporaryTable(10L, 11L);
        proxy.deleteTempTable();

        new Verifications() {
            {
                env.getInternalCatalog();
                times = 0;
            }
        };
    }
}
