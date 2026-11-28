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

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.common.ClientPool;
import org.apache.doris.common.GenericPool;
import org.apache.doris.common.UserException;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.mysql.MysqlHandshakePacket;
import org.apache.doris.mysql.privilege.Auth;
import org.apache.doris.planner.StreamLoadPlanner;
import org.apache.doris.proto.InternalService;
import org.apache.doris.proto.Types;
import org.apache.doris.qe.QueryState.MysqlStateType;
import org.apache.doris.rpc.BackendServiceProxy;
import org.apache.doris.service.FrontendServiceImpl;
import org.apache.doris.system.Backend;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.thrift.FrontendService;
import org.apache.doris.thrift.TDropSessionTemporaryTableRequest;
import org.apache.doris.thrift.TDropSessionTemporaryTableResult;
import org.apache.doris.thrift.TFileFormatType;
import org.apache.doris.thrift.TFileType;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TStatus;
import org.apache.doris.thrift.TStatusCode;
import org.apache.doris.thrift.TStreamLoadPutRequest;
import org.apache.doris.thrift.TTxnParams;
import org.apache.doris.thrift.TUniqueId;
import org.apache.doris.thrift.TWaitingTxnStatusRequest;
import org.apache.doris.transaction.GlobalTransactionMgr;
import org.apache.doris.transaction.TransactionEntry;
import org.apache.doris.transaction.TransactionState;
import org.apache.doris.transaction.TransactionState.LoadJobSourceType;
import org.apache.doris.transaction.TransactionState.TxnCoordinator;
import org.apache.doris.transaction.TransactionState.TxnSourceType;
import org.apache.doris.transaction.TransactionStatus;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;

class ValuesTemporaryTableResetTest {
    private final AtomicBoolean master = new AtomicBoolean(true);
    private Env env;
    private InternalCatalog catalog;
    private Database db;
    private GlobalTransactionMgr transactions;
    private ConnectContext context;
    private TransactionEntry transaction;
    private BackendServiceProxy backend;
    private FrontendService.Client masterClient;
    private GenericPool<FrontendService.Client> originalPool;
    private MockedStatic<Env> mockedEnv;
    private MockedStatic<BackendServiceProxy> mockedBackend;
    private String owner;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void prepareValuesTransaction() throws Exception {
        env = Mockito.mock(Env.class, Mockito.RETURNS_DEEP_STUBS);
        catalog = Mockito.mock(InternalCatalog.class);
        db = new Database(10L, "db");
        Mockito.when(env.getInternalCatalog()).thenReturn(catalog);
        Mockito.when(env.isMaster()).thenAnswer(call -> master.get());
        Mockito.when(env.isReady()).thenReturn(true);
        Mockito.when(env.getMasterHost()).thenReturn("master");
        Mockito.when(env.getMasterRpcPort()).thenReturn(9020);
        Mockito.when(catalog.getName()).thenReturn("internal");
        Mockito.when(catalog.getDbNullable(10L)).thenReturn(db);
        Mockito.when(catalog.getDbOrDdlException(10L)).thenReturn(db);
        Mockito.when(catalog.getDbOrMetaException(10L)).thenReturn(db);
        Mockito.when(catalog.getDbOrAnalysisException(10L)).thenReturn(db);
        transactions = Mockito.spy(new GlobalTransactionMgr(env));
        transactions.addDatabaseTransactionMgr(10L);
        mockedEnv = Mockito.mockStatic(Env.class);
        mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
        mockedEnv.when(Env::getCurrentInternalCatalog).thenReturn(catalog);
        mockedEnv.when(Env::getCurrentGlobalTransactionMgr).thenReturn(transactions);
        mockedEnv.when(Env::getCurrentSystemInfo).thenReturn(Mockito.mock(SystemInfoService.class));

        context = Mockito.spy(new ConnectContext());
        context.setEnv(env);
        context.setThreadLocalInfo();
        context.setConnectionId(7);
        UserIdentity user = UserIdentity.createAnalyzedUserIdentWithIp("pool_user", "%");
        context.setCurrentUserIdentity(user);
        context.setQualifiedUser(user.getQualifiedUser());
        context.setRemoteIP("localhost");
        Mockito.doReturn(new byte[20]).when(context).getAuthPluginData();
        Auth auth = env.getAuth();
        Mockito.doAnswer(call -> {
            call.<List<UserIdentity>>getArgument(4).add(user);
            return null;
        }).when(auth).checkPassword(Mockito.eq("pool_user"), Mockito.anyString(), Mockito.any(byte[].class),
                Mockito.any(byte[].class), Mockito.anyList());

        TTxnParams params = new TTxnParams().setNeedTxn(true).setTxnId(19L).setDbId(10L)
                .setFragmentInstanceId(new TUniqueId(1L, 2L));
        transaction = new TransactionEntry(params, db, null);
        transaction.setLabel("values_reset");
        transaction.setBackend(new Backend(100L, "backend", 9050));
        transaction.setpLoadId(Types.PUniqueId.newBuilder().setHi(1L).setLo(2L).build());
        Deencapsulation.setField(transaction, "timeoutTimestamp", 1L);
        context.setTxnEntry(transaction);
        owner = context.getSessionId();
        Assertions.assertFalse(transaction.isTransactionBegan());

        backend = Mockito.mock(BackendServiceProxy.class);
        mockedBackend = Mockito.mockStatic(BackendServiceProxy.class);
        mockedBackend.when(BackendServiceProxy::getInstance).thenReturn(backend);
        Mockito.when(backend.rollback(Mockito.any(), Mockito.any(), Mockito.any()))
                .thenReturn(CompletableFuture.completedFuture(rollbackReply(1)));

        originalPool = ClientPool.frontendPool;
        ClientPool.frontendPool = Mockito.mock(GenericPool.class);
        masterClient = Mockito.mock(FrontendService.Client.class);
        Mockito.when(ClientPool.frontendPool.borrowObject(Mockito.any(TNetworkAddress.class), Mockito.anyInt()))
                .thenReturn(masterClient);
        FrontendServiceImpl service = new FrontendServiceImpl(null);
        Mockito.when(masterClient.waitingTxnStatus(Mockito.any(TWaitingTxnStatusRequest.class))).thenAnswer(call -> {
            boolean wasMaster = master.getAndSet(true);
            try {
                return service.waitingTxnStatus(call.getArgument(0));
            } finally {
                master.set(wasMaster);
            }
        });
        Mockito.when(masterClient.dropSessionTemporaryTable(Mockito.any(TDropSessionTemporaryTableRequest.class)))
                .thenAnswer(call -> {
                    TDropSessionTemporaryTableRequest request = call.getArgument(0);
                    Assertions.assertEquals(owner, request.getSessionId());
                    Assertions.assertEquals(10L, request.getDbId());
                    Assertions.assertEquals(11L, request.getTableId());
                    catalog.dropTemporaryTable(db, request.getTableId(), request.getSessionId());
                    return new TDropSessionTemporaryTableResult(new TStatus(TStatusCode.OK), 42L);
                });
    }

    @AfterEach
    void restorePoolsAndThreadContext() {
        ClientPool.frontendPool = originalPool;
        ConnectContext.remove();
        mockedBackend.close();
        mockedEnv.close();
    }

    static Stream<Arguments> resetModes() {
        return Stream.of(Arguments.of(false, false), Arguments.of(false, true),
                Arguments.of(true, false), Arguments.of(true, true));
    }

    static Stream<Arguments> unconfirmedStates() {
        return resetModes().flatMap(mode -> {
            Object[] values = mode.get();
            return Stream.of(TransactionStatus.PREPARE, TransactionStatus.UNKNOWN, TransactionStatus.COMMITTED,
                    TransactionStatus.VISIBLE).map(status -> Arguments.of(values[0], values[1], status));
        });
    }

    private InternalService.PRollbackResult rollbackReply(int code) {
        return InternalService.PRollbackResult.newBuilder()
                .setStatus(Types.PStatus.newBuilder().setStatusCode(code)
                        .addErrorMsgs("could not find stream load context")).build();
    }

    private void replayTransaction(TransactionStatus status) throws Exception {
        TransactionState state = new TransactionState(10L, Collections.emptyList(), 19L, "values_reset", null,
                LoadJobSourceType.INSERT_STREAMING, new TxnCoordinator(TxnSourceType.FE, 0L, "localhost", 0L), -1L, 1L);
        state.setTransactionStatus(status);
        if (status.isFinalStatus()) {
            state.setFinishTime(1L);
        }
        transactions.replayUpsertTransactionState(state);
    }

    private void reset(boolean changeUser) {
        MysqlConnectProcessor processor = new MysqlConnectProcessor(context);
        if (changeUser) {
            ByteBuffer payload = ByteBuffer.allocate(128);
            payload.put("pool_user".getBytes(StandardCharsets.UTF_8)).put((byte) 0).put((byte) 0);
            payload.put((byte) 0).putShort((short) 33);
            payload.put(MysqlHandshakePacket.AUTH_PLUGIN_NAME.getBytes(StandardCharsets.UTF_8)).put((byte) 0).flip();
            Deencapsulation.setField(processor, "packetBuf", payload);
            Deencapsulation.invoke(processor, "handleChangeUser");
        } else {
            processor.handleResetConnection();
        }
        Assertions.assertEquals(7, context.getConnectionId());
    }

    private void assertTwoSuccessfulResets(boolean changeUser) throws Exception {
        for (int i = 0; i < 2; i++) {
            reset(changeUser);
            Assertions.assertEquals(MysqlStateType.OK, context.getState().getStateType(),
                    context.getState().getErrorMessage());
            Assertions.assertNull(context.getTxnEntry());
            Assertions.assertTrue(context.snapshotTemporaryTables().isEmpty());
        }
        Assertions.assertNotEquals(owner, context.getSessionId());
        Mockito.verify(backend).rollback(Mockito.any(), Mockito.any(), Mockito.any());
        Mockito.verify(catalog).dropTemporaryTable(db, 11L, owner);
    }

    private void assertTwoRejectedResets(boolean changeUser) throws Exception {
        for (int i = 0; i < 2; i++) {
            reset(changeUser);
            Assertions.assertEquals(MysqlStateType.ERR, context.getState().getStateType());
            Assertions.assertSame(transaction, context.getTxnEntry());
            Assertions.assertEquals(owner, context.getSessionId());
            Assertions.assertEquals(Collections.singletonMap(11L, 10L), context.snapshotTemporaryTables());
        }
        Mockito.verify(catalog, Mockito.never()).dropTemporaryTable(Mockito.any(), Mockito.anyLong(),
                Mockito.anyString());
    }

    @ParameterizedTest
    @MethodSource("resetModes")
    void resetsTwiceAfterMasterConfirmsValuesTransactionAborted(boolean follower, boolean changeUser) throws Exception {
        master.set(!follower);
        context.addTemporaryTable(10L, 11L);
        replayTransaction(TransactionStatus.ABORTED);
        assertTwoSuccessfulResets(changeUser);
    }

    @ParameterizedTest
    @MethodSource("resetModes")
    void resetsTwiceAfterExpiredValuesTransactionRecordWasPurged(boolean follower, boolean changeUser)
            throws Exception {
        context.addTemporaryTable(10L, 11L);
        replayTransaction(TransactionStatus.ABORTED);
        transactions.removeExpiredAndTimeoutTxns();
        Assertions.assertNull(transactions.getTransactionState(10L, 19L));
        master.set(!follower);
        assertTwoSuccessfulResets(changeUser);
    }

    @ParameterizedTest
    @MethodSource("unconfirmedStates")
    void retainsValuesTransactionAndTablesUntilTerminationIsConfirmed(boolean follower, boolean changeUser,
            TransactionStatus status) throws Exception {
        master.set(!follower);
        context.addTemporaryTable(10L, 11L);
        replayTransaction(status);
        assertTwoRejectedResets(changeUser);
    }

    @ParameterizedTest
    @MethodSource("resetModes")
    void rejectsMissingValuesTransactionBeforeItsOriginalDeadline(boolean follower, boolean changeUser)
            throws Exception {
        master.set(!follower);
        context.addTemporaryTable(10L, 11L);
        transaction.recordInsertValuesTransactionTimeout(60);
        assertTwoRejectedResets(changeUser);
    }

    @ParameterizedTest
    @MethodSource("resetModes")
    void rejectsUnavailableMasterInsteadOfTreatingMissingRecordAsPurged(boolean follower, boolean changeUser)
            throws Exception {
        master.set(!follower);
        context.addTemporaryTable(10L, 11L);
        Mockito.when(env.isReady()).thenReturn(false);
        assertTwoRejectedResets(changeUser);
    }

    @ParameterizedTest
    @MethodSource("resetModes")
    void ordinaryConnectionSkipsValuesRollbackAndStatusLookup(boolean follower, boolean changeUser) throws Exception {
        master.set(!follower);
        for (int i = 0; i < 2; i++) {
            reset(changeUser);
            Assertions.assertEquals(MysqlStateType.OK, context.getState().getStateType());
            Assertions.assertSame(transaction, context.getTxnEntry());
            Assertions.assertEquals(owner, context.getSessionId());
        }
        Mockito.verifyNoInteractions(backend, masterClient);
        Mockito.verify(transactions, Mockito.never()).getTransactionStatusForTemporaryTableReset(Mockito.anyLong(),
                Mockito.anyLong());
    }

    @Test
    void streamStartupRecordsOriginalDeadlineBeforePlanFailure() throws Exception {
        OlapTable table = Mockito.mock(OlapTable.class);
        transaction.setTable(table);
        Mockito.when(table.tryReadLock(Mockito.anyLong(), Mockito.any(TimeUnit.class))).thenReturn(true);
        TStreamLoadPutRequest request = new TStreamLoadPutRequest().setTxnId(19L).setLoadId(new TUniqueId(1L, 2L))
                .setFileType(TFileType.FILE_STREAM).setFormatType(TFileFormatType.FORMAT_CSV_PLAIN).setTimeout(60);
        long before = System.currentTimeMillis();
        try (MockedConstruction<StreamLoadPlanner> planners = Mockito.mockConstruction(StreamLoadPlanner.class,
                (planner, construction) -> Mockito.when(planner.plan(Mockito.any(TUniqueId.class)))
                        .thenThrow(new UserException("injected plan failure")))) {
            UserException error = Assertions.assertThrows(UserException.class,
                    () -> new InsertStreamTxnExecutor(transaction).beginTransaction(request));
            Assertions.assertTrue(error.getMessage().contains("injected plan failure"));
            Assertions.assertEquals(1, planners.constructed().size());
        }
        long deadline = Deencapsulation.getField(transaction, "timeoutTimestamp");
        Assertions.assertTrue(deadline >= before + 60000 && deadline <= System.currentTimeMillis() + 60000);
        context.addTemporaryTable(10L, 11L);
        assertTwoRejectedResets(false);
        Assertions.assertEquals(deadline, (long) Deencapsulation.getField(transaction, "timeoutTimestamp"));
    }

    @Test
    @SuppressWarnings("unchecked")
    void valuesRollbackSharesTheRemainingResetBudget() throws Exception {
        Future<InternalService.PRollbackResult> pending = Mockito.mock(Future.class);
        Mockito.when(backend.rollback(Mockito.any(), Mockito.any(), Mockito.any())).thenReturn(pending);
        Mockito.when(pending.get(Mockito.anyLong(), Mockito.eq(TimeUnit.MILLISECONDS)))
                .thenThrow(new TimeoutException("injected slow backend"));
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
        Assertions.assertThrows(TimeoutException.class,
                () -> new InsertStreamTxnExecutor(transaction).abortTransactionForTemporaryTableReset(deadline));
        ArgumentCaptor<Long> timeout = ArgumentCaptor.forClass(Long.class);
        Mockito.verify(pending).get(timeout.capture(), Mockito.eq(TimeUnit.MILLISECONDS));
        Assertions.assertTrue(timeout.getValue() > 0 && timeout.getValue() <= 2000);
    }

    @Test
    @SuppressWarnings("unchecked")
    void ordinaryValuesRollbackRetainsItsFiveSecondWait() throws Exception {
        Future<InternalService.PRollbackResult> reply = Mockito.mock(Future.class);
        Mockito.when(backend.rollback(Mockito.any(), Mockito.any(), Mockito.any())).thenReturn(reply);
        Mockito.when(reply.get(5000L, TimeUnit.MILLISECONDS)).thenReturn(rollbackReply(0));
        new InsertStreamTxnExecutor(transaction).abortTransaction();
        Mockito.verify(reply).get(5000L, TimeUnit.MILLISECONDS);
    }
}
