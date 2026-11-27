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

package org.apache.doris.service;

import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.SinglePartitionInfo;
import org.apache.doris.common.FeNameFormat;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.persist.EditLog;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.MysqlConnectProcessor;
import org.apache.doris.system.Frontend;
import org.apache.doris.thrift.TDropSessionTemporaryTableRequest;
import org.apache.doris.thrift.TDropSessionTemporaryTableResult;
import org.apache.doris.thrift.TFrontendReportAliveSessionResult;
import org.apache.doris.thrift.TMasterOpRequest;
import org.apache.doris.thrift.TMasterOpResult;
import org.apache.doris.thrift.TStatusCode;
import org.apache.doris.thrift.TWaitingTxnStatusRequest;
import org.apache.doris.transaction.GlobalTransactionMgr;
import org.apache.doris.transaction.TransactionState;
import org.apache.doris.transaction.TransactionState.LoadJobSourceType;
import org.apache.doris.transaction.TransactionState.TxnCoordinator;
import org.apache.doris.transaction.TransactionState.TxnSourceType;
import org.apache.doris.transaction.TransactionStatus;

import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TField;
import org.apache.thrift.protocol.TProtocolUtil;
import org.apache.thrift.protocol.TType;
import org.apache.thrift.transport.TMemoryBuffer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;

class ForwardedTemporaryTableCleanupTest {
    @Test
    void dropsOnlyOwnedIdAfterRenameAndAcknowledgesRepeats() throws Exception {
        Env env = Mockito.mock(Env.class, Mockito.RETURNS_DEEP_STUBS);
        InternalCatalog catalog = Mockito.mock(InternalCatalog.class);
        Mockito.when(env.getInternalCatalog()).thenReturn(catalog);
        Mockito.when(env.isMaster()).thenReturn(true);
        Mockito.when(env.isReady()).thenReturn(true);
        Mockito.when(env.getClusterId()).thenReturn(7);
        Mockito.when(env.getToken()).thenReturn("test-token");
        Mockito.when(env.getMaxJournalId()).thenReturn(43L);
        Database db = new Database(10L, "original");
        Mockito.when(catalog.getDbNullable(10L)).thenReturn(db);
        Mockito.doCallRealMethod().when(catalog).dropTemporaryTable(Mockito.eq(db), Mockito.anyLong(),
                Mockito.anyString());
        Mockito.doAnswer(call -> {
            OlapTable table = call.getArgument(1);
            db.unregisterTable(table.getId());
            return true;
        }).when(catalog).unprotectDropTable(Mockito.eq(db), Mockito.any(), Mockito.eq(true),
                Mockito.eq(false), Mockito.eq(0L));
        FrontendServiceImpl service = Mockito.mock(FrontendServiceImpl.class, Mockito.CALLS_REAL_METHODS);
        TDropSessionTemporaryTableRequest request = new TDropSessionTemporaryTableRequest(7, "test-token",
                "owner", 10L, 11L);
        try (MockedStatic<Env> mocked = Mockito.mockStatic(Env.class)) {
            mocked.when(Env::getCurrentEnv).thenReturn(env);
            OlapTable owned = new OlapTable(11L, "owner" + FeNameFormat.TEMPORARY_TABLE_SIGN + "tmp", true,
                    Collections.emptyList(), KeysType.DUP_KEYS, new SinglePartitionInfo(), null, null);
            OlapTable permanent = new OlapTable(12L, "tmp", Collections.emptyList(), KeysType.DUP_KEYS,
                    new SinglePartitionInfo(), null);
            db.registerTable(owned);
            db.registerTable(permanent);
            db.setNameWithLock("renamed");

            Assertions.assertEquals(TStatusCode.NOT_AUTHORIZED, service.dropSessionTemporaryTable(
                    request.deepCopy().setToken("invalid")).getStatus().getStatusCode());
            Assertions.assertEquals(TStatusCode.INTERNAL_ERROR, service.dropSessionTemporaryTable(
                    request.deepCopy().setSessionId("another-session")).getStatus().getStatusCode());
            Assertions.assertEquals(TStatusCode.INTERNAL_ERROR, service.dropSessionTemporaryTable(
                    request.deepCopy().setTableId(12L)).getStatus().getStatusCode());
            Assertions.assertEquals(TStatusCode.INVALID_ARGUMENT, service.dropSessionTemporaryTable(
                    request.deepCopy().setSessionId("")).getStatus().getStatusCode());
            Assertions.assertSame(owned, db.getTableNullable(11L));
            TDropSessionTemporaryTableResult response = service.dropSessionTemporaryTable(request);
            Assertions.assertEquals(TStatusCode.OK, response.getStatus().getStatusCode());
            Assertions.assertEquals(43L, response.getMaxJournalId());
            Assertions.assertNull(db.getTableNullable(11L));
            Assertions.assertEquals(TStatusCode.OK, service.dropSessionTemporaryTable(request).getStatus()
                    .getStatusCode());
            Assertions.assertSame(permanent, db.getTableNullable(12L));
            Mockito.verify(catalog).unprotectDropTable(db, owned, true, false, 0L);
        }
    }

    @Test
    void reportsCreatedIdsBeforeCheckingOriginAndSkipsOrdinaryRequests() throws Exception {
        Env env = Mockito.mock(Env.class);
        Frontend origin = Mockito.mock(Frontend.class);
        Mockito.when(env.checkFeExist("origin", 9010)).thenReturn(origin);
        Mockito.when(env.getMaxJournalId()).thenReturn(43L);
        Executor originChecks = Mockito.mock(Executor.class);
        FrontendServiceImpl service = Mockito.spy(new FrontendServiceImpl(null, originChecks));
        Mockito.doReturn(false).when(service).isTemporaryTableOriginClosed(origin, 7, "owner");
        TMasterOpRequest request = new TMasterOpRequest().setClientNodeHost("origin").setClientNodePort(9010)
                .setSessionId("owner").setTempTableOriginConnectionId(7);
        try (MockedStatic<Env> mocked = Mockito.mockStatic(Env.class);
                MockedConstruction<MysqlConnectProcessor> processors = Mockito.mockConstruction(
                        MysqlConnectProcessor.class, (processor, construction) -> {
                            ConnectContext proxy = (ConnectContext) construction.arguments().get(0);
                            Mockito.when(processor.proxyExecute(request)).thenAnswer(call -> {
                                proxy.addTemporaryTable(10L, 11L);
                                return new TMasterOpResult().setStatus("OK");
                            });
                        })) {
            mocked.when(Env::getCurrentEnv).thenReturn(env);
            TMasterOpResult result = service.forward(request);
            Assertions.assertEquals(Collections.singletonMap(11L, 10L), result.getCreatedTemporaryTableDbIds());
            Assertions.assertEquals(43L, result.getMaxJournalId());
            Mockito.verify(service, Mockito.never()).isTemporaryTableOriginClosed(origin, 7, "owner");

            ArgumentCaptor<Runnable> backgroundTask = ArgumentCaptor.forClass(Runnable.class);
            Mockito.verify(originChecks).execute(backgroundTask.capture());
            backgroundTask.getValue().run();
            Mockito.verify(service).isTemporaryTableOriginClosed(origin, 7, "owner");

            Mockito.doThrow(new RejectedExecutionException()).when(originChecks).execute(Mockito.any(Runnable.class));
            Assertions.assertEquals(result, service.forward(request));
            Mockito.verify(service).isTemporaryTableOriginClosed(origin, 7, "owner");

            request.unsetTempTableOriginConnectionId();
            Assertions.assertFalse(service.forward(request).isSetCreatedTemporaryTableDbIds());
            Assertions.assertEquals(3, processors.constructed().size());
            Mockito.verify(originChecks, Mockito.times(2)).execute(Mockito.any(Runnable.class));
        }
    }

    @Test
    void cleansTableWhenCreateFinishesAfterOriginDisconnected() throws Exception {
        Env env = Mockito.mock(Env.class);
        InternalCatalog catalog = Mockito.mock(InternalCatalog.class);
        Database database = Mockito.mock(Database.class);
        Mockito.when(env.isMaster()).thenReturn(true);
        Mockito.when(env.getInternalCatalog()).thenReturn(catalog);
        Mockito.when(catalog.getDbNullable(10L)).thenReturn(database);
        Frontend origin = Mockito.mock(Frontend.class);

        FrontendServiceImpl service = Mockito.mock(FrontendServiceImpl.class, Mockito.CALLS_REAL_METHODS);
        try (MockedStatic<Env> mocked = Mockito.mockStatic(Env.class)) {
            mocked.when(Env::getCurrentEnv).thenReturn(env);
            String sessionId = "original-session";
            Map<Long, Long> tables = Collections.singletonMap(11L, 10L);
            Mockito.doReturn(true).when(service).isTemporaryTableOriginClosed(origin, 7, sessionId);
            service.cleanupLateTemporaryTables(origin, 7, sessionId, tables);
            Mockito.verify(catalog).dropTemporaryTable(database, 11L, sessionId);
        }
    }

    @Test
    void leavesLateTableWhileOriginConnectionIsStillAlive() throws Exception {
        Env env = Mockito.mock(Env.class);
        Frontend origin = Mockito.mock(Frontend.class);
        FrontendServiceImpl service = Mockito.mock(FrontendServiceImpl.class, Mockito.CALLS_REAL_METHODS);
        String sessionId = "original-session";
        Mockito.doReturn(false).when(service).isTemporaryTableOriginClosed(origin, 7, sessionId);
        try (MockedStatic<Env> mocked = Mockito.mockStatic(Env.class)) {
            mocked.when(Env::getCurrentEnv).thenReturn(env);
            service.cleanupLateTemporaryTables(origin, 7, sessionId, Collections.singletonMap(11L, 10L));
            Mockito.verifyNoInteractions(env);
        }
    }

    @Test
    void preservesOrdinaryForwardingFieldsAndUsesSeparateThriftIdsForTemporaryTables() throws TException {
        TMasterOpRequest request = new TMasterOpRequest("user", "db", "SELECT 1");
        TMasterOpResult result = new TMasterOpResult(43L, ByteBuffer.wrap(new byte[0]));

        Assertions.assertEquals(Arrays.asList((short) 1, (short) 2, (short) 3), serializedFieldIds(request));
        Assertions.assertEquals(Arrays.asList((short) 1, (short) 2), serializedFieldIds(result));
        Assertions.assertEquals(Arrays.asList((short) 1, (short) 2, (short) 3, (short) 5001),
                serializedFieldIds(request.setTempTableOriginConnectionId(7)));
        Assertions.assertEquals(Arrays.asList((short) 1, (short) 2, (short) 5001),
                serializedFieldIds(result.setCreatedTemporaryTableDbIds(Collections.singletonMap(11L, 10L))));
    }

    private List<Short> serializedFieldIds(TBase<?, ?> value) throws TException {
        TBinaryProtocol protocol = new TBinaryProtocol(new TMemoryBuffer(128));
        value.write(protocol);
        List<Short> ids = new ArrayList<>();
        protocol.readStructBegin();
        for (TField field = protocol.readFieldBegin(); field.type != TType.STOP; field = protocol.readFieldBegin()) {
            ids.add(field.id);
            TProtocolUtil.skip(protocol, field.type);
            protocol.readFieldEnd();
        }
        protocol.readStructEnd();
        return ids;
    }

    @Test
    void onlyReadyMasterConfirmsPurgedTransactionForTemporaryTableReset() throws Exception {
        Env env = Mockito.mock(Env.class);
        InternalCatalog catalog = Mockito.mock(InternalCatalog.class);
        Mockito.when(env.getInternalCatalog()).thenReturn(catalog);
        Mockito.when(env.getEditLog()).thenReturn(Mockito.mock(EditLog.class));
        Mockito.when(env.isMaster()).thenReturn(true);
        Mockito.when(env.isReady()).thenReturn(true);
        Database db = new Database(10L, "db");
        Mockito.when(catalog.getDbNullable(10L)).thenReturn(db);
        Mockito.when(catalog.getDbOrAnalysisException(10L)).thenReturn(db);
        Mockito.when(catalog.getDbNullable(20L)).thenReturn(new Database(20L, "no_transaction_manager"));
        GlobalTransactionMgr txnManager = new GlobalTransactionMgr(env);
        txnManager.addDatabaseTransactionMgr(10L);
        TransactionState aborted = new TransactionState(10L, Collections.emptyList(), 19L, "aborted", null,
                LoadJobSourceType.INSERT_STREAMING, new TxnCoordinator(TxnSourceType.FE, 0L, "localhost", 0L), -1L, 1L);
        TWaitingTxnStatusRequest request = new TWaitingTxnStatusRequest().setDbId(10L).setTxnId(19L)
                .setCheckExpiredTxnForTemporaryTableReset(true);
        FrontendServiceImpl service = new FrontendServiceImpl(null);
        try (MockedStatic<Env> mocked = Mockito.mockStatic(Env.class)) {
            mocked.when(Env::getCurrentEnv).thenReturn(env);
            mocked.when(Env::getCurrentInternalCatalog).thenReturn(catalog);
            mocked.when(Env::getCurrentGlobalTransactionMgr).thenReturn(txnManager);

            txnManager.replayUpsertTransactionState(aborted);
            Assertions.assertEquals(TransactionStatus.PREPARE.value(),
                    service.waitingTxnStatus(request).getTxnStatusId());
            aborted.setTransactionStatus(TransactionStatus.ABORTED);
            aborted.setFinishTime(1L);
            txnManager.replayUpsertTransactionState(aborted);
            Assertions.assertEquals(TransactionStatus.ABORTED.value(),
                    service.waitingTxnStatus(request).getTxnStatusId());
            txnManager.removeExpiredAndTimeoutTxns();
            Assertions.assertEquals(TStatusCode.NOT_FOUND, service.waitingTxnStatus(request).getStatus()
                    .getStatusCode());
            Assertions.assertEquals(TStatusCode.INTERNAL_ERROR,
                    service.waitingTxnStatus(new TWaitingTxnStatusRequest().setDbId(10L).setTxnId(19L))
                            .getStatus().getStatusCode());
            Assertions.assertEquals(TStatusCode.INTERNAL_ERROR, service.waitingTxnStatus(request.deepCopy()
                    .setDbId(20L)).getStatus().getStatusCode());

            Mockito.when(env.isReady()).thenReturn(false);
            Assertions.assertEquals(TStatusCode.INTERNAL_ERROR, service.waitingTxnStatus(request).getStatus()
                    .getStatusCode());
            Mockito.when(env.isReady()).thenReturn(true);
            Mockito.when(env.isMaster()).thenReturn(false);
            Assertions.assertEquals(TStatusCode.INTERNAL_ERROR, service.waitingTxnStatus(request).getStatus()
                    .getStatusCode());
        }
    }

    @Test
    void requiresAnAuthoritativeTargetedSessionResponse() {
        TFrontendReportAliveSessionResult result = new TFrontendReportAliveSessionResult()
                .setStatus(TStatusCode.OK).setSessionIdList(Collections.emptyList());
        Assertions.assertFalse(FrontendServiceImpl.confirmsTemporaryTableOriginClosed(result, 7, "old-session"));
        result.setCheckedConnectionId(8);
        Assertions.assertFalse(FrontendServiceImpl.confirmsTemporaryTableOriginClosed(result, 7, "old-session"));
        result.setCheckedConnectionId(7).setSessionIdList(Collections.singletonList("old-session"));
        Assertions.assertFalse(FrontendServiceImpl.confirmsTemporaryTableOriginClosed(result, 7, "old-session"));
        result.setSessionIdList(Collections.singletonList("reused-connection"));
        Assertions.assertTrue(FrontendServiceImpl.confirmsTemporaryTableOriginClosed(result, 7, "old-session"));
    }
}
