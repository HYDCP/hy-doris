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

import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.persist.EditLog;
import org.apache.doris.qe.QueryState.MysqlStateType;
import org.apache.doris.statistics.AnalysisManager;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.thrift.TTxnLoadInfo;
import org.apache.doris.thrift.TTxnParams;
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
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Collections;

class PromotedTemporaryTableResetTest {
    private Env env;
    private InternalCatalog catalog;
    private Database db;
    private GlobalTransactionMgr transactions;
    private ConnectContext context;
    private TransactionEntry transaction;
    private MockedStatic<Env> mockedEnv;
    private String owner;

    @BeforeEach
    void prepareFollowerConnectionThenPromote() throws Exception {
        env = Mockito.mock(Env.class);
        catalog = Mockito.mock(InternalCatalog.class);
        db = new Database(10L, "db");
        Mockito.when(env.getInternalCatalog()).thenReturn(catalog);
        Mockito.when(env.getEditLog()).thenReturn(Mockito.mock(EditLog.class));
        Mockito.when(env.getAnalysisManager()).thenReturn(Mockito.mock(AnalysisManager.class));
        Mockito.when(env.isReady()).thenReturn(true);
        Mockito.when(catalog.getName()).thenReturn("internal");
        Mockito.when(catalog.getDbNullable(10L)).thenReturn(db);
        Mockito.when(catalog.getDbOrDdlException(10L)).thenReturn(db);
        Mockito.when(catalog.getDbOrMetaException(10L)).thenReturn(db);
        transactions = Mockito.spy(new GlobalTransactionMgr(env));
        transactions.addDatabaseTransactionMgr(10L);
        mockedEnv = Mockito.mockStatic(Env.class);
        mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
        mockedEnv.when(Env::getCurrentInternalCatalog).thenReturn(catalog);
        mockedEnv.when(Env::getCurrentGlobalTransactionMgr).thenReturn(transactions);
        mockedEnv.when(Env::getCurrentSystemInfo).thenReturn(Mockito.mock(SystemInfoService.class));
        context = new ConnectContext();
        context.setEnv(env);
        transaction = new TransactionEntry();
        transaction.setTxnConf(new TTxnParams().setNeedTxn(true).setTxnId(-1L));
        transaction.setLabel("original_transaction");
        transaction.setTxnLoadInfoInObserver(new TTxnLoadInfo().setLabel("original_transaction").setDbId(10L)
                .setTxnId(19L).setTimeoutTimestamp(1L));
        context.setTxnEntry(transaction);
        owner = context.getSessionId();
        Assertions.assertFalse(env.isMaster());
        Mockito.when(env.isMaster()).thenReturn(true);
    }

    @AfterEach
    void releaseThreadContext() {
        ConnectContext.remove();
        mockedEnv.close();
    }

    private TransactionState replayTransaction(TransactionStatus status) throws Exception {
        TransactionState state = new TransactionState(10L, Collections.emptyList(), 19L, "original_transaction",
                null, LoadJobSourceType.INSERT_STREAMING,
                new TxnCoordinator(TxnSourceType.FE, 0L, "localhost", 0L), -1L, 1L);
        state.setTransactionStatus(status);
        if (status.isFinalStatus()) {
            state.setFinishTime(1L);
        }
        transactions.replayUpsertTransactionState(state);
        return state;
    }

    private void assertTwoSuccessfulResets() throws Exception {
        MysqlConnectProcessor processor = new MysqlConnectProcessor(context);
        for (int i = 0; i < 2; i++) {
            processor.handleResetConnection();
            Assertions.assertEquals(MysqlStateType.OK, context.getState().getStateType(),
                    context.getState().getErrorMessage());
            Assertions.assertNull(context.getTxnEntry());
            Assertions.assertTrue(context.snapshotTemporaryTables().isEmpty());
        }
        Mockito.verify(catalog).dropTemporaryTable(db, 11L, owner);
        Assertions.assertNotEquals(owner, context.getSessionId());
    }

    private void assertTwoRejectedResets() throws Exception {
        MysqlConnectProcessor processor = new MysqlConnectProcessor(context);
        for (int i = 0; i < 2; i++) {
            processor.handleResetConnection();
            Assertions.assertEquals(MysqlStateType.ERR, context.getState().getStateType());
            Assertions.assertSame(transaction, context.getTxnEntry());
            Assertions.assertEquals(Collections.singletonMap(11L, 10L), context.snapshotTemporaryTables());
            Assertions.assertEquals(owner, context.getSessionId());
        }
        Mockito.verify(catalog, Mockito.never()).dropTemporaryTable(Mockito.any(), Mockito.anyLong(),
                Mockito.anyString());
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void resetsTwiceWithAbortedOrPurgedTransaction(boolean purged) throws Exception {
        context.addTemporaryTable(10L, 11L);
        replayTransaction(TransactionStatus.ABORTED);
        if (purged) {
            transactions.removeExpiredAndTimeoutTxns();
            Assertions.assertNull(transactions.getTransactionState(10L, 19L));
        }
        assertTwoSuccessfulResets();
    }

    @ParameterizedTest
    @EnumSource(value = TransactionStatus.class, names = {"PREPARE", "PRECOMMITTED"})
    void rollsBackRetainedTransactionBeforeDroppingTemporaryTable(TransactionStatus status) throws Exception {
        context.addTemporaryTable(10L, 11L);
        TransactionState state = replayTransaction(status);
        Mockito.doAnswer(call -> {
            Assertions.assertEquals(TransactionStatus.ABORTED, state.getTransactionStatus());
            return null;
        }).when(catalog).dropTemporaryTable(db, 11L, owner);
        assertTwoSuccessfulResets();
        Mockito.verify(transactions).abortTransaction(10L, 19L, "user rollback");
    }

    @ParameterizedTest
    @EnumSource(value = TransactionStatus.class, names = {"UNKNOWN", "COMMITTED", "VISIBLE"})
    void retainsCleanupStateWhenTransactionCannotBeRolledBack(TransactionStatus status) throws Exception {
        context.addTemporaryTable(10L, 11L);
        replayTransaction(status);
        assertTwoRejectedResets();
        Mockito.verify(transactions, Mockito.never()).abortTransaction(Mockito.anyLong(), Mockito.anyLong(),
                Mockito.anyString());
    }

    @Test
    void rejectsMissingTransactionBeforeItsOriginalDeadline() throws Exception {
        context.addTemporaryTable(10L, 11L);
        transaction.setTxnLoadInfoInObserver(new TTxnLoadInfo().setLabel("original_transaction").setDbId(10L)
                .setTxnId(19L).setTimeoutTimestamp(System.currentTimeMillis() + 60000));
        assertTwoRejectedResets();
    }

    @Test
    void rejectsUnreadyMasterWithRetainedTransaction() throws Exception {
        context.addTemporaryTable(10L, 11L);
        replayTransaction(TransactionStatus.PREPARE);
        Mockito.when(env.isReady()).thenReturn(false);
        assertTwoRejectedResets();
    }

    @Test
    void rejectsMissingDatabaseTransactionManager() throws Exception {
        context.addTemporaryTable(10L, 11L);
        transactions.removeDatabaseTransactionMgr(10L);
        assertTwoRejectedResets();
    }

    @Test
    void rejectsFailedStateLookup() throws Exception {
        context.addTemporaryTable(10L, 11L);
        Mockito.doThrow(new AnalysisException("injected unavailable state"))
                .when(transactions).getTransactionStatusForTemporaryTableReset(10L, 19L);
        assertTwoRejectedResets();
    }

    @Test
    void ordinaryConnectionSkipsStateLookupOnBothResets() throws Exception {
        context.resetTemporaryTables();
        context.resetTemporaryTables();
        Assertions.assertSame(transaction, context.getTxnEntry());
        Mockito.verify(transactions, Mockito.never()).getTransactionStatusForTemporaryTableReset(Mockito.anyLong(),
                Mockito.anyLong());
        Mockito.verify(transactions, Mockito.never()).abortTransaction(Mockito.anyLong(), Mockito.anyLong(),
                Mockito.anyString());
    }
}
