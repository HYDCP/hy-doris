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
import org.apache.doris.catalog.Table;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.FeNameFormat;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.qe.QueryState.MysqlStateType;
import org.apache.doris.transaction.TransactionEntry;

import mockit.Expectations;
import mockit.Invocation;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import mockit.Verifications;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

class ConnectContextTempTableTest {
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
        ctx.addTemporaryTable(10L, 11L);
        ctx.addTemporaryTable(10L, 12L);
        new Expectations() {
            {
                catalog.dropTemporaryTable(db, 11L, ctx.getSessionId());
                result = new DdlException("injected DROP failure");
                result = null;
                times = 2;
            }
        };

        ConnectProcessor processor = new MysqlConnectProcessor(ctx);
        processor.handleResetConnection();
        Assertions.assertEquals(MysqlStateType.ERR, ctx.getState().getStateType());
        processor.handleResetConnection();
        Assertions.assertEquals(MysqlStateType.OK, ctx.getState().getStateType());

        new Verifications() {
            {
                catalog.dropTemporaryTable(db, 12L, ctx.getSessionId());
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
        ctx.cleanup();
        new Verifications() {
            {
                catalog.dropTemporaryTable(db, anyLong, anyString);
                times = 0;
            }
        };

        ctx.addTemporaryTable(10L, 12L);
        ctx.endStatementExecution();
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
    void followerQualifiesDropAndChecksMasterStatus(@Mocked Table table) throws Exception {
        mockLocalCatalog(false);
        ConnectContext ctx = new ConnectContext();
        ctx.setEnv(env);
        ctx.changeDefaultCatalog("external_catalog");
        ctx.addTemporaryTable(10L, 11L);
        String internalName = ctx.getSessionId() + FeNameFormat.TEMPORARY_TABLE_SIGN + "tmp";
        new Expectations() {
            {
                db.getFullName();
                result = "other_db";
                db.getTableNullable(11L);
                result = table;
                table.isTemporary();
                result = true;
                table.getName();
                result = internalName;
            }
        };
        List<String> sentSql = new ArrayList<>();
        new MockUp<MasterOpExecutor>() {
            @Mock
            public void execute(Invocation invocation) {
                MasterOpExecutor operation = invocation.getInvokedInstance();
                sentSql.add(operation.originStmt.originStmt);
            }
        };
        new MockUp<FEOpExecutor>() {
            @Mock
            public int getStatusCode() {
                return 1105;
            }

            @Mock
            public String getErrMsg() {
                return "injected forwarded error";
            }
        };

        DdlException failure = Assertions.assertThrows(DdlException.class, ctx::resetTemporaryTables);
        Assertions.assertTrue(failure.getCause().getMessage().contains("injected forwarded error"));
        Assertions.assertEquals(1, sentSql.size());
        Assertions.assertEquals("DROP TEMPORARY TABLE IF EXISTS `internal`.`other_db`.`" + internalName + "` FORCE",
                sentSql.get(0));
    }

    @Test
    void rejectsResetAfterUncertainCreate() {
        mockLocalCatalog(false);
        ConnectContext ctx = new ConnectContext();
        ctx.markTemporaryTableDdlUncertain();
        DdlException error = Assertions.assertThrows(DdlException.class, ctx::resetTemporaryTables);
        Assertions.assertTrue(error.getMessage().contains("unknown"));
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
