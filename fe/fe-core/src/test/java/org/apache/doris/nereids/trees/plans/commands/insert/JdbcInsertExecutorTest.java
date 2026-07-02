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

import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.EnvFactory;
import org.apache.doris.datasource.ExternalCatalog;
import org.apache.doris.datasource.jdbc.JdbcExternalTable;
import org.apache.doris.nereids.NereidsPlanner;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.Coordinator;
import org.apache.doris.qe.InsertResult;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.transaction.TransactionManager;
import org.apache.doris.transaction.TransactionStatus;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Optional;

/**
 * Tests for JDBC external table DML return status.
 */
public class JdbcInsertExecutorTest {

    @Test
    public void testVisibleReturnStatusByDefault() throws Exception {
        assertReturnStatus(null, TransactionStatus.VISIBLE);
    }

    @Test
    public void testCommittedReturnStatus() throws Exception {
        assertReturnStatus(SessionVariable.EXTERNAL_TABLE_DML_RETURN_STATUS_COMMITTED,
                TransactionStatus.COMMITTED);
    }

    private void assertReturnStatus(String returnStatus, TransactionStatus expectedStatus) throws Exception {
        ConnectContext ctx = new ConnectContext();
        ctx.setThreadLocalInfo();
        try {
            if (returnStatus != null) {
                ctx.getSessionVariable().setExternalTableDmlReturnStatus(returnStatus);
            }

            Coordinator coordinator = Mockito.mock(Coordinator.class);
            try (MockedStatic<EnvFactory> envFactoryMock = Mockito.mockStatic(EnvFactory.class)) {
                EnvFactory envFactory = Mockito.mock(EnvFactory.class);
                envFactoryMock.when(EnvFactory::getInstance).thenReturn(envFactory);
                Mockito.when(envFactory.createCoordinator(
                        Mockito.any(), Mockito.isNull(), Mockito.any(), Mockito.any()))
                        .thenReturn(coordinator);

                JdbcInsertExecutor executor = createExecutor(ctx);
                executor.onComplete();
                executor.afterExec(Mockito.mock(StmtExecutor.class));

                Assertions.assertEquals(expectedStatus, executor.txnStatus);
                Assertions.assertTrue(ctx.getState().getInfoMessage()
                        .contains("'status':'" + expectedStatus.name() + "'"));
                Assertions.assertEquals(-1L, executor.txnId);

                InsertResult insertResult = ctx.getInsertResult();
                Assertions.assertNotNull(insertResult);
                Assertions.assertEquals(expectedStatus, insertResult.txnStatus);
            }
        } finally {
            ConnectContext.remove();
        }
    }

    private JdbcInsertExecutor createExecutor(ConnectContext ctx) {
        DatabaseIf database = Mockito.mock(DatabaseIf.class);
        Mockito.when(database.getFullName()).thenReturn("test_jdbc_db");

        TransactionManager transactionManager = Mockito.mock(TransactionManager.class);
        ExternalCatalog catalog = Mockito.mock(ExternalCatalog.class);
        Mockito.when(catalog.getName()).thenReturn("test_jdbc_catalog");
        Mockito.when(catalog.getTransactionManager()).thenReturn(transactionManager);

        JdbcExternalTable table = Mockito.mock(JdbcExternalTable.class);
        Mockito.when(table.getDatabase()).thenReturn(database);
        Mockito.when(table.getCatalog()).thenReturn(catalog);
        Mockito.when(table.getName()).thenReturn("test_jdbc_table");

        return new JdbcInsertExecutor(ctx, table, "test_label", Mockito.mock(NereidsPlanner.class),
                Optional.empty(), false);
    }
}
