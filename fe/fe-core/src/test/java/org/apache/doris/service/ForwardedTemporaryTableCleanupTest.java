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
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.MysqlConnectProcessor;
import org.apache.doris.system.Frontend;
import org.apache.doris.thrift.TDropSessionTemporaryTableRequest;
import org.apache.doris.thrift.TDropSessionTemporaryTableResult;
import org.apache.doris.thrift.TFrontendReportAliveSessionResult;
import org.apache.doris.thrift.TMasterOpRequest;
import org.apache.doris.thrift.TMasterOpResult;
import org.apache.doris.thrift.TStatusCode;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Collections;

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
    void reportsCreatedIdsToOriginWithoutChangingLegacyReplies() throws Exception {
        Env env = Mockito.mock(Env.class);
        Frontend origin = Mockito.mock(Frontend.class);
        Mockito.when(env.checkFeExist("origin", 9010)).thenReturn(origin);
        FrontendServiceImpl service = Mockito.mock(FrontendServiceImpl.class, Mockito.CALLS_REAL_METHODS);
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
            Assertions.assertEquals(Collections.singletonMap(11L, 10L),
                    service.forward(request).getCreatedTemporaryTableDbIds());
            request.unsetTempTableOriginConnectionId();
            Assertions.assertFalse(service.forward(request).isSetCreatedTemporaryTableDbIds());
            Assertions.assertEquals(2, processors.constructed().size());
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
            ConnectContext proxy = new ConnectContext(null, true, "original-session");
            Mockito.doReturn(true).when(service).isTemporaryTableOriginClosed(origin, 7, proxy.getSessionId());

            // The close path observes no table before the forwarded CREATE registers it.
            service.cleanupLateTemporaryTables(origin, 7, proxy);
            Mockito.verifyNoInteractions(catalog);
            proxy.addTemporaryTable(10L, 11L);
            service.cleanupLateTemporaryTables(origin, 7, proxy);
            Mockito.verify(catalog).dropTemporaryTable(database, 11L, proxy.getSessionId());
        }
    }

    @Test
    void leavesLateTableWhileOriginConnectionIsStillAlive() throws Exception {
        ConnectContext proxy;
        Env env = Mockito.mock(Env.class);
        try (MockedStatic<Env> mocked = Mockito.mockStatic(Env.class)) {
            mocked.when(Env::getCurrentEnv).thenReturn(env);
            proxy = new ConnectContext(null, true, "original-session");
        }
        proxy.addTemporaryTable(10L, 11L);
        Frontend origin = Mockito.mock(Frontend.class);
        FrontendServiceImpl service = Mockito.mock(FrontendServiceImpl.class, Mockito.CALLS_REAL_METHODS);
        Mockito.doReturn(false).when(service).isTemporaryTableOriginClosed(origin, 7, proxy.getSessionId());
        service.cleanupLateTemporaryTables(origin, 7, proxy);
        Mockito.verify(env, Mockito.never()).getInternalCatalog();
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
