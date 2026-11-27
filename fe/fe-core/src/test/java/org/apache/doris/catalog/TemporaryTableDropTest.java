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

package org.apache.doris.catalog;

import org.apache.doris.analysis.Analyzer;
import org.apache.doris.analysis.CreateTableStmt;
import org.apache.doris.analysis.DropTableStmt;
import org.apache.doris.analysis.NativeInsertStmt;
import org.apache.doris.analysis.QueryStmt;
import org.apache.doris.analysis.TableName;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.common.util.Util;
import org.apache.doris.datasource.CatalogMgr;
import org.apache.doris.datasource.ExternalCatalog;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.analyzer.UnboundTableSink;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.commands.insert.InsertUtils;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

class TemporaryTableDropTest {
    @Test
    void rollbackJdbcCtasPreservesShadowingTemporaryTable() throws Exception {
        Env env = Mockito.mock(Env.class, Mockito.RETURNS_DEEP_STUBS);
        InternalCatalog catalog = Mockito.mock(InternalCatalog.class);
        Database db = new Database(7L, "db");
        Mockito.when(env.getInternalCatalog()).thenReturn(catalog);
        Mockito.when(catalog.getDbOrDdlException("db")).thenReturn(db);
        Mockito.when(catalog.getDbNullable(7L)).thenReturn(db);
        Mockito.when(env.getNextId()).thenReturn(9L);
        JdbcResource resource = Mockito.mock(JdbcResource.class);
        Mockito.when(resource.getType()).thenReturn(Resource.ResourceType.JDBC);
        Mockito.when(resource.getProperty(Mockito.anyString())).thenReturn("1");
        Mockito.when(resource.getProperty(JdbcResource.JDBC_URL)).thenReturn("jdbc:mysql://localhost/test");
        Mockito.when(env.getResourceMgr().getResource("test_resource")).thenReturn(resource);
        Mockito.doCallRealMethod().when(catalog).createTable(Mockito.any());
        Mockito.doCallRealMethod().when(catalog).rollbackCreatedTable(Mockito.anyLong(), Mockito.any());
        Mockito.doCallRealMethod().when(catalog).unprotectDropTable(Mockito.any(), Mockito.any(),
                Mockito.anyBoolean(), Mockito.anyBoolean(), Mockito.anyLong());
        Mockito.doCallRealMethod().when(env).rollbackCtas(Mockito.any(), Mockito.anyBoolean());
        CatalogRecycleBin recycleBin = Mockito.mock(CatalogRecycleBin.class);
        try (MockedStatic<Env> mocked = Mockito.mockStatic(Env.class)) {
            mocked.when(Env::getCurrentEnv).thenReturn(env);
            mocked.when(Env::getCurrentRecycleBin).thenReturn(recycleBin);
            mocked.when(Env::isTableNamesCaseInsensitive).thenReturn(true);
            new ConnectContext().setThreadLocalInfo();
            OlapTable temp = new OlapTable(8L, Util.generateTempTableInnerName("Mixed"), true,
                    ImmutableList.of(), KeysType.DUP_KEYS, new SinglePartitionInfo(), null, null);
            Assertions.assertTrue(db.registerTable(temp));
            CreateTableStmt stmt = new CreateTableStmt(false, true, new TableName("internal", "db", "mixed"),
                    null, "jdbc", null, null, null,
                    ImmutableMap.of("resource", "test_resource", "table", "remote_table", "table_type", "mysql"), null, "");
            Assertions.assertFalse(catalog.createTable(stmt));
            Assertions.assertSame(db.getTableNullable(9L), stmt.getCreatedTable());
            Assertions.assertSame(temp, db.getTableNullable("mixed"));

            // RENAME may update the name index while CTAS is rolling back. The database ID stays valid.
            db.setNameWithLock("renamed_db");
            env.rollbackCtas(stmt, false);
            Assertions.assertSame(temp, db.getTableNullable(8L));
            Assertions.assertNull(db.getTableNullable(9L));
            Mockito.verify(recycleBin).recycleTable(7L, stmt.getCreatedTable(), false, true, 0L);
        } finally {
            ConnectContext.remove();
        }
    }

    @Test
    void bindsCtasSinkByIdAfterCopyingThePlan() {
        Env env = Mockito.mock(Env.class);
        InternalCatalog catalog = Mockito.mock(InternalCatalog.class);
        Database db = new Database(10L, "db");
        OlapTable created = new OlapTable(12L, "target", ImmutableList.of(), KeysType.DUP_KEYS,
                new SinglePartitionInfo(), null);
        db.getIdToTableRef().put(created.getId(), created);
        Mockito.when(env.getInternalCatalog()).thenReturn(catalog);
        Mockito.when(catalog.getName()).thenReturn(InternalCatalog.INTERNAL_CATALOG_NAME);
        Mockito.when(catalog.getDbNullable("db")).thenReturn(db);
        ConnectContext ctx = new ConnectContext();
        ctx.setEnv(env);

        Plan child = Mockito.mock(Plan.class);
        Plan sink = new UnboundTableSink<>(ImmutableList.of("internal", "db", "target"),
                ImmutableList.of(), ImmutableList.of(), ImmutableList.of(), child)
                .withTargetTableId(created.getId()).withChildren(ImmutableList.of(child));

        Assertions.assertSame(created, InsertUtils.getTargetTable(sink, ctx));
        db.getIdToTableRef().remove(created.getId());
        Assertions.assertThrows(AnalysisException.class,
                () -> InsertUtils.getTargetTable(sink, ctx));
    }

    @Test
    void legacyCtasCollectsAndBindsTheCreatedTable() throws Exception {
        Env env = Mockito.mock(Env.class, Mockito.RETURNS_DEEP_STUBS);
        Analyzer analyzer = Mockito.mock(Analyzer.class);
        InternalCatalog catalog = Mockito.mock(InternalCatalog.class);
        Database db = new Database(10L, "db");
        OlapTable created = new OlapTable(12L, "ctas_target", ImmutableList.of(), KeysType.DUP_KEYS,
                new SinglePartitionInfo(), null);
        ConnectContext ctx = new ConnectContext();
        ctx.setThreadLocalInfo();
        try (MockedStatic<Env> mocked = Mockito.mockStatic(Env.class)) {
            mocked.when(Env::getCurrentEnv).thenReturn(env);
            OlapTable shadow = new OlapTable(13L, Util.generateTempTableInnerName("ctas_target"), true,
                    ImmutableList.of(), KeysType.DUP_KEYS, new SinglePartitionInfo(), null, null);
            Assertions.assertTrue(db.registerTable(created));
            Assertions.assertTrue(db.registerTable(shadow));
            Assertions.assertSame(shadow, db.getTableNullable("ctas_target"));

            Mockito.when(analyzer.getEnv()).thenReturn(env);
            CatalogMgr catalogs = env.getCatalogMgr();
            Mockito.when(catalogs.getCatalog("internal")).thenReturn(catalog);
            Mockito.when(catalog.getDbOrAnalysisException("db")).thenReturn(db);
            Mockito.when(env.getAccessManager().checkTblPriv(ctx, "internal", "db", "ctas_target",
                    PrivPredicate.LOAD)).thenReturn(true);
            NativeInsertStmt insert = new NativeInsertStmt(new TableName("internal", "db", "ctas_target"),
                    null, null, Mockito.mock(QueryStmt.class), null, null, true);
            insert.setCtasTargetTableId(created.getId());
            Map<Long, TableIf> locked = new HashMap<>();
            insert.getTables(analyzer, locked, Collections.emptySet());
            Assertions.assertEquals(Collections.singletonMap(created.getId(), created), locked);
            Deencapsulation.invoke(insert, "initTargetTable", analyzer);
            Assertions.assertSame(created, insert.getTargetTable());
        } finally {
            ConnectContext.remove();
        }
    }

    @Test
    void neverDelegatesTemporaryDropToExternalCatalog() throws Exception {
        Env env = Mockito.mock(Env.class, Mockito.CALLS_REAL_METHODS);
        CatalogMgr catalogMgr = Mockito.mock(CatalogMgr.class);
        ExternalCatalog external = Mockito.mock(ExternalCatalog.class);
        Deencapsulation.setField(env, "catalogMgr", catalogMgr);
        Mockito.when(catalogMgr.getCatalogOrException(Mockito.eq("ext"), Mockito.any())).thenReturn(external);

        TableName target = new TableName("ext", "db", "permanent");
        env.dropTable(new DropTableStmt(true, true, target, false));
        DdlException e = Assertions.assertThrows(DdlException.class,
                () -> env.dropTable(new DropTableStmt(false, true, target, false)));
        Assertions.assertTrue(e.getMessage().contains("Unknown table"));
        Mockito.verifyNoInteractions(external);
    }
}
