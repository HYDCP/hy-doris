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

import org.apache.doris.analysis.ShowTableStmt;
import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.TableIf.TableType;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.FeConstants;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.datasource.CatalogMgr;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.mysql.privilege.AccessControllerManager;
import org.apache.doris.mysql.privilege.PrivPredicate;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

public class ShowExecutorExternalTableTest {
    private static final String CATALOG_NAME = "hive_catalog";
    private static final String DB_NAME = "hive_db";

    private final ConnectContext ctx = Mockito.mock(ConnectContext.class);
    private final Env env = Mockito.mock(Env.class);
    private final AccessControllerManager accessManager = Mockito.mock(AccessControllerManager.class);
    @SuppressWarnings("unchecked")
    private final DatabaseIf<TableIf> db = Mockito.mock(DatabaseIf.class);

    @SuppressWarnings("unchecked")
    private void prepareCatalog(CatalogIf<?> catalog) throws AnalysisException {
        CatalogMgr catalogMgr = Mockito.mock(CatalogMgr.class);
        Mockito.when(ctx.getEnv()).thenReturn(env);
        Mockito.when(env.getCatalogMgr()).thenReturn(catalogMgr);
        Mockito.when(env.getAccessManager()).thenReturn(accessManager);
        Mockito.when(catalogMgr.getCatalogOrAnalysisException(Mockito.anyString())).thenReturn(catalog);
        Mockito.when(catalog.getDbOrAnalysisException(DB_NAME)).thenReturn(db);
        Mockito.when(db.getCatalog()).thenReturn(catalog);
        Mockito.when(db.getFullName()).thenReturn(DB_NAME);
        Mockito.when(accessManager.checkTblPriv(Mockito.eq(ctx), Mockito.anyString(), Mockito.eq(DB_NAME),
                Mockito.anyString(), Mockito.eq(PrivPredicate.SHOW))).thenReturn(true);
    }

    private ShowResultSet execute(ShowTableStmt stmt) throws AnalysisException {
        return execute(stmt, true);
    }

    private ShowResultSet execute(ShowTableStmt stmt, boolean caseSensitive) throws AnalysisException {
        try (MockedStatic<Env> envMock = Mockito.mockStatic(Env.class);
                MockedStatic<ConnectContext> contextMock = Mockito.mockStatic(ConnectContext.class)) {
            envMock.when(Env::getCurrentEnv).thenReturn(env);
            contextMock.when(ConnectContext::get).thenReturn(ctx);
            ShowExecutor executor = Mockito.spy(new ShowExecutor(ctx, stmt));
            Mockito.doReturn(caseSensitive).when(executor).isShowTablesCaseSensitive();
            return executor.execute();
        }
    }

    private TableIf table(String name, TableType type) {
        TableIf table = Mockito.mock(TableIf.class);
        Mockito.when(table.getName()).thenReturn(name);
        Mockito.when(table.getType()).thenReturn(type);
        return table;
    }

    @Test
    public void testExternalShowTablesListsNamesWithoutLoadingTables() throws Exception {
        prepareCatalog(Mockito.mock(CatalogIf.class));
        Mockito.when(db.getTableNamesWithLock()).thenReturn(ImmutableSet.of("t2", "t1", "denied",
                FeConstants.TEMP_MATERIZLIZE_DVIEW_PREFIX + "hidden"));
        Mockito.when(accessManager.checkTblPriv(ctx, CATALOG_NAME, DB_NAME, "denied", PrivPredicate.SHOW))
                .thenReturn(false);

        ShowResultSet result = execute(new ShowTableStmt(DB_NAME, CATALOG_NAME, false, null));

        Assertions.assertEquals(ImmutableList.of(ImmutableList.of("t1"), ImmutableList.of("t2")),
                result.getResultRows());
        Assertions.assertEquals(1, result.getMetaData().getColumnCount());
        Mockito.verify(db).getTableNamesWithLock();
        Mockito.verify(db, Mockito.never()).getTables();
        Mockito.verify(db, Mockito.never()).getTableNullable(Mockito.anyString());
    }

    @Test
    public void testExternalShowTablesLikeFiltersNames() throws Exception {
        prepareCatalog(Mockito.mock(CatalogIf.class));
        Mockito.when(db.getTableNamesWithLock()).thenReturn(ImmutableSet.of("tbl2", "other_tbl", "tbl1"));

        ShowResultSet result = execute(new ShowTableStmt(DB_NAME, CATALOG_NAME, false, "tbl_"));

        Assertions.assertEquals(ImmutableList.of(ImmutableList.of("tbl1"), ImmutableList.of("tbl2")),
                result.getResultRows());
        Mockito.verify(accessManager, Mockito.never())
                .checkTblPriv(ctx, CATALOG_NAME, DB_NAME, "other_tbl", PrivPredicate.SHOW);
        Mockito.verify(db, Mockito.never()).getTables();
    }

    @Test
    public void testExternalShowTablesLikeRespectsCaseSensitivity() throws Exception {
        prepareCatalog(Mockito.mock(CatalogIf.class));
        Mockito.when(db.getTableNamesWithLock()).thenReturn(ImmutableSet.of("Table1", "table2"));
        ShowTableStmt stmt = new ShowTableStmt(DB_NAME, CATALOG_NAME, false, "table_");

        Assertions.assertEquals(ImmutableList.of(ImmutableList.of("table2")),
                execute(stmt, true).getResultRows());
        Assertions.assertEquals(ImmutableList.of(ImmutableList.of("Table1"), ImmutableList.of("table2")),
                execute(stmt, false).getResultRows());
        Mockito.verify(db, Mockito.never()).getTables();
    }

    @Test
    public void testExternalShowTablesPropagatesNameListingFailure() throws Exception {
        prepareCatalog(Mockito.mock(CatalogIf.class));
        RuntimeException failure = new RuntimeException(
                "Found conflicting table names under case-insensitive conditions");
        Mockito.when(db.getTableNamesWithLock()).thenThrow(failure);

        Assertions.assertSame(failure, Assertions.assertThrows(RuntimeException.class,
                () -> execute(new ShowTableStmt(DB_NAME, CATALOG_NAME, false, null))));
        Mockito.verify(db, Mockito.never()).getTables();
    }

    @Test
    public void testExternalShowTablesEmptyDatabase() throws Exception {
        prepareCatalog(Mockito.mock(CatalogIf.class));
        Mockito.when(db.getTableNamesWithLock()).thenReturn(ImmutableSet.of());

        Assertions.assertTrue(execute(new ShowTableStmt(DB_NAME, CATALOG_NAME, false, null))
                .getResultRows().isEmpty());
        Mockito.verify(db, Mockito.never()).getTables();
    }

    @Test
    public void testExternalShowFullTablesPreservesFourColumns() throws Exception {
        prepareCatalog(Mockito.mock(CatalogIf.class));
        TableIf table = table("t1", TableType.HMS_EXTERNAL_TABLE);
        Mockito.when(table.getMysqlType()).thenReturn("BASE TABLE");
        Mockito.when(db.getTables()).thenReturn(Lists.newArrayList(table));

        ShowResultSet result = execute(new ShowTableStmt(DB_NAME, CATALOG_NAME, true, null));

        Assertions.assertEquals(ImmutableList.of(ImmutableList.of("t1", "BASE TABLE", "NONE", "NONE")),
                result.getResultRows());
        Assertions.assertEquals(4, result.getMetaData().getColumnCount());
        Mockito.verify(db).getTables();
        Mockito.verify(db, Mockito.never()).getTableNamesWithLock();
    }

    @Test
    public void testExternalShowViewsPreservesTypeFiltering() throws Exception {
        prepareCatalog(Mockito.mock(CatalogIf.class));
        TableIf externalTable = table("t1", TableType.HMS_EXTERNAL_TABLE);
        TableIf view = table("v1", TableType.VIEW);
        Mockito.when(db.getTables()).thenReturn(Lists.newArrayList(externalTable, view));

        ShowResultSet result = execute(new ShowTableStmt(DB_NAME, CATALOG_NAME, false, TableType.VIEW, null, null));

        Assertions.assertEquals(ImmutableList.of(ImmutableList.of("v1")), result.getResultRows());
        Mockito.verify(db).getTables();
        Mockito.verify(db, Mockito.never()).getTableNamesWithLock();
    }

    @Test
    public void testInternalShowTablesPreservesTemporaryTableFiltering() throws Exception {
        prepareCatalog(Mockito.mock(InternalCatalog.class));
        TableIf temporaryTable = table("temporary", TableType.OLAP);
        TableIf regularTable = table("t1", TableType.OLAP);
        Mockito.when(temporaryTable.isTemporary()).thenReturn(true);
        Mockito.when(db.getTables()).thenReturn(Lists.newArrayList(temporaryTable, regularTable));

        ShowResultSet result = execute(new ShowTableStmt(DB_NAME, InternalCatalog.INTERNAL_CATALOG_NAME, false, null));

        Assertions.assertEquals(ImmutableList.of(ImmutableList.of("t1")), result.getResultRows());
        Mockito.verify(db).getTables();
        Mockito.verify(db, Mockito.never()).getTableNamesWithLock();
    }
}
