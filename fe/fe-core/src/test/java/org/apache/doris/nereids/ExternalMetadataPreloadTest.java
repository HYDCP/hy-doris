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

package org.apache.doris.nereids;

import org.apache.doris.analysis.TableSnapshot;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.datasource.ExternalCatalog;
import org.apache.doris.datasource.ExternalMetaCacheMgr;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.datasource.hive.HMSExternalTable;
import org.apache.doris.datasource.hive.HMSExternalTable.DLAType;
import org.apache.doris.datasource.mvcc.MvccSnapshot;
import org.apache.doris.datasource.mvcc.MvccTable;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.rules.analysis.PreloadExternalMetadata;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SessionVariable;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.Collections;
import java.util.Optional;

public class ExternalMetadataPreloadTest {

    @Test
    public void testDisabledSessionDoesNotRegisterCandidate() {
        StatementContext statementContext = createStatementContext(false);
        ExternalTable externalTable = Mockito.mock(ExternalTable.class);

        statementContext.registerExternalTableForPreload(externalTable, Optional.empty(), Optional.empty());
        ExternalMetadataPreloadResult result = new PreloadExternalMetadata().executePreload(statementContext);

        Assertions.assertFalse(result.isExecuted());
        Assertions.assertEquals(0, result.getCandidateTableCount());
        Assertions.assertEquals("session variable enable_preload_external_metadata is disabled",
                result.getSkipReason());
        Mockito.verifyNoInteractions(externalTable);
    }

    @Test
    public void testPreloadSchemaForNonSnapshotExternalTable() {
        StatementContext statementContext = createStatementContext(true);
        addInternalTable(statementContext, true);
        ExternalTable externalTable = mockExternalTable(1L, false);

        statementContext.registerExternalTableForPreload(externalTable, Optional.empty(), Optional.empty());
        ExternalMetadataPreloadResult result = new PreloadExternalMetadata().executePreload(statementContext);

        Assertions.assertTrue(result.isExecuted());
        Assertions.assertEquals(1, result.getCandidateTableCount());
        Assertions.assertEquals(1, result.getPreloadedTableCount());
        Mockito.verify(externalTable).getBaseSchema();
        Mockito.verify(externalTable, Mockito.never()).initSelectedPartitions(Mockito.any());
    }

    @Test
    public void testPreloadLatestSnapshotForLatestOnlyRelation() {
        StatementContext statementContext = createStatementContext(true);
        addInternalTable(statementContext, true);
        ExternalTable externalTable = mockMvccExternalTable(2L, true);
        MvccTable mvccTable = (MvccTable) externalTable;

        statementContext.registerExternalTableForPreload(externalTable, Optional.empty(), Optional.empty());
        ExternalMetadataPreloadResult result = new PreloadExternalMetadata().executePreload(statementContext);

        Assertions.assertTrue(result.isExecuted());
        Assertions.assertEquals(1, result.getPreloadedTableCount());
        Mockito.verify(mvccTable).loadSnapshot(Optional.empty(), Optional.empty());
        Mockito.verify(externalTable).getBaseSchema();
    }

    @Test
    public void testSkipLatestSnapshotPreloadForNonLatestRelation() {
        StatementContext statementContext = createStatementContext(true);
        addInternalTable(statementContext, true);
        ExternalTable externalTable = mockMvccExternalTable(3L, true);
        MvccTable mvccTable = (MvccTable) externalTable;

        statementContext.registerExternalTableForPreload(
                externalTable, Optional.of(Mockito.mock(TableSnapshot.class)), Optional.empty());
        ExternalMetadataPreloadResult result = new PreloadExternalMetadata().executePreload(statementContext);

        Assertions.assertTrue(result.isExecuted());
        Assertions.assertEquals(0, result.getPreloadedTableCount());
        Mockito.verify(mvccTable, Mockito.never()).loadSnapshot(Mockito.any(), Mockito.any());
        Mockito.verify(externalTable, Mockito.never()).getBaseSchema();
        Mockito.verify(externalTable, Mockito.never()).initSelectedPartitions(Mockito.any());
    }

    @Test
    public void testPreloadLatestSnapshotForHmsIcebergLatestRelation() {
        StatementContext statementContext = createStatementContext(true);
        addInternalTable(statementContext, true);
        HMSExternalTable externalTable = mockHmsIcebergExternalTable(4L);

        Assertions.assertTrue(externalTable.supportsExternalMetadataPreload());
        Assertions.assertTrue(externalTable.supportsLatestSnapshotPreload());

        statementContext.registerExternalTableForPreload(externalTable, Optional.empty(), Optional.empty());
        ExternalMetadataPreloadResult result = new PreloadExternalMetadata().executePreload(statementContext);

        Assertions.assertTrue(result.isExecuted());
        Assertions.assertEquals(1, result.getPreloadedTableCount());
        Mockito.verify(externalTable).loadSnapshot(Optional.empty(), Optional.empty());
        Mockito.verify(externalTable).getBaseSchema();
    }

    @Test
    public void testSkipPreloadWhenNoTableNeedsPlanReadLock() {
        StatementContext statementContext = createStatementContext(true);
        addInternalTable(statementContext, false);
        ExternalTable externalTable = mockExternalTable(5L, false);

        statementContext.registerExternalTableForPreload(externalTable, Optional.empty(), Optional.empty());
        ExternalMetadataPreloadResult result = new PreloadExternalMetadata().executePreload(statementContext);

        Assertions.assertFalse(result.isExecuted());
        Assertions.assertEquals("no internal tables require plan-time read lock", result.getSkipReason());
        Mockito.verify(externalTable, Mockito.never()).getBaseSchema();
    }

    @Test
    public void testEmptySchemaFailsPreloadAndInvalidatesCache() {
        StatementContext statementContext = createStatementContext(true);
        addInternalTable(statementContext, true);
        ExternalTable externalTable = mockExternalTable(6L, false);
        Mockito.when(externalTable.getBaseSchema()).thenReturn(null);
        mockDatabase(externalTable);

        Env env = Mockito.mock(Env.class);
        ExternalMetaCacheMgr externalMetaCacheMgr = Mockito.mock(ExternalMetaCacheMgr.class);
        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            Mockito.when(env.getExtMetaCacheMgr()).thenReturn(externalMetaCacheMgr);

            statementContext.registerExternalTableForPreload(externalTable, Optional.empty(), Optional.empty());
            AnalysisException exception = Assertions.assertThrows(AnalysisException.class,
                    () -> new PreloadExternalMetadata().executePreload(statementContext));

            Assertions.assertEquals("Failed to preload schema for external table ctl.db.tbl: no columns returned",
                    exception.getMessage());
            Mockito.verify(externalMetaCacheMgr).invalidateTableCache(externalTable);
            Mockito.verify(externalTable, Mockito.never()).initSelectedPartitions(Mockito.any());
        }
    }

    private StatementContext createStatementContext(boolean enablePreload) {
        ConnectContext connectContext = new ConnectContext();
        SessionVariable sessionVariable = new SessionVariable();
        sessionVariable.setEnablePreloadExternalMetadata(enablePreload);
        connectContext.setSessionVariable(sessionVariable);
        return new StatementContext(connectContext, null);
    }

    private void addInternalTable(StatementContext statementContext, boolean needReadLock) {
        TableIf internalTable = Mockito.mock(TableIf.class);
        Mockito.when(internalTable.needReadLockWhenPlan()).thenReturn(needReadLock);
        statementContext.getTables().put(Arrays.asList("internal", "db", "tbl"), internalTable);
    }

    private ExternalTable mockExternalTable(long id, boolean supportPartitionPreload) {
        ExternalTable externalTable = Mockito.mock(ExternalTable.class);
        mockExternalTableBase(externalTable, id, supportPartitionPreload);
        return externalTable;
    }

    private ExternalTable mockMvccExternalTable(long id, boolean supportLatestSnapshotPreload) {
        ExternalTable externalTable = Mockito.mock(ExternalTable.class,
                Mockito.withSettings().extraInterfaces(MvccTable.class));
        mockExternalTableBase(externalTable, id, false);
        Mockito.when(externalTable.supportsLatestSnapshotPreload()).thenReturn(supportLatestSnapshotPreload);
        mockDatabase(externalTable);
        Mockito.when(((MvccTable) externalTable).loadSnapshot(Optional.empty(), Optional.empty()))
                .thenReturn(Mockito.mock(MvccSnapshot.class));
        return externalTable;
    }

    private HMSExternalTable mockHmsIcebergExternalTable(long id) {
        HMSExternalTable externalTable = Mockito.mock(HMSExternalTable.class);
        Mockito.when(externalTable.getDlaType()).thenReturn(DLAType.ICEBERG);
        Mockito.when(externalTable.supportsExternalMetadataPreload()).thenCallRealMethod();
        Mockito.when(externalTable.supportsLatestSnapshotPreload()).thenCallRealMethod();
        Mockito.when(externalTable.getId()).thenReturn(id);
        Mockito.when(externalTable.supportInternalPartitionPruned()).thenReturn(false);
        Mockito.when(externalTable.getBaseSchema())
                .thenReturn(Collections.singletonList(Mockito.mock(Column.class)));
        mockDatabase(externalTable);
        Mockito.when(externalTable.loadSnapshot(Optional.empty(), Optional.empty()))
                .thenReturn(Mockito.mock(MvccSnapshot.class));
        return externalTable;
    }

    private void mockExternalTableBase(ExternalTable externalTable, long id, boolean supportPartitionPreload) {
        Mockito.when(externalTable.getId()).thenReturn(id);
        Mockito.when(externalTable.supportsExternalMetadataPreload()).thenReturn(true);
        Mockito.when(externalTable.supportsLatestSnapshotPreload()).thenReturn(false);
        Mockito.when(externalTable.supportInternalPartitionPruned()).thenReturn(supportPartitionPreload);
        Mockito.when(externalTable.getBaseSchema())
                .thenReturn(Collections.singletonList(Mockito.mock(Column.class)));
    }

    private void mockDatabase(ExternalTable externalTable) {
        DatabaseIf database = Mockito.mock(DatabaseIf.class);
        ExternalCatalog catalog = Mockito.mock(ExternalCatalog.class);
        Mockito.when(externalTable.getDatabase()).thenReturn(database);
        Mockito.when(externalTable.getCatalog()).thenReturn(catalog);
        Mockito.when(externalTable.getDbName()).thenReturn("db");
        Mockito.when(externalTable.getName()).thenReturn("tbl");
        Mockito.when(database.getFullName()).thenReturn("db");
        Mockito.when(database.getCatalog()).thenReturn(catalog);
        Mockito.when(catalog.getName()).thenReturn("ctl");
    }
}
