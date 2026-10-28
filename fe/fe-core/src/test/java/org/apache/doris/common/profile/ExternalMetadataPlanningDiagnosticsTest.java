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

package org.apache.doris.common.profile;

import org.apache.doris.analysis.TableSnapshot;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.PartitionItem;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.Config;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.common.profile.PlanningDiagnostics.Phase;
import org.apache.doris.datasource.ExternalMetaCacheMgr;
import org.apache.doris.datasource.ExternalSchemaCache;
import org.apache.doris.datasource.ExternalSchemaCache.SchemaCacheKey;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.datasource.SchemaCacheValue;
import org.apache.doris.datasource.TablePartitionValues;
import org.apache.doris.datasource.hive.HMSExternalCatalog;
import org.apache.doris.datasource.hive.HMSExternalDatabase;
import org.apache.doris.datasource.hive.HMSExternalTable;
import org.apache.doris.datasource.hive.HMSExternalTable.DLAType;
import org.apache.doris.datasource.hive.HudiDlaTable;
import org.apache.doris.datasource.hudi.HudiMvccSnapshot;
import org.apache.doris.datasource.hudi.HudiSchemaCacheKey;
import org.apache.doris.datasource.hudi.HudiSchemaCacheValue;
import org.apache.doris.datasource.hudi.HudiUtils;
import org.apache.doris.datasource.iceberg.IcebergExternalCatalog;
import org.apache.doris.datasource.iceberg.IcebergExternalDatabase;
import org.apache.doris.datasource.iceberg.IcebergExternalTable;
import org.apache.doris.datasource.iceberg.IcebergMvccSnapshot;
import org.apache.doris.datasource.iceberg.IcebergSchemaCacheValue;
import org.apache.doris.datasource.iceberg.IcebergSnapshotCacheValue;
import org.apache.doris.datasource.iceberg.IcebergUtils;
import org.apache.doris.datasource.mvcc.MvccTableInfo;
import org.apache.doris.mysql.authenticate.TestLogAppender;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.rules.analysis.PreloadExternalMetadata;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.trees.plans.logical.LogicalFileScan.SelectedPartitions;
import org.apache.doris.nereids.trees.plans.logical.LogicalHudiScan;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.OriginStatement;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.thrift.TUniqueId;

import com.google.common.collect.ImmutableList;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.ThreadContext;
import org.apache.logging.log4j.core.LogEvent;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

public class ExternalMetadataPlanningDiagnosticsTest {
    private final AtomicLong clock = new AtomicLong(TimeUnit.SECONDS.toNanos(1));
    private ConnectContext context;
    private ConnectContext previousContext;
    private StatementContext statement;
    private SummaryProfile summary;
    private ExternalSchemaCache schemaCache;
    private MockedStatic<Env> mockedEnv;
    private long savedThreshold;
    private long savedOperationThreshold;
    private long savedInterval;
    private boolean savedUnitTest;
    private String savedQueryId;

    @BeforeEach
    public void setUp() {
        previousContext = ConnectContext.get();
        savedQueryId = ThreadContext.get("query_id");
        savedThreshold = Config.nereids_planning_log_threshold_ms;
        savedOperationThreshold = Config.nereids_planning_operation_log_threshold_ms;
        savedInterval = Config.nereids_planning_log_interval_ms;
        savedUnitTest = FeConstants.runningUnitTest;
        Config.nereids_planning_log_threshold_ms = 5000;
        Config.nereids_planning_operation_log_threshold_ms = 1000;
        Config.nereids_planning_log_interval_ms = 30000;
        FeConstants.runningUnitTest = true;
        context = new ConnectContext();
        context.setQueryId(new TUniqueId(1, 2));
        context.setStmtId(7);
        context.setThreadLocalInfo();
        statement = new StatementContext(context, new OriginStatement("select 1", 0));
        context.setStatementContext(statement);
        summary = new SummaryProfile();
        StmtExecutor executor = Mockito.mock(StmtExecutor.class);
        Mockito.when(executor.getSummaryProfile()).thenReturn(summary);
        context.setExecutor(executor);
        schemaCache = Mockito.mock(ExternalSchemaCache.class);
        ExternalMetaCacheMgr cacheManager = Mockito.mock(ExternalMetaCacheMgr.class);
        Mockito.when(cacheManager.getSchemaCache(Mockito.any())).thenReturn(schemaCache);
        Env env = Mockito.mock(Env.class);
        Mockito.when(env.getExtMetaCacheMgr()).thenReturn(cacheManager);
        mockedEnv = Mockito.mockStatic(Env.class);
        mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
    }

    @AfterEach
    public void tearDown() {
        mockedEnv.close();
        ConnectContext.remove();
        if (previousContext != null) {
            previousContext.setThreadLocalInfo();
        }
        if (savedQueryId == null) {
            ThreadContext.remove("query_id");
        } else {
            ThreadContext.put("query_id", savedQueryId);
        }
        Config.nereids_planning_log_threshold_ms = savedThreshold;
        Config.nereids_planning_operation_log_threshold_ms = savedOperationThreshold;
        Config.nereids_planning_log_interval_ms = savedInterval;
        FeConstants.runningUnitTest = savedUnitTest;
    }

    @Test
    public void testHiveSchemaUsesRealOverrideAndDefersOneOperation() {
        HMSExternalTable table = hmsTable(DLAType.HIVE);
        List<Column> schema = ImmutableList.of(new Column("id", Type.INT));
        try (TestLogAppender appender = TestLogAppender.attach(PlanningDiagnostics.class, Level.WARN)) {
            Mockito.when(schemaCache.getSchemaValue(Mockito.any())).thenAnswer(invocation -> {
                advance(1000);
                Assertions.assertTrue(events(appender, "Planning operation").isEmpty());
                return Optional.of(new SchemaCacheValue(schema));
            });
            inPhase(Phase.PRELOAD_METADATA, () -> Assertions.assertSame(schema, table.getFullSchema()));
            assertOperation(appender, "preload_metadata", "load_schema", "hms.db.external", 1000);
        }
        Mockito.verify(schemaCache).getSchemaValue(Mockito.any());
    }

    @Test
    public void testHmsInitializationIsInsideSchemaOperation() {
        TestHmsTable table = hmsTable(DLAType.HIVE);
        Mockito.when(schemaCache.getSchemaValue(Mockito.any()))
                .thenReturn(Optional.of(new SchemaCacheValue(Collections.emptyList())));
        try (TestLogAppender appender = TestLogAppender.attach(PlanningDiagnostics.class, Level.WARN)) {
            table.initialize = () -> {
                advance(5000);
                context.checkTimeout(0);
                JsonObject running = events(appender, "Slow planning").get(0);
                Assertions.assertEquals("load_schema", running.get("operation").getAsString());
                Assertions.assertEquals("hms.db.external", running.get("target").getAsString());
                Assertions.assertEquals(5000, running.get("operation_elapsed_ms").getAsLong());
                Assertions.assertEquals(0, running.get("held_locks").getAsInt());
            };
            inPhase(Phase.PRELOAD_METADATA, table::getFullSchema);
            assertOperation(appender, "preload_metadata", "load_schema", "hms.db.external", 5000);
        }
    }

    @Test
    public void testHmsInitializationFailureIsAttributedToSchema() {
        TestHmsTable table = hmsTable(DLAType.HIVE);
        IllegalStateException failure = new IllegalStateException("HMS initialization failed");
        table.initialize = () -> {
            throw failure;
        };
        try (TestLogAppender appender = TestLogAppender.attach(PlanningDiagnostics.class, Level.WARN)) {
            Assertions.assertSame(failure, Assertions.assertThrows(IllegalStateException.class,
                    () -> inPhase(Phase.PRELOAD_METADATA, table::getFullSchema)));
            assertFailure(appender, "preload_metadata/load_schema");
            Mockito.verifyNoInteractions(schemaCache);
        }
    }

    @Test
    public void testHudiSchemaPreservesStatementSnapshotTimestamp() {
        HMSExternalTable table = hmsTable(DLAType.HUDI);
        statement.setSnapshot(new MvccTableInfo(table), new HudiMvccSnapshot(new TablePartitionValues(), 123L));
        List<Column> schema = ImmutableList.of(new Column("historical_id", Type.INT));
        Mockito.when(schemaCache.getSchemaValue(Mockito.any())).thenAnswer(invocation -> {
            advance(1000);
            return Optional.of(new HudiSchemaCacheValue(schema, Collections.emptyList(), true));
        });
        try (MockedStatic<HudiUtils> hudi = Mockito.mockStatic(HudiUtils.class);
                TestLogAppender appender = TestLogAppender.attach(PlanningDiagnostics.class, Level.WARN)) {
            inPhase(Phase.ANALYZE, () -> Assertions.assertSame(schema, table.getFullSchema()));
            assertOperation(appender, "analyze", "load_schema", "hms.db.external", 1000);
            ArgumentCaptor<SchemaCacheKey> key = ArgumentCaptor.forClass(SchemaCacheKey.class);
            Mockito.verify(schemaCache).getSchemaValue(key.capture());
            Assertions.assertEquals(123, ((HudiSchemaCacheKey) key.getValue()).getTimestamp());
            hudi.verifyNoInteractions();
        }
    }

    @Test
    public void testHmsIcebergSchemaPreservesSelectedSchema() {
        assertIcebergSchema(hmsTable(DLAType.ICEBERG), "hms.db.external");
    }

    @Test
    public void testNativeIcebergSchemaPreservesSelectedSchema() {
        assertIcebergSchema(icebergTable(), "iceberg.db.external");
    }

    @Test
    public void testSchemaFailuresKeepOriginalExceptionAndAttribution() {
        Config.nereids_planning_log_threshold_ms = 0;
        Config.nereids_planning_operation_log_threshold_ms = 0;
        IllegalStateException hiveFailure = new IllegalStateException("HMS schema failed");
        Mockito.when(schemaCache.getSchemaValue(Mockito.any())).thenThrow(hiveFailure);
        HMSExternalTable hive = hmsTable(DLAType.HIVE);
        try (TestLogAppender appender = TestLogAppender.attach(PlanningDiagnostics.class, Level.WARN)) {
            Assertions.assertSame(hiveFailure, Assertions.assertThrows(IllegalStateException.class,
                    () -> inPhase(Phase.ANALYZE, hive::getFullSchema)));
            assertFailure(appender, "analyze/load_schema");
        }
        IcebergExternalTable iceberg = icebergTable();
        IllegalArgumentException icebergFailure = new IllegalArgumentException("Iceberg schema failed");
        try (MockedStatic<IcebergUtils> utils = Mockito.mockStatic(IcebergUtils.class);
                TestLogAppender appender = TestLogAppender.attach(PlanningDiagnostics.class, Level.WARN)) {
            utils.when(() -> IcebergUtils.getIcebergSchema(iceberg)).thenThrow(icebergFailure);
            Assertions.assertSame(icebergFailure, Assertions.assertThrows(IllegalArgumentException.class,
                    () -> inPhase(Phase.PRELOAD_METADATA, iceberg::getFullSchema)));
            assertFailure(appender, "preload_metadata/load_schema");
            utils.verify(() -> IcebergUtils.getIcebergSchema(iceberg));
        }
    }

    @Test
    public void testHudiScanRecordsLatestPartitionLoad() {
        assertHudiScan(Optional.empty());
    }

    @Test
    public void testHudiScanPreservesExplicitSnapshotAndPartitions() {
        assertHudiScan(Optional.of(TableSnapshot.versionOf("20260102030405")));
    }

    @Test
    public void testHudiPartitionEarlyReturnsDoNotFetchPartitions() {
        TestHmsTable hive = hmsTable(DLAType.HIVE);
        TestHmsTable hudi = hmsTable(DLAType.HUDI);
        Mockito.when(schemaCache.getSchemaValue(Mockito.any())).thenReturn(Optional.of(
                new HudiSchemaCacheValue(Collections.emptyList(), Collections.emptyList(), false)));
        try (MockedStatic<HudiUtils> utils = Mockito.mockStatic(HudiUtils.class);
                TestLogAppender appender = TestLogAppender.attach(PlanningDiagnostics.class, Level.WARN)) {
            utils.when(() -> HudiUtils.getLastTimeStamp(hudi)).thenReturn(123L);
            inPhase(Phase.ANALYZE, () -> {
                Assertions.assertSame(SelectedPartitions.NOT_PRUNED, hive.initHudiSelectedPartitions(Optional.empty()));
                Assertions.assertSame(SelectedPartitions.NOT_PRUNED, hudi.initHudiSelectedPartitions(Optional.empty()));
            });
            utils.verify(() -> HudiUtils.getPartitionValues(Mockito.any(), Mockito.any()), Mockito.never());
            Assertions.assertTrue(events(appender, "Planning operation").isEmpty());
        }
    }

    @Test
    public void testHudiPartitionFailureKeepsSnapshotAndOriginalException() {
        TestHmsTable table = partitionedHudiTable();
        Optional<TableSnapshot> snapshot = Optional.of(TableSnapshot.timeOf("2026-01-02 03:04:05"));
        IllegalStateException failure = new IllegalStateException("Hudi partition load failed");
        try (MockedStatic<HudiUtils> utils = Mockito.mockStatic(HudiUtils.class);
                TestLogAppender appender = TestLogAppender.attach(PlanningDiagnostics.class, Level.WARN)) {
            utils.when(() -> HudiUtils.getPartitionValues(snapshot, table)).thenThrow(failure);
            Assertions.assertSame(failure, Assertions.assertThrows(IllegalStateException.class,
                    () -> inPhase(Phase.ANALYZE, () -> table.initHudiSelectedPartitions(snapshot))));
            assertFailure(appender, "analyze/load_partitions");
            utils.verify(() -> HudiUtils.getPartitionValues(snapshot, table));
        }
    }

    @Test
    public void testPreloadAndHudiScanRecordSeparateRealCallsAfterCleanup() {
        TestHmsTable table = partitionedHudiTable();
        context.getSessionVariable().setEnablePreloadExternalMetadata(true);
        statement.registerExternalTableForPreload(table, Optional.empty(), Optional.empty());
        TableIf internal = internalTable();
        statement.getTables().put(ImmutableList.of("internal"), internal);
        statement.getTables().put(ImmutableList.of("external"), table);
        Mockito.doAnswer(invocation -> {
            advance(1000);
            return Collections.emptyMap();
        }).when(table).getNameToPartitionItems(Mockito.any());
        TablePartitionValues values = partitions();
        Mockito.when(schemaCache.getSchemaValue(Mockito.any())).thenReturn(Optional.of(
                new HudiSchemaCacheValue(ImmutableList.of(new Column("dt", Type.STRING)),
                        ImmutableList.of(new Column("dt", Type.STRING)), false)));
        try (MockedStatic<HudiUtils> utils = Mockito.mockStatic(HudiUtils.class);
                TestLogAppender appender = TestLogAppender.attach(PlanningDiagnostics.class, Level.WARN)) {
            utils.when(() -> HudiUtils.getHudiMvccSnapshot(Optional.empty(), table))
                    .thenReturn(new HudiMvccSnapshot(values, 123L));
            utils.when(() -> HudiUtils.getPartitionValues(Optional.empty(), table)).thenAnswer(invocation -> {
                advance(1000);
                return values;
            });
            run(() -> {
                Assertions.assertTrue(new PreloadExternalMetadata().executePreload(statement).isExecuted());
                PlanningDiagnostics.runPhase(context, Phase.LOCK_TABLES, statement::lock);
                try {
                    PlanningDiagnostics.runPhase(context, Phase.ANALYZE,
                            () -> new LogicalHudiScan(new RelationId(1), table,
                                    ImmutableList.of("hms", "db"), Optional.empty(),
                                    Optional.empty(), Optional.empty()));
                    Assertions.assertTrue(events(appender, "Planning operation").isEmpty());
                } finally {
                    PlanningDiagnostics.runPhase(context, Phase.RELEASE_RESOURCES, statement::releasePlannerResources);
                }
            });
            List<JsonObject> partitions = events(appender, "Planning operation").stream()
                    .filter(event -> "load_partitions".equals(event.get("operation").getAsString()))
                    .collect(Collectors.toList());
            Assertions.assertEquals(2, partitions.size());
            Assertions.assertEquals("preload_metadata", partitions.get(0).get("phase").getAsString());
            Assertions.assertEquals("analyze", partitions.get(1).get("phase").getAsString());
            Mockito.verify(internal).readUnlock();
            // The snapshot resolves its own target; schema and partition calls share one cached name.
            Mockito.verify(table, Mockito.times(2)).getNameWithFullQualifiers();
            utils.verify(() -> HudiUtils.getPartitionValues(Optional.empty(), table));
        }
    }

    @Test
    public void testRunningSchemaReportShowsHeldInternalLockWithoutBlockingChecker() throws Exception {
        IcebergExternalTable table = icebergTable();
        TableIf internal = internalTable();
        statement.getTables().put(ImmutableList.of("internal"), internal);
        ExecutorService checker = Executors.newSingleThreadExecutor();
        try (MockedStatic<IcebergUtils> utils = Mockito.mockStatic(IcebergUtils.class);
                TestLogAppender appender = TestLogAppender.attach(PlanningDiagnostics.class, Level.WARN)) {
            utils.when(() -> IcebergUtils.getIcebergSchema(table)).thenAnswer(invocation -> {
                advance(5000);
                checker.submit(() -> context.checkTimeout(0)).get(10, TimeUnit.SECONDS);
                JsonObject report = events(appender, "Slow planning").get(0);
                Assertions.assertEquals("analyze", report.get("phase").getAsString());
                Assertions.assertEquals("load_schema", report.get("operation").getAsString());
                Assertions.assertEquals("iceberg.db.external", report.get("target").getAsString());
                Assertions.assertEquals(1, report.get("held_locks").getAsInt());
                Assertions.assertEquals("internal.db.dim", report.get("oldest_lock").getAsString());
                Assertions.assertEquals(5000, report.get("oldest_lock_hold_ms").getAsLong());
                Assertions.assertTrue(events(appender, "Planning operation").isEmpty());
                return Collections.emptyList();
            });
            run(() -> {
                PlanningDiagnostics.runPhase(context, Phase.LOCK_TABLES, statement::lock);
                try {
                    PlanningDiagnostics.runPhase(context, Phase.ANALYZE, table::getFullSchema);
                } finally {
                    PlanningDiagnostics.runPhase(context, Phase.RELEASE_RESOURCES, statement::releasePlannerResources);
                }
            });
            Assertions.assertEquals(2, events(appender, "Planning operation").size());
            Mockito.verify(internal).readUnlock();
        } finally {
            checker.shutdownNow();
            Assertions.assertTrue(checker.awaitTermination(10, TimeUnit.SECONDS));
        }
    }

    @Test
    public void testNoDiagnosticsDoesNotResolveTargetsOrChangeResults() {
        TestHmsTable hive = Mockito.spy(hmsTable(DLAType.HIVE));
        IcebergExternalTable iceberg = Mockito.spy(icebergTable());
        TestHmsTable hudi = partitionedHudiTable();
        List<Column> schema = ImmutableList.of(new Column("id", Type.INT));
        Mockito.when(schemaCache.getSchemaValue(Mockito.any())).thenReturn(Optional.of(new SchemaCacheValue(schema)));
        TablePartitionValues values = partitions();
        try (MockedStatic<IcebergUtils> icebergUtils = Mockito.mockStatic(IcebergUtils.class);
                MockedStatic<HudiUtils> hudiUtils = Mockito.mockStatic(HudiUtils.class)) {
            icebergUtils.when(() -> IcebergUtils.getIcebergSchema(iceberg)).thenReturn(schema);
            hudiUtils.when(() -> HudiUtils.getPartitionValues(Optional.empty(), hudi)).thenReturn(values);
            for (ConnectContext testContext : new ConnectContext[] {null, context}) {
                ConnectContext.remove();
                if (testContext != null) {
                    testContext.setThreadLocalInfo();
                }
                Assertions.assertSame(schema, hive.getFullSchema());
                Assertions.assertSame(schema, iceberg.getFullSchema());
                Assertions.assertEquals(2, hudi.initHudiSelectedPartitions(Optional.empty()).totalPartitionNum);
            }
            Mockito.verify(hive, Mockito.never()).getNameWithFullQualifiers();
            Mockito.verify(iceberg, Mockito.never()).getNameWithFullQualifiers();
            Mockito.verify(hudi, Mockito.never()).getNameWithFullQualifiers();
            Mockito.verify(schemaCache, Mockito.times(2)).getSchemaValue(Mockito.any());
            icebergUtils.verify(() -> IcebergUtils.getIcebergSchema(iceberg), Mockito.times(2));
            hudiUtils.verify(() -> HudiUtils.getPartitionValues(Optional.empty(), hudi), Mockito.times(2));
        }
    }

    @Test
    public void testRepeatedSubclassSchemaCallsReuseTargetAndKeepBoundedEvents() {
        HMSExternalTable table = Mockito.spy(hmsTable(DLAType.HIVE));
        Mockito.when(schemaCache.getSchemaValue(Mockito.any())).thenAnswer(invocation -> {
            advance(1000);
            return Optional.of(new SchemaCacheValue(Collections.emptyList()));
        });
        try (TestLogAppender appender = TestLogAppender.attach(PlanningDiagnostics.class, Level.WARN)) {
            inPhase(Phase.ANALYZE, () -> {
                for (int i = 0; i < 20; i++) {
                    table.getFullSchema();
                }
            });
            Assertions.assertEquals(16, events(appender, "Planning operation").size());
            Assertions.assertEquals(4, events(appender, "Planning finished").get(0)
                    .get("omitted_operations").getAsInt());
            Assertions.assertEquals(20000, summary.getPlanningDetails().getAsJsonObject("phase_time_ms")
                    .get("analyze").getAsLong());
            Mockito.verify(table).getNameWithFullQualifiers();
            Mockito.verify(schemaCache, Mockito.times(20)).getSchemaValue(Mockito.any());
        }
    }

    private void assertIcebergSchema(ExternalTable table, String target) {
        IcebergSnapshotCacheValue snapshot = Mockito.mock(IcebergSnapshotCacheValue.class, Mockito.RETURNS_DEEP_STUBS);
        Mockito.when(snapshot.getSnapshot().getSchemaId()).thenReturn(42L);
        IcebergMvccSnapshot selectedSnapshot = new IcebergMvccSnapshot(snapshot);
        statement.setSnapshot(new MvccTableInfo(table), selectedSnapshot);
        List<Column> schema = ImmutableList.of(new Column("historical_id", Type.INT));
        try (MockedStatic<IcebergUtils> utils = Mockito.mockStatic(IcebergUtils.class);
                TestLogAppender appender = TestLogAppender.attach(PlanningDiagnostics.class, Level.WARN)) {
            utils.when(() -> IcebergUtils.getIcebergSchema(table)).thenCallRealMethod();
            utils.when(() -> IcebergUtils.getOrFetchSnapshotCacheValue(Optional.of(selectedSnapshot), table))
                    .thenCallRealMethod();
            utils.when(() -> IcebergUtils.getSchemaCacheValue(table, 42L)).thenAnswer(invocation -> {
                advance(1000);
                return new IcebergSchemaCacheValue(schema, Collections.emptyList());
            });
            inPhase(Phase.ANALYZE, () -> Assertions.assertSame(schema, table.getFullSchema()));
            assertOperation(appender, "analyze", "load_schema", target, 1000);
            utils.verify(() -> IcebergUtils.getSchemaCacheValue(table, 42L));
            utils.verify(() -> IcebergUtils.getIcebergSnapshotCacheValue(Mockito.any(), Mockito.any(), Mockito.any()),
                    Mockito.never());
        }
    }

    private void assertHudiScan(Optional<TableSnapshot> snapshot) {
        TestHmsTable table = partitionedHudiTable();
        TablePartitionValues values = partitions();
        try (MockedStatic<HudiUtils> utils = Mockito.mockStatic(HudiUtils.class);
                TestLogAppender appender = TestLogAppender.attach(PlanningDiagnostics.class, Level.WARN)) {
            utils.when(() -> HudiUtils.getPartitionValues(snapshot, table)).thenAnswer(invocation -> {
                advance(1000);
                return values;
            });
            inPhase(Phase.ANALYZE, () -> {
                LogicalHudiScan scan = new LogicalHudiScan(new RelationId(1), table,
                        ImmutableList.of("hms", "db"), Optional.empty(), snapshot, Optional.empty());
                Assertions.assertSame(snapshot, scan.getTableSnapshot());
                Assertions.assertEquals(2, scan.getSelectedPartitions().totalPartitionNum);
                Assertions.assertSame(values.getIdToPartitionItem().get(1L),
                        scan.getSelectedPartitions().selectedPartitions.get("dt=2026-01-01"));
                Assertions.assertSame(values.getIdToPartitionItem().get(2L),
                        scan.getSelectedPartitions().selectedPartitions.get("dt=2026-01-02"));
            });
            assertOperation(appender, "analyze", "load_partitions", "hms.db.external", 1000);
            utils.verify(() -> HudiUtils.getPartitionValues(snapshot, table));
        }
    }

    private TestHmsTable hmsTable(DLAType type) {
        HMSExternalCatalog catalog = Mockito.mock(HMSExternalCatalog.class);
        Mockito.when(catalog.getName()).thenReturn("hms");
        HMSExternalDatabase db = Mockito.mock(HMSExternalDatabase.class);
        Mockito.when(db.getFullName()).thenReturn("db");
        Mockito.when(db.getRemoteName()).thenReturn("db");
        Mockito.when(db.getCatalog()).thenReturn(catalog);
        TestHmsTable table = new TestHmsTable(catalog, db);
        Deencapsulation.setField(table, "dlaType", type);
        if (type == DLAType.HUDI) {
            Deencapsulation.setField(table, "dlaTable", new HudiDlaTable(table));
        }
        return table;
    }

    private TestHmsTable partitionedHudiTable() {
        TestHmsTable table = Mockito.spy(hmsTable(DLAType.HUDI));
        Mockito.doReturn(ImmutableList.of(new Column("dt", Type.STRING))).when(table).getPartitionColumns();
        return table;
    }

    private IcebergExternalTable icebergTable() {
        IcebergExternalCatalog catalog = Mockito.mock(IcebergExternalCatalog.class);
        Mockito.when(catalog.getName()).thenReturn("iceberg");
        IcebergExternalDatabase db = Mockito.mock(IcebergExternalDatabase.class);
        Mockito.when(db.getFullName()).thenReturn("db");
        Mockito.when(db.getRemoteName()).thenReturn("db");
        Mockito.when(db.getCatalog()).thenReturn(catalog);
        return new IcebergExternalTable(2, "external", "external", catalog, db);
    }

    private TableIf internalTable() {
        TableIf table = Mockito.mock(TableIf.class);
        Mockito.when(table.getId()).thenReturn(10L);
        Mockito.when(table.getNameWithFullQualifiers()).thenReturn("internal.db.dim");
        Mockito.when(table.needReadLockWhenPlan()).thenReturn(true);
        Mockito.when(table.tryReadLock(1, TimeUnit.MINUTES)).thenReturn(true);
        return table;
    }

    private TablePartitionValues partitions() {
        TablePartitionValues values = new TablePartitionValues();
        values.getIdToPartitionItem().put(1L, Mockito.mock(PartitionItem.class));
        values.getIdToPartitionItem().put(2L, Mockito.mock(PartitionItem.class));
        values.getPartitionIdToNameMap().put(1L, "dt=2026-01-01");
        values.getPartitionIdToNameMap().put(2L, "dt=2026-01-02");
        return values;
    }

    private void run(Runnable action) {
        PlanningDiagnostics.execute(new PlanningDiagnostics(context, clock::get), () -> {
            action.run();
            return null;
        });
    }

    private void inPhase(Phase phase, Runnable action) {
        run(() -> PlanningDiagnostics.runPhase(context, phase, action));
    }

    private void advance(long millis) {
        clock.addAndGet(TimeUnit.MILLISECONDS.toNanos(millis));
    }

    private List<JsonObject> events(TestLogAppender appender, String prefix) {
        List<LogEvent> events = Deencapsulation.getField(appender, "events");
        return events.stream().filter(event -> event.getMessage().getFormattedMessage().startsWith(prefix + ": "))
                .map(event -> {
                    JsonObject json = JsonParser.parseString(event.getMessage().getFormattedMessage()
                            .split(": ", 2)[1]).getAsJsonObject();
                    Assertions.assertEquals("1-2", event.getContextData().getValue("query_id"));
                    Assertions.assertEquals("1-2", json.get("query_id").getAsString());
                    return json;
                }).collect(Collectors.toList());
    }

    private void assertOperation(TestLogAppender appender, String phase, String operation,
            String target, long elapsed) {
        List<JsonObject> events = events(appender, "Planning operation");
        Assertions.assertEquals(1, events.size());
        JsonObject event = events.get(0);
        Assertions.assertEquals(phase, event.get("phase").getAsString());
        Assertions.assertEquals(operation, event.get("operation").getAsString());
        Assertions.assertEquals(target, event.get("target").getAsString());
        Assertions.assertEquals(elapsed, event.get("elapsed_ms").getAsLong());
        Assertions.assertEquals(-1, event.get("operation_timeout_ms").getAsLong());
        Assertions.assertEquals("completed", event.get("status").getAsString());
        Assertions.assertEquals(elapsed, summary.getPlanningDetails().getAsJsonObject("phase_time_ms")
                .get(phase).getAsLong());
    }

    private void assertFailure(TestLogAppender appender, String failedStep) {
        Assertions.assertEquals("failed", events(appender, "Planning operation").get(0).get("status").getAsString());
        Assertions.assertEquals(failedStep, events(appender, "Planning finished").get(0)
                .get("failed_step").getAsString());
        Assertions.assertNull(context.getPlanningDiagnostics());
    }

    private static class TestHmsTable extends HMSExternalTable {
        private Runnable initialize = () -> {};
        private boolean initialized;

        TestHmsTable(HMSExternalCatalog catalog, HMSExternalDatabase db) {
            super(1, "external", "external", catalog, db);
        }

        @Override
        protected synchronized void makeSureInitialized() {
            if (!initialized) {
                initialize.run();
                initialized = true;
            }
        }
    }
}
