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

import org.apache.doris.catalog.MaterializedIndex.IndexState;
import org.apache.doris.catalog.Replica.ReplicaState;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.metric.GaugeMetricImpl;
import org.apache.doris.metric.Metric.MetricUnit;
import org.apache.doris.metric.MetricRepo;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.thrift.TStorageType;

import com.google.common.collect.ImmutableMap;
import mockit.Expectations;
import mockit.Mocked;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.apache.logging.log4j.core.config.Property;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class TabletStatMgrExtremaTest {
    private static final long DB_ID = 1L;
    private static final String LOGGER_NAME = TabletStatMgr.class.getName();

    @Mocked
    private Env env;
    @Mocked
    private InternalCatalog catalog;
    @Mocked
    private SystemInfoService systemInfoService;

    private final List<Long> dbIds = new ArrayList<>();
    private final Database database = new Database(DB_ID, "tablet_stat_extrema_db");
    private final SummaryAppender appender = new SummaryAppender();
    private List<GaugeMetricImpl<Long>> savedGauges;
    private boolean savedMetricInit;
    private LoggerContext loggerContext;
    private Configuration logConfiguration;
    private LoggerConfig savedLoggerConfig;

    @Before
    public void setUp() throws Exception {
        savedMetricInit = MetricRepo.isInit;
        savedGauges = getGauges();
        List<GaugeMetricImpl<Long>> gauges = new ArrayList<>();
        for (int i = 0; i < savedGauges.size(); i++) {
            gauges.add(new GaugeMetricImpl<>("tablet_stat_extrema_" + i, MetricUnit.BYTES, "", -1L));
        }
        setGauges(gauges);
        MetricRepo.isInit = true;

        loggerContext = (LoggerContext) LogManager.getContext(false);
        logConfiguration = loggerContext.getConfiguration();
        savedLoggerConfig = logConfiguration.getLoggers().get(LOGGER_NAME);
        appender.start();
        LoggerConfig loggerConfig = new LoggerConfig(LOGGER_NAME, Level.INFO, false);
        loggerConfig.addAppender(appender, Level.INFO, null);
        logConfiguration.removeLogger(LOGGER_NAME);
        logConfiguration.addLogger(LOGGER_NAME, loggerConfig);
        loggerContext.updateLoggers();

        dbIds.add(DB_ID);
        new Expectations() {
            {
                Env.getCurrentSystemInfo();
                result = systemInfoService;
                systemInfoService.getAllBackendsByAllCluster();
                result = ImmutableMap.of();
                Env.getCurrentInternalCatalog();
                result = catalog;
                catalog.getDbIds();
                result = dbIds;
                catalog.getDbNullable(DB_ID);
                result = database;
                minTimes = 0;
            }
        };
    }

    @After
    public void tearDown() {
        try {
            logConfiguration.removeLogger(LOGGER_NAME);
            if (savedLoggerConfig != null) {
                logConfiguration.addLogger(LOGGER_NAME, savedLoggerConfig);
            }
            loggerContext.updateLoggers();
            appender.stop();
        } finally {
            setGauges(savedGauges);
            MetricRepo.isInit = savedMetricInit;
        }
    }

    @Test
    public void testDistinctSizes() {
        OlapTable first = addTable(100L);
        Partition firstPartition = addPartition(first, 1000L);
        addTablet(firstPartition, 10000L, 2L);
        addTablet(firstPartition, 10001L, 3L);
        addTablet(addPartition(first, 1001L), 10002L, 7L);
        Partition secondPartition = addPartition(addTable(200L), 2000L);
        addTablet(secondPartition, 20000L, 11L);
        addTablet(secondPartition, 20001L, 17L);

        new TabletStatMgr().runAfterCatalogReady();

        assertMetrics(17L, 2L, 8L, 28L, 5L, 13L, 28L, 12L, 20L);
        assertSummary(2L, 3L, 5L, 20001L, 10000L, 2000L, 1000L, 200L, 100L);
    }

    @Test
    public void testEqualNonzeroSizesSelectLastVisitedIds() {
        assertEqualSizesSelectLastVisitedIds(5L);
    }

    @Test
    public void testMultipleZeroSizesSelectLastVisitedIds() {
        assertEqualSizesSelectLastVisitedIds(0L);
    }

    @Test
    public void testZeroIdsAreSelectedForZeroSizes() {
        addTablet(addPartition(addTable(0L), 0L), 0L, 0L);

        new TabletStatMgr().runAfterCatalogReady();

        assertMetrics(0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L);
        assertSummary(1L, 1L, 1L, 0L, 0L, 0L, 0L, 0L, 0L);
    }

    @Test
    public void testDisabledMetricsDoNotUpdateGaugesOrLogSummary() {
        OlapTable table = addTable(100L);
        addTablet(addPartition(table, 1000L), 10000L, 7L);
        MetricRepo.isInit = false;

        new TabletStatMgr().runAfterCatalogReady();

        Assert.assertEquals(7L, table.getDataSize(true));
        assertMetrics(-1L, -1L, -1L, -1L, -1L, -1L, -1L, -1L, -1L);
        Assert.assertTrue(appender.summaries.isEmpty());
    }

    @Test
    public void testEmptyCatalog() {
        dbIds.clear();

        new TabletStatMgr().runAfterCatalogReady();

        assertMetrics(0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L);
        assertSummary(0L, 0L, 0L, null, null, null, null, null, null);
    }

    @Test
    public void testEmptyTableAndPartition() {
        addTable(100L);
        addPartition(addTable(200L), 2000L);
        List<Table> tables = database.getTables();
        long lastTableId = tables.get(tables.size() - 1).getId();

        new TabletStatMgr().runAfterCatalogReady();

        assertMetrics(0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L);
        assertSummary(2L, 1L, 0L, null, null, 2000L, 2000L, lastTableId, lastTableId);
    }

    @Test
    public void testLongMaxMinimaAreNormalizedButKeepSelectedIds() {
        addTablet(addPartition(addTable(100L), 1000L), 10000L, Long.MAX_VALUE);

        new TabletStatMgr().runAfterCatalogReady();

        assertMetrics(Long.MAX_VALUE, 0L, Long.MAX_VALUE,
                Long.MAX_VALUE, 0L, Long.MAX_VALUE, Long.MAX_VALUE, 0L, Long.MAX_VALUE);
        assertSummary(1L, 1L, 1L, 10000L, 10000L, 1000L, 1000L, 100L, 100L);
    }

    @Test
    public void testPartitionAndTableOverflowKeepUnselectedMaxima() {
        Partition partition = addPartition(addTable(100L), 1000L);
        addTablet(partition, 10000L, Long.MAX_VALUE);
        addTablet(partition, 10001L, 1L);

        new TabletStatMgr().runAfterCatalogReady();

        assertMetrics(Long.MAX_VALUE, 1L, Long.MIN_VALUE / 2,
                0L, Long.MIN_VALUE, Long.MIN_VALUE, 0L, Long.MIN_VALUE, Long.MIN_VALUE);
        assertSummary(1L, 1L, 2L, 10000L, 10001L, null, 1000L, null, 100L);
    }

    @Test
    public void testTableOverflowKeepsPartitionExtrema() {
        OlapTable table = addTable(100L);
        addTablet(addPartition(table, 1000L), 10000L, Long.MAX_VALUE);
        addTablet(addPartition(table, 1001L), 10001L, 1L);

        new TabletStatMgr().runAfterCatalogReady();

        assertMetrics(Long.MAX_VALUE, 1L, Long.MIN_VALUE / 2,
                Long.MAX_VALUE, 1L, Long.MIN_VALUE / 2, 0L, Long.MIN_VALUE, Long.MIN_VALUE);
        assertSummary(1L, 2L, 2L, 10000L, 10001L, 1000L, 1001L, null, 100L);
    }

    @Test
    public void testRepeatedCollectionResetsExtremaAndIds() {
        addTablet(addPartition(addTable(100L), 1000L), 10000L, 7L);
        TabletStatMgr manager = new TabletStatMgr();
        manager.runAfterCatalogReady();
        assertMetrics(7L, 7L, 7L, 7L, 7L, 7L, 7L, 7L, 7L);
        assertSummary(1L, 1L, 1L, 10000L, 10000L, 1000L, 1000L, 100L, 100L);

        dbIds.clear();
        appender.summaries.clear();
        manager.runAfterCatalogReady();

        assertMetrics(0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L);
        assertSummary(0L, 0L, 0L, null, null, null, null, null, null);
    }

    private void assertEqualSizesSelectLastVisitedIds(long size) {
        for (long tableId : new long[] {17L, 1L}) {
            OlapTable table = addTable(tableId);
            for (long partitionOffset : new long[] {37L, 2L}) {
                Partition partition = addPartition(table, tableId * 100 + partitionOffset);
                addTablet(partition, partition.getId() * 100 + 91L, size);
                addTablet(partition, partition.getId() * 100 + 5L, size);
            }
        }
        // Database and partition maps do not promise insertion order. Use the actual traversal order.
        List<Table> tables = database.getTables();
        OlapTable lastTable = (OlapTable) tables.get(tables.size() - 1);
        List<Partition> partitions = lastTable.getAllPartitions();
        Partition lastPartition = partitions.get(partitions.size() - 1);
        List<Tablet> tablets = lastPartition.getBaseIndex().getTablets();
        long lastTabletId = tablets.get(tablets.size() - 1).getId();

        new TabletStatMgr().runAfterCatalogReady();

        assertMetrics(size, size, size, size * 2, size * 2, size * 2, size * 4, size * 4, size * 4);
        assertSummary(2L, 4L, 8L, lastTabletId, lastTabletId, lastPartition.getId(), lastPartition.getId(),
                lastTable.getId(), lastTable.getId());
    }

    private OlapTable addTable(long id) {
        Column key = new Column("k1", PrimitiveType.INT);
        key.setIsKey(true);
        List<Column> columns = Collections.singletonList(key);
        HashDistributionInfo distribution = new HashDistributionInfo(1, columns);
        OlapTable table = new OlapTable(id, "tablet_stat_extrema_" + id, columns, KeysType.DUP_KEYS,
                new SinglePartitionInfo(), distribution);
        table.setIndexMeta(id, table.getName(), columns, 0, 0, (short) 1,
                TStorageType.COLUMN, KeysType.DUP_KEYS);
        table.setBaseIndexId(id);
        Assert.assertTrue(database.registerTable(table));
        return table;
    }

    private Partition addPartition(OlapTable table, long id) {
        MaterializedIndex index = new MaterializedIndex(table.getBaseIndexId(), IndexState.NORMAL);
        Partition partition = new Partition(id, "p" + id, index, table.getDefaultDistributionInfo());
        table.addPartition(partition);
        return partition;
    }

    private void addTablet(Partition partition, long id, long dataSize) {
        Tablet tablet = new Tablet(id);
        Replica replica = new Replica(id, id, ReplicaState.NORMAL, Partition.PARTITION_INIT_VERSION, 0);
        replica.setLastReportVersion(Partition.PARTITION_INIT_VERSION);
        replica.setRowCount(1L);
        replica.setDataSize(dataSize);
        tablet.getReplicas().add(replica);
        partition.getBaseIndex().addTablet(tablet, null, true);
    }

    private void assertMetrics(long... expected) {
        List<GaugeMetricImpl<Long>> gauges = getGauges();
        Assert.assertEquals(gauges.size(), expected.length);
        for (int i = 0; i < expected.length; i++) {
            Assert.assertEquals(gauges.get(i).getName(), expected[i], gauges.get(i).getValue().longValue());
        }
    }

    private void assertSummary(long tables, long partitions, long tablets, Long maxTabletId, Long minTabletId,
            Long maxPartitionId, Long minPartitionId, Long maxTableId, Long minTableId) {
        Assert.assertEquals(1, appender.summaries.size());
        Assert.assertEquals("OlapTable num=" + tables + ", partition num=" + partitions + ", tablet num=" + tablets
                + ", max tablet byte size=" + MetricRepo.GAUGE_MAX_TABLET_SIZE_BYTES.getValue()
                + "(tablet_id=" + maxTabletId + ")"
                + ", min tablet byte size=" + MetricRepo.GAUGE_MIN_TABLET_SIZE_BYTES.getValue()
                + "(tablet_id=" + minTabletId + ")"
                + ", avg tablet byte size=" + MetricRepo.GAUGE_AVG_TABLET_SIZE_BYTES.getValue()
                + ", max partition byte size=" + MetricRepo.GAUGE_MAX_PARTITION_SIZE_BYTES.getValue()
                + "(partition_id=" + maxPartitionId + ")"
                + ", min partition byte size=" + MetricRepo.GAUGE_MIN_PARTITION_SIZE_BYTES.getValue()
                + "(partition_id=" + minPartitionId + ")"
                + ", avg partition byte size=" + MetricRepo.GAUGE_AVG_PARTITION_SIZE_BYTES.getValue()
                + ", max table byte size=" + MetricRepo.GAUGE_MAX_TABLE_SIZE_BYTES.getValue()
                + "(table_id=" + maxTableId + ")"
                + ", min table byte size=" + MetricRepo.GAUGE_MIN_TABLE_SIZE_BYTES.getValue()
                + "(table_id=" + minTableId + ")"
                + ", avg table byte size=" + MetricRepo.GAUGE_AVG_TABLE_SIZE_BYTES.getValue(),
                appender.summaries.get(0));
    }

    private List<GaugeMetricImpl<Long>> getGauges() {
        return Arrays.asList(MetricRepo.GAUGE_MAX_TABLET_SIZE_BYTES, MetricRepo.GAUGE_MIN_TABLET_SIZE_BYTES,
                MetricRepo.GAUGE_AVG_TABLET_SIZE_BYTES, MetricRepo.GAUGE_MAX_PARTITION_SIZE_BYTES,
                MetricRepo.GAUGE_MIN_PARTITION_SIZE_BYTES, MetricRepo.GAUGE_AVG_PARTITION_SIZE_BYTES,
                MetricRepo.GAUGE_MAX_TABLE_SIZE_BYTES, MetricRepo.GAUGE_MIN_TABLE_SIZE_BYTES,
                MetricRepo.GAUGE_AVG_TABLE_SIZE_BYTES);
    }

    private void setGauges(List<GaugeMetricImpl<Long>> gauges) {
        MetricRepo.GAUGE_MAX_TABLET_SIZE_BYTES = gauges.get(0);
        MetricRepo.GAUGE_MIN_TABLET_SIZE_BYTES = gauges.get(1);
        MetricRepo.GAUGE_AVG_TABLET_SIZE_BYTES = gauges.get(2);
        MetricRepo.GAUGE_MAX_PARTITION_SIZE_BYTES = gauges.get(3);
        MetricRepo.GAUGE_MIN_PARTITION_SIZE_BYTES = gauges.get(4);
        MetricRepo.GAUGE_AVG_PARTITION_SIZE_BYTES = gauges.get(5);
        MetricRepo.GAUGE_MAX_TABLE_SIZE_BYTES = gauges.get(6);
        MetricRepo.GAUGE_MIN_TABLE_SIZE_BYTES = gauges.get(7);
        MetricRepo.GAUGE_AVG_TABLE_SIZE_BYTES = gauges.get(8);
    }

    private static class SummaryAppender extends AbstractAppender {
        private final List<String> summaries = new ArrayList<>();

        private SummaryAppender() {
            super("tablet-stat-extrema-test", null, null, false, Property.EMPTY_ARRAY);
        }

        @Override
        public void append(LogEvent event) {
            String message = event.getMessage().getFormattedMessage();
            if (message.startsWith("OlapTable num=")) {
                summaries.add(message);
            }
        }
    }
}
