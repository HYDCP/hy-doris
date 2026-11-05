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

package org.apache.doris.planner;

import org.apache.doris.alter.SchemaChangeHandler;
import org.apache.doris.analysis.DescriptorTable;
import org.apache.doris.analysis.IndexDef.IndexType;
import org.apache.doris.analysis.SlotDescriptor;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.HashDistributionInfo;
import org.apache.doris.catalog.Index;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.Replica;
import org.apache.doris.catalog.ReplicaAllocation;
import org.apache.doris.catalog.SinglePartitionInfo;
import org.apache.doris.catalog.Tablet;
import org.apache.doris.catalog.TabletInvertedIndex;
import org.apache.doris.clone.TabletScheduler;
import org.apache.doris.clone.TabletSchedulerStat;
import org.apache.doris.cloud.qe.ComputeGroupException;
import org.apache.doris.cloud.qe.ComputeGroupException.FailedTypeEnum;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.Status;
import org.apache.doris.common.UserException;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.system.Backend;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.thrift.TColumn;
import org.apache.doris.thrift.TOlapTableIndexSchema;
import org.apache.doris.thrift.TOlapTableLocationParam;
import org.apache.doris.thrift.TOlapTableSchemaParam;
import org.apache.doris.thrift.TStatusCode;
import org.apache.doris.thrift.TStorageType;
import org.apache.doris.thrift.TTabletLocation;
import org.apache.doris.thrift.TUniqueKeyUpdateMode;

import com.google.common.collect.Multimap;
import mockit.Expectations;
import mockit.Mock;
import mockit.Mocked;
import mockit.MockUp;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class OlapTableSinkMetadataTest {
    @Mocked
    private Env env;
    private TabletScheduler scheduler;
    private int repairRequests;

    private SystemInfoService infoService;
    private OlapTable table;
    private Partition partition;
    private MaterializedIndex materializedIndex;
    private List<Column> columns;
    private String savedDeployMode;
    private String savedCloudUniqueId;
    private short savedMinLoadReplicaNum;
    private ConnectContext savedContext;

    @Before
    public void setUp() throws Exception {
        savedContext = ConnectContext.get();
        ConnectContext.remove();
        savedDeployMode = Config.deploy_mode;
        savedCloudUniqueId = Config.cloud_unique_id;
        savedMinLoadReplicaNum = Config.min_load_replica_num;
        Config.deploy_mode = "";
        Config.cloud_unique_id = "";
        Config.min_load_replica_num = -1;
        infoService = new SystemInfoService();
        for (long id = 1L; id <= 3L; id++) {
            Backend backend = new Backend(id, "127.0.0." + id, 9050);
            backend.setAlive(true);
            infoService.addBackend(backend);
        }
        scheduler = new TabletScheduler(env, infoService, new TabletInvertedIndex(),
                new TabletSchedulerStat(), "") {
            @Override
            public void tryAddRepairTablet(Tablet tablet, long dbId, OlapTable repairTable,
                    Partition repairPartition, MaterializedIndex index, int finishedCounter) {
                Assert.assertEquals(100L, tablet.getId());
                Assert.assertEquals(1L, dbId);
                Assert.assertSame(table, repairTable);
                Assert.assertSame(partition, repairPartition);
                Assert.assertSame(materializedIndex, index);
                Assert.assertEquals(0, finishedCounter);
                repairRequests++;
            }
        };
        new Expectations() {
            {
                Env.getCurrentSystemInfo();
                result = infoService;
                minTimes = 0;
                Env.getCurrentEnv();
                result = env;
                minTimes = 0;
            }
        };
        new MockUp<Env>() {
            @Mock
            public TabletScheduler getTabletScheduler() {
                return scheduler;
            }
        };
        Column key = new Column("k1", PrimitiveType.INT);
        key.setIsKey(true);
        key.setUniqueId(1);
        Column value = new Column("v1", PrimitiveType.BIGINT);
        value.setUniqueId(2);
        Column shadow = new Column(SchemaChangeHandler.SHADOW_NAME_PREFIX + "v2", PrimitiveType.INT);
        shadow.setUniqueId(3);
        Column hidden = new Column("hidden", PrimitiveType.INT);
        hidden.setUniqueId(4);
        hidden.setIsVisible(false);
        columns = Arrays.asList(key, value, shadow, hidden);
        HashDistributionInfo distribution = new HashDistributionInfo(1, Collections.singletonList(key));
        SinglePartitionInfo partitionInfo = new SinglePartitionInfo();
        partitionInfo.setReplicaAllocation(30L, new ReplicaAllocation((short) 3));
        table = new OlapTable(2L, "sink_metadata", columns, KeysType.DUP_KEYS, partitionInfo, distribution);
        table.setIndexMeta(10L, table.getName(), columns, 7, 123, (short) 1,
                TStorageType.COLUMN, KeysType.DUP_KEYS);
        table.setBaseIndexId(10L);
        materializedIndex = new MaterializedIndex(10L, MaterializedIndex.IndexState.NORMAL);
        partition = new Partition(30L, "p1", materializedIndex, distribution);
        partition.updateVisibleVersion(5L);
        table.addPartition(partition);
    }

    @After
    public void tearDown() {
        Config.deploy_mode = savedDeployMode;
        Config.cloud_unique_id = savedCloudUniqueId;
        Config.min_load_replica_num = savedMinLoadReplicaNum;
        ConnectContext.remove();
        if (savedContext != null) {
            savedContext.setThreadLocalInfo();
        }
    }

    @Test
    public void testSchemaFlagsShadowNamesAndIndexColumnIds() throws Exception {
        Index bitmap = new Index(100L, "bitmap_v2", Collections.singletonList("v2"),
                IndexType.BITMAP, Collections.emptyMap(), "");
        Index inverted = new Index(101L, "inverted_v1", Collections.singletonList("V1"),
                IndexType.INVERTED, Collections.emptyMap(), "");
        Index hidden = new Index(102L, "hidden_index", Collections.singletonList("hidden"),
                IndexType.INVERTED, Collections.emptyMap(), "");
        table.setIndexes(Arrays.asList(bitmap, inverted, hidden));
        table.setBloomFilterInfo(new HashSet<>(Arrays.asList("v1", "v2")), 0.01);
        // Base metadata has no indexes, so the legacy table-level index fallback applies.
        table.getIndexMetaByIndexId(10L).setIndexes(Collections.emptyList());
        TOlapTableSchemaParam schema = sink(false).createSchema(1L, table, null);
        TOlapTableIndexSchema index = schema.getIndexes().get(0);
        Assert.assertEquals(Arrays.asList("k1", "v1", "v2", "hidden"), index.getColumns());
        Assert.assertEquals("v2", index.getColumnsDesc().get(2).getColumnName());
        Assert.assertEquals(Boolean.TRUE,
                index.getColumnsDesc().get(2).getFieldValue(TColumn._Fields.HAS_BITMAP_INDEX));
        Assert.assertEquals(Boolean.TRUE,
                index.getColumnsDesc().get(1).getFieldValue(TColumn._Fields.IS_BLOOM_FILTER_COLUMN));
        Assert.assertEquals(Boolean.TRUE,
                index.getColumnsDesc().get(2).getFieldValue(TColumn._Fields.IS_BLOOM_FILTER_COLUMN));
        Assert.assertEquals(Collections.singletonList(2), index.getIndexesDesc().get(1).getColumnUniqueIds());
        Assert.assertTrue(index.getIndexesDesc().get(2).getColumnUniqueIds().isEmpty());
        Assert.assertEquals(7L, schema.getVersion());
    }

    @Test
    public void testIndexFlagsRemainCaseSensitive() throws Exception {
        Index upperCaseBitmap = new Index(100L, "bitmap_k1", Collections.singletonList("K1"),
                IndexType.BITMAP, Collections.emptyMap(), "");
        table.setIndexes(Collections.singletonList(upperCaseBitmap));
        table.setBloomFilterInfo(Collections.singleton("V1"), 0.01);
        TOlapTableIndexSchema schema = sink(false).createSchema(1L, table, null).getIndexes().get(0);
        Assert.assertEquals(Boolean.FALSE,
                schema.getColumnsDesc().get(0).getFieldValue(TColumn._Fields.HAS_BITMAP_INDEX));
        Assert.assertEquals(Boolean.FALSE,
                schema.getColumnsDesc().get(1).getFieldValue(TColumn._Fields.IS_BLOOM_FILTER_COLUMN));
        // Index-to-column-ID matching independently remains case insensitive.
        Assert.assertEquals(Collections.singletonList(1), schema.getIndexesDesc().get(0).getColumnUniqueIds());
        for (Column column : columns) {
            TColumn original = column.toThrift();
            TColumn snapshot = column.toThrift();
            column.setIndexFlag(original, table);
            column.setIndexFlag(snapshot, table.getIndexes(), table.getCopiedBfColumns());
            Assert.assertEquals(original, snapshot);
        }
    }

    @Test
    public void testPartialUpdateAndSchemaChangeChecksRemain() throws Exception {
        OlapTableSink sink = sink(false);
        sink.setPartialUpdateInputColumns(true, new HashSet<>(Arrays.asList("k1", "v1")));
        TOlapTableSchemaParam schema = sink.createSchema(1L, table, null);
        Assert.assertEquals(TUniqueKeyUpdateMode.UPDATE_FIXED_COLUMNS, schema.getUniqueKeyUpdateMode());
        Assert.assertEquals(new HashSet<>(Arrays.asList("k1", "v1")),
                new HashSet<>(schema.getPartialUpdateInputColumns()));
        table.setState(OlapTable.OlapTableState.SCHEMA_CHANGE);
        AnalysisException error = Assert.assertThrows(AnalysisException.class,
                () -> sink.createSchema(1L, table, null));
        Assert.assertEquals("Can't do partial update when table is doing schema change.", error.getDetailMessage());
    }

    @Test
    public void testReplicaLocationsAndSingleReplicaBackendLists() throws Exception {
        addTablet(new Tablet(100L));
        addTablet(new Tablet(101L));
        List<TOlapTableLocationParam> normal = sink(false).createLocation(1L, table);
        Assert.assertEquals(2, normal.get(0).getTabletsSize());
        for (TTabletLocation location : normal.get(0).getTablets()) {
            Assert.assertEquals(new HashSet<>(Arrays.asList(1L, 2L, 3L)), new HashSet<>(location.getNodeIds()));
        }
        List<TOlapTableLocationParam> single = sink(true).createLocation(1L, table);
        for (int i = 0; i < 2; i++) {
            List<Long> master = single.get(0).getTablets().get(i).getNodeIds();
            List<Long> slaves = single.get(1).getTablets().get(i).getNodeIds();
            Assert.assertEquals(1, master.size());
            Assert.assertEquals(2, slaves.size());
            Assert.assertFalse(slaves.contains(master.get(0)));
            Set<Long> all = new HashSet<>(slaves);
            all.addAll(master);
            Assert.assertEquals(new HashSet<>(Arrays.asList(1L, 2L, 3L)), all);
            master.add(4L);
            Assert.assertEquals(Long.valueOf(4L), master.remove(1));
        }
    }

    @Test
    public void testReplicaShortageStillTriggersThrottledRepair() throws Exception {
        Tablet tablet = new Tablet(100L);
        addTablet(tablet);
        infoService.getBackend(2L).setAlive(false);
        infoService.getBackend(3L).setAlive(false);
        OlapTableSink sink = sink(false);
        UserException first = Assert.assertThrows(UserException.class, () -> sink.createLocation(1L, table));
        Assert.assertTrue(first.getMessage().contains("tablet 100 alive replica num 1 < load required replica num 2"));
        Assert.assertTrue(first.getMessage().contains("detail:"));
        long firstFailure = tablet.getLastLoadFailedTime();
        Assert.assertTrue(firstFailure > 0);
        Assert.assertThrows(UserException.class, () -> sink.createLocation(1L, table));
        Assert.assertEquals(1, repairRequests);
    }

    @Test
    public void testComputeGroupErrorKeepsOriginalMessage() throws Exception {
        ComputeGroupException failure = new ComputeGroupException("compute group unavailable",
                FailedTypeEnum.CURRENT_COMPUTE_GROUP_NO_BE);
        Tablet tablet = new Tablet(100L) {
            @Override
            public Multimap<Long, Long> getNormalReplicaBackendPathMap() throws UserException {
                throw failure;
            }
        };
        materializedIndex.addTablet(tablet, null, true);
        UserException error = Assert.assertThrows(UserException.class, () -> sink(false).createLocation(1L, table));
        Assert.assertTrue(error.getMessage().endsWith(", " + failure.toString()));
    }

    @Test
    public void testDiskLimitStillChecked() throws Exception {
        addTablet(new Tablet(100L));
        new Expectations(infoService) {
            {
                infoService.checkExceedDiskCapacityLimit((Multimap<Long, Long>) any, true);
                result = new Status(TStatusCode.CANCELLED, "disk full");
            }
        };
        DdlException error = Assert.assertThrows(DdlException.class, () -> sink(false).createLocation(1L, table));
        Assert.assertEquals("disk full", error.getDetailMessage());
    }

    private OlapTableSink sink(boolean singleReplica) {
        DescriptorTable descriptors = new DescriptorTable();
        TupleDescriptor tuple = descriptors.createTupleDescriptor("sink_metadata");
        for (Column column : columns) {
            SlotDescriptor slot = descriptors.addSlotDescriptor(tuple);
            slot.setColumn(column);
            slot.setIsMaterialized(true);
        }
        return new OlapTableSink(table, tuple, Collections.singletonList(partition.getId()), singleReplica);
    }

    private void addTablet(Tablet tablet) {
        for (long backend = 1L; backend <= 3L; backend++) {
            Replica replica = new Replica(tablet.getId() * 10 + backend, backend, Replica.ReplicaState.NORMAL, 5L, 0);
            replica.setPathHash(1000L + backend);
            tablet.getReplicas().add(replica);
        }
        materializedIndex.addTablet(tablet, null, true);
    }
}
