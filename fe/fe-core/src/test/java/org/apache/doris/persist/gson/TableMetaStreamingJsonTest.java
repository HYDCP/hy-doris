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

package org.apache.doris.persist.gson;

import org.apache.doris.catalog.AggregateType;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.DataProperty;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.MaterializedIndex.IndexState;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.PartitionInfo;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.RandomDistributionInfo;
import org.apache.doris.catalog.Replica;
import org.apache.doris.catalog.Replica.ReplicaState;
import org.apache.doris.catalog.ReplicaAllocation;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.SinglePartitionInfo;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.Tablet;
import org.apache.doris.catalog.TabletMeta;
import org.apache.doris.common.Config;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.datasource.hive.HMSExternalCatalog;
import org.apache.doris.datasource.hive.HMSExternalDatabase;
import org.apache.doris.datasource.hive.HMSExternalTable;
import org.apache.doris.thrift.TStorageMedium;
import org.apache.doris.thrift.TStorageType;
import org.apache.doris.thrift.TTabletType;

import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;

/*
 * Verifies that the streaming json serialization of table meta
 * (table/partition/tablet/replica) produces byte-identical output with the legacy
 * tree mode, that both modes can read each other's output, that legacy payloads
 * without the type field are still readable in streaming mode (default subtype
 * replay), and that the streaming Text io helpers of GsonUtils are byte-compatible
 * with Text.writeString/readString.
 */
public class TableMetaStreamingJsonTest {

    @Test
    public void testSafeRolloutDefaults() {
        Assert.assertFalse(Config.enable_table_meta_streaming_json);
        Assert.assertTrue(Config.backup_meta_reserve_replica_info);
    }

    private static OlapTable createTable() {
        long dbId = 1L;
        long tableId = 2L;
        long partitionId = 3L;
        long indexId = 4L;
        long tabletId = 5L;
        long backendId = 10000L;
        long version = 6L;

        Tablet tablet = new Tablet(tabletId);
        MaterializedIndex index = new MaterializedIndex(indexId, IndexState.NORMAL);
        TabletMeta tabletMeta = new TabletMeta(dbId, tableId, partitionId, indexId, 0, TStorageMedium.HDD);
        // use restore mode to skip the inverted index which requires a running Env
        index.addTablet(tablet, tabletMeta, true);
        tablet.addReplica(new Replica(7L, backendId, ReplicaState.NORMAL, version, 0), true);
        tablet.addReplica(new Replica(8L, backendId + 1, ReplicaState.NORMAL, version, 0), true);
        tablet.addReplica(new Replica(9L, backendId + 2, ReplicaState.NORMAL, version, 0), true);

        RandomDistributionInfo distributionInfo = new RandomDistributionInfo(10);
        Partition partition = new Partition(partitionId, "p1", index, distributionInfo);

        List<Column> columns = Lists.newArrayList();
        Column k1 = new Column("k1", PrimitiveType.INT);
        k1.setIsKey(true);
        columns.add(k1);
        columns.add(new Column("v", ScalarType.createType(PrimitiveType.DOUBLE), false, AggregateType.SUM,
                "0", ""));

        PartitionInfo partitionInfo = new SinglePartitionInfo();
        partitionInfo.setDataProperty(partitionId, new DataProperty(DataProperty.DEFAULT_STORAGE_MEDIUM));
        partitionInfo.setReplicaAllocation(partitionId, new ReplicaAllocation((short) 3));
        partitionInfo.setIsInMemory(partitionId, false);
        partitionInfo.setIsMutable(partitionId, true);
        partitionInfo.setTabletType(partitionId, TTabletType.TABLET_TYPE_DISK);

        OlapTable table = new OlapTable(tableId, "tbl1", columns, KeysType.AGG_KEYS, partitionInfo,
                distributionInfo);
        Deencapsulation.setField(table, "baseIndexId", indexId);
        table.addPartition(partition);
        table.setIndexMeta(indexId, "tbl1", columns, 0, 0, (short) 1, TStorageType.COLUMN, KeysType.AGG_KEYS);
        return table;
    }

    private static HMSExternalTable createHmsExternalTable() {
        HMSExternalCatalog catalog = Mockito.mock(HMSExternalCatalog.class);
        HMSExternalDatabase database = Mockito.mock(HMSExternalDatabase.class);
        Mockito.when(catalog.getId()).thenReturn(10L);
        Mockito.when(database.getFullName()).thenReturn("hms_db");
        Mockito.when(database.getRemoteName()).thenReturn("remote_hms_db");
        return new HMSExternalTable(11L, "hms_table", "remote_hms_table", catalog, database);
    }

    private static String toJsonWithStreaming(Object src, Class<?> declaredType, boolean streaming) {
        boolean oldValue = Config.enable_table_meta_streaming_json;
        try {
            Config.enable_table_meta_streaming_json = streaming;
            return GsonUtils.GSON.toJson(src, declaredType);
        } finally {
            Config.enable_table_meta_streaming_json = oldValue;
        }
    }

    private static <T> T fromJsonWithStreaming(String json, Class<T> clazz, boolean streaming) {
        boolean oldValue = Config.enable_table_meta_streaming_json;
        try {
            Config.enable_table_meta_streaming_json = streaming;
            return GsonUtils.GSON.fromJson(json, clazz);
        } finally {
            Config.enable_table_meta_streaming_json = oldValue;
        }
    }

    @Test
    public void testOlapTableStreamingWriteByteIdenticalWithTree() {
        OlapTable table = createTable();
        String treeJson = toJsonWithStreaming(table, Table.class, false);
        String streamingJson = toJsonWithStreaming(table, Table.class, true);
        Assert.assertEquals(treeJson, streamingJson);
        Assert.assertTrue(streamingJson, streamingJson.startsWith("{\"clazz\":\"OlapTable\","));
        // nested polymorphic types are dispatched in streaming mode too
        Assert.assertTrue(streamingJson, streamingJson.contains("\"clazz\":\"Partition\""));
        Assert.assertTrue(streamingJson, streamingJson.contains("\"clazz\":\"Tablet\""));
        Assert.assertTrue(streamingJson, streamingJson.contains("\"clazz\":\"Replica\""));
    }

    @Test
    public void testOlapTableCrossModeRoundTrip() {
        OlapTable table = createTable();
        String json = toJsonWithStreaming(table, Table.class, true);

        for (boolean streaming : new boolean[] {true, false}) {
            OlapTable restored = (OlapTable) fromJsonWithStreaming(json, Table.class, streaming);
            Assert.assertEquals(table.getId(), restored.getId());
            Assert.assertEquals(table.getName(), restored.getName());
            Partition partition = restored.getPartition("p1");
            Assert.assertNotNull(partition);
            MaterializedIndex index = partition.getBaseIndex();
            Assert.assertEquals(1, index.getTablets().size());
            Tablet tablet = index.getTablets().get(0);
            Assert.assertEquals(5L, tablet.getId());
            Assert.assertEquals(3, tablet.getReplicas().size());
        }
    }

    @Test
    public void testHmsExternalTableStreamingWriteByteIdenticalAndCrossModeReadable() {
        HMSExternalTable table = createHmsExternalTable();
        String treeJson = toJsonWithStreaming(table, TableIf.class, false);
        String streamingJson = toJsonWithStreaming(table, TableIf.class, true);

        Assert.assertEquals(treeJson, streamingJson);
        Assert.assertTrue(streamingJson, streamingJson.startsWith("{\"clazz\":\"HMSExternalTable\","));

        for (String json : new String[] {treeJson, streamingJson}) {
            for (boolean streaming : new boolean[] {false, true}) {
                TableIf restored = fromJsonWithStreaming(json, TableIf.class, streaming);
                Assert.assertTrue(restored instanceof HMSExternalTable);
                Assert.assertEquals(table.getId(), restored.getId());
                Assert.assertEquals(table.getName(), restored.getName());
                Assert.assertEquals(table.getType(), restored.getType());
                Assert.assertEquals(table.getRemoteName(), ((HMSExternalTable) restored).getRemoteName());
            }
        }
    }

    @Test
    public void testLegacyPayloadWithoutTypeFieldReadableInStreamingMode() {
        // legacy data written before partition/tablet/replica became polymorphic has
        // no clazz field; the streaming read must dispatch it to the default subtype
        // by replaying the consumed first field name
        Replica replica = new Replica(7L, 10000L, ReplicaState.NORMAL, 6L, 0);
        String replicaJson = toJsonWithStreaming(replica, Replica.class, false);
        Assert.assertTrue(replicaJson, replicaJson.startsWith("{\"clazz\":\"Replica\","));
        String legacyReplicaJson = "{" + replicaJson.substring("{\"clazz\":\"Replica\",".length());

        Replica restored = fromJsonWithStreaming(legacyReplicaJson, Replica.class, true);
        Assert.assertEquals(replica.getId(), restored.getId());
        Assert.assertEquals(replica.getBackendIdWithoutException(), restored.getBackendIdWithoutException());
        Assert.assertEquals(replica.getVersion(), restored.getVersion());

        // same for the tree mode
        Replica treeRestored = fromJsonWithStreaming(legacyReplicaJson, Replica.class, false);
        Assert.assertEquals(replica.getId(), treeRestored.getId());
    }

    @Test
    public void testToJsonAsTextByteIdenticalWithTextWriteString() throws IOException {
        OlapTable table = createTable();

        ByteArrayOutputStream legacyBytes = new ByteArrayOutputStream();
        Text.writeString(new DataOutputStream(legacyBytes), GsonUtils.GSON.toJson(table));

        ByteArrayOutputStream streamingBytes = new ByteArrayOutputStream();
        GsonUtils.toJsonAsText(new DataOutputStream(streamingBytes), table);

        Assert.assertArrayEquals(legacyBytes.toByteArray(), streamingBytes.toByteArray());

        // cross read: streaming written bytes are readable by the legacy Text reader
        DataInputStream in = new DataInputStream(new ByteArrayInputStream(streamingBytes.toByteArray()));
        OlapTable legacyRead = GsonUtils.GSON.fromJson(Text.readString(in), OlapTable.class);
        Assert.assertEquals(table.getId(), legacyRead.getId());

        // and legacy written bytes are readable by the streaming reader
        in = new DataInputStream(new ByteArrayInputStream(legacyBytes.toByteArray()));
        OlapTable streamingRead = GsonUtils.fromJsonAsText(in, OlapTable.class);
        Assert.assertEquals(table.getId(), streamingRead.getId());
        Assert.assertEquals(table.getName(), streamingRead.getName());
    }

    @Test
    public void testOlapTableWriteReadRoundTrip() throws IOException {
        OlapTable table = createTable();
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        table.write(new DataOutputStream(bytes));

        OlapTable restored = OlapTable.read(new DataInputStream(new ByteArrayInputStream(bytes.toByteArray())));
        Assert.assertEquals(table.getId(), restored.getId());
        Assert.assertEquals(table.getName(), restored.getName());
        Assert.assertEquals(3, restored.getPartition("p1").getBaseIndex()
                .getTablet(5L).getReplicas().size());
    }

    @Test
    public void testDeepCopyViaJsonStream() {
        OlapTable table = createTable();
        OlapTable copied = GsonUtils.deepCopyViaJsonStream(table, OlapTable.class, FeConstants.meta_version);
        Assert.assertNotNull(copied);
        Assert.assertEquals(table.getId(), copied.getId());
        Assert.assertEquals(table.getName(), copied.getName());
        Tablet copiedTablet = copied.getPartition("p1").getBaseIndex().getTablet(5L);
        Assert.assertEquals(3, copiedTablet.getReplicas().size());
        // the copy is detached from the source
        copiedTablet.clearReplicas();
        Assert.assertEquals(3, table.getPartition("p1").getBaseIndex().getTablet(5L).getReplicas().size());
        Assert.assertEquals(0, copiedTablet.getReplicas().size());
    }

    @Test
    public void testSelectiveCopyStripReplicasForBackup() {
        boolean oldValue = Config.backup_meta_reserve_replica_info;
        try {
            Config.backup_meta_reserve_replica_info = false;
            OlapTable table = createTable();
            OlapTable copied = table.selectiveCopy(null, MaterializedIndex.IndexExtState.VISIBLE, true);
            Assert.assertNotNull(copied);
            Assert.assertEquals(0, copied.getPartition("p1").getBaseIndex()
                    .getTablet(5L).getReplicas().size());
            // the source table in the catalog is untouched
            Assert.assertEquals(3, table.getPartition("p1").getBaseIndex()
                    .getTablet(5L).getReplicas().size());

            Config.backup_meta_reserve_replica_info = true;
            copied = table.selectiveCopy(null, MaterializedIndex.IndexExtState.VISIBLE, true);
            Assert.assertNotNull(copied);
            Assert.assertEquals(3, copied.getPartition("p1").getBaseIndex()
                    .getTablet(5L).getReplicas().size());

            // non-backup copies always keep replicas
            Config.backup_meta_reserve_replica_info = false;
            copied = table.selectiveCopy(null, MaterializedIndex.IndexExtState.VISIBLE, false);
            Assert.assertNotNull(copied);
            Assert.assertEquals(3, copied.getPartition("p1").getBaseIndex()
                    .getTablet(5L).getReplicas().size());
        } finally {
            Config.backup_meta_reserve_replica_info = oldValue;
        }
    }
}
