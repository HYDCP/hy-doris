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
import org.apache.doris.metric.MetricRepo;
import org.apache.doris.system.SystemInfoService;

import com.google.common.collect.ImmutableMap;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;

public class TabletStatMgrTest {
    @Mocked
    private Env env;
    @Mocked
    private InternalCatalog catalog;
    @Mocked
    private SystemInfoService systemInfoService;

    private boolean savedMetricInit;
    private OlapTable table;
    private MaterializedIndex index;
    private Partition partition;

    @Before
    public void setUp() throws Exception {
        savedMetricInit = MetricRepo.isInit;
        MetricRepo.isInit = false;
        table = new OlapTable();
        table.setId(2L);
        table.setName("tablet_stat_test");
        table.setBaseIndexId(3L);
        index = new MaterializedIndex(3L, IndexState.NORMAL);
        partition = new Partition(4L, "p1", index, null);
        partition.setVisibleVersion(5L);
        table.addPartition(partition);
        Database db = new Database(1L, "tablet_stat_db");
        db.registerTable(table);
        new Expectations() {
            {
                Env.getCurrentSystemInfo();
                result = systemInfoService;
                systemInfoService.getAllBackendsByAllCluster();
                result = ImmutableMap.of();
                Env.getCurrentInternalCatalog();
                result = catalog;
                catalog.getDbIds();
                result = Collections.singletonList(1L);
                catalog.getDbNullable(1L);
                result = db;
            }
        };
    }

    @After
    public void tearDown() {
        MetricRepo.isInit = savedMetricInit;
    }

    @Test
    public void testAggregatesReplicaStatistics() {
        Tablet first = addTablet(10L);
        addReplica(first, 100L, 5L, 5L, 300L, 1000L, 100L);
        addReplica(first, 101L, 5L, 5L, 200L, 2000L, 200L);
        addReplica(first, 102L, 4L, 4L, 1L, 3000L, 300L);
        Tablet second = addTablet(11L);
        addReplica(second, 103L, 5L, 5L, 700L, 4000L, 400L);
        addReplica(second, 104L, 5L, 5L, 600L, 5000L, 500L);

        new TabletStatMgr().runAfterCatalogReady();

        Assert.assertEquals(8000L, table.getDataSize(true));
        Assert.assertEquals(15000L, table.getDataSize(false));
        Assert.assertEquals(800L, table.getRemoteDataSize());
        Assert.assertEquals(5L, table.getReplicaCount());
        Assert.assertEquals(800L, index.getRowCount());
        Assert.assertTrue(index.getRowCountReported());
        Assert.assertEquals(1500L, table.getLocalIndexFileSize());
        Assert.assertEquals(3000L, table.getLocalSegmentSize());
        Assert.assertEquals(4500L, table.getRemoteIndexFileSize());
        Assert.assertEquals(6000L, table.getRemoteSegmentSize());
    }

    @Test
    public void testUnreportedReplicaDoesNotMarkIndexReported() {
        Tablet tablet = addTablet(10L);
        Replica replica = addReplica(tablet, 100L, 5L, 0L, 200L, 1000L, 100L);
        TabletStatMgr manager = new TabletStatMgr();

        manager.runAfterCatalogReady();
        Assert.assertEquals(200L, index.getRowCount());
        Assert.assertFalse(index.getRowCountReported());

        replica.setLastReportVersion(5L);
        manager.runAfterCatalogReady();
        Assert.assertEquals(200L, index.getRowCount());
        Assert.assertTrue(index.getRowCountReported());
    }

    @Test
    public void testUnavailableRowsUseZeroInsteadOfSentinel() {
        Tablet stale = addTablet(10L);
        addReplica(stale, 100L, 4L, 4L, 200L, 1000L, 100L);
        addTablet(11L);

        new TabletStatMgr().runAfterCatalogReady();

        Assert.assertEquals(0L, index.getRowCount());
        Assert.assertFalse(index.getRowCountReported());
        Assert.assertEquals(1000L, table.getDataSize(true));
        Assert.assertEquals(1L, table.getReplicaCount());
    }

    @Test
    public void testInitialVersionReportsAnEmptyTablet() {
        partition.setVisibleVersion(Partition.PARTITION_INIT_VERSION);
        Tablet tablet = addTablet(10L);
        addReplica(tablet, 100L, Partition.PARTITION_INIT_VERSION,
                Partition.PARTITION_INIT_VERSION, 0L, 0L, 0L);

        new TabletStatMgr().runAfterCatalogReady();

        Assert.assertEquals(0L, index.getRowCount());
        Assert.assertTrue(index.getRowCountReported());
        Assert.assertEquals(0L, table.getDataSize(true));
    }

    private Tablet addTablet(long id) {
        Tablet tablet = new Tablet(id);
        index.addTablet(tablet, null, true);
        return tablet;
    }

    private Replica addReplica(Tablet tablet, long id, long version, long reportVersion,
            long rows, long dataSize, long remoteSize) {
        Replica replica = new Replica(id, id, ReplicaState.NORMAL, version, 0);
        replica.setLastReportVersion(reportVersion);
        replica.setRowCount(rows);
        replica.setDataSize(dataSize);
        replica.setRemoteDataSize(remoteSize);
        replica.setLocalInvertedIndexSize(dataSize / 10);
        replica.setLocalSegmentSize(dataSize / 5);
        replica.setRemoteInvertedIndexSize(dataSize * 3 / 10);
        replica.setRemoteSegmentSize(dataSize * 2 / 5);
        tablet.getReplicas().add(replica);
        return replica;
    }
}
