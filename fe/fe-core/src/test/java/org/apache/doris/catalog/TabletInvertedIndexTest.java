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

import org.apache.doris.catalog.Replica.ReplicaState;
import org.apache.doris.common.Config;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.thrift.TStorageMedium;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.HashMultimap;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

public class TabletInvertedIndexTest {
    private String savedDeployMode;
    private String savedCloudUniqueId;

    @Before
    public void setUp() {
        savedDeployMode = Config.deploy_mode;
        savedCloudUniqueId = Config.cloud_unique_id;
        Config.deploy_mode = "";
        Config.cloud_unique_id = "";
    }

    @After
    public void tearDown() {
        Config.deploy_mode = savedDeployMode;
        Config.cloud_unique_id = savedCloudUniqueId;
    }

    @Test
    public void testGetReplica() {
        TabletInvertedIndex index = new TabletInvertedIndex();
        long tabletId = 10001L;
        long backendId = 20001L;
        index.addTablet(tabletId, new TabletMeta(1, 2, 3, 4, 5, TStorageMedium.HDD));
        Replica replica = new Replica(30001L, backendId, 5, ReplicaState.NORMAL);
        index.addReplica(tabletId, replica);

        Assert.assertSame(replica, index.getReplica(tabletId, backendId));
        Assert.assertNull(index.getReplica(tabletId, backendId + 1));
    }

    @Test
    public void testMissingTabletMessage() {
        TabletInvertedIndex index = new TabletInvertedIndex();
        IllegalStateException exception = Assert.assertThrows(IllegalStateException.class,
                () -> index.getReplica(Long.MAX_VALUE, Long.MIN_VALUE));
        Assert.assertEquals("tablet 9223372036854775807 not exists, backend -9223372036854775808",
                exception.getMessage());
    }

    @Test
    public void testGetReplicaInCloudMode() {
        Config.deploy_mode = "cloud";
        TabletInvertedIndex index = new TabletInvertedIndex();
        long tabletId = 10001L;
        index.addTablet(tabletId, new TabletMeta(1, 2, 3, 4, 5, TStorageMedium.HDD));
        Replica replica = new Replica(30001L, 20001L, 5, ReplicaState.NORMAL);
        index.addReplica(tabletId, replica);

        Assert.assertSame(replica, index.getReplica(tabletId, 20001L));
        Assert.assertSame(replica, index.getReplica(tabletId, -1L));
        IllegalStateException exception = Assert.assertThrows(IllegalStateException.class,
                () -> index.getReplica(tabletId + 1, 20002L));
        Assert.assertEquals("tablet 10002 not exists, backend 20002", exception.getMessage());
    }

    @Test
    public void testReplicaCountsByBackendAndStorageMedium() {
        TabletInvertedIndex index = new TabletInvertedIndex();
        long backendId = 20001L;
        for (long tabletId = 10001L; tabletId <= 10003L; tabletId++) {
            TStorageMedium medium = tabletId == 10003L ? TStorageMedium.SSD : TStorageMedium.HDD;
            index.addTablet(tabletId, new TabletMeta(1, 2, 3, 4, 5, medium));
            index.addReplica(tabletId, new Replica(30000L + tabletId, backendId, 5, ReplicaState.NORMAL));
        }
        index.addReplica(10003L, new Replica(40001L, backendId + 1, 5, ReplicaState.NORMAL));

        assertReplicaCounts(index, backendId, 2, 1);
        assertReplicaCounts(index, backendId + 1, 0, 1);
        assertReplicaCounts(index, backendId + 2, 0, 0);
        index.deleteTablet(10003L);
        assertReplicaCounts(index, backendId, 2, 0);
        assertReplicaCounts(index, backendId + 1, 0, 0);
    }

    private void assertReplicaCounts(TabletInvertedIndex index, long backendId, long hdd, long ssd) {
        Map<TStorageMedium, Long> counts = index.getReplicaNumByBeIdAndStorageMedium(backendId);
        Assert.assertEquals(2, counts.size());
        Assert.assertEquals(Long.valueOf(hdd), counts.get(TStorageMedium.HDD));
        Assert.assertEquals(Long.valueOf(ssd), counts.get(TStorageMedium.SSD));
    }

    @Test
    public void testReportMissingTabletMessage() {
        TabletInvertedIndex index = new TabletInvertedIndex();
        Map.Entry<Long, Replica> entry = new AbstractMap.SimpleImmutableEntry<>(Long.MAX_VALUE,
                new Replica(30001L, Long.MIN_VALUE, 5, ReplicaState.NORMAL));
        IllegalStateException exception = Assert.assertThrows(IllegalStateException.class,
                () -> Deencapsulation.invoke(index, "processTabletEntry", Long.MIN_VALUE,
                        new HashMap<>(), new HashMap<>(), ArrayListMultimap.create(),
                        ArrayListMultimap.create(), new HashSet<>(), ArrayListMultimap.create(),
                        new HashMap<>(), HashMultimap.create(), ArrayListMultimap.create(),
                        new ArrayList<>(), new ArrayList<>(), entry));
        Assert.assertEquals("tablet 9223372036854775807 not exists, backend -9223372036854775808",
                exception.getMessage());
    }
}
