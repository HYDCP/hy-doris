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
import org.apache.doris.clone.TabletSchedCtx.Priority;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.FeConstants;
import org.apache.doris.resource.Tag;
import org.apache.doris.system.Backend;
import org.apache.doris.system.SystemInfoService;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TabletHealthCollectionTest {
    @Mocked
    private Env env;

    private SystemInfoService infoService;
    private Tablet tablet;
    private boolean savedAllowSameHost;
    private boolean savedRunningUnitTest;
    private boolean savedRepairSlowReplica;
    private String savedDeployMode;
    private String savedCloudUniqueId;

    @Before
    public void setUp() {
        savedAllowSameHost = Config.allow_replica_on_same_host;
        savedRunningUnitTest = FeConstants.runningUnitTest;
        savedRepairSlowReplica = Config.repair_slow_replica;
        savedDeployMode = Config.deploy_mode;
        savedCloudUniqueId = Config.cloud_unique_id;
        Config.allow_replica_on_same_host = false;
        FeConstants.runningUnitTest = false;
        Config.repair_slow_replica = false;
        Config.deploy_mode = "";
        Config.cloud_unique_id = "";
        infoService = new SystemInfoService();
        tablet = new Tablet(10001L);
        new Expectations() {
            {
                Env.getCurrentSystemInfo();
                result = infoService;
                minTimes = 0;
            }
        };
    }

    @After
    public void tearDown() {
        Config.allow_replica_on_same_host = savedAllowSameHost;
        FeConstants.runningUnitTest = savedRunningUnitTest;
        Config.repair_slow_replica = savedRepairSlowReplica;
        Config.deploy_mode = savedDeployMode;
        Config.cloud_unique_id = savedCloudUniqueId;
    }

    @Test
    public void testSingleTagAndMultipleTags() throws AnalysisException {
        addReplica(1L, "127.0.0.1", "zone_a");
        addReplica(2L, "127.0.0.2", "zone_a");
        addReplica(3L, "127.0.0.3", "zone_b");
        ReplicaAllocation allocation = new ReplicaAllocation(ImmutableMap.of(
                location("zone_a"), (short) 2, location("zone_b"), (short) 1));

        assertHealth(allocation, Tablet.TabletStatus.HEALTHY, Priority.NORMAL, 3);

        infoService.getBackend(3L).setTagMap(ImmutableMap.of(Tag.TYPE_LOCATION, "zone_a"));
        allocation = new ReplicaAllocation(ImmutableMap.of(location("zone_a"), (short) 3));
        assertHealth(allocation, Tablet.TabletStatus.HEALTHY, Priority.NORMAL, 3);
    }

    @Test
    public void testMissingRequestedTag() throws AnalysisException {
        addReplica(1L, "127.0.0.1", "zone_a");
        addReplica(2L, "127.0.0.2", "zone_a");
        addReplica(3L, "127.0.0.3", "zone_b");
        ReplicaAllocation allocation = new ReplicaAllocation(ImmutableMap.of(
                location("zone_a"), (short) 1, location("zone_b"), (short) 1,
                location("zone_c"), (short) 1));

        assertHealth(allocation, Tablet.TabletStatus.REPLICA_MISSING_FOR_TAG, Priority.NORMAL, 3);
    }

    @Test
    public void testUnexpectedTagsCanExceedConfiguredTagCount() throws AnalysisException {
        addReplica(1L, "127.0.0.1", "zone_b");
        addReplica(2L, "127.0.0.2", "zone_c");
        addReplica(3L, "127.0.0.3", "zone_d");
        ReplicaAllocation allocation = new ReplicaAllocation(ImmutableMap.of(location("zone_a"), (short) 3));

        assertHealth(allocation, Tablet.TabletStatus.REPLICA_MISSING_FOR_TAG, Priority.NORMAL, 3);

        for (long id = 4L; id <= 20L; id++) {
            addReplica(id, "127.0.0." + id, "zone_" + id);
        }
        assertHealth(allocation, Tablet.TabletStatus.REDUNDANT, Priority.VERY_HIGH, 20);
    }

    @Test
    public void testDuplicateHostStillRequiresSeparateHost() throws AnalysisException {
        addReplica(1L, "127.0.0.1", "default");
        addReplica(2L, "127.0.0.1", "default");
        addReplica(3L, "127.0.0.3", "default");
        Backend availableBackend = new Backend(4L, "127.0.0.4", 8030);
        availableBackend.setAlive(true);
        infoService.addBackend(availableBackend);
        ReplicaAllocation allocation = new ReplicaAllocation((short) 3);

        assertHealth(allocation, Tablet.TabletStatus.REPLICA_MISSING, Priority.NORMAL, 2);

        Config.allow_replica_on_same_host = true;
        assertHealth(allocation, Tablet.TabletStatus.HEALTHY, Priority.NORMAL, 3);
    }

    @Test
    public void testColocateHostCollectionAndExtraReplica() throws AnalysisException {
        addReplica(1L, "127.0.0.1", "default");
        addReplica(2L, "127.0.0.2", "default");
        addReplica(3L, "127.0.0.3", "default");
        ReplicaAllocation allocation = new ReplicaAllocation((short) 3);

        Tablet.TabletHealth health = tablet.getColocateHealth(100L, allocation, Sets.newHashSet(1L, 2L, 3L));
        Assert.assertEquals(Tablet.TabletStatus.HEALTHY, health.status);
        Assert.assertEquals(3, health.aliveAndVersionCompleteNum);

        addReplica(4L, "127.0.0.4", "default");
        health = tablet.getColocateHealth(100L, allocation, Sets.newHashSet(1L, 2L, 3L));
        Assert.assertEquals(Tablet.TabletStatus.COLOCATE_REDUNDANT, health.status);
        Assert.assertEquals(Priority.NORMAL, health.priority);
    }

    @Test
    public void testEmptyTablet() {
        assertHealth(new ReplicaAllocation((short) 3), Tablet.TabletStatus.UNRECOVERABLE, Priority.NORMAL, 0);
    }

    private void addReplica(long backendId, String host, String zone) {
        Backend backend = new Backend(backendId, host, 8030);
        backend.setAlive(true);
        backend.setTagMap(ImmutableMap.of(Tag.TYPE_LOCATION, zone));
        infoService.addBackend(backend);
        Replica replica = new Replica(backendId + 100L, backendId, ReplicaState.NORMAL, 100L, 0);
        tablet.getReplicas().add(replica);
    }

    private static Tag location(String zone) throws AnalysisException {
        return Tag.create(Tag.TYPE_LOCATION, zone);
    }

    private void assertHealth(ReplicaAllocation allocation, Tablet.TabletStatus expectedStatus,
            Priority expectedPriority, int expectedAlive) {
        Tablet.TabletHealth health = tablet.getHealth(infoService, 100L, allocation,
                infoService.getAllBackendIds(true));
        Assert.assertEquals(expectedStatus, health.status);
        Assert.assertEquals(expectedPriority, health.priority);
        Assert.assertEquals(expectedAlive, health.aliveAndVersionCompleteNum);
    }
}
