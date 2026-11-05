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

package org.apache.doris.clone;

import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.HashDistributionInfo;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.PartitionInfo;
import org.apache.doris.catalog.PartitionType;
import org.apache.doris.catalog.Replica;
import org.apache.doris.catalog.ReplicaAllocation;
import org.apache.doris.catalog.Tablet;
import org.apache.doris.catalog.TabletInvertedIndex;
import org.apache.doris.catalog.TabletMeta;
import org.apache.doris.common.Config;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.system.Backend;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.thrift.TStorageMedium;

import mockit.Expectations;
import mockit.Mocked;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;

public class TabletCheckerTest {
    @Mocked
    private Env env;
    private TabletScheduler scheduler;

    private boolean originalDisableScheduler;
    private int originalMaxSchedulingTablets;

    private SystemInfoService infoService;
    private TabletInvertedIndex invertedIndex;
    private TabletChecker checker;
    private OlapTable table;
    private PartitionInfo partitionInfo;
    private Database db;

    @Before
    public void setUp() {
        originalDisableScheduler = Config.disable_tablet_scheduler;
        originalMaxSchedulingTablets = Config.max_scheduling_tablets;
        Config.disable_tablet_scheduler = false;
        Config.max_scheduling_tablets = 2000;
        infoService = new SystemInfoService();
        invertedIndex = new TabletInvertedIndex();
        new Expectations() {
            {
                Env.getCurrentSystemInfo();
                result = infoService;
                minTimes = 0;
                Env.getCurrentInvertedIndex();
                result = invertedIndex;
                minTimes = 0;
            }
        };
        for (long beId = 1; beId <= 3; beId++) {
            Backend backend = new Backend(beId, "127.0.0." + beId, 9050);
            backend.setAlive(true);
            infoService.addBackend(backend);
        }
        partitionInfo = new PartitionInfo(PartitionType.RANGE);
        table = new OlapTable(20001L, "checker_local_allocation", Collections.emptyList(),
                KeysType.DUP_KEYS, partitionInfo, new HashDistributionInfo(2, Collections.emptyList()));
        table.setBaseIndexId(40001L);
        db = new Database(10001L, "checker_local_allocation_db");
        TabletSchedulerStat stat = new TabletSchedulerStat();
        scheduler = new TabletScheduler(env, infoService, invertedIndex, stat, "be");
        checker = new TabletChecker(env, infoService, scheduler, stat);
    }

    @After
    public void restoreSchedulerConfig() {
        Config.disable_tablet_scheduler = originalDisableScheduler;
        Config.max_scheduling_tablets = originalMaxSchedulingTablets;
    }

    private Partition addPartition(long partitionId, short replicaNum) {
        MaterializedIndex index = new MaterializedIndex(40001L, MaterializedIndex.IndexState.NORMAL);
        Partition partition = new Partition(partitionId, "p" + partitionId, index,
                new HashDistributionInfo(2, Collections.emptyList()));
        partition.updateVisibleVersion(100L);
        for (long bucket = 1; bucket <= 2; bucket++) {
            long tabletId = partitionId * 10 + bucket;
            Tablet tablet = new Tablet(tabletId);
            TabletMeta meta = new TabletMeta(db.getId(), table.getId(), partitionId,
                    index.getId(), 5, TStorageMedium.HDD);
            index.addTablet(tablet, meta, true);
            invertedIndex.addTablet(tabletId, meta);
            for (long beId = 1; beId <= replicaNum; beId++) {
                Replica replica = new Replica(tabletId * 10 + beId, beId, Replica.ReplicaState.NORMAL, 100L, 5);
                replica.updateLastFailedVersion(-1L);
                tablet.addReplica(replica);
            }
        }
        table.addPartition(partition);
        partitionInfo.setReplicaAllocation(partitionId, new ReplicaAllocation(replicaNum));
        return partition;
    }

    private TabletChecker.CheckerCounter checkPartition(Partition partition, boolean priority, long checkTime) {
        TabletChecker.CheckerCounter counter = new TabletChecker.CheckerCounter();
        table.readLock();
        try {
            Deencapsulation.invoke(checker, "handlePartitionTablet", db, table, partition, priority,
                    infoService.getAllBackendIds(true), checkTime, counter);
        } finally {
            table.readUnlock();
        }
        return counter;
    }

    private void assertHealthy(Partition partition, boolean priority, long checkTime) {
        TabletChecker.CheckerCounter counter = checkPartition(partition, priority, checkTime);
        Assert.assertEquals(2, counter.totalTabletNum);
        Assert.assertEquals(0, counter.unhealthyTabletNum);
        for (Tablet tablet : partition.getBaseIndex().getTablets()) {
            Assert.assertEquals(checkTime, (long) Deencapsulation.getField(tablet, "lastStatusCheckTime"));
        }
    }

    @Test
    public void testDifferentReplicaAllocationsWithinOneTable() {
        Partition single = addPartition(30001L, (short) 1);
        Partition triple = addPartition(30002L, (short) 3);
        assertHealthy(single, false, 1000L);
        assertHealthy(triple, false, 2000L);
        assertHealthy(single, true, 3000L);
        assertHealthy(triple, true, 4000L);
    }

    @Test
    public void testReplicaAllocationChangesBetweenRounds() {
        Partition partition = addPartition(30001L, (short) 1);
        assertHealthy(partition, false, 1000L);
        ReplicaAllocation changedAllocation = new ReplicaAllocation((short) 3);
        partitionInfo.setReplicaAllocation(partition.getId(), changedAllocation);
        TabletChecker.CheckerCounter counter = checkPartition(partition, false, 2000L);
        Assert.assertEquals(2, counter.totalTabletNum);
        Assert.assertEquals(2, counter.unhealthyTabletNum);
        Assert.assertEquals(2, counter.addToSchedulerTabletNum);
        Assert.assertEquals(2, scheduler.getPendingTabletQueue().size());
        for (TabletSchedCtx context : scheduler.getPendingTabletQueue()) {
            Assert.assertSame(changedAllocation, context.getReplicaAlloc());
        }
        for (Tablet tablet : partition.getBaseIndex().getTablets()) {
            Assert.assertEquals(1000L, (long) Deencapsulation.getField(tablet, "lastStatusCheckTime"));
        }
        scheduler.clear();
        partitionInfo.setReplicaAllocation(partition.getId(), new ReplicaAllocation((short) 1));
        assertHealthy(partition, false, 3000L);
    }

    @Test
    public void testScheduledTabletsRemainSkipped() {
        Partition partition = addPartition(30001L, (short) 1);
        for (Tablet tablet : partition.getBaseIndex().getTablets()) {
            TabletSchedCtx context = new TabletSchedCtx(TabletSchedCtx.Type.REPAIR,
                    db.getId(), table.getId(), partition.getId(), partition.getBaseIndex().getId(), tablet.getId(),
                    partitionInfo.getReplicaAllocation(partition.getId()), System.currentTimeMillis());
            Assert.assertEquals(TabletScheduler.AddResult.ADDED, scheduler.addTablet(context, false));
        }
        TabletChecker.CheckerCounter counter = checkPartition(partition, false, 1000L);
        Assert.assertEquals(2, counter.totalTabletNum);
        Assert.assertEquals(2, counter.tabletInScheduler);
        Assert.assertEquals(0, counter.unhealthyTabletNum);
    }

    @Test
    public void testNonNormalPartitionRemainsSkipped() {
        Partition partition = addPartition(30001L, (short) 1);
        partition.setState(Partition.PartitionState.RESTORE);
        TabletChecker.CheckerCounter counter = checkPartition(partition, false, 1000L);
        Assert.assertEquals(0, counter.totalTabletNum);
    }

    @Test
    public void testEmptyPartitionRemainsSkipped() {
        Partition partition = new Partition(30001L, "empty",
                new MaterializedIndex(40001L, MaterializedIndex.IndexState.NORMAL),
                new HashDistributionInfo(1, Collections.emptyList()));
        table.addPartition(partition);
        TabletChecker.CheckerCounter counter = checkPartition(partition, false, 1000L);
        Assert.assertEquals(0, counter.totalTabletNum);
        Assert.assertEquals(0, counter.unhealthyTabletNum);
    }
}
