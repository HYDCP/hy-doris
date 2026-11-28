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

import org.apache.doris.persist.gson.GsonUtils;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class PartitionStateTokenTest {

    private static Partition partition(long id, long version, long versionTime) {
        Partition p = new Partition(id, "p" + id, new MaterializedIndex(id + 1, MaterializedIndex.IndexState.NORMAL),
                new RandomDistributionInfo(1));
        p.updateVisibleVersionAndTime(version, versionTime);
        return p;
    }

    @Test
    public void testExplicitTokenOnlyAtItsBinding() {
        Partition p = partition(10, 5, 1000);
        p.setStateToken(new PartitionStateToken("aaaa", 5, 1000));
        Assertions.assertEquals("aaaa", PartitionStateToken.effective(p));
        // a write that did not set the token: the binding breaks, the partition has no token
        p.updateVisibleVersionAndTime(6, 2000);
        Assertions.assertNull(PartitionStateToken.effective(p));
        // the same version with another version time (a rewrite after a metadata rollback) has none either
        p.setStateToken(new PartitionStateToken("aaaa", 6, 2000));
        p.updateVisibleVersionAndTime(6, 2001);
        Assertions.assertNull(PartitionStateToken.effective(p));
    }

    @Test
    public void testInvalidHasNoToken() {
        Partition p = partition(10, 5, 1000);
        p.setStateToken(PartitionStateToken.invalid());
        Assertions.assertNull(PartitionStateToken.effective(p));
        // also at the initial version: a partition set back to it by an admin operation is not EMPTY
        Partition q = new Partition(20, "p20", new MaterializedIndex(21, MaterializedIndex.IndexState.NORMAL),
                new RandomDistributionInfo(1));
        q.setStateToken(PartitionStateToken.invalid());
        Assertions.assertNull(PartitionStateToken.effective(q));
    }

    @Test
    public void testEmptyOnlyAtTheInitialVersion() {
        // a new partition: no visible transaction, no data, the same name on every cluster
        Partition p = new Partition(10, "p10", new MaterializedIndex(11, MaterializedIndex.IndexState.NORMAL),
                new RandomDistributionInfo(1));
        Assertions.assertEquals(Partition.PARTITION_INIT_VERSION, p.getVisibleVersion());
        Assertions.assertEquals(PartitionStateToken.EMPTY, PartitionStateToken.effective(p));
        Partition other = new Partition(30, "p30", new MaterializedIndex(31, MaterializedIndex.IndexState.NORMAL),
                new RandomDistributionInfo(1));
        Assertions.assertEquals(PartitionStateToken.effective(p), PartitionStateToken.effective(other));
        // a version without a token (no name is made up from the ids and the version)
        p.updateVisibleVersionAndTime(2, 1000);
        Assertions.assertNull(PartitionStateToken.effective(p));
        // the first write from EMPTY gets a new token, never EMPTY
        Assertions.assertNotEquals(PartitionStateToken.EMPTY,
                PartitionStateToken.fresh(PartitionStateToken.EMPTY, 1, 10, 2));
    }

    @Test
    public void testFreshNeverRepeats() {
        // two writes of the same version from the same state (both sides of a failover) get different tokens
        Assertions.assertNotEquals(PartitionStateToken.fresh("prev", 1, 10, 6),
                PartitionStateToken.fresh("prev", 1, 10, 6));
    }

    @Test
    public void testPersisted() {
        Partition p = partition(10, 5, 1000);
        p.setStateToken(new PartitionStateToken("bbbb", 5, 1000));
        Partition q = GsonUtils.GSON.fromJson(GsonUtils.GSON.toJson(p), Partition.class);
        Assertions.assertEquals("bbbb", PartitionStateToken.effective(q));
        p.setStateToken(PartitionStateToken.invalid());
        q = GsonUtils.GSON.fromJson(GsonUtils.GSON.toJson(p), Partition.class);
        Assertions.assertNull(PartitionStateToken.effective(q));
    }
}
