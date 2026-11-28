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

package org.apache.doris.system;

import org.apache.doris.catalog.PartitionStateToken;
import org.apache.doris.common.Config;
import org.apache.doris.ha.FrontendNodeType;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.system.SystemInfoService.HostInfo;
import org.apache.doris.thrift.TFrontendPingFrontendResult;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Whether a frontend replays OP_SET_PARTITION_STATE_TOKEN, as it reports in the heartbeat.
 */
public class PartitionStateTokenCapabilityHeartbeatTest {

    @Test
    public void testFrontendCapabilityBeforeAndAfterTheHeartbeat() {
        // a frontend coming back to alive is removed from the unready electable nodes of the BDB
        String origEditLogType = Config.edit_log_type;
        Config.edit_log_type = "local";
        try {
            Frontend fe = new Frontend(FrontendNodeType.OBSERVER, "fe1", "127.0.0.1", 9010);
            // before the first heartbeat
            Assertions.assertEquals(0, fe.getPartitionStateTokenCapability());

            // a frontend of an old version does not report it
            FrontendHbResponse old = new FrontendHbResponse("fe1", 9030, 9020, -1, 1L, System.currentTimeMillis(),
                    "old", 1L, null, 1L);
            fe.handleHbResponse(old, false);
            Assertions.assertEquals(0, fe.getPartitionStateTokenCapability());

            FrontendHbResponse current = new FrontendHbResponse("fe1", 9030, 9020, -1, 1L,
                    System.currentTimeMillis(), "new", 1L, null, 1L)
                    .setPartitionStateTokenCapability(PartitionStateToken.CAPABILITY);
            // the replay of the edit log does not carry it
            fe.handleHbResponse(current, true);
            Assertions.assertEquals(0, fe.getPartitionStateTokenCapability());
            fe.handleHbResponse(current, false);
            Assertions.assertEquals(1, fe.getPartitionStateTokenCapability());

            // a frontend that does not answer may come back as an old version
            fe.handleHbResponse(new FrontendHbResponse("fe1", "down"), false);
            Assertions.assertEquals(0, fe.getPartitionStateTokenCapability());
            Assertions.assertFalse(fe.isAlive());
        } finally {
            Config.edit_log_type = origEditLogType;
        }
    }

    // Only the host and the edit log port of the master are the master: another frontend on its host (an old version
    // may run there) is pinged, and reports its own capability.
    @Test
    public void testOnlyTheMasterItselfIsSelf() {
        HostInfo master = new HostInfo("10.0.0.1", 9010);
        Assertions.assertTrue(HeartbeatMgr.isSelf(new Frontend(FrontendNodeType.FOLLOWER, "m", "10.0.0.1", 9010),
                master));
        Assertions.assertFalse(HeartbeatMgr.isSelf(new Frontend(FrontendNodeType.OBSERVER, "o", "10.0.0.1", 9011),
                master));
        Assertions.assertFalse(HeartbeatMgr.isSelf(new Frontend(FrontendNodeType.FOLLOWER, "f", "10.0.0.2", 9010),
                master));
    }

    @Test
    public void testCapabilityOfThePing() {
        TFrontendPingFrontendResult ping = new TFrontendPingFrontendResult();
        Assertions.assertFalse(ping.isSetPartitionStateTokenCapability());
        ping.setPartitionStateTokenCapability(PartitionStateToken.CAPABILITY);
        Assertions.assertEquals(1, ping.getPartitionStateTokenCapability());
    }

    @Test
    public void testCapabilityIsNotPersisted() {
        Frontend fe = new Frontend(FrontendNodeType.FOLLOWER, "fe1", "127.0.0.1", 9010);
        fe.setPartitionStateTokenCapability(1);
        Assertions.assertFalse(GsonUtils.GSON.toJson(fe).toLowerCase().contains("capability"));
        Assertions.assertEquals(0, GsonUtils.GSON.fromJson(GsonUtils.GSON.toJson(fe), Frontend.class)
                .getPartitionStateTokenCapability());
        // nor in the heartbeat package written to the edit log
        FrontendHbResponse response = new FrontendHbResponse("fe1", 9030, 9020, -1, 1L, 1L, "v", 1L, null, 1L)
                .setPartitionStateTokenCapability(1);
        Assertions.assertFalse(GsonUtils.GSON.toJson(response).toLowerCase().contains("capability"));
    }
}
