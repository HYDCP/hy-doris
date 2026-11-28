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

import org.apache.doris.backup.RestoreJob;
import org.apache.doris.common.Config;
import org.apache.doris.ha.FrontendNodeType;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.system.HeartbeatResponse.HbStatus;
import org.apache.doris.thrift.TBackendInfo;
import org.apache.doris.thrift.TFrontendPingFrontendResult;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * The capability of the restore reuse that a frontend or a backend reports in the heartbeat.
 */
public class RestoreReuseCapabilityHeartbeatTest {

    @Test
    public void testMissingCapabilityIsZero() {
        TBackendInfo backendInfo = new TBackendInfo();
        Assertions.assertEquals(0, HeartbeatMgr.restoreReuseCapabilityOf(backendInfo));
        backendInfo.setRestoreReuseCapability(1);
        Assertions.assertEquals(1, HeartbeatMgr.restoreReuseCapabilityOf(backendInfo));

        TFrontendPingFrontendResult ping = new TFrontendPingFrontendResult();
        Assertions.assertEquals(0, HeartbeatMgr.restoreReuseCapabilityOf(ping));
        ping.setRestoreReuseCapability(1);
        Assertions.assertEquals(1, HeartbeatMgr.restoreReuseCapabilityOf(ping));
    }

    // The version 2 of the capability (the logical digest of the merge-on-write tables) is reported as it is, and the
    // frontend reports it, while the minimum that the reuse of the other tables needs stays 1.
    @Test
    public void testCapability2IsReportedAndKept() {
        TBackendInfo backendInfo = new TBackendInfo();
        backendInfo.setRestoreReuseCapability(2);
        Assertions.assertEquals(2, HeartbeatMgr.restoreReuseCapabilityOf(backendInfo));
        TFrontendPingFrontendResult ping = new TFrontendPingFrontendResult();
        ping.setRestoreReuseCapability(RestoreJob.RESTORE_REUSE_CAPABILITY_REPORTED);
        // 3: the load of a kept partition by the snapshot task, which no node is required to have
        Assertions.assertEquals(3, HeartbeatMgr.restoreReuseCapabilityOf(ping));
        Assertions.assertEquals(3, RestoreJob.RESTORE_REUSE_CAPABILITY_REPORTED);
        Assertions.assertEquals(1, RestoreJob.RESTORE_REUSE_CAPABILITY_MIN_REQUIRED);
        Assertions.assertTrue(RestoreJob.RESTORE_REUSE_CAPABILITY_REPORTED >= RestoreJob.RESTORE_REUSE_CAPABILITY_MOW);

        Backend be = new Backend(10001, "127.0.0.1", 9050);
        be.handleHbResponse(new BackendHbResponse(10001, 9060, 8040, 8060, System.currentTimeMillis(), 1L,
                "new", "mix", 0, 0, false, -1, 0).setRestoreReuseCapability(2), false);
        Assertions.assertEquals(2, be.getRestoreReuseCapability());
    }

    @Test
    public void testBackendCapabilityBeforeAndAfterTheHeartbeat() {
        Backend be = new Backend(10001, "127.0.0.1", 9050);
        // before the first heartbeat
        Assertions.assertEquals(0, be.getRestoreReuseCapability());

        // a heartbeat of a backend which does not report it
        BackendHbResponse old = new BackendHbResponse(10001, 9060, 8040, 8060, System.currentTimeMillis(), 1L,
                "old", "mix", 0, 0, false, -1, 0);
        be.handleHbResponse(old, false);
        Assertions.assertEquals(0, be.getRestoreReuseCapability());

        BackendHbResponse current = new BackendHbResponse(10001, 9060, 8040, 8060, System.currentTimeMillis(), 1L,
                "new", "mix", 0, 0, false, -1, 0).setRestoreReuseCapability(1);
        // the replay of the edit log does not carry it
        be.handleHbResponse(current, true);
        Assertions.assertEquals(0, be.getRestoreReuseCapability());
        be.handleHbResponse(current, false);
        Assertions.assertEquals(1, be.getRestoreReuseCapability());
        // a backend downgraded in place reports none again
        be.handleHbResponse(old, false);
        Assertions.assertEquals(0, be.getRestoreReuseCapability());
    }

    @Test
    public void testFrontendCapabilityBeforeAndAfterTheHeartbeat() {
        // a frontend coming back to alive is removed from the unready electable nodes of the BDB
        String origEditLogType = Config.edit_log_type;
        Config.edit_log_type = "local";
        try {
            checkFrontendCapability();
        } finally {
            Config.edit_log_type = origEditLogType;
        }
    }

    private void checkFrontendCapability() {
        Frontend fe = new Frontend(FrontendNodeType.FOLLOWER, "fe1", "127.0.0.1", 9010);
        Assertions.assertEquals(0, fe.getRestoreReuseCapability());

        FrontendHbResponse old = new FrontendHbResponse("fe1", 9030, 9020, -1, 1L, System.currentTimeMillis(), "old",
                1L, null, 1L);
        fe.handleHbResponse(old, false);
        Assertions.assertEquals(0, fe.getRestoreReuseCapability());

        FrontendHbResponse current = new FrontendHbResponse("fe1", 9030, 9020, -1, 1L, System.currentTimeMillis(),
                "new", 1L, null, 1L).setRestoreReuseCapability(1);
        Assertions.assertEquals(HbStatus.OK, current.getStatus());
        fe.handleHbResponse(current, true);
        Assertions.assertEquals(0, fe.getRestoreReuseCapability());
        fe.handleHbResponse(current, false);
        Assertions.assertEquals(1, fe.getRestoreReuseCapability());
        // a frontend that does not answer may come back as an old version
        fe.handleHbResponse(new FrontendHbResponse("fe1", "down"), false);
        Assertions.assertEquals(0, fe.getRestoreReuseCapability());
    }

    @Test
    public void testCapabilityIsNotPersisted() {
        Backend be = new Backend(10001, "127.0.0.1", 9050);
        be.setRestoreReuseCapability(1);
        Assertions.assertFalse(GsonUtils.GSON.toJson(be).toLowerCase().contains("capability"));
        Assertions.assertEquals(0, GsonUtils.GSON.fromJson(GsonUtils.GSON.toJson(be), Backend.class)
                .getRestoreReuseCapability());

        Frontend fe = new Frontend(FrontendNodeType.FOLLOWER, "fe1", "127.0.0.1", 9010);
        fe.setRestoreReuseCapability(1);
        Assertions.assertFalse(GsonUtils.GSON.toJson(fe).toLowerCase().contains("capability"));
        Assertions.assertEquals(0, GsonUtils.GSON.fromJson(GsonUtils.GSON.toJson(fe), Frontend.class)
                .getRestoreReuseCapability());

        // nor in the heartbeat package written to the edit log
        BackendHbResponse response = new BackendHbResponse(10001, 9060, 8040, 8060, 1L, 1L, "v", "mix", 0, 0, false,
                -1, 0).setRestoreReuseCapability(1);
        Assertions.assertFalse(GsonUtils.GSON.toJson(response).toLowerCase().contains("capability"));
    }
}
