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

package org.apache.doris.resource.workloadschedpolicy;

import org.apache.doris.common.UserException;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.thrift.TWorkloadMetricType;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class WorkloadRemoteScanBytesTest {
    @Test
    void remoteConditionSurvivesReplayAndPublication() throws Exception {
        WorkloadConditionMeta condition = new WorkloadConditionMeta("be_scan_bytes_from_remote_storage", ">", "1");
        WorkloadActionMeta action = new WorkloadActionMeta("cancel_query", "");
        WorkloadSchedPolicy policy = new WorkloadSchedPolicy(1L, "remote_scan_policy",
                ImmutableList.of(WorkloadCondition.createWorkloadCondition(condition)),
                ImmutableList.of(WorkloadAction.createWorkloadAction(action)), ImmutableMap.of(), ImmutableList.of());
        policy.setConditionMeta(ImmutableList.of(condition));
        policy.setActionMeta(ImmutableList.of(action));

        WorkloadSchedPolicy replayed = GsonUtils.GSON.fromJson(GsonUtils.GSON.toJson(policy), WorkloadSchedPolicy.class);
        Assertions.assertEquals(TWorkloadMetricType.BE_SCAN_BYTES_FROM_REMOTE_STORAGE,
                replayed.toTopicInfo().getWorkloadSchedPolicy().getConditionList().get(0).getMetricName());
    }

    @Test
    void rejectsInvalidRemoteByteThresholds() throws Exception {
        for (String value : ImmutableList.of("-1", "9223372036854775808", "1.5")) {
            WorkloadConditionMeta condition = new WorkloadConditionMeta("be_scan_bytes_from_remote_storage", ">", value);
            Assertions.assertThrows(UserException.class, () -> WorkloadCondition.createWorkloadCondition(condition));
        }
    }
}
