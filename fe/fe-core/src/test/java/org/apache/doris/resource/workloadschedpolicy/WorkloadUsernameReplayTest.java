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

import org.apache.doris.analysis.CreateWorkloadSchedPolicyStmt;
import org.apache.doris.common.UserException;
import org.apache.doris.persist.gson.GsonUtils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class WorkloadUsernameReplayTest {
    @Test
    void replayPreservesHistoricalBlankUsername() throws Exception {
        WorkloadConditionMeta condition = new WorkloadConditionMeta("username", "=", " ");
        WorkloadActionMeta action = new WorkloadActionMeta("set_session_variable", "query_timeout=10");
        WorkloadSchedPolicy policy = new WorkloadSchedPolicy(1L, "old_policy",
                ImmutableList.of(WorkloadCondition.createWorkloadCondition(condition)),
                ImmutableList.of(WorkloadAction.createWorkloadAction(action)), ImmutableMap.of(), ImmutableList.of());
        policy.setConditionMeta(ImmutableList.of(condition));
        policy.setActionMeta(ImmutableList.of(action));

        WorkloadSchedPolicy replayed = GsonUtils.GSON.fromJson(GsonUtils.GSON.toJson(policy), WorkloadSchedPolicy.class);
        WorkloadQueryInfo query = new WorkloadQueryInfo();
        query.metricMap = ImmutableMap.of(WorkloadMetricType.USERNAME, "another_user");
        Assertions.assertFalse(replayed.isMatch(query));
    }

    @Test
    void rejectsInvalidNewUsernameConditions() throws Exception {
        WorkloadSchedPolicyMgr mgr = new WorkloadSchedPolicyMgr();
        WorkloadActionMeta cancel = new WorkloadActionMeta("cancel_query", "");
        for (WorkloadConditionMeta condition : ImmutableList.of(
                new WorkloadConditionMeta("username", "=", " "),
                new WorkloadConditionMeta("username", ">", "test_user"))) {
            CreateWorkloadSchedPolicyStmt stmt = new CreateWorkloadSchedPolicyStmt(false, "invalid_policy",
                    ImmutableList.of(condition), ImmutableList.of(cancel), ImmutableMap.of());
            Assertions.assertThrows(UserException.class, () -> mgr.createWorkloadSchedPolicy(stmt));
        }
    }
}
