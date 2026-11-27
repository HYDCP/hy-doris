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

#include <gtest/gtest.h>

#include "runtime/stream_load/stream_load_context.h"
#include "runtime/workload_management/workload_sched_policy.h"

namespace doris {

TEST(WorkloadUsernameConditionTest, RequiresUsernameAndOtherMetric) {
    TWorkloadCondition username;
    username.__set_metric_name(TWorkloadMetricType::USERNAME);
    username.__set_op(TCompareOperator::EQUAL);
    username.__set_value("alice");
    std::vector<std::unique_ptr<WorkloadCondition>> conditions;
    conditions.push_back(WorkloadConditionFactory::create_workload_condition(&username));
    conditions.push_back(std::make_unique<WorkloadConditionQueryTime>(GREATER, "100"));
    std::vector<std::unique_ptr<WorkloadAction>> actions;
    actions.push_back(std::make_unique<WorkloadActionCancelQuery>());
    WorkloadSchedPolicy policy;
    policy.init(1, "by_user", 0, true, 0, {}, std::move(conditions), std::move(actions));

    WorkloadQueryInfo query {};
    query.metric_map[QUERY_TIME] = "101";
    EXPECT_FALSE(policy.is_match(&query));
    query.metric_map[USERNAME] = "bob";
    EXPECT_FALSE(policy.is_match(&query));
    query.metric_map[USERNAME] = "alice";
    EXPECT_TRUE(policy.is_match(&query));
    query.metric_map[QUERY_TIME] = "100";
    EXPECT_FALSE(policy.is_match(&query));
}

TEST(WorkloadUsernameConditionTest, WalReplayDoesNotMatchItsInternalAdmin) {
    StreamLoadContext replay(nullptr, QuerySource::GROUP_COMMIT_LOAD);
    StreamLoadContext client(nullptr);
    for (auto* ctx : {&replay, &client}) {
        ctx->auth.user = "admin";
        ctx->wal_id = 42;
        WorkloadQueryInfo info {};
        info.set_username(ctx->query_source, ctx->auth.user);
        EXPECT_EQ(info.metric_map.contains(USERNAME), ctx == &client);
    }
}

TEST(WorkloadUsernameConditionTest, SharedGroupCommitHasNoPolicyUsername) {
    std::vector<std::unique_ptr<WorkloadCondition>> conditions;
    conditions.push_back(std::make_unique<WorkloadConditionUsername>(EQUAL, "admin"));
    std::vector<std::unique_ptr<WorkloadAction>> actions;
    actions.push_back(std::make_unique<WorkloadActionCancelQuery>());
    WorkloadSchedPolicy policy;
    policy.init(1, "admin_only", 0, true, 0, {}, std::move(conditions), std::move(actions));
    for (auto source : {QuerySource::INTERNAL_FRONTEND, QuerySource::STREAM_LOAD,
                        QuerySource::GROUP_COMMIT_LOAD}) {
        WorkloadQueryInfo query {};
        query.set_username(source, "admin");
        EXPECT_EQ(policy.is_match(&query), source != QuerySource::GROUP_COMMIT_LOAD);
    }
}

} // namespace doris
