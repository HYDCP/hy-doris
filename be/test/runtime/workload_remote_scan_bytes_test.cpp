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

#include "runtime/workload_management/workload_sched_policy.h"

namespace doris {

TEST(WorkloadRemoteScanBytesTest, IgnoresTotalBytesForRemoteThreshold) {
    TWorkloadCondition remote;
    remote.__set_metric_name(TWorkloadMetricType::BE_SCAN_BYTES_FROM_REMOTE_STORAGE);
    remote.__set_op(TCompareOperator::GREATER);
    remote.__set_value("100");
    std::vector<std::unique_ptr<WorkloadCondition>> conditions;
    conditions.push_back(WorkloadConditionFactory::create_workload_condition(&remote));
    std::vector<std::unique_ptr<WorkloadAction>> actions;
    actions.push_back(std::make_unique<WorkloadActionCancelQuery>());
    WorkloadSchedPolicy policy;
    policy.init(1, "remote_scan_policy", 0, true, 0, {}, std::move(conditions), std::move(actions));

    WorkloadQueryInfo query {};
    query.metric_map[SCAN_BYTES] = "1000";
    EXPECT_FALSE(policy.is_match(&query));
    query.metric_map[SCAN_BYTES_FROM_REMOTE_STORAGE] = "100";
    EXPECT_FALSE(policy.is_match(&query));
    query.metric_map[SCAN_BYTES_FROM_REMOTE_STORAGE] = "101";
    EXPECT_TRUE(policy.is_match(&query));
}

TEST(WorkloadRemoteScanBytesTest, InvalidThresholdDoesNotInterruptSubsequentConditions) {
    TWorkloadCondition condition;
    condition.__set_metric_name(TWorkloadMetricType::BE_SCAN_BYTES_FROM_REMOTE_STORAGE);
    condition.__set_op(TCompareOperator::EQUAL);
    for (const auto* value : {"1２", "２", "+-0", "-1", "", "+", "9223372036854775808"}) {
        condition.__set_value(value);
        EXPECT_EQ(nullptr, WorkloadConditionFactory::create_workload_condition(&condition))
                << value;
    }
    condition.__set_value("+12");
    auto parsed = WorkloadConditionFactory::create_workload_condition(&condition);
    ASSERT_NE(nullptr, parsed);
    EXPECT_TRUE(parsed->eval("12"));
    condition.__set_value("-00");
    parsed = WorkloadConditionFactory::create_workload_condition(&condition);
    ASSERT_NE(nullptr, parsed);
    EXPECT_TRUE(parsed->eval("0"));
}

} // namespace doris
