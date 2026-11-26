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

#pragma once

#include <map>

#include "runtime/query_source.h"
#include "runtime/workload_management/workload_condition.h"

namespace doris {

class WorkloadQueryInfo {
public:
    void set_username(QuerySource source, const std::string& user) {
        // Group Commit can combine different users in a transaction. Its admin identity
        // is only used for internal authentication of the shared plan.
        if (source != QuerySource::GROUP_COMMIT_LOAD) {
            metric_map.emplace(WorkloadMetricType::USERNAME, user);
        }
    }

    std::map<WorkloadMetricType, std::string> metric_map;
    TUniqueId tquery_id;
    std::string query_id;
    int64_t wg_id;
    int64_t policy_id;
    std::string policy_name;
};

} // namespace doris