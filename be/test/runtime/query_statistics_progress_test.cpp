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

#include <gen_cpp/FrontendService_types.h>
#include <gtest/gtest.h>

#include "runtime/query_statistics.h"

namespace doris {

TEST(QueryStatisticsProgressTest, MergesAndReportsPipelineCounters) {
    QueryStatistics first;
    first.add_total_pipeline_tasks(5);
    first.finish_pipeline_task();
    first.add_process_rows(12);
    QueryStatistics second;
    second.add_total_pipeline_tasks(3);
    second.add_process_rows(7);
    first.merge(second);

    TQueryStatistics thrift;
    first.to_thrift(&thrift);
    EXPECT_TRUE(thrift.__isset.total_tasks_num);
    EXPECT_EQ(8, thrift.total_tasks_num);
    EXPECT_EQ(1, thrift.finished_tasks_num);
    EXPECT_EQ(19, thrift.process_rows);
}

} // namespace doris
