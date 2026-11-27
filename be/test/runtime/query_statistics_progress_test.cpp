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

#include "pipeline/exec/empty_set_operator.h"
#include "runtime/query_statistics.h"
#include "runtime/runtime_query_statistics_mgr.h"
#include "vec/data_types/data_type_number.h"

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

TEST(QueryStatisticsProgressTest, OperatorOutputIsCountedOnceWithAndWithoutLimitCheck) {
    class Source : public pipeline::OperatorX<pipeline::EmptySetLocalState> {
    public:
        Source(ObjectPool* pool, bool check_limit)
                : OperatorX(pool, 0, 0), _check_limit(check_limit) {}
        Status get_block(RuntimeState* state, vectorized::Block* block, bool* eos) override {
            auto type = std::make_shared<vectorized::DataTypeInt32>();
            auto column = type->create_column();
            column->insert_many_defaults(3);
            block->insert({std::move(column), type, "value"});
            if (_check_limit) {
                get_local_state(state).reached_limit(block, eos);
            }
            return Status::OK();
        }

    private:
        bool _check_limit;
    };

    for (bool check_limit : {false, true}) {
        RuntimeState state;
        Source source(state.obj_pool(), check_limit);
        RuntimeProfile profile("test");
        pipeline::LocalStateInfo info {&profile, {}, nullptr, {}, 0};
        state.resize_op_id_to_local_state(1);
        ASSERT_TRUE(source.setup_local_state(&state, info).ok());
        vectorized::Block block;
        bool eos = false;
        ASSERT_TRUE(source.get_block_after_projects(&state, &block, &eos).ok());
        TQueryStatistics statistics;
        source.get_local_state(&state).get_query_statistics_ptr()->to_thrift(&statistics);
        EXPECT_EQ(3, block.rows());
        EXPECT_EQ(3, statistics.process_rows);
    }
}

TEST(QueryStatisticsProgressTest, ForwardedReportKeepsAuditAndReachesCoordinator) {
    TNetworkAddress audit_fe;
    audit_fe.__set_hostname("follower");
    audit_fe.__set_port(9020);
    TNetworkAddress coord_fe = audit_fe;
    coord_fe.__set_hostname("master");

    auto counters = std::make_shared<QueryStatistics>();
    counters->add_total_pipeline_tasks(5);
    counters->finish_pipeline_task();

    QueryStatisticsCtx ctx(audit_fe, TQueryType::SELECT, coord_fe);
    ctx._qs_list.push_back(counters);
    QueryStatisticsReports reports;
    ctx.append_to_report("query", reports);
    ASSERT_EQ(2, reports.size());
    EXPECT_EQ(5, reports.at(audit_fe).at("query").total_tasks_num);
    EXPECT_EQ(1, reports.at(coord_fe).at("query").finished_tasks_num);

    QueryStatisticsCtx local_ctx(audit_fe, TQueryType::SELECT);
    QueryStatisticsReports local_reports;
    local_ctx.append_to_report("local", local_reports);
    ASSERT_EQ(1, local_reports.size());
    EXPECT_TRUE(local_reports.at(audit_fe).contains("local"));
}

} // namespace doris
