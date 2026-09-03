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

#include "runtime/group_commit_mgr.h"

#include <gtest/gtest.h>

#include <cstdint>
#include <limits>
#include <memory>
#include <string>

#include "common/config.h"
#include "runtime/runtime_state.h"
#include "vec/columns/columns_number.h"
#include "vec/data_types/data_type_number.h"

namespace doris {

class GroupCommitMgrTest : public testing::Test {
protected:
    void SetUp() override {
        _original_wait_replay_wal_finish = config::group_commit_wait_replay_wal_finish;
        _original_queue_mem_limit = config::group_commit_queue_mem_limit;
        config::group_commit_wait_replay_wal_finish = false;
        config::group_commit_queue_mem_limit = std::numeric_limits<int32_t>::max();
    }

    void TearDown() override {
        config::group_commit_wait_replay_wal_finish = _original_wait_replay_wal_finish;
        config::group_commit_queue_mem_limit = _original_queue_mem_limit;
    }

    static std::shared_ptr<vectorized::Block> create_block(size_t rows) {
        auto column = vectorized::ColumnInt32::create();
        for (size_t i = 0; i < rows; ++i) {
            column->insert_value(static_cast<int32_t>(i));
        }
        auto block = std::make_shared<vectorized::Block>();
        block->insert({std::move(column), std::make_shared<vectorized::DataTypeInt32>(), "c1"});
        return block;
    }

    bool _original_wait_replay_wal_finish = false;
    int32_t _original_queue_mem_limit = 0;
};

TEST_F(GroupCommitMgrTest, CancelUsesEnqueuedBlockBytes) {
    auto all_block_queues_bytes = std::make_shared<std::atomic_size_t>(0);
    std::string label = "group_commit_cancel_test";
    LoadBlockQueue queue(UniqueId::gen_uid(), label, 1, 1, 1, all_block_queues_bytes, false,
                         std::numeric_limits<int64_t>::max(), std::numeric_limits<int64_t>::max());
    RuntimeState runtime_state;
    UniqueId load_id = UniqueId::gen_uid();

    auto first_block = create_block(1);
    auto second_block = create_block(2);
    const size_t enqueued_bytes = first_block->bytes() + second_block->bytes();
    ASSERT_GT(enqueued_bytes, 0);
    ASSERT_TRUE(queue.add_block(&runtime_state, first_block, false, load_id).ok());
    ASSERT_TRUE(queue.add_block(&runtime_state, second_block, false, load_id).ok());
    ASSERT_EQ(enqueued_bytes, all_block_queues_bytes->load());

    auto larger_first_block = create_block(1024);
    auto larger_second_block = create_block(2048);
    first_block->swap(*larger_first_block);
    second_block->swap(*larger_second_block);
    ASSERT_GT(first_block->bytes() + second_block->bytes(), enqueued_bytes);

    queue.cancel(Status::InternalError("cancel for test"));

    EXPECT_EQ(0, all_block_queues_bytes->load());
}

} // namespace doris
