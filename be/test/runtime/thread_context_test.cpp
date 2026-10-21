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

#include "runtime/thread_context.h"

#include <gtest/gtest.h>

#include <array>

namespace doris {
namespace {

struct ContextObservation {
    bool context_preserved = true;
    bool handles_balanced = true;
};

void* observe_bthread_context(void* arg) {
    auto* observation = static_cast<ContextObservation*>(arg);
    ScopedInitThreadContext scoped_context;
    auto* context = thread_context();
    const int handle_count = context->thread_local_handle_count;

    for (int i = 0; i < 16; ++i) {
        init_thread_context_btls_key();
        {
            ScopedInitThreadContext nested_context;
            observation->context_preserved &= thread_context() == context;
            observation->handles_balanced &= context->thread_local_handle_count == handle_count + 1;
            bthread_usleep(1000);
            observation->context_preserved &= thread_context() == context;
        }
        observation->handles_balanced &= context->thread_local_handle_count == handle_count;
    }
    return nullptr;
}

} // namespace

TEST(ThreadContextTest, ContextKeyReadyBeforeInternalService) {
    // The UT main creates a pthread context before constructing any PInternalService.
    // That entry point must initialize the bthread key too, as BE startup does.
    EXPECT_NE(0u, btls_key.version);
    EXPECT_NE(nullptr, thread_context());
}

TEST(ThreadContextTest, BthreadContextSurvivesConcurrentInitialization) {
    const auto initial_key = btls_key;
    constexpr size_t num_bthreads = 8;
    std::array<bthread_t, num_bthreads> threads {};
    std::array<ContextObservation, num_bthreads> observations;
    size_t started = 0;

    for (size_t i = 0; i < num_bthreads; ++i) {
        const int result = bthread_start_background(&threads[i], nullptr, observe_bthread_context,
                                                    &observations[i]);
        EXPECT_EQ(0, result);
        if (result != 0) {
            break;
        }
        ++started;
    }
    // Join already-started bthreads even if a later start failed: their arguments live on this stack.
    for (size_t i = 0; i < started; ++i) {
        EXPECT_EQ(0, bthread_join(threads[i], nullptr));
    }

    ASSERT_EQ(num_bthreads, started);
    EXPECT_EQ(initial_key.index, btls_key.index);
    EXPECT_EQ(initial_key.version, btls_key.version);
    for (const auto& observation : observations) {
        EXPECT_TRUE(observation.context_preserved);
        EXPECT_TRUE(observation.handles_balanced);
    }
}

} // namespace doris
