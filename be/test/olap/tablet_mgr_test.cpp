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

#include <gen_cpp/AgentService_types.h>
#include <gen_cpp/Descriptors_types.h>
#include <gen_cpp/Types_types.h>
#include <gmock/gmock-actions.h>
#include <gmock/gmock-matchers.h>
#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>

#include <algorithm>
#include <future>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "bvar/variable.h"
#include "common/config.h"
#include "common/status.h"
#include "gtest/gtest_pred_impl.h"
#include "io/fs/local_file_system.h"
#include "olap/cumulative_compaction_policy.h"
#include "olap/cumulative_compaction_time_series_policy.h"
#include "olap/data_dir.h"
#include "olap/olap_common.h"
#include "olap/olap_define.h"
#include "olap/options.h"
#include "olap/rowset/beta_rowset.h"
#include "olap/rowset/rowset.h"
#include "olap/rowset/rowset_meta.h"
#include "olap/storage_engine.h"
#include "olap/tablet.h"
#include "olap/tablet_manager.h"
#include "olap/tablet_meta.h"
#include "olap/tablet_meta_manager.h"
#include "runtime/exec_env.h"
#include "util/debug_points.h"
#include "util/uid_util.h"

using ::testing::_;
using ::testing::Return;
using ::testing::SetArgPointee;
using std::string;

namespace doris {

class TabletMgrTest : public testing::Test {
public:
    virtual void SetUp() {
        _original_enable_debug_points = config::enable_debug_points;
        DebugPoints::instance()->remove("DataDir.gc_tablet_path.delete_directly_failed");
        DebugPoints::instance()->remove(
                "TabletManager._resolve_shutdown_tablet.remove_meta_failed");
        DebugPoints::instance()->remove("TabletManager._resolve_shutdown_tablet.save_meta_failed");

        _engine_data_path = "./be/test/olap/test_data/converter_test_data/tmp";
        auto st = io::global_local_filesystem()->delete_directory(_engine_data_path);
        ASSERT_TRUE(st.ok()) << st;
        st = io::global_local_filesystem()->create_directory(_engine_data_path);
        ASSERT_TRUE(st.ok()) << st;
        EXPECT_TRUE(
                io::global_local_filesystem()->create_directory(_engine_data_path + "/meta").ok());

        config::tablet_map_shard_size = 1;
        config::txn_map_shard_size = 1;
        config::txn_shard_size = 1;
        EngineOptions options;
        // won't open engine, options.path is needless
        options.backend_uid = UniqueId::gen_uid();
        auto engine = std::make_unique<StorageEngine>(options);
        ExecEnv::GetInstance()->set_storage_engine(std::move(engine));
        k_engine = &ExecEnv::GetInstance()->storage_engine().to_local();
        _data_dir = new DataDir(*k_engine, _engine_data_path, 1000000000);
        static_cast<void>(_data_dir->init());
        _tablet_mgr = k_engine->tablet_manager();
        _shutdown_backlog_base = _tablet_mgr->_shutdown_tablet_backlog_value();
    }

    virtual void TearDown() {
        _secondary_data_dir.reset();
        if (!_secondary_engine_data_path.empty()) {
            EXPECT_TRUE(io::global_local_filesystem()
                                ->delete_directory(_secondary_engine_data_path)
                                .ok());
        }
        if (_tablet_mgr != nullptr) {
            _tablet_mgr->_adjust_shutdown_tablet_backlog(
                    _shutdown_backlog_base - _tablet_mgr->_shutdown_tablet_backlog_value());
        }
        SAFE_DELETE(_data_dir);
        EXPECT_TRUE(io::global_local_filesystem()->delete_directory(_engine_data_path).ok());
        ExecEnv::GetInstance()->set_storage_engine(nullptr);
        _tablet_mgr = nullptr;
        config::compaction_num_per_round = 1;
        config::shutdown_tablet_sweep_round_budget = 200;
        config::shutdown_tablet_sweep_interval_ms = 1000;
        DebugPoints::instance()->remove("DataDir.gc_tablet_path.delete_directly_failed");
        DebugPoints::instance()->remove(
                "TabletManager._resolve_shutdown_tablet.remove_meta_failed");
        DebugPoints::instance()->remove("TabletManager._resolve_shutdown_tablet.save_meta_failed");
        config::enable_debug_points = _original_enable_debug_points;
    }

    DataDirSweepPolicy sweep_policy(
            TabletPathGcMode mode = TabletPathGcMode::DELETE_DIRECTLY,
            TabletPathGcReason reason = TabletPathGcReason::TRASH_RETENTION_DISABLED,
            bool eligible = true) const {
        DataDirSweepPolicy policy;
        policy.is_used = eligible;
        policy.effective_trash_expire_seconds =
                mode == TabletPathGcMode::DELETE_DIRECTLY ? 0 : 3600;
        policy.shutdown_tablet_gc.eligible = eligible;
        policy.shutdown_tablet_gc.mode = mode;
        policy.shutdown_tablet_gc.reason = reason;
        return policy;
    }

    DataDirSweepPolicies sweep_policies(
            TabletPathGcMode mode = TabletPathGcMode::DELETE_DIRECTLY,
            TabletPathGcReason reason = TabletPathGcReason::TRASH_RETENTION_DISABLED,
            bool eligible = true) const {
        DataDirSweepPolicies policies;
        policies.emplace(_data_dir, sweep_policy(mode, reason, eligible));
        return policies;
    }

    TabletSharedPtr create_test_tablet(int64_t tablet_id, int32_t schema_hash = 3333) {
        return create_test_tablet_on_data_dir(_data_dir, tablet_id, schema_hash);
    }

    TabletSharedPtr create_test_tablet_on_data_dir(DataDir* data_dir, int64_t tablet_id,
                                                   int32_t schema_hash = 3333) {
        TColumnType col_type;
        col_type.__set_type(TPrimitiveType::SMALLINT);
        TColumn column;
        column.__set_column_name("col1");
        column.__set_column_type(col_type);
        column.__set_is_key(true);

        TTabletSchema tablet_schema;
        tablet_schema.__set_short_key_column_count(1);
        tablet_schema.__set_schema_hash(schema_hash);
        tablet_schema.__set_keys_type(TKeysType::AGG_KEYS);
        tablet_schema.__set_storage_type(TStorageType::COLUMN);
        tablet_schema.__set_columns({column});

        TCreateTabletReq request;
        request.__set_tablet_schema(tablet_schema);
        request.__set_tablet_id(tablet_id);
        request.__set_version(2);

        RuntimeProfile profile("CreateTablet");
        Status status = _tablet_mgr->create_tablet(request, {data_dir}, &profile);
        EXPECT_TRUE(status.ok()) << status;
        return _tablet_mgr->get_tablet(tablet_id);
    }

    Status create_secondary_data_dir() {
        _secondary_engine_data_path = _engine_data_path + "_secondary";
        RETURN_IF_ERROR(
                io::global_local_filesystem()->delete_directory(_secondary_engine_data_path));
        RETURN_IF_ERROR(
                io::global_local_filesystem()->create_directory(_secondary_engine_data_path));
        RETURN_IF_ERROR(io::global_local_filesystem()->create_directory(
                _secondary_engine_data_path + "/meta"));
        _secondary_data_dir =
                std::make_unique<DataDir>(*k_engine, _secondary_engine_data_path, 1000000000);
        return _secondary_data_dir->init();
    }

    DataDir* secondary_data_dir() const { return _secondary_data_dir.get(); }

    int64_t metric_value(const std::string& name) const {
        return std::stoll(bvar::Variable::describe_exposed(name));
    }

    // Build a shared_ptr control block without constructing a real Tablet instance.
    // The returned pointer is only valid for ownership and use_count() assertions.
    // Tests must never dereference the Tablet* value behind this aliasing shared_ptr.
    TabletSharedPtr create_mock_shutdown_tablet() {
        auto owner = std::make_shared<int>(1);
        return TabletSharedPtr(owner, reinterpret_cast<Tablet*>(owner.get()));
    }

    // Replace the shutdown tablet queue with the provided test entries.
    void reset_shutdown_tablets(const std::vector<TabletSharedPtr>& tablets) {
        std::lock_guard<std::shared_mutex> wrlock(_tablet_mgr->_shutdown_tablets_lock);
        const int64_t old_size = _tablet_mgr->_shutdown_tablets.size();
        _tablet_mgr->_shutdown_tablets.clear();
        for (const auto& tablet : tablets) {
            _tablet_mgr->_shutdown_tablets.push_back(tablet);
        }
        // Keep the backlog metric aligned with direct queue mutations in unit tests.
        _tablet_mgr->_adjust_shutdown_tablet_backlog(static_cast<int64_t>(tablets.size()) -
                                                     old_size);
    }

    // Snapshot the current shutdown tablet raw pointers for order assertions.
    std::vector<Tablet*> list_shutdown_tablet_ptrs() {
        std::shared_lock<std::shared_mutex> rdlock(_tablet_mgr->_shutdown_tablets_lock);
        std::vector<Tablet*> tablets;
        tablets.reserve(_tablet_mgr->_shutdown_tablets.size());
        for (const auto& tablet : _tablet_mgr->_shutdown_tablets) {
            tablets.push_back(tablet.get());
        }
        return tablets;
    }

    // Read the current shutdown tablet count under the queue lock.
    size_t shutdown_tablet_count() {
        std::shared_lock<std::shared_mutex> rdlock(_tablet_mgr->_shutdown_tablets_lock);
        return _tablet_mgr->_shutdown_tablets.size();
    }

    int64_t shutdown_tablet_backlog_value() {
        return _tablet_mgr->_shutdown_tablet_backlog_value();
    }

    int64_t shutdown_tablet_last_sweep_ms_value() {
        return _tablet_mgr->_shutdown_tablet_last_sweep_ms_value();
    }

    StorageEngine* k_engine;

private:
    DataDir* _data_dir = nullptr;
    std::unique_ptr<DataDir> _secondary_data_dir;
    std::string _engine_data_path;
    std::string _secondary_engine_data_path;
    TabletManager* _tablet_mgr = nullptr;
    bool _original_enable_debug_points = false;
    int64_t _shutdown_backlog_base = 0;
};

TEST_F(TabletMgrTest, CreateTablet) {
    TColumnType col_type;
    col_type.__set_type(TPrimitiveType::SMALLINT);
    TColumn col1;
    col1.__set_column_name("col1");
    col1.__set_column_type(col_type);
    col1.__set_is_key(true);
    std::vector<TColumn> cols;
    cols.push_back(col1);
    TTabletSchema tablet_schema;
    tablet_schema.__set_short_key_column_count(1);
    tablet_schema.__set_schema_hash(3333);
    tablet_schema.__set_keys_type(TKeysType::AGG_KEYS);
    tablet_schema.__set_storage_type(TStorageType::COLUMN);
    tablet_schema.__set_columns(cols);
    TCreateTabletReq create_tablet_req;
    create_tablet_req.__set_tablet_schema(tablet_schema);
    create_tablet_req.__set_tablet_id(111);
    create_tablet_req.__set_version(2);
    std::vector<DataDir*> data_dirs;
    data_dirs.push_back(_data_dir);
    RuntimeProfile profile("CreateTablet");
    Status create_st = _tablet_mgr->create_tablet(create_tablet_req, data_dirs, &profile);
    EXPECT_TRUE(create_st == Status::OK());
    TabletSharedPtr tablet = _tablet_mgr->get_tablet(111);
    EXPECT_TRUE(tablet != nullptr);
    // check dir exist
    bool dir_exist = false;
    EXPECT_TRUE(io::global_local_filesystem()->exists(tablet->tablet_path(), &dir_exist).ok());
    EXPECT_TRUE(dir_exist);
    // check meta has this tablet
    TabletMetaSharedPtr new_tablet_meta(new TabletMeta());
    Status check_meta_st = TabletMetaManager::get_meta(_data_dir, 111, 3333, new_tablet_meta);
    EXPECT_TRUE(check_meta_st == Status::OK());

    // retry create should be successfully
    create_st = _tablet_mgr->create_tablet(create_tablet_req, data_dirs, &profile);
    EXPECT_TRUE(create_st == Status::OK());

    Status drop_st = _tablet_mgr->drop_tablet(111, create_tablet_req.replica_id, false);
    EXPECT_TRUE(drop_st == Status::OK());
    tablet.reset();
    Status trash_st = _tablet_mgr->start_trash_sweep(sweep_policies());
    EXPECT_TRUE(trash_st == Status::OK());
}

TEST_F(TabletMgrTest, CreateTabletWithSequence) {
    std::vector<TColumn> cols;
    TColumn col1;
    col1.column_type.type = TPrimitiveType::SMALLINT;
    col1.__set_column_name("col1");
    col1.__set_is_key(true);
    cols.push_back(col1);

    TColumn col2;
    col2.column_type.type = TPrimitiveType::INT;
    col2.__set_column_name(SEQUENCE_COL);
    col2.__set_is_key(false);
    col2.__set_aggregation_type(TAggregationType::REPLACE);
    cols.push_back(col2);

    TColumn col3;
    col3.column_type.type = TPrimitiveType::INT;
    col3.__set_column_name("v1");
    col3.__set_is_key(false);
    col3.__set_aggregation_type(TAggregationType::REPLACE);
    cols.push_back(col3);

    RuntimeProfile profile("CreateTablet");
    TTabletSchema tablet_schema;
    tablet_schema.__set_short_key_column_count(1);
    tablet_schema.__set_schema_hash(3333);
    tablet_schema.__set_keys_type(TKeysType::UNIQUE_KEYS);
    tablet_schema.__set_storage_type(TStorageType::COLUMN);
    tablet_schema.__set_columns(cols);
    tablet_schema.__set_sequence_col_idx(1);
    TCreateTabletReq create_tablet_req;
    create_tablet_req.__set_tablet_schema(tablet_schema);
    create_tablet_req.__set_tablet_id(111);
    create_tablet_req.__set_version(2);
    std::vector<DataDir*> data_dirs;
    data_dirs.push_back(_data_dir);
    Status create_st = _tablet_mgr->create_tablet(create_tablet_req, data_dirs, &profile);
    EXPECT_TRUE(create_st == Status::OK());

    TabletSharedPtr tablet = _tablet_mgr->get_tablet(111);
    EXPECT_TRUE(tablet != nullptr);
    // check dir exist
    bool dir_exist = false;
    EXPECT_TRUE(io::global_local_filesystem()->exists(tablet->tablet_path(), &dir_exist).ok());
    EXPECT_TRUE(dir_exist);
    // check meta has this tablet
    TabletMetaSharedPtr new_tablet_meta(new TabletMeta());
    Status check_meta_st = TabletMetaManager::get_meta(_data_dir, 111, 3333, new_tablet_meta);
    EXPECT_TRUE(check_meta_st == Status::OK());

    Status drop_st = _tablet_mgr->drop_tablet(111, create_tablet_req.replica_id, false);
    EXPECT_TRUE(drop_st == Status::OK());
    tablet.reset();
    Status trash_st = _tablet_mgr->start_trash_sweep(sweep_policies());
    EXPECT_TRUE(trash_st == Status::OK());
}

TEST_F(TabletMgrTest, DropTablet) {
    RuntimeProfile profile("CreateTablet");
    TColumnType col_type;
    col_type.__set_type(TPrimitiveType::SMALLINT);
    TColumn col1;
    col1.__set_column_name("col1");
    col1.__set_column_type(col_type);
    col1.__set_is_key(true);
    std::vector<TColumn> cols;
    cols.push_back(col1);
    TTabletSchema tablet_schema;
    tablet_schema.__set_short_key_column_count(1);
    tablet_schema.__set_schema_hash(3333);
    tablet_schema.__set_keys_type(TKeysType::AGG_KEYS);
    tablet_schema.__set_storage_type(TStorageType::COLUMN);
    tablet_schema.__set_columns(cols);
    TCreateTabletReq create_tablet_req;
    create_tablet_req.__set_tablet_schema(tablet_schema);
    create_tablet_req.__set_tablet_id(111);
    create_tablet_req.__set_version(2);
    std::vector<DataDir*> data_dirs;
    data_dirs.push_back(_data_dir);
    Status create_st = _tablet_mgr->create_tablet(create_tablet_req, data_dirs, &profile);
    EXPECT_TRUE(create_st == Status::OK());
    TabletSharedPtr tablet = _tablet_mgr->get_tablet(111);
    EXPECT_TRUE(tablet != nullptr);

    // drop unexist tablet will be success
    Status drop_st = _tablet_mgr->drop_tablet(1121, create_tablet_req.replica_id, false);
    EXPECT_TRUE(drop_st == Status::OK());
    tablet = _tablet_mgr->get_tablet(111);
    EXPECT_TRUE(tablet != nullptr);

    // drop exist tablet will be success
    drop_st = _tablet_mgr->drop_tablet(111, create_tablet_req.replica_id, false);
    EXPECT_TRUE(drop_st == Status::OK());
    tablet = _tablet_mgr->get_tablet(111);
    EXPECT_TRUE(tablet == nullptr);
    tablet = _tablet_mgr->get_tablet(111, true);
    EXPECT_TRUE(tablet != nullptr);

    // check dir exist
    std::string tablet_path = tablet->tablet_path();
    bool dir_exist = false;
    EXPECT_TRUE(io::global_local_filesystem()->exists(tablet_path, &dir_exist).ok());
    EXPECT_TRUE(dir_exist);

    // do trash sweep, tablet will not be garbage collected
    // because tablet ptr referenced it
    Status trash_st = _tablet_mgr->start_trash_sweep(sweep_policies());
    EXPECT_TRUE(trash_st == Status::OK());
    tablet = _tablet_mgr->get_tablet(111, true);
    EXPECT_TRUE(tablet != nullptr);
    EXPECT_TRUE(io::global_local_filesystem()->exists(tablet_path, &dir_exist).ok());
    EXPECT_TRUE(dir_exist);

    // reset tablet ptr
    tablet.reset();
    trash_st = _tablet_mgr->start_trash_sweep(sweep_policies());
    EXPECT_TRUE(trash_st == Status::OK());
    tablet = _tablet_mgr->get_tablet(111, true);
    EXPECT_TRUE(tablet == nullptr);
    EXPECT_TRUE(io::global_local_filesystem()->exists(tablet_path, &dir_exist).ok());
    EXPECT_FALSE(dir_exist);
}

TEST_F(TabletMgrTest, ShutdownTabletMovesToTrashWithRetentionPolicy) {
    constexpr int64_t tablet_id = 201;
    auto tablet = create_test_tablet(tablet_id);
    ASSERT_NE(tablet, nullptr);
    const std::string tablet_path = tablet->tablet_path();

    Status status = _tablet_mgr->drop_tablet(tablet_id, 0, false);
    ASSERT_TRUE(status.ok()) << status;
    tablet.reset();

    status = _tablet_mgr->start_trash_sweep(
            sweep_policies(TabletPathGcMode::MOVE_TO_TRASH, TabletPathGcReason::NORMAL_RETENTION));
    ASSERT_TRUE(status.ok()) << status;

    bool exists = true;
    ASSERT_TRUE(io::global_local_filesystem()->exists(tablet_path, &exists).ok());
    EXPECT_FALSE(exists);
    EXPECT_EQ(_tablet_mgr->get_tablet(tablet_id, true), nullptr);

    std::vector<std::string> trash_paths;
    _data_dir->find_tablet_in_trash(tablet_id, &trash_paths);
    EXPECT_EQ(trash_paths.size(), 1);
}

TEST_F(TabletMgrTest, ShutdownTabletDeletesDirectlyWithSweepPolicy) {
    constexpr int64_t tablet_id = 202;
    auto tablet = create_test_tablet(tablet_id);
    ASSERT_NE(tablet, nullptr);
    const std::string tablet_path = tablet->tablet_path();

    Status status = _tablet_mgr->drop_tablet(tablet_id, 0, false);
    ASSERT_TRUE(status.ok()) << status;
    tablet.reset();

    status = _tablet_mgr->start_trash_sweep(sweep_policies(
            TabletPathGcMode::DELETE_DIRECTLY, TabletPathGcReason::HIGH_DISK_WATERMARK));
    ASSERT_TRUE(status.ok()) << status;

    bool exists = true;
    ASSERT_TRUE(io::global_local_filesystem()->exists(tablet_path, &exists).ok());
    EXPECT_FALSE(exists);
    EXPECT_EQ(_tablet_mgr->get_tablet(tablet_id, true), nullptr);

    std::vector<std::string> trash_paths;
    _data_dir->find_tablet_in_trash(tablet_id, &trash_paths);
    EXPECT_TRUE(trash_paths.empty());
}

TEST_F(TabletMgrTest, ShutdownTabletsUseDifferentDataDirPoliciesInSameSweep) {
    Status status = create_secondary_data_dir();
    ASSERT_TRUE(status.ok()) << status;
    DataDir* secondary_data_dir = this->secondary_data_dir();
    ASSERT_NE(secondary_data_dir, nullptr);

    constexpr int64_t direct_delete_tablet_id = 203;
    auto direct_delete_tablet = create_test_tablet(direct_delete_tablet_id);
    ASSERT_NE(direct_delete_tablet, nullptr);
    const std::string direct_delete_path = direct_delete_tablet->tablet_path();

    constexpr int64_t move_to_trash_tablet_id = 204;
    auto move_to_trash_tablet =
            create_test_tablet_on_data_dir(secondary_data_dir, move_to_trash_tablet_id);
    ASSERT_NE(move_to_trash_tablet, nullptr);
    const std::string move_to_trash_path = move_to_trash_tablet->tablet_path();

    status = _tablet_mgr->drop_tablet(direct_delete_tablet_id, 0, false);
    ASSERT_TRUE(status.ok()) << status;
    status = _tablet_mgr->drop_tablet(move_to_trash_tablet_id, 0, false);
    ASSERT_TRUE(status.ok()) << status;
    direct_delete_tablet.reset();
    move_to_trash_tablet.reset();

    auto policies = sweep_policies(TabletPathGcMode::DELETE_DIRECTLY,
                                   TabletPathGcReason::HIGH_DISK_WATERMARK);
    auto [_, inserted] = policies.emplace(
            secondary_data_dir,
            sweep_policy(TabletPathGcMode::MOVE_TO_TRASH, TabletPathGcReason::NORMAL_RETENTION));
    ASSERT_TRUE(inserted);

    status = _tablet_mgr->start_trash_sweep(policies);
    ASSERT_TRUE(status.ok()) << status;

    bool exists = true;
    ASSERT_TRUE(io::global_local_filesystem()->exists(direct_delete_path, &exists).ok());
    EXPECT_FALSE(exists);
    ASSERT_TRUE(io::global_local_filesystem()->exists(move_to_trash_path, &exists).ok());
    EXPECT_FALSE(exists);
    EXPECT_EQ(_tablet_mgr->get_tablet(direct_delete_tablet_id, true), nullptr);
    EXPECT_EQ(_tablet_mgr->get_tablet(move_to_trash_tablet_id, true), nullptr);

    std::vector<std::string> trash_paths;
    _data_dir->find_tablet_in_trash(direct_delete_tablet_id, &trash_paths);
    EXPECT_TRUE(trash_paths.empty());
    secondary_data_dir->find_tablet_in_trash(move_to_trash_tablet_id, &trash_paths);
    EXPECT_EQ(trash_paths.size(), 1);
}

TEST_F(TabletMgrTest, ShutdownTabletOnUnusedDataDirRemainsQueued) {
    constexpr int64_t tablet_id = 205;
    auto tablet = create_test_tablet(tablet_id);
    ASSERT_NE(tablet, nullptr);
    const std::string tablet_path = tablet->tablet_path();

    Status status = _tablet_mgr->drop_tablet(tablet_id, 0, false);
    ASSERT_TRUE(status.ok()) << status;
    tablet.reset();
    const int64_t deferred_backlog = shutdown_tablet_backlog_value();

    status = _tablet_mgr->start_trash_sweep(sweep_policies(
            TabletPathGcMode::MOVE_TO_TRASH, TabletPathGcReason::UNUSED_DATA_DIR, false));
    ASSERT_TRUE(status.ok()) << status;
    EXPECT_EQ(shutdown_tablet_backlog_value(), deferred_backlog);
    EXPECT_EQ(metric_value("shutdown_tablet_last_round_resolved"), 0);
    EXPECT_EQ(metric_value("shutdown_tablet_last_round_move_failed_attempts"), 0);

    tablet = _tablet_mgr->get_tablet(tablet_id, true);
    ASSERT_NE(tablet, nullptr);
    bool exists = false;
    ASSERT_TRUE(io::global_local_filesystem()->exists(tablet_path, &exists).ok());
    EXPECT_TRUE(exists);

    tablet.reset();
    status = _tablet_mgr->start_trash_sweep(sweep_policies());
    ASSERT_TRUE(status.ok()) << status;
    EXPECT_EQ(_tablet_mgr->get_tablet(tablet_id, true), nullptr);
    EXPECT_EQ(shutdown_tablet_backlog_value(), deferred_backlog - 1);
}

TEST_F(TabletMgrTest, ShutdownTabletFailureIsRequeued) {
    constexpr int64_t tablet_id = 206;
    auto tablet = create_test_tablet(tablet_id);
    ASSERT_NE(tablet, nullptr);
    const std::string tablet_path = tablet->tablet_path();

    Status status = _tablet_mgr->drop_tablet(tablet_id, 0, false);
    ASSERT_TRUE(status.ok()) << status;
    tablet.reset();

    Status transition_status;
    std::promise<void> transition_registered;
    std::promise<void> release_transition;
    auto transition_registered_future = transition_registered.get_future();
    auto release_future = release_transition.get_future();
    std::thread transition_holder([&] {
        transition_status = _tablet_mgr->register_transition_tablet(tablet_id, "test transition");
        transition_registered.set_value();
        release_future.wait();
        if (transition_status.ok()) {
            _tablet_mgr->unregister_transition_tablet(tablet_id, "test transition");
        }
    });

    transition_registered_future.wait();
    status = _tablet_mgr->start_trash_sweep(sweep_policies());
    release_transition.set_value();
    transition_holder.join();
    ASSERT_TRUE(transition_status.ok()) << transition_status;
    ASSERT_TRUE(status.ok()) << status;

    tablet = _tablet_mgr->get_tablet(tablet_id, true);
    ASSERT_NE(tablet, nullptr);
    bool exists = false;
    ASSERT_TRUE(io::global_local_filesystem()->exists(tablet_path, &exists).ok());
    EXPECT_TRUE(exists);

    tablet.reset();
    status = _tablet_mgr->start_trash_sweep(sweep_policies());
    ASSERT_TRUE(status.ok()) << status;
    EXPECT_EQ(_tablet_mgr->get_tablet(tablet_id, true), nullptr);
}

TEST_F(TabletMgrTest, ShutdownTabletDirectDeleteFailureIsRequeued) {
    constexpr int64_t tablet_id = 207;
    auto tablet = create_test_tablet(tablet_id);
    ASSERT_NE(tablet, nullptr);
    const std::string tablet_path = tablet->tablet_path();

    Status status = _tablet_mgr->drop_tablet(tablet_id, 0, false);
    ASSERT_TRUE(status.ok()) << status;
    tablet.reset();

    const int64_t attempts_before = metric_value("shutdown_tablet_direct_delete_attempts_total");
    const int64_t success_before = metric_value("shutdown_tablet_direct_delete_success_total");
    const int64_t failed_attempts_before =
            metric_value("shutdown_tablet_direct_delete_failed_attempts_total");
    config::enable_debug_points = true;
    DebugPoints::instance()->add("DataDir.gc_tablet_path.delete_directly_failed");

    status = _tablet_mgr->start_trash_sweep(sweep_policies());
    ASSERT_TRUE(status.ok()) << status;

    EXPECT_EQ(metric_value("shutdown_tablet_direct_delete_attempts_total"), attempts_before + 1);
    EXPECT_EQ(metric_value("shutdown_tablet_direct_delete_success_total"), success_before);
    EXPECT_EQ(metric_value("shutdown_tablet_direct_delete_failed_attempts_total"),
              failed_attempts_before + 1);

    tablet = _tablet_mgr->get_tablet(tablet_id, true);
    ASSERT_NE(tablet, nullptr);
    bool exists = false;
    ASSERT_TRUE(io::global_local_filesystem()->exists(tablet_path, &exists).ok());
    EXPECT_TRUE(exists);
    TabletMetaSharedPtr tablet_meta(new TabletMeta());
    status = TabletMetaManager::get_meta(_data_dir, tablet_id, tablet->schema_hash(), tablet_meta);
    EXPECT_TRUE(status.ok()) << status;

    DebugPoints::instance()->remove("DataDir.gc_tablet_path.delete_directly_failed");
    tablet.reset();
    status = _tablet_mgr->start_trash_sweep(sweep_policies());
    ASSERT_TRUE(status.ok()) << status;
    EXPECT_EQ(_tablet_mgr->get_tablet(tablet_id, true), nullptr);
    ASSERT_TRUE(io::global_local_filesystem()->exists(tablet_path, &exists).ok());
    EXPECT_FALSE(exists);
}

TEST_F(TabletMgrTest, ShutdownTabletMetaDeleteFailureIsRetriedAfterPathDeletion) {
    constexpr int64_t tablet_id = 208;
    auto tablet = create_test_tablet(tablet_id);
    ASSERT_NE(tablet, nullptr);
    const int32_t schema_hash = tablet->schema_hash();
    const std::string tablet_path = tablet->tablet_path();

    Status status = _tablet_mgr->drop_tablet(tablet_id, 0, false);
    ASSERT_TRUE(status.ok()) << status;
    tablet.reset();

    const int64_t attempts_before = metric_value("shutdown_tablet_direct_delete_attempts_total");
    const int64_t success_before = metric_value("shutdown_tablet_direct_delete_success_total");
    config::enable_debug_points = true;
    DebugPoints::instance()->add("TabletManager._resolve_shutdown_tablet.remove_meta_failed");

    status = _tablet_mgr->start_trash_sweep(sweep_policies());
    ASSERT_TRUE(status.ok()) << status;

    EXPECT_EQ(metric_value("shutdown_tablet_direct_delete_attempts_total"), attempts_before + 1);
    EXPECT_EQ(metric_value("shutdown_tablet_direct_delete_success_total"), success_before + 1);
    bool exists = true;
    ASSERT_TRUE(io::global_local_filesystem()->exists(tablet_path, &exists).ok());
    EXPECT_FALSE(exists);

    tablet = _tablet_mgr->get_tablet(tablet_id, true);
    ASSERT_NE(tablet, nullptr);
    TabletMetaSharedPtr tablet_meta(new TabletMeta());
    status = TabletMetaManager::get_meta(_data_dir, tablet_id, schema_hash, tablet_meta);
    EXPECT_TRUE(status.ok()) << status;

    DebugPoints::instance()->remove("TabletManager._resolve_shutdown_tablet.remove_meta_failed");
    tablet.reset();
    status = _tablet_mgr->start_trash_sweep(sweep_policies());
    ASSERT_TRUE(status.ok()) << status;
    EXPECT_EQ(_tablet_mgr->get_tablet(tablet_id, true), nullptr);
    EXPECT_EQ(metric_value("shutdown_tablet_direct_delete_attempts_total"), attempts_before + 1);
    EXPECT_EQ(metric_value("shutdown_tablet_direct_delete_success_total"), success_before + 1);

    status = TabletMetaManager::get_meta(_data_dir, tablet_id, schema_hash, tablet_meta);
    EXPECT_TRUE(status.is<ErrorCode::META_KEY_NOT_FOUND>()) << status;
}

TEST_F(TabletMgrTest, ShutdownTabletIntentionalSkipDoesNotCountDirectDeleteSuccess) {
    constexpr int64_t tablet_id = 209;
    auto tablet = create_test_tablet(tablet_id);
    ASSERT_NE(tablet, nullptr);
    const int32_t schema_hash = tablet->schema_hash();
    const std::string tablet_path = tablet->tablet_path();
    std::string running_tablet_meta;
    tablet->tablet_meta()->serialize(&running_tablet_meta);

    Status status = _tablet_mgr->drop_tablet(tablet_id, 0, false);
    ASSERT_TRUE(status.ok()) << status;
    status = _tablet_mgr->load_tablet_from_meta(_data_dir, tablet_id, schema_hash,
                                                running_tablet_meta, true, true, false, true);
    ASSERT_TRUE(status.ok()) << status;
    tablet.reset();

    const int64_t attempts_before = metric_value("shutdown_tablet_direct_delete_attempts_total");
    const int64_t success_before = metric_value("shutdown_tablet_direct_delete_success_total");
    status = _tablet_mgr->start_trash_sweep(sweep_policies());
    ASSERT_TRUE(status.ok()) << status;

    EXPECT_EQ(metric_value("shutdown_tablet_direct_delete_attempts_total"), attempts_before);
    EXPECT_EQ(metric_value("shutdown_tablet_direct_delete_success_total"), success_before);
    tablet = _tablet_mgr->get_tablet(tablet_id);
    ASSERT_NE(tablet, nullptr);
    EXPECT_EQ(tablet->tablet_path(), tablet_path);
    bool exists = false;
    ASSERT_TRUE(io::global_local_filesystem()->exists(tablet_path, &exists).ok());
    EXPECT_TRUE(exists);
}

TEST_F(TabletMgrTest, ShutdownTabletMoveToTrashSaveMetaFailureIsRequeued) {
    constexpr int64_t tablet_id = 210;
    auto tablet = create_test_tablet(tablet_id);
    ASSERT_NE(tablet, nullptr);
    const std::string tablet_path = tablet->tablet_path();

    Status status = _tablet_mgr->drop_tablet(tablet_id, 0, false);
    ASSERT_TRUE(status.ok()) << status;
    tablet.reset();

    // A header-save failure must NOT delete the tablet: MOVE_TO_TRASH must remain
    // failure-safe and keep the tablet queued for retry instead of losing data.
    config::enable_debug_points = true;
    DebugPoints::instance()->add("TabletManager._resolve_shutdown_tablet.save_meta_failed");

    status = _tablet_mgr->start_trash_sweep(
            sweep_policies(TabletPathGcMode::MOVE_TO_TRASH, TabletPathGcReason::NORMAL_RETENTION));
    ASSERT_TRUE(status.ok()) << status;

    tablet = _tablet_mgr->get_tablet(tablet_id, true);
    ASSERT_NE(tablet, nullptr);
    bool exists = false;
    ASSERT_TRUE(io::global_local_filesystem()->exists(tablet_path, &exists).ok());
    EXPECT_TRUE(exists);

    DebugPoints::instance()->remove("TabletManager._resolve_shutdown_tablet.save_meta_failed");
    tablet.reset();
    // Resolve normally so the tablet is reclaimed and not leaked across tests.
    status = _tablet_mgr->start_trash_sweep(
            sweep_policies(TabletPathGcMode::MOVE_TO_TRASH, TabletPathGcReason::NORMAL_RETENTION));
    ASSERT_TRUE(status.ok()) << status;
    EXPECT_EQ(_tablet_mgr->get_tablet(tablet_id, true), nullptr);
}

TEST_F(TabletMgrTest, ShutdownTabletDirectDeleteImmuneToHeaderSaveFailure) {
    constexpr int64_t tablet_id = 211;
    auto tablet = create_test_tablet(tablet_id);
    ASSERT_NE(tablet, nullptr);
    const std::string tablet_path = tablet->tablet_path();

    Status status = _tablet_mgr->drop_tablet(tablet_id, 0, false);
    ASSERT_TRUE(status.ok()) << status;
    tablet.reset();

    // The same header-save failure that blocks MOVE_TO_TRASH must NOT block direct
    // deletion: direct mode skips the snapshot write, so a full/RO disk cannot stall
    // the urgent reclamation path. This is the regression guard for the P1 review.
    const int64_t attempts_before = metric_value("shutdown_tablet_direct_delete_attempts_total");
    const int64_t success_before = metric_value("shutdown_tablet_direct_delete_success_total");
    config::enable_debug_points = true;
    DebugPoints::instance()->add("TabletManager._resolve_shutdown_tablet.save_meta_failed");

    status = _tablet_mgr->start_trash_sweep(sweep_policies(
            TabletPathGcMode::DELETE_DIRECTLY, TabletPathGcReason::HIGH_DISK_WATERMARK));
    ASSERT_TRUE(status.ok()) << status;

    EXPECT_EQ(metric_value("shutdown_tablet_direct_delete_attempts_total"), attempts_before + 1);
    EXPECT_EQ(metric_value("shutdown_tablet_direct_delete_success_total"), success_before + 1);
    EXPECT_EQ(_tablet_mgr->get_tablet(tablet_id, true), nullptr);
    bool exists = true;
    ASSERT_TRUE(io::global_local_filesystem()->exists(tablet_path, &exists).ok());
    EXPECT_FALSE(exists);

    DebugPoints::instance()->remove("TabletManager._resolve_shutdown_tablet.save_meta_failed");
}

TEST_F(TabletMgrTest, DeleteShutdownTabletsRoundRespectsBudget) {
    struct TestCase {
        int round_budget;
        size_t expected_remaining;
    };

    const std::vector<TestCase> test_cases = {
            {1, 204},
            {199, 6},
            {200, 5},
            {201, 4},
    };
    for (const auto& test_case : test_cases) {
        // Rebuild the shutdown queue for each budget case.
        std::vector<TabletSharedPtr> tablets;
        for (int i = 0; i < 205; ++i) {
            tablets.push_back(create_mock_shutdown_tablet());
        }
        reset_shutdown_tablets(tablets);
        // Drop external references so movable tablets keep use_count() == 1 in the queue.
        tablets.clear();

        TabletManager::ShutdownTabletIter last_it;
        {
            std::shared_lock<std::shared_mutex> rdlock(_tablet_mgr->_shutdown_tablets_lock);
            last_it = _tablet_mgr->_shutdown_tablets.begin();
        }
        std::list<TabletSharedPtr> failed_tablets;
        const auto round_result = _tablet_mgr->_delete_shutdown_tablets_one_round(
                last_it, failed_tablets, [](const TabletSharedPtr&) { return true; },
                test_case.round_budget, 200, 200);

        EXPECT_TRUE(round_result.need_continue);
        EXPECT_EQ(round_result.resolved_count, test_case.round_budget);
        EXPECT_EQ(round_result.failed_count, 0);
        EXPECT_TRUE(failed_tablets.empty());
        EXPECT_EQ(shutdown_tablet_count(), test_case.expected_remaining);
    }
}

TEST_F(TabletMgrTest, DeleteShutdownTabletsRoundStopsAtExactQueueEnd) {
    const int64_t backlog_base = shutdown_tablet_backlog_value();
    std::vector<TabletSharedPtr> tablets;
    for (int i = 0; i < 3; ++i) {
        tablets.push_back(create_mock_shutdown_tablet());
    }
    reset_shutdown_tablets(tablets);
    tablets.clear();

    TabletManager::ShutdownTabletIter last_it;
    {
        std::shared_lock<std::shared_mutex> rdlock(_tablet_mgr->_shutdown_tablets_lock);
        last_it = _tablet_mgr->_shutdown_tablets.begin();
    }
    std::list<TabletSharedPtr> failed_tablets;
    const auto round_result = _tablet_mgr->_delete_shutdown_tablets_one_round(
            last_it, failed_tablets, [](const TabletSharedPtr&) { return true; }, 3, 200, 200);

    EXPECT_FALSE(round_result.need_continue);
    EXPECT_EQ(round_result.resolved_count, 3);
    EXPECT_EQ(round_result.failed_count, 0);
    EXPECT_TRUE(failed_tablets.empty());
    EXPECT_EQ(shutdown_tablet_count(), 0);
    EXPECT_EQ(shutdown_tablet_backlog_value(), backlog_base);
}

TEST_F(TabletMgrTest, SweepShutdownTabletsRecordsZeroDurationWhenEmpty) {
    Status sweep_st = _tablet_mgr->_sweep_shutdown_tablets(
            [](const TabletSharedPtr&) { return true; }, [](int) {});

    EXPECT_TRUE(sweep_st.ok());
    EXPECT_EQ(shutdown_tablet_last_sweep_ms_value(), 0);
}

TEST_F(TabletMgrTest, FetchShutdownTabletsRespectsScanChunkForReferencedTablets) {
    // Hold extra references for the first 200 entries so the sweep must skip them.
    std::vector<TabletSharedPtr> queue_tablets;
    std::vector<TabletSharedPtr> referenced_tablets;
    for (int i = 0; i < 200; ++i) {
        auto tablet = create_mock_shutdown_tablet();
        queue_tablets.push_back(tablet);
        referenced_tablets.push_back(tablet);
    }
    queue_tablets.push_back(create_mock_shutdown_tablet());
    Tablet* last_tablet_ptr = queue_tablets.back().get();
    reset_shutdown_tablets(queue_tablets);
    // Keep external references only for the entries that should be skipped by use_count().
    queue_tablets.clear();

    TabletManager::ShutdownTabletIter last_it;
    {
        std::shared_lock<std::shared_mutex> rdlock(_tablet_mgr->_shutdown_tablets_lock);
        last_it = _tablet_mgr->_shutdown_tablets.begin();
    }

    auto first_fetch = _tablet_mgr->_fetch_shutdown_tablets(last_it, 1, 200);
    EXPECT_TRUE(first_fetch.tablets.empty());
    EXPECT_FALSE(first_fetch.reached_end);
    EXPECT_EQ(first_fetch.scanned_count, 200);
    EXPECT_EQ(shutdown_tablet_count(), 201);

    auto second_fetch = _tablet_mgr->_fetch_shutdown_tablets(last_it, 1, 200);
    EXPECT_EQ(second_fetch.tablets.size(), 1);
    EXPECT_EQ(second_fetch.tablets[0].get(), last_tablet_ptr);
    EXPECT_TRUE(second_fetch.reached_end);
    EXPECT_EQ(second_fetch.scanned_count, 1);
    EXPECT_EQ(shutdown_tablet_count(), 200);
}

TEST_F(TabletMgrTest, SweepShutdownTabletsRequeuesFailedTablets) {
    // Keep the queue order unchanged when every move attempt fails.
    std::vector<TabletSharedPtr> tablets;
    for (int i = 0; i < 3; ++i) {
        tablets.push_back(create_mock_shutdown_tablet());
    }
    std::vector<Tablet*> tablet_ptrs;
    tablet_ptrs.reserve(tablets.size());
    for (const auto& tablet : tablets) {
        tablet_ptrs.push_back(tablet.get());
    }
    reset_shutdown_tablets(tablets);
    const int64_t expected_backlog = shutdown_tablet_backlog_value();
    // Drop external references so the sweep can fetch these queue entries.
    tablets.clear();

    int move_attempts = 0;
    Status sweep_st = _tablet_mgr->_sweep_shutdown_tablets(
            [&](const TabletSharedPtr&) {
                ++move_attempts;
                return false;
            },
            [](int) {});

    EXPECT_TRUE(sweep_st.ok());
    EXPECT_EQ(move_attempts, 3);
    EXPECT_EQ(list_shutdown_tablet_ptrs(), tablet_ptrs);
    EXPECT_EQ(shutdown_tablet_backlog_value(), expected_backlog);
}

TEST_F(TabletMgrTest, DeleteShutdownTabletsRoundKeepsFailuresOutOfBudget) {
    std::vector<TabletSharedPtr> tablets;
    for (int i = 0; i < 3; ++i) {
        tablets.push_back(create_mock_shutdown_tablet());
    }
    reset_shutdown_tablets(tablets);
    tablets.clear();

    TabletManager::ShutdownTabletIter last_it;
    {
        std::shared_lock<std::shared_mutex> rdlock(_tablet_mgr->_shutdown_tablets_lock);
        last_it = _tablet_mgr->_shutdown_tablets.begin();
    }

    int move_attempts = 0;
    std::list<TabletSharedPtr> failed_tablets;
    auto round_result = _tablet_mgr->_delete_shutdown_tablets_one_round(
            last_it, failed_tablets,
            [&](const TabletSharedPtr&) {
                ++move_attempts;
                return move_attempts != 1;
            },
            1, 200, 200);

    EXPECT_TRUE(round_result.need_continue);
    EXPECT_EQ(round_result.resolved_count, 1);
    EXPECT_EQ(round_result.failed_count, 1);
    EXPECT_EQ(move_attempts, 2);
    EXPECT_EQ(failed_tablets.size(), 1);
    EXPECT_EQ(shutdown_tablet_count(), 1);
}

TEST_F(TabletMgrTest, SweepShutdownTabletsReloadsBudgetEachRound) {
    // Update the config after the first round and verify the next round observes it.
    std::vector<TabletSharedPtr> tablets;
    for (int i = 0; i < 201; ++i) {
        tablets.push_back(create_mock_shutdown_tablet());
    }
    reset_shutdown_tablets(tablets);
    // Drop external references so the sweep can move these queue entries across rounds.
    tablets.clear();

    config::shutdown_tablet_sweep_round_budget = 200;
    int move_attempts = 0;
    int wait_count = 0;
    Status sweep_st = _tablet_mgr->_sweep_shutdown_tablets(
            [&](const TabletSharedPtr&) {
                ++move_attempts;
                return true;
            },
            [&](int) {
                ++wait_count;
                config::shutdown_tablet_sweep_round_budget = 500;
            });

    EXPECT_TRUE(sweep_st.ok());
    EXPECT_EQ(move_attempts, 201);
    EXPECT_EQ(wait_count, 1);
    EXPECT_EQ(shutdown_tablet_count(), 0);
}

TEST_F(TabletMgrTest, ShutdownTabletSweepConfigsAcceptInvalidThenValidUpdates) {
    Status status = config::set_config("shutdown_tablet_sweep_round_budget", "199", false, false);
    ASSERT_TRUE(status.ok()) << status;
    EXPECT_EQ(config::shutdown_tablet_sweep_round_budget, 199);

    status = config::set_config("shutdown_tablet_sweep_round_budget", "200", false, false);
    ASSERT_TRUE(status.ok()) << status;
    EXPECT_EQ(config::shutdown_tablet_sweep_round_budget, 200);

    status = config::set_config("shutdown_tablet_sweep_interval_ms", "-1", false, false);
    ASSERT_TRUE(status.ok()) << status;
    EXPECT_EQ(config::shutdown_tablet_sweep_interval_ms, -1);

    status = config::set_config("shutdown_tablet_sweep_interval_ms", "50", false, false);
    ASSERT_TRUE(status.ok()) << status;
    EXPECT_EQ(config::shutdown_tablet_sweep_interval_ms, 50);
}

TEST_F(TabletMgrTest, SweepShutdownTabletsFallsBackForInvalidConfigs) {
    const int64_t backlog_base = shutdown_tablet_backlog_value();
    std::vector<TabletSharedPtr> tablets;
    for (int i = 0; i < 201; ++i) {
        tablets.push_back(create_mock_shutdown_tablet());
    }
    reset_shutdown_tablets(tablets);
    tablets.clear();

    ASSERT_TRUE(config::set_config("shutdown_tablet_sweep_round_budget", "199", false, false).ok());
    ASSERT_TRUE(config::set_config("shutdown_tablet_sweep_interval_ms", "-1", false, false).ok());
    int move_attempts = 0;
    std::vector<int> wait_intervals;
    Status sweep_st = _tablet_mgr->_sweep_shutdown_tablets(
            [&](const TabletSharedPtr&) {
                ++move_attempts;
                return true;
            },
            [&](int interval_ms) { wait_intervals.push_back(interval_ms); });

    EXPECT_TRUE(sweep_st.ok());
    EXPECT_EQ(move_attempts, 201);
    EXPECT_EQ(wait_intervals, std::vector<int>({1000}));
    EXPECT_EQ(shutdown_tablet_count(), 0);
    EXPECT_EQ(shutdown_tablet_backlog_value(), backlog_base);
}

TEST_F(TabletMgrTest, SweepShutdownTabletsReloadsIntervalEachRound) {
    std::vector<TabletSharedPtr> tablets;
    for (int i = 0; i < 401; ++i) {
        tablets.push_back(create_mock_shutdown_tablet());
    }
    reset_shutdown_tablets(tablets);
    tablets.clear();

    config::shutdown_tablet_sweep_round_budget = 200;
    config::shutdown_tablet_sweep_interval_ms = 10;
    std::vector<int> wait_intervals;
    Status sweep_st =
            _tablet_mgr->_sweep_shutdown_tablets([](const TabletSharedPtr&) { return true; },
                                                 [&](int interval_ms) {
                                                     wait_intervals.push_back(interval_ms);
                                                     config::shutdown_tablet_sweep_interval_ms = 20;
                                                 });

    EXPECT_TRUE(sweep_st.ok());
    EXPECT_EQ(wait_intervals, std::vector<int>({10, 20}));
    EXPECT_EQ(shutdown_tablet_count(), 0);
}

TEST_F(TabletMgrTest, SweepShutdownTabletsKeepsNewEntriesAheadOfFailedRetries) {
    std::vector<TabletSharedPtr> tablets;
    for (int i = 0; i < 201; ++i) {
        tablets.push_back(create_mock_shutdown_tablet());
    }
    Tablet* failed_tablet_ptr = tablets.back().get();
    TabletSharedPtr referenced_new_tablet = create_mock_shutdown_tablet();
    Tablet* new_tablet_ptr = referenced_new_tablet.get();
    reset_shutdown_tablets(tablets);
    tablets.clear();

    config::shutdown_tablet_sweep_round_budget = 200;
    int wait_count = 0;
    int move_attempts = 0;
    Status sweep_st = _tablet_mgr->_sweep_shutdown_tablets(
            [&](const TabletSharedPtr&) {
                ++move_attempts;
                return move_attempts <= 200;
            },
            [&](int) {
                ++wait_count;
                _tablet_mgr->_enqueue_shutdown_tablet(referenced_new_tablet);
            });

    EXPECT_TRUE(sweep_st.ok());
    EXPECT_EQ(wait_count, 1);
    EXPECT_EQ(move_attempts, 201);
    EXPECT_EQ(list_shutdown_tablet_ptrs(),
              std::vector<Tablet*>({new_tablet_ptr, failed_tablet_ptr}));
}

TEST_F(TabletMgrTest, SweepShutdownTabletsSupportsZeroInterval) {
    std::vector<TabletSharedPtr> tablets;
    for (int i = 0; i < 201; ++i) {
        tablets.push_back(create_mock_shutdown_tablet());
    }
    reset_shutdown_tablets(tablets);
    tablets.clear();

    config::shutdown_tablet_sweep_round_budget = 200;
    config::shutdown_tablet_sweep_interval_ms = 0;
    std::vector<int> wait_intervals;
    Status sweep_st = _tablet_mgr->_sweep_shutdown_tablets(
            [](const TabletSharedPtr&) { return true; },
            [&](int interval_ms) { wait_intervals.push_back(interval_ms); });

    EXPECT_TRUE(sweep_st.ok());
    EXPECT_EQ(wait_intervals, std::vector<int>({0}));
    EXPECT_EQ(shutdown_tablet_count(), 0);
}

TEST_F(TabletMgrTest, SweepShutdownTabletsFallsBackForConfigsAboveMaxAndRecovers) {
    std::vector<TabletSharedPtr> tablets;
    for (int i = 0; i < 401; ++i) {
        tablets.push_back(create_mock_shutdown_tablet());
    }
    reset_shutdown_tablets(tablets);
    tablets.clear();

    config::shutdown_tablet_sweep_round_budget = 501;
    config::shutdown_tablet_sweep_interval_ms = 1001;
    int move_attempts = 0;
    std::vector<int> wait_intervals;
    Status sweep_st = _tablet_mgr->_sweep_shutdown_tablets(
            [&](const TabletSharedPtr&) {
                ++move_attempts;
                return true;
            },
            [&](int interval_ms) {
                wait_intervals.push_back(interval_ms);
                if (wait_intervals.size() == 1) {
                    config::shutdown_tablet_sweep_round_budget = 200;
                    config::shutdown_tablet_sweep_interval_ms = 7;
                }
            });

    EXPECT_TRUE(sweep_st.ok());
    EXPECT_EQ(move_attempts, 401);
    EXPECT_EQ(wait_intervals, std::vector<int>({1000, 7}));
    EXPECT_EQ(shutdown_tablet_count(), 0);
}

TEST_F(TabletMgrTest, SweepShutdownTabletsAcceptsUpperBoundConfigs) {
    std::vector<TabletSharedPtr> tablets;
    for (int i = 0; i < 501; ++i) {
        tablets.push_back(create_mock_shutdown_tablet());
    }
    reset_shutdown_tablets(tablets);
    tablets.clear();

    config::shutdown_tablet_sweep_round_budget = 500;
    config::shutdown_tablet_sweep_interval_ms = 1000;
    int move_attempts = 0;
    std::vector<int> wait_intervals;
    Status sweep_st = _tablet_mgr->_sweep_shutdown_tablets(
            [&](const TabletSharedPtr&) {
                ++move_attempts;
                return true;
            },
            [&](int interval_ms) { wait_intervals.push_back(interval_ms); });

    EXPECT_TRUE(sweep_st.ok());
    EXPECT_EQ(move_attempts, 501);
    EXPECT_EQ(wait_intervals, std::vector<int>({1000}));
    EXPECT_EQ(shutdown_tablet_count(), 0);
}

TEST_F(TabletMgrTest, SweepShutdownTabletsRequeuesMixedFailureAtTail) {
    const int64_t backlog_base = shutdown_tablet_backlog_value();
    std::vector<TabletSharedPtr> tablets;
    for (int i = 0; i < 202; ++i) {
        tablets.push_back(create_mock_shutdown_tablet());
    }
    Tablet* failed_tablet_ptr = tablets.front().get();
    reset_shutdown_tablets(tablets);
    tablets.clear();

    config::shutdown_tablet_sweep_round_budget = 200;
    int move_attempts = 0;
    int wait_count = 0;
    TabletSharedPtr referenced_tablet;
    Tablet* referenced_tablet_ptr = nullptr;
    Status sweep_st = _tablet_mgr->_sweep_shutdown_tablets(
            [&](const TabletSharedPtr&) {
                ++move_attempts;
                return move_attempts != 1;
            },
            [&](int) {
                ++wait_count;
                if (wait_count == 1) {
                    referenced_tablet = create_mock_shutdown_tablet();
                    referenced_tablet_ptr = referenced_tablet.get();
                    _tablet_mgr->_enqueue_shutdown_tablet(referenced_tablet);
                }
            });

    EXPECT_TRUE(sweep_st.ok());
    EXPECT_EQ(move_attempts, 202);
    EXPECT_EQ(wait_count, 1);
    EXPECT_EQ(list_shutdown_tablet_ptrs(),
              std::vector<Tablet*>({referenced_tablet_ptr, failed_tablet_ptr}));
    EXPECT_EQ(shutdown_tablet_backlog_value(), backlog_base + 2);
}

TEST_F(TabletMgrTest, GetRowsetId) {
    // normal case
    {
        std::string path = _engine_data_path + "/data/0/15007/368169781";
        TTabletId tid;
        TSchemaHash schema_hash;
        EXPECT_TRUE(_tablet_mgr->get_tablet_id_and_schema_hash_from_path(path, &tid, &schema_hash));
        EXPECT_EQ(15007, tid);
        EXPECT_EQ(368169781, schema_hash);
    }
    {
        std::string path = _engine_data_path + "/data/0/15007/368169781/";
        TTabletId tid;
        TSchemaHash schema_hash;
        EXPECT_TRUE(_tablet_mgr->get_tablet_id_and_schema_hash_from_path(path, &tid, &schema_hash));
        EXPECT_EQ(15007, tid);
        EXPECT_EQ(368169781, schema_hash);
    }
    // normal case
    {
        std::string path =
                _engine_data_path +
                "/data/0/15007/368169781/020000000000000100000000000000020000000000000003_0_0.dat";
        TTabletId tid;
        TSchemaHash schema_hash;
        EXPECT_TRUE(_tablet_mgr->get_tablet_id_and_schema_hash_from_path(path, &tid, &schema_hash));
        EXPECT_EQ(15007, tid);
        EXPECT_EQ(368169781, schema_hash);

        RowsetId id;
        EXPECT_TRUE(_tablet_mgr->get_rowset_id_from_path(path, &id));
        EXPECT_EQ(2UL << 56 | 1, id.hi);
        EXPECT_EQ(2, id.mi);
        EXPECT_EQ(3, id.lo);
    }
    // empty tablet directory
    {
        std::string path = _engine_data_path + "/data/0/15007";
        TTabletId tid;
        TSchemaHash schema_hash;
        EXPECT_TRUE(_tablet_mgr->get_tablet_id_and_schema_hash_from_path(path, &tid, &schema_hash));
        EXPECT_EQ(15007, tid);
        EXPECT_EQ(0, schema_hash);

        RowsetId id;
        EXPECT_FALSE(_tablet_mgr->get_rowset_id_from_path(path, &id));
    }
    // empty tablet directory
    {
        std::string path = _engine_data_path + "/data/0/15007/";
        TTabletId tid;
        TSchemaHash schema_hash;
        EXPECT_TRUE(_tablet_mgr->get_tablet_id_and_schema_hash_from_path(path, &tid, &schema_hash));
        EXPECT_EQ(15007, tid);
        EXPECT_EQ(0, schema_hash);
    }
    // empty tablet directory
    {
        std::string path = _engine_data_path + "/data/0/15007abc";
        TTabletId tid;
        TSchemaHash schema_hash;
        EXPECT_FALSE(
                _tablet_mgr->get_tablet_id_and_schema_hash_from_path(path, &tid, &schema_hash));
    }
    // not match pattern
    {
        std::string path =
                _engine_data_path +
                "/data/0/15007/123abc/020000000000000100000000000000020000000000000003_0_0.dat";
        TTabletId tid;
        TSchemaHash schema_hash;
        EXPECT_FALSE(
                _tablet_mgr->get_tablet_id_and_schema_hash_from_path(path, &tid, &schema_hash));

        RowsetId id;
        EXPECT_FALSE(_tablet_mgr->get_rowset_id_from_path(path, &id));
    }
}

TEST_F(TabletMgrTest, FindTabletWithCompact) {
    auto create_tablet = [this](int64_t tablet_id, bool enable_single_compact, int rowset_size) {
        std::vector<TColumn> cols;
        TColumn col1;
        col1.column_type.type = TPrimitiveType::SMALLINT;
        col1.__set_column_name("col1");
        col1.__set_is_key(true);
        cols.push_back(col1);

        TColumn col2;
        col2.column_type.type = TPrimitiveType::INT;
        col2.__set_column_name(SEQUENCE_COL);
        col2.__set_is_key(false);
        col2.__set_aggregation_type(TAggregationType::REPLACE);
        cols.push_back(col2);

        TColumn col3;
        col3.column_type.type = TPrimitiveType::INT;
        col3.__set_column_name("v1");
        col3.__set_is_key(false);
        col3.__set_aggregation_type(TAggregationType::REPLACE);
        cols.push_back(col3);

        RuntimeProfile profile("CreateTablet");
        TTabletSchema tablet_schema;
        tablet_schema.__set_short_key_column_count(1);
        tablet_schema.__set_schema_hash(3333);
        tablet_schema.__set_keys_type(TKeysType::UNIQUE_KEYS);
        tablet_schema.__set_storage_type(TStorageType::COLUMN);
        tablet_schema.__set_columns(cols);
        tablet_schema.__set_sequence_col_idx(1);
        tablet_schema.__set_enable_single_replica_compaction(enable_single_compact);
        TCreateTabletReq create_tablet_req;
        create_tablet_req.__set_tablet_schema(tablet_schema);
        create_tablet_req.__set_tablet_id(tablet_id);
        create_tablet_req.__set_version(1);
        create_tablet_req.__set_replica_id(tablet_id * 10);
        std::vector<DataDir*> data_dirs;
        data_dirs.push_back(_data_dir);
        Status create_st = _tablet_mgr->create_tablet(create_tablet_req, data_dirs, &profile);
        ASSERT_TRUE(create_st.ok()) << create_st;

        TabletSharedPtr tablet = _tablet_mgr->get_tablet(tablet_id);
        ASSERT_TRUE(tablet);
        // check dir exist
        bool dir_exist = false;
        Status exist_st = io::global_local_filesystem()->exists(tablet->tablet_path(), &dir_exist);
        ASSERT_TRUE(exist_st.ok()) << exist_st;
        ASSERT_TRUE(dir_exist);
        // check meta has this tablet
        TabletMetaSharedPtr new_tablet_meta(new TabletMeta());
        Status check_meta_st =
                TabletMetaManager::get_meta(_data_dir, tablet_id, 3333, new_tablet_meta);
        ASSERT_TRUE(check_meta_st.ok()) << check_meta_st;
        // insert into rowset
        auto create_rowset = [=, this](int64_t start, int64 end) {
            auto rowset_meta = std::make_shared<RowsetMeta>();
            Version version(start, end);
            rowset_meta->set_version(version);
            rowset_meta->set_tablet_id(tablet->tablet_id());
            rowset_meta->set_tablet_uid(tablet->tablet_uid());
            rowset_meta->set_rowset_id(k_engine->next_rowset_id());
            return std::make_shared<BetaRowset>(tablet->tablet_schema(), std::move(rowset_meta),
                                                tablet->tablet_path());
        };
        auto st = tablet->init();
        ASSERT_TRUE(st.ok()) << st;
        for (int i = 2; i <= rowset_size; ++i) {
            auto rs = create_rowset(i, i);
            auto st = tablet->add_inc_rowset(rs);
            ASSERT_TRUE(st.ok()) << st;
        }
    };

    int rowset_size = 5;

    // create 10 tablets
    for (int64_t id = 1; id <= 10; ++id) {
        create_tablet(id, false, rowset_size++);
    }

    std::unordered_set<TabletSharedPtr> cumu_set;
    std::unordered_map<std::string_view, std::shared_ptr<CumulativeCompactionPolicy>>
            cumulative_compaction_policies;
    cumulative_compaction_policies[CUMULATIVE_SIZE_BASED_POLICY] =
            CumulativeCompactionPolicyFactory::create_cumulative_compaction_policy(
                    CUMULATIVE_SIZE_BASED_POLICY);
    cumulative_compaction_policies[CUMULATIVE_TIME_SERIES_POLICY] =
            CumulativeCompactionPolicyFactory::create_cumulative_compaction_policy(
                    CUMULATIVE_TIME_SERIES_POLICY);
    uint32_t score = 0;
    auto compact_tablets = _tablet_mgr->find_best_tablets_to_compaction(
            CompactionType::CUMULATIVE_COMPACTION, _data_dir, cumu_set, &score,
            cumulative_compaction_policies);
    ASSERT_EQ(compact_tablets.size(), 1);
    ASSERT_EQ(compact_tablets[0]->tablet_id(), 10);
    ASSERT_EQ(score, 13);

    // create 10 tablets enable single compact
    // 5 tablets do cumu compaction, 5 tablets do single compaction
    // if BE_TEST is defined, tablet_id % 2 == 0 means that tablet needs to do single compact
    for (int64_t id = 11; id <= 20; ++id) {
        create_tablet(id, true, rowset_size++);
    }

    compact_tablets = _tablet_mgr->find_best_tablets_to_compaction(
            CompactionType::CUMULATIVE_COMPACTION, _data_dir, cumu_set, &score,
            cumulative_compaction_policies);
    ASSERT_EQ(compact_tablets.size(), 2);
    ASSERT_EQ(compact_tablets[0]->tablet_id(), 19);
    ASSERT_EQ(compact_tablets[1]->tablet_id(), 20);
    ASSERT_EQ(score, 23);

    create_tablet(21, false, rowset_size++);

    compact_tablets = _tablet_mgr->find_best_tablets_to_compaction(
            CompactionType::CUMULATIVE_COMPACTION, _data_dir, cumu_set, &score,
            cumulative_compaction_policies);
    ASSERT_EQ(compact_tablets.size(), 1);
    ASSERT_EQ(compact_tablets[0]->tablet_id(), 21);
    ASSERT_EQ(score, 24);

    // drop all tablets
    for (int64_t id = 1; id <= 21; ++id) {
        Status drop_st = _tablet_mgr->drop_tablet(id, id * 10, false);
        ASSERT_TRUE(drop_st.ok()) << drop_st;
    }

    {
        k_engine->_compaction_num_per_round = 10;
        for (int64_t i = 1; i <= 100; ++i) {
            create_tablet(10000 + i, false, i);
        }

        compact_tablets = _tablet_mgr->find_best_tablets_to_compaction(
                CompactionType::CUMULATIVE_COMPACTION, _data_dir, cumu_set, &score,
                cumulative_compaction_policies);
        ASSERT_EQ(compact_tablets.size(), 10);
        int index = 0;
        for (auto t : compact_tablets) {
            ASSERT_EQ(t->tablet_id(), 10100 - index);
            ASSERT_EQ(t->calc_compaction_score(
                              CompactionType::CUMULATIVE_COMPACTION,
                              cumulative_compaction_policies[CUMULATIVE_SIZE_BASED_POLICY]),
                      100 - index - 1);
            index++;
        }
        k_engine->_compaction_num_per_round = 1;
        // drop all tablets
        for (int64_t id = 10001; id <= 10100; ++id) {
            Status drop_st = _tablet_mgr->drop_tablet(id, id * 10, false);
            ASSERT_TRUE(drop_st.ok()) << drop_st;
        }
    }

    {
        k_engine->_compaction_num_per_round = 10;
        for (int64_t i = 1; i <= 100; ++i) {
            create_tablet(20000 + i, false, i);
        }
        create_tablet(20102, true, 200);

        compact_tablets = _tablet_mgr->find_best_tablets_to_compaction(
                CompactionType::CUMULATIVE_COMPACTION, _data_dir, cumu_set, &score,
                cumulative_compaction_policies);
        ASSERT_EQ(compact_tablets.size(), 11);
        for (int i = 0; i < 10; ++i) {
            ASSERT_EQ(compact_tablets[i]->tablet_id(), 20100 - i);
            ASSERT_EQ(compact_tablets[i]->calc_compaction_score(
                              CompactionType::CUMULATIVE_COMPACTION,
                              cumulative_compaction_policies[CUMULATIVE_SIZE_BASED_POLICY]),
                      100 - i - 1);
        }
        ASSERT_EQ(compact_tablets[10]->tablet_id(), 20102);
        ASSERT_EQ(compact_tablets[10]->calc_compaction_score(
                          CompactionType::CUMULATIVE_COMPACTION,
                          cumulative_compaction_policies[CUMULATIVE_SIZE_BASED_POLICY]),
                  200 - 1);

        k_engine->_compaction_num_per_round = 1;
        // drop all tablets
        for (int64_t id = 20001; id <= 20100; ++id) {
            Status drop_st = _tablet_mgr->drop_tablet(id, id * 10, false);
            ASSERT_TRUE(drop_st.ok()) << drop_st;
        }

        Status drop_st = _tablet_mgr->drop_tablet(20102, 20102 * 10, false);
        ASSERT_TRUE(drop_st.ok()) << drop_st;
    }

    {
        k_engine->_compaction_num_per_round = 10;
        for (int64_t i = 1; i <= 5; ++i) {
            create_tablet(30000 + i, false, i + 5);
        }

        compact_tablets = _tablet_mgr->find_best_tablets_to_compaction(
                CompactionType::CUMULATIVE_COMPACTION, _data_dir, cumu_set, &score,
                cumulative_compaction_policies);
        ASSERT_EQ(compact_tablets.size(), 5);
        for (int i = 0; i < 5; ++i) {
            ASSERT_EQ(compact_tablets[i]->tablet_id(), 30000 + 5 - i);
            ASSERT_EQ(compact_tablets[i]->calc_compaction_score(
                              CompactionType::CUMULATIVE_COMPACTION,
                              cumulative_compaction_policies[CUMULATIVE_SIZE_BASED_POLICY]),
                      10 - i - 1);
        }

        k_engine->_compaction_num_per_round = 1;
        // drop all tablets
        for (int64_t id = 30001; id <= 30005; ++id) {
            Status drop_st = _tablet_mgr->drop_tablet(id, id * 10, false);
            ASSERT_TRUE(drop_st.ok()) << drop_st;
        }
    }

    Status trash_st = _tablet_mgr->start_trash_sweep(sweep_policies());
    ASSERT_TRUE(trash_st.ok()) << trash_st;
}

TEST_F(TabletMgrTest, LoadTabletFromMeta) {
    TTabletId tablet_id = 111;
    TSchemaHash schema_hash = 3333;
    TColumnType col_type;
    col_type.__set_type(TPrimitiveType::SMALLINT);
    TColumn col1;
    col1.__set_column_name("col1");
    col1.__set_column_type(col_type);
    col1.__set_is_key(true);
    std::vector<TColumn> cols;
    cols.push_back(col1);
    TTabletSchema tablet_schema;
    tablet_schema.__set_short_key_column_count(1);
    tablet_schema.__set_schema_hash(3333);
    tablet_schema.__set_keys_type(TKeysType::AGG_KEYS);
    tablet_schema.__set_storage_type(TStorageType::COLUMN);
    tablet_schema.__set_columns(cols);
    TCreateTabletReq create_tablet_req;
    create_tablet_req.__set_tablet_schema(tablet_schema);
    create_tablet_req.__set_tablet_id(111);
    create_tablet_req.__set_version(2);
    std::vector<DataDir*> data_dirs;
    data_dirs.push_back(_data_dir);
    RuntimeProfile profile("CreateTablet");
    Status create_st =
            k_engine->tablet_manager()->create_tablet(create_tablet_req, data_dirs, &profile);
    EXPECT_TRUE(create_st == Status::OK());
    TabletSharedPtr tablet = k_engine->tablet_manager()->get_tablet(111);
    EXPECT_TRUE(tablet != nullptr);

    std::string serialized_tablet_meta;
    tablet->tablet_meta()->serialize(&serialized_tablet_meta);
    bool update_meta = true;
    bool force = true;
    bool restore = false;
    bool check_path = true;
    Status st = _tablet_mgr->load_tablet_from_meta(_data_dir, tablet_id, schema_hash,
                                                   serialized_tablet_meta, update_meta, force,
                                                   restore, check_path);
    ASSERT_TRUE(st.ok()) << st.to_string();

    // After reload, the original tablet should not be allowed to save meta.
    ASSERT_FALSE(tablet->do_tablet_meta_checkpoint());
}

} // namespace doris
