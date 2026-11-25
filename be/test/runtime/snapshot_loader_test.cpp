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

#include "runtime/snapshot_loader.h"

#include <fmt/core.h>
#include <gen_cpp/AgentService_types.h>
#include <gen_cpp/Descriptors_types.h>
#include <gen_cpp/Types_types.h>
#include <gen_cpp/internal_service.pb.h>
#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>
#include <gtest/gtest.h>
#include <gtest/gtest_pred_impl.h>

#include <boost/algorithm/string/replace.hpp>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <functional>
#include <iostream>
#include <string>
#include <system_error>
#include <utility>

#include "agent/task_worker_pool.h"
#include "common/config.h"
#include "common/object_pool.h"
#include "exec/tablet_info.h"
#include "http/action/download_action.h"
#include "http/ev_http_server.h"
#include "http/http_channel.h"
#include "http/http_handler.h"
#include "http/http_request.h"
#include "io/fs/file_reader.h"
#include "io/fs/file_writer.h"
#include "io/fs/local_file_system.h"
#include "io/fs/remote_file_system.h"
#include "olap/data_dir.h"
#include "olap/delta_writer.h"
#include "olap/iterators.h"
#include "olap/olap_define.h"
#include "olap/options.h"
#include "olap/rowset/beta_rowset.h"
#include "olap/rowset/segment_v2/segment.h"
#include "olap/schema.h"
#include "olap/segment_loader.h"
#include "olap/snapshot_manager.h"
#include "olap/storage_engine.h"
#include "olap/tablet.h"
#include "olap/tablet_manager.h"
#include "olap/tablet_meta.h"
#include "olap/task/engine_publish_version_task.h"
#include "olap/txn_manager.h"
#include "runtime/cluster_info.h"
#include "runtime/define_primitive_type.h"
#include "runtime/descriptor_helper.h"
#include "runtime/descriptors.h"
#include "runtime/exec_env.h"
#include "vec/columns/column.h"
#include "vec/core/block.h"
#include "vec/core/column_with_type_and_name.h"

namespace doris {

static std::string storage_root_path;

class MockDownloadHandler : public HttpHandler {
public:
    MockDownloadHandler() = default;
    void handle(HttpRequest* req) override {
        std::vector<std::string> allow_path {fmt::format("{}", storage_root_path)};
        DownloadAction action(ExecEnv::GetInstance(), nullptr, allow_path);
        action.handle(req);
    }
};

static const uint32_t MAX_PATH_LEN = 1024;
static StorageEngine* engine_ref = nullptr;
static EvHttpServer* s_server = nullptr;
static MockDownloadHandler mock_download_handler;
static std::string hostname;
static std::string address;
static ClusterInfo cluster_info;

static void set_up() {
    char buffer[MAX_PATH_LEN];
    EXPECT_NE(getcwd(buffer, MAX_PATH_LEN), nullptr);
    storage_root_path = std::string(buffer) + "/snapshot_data_test";
    auto st = io::global_local_filesystem()->delete_directory(storage_root_path);
    ASSERT_TRUE(st.ok()) << st;
    st = io::global_local_filesystem()->create_directory(storage_root_path);
    ASSERT_TRUE(st.ok()) << st;
    std::vector<StorePath> paths;
    paths.emplace_back(storage_root_path, -1);

    doris::EngineOptions options;
    options.store_paths = paths;
    options.backend_uid = UniqueId::gen_uid();
    auto engine = std::make_unique<StorageEngine>(options);
    engine_ref = engine.get();
    Status s = engine->open();
    ASSERT_TRUE(s.ok()) << s;
    ASSERT_TRUE(s.ok()) << s;

    ExecEnv* exec_env = doris::ExecEnv::GetInstance();
    cluster_info.token = "fake_token";
    exec_env->set_cluster_info(&cluster_info);
    exec_env->set_memtable_memory_limiter(new MemTableMemoryLimiter());
    exec_env->set_storage_engine(std::move(engine));
    s_server = new EvHttpServer(1234, 3);
    s_server->register_handler(GET, "/api/_tablet/_download", &mock_download_handler);
    s_server->register_handler(POST, "/api/_tablet/_download", &mock_download_handler);
    s_server->register_handler(HEAD, "/api/_tablet/_download", &mock_download_handler);

    static_cast<void>(s_server->start());
    address = "127.0.0.1:" + std::to_string(1234);
    hostname = "http://" + address;
}

static void tear_down() {
    ExecEnv* exec_env = doris::ExecEnv::GetInstance();
    exec_env->set_memtable_memory_limiter(nullptr);
    engine_ref = nullptr;
    exec_env->set_storage_engine(nullptr);

    if (storage_root_path.empty()) {
        return;
    }

    Status st = io::global_local_filesystem()->delete_directory(storage_root_path);
    ASSERT_TRUE(st.ok()) << st;
    delete s_server;
    // Status s = io::global_local_filesystem()->delete_directory(storage_root_path);
    // EXPECT_TRUE(s.ok()) << "delete directory " << s;
}

static TCreateTabletReq create_tablet(int64_t partition_id, int64_t tablet_id,
                                      int32_t schema_hash) {
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
    tablet_schema.__set_schema_hash(schema_hash);
    tablet_schema.__set_keys_type(TKeysType::AGG_KEYS);
    tablet_schema.__set_storage_type(TStorageType::COLUMN);
    tablet_schema.__set_columns(cols);
    TCreateTabletReq create_tablet_req;
    create_tablet_req.__set_tablet_schema(tablet_schema);
    create_tablet_req.__set_tablet_id(tablet_id);
    create_tablet_req.__set_partition_id(partition_id);
    create_tablet_req.__set_version(2);
    return create_tablet_req;
}

static TDescriptorTable create_descriptor_tablet() {
    TDescriptorTableBuilder dtb;
    TTupleDescriptorBuilder tuple_builder;
    tuple_builder.add_slot(
            TSlotDescriptorBuilder().type(TYPE_SMALLINT).column_name("col1").column_pos(0).build());
    tuple_builder.build(&dtb);
    return dtb.desc_tbl();
}

static void add_rowset(int64_t tablet_id, int32_t schema_hash, int64_t partition_id, int64_t txn_id,
                       int16_t value) {
    TDescriptorTable tdesc_tbl = create_descriptor_tablet();
    ObjectPool obj_pool;
    DescriptorTbl* desc_tbl = nullptr;
    static_cast<void>(DescriptorTbl::create(&obj_pool, tdesc_tbl, &desc_tbl));
    TupleDescriptor* tuple_desc = desc_tbl->get_tuple_descriptor(0);
    auto param = std::make_shared<OlapTableSchemaParam>();

    PUniqueId load_id;
    load_id.set_hi(0);
    load_id.set_lo(0);
    WriteRequest write_req;
    write_req.tablet_id = tablet_id;
    write_req.schema_hash = schema_hash;
    write_req.txn_id = txn_id;
    write_req.partition_id = partition_id;
    write_req.load_id = load_id;
    write_req.tuple_desc = tuple_desc;
    write_req.slots = &(tuple_desc->slots());
    write_req.is_high_priority = false;
    write_req.table_schema_param = param;
    auto profile = std::make_unique<RuntimeProfile>("LoadChannels");
    auto delta_writer =
            std::make_unique<DeltaWriter>(*engine_ref, write_req, profile.get(), TUniqueId {});

    vectorized::Block block;
    for (const auto& slot_desc : tuple_desc->slots()) {
        std::cout << "slot_desc: " << slot_desc->col_name() << std::endl;
        block.insert(vectorized::ColumnWithTypeAndName(slot_desc->get_empty_mutable_column(),
                                                       slot_desc->get_data_type_ptr(),
                                                       slot_desc->col_name()));
    }

    std::cout << "total column " << block.mutate_columns().size() << std::endl;
    auto columns = block.mutate_columns();
    int16_t c1 = value;
    columns[0]->insert_data((const char*)&c1, sizeof(c1));
    Status res = delta_writer->write(&block, {0});
    EXPECT_TRUE(res.ok());

    res = delta_writer->close();
    ASSERT_TRUE(res.ok());
    res = delta_writer->wait_flush();
    ASSERT_TRUE(res.ok());
    res = delta_writer->build_rowset();
    ASSERT_TRUE(res.ok());
    res = delta_writer->submit_calc_delete_bitmap_task();
    ASSERT_TRUE(res.ok());
    res = delta_writer->wait_calc_delete_bitmap();
    ASSERT_TRUE(res.ok());
    res = delta_writer->commit_txn(PSlaveTabletNodes());
    ASSERT_TRUE(res.ok()) << res;

    TabletSharedPtr tablet = engine_ref->tablet_manager()->get_tablet(tablet_id);
    ASSERT_TRUE(tablet != nullptr);

    std::cout << "before publish, tablet row nums:" << tablet->num_rows() << std::endl;
    Version version;
    version.first = tablet->get_rowset_with_max_version()->end_version() + 1;
    version.second = tablet->get_rowset_with_max_version()->end_version() + 1;
    std::cout << "start to add rowset version:" << version.first << "-" << version.second
              << std::endl;
    std::map<TabletInfo, RowsetSharedPtr> tablet_related_rs;
    engine_ref->txn_manager()->get_txn_related_tablets(txn_id, partition_id, &tablet_related_rs);
    ASSERT_EQ(1, tablet_related_rs.size());

    std::cout << "start to publish txn" << std::endl;
    RowsetSharedPtr rowset = tablet_related_rs.begin()->second;

    TabletPublishStatistics stats;
    std::shared_ptr<TabletTxnInfo> extend_tablet_txn_info_lifetime = nullptr;
    res = engine_ref->txn_manager()->publish_txn(partition_id, tablet, txn_id, version, &stats,
                                                 extend_tablet_txn_info_lifetime);
    ASSERT_TRUE(res.ok()) << res;
    std::cout << "start to add inc rowset:" << rowset->rowset_id()
              << ", num rows:" << rowset->num_rows() << ", version:" << rowset->version().first
              << "-" << rowset->version().second << std::endl;
    res = tablet->add_inc_rowset(rowset);
    ASSERT_TRUE(res.ok()) << res;
}

class SnapshotLoaderTest : public ::testing::Test {
public:
    SnapshotLoaderTest() {}
    ~SnapshotLoaderTest() {}
    static void SetUpTestSuite() { set_up(); }

    static void TearDownTestSuite() { tear_down(); }
};

TEST_F(SnapshotLoaderTest, NormalCase) {
    StorageEngine engine({});
    SnapshotLoader loader(engine, ExecEnv::GetInstance(), 1L, 2L);

    int64_t tablet_id = 0;
    int32_t schema_hash = 0;
    Status st = loader._get_tablet_id_and_schema_hash_from_file_path("/path/to/1234/5678",
                                                                     &tablet_id, &schema_hash);
    EXPECT_TRUE(st.ok());
    EXPECT_EQ(1234, tablet_id);
    EXPECT_EQ(5678, schema_hash);

    st = loader._get_tablet_id_and_schema_hash_from_file_path("/path/to/1234/5678/", &tablet_id,
                                                              &schema_hash);
    EXPECT_FALSE(st.ok());

    std::filesystem::remove_all("./ss_test/");
    std::map<std::string, std::string> src_to_dest;
    src_to_dest["./ss_test/"] = "./ss_test";
    st = loader._check_local_snapshot_paths(src_to_dest, true);
    EXPECT_FALSE(st.ok());
    st = loader._check_local_snapshot_paths(src_to_dest, false);
    EXPECT_FALSE(st.ok());

    std::filesystem::create_directory("./ss_test/");
    st = loader._check_local_snapshot_paths(src_to_dest, true);
    EXPECT_TRUE(st.ok());
    st = loader._check_local_snapshot_paths(src_to_dest, false);
    EXPECT_TRUE(st.ok());
    std::filesystem::remove_all("./ss_test/");

    std::filesystem::create_directory("./ss_test/");
    std::vector<std::string> files;
    st = loader._get_existing_files_from_local("./ss_test/", &files);
    EXPECT_EQ(0, files.size());
    std::filesystem::remove_all("./ss_test/");

    std::string new_name;
    st = loader._replace_tablet_id("12345.hdr", 5678, &new_name);
    EXPECT_TRUE(st.ok());
    EXPECT_EQ("5678.hdr", new_name);

    st = loader._replace_tablet_id("1234_2_5_12345_1.dat", 5678, &new_name);
    EXPECT_TRUE(st.ok());
    EXPECT_EQ("1234_2_5_12345_1.dat", new_name);

    st = loader._replace_tablet_id("1234_2_5_12345_1.idx", 5678, &new_name);
    EXPECT_TRUE(st.ok());
    EXPECT_EQ("1234_2_5_12345_1.idx", new_name);

    st = loader._replace_tablet_id("1234_2_5_12345_1.xxx", 5678, &new_name);
    EXPECT_FALSE(st.ok());

    st = loader._get_tablet_id_from_remote_path("/__tbl_10004/__part_10003/__idx_10004/__10005",
                                                &tablet_id);
    EXPECT_TRUE(st.ok());
    EXPECT_EQ(10005, tablet_id);
}

TEST_F(SnapshotLoaderTest, DirMoveTaskIsIdempotent) {
    // 1. create a tablet
    int64_t tablet_id = 111;
    int32_t schema_hash = 222;
    int64_t partition_id = 333;
    TCreateTabletReq req = create_tablet(partition_id, tablet_id, schema_hash);
    RuntimeProfile profile("CreateTablet");
    Status status = engine_ref->create_tablet(req, &profile);
    EXPECT_TRUE(status.ok());
    TabletSharedPtr tablet = engine_ref->tablet_manager()->get_tablet(tablet_id);
    EXPECT_TRUE(tablet != nullptr);

    // 2. add a rowset
    add_rowset(tablet_id, schema_hash, partition_id, 100, 100);
    auto version = tablet->max_version();
    std::cout << "version: " << version.first << ", " << version.second << std::endl;

    // 3. make a snapshot
    string snapshot_path;
    bool allow_incremental_clone = false; // not used
    TSnapshotRequest snapshot_request;
    snapshot_request.tablet_id = tablet_id;
    snapshot_request.schema_hash = schema_hash;
    snapshot_request.version = version.second;
    status = engine_ref->snapshot_mgr()->make_snapshot(snapshot_request, &snapshot_path,
                                                       &allow_incremental_clone);
    ASSERT_TRUE(status.ok());

    // 4. load the snapshot to another tablet
    snapshot_path = fmt::format("{}/{}/{}", snapshot_path, tablet_id, schema_hash);
    SnapshotLoader loader1(*engine_ref, ExecEnv::GetInstance(), 1L, tablet_id);
    status = loader1.move(snapshot_path, tablet, true);
    ASSERT_TRUE(status.ok()) << status;

    // 5. Insert a rowset to the tablet
    // reload tablet
    tablet = engine_ref->tablet_manager()->get_tablet(tablet_id);
    EXPECT_TRUE(tablet != nullptr);
    add_rowset(tablet_id, schema_hash, partition_id, 200, 200);
    version = tablet->max_version();
    std::cout << "version: " << version.first << ", " << version.second << std::endl;

    // 6. load the snapshot to the tablet again, this request should be idempotent
    SnapshotLoader loader2(*engine_ref, ExecEnv::GetInstance(), 2L, tablet_id);
    status = loader2.move(snapshot_path, tablet, true);
    ASSERT_TRUE(status.ok()) << status;

    // reload tablet
    tablet = engine_ref->tablet_manager()->get_tablet(tablet_id);
    EXPECT_TRUE(tablet != nullptr);
    auto last_version = tablet->max_version();
    std::cout << "last version: " << last_version.first << ", " << last_version.second << std::endl;
    ASSERT_EQ(version.first, last_version.first);
    ASSERT_EQ(version.second, last_version.second);
}
TEST_F(SnapshotLoaderTest, TestLinkSameRowsetFiles) {
    // 1. Create a tablet
    int64_t tablet_id = 222;
    int32_t schema_hash = 333;
    int64_t partition_id = 444;
    TCreateTabletReq req = create_tablet(partition_id, tablet_id, schema_hash);
    RuntimeProfile profile("CreateTablet");
    Status status = engine_ref->create_tablet(req, &profile);
    EXPECT_TRUE(status.ok());
    TabletSharedPtr tablet = engine_ref->tablet_manager()->get_tablet(tablet_id);
    EXPECT_TRUE(tablet != nullptr);

    // 2. Add a rowset to the tablet
    add_rowset(tablet_id, schema_hash, partition_id, 100, 100);
    auto version = tablet->max_version();
    std::cout << "Original version: " << version.first << ", " << version.second << std::endl;

    // 3. Make a snapshot of the tablet
    string snapshot_path;
    bool allow_incremental_clone = false;
    TSnapshotRequest snapshot_request;
    snapshot_request.tablet_id = tablet_id;
    snapshot_request.schema_hash = schema_hash;
    snapshot_request.version = version.second;
    status = engine_ref->snapshot_mgr()->make_snapshot(snapshot_request, &snapshot_path,
                                                       &allow_incremental_clone);
    ASSERT_TRUE(status.ok());
    std::cout << "snapshot_path: " << snapshot_path << std::endl;
    snapshot_path = fmt::format("{}/{}/{}", snapshot_path, tablet_id, schema_hash);

    // 4. Create a destination path for "remote" snapshot
    std::string remote_snapshot_dir = storage_root_path + "/remote_snapshot";
    ASSERT_TRUE(io::global_local_filesystem()->create_directory(remote_snapshot_dir).ok());
    std::string remote_tablet_path =
            fmt::format("{}/{}/{}", remote_snapshot_dir, tablet_id, schema_hash);
    ASSERT_TRUE(io::global_local_filesystem()->create_directory(remote_tablet_path).ok());

    // 5. Copy snapshot files to remote path and calls convert_rowset_ids
    std::vector<io::FileInfo> snapshot_files;
    bool is_exists = false;
    ASSERT_TRUE(io::global_local_filesystem()
                        ->list(snapshot_path, true, &snapshot_files, &is_exists)
                        .ok());
    for (const auto& file : snapshot_files) {
        std::string src_file = snapshot_path + "/" + file.file_name;
        std::string dst_file = remote_tablet_path + "/" + file.file_name;
        ASSERT_TRUE(io::global_local_filesystem()->copy_path(src_file, dst_file).ok());
    }

    int64_t dest_tablet_id = 333;
    int32_t dest_schema_hash = 444;
    std::string dest_path = fmt::format("{}/dest_snapshot/{}/{}", storage_root_path, dest_tablet_id,
                                        dest_schema_hash);
    ASSERT_TRUE(io::global_local_filesystem()->create_directory(dest_path).ok());

    std::string src_hdr = remote_tablet_path + "/" + std::to_string(tablet_id) + ".hdr";
    std::string dst_hdr = remote_tablet_path + "/" + std::to_string(dest_tablet_id) + ".hdr";
    ASSERT_TRUE(io::global_local_filesystem()->rename(src_hdr, dst_hdr).ok());
    auto guards = engine_ref->snapshot_mgr()->convert_rowset_ids(
            remote_tablet_path, dest_tablet_id, 0, 0, partition_id, dest_schema_hash);

    // 7. Setup a remote tablet snapshot for download
    TRemoteTabletSnapshot remote_snapshot;
    remote_snapshot.remote_tablet_id = dest_tablet_id;
    remote_snapshot.local_tablet_id = tablet_id;
    remote_snapshot.local_snapshot_path = snapshot_path;
    remote_snapshot.remote_snapshot_path = remote_tablet_path;
    remote_snapshot.remote_be_addr.hostname = "127.0.0.1";
    remote_snapshot.remote_be_addr.port = 1234;
    remote_snapshot.remote_token = "fake_token";

    // 8. Download the snapshot
    std::vector<TRemoteTabletSnapshot> remote_snapshots = {remote_snapshot};
    std::vector<int64_t> downloaded_tablet_ids;
    SnapshotLoader loader(*engine_ref, ExecEnv::GetInstance(), 3L, tablet_id);
    status = loader.remote_http_download(remote_snapshots, &downloaded_tablet_ids);
    ASSERT_TRUE(status.ok());

    // 9. Verify skip download files
    ASSERT_EQ(loader.get_http_download_files_num(), 0);
}

static void write_test_file(const std::string& path, const std::string& content) {
    io::FileWriterPtr writer;
    ASSERT_TRUE(io::global_local_filesystem()->create_file(path, &writer).ok());
    ASSERT_TRUE(writer->append(Slice(content)).ok());
    ASSERT_TRUE(writer->close().ok());
}

static SnapshotManifestFile manifest_file(const std::string& name, int64_t size,
                                          const std::string& sha256 = "") {
    return SnapshotManifestFile {.name = name, .size = size, .md5 = "", .sha256 = sha256};
}

TEST_F(SnapshotLoaderTest, ComputeFileDigests) {
    std::string dir = storage_root_path + "/digest_test";
    ASSERT_TRUE(io::global_local_filesystem()->create_directory(dir).ok());

    std::string md5;
    std::string sha256;
    int64_t size = -1;
    write_test_file(dir + "/abc", "abc");
    ASSERT_TRUE(compute_file_digests(dir + "/abc", &md5, &sha256, &size).ok());
    EXPECT_EQ("900150983cd24fb0d6963f7d28e17f72", md5);
    EXPECT_EQ("ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad", sha256);
    EXPECT_EQ(3, size);

    write_test_file(dir + "/empty", "");
    ASSERT_TRUE(compute_file_digests(dir + "/empty", &md5, &sha256, &size).ok());
    EXPECT_EQ("d41d8cd98f00b204e9800998ecf8427e", md5);
    EXPECT_EQ("e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855", sha256);
    EXPECT_EQ(0, size);

    // larger than the read buffer, the md5 must be the same as the one used in the repository.
    std::string big(3 * 1024 * 1024 + 17, 'x');
    for (size_t i = 0; i < big.size(); i += 4096) {
        big[i] = static_cast<char>('a' + (i / 4096) % 26);
    }
    write_test_file(dir + "/big", big);
    std::string expected_md5;
    ASSERT_TRUE(io::global_local_filesystem()->md5sum(dir + "/big", &expected_md5).ok());
    std::string only_sha256;
    ASSERT_TRUE(compute_file_digests(dir + "/big", &md5, &sha256, &size).ok());
    ASSERT_TRUE(compute_file_digests(dir + "/big", nullptr, &only_sha256).ok());
    EXPECT_EQ(expected_md5, md5);
    EXPECT_EQ(only_sha256, sha256);
    EXPECT_EQ(64, sha256.size());
    EXPECT_EQ(static_cast<int64_t>(big.size()), size);

    EXPECT_FALSE(compute_file_digests(dir + "/not_exist", &md5, &sha256).ok());
    ASSERT_TRUE(io::global_local_filesystem()->delete_directory(dir).ok());
}

TEST_F(SnapshotLoaderTest, SnapshotManifestSerializeAndParse) {
    SnapshotManifest manifest;
    manifest.tablet_id = 1001;
    manifest.files = {
            SnapshotManifestFile {.name = "rs1_0.idx", .size = 5, .md5 = "", .sha256 = ""},
            SnapshotManifestFile {.name = "1001.hdr",
                                  .size = 6,
                                  .md5 = "4f158689243a3d6030352fec3cfd3798",
                                  .sha256 = ""},
            SnapshotManifestFile {
                    .name = "rs1_0.dat", .size = 7, .md5 = "", .sha256 = std::string(64, 'a')}};
    std::string content = manifest.serialize();
    // sorted by name, empty "m" and "d" are omitted
    EXPECT_EQ(
            "{\"version\":1,\"tablet_id\":1001,\"files\":[{\"n\":\"1001.hdr\",\"s\":6,\"m\":"
            "\"4f158689243a3d6030352fec3cfd3798\"},{\"n\":\"rs1_0.dat\",\"s\":7,\"d\":\"" +
                    std::string(64, 'a') + "\"},{\"n\":\"rs1_0.idx\",\"s\":5}]}",
            content);
    std::string root = sha256_hex(content);
    EXPECT_EQ(64, root.size());
    EXPECT_EQ("__manifest__1001." + root.substr(0, 32),
              SnapshotManifest::remote_file_name(1001, root));

    SnapshotManifest parsed;
    auto st = SnapshotManifest::parse(content, root, 1001, &parsed);
    ASSERT_TRUE(st.ok()) << st;
    ASSERT_EQ(3, parsed.files.size());
    EXPECT_EQ("1001.hdr", parsed.files[0].name);
    EXPECT_EQ("4f158689243a3d6030352fec3cfd3798", parsed.files[0].md5);
    EXPECT_EQ(7, parsed.files[1].size);
    EXPECT_EQ(std::string(64, 'a'), parsed.files[1].sha256);
    EXPECT_TRUE(parsed.files[2].sha256.empty());
    EXPECT_EQ(content, parsed.serialize());

    // the root does not match: a tampered manifest
    std::string tampered = content;
    tampered.replace(tampered.find("\"s\":7"), 5, "\"s\":8");
    st = SnapshotManifest::parse(tampered, root, 1001, &parsed);
    EXPECT_TRUE(st.is<ErrorCode::RESTORE_MANIFEST_MISMATCH>()) << st;
    EXPECT_NE(st.to_string().find("does not match the root"), std::string::npos) << st;
    // a manifest of another tablet
    st = SnapshotManifest::parse(content, root, 1002, &parsed);
    EXPECT_TRUE(st.is<ErrorCode::RESTORE_MANIFEST_MISMATCH>()) << st;
    // not a valid manifest, although the root matches
    for (std::string bad : {std::string("not json"), std::string("{\"version\":2}"),
                            std::string("{\"version\":1,\"tablet_id\":1001,\"files\":[{\"n\":"
                                        "\"a.dat\",\"s\":1},{\"n\":\"a.dat\",\"s\":1}]}"),
                            std::string("{\"version\":1,\"tablet_id\":1001,\"files\":[{\"n\":"
                                        "\"a.dat\"}]}")}) {
        st = SnapshotManifest::parse(bad, sha256_hex(bad), 1001, &parsed);
        EXPECT_TRUE(st.is<ErrorCode::RESTORE_MANIFEST_MISMATCH>()) << bad << ": " << st;
    }

    // written to a file, the root is the SHA-256 of the file
    std::string dir = storage_root_path + "/manifest_write";
    ASSERT_TRUE(io::global_local_filesystem()->create_directory(dir).ok());
    std::string written_root;
    ASSERT_TRUE(write_snapshot_manifest(manifest, dir + "/manifest", &written_root).ok());
    EXPECT_EQ(root, written_root);
    std::string file_sha256;
    ASSERT_TRUE(compute_file_digests(dir + "/manifest", nullptr, &file_sha256).ok());
    EXPECT_EQ(root, file_sha256);
    ASSERT_TRUE(io::global_local_filesystem()->delete_directory(dir).ok());

    std::string parent;
    ASSERT_TRUE(parent_path_of("/a/b/10001/12345", &parent).ok());
    EXPECT_EQ("/a/b/10001", parent);
    ASSERT_TRUE(parent_path_of("s3://bucket/x/__idx_1/__10005/", &parent).ok());
    EXPECT_EQ("s3://bucket/x/__idx_1", parent);
    EXPECT_FALSE(parent_path_of("noslash", &parent).ok());
}

// The decomposed digest file (rdigest) is an entry of the manifest, in the order of the names. The manifest
// is version 2 then, and parse() keeps the entry out of the files.
TEST_F(SnapshotLoaderTest, SnapshotManifestWithPrefixDigest) {
    SnapshotManifest manifest;
    manifest.tablet_id = 1001;
    manifest.files = {
            SnapshotManifestFile {.name = "rs1_0.idx", .size = 5, .md5 = "", .sha256 = ""},
            SnapshotManifestFile {.name = "1001.hdr",
                                  .size = 6,
                                  .md5 = "4f158689243a3d6030352fec3cfd3798",
                                  .sha256 = ""}};
    std::string old_content = manifest.serialize();
    // without the entry it is the version 1 manifest as before
    EXPECT_EQ(0U, old_content.find("{\"version\":1,"));
    EXPECT_EQ(std::string::npos, old_content.find("__rdigest__"));
    EXPECT_FALSE(manifest.has_prefix_digest());

    manifest.prefix_digest_sha256 = std::string(64, 'c');
    manifest.prefix_digest_size = 123;
    ASSERT_TRUE(manifest.has_prefix_digest());
    std::string content = manifest.serialize();
    EXPECT_EQ(
            "{\"version\":2,\"tablet_id\":1001,\"files\":[{\"n\":\"1001.hdr\",\"s\":6,\"m\":"
            "\"4f158689243a3d6030352fec3cfd3798\"},{\"n\":\"__rdigest__\",\"s\":123,\"d\":\"" +
                    std::string(64, 'c') + "\"},{\"n\":\"rs1_0.idx\",\"s\":5}]}",
            content);
    // the files of the manifest are not changed by the entry
    ASSERT_EQ(2, manifest.files.size());
    std::string root = sha256_hex(content);
    EXPECT_NE(sha256_hex(old_content), root);

    SnapshotManifest parsed;
    auto st = SnapshotManifest::parse(content, root, 1001, &parsed);
    ASSERT_TRUE(st.ok()) << st;
    // the entry is not a file to download, link or check
    ASSERT_EQ(2, parsed.files.size());
    EXPECT_EQ("1001.hdr", parsed.files[0].name);
    EXPECT_EQ("rs1_0.idx", parsed.files[1].name);
    EXPECT_TRUE(parsed.has_prefix_digest());
    EXPECT_EQ(std::string(64, 'c'), parsed.prefix_digest_sha256);
    EXPECT_EQ(123, parsed.prefix_digest_size);
    EXPECT_EQ(content, parsed.serialize());

    // a manifest written before the entry is still read, and parsing resets the entry of the target
    st = SnapshotManifest::parse(old_content, sha256_hex(old_content), 1001, &parsed);
    ASSERT_TRUE(st.ok()) << st;
    EXPECT_FALSE(parsed.has_prefix_digest());
    EXPECT_EQ(0, parsed.prefix_digest_size);
    EXPECT_EQ(2, parsed.files.size());

    // an entry without the SHA-256, or twice
    for (std::string bad :
         {std::string(
                  "{\"version\":2,\"tablet_id\":1001,\"files\":[{\"n\":\"__rdigest__\",\"s\":1}]}"),
          std::string("{\"version\":2,\"tablet_id\":1001,\"files\":[{\"n\":\"__rdigest__\",\"s\":1,"
                      "\"d\":\"abc\"}]}"),
          "{\"version\":2,\"tablet_id\":1001,\"files\":[{\"n\":\"__rdigest__\",\"s\":1,\"d\":\"" +
                  std::string(64, 'a') + "\"},{\"n\":\"__rdigest__\",\"s\":1,\"d\":\"" +
                  std::string(64, 'a') + "\"}]}"}) {
        st = SnapshotManifest::parse(bad, sha256_hex(bad), 1001, &parsed);
        EXPECT_TRUE(st.is<ErrorCode::RESTORE_MANIFEST_MISMATCH>()) << bad << ": " << st;
    }
    // an unknown version
    std::string v3 = "{\"version\":3,\"tablet_id\":1001,\"files\":[]}";
    st = SnapshotManifest::parse(v3, sha256_hex(v3), 1001, &parsed);
    EXPECT_TRUE(st.is<ErrorCode::RESTORE_MANIFEST_MISMATCH>()) << st;
}

// The entry of the decomposed digest is not a file of the tablet snapshot dir: it is never missing, and
// the snapshot is not required to have it.
TEST_F(SnapshotLoaderTest, CheckTabletSnapshotManifestIgnoresPrefixDigestEntry) {
    std::string dir = storage_root_path + "/manifest_check_rdigest/2002/3003";
    ASSERT_TRUE(io::global_local_filesystem()->create_directory(dir).ok());
    write_test_file(dir + "/2002.hdr", "header");
    write_test_file(dir + "/rs1_0.dat", "segment");
    std::string dat_sha256;
    ASSERT_TRUE(compute_file_digests(dir + "/rs1_0.dat", nullptr, &dat_sha256).ok());

    SnapshotManifest manifest;
    manifest.tablet_id = 1001;
    manifest.files = {
            SnapshotManifestFile {.name = "1001.hdr", .size = 6, .md5 = "", .sha256 = ""},
            SnapshotManifestFile {.name = "rs1_0.dat", .size = 7, .md5 = "", .sha256 = dat_sha256}};
    manifest.prefix_digest_sha256 = std::string(64, 'd');
    manifest.prefix_digest_size = 99;
    // through the serialized form, as a restore gets it
    std::string content = manifest.serialize();
    SnapshotManifest parsed;
    ASSERT_TRUE(SnapshotManifest::parse(content, sha256_hex(content), 1001, &parsed).ok());

    ManifestCheckResult result;
    auto st = check_tablet_snapshot_manifest(dir, 2002, parsed, true, &result);
    ASSERT_TRUE(st.ok()) << st;
    EXPECT_TRUE(result.checked);
    EXPECT_TRUE(result.digest_checked);
    // a file of the snapshot dir is still not allowed to be missing
    ASSERT_TRUE(io::global_local_filesystem()->delete_file(dir + "/rs1_0.dat").ok());
    st = check_tablet_snapshot_manifest(dir, 2002, parsed, true, &result);
    EXPECT_TRUE(st.is<ErrorCode::RESTORE_MANIFEST_MISMATCH>()) << st;
    ASSERT_TRUE(io::global_local_filesystem()
                        ->delete_directory(storage_root_path + "/manifest_check_rdigest")
                        .ok());
}

TEST_F(SnapshotLoaderTest, CheckTabletSnapshotManifest) {
    // A downloaded local snapshot of local tablet 2002, the source tablet is 1001.
    std::string dir = storage_root_path + "/manifest_check/2002/3003";
    ASSERT_TRUE(io::global_local_filesystem()->create_directory(dir).ok());
    write_test_file(dir + "/2002.hdr", "header");
    write_test_file(dir + "/rs1_0.dat", "segment");
    write_test_file(dir + "/rs1_0.idx", "index");
    std::string dat_sha256;
    std::string idx_sha256;
    ASSERT_TRUE(compute_file_digests(dir + "/rs1_0.dat", nullptr, &dat_sha256).ok());
    ASSERT_TRUE(compute_file_digests(dir + "/rs1_0.idx", nullptr, &idx_sha256).ok());

    auto make_manifest = [&]() {
        SnapshotManifest manifest;
        manifest.tablet_id = 1001;
        manifest.files = {manifest_file("1001.hdr", 6), manifest_file("rs1_0.dat", 7, dat_sha256),
                          manifest_file("rs1_0.idx", 5, idx_sha256)};
        return manifest;
    };

    // 1. matched, the tablet meta file is expected with the local tablet id
    ManifestCheckResult result;
    auto st = check_tablet_snapshot_manifest(dir, 2002, make_manifest(), false, &result);
    ASSERT_TRUE(st.ok()) << st;
    EXPECT_TRUE(result.checked);
    EXPECT_FALSE(result.digest_checked);

    st = check_tablet_snapshot_manifest(dir, 2002, make_manifest(), true, &result);
    ASSERT_TRUE(st.ok()) << st;
    EXPECT_TRUE(result.checked);
    EXPECT_TRUE(result.digest_checked);

    // the LOADED tag is not a snapshot file
    write_test_file(dir + "/LOADED", "");
    st = check_tablet_snapshot_manifest(dir, 2002, make_manifest(), true, &result);
    ASSERT_TRUE(st.ok()) << st;
    ASSERT_TRUE(io::global_local_filesystem()->delete_file(dir + "/LOADED").ok());

    // 2. one file less in local
    SnapshotManifest more = make_manifest();
    more.files.push_back(manifest_file("rs2_0.dat", 10));
    st = check_tablet_snapshot_manifest(dir, 2002, more, false, &result);
    EXPECT_TRUE(st.is<ErrorCode::RESTORE_MANIFEST_MISMATCH>()) << st;
    EXPECT_NE(st.to_string().find("missing file rs2_0.dat, expected size 10"), std::string::npos)
            << st;
    EXPECT_FALSE(result.checked);

    // 3. one file more in local
    write_test_file(dir + "/rs1_1.dat", "extra");
    st = check_tablet_snapshot_manifest(dir, 2002, make_manifest(), false, &result);
    EXPECT_TRUE(st.is<ErrorCode::RESTORE_MANIFEST_MISMATCH>()) << st;
    EXPECT_NE(st.to_string().find("unexpected file rs1_1.dat"), std::string::npos) << st;
    EXPECT_NE(st.to_string().find("actual size 5"), std::string::npos) << st;
    ASSERT_TRUE(io::global_local_filesystem()->delete_file(dir + "/rs1_1.dat").ok());

    // the tablet meta file of the source tablet id is not expected in local
    write_test_file(dir + "/1001.hdr", "header");
    st = check_tablet_snapshot_manifest(dir, 2002, make_manifest(), false, &result);
    EXPECT_TRUE(st.is<ErrorCode::RESTORE_MANIFEST_MISMATCH>()) << st;
    EXPECT_NE(st.to_string().find("unexpected file 1001.hdr"), std::string::npos) << st;
    ASSERT_TRUE(io::global_local_filesystem()->delete_file(dir + "/1001.hdr").ok());

    // 4. size mismatch
    SnapshotManifest wrong_size = make_manifest();
    wrong_size.files[1].size = 8;
    st = check_tablet_snapshot_manifest(dir, 2002, wrong_size, false, &result);
    EXPECT_TRUE(st.is<ErrorCode::RESTORE_MANIFEST_MISMATCH>()) << st;
    EXPECT_NE(st.to_string().find("size mismatch of file rs1_0.dat, expected 8, actual 7"),
              std::string::npos)
            << st;

    // 5. digest mismatch, only checked when required
    SnapshotManifest wrong_digest = make_manifest();
    wrong_digest.files[2].sha256 = std::string(64, '0');
    st = check_tablet_snapshot_manifest(dir, 2002, wrong_digest, false, &result);
    EXPECT_TRUE(st.ok()) << st;
    st = check_tablet_snapshot_manifest(dir, 2002, wrong_digest, true, &result);
    EXPECT_TRUE(st.is<ErrorCode::RESTORE_MANIFEST_MISMATCH>()) << st;
    EXPECT_NE(st.to_string().find("sha256 mismatch of file rs1_0.idx, expected " +
                                  std::string(64, '0') + ", actual " + idx_sha256),
              std::string::npos)
            << st;

    // the digest of the tablet meta file is never checked, it is rewritten in restore
    SnapshotManifest hdr_digest = make_manifest();
    hdr_digest.files[0].sha256 = std::string(64, '0');
    st = check_tablet_snapshot_manifest(dir, 2002, hdr_digest, true, &result);
    EXPECT_TRUE(st.ok()) << st;
    EXPECT_TRUE(result.digest_checked);

    // a file without digest in the manifest: checked, but not all digests are checked
    SnapshotManifest no_digest = make_manifest();
    no_digest.files[1].sha256.clear();
    st = check_tablet_snapshot_manifest(dir, 2002, no_digest, true, &result);
    EXPECT_TRUE(st.ok()) << st;
    EXPECT_TRUE(result.checked);
    EXPECT_FALSE(result.digest_checked);

    ASSERT_TRUE(io::global_local_filesystem()
                        ->delete_directory(storage_root_path + "/manifest_check")
                        .ok());
}

// Write the manifest of a "remote" tablet snapshot dir next to it (<parent>/manifest), as the source BE
// does when making a snapshot. Returns the root. size_delta is added to the size of the first segment
// file, to make a manifest which does not match the snapshot.
static std::string write_remote_manifest(const std::string& remote_tablet_path,
                                         int64_t remote_tablet_id, bool with_digest,
                                         int64_t size_delta = 0) {
    SnapshotManifest manifest;
    manifest.tablet_id = remote_tablet_id;
    std::vector<io::FileInfo> files;
    bool exists = false;
    EXPECT_TRUE(
            io::global_local_filesystem()->list(remote_tablet_path, true, &files, &exists).ok());
    bool changed = false;
    for (const auto& file : files) {
        SnapshotManifestFile entry {
                .name = file.file_name, .size = file.file_size, .md5 = "", .sha256 = ""};
        if (with_digest && !file.file_name.ends_with(".hdr")) {
            EXPECT_TRUE(compute_file_digests(remote_tablet_path + "/" + file.file_name, nullptr,
                                             &entry.sha256)
                                .ok());
        }
        if (!changed && size_delta != 0 && file.file_name.ends_with(".dat")) {
            entry.size += size_delta;
            changed = true;
        }
        manifest.files.push_back(std::move(entry));
    }
    std::string parent;
    EXPECT_TRUE(parent_path_of(remote_tablet_path, &parent).ok());
    std::string root;
    EXPECT_TRUE(write_snapshot_manifest(manifest, parent + "/manifest", &root).ok());
    return root;
}

// Prepare a local tablet with a snapshot, and a "remote" snapshot derived from it, as in
// TestLinkSameRowsetFiles, so that the http download links the local files instead of downloading.
static void prepare_http_snapshots(int64_t tablet_id, int32_t schema_hash, int64_t partition_id,
                                   int64_t remote_tablet_id, int32_t remote_schema_hash,
                                   TRemoteTabletSnapshot* remote_snapshot) {
    TCreateTabletReq req = create_tablet(partition_id, tablet_id, schema_hash);
    RuntimeProfile profile("CreateTablet");
    ASSERT_TRUE(engine_ref->create_tablet(req, &profile).ok());
    TabletSharedPtr tablet = engine_ref->tablet_manager()->get_tablet(tablet_id);
    ASSERT_TRUE(tablet != nullptr);
    add_rowset(tablet_id, schema_hash, partition_id, tablet_id * 10, 100);
    auto version = tablet->max_version();

    std::string snapshot_path;
    bool allow_incremental_clone = false;
    TSnapshotRequest snapshot_request;
    snapshot_request.tablet_id = tablet_id;
    snapshot_request.schema_hash = schema_hash;
    snapshot_request.version = version.second;
    ASSERT_TRUE(engine_ref->snapshot_mgr()
                        ->make_snapshot(snapshot_request, &snapshot_path, &allow_incremental_clone)
                        .ok());
    snapshot_path = fmt::format("{}/{}/{}", snapshot_path, tablet_id, schema_hash);

    std::string remote_dir = fmt::format("{}/remote_snapshot_{}", storage_root_path, tablet_id);
    std::string remote_tablet_path =
            fmt::format("{}/{}/{}", remote_dir, remote_tablet_id, remote_schema_hash);
    ASSERT_TRUE(io::global_local_filesystem()->create_directory(remote_tablet_path).ok());
    std::vector<io::FileInfo> snapshot_files;
    bool is_exists = false;
    ASSERT_TRUE(io::global_local_filesystem()
                        ->list(snapshot_path, true, &snapshot_files, &is_exists)
                        .ok());
    for (const auto& file : snapshot_files) {
        ASSERT_TRUE(io::global_local_filesystem()
                            ->copy_path(snapshot_path + "/" + file.file_name,
                                        remote_tablet_path + "/" + file.file_name)
                            .ok());
    }
    ASSERT_TRUE(io::global_local_filesystem()
                        ->rename(fmt::format("{}/{}.hdr", remote_tablet_path, tablet_id),
                                 fmt::format("{}/{}.hdr", remote_tablet_path, remote_tablet_id))
                        .ok());
    auto guards = engine_ref->snapshot_mgr()->convert_rowset_ids(
            remote_tablet_path, remote_tablet_id, 0, 0, partition_id, remote_schema_hash);
    ASSERT_TRUE(guards.has_value());

    remote_snapshot->__set_remote_tablet_id(remote_tablet_id);
    remote_snapshot->__set_local_tablet_id(tablet_id);
    remote_snapshot->__set_local_snapshot_path(snapshot_path);
    remote_snapshot->__set_remote_snapshot_path(remote_tablet_path);
    TNetworkAddress addr;
    addr.hostname = "127.0.0.1";
    addr.port = 1234;
    remote_snapshot->__set_remote_be_addr(addr);
    remote_snapshot->__set_remote_token("fake_token");
}

class ManifestConfigGuard {
public:
    ManifestConfigGuard(bool check, bool digest_check)
            : _check(config::restore_manifest_check),
              _digest_check(config::restore_manifest_digest_check) {
        config::restore_manifest_check = check;
        config::restore_manifest_digest_check = digest_check;
    }
    ~ManifestConfigGuard() {
        config::restore_manifest_check = _check;
        config::restore_manifest_digest_check = _digest_check;
    }

private:
    bool _check;
    bool _digest_check;
};

TEST_F(SnapshotLoaderTest, HttpDownloadCheckManifest) {
    ManifestConfigGuard guard(true, true);
    TRemoteTabletSnapshot remote_snapshot;
    prepare_http_snapshots(1101, 1102, 1103, 1111, 1112, &remote_snapshot);
    ASSERT_FALSE(HasFatalFailure());
    remote_snapshot.__set_manifest_root(
            write_remote_manifest(remote_snapshot.remote_snapshot_path, 1111, true));

    std::vector<int64_t> downloaded_tablet_ids;
    SnapshotLoader loader(*engine_ref, ExecEnv::GetInstance(), 4L, 1101);
    auto status = loader.remote_http_download({remote_snapshot}, &downloaded_tablet_ids);
    ASSERT_TRUE(status.ok()) << status;
    // the linked files are checked too, nothing needs to be downloaded again.
    EXPECT_EQ(0, loader.get_http_download_files_num());
    EXPECT_EQ(std::vector<int64_t> {1101}, loader.manifest_verified_tablets());
    EXPECT_TRUE(loader.manifest_digest_checked());
    // the manifest is next to the snapshot dir, never downloaded into it
    bool exists = true;
    ASSERT_TRUE(io::global_local_filesystem()
                        ->exists(remote_snapshot.local_snapshot_path + "/manifest", &exists)
                        .ok());
    EXPECT_FALSE(exists);
}

// The decomposed digest (rdigest) of a snapshot kept on a remote BE: next to the manifest, fetched
// over the BE http download and checked against the root of the job info.
TEST_F(SnapshotLoaderTest, FetchPrefixDigestFromRemoteBe) {
    const int64_t tablet_id = 2111;
    RestoreDigestDecomposed digest;
    digest.schema_sig = std::string(64, 'a');
    digest.tablet_id = tablet_id;
    digest.base_version = 3;
    digest.root = std::string(64, 'b');
    digest.rows = 2;
    for (int64_t v = 1; v <= 3; ++v) {
        RestoreDigestRowsetPart part;
        part.rowset_id = fmt::format("rowset{}", v);
        part.start_version = part.end_version = v;
        part.buckets[static_cast<size_t>(v)].count = static_cast<uint64_t>(v);
        part.buckets[static_cast<size_t>(v)].sum = static_cast<unsigned __int128>(v) << 100;
        digest.rowsets.push_back(std::move(part));
    }
    RestoreDigestMarkPart mark;
    mark.mark_version = 3;
    mark.buckets[1].count = 1;
    mark.buckets[1].sum = static_cast<unsigned __int128>(1) << 100;
    digest.marks.push_back(std::move(mark));

    std::string remote_dir = fmt::format("{}/remote_prefix_{}", storage_root_path, tablet_id);
    std::string remote_tablet_path = fmt::format("{}/{}/{}", remote_dir, tablet_id, 777);
    ASSERT_TRUE(io::global_local_filesystem()->create_directory(remote_tablet_path).ok());
    std::string root;
    ASSERT_TRUE(write_prefix_digest_file(digest.serialize(),
                                         fmt::format("{}/{}/{}", remote_dir, tablet_id,
                                                     RestoreDigestDecomposed::kLocalFileName),
                                         &root)
                        .ok());
    EXPECT_EQ(digest.file_root(), root);
    EXPECT_EQ("__rdigest__2111." + root.substr(0, 32),
              prefix_digest_remote_file_name(tablet_id, root));

    TRemoteTabletSnapshot remote_snapshot;
    remote_snapshot.__set_remote_tablet_id(tablet_id);
    remote_snapshot.__set_remote_snapshot_path(remote_tablet_path);
    TNetworkAddress addr;
    addr.hostname = "127.0.0.1";
    addr.port = 1234;
    remote_snapshot.__set_remote_be_addr(addr);
    remote_snapshot.__set_remote_token("fake_token");

    RestoreDigestDecomposed fetched;
    auto st = fetch_prefix_digest_from_remote_be(remote_snapshot, root, &fetched);
    ASSERT_TRUE(st.ok()) << st;
    EXPECT_EQ(digest.serialize(), fetched.serialize());
    RestoreDigest composed;
    ASSERT_TRUE(fetched.compose(2, &composed).ok());
    EXPECT_EQ(3U, composed.rows);
    ASSERT_TRUE(fetched.compose(3, &composed).ok());
    EXPECT_EQ(5U, composed.rows);
    EXPECT_TRUE(fetched.compose(0, &composed).is<ErrorCode::NOT_IMPLEMENTED_ERROR>());

    // not the file recorded in the job info
    st = fetch_prefix_digest_from_remote_be(remote_snapshot, std::string(64, '0'), &fetched);
    EXPECT_TRUE(st.is<ErrorCode::RESTORE_MANIFEST_MISMATCH>()) << st;
    // the file of another tablet
    remote_snapshot.__set_remote_tablet_id(tablet_id + 1);
    st = fetch_prefix_digest_from_remote_be(remote_snapshot, root, &fetched);
    EXPECT_TRUE(st.is<ErrorCode::RESTORE_MANIFEST_MISMATCH>()) << st;
    // no file next to the snapshot (a tablet without a decomposed digest)
    remote_snapshot.__set_remote_tablet_id(tablet_id);
    remote_snapshot.__set_remote_snapshot_path(
            fmt::format("{}/remote_prefix_none/{}/{}", storage_root_path, tablet_id, 777));
    st = fetch_prefix_digest_from_remote_be(remote_snapshot, root, &fetched);
    EXPECT_FALSE(st.ok());
}

// The decomposed digest of a snapshot kept on a remote BE is located by the manifest: the manifest is next to the
// snapshot dir, checked against the manifest root, and the SHA-256 of rdigest is the one in it.
TEST_F(SnapshotLoaderTest, FetchPrefixDigestFromRemoteBeByManifest) {
    const int64_t tablet_id = 2211;
    RestoreDigestDecomposed digest;
    digest.schema_sig = std::string(64, 'a');
    digest.tablet_id = tablet_id;
    digest.base_version = 1;
    digest.root = std::string(64, 'b');
    digest.rows = 1;
    RestoreDigestRowsetPart part;
    part.rowset_id = "rowset1";
    part.start_version = part.end_version = 1;
    part.buckets[1].count = 1;
    part.buckets[1].sum = static_cast<unsigned __int128>(1) << 100;
    digest.rowsets.push_back(std::move(part));

    std::string remote_dir =
            fmt::format("{}/remote_prefix_manifest_{}", storage_root_path, tablet_id);
    std::string remote_tablet_path = fmt::format("{}/{}/{}", remote_dir, tablet_id, 777);
    ASSERT_TRUE(io::global_local_filesystem()->create_directory(remote_tablet_path).ok());
    std::string content = digest.serialize();
    std::string root;
    ASSERT_TRUE(write_prefix_digest_file(content,
                                         fmt::format("{}/{}/{}", remote_dir, tablet_id,
                                                     RestoreDigestDecomposed::kLocalFileName),
                                         &root)
                        .ok());
    SnapshotManifest manifest;
    manifest.tablet_id = tablet_id;
    manifest.files = {
            SnapshotManifestFile {.name = "rs1_0.dat", .size = 7, .md5 = "", .sha256 = ""}};
    manifest.prefix_digest_sha256 = root;
    manifest.prefix_digest_size = static_cast<int64_t>(content.size());
    std::string manifest_root;
    ASSERT_TRUE(write_snapshot_manifest(manifest,
                                        fmt::format("{}/{}/{}", remote_dir, tablet_id,
                                                    SnapshotManifest::kLocalFileName),
                                        &manifest_root)
                        .ok());

    TRemoteTabletSnapshot remote_snapshot;
    remote_snapshot.__set_remote_tablet_id(tablet_id);
    remote_snapshot.__set_remote_snapshot_path(remote_tablet_path);
    TNetworkAddress addr;
    addr.hostname = "127.0.0.1";
    addr.port = 1234;
    remote_snapshot.__set_remote_be_addr(addr);
    remote_snapshot.__set_remote_token("fake_token");

    RestoreDigestDecomposed fetched;
    auto st = fetch_prefix_digest_from_remote_be_by_manifest(remote_snapshot, manifest_root,
                                                             &fetched);
    ASSERT_TRUE(st.ok()) << st;
    EXPECT_EQ(content, fetched.serialize());
    // the root of the rdigest file is not the root of the manifest
    EXPECT_NE(root, manifest_root);
    st = fetch_prefix_digest_from_remote_be_by_manifest(remote_snapshot, root, &fetched);
    EXPECT_TRUE(st.is<ErrorCode::RESTORE_MANIFEST_MISMATCH>()) << st;
    // not the manifest recorded in the job info
    st = fetch_prefix_digest_from_remote_be_by_manifest(remote_snapshot, std::string(64, '0'),
                                                        &fetched);
    EXPECT_TRUE(st.is<ErrorCode::RESTORE_MANIFEST_MISMATCH>()) << st;

    // rdigest is not the file the manifest has: the SHA-256 in the manifest decides
    SnapshotManifest other = manifest;
    other.prefix_digest_sha256 = std::string(64, '1');
    std::string other_root;
    ASSERT_TRUE(write_snapshot_manifest(other,
                                        fmt::format("{}/{}/{}", remote_dir, tablet_id,
                                                    SnapshotManifest::kLocalFileName),
                                        &other_root)
                        .ok());
    st = fetch_prefix_digest_from_remote_be_by_manifest(remote_snapshot, other_root, &fetched);
    EXPECT_TRUE(st.is<ErrorCode::RESTORE_MANIFEST_MISMATCH>()) << st;

    // a manifest without the entry (a tablet without a decomposed digest): an error, not a mismatch
    SnapshotManifest none = manifest;
    none.prefix_digest_sha256.clear();
    none.prefix_digest_size = 0;
    std::string none_root;
    ASSERT_TRUE(write_snapshot_manifest(none,
                                        fmt::format("{}/{}/{}", remote_dir, tablet_id,
                                                    SnapshotManifest::kLocalFileName),
                                        &none_root)
                        .ok());
    st = fetch_prefix_digest_from_remote_be_by_manifest(remote_snapshot, none_root, &fetched);
    EXPECT_FALSE(st.ok());
    EXPECT_FALSE(st.is<ErrorCode::RESTORE_MANIFEST_MISMATCH>()) << st;
}

TEST_F(SnapshotLoaderTest, HttpDownloadWithoutManifestOrCheckDisabled) {
    TRemoteTabletSnapshot remote_snapshot;
    prepare_http_snapshots(1201, 1202, 1203, 1211, 1212, &remote_snapshot);
    ASSERT_FALSE(HasFatalFailure());
    write_remote_manifest(remote_snapshot.remote_snapshot_path, 1211, false);

    {
        // disabled: behaves as before, the manifest root is ignored, even a wrong one
        ManifestConfigGuard guard(false, true);
        remote_snapshot.__set_manifest_root(std::string(64, '0'));
        std::vector<int64_t> downloaded_tablet_ids;
        SnapshotLoader loader(*engine_ref, ExecEnv::GetInstance(), 5L, 1201);
        auto status = loader.remote_http_download({remote_snapshot}, &downloaded_tablet_ids);
        ASSERT_TRUE(status.ok()) << status;
        EXPECT_EQ(0, loader.get_http_download_files_num());
        EXPECT_TRUE(loader.manifest_verified_tablets().empty());
        EXPECT_FALSE(loader.manifest_digest_checked());
    }
    {
        // no manifest root in the request (old FE or old backup): not checked
        ManifestConfigGuard guard(true, true);
        remote_snapshot.__isset.manifest_root = false;
        std::vector<int64_t> downloaded_tablet_ids;
        SnapshotLoader loader(*engine_ref, ExecEnv::GetInstance(), 6L, 1201);
        auto status = loader.remote_http_download({remote_snapshot}, &downloaded_tablet_ids);
        ASSERT_TRUE(status.ok()) << status;
        EXPECT_TRUE(loader.manifest_verified_tablets().empty());
    }
}

TEST_F(SnapshotLoaderTest, HttpDownloadManifestRootMismatch) {
    ManifestConfigGuard guard(true, false);
    TRemoteTabletSnapshot remote_snapshot;
    prepare_http_snapshots(1601, 1602, 1603, 1611, 1612, &remote_snapshot);
    ASSERT_FALSE(HasFatalFailure());
    std::string root = write_remote_manifest(remote_snapshot.remote_snapshot_path, 1611, false);
    // the manifest file is not the one recorded by the backup
    std::string wrong_root = root;
    wrong_root[0] = wrong_root[0] == 'a' ? 'b' : 'a';
    remote_snapshot.__set_manifest_root(wrong_root);

    std::vector<int64_t> downloaded_tablet_ids;
    SnapshotLoader loader(*engine_ref, ExecEnv::GetInstance(), 10L, 1601);
    auto status = loader.remote_http_download({remote_snapshot}, &downloaded_tablet_ids);
    ASSERT_TRUE(status.is<ErrorCode::RESTORE_MANIFEST_MISMATCH>()) << status;
    EXPECT_NE(status.to_string().find("does not match the root, expected " + wrong_root +
                                      ", actual " + root),
              std::string::npos)
            << status;
    EXPECT_TRUE(loader.manifest_verified_tablets().empty());
    // reported to FE as an internal error with the name of the code as the prefix of the message
    ASSERT_TRUE(status.is<ErrorCode::RESTORE_MANIFEST_MISMATCH>()) << status;
    EXPECT_EQ(TStatusCode::INTERNAL_ERROR, status.to_thrift().status_code);
    EXPECT_NE(status.to_thrift().error_msgs[0].find("[RESTORE_MANIFEST_MISMATCH]"),
              std::string::npos);
}

TEST_F(SnapshotLoaderTest, HttpDownloadIgnoresRemoteFilesNotInManifest) {
    ManifestConfigGuard guard(true, false);
    TRemoteTabletSnapshot remote_snapshot;
    prepare_http_snapshots(1701, 1702, 1703, 1711, 1712, &remote_snapshot);
    ASSERT_FALSE(HasFatalFailure());
    remote_snapshot.__set_manifest_root(
            write_remote_manifest(remote_snapshot.remote_snapshot_path, 1711, false));
    // an orphan file in the remote snapshot dir, not in the manifest
    std::string orphan = "0200000000000000ffffffffffffffffffffffffffffffff_0.dat";
    write_test_file(remote_snapshot.remote_snapshot_path + "/" + orphan, "orphan");

    std::vector<int64_t> downloaded_tablet_ids;
    SnapshotLoader loader(*engine_ref, ExecEnv::GetInstance(), 11L, 1701);
    auto status = loader.remote_http_download({remote_snapshot}, &downloaded_tablet_ids);
    ASSERT_TRUE(status.ok()) << status;
    EXPECT_EQ(std::vector<int64_t> {1701}, loader.manifest_verified_tablets());
    bool exists = true;
    ASSERT_TRUE(io::global_local_filesystem()
                        ->exists(remote_snapshot.local_snapshot_path + "/" + orphan, &exists)
                        .ok());
    EXPECT_FALSE(exists);
}

TEST_F(SnapshotLoaderTest, HttpDownloadManifestMismatchRetryFails) {
    ManifestConfigGuard guard(true, false);
    TRemoteTabletSnapshot remote_snapshot;
    prepare_http_snapshots(1301, 1302, 1303, 1311, 1312, &remote_snapshot);
    ASSERT_FALSE(HasFatalFailure());
    // the manifest records a wrong size of a segment file
    remote_snapshot.__set_manifest_root(
            write_remote_manifest(remote_snapshot.remote_snapshot_path, 1311, false, 1));

    std::vector<int64_t> downloaded_tablet_ids;
    SnapshotLoader loader(*engine_ref, ExecEnv::GetInstance(), 7L, 1301);
    auto status = loader.remote_http_download({remote_snapshot}, &downloaded_tablet_ids);
    ASSERT_TRUE(status.is<ErrorCode::RESTORE_MANIFEST_MISMATCH>()) << status;
    EXPECT_NE(status.to_string().find("size mismatch of file"), std::string::npos) << status;
    // retried without reusing local files: all the files are downloaded
    EXPECT_GT(loader.get_http_download_files_num(), 0);
    EXPECT_TRUE(loader.manifest_verified_tablets().empty());
    ASSERT_TRUE(status.is<ErrorCode::RESTORE_MANIFEST_MISMATCH>()) << status;
    EXPECT_NE(status.to_thrift().error_msgs[0].find("[RESTORE_MANIFEST_MISMATCH]"),
              std::string::npos);
}

TEST_F(SnapshotLoaderTest, HttpDownloadLinkedFileTamperedRetrySucceeds) {
    ManifestConfigGuard guard(true, true);
    TRemoteTabletSnapshot remote_snapshot;
    prepare_http_snapshots(1401, 1402, 1403, 1411, 1412, &remote_snapshot);
    ASSERT_FALSE(HasFatalFailure());
    remote_snapshot.__set_manifest_root(
            write_remote_manifest(remote_snapshot.remote_snapshot_path, 1411, true));

    // Tamper a local segment file which will be linked instead of downloaded: same size, different
    // content. Replace the file instead of writing in place, to keep the tablet data intact.
    const std::string& local_path = remote_snapshot.local_snapshot_path;
    std::vector<io::FileInfo> local_files;
    bool exists = false;
    ASSERT_TRUE(io::global_local_filesystem()->list(local_path, true, &local_files, &exists).ok());
    std::string tampered;
    int64_t tampered_size = 0;
    for (const auto& file : local_files) {
        if (file.file_name.ends_with(".dat") && file.file_size > 0) {
            tampered = file.file_name;
            tampered_size = file.file_size;
            break;
        }
    }
    ASSERT_FALSE(tampered.empty());
    std::string tmp = local_path + "/tampered.tmp";
    write_test_file(tmp, std::string(tampered_size, 'z'));
    ASSERT_TRUE(io::global_local_filesystem()->rename(tmp, local_path + "/" + tampered).ok());

    std::vector<int64_t> downloaded_tablet_ids;
    SnapshotLoader loader(*engine_ref, ExecEnv::GetInstance(), 8L, 1401);
    auto status = loader.remote_http_download({remote_snapshot}, &downloaded_tablet_ids);
    ASSERT_TRUE(status.ok()) << status;
    // the linked file is caught by the digest check, and the retry downloads all the files
    EXPECT_GT(loader.get_http_download_files_num(), 0);
    EXPECT_EQ(std::vector<int64_t> {1401}, loader.manifest_verified_tablets());
    EXPECT_TRUE(loader.manifest_digest_checked());
}

TEST_F(SnapshotLoaderTest, HttpDownloadLinkedFileTamperedWithoutDigestCheck) {
    // With only the size check, a same size tampered linked file is not detected: the digest
    // check is off by default, see the design.
    ManifestConfigGuard guard(true, false);
    TRemoteTabletSnapshot remote_snapshot;
    prepare_http_snapshots(1501, 1502, 1503, 1511, 1512, &remote_snapshot);
    ASSERT_FALSE(HasFatalFailure());
    remote_snapshot.__set_manifest_root(
            write_remote_manifest(remote_snapshot.remote_snapshot_path, 1511, true));

    std::vector<int64_t> downloaded_tablet_ids;
    SnapshotLoader loader(*engine_ref, ExecEnv::GetInstance(), 9L, 1501);
    auto status = loader.remote_http_download({remote_snapshot}, &downloaded_tablet_ids);
    ASSERT_TRUE(status.ok()) << status;
    EXPECT_EQ(0, loader.get_http_download_files_num());
    EXPECT_EQ(std::vector<int64_t> {1501}, loader.manifest_verified_tablets());
    EXPECT_FALSE(loader.manifest_digest_checked());
}

// ---- download stats ----

// The number and the total size of the data files (all but the tablet meta file) in a dir.
static void data_files_of(const std::string& dir, int64_t* num, int64_t* bytes) {
    std::vector<io::FileInfo> files;
    bool exists = false;
    ASSERT_TRUE(io::global_local_filesystem()->list(dir, true, &files, &exists).ok());
    *num = 0;
    *bytes = 0;
    for (const auto& file : files) {
        if (!file.file_name.ends_with(".hdr")) {
            ++*num;
            *bytes += file.file_size;
        }
    }
}

// Change a tablet meta file in place.
static void edit_tablet_meta(const std::string& path,
                             const std::function<void(TabletMetaPB*)>& edit) {
    TabletMetaPB meta;
    ASSERT_TRUE(TabletMeta::load_from_file(path, &meta).ok());
    edit(&meta);
    ASSERT_TRUE(TabletMeta::save(path, meta).ok());
}

static RowsetMetaPB* first_rowset_with_segments(TabletMetaPB* meta) {
    for (auto& rs_meta : *meta->mutable_rs_metas()) {
        if (rs_meta.num_segments() > 0) {
            return &rs_meta;
        }
    }
    return nullptr;
}

static RowsetMetaPB make_rowset_meta(const std::string& id, int64_t start, int64_t end,
                                     const std::string& source = "", bool remote = false,
                                     int64_t num_segments = 1) {
    RowsetMetaPB meta;
    meta.set_rowset_id_v2(id);
    meta.set_start_version(start);
    meta.set_end_version(end);
    meta.set_num_segments(num_segments);
    if (!source.empty()) {
        meta.set_source_rowset_id(source);
    }
    if (remote) {
        meta.set_resource_id("resource");
    }
    return meta;
}

TEST_F(SnapshotLoaderTest, HttpDownloadStatsLinked) {
    TRemoteTabletSnapshot remote_snapshot;
    prepare_http_snapshots(2601, 2602, 2603, 2611, 2612, &remote_snapshot);
    ASSERT_FALSE(HasFatalFailure());
    int64_t files = 0;
    int64_t bytes = 0;
    data_files_of(remote_snapshot.remote_snapshot_path, &files, &bytes);
    ASSERT_GT(files, 0);

    std::vector<int64_t> downloaded_tablet_ids;
    SnapshotLoader loader(*engine_ref, ExecEnv::GetInstance(), 20L, 2601);
    ASSERT_TRUE(loader.remote_http_download({remote_snapshot}, &downloaded_tablet_ids).ok());
    const auto& stats = loader.download_stats();
    EXPECT_EQ(files, stats.linked_files);
    EXPECT_EQ(bytes, stats.linked_bytes);
    EXPECT_EQ(0, stats.skipped_files);
    EXPECT_EQ(0, stats.skipped_bytes);
    EXPECT_EQ(0, stats.downloaded_files);
    EXPECT_EQ(0, stats.downloaded_bytes);
    EXPECT_EQ(1, stats.tablets_full_reuse);
    EXPECT_EQ(0, stats.tablets_partial_reuse);
    EXPECT_EQ(0, stats.tablets_no_reuse);
    EXPECT_EQ(0, stats.unmatched_rowsets);

    // the thrift struct carries all the fields
    TDownloadStats thrift_stats = stats.to_thrift();
    EXPECT_EQ(files, thrift_stats.linked_files);
    EXPECT_EQ(bytes, thrift_stats.linked_bytes);
    EXPECT_EQ(1, thrift_stats.tablets_full_reuse);
}

TEST_F(SnapshotLoaderTest, HttpDownloadStatsSkippedAndDownloaded) {
    TRemoteTabletSnapshot remote_snapshot;
    prepare_http_snapshots(2701, 2702, 2703, 2711, 2712, &remote_snapshot);
    ASSERT_FALSE(HasFatalFailure());
    int64_t files = 0;
    int64_t bytes = 0;
    data_files_of(remote_snapshot.remote_snapshot_path, &files, &bytes);
    // a file which exists in both, and a file which only exists in the remote.
    write_test_file(remote_snapshot.remote_snapshot_path + "/skip_0.dat", "0123456789");
    write_test_file(remote_snapshot.local_snapshot_path + "/skip_0.dat", "0123456789");
    write_test_file(remote_snapshot.remote_snapshot_path + "/download_0.dat",
                    "abcdefghijklmnopqrst");
    ASSERT_FALSE(HasFatalFailure());

    std::vector<int64_t> downloaded_tablet_ids;
    SnapshotLoader loader(*engine_ref, ExecEnv::GetInstance(), 21L, 2701);
    auto status = loader.remote_http_download({remote_snapshot}, &downloaded_tablet_ids);
    ASSERT_TRUE(status.ok()) << status;
    const auto& stats = loader.download_stats();
    EXPECT_EQ(files, stats.linked_files);
    EXPECT_EQ(bytes, stats.linked_bytes);
    EXPECT_EQ(1, stats.skipped_files);
    EXPECT_EQ(10, stats.skipped_bytes);
    EXPECT_EQ(1, stats.downloaded_files);
    EXPECT_EQ(20, stats.downloaded_bytes);
    EXPECT_EQ(1, loader.get_http_download_files_num());
    // partly reused
    EXPECT_EQ(0, stats.tablets_full_reuse);
    EXPECT_EQ(1, stats.tablets_partial_reuse);
    EXPECT_EQ(0, stats.tablets_no_reuse);
    EXPECT_EQ(0, stats.unmatched_rowsets);
}

// Download a "remote" snapshot made from the local tablet, after changing the tablet meta of the local or the
// remote snapshot. Returns the stats of the download.
static SnapshotDownloadStats download_with_edited_meta(
        int64_t tablet_id, int64_t remote_tablet_id, int64_t task_id,
        const std::function<void(TabletMetaPB*)>& edit_local,
        const std::function<void(TabletMetaPB*)>& edit_remote, int64_t* downloaded_bytes) {
    TRemoteTabletSnapshot remote_snapshot;
    prepare_http_snapshots(tablet_id, tablet_id + 1, tablet_id + 2, remote_tablet_id,
                           remote_tablet_id + 1, &remote_snapshot);
    if (::testing::Test::HasFatalFailure()) {
        return {};
    }
    int64_t files = 0;
    data_files_of(remote_snapshot.remote_snapshot_path, &files, downloaded_bytes);
    if (edit_local) {
        edit_tablet_meta(fmt::format("{}/{}.hdr", remote_snapshot.local_snapshot_path, tablet_id),
                         edit_local);
    }
    if (edit_remote) {
        edit_tablet_meta(
                fmt::format("{}/{}.hdr", remote_snapshot.remote_snapshot_path, remote_tablet_id),
                edit_remote);
    }
    std::vector<int64_t> downloaded_tablet_ids;
    SnapshotLoader loader(*engine_ref, ExecEnv::GetInstance(), task_id, tablet_id);
    auto status = loader.remote_http_download({remote_snapshot}, &downloaded_tablet_ids);
    EXPECT_TRUE(status.ok()) << status;
    return loader.download_stats();
}

TEST_F(SnapshotLoaderTest, HttpDownloadStatsUnmatchedNoSourceRowsetId) {
    // the remote rowset has no source, and the local rowset has no source either: no lineage at all.
    int64_t bytes = 0;
    auto stats = download_with_edited_meta(
            2801, 2811, 22L, nullptr,
            [](TabletMetaPB* meta) { first_rowset_with_segments(meta)->clear_source_rowset_id(); },
            &bytes);
    ASSERT_FALSE(HasFatalFailure());
    EXPECT_EQ(1, stats.unmatched_rowsets);
    EXPECT_EQ(1, stats.unmatched_no_source_rowset_id);
    EXPECT_EQ(0, stats.unmatched_source_not_in_snapshot);
    EXPECT_EQ(0, stats.unmatched_version_mismatch);
    // nothing reused, everything is downloaded
    EXPECT_EQ(0, stats.linked_files);
    EXPECT_EQ(0, stats.skipped_files);
    EXPECT_GT(stats.downloaded_files, 0);
    EXPECT_EQ(bytes, stats.downloaded_bytes);
    EXPECT_EQ(1, stats.tablets_no_reuse);
}

TEST_F(SnapshotLoaderTest, HttpDownloadStatsUnmatchedSourceNotInSnapshot) {
    // the local rowset was downloaded from another rowset, which is not in the remote snapshot.
    int64_t bytes = 0;
    auto stats = download_with_edited_meta(
            2901, 2911, 23L,
            [](TabletMetaPB* meta) {
                first_rowset_with_segments(meta)->set_source_rowset_id("another_rowset");
            },
            [](TabletMetaPB* meta) { first_rowset_with_segments(meta)->clear_source_rowset_id(); },
            &bytes);
    ASSERT_FALSE(HasFatalFailure());
    EXPECT_EQ(1, stats.unmatched_rowsets);
    EXPECT_EQ(0, stats.unmatched_no_source_rowset_id);
    EXPECT_EQ(1, stats.unmatched_source_not_in_snapshot);
    EXPECT_EQ(0, stats.unmatched_version_mismatch);
    EXPECT_EQ(0, stats.linked_files);
    EXPECT_EQ(bytes, stats.downloaded_bytes);
    EXPECT_EQ(1, stats.tablets_no_reuse);
}

TEST_F(SnapshotLoaderTest, HttpDownloadStatsUnmatchedVersionMismatch) {
    // the remote rowset is derived from the local rowset, but with another version range.
    int64_t bytes = 0;
    auto stats = download_with_edited_meta(
            3001, 3011, 24L, nullptr,
            [](TabletMetaPB* meta) {
                auto* rs_meta = first_rowset_with_segments(meta);
                rs_meta->set_end_version(rs_meta->end_version() + 1);
            },
            &bytes);
    ASSERT_FALSE(HasFatalFailure());
    EXPECT_EQ(1, stats.unmatched_rowsets);
    EXPECT_EQ(0, stats.unmatched_no_source_rowset_id);
    EXPECT_EQ(0, stats.unmatched_source_not_in_snapshot);
    EXPECT_EQ(1, stats.unmatched_version_mismatch);
    EXPECT_EQ(0, stats.linked_files);
    EXPECT_EQ(bytes, stats.downloaded_bytes);
    EXPECT_EQ(1, stats.tablets_no_reuse);
}

TEST_F(SnapshotLoaderTest, CountUnmatchedRowsets) {
    TabletMetaPB local;
    TabletMetaPB remote;
    // local: l1 [2,2] derived from r1, l2 [3,3] derived from a rowset not in the remote, l3 [4,4] without
    // source, l4 [5,5] derived from r5, l5 [6,6] is empty, l6 is in remote storage.
    *local.add_rs_metas() = make_rowset_meta("l1", 2, 2, "r1");
    *local.add_rs_metas() = make_rowset_meta("l2", 3, 3, "gone");
    *local.add_rs_metas() = make_rowset_meta("l3", 4, 4);
    *local.add_rs_metas() = make_rowset_meta("l4", 5, 5, "r5");
    *local.add_rs_metas() = make_rowset_meta("l5", 6, 6, "r6", false, 0);
    *local.add_rs_metas() = make_rowset_meta("l6", 7, 7, "r7", true);
    // remote:
    // r0 [0,1] empty, not counted
    *remote.add_rs_metas() = make_rowset_meta("r0", 0, 1, "", false, 0);
    // r1 [2,2] matched: local l1 is derived from it
    *remote.add_rs_metas() = make_rowset_meta("r1", 2, 2);
    // r2 [3,3] unmatched, overlaps l2 which has another source: source_not_in_snapshot
    *remote.add_rs_metas() = make_rowset_meta("r2", 3, 3);
    // r3 [4,4] unmatched, overlaps l3 which has no source: no_source_rowset_id
    *remote.add_rs_metas() = make_rowset_meta("r3", 4, 4);
    // r4 [8,9] unmatched, no local rowset overlaps: no_source_rowset_id
    *remote.add_rs_metas() = make_rowset_meta("r4", 8, 9);
    // r5 [5,6] derived from the same id but another version: version_mismatch
    *remote.add_rs_metas() = make_rowset_meta("r5", 5, 6);
    // r6 [6,6] derived from l5 which is empty, so not a lineage match, and no local rowset with segments
    // overlaps it: no_source_rowset_id
    *remote.add_rs_metas() = make_rowset_meta("r6", 6, 6, "l5");
    // r7 in remote storage, not counted
    *remote.add_rs_metas() = make_rowset_meta("r7", 7, 7, "", true);
    // r8 [10,10] derived from l1 but another version: version_mismatch, found by the remote source
    *remote.add_rs_metas() = make_rowset_meta("r8", 10, 10, "l1");

    SnapshotDownloadStats stats;
    count_unmatched_rowsets(local, remote, &stats);
    // r2, r3, r4, r5, r6, r8
    EXPECT_EQ(6, stats.unmatched_rowsets);
    EXPECT_EQ(1, stats.unmatched_source_not_in_snapshot); // r2
    EXPECT_EQ(3, stats.unmatched_no_source_rowset_id);    // r3, r4, r6
    EXPECT_EQ(2, stats.unmatched_version_mismatch);       // r5, r8
    EXPECT_EQ(stats.unmatched_rowsets, stats.unmatched_no_source_rowset_id +
                                               stats.unmatched_source_not_in_snapshot +
                                               stats.unmatched_version_mismatch);
}

TEST_F(SnapshotLoaderTest, DownloadStatsClassifyAndMerge) {
    SnapshotDownloadStats full;
    full.linked_files = 2;
    full.linked_bytes = 20;
    full.classify_tablet();
    SnapshotDownloadStats partial;
    partial.skipped_files = 1;
    partial.skipped_bytes = 5;
    partial.downloaded_files = 1;
    partial.downloaded_bytes = 7;
    partial.unmatched_rowsets = 1;
    partial.unmatched_version_mismatch = 1;
    partial.classify_tablet();
    SnapshotDownloadStats none;
    none.downloaded_files = 3;
    none.downloaded_bytes = 30;
    none.classify_tablet();
    // a tablet without data files is not counted
    SnapshotDownloadStats empty;
    empty.classify_tablet();

    SnapshotDownloadStats total;
    for (const auto& stats : {full, partial, none, empty}) {
        total.merge(stats);
    }
    EXPECT_EQ(2, total.linked_files);
    EXPECT_EQ(20, total.linked_bytes);
    EXPECT_EQ(1, total.skipped_files);
    EXPECT_EQ(5, total.skipped_bytes);
    EXPECT_EQ(4, total.downloaded_files);
    EXPECT_EQ(37, total.downloaded_bytes);
    EXPECT_EQ(1, total.tablets_full_reuse);
    EXPECT_EQ(1, total.tablets_partial_reuse);
    EXPECT_EQ(1, total.tablets_no_reuse);
    EXPECT_EQ(1, total.unmatched_rowsets);
    EXPECT_EQ(1, total.unmatched_version_mismatch);
    EXPECT_FALSE(total.to_string().empty());
}

// ---------------------------------------------------------------------------------------------
// The snapshot which holds only an increment: its tablet meta is "<tablet id>.hdr.increment", and there is no
// "<tablet id>.hdr" (an old version then fails to move it instead of replacing the tablet with the increment).

static bool file_exists(const std::string& path) {
    bool exists = false;
    EXPECT_TRUE(io::global_local_filesystem()->exists(path, &exists).ok());
    return exists;
}

// The range (base, end] of the last rowset of the tablet meta file.
static TRestoreIncrementalRange last_rowset_range(const std::string& hdr) {
    TabletMetaPB meta;
    EXPECT_TRUE(TabletMeta::load_from_file(hdr, &meta).ok());
    int64_t start = 0;
    int64_t end = 0;
    for (const auto& rs : meta.rs_metas()) {
        if (rs.end_version() >= end) {
            start = rs.start_version();
            end = rs.end_version();
        }
    }
    TRestoreIncrementalRange range;
    range.base_version = start - 1;
    range.end_version = end;
    return range;
}

static void expect_increment_only_dir(const std::string& dir, int64_t tablet_id,
                                      const TRestoreIncrementalRange& range) {
    EXPECT_FALSE(file_exists(fmt::format("{}/{}.hdr", dir, tablet_id)));
    ASSERT_TRUE(file_exists(fmt::format("{}/{}.hdr.increment", dir, tablet_id)));
    TabletMetaPB increment;
    ASSERT_TRUE(TabletMeta::load_from_file(fmt::format("{}/{}.hdr.increment", dir, tablet_id),
                                           &increment)
                        .ok());
    ASSERT_EQ(1, increment.rs_metas_size());
    EXPECT_EQ(range.base_version + 1, increment.rs_metas(0).start_version());
    EXPECT_EQ(range.end_version, increment.rs_metas(0).end_version());
}

TEST_F(SnapshotLoaderTest, HttpDownloadOfAnIncrementLeavesOnlyTheIncrementHdr) {
    ManifestConfigGuard guard(true, false);
    TRemoteTabletSnapshot remote_snapshot;
    prepare_http_snapshots(3101, 3102, 3103, 3111, 3112, &remote_snapshot);
    ASSERT_FALSE(HasFatalFailure());
    remote_snapshot.__set_manifest_root(
            write_remote_manifest(remote_snapshot.remote_snapshot_path, 3111, false));
    TRestoreIncrementalRange range =
            last_rowset_range(fmt::format("{}/{}.hdr", remote_snapshot.remote_snapshot_path, 3111));
    ASSERT_GE(range.base_version, 1);
    remote_snapshot.__set_incremental(range);

    // the second round is a resent task: the increment tablet meta of the first one is still in the dir, which is
    // neither a mismatch with the manifest nor a reason to fail
    for (int round = 0; round < 2; ++round) {
        std::vector<int64_t> downloaded_tablet_ids;
        SnapshotLoader loader(*engine_ref, ExecEnv::GetInstance(), 31L + round, 3101);
        auto status = loader.remote_http_download({remote_snapshot}, &downloaded_tablet_ids);
        ASSERT_TRUE(status.ok()) << "round " << round << ": " << status;
        expect_increment_only_dir(remote_snapshot.local_snapshot_path, 3101, range);
        EXPECT_EQ(std::vector<int64_t> {3101}, loader.manifest_verified_tablets());
        EXPECT_EQ(1, loader.download_stats().tablets_incremental);
    }
}

// the download of a whole tablet keeps the name of the tablet meta file
TEST_F(SnapshotLoaderTest, HttpDownloadOfAWholeTabletKeepsTheHdrName) {
    ManifestConfigGuard guard(true, false);
    TRemoteTabletSnapshot remote_snapshot;
    prepare_http_snapshots(3201, 3202, 3203, 3211, 3212, &remote_snapshot);
    ASSERT_FALSE(HasFatalFailure());
    remote_snapshot.__set_manifest_root(
            write_remote_manifest(remote_snapshot.remote_snapshot_path, 3211, false));
    std::vector<int64_t> downloaded_tablet_ids;
    SnapshotLoader loader(*engine_ref, ExecEnv::GetInstance(), 32L, 3201);
    auto status = loader.remote_http_download({remote_snapshot}, &downloaded_tablet_ids);
    ASSERT_TRUE(status.ok()) << status;
    EXPECT_TRUE(file_exists(remote_snapshot.local_snapshot_path + "/3201.hdr"));
    EXPECT_FALSE(file_exists(remote_snapshot.local_snapshot_path + "/3201.hdr.increment"));
}

// The same snapshot dir was used by an increment before: the tablet meta of the increment is deleted by the whole
// download (it is no file of the remote snapshot), so that the move is not refused.
TEST_F(SnapshotLoaderTest, HttpWholeDownloadDeletesTheLeftoverIncrementHdr) {
    ManifestConfigGuard guard(true, false);
    TRemoteTabletSnapshot remote_snapshot;
    prepare_http_snapshots(3401, 3402, 3403, 3411, 3412, &remote_snapshot);
    ASSERT_FALSE(HasFatalFailure());
    remote_snapshot.__set_manifest_root(
            write_remote_manifest(remote_snapshot.remote_snapshot_path, 3411, false));
    const std::string leftover = remote_snapshot.local_snapshot_path + "/3401.hdr.increment";
    write_test_file(leftover, "the increment of an earlier task");
    ASSERT_TRUE(file_exists(leftover));

    std::vector<int64_t> downloaded_tablet_ids;
    SnapshotLoader loader(*engine_ref, ExecEnv::GetInstance(), 34L, 3401);
    auto status = loader.remote_http_download({remote_snapshot}, &downloaded_tablet_ids);
    ASSERT_TRUE(status.ok()) << status;
    EXPECT_FALSE(file_exists(leftover));
    EXPECT_TRUE(file_exists(remote_snapshot.local_snapshot_path + "/3401.hdr"));

    TabletSharedPtr tablet = engine_ref->tablet_manager()->get_tablet(3401);
    ASSERT_TRUE(tablet != nullptr);
    status = loader.move(remote_snapshot.local_snapshot_path, tablet, true);
    EXPECT_TRUE(status.ok()) << status;

    // the helper: no file is fine
    EXPECT_TRUE(remove_increment_hdr(remote_snapshot.local_snapshot_path, 3401).ok());
}

// A leftover tablet meta of an increment is not a file to check against the manifest, like the loaded tag.
TEST_F(SnapshotLoaderTest, ManifestCheckIgnoresTheIncrementHdr) {
    std::string dir = fmt::format("{}/manifest_check_increment_hdr", storage_root_path);
    ASSERT_TRUE(io::global_local_filesystem()->delete_directory(dir).ok());
    ASSERT_TRUE(io::global_local_filesystem()->create_directory(dir).ok());
    write_test_file(dir + "/4001.hdr", "header");
    write_test_file(dir + "/4001.hdr.increment", "the increment");
    SnapshotManifest manifest;
    manifest.tablet_id = 4001;
    manifest.files = {
            SnapshotManifestFile {.name = "4001.hdr", .size = 6, .md5 = "", .sha256 = ""}};
    ManifestCheckResult result;
    Status st = check_tablet_snapshot_manifest(dir, 4001, manifest, false, &result);
    EXPECT_TRUE(st.ok()) << st;
    EXPECT_TRUE(result.checked);
    // a file of another name is still unexpected
    write_test_file(dir + "/4001.hdr.other", "x");
    st = check_tablet_snapshot_manifest(dir, 4001, manifest, false, &result);
    EXPECT_TRUE(st.is<ErrorCode::RESTORE_MANIFEST_MISMATCH>()) << st;
}

// The mismatch with the manifest is an internal error code, an old FE understands it as INTERNAL_ERROR, and the
// current FE recognizes it by the prefix of the message.
TEST_F(SnapshotLoaderTest, ManifestMismatchIsReportedAsInternalErrorWithPrefix) {
    Status st = Status::Error<ErrorCode::RESTORE_MANIFEST_MISMATCH, false>(
            "size mismatch of file {}", "1.dat");
    ASSERT_TRUE(st.is<ErrorCode::RESTORE_MANIFEST_MISMATCH>());
    EXPECT_LT(ErrorCode::RESTORE_MANIFEST_MISMATCH, 0);
    TStatus thrift = st.to_thrift();
    EXPECT_EQ(TStatusCode::INTERNAL_ERROR, thrift.status_code);
    ASSERT_EQ(1, thrift.error_msgs.size());
    EXPECT_NE(thrift.error_msgs[0].find("[RESTORE_MANIFEST_MISMATCH]size mismatch of file 1.dat"),
              std::string::npos)
            << thrift.error_msgs[0];
    EXPECT_NE(st.to_string().find("[RESTORE_MANIFEST_MISMATCH]"), std::string::npos);
    // the Status read back from the thrift one is a plain internal error
    EXPECT_FALSE(Status::create<false>(thrift).is<ErrorCode::RESTORE_MANIFEST_MISMATCH>());
}

// A remote file system over the local disk, only to download from: <remote path> is a local path.
class LocalDirRemoteFileSystem : public io::RemoteFileSystem {
public:
    LocalDirRemoteFileSystem() : RemoteFileSystem("", "local_dir_remote", io::FileSystemType::S3) {}

protected:
    Status create_file_impl(const io::Path&, io::FileWriterPtr*,
                            const io::FileWriterOptions*) override {
        return Status::NotSupported("create_file");
    }
    Status create_directory_impl(const io::Path&, bool) override {
        return Status::NotSupported("create_directory");
    }
    Status delete_file_impl(const io::Path&) override { return Status::NotSupported("delete"); }
    Status batch_delete_impl(const std::vector<io::Path>&) override {
        return Status::NotSupported("delete");
    }
    Status delete_directory_impl(const io::Path&) override {
        return Status::NotSupported("delete");
    }
    Status exists_impl(const io::Path& path, bool* res) const override {
        return io::global_local_filesystem()->exists(path, res);
    }
    Status file_size_impl(const io::Path& path, int64_t* size) const override {
        return io::global_local_filesystem()->file_size(path, size);
    }
    Status list_impl(const io::Path& dir, bool, std::vector<io::FileInfo>* files,
                     bool* exists) override {
        return io::global_local_filesystem()->list(dir, true, files, exists);
    }
    Status rename_impl(const io::Path&, const io::Path&) override {
        return Status::NotSupported("rename");
    }
    Status upload_impl(const io::Path&, const io::Path&) override {
        return Status::NotSupported("upload");
    }
    Status batch_upload_impl(const std::vector<io::Path>&, const std::vector<io::Path>&) override {
        return Status::NotSupported("upload");
    }
    Status download_impl(const io::Path& remote_file, const io::Path& local_file) override {
        // a download replaces the local file, as the remote file systems do
        bool exists = false;
        RETURN_IF_ERROR(io::global_local_filesystem()->exists(local_file, &exists));
        if (exists) {
            RETURN_IF_ERROR(io::global_local_filesystem()->delete_file(local_file));
        }
        return io::global_local_filesystem()->copy_path(remote_file, local_file);
    }
    Status open_file_internal(const io::Path& file, io::FileReaderSPtr* reader,
                              const io::FileReaderOptions&) override {
        return io::global_local_filesystem()->open_file(file, reader);
    }
};

class RepositoryTestLoader : public SnapshotLoader {
public:
    using SnapshotLoader::SnapshotLoader;
    void use_remote_fs(std::shared_ptr<io::RemoteFileSystem> fs) { _remote_fs = std::move(fs); }
};

// A remote file system over the local disk which also uploads (copies), to upload to.
class LocalDirUploadFileSystem : public LocalDirRemoteFileSystem {
protected:
    Status create_directory_impl(const io::Path& path, bool failed_if_exists) override {
        return io::global_local_filesystem()->create_directory(path, failed_if_exists);
    }
    Status upload_impl(const io::Path& local_file, const io::Path& remote_file) override {
        RETURN_IF_ERROR(io::global_local_filesystem()->create_directory(remote_file.parent_path()));
        return io::global_local_filesystem()->copy_path(local_file, remote_file);
    }
    Status rename_impl(const io::Path& orig, const io::Path& new_path) override {
        return io::global_local_filesystem()->rename(orig, new_path);
    }
};

// Whether any file named __manifest__* is in the dir.
static bool has_uploaded_manifest(const std::string& dir) {
    std::vector<io::FileInfo> files;
    bool exists = false;
    EXPECT_TRUE(io::global_local_filesystem()->list(dir, true, &files, &exists).ok());
    for (const auto& file : files) {
        if (file.file_name.starts_with("__manifest__")) {
            return true;
        }
    }
    return false;
}

TEST_F(SnapshotLoaderTest, UploadWritesManifestOnlyIfAsked) {
    for (bool skip : {false, true}) {
        int64_t tablet_id = skip ? 3501 : 3401;
        TRemoteTabletSnapshot snapshot;
        prepare_http_snapshots(tablet_id, tablet_id + 1, tablet_id + 2, tablet_id + 10,
                               tablet_id + 11, &snapshot);
        ASSERT_FALSE(HasFatalFailure());
        std::string idx_dir = fmt::format("{}/repo_up_{}/__idx_1", storage_root_path, tablet_id);
        std::string dest = fmt::format("{}/__{}", idx_dir, tablet_id);
        ASSERT_TRUE(io::global_local_filesystem()->create_directory(dest).ok());

        RepositoryTestLoader loader(*engine_ref, ExecEnv::GetInstance(), 77L, tablet_id);
        loader.use_remote_fs(std::make_shared<LocalDirUploadFileSystem>());
        loader.set_write_manifest(!skip);
        std::map<int64_t, std::vector<std::string>> tablet_files;
        Status st = loader.upload({{snapshot.local_snapshot_path, dest}}, &tablet_files);
        ASSERT_TRUE(st.ok()) << st;
        // the data files are uploaded either way
        ASSERT_EQ(1, tablet_files.count(tablet_id));
        EXPECT_FALSE(tablet_files[tablet_id].empty());
        EXPECT_EQ(!skip, loader.write_manifest());
        EXPECT_EQ(!skip, has_uploaded_manifest(idx_dir));
        EXPECT_EQ(skip ? 0 : 1, loader.uploaded_manifest_roots().size());
    }
}

// The snapshot task writes the manifest only if the frontend asks for it (write_manifest): without it, as from an
// older frontend, the files are only listed, no manifest is written and no root is reported.
TEST_F(SnapshotLoaderTest, SnapshotTaskWritesManifestOnlyIfAsked) {
    for (bool write_manifest : {false, true}) {
        int64_t tablet_id = write_manifest ? 3201 : 3101;
        std::string snapshot_path =
                fmt::format("{}/snapshot_task_{}", storage_root_path, write_manifest);
        std::string dir = fmt::format("{}/{}/{}", snapshot_path, tablet_id, 12345);
        ASSERT_TRUE(io::global_local_filesystem()->create_directory(dir).ok());
        write_test_file(dir + "/rs1_0.dat", "data");
        write_test_file(dir + "/rs1_0.idx", "index");
        write_test_file(dir + "/" + std::to_string(tablet_id) + ".hdr", "meta");

        TSnapshotRequest request;
        request.__set_tablet_id(tablet_id);
        request.__set_schema_hash(12345);
        request.__set_list_files(true);
        // compute_digest alone does not make the backend write anything
        request.__set_compute_digest(true);

        std::vector<std::string> files;
        std::string root;
        Status st = list_snapshot_files_and_write_manifest(request, snapshot_path, write_manifest,
                                                           nullptr, &files, &root);
        ASSERT_TRUE(st.ok()) << st;
        // the files are listed either way
        EXPECT_EQ(3, files.size());
        bool exists = true;
        ASSERT_TRUE(io::global_local_filesystem()
                            ->exists(fmt::format("{}/{}/{}", snapshot_path, tablet_id,
                                                 SnapshotManifest::kLocalFileName),
                                     &exists)
                            .ok());
        EXPECT_EQ(write_manifest, exists);
        EXPECT_EQ(write_manifest ? 64 : 0, root.size());
        ASSERT_TRUE(io::global_local_filesystem()->delete_directory(snapshot_path).ok());
    }
}

// The decomposed digest file next to the snapshot is uploaded before the manifest, and the uploaded manifest has it
// as an entry (its size and SHA-256), outside of the data files. Without the manifest it is not uploaded.
TEST_F(SnapshotLoaderTest, UploadRecordsPrefixDigestInManifest) {
    for (bool skip : {false, true}) {
        int64_t tablet_id = skip ? 3701 : 3601;
        TRemoteTabletSnapshot snapshot;
        prepare_http_snapshots(tablet_id, tablet_id + 1, tablet_id + 2, tablet_id + 10,
                               tablet_id + 11, &snapshot);
        ASSERT_FALSE(HasFatalFailure());
        // <snapshot>/<tablet_id>/rdigest next to the snapshot dir <snapshot>/<tablet_id>/<schema_hash>
        std::string local_parent;
        ASSERT_TRUE(parent_path_of(snapshot.local_snapshot_path, &local_parent).ok());
        std::string rdigest_content = "decomposed digest of the tablet";
        write_test_file(local_parent + "/" + std::string(RestoreDigestDecomposed::kLocalFileName),
                        rdigest_content);
        std::string rdigest_root = sha256_hex(rdigest_content);

        std::string idx_dir = fmt::format("{}/repo_up_rd_{}/__idx_1", storage_root_path, tablet_id);
        std::string dest = fmt::format("{}/__{}", idx_dir, tablet_id);
        ASSERT_TRUE(io::global_local_filesystem()->create_directory(dest).ok());

        RepositoryTestLoader loader(*engine_ref, ExecEnv::GetInstance(), 78L, tablet_id);
        loader.use_remote_fs(std::make_shared<LocalDirUploadFileSystem>());
        loader.set_write_manifest(!skip);
        std::map<int64_t, std::vector<std::string>> tablet_files;
        Status st = loader.upload({{snapshot.local_snapshot_path, dest}}, &tablet_files);
        ASSERT_TRUE(st.ok()) << st;
        // the list of the data files does not have the decomposed digest
        ASSERT_EQ(1, tablet_files.count(tablet_id));
        for (const auto& file : tablet_files[tablet_id]) {
            EXPECT_EQ(std::string::npos, file.find("rdigest")) << file;
        }

        std::string rdigest_object =
                idx_dir + "/" + prefix_digest_remote_file_name(tablet_id, rdigest_root);
        bool exists = true;
        ASSERT_TRUE(io::global_local_filesystem()->exists(rdigest_object, &exists).ok());
        if (skip) {
            EXPECT_FALSE(exists);
            EXPECT_TRUE(loader.uploaded_prefix_digest_roots().empty());
            continue;
        }
        EXPECT_TRUE(exists);
        ASSERT_EQ(1, loader.uploaded_prefix_digest_roots().count(tablet_id));
        EXPECT_EQ(rdigest_root, loader.uploaded_prefix_digest_roots().at(tablet_id));

        ASSERT_EQ(1, loader.uploaded_manifest_roots().count(tablet_id));
        std::string manifest_root = loader.uploaded_manifest_roots().at(tablet_id);
        std::string manifest_file =
                idx_dir + "/" + SnapshotManifest::remote_file_name(tablet_id, manifest_root);
        std::string manifest_content;
        io::FileReaderSPtr reader;
        ASSERT_TRUE(io::global_local_filesystem()->open_file(manifest_file, &reader).ok());
        manifest_content.resize(reader->size());
        size_t bytes_read = 0;
        ASSERT_TRUE(reader->read_at(0, Slice(manifest_content.data(), manifest_content.size()),
                                    &bytes_read)
                            .ok());
        ASSERT_TRUE(reader->close().ok());
        SnapshotManifest manifest;
        st = SnapshotManifest::parse(manifest_content, manifest_root, tablet_id, &manifest);
        ASSERT_TRUE(st.ok()) << st;
        EXPECT_TRUE(manifest.has_prefix_digest());
        EXPECT_EQ(rdigest_root, manifest.prefix_digest_sha256);
        EXPECT_EQ(static_cast<int64_t>(rdigest_content.size()), manifest.prefix_digest_size);
        EXPECT_FALSE(manifest.files.empty());
        for (const auto& file : manifest.files) {
            EXPECT_NE(SnapshotManifest::kPrefixDigestEntryName, file.name);
        }
        EXPECT_EQ(tablet_files[tablet_id].size(), manifest.files.size());
    }
}

TEST_F(SnapshotLoaderTest, RepositoryDownloadOfAnIncrementLeavesOnlyTheIncrementHdr) {
    ManifestConfigGuard guard(true, false);
    TRemoteTabletSnapshot remote_snapshot;
    prepare_http_snapshots(3301, 3302, 3303, 3311, 3312, &remote_snapshot);
    ASSERT_FALSE(HasFatalFailure());
    TRestoreIncrementalRange range =
            last_rowset_range(fmt::format("{}/{}.hdr", remote_snapshot.remote_snapshot_path, 3311));
    ASSERT_GE(range.base_version, 1);

    // The repository: .../__idx_1/__3311/<file>.<md5> and .../__idx_1/__manifest__3311.<root>
    std::string idx_dir = fmt::format("{}/repo_3311/__idx_1", storage_root_path);
    std::string repo_path = idx_dir + "/__3311";
    ASSERT_TRUE(io::global_local_filesystem()->delete_directory(idx_dir).ok());
    ASSERT_TRUE(io::global_local_filesystem()->create_directory(repo_path).ok());
    SnapshotManifest manifest;
    manifest.tablet_id = 3311;
    std::vector<io::FileInfo> files;
    bool exists = false;
    ASSERT_TRUE(io::global_local_filesystem()
                        ->list(remote_snapshot.remote_snapshot_path, true, &files, &exists)
                        .ok());
    for (const auto& file : files) {
        std::string src = remote_snapshot.remote_snapshot_path + "/" + file.file_name;
        std::string md5;
        ASSERT_TRUE(io::global_local_filesystem()->md5sum(src, &md5).ok());
        ASSERT_TRUE(
                io::global_local_filesystem()
                        ->copy_path(src, fmt::format("{}/{}.{}", repo_path, file.file_name, md5))
                        .ok());
        manifest.files.push_back(SnapshotManifestFile {
                .name = file.file_name, .size = file.file_size, .md5 = md5, .sha256 = ""});
    }
    std::string root;
    ASSERT_TRUE(write_snapshot_manifest(manifest, idx_dir + "/manifest.tmp", &root).ok());
    ASSERT_TRUE(io::global_local_filesystem()
                        ->rename(idx_dir + "/manifest.tmp",
                                 idx_dir + "/" + SnapshotManifest::remote_file_name(3311, root))
                        .ok());

    // a resent task: the second round finds the increment tablet meta of the first one
    for (int round = 0; round < 2; ++round) {
        RepositoryTestLoader loader(*engine_ref, ExecEnv::GetInstance(), 33L + round, 3301);
        loader.use_remote_fs(std::make_shared<LocalDirRemoteFileSystem>());
        loader.set_manifest_roots({{repo_path, root}});
        loader.set_incremental_ranges({{repo_path, range}});
        std::vector<int64_t> downloaded_tablet_ids;
        Status st = loader.download({{repo_path, remote_snapshot.local_snapshot_path}},
                                    &downloaded_tablet_ids);
        ASSERT_TRUE(st.ok()) << "round " << round << ": " << st;
        expect_increment_only_dir(remote_snapshot.local_snapshot_path, 3301, range);
        EXPECT_EQ(std::vector<int64_t> {3301}, loader.manifest_verified_tablets());
        EXPECT_EQ(1, loader.download_stats().tablets_incremental);
    }
}

} // namespace doris
