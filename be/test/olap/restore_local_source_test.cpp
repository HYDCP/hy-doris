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

// The local source of a restore kept by its state token: the snapshot of the tablet being replaced, the load of it
// by the snapshot task (restore_kept_load) with the evidence in the tablet meta (RestoreStagePB), and the move of
// the empty snapshot dir which confirms the load. The data is compared by the rowsets (versions, rows, segments,
// delete bitmap marks) and the segment files (hard linked or copied), not by a logical digest.

#include <gen_cpp/AgentService_types.h>
#include <gen_cpp/Descriptors_types.h>
#include <gen_cpp/PaloInternalService_types.h>
#include <gen_cpp/Types_types.h>
#include <gen_cpp/olap_file.pb.h>
#include <gtest/gtest.h>
#include <sys/stat.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <functional>
#include <iterator>
#include <map>
#include <memory>
#include <ostream>
#include <shared_mutex>
#include <sstream>
#include <string>
#include <thread>
#include <tuple>
#include <vector>

#include "common/config.h"
#include "common/status.h"
#include "io/fs/local_file_system.h"
#include "olap/data_dir.h"
#include "olap/delete_handler.h"
#include "olap/merger.h"
#include "olap/olap_common.h"
#include "olap/olap_define.h"
#include "olap/options.h"
#include "olap/rowid_conversion.h"
#include "olap/rowset/rowset.h"
#include "olap/rowset/rowset_factory.h"
#include "olap/rowset/rowset_reader.h"
#include "olap/rowset/rowset_writer.h"
#include "olap/rowset/rowset_writer_context.h"
#include "olap/snapshot_manager.h"
#include "olap/storage_engine.h"
#include "olap/tablet.h"
#include "olap/tablet_manager.h"
#include "olap/tablet_meta.h"
#include "olap/tablet_schema.h"
#include "runtime/exec_env.h"
#include "runtime/snapshot_loader.h"
#include "runtime/thread_context.h"
#include "service/backend_options.h"
#include "util/debug_points.h"
#include "util/runtime_profile.h"
#include "vec/core/block.h"

namespace doris {
using vectorized::Block;

namespace {

struct TokenRow {
    int32_t k;
    int32_t v;
    int8_t del = 0;
};

struct TokenColumnDef {
    std::string column_name;
    TPrimitiveType::type type;
    bool is_key = false;
    bool is_allow_null = false;
};

TCreateTabletReq token_create_tablet_request(int64_t tablet_id, bool unique,
                                             std::initializer_list<TokenColumnDef> column_defs) {
    TCreateTabletReq request;
    request.tablet_id = tablet_id;
    request.__set_version(1);
    request.partition_id = 10;
    request.tablet_schema.schema_hash = 1;
    request.tablet_schema.short_key_column_count = 1;
    request.tablet_schema.keys_type = unique ? TKeysType::UNIQUE_KEYS : TKeysType::DUP_KEYS;
    request.tablet_schema.storage_type = TStorageType::COLUMN;
    request.__set_storage_format(TStorageFormat::V2);
    for (const auto& def : column_defs) {
        TColumn column;
        column.column_name = def.column_name;
        column.__set_is_key(def.is_key);
        column.__set_is_allow_null(def.is_allow_null);
        column.column_type.type = def.type;
        request.tablet_schema.columns.push_back(column);
    }
    return request;
}

// A rowset as the restore sees it: the version range, the rows, the segments and the delete condition. The rowset
// id is not part of it, the load gives the rowsets new ids.
struct RowsetShape {
    int64_t start = 0;
    int64_t end = 0;
    int64_t num_rows = 0;
    int64_t num_segments = 0;
    bool has_delete_predicate = false;
    bool operator==(const RowsetShape&) const = default;
};

std::ostream& operator<<(std::ostream& os, const RowsetShape& s) {
    return os << "[" << s.start << "-" << s.end << " rows=" << s.num_rows
              << " segs=" << s.num_segments << " del=" << s.has_delete_predicate << "]";
}

// A mark of the merge-on-write delete bitmap, by the start version of its rowset instead of the rowset id.
using BitmapMark = std::tuple<int64_t, uint32_t, uint64_t, uint64_t>;

bool read_file(const std::string& path, std::string* content) {
    std::ifstream in(path, std::ios::binary);
    if (!in) {
        return false;
    }
    std::ostringstream ss;
    ss << in.rdbuf();
    *content = ss.str();
    return true;
}

} // namespace

// A real StorageEngine over a fresh local directory, installed in the ExecEnv, with real tablets and rowsets.
class RestoreTokenEngineTest : public testing::Test {
public:
    void SetUp() override {
        char buffer[1024];
        EXPECT_NE(getcwd(buffer, sizeof(buffer)), nullptr);
        _root = std::string(buffer) + "/restore_token_engine_ut";
        config::storage_root_path = _root;

        auto fs = io::global_local_filesystem();
        ASSERT_TRUE(fs->delete_directory(_root).ok());
        ASSERT_TRUE(fs->create_directory(_root).ok());

        std::vector<StorePath> paths;
        paths.emplace_back(_root, -1);
        EngineOptions options;
        options.store_paths = paths;
        auto engine = std::make_unique<StorageEngine>(options);
        ASSERT_TRUE(engine->open().ok());
        _engine = engine.get();
        ExecEnv::GetInstance()->set_storage_engine(std::move(engine));
    }

    void TearDown() override {
        DebugPoints::instance()->clear();
        config::enable_debug_points = false;
        _engine = nullptr;
        ExecEnv::GetInstance()->set_storage_engine(nullptr);
        static_cast<void>(io::global_local_filesystem()->delete_directory(_root));
    }

protected:
    // the tablets of the restore: the tablet being replaced (origin) and the tablet of the job (target)
    struct Prepared {
        TabletSharedPtr origin;
        TabletSharedPtr target;
        std::string dir;
    };

    // keys: DUP, or UNIQUE merge on write
    TabletSharedPtr create_tablet(int64_t tablet_id, bool mow) {
        auto request = token_create_tablet_request(
                tablet_id, mow,
                {{"k", TPrimitiveType::INT, true}, {"v", TPrimitiveType::INT, false, true}});
        if (mow) {
            TColumn del;
            del.column_name = DELETE_SIGN;
            del.__set_is_key(false);
            del.__set_is_allow_null(false);
            del.column_type.type = TPrimitiveType::TINYINT;
            del.__set_default_value("0");
            request.tablet_schema.columns.push_back(del);
            request.tablet_schema.__set_delete_sign_idx(2);
            request.__set_enable_unique_key_merge_on_write(true);
        }
        RuntimeProfile profile("restore_token_ut");
        Status st = _engine->create_tablet(request, &profile);
        EXPECT_TRUE(st.ok()) << st;
        return _engine->tablet_manager()->get_tablet(tablet_id);
    }

    // Writes one rowset, one segment per entry of `segments`; every segment sorted by key.
    RowsetSharedPtr write(const TabletSharedPtr& tablet, const TabletSchemaSPtr& schema,
                          int64_t version, const std::vector<std::vector<TokenRow>>& segments,
                          bool add_to_tablet) {
        RowsetWriterContext ctx;
        RowsetId id;
        id.init(_next_id++);
        ctx.rowset_id = id;
        ctx.tablet_id = tablet->tablet_id();
        ctx.tablet_schema_hash = tablet->schema_hash();
        ctx.partition_id = tablet->partition_id();
        ctx.rowset_type = BETA_ROWSET;
        ctx.tablet_path = tablet->tablet_path();
        ctx.data_dir = tablet->data_dir();
        ctx.rowset_state = VISIBLE;
        ctx.tablet_schema = schema;
        ctx.version = {version, version};
        ctx.enable_unique_key_merge_on_write = tablet->enable_unique_key_merge_on_write();
        ctx.write_type = DataWriteType::TYPE_DIRECT;
        ctx.tablet = tablet;
        ctx.segments_overlap = NONOVERLAPPING;
        ctx.max_rows_per_segment = UINT32_MAX;
        ctx.enable_segcompaction = false;
        auto res = RowsetFactory::create_rowset_writer(*_engine, ctx, false);
        EXPECT_TRUE(res.has_value()) << res.error();
        auto writer = std::move(res).value();
        for (const auto& seg : segments) {
            Block block = schema->create_block();
            auto cols = std::move(block).mutate_columns();
            for (const TokenRow& r : seg) {
                size_t c = 0;
                cols[c++]->insert_data(reinterpret_cast<const char*>(&r.k), sizeof(r.k));
                cols[c++]->insert_data(reinterpret_cast<const char*>(&r.v), sizeof(r.v));
                if (c < cols.size()) {
                    cols[c++]->insert_data(reinterpret_cast<const char*>(&r.del), sizeof(r.del));
                }
            }
            block.set_columns(std::move(cols));
            EXPECT_TRUE(writer->add_block(&block).ok());
            EXPECT_TRUE(writer->flush().ok());
        }
        RowsetSharedPtr rowset;
        EXPECT_TRUE(writer->build(rowset).ok());
        if (add_to_tablet) {
            EXPECT_TRUE(tablet->add_rowset(rowset).ok());
        }
        return rowset;
    }

    RowsetSharedPtr write_delete(const TabletSharedPtr& tablet, int64_t version,
                                 const std::vector<TCondition>& conds) {
        auto schema = tablet->tablet_schema();
        DeletePredicatePB pred;
        Status st = DeleteHandler::generate_delete_predicate(*schema, conds, &pred);
        EXPECT_TRUE(st.ok()) << st;
        auto rowset = write(tablet, schema, version, {}, false);
        rowset->rowset_meta()->set_delete_predicate(std::move(pred));
        EXPECT_TRUE(tablet->add_rowset(rowset).ok());
        return rowset;
    }

    static TCondition cond(const std::string& column, const std::string& op,
                           const std::string& value) {
        TCondition c;
        c.column_name = column;
        c.condition_op = op;
        c.condition_values = {value};
        return c;
    }

    // Merges the two rowsets of versions lo..hi into one rowset of the same rows (what a compaction does) and adds
    // it to the tablet, which retires the inputs.
    RowsetSharedPtr compact(const TabletSharedPtr& tablet, const TabletSchemaSPtr& schema,
                            int64_t lo, int64_t hi) {
        std::vector<RowsetSharedPtr> inputs;
        {
            std::shared_lock lock(tablet->get_header_lock());
            auto ret = tablet->capture_consistent_rowsets_unlocked(Version(0, hi),
                                                                   CaptureRowsetOps {});
            EXPECT_TRUE(ret.has_value());
            for (const auto& rs : ret->rowsets) {
                if (rs->version().first >= lo) {
                    inputs.push_back(rs);
                }
            }
        }
        EXPECT_EQ(2U, inputs.size());
        std::vector<RowsetReaderSharedPtr> readers;
        for (const auto& rs : inputs) {
            RowsetReaderSharedPtr reader;
            EXPECT_TRUE(rs->create_reader(&reader).ok());
            readers.push_back(reader);
        }
        RowsetWriterContext ctx;
        RowsetId id;
        id.init(_next_id++);
        ctx.rowset_id = id;
        ctx.tablet_id = tablet->tablet_id();
        ctx.tablet_schema_hash = tablet->schema_hash();
        ctx.partition_id = tablet->partition_id();
        ctx.rowset_type = BETA_ROWSET;
        ctx.tablet_path = tablet->tablet_path();
        ctx.data_dir = tablet->data_dir();
        ctx.rowset_state = VISIBLE;
        ctx.tablet_schema = schema;
        ctx.version = {lo, hi};
        ctx.segments_overlap = NONOVERLAPPING;
        ctx.max_rows_per_segment = UINT32_MAX;
        ctx.tablet = tablet;
        auto res = RowsetFactory::create_rowset_writer(*_engine, ctx, true);
        EXPECT_TRUE(res.has_value()) << res.error();
        auto writer = std::move(res).value();
        Merger::Statistics stats;
        RowIdConversion conversion;
        stats.rowid_conversion = &conversion;
        EXPECT_TRUE(Merger::vertical_merge_rowsets(tablet, ReaderType::READER_BASE_COMPACTION,
                                                   *schema, readers, writer.get(), 100, 2, &stats)
                            .ok());
        RowsetSharedPtr merged;
        EXPECT_TRUE(writer->build(merged).ok());
        EXPECT_TRUE(tablet->add_rowset(merged).ok());
        return merged;
    }

    // the tablet being replaced: data up to version 6, with a delete condition (duplicate) or the marks of the
    // merge-on-write bitmap
    void build_source(const TabletSharedPtr& src, bool mow) {
        auto schema = src->tablet_schema();
        if (mow) {
            auto rs2 = write(src, schema, 2,
                             {{{1, 10}, {2, 20}, {3, 30}, {4, 40}, {5, 50}, {6, 60}}}, true);
            write(src, schema, 3, {{{3, 31}, {5, 51}}}, true);
            auto& bitmap = *src->tablet_meta()->delete_bitmap();
            bitmap.add({rs2->rowset_id(), 0, 3}, 2);
            bitmap.add({rs2->rowset_id(), 0, 3}, 4);
            write(src, schema, 4, {{{1, 0, 1}, {6, 61}}}, true);
            bitmap.add({rs2->rowset_id(), 0, 4}, 0);
            bitmap.add({rs2->rowset_id(), 0, 4}, 5);
            auto rs5 = write(src, schema, 5, {{{2, 21}, {7, 70}}}, true);
            bitmap.add({rs2->rowset_id(), 0, 5}, 1);
            write(src, schema, 6, {{{2, 0, 1}}}, true);
            bitmap.add({rs5->rowset_id(), 0, 6}, 0);
        } else {
            write(src, schema, 2, {{{1, 10}, {2, 20}, {3, 30}}}, true);
            write(src, schema, 3, {{{4, 40}, {5, 50}}}, true);
            write(src, schema, 4, {{{6, 60}, {7, 70}}}, true);
            write_delete(src, 5, {cond("v", "=", "20")});
            write(src, schema, 6, {{{1, 11}, {8, 80}}}, true);
        }
    }

    // the tablet being replaced at its kept version 3: one rowset of version 2 and an empty one of version 3
    void build_local(const TabletSharedPtr& local, bool mow) {
        auto schema = local->tablet_schema();
        if (mow) {
            write(local, schema, 2, {{{1, 10}, {2, 20}, {3, 31}, {4, 40}, {5, 51}, {6, 60}}}, true);
        } else {
            write(local, schema, 2, {{{1, 10}, {2, 20}, {3, 30}, {4, 40}, {5, 50}}}, true);
        }
        write(local, schema, 3, {}, true);
    }

    TabletSharedPtr current(const TabletSharedPtr& tablet) {
        return _engine->tablet_manager()->get_tablet(tablet->tablet_id());
    }

    std::vector<std::string> tablet_files(const TabletSharedPtr& tablet) {
        std::vector<io::FileInfo> files;
        bool exists = false;
        EXPECT_TRUE(io::global_local_filesystem()
                            ->list(tablet->tablet_path(), true, &files, &exists)
                            .ok());
        std::vector<std::string> names;
        for (const auto& f : files) {
            names.push_back(f.file_name);
        }
        std::sort(names.begin(), names.end());
        return names;
    }

    size_t snapshot_dirs(const TabletSharedPtr& tablet) {
        std::string dir = tablet->data_dir()->path() + "/snapshot";
        if (!std::filesystem::exists(dir)) {
            return 0;
        }
        return std::distance(std::filesystem::directory_iterator(dir),
                             std::filesystem::directory_iterator());
    }

    bool dir_is_empty(const std::string& dir) {
        std::vector<io::FileInfo> files;
        bool exists = false;
        EXPECT_TRUE(io::global_local_filesystem()->list(dir, true, &files, &exists).ok());
        return exists && files.empty();
    }

    // the rowsets the tablet reads at `version`, by version
    std::vector<RowsetSharedPtr> rowsets_at(const TabletSharedPtr& tablet, int64_t version) {
        std::vector<RowsetSharedPtr> rowsets;
        {
            std::shared_lock lock(tablet->get_header_lock());
            auto ret = tablet->capture_consistent_rowsets_unlocked(Version(0, version),
                                                                   CaptureRowsetOps {});
            EXPECT_TRUE(ret.has_value());
            if (ret.has_value()) {
                rowsets = ret->rowsets;
            }
        }
        std::sort(rowsets.begin(), rowsets.end(), [](const auto& a, const auto& b) {
            return a->version().first < b->version().first;
        });
        return rowsets;
    }

    std::vector<RowsetShape> shapes(const TabletSharedPtr& tablet, int64_t version) {
        std::vector<RowsetShape> out;
        for (const auto& rs : rowsets_at(tablet, version)) {
            out.push_back({rs->version().first, rs->version().second,
                           static_cast<int64_t>(rs->num_rows()),
                           static_cast<int64_t>(rs->num_segments()),
                           rs->rowset_meta()->has_delete_predicate()});
        }
        return out;
    }

    int64_t total_rows(const TabletSharedPtr& tablet, int64_t version) {
        int64_t rows = 0;
        for (const auto& s : shapes(tablet, version)) {
            rows += s.num_rows;
        }
        return rows;
    }

    // the marks of the delete bitmap up to `version` on the rowsets read at `version`
    std::vector<BitmapMark> bitmap_marks(const TabletSharedPtr& tablet, int64_t version) {
        std::map<RowsetId, int64_t> start_of;
        for (const auto& rs : rowsets_at(tablet, version)) {
            start_of[rs->rowset_id()] = rs->version().first;
        }
        DeleteBitmap bitmap = tablet->tablet_meta()->delete_bitmap()->snapshot(version);
        std::vector<BitmapMark> marks;
        for (const auto& [key, bits] : bitmap.delete_bitmap) {
            auto it = start_of.find(std::get<0>(key));
            if (it == start_of.end() || std::get<1>(key) == DeleteBitmap::INVALID_SEGMENT_ID) {
                continue;
            }
            marks.emplace_back(it->second, std::get<1>(key), std::get<2>(key), bits.cardinality());
        }
        std::sort(marks.begin(), marks.end());
        return marks;
    }

    // the segment files of the rowsets read at `version`, by version and segment
    std::vector<std::string> segment_paths(const TabletSharedPtr& tablet, int64_t version) {
        std::vector<std::string> paths;
        for (const auto& rs : rowsets_at(tablet, version)) {
            for (int64_t seg = 0; seg < rs->num_segments(); ++seg) {
                paths.push_back(local_segment_path(tablet->tablet_path(),
                                                   rs->rowset_id().to_string(), seg));
            }
        }
        return paths;
    }

    enum class Files { LINKED, COPIED };

    // The data of `dst` at `version` is the one of `src` at `version`: the same rowsets, rows and delete bitmap
    // marks, and every segment file is the one of the source, hard linked (the same inode) or copied (the same
    // bytes in another inode).
    void expect_same_data(const TabletSharedPtr& src, const TabletSharedPtr& dst, int64_t version,
                          Files files) {
        EXPECT_EQ(shapes(src, version), shapes(dst, version));
        EXPECT_EQ(bitmap_marks(src, version), bitmap_marks(dst, version));
        auto src_paths = segment_paths(src, version);
        auto dst_paths = segment_paths(dst, version);
        ASSERT_EQ(src_paths.size(), dst_paths.size());
        EXPECT_FALSE(src_paths.empty());
        for (size_t i = 0; i < src_paths.size(); ++i) {
            struct stat s1;
            struct stat s2;
            ASSERT_EQ(0, stat(src_paths[i].c_str(), &s1)) << src_paths[i];
            ASSERT_EQ(0, stat(dst_paths[i].c_str(), &s2)) << dst_paths[i];
            const bool same_inode = s1.st_dev == s2.st_dev && s1.st_ino == s2.st_ino;
            if (files == Files::LINKED) {
                EXPECT_TRUE(same_inode) << src_paths[i] << " " << dst_paths[i];
            } else {
                EXPECT_FALSE(same_inode) << src_paths[i] << " " << dst_paths[i];
                std::string c1;
                std::string c2;
                ASSERT_TRUE(read_file(src_paths[i], &c1));
                ASSERT_TRUE(read_file(dst_paths[i], &c2));
                EXPECT_EQ(c1, c2) << src_paths[i] << " " << dst_paths[i];
            }
        }
    }

    TSnapshotRequest local_source_request(const TabletSharedPtr& src, const TabletSharedPtr& target,
                                          int64_t version) {
        TSnapshotRequest req;
        req.__set_tablet_id(target->tablet_id());
        req.__set_schema_hash(target->schema_hash());
        req.__set_ref_tablet_id(src->tablet_id());
        req.__set_version(version);
        req.__set_restore_local_source(true);
        return req;
    }

    // keep the data of src at `version` in target, as the local source snapshot and the move of the restore do
    LocalSourceStats keep(const TabletSharedPtr& src, const TabletSharedPtr& target,
                          int64_t version) {
        TSnapshotRequest req = local_source_request(src, target, version);
        std::string path;
        bool allow = false;
        LocalSourceStats stats;
        Status st = _engine->snapshot_mgr()->make_snapshot(req, &path, &allow, &stats);
        EXPECT_TRUE(st.ok()) << st;
        std::string dir = fmt::format("{}/{}/{}", path, target->tablet_id(), target->schema_hash());
        SnapshotLoader loader(*_engine, ExecEnv::GetInstance(), 1L, 2L);
        st = loader.move(dir, target, true);
        EXPECT_TRUE(st.ok()) << st;
        return stats;
    }

    void debug_on() {
        config::enable_debug_points = true;
        DebugPoints::instance()->clear();
    }

    void debug_off() {
        DebugPoints::instance()->clear();
        config::enable_debug_points = false;
    }

    void force_copy() {
        debug_on();
        DebugPoints::instance()->add("SnapshotManager.restore_local_source.force_copy");
    }

    static bool not_retryable(const Status& st) {
        return !st.ok() &&
               st.to_string().find(SnapshotLoader::kRestoreNotRetryable) != std::string::npos;
    }

    // The evidence that the restore job `job_id` loaded the local source of `base` into the tablet, written by
    // hand to drive the judgement.
    static void mark_loaded(const TabletSharedPtr& tablet, int64_t job_id, int64_t base,
                            RestoreStagePB::Stage stage = RestoreStagePB::LOCAL_LOADED,
                            int64_t end = 0) {
        RestoreStagePB pb;
        pb.set_job_id(job_id);
        pb.set_stage(stage);
        pb.set_base_version(base);
        if (stage == RestoreStagePB::APPENDED) {
            pb.set_end_version(end);
        }
        std::lock_guard lock(tablet->get_header_lock());
        tablet->tablet_meta()->set_restore_stage(pb);
        tablet->save_meta();
    }

    RestoreStagePB stage_of(const TabletSharedPtr& tablet) {
        auto t = current(tablet);
        EXPECT_TRUE(t->tablet_meta()->has_restore_stage());
        return t->tablet_meta()->restore_stage();
    }

    // origin (the tablet being replaced, kept at version 3) and a fresh target
    Prepared prepare(bool mow, int64_t base_id) {
        _next_id = 1200000 + base_id;
        Prepared p;
        p.origin = create_tablet(base_id + 1, mow);
        p.target = create_tablet(base_id + 2, mow);
        EXPECT_NE(nullptr, p.origin);
        EXPECT_NE(nullptr, p.target);
        build_local(p.origin, mow);
        return p;
    }

    // the snapshot task of a kept partition (restore_kept_load)
    TSnapshotRequest kept_request(const Prepared& p, int64_t job_id) {
        TSnapshotRequest req = local_source_request(p.origin, p.target, 3);
        req.__set_restore_kept_load(true);
        req.__set_job_id(job_id);
        return req;
    }

    // loads the origin at 3 into the target, returns the empty snapshot dir in p.dir
    Status load_kept(Prepared& p, SnapshotLoader& loader, int64_t job_id, LocalSourceStats* stats) {
        std::string dir_path;
        Status st = loader.make_local_source_snapshot_and_load(kept_request(p, job_id), &dir_path,
                                                               stats);
        if (st.ok()) {
            p.dir = fmt::format("{}/{}/{}", dir_path, p.target->tablet_id(),
                                p.target->schema_hash());
        }
        return st;
    }

    std::string _root;
    StorageEngine* _engine = nullptr;
    int64_t _next_id = 890000;
};

// The snapshot of a local source (restore_local_source) and the move of it.
class RestoreTokenLocalSourceTest : public RestoreTokenEngineTest {
protected:
    void run_keep(bool mow, int64_t base_id, bool copy) {
        _next_id = 970000 + base_id;
        if (copy) {
            force_copy();
        }
        auto src = create_tablet(base_id, mow);
        auto target = create_tablet(base_id + 1, mow);
        ASSERT_NE(nullptr, src);
        ASSERT_NE(nullptr, target);
        build_source(src, mow);
        auto files_before = tablet_files(src);
        auto shapes_before = shapes(src, 6);
        if (mow) {
            EXPECT_FALSE(bitmap_marks(src, 6).empty());
        }

        LocalSourceStats stats = keep(src, target, 6);
        EXPECT_EQ(1, stats.tablets);
        EXPECT_EQ(0, stats.loaded_tablets);
        // the files are linked or copied, one of the two
        if (copy) {
            EXPECT_EQ(0, stats.linked_files);
            EXPECT_EQ(0, stats.linked_bytes);
            EXPECT_GT(stats.copied_files, 0);
            EXPECT_GT(stats.copied_bytes, 0);
        } else {
            EXPECT_EQ(0, stats.copied_files);
            EXPECT_EQ(0, stats.copied_bytes);
            EXPECT_GT(stats.linked_files, 0);
            EXPECT_GT(stats.linked_bytes, 0);
        }
        auto moved = current(target);
        ASSERT_NE(nullptr, moved);
        EXPECT_EQ(6, moved->max_version_unlocked());
        expect_same_data(src, moved, 6, copy ? Files::COPIED : Files::LINKED);
        // no evidence: the plain move of a full snapshot
        EXPECT_FALSE(moved->tablet_meta()->has_restore_stage());
        // the source tablet is not changed in any way
        EXPECT_EQ(files_before, tablet_files(src));
        EXPECT_EQ(6, src->max_version_unlocked());
        EXPECT_EQ(shapes_before, shapes(src, 6));
    }
};

TEST_F(RestoreTokenLocalSourceTest, DuplicateKeepEqualsSourceAndLinks) {
    run_keep(false, 8501, false);
}

TEST_F(RestoreTokenLocalSourceTest, MergeOnWriteKeepEqualsSourceAndLinks) {
    run_keep(true, 8511, false);
}

// the source tablet is on another disk: the files are copied
TEST_F(RestoreTokenLocalSourceTest, DuplicateKeepCopiesOnAnotherDisk) {
    run_keep(false, 8521, true);
}

TEST_F(RestoreTokenLocalSourceTest, MergeOnWriteKeepCopiesOnAnotherDisk) {
    run_keep(true, 8531, true);
}

// the version is a boundary inside the source tablet: the data of the source at that version, not the latest
TEST_F(RestoreTokenLocalSourceTest, KeepsTheDataAtTheVersionNotTheLatest) {
    _next_id = 980000;
    auto src = create_tablet(8541, false);
    auto target = create_tablet(8542, false);
    build_source(src, false);
    EXPECT_NE(shapes(src, 3), shapes(src, 6));
    keep(src, target, 3);
    auto moved = current(target);
    EXPECT_EQ(3, moved->max_version_unlocked());
    // only the rowsets up to version 3, the rowsets after it are not taken
    EXPECT_EQ(shapes(src, 3).size(), moved->tablet_meta()->all_rs_metas().size());
    expect_same_data(src, moved, 3, Files::LINKED);
    EXPECT_EQ(5, total_rows(moved, 3));
    EXPECT_EQ(6, src->max_version_unlocked());
}

// a version the source does not have, no source tablet, no version, the tablet itself as the source: the task
// fails and no snapshot is left
TEST_F(RestoreTokenLocalSourceTest, FailureLeavesNoSnapshot) {
    _next_id = 990000;
    auto src = create_tablet(8551, false);
    auto target = create_tablet(8552, false);
    build_source(src, false);
    size_t dirs = snapshot_dirs(target);
    std::string path;
    bool allow = false;
    // a version after the last one of the source
    TSnapshotRequest req = local_source_request(src, target, 9);
    EXPECT_FALSE(_engine->snapshot_mgr()->make_snapshot(req, &path, &allow).ok());
    EXPECT_EQ(dirs, snapshot_dirs(target));
    // the source tablet does not exist (moved to another backend)
    TSnapshotRequest missing = local_source_request(src, target, 6);
    missing.__set_ref_tablet_id(8559);
    EXPECT_FALSE(_engine->snapshot_mgr()->make_snapshot(missing, &path, &allow).ok());
    EXPECT_EQ(dirs, snapshot_dirs(target));
    // no version
    TSnapshotRequest no_version = local_source_request(src, target, 6);
    no_version.__isset.version = false;
    EXPECT_FALSE(_engine->snapshot_mgr()->make_snapshot(no_version, &path, &allow).ok());
    // the tablet itself as the source
    TSnapshotRequest self = local_source_request(src, target, 6);
    self.__set_ref_tablet_id(target->tablet_id());
    EXPECT_FALSE(_engine->snapshot_mgr()->make_snapshot(self, &path, &allow).ok());
    EXPECT_EQ(dirs, snapshot_dirs(target));
    EXPECT_EQ(6, src->max_version_unlocked());
    EXPECT_EQ(1, current(target)->max_version_unlocked());
}

// The evidence of the restore job in the tablet meta (RestoreStagePB) and the judgement of the resent snapshot task
// which loads the local source.
class RestoreTokenStageTest : public RestoreTokenEngineTest {};

TEST_F(RestoreTokenStageTest, TabletMetaKeepsTheStageAcrossSerialization) {
    auto tablet = create_tablet(8701, false);
    ASSERT_NE(nullptr, tablet);
    EXPECT_FALSE(tablet->tablet_meta()->has_restore_stage());
    TabletMetaPB pb;
    tablet->tablet_meta()->to_meta_pb(&pb);
    EXPECT_FALSE(pb.has_restore_stage());

    mark_loaded(tablet, 42, 3, RestoreStagePB::APPENDED, 6);
    std::string binary;
    tablet->tablet_meta()->serialize(&binary);
    TabletMeta restored;
    ASSERT_TRUE(restored.deserialize(binary).ok());
    ASSERT_TRUE(restored.has_restore_stage());
    EXPECT_EQ(42, restored.restore_stage().job_id());
    EXPECT_EQ(RestoreStagePB::APPENDED, restored.restore_stage().stage());
    EXPECT_EQ(3, restored.restore_stage().base_version());
    EXPECT_EQ(6, restored.restore_stage().end_version());
    // the copy and the comparison know it
    TabletMeta copy(restored);
    EXPECT_TRUE(copy.has_restore_stage());
    EXPECT_EQ(42, copy.restore_stage().job_id());
    copy.clear_restore_stage();
    EXPECT_FALSE(copy.has_restore_stage());
    EXPECT_FALSE(restored == copy);
    // the file of a snapshot dir
    TabletMetaPB file_pb;
    restored.to_meta_pb(&file_pb);
    std::string file = tablet->tablet_path() + "/stage_roundtrip.hdr";
    ASSERT_TRUE(TabletMeta::save(file, file_pb).ok());
    TabletMetaPB loaded;
    ASSERT_TRUE(TabletMeta::load_from_file(file, &loaded).ok());
    EXPECT_EQ(42, loaded.restore_stage().job_id());
    std::filesystem::remove(file);
}

// the snapshot of a tablet which an earlier restore loaded has its evidence, the load drops it; the evidence of this
// job is saved by the same step; a move of the tablet to another disk (convert without reset) keeps it
TEST_F(RestoreTokenStageTest, ConvertRowsetIdsDropsTheSourceEvidenceAndWritesTheOneOfTheJob) {
    _next_id = 1110000;
    auto src = create_tablet(8711, false);
    build_source(src, false);
    mark_loaded(src, 7, 3, RestoreStagePB::APPENDED, 6);
    auto make_dir = [&]() {
        TSnapshotRequest req;
        req.__set_tablet_id(src->tablet_id());
        req.__set_schema_hash(src->schema_hash());
        req.__set_version(6);
        std::string path;
        bool allow = false;
        EXPECT_TRUE(_engine->snapshot_mgr()->make_snapshot(req, &path, &allow).ok());
        return fmt::format("{}/{}/{}", path, src->tablet_id(), src->schema_hash());
    };
    auto hdr_stage = [&](const std::string& dir, bool* has) {
        TabletMetaPB pb;
        EXPECT_TRUE(TabletMeta::load_from_file(fmt::format("{}/{}.hdr", dir, src->tablet_id()), &pb)
                            .ok());
        *has = pb.has_restore_stage();
        return pb.restore_stage();
    };
    bool has = false;
    // the snapshot of the source carries the evidence (that is the trap)
    std::string dir1 = make_dir();
    EXPECT_EQ(7, hdr_stage(dir1, &has).job_id());
    EXPECT_TRUE(has);
    // without reset (storage migration, clone) it is kept
    auto guards1 = _engine->snapshot_mgr()->convert_rowset_ids(
            dir1, src->tablet_id(), src->replica_id(), src->table_id(), src->partition_id(),
            src->schema_hash());
    ASSERT_TRUE(guards1.has_value());
    EXPECT_EQ(7, hdr_stage(dir1, &has).job_id());
    EXPECT_TRUE(has);
    // reset: dropped
    std::string dir2 = make_dir();
    auto guards2 = _engine->snapshot_mgr()->convert_rowset_ids(
            dir2, src->tablet_id(), src->replica_id(), src->table_id(), src->partition_id(),
            src->schema_hash(), true);
    ASSERT_TRUE(guards2.has_value());
    hdr_stage(dir2, &has);
    EXPECT_FALSE(has);
    // reset and the evidence of this job
    std::string dir3 = make_dir();
    RestoreStagePB mine;
    mine.set_job_id(9);
    mine.set_stage(RestoreStagePB::LOCAL_LOADED);
    mine.set_base_version(3);
    auto guards3 = _engine->snapshot_mgr()->convert_rowset_ids(
            dir3, src->tablet_id(), src->replica_id(), src->table_id(), src->partition_id(),
            src->schema_hash(), true, &mine);
    ASSERT_TRUE(guards3.has_value());
    auto stage = hdr_stage(dir3, &has);
    EXPECT_TRUE(has);
    EXPECT_EQ(9, stage.job_id());
    EXPECT_EQ(RestoreStagePB::LOCAL_LOADED, stage.stage());
    EXPECT_EQ(3, stage.base_version());
}

// the load of the clean tablet writes the evidence; the resent task finds it and skips the load
TEST_F(RestoreTokenStageTest, LoadWritesTheEvidenceAndTheResentTaskIsSkipped) {
    Prepared p = prepare(false, 8721);
    SnapshotLoader loader(*_engine, ExecEnv::GetInstance(), 1L, 2L);
    TabletUid uid_before = p.target->tablet_uid();
    LocalSourceStats stats;
    // no evidence, the empty initial tablet: loaded, with the evidence of the job
    ASSERT_TRUE(load_kept(p, loader, 1, &stats).ok());
    EXPECT_EQ(1, stats.tablets);
    auto loaded = current(p.target);
    EXPECT_EQ(3, loaded->max_version_unlocked());
    RestoreStagePB stage = stage_of(p.target);
    EXPECT_EQ(1, stage.job_id());
    EXPECT_EQ(RestoreStagePB::LOCAL_LOADED, stage.stage());
    EXPECT_EQ(3, stage.base_version());
    // the load changed the uid of the tablet (convert_rowset_ids generates one), the judgement does not depend on it
    EXPECT_NE(uid_before, loaded->tablet_uid());
    auto files = tablet_files(loaded);
    TabletUid uid_loaded = loaded->tablet_uid();
    // resent, the evidence of this job and the version: nothing is loaded again, the tablet is counted (the report
    // of the load was lost)
    LocalSourceStats again;
    ASSERT_TRUE(load_kept(p, loader, 1, &again).ok());
    EXPECT_EQ(1, again.tablets);
    EXPECT_EQ(1, again.loaded_tablets);
    EXPECT_EQ(0, again.linked_files + again.copied_files);
    EXPECT_EQ(uid_loaded, current(p.target)->tablet_uid());
    EXPECT_EQ(files, tablet_files(current(p.target)));
    // and the empty snapshot dir is there for the move
    EXPECT_TRUE(std::filesystem::exists(p.dir));
    expect_same_data(p.origin, current(p.target), 3, Files::LINKED);
}

TEST_F(RestoreTokenStageTest, LoadRefusesTheEvidenceOfAnotherJobAndTheTabletWhichIsNotClean) {
    Prepared p = prepare(false, 8731);
    SnapshotLoader loader(*_engine, ExecEnv::GetInstance(), 1L, 2L);
    ASSERT_TRUE(load_kept(p, loader, 1, nullptr).ok());
    auto files = tablet_files(current(p.target));
    // the evidence of another job
    SnapshotLoader other(*_engine, ExecEnv::GetInstance(), 2L, 2L);
    Status st = load_kept(p, other, 2, nullptr);
    EXPECT_TRUE(not_retryable(st)) << st;
    // the evidence of this job, but not of a load at the version the tablet is at
    mark_loaded(current(p.target), 1, 3, RestoreStagePB::APPENDED, 6);
    st = load_kept(p, loader, 1, nullptr);
    EXPECT_TRUE(not_retryable(st)) << st;
    // the request asks for another local source version than the evidence
    mark_loaded(current(p.target), 1, 2);
    st = load_kept(p, loader, 1, nullptr);
    EXPECT_TRUE(not_retryable(st)) << st;
    // nothing is replaced by the refused tasks
    EXPECT_EQ(files, tablet_files(current(p.target)));
    EXPECT_EQ(3, current(p.target)->max_version_unlocked());

    // no evidence, the tablet is not the empty one the job created
    Prepared q = prepare(false, 8741);
    write(q.target, q.target->tablet_schema(), 2, {{{1, 10}}}, true);
    auto q_files = tablet_files(q.target);
    st = load_kept(q, loader, 1, nullptr);
    EXPECT_TRUE(not_retryable(st)) << st;
    EXPECT_EQ(q_files, tablet_files(q.target));
    EXPECT_EQ(2, q.target->max_version_unlocked());
    EXPECT_FALSE(current(q.target)->tablet_meta()->has_restore_stage());
    // no job id: refused as well
    TSnapshotRequest no_job = kept_request(q, 1);
    no_job.__isset.job_id = false;
    SnapshotLoader no_id(*_engine, ExecEnv::GetInstance(), 0L, 2L);
    std::string path;
    st = no_id.make_local_source_snapshot_and_load(no_job, &path, nullptr);
    EXPECT_TRUE(not_retryable(st)) << st;
}

// an empty local source, and the evidence lost: the shape of the loaded tablet (empty rowsets) is not taken for the
// clean tablet of the job, the judgement is by the evidence only
TEST_F(RestoreTokenStageTest, EmptyLoadedTabletIsNotTakenForACleanOneWithoutEvidence) {
    _next_id = 1120000;
    Prepared p;
    p.origin = create_tablet(8751, false);
    p.target = create_tablet(8752, false);
    write(p.origin, p.origin->tablet_schema(), 2, {}, true);
    write(p.origin, p.origin->tablet_schema(), 3, {}, true);
    SnapshotLoader loader(*_engine, ExecEnv::GetInstance(), 1L, 2L);
    ASSERT_TRUE(load_kept(p, loader, 1, nullptr).ok());
    auto loaded = current(p.target);
    EXPECT_EQ(3, loaded->max_version_unlocked());
    EXPECT_TRUE(loaded->tablet_meta()->has_restore_stage());
    EXPECT_EQ(0, total_rows(loaded, 3));
    // resent: skipped by the evidence
    LocalSourceStats again;
    ASSERT_TRUE(load_kept(p, loader, 1, &again).ok());
    EXPECT_EQ(1, again.tablets);
    EXPECT_EQ(0, again.linked_files + again.copied_files);
    // the evidence is lost: it is refused, not loaded over
    {
        std::lock_guard lock(loaded->get_header_lock());
        loaded->tablet_meta()->clear_restore_stage();
        loaded->save_meta();
    }
    Status st = load_kept(p, loader, 1, nullptr);
    EXPECT_TRUE(not_retryable(st)) << st;
    EXPECT_FALSE(current(p.target)->tablet_meta()->has_restore_stage());
}

// the delay debug point of the load: for the tablet and the backend it is set for
TEST_F(RestoreTokenStageTest, DelayDebugPointsDelayTheTaskOfTheTabletOnly) {
    Prepared p = prepare(false, 8831);
    debug_on();
    SnapshotLoader loader(*_engine, ExecEnv::GetInstance(), 1L, 2L);
    auto elapsed_ms = [](const std::function<void()>& f) {
        auto start = std::chrono::steady_clock::now();
        f();
        return std::chrono::duration_cast<std::chrono::milliseconds>(
                       std::chrono::steady_clock::now() - start)
                .count();
    };
    // a debug point for another tablet or another backend: no delay
    DebugPoints::instance()->add_with_params("SnapshotLoader.make_local_source.delay",
                                             {{"sleep_ms", "400"}, {"tablet_id", "1"}});
    EXPECT_LT(elapsed_ms([&]() { ASSERT_TRUE(load_kept(p, loader, 1, nullptr).ok()); }), 400);
    DebugPoints::instance()->add_with_params("SnapshotLoader.make_local_source.delay",
                                             {{"sleep_ms", "400"}, {"backend", "no.such.host"}});
    EXPECT_LT(elapsed_ms([&]() { ASSERT_TRUE(load_kept(p, loader, 1, nullptr).ok()); }), 400);
    // the tablet
    DebugPoints::instance()->add_with_params(
            "SnapshotLoader.make_local_source.delay",
            {{"sleep_ms", "400"}, {"tablet_id", std::to_string(p.target->tablet_id())}});
    EXPECT_GE(elapsed_ms([&]() { ASSERT_TRUE(load_kept(p, loader, 1, nullptr).ok()); }), 400);
    // the backend
    DebugPoints::instance()->add_with_params(
            "SnapshotLoader.make_local_source.delay",
            {{"sleep_ms", "400"}, {"backend", BackendOptions::get_localhost()}});
    EXPECT_GE(elapsed_ms([&]() { ASSERT_TRUE(load_kept(p, loader, 1, nullptr).ok()); }), 400);
    debug_off();
    EXPECT_EQ(RestoreStagePB::LOCAL_LOADED, stage_of(p.target).stage());
}

// The kept partition of an atomic restore loaded by the snapshot task (TSnapshotRequest.restore_kept_load), and the
// move of the empty snapshot it leaves, which only checks the evidence of the load.
class RestoreTokenKeptLoadTest : public RestoreTokenEngineTest {};

TEST_F(RestoreTokenKeptLoadTest, KeptLoadWritesTheEvidenceAndReturnsAnEmptyDir) {
    Prepared p = prepare(false, 8901);
    SnapshotLoader loader(*_engine, ExecEnv::GetInstance(), 1L, 2L);
    size_t dirs = snapshot_dirs(p.target);
    auto origin_files = tablet_files(p.origin);
    LocalSourceStats stats;
    ASSERT_TRUE(load_kept(p, loader, 1, &stats).ok());
    EXPECT_EQ(1, stats.tablets);
    EXPECT_EQ(1, stats.loaded_tablets);
    EXPECT_EQ(0, stats.copied_files);
    EXPECT_GT(stats.linked_files, 0);
    // the tablet is the origin at the kept version, with the evidence of the load
    auto loaded = current(p.target);
    EXPECT_EQ(3, loaded->max_version_unlocked());
    RestoreStagePB stage = stage_of(p.target);
    EXPECT_EQ(1, stage.job_id());
    EXPECT_EQ(RestoreStagePB::LOCAL_LOADED, stage.stage());
    EXPECT_EQ(3, stage.base_version());
    expect_same_data(p.origin, loaded, 3, Files::LINKED);
    // the snapshot of the local source is released, the snapshot dir is empty: nothing to move
    EXPECT_EQ(dirs + 1, snapshot_dirs(p.target));
    EXPECT_TRUE(dir_is_empty(p.dir));
    // the origin is not changed
    EXPECT_EQ(origin_files, tablet_files(p.origin));
    EXPECT_EQ(3, p.origin->max_version_unlocked());
}

// the origin on another disk: the files are copied into the tablet of the restore
TEST_F(RestoreTokenKeptLoadTest, KeptLoadCopiesOnAnotherDisk) {
    Prepared p = prepare(false, 8981);
    force_copy();
    SnapshotLoader loader(*_engine, ExecEnv::GetInstance(), 1L, 2L);
    size_t dirs = snapshot_dirs(p.target);
    LocalSourceStats stats;
    ASSERT_TRUE(load_kept(p, loader, 1, &stats).ok());
    debug_off();
    EXPECT_EQ(1, stats.tablets);
    EXPECT_EQ(1, stats.loaded_tablets);
    EXPECT_EQ(0, stats.linked_files);
    EXPECT_EQ(0, stats.linked_bytes);
    EXPECT_GT(stats.copied_files, 0);
    EXPECT_GT(stats.copied_bytes, 0);
    auto loaded = current(p.target);
    EXPECT_EQ(3, loaded->max_version_unlocked());
    EXPECT_EQ(RestoreStagePB::LOCAL_LOADED, stage_of(p.target).stage());
    expect_same_data(p.origin, loaded, 3, Files::COPIED);
    EXPECT_EQ(dirs + 1, snapshot_dirs(p.target));
    EXPECT_TRUE(dir_is_empty(p.dir));
    ASSERT_TRUE(loader.move(p.dir, loaded, true).ok());
    EXPECT_EQ(3, current(p.target)->max_version_unlocked());
}

// the origin can not be read at the version: the task fails, the tablet is untouched and no snapshot is left
TEST_F(RestoreTokenKeptLoadTest, KeptLoadFailureLeavesNoSnapshot) {
    Prepared p = prepare(false, 8991);
    SnapshotLoader loader(*_engine, ExecEnv::GetInstance(), 1L, 2L);
    size_t dirs = snapshot_dirs(p.target);
    std::string path;
    // the origin has no version 5
    TSnapshotRequest req = kept_request(p, 1);
    req.__set_version(5);
    EXPECT_FALSE(loader.make_local_source_snapshot_and_load(req, &path, nullptr).ok());
    EXPECT_EQ(dirs, snapshot_dirs(p.target));
    // the origin is not on this backend
    TSnapshotRequest missing = kept_request(p, 1);
    missing.__set_ref_tablet_id(8999);
    EXPECT_FALSE(loader.make_local_source_snapshot_and_load(missing, &path, nullptr).ok());
    EXPECT_EQ(dirs, snapshot_dirs(p.target));
    // nothing was loaded: no evidence, the clean tablet, which a retry loads
    EXPECT_FALSE(current(p.target)->tablet_meta()->has_restore_stage());
    EXPECT_EQ(1, current(p.target)->max_version_unlocked());
    ASSERT_TRUE(load_kept(p, loader, 1, nullptr).ok());
    expect_same_data(p.origin, current(p.target), 3, Files::LINKED);
}

TEST_F(RestoreTokenKeptLoadTest, KeptLoadResentIsSkipped) {
    Prepared p = prepare(false, 8911);
    SnapshotLoader loader(*_engine, ExecEnv::GetInstance(), 1L, 2L);
    ASSERT_TRUE(load_kept(p, loader, 1, nullptr).ok());
    auto files = tablet_files(current(p.target));
    TabletUid uid = current(p.target)->tablet_uid();
    LocalSourceStats again;
    ASSERT_TRUE(load_kept(p, loader, 1, &again).ok());
    // the tablet is counted (the report of the first task was lost), nothing is linked or copied again
    EXPECT_EQ(1, again.tablets);
    EXPECT_EQ(1, again.loaded_tablets);
    EXPECT_EQ(0, again.linked_files + again.copied_files);
    EXPECT_EQ(files, tablet_files(current(p.target)));
    EXPECT_EQ(uid, current(p.target)->tablet_uid());
    EXPECT_TRUE(dir_is_empty(p.dir));
}

TEST_F(RestoreTokenKeptLoadTest, KeptLoadIsSerializedPerTablet) {
    Prepared p = prepare(false, 8921);
    // two tasks of the same tablet at once: one loads, the other finds the evidence and skips
    std::atomic<int64_t> loaded_tablets {0};
    std::atomic<int> loads {0};
    std::atomic<int> failures {0};
    auto run = [&]() {
        SCOPED_ATTACH_TASK(ExecEnv::GetInstance()->orphan_mem_tracker());
        SnapshotLoader loader(*_engine, ExecEnv::GetInstance(), 1L, 2L);
        TSnapshotRequest req = kept_request(p, 1);
        std::string dir_path;
        LocalSourceStats stats;
        Status st = loader.make_local_source_snapshot_and_load(req, &dir_path, &stats);
        if (!st.ok()) {
            ++failures;
        }
        loaded_tablets += stats.loaded_tablets;
        if (stats.linked_files + stats.copied_files > 0) {
            ++loads;
        }
    };
    std::thread t1(run);
    std::thread t2(run);
    t1.join();
    t2.join();
    EXPECT_EQ(0, failures.load());
    // one task loads (links the files), the other finds the tablet loaded; both count the tablet
    EXPECT_EQ(1, loads.load());
    EXPECT_EQ(2, loaded_tablets.load());
    EXPECT_EQ(3, current(p.target)->max_version_unlocked());
    EXPECT_EQ(RestoreStagePB::LOCAL_LOADED, stage_of(p.target).stage());
    expect_same_data(p.origin, current(p.target), 3, Files::LINKED);
}

TEST_F(RestoreTokenKeptLoadTest, MoveOnAnEmptyDirConfirmsTheLoadedTablet) {
    Prepared p = prepare(false, 8931);
    SnapshotLoader loader(*_engine, ExecEnv::GetInstance(), 1L, 2L);
    ASSERT_TRUE(load_kept(p, loader, 1, nullptr).ok());
    auto loaded = current(p.target);
    auto files = tablet_files(loaded);
    TabletUid uid = loaded->tablet_uid();
    // the move of the restore, the plain request: nothing is moved, the evidence is checked
    Status st = loader.move(p.dir, loaded, true);
    ASSERT_TRUE(st.ok()) << st;
    EXPECT_EQ(files, tablet_files(current(p.target)));
    EXPECT_EQ(uid, current(p.target)->tablet_uid());
    EXPECT_EQ(3, current(p.target)->max_version_unlocked());
    EXPECT_EQ(RestoreStagePB::LOCAL_LOADED, stage_of(p.target).stage());
    // the dir is marked as moved
    EXPECT_TRUE(std::filesystem::exists(p.dir + "/LOADED"));
    // resent: ok again
    st = loader.move(p.dir, current(p.target), true);
    EXPECT_TRUE(st.ok()) << st;
    expect_same_data(p.origin, current(p.target), 3, Files::LINKED);
}

TEST_F(RestoreTokenKeptLoadTest, MoveOnAnEmptyDirRefusesWithoutTheRightEvidence) {
    Prepared p = prepare(false, 8941);
    SnapshotLoader loader(*_engine, ExecEnv::GetInstance(), 1L, 2L);
    ASSERT_TRUE(load_kept(p, loader, 1, nullptr).ok());
    auto files = tablet_files(current(p.target));
    // the evidence of another job
    SnapshotLoader other(*_engine, ExecEnv::GetInstance(), 2L, 2L);
    Status st = other.move(p.dir, current(p.target), true);
    EXPECT_TRUE(not_retryable(st)) << st;
    // the evidence of this job, but not of a load
    mark_loaded(current(p.target), 1, 3, RestoreStagePB::APPENDED, 6);
    st = loader.move(p.dir, current(p.target), true);
    EXPECT_TRUE(not_retryable(st)) << st;
    // loaded to another version than the tablet is at
    mark_loaded(current(p.target), 1, 2);
    st = loader.move(p.dir, current(p.target), true);
    EXPECT_TRUE(not_retryable(st)) << st;
    // the evidence dropped, as a tablet overwritten by a clone (the debug point)
    mark_loaded(current(p.target), 1, 3);
    debug_on();
    DebugPoints::instance()->add_with_params(
            "SnapshotLoader.confirm_local_source.clear_stage",
            {{"tablet_id", std::to_string(p.target->tablet_id())}});
    st = loader.move(p.dir, current(p.target), true);
    EXPECT_TRUE(not_retryable(st)) << st;
    EXPECT_FALSE(current(p.target)->tablet_meta()->has_restore_stage());
    debug_off();
    // nothing was moved by the refused tasks
    EXPECT_EQ(files, tablet_files(current(p.target)));
    EXPECT_EQ(3, current(p.target)->max_version_unlocked());
    EXPECT_TRUE(dir_is_empty(p.dir));
}

// a snapshot with the tablet meta (the one a backend without restore_kept_load makes) replaces the tablet as before,
// whether the tablet has the evidence of a load or not
TEST_F(RestoreTokenKeptLoadTest, MoveOnAFullSnapshotDirReplacesEvenWithoutEvidence) {
    Prepared p = prepare(false, 8951);
    // no evidence: the plain local snapshot and move of the kept partition
    LocalSourceStats stats = keep(p.origin, p.target, 3);
    EXPECT_EQ(1, stats.tablets);
    EXPECT_EQ(0, stats.loaded_tablets);
    EXPECT_EQ(3, current(p.target)->max_version_unlocked());
    EXPECT_FALSE(current(p.target)->tablet_meta()->has_restore_stage());
    expect_same_data(p.origin, current(p.target), 3, Files::LINKED);
    // with the evidence of a load: replaced as well, the evidence of the source is dropped by the move
    mark_loaded(current(p.target), 1, 3);
    TabletUid uid = current(p.target)->tablet_uid();
    stats = keep(p.origin, current(p.target), 3);
    EXPECT_EQ(1, stats.tablets);
    EXPECT_EQ(3, current(p.target)->max_version_unlocked());
    EXPECT_FALSE(current(p.target)->tablet_meta()->has_restore_stage());
    EXPECT_NE(uid, current(p.target)->tablet_uid());
    expect_same_data(p.origin, current(p.target), 3, Files::LINKED);
}

TEST_F(RestoreTokenKeptLoadTest, KeptConfirmAfterCompactionIsOk) {
    Prepared p = prepare(false, 8961);
    SnapshotLoader loader(*_engine, ExecEnv::GetInstance(), 1L, 2L);
    ASSERT_TRUE(load_kept(p, loader, 1, nullptr).ok());
    auto loaded = current(p.target);
    compact(loaded, loaded->tablet_schema(), 2, 3);
    EXPECT_EQ(3, loaded->max_version_unlocked());
    Status st = loader.move(p.dir, loaded, true);
    ASSERT_TRUE(st.ok()) << st;
    EXPECT_EQ(RestoreStagePB::LOCAL_LOADED, stage_of(p.target).stage());
    // the compacted rowset holds the rows of the origin at 3
    auto after = shapes(current(p.target), 3);
    ASSERT_EQ(2U, after.size());
    EXPECT_EQ(2, after.back().start);
    EXPECT_EQ(3, after.back().end);
    EXPECT_EQ(total_rows(p.origin, 3), total_rows(current(p.target), 3));
    EXPECT_EQ(5, total_rows(current(p.target), 3));
}

TEST_F(RestoreTokenKeptLoadTest, MergeOnWriteKeptLoadThenCompactionThenConfirmEqualsSource) {
    Prepared p = prepare(true, 8971);
    SnapshotLoader loader(*_engine, ExecEnv::GetInstance(), 1L, 2L);
    LocalSourceStats stats;
    ASSERT_TRUE(load_kept(p, loader, 1, &stats).ok());
    EXPECT_EQ(1, stats.loaded_tablets);
    auto loaded = current(p.target);
    expect_same_data(p.origin, loaded, 3, Files::LINKED);
    compact(loaded, loaded->tablet_schema(), 2, 3);
    Status st = loader.move(p.dir, loaded, true);
    ASSERT_TRUE(st.ok()) << st;
    EXPECT_EQ(3, current(p.target)->max_version_unlocked());
    EXPECT_EQ(RestoreStagePB::LOCAL_LOADED, stage_of(p.target).stage());
    auto after = shapes(current(p.target), 3);
    ASSERT_EQ(2U, after.size());
    EXPECT_EQ(2, after.back().start);
    EXPECT_EQ(3, after.back().end);
    EXPECT_EQ(total_rows(p.origin, 3), total_rows(current(p.target), 3));
    EXPECT_EQ(6, total_rows(current(p.target), 3));
    EXPECT_TRUE(bitmap_marks(current(p.target), 3).empty());
}

} // namespace doris
