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

#include "storage/restore_digest/restore_digest.h"

#include <gen_cpp/AgentService_types.h>
#include <gen_cpp/Descriptors_types.h>
#include <gen_cpp/PaloInternalService_types.h>
#include <gen_cpp/Types_types.h>
#include <gen_cpp/olap_file.pb.h>
#include <gtest/gtest.h>
#include <sys/stat.h>
#include <unistd.h>
#include <xxh3.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <cstring>
#include <filesystem>
#include <functional>
#include <map>
#include <memory>
#include <optional>
#include <random>
#include <set>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "agent/task_worker_pool.h"
#include "common/config.h"
#include "common/status.h"
#include "core/block/block.h"
#include "core/column/column.h"
#include "core/field.h"
#include "core/value/vdatetime_value.h"
#include "io/fs/local_file_system.h"
#include "runtime/exec_env.h"
#include "runtime/snapshot_loader.h"
#include "service/backend_options.h"
#include "storage/data_dir.h"
#include "storage/delete/delete_handler.h"
#include "storage/merger.h"
#include "storage/mow/mow_transform_test_base.h"
#include "storage/olap_common.h"
#include "storage/options.h"
#include "storage/rowid_conversion.h"
#include "storage/rowset/rowset.h"
#include "storage/rowset/rowset_factory.h"
#include "storage/rowset/rowset_reader.h"
#include "storage/rowset/rowset_writer.h"
#include "storage/rowset/rowset_writer_context.h"
#include "storage/snapshot/snapshot_manager.h"
#include "storage/storage_engine.h"
#include "storage/tablet/tablet.h"
#include "storage/tablet/tablet_manager.h"
#include "storage/tablet/tablet_meta.h"
#include "storage/tablet/tablet_schema.h"
#include "storage/utils.h"
#include "testutil/creators.h"
#include "util/debug_points.h"
#include "util/defer_op.h"
#include "util/jsonb_parser_simd.h"
#include "util/jsonb_writer.h"
#include "util/sha.h"

namespace doris {
using namespace ErrorCode;

namespace restore_digest_ut {

struct Cell {
    bool null = false;
    std::string b; // raw bytes for fixed width types, content for strings
    std::optional<Field> field; // ARRAY / MAP / STRUCT values
};
using Row = std::vector<Cell>;

Cell N() {
    Cell c;
    c.null = true;
    return c;
}
template <typename T>
Cell V(T v) {
    Cell c;
    c.b.assign(reinterpret_cast<const char*>(&v), sizeof(T));
    return c;
}
Cell S(std::string s) {
    Cell c;
    c.b = std::move(s);
    return c;
}
// the JSONB binary of a JSON text
std::string jsonb(const std::string& json) {
    JsonbWriter writer;
    Status st = JsonbParser::parse(json.data(), json.size(), writer);
    EXPECT_TRUE(st.ok()) << st;
    return std::string(writer.getOutput()->getBuffer(), writer.getOutput()->getSize());
}
Cell F(Field f) {
    Cell c;
    c.field = std::move(f);
    return c;
}
// DATE / DATETIME v1 (the in-memory packed VecDateTimeValue)
Cell D1(int y, int mo, int d, int h = 0, int mi = 0, int sec = 0, bool datetime = false) {
    VecDateTimeValue v;
    v.unchecked_set_time(y, mo, d, h, mi, sec);
    v.set_type(datetime ? TIME_DATETIME : TIME_DATE);
    Cell c;
    c.b.assign(reinterpret_cast<const char*>(&v), sizeof(v));
    return c;
}
// nested values
Field FI(int32_t v) {
    return Field::create_field<TYPE_INT>(v);
}
Field FS(std::string v) {
    return Field::create_field<TYPE_STRING>(std::move(v));
}
Field FNull() {
    return Field();
}
Field FArr(Array a) {
    return Field::create_field<TYPE_ARRAY>(std::move(a));
}
Field FMap(Array keys, Array values) {
    Map m;
    m.push_back(FArr(std::move(keys)));
    m.push_back(FArr(std::move(values)));
    return Field::create_field<TYPE_MAP>(std::move(m));
}
Field FStruct(Struct st) {
    return Field::create_field<TYPE_STRUCT>(std::move(st));
}

struct ColSpec {
    ColSpec(std::string name_, std::string type_, bool key_ = false, bool nullable_ = true,
            int length_ = 4, int precision_ = 0, int frac_ = 0)
            : name(std::move(name_)),
              type(std::move(type_)),
              key(key_),
              nullable(nullable_),
              length(length_),
              precision(precision_),
              frac(frac_) {}
    std::string name;
    std::string type;
    bool key;
    bool nullable;
    int length;
    int precision;
    int frac;
    std::string aggregation;
    std::string default_value;
    std::vector<ColSpec> children; // ARRAY: element, MAP: key and value, STRUCT: fields
};

int32_t key_of(const Row& r) {
    int32_t k = 0;
    std::memcpy(&k, r[0].b.data(), sizeof(k));
    return k;
}

} // namespace restore_digest_ut

using namespace restore_digest_ut;

static StorageEngine* rd_engine_ref = nullptr;

class RestoreDigestTest : public ::testing::Test {
protected:
    void SetUp() override {
        char buffer[1024];
        ASSERT_NE(getcwd(buffer, sizeof(buffer)), nullptr);
        _dir = std::string(buffer) + "/ut_dir/restore_digest_test";
        auto st = io::global_local_filesystem()->delete_directory(_dir);
        ASSERT_TRUE(st.ok()) << st;
        st = io::global_local_filesystem()->create_directory(_dir);
        ASSERT_TRUE(st.ok()) << st;
        ASSERT_TRUE(io::global_local_filesystem()->create_directory(_dir + "/tablet_path").ok());

        doris::EngineOptions options;
        auto engine = std::make_unique<StorageEngine>(options);
        rd_engine_ref = engine.get();
        ExecEnv::GetInstance()->set_storage_engine(std::move(engine));
        _data_dir = new DataDir(*rd_engine_ref, _dir, 100000000);
        static_cast<void>(_data_dir->init());
    }
    void TearDown() override {
        SAFE_DELETE(_data_dir);
        EXPECT_TRUE(io::global_local_filesystem()->delete_directory(_dir).ok());
        rd_engine_ref = nullptr;
        ExecEnv::GetInstance()->set_storage_engine(nullptr);
    }

    // ---------------------------------------------------------------- schema
    TabletSchemaSPtr make_schema(KeysType keys_type, const std::vector<ColSpec>& cols,
                                 int sequence_idx = -1,
                                 const std::vector<uint32_t>& cluster_key_uids = {},
                                 int schema_version = 0) {
        auto schema = std::make_shared<TabletSchema>();
        TabletSchemaPB pb;
        pb.set_keys_type(keys_type);
        pb.set_num_short_key_columns(1);
        pb.set_num_rows_per_row_block(1024);
        pb.set_compress_kind(COMPRESS_NONE);
        pb.set_next_column_unique_id(static_cast<int32_t>(cols.size()) + 1);
        pb.set_schema_version(schema_version);
        if (sequence_idx >= 0) {
            pb.set_sequence_col_idx(sequence_idx);
        }
        for (uint32_t uid : cluster_key_uids) {
            pb.add_cluster_key_uids(uid);
        }
        int uid = 1;
        std::function<void(ColumnPB*, const ColSpec&, int)> fill = [&](ColumnPB* c,
                                                                       const ColSpec& spec,
                                                                       int unique_id) {
            c->set_unique_id(unique_id);
            c->set_name(spec.name);
            c->set_type(spec.type);
            c->set_is_key(spec.key);
            c->set_length(spec.length);
            c->set_index_length(spec.length);
            c->set_is_nullable(spec.nullable);
            c->set_is_bf_column(false);
            if (spec.precision > 0 || spec.frac > 0) {
                c->set_precision(spec.precision);
                c->set_frac(spec.frac);
            }
            if (!spec.aggregation.empty()) {
                c->set_aggregation(spec.aggregation);
            }
            if (!spec.default_value.empty()) {
                c->set_default_value(spec.default_value);
            }
            for (const auto& child : spec.children) {
                fill(c->add_children_columns(), child, -1);
            }
        };
        for (const auto& spec : cols) {
            fill(pb.add_column(), spec, uid++);
        }
        schema->init_from_pb(pb);
        return schema;
    }

    static std::vector<ColSpec> wide_cols() {
        return {{"k", "INT", true, false, 4},
                {"c_bool", "BOOLEAN", false, true, 1},
                {"c_i8", "TINYINT", false, true, 1},
                {"c_i16", "SMALLINT", false, true, 2},
                {"c_i64", "BIGINT", false, true, 8},
                {"c_li", "LARGEINT", false, true, 16},
                {"c_f", "FLOAT", false, true, 4},
                {"c_d", "DOUBLE", false, true, 8},
                {"c_dec32", "DECIMAL32", false, true, 4, 9, 2},
                {"c_dec64", "DECIMAL64", false, true, 8, 18, 4},
                {"c_dec128", "DECIMAL128I", false, true, 16, 38, 10},
                {"c_dec256", "DECIMAL256", false, true, 32, 76, 20},
                {"c_date", "DATEV2", false, true, 4},
                {"c_dt", "DATETIMEV2", false, true, 8, 0, 3},
                {"c_ip4", "IPV4", false, true, 4},
                {"c_ip6", "IPV6", false, true, 16},
                {"c_vc", "VARCHAR", false, true, 32},
                {"c_ch", "CHAR", false, true, 8},
                {"c_str", "STRING", false, true, 64}};
    }

    // Deterministic row for a wide schema; every nullable column is sometimes NULL.
    static Row wide_row(int i) {
        auto nul = [&](int c) { return (i + c) % (c + 3) == 0; };
        uint64_t m = static_cast<uint64_t>(i) * 2654435761ULL + 12345;
        Row r;
        r.push_back(V<int32_t>(i));
        r.push_back(nul(1) ? N() : V<uint8_t>(i % 2));
        r.push_back(nul(2) ? N() : V<int8_t>(static_cast<int8_t>(m % 100)));
        r.push_back(nul(3) ? N() : V<int16_t>(static_cast<int16_t>(m % 30000)));
        r.push_back(nul(4) ? N() : V<int64_t>(static_cast<int64_t>(m) * 7));
        r.push_back(nul(5) ? N() : V<__int128>((static_cast<__int128>(m) << 40) + i));
        r.push_back(nul(6) ? N() : V<float>(static_cast<float>(i) * 0.25F));
        r.push_back(nul(7) ? N() : V<double>(static_cast<double>(i) * 0.125));
        r.push_back(nul(8) ? N() : V<int32_t>(static_cast<int32_t>(m % 1000000)));
        r.push_back(nul(9) ? N() : V<int64_t>(static_cast<int64_t>(m % 100000000)));
        r.push_back(nul(10) ? N() : V<__int128>(static_cast<__int128>(m) * 1000003));
        {
            Cell c;
            if (nul(11)) {
                c.null = true;
            } else {
                c.b.assign(32, '\0');
                std::memcpy(c.b.data(), &m, sizeof(m));
                c.b[16] = static_cast<char>(i);
            }
            r.push_back(std::move(c));
        }
        r.push_back(nul(12) ? N() : V<uint32_t>(0x2F0000U + (i % 28)));
        r.push_back(nul(13) ? N() : V<uint64_t>(0x1F00000000ULL + static_cast<uint64_t>(i) * 1000));
        r.push_back(nul(14) ? N() : V<uint32_t>(0x0A000000U + i));
        r.push_back(nul(15) ? N()
                            : V<unsigned __int128>((static_cast<unsigned __int128>(1) << 100) + i));
        r.push_back(nul(16) ? N() : S(std::string("vc") + std::to_string(i % 97)));
        r.push_back(nul(17) ? N() : S(std::string("ab").substr(0, i % 3) + std::to_string(i % 10)));
        r.push_back(nul(18) ? N() : S(std::string(static_cast<size_t>(i % 5), 'z') + "s"));
        return r;
    }

    // ---------------------------------------------------------------- rowsets
    RowsetWriterContext make_writer_context(const TabletSchemaSPtr& schema, const Version& version,
                                            bool overlapping) {
        RowsetWriterContext ctx;
        RowsetId id;
        id.init(_next_rowset_id++);
        ctx.rowset_id = id;
        ctx.rowset_type = BETA_ROWSET;
        ctx.data_dir = _data_dir;
        ctx.rowset_state = VISIBLE;
        ctx.tablet_schema = schema;
        ctx.tablet_path = _dir + "/tablet_path";
        ctx.version = version;
        ctx.segments_overlap = overlapping ? OVERLAPPING : NONOVERLAPPING;
        ctx.max_rows_per_segment = UINT32_MAX;
        return ctx;
    }

    Status append_block(RowsetWriter* writer, const TabletSchemaSPtr& schema,
                        const std::vector<Row>& rows) {
        Block block = schema->create_storage_block();
        auto columns = std::move(block).mutate_columns();
        for (const Row& r : rows) {
            for (size_t c = 0; c < r.size(); ++c) {
                if (r[c].null) {
                    columns[c]->insert_data(nullptr, 0);
                } else if (r[c].field.has_value()) {
                    columns[c]->insert(*r[c].field);
                } else {
                    columns[c]->insert_data(r[c].b.data(), r[c].b.size());
                }
            }
        }
        block.set_columns(std::move(columns));
        RETURN_IF_ERROR(writer->add_block(&block));
        return writer->flush();
    }

    // Writes `rows` as one rowset of version `version`. `rows_per_segment` decides the segment
    // split; with `overlapping` the rows are dealt round-robin to the segments, otherwise the
    // rowset is globally sorted by key and cut into consecutive segments.
    RowsetSharedPtr write_rowset(const TabletSchemaSPtr& schema, std::vector<Row> rows,
                                 int64_t version, size_t rows_per_segment, bool overlapping,
                                 int64_t start_version = -1) {
        auto by_key = [](const Row& a, const Row& b) { return key_of(a) < key_of(b); };
        std::vector<std::vector<Row>> segments;
        if (overlapping) {
            size_t n = std::max<size_t>(1, (rows.size() + rows_per_segment - 1) / rows_per_segment);
            segments.resize(n);
            for (size_t i = 0; i < rows.size(); ++i) {
                segments[i % n].push_back(std::move(rows[i]));
            }
        } else {
            std::stable_sort(rows.begin(), rows.end(), by_key);
            for (size_t i = 0; i < rows.size(); i += rows_per_segment) {
                size_t end = std::min(rows.size(), i + rows_per_segment);
                segments.emplace_back(std::make_move_iterator(rows.begin() + i),
                                      std::make_move_iterator(rows.begin() + end));
            }
        }
        auto ctx = make_writer_context(
                schema, Version(start_version < 0 ? version : start_version, version), overlapping);
        auto res = RowsetFactory::create_rowset_writer(*rd_engine_ref, ctx, true);
        EXPECT_TRUE(res.has_value()) << res.error();
        auto writer = std::move(res).value();
        for (auto& seg : segments) {
            std::stable_sort(seg.begin(), seg.end(), by_key);
            Status st = append_block(writer.get(), schema, seg);
            EXPECT_TRUE(st.ok()) << st;
        }
        RowsetSharedPtr rowset;
        EXPECT_EQ(Status::OK(), writer->build(rowset));
        return rowset;
    }

    // ---------------------------------------------------------------- digest
    RestoreDigest digest_of(const TabletSchemaSPtr& schema, std::vector<RowsetSharedPtr> rowsets,
                            KeysType keys_type = DUP_KEYS, bool mow = false,
                            DeleteBitmapPtr bitmap = nullptr, int64_t version = 100) {
        RestoreDigestInput in;
        in.rowsets = std::move(rowsets);
        in.schema = schema;
        in.keys_type = keys_type;
        in.enable_mow = mow;
        in.delete_bitmap = std::move(bitmap);
        in.version = version;
        RestoreDigest d;
        Status st = compute_restore_digest(in, &d);
        EXPECT_TRUE(st.ok()) << st;
        return d;
    }

    static bool same_digest(const RestoreDigest& a, const RestoreDigest& b) {
        if (a.root != b.root || a.rows != b.rows) {
            return false;
        }
        for (size_t i = 0; i < RestoreDigest::kNumBuckets; ++i) {
            if (a.buckets[i].sum != b.buckets[i].sum || a.buckets[i].count != b.buckets[i].count) {
                return false;
            }
        }
        return true;
    }

    // digest of rows written as one rowset
    RestoreDigest digest_rows(const TabletSchemaSPtr& schema, const std::vector<Row>& rows) {
        return digest_of(schema, {write_rowset(schema, rows, 2, 1000, false)});
    }


    // ---------------------------------------------------------------- helpers P2
    static ColSpec with_children(ColSpec c, std::vector<ColSpec> children) {
        c.children = std::move(children);
        return c;
    }

    // A tablet object for the Merger and the MoR reader; only its meta (keys type) matters.
    TabletSharedPtr make_meta_tablet(KeysType keys_type, const TabletSchemaSPtr& schema) {
        std::vector<TColumn> tcols;
        std::unordered_map<uint32_t, uint32_t> col_ordinal_to_unique_id;
        for (size_t i = 0; i < schema->num_columns(); ++i) {
            TColumn col;
            col.column_type.type = TPrimitiveType::INT;
            col.__set_column_name(schema->column(i).name());
            col.__set_is_key(schema->column(i).is_key());
            tcols.push_back(col);
            col_ordinal_to_unique_id[i] = schema->column(i).unique_id();
        }
        TTabletSchema t_schema;
        t_schema.__set_short_key_column_count(1);
        t_schema.__set_schema_hash(3333);
        t_schema.__set_keys_type(keys_type == UNIQUE_KEYS ? TKeysType::UNIQUE_KEYS
                                                          : TKeysType::DUP_KEYS);
        t_schema.__set_storage_type(TStorageType::COLUMN);
        t_schema.__set_columns(tcols);
        TabletMetaSharedPtr tablet_meta(new TabletMeta(
                2, 2, 2, 2, 2, 2, t_schema, 2, col_ordinal_to_unique_id, UniqueId(1, 2),
                TTabletType::TABLET_TYPE_DISK, TCompressionType::LZ4F, 0, false));
        TabletSharedPtr tablet(new Tablet(*rd_engine_ref, tablet_meta, _data_dir));
        static_cast<void>(tablet->init());
        return tablet;
    }

    // Merges `inputs` into one rowset of version `out`, the way a compaction does (vertical
    // merge: the delete predicates among the inputs are applied, unique keys are merged).
    RowsetSharedPtr compact(const TabletSchemaSPtr& schema, const TabletSharedPtr& tablet,
                            const std::vector<RowsetSharedPtr>& inputs, Version out,
                            ReaderType reader_type = ReaderType::READER_BASE_COMPACTION) {
        std::vector<RowsetReaderSharedPtr> readers;
        for (const auto& rs : inputs) {
            RowsetReaderSharedPtr reader;
            EXPECT_TRUE(rs->create_reader(&reader).ok());
            readers.push_back(std::move(reader));
        }
        auto ctx = make_writer_context(schema, out, false);
        auto res = RowsetFactory::create_rowset_writer(*rd_engine_ref, ctx, true);
        EXPECT_TRUE(res.has_value()) << res.error();
        auto writer = std::move(res).value();
        Merger::Statistics stats;
        RowIdConversion rowid_conversion;
        stats.rowid_conversion = &rowid_conversion;
        Status st = Merger::vertical_merge_rowsets(tablet, reader_type, *schema, readers,
                                                   writer.get(), 100, 2, &stats);
        EXPECT_TRUE(st.ok()) << st;
        RowsetSharedPtr merged;
        EXPECT_EQ(Status::OK(), writer->build(merged));
        return merged;
    }

    static TCondition cond(const std::string& column, const std::string& op,
                           std::vector<std::string> values) {
        TCondition c;
        c.column_name = column;
        c.condition_op = op;
        c.condition_values = std::move(values);
        return c;
    }

    // A rowset without rows which carries a DELETE condition (what DELETE FROM ... WHERE writes).
    RowsetSharedPtr write_delete_rowset(const TabletSchemaSPtr& schema, int64_t version,
                                        const std::vector<TCondition>& conditions) {
        DeletePredicatePB pred;
        Status st = DeleteHandler::generate_delete_predicate(*schema, conditions, &pred);
        EXPECT_TRUE(st.ok()) << st;
        auto ctx = make_writer_context(schema, Version(version, version), false);
        auto res = RowsetFactory::create_rowset_writer(*rd_engine_ref, ctx, true);
        EXPECT_TRUE(res.has_value()) << res.error();
        auto writer = std::move(res).value();
        RowsetSharedPtr rowset;
        EXPECT_EQ(Status::OK(), writer->build(rowset));
        rowset->rowset_meta()->set_delete_predicate(std::move(pred));
        return rowset;
    }

    RestoreDigest mor_digest(const TabletSchemaSPtr& schema, std::vector<RowsetSharedPtr> rowsets,
                             const TabletSharedPtr& tablet, int64_t version = 100) {
        RestoreDigestInput in;
        in.rowsets = std::move(rowsets);
        in.schema = schema;
        in.keys_type = UNIQUE_KEYS;
        in.enable_mow = false;
        in.version = version;
        in.tablet = tablet;
        RestoreDigest d;
        Status st = compute_restore_digest(in, &d);
        EXPECT_TRUE(st.ok()) << st;
        return d;
    }

    RestoreDigest threaded_digest(const TabletSchemaSPtr& schema,
                                  std::vector<RowsetSharedPtr> rowsets, KeysType keys_type,
                                  bool mow, DeleteBitmapPtr bitmap, int64_t version, int threads) {
        RestoreDigestInput in;
        in.rowsets = std::move(rowsets);
        in.schema = schema;
        in.keys_type = keys_type;
        in.enable_mow = mow;
        in.delete_bitmap = std::move(bitmap);
        in.version = version;
        in.threads = threads;
        RestoreDigest d;
        Status st = compute_restore_digest(in, &d);
        EXPECT_TRUE(st.ok()) << st;
        return d;
    }

    std::string _dir;
    DataDir* _data_dir = nullptr;
    int64_t _next_rowset_id = 10000;
};

// ===== 1. layout independence ===============================================================

TEST_F(RestoreDigestTest, LayoutIndependence) {
    auto schema = make_schema(DUP_KEYS, wide_cols());
    std::vector<Row> rows;
    for (int i = 0; i < 400; ++i) {
        rows.push_back(wide_row(i));
    }
    // exact duplicate rows: multiplicity must be preserved by every layout
    for (int i = 0; i < 20; ++i) {
        rows.push_back(wide_row(i * 3));
    }
    RestoreDigest base = digest_of(schema, {write_rowset(schema, rows, 2, 100000, false)});
    ASSERT_EQ(rows.size(), base.rows);
    ASSERT_EQ(rows.size(), base.rows_scanned);

    std::mt19937 rng(42);
    // layout B: three rowsets, shuffled arrival, overlapping segments of different sizes
    {
        auto shuffled = rows;
        std::shuffle(shuffled.begin(), shuffled.end(), rng);
        size_t third = shuffled.size() / 3;
        std::vector<Row> p1(shuffled.begin(), shuffled.begin() + third);
        std::vector<Row> p2(shuffled.begin() + third, shuffled.begin() + 2 * third);
        std::vector<Row> p3(shuffled.begin() + 2 * third, shuffled.end());
        auto b = digest_of(schema, {write_rowset(schema, p3, 4, 37, true),
                                    write_rowset(schema, p1, 2, 50, true),
                                    write_rowset(schema, p2, 3, 17, false)});
        EXPECT_TRUE(same_digest(base, b));
        EXPECT_EQ(base.schema_sig, b.schema_sig);
    }
    // layout C: many small rowsets and tiny segments
    {
        auto shuffled = rows;
        std::shuffle(shuffled.begin(), shuffled.end(), rng);
        std::vector<RowsetSharedPtr> rowsets;
        int64_t version = 2;
        for (size_t i = 0; i < shuffled.size(); i += 21) {
            size_t end = std::min(shuffled.size(), i + 21);
            std::vector<Row> part(shuffled.begin() + i, shuffled.begin() + end);
            const bool overlapping = version % 2 == 0;
            rowsets.push_back(write_rowset(schema, part, version, 5, overlapping));
            ++version;
        }
        auto c = digest_of(schema, rowsets);
        EXPECT_TRUE(same_digest(base, c));
    }
    // the digest is read only and repeatable
    EXPECT_TRUE(same_digest(base, digest_of(schema, {write_rowset(schema, rows, 2, 64, true)})));
}

TEST_F(RestoreDigestTest, CompactionKeepsDigest) {
    // (k INT key, v INT) duplicate table, the same shape the vertical compaction test uses
    std::vector<ColSpec> cols = {{"c1", "INT", true, false, 4}, {"c2", "INT", false, false, 4}};
    auto schema = make_schema(DUP_KEYS, cols);
    std::vector<Row> r1;
    std::vector<Row> r2;
    for (int i = 0; i < 3000; ++i) {
        r1.push_back({V<int32_t>(i * 2), V<int32_t>(i)});
        r2.push_back({V<int32_t>(i * 3), V<int32_t>(i % 11)});
    }
    for (int i = 0; i < 50; ++i) { // identical duplicates across the inputs
        r2.push_back({V<int32_t>(i * 2), V<int32_t>(i)});
    }
    std::vector<RowsetSharedPtr> inputs = {write_rowset(schema, r1, 2, 700, false),
                                           write_rowset(schema, r2, 3, 900, true)};
    RestoreDigest before = digest_of(schema, inputs);
    ASSERT_EQ(r1.size() + r2.size(), before.rows);

    // compact the two rowsets into one
    std::vector<TColumn> tcols;
    std::unordered_map<uint32_t, uint32_t> col_ordinal_to_unique_id;
    for (size_t i = 0; i < schema->num_columns(); ++i) {
        TColumn col;
        col.column_type.type = TPrimitiveType::INT;
        col.__set_column_name(schema->column(i).name());
        col.__set_is_key(schema->column(i).is_key());
        tcols.push_back(col);
        col_ordinal_to_unique_id[i] = schema->column(i).unique_id();
    }
    TTabletSchema t_schema;
    t_schema.__set_short_key_column_count(1);
    t_schema.__set_schema_hash(3333);
    t_schema.__set_keys_type(TKeysType::DUP_KEYS);
    t_schema.__set_storage_type(TStorageType::COLUMN);
    t_schema.__set_columns(tcols);
    TabletMetaSharedPtr tablet_meta(
            new TabletMeta(2, 2, 2, 2, 2, 2, t_schema, 2, col_ordinal_to_unique_id, UniqueId(1, 2),
                           TTabletType::TABLET_TYPE_DISK, TCompressionType::LZ4F, 0, false));
    TabletSharedPtr tablet(new Tablet(*rd_engine_ref, tablet_meta, _data_dir));
    static_cast<void>(tablet->init());

    std::vector<RowsetReaderSharedPtr> readers;
    for (auto& rs : inputs) {
        RowsetReaderSharedPtr reader;
        ASSERT_TRUE(rs->create_reader(&reader).ok());
        readers.push_back(std::move(reader));
    }
    auto ctx = make_writer_context(schema, Version(0, 3), false);
    auto res = RowsetFactory::create_rowset_writer(*rd_engine_ref, ctx, true);
    ASSERT_TRUE(res.has_value()) << res.error();
    auto out_writer = std::move(res).value();
    Merger::Statistics stats;
    RowIdConversion rowid_conversion;
    stats.rowid_conversion = &rowid_conversion;
    Status st = Merger::vertical_merge_rowsets(tablet, ReaderType::READER_BASE_COMPACTION, *schema,
                                               readers, out_writer.get(), 100, 2, &stats);
    ASSERT_TRUE(st.ok()) << st;
    RowsetSharedPtr merged;
    ASSERT_EQ(Status::OK(), out_writer->build(merged));
    ASSERT_EQ(before.rows, merged->num_rows());

    RestoreDigest after = digest_of(schema, {merged});
    EXPECT_TRUE(same_digest(before, after));
}

// ===== 2. any change shows ===================================================================

TEST_F(RestoreDigestTest, AnyChangeChangesDigest) {
    auto schema = make_schema(DUP_KEYS, wide_cols());
    std::vector<Row> rows;
    for (int i = 0; i < 60; ++i) {
        rows.push_back(wide_row(i));
    }
    RestoreDigest base = digest_rows(schema, rows);
    std::vector<std::string> roots {base.root};

    // change every non-key column of one row, one at a time
    const size_t target = 7;
    for (size_t c = 1; c < rows[target].size(); ++c) {
        auto mutated = rows;
        // pick a replacement which is different from the original value
        const Cell& orig = rows[target][c];
        Cell repl;
        if (orig.null) {
            // NULL -> some non-NULL value of the same column
            for (int i = 0; i < 200 && (repl.null || repl.b.empty()); ++i) {
                repl = wide_row(i)[c];
                if (!repl.null && c < 16) {
                    break;
                }
                if (!repl.null && !repl.b.empty()) {
                    break;
                }
                repl = N();
            }
            ASSERT_FALSE(repl.null);
        } else {
            repl = orig;
            if (c >= 16) { // strings
                repl.b += "x";
            } else {
                repl.b[0] = static_cast<char>(repl.b[0] ^ 0x01);
            }
        }
        mutated[target][c] = repl;
        RestoreDigest d = digest_rows(schema, mutated);
        EXPECT_NE(base.root, d.root) << "column " << c << " changed but digest did not";
        roots.push_back(d.root);

        // and set to NULL when it was not NULL
        if (!orig.null) {
            auto nulled = rows;
            nulled[target][c] = N();
            RestoreDigest dn = digest_rows(schema, nulled);
            EXPECT_NE(base.root, dn.root) << "column " << c << " set to NULL";
            roots.push_back(dn.root);
        }
    }
    // delete one row
    {
        auto fewer = rows;
        fewer.erase(fewer.begin() + 13);
        RestoreDigest d = digest_rows(schema, fewer);
        EXPECT_NE(base.root, d.root);
        EXPECT_EQ(base.rows - 1, d.rows);
        roots.push_back(d.root);
    }
    // add one duplicate row
    {
        auto more = rows;
        more.push_back(rows[21]);
        RestoreDigest d = digest_rows(schema, more);
        EXPECT_NE(base.root, d.root);
        EXPECT_EQ(base.rows + 1, d.rows);
        roots.push_back(d.root);
    }
    // all of these digests are pairwise different
    std::set<std::string> uniq(roots.begin(), roots.end());
    EXPECT_EQ(roots.size(), uniq.size());

    // swapping the values of two rows (same multiset of column values) changes the digest
    {
        std::vector<ColSpec> cols = {{"k", "INT", true, false, 4}, {"v", "INT", false, false, 4}};
        auto s2 = make_schema(DUP_KEYS, cols);
        auto d1 =
                digest_rows(s2, {{V<int32_t>(1), V<int32_t>(10)}, {V<int32_t>(2), V<int32_t>(20)}});
        auto d2 =
                digest_rows(s2, {{V<int32_t>(1), V<int32_t>(20)}, {V<int32_t>(2), V<int32_t>(10)}});
        EXPECT_NE(d1.root, d2.root);
    }
}

// ===== 3. encoding is injective ================================================================

namespace {
void expect_pairwise_different(const std::vector<std::pair<std::string, std::string>>& cases) {
    std::set<std::string> seen;
    for (const auto& [name, root] : cases) {
        EXPECT_TRUE(seen.insert(root).second) << "digest collision at case " << name;
    }
}
} // namespace

TEST_F(RestoreDigestTest, EncodingIsInjective) {
    // two nullable strings
    {
        auto schema = make_schema(DUP_KEYS, {{"k", "INT", true, false, 4},
                                             {"a", "VARCHAR", false, true, 16},
                                             {"b", "VARCHAR", false, true, 16}});
        auto one = [&](Cell a, Cell b) {
            return digest_rows(schema, {{V<int32_t>(1), std::move(a), std::move(b)}}).root;
        };
        expect_pairwise_different({{"(a,bc)", one(S("a"), S("bc"))},
                                   {"(ab,c)", one(S("ab"), S("c"))},
                                   {"(NULL,'')", one(N(), S(""))},
                                   {"('',NULL)", one(S(""), N())},
                                   {"(NULL,NULL)", one(N(), N())},
                                   {"('','')", one(S(""), S(""))},
                                   {"(abc,NULL)", one(S("abc"), N())},
                                   {"(NULL,abc)", one(N(), S("abc"))},
                                   {"(abc,'')", one(S("abc"), S(""))},
                                   {"('',abc)", one(S(""), S("abc"))}});
    }
    // NULL vs 0, and which column is NULL
    {
        auto schema = make_schema(DUP_KEYS, {{"k", "INT", true, false, 4},
                                             {"a", "INT", false, true, 4},
                                             {"b", "INT", false, true, 4}});
        auto one = [&](Cell a, Cell b) {
            return digest_rows(schema, {{V<int32_t>(1), std::move(a), std::move(b)}}).root;
        };
        expect_pairwise_different({{"(NULL,NULL)", one(N(), N())},
                                   {"(0,0)", one(V<int32_t>(0), V<int32_t>(0))},
                                   {"(NULL,0)", one(N(), V<int32_t>(0))},
                                   {"(0,NULL)", one(V<int32_t>(0), N())},
                                   {"(NULL,1)", one(N(), V<int32_t>(1))},
                                   {"(1,NULL)", one(V<int32_t>(1), N())}});
    }
    // CHAR padding boundaries: padding is not part of the value, spaces are
    {
        auto schema = make_schema(DUP_KEYS,
                                  {{"k", "INT", true, false, 4}, {"c", "CHAR", false, true, 8}});
        auto one = [&](Cell c) {
            return digest_rows(schema, {{V<int32_t>(1), std::move(c)}}).root;
        };
        expect_pairwise_different({{"'a'", one(S("a"))},
                                   {"'a '", one(S("a "))},
                                   {"' a'", one(S(" a"))},
                                   {"''", one(S(""))},
                                   {"' '", one(S(" "))},
                                   {"NULL", one(N())},
                                   {"'ab'", one(S("ab"))}});
    }
    // float / double use the raw bits: -0 and +0 differ, NULL differs from both
    {
        auto schema = make_schema(DUP_KEYS, {{"k", "INT", true, false, 4},
                                             {"f", "FLOAT", false, true, 4},
                                             {"d", "DOUBLE", false, true, 8}});
        auto one = [&](Cell f, Cell d) {
            return digest_rows(schema, {{V<int32_t>(1), std::move(f), std::move(d)}}).root;
        };
        expect_pairwise_different({{"(0,0)", one(V<float>(0.0F), V<double>(0.0))},
                                   {"(-0,0)", one(V<float>(-0.0F), V<double>(0.0))},
                                   {"(0,-0)", one(V<float>(0.0F), V<double>(-0.0))},
                                   {"(NULL,0)", one(N(), V<double>(0.0))},
                                   {"(0,NULL)", one(V<float>(0.0F), N())}});
    }
    // boolean, wide integers, ipv6 corner values
    {
        auto schema = make_schema(DUP_KEYS, {{"k", "INT", true, false, 4},
                                             {"b", "BOOLEAN", false, true, 1},
                                             {"l", "LARGEINT", false, true, 16},
                                             {"ip", "IPV6", false, true, 16}});
        auto one = [&](Cell b, Cell l, Cell ip) {
            return digest_rows(schema, {{V<int32_t>(1), std::move(b), std::move(l), std::move(ip)}})
                    .root;
        };
        const __int128 big = static_cast<__int128>(1) << 100;
        expect_pairwise_different(
                {{"base", one(V<uint8_t>(0), V<__int128>(0), V<unsigned __int128>(0))},
                 {"bool1", one(V<uint8_t>(1), V<__int128>(0), V<unsigned __int128>(0))},
                 {"li=1", one(V<uint8_t>(0), V<__int128>(1), V<unsigned __int128>(0))},
                 {"li=-1", one(V<uint8_t>(0), V<__int128>(-1), V<unsigned __int128>(0))},
                 {"li=2^100", one(V<uint8_t>(0), V<__int128>(big), V<unsigned __int128>(0))},
                 {"ip=1", one(V<uint8_t>(0), V<__int128>(0), V<unsigned __int128>(1))},
                 {"ip=2^100",
                  one(V<uint8_t>(0), V<__int128>(0),
                      V<unsigned __int128>(static_cast<unsigned __int128>(1) << 100))}});
    }
}

TEST_F(RestoreDigestTest, EncodingSpecGolden) {
    // locks the algo_version=1 byte layout and the bucket / sum arithmetic
    auto schema = make_schema(DUP_KEYS,
                              {{"k", "INT", true, false, 4}, {"s", "VARCHAR", false, true, 16}});
    RestoreDigest d = digest_rows(schema, {{V<int32_t>(1), S("ab")}, {V<int32_t>(2), N()}});
    ASSERT_EQ(2, d.rows);

    auto hash_of = [](const std::string& bytes) {
        return XXH3_128bits_withSeed(bytes.data(), bytes.size(), 1);
    };
    std::string r1;
    r1 += '\x01';
    r1.append("\x01\x00\x00\x00", 4);
    r1 += '\x01';
    r1.append("\x02\x00\x00\x00", 4);
    r1 += "ab";
    std::string r2;
    r2 += '\x01';
    r2.append("\x02\x00\x00\x00", 4);
    r2 += '\x00';
    std::array<RestoreDigest::Bucket, RestoreDigest::kNumBuckets> expect {};
    for (const std::string& r : {r1, r2}) {
        XXH128_hash_t h = hash_of(r);
        auto& b = expect[h.high64 >> 56];
        b.sum += (static_cast<unsigned __int128>(h.high64) << 64) | h.low64;
        b.count++;
    }
    for (size_t i = 0; i < expect.size(); ++i) {
        EXPECT_TRUE(expect[i].sum == d.buckets[i].sum) << "bucket " << i;
        EXPECT_EQ(expect[i].count, d.buckets[i].count) << "bucket " << i;
    }
    EXPECT_EQ(64, d.root.size());
    EXPECT_EQ(64, d.schema_sig.size());
    std::string json = d.to_json();
    EXPECT_NE(std::string::npos, json.find("\"algo_version\":1"));
    EXPECT_NE(std::string::npos, json.find("\"rows\":2"));
}

TEST_F(RestoreDigestTest, HiddenColumnsAreExcluded) {
    // version / row store columns are neither compared nor allowed to break the digest
    auto schema = make_schema(DUP_KEYS, {{"k", "INT", true, false, 4},
                                         {"v", "INT", false, true, 4},
                                         {VERSION_COL, "BIGINT", false, false, 8},
                                         {"__DORIS_ROW_STORE_COL__", "STRING", false, true, 64}});
    std::vector<Row> a;
    std::vector<Row> b;
    for (int i = 0; i < 100; ++i) {
        a.push_back(
                {V<int32_t>(i), V<int32_t>(i * 3), V<int64_t>(5), S("row" + std::to_string(i))});
        b.push_back({V<int32_t>(i), V<int32_t>(i * 3), V<int64_t>(900 + i), S("other-writer")});
    }
    RestoreDigest da = digest_rows(schema, a);
    RestoreDigest db = digest_rows(schema, b);
    EXPECT_TRUE(same_digest(da, db));
    // a visible column still matters
    b[3][1] = V<int32_t>(-1);
    EXPECT_NE(da.root, digest_rows(schema, b).root);
}

// ===== 4. merge-on-write =====================================================================

class RestoreDigestMowTest : public RestoreDigestTest {
protected:
    static std::vector<ColSpec> mow_cols() {
        return {{"k", "INT", true, false, 4},
                {"v1", "INT", false, true, 4},
                {"v2", "VARCHAR", false, true, 16},
                {SEQUENCE_COL, "INT", false, false, 4},
                {DELETE_SIGN, "TINYINT", false, false, 1}};
    }
    static Row mow_row(int k, int v1, const std::string& v2, int seq, int del) {
        return {V<int32_t>(k), V<int32_t>(v1), S(v2), V<int32_t>(seq), V<int8_t>(del)};
    }
    RestoreDigest mow_digest(const TabletSchemaSPtr& schema, std::vector<RowsetSharedPtr> rowsets,
                             DeleteBitmapPtr bitmap, int64_t version) {
        return digest_of(schema, std::move(rowsets), UNIQUE_KEYS, true, std::move(bitmap), version);
    }
};

TEST_F(RestoreDigestMowTest, SameVisibleRowsSameDigest) {
    auto schema = make_schema(UNIQUE_KEYS, mow_cols(), 3);
    auto bitmap = std::make_shared<DeleteBitmap>(990001);
    auto mark = [&](const RowsetSharedPtr& rs, uint32_t row_id, int64_t version) {
        bitmap->add({rs->rowset_id(), 0, version}, row_id);
    };

    // v2: keys 1..10
    std::vector<Row> r2;
    for (int k = 1; k <= 10; ++k) {
        r2.push_back(mow_row(k, k * 10, "a" + std::to_string(k), 1, 0));
    }
    auto rs2 = write_rowset(schema, r2, 2, 1000, false);
    // v3: update keys 3 and 5
    auto rs3 = write_rowset(schema, {mow_row(3, 31, "b3", 2, 0), mow_row(5, 51, "b5", 2, 0)}, 3,
                            1000, false);
    mark(rs2, 2, 3); // key 3
    mark(rs2, 4, 3); // key 5
    // v4: delete key 7, update key 3 again, insert key 11
    auto rs4 = write_rowset(
            schema,
            {mow_row(3, 32, "c3", 4, 0), mow_row(7, 0, "", 3, 1), mow_row(11, 110, "n11", 3, 0)}, 4,
            1000, false);
    mark(rs2, 6, 4); // key 7
    mark(rs3, 0, 4); // key 3 of v3
    // v5: delete key 9 and bring key 7 back
    auto rs5 = write_rowset(schema, {mow_row(7, 70, "d7", 5, 0), mow_row(9, 0, "", 5, 1)}, 5, 1000,
                            false);
    mark(rs2, 8, 5); // key 9
    mark(rs4, 1, 5); // the delete row of key 7
    // a mark written by a later version must not leak into reads at version 5
    mark(rs2, 0, 6); // key 1

    auto expect_state = [&](std::vector<Row> visible, int64_t version,
                            std::vector<RowsetSharedPtr> rowsets) {
        // reference tablet: only the final visible rows, no bitmap marks, written in two rowsets
        std::vector<Row> first(visible.begin(), visible.begin() + visible.size() / 2);
        std::vector<Row> second(visible.begin() + visible.size() / 2, visible.end());
        auto ref = mow_digest(schema,
                              {write_rowset(schema, first, 2, 3, true),
                               write_rowset(schema, second, 3, 1000, false)},
                              std::make_shared<DeleteBitmap>(990002), 100);
        auto actual = mow_digest(schema, rowsets, bitmap, version);
        EXPECT_TRUE(same_digest(ref, actual)) << "version " << version;
        EXPECT_EQ(visible.size(), actual.rows) << "version " << version;
    };

    // version 5
    expect_state(
            {mow_row(1, 10, "a1", 1, 0), mow_row(2, 20, "a2", 1, 0), mow_row(3, 32, "c3", 4, 0),
             mow_row(4, 40, "a4", 1, 0), mow_row(5, 51, "b5", 2, 0), mow_row(6, 60, "a6", 1, 0),
             mow_row(7, 70, "d7", 5, 0), mow_row(8, 80, "a8", 1, 0), mow_row(10, 100, "a10", 1, 0),
             mow_row(11, 110, "n11", 3, 0)},
            5, {rs2, rs3, rs4, rs5});
    // version 4: key 7 deleted, key 9 still there
    expect_state(
            {mow_row(1, 10, "a1", 1, 0), mow_row(2, 20, "a2", 1, 0), mow_row(3, 32, "c3", 4, 0),
             mow_row(4, 40, "a4", 1, 0), mow_row(5, 51, "b5", 2, 0), mow_row(6, 60, "a6", 1, 0),
             mow_row(8, 80, "a8", 1, 0), mow_row(9, 90, "a9", 1, 0), mow_row(10, 100, "a10", 1, 0),
             mow_row(11, 110, "n11", 3, 0)},
            4, {rs2, rs3, rs4});
    // version 3
    expect_state(
            {mow_row(1, 10, "a1", 1, 0), mow_row(2, 20, "a2", 1, 0), mow_row(3, 31, "b3", 2, 0),
             mow_row(4, 40, "a4", 1, 0), mow_row(5, 51, "b5", 2, 0), mow_row(6, 60, "a6", 1, 0),
             mow_row(7, 70, "a7", 1, 0), mow_row(8, 80, "a8", 1, 0), mow_row(9, 90, "a9", 1, 0),
             mow_row(10, 100, "a10", 1, 0)},
            3, {rs2, rs3});

    // dropping a bitmap mark (a stale row becomes visible again) changes the digest
    auto full = mow_digest(schema, {rs2, rs3, rs4, rs5}, bitmap, 5);
    auto missing_mark = std::make_shared<DeleteBitmap>(990003);
    for (const auto& [k, bm] : bitmap->delete_bitmap) {
        for (uint32_t row : bm) {
            if (std::get<0>(k) == rs2->rowset_id() && row == 2) {
                continue;
            }
            missing_mark->add(k, row);
        }
    }
    EXPECT_NE(full.root, mow_digest(schema, {rs2, rs3, rs4, rs5}, missing_mark, 5).root);

    // the sequence column takes part in the digest
    auto seq_changed =
            mow_digest(schema, {write_rowset(schema, {mow_row(1, 10, "a1", 1, 0)}, 2, 10, false)},
                       std::make_shared<DeleteBitmap>(990004), 5);
    auto seq_other =
            mow_digest(schema, {write_rowset(schema, {mow_row(1, 10, "a1", 2, 0)}, 2, 10, false)},
                       std::make_shared<DeleteBitmap>(990005), 5);
    EXPECT_NE(seq_changed.root, seq_other.root);
}

// ===== 6. delete predicates =================================================================

namespace {
struct Rec {
    int k;
    std::optional<int> v;
    std::string s;
};
} // namespace

class RestoreDigestDeleteTest : public RestoreDigestTest {
protected:
    static std::vector<ColSpec> cols() {
        return {{"k", "INT", true, false, 4},
                {"v", "INT", false, true, 4},
                {"s", "VARCHAR", false, true, 16}};
    }
    static Row to_row(const Rec& r) {
        return {V<int32_t>(r.k), r.v.has_value() ? V<int32_t>(*r.v) : N(), S(r.s)};
    }
    static std::vector<Row> rows_of(const std::vector<Rec>& recs) {
        std::vector<Row> out;
        for (const Rec& r : recs) {
            out.push_back(to_row(r));
        }
        return out;
    }
    static std::vector<Rec> concat(std::vector<Rec> a, const std::vector<Rec>& b) {
        a.insert(a.end(), b.begin(), b.end());
        return a;
    }
};

TEST_F(RestoreDigestDeleteTest, AppliedByVersionAndStableAcrossCompaction) {
    auto schema = make_schema(DUP_KEYS, cols());
    std::vector<Rec> a;
    std::vector<Rec> b;
    std::vector<Rec> c;
    std::vector<Rec> e;
    for (int k = 0; k < 200; ++k) {
        a.push_back({k, k % 7 == 0 ? std::nullopt : std::optional<int>(k % 10), "a" + std::to_string(k)});
    }
    for (int k = 100; k < 260; ++k) { // keys 100..199 exist in `a` as well: duplicates stay
        b.push_back({k, (k * 3) % 10, "b" + std::to_string(k)});
    }
    for (int k = 300; k < 341; ++k) {
        c.push_back({k, 3, "c" + std::to_string(k)});
    }
    for (int k = 400; k < 411; ++k) {
        e.push_back({k, 7, "e" + std::to_string(k)});
    }
    // d4: DELETE WHERE k >= 10 AND v = 3 (version 4), d6: DELETE WHERE v = 7 (version 6)
    auto d4 = [](const Rec& r) { return r.k >= 10 && r.v.has_value() && *r.v == 3; };
    auto d6 = [](const Rec& r) { return r.v.has_value() && *r.v == 7; };
    auto survivors_at = [&](int ver) {
        std::vector<Rec> out;
        auto keep_old = [&](const Rec& r) { return !((ver >= 4 && d4(r)) || (ver >= 6 && d6(r))); };
        for (const Rec& r : a) {
            if (keep_old(r)) {
                out.push_back(r);
            }
        }
        for (const Rec& r : b) {
            if (ver >= 3 && keep_old(r)) {
                out.push_back(r);
            }
        }
        if (ver >= 5) {
            for (const Rec& r : c) {
                if (!(ver >= 6 && d6(r))) {
                    out.push_back(r);
                }
            }
        }
        if (ver >= 7) {
            out.insert(out.end(), e.begin(), e.end());
        }
        return out;
    };

    auto rs2 = write_rowset(schema, rows_of(a), 2, 50, false);
    auto rs3 = write_rowset(schema, rows_of(b), 3, 37, true);
    auto rs4 = write_delete_rowset(schema, 4, {cond("k", ">=", {"10"}), cond("v", "=", {"3"})});
    auto rs5 = write_rowset(schema, rows_of(c), 5, 20, false);
    auto rs6 = write_delete_rowset(schema, 6, {cond("v", "=", {"7"})});
    auto rs7 = write_rowset(schema, rows_of(e), 7, 20, false);
    ASSERT_TRUE(rs4->rowset_meta()->has_delete_predicate());
    ASSERT_EQ(0, rs4->num_rows());

    // every prefix of the version chain sees the conditions up to its version
    const std::vector<RowsetSharedPtr> all = {rs2, rs3, rs4, rs5, rs6, rs7};
    for (int ver = 3; ver <= 7; ++ver) {
        std::vector<RowsetSharedPtr> prefix(all.begin(), all.begin() + (ver - 1));
        RestoreDigest d = digest_of(schema, prefix, DUP_KEYS, false, nullptr, ver);
        auto expect = survivors_at(ver);
        RestoreDigest ref = digest_rows(schema, rows_of(expect));
        EXPECT_TRUE(same_digest(ref, d)) << "version " << ver;
        EXPECT_EQ(expect.size(), d.rows) << "version " << ver;
        EXPECT_EQ(ver >= 6 ? 2U : (ver >= 4 ? 1U : 0U), d.delete_predicates) << "version " << ver;
    }

    RestoreDigest with_pred = digest_of(schema, all, DUP_KEYS, false, nullptr, 7);
    // the same rowsets without the delete conditions give another digest
    RestoreDigest without_pred = digest_of(schema, {rs2, rs3, rs5, rs7}, DUP_KEYS, false, nullptr, 7);
    EXPECT_NE(with_pred.root, without_pred.root);
    EXPECT_GT(without_pred.rows, with_pred.rows);

    // layout: the same rows below the first condition cut into other rowsets and segments
    {
        auto ab = concat(a, b);
        std::mt19937 rng(7);
        std::shuffle(ab.begin(), ab.end(), rng);
        std::vector<Rec> h1(ab.begin(), ab.begin() + 170);
        std::vector<Rec> h2(ab.begin() + 170, ab.end());
        auto x2 = write_rowset(schema, rows_of(h1), 2, 11, true);
        auto x3 = write_rowset(schema, rows_of(h2), 3, 1000, false);
        RestoreDigest d = digest_of(schema, {x2, x3, rs4, rs5, rs6, rs7}, DUP_KEYS, false, nullptr, 7);
        EXPECT_TRUE(same_digest(with_pred, d));
    }

    // compaction physically drops the rows; the digest does not move
    auto tablet = make_meta_tablet(DUP_KEYS, schema);
    const size_t expect_rows = survivors_at(7).size();
    auto m24 = compact(schema, tablet, {rs2, rs3, rs4}, Version(2, 4));
    ASSERT_NE(nullptr, m24);
    EXPECT_EQ(survivors_at(4).size(), m24->num_rows()) << "compaction did not apply the condition";
    EXPECT_TRUE(same_digest(with_pred, digest_of(schema, {m24, rs5, rs6, rs7}, DUP_KEYS, false, nullptr, 7)));
    EXPECT_TRUE(same_digest(digest_rows(schema, rows_of(survivors_at(4))),
                            digest_of(schema, {m24}, DUP_KEYS, false, nullptr, 4)));

    // a cumulative compaction below the conditions keeps them effective for its rows
    auto m23 = compact(schema, tablet, {rs2, rs3}, Version(2, 3),
                       ReaderType::READER_CUMULATIVE_COMPACTION);
    ASSERT_NE(nullptr, m23);
    EXPECT_EQ(a.size() + b.size(), m23->num_rows());
    EXPECT_TRUE(same_digest(with_pred, digest_of(schema, {m23, rs4, rs5, rs6, rs7}, DUP_KEYS, false, nullptr, 7)));

    auto m27 = compact(schema, tablet, {rs2, rs3, rs4, rs5, rs6, rs7}, Version(2, 7));
    ASSERT_NE(nullptr, m27);
    EXPECT_EQ(expect_rows, m27->num_rows());
    RestoreDigest after = digest_of(schema, {m27}, DUP_KEYS, false, nullptr, 7);
    EXPECT_TRUE(same_digest(with_pred, after));
    EXPECT_EQ(0U, after.delete_predicates);
}

TEST_F(RestoreDigestDeleteTest, Operators) {
    auto schema = make_schema(DUP_KEYS, cols());
    std::vector<Rec> rows;
    for (int k = 0; k < 60; ++k) {
        rows.push_back({k, k % 4 == 0 ? std::nullopt : std::optional<int>(k % 6), "a" + std::to_string(k)});
    }
    struct Case {
        std::string name;
        std::vector<TCondition> conds;
        std::function<bool(const Rec&)> deleted;
    };
    auto is = [](const Rec& r, int x) { return r.v.has_value() && *r.v == x; };
    std::vector<Case> cases = {
            {"v = 3", {cond("v", "=", {"3"})}, [&](const Rec& r) { return is(r, 3); }},
            {"v != 3", {cond("v", "!=", {"3"})}, [&](const Rec& r) { return r.v.has_value() && *r.v != 3; }},
            {"v < 2", {cond("v", "<", {"2"})}, [&](const Rec& r) { return r.v.has_value() && *r.v < 2; }},
            {"v >= 4", {cond("v", ">=", {"4"})}, [&](const Rec& r) { return r.v.has_value() && *r.v >= 4; }},
            {"v IS NULL", {cond("v", "IS", {"NULL"})}, [&](const Rec& r) { return !r.v.has_value(); }},
            {"v IS NOT NULL", {cond("v", "IS", {"NOT NULL"})}, [&](const Rec& r) { return r.v.has_value(); }},
            {"v IN (1,2)", {cond("v", "*=", {"1", "2"})}, [&](const Rec& r) { return is(r, 1) || is(r, 2); }},
            {"v NOT IN (1,2)", {cond("v", "!*=", {"1", "2"})},
             [&](const Rec& r) { return r.v.has_value() && !is(r, 1) && !is(r, 2); }},
            {"k >= 10 AND k < 30 AND v = 1",
             {cond("k", ">=", {"10"}), cond("k", "<", {"30"}), cond("v", "=", {"1"})},
             [&](const Rec& r) { return r.k >= 10 && r.k < 30 && is(r, 1); }},
            {"s = a5", {cond("s", "=", {"a5"})}, [&](const Rec& r) { return r.s == "a5"; }},
    };
    std::vector<Rec> first(rows.begin(), rows.begin() + 25);
    std::vector<Rec> second(rows.begin() + 25, rows.end());
    for (const Case& cs : cases) {
        auto rs2 = write_rowset(schema, rows_of(first), 2, 7, true);
        auto rs3 = write_rowset(schema, rows_of(second), 3, 13, false);
        auto del = write_delete_rowset(schema, 4, cs.conds);
        std::vector<Rec> expect;
        for (const Rec& r : rows) {
            if (!cs.deleted(r)) {
                expect.push_back(r);
            }
        }
        RestoreDigest d = digest_of(schema, {rs2, rs3, del}, DUP_KEYS, false, nullptr, 4);
        EXPECT_TRUE(same_digest(digest_rows(schema, rows_of(expect)), d)) << cs.name;
        EXPECT_EQ(expect.size(), d.rows) << cs.name;
        EXPECT_LT(expect.size(), rows.size()) << cs.name << " matched nothing";
    }
}

TEST_F(RestoreDigestDeleteTest, MergeOnWriteWithBitmapAndCondition) {
    std::vector<ColSpec> cols = {{"k", "INT", true, false, 4},
                                 {"v1", "INT", false, true, 4},
                                 {DELETE_SIGN, "TINYINT", false, false, 1}};
    auto schema = make_schema(UNIQUE_KEYS, cols);
    auto row = [](int k, int v, int del = 0) { return Row {V<int32_t>(k), V<int32_t>(v), V<int8_t>(del)}; };
    std::vector<Row> r2;
    for (int k = 1; k <= 10; ++k) {
        r2.push_back(row(k, k * 10));
    }
    auto rs2 = write_rowset(schema, r2, 2, 1000, false);
    auto rs3 = write_rowset(schema, {row(3, 31)}, 3, 1000, false);
    auto rs4 = write_delete_rowset(schema, 4, {cond("v1", "=", {"50"})});
    auto rs5 = write_rowset(schema, {row(5, 50), row(8, 0, 1)}, 5, 1000, false);
    auto bitmap = std::make_shared<DeleteBitmap>(990010);
    bitmap->add({rs2->rowset_id(), 0, 3}, 2); // k=3 replaced at v3
    bitmap->add({rs2->rowset_id(), 0, 5}, 4); // k=5 rewritten at v5 (the old row is also hit by d4)
    bitmap->add({rs2->rowset_id(), 0, 5}, 7); // k=8 deleted at v5

    auto expect_at = [&](int ver) {
        std::vector<Row> rows;
        for (int k = 1; k <= 10; ++k) {
            int v = k * 10;
            if (k == 3 && ver >= 3) {
                v = 31;
            }
            if (k == 5) {
                if (ver >= 5) {
                    v = 50; // the new row of version 5 is not hit by the condition of version 4
                } else if (ver >= 4) {
                    continue; // deleted by v1 = 50
                }
            }
            if (k == 8 && ver >= 5) {
                continue;
            }
            rows.push_back(row(k, v));
        }
        return rows;
    };
    const std::vector<RowsetSharedPtr> all = {rs2, rs3, rs4, rs5};
    for (int ver = 3; ver <= 5; ++ver) {
        std::vector<RowsetSharedPtr> prefix(all.begin(), all.begin() + (ver - 1));
        RestoreDigest d = digest_of(schema, prefix, UNIQUE_KEYS, true, bitmap, ver);
        auto expect = expect_at(ver);
        RestoreDigest ref = digest_of(schema, {write_rowset(schema, expect, 2, 3, true)}, UNIQUE_KEYS,
                                      true, std::make_shared<DeleteBitmap>(990011), 100);
        EXPECT_TRUE(same_digest(ref, d)) << "version " << ver;
        EXPECT_EQ(expect.size(), d.rows) << "version " << ver;
    }
}

// ===== 7. unique merge-on-read ===============================================================

class RestoreDigestMorTest : public RestoreDigestTest {
protected:
    struct MRow {
        int k;
        int v1;
        std::string v2;
        int seq;
        int del;
    };
    static std::vector<ColSpec> mor_cols(bool has_seq) {
        std::vector<ColSpec> c = {{"k", "INT", true, false, 4},
                                  {"v1", "INT", false, true, 4},
                                  {"v2", "VARCHAR", false, true, 16}};
        if (has_seq) {
            c.push_back({SEQUENCE_COL, "INT", false, false, 4});
        }
        c.push_back({DELETE_SIGN, "TINYINT", false, false, 1});
        return c;
    }
    static Row to_row(bool has_seq, const MRow& r) {
        Row out = {V<int32_t>(r.k), V<int32_t>(r.v1), S(r.v2)};
        if (has_seq) {
            out.push_back(V<int32_t>(r.seq));
        }
        out.push_back(V<int8_t>(static_cast<int8_t>(r.del)));
        return out;
    }
    static std::vector<Row> rows_of(bool has_seq, const std::vector<MRow>& rs) {
        std::vector<Row> out;
        for (const auto& r : rs) {
            out.push_back(to_row(has_seq, r));
        }
        return out;
    }
    // the visible rows after the versions `1..n` of `versions` were applied
    static std::vector<MRow> visible(bool has_seq, const std::vector<std::vector<MRow>>& versions,
                                     size_t n) {
        std::map<int, MRow> state;
        for (size_t i = 0; i < n; ++i) {
            for (const MRow& r : versions[i]) {
                auto it = state.find(r.k);
                if (it == state.end() || !has_seq || r.seq >= it->second.seq) {
                    state[r.k] = r;
                }
            }
        }
        std::vector<MRow> out;
        for (const auto& [k, r] : state) {
            if (r.del == 0) {
                out.push_back(r);
            }
        }
        return out;
    }

    void run(bool has_seq) {
        auto schema = make_schema(UNIQUE_KEYS, mor_cols(has_seq), has_seq ? 3 : -1);
        auto tablet = make_meta_tablet(UNIQUE_KEYS, schema);
        std::vector<std::vector<MRow>> versions(5);
        for (int k = 1; k <= 10; ++k) {
            versions[0].push_back({k, k * 10, "a" + std::to_string(k), 1, 0});
        }
        versions[1] = {{3, 31, "b3", 2, 0}, {5, 51, "b5", 2, 0}};
        versions[2] = {{3, 32, "c3", 4, 0}, {7, 0, "", 3, 1}, {11, 110, "n11", 3, 0}};
        versions[3] = {{7, 70, "d7", 5, 0}, {9, 0, "", 5, 1}};
        // with a sequence column this row loses against seq 4, without one it is simply newer
        versions[4] = {{3, 99, "z3", 1, 0}};

        std::vector<RowsetSharedPtr> rowsets;
        for (size_t i = 0; i < versions.size(); ++i) {
            rowsets.push_back(write_rowset(schema, rows_of(has_seq, versions[i]),
                                           static_cast<int64_t>(i) + 2, 4, i % 2 == 0));
        }
        std::vector<RestoreDigest> expected;
        for (size_t n = 1; n <= versions.size(); ++n) {
            auto vis = visible(has_seq, versions, n);
            RestoreDigest ref =
                    mor_digest(schema, {write_rowset(schema, rows_of(has_seq, vis), 2, 1000, false)}, tablet);
            ASSERT_EQ(vis.size(), ref.rows);
            expected.push_back(ref);
            std::vector<RowsetSharedPtr> prefix(rowsets.begin(), rowsets.begin() + n);
            RestoreDigest d = mor_digest(schema, prefix, tablet, static_cast<int64_t>(n) + 1);
            EXPECT_TRUE(same_digest(ref, d)) << "has_seq=" << has_seq << " version " << n + 1;
            EXPECT_EQ(vis.size(), d.rows);
            EXPECT_EQ(1U, d.threads);
        }
        if (has_seq) {
            // key 3 keeps the row with the highest sequence value, v5 changed nothing
            EXPECT_TRUE(same_digest(expected[3], expected[4]));
        } else {
            EXPECT_FALSE(same_digest(expected[3], expected[4]));
        }

        // other layout: every version in one rowset per key range, tiny segments
        {
            std::vector<RowsetSharedPtr> other;
            for (size_t i = 0; i < versions.size(); ++i) {
                other.push_back(write_rowset(schema, rows_of(has_seq, versions[i]),
                                             static_cast<int64_t>(i) + 2, 1, i % 2 == 1));
            }
            EXPECT_TRUE(same_digest(expected[4], mor_digest(schema, other, tablet, 6)));
        }

        // compaction of a prefix, and of everything
        auto c24 = compact(schema, tablet, {rowsets[0], rowsets[1], rowsets[2]}, Version(2, 4));
        ASSERT_NE(nullptr, c24);
        EXPECT_TRUE(same_digest(expected[4],
                                mor_digest(schema, {c24, rowsets[3], rowsets[4]}, tablet, 6)));
        EXPECT_TRUE(same_digest(expected[2], mor_digest(schema, {c24}, tablet, 4)));
        auto c26 = compact(schema, tablet, rowsets, Version(2, 6));
        ASSERT_NE(nullptr, c26);
        EXPECT_TRUE(same_digest(expected[4], mor_digest(schema, {c26}, tablet, 6)));
        auto c46 = compact(schema, tablet, {rowsets[2], rowsets[3], rowsets[4]}, Version(4, 6),
                           ReaderType::READER_CUMULATIVE_COMPACTION);
        ASSERT_NE(nullptr, c46);
        EXPECT_TRUE(same_digest(expected[4], mor_digest(schema, {rowsets[0], rowsets[1], c46}, tablet, 6)));

        // any change shows: a value, a delete sign, a missing update
        auto mutate = [&](size_t version_idx, size_t row_idx, const std::function<void(MRow&)>& f) {
            auto copy = versions;
            f(copy[version_idx][row_idx]);
            std::vector<RowsetSharedPtr> rs;
            for (size_t i = 0; i < copy.size(); ++i) {
                rs.push_back(write_rowset(schema, rows_of(has_seq, copy[i]), static_cast<int64_t>(i) + 2, 4, false));
            }
            return mor_digest(schema, rs, tablet, 6).root;
        };
        std::set<std::string> roots {expected[4].root};
        roots.insert(mutate(0, 0, [](MRow& r) { r.v1 += 1; }));
        roots.insert(mutate(3, 0, [](MRow& r) { r.v2 = "d7x"; }));
        roots.insert(mutate(3, 1, [](MRow& r) { r.del = 0; })); // key 9 stays
        roots.insert(mutate(2, 1, [](MRow& r) { r.del = 0; })); // key 7 deleted -> not deleted: still replaced at v5
        roots.insert(mutate(1, 1, [](MRow& r) { r.v1 = 52; }));
        // the last one only matters when the version / sequence order decides
        EXPECT_GE(roots.size(), 5U);
    }
};

TEST_F(RestoreDigestMorTest, MultipleUpdatesEqualFinalRowsWithSequenceColumn) {
    run(true);
}

TEST_F(RestoreDigestMorTest, MultipleUpdatesEqualFinalRowsWithoutSequenceColumn) {
    run(false);
}

// ===== 8. types added in P2 ==================================================================

class RestoreDigestTypesTest : public RestoreDigestTest {
protected:
    static std::vector<ColSpec> type_cols() {
        return {{"k", "INT", true, false, 4},
                with_children({"c_arr", "ARRAY", false, true, 24}, {{"item", "INT", false, true, 4}}),
                with_children({"c_arr_s", "ARRAY", false, true, 24},
                              {{"item", "VARCHAR", false, true, 16}}),
                with_children({"c_arr2", "ARRAY", false, true, 24},
                              {with_children({"item", "ARRAY", false, true, 24},
                                             {{"item", "INT", false, true, 4}})}),
                with_children({"c_map", "MAP", false, true, 24},
                              {{"key", "VARCHAR", false, true, 16}, {"value", "INT", false, true, 4}}),
                with_children({"c_st", "STRUCT", false, true, 24},
                              {{"a", "INT", false, true, 4}, {"b", "VARCHAR", false, true, 16}}),
                {"c_json", "JSONB", false, true, 64},
                {"c_dec2", "DECIMAL", false, true, 16, 27, 9},
                {"c_date1", "DATE", false, true, 3},
                {"c_dt1", "DATETIME", false, true, 8}};
    }
    static Field opt(bool null, int v) { return null ? FNull() : FI(v); }

    // Deterministic row; every nullable column, and the elements, are sometimes NULL.
    static Row typed_row(int i) {
        auto nul = [&](int c) { return (i + c) % (c + 4) == 0; };
        Row r;
        r.push_back(V<int32_t>(i));
        {
            Array a;
            for (int j = 0; j < i % 4; ++j) {
                a.push_back(opt((i + j) % 5 == 0, i * 10 + j));
            }
            r.push_back(nul(1) ? N() : F(FArr(a)));
        }
        {
            Array a;
            for (int j = 0; j < i % 3; ++j) {
                a.push_back((i + j) % 4 == 0 ? FNull() : FS(std::string(static_cast<size_t>(j), 'q') + std::to_string(i)));
            }
            r.push_back(nul(2) ? N() : F(FArr(a)));
        }
        {
            Array outer;
            for (int j = 0; j < i % 3; ++j) {
                Array inner;
                for (int m = 0; m < (i + j) % 3; ++m) {
                    inner.push_back(opt(m == 1 && i % 2 == 0, j * 100 + m));
                }
                outer.push_back(FArr(inner));
            }
            r.push_back(nul(3) ? N() : F(FArr(outer)));
        }
        {
            Array keys;
            Array values;
            for (int j = 0; j < i % 3; ++j) {
                keys.push_back(FS("k" + std::to_string(j)));
                values.push_back(opt((i + j) % 4 == 0, i + j));
            }
            r.push_back(nul(4) ? N() : F(FMap(keys, values)));
        }
        {
            Struct st;
            st.push_back(opt(i % 3 == 0, i));
            st.push_back(i % 5 == 0 ? FNull() : FS("s" + std::to_string(i % 11)));
            r.push_back(nul(5) ? N() : F(FStruct(st)));
        }
        r.push_back(nul(6) ? N() : S(jsonb("{\"id\":" + std::to_string(i) + "}")));
        r.push_back(nul(7) ? N() : V<__int128>(static_cast<__int128>(i) * 1000000007LL));
        r.push_back(nul(8) ? N() : D1(2000 + i % 30, 1 + i % 12, 1 + i % 28));
        r.push_back(nul(9) ? N() : D1(2000 + i % 30, 1 + i % 12, 1 + i % 28, i % 24, i % 60, (i * 7) % 60, true));
        return r;
    }
};

TEST_F(RestoreDigestTypesTest, LayoutIndependenceAndCompaction) {
    auto schema = make_schema(DUP_KEYS, type_cols());
    std::vector<Row> rows;
    for (int i = 0; i < 300; ++i) {
        rows.push_back(typed_row(i));
    }
    for (int i = 0; i < 12; ++i) { // exact duplicates
        rows.push_back(typed_row(i * 5));
    }
    RestoreDigest base = digest_of(schema, {write_rowset(schema, rows, 2, 100000, false)});
    ASSERT_EQ(rows.size(), base.rows);

    std::mt19937 rng(11);
    auto shuffled = rows;
    std::shuffle(shuffled.begin(), shuffled.end(), rng);
    size_t half = shuffled.size() / 2;
    std::vector<Row> p1(shuffled.begin(), shuffled.begin() + half);
    std::vector<Row> p2(shuffled.begin() + half, shuffled.end());
    auto x2 = write_rowset(schema, p1, 2, 33, true);
    auto x3 = write_rowset(schema, p2, 3, 41, false);
    EXPECT_TRUE(same_digest(base, digest_of(schema, {x2, x3})));
    EXPECT_TRUE(same_digest(base, digest_of(schema, {write_rowset(schema, rows, 2, 7, true)})));

    // compaction (vertical merge) of the two rowsets
    auto tablet = make_meta_tablet(DUP_KEYS, schema);
    auto merged = compact(schema, tablet, {x2, x3}, Version(2, 3));
    ASSERT_NE(nullptr, merged);
    ASSERT_EQ(rows.size(), merged->num_rows());
    EXPECT_TRUE(same_digest(base, digest_of(schema, {merged})));
}

TEST_F(RestoreDigestTypesTest, AnyChangeChangesDigest) {
    auto schema = make_schema(DUP_KEYS, type_cols());
    auto arr = [](std::vector<Field> v) {
        Array a;
        for (auto& f : v) {
            a.push_back(f);
        }
        return F(FArr(a));
    };
    auto arr_s = arr;
    auto st = [](Field a, Field b) {
        Struct s;
        s.push_back(std::move(a));
        s.push_back(std::move(b));
        return F(FStruct(s));
    };
    auto mp = [](std::vector<std::pair<std::string, std::optional<int>>> kv) {
        Array keys;
        Array values;
        for (auto& [k, v] : kv) {
            keys.push_back(FS(k));
            values.push_back(v.has_value() ? FI(*v) : FNull());
        }
        return F(FMap(keys, values));
    };
    auto nested = [](std::vector<std::vector<std::optional<int>>> v) {
        Array outer;
        for (auto& in : v) {
            Array inner;
            for (auto& e : in) {
                inner.push_back(e.has_value() ? FI(*e) : FNull());
            }
            outer.push_back(FArr(inner));
        }
        return F(FArr(outer));
    };
    // base: col index -> value
    auto base_row = [&]() {
        Row r;
        r.push_back(V<int32_t>(1));
        r.push_back(arr({FI(1), FNull(), FI(3)}));
        r.push_back(arr_s({FS("a"), FS("bc")}));
        r.push_back(nested({{1}, {2, 3}}));
        r.push_back(mp({{"x", 1}, {"y", std::nullopt}}));
        r.push_back(st(FI(5), FS("s")));
        r.push_back(S(jsonb("{\"a\":1}")));
        r.push_back(V<__int128>(static_cast<__int128>(12345)));
        r.push_back(D1(2024, 3, 15));
        r.push_back(D1(2024, 3, 15, 10, 20, 30, true));
        return r;
    };
    std::vector<std::pair<std::string, Row>> cases;
    cases.push_back({"base", base_row()});
    auto add = [&](const std::string& name, size_t col, Cell c) {
        Row r = base_row();
        r[col] = std::move(c);
        cases.push_back({name, std::move(r)});
    };
    add("arr [1,NULL,4]", 1, arr({FI(1), FNull(), FI(4)}));
    add("arr [1,3]", 1, arr({FI(1), FI(3)}));
    add("arr [1,NULL,3,3]", 1, arr({FI(1), FNull(), FI(3), FI(3)}));
    add("arr [NULL,1,3]", 1, arr({FNull(), FI(1), FI(3)}));
    add("arr []", 1, arr({}));
    add("arr NULL", 1, N());
    add("arr [0]", 1, arr({FI(0)}));
    add("arr [NULL]", 1, arr({FNull()}));
    add("arr_s [ab,c]", 2, arr_s({FS("ab"), FS("c")}));
    add("arr_s [a,bc,'']", 2, arr_s({FS("a"), FS("bc"), FS("")}));
    add("arr_s [a,NULL]", 2, arr_s({FS("a"), FNull()}));
    add("arr_s ['']", 2, arr_s({FS("")}));
    add("arr_s []", 2, arr_s({}));
    add("arr2 [[1,2],[3]]", 3, nested({{1, 2}, {3}}));
    add("arr2 [[1],[2],[3]]", 3, nested({{1}, {2}, {3}}));
    add("arr2 [[1],[2,3],[]]", 3, nested({{1}, {2, 3}, {}}));
    add("arr2 [[1],[2,NULL]]", 3, nested({{1}, {2, std::nullopt}}));
    add("arr2 [[1,2,3]]", 3, nested({{1, 2, 3}}));
    add("arr2 []", 3, nested({}));
    add("map x:1,y:2", 4, mp({{"x", 1}, {"y", 2}}));
    add("map y:NULL,x:1 (order)", 4, mp({{"y", std::nullopt}, {"x", 1}}));
    add("map x:1", 4, mp({{"x", 1}}));
    add("map +z:0", 4, mp({{"x", 1}, {"y", std::nullopt}, {"z", 0}}));
    add("map x:NULL,y:1", 4, mp({{"x", std::nullopt}, {"y", 1}}));
    add("map xy:1", 4, mp({{"xy", 1}}));
    add("map {}", 4, mp({}));
    add("map NULL", 4, N());
    add("st (5,t)", 5, st(FI(5), FS("t")));
    add("st (6,s)", 5, st(FI(6), FS("s")));
    add("st (NULL,s)", 5, st(FNull(), FS("s")));
    add("st (5,NULL)", 5, st(FI(5), FNull()));
    add("st (NULL,NULL)", 5, st(FNull(), FNull()));
    add("st NULL", 5, N());
    add("json byte", 6, S(jsonb("{\"a\":2}")));
    add("json longer", 6, S(jsonb("{\"a\":1,\"b\":[1,2]}")));
    add("json empty obj", 6, S(jsonb("{}")));
    add("json NULL", 6, N());
    add("dec +1", 7, V<__int128>(static_cast<__int128>(12346)));
    add("dec NULL", 7, N());
    add("date +1 day", 8, D1(2024, 3, 16));
    add("date other month", 8, D1(2024, 4, 15));
    add("date NULL", 8, N());
    add("dt +1 sec", 9, D1(2024, 3, 15, 10, 20, 31, true));
    add("dt other hour", 9, D1(2024, 3, 15, 11, 20, 30, true));
    add("dt NULL", 9, N());

    std::vector<std::pair<std::string, std::string>> roots;
    for (const auto& [name, row] : cases) {
        roots.push_back({name, digest_rows(schema, {row}).root});
    }
    expect_pairwise_different(roots);
}

TEST_F(RestoreDigestTypesTest, EncodingSpecGolden) {
    // locks the byte layout of the types added after the first cut
    auto schema = make_schema(
            DUP_KEYS,
            {{"k", "INT", true, false, 4},
             with_children({"a", "ARRAY", false, true, 24}, {{"item", "INT", false, true, 4}}),
             with_children({"m", "MAP", false, true, 24},
                           {{"key", "VARCHAR", false, true, 16}, {"value", "INT", false, true, 4}}),
             with_children({"st", "STRUCT", false, true, 24},
                           {{"x", "INT", false, true, 4}, {"y", "VARCHAR", false, true, 16}}),
             {"d", "DATE", false, true, 3},
             {"dt", "DATETIME", false, true, 8},
             {"dec", "DECIMAL", false, true, 16, 27, 9},
             {"j", "JSONB", false, true, 64}});
    Array elems = {FI(1), FNull()};
    Struct fields;
    fields.push_back(FI(5));
    fields.push_back(FNull());
    const std::string json = jsonb("{\"q\":[1,null]}");
    Row row = {V<int32_t>(1),
               F(FArr(elems)),
               F(FMap({FS("x")}, {FI(7)})),
               F(FStruct(fields)),
               D1(2024, 3, 15),
               D1(2024, 3, 15, 10, 20, 30, true),
               V<__int128>(static_cast<__int128>(123456789012345678LL)),
               S(json)};
    RestoreDigest d = digest_rows(schema, {row});
    ASSERT_EQ(1, d.rows);

    std::string e;
    auto u32 = [&](uint32_t v) { e.append(reinterpret_cast<const char*>(&v), 4); };
    auto u64 = [&](uint64_t v) { e.append(reinterpret_cast<const char*>(&v), 8); };
    auto u16 = [&](uint16_t v) { e.append(reinterpret_cast<const char*>(&v), 2); };
    e += '\x01';
    u32(1); // k
    e += '\x01';
    u64(2); // a = [1, NULL]
    e += '\x01';
    u32(1);
    e += '\x00';
    e += '\x01';
    u64(1); // m = {"x": 7}
    e += '\x01';
    u32(1);
    e += 'x';
    e += '\x01';
    u32(7);
    e += '\x01'; // st = (5, NULL), no count
    e += '\x01';
    u32(5);
    e += '\x00';
    e += '\x01'; // d = 2024-03-15
    u16(2024);
    e += '\x03';
    e += '\x0f';
    e += '\x01'; // dt = 2024-03-15 10:20:30
    u16(2024);
    e += '\x03';
    e += '\x0f';
    e += '\x0a';
    e += '\x14';
    e += '\x1e';
    e += '\x01'; // dec as int128
    {
        __int128 v = 123456789012345678LL;
        e.append(reinterpret_cast<const char*>(&v), 16);
    }
    e += '\x01'; // j
    u32(static_cast<uint32_t>(json.size()));
    e += json;

    XXH128_hash_t h = XXH3_128bits_withSeed(e.data(), e.size(), 1);
    auto& expect_bucket = d.buckets[h.high64 >> 56];
    EXPECT_EQ(1, expect_bucket.count);
    EXPECT_TRUE(((static_cast<unsigned __int128>(h.high64) << 64) | h.low64) == expect_bucket.sum);
    EXPECT_EQ(e.size(), d.encoded_bytes);
}

// ===== 9. light schema change ================================================================

TEST_F(RestoreDigestTest, ColumnAddedByLightSchemaChangeReadsItsDefault) {
    // rowset 2 was written before the column `c` / `d` were added, rowset 3 after
    auto old_schema = make_schema(DUP_KEYS, {{"k", "INT", true, false, 4}, {"v", "INT", false, true, 4}});
    ColSpec c_col("c", "INT", false, true, 4);
    c_col.default_value = "7";
    auto new_schema = make_schema(DUP_KEYS,
                                  {{"k", "INT", true, false, 4},
                                   {"v", "INT", false, true, 4},
                                   c_col,
                                   {"d", "VARCHAR", false, true, 16}},
                                  -1, {}, 1);
    std::vector<Row> old_rows;
    std::vector<Row> old_rows_materialized;
    for (int i = 0; i < 50; ++i) {
        old_rows.push_back({V<int32_t>(i), V<int32_t>(i * 2)});
        old_rows_materialized.push_back({V<int32_t>(i), V<int32_t>(i * 2), V<int32_t>(7), N()});
    }
    std::vector<Row> new_rows;
    for (int i = 100; i < 130; ++i) {
        new_rows.push_back({V<int32_t>(i), V<int32_t>(i), V<int32_t>(i + 1), S("n")});
    }
    auto rs_old = write_rowset(old_schema, old_rows, 2, 20, false);
    auto rs_new = write_rowset(new_schema, new_rows, 3, 20, false);
    RestoreDigest lazy = digest_of(new_schema, {rs_old, rs_new});

    // the same data after a compaction (or a reload) has the default materialized
    auto all = old_rows_materialized;
    all.insert(all.end(), new_rows.begin(), new_rows.end());
    RestoreDigest materialized = digest_of(new_schema, {write_rowset(new_schema, all, 2, 1000, false)});
    EXPECT_TRUE(same_digest(lazy, materialized));
    EXPECT_EQ(80, lazy.rows);

    // a different default is a different tablet content
    ColSpec c_col8("c", "INT", false, true, 4);
    c_col8.default_value = "8";
    auto new_schema8 = make_schema(DUP_KEYS,
                                   {{"k", "INT", true, false, 4},
                                    {"v", "INT", false, true, 4},
                                    c_col8,
                                    {"d", "VARCHAR", false, true, 16}},
                                   -1, {}, 1);
    RestoreDigest other = digest_of(new_schema8, {rs_old, write_rowset(new_schema8, new_rows, 3, 20, false)});
    EXPECT_NE(lazy.root, other.root);
}

// ===== 10. parallel read =====================================================================

TEST_F(RestoreDigestTest, ThreadsDoNotChangeTheDigest) {
    auto schema = make_schema(DUP_KEYS, wide_cols());
    std::vector<Row> rows;
    for (int i = 0; i < 1500; ++i) {
        rows.push_back(wide_row(i));
    }
    std::vector<Row> p1(rows.begin(), rows.begin() + 900);
    std::vector<Row> p2(rows.begin() + 900, rows.end());
    auto rs2 = write_rowset(schema, p1, 2, 37, false); // ~25 segments
    auto rs3 = write_rowset(schema, p2, 3, 50, true);  // 12 overlapping segments
    auto rs4 = write_rowset(schema, {wide_row(9000)}, 4, 10, false);
    auto del = write_delete_rowset(schema, 5, {cond("k", ">=", {"600"}), cond("k", "<", {"900"})});
    auto rs6 = write_rowset(schema, {wide_row(600)}, 6, 10, false); // newer than the condition
    std::vector<RowsetSharedPtr> rowsets = {rs2, rs3, rs4, del, rs6};
    RestoreDigest one = threaded_digest(schema, rowsets, DUP_KEYS, false, nullptr, 6, 1);
    EXPECT_EQ(1U, one.threads);
    EXPECT_EQ(1500U + 2 - 300, one.rows);
    for (int threads : {2, 3, 8, 64}) {
        RestoreDigest d = threaded_digest(schema, rowsets, DUP_KEYS, false, nullptr, 6, threads);
        EXPECT_TRUE(same_digest(one, d)) << "threads " << threads;
        EXPECT_EQ(one.rows_scanned, d.rows_scanned);
        EXPECT_EQ(one.encoded_bytes, d.encoded_bytes);
        EXPECT_GT(d.threads, 1U);
    }
}

TEST_F(RestoreDigestMowTest, ThreadsDoNotChangeTheDigest) {
    auto schema = make_schema(UNIQUE_KEYS, mow_cols(), 3);
    auto bitmap = std::make_shared<DeleteBitmap>(990020);
    std::vector<Row> r2;
    for (int k = 1; k <= 2000; ++k) {
        r2.push_back(mow_row(k, k, "v" + std::to_string(k), 1, 0));
    }
    auto rs2 = write_rowset(schema, r2, 2, 100, false); // 20 segments, key k is at (k-1)/100, (k-1)%100
    std::vector<int> updated = {5, 150, 777, 1999, 2000};
    std::vector<Row> r3;
    std::vector<Row> final_rows = r2;
    for (int k : updated) {
        bitmap->add({rs2->rowset_id(), static_cast<uint32_t>((k - 1) / 100), 3},
                    static_cast<uint32_t>((k - 1) % 100));
        r3.push_back(mow_row(k, -k, "upd", 2, 0));
        final_rows[static_cast<size_t>(k - 1)] = mow_row(k, -k, "upd", 2, 0);
    }
    auto rs3 = write_rowset(schema, r3, 3, 2, false);
    // a deleted key
    bitmap->add({rs2->rowset_id(), 3, 4}, 0); // key 301
    auto rs4 = write_rowset(schema, {mow_row(301, 0, "", 3, 1)}, 4, 10, false);
    final_rows.erase(final_rows.begin() + 300);

    RestoreDigest ref = mow_digest(schema, {write_rowset(schema, final_rows, 2, 333, true)},
                                   std::make_shared<DeleteBitmap>(990021), 100);
    for (int threads : {1, 2, 5, 16}) {
        RestoreDigest d = threaded_digest(schema, {rs2, rs3, rs4}, UNIQUE_KEYS, true, bitmap, 4, threads);
        EXPECT_TRUE(same_digest(ref, d)) << "threads " << threads;
        EXPECT_EQ(final_rows.size(), d.rows);
    }
}

// ===== 5. not supported ======================================================================

TEST_F(RestoreDigestTest, UnsupportedModelsAndTypes) {
    auto expect_unsupported = [&](const TabletSchemaSPtr& schema, KeysType kt, bool mow,
                                  const std::string& what) {
        Status st = check_restore_digest_supported(*schema, kt, mow);
        EXPECT_TRUE(st.is<NOT_IMPLEMENTED_ERROR>()) << what << ": " << st;
        // and the compute entry never produces a digest
        RestoreDigestInput in;
        in.schema = schema;
        in.keys_type = kt;
        in.enable_mow = mow;
        in.delete_bitmap = std::make_shared<DeleteBitmap>(1);
        RestoreDigest d;
        EXPECT_TRUE(compute_restore_digest(in, &d).is<NOT_IMPLEMENTED_ERROR>()) << what;
    };
    std::vector<ColSpec> base = {{"k", "INT", true, false, 4}, {"v", "INT", false, true, 4}};

    // supported baselines
    EXPECT_TRUE(check_restore_digest_supported(*make_schema(DUP_KEYS, wide_cols()), DUP_KEYS, false)
                        .ok());
    EXPECT_TRUE(check_restore_digest_supported(
                        *make_schema(UNIQUE_KEYS, {{"k", "INT", true, false, 4},
                                                   {DELETE_SIGN, "TINYINT", false, false, 1}}),
                        UNIQUE_KEYS, true)
                        .ok());
    // merge on read is supported since P2
    EXPECT_TRUE(check_restore_digest_supported(
                        *make_schema(UNIQUE_KEYS, {{"k", "INT", true, false, 4},
                                                   {DELETE_SIGN, "TINYINT", false, false, 1}}),
                        UNIQUE_KEYS, false)
                        .ok());

    // models
    {
        auto cols = base;
        cols[1].aggregation = "SUM";
        expect_unsupported(make_schema(AGG_KEYS, cols), AGG_KEYS, false, "aggregate");
    }
    auto uniq_cols = base;
    uniq_cols.push_back({DELETE_SIGN, "TINYINT", false, false, 1});
    expect_unsupported(make_schema(UNIQUE_KEYS, base), UNIQUE_KEYS, true, "no delete sign");
    expect_unsupported(make_schema(UNIQUE_KEYS, base), UNIQUE_KEYS, false, "mor no delete sign");
    expect_unsupported(make_schema(UNIQUE_KEYS, uniq_cols, -1, {1}), UNIQUE_KEYS, true,
                       "cluster key");

    // types
    for (const char* type : {"HLL", "BITMAP", "QUANTILE_STATE", "VARIANT"}) {
        auto cols = base;
        cols.push_back({"x", type, false, true, 16});
        expect_unsupported(make_schema(DUP_KEYS, cols), DUP_KEYS, false, type);
    }
    // a nested element of an unsupported type
    {
        auto cols = base;
        cols.push_back(with_children({"m", "MAP", false, true, 24},
                                     {{"key", "VARCHAR", false, true, 16},
                                      {"value", "HLL", false, true, 16}}));
        expect_unsupported(make_schema(DUP_KEYS, cols), DUP_KEYS, false, "map<varchar,hll>");
    }
    // unknown hidden column, row binlog column
    {
        auto cols = base;
        cols.push_back({"__DORIS_SOMETHING_NEW__", "BIGINT", false, true, 8});
        expect_unsupported(make_schema(DUP_KEYS, cols), DUP_KEYS, false, "unknown hidden column");
    }
    {
        auto cols = base;
        cols.push_back({"__DORIS_BINLOG_OP__", "TINYINT", false, true, 1});
        expect_unsupported(make_schema(DUP_KEYS, cols), DUP_KEYS, false, "row binlog");
    }
}


// ===== 11. tablet level entry =================================================================

namespace {
struct TabRow {
    int32_t k;
    int32_t v;
    int8_t del = 0;
};

bool digests_equal(const RestoreDigest& a, const RestoreDigest& b) {
    if (a.root != b.root || a.rows != b.rows) {
        return false;
    }
    for (size_t i = 0; i < RestoreDigest::kNumBuckets; ++i) {
        if (a.buckets[i].sum != b.buckets[i].sum || a.buckets[i].count != b.buckets[i].count) {
            return false;
        }
    }
    return true;
}
} // namespace

// Real tablets created through the engine, rowsets added to them, and the tablet level entry
// (capture under the header lock, delete bitmap snapshot, schema choice) driven.
class RestoreDigestTabletTest : public MowTransformTestBase {
protected:
    // keys: DUP, UNIQUE (merge on write when `mow`, otherwise merge on read)
    TabletSharedPtr create_tablet(int64_t tablet_id, bool mow, bool unique = false) {
        unique = unique || mow;
        auto request = testutil::create_tablet_request(
                tablet_id, /*schema_hash=*/1, /*partition_id=*/10, /*short_key_column_count=*/1,
                unique ? TKeysType::UNIQUE_KEYS : TKeysType::DUP_KEYS,
                {{"k", TPrimitiveType::INT, true},
                 {"v", TPrimitiveType::INT, false, false, TAggregationType::NONE, true}});
        if (unique) {
            TColumn del;
            del.column_name = DELETE_SIGN;
            del.__set_is_key(false);
            del.__set_is_allow_null(false);
            del.column_type.type = TPrimitiveType::TINYINT;
            del.__set_default_value("0");
            request.tablet_schema.columns.push_back(del);
            request.tablet_schema.__set_delete_sign_idx(2);
            request.__set_enable_unique_key_merge_on_write(mow);
        }
        RuntimeProfile profile("restore_digest_ut");
        Status st = _engine->create_tablet(request, &profile);
        EXPECT_TRUE(st.ok()) << st;
        return _engine->tablet_manager()->get_tablet(tablet_id);
    }

    // Writes one rowset, one segment per entry of `segments`; every segment sorted by key.
    RowsetSharedPtr write(const TabletSharedPtr& tablet, const TabletSchemaSPtr& schema,
                          int64_t version, const std::vector<std::vector<TabRow>>& segments,
                          bool overlapping, bool add_to_tablet) {
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
        ctx.segments_overlap = overlapping ? OVERLAPPING : NONOVERLAPPING;
        ctx.max_rows_per_segment = UINT32_MAX;
        ctx.enable_segcompaction = false;
        auto res = RowsetFactory::create_rowset_writer(*_engine, ctx, false);
        EXPECT_TRUE(res.has_value()) << res.error();
        auto writer = std::move(res).value();
        for (const auto& seg : segments) {
            Block block = schema->create_storage_block();
            auto cols = std::move(block).mutate_columns();
            for (const TabRow& r : seg) {
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
        auto rowset = write(tablet, schema, version, {}, false, false);
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

    // digest of rows written as plain rowsets of a throw away layout, same schema and model
    RestoreDigest reference(const TabletSharedPtr& tablet, const TabletSchemaSPtr& schema,
                            const std::vector<TabRow>& rows) {
        RestoreDigestInput in;
        in.schema = schema;
        in.keys_type = tablet->keys_type();
        in.enable_mow = tablet->enable_unique_key_merge_on_write();
        if (in.enable_mow) {
            in.delete_bitmap = std::make_shared<DeleteBitmap>(tablet->tablet_id());
        }
        in.version = 100;
        in.tablet = tablet;
        std::vector<TabRow> sorted = rows;
        std::sort(sorted.begin(), sorted.end(), [](const TabRow& a, const TabRow& b) { return a.k < b.k; });
        in.rowsets.push_back(write(tablet, schema, 2, {sorted}, false, false));
        RestoreDigest d;
        Status st = compute_restore_digest(in, &d);
        EXPECT_TRUE(st.ok()) << st;
        return d;
    }

    RestoreDigest tablet_digest(int64_t tablet_id, int64_t version, int threads = 1) {
        RestoreDigest d;
        Status st = compute_tablet_restore_digest(*_engine, tablet_id, version, &d, threads);
        EXPECT_TRUE(st.ok()) << st;
        return d;
    }

    // Merges the rowsets of versions lo..hi into one rowset of the same rows (what a compaction
    // does) and adds it to the tablet, which retires the inputs.
    RowsetSharedPtr compact(const TabletSharedPtr& tablet, const TabletSchemaSPtr& schema,
                            int64_t lo, int64_t hi) {
    std::vector<RowsetSharedPtr> inputs;
    {
        std::shared_lock lock(tablet->get_header_lock());
        auto ret = tablet->capture_consistent_rowsets_unlocked(Version(0, hi), CaptureRowsetOps {});
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
    EXPECT_TRUE(Merger::vertical_merge_rowsets(tablet, ReaderType::READER_BASE_COMPACTION, *schema,
                                               readers, writer.get(), 100, 2, &stats)
                        .ok());
    RowsetSharedPtr merged;
    EXPECT_TRUE(writer->build(merged).ok());
    EXPECT_TRUE(tablet->add_rowset(merged).ok());

    return merged;
    }

    // Replaces the rowset of the single version `version` by a new one with other rows (what a
    // restore overwriting the tablet or a clone of the same version does), under the header lock.
    void replace_rowset(const TabletSharedPtr& tablet, const TabletSchemaSPtr& schema,
                        int64_t version, const std::vector<TabRow>& rows) {
        RowsetSharedPtr fresh = write(tablet, schema, version, {rows}, false, false);
        std::unique_lock lock(tablet->get_header_lock());
        RowsetSharedPtr old = tablet->get_rowset_by_version({version, version});
        EXPECT_NE(nullptr, old);
        std::vector<RowsetSharedPtr> to_add {fresh};
        std::vector<RowsetSharedPtr> to_delete {old};
        EXPECT_TRUE(tablet->modify_rowsets(to_add, to_delete).ok());
    }

    int64_t _next_id = 880000;
};

namespace {
struct ConfigRestore {
    int64_t decomposed_mb = config::restore_digest_cache_decomposed_mb;
    int32_t max_scans = config::restore_digest_prefix_max_scans;
    int32_t boundaries = config::restore_digest_mor_prefix_boundaries;
    ~ConfigRestore() {
        config::restore_digest_cache_decomposed_mb = decomposed_mb;
        config::restore_digest_prefix_max_scans = max_scans;
        config::restore_digest_mor_prefix_boundaries = boundaries;
    }
};

struct CachedDigest {
    Status status = Status::InternalError("not set");
    Status decomposed_status = Status::InternalError("not set");
    LogicalDigestResult result;
    std::shared_ptr<const RestoreDigestDecomposed> decomposed;
    bool decomposed_from_cache = false;
    uint64_t scans = 0; // scans this call started
};

CachedDigest get_cached(StorageEngine& engine, RestoreDigestCache* cache, int64_t tablet_id,
                        int64_t version, bool want_decomposed = true) {
    CachedDigest out;
    const uint64_t before = restore_digest_scan_count();
    out.status = get_tablet_logical_digest(
            engine, tablet_id, version, 2, cache, &out.result,
            want_decomposed ? &out.decomposed : nullptr,
            want_decomposed ? &out.decomposed_status : nullptr,
            want_decomposed ? &out.decomposed_from_cache : nullptr);
    out.scans = restore_digest_scan_count() - before;
    return out;
}

RestoreDigestCache::Key cache_key(int64_t tablet_id, int64_t version, const std::string& sig) {
    RestoreDigestCache::Key key;
    key.tablet_id = tablet_id;
    key.version = version;
    key.algo_version = RestoreDigest::kAlgoVersion;
    key.schema_sig = sig;
    return key;
}

// The key get_tablet_logical_digest uses for the tablet as it is now.
RestoreDigestCache::Key tablet_cache_key(StorageEngine& engine, int64_t tablet_id, int64_t version,
                                         const std::string& sig) {
    RestoreDigestCache::Key key = cache_key(tablet_id, version, sig);
    TabletSharedPtr tablet = engine.tablet_manager()->get_tablet(tablet_id);
    EXPECT_NE(nullptr, tablet);
    key.tablet_uid = tablet->tablet_uid();
    std::shared_lock lock(tablet->get_header_lock());
    auto ret = tablet->capture_consistent_rowsets_unlocked(Version(0, version), CaptureRowsetOps {});
    EXPECT_TRUE(ret.has_value());
    key.rowsets_sig = restore_digest_rowsets_sig(ret->rowsets);
    if (tablet->enable_unique_key_merge_on_write()) {
        key.bitmap_keys = static_cast<int64_t>(
                tablet->tablet_meta()->delete_bitmap().snapshot(version).delete_bitmap.size());
    }
    return key;
}

// A decomposed digest of about `parts` * 8 KB in memory, which is not a real digest.
std::shared_ptr<const RestoreDigestDecomposed> fake_decomposed(int parts) {
    auto d = std::make_shared<RestoreDigestDecomposed>();
    d->rowsets.resize(static_cast<size_t>(parts));
    return d;
}
} // namespace

TEST_F(RestoreDigestTabletTest, MowPartialUpdateConflictRewriteNeedsTheBitmap) {
    auto tablet = create_tablet(7001, true);
    ASSERT_NE(nullptr, tablet);
    auto schema = tablet->tablet_schema();
    ASSERT_TRUE(tablet->enable_unique_key_merge_on_write());

    auto rs2 = write(tablet, schema, 2, {{{1, 10}, {2, 20}, {3, 30}, {4, 40}, {5, 50}, {6, 60}}}, false, true);
    // version 3: a partial column update which hit a concurrent change at publish time. Key 3 was
    // first written (segment 0) from the old row and then rewritten (segment 1): the same key
    // twice in one rowset, only the delete bitmap says which row is visible.
    auto rs3 = write(tablet, schema, 3, {{{3, 31}, {5, 51}}, {{3, 32}}}, true, true);
    auto& bitmap = tablet->tablet_meta()->delete_bitmap();
    bitmap.add({rs2->rowset_id(), 0, 3}, 2); // key 3 of version 2
    bitmap.add({rs2->rowset_id(), 0, 3}, 4); // key 5 of version 2
    bitmap.add({rs3->rowset_id(), 0, 3}, 0); // key 3, the first write of version 3
    // version 4: key 6 updated, key 1 deleted (delete sign)
    auto rs4 = write(tablet, schema, 4, {{{1, 0, 1}, {6, 61}}}, false, true);
    bitmap.add({rs2->rowset_id(), 0, 4}, 0);
    bitmap.add({rs2->rowset_id(), 0, 4}, 5);

    std::vector<TabRow> at3 = {{1, 10}, {2, 20}, {3, 32}, {4, 40}, {5, 51}, {6, 60}};
    std::vector<TabRow> at4 = {{2, 20}, {3, 32}, {4, 40}, {5, 51}, {6, 61}};
    RestoreDigest d3 = tablet_digest(7001, 3);
    RestoreDigest d4 = tablet_digest(7001, 4);
    EXPECT_TRUE(digests_equal(reference(tablet, schema, at3), d3));
    EXPECT_TRUE(digests_equal(reference(tablet, schema, at4), d4));
    EXPECT_EQ(at3.size(), d3.rows);
    EXPECT_EQ(at4.size(), d4.rows);
    EXPECT_NE(d3.root, d4.root);
    for (int threads : {2, 4}) {
        EXPECT_TRUE(digests_equal(d4, tablet_digest(7001, 4, threads))) << threads;
    }

    // what a reader without the bitmap would see differs: key 3 twice, stale rows included
    RestoreDigestInput no_bitmap;
    no_bitmap.rowsets = {rs2, rs3};
    no_bitmap.schema = schema;
    no_bitmap.keys_type = UNIQUE_KEYS;
    no_bitmap.enable_mow = true;
    no_bitmap.delete_bitmap = std::make_shared<DeleteBitmap>(7999); // get_agg caches by tablet id
    no_bitmap.version = 3;
    RestoreDigest stale;
    ASSERT_TRUE(compute_restore_digest(no_bitmap, &stale).ok());
    EXPECT_NE(d3.root, stale.root);
    EXPECT_GT(stale.rows, d3.rows);

    // a version which does not exist is an error, not a digest
    RestoreDigest none;
    EXPECT_FALSE(compute_tablet_restore_digest(*_engine, 7001, 9, &none).ok());
    EXPECT_TRUE(compute_tablet_restore_digest(*_engine, 987654, 3, &none).is<ErrorCode::NOT_FOUND>());
}

TEST_F(RestoreDigestTabletTest, MergeOnReadMergesVersionsAndDropsDeleteSign) {
    auto tablet = create_tablet(7004, /*mow=*/false, /*unique=*/true);
    ASSERT_NE(nullptr, tablet);
    auto schema = tablet->tablet_schema();
    ASSERT_EQ(UNIQUE_KEYS, tablet->keys_type());
    ASSERT_FALSE(tablet->enable_unique_key_merge_on_write());
    write(tablet, schema, 2, {{{1, 10}, {2, 20}, {3, 30}, {4, 40}}}, false, true);
    write(tablet, schema, 3, {{{2, 21}, {3, 31}}}, false, true);
    write(tablet, schema, 4, {{{1, 0, 1}, {4, 41}, {5, 50}}}, true, true);
    write(tablet, schema, 5, {{{1, 11}, {3, 0, 1}}}, false, true);
    // plus a condition on v: DELETE WHERE v = 41 removes the version 4 row of key 4 only
    write_delete(tablet, 6, {cond("v", "=", "41")});
    std::vector<std::vector<TabRow>> expect = {
            {{1, 10}, {2, 20}, {3, 30}, {4, 40}},
            {{1, 10}, {2, 21}, {3, 31}, {4, 40}},
            {{2, 21}, {3, 31}, {4, 41}, {5, 50}},
            {{1, 11}, {2, 21}, {4, 41}, {5, 50}},
            // the condition is applied to the rows before they are merged: key 4 falls back to
            // its older row, the way a query on a merge-on-read table reads it
            {{1, 11}, {2, 21}, {4, 40}, {5, 50}}};
    for (size_t i = 0; i < expect.size(); ++i) {
        RestoreDigest d = tablet_digest(7004, static_cast<int64_t>(i) + 2);
        EXPECT_EQ(expect[i].size(), d.rows) << "version " << i + 2;
        EXPECT_TRUE(digests_equal(reference(tablet, schema, expect[i]), d)) << "version " << i + 2;
        EXPECT_EQ(1U, d.threads);
    }
}

TEST_F(RestoreDigestTabletTest, DuplicateWithDeleteConditionBeforeAndAfterCompaction) {
    auto tablet = create_tablet(7002, false);
    ASSERT_NE(nullptr, tablet);
    auto schema = tablet->tablet_schema();
    std::vector<TabRow> a;
    std::vector<TabRow> b;
    for (int k = 0; k < 100; ++k) {
        a.push_back({k, k % 10});
    }
    for (int k = 50; k < 150; ++k) {
        b.push_back({k, (k * 7) % 10});
    }
    auto rs2 = write(tablet, schema, 2, {a}, false, true);
    auto rs3 = write(tablet, schema, 3, {std::vector<TabRow>(b.begin(), b.begin() + 40), std::vector<TabRow>(b.begin() + 40, b.end())}, false, true);
    write_delete(tablet, 4, {cond("v", "=", "3")});
    std::vector<TabRow> c = {{500, 3}, {501, 4}};
    write(tablet, schema, 5, {c}, false, true);

    std::vector<TabRow> expect;
    for (const auto& r : a) {
        if (r.v != 3) {
            expect.push_back(r);
        }
    }
    for (const auto& r : b) {
        if (r.v != 3) {
            expect.push_back(r);
        }
    }
    expect.insert(expect.end(), c.begin(), c.end());
    RestoreDigest before = tablet_digest(7002, 5);
    EXPECT_EQ(1U, before.delete_predicates);
    EXPECT_EQ(expect.size(), before.rows);
    EXPECT_TRUE(digests_equal(reference(tablet, schema, expect), before));
    // version 3 is below the condition: nothing deleted yet
    std::vector<TabRow> at3 = a;
    at3.insert(at3.end(), b.begin(), b.end());
    EXPECT_TRUE(digests_equal(reference(tablet, schema, at3), tablet_digest(7002, 3)));

    // the compaction of versions 2..4: rows are physically gone, the condition rowset too
    std::vector<RowsetReaderSharedPtr> readers;
    std::vector<RowsetSharedPtr> inputs;
    {
        std::shared_lock lock(tablet->get_header_lock());
        auto ret = tablet->capture_consistent_rowsets_unlocked(Version(0, 4), CaptureRowsetOps {});
        ASSERT_TRUE(ret.has_value());
        for (const auto& rs : ret->rowsets) {
            if (rs->version().first >= 2) {
                inputs.push_back(rs);
            }
        }
    }
    ASSERT_EQ(3U, inputs.size());
    for (const auto& rs : inputs) {
        RowsetReaderSharedPtr reader;
        ASSERT_TRUE(rs->create_reader(&reader).ok());
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
    ctx.version = {2, 4};
    ctx.segments_overlap = NONOVERLAPPING;
    ctx.max_rows_per_segment = UINT32_MAX;
    ctx.tablet = tablet;
    auto res = RowsetFactory::create_rowset_writer(*_engine, ctx, true);
    ASSERT_TRUE(res.has_value()) << res.error();
    auto writer = std::move(res).value();
    Merger::Statistics stats;
    RowIdConversion conversion;
    stats.rowid_conversion = &conversion;
    ASSERT_TRUE(Merger::vertical_merge_rowsets(tablet, ReaderType::READER_BASE_COMPACTION, *schema,
                                               readers, writer.get(), 100, 2, &stats)
                        .ok());
    RowsetSharedPtr merged;
    ASSERT_TRUE(writer->build(merged).ok());
    EXPECT_EQ(a.size() + b.size() - 20, merged->num_rows()); // 10 rows with v = 3 in a, 10 in b
    ASSERT_TRUE(tablet->add_rowset(merged).ok());

    RestoreDigest after = tablet_digest(7002, 5);
    EXPECT_TRUE(digests_equal(before, after));
    EXPECT_EQ(0U, after.delete_predicates);
}

TEST_F(RestoreDigestTabletTest, ColumnAddedLaterIsReadAsItsDefault) {
    auto tablet = create_tablet(7003, false);
    ASSERT_NE(nullptr, tablet);
    auto old_schema = tablet->tablet_schema();
    std::vector<TabRow> rows;
    for (int k = 0; k < 40; ++k) {
        rows.push_back({k, k * 3});
    }
    write(tablet, old_schema, 2, {rows}, false, true);

    // light schema change: a column c INT DEFAULT 7 is added, the rowset is not rewritten
    TabletSchemaPB pb;
    old_schema->to_schema_pb(&pb);
    pb.set_schema_version(old_schema->schema_version() + 1);
    ColumnPB* c = pb.add_column();
    c->set_unique_id(pb.next_column_unique_id());
    pb.set_next_column_unique_id(pb.next_column_unique_id() + 1);
    c->set_name("c");
    c->set_type("INT");
    c->set_is_key(false);
    c->set_length(4);
    c->set_index_length(4);
    c->set_is_nullable(true);
    c->set_default_value("7");
    c->set_aggregation("NONE");
    auto new_schema = std::make_shared<TabletSchema>();
    new_schema->init_from_pb(pb);
    tablet->update_max_version_schema(new_schema);

    RestoreDigest d = tablet_digest(7003, 2);
    EXPECT_EQ(rows.size(), d.rows);

    // reference: the same rows with the default written out
    RestoreDigestInput ref_in;
    ref_in.schema = new_schema;
    ref_in.keys_type = DUP_KEYS;
    ref_in.version = 100;
    {
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
        ctx.tablet_schema = new_schema;
        ctx.version = {2, 2};
        ctx.segments_overlap = NONOVERLAPPING;
        ctx.max_rows_per_segment = UINT32_MAX;
        ctx.tablet = tablet;
        auto res = RowsetFactory::create_rowset_writer(*_engine, ctx, false);
        ASSERT_TRUE(res.has_value()) << res.error();
        auto writer = std::move(res).value();
        Block block = new_schema->create_storage_block();
        auto cols = std::move(block).mutate_columns();
        const int32_t seven = 7;
        for (const TabRow& r : rows) {
            cols[0]->insert_data(reinterpret_cast<const char*>(&r.k), 4);
            cols[1]->insert_data(reinterpret_cast<const char*>(&r.v), 4);
            cols[2]->insert_data(reinterpret_cast<const char*>(&seven), 4);
        }
        block.set_columns(std::move(cols));
        ASSERT_TRUE(writer->add_block(&block).ok());
        ASSERT_TRUE(writer->flush().ok());
        RowsetSharedPtr rs;
        ASSERT_TRUE(writer->build(rs).ok());
        ref_in.rowsets.push_back(rs);
    }
    RestoreDigest ref;
    ASSERT_TRUE(compute_restore_digest(ref_in, &ref).ok());
    EXPECT_TRUE(digests_equal(ref, d));
    EXPECT_EQ(ref.schema_sig, d.schema_sig);
}


// ---------------------------------------------------------------------------------------------
// Backup / restore side: the digest cache and the digest of the snapshot and RESTORE_DIGEST tasks.
// ---------------------------------------------------------------------------------------------

TEST_F(RestoreDigestTabletTest, LogicalDigestIsCachedByTabletVersionAndSchema) {
    auto tablet = create_tablet(7101, false);
    ASSERT_NE(nullptr, tablet);
    auto schema = tablet->tablet_schema();
    write(tablet, schema, 2, {{{1, 10}, {2, 20}, {3, 30}}}, false, true);
    write(tablet, schema, 3, {{{4, 40}, {5, 50}}}, false, true);

    RestoreDigestCache cache(/*capacity=*/16);
    LogicalDigestResult first;
    ASSERT_TRUE(get_tablet_logical_digest(*_engine, 7101, 3, 2, &cache, &first).ok());
    EXPECT_FALSE(first.from_cache);
    EXPECT_EQ(5U, first.rows);
    EXPECT_EQ(RestoreDigest::kAlgoVersion, first.algo_version);
    EXPECT_EQ(1U, cache.size());
    EXPECT_EQ(0U, cache.hits());
    // same as the digest the debug endpoint computes
    RestoreDigest direct = tablet_digest(7101, 3);
    EXPECT_EQ(direct.root, first.root);
    EXPECT_EQ(direct.schema_sig, first.schema_sig);

    LogicalDigestResult second;
    ASSERT_TRUE(get_tablet_logical_digest(*_engine, 7101, 3, 1, &cache, &second).ok());
    EXPECT_TRUE(second.from_cache);
    EXPECT_EQ(first.root, second.root);
    EXPECT_EQ(first.schema_sig, second.schema_sig);
    EXPECT_EQ(first.rows, second.rows);
    EXPECT_EQ(1U, cache.hits());

    // another version is another entry
    LogicalDigestResult v2;
    ASSERT_TRUE(get_tablet_logical_digest(*_engine, 7101, 2, 1, &cache, &v2).ok());
    EXPECT_FALSE(v2.from_cache);
    EXPECT_EQ(3U, v2.rows);
    EXPECT_NE(first.root, v2.root);
    EXPECT_EQ(2U, cache.size());

    // no cache: always computed
    LogicalDigestResult nocache;
    ASSERT_TRUE(get_tablet_logical_digest(*_engine, 7101, 3, 1, nullptr, &nocache).ok());
    EXPECT_FALSE(nocache.from_cache);
    EXPECT_EQ(first.root, nocache.root);
}

TEST(RestoreDigestCacheTest, LruEvictionAndKey) {
    RestoreDigestCache cache(/*capacity=*/2);
    auto key = [](int64_t tablet, int64_t version, uint32_t algo, const std::string& sig) {
        RestoreDigestCache::Key k;
        k.tablet_id = tablet;
        k.version = version;
        k.algo_version = algo;
        k.schema_sig = sig;
        return k;
    };
    auto value = [](const std::string& root) {
        LogicalDigestResult r;
        r.root = root;
        r.schema_sig = "sig";
        r.rows = 1;
        return r;
    };
    cache.insert(key(1, 5, 1, "a"), value("r1"));
    cache.insert(key(2, 5, 1, "a"), value("r2"));
    LogicalDigestResult out;
    // touch 1 so that 2 is the eviction victim
    ASSERT_TRUE(cache.lookup(key(1, 5, 1, "a"), &out));
    EXPECT_EQ("r1", out.root);
    EXPECT_TRUE(out.from_cache);
    cache.insert(key(3, 5, 1, "a"), value("r3"));
    EXPECT_EQ(2U, cache.size());
    EXPECT_FALSE(cache.lookup(key(2, 5, 1, "a"), &out));
    EXPECT_TRUE(cache.lookup(key(3, 5, 1, "a"), &out));
    // every field of the key matters
    EXPECT_FALSE(cache.lookup(key(1, 6, 1, "a"), &out));
    EXPECT_FALSE(cache.lookup(key(1, 5, 2, "a"), &out));
    EXPECT_FALSE(cache.lookup(key(1, 5, 1, "b"), &out));
    {
        // the data identity matters too: the tablet instance and the rowsets
        RestoreDigestCache::Key k = key(1, 5, 1, "a");
        k.tablet_uid = TabletUid(1, 2);
        EXPECT_FALSE(cache.lookup(k, &out));
        k = key(1, 5, 1, "a");
        k.rowsets_sig = "other";
        EXPECT_FALSE(cache.lookup(k, &out));
    }
    EXPECT_TRUE(cache.lookup(key(1, 5, 1, "a"), &out));
    // overwriting does not grow the cache
    cache.insert(key(1, 5, 1, "a"), value("r1x"));
    EXPECT_EQ(2U, cache.size());
    ASSERT_TRUE(cache.lookup(key(1, 5, 1, "a"), &out));
    EXPECT_EQ("r1x", out.root);
    cache.clear();
    EXPECT_EQ(0U, cache.size());
}

// What the snapshot task (compute_logical_digest) and the RESTORE_DIGEST task report.
TEST_F(RestoreDigestTabletTest, TaskReportsTheDigestAndHitsTheCache) {
    auto tablet = create_tablet(7102, false);
    ASSERT_NE(nullptr, tablet);
    auto schema = tablet->tablet_schema();
    write(tablet, schema, 2, {{{1, 10}, {2, 20}}}, false, true);
    write(tablet, schema, 3, {{{3, 30}}}, false, true);
    RestoreDigestCache::instance()->clear();
    const uint64_t hits_before = RestoreDigestCache::instance()->hits();

    TLogicalDigest d = compute_logical_digest_for_task(*_engine, 7102, 3, /*threads=*/0);
    EXPECT_EQ("OK", d.status_code);
    EXPECT_TRUE(d.__isset.root);
    EXPECT_EQ(3, d.rows);
    EXPECT_EQ(static_cast<int32_t>(RestoreDigest::kAlgoVersion), d.algo_version);
    RestoreDigest direct = tablet_digest(7102, 3);
    EXPECT_EQ(direct.root, d.root);
    EXPECT_EQ(direct.schema_sig, d.schema_sig);
    EXPECT_FALSE(d.__isset.status_msg);
    EXPECT_EQ(hits_before, RestoreDigestCache::instance()->hits());

    // the second request (e.g. the RESTORE_DIGEST task after the snapshot task) is a cache hit
    TLogicalDigest again = compute_logical_digest_for_task(*_engine, 7102, 3, /*threads=*/2);
    EXPECT_EQ("OK", again.status_code);
    EXPECT_EQ(d.root, again.root);
    EXPECT_EQ(hits_before + 1, RestoreDigestCache::instance()->hits());
}

TEST_F(RestoreDigestTabletTest, TaskReportsNotSupportedAndErrors) {
    // aggregate keys: NOT_SUPPORTED, no root
    auto request = testutil::create_tablet_request(
            7103, /*schema_hash=*/1, /*partition_id=*/10, /*short_key_column_count=*/1,
            TKeysType::AGG_KEYS,
            {{"k", TPrimitiveType::INT, true},
             {"v", TPrimitiveType::INT, false, false, TAggregationType::SUM, true}});
    RuntimeProfile profile("restore_digest_ut");
    ASSERT_TRUE(_engine->create_tablet(request, &profile).ok());
    ASSERT_NE(nullptr, _engine->tablet_manager()->get_tablet(7103));
    RestoreDigestCache::instance()->clear();
    TLogicalDigest d = compute_logical_digest_for_task(*_engine, 7103, 1, 0);
    EXPECT_EQ("NOT_SUPPORTED", d.status_code);
    EXPECT_FALSE(d.__isset.root);
    EXPECT_TRUE(d.__isset.status_msg);
    EXPECT_FALSE(d.status_msg.empty());
    // a failure is not cached
    EXPECT_EQ(0U, RestoreDigestCache::instance()->size());

    // unknown tablet: ERROR
    TLogicalDigest missing = compute_logical_digest_for_task(*_engine, 7199, 2, 0);
    EXPECT_EQ("ERROR", missing.status_code);
    EXPECT_FALSE(missing.__isset.root);
    EXPECT_FALSE(missing.status_msg.empty());

    // a version the tablet does not have: ERROR
    auto tablet = create_tablet(7104, false);
    ASSERT_NE(nullptr, tablet);
    write(tablet, tablet->tablet_schema(), 2, {{{1, 10}}}, false, true);
    TLogicalDigest bad_version = compute_logical_digest_for_task(*_engine, 7104, 9, 0);
    EXPECT_EQ("ERROR", bad_version.status_code);
    EXPECT_FALSE(bad_version.__isset.root);
}


// ===== 12. decomposed digest: compose(V) == the digest computed at V ==========================

namespace {
std::atomic<int64_t> g_prefix_bitmap_tablet_id {5000000};
int64_t next_bitmap_tablet_id() {
    return g_prefix_bitmap_tablet_id.fetch_add(1);
}
constexpr int64_t kPrefixTabletId = 777;
constexpr int64_t kNever = INT64_MAX;

struct MRow {
    int k;
    int v;
    int seq;
    int del;
};
struct MBatch {
    RowsetSharedPtr rs;
    int64_t start = 0;
    int64_t version = 0;
    std::vector<MRow> rows;
    std::vector<std::pair<int, int>> loc; // segment, row id of every row
    std::vector<int64_t> death;           // the lowest version at which the bitmap marks the row
};
} // namespace

class RestoreDigestPrefixTest : public RestoreDigestTest {
protected:
    void SetUp() override {
        RestoreDigestTest::SetUp();
        // The segment cache is keyed by the rowset id and outlives the test: never reuse the ids of
        // the other tests, whose files have other contents.
        static std::atomic<int64_t> next_base {20000000};
        _next_rowset_id = next_base.fetch_add(1000000);
    }

    static std::vector<ColSpec> dup_cols() {
        return {{"k", "INT", true, false, 4},
                {"v", "INT", false, true, 4},
                {"s", "VARCHAR", false, true, 16}};
    }
    static std::vector<ColSpec> mow_cols() {
        return {{"k", "INT", true, false, 4},
                {"v1", "INT", false, true, 4},
                {"v2", "VARCHAR", false, true, 16},
                {SEQUENCE_COL, "INT", false, false, 4},
                {DELETE_SIGN, "TINYINT", false, false, 1}};
    }
    static Row dup_row(int k, std::optional<int> v) {
        return {V<int32_t>(k), v.has_value() ? V<int32_t>(*v) : N(),
                S("s" + std::to_string(k % 13))};
    }
    static Row mow_row(const MRow& r) {
        return {V<int32_t>(r.k), V<int32_t>(r.v), S("m" + std::to_string(r.v % 7)),
                V<int32_t>(r.seq), V<int8_t>(r.del)};
    }

    RestoreDigestDecomposed decompose(const TabletSchemaSPtr& schema,
                                      const std::vector<RowsetSharedPtr>& chain, KeysType keys_type,
                                      bool mow, const DeleteBitmapPtr& bitmap, int threads = 1) {
        RestoreDigestInput in;
        in.rowsets = chain;
        in.schema = schema;
        in.keys_type = keys_type;
        in.enable_mow = mow;
        in.delete_bitmap = bitmap;
        in.version = chain.back()->end_version();
        in.threads = threads;
        RestoreDigestDecomposed d;
        Status st = decompose_restore_digest(in, kPrefixTabletId, &d);
        EXPECT_TRUE(st.ok()) << st;
        return d;
    }

    // compose(V) at every rowset boundary is bucket by bucket the digest computed at V; also through
    // a serialize / parse round trip.
    void expect_compose_equals_direct(const TabletSchemaSPtr& schema,
                                      const std::vector<RowsetSharedPtr>& chain, KeysType keys_type,
                                      bool mow, const DeleteBitmapPtr& bitmap,
                                      const RestoreDigestDecomposed& dec, const std::string& what,
                                      std::map<int64_t, RestoreDigest>* directs = nullptr) {
        ASSERT_EQ(chain.size(), dec.rowsets.size()) << what;
        std::string content = dec.serialize();
        RestoreDigestDecomposed parsed;
        Status pst = RestoreDigestDecomposed::parse(content, dec.file_root(), kPrefixTabletId, &parsed);
        ASSERT_TRUE(pst.ok()) << pst << " " << what;
        EXPECT_EQ(content, parsed.serialize()) << what;
        for (size_t i = 0; i < chain.size(); ++i) {
            const int64_t v = chain[i]->end_version();
            std::vector<RowsetSharedPtr> prefix(chain.begin(), chain.begin() + i + 1);
            RestoreDigest direct = digest_of(schema, prefix, keys_type, mow, bitmap, v);
            const RestoreDigestDecomposed* both[] = {&dec, &parsed};
            for (const RestoreDigestDecomposed* d : both) {
                ASSERT_TRUE(d->is_boundary(v)) << what << " version " << v;
                RestoreDigest composed;
                Status st = d->compose(v, &composed);
                ASSERT_TRUE(st.ok()) << st << " " << what << " version " << v;
                EXPECT_TRUE(same_digest(direct, composed)) << what << " version " << v;
                EXPECT_EQ(direct.root, composed.root) << what << " version " << v;
                EXPECT_EQ(direct.rows, composed.rows) << what << " version " << v;
                EXPECT_EQ(direct.schema_sig, composed.schema_sig) << what;
            }
            if (directs != nullptr) {
                (*directs)[v] = direct;
            }
        }
        // the digest recorded in the file is the one at the base version
        RestoreDigest base;
        ASSERT_TRUE(dec.compose(dec.base_version, &base).ok());
        EXPECT_EQ(base.root, dec.root) << what;
        EXPECT_EQ(base.rows, dec.rows) << what;
    }

    static std::vector<std::pair<int, int>> locate(const std::vector<int>& keys, size_t rps,
                                                   bool overlapping) {
        std::vector<std::pair<int, int>> loc(keys.size());
        if (keys.empty()) {
            return loc;
        }
        if (overlapping) {
            const size_t segs = std::max<size_t>(1, (keys.size() + rps - 1) / rps);
            std::vector<std::vector<size_t>> by_seg(segs);
            for (size_t i = 0; i < keys.size(); ++i) {
                by_seg[i % segs].push_back(i);
            }
            for (size_t s = 0; s < segs; ++s) {
                auto& idx = by_seg[s];
                std::stable_sort(idx.begin(), idx.end(),
                                 [&](size_t a, size_t b) { return keys[a] < keys[b]; });
                for (size_t r = 0; r < idx.size(); ++r) {
                    loc[idx[r]] = {static_cast<int>(s), static_cast<int>(r)};
                }
            }
        } else {
            std::vector<size_t> idx(keys.size());
            for (size_t i = 0; i < idx.size(); ++i) {
                idx[i] = i;
            }
            std::stable_sort(idx.begin(), idx.end(),
                             [&](size_t a, size_t b) { return keys[a] < keys[b]; });
            for (size_t p = 0; p < idx.size(); ++p) {
                loc[idx[p]] = {static_cast<int>(p / rps), static_cast<int>(p % rps)};
            }
        }
        return loc;
    }

    // ---- duplicate

    struct DupChain {
        TabletSchemaSPtr schema;
        std::vector<RowsetSharedPtr> chain;
        std::vector<bool> is_delete;
    };

    // `deletes`: also write DELETE conditions among the batches.
    DupChain make_dup_chain(unsigned seed, int batches, bool deletes) {
        std::mt19937 rng(seed);
        DupChain out;
        out.schema = make_schema(DUP_KEYS, dup_cols());
        int64_t version = 2;
        for (int b = 0; b < batches; ++b) {
            if (deletes && b > 0 && rng() % 3 == 0) {
                std::vector<TCondition> conds;
                switch (rng() % 3) {
                case 0:
                    conds = {cond("v", "=", {std::to_string(rng() % 6)})};
                    break;
                case 1:
                    conds = {cond("k", ">=", {std::to_string(20 + rng() % 20)}),
                             cond("v", "<=", {std::to_string(rng() % 6)})};
                    break;
                default:
                    conds = {cond("k", "<", {std::to_string(rng() % 8)})};
                    break;
                }
                out.chain.push_back(write_delete_rowset(out.schema, version++, conds));
                out.is_delete.push_back(true);
                continue;
            }
            const size_t n = rng() % 5 == 0 ? 0 : 1 + rng() % 60;
            std::vector<Row> rows;
            for (size_t i = 0; i < n; ++i) {
                std::optional<int> v;
                if (rng() % 7 != 0) {
                    v = static_cast<int>(rng() % 6);
                }
                rows.push_back(dup_row(static_cast<int>(rng() % 50), v));
            }
            static const size_t kSegRows[] = {3, 7, 1000};
            out.chain.push_back(write_rowset(out.schema, rows, version++, kSegRows[rng() % 3],
                                             rng() % 2 == 0));
            out.is_delete.push_back(false);
        }
        return out;
    }

    // ---- merge on write

    struct MowChain {
        TabletSchemaSPtr schema;
        std::vector<MBatch> batches;
        DeleteBitmapPtr bitmap;
        std::vector<RowsetSharedPtr> chain() const {
            std::vector<RowsetSharedPtr> c;
            for (const auto& b : batches) {
                c.push_back(b.rs);
            }
            return c;
        }
        // the number of visible rows at `version` by the model
        uint64_t visible_at(int64_t version) const {
            uint64_t n = 0;
            for (const auto& b : batches) {
                if (b.version > version) {
                    break;
                }
                for (size_t i = 0; i < b.rows.size(); ++i) {
                    n += b.rows[i].del == 0 && b.death[i] > version ? 1 : 0;
                }
            }
            return n;
        }
    };

    // `deletes`: also write DELETE conditions among the batches (rowsets without rows). The model of the
    // batches (death, visible_at) does not know the conditions, the tests compare with the direct digest.
    MowChain make_mow_chain(unsigned seed, int batches, bool deletes = false) {
        std::mt19937 rng(seed);
        MowChain out;
        out.schema = make_schema(UNIQUE_KEYS, mow_cols(), 3);
        out.bitmap = std::make_shared<DeleteBitmap>(next_bitmap_tablet_id());
        std::map<int, std::pair<size_t, size_t>> latest; // key -> (batch, row)
        int64_t version = 2;
        for (int b = 0; b < batches; ++b) {
            if (deletes && b > 0 && rng() % 3 == 0) {
                std::vector<TCondition> conds;
                switch (rng() % 3) {
                case 0:
                    conds = {cond("v1", "<", {std::to_string(100 + rng() % 400)})};
                    break;
                case 1:
                    conds = {cond("k", ">=", {std::to_string(15 + rng() % 15)}),
                             cond("v1", ">", {std::to_string(rng() % 500)})};
                    break;
                default:
                    conds = {cond("k", "<", {std::to_string(1 + rng() % 8)})};
                    break;
                }
                MBatch del;
                del.version = del.start = version++;
                del.rs = write_delete_rowset(out.schema, del.version, conds);
                out.batches.push_back(std::move(del));
                continue;
            }
            MBatch batch;
            batch.version = batch.start = version++;
            const size_t n = rng() % 6 == 0 ? 0 : 1 + rng() % 24;
            std::set<int> used;
            for (size_t i = 0; i < n; ++i) {
                int k = static_cast<int>(rng() % 30);
                if (!used.insert(k).second) {
                    continue;
                }
                batch.rows.push_back({k, static_cast<int>(rng() % 1000),
                                      static_cast<int>(batch.version), rng() % 5 == 0 ? 1 : 0});
            }
            // overlapping rowset with segments of two rows, and a key written twice in it: the
            // first write is marked at the version of the rowset itself
            const bool overlapping = batch.rows.size() >= 3 && rng() % 3 == 0;
            const size_t rps = overlapping ? 2 : (rng() % 2 == 0 ? 4 : 1000);
            int self_dup = -1;
            if (overlapping) {
                self_dup = static_cast<int>(rng() % (batch.rows.size() - 1));
                MRow again = batch.rows[static_cast<size_t>(self_dup)];
                again.v += 1;
                again.del = 0;
                batch.rows.insert(batch.rows.begin() + self_dup + 1, again);
            }
            std::vector<int> keys;
            for (const auto& r : batch.rows) {
                keys.push_back(r.k);
            }
            batch.loc = locate(keys, rps, overlapping);
            batch.death.assign(batch.rows.size(), kNever);

            const size_t bi = out.batches.size();
            std::vector<Row> rows;
            for (const auto& r : batch.rows) {
                rows.push_back(mow_row(r));
            }
            batch.rs = write_rowset(out.schema, rows, batch.version, rps, overlapping);
            // marks
            const int64_t mark_version = batch.version;
            auto mark = [&](MBatch& target, size_t row) {
                out.bitmap->add({target.rs->rowset_id(), static_cast<uint32_t>(target.loc[row].first),
                                 static_cast<uint64_t>(mark_version)},
                                static_cast<uint32_t>(target.loc[row].second));
                target.death[row] = std::min(target.death[row], mark_version);
            };
            out.batches.push_back(std::move(batch));
            MBatch& cur = out.batches.back();
            if (self_dup >= 0) {
                mark(cur, static_cast<size_t>(self_dup));
            }
            for (size_t i = 0; i < cur.rows.size(); ++i) {
                if (static_cast<int>(i) == self_dup) {
                    continue;
                }
                auto it = latest.find(cur.rows[i].k);
                if (it != latest.end()) {
                    MBatch& old = out.batches[it->second.first];
                    const size_t old_row = it->second.second;
                    // a delete sign row is hidden anyway, whether it is marked or not
                    if (old.rows[old_row].del == 0 || rng() % 4 != 0) {
                        mark(old, old_row);
                    }
                }
                latest[cur.rows[i].k] = {bi, i};
            }
            // noise: a row which is dead already is marked again at a later version
            if (bi > 1 && rng() % 6 == 0) {
                MBatch& old = out.batches[rng() % (bi - 1)];
                for (size_t i = 0; i < old.rows.size(); ++i) {
                    if (old.death[i] < mark_version) {
                        mark(old, i);
                        break;
                    }
                }
            }
        }
        return out;
    }

    // Merges the batches [a, b] into one rowset with the version range [start of a, end of b]: the
    // rows alive at the end version are kept (also the delete sign rows), the marks of later
    // versions follow them. Returns the chain after the compaction.
    MowChain compact_mow(const MowChain& in, size_t a, size_t b, unsigned seed) {
        std::mt19937 rng(seed);
        const int64_t end = in.batches[b].version;
        struct Kept {
            MRow row;
            int64_t death;
        };
        std::vector<Kept> kept;
        for (size_t i = a; i <= b; ++i) {
            for (size_t r = 0; r < in.batches[i].rows.size(); ++r) {
                if (in.batches[i].death[r] > end) {
                    kept.push_back({in.batches[i].rows[r], in.batches[i].death[r]});
                }
            }
        }
        const bool overlapping = false;
        const size_t rps = rng() % 2 == 0 ? 5 : 1000;
        std::vector<int> keys;
        for (const auto& k : kept) {
            keys.push_back(k.row.k);
        }
        MBatch merged;
        merged.start = in.batches[a].start;
        merged.version = end;
        merged.loc = locate(keys, rps, overlapping);
        std::vector<Row> rows;
        for (const auto& k : kept) {
            merged.rows.push_back(k.row);
            merged.death.push_back(k.death);
            rows.push_back(mow_row(k.row));
        }
        MowChain out;
        out.schema = in.schema;
        out.bitmap = std::make_shared<DeleteBitmap>(next_bitmap_tablet_id());
        merged.rs = write_rowset(in.schema, rows, end, rps, overlapping, merged.start);
        for (size_t i = 0; i < merged.rows.size(); ++i) {
            if (merged.death[i] != kNever) {
                out.bitmap->add({merged.rs->rowset_id(), static_cast<uint32_t>(merged.loc[i].first),
                                 merged.death[i]},
                                static_cast<uint32_t>(merged.loc[i].second));
            }
        }
        for (size_t i = 0; i < in.batches.size(); ++i) {
            if (i == a) {
                out.batches.push_back(merged);
            }
            if (i >= a && i <= b) {
                continue;
            }
            out.batches.push_back(in.batches[i]);
            for (size_t r = 0; r < in.batches[i].rows.size(); ++r) {
                if (in.batches[i].death[r] != kNever) {
                    out.bitmap->add({in.batches[i].rs->rowset_id(),
                                     static_cast<uint32_t>(in.batches[i].loc[r].first),
                                     in.batches[i].death[r]},
                                    static_cast<uint32_t>(in.batches[i].loc[r].second));
                }
            }
        }
        return out;
    }
};

TEST_F(RestoreDigestPrefixTest, DuplicateComposeEqualsDirectAtEveryBoundary) {
    for (unsigned seed : {1U, 2U, 3U, 4U, 5U, 6U}) {
        auto c = make_dup_chain(seed, 10, /*deletes=*/false);
        auto dec = decompose(c.schema, c.chain, DUP_KEYS, false, nullptr);
        ASSERT_EQ(10U, dec.rowsets.size());
        EXPECT_TRUE(dec.marks.empty()) << "a duplicate table without conditions has no marks";
        EXPECT_FALSE(dec.mow);
        expect_compose_equals_direct(c.schema, c.chain, DUP_KEYS, false, nullptr, dec,
                                     "seed " + std::to_string(seed));
        // the worker threads do not change a byte
        EXPECT_EQ(dec.serialize(),
                  decompose(c.schema, c.chain, DUP_KEYS, false, nullptr, 3).serialize());
        // not a boundary: nothing can be composed
        RestoreDigest d;
        for (int64_t v : {int64_t(0), int64_t(1), int64_t(100)}) {
            EXPECT_FALSE(dec.is_boundary(v));
            Status st = dec.compose(v, &d);
            EXPECT_TRUE(st.is<ErrorCode::NOT_IMPLEMENTED_ERROR>()) << st;
        }
    }
}

TEST_F(RestoreDigestPrefixTest, DuplicateWithDeleteConditions) {
    bool any_marks = false;
    for (unsigned seed : {11U, 12U, 13U, 14U, 15U, 16U, 17U, 18U}) {
        auto c = make_dup_chain(seed, 12, /*deletes=*/true);
        auto dec = decompose(c.schema, c.chain, DUP_KEYS, false, nullptr);
        any_marks = any_marks || !dec.marks.empty();
        // a mark version is the version of a DELETE condition
        for (const auto& mark : dec.marks) {
            bool is_condition = false;
            for (size_t i = 0; i < c.chain.size(); ++i) {
                is_condition = is_condition || (c.is_delete[i] && c.chain[i]->end_version() == mark.mark_version);
            }
            EXPECT_TRUE(is_condition) << "mark version " << mark.mark_version;
        }
        expect_compose_equals_direct(c.schema, c.chain, DUP_KEYS, false, nullptr, dec,
                                     "seed " + std::to_string(seed));
        EXPECT_EQ(dec.serialize(),
                  decompose(c.schema, c.chain, DUP_KEYS, false, nullptr, 4).serialize());
    }
    EXPECT_TRUE(any_marks) << "no DELETE condition matched a row, the test covers nothing";
}

TEST_F(RestoreDigestPrefixTest, DuplicateStaysComposableAfterCompaction) {
    for (bool deletes : {false, true}) {
        for (unsigned seed : {21U, 22U, 23U, 24U}) {
            auto c = make_dup_chain(seed, 9, deletes);
            auto schema = c.schema;
            auto tablet = make_meta_tablet(DUP_KEYS, schema);
            auto before = decompose(schema, c.chain, DUP_KEYS, false, nullptr);
            std::map<int64_t, RestoreDigest> directs;
            expect_compose_equals_direct(schema, c.chain, DUP_KEYS, false, nullptr, before, "before",
                                         &directs);
            // A base compaction covers the rowsets from the start of the chain (the DELETE
            // conditions inside are applied for good). A cumulative compaction takes a run of data
            // rowsets: the conditions after it stay effective for its rows.
            struct Run {
                size_t first;
                size_t last;
                ReaderType type;
            };
            std::vector<Run> runs = {{0, 4, ReaderType::READER_BASE_COMPACTION},
                                     {0, 7, ReaderType::READER_BASE_COMPACTION}};
            for (size_t i = 0; i + 1 < c.chain.size(); ++i) {
                if (!c.is_delete[i] && !c.is_delete[i + 1] && c.chain[i]->num_rows() > 0) {
                    size_t j = i + 1;
                    while (j + 1 < c.chain.size() && !c.is_delete[j + 1] && j - i < 3) {
                        ++j;
                    }
                    runs.push_back({i, j, ReaderType::READER_CUMULATIVE_COMPACTION});
                    break;
                }
            }
            for (const Run& run : runs) {
                std::vector<RowsetSharedPtr> inputs(c.chain.begin() + run.first,
                                                    c.chain.begin() + run.last + 1);
                auto merged = compact(schema, tablet, inputs,
                                      Version(inputs.front()->start_version(),
                                              inputs.back()->end_version()),
                                      run.type);
                ASSERT_NE(nullptr, merged);
                std::vector<RowsetSharedPtr> chain(c.chain.begin(), c.chain.begin() + run.first);
                chain.push_back(merged);
                chain.insert(chain.end(), c.chain.begin() + run.last + 1, c.chain.end());
                auto dec = decompose(schema, chain, DUP_KEYS, false, nullptr);
                const std::string what = std::string(deletes ? "deletes " : "") + "seed " +
                                         std::to_string(seed) + " run " + std::to_string(run.first) +
                                         "-" + std::to_string(run.last);
                expect_compose_equals_direct(schema, chain, DUP_KEYS, false, nullptr, dec, what);
                // the versions which are still boundaries give the digests of before the compaction
                for (const auto& rs : chain) {
                    RestoreDigest composed;
                    ASSERT_TRUE(dec.compose(rs->end_version(), &composed).ok());
                    EXPECT_TRUE(same_digest(directs.at(rs->end_version()), composed))
                            << what << " version " << rs->end_version();
                }
                // the versions inside the merged rowset are not
                for (int64_t v = merged->start_version(); v < merged->end_version(); ++v) {
                    EXPECT_FALSE(dec.is_boundary(v)) << what << " version " << v;
                    RestoreDigest composed;
                    EXPECT_TRUE(dec.compose(v, &composed).is<ErrorCode::NOT_IMPLEMENTED_ERROR>());
                }
            }
        }
    }
}

TEST_F(RestoreDigestPrefixTest, MergeOnWriteComposeEqualsDirectAtEveryBoundary) {
    for (unsigned seed : {31U, 32U, 33U, 34U, 35U, 36U, 37U, 38U}) {
        auto c = make_mow_chain(seed, 12);
        auto chain = c.chain();
        auto dec = decompose(c.schema, chain, UNIQUE_KEYS, true, c.bitmap);
        EXPECT_TRUE(dec.mow);
        EXPECT_FALSE(dec.marks.empty());
        // the simulation itself: what the model says is visible is what the digest reads
        for (const auto& b : c.batches) {
            RestoreDigest composed;
            ASSERT_TRUE(dec.compose(b.version, &composed).ok());
            EXPECT_EQ(c.visible_at(b.version), composed.rows) << "seed " << seed << " version " << b.version;
        }
        expect_compose_equals_direct(c.schema, chain, UNIQUE_KEYS, true, c.bitmap, dec,
                                     "seed " + std::to_string(seed));
        EXPECT_EQ(dec.serialize(),
                  decompose(c.schema, chain, UNIQUE_KEYS, true, c.bitmap, 3).serialize());
        // marks only at versions of the chain, never at a version of the base or below
        for (const auto& mark : dec.marks) {
            EXPECT_GT(mark.mark_version, 2);
            EXPECT_LE(mark.mark_version, dec.base_version);
        }
    }
}

TEST_F(RestoreDigestPrefixTest, MergeOnWriteIgnoresMarksAboveTheBaseVersion) {
    auto c = make_mow_chain(41, 10);
    auto chain = c.chain();
    // the backup is taken at version 7: later marks must not leak into any part
    std::vector<RowsetSharedPtr> prefix;
    for (const auto& b : c.batches) {
        if (b.version <= 7) {
            prefix.push_back(b.rs);
        }
    }
    auto dec = decompose(c.schema, prefix, UNIQUE_KEYS, true, c.bitmap);
    EXPECT_EQ(7, dec.base_version);
    expect_compose_equals_direct(c.schema, prefix, UNIQUE_KEYS, true, c.bitmap, dec, "base 7");
    for (const auto& mark : dec.marks) {
        EXPECT_LE(mark.mark_version, 7);
    }
}

TEST_F(RestoreDigestPrefixTest, MergeOnWriteStaysComposableAfterCompaction) {
    for (unsigned seed : {51U, 52U, 53U, 54U, 55U, 56U}) {
        auto c = make_mow_chain(seed, 12);
        auto chain = c.chain();
        auto before = decompose(c.schema, chain, UNIQUE_KEYS, true, c.bitmap);
        std::map<int64_t, RestoreDigest> directs;
        expect_compose_equals_direct(c.schema, chain, UNIQUE_KEYS, true, c.bitmap, before, "before",
                                     &directs);
        // compact twice in a row: [2, 5] first, then the result with its successor
        MowChain once = compact_mow(c, 1, 4, seed);
        MowChain twice = compact_mow(once, 0, 1, seed + 1);
        for (const MowChain* after : {&once, &twice}) {
            auto after_chain = after->chain();
            auto dec = decompose(after->schema, after_chain, UNIQUE_KEYS, true, after->bitmap);
            const std::string what = "seed " + std::to_string(seed);
            expect_compose_equals_direct(after->schema, after_chain, UNIQUE_KEYS, true, after->bitmap,
                                         dec, what);
            for (const auto& b : after->batches) {
                RestoreDigest composed;
                ASSERT_TRUE(dec.compose(b.version, &composed).ok());
                EXPECT_TRUE(same_digest(directs.at(b.version), composed))
                        << what << " version " << b.version;
                EXPECT_EQ(after->visible_at(b.version), composed.rows) << what;
            }
            for (const auto& b : after->batches) {
                for (int64_t v = b.start; v < b.version; ++v) {
                    EXPECT_FALSE(dec.is_boundary(v)) << what << " version " << v;
                }
            }
        }
    }
}

TEST_F(RestoreDigestPrefixTest, MergeOnWriteWithDeleteConditionsComposeEqualsDirect) {
    bool any_condition_mark = false;
    bool any_chain_with_conditions = false;
    for (unsigned seed : {101U, 102U, 103U, 104U, 105U, 106U, 107U, 108U, 109U, 110U, 111U, 112U}) {
        auto c = make_mow_chain(seed, 14, /*deletes=*/true);
        auto chain = c.chain();
        std::set<int64_t> condition_versions;
        for (const auto& b : c.batches) {
            if (b.rs->rowset_meta()->has_delete_predicate()) {
                condition_versions.insert(b.version);
            }
        }
        any_chain_with_conditions = any_chain_with_conditions || !condition_versions.empty();
        auto dec = decompose(c.schema, chain, UNIQUE_KEYS, true, c.bitmap);
        EXPECT_TRUE(dec.mow);
        EXPECT_FALSE(dec.mor);
        for (const auto& mark : dec.marks) {
            any_condition_mark = any_condition_mark || condition_versions.contains(mark.mark_version);
        }
        expect_compose_equals_direct(c.schema, chain, UNIQUE_KEYS, true, c.bitmap, dec,
                                     "seed " + std::to_string(seed));
        // the format of the models which compose by rowset has not changed
        const std::string content = dec.serialize();
        uint32_t format = 0;
        std::memcpy(&format, content.data() + 4, sizeof(format));
        EXPECT_EQ(1U, format);
        EXPECT_EQ(content, decompose(c.schema, chain, UNIQUE_KEYS, true, c.bitmap, 3).serialize());
        // a backup taken before the last conditions: the bitmap marks and conditions above the base
        // version do not leak into any part
        if (chain.size() > 6) {
            std::vector<RowsetSharedPtr> prefix(chain.begin(), chain.begin() + 6);
            auto part = decompose(c.schema, prefix, UNIQUE_KEYS, true, c.bitmap);
            expect_compose_equals_direct(c.schema, prefix, UNIQUE_KEYS, true, c.bitmap, part,
                                         "prefix of seed " + std::to_string(seed));
            for (const auto& mark : part.marks) {
                EXPECT_LE(mark.mark_version, part.base_version);
            }
        }
    }
    EXPECT_TRUE(any_chain_with_conditions);
    EXPECT_TRUE(any_condition_mark) << "no DELETE condition killed a row, the test covers nothing";
}

TEST_F(RestoreDigestPrefixTest, MergeOnWriteConditionAndBitmapFirstOneWins) {
    // v2: keys 1..6 (v1 = 10 * key). v3: key 2 rewritten (the bitmap kills the old row at 3). v4: the condition
    // v1 <= 30 hits key 1, the old key 3 and the key 2 of v3 (the old key 2 is dead already). v5: key 3
    // rewritten, so the bitmap marks the old key 3 at 5, after the condition killed it at 4: it dies at 4.
    // v6: key 5 rewritten, the bitmap kills the old key 5 at 6; v7: key 4 rewritten. v8: the condition
    // v1 = 60 kills key 6.
    auto schema = make_schema(UNIQUE_KEYS, mow_cols(), 3);
    auto bitmap = std::make_shared<DeleteBitmap>(next_bitmap_tablet_id());
    auto rs2 = write_rowset(schema,
                            {mow_row({1, 10, 1, 0}), mow_row({2, 20, 1, 0}), mow_row({3, 30, 1, 0}),
                             mow_row({4, 40, 1, 0}), mow_row({5, 50, 1, 0}), mow_row({6, 60, 1, 0})},
                            2, 1000, false);
    auto rs3 = write_rowset(schema, {mow_row({2, 21, 3, 0})}, 3, 1000, false);
    bitmap->add({rs2->rowset_id(), 0, 3}, 1);
    auto rs4 = write_delete_rowset(schema, 4, {cond("v1", "<=", {"30"})});
    auto rs5 = write_rowset(schema, {mow_row({3, 31, 5, 0})}, 5, 1000, false);
    bitmap->add({rs2->rowset_id(), 0, 5}, 2); // the old key 3, which the condition killed at 4 already
    auto rs6 = write_rowset(schema, {mow_row({5, 51, 6, 0})}, 6, 1000, false);
    bitmap->add({rs2->rowset_id(), 0, 6}, 4);
    auto rs7 = write_rowset(schema, {mow_row({4, 41, 7, 0})}, 7, 1000, false);
    bitmap->add({rs2->rowset_id(), 0, 7}, 3);
    auto rs8 = write_delete_rowset(schema, 8, {cond("v1", "=", {"60"})});
    std::vector<RowsetSharedPtr> chain = {rs2, rs3, rs4, rs5, rs6, rs7, rs8};
    auto dec = decompose(schema, chain, UNIQUE_KEYS, true, bitmap);
    expect_compose_equals_direct(schema, chain, UNIQUE_KEYS, true, bitmap, dec, "hand made");
    auto rows_at = [&](int64_t v) {
        RestoreDigest d;
        EXPECT_TRUE(dec.compose(v, &d).ok());
        return d.rows;
    };
    EXPECT_EQ(6U, rows_at(2));
    EXPECT_EQ(6U, rows_at(3));
    EXPECT_EQ(3U, rows_at(4)); // keys 4, 5, 6
    EXPECT_EQ(4U, rows_at(5)); // + key 3 (v5)
    EXPECT_EQ(4U, rows_at(6));
    EXPECT_EQ(4U, rows_at(7));
    EXPECT_EQ(3U, rows_at(8)); // key 6 killed by the condition of v8
    // the old key 3 dies at 4 (the condition came first), not at 5; the old key 5 at 6, key 4 at 7
    std::map<int64_t, uint64_t> marked;
    for (const auto& mark : dec.marks) {
        for (const auto& b : mark.buckets) {
            marked[mark.mark_version] += b.count;
        }
    }
    EXPECT_EQ(3U, marked[4]); // keys 1 and 3 of v2, key 2 of v3
    EXPECT_EQ(1U, marked[3]);
    EXPECT_EQ(0U, marked.count(5));
    EXPECT_EQ(1U, marked[6]);
    EXPECT_EQ(1U, marked[7]);
    EXPECT_EQ(1U, marked[8]);
}

TEST_F(RestoreDigestPrefixTest, MergeOnWriteWithDeleteConditionsStaysComposableAfterCompaction) {
    for (unsigned seed : {121U, 122U, 123U, 124U, 125U, 126U}) {
        auto c = make_mow_chain(seed, 14, /*deletes=*/true);
        auto chain = c.chain();
        auto before = decompose(c.schema, chain, UNIQUE_KEYS, true, c.bitmap);
        std::map<int64_t, RestoreDigest> directs;
        expect_compose_equals_direct(c.schema, chain, UNIQUE_KEYS, true, c.bitmap, before, "before",
                                     &directs);
        // a run of batches without conditions: the cumulative compaction keeps the rows alive at
        // its end version, the conditions after it stay effective for them
        size_t a = 0;
        size_t b = 0;
        for (size_t i = 0; i + 1 < c.batches.size(); ++i) {
            if (!c.batches[i].rs->rowset_meta()->has_delete_predicate() &&
                !c.batches[i + 1].rs->rowset_meta()->has_delete_predicate()) {
                a = i;
                b = i + 1;
                while (b + 1 < c.batches.size() && b - a < 3 &&
                       !c.batches[b + 1].rs->rowset_meta()->has_delete_predicate()) {
                    ++b;
                }
                break;
            }
        }
        ASSERT_GT(b, a);
        MowChain once = compact_mow(c, a, b, seed);
        auto after_chain = once.chain();
        auto dec = decompose(once.schema, after_chain, UNIQUE_KEYS, true, once.bitmap);
        const std::string what = "seed " + std::to_string(seed);
        expect_compose_equals_direct(once.schema, after_chain, UNIQUE_KEYS, true, once.bitmap, dec,
                                     what);
        for (const auto& batch : once.batches) {
            RestoreDigest composed;
            ASSERT_TRUE(dec.compose(batch.version, &composed).ok());
            EXPECT_TRUE(same_digest(directs.at(batch.version), composed))
                    << what << " version " << batch.version;
        }
    }
}

// ---- unique merge on read

class RestoreDigestMorPrefixTest : public RestoreDigestPrefixTest {
protected:
    struct MorChain {
        TabletSchemaSPtr schema;
        TabletSharedPtr tablet;
        std::vector<RowsetSharedPtr> chain;
        std::vector<bool> is_delete;
    };

    static std::vector<ColSpec> mor_cols(bool has_seq) {
        std::vector<ColSpec> c = {{"k", "INT", true, false, 4},
                                  {"v1", "INT", false, true, 4},
                                  {"v2", "VARCHAR", false, true, 16}};
        if (has_seq) {
            c.push_back({SEQUENCE_COL, "INT", false, false, 4});
        }
        c.push_back({DELETE_SIGN, "TINYINT", false, false, 1});
        return c;
    }

    MorChain make_mor_chain(unsigned seed, int batches, bool has_seq, bool deletes) {
        std::mt19937 rng(seed);
        MorChain out;
        out.schema = make_schema(UNIQUE_KEYS, mor_cols(has_seq), has_seq ? 3 : -1);
        out.tablet = make_meta_tablet(UNIQUE_KEYS, out.schema);
        int64_t version = 2;
        for (int b = 0; b < batches; ++b) {
            if (deletes && b > 0 && rng() % 4 == 0) {
                std::vector<TCondition> conds = {cond("v1", "<", {std::to_string(100 + rng() % 300)})};
                out.chain.push_back(write_delete_rowset(out.schema, version++, conds));
                out.is_delete.push_back(true);
                continue;
            }
            const size_t n = rng() % 6 == 0 ? 0 : 1 + rng() % 20;
            std::set<int> used;
            std::vector<Row> rows;
            for (size_t i = 0; i < n; ++i) {
                const int k = static_cast<int>(rng() % 25);
                if (!used.insert(k).second) {
                    continue;
                }
                Row r = {V<int32_t>(k), V<int32_t>(static_cast<int32_t>(rng() % 1000)),
                         S("m" + std::to_string(rng() % 7))};
                if (has_seq) {
                    // a smaller sequence than the one written before loses against it
                    r.push_back(V<int32_t>(static_cast<int32_t>(1 + rng() % 30)));
                }
                r.push_back(V<int8_t>(rng() % 5 == 0 ? 1 : 0));
                rows.push_back(std::move(r));
            }
            static const size_t kSegRows[] = {3, 1000};
            out.chain.push_back(write_rowset(out.schema, rows, version++, kSegRows[rng() % 2],
                                             rng() % 3 == 0));
            out.is_delete.push_back(false);
        }
        return out;
    }

    RestoreDigestDecomposed decompose_mor(const MorChain& c, int threads = 1,
                                          const std::vector<RowsetSharedPtr>* chain = nullptr) {
        RestoreDigestInput in;
        in.rowsets = chain != nullptr ? *chain : c.chain;
        in.schema = c.schema;
        in.keys_type = UNIQUE_KEYS;
        in.enable_mow = false;
        in.version = in.rowsets.back()->end_version();
        in.tablet = c.tablet;
        in.threads = threads;
        RestoreDigestDecomposed d;
        Status st = decompose_restore_digest(in, kPrefixTabletId, &d);
        EXPECT_TRUE(st.ok()) << st;
        return d;
    }

    // The versions with a whole digest are the last `boundaries + 1` rowset ends, composing there is
    // the digest computed directly (also through the file), every other version is no boundary.
    void expect_mor_compose(const MorChain& c, const RestoreDigestDecomposed& dec, size_t boundaries,
                            const std::string& what,
                            const std::vector<RowsetSharedPtr>* chain_in = nullptr) {
        const std::vector<RowsetSharedPtr>& chain = chain_in != nullptr ? *chain_in : c.chain;
        EXPECT_TRUE(dec.mor) << what;
        EXPECT_FALSE(dec.mow) << what;
        EXPECT_TRUE(dec.marks.empty()) << what;
        ASSERT_EQ(chain.size(), dec.rowsets.size()) << what;
        const size_t expect_wholes = std::min(chain.size(), boundaries + 1);
        ASSERT_EQ(expect_wholes, dec.wholes.size()) << what;
        const std::string content = dec.serialize();
        uint32_t format = 0;
        std::memcpy(&format, content.data() + 4, sizeof(format));
        EXPECT_EQ(2U, format) << what;
        RestoreDigestDecomposed parsed;
        Status pst = RestoreDigestDecomposed::parse(content, dec.file_root(), kPrefixTabletId, &parsed);
        ASSERT_TRUE(pst.ok()) << pst << " " << what;
        EXPECT_EQ(content, parsed.serialize()) << what;
        for (size_t i = 0; i < chain.size(); ++i) {
            const int64_t v = chain[i]->end_version();
            const bool expect_boundary = i + expect_wholes >= chain.size();
            const RestoreDigestDecomposed* both[] = {&dec, &parsed};
            for (const RestoreDigestDecomposed* d : both) {
                EXPECT_EQ(expect_boundary, d->is_boundary(v)) << what << " version " << v;
                RestoreDigest composed;
                Status st = d->compose(v, &composed);
                if (!expect_boundary) {
                    EXPECT_TRUE(st.is<ErrorCode::NOT_IMPLEMENTED_ERROR>()) << what << " " << st;
                    continue;
                }
                ASSERT_TRUE(st.ok()) << st << " " << what << " version " << v;
                std::vector<RowsetSharedPtr> prefix(chain.begin(), chain.begin() + i + 1);
                RestoreDigest direct = mor_digest(c.schema, prefix, c.tablet, v);
                EXPECT_TRUE(same_digest(direct, composed)) << what << " version " << v;
                EXPECT_EQ(direct.root, composed.root) << what << " version " << v;
                EXPECT_EQ(direct.rows, composed.rows) << what << " version " << v;
                EXPECT_EQ(direct.schema_sig, composed.schema_sig) << what;
            }
        }
        RestoreDigest base;
        ASSERT_TRUE(dec.compose(dec.base_version, &base).ok());
        EXPECT_EQ(base.root, dec.root) << what;
        EXPECT_EQ(base.rows, dec.rows) << what;
        // a version in the middle of nowhere
        RestoreDigest none;
        EXPECT_TRUE(dec.compose(0, &none).is<ErrorCode::NOT_IMPLEMENTED_ERROR>());
        EXPECT_TRUE(dec.compose(dec.base_version + 1, &none).is<ErrorCode::NOT_IMPLEMENTED_ERROR>());
    }
};

TEST_F(RestoreDigestMorPrefixTest, WholeDigestsAtTheRecentBoundariesEqualDirect) {
    for (bool has_seq : {true, false}) {
        for (bool deletes : {false, true}) {
            for (unsigned seed : {201U, 202U, 203U, 204U}) {
                auto c = make_mor_chain(seed, 9, has_seq, deletes);
                const std::string what = std::string(has_seq ? "seq " : "noseq ") +
                                         (deletes ? "deletes " : "") + "seed " + std::to_string(seed);
                auto dec = decompose_mor(c);
                expect_mor_compose(c, dec, 3, what);
                // the worker threads do not change a byte
                EXPECT_EQ(dec.serialize(), decompose_mor(c, 4).serialize()) << what;
                // another number of boundaries
                for (int32_t k : {1, 2, 20}) {
                    const int32_t saved = config::restore_digest_mor_prefix_boundaries;
                    config::restore_digest_mor_prefix_boundaries = k;
                    auto other = decompose_mor(c);
                    config::restore_digest_mor_prefix_boundaries = saved;
                    expect_mor_compose(c, other, static_cast<size_t>(k), what + " k " + std::to_string(k));
                    EXPECT_EQ(dec.root, other.root);
                }
            }
        }
    }
}

TEST_F(RestoreDigestMorPrefixTest, StaysComposableAfterCompaction) {
    for (bool has_seq : {true, false}) {
        for (unsigned seed : {211U, 212U, 213U}) {
            auto c = make_mor_chain(seed, 9, has_seq, /*deletes=*/false);
            auto before = decompose_mor(c);
            // the digest of the last version is the same after any compaction, and the versions which
            // are still among the last boundaries keep their digests
            std::map<int64_t, RestoreDigest> expect;
            for (const auto& w : before.wholes) {
                RestoreDigest d;
                ASSERT_TRUE(before.compose(w.version, &d).ok());
                expect[w.version] = d;
            }
            auto merged = compact(c.schema, c.tablet, {c.chain[2], c.chain[3], c.chain[4]},
                                  Version(c.chain[2]->start_version(), c.chain[4]->end_version()),
                                  ReaderType::READER_CUMULATIVE_COMPACTION);
            ASSERT_NE(nullptr, merged);
            std::vector<RowsetSharedPtr> chain(c.chain.begin(), c.chain.begin() + 2);
            chain.push_back(merged);
            chain.insert(chain.end(), c.chain.begin() + 5, c.chain.end());
            auto dec = decompose_mor(c, 1, &chain);
            const std::string what = std::string(has_seq ? "seq " : "noseq ") + "seed " + std::to_string(seed);
            expect_mor_compose(c, dec, 3, what, &chain);
            EXPECT_EQ(before.root, dec.root) << what;
            for (const auto& w : dec.wholes) {
                auto it = expect.find(w.version);
                if (it == expect.end()) {
                    continue;
                }
                RestoreDigest d;
                ASSERT_TRUE(dec.compose(w.version, &d).ok());
                EXPECT_TRUE(same_digest(it->second, d)) << what << " version " << w.version;
            }
        }
    }
}

TEST_F(RestoreDigestMorPrefixTest, OldFilesStillParse) {
    // a duplicate and a MoW file are format version 1, as before, and a MoR file does not parse as
    // one of them: format 2 with the whole digests is a different file
    auto dup = make_dup_chain(251, 5, true);
    auto dd = decompose(dup.schema, dup.chain, DUP_KEYS, false, nullptr);
    std::string content = dd.serialize();
    uint32_t format = 0;
    std::memcpy(&format, content.data() + 4, sizeof(format));
    EXPECT_EQ(1U, format);
    RestoreDigestDecomposed out;
    EXPECT_TRUE(RestoreDigestDecomposed::parse(content, dd.file_root(), kPrefixTabletId, &out).ok());
    EXPECT_FALSE(out.mor);
    EXPECT_TRUE(out.wholes.empty());
    // a version 1 file with the version field changed to 2 lacks the section: rejected
    auto sha = [](const std::string& data) {
        SHA256Digest d;
        d.reset(data.data(), data.size());
        return std::string(d.digest());
    };
    std::string bad = content;
    bad[4] = 2;
    EXPECT_TRUE(RestoreDigestDecomposed::parse(bad, sha(bad), kPrefixTabletId, &out)
                        .is<ErrorCode::RESTORE_MANIFEST_MISMATCH>());
    // an unknown format version
    bad[4] = 3;
    EXPECT_TRUE(RestoreDigestDecomposed::parse(bad, sha(bad), kPrefixTabletId, &out)
                        .is<ErrorCode::RESTORE_MANIFEST_MISMATCH>());
    // a MoR file damaged in the whole digest section
    auto c = make_mor_chain(252, 5, true, false);
    auto dec = decompose_mor(c);
    content = dec.serialize();
    EXPECT_TRUE(RestoreDigestDecomposed::parse(content, dec.file_root(), kPrefixTabletId, &out).ok());
    EXPECT_TRUE(out.mor);
    EXPECT_EQ(dec.wholes.size(), out.wholes.size());
    for (std::string damaged : {content.substr(0, content.size() - 5), content + "xx"}) {
        EXPECT_TRUE(RestoreDigestDecomposed::parse(damaged, sha(damaged), kPrefixTabletId, &out)
                            .is<ErrorCode::RESTORE_MANIFEST_MISMATCH>());
    }
    // a whole digest at a version which is not the end of a rowset
    auto forged = dec;
    forged.wholes.front().version += 100000;
    std::sort(forged.wholes.begin(), forged.wholes.end(),
              [](const auto& a, const auto& b) { return a.version < b.version; });
    const std::string fc = forged.serialize();
    EXPECT_TRUE(RestoreDigestDecomposed::parse(fc, sha(fc), kPrefixTabletId, &out)
                        .is<ErrorCode::RESTORE_MANIFEST_MISMATCH>());
}

TEST_F(RestoreDigestPrefixTest, DeleteSignRowsAndTheirMarksAreNotCounted) {
    auto schema = make_schema(UNIQUE_KEYS, mow_cols(), 3);
    auto bitmap = std::make_shared<DeleteBitmap>(next_bitmap_tablet_id());
    // v2: keys 1..4. v3: key 2 deleted (delete sign), key 3 updated. v4: key 2 written again, and
    // the delete sign row of v3 is marked (or not, both are legal), key 3 deleted.
    auto rs2 = write_rowset(schema,
                            {mow_row({1, 10, 1, 0}), mow_row({2, 20, 1, 0}), mow_row({3, 30, 1, 0}),
                             mow_row({4, 40, 1, 0})},
                            2, 1000, false);
    auto rs3 = write_rowset(schema, {mow_row({2, 0, 3, 1}), mow_row({3, 31, 3, 0})}, 3, 1000, false);
    bitmap->add({rs2->rowset_id(), 0, 3}, 1); // key 2
    bitmap->add({rs2->rowset_id(), 0, 3}, 2); // key 3
    auto rs4 = write_rowset(schema, {mow_row({2, 21, 4, 0}), mow_row({3, 0, 4, 1})}, 4, 1000, false);
    bitmap->add({rs3->rowset_id(), 0, 4}, 0); // the delete sign row of key 2
    bitmap->add({rs3->rowset_id(), 0, 4}, 1); // key 3 of v3
    std::vector<RowsetSharedPtr> chain = {rs2, rs3, rs4};
    auto dec = decompose(schema, chain, UNIQUE_KEYS, true, bitmap);
    // rowset parts hold the rows without the delete sign: 4 + 1 + 1
    uint64_t counted = 0;
    for (const auto& rs : dec.rowsets) {
        for (const auto& b : rs.buckets) {
            counted += b.count;
        }
    }
    EXPECT_EQ(6U, counted);
    // marks: v3 kills keys 2 and 3 of v2; v4 kills only key 3 of v3 (the delete sign row is not counted)
    ASSERT_EQ(2U, dec.marks.size());
    EXPECT_EQ(3, dec.marks[0].mark_version);
    EXPECT_EQ(4, dec.marks[1].mark_version);
    auto marked_rows = [](const RestoreDigestMarkPart& m) {
        uint64_t n = 0;
        for (const auto& b : m.buckets) {
            n += b.count;
        }
        return n;
    };
    EXPECT_EQ(2U, marked_rows(dec.marks[0]));
    EXPECT_EQ(1U, marked_rows(dec.marks[1]));
    expect_compose_equals_direct(schema, chain, UNIQUE_KEYS, true, bitmap, dec, "delete sign");
    RestoreDigest at4;
    ASSERT_TRUE(dec.compose(4, &at4).ok());
    EXPECT_EQ(3U, at4.rows); // keys 1 and 4 of v2, key 2 of v4
}

TEST_F(RestoreDigestPrefixTest, UnsupportedCasesAndBadVersions) {
    // unique merge on read needs the tablet to merge with, and the number of boundaries must be > 0
    {
        auto schema = make_schema(UNIQUE_KEYS, mow_cols(), 3);
        auto rs = write_rowset(schema, {mow_row({1, 1, 1, 0})}, 2, 100, false);
        RestoreDigestInput in;
        in.rowsets = {rs};
        in.schema = schema;
        in.keys_type = UNIQUE_KEYS;
        in.enable_mow = false;
        in.version = 2;
        RestoreDigestDecomposed d;
        Status st = decompose_restore_digest(in, kPrefixTabletId, &d);
        EXPECT_TRUE(st.is<ErrorCode::INVALID_ARGUMENT>()) << st;
        in.tablet = make_meta_tablet(UNIQUE_KEYS, schema);
        EXPECT_TRUE(decompose_restore_digest(in, kPrefixTabletId, &d).ok());
        const int32_t saved = config::restore_digest_mor_prefix_boundaries;
        config::restore_digest_mor_prefix_boundaries = 0;
        st = decompose_restore_digest(in, kPrefixTabletId, &d);
        config::restore_digest_mor_prefix_boundaries = saved;
        EXPECT_TRUE(st.is<ErrorCode::NOT_IMPLEMENTED_ERROR>()) << st;
        EXPECT_NE(std::string::npos, st.to_string().find("disabled")) << st;
    }
    // unique merge on write with a DELETE condition needs extra scans, and gives up above the limit
    {
        auto c = make_mow_chain(63, 12, /*deletes=*/true);
        const int32_t saved = config::restore_digest_prefix_max_scans;
        config::restore_digest_prefix_max_scans = 0;
        RestoreDigestInput in;
        in.rowsets = c.chain();
        in.schema = c.schema;
        in.keys_type = UNIQUE_KEYS;
        in.enable_mow = true;
        in.delete_bitmap = c.bitmap;
        in.version = in.rowsets.back()->end_version();
        RestoreDigestDecomposed d;
        Status st = decompose_restore_digest(in, kPrefixTabletId, &d);
        config::restore_digest_prefix_max_scans = saved;
        EXPECT_TRUE(st.is<ErrorCode::NOT_IMPLEMENTED_ERROR>()) << st;
        EXPECT_NE(std::string::npos, st.to_string().find("extra scans")) << st;
    }
    // too many extra scans
    {
        auto c = make_dup_chain(61, 12, /*deletes=*/true);
        int deletes = 0;
        for (bool is_delete : c.is_delete) {
            deletes += is_delete ? 1 : 0;
        }
        ASSERT_GT(deletes, 0);
        const int32_t saved = config::restore_digest_prefix_max_scans;
        config::restore_digest_prefix_max_scans = 1;
        RestoreDigestInput in;
        in.rowsets = c.chain;
        in.schema = c.schema;
        in.keys_type = DUP_KEYS;
        in.version = c.chain.back()->end_version();
        RestoreDigestDecomposed d;
        Status st = decompose_restore_digest(in, kPrefixTabletId, &d);
        config::restore_digest_prefix_max_scans = saved;
        EXPECT_TRUE(st.is<ErrorCode::NOT_IMPLEMENTED_ERROR>()) << st;
        EXPECT_NE(std::string::npos, st.to_string().find("extra scans")) << st;
    }
    // the base version must be the end of the last rowset
    {
        auto c = make_dup_chain(62, 3, false);
        RestoreDigestInput in;
        in.rowsets = c.chain;
        in.schema = c.schema;
        in.keys_type = DUP_KEYS;
        in.version = c.chain.back()->end_version() + 1;
        RestoreDigestDecomposed d;
        EXPECT_TRUE(decompose_restore_digest(in, kPrefixTabletId, &d).is<ErrorCode::INVALID_ARGUMENT>());
    }
}

TEST_F(RestoreDigestPrefixTest, FileIsCheckedAgainstItsRoot) {
    auto c = make_dup_chain(71, 6, true);
    auto dec = decompose(c.schema, c.chain, DUP_KEYS, false, nullptr);
    const std::string content = dec.serialize();
    const std::string root = dec.file_root();
    EXPECT_EQ(64U, root.size());
    RestoreDigestDecomposed out;
    EXPECT_TRUE(RestoreDigestDecomposed::parse(content, root, kPrefixTabletId, &out).ok());
    // wrong root, wrong tablet
    EXPECT_TRUE(RestoreDigestDecomposed::parse(content, std::string(64, '0'), kPrefixTabletId, &out)
                        .is<ErrorCode::RESTORE_MANIFEST_MISMATCH>());
    EXPECT_TRUE(RestoreDigestDecomposed::parse(content, root, kPrefixTabletId + 1, &out)
                        .is<ErrorCode::RESTORE_MANIFEST_MISMATCH>());
    // a flipped byte, a truncated file, trailing bytes: the root catches them, and a file whose root
    // was recomputed over bad content is rejected by the parser
    auto sha = [](const std::string& data) {
        SHA256Digest d;
        d.reset(data.data(), data.size());
        return std::string(d.digest());
    };
    for (size_t pos : {size_t(0), size_t(5), content.size() / 2, content.size() - 1}) {
        std::string bad = content;
        bad[pos] = static_cast<char>(bad[pos] ^ 0x5A);
        EXPECT_TRUE(RestoreDigestDecomposed::parse(bad, root, kPrefixTabletId, &out)
                            .is<ErrorCode::RESTORE_MANIFEST_MISMATCH>());
        Status st = RestoreDigestDecomposed::parse(bad, sha(bad), kPrefixTabletId, &out);
        // a flip inside a bucket value is a valid file with other numbers; anything structural is not
        if (!st.ok()) {
            EXPECT_TRUE(st.is<ErrorCode::RESTORE_MANIFEST_MISMATCH>()) << st;
        }
    }
    for (std::string bad : {content.substr(0, content.size() - 3), content + "x", std::string(),
                            content.substr(0, 10)}) {
        EXPECT_TRUE(RestoreDigestDecomposed::parse(bad, sha(bad), kPrefixTabletId, &out)
                            .is<ErrorCode::RESTORE_MANIFEST_MISMATCH>());
    }
    // sparse: a rowset with few rows takes few bytes
    EXPECT_LT(content.size(), 6 * (RestoreDigest::kNumBuckets * 26 + 64) + 4096);
}

// A digest of a different shape (complex columns, a wide schema) decomposes the same way.
TEST_F(RestoreDigestPrefixTest, WideSchemaComposes) {
    auto schema = make_schema(DUP_KEYS, wide_cols());
    std::vector<RowsetSharedPtr> chain;
    int next = 0;
    for (int64_t v = 2; v <= 5; ++v) {
        std::vector<Row> rows;
        for (int i = 0; i < 40; ++i) {
            rows.push_back(wide_row(next++ % 90));
        }
        chain.push_back(write_rowset(schema, rows, v, 17, v % 2 == 0));
    }
    auto dec = decompose(schema, chain, DUP_KEYS, false, nullptr, 2);
    expect_compose_equals_direct(schema, chain, DUP_KEYS, false, nullptr, dec, "wide");
}

// ===== 13. decomposed digest of real tablets ==================================================

TEST_F(RestoreDigestTabletTest, DecomposedDigestOfMowAndDuplicateTablets) {
    _next_id = 910000; // not the ids of another test, see the segment cache
    // unique merge on write: the scenario of the partial update test, with versions 2..4
    auto mow = create_tablet(7201, true);
    ASSERT_NE(nullptr, mow);
    auto schema = mow->tablet_schema();
    auto rs2 = write(mow, schema, 2, {{{1, 10}, {2, 20}, {3, 30}, {4, 40}, {5, 50}, {6, 60}}}, false, true);
    auto rs3 = write(mow, schema, 3, {{{3, 31}, {5, 51}}, {{3, 32}}}, true, true);
    auto& bitmap = mow->tablet_meta()->delete_bitmap();
    bitmap.add({rs2->rowset_id(), 0, 3}, 2);
    bitmap.add({rs2->rowset_id(), 0, 3}, 4);
    bitmap.add({rs3->rowset_id(), 0, 3}, 0);
    auto rs4 = write(mow, schema, 4, {{{1, 0, 1}, {6, 61}}}, false, true);
    bitmap.add({rs2->rowset_id(), 0, 4}, 0);
    bitmap.add({rs2->rowset_id(), 0, 4}, 5);
    (void)rs4;

    RestoreDigestDecomposed dec;
    ASSERT_TRUE(compute_tablet_restore_digest_decomposed(*_engine, 7201, 4, 2, &dec).ok());
    EXPECT_EQ(7201, dec.tablet_id);
    EXPECT_EQ(4, dec.base_version);
    EXPECT_TRUE(dec.mow);
    for (int64_t v : {int64_t(1), int64_t(2), int64_t(3), int64_t(4)}) {
        RestoreDigest composed;
        ASSERT_TRUE(dec.compose(v, &composed).ok()) << v;
        EXPECT_TRUE(digests_equal(tablet_digest(7201, v), composed)) << "version " << v;
    }
    // the base version of the backup may be below the head of the tablet
    RestoreDigestDecomposed at3;
    ASSERT_TRUE(compute_tablet_restore_digest_decomposed(*_engine, 7201, 3, 1, &at3).ok());
    EXPECT_EQ(3, at3.base_version);
    RestoreDigest c3;
    RestoreDigest c3_of_head;
    ASSERT_TRUE(at3.compose(3, &c3).ok());
    ASSERT_TRUE(dec.compose(3, &c3_of_head).ok());
    EXPECT_TRUE(digests_equal(c3_of_head, c3));
    EXPECT_FALSE(at3.is_boundary(4));

    // duplicate with a DELETE condition
    auto dup = create_tablet(7202, false);
    ASSERT_NE(nullptr, dup);
    auto dschema = dup->tablet_schema();
    std::vector<TabRow> a;
    std::vector<TabRow> b;
    for (int k = 0; k < 100; ++k) {
        a.push_back({k, k % 10});
    }
    for (int k = 50; k < 150; ++k) {
        b.push_back({k, (k * 7) % 10});
    }
    write(dup, dschema, 2, {a}, false, true);
    write(dup, dschema, 3, {b}, false, true);
    write_delete(dup, 4, {cond("v", "=", "3")});
    write(dup, dschema, 5, {{{500, 3}, {501, 4}}}, false, true);
    write_delete(dup, 6, {cond("k", "<", "20")});
    RestoreDigestDecomposed ddec;
    ASSERT_TRUE(compute_tablet_restore_digest_decomposed(*_engine, 7202, 6, 3, &ddec).ok());
    EXPECT_FALSE(ddec.mow);
    ASSERT_EQ(2U, ddec.marks.size());
    EXPECT_EQ(4, ddec.marks[0].mark_version);
    EXPECT_EQ(6, ddec.marks[1].mark_version);
    for (int64_t v : {int64_t(1), int64_t(2), int64_t(3), int64_t(4), int64_t(5), int64_t(6)}) {
        RestoreDigest composed;
        ASSERT_TRUE(ddec.compose(v, &composed).ok()) << v;
        EXPECT_TRUE(digests_equal(tablet_digest(7202, v), composed)) << "version " << v;
    }

    // unique merge on write, then DELETE conditions among the loads
    write_delete(mow, 5, {cond("v", "=", "20")});
    write(mow, schema, 6, {{{2, 22}, {7, 70}}}, false, true);
    bitmap.add({rs2->rowset_id(), 0, 6}, 1); // the old key 2, which the condition killed at 5 already
    write_delete(mow, 7, {cond("k", ">=", "6")});
    RestoreDigestDecomposed cdec;
    ASSERT_TRUE(compute_tablet_restore_digest_decomposed(*_engine, 7201, 7, 2, &cdec).ok());
    EXPECT_TRUE(cdec.mow);
    for (int64_t v = 2; v <= 7; ++v) {
        RestoreDigest composed;
        ASSERT_TRUE(cdec.compose(v, &composed).ok()) << v;
        EXPECT_TRUE(digests_equal(tablet_digest(7201, v), composed)) << "MoW with conditions, version " << v;
    }
    {
        std::set<int64_t> mark_versions;
        for (const auto& mark : cdec.marks) {
            mark_versions.insert(mark.mark_version);
        }
        EXPECT_TRUE(mark_versions.contains(5));
        EXPECT_TRUE(mark_versions.contains(7));
    }

    // merge on read: whole digests at the base version and the 3 boundaries below
    auto mor = create_tablet(7203, /*mow=*/false, /*unique=*/true);
    ASSERT_NE(nullptr, mor);
    auto mschema = mor->tablet_schema();
    write(mor, mschema, 2, {{{1, 10}, {2, 20}, {3, 30}}}, false, true);
    write(mor, mschema, 3, {{{2, 21}}}, false, true);
    write(mor, mschema, 4, {{{1, 0, 1}, {4, 40}}}, false, true);
    write_delete(mor, 5, {cond("v", "=", "30")});
    write(mor, mschema, 6, {{{3, 33}, {5, 50}}}, false, true);
    RestoreDigestDecomposed mdec;
    ASSERT_TRUE(compute_tablet_restore_digest_decomposed(*_engine, 7203, 6, 2, &mdec).ok());
    EXPECT_TRUE(mdec.mor);
    EXPECT_FALSE(mdec.mow);
    ASSERT_EQ(4U, mdec.wholes.size());
    EXPECT_EQ(3, mdec.wholes.front().version);
    EXPECT_EQ(6, mdec.wholes.back().version);
    for (int64_t v = 2; v <= 6; ++v) {
        RestoreDigest composed;
        Status cst = mdec.compose(v, &composed);
        if (v == 2) {
            EXPECT_TRUE(cst.is<ErrorCode::NOT_IMPLEMENTED_ERROR>()) << cst;
            continue;
        }
        ASSERT_TRUE(cst.ok()) << v << " " << cst;
        EXPECT_TRUE(digests_equal(tablet_digest(7203, v), composed)) << "MoR, version " << v;
    }
}

TEST_F(RestoreDigestTabletTest, LogicalDigestComesFromTheDecomposedDigest) {
    _next_id = 920000; // not the ids of another test, see the segment cache
    auto tablet = create_tablet(7211, false);
    ASSERT_NE(nullptr, tablet);
    auto schema = tablet->tablet_schema();
    write(tablet, schema, 2, {{{1, 10}, {2, 20}, {3, 30}}}, false, true);
    write(tablet, schema, 3, {{{4, 40}, {5, 50}}}, false, true);
    RestoreDigest direct = tablet_digest(7211, 3);

    // a miss: one pass, the digest is composed from the parts and cached
    RestoreDigestCache cache(/*capacity=*/16);
    LogicalDigestResult first;
    std::shared_ptr<const RestoreDigestDecomposed> dec;
    Status dec_status = Status::InternalError("not set");
    ASSERT_TRUE(get_tablet_logical_digest(*_engine, 7211, 3, 2, &cache, &first, &dec, &dec_status).ok());
    EXPECT_TRUE(dec_status.ok()) << dec_status;
    EXPECT_FALSE(first.from_cache);
    EXPECT_EQ(direct.root, first.root);
    EXPECT_EQ(direct.schema_sig, first.schema_sig);
    EXPECT_EQ(direct.rows, first.rows);
    EXPECT_EQ(1U, cache.size());
    EXPECT_EQ(first.root, dec->root);
    ASSERT_NE(nullptr, dec);
    EXPECT_EQ(3, dec->base_version);

    // a hit with the same root: the decomposed digest comes from the cache as well, nothing is scanned
    const uint64_t scans_before = restore_digest_scan_count();
    LogicalDigestResult second;
    std::shared_ptr<const RestoreDigestDecomposed> dec2;
    dec_status = Status::InternalError("not set");
    ASSERT_TRUE(get_tablet_logical_digest(*_engine, 7211, 3, 1, &cache, &second, &dec2, &dec_status).ok());
    EXPECT_TRUE(dec_status.ok()) << dec_status;
    EXPECT_TRUE(second.from_cache);
    ASSERT_NE(nullptr, dec2);
    EXPECT_EQ(scans_before, restore_digest_scan_count());
    EXPECT_EQ(dec.get(), dec2.get());
    EXPECT_EQ(dec->serialize(), dec2->serialize());

    // without asking for it: nothing changes
    LogicalDigestResult plain;
    ASSERT_TRUE(get_tablet_logical_digest(*_engine, 7211, 3, 1, &cache, &plain).ok());
    EXPECT_EQ(direct.root, plain.root);

    // the cache holds another root for the key: the recomputed digest wins and replaces the entry
    RestoreDigestCache poisoned(/*capacity=*/16);
    const RestoreDigestCache::Key key = tablet_cache_key(*_engine, 7211, 3, direct.schema_sig);
    LogicalDigestResult bogus;
    bogus.schema_sig = direct.schema_sig;
    bogus.root = std::string(64, 'f');
    bogus.rows = 5;
    poisoned.insert(key, bogus);
    LogicalDigestResult got;
    std::shared_ptr<const RestoreDigestDecomposed> dec3;
    dec_status = Status::InternalError("not set");
    ASSERT_TRUE(get_tablet_logical_digest(*_engine, 7211, 3, 1, &poisoned, &got, &dec3, &dec_status).ok());
    EXPECT_TRUE(dec_status.ok()) << dec_status;
    ASSERT_NE(nullptr, dec3);
    EXPECT_FALSE(got.from_cache);
    EXPECT_EQ(direct.root, got.root);
    EXPECT_EQ(1U, poisoned.stale_count());

    // merge on read: the digest is the whole digest at the base version of the decomposed digest
    auto mor = create_tablet(7212, /*mow=*/false, /*unique=*/true);
    ASSERT_NE(nullptr, mor);
    write(mor, mor->tablet_schema(), 2, {{{1, 10}, {2, 20}}}, false, true);
    write(mor, mor->tablet_schema(), 3, {{{2, 21}}}, false, true);
    LogicalDigestResult mor_result;
    std::shared_ptr<const RestoreDigestDecomposed> mor_dec;
    dec_status = Status::InternalError("not set");
    ASSERT_TRUE(get_tablet_logical_digest(*_engine, 7212, 3, 1, nullptr, &mor_result, &mor_dec, &dec_status).ok());
    EXPECT_TRUE(dec_status.ok()) << dec_status;
    ASSERT_NE(nullptr, mor_dec);
    EXPECT_TRUE(mor_dec->mor);
    // the empty rowset of version 1 that the tablet is created with, and the two loads
    EXPECT_EQ(3U, mor_dec->rowsets.size());
    EXPECT_EQ(3U, mor_dec->wholes.size());
    EXPECT_EQ(tablet_digest(7212, 3).root, mor_result.root);
    EXPECT_EQ(mor_result.root, mor_dec->root);
    EXPECT_EQ(2U, mor_result.rows);

    // the number of boundaries 0 turns the decomposed digest of merge on read off, the digest stays
    const int32_t saved = config::restore_digest_mor_prefix_boundaries;
    config::restore_digest_mor_prefix_boundaries = 0;
    LogicalDigestResult off_result;
    std::shared_ptr<const RestoreDigestDecomposed> off_dec;
    dec_status = Status::OK();
    Status off_st = get_tablet_logical_digest(*_engine, 7212, 3, 1, nullptr, &off_result, &off_dec, &dec_status);
    config::restore_digest_mor_prefix_boundaries = saved;
    ASSERT_TRUE(off_st.ok()) << off_st;
    EXPECT_TRUE(dec_status.is<ErrorCode::NOT_IMPLEMENTED_ERROR>()) << dec_status;
    EXPECT_EQ(nullptr, off_dec);
    EXPECT_EQ(mor_result.root, off_result.root);
}

// ===== the decomposed digest in the cache =====================================================

TEST_F(RestoreDigestTabletTest, HitTakesBothDigestsFromTheCacheWithoutScanning) {
    _next_id = 940000; // not the ids of another test, see the segment cache
    ConfigRestore restore;
    config::restore_digest_cache_decomposed_mb = 256;

    // duplicate with DELETE conditions
    auto dup = create_tablet(7231, false);
    ASSERT_NE(nullptr, dup);
    auto dschema = dup->tablet_schema();
    std::vector<TabRow> a;
    std::vector<TabRow> b;
    for (int k = 0; k < 100; ++k) {
        a.push_back({k, k % 10});
    }
    for (int k = 50; k < 150; ++k) {
        b.push_back({k, (k * 7) % 10});
    }
    write(dup, dschema, 2, {a}, false, true);
    write(dup, dschema, 3, {b}, false, true);
    write_delete(dup, 4, {cond("v", "=", "3")});
    write(dup, dschema, 5, {{{500, 3}, {501, 4}}}, false, true);

    // unique merge on write with a DELETE condition
    auto mow = create_tablet(7232, true);
    ASSERT_NE(nullptr, mow);
    auto mschema = mow->tablet_schema();
    auto rs2 = write(mow, mschema, 2, {{{1, 10}, {2, 20}, {3, 30}, {4, 40}, {5, 50}, {6, 60}}}, false, true);
    auto rs3 = write(mow, mschema, 3, {{{3, 31}, {5, 51}}, {{3, 32}}}, true, true);
    auto& bitmap = mow->tablet_meta()->delete_bitmap();
    bitmap.add({rs2->rowset_id(), 0, 3}, 2);
    bitmap.add({rs2->rowset_id(), 0, 3}, 4);
    bitmap.add({rs3->rowset_id(), 0, 3}, 0);
    write(mow, mschema, 4, {{{1, 0, 1}, {6, 61}}}, false, true);
    bitmap.add({rs2->rowset_id(), 0, 4}, 0);
    bitmap.add({rs2->rowset_id(), 0, 4}, 5);
    write_delete(mow, 5, {cond("v", "=", "20")});

    // unique merge on read
    auto mor = create_tablet(7233, false, true);
    ASSERT_NE(nullptr, mor);
    write(mor, mor->tablet_schema(), 2, {{{1, 10}, {2, 20}}}, false, true);
    write(mor, mor->tablet_schema(), 3, {{{2, 21}, {3, 30}}}, false, true);
    write(mor, mor->tablet_schema(), 4, {{{3, 31}}}, false, true);

    struct Case {
        int64_t tablet_id;
        int64_t version;
        const char* name;
    };
    for (const Case& c : {Case {7231, 5, "duplicate"}, Case {7232, 5, "mow"}, Case {7233, 4, "mor"}}) {
        SCOPED_TRACE(c.name);
        RestoreDigestCache cache(/*capacity=*/16);
        CachedDigest first = get_cached(*_engine, &cache, c.tablet_id, c.version);
        ASSERT_TRUE(first.status.ok()) << first.status;
        ASSERT_TRUE(first.decomposed_status.ok()) << first.decomposed_status;
        ASSERT_NE(nullptr, first.decomposed);
        EXPECT_FALSE(first.result.from_cache);
        EXPECT_FALSE(first.decomposed_from_cache);
        EXPECT_EQ(1U, first.scans);
        EXPECT_EQ(1U, cache.decomposed_count());
        EXPECT_GT(cache.decomposed_bytes(), 0);

        for (int i = 0; i < 2; ++i) {
            CachedDigest again = get_cached(*_engine, &cache, c.tablet_id, c.version);
            ASSERT_TRUE(again.status.ok()) << again.status;
            ASSERT_TRUE(again.decomposed_status.ok()) << again.decomposed_status;
            ASSERT_NE(nullptr, again.decomposed);
            EXPECT_EQ(0U, again.scans);
            EXPECT_TRUE(again.result.from_cache);
            EXPECT_TRUE(again.decomposed_from_cache);
            EXPECT_EQ(first.result.root, again.result.root);
            EXPECT_EQ(first.result.rows, again.result.rows);
            EXPECT_EQ(first.decomposed.get(), again.decomposed.get());
            // the same bytes as a new computation gives
            RestoreDigestDecomposed fresh;
            ASSERT_TRUE(compute_tablet_restore_digest_decomposed(*_engine, c.tablet_id, c.version, 2,
                                                                 &fresh)
                                .ok());
            EXPECT_EQ(fresh.serialize(), again.decomposed->serialize());
            EXPECT_EQ(tablet_digest(c.tablet_id, c.version).root, again.result.root);
        }
        EXPECT_EQ(2U, cache.decomposed_hits());
        EXPECT_EQ(1U, cache.decomposed_misses());

        // not asking for the decomposed digest: the whole digest from the cache, the decomposed
        // part is left alone
        CachedDigest plain = get_cached(*_engine, &cache, c.tablet_id, c.version, false);
        ASSERT_TRUE(plain.status.ok());
        EXPECT_EQ(0U, plain.scans);
        EXPECT_TRUE(plain.result.from_cache);
        EXPECT_EQ(1U, cache.decomposed_count());
        EXPECT_EQ(2U, cache.decomposed_hits());
    }

    // a cache which is not asked for the decomposed digest does not get one
    RestoreDigestCache plain_cache(/*capacity=*/16);
    CachedDigest only_whole = get_cached(*_engine, &plain_cache, 7231, 5, false);
    ASSERT_TRUE(only_whole.status.ok());
    EXPECT_EQ(1U, plain_cache.size());
    EXPECT_EQ(0U, plain_cache.decomposed_count());
    EXPECT_EQ(0, plain_cache.decomposed_bytes());
    // and a later request for it scans once, then it is there
    CachedDigest late = get_cached(*_engine, &plain_cache, 7231, 5);
    ASSERT_TRUE(late.decomposed_status.ok()) << late.decomposed_status;
    ASSERT_NE(nullptr, late.decomposed);
    EXPECT_TRUE(late.result.from_cache);
    EXPECT_FALSE(late.decomposed_from_cache);
    EXPECT_EQ(1U, late.scans);
    EXPECT_EQ(only_whole.result.root, late.result.root);
    EXPECT_EQ(1U, plain_cache.decomposed_count());
    EXPECT_EQ(0U, get_cached(*_engine, &plain_cache, 7231, 5).scans);
}

TEST_F(RestoreDigestTabletTest, CompactionRecomputesTheWholeDigestAndTheDecomposedOne) {
    _next_id = 941000; // not the ids of another test, see the segment cache
    ConfigRestore restore;
    config::restore_digest_cache_decomposed_mb = 256;
    auto tablet = create_tablet(7241, false);
    ASSERT_NE(nullptr, tablet);
    auto schema = tablet->tablet_schema();
    std::vector<TabRow> a;
    std::vector<TabRow> b;
    for (int k = 0; k < 100; ++k) {
        a.push_back({k, k % 10});
    }
    for (int k = 50; k < 150; ++k) {
        b.push_back({k, (k * 7) % 10});
    }
    write(tablet, schema, 2, {a}, false, true);
    write(tablet, schema, 3, {b}, false, true);
    write(tablet, schema, 4, {{{500, 3}, {501, 4}}}, false, true);

    RestoreDigestCache cache(/*capacity=*/16);
    CachedDigest first = get_cached(*_engine, &cache, 7241, 4);
    ASSERT_TRUE(first.decomposed_status.ok()) << first.decomposed_status;
    ASSERT_NE(nullptr, first.decomposed);
    const std::string old_bytes = first.decomposed->serialize();
    EXPECT_EQ(0U, get_cached(*_engine, &cache, 7241, 4).scans);

    // the compaction of versions 2..3: the same rows, other rowsets
    compact(tablet, schema, 2, 3);

    CachedDigest after = get_cached(*_engine, &cache, 7241, 4);
    ASSERT_TRUE(after.status.ok()) << after.status;
    ASSERT_TRUE(after.decomposed_status.ok()) << after.decomposed_status;
    ASSERT_NE(nullptr, after.decomposed);
    // The key holds the fingerprint of the rowsets (ids and versions), which a compaction changes:
    // the whole digest is recomputed too, although it is the same (correctness over reuse).
    EXPECT_FALSE(after.result.from_cache);
    EXPECT_FALSE(after.decomposed_from_cache);
    EXPECT_EQ(1U, after.scans);
    EXPECT_EQ(first.result.root, after.result.root);
    EXPECT_NE(first.decomposed.get(), after.decomposed.get());
    EXPECT_EQ(first.decomposed->rowsets.size() - 1, after.decomposed->rowsets.size());
    EXPECT_NE(old_bytes, after.decomposed->serialize());
    RestoreDigestDecomposed fresh;
    ASSERT_TRUE(compute_tablet_restore_digest_decomposed(*_engine, 7241, 4, 2, &fresh).ok());
    EXPECT_EQ(fresh.serialize(), after.decomposed->serialize());
    EXPECT_EQ(1U, cache.decomposed_count());
    // the entry of the old generation was evicted, not left behind
    EXPECT_EQ(1U, cache.size());
    // the new one replaced the old one, and now it hits
    CachedDigest third = get_cached(*_engine, &cache, 7241, 4);
    EXPECT_EQ(0U, third.scans);
    EXPECT_TRUE(third.decomposed_from_cache);
    EXPECT_EQ(after.decomposed.get(), third.decomposed.get());
}

TEST_F(RestoreDigestTabletTest, ChangedMorBoundariesDropTheCachedDecomposedDigest) {
    _next_id = 942000; // not the ids of another test, see the segment cache
    ConfigRestore restore;
    config::restore_digest_cache_decomposed_mb = 256;
    config::restore_digest_mor_prefix_boundaries = 3;
    auto mor = create_tablet(7251, false, true);
    ASSERT_NE(nullptr, mor);
    for (int v = 2; v <= 5; ++v) {
        write(mor, mor->tablet_schema(), v, {{{1, v}, {v, v * 10}}}, false, true);
    }
    RestoreDigestCache cache(/*capacity=*/16);
    CachedDigest first = get_cached(*_engine, &cache, 7251, 5);
    ASSERT_TRUE(first.decomposed_status.ok()) << first.decomposed_status;
    ASSERT_NE(nullptr, first.decomposed);
    EXPECT_EQ(4U, first.decomposed->wholes.size());
    EXPECT_TRUE(get_cached(*_engine, &cache, 7251, 5).decomposed_from_cache);

    config::restore_digest_mor_prefix_boundaries = 1;
    CachedDigest fewer = get_cached(*_engine, &cache, 7251, 5);
    ASSERT_TRUE(fewer.decomposed_status.ok()) << fewer.decomposed_status;
    ASSERT_NE(nullptr, fewer.decomposed);
    EXPECT_TRUE(fewer.result.from_cache);
    EXPECT_FALSE(fewer.decomposed_from_cache);
    EXPECT_EQ(1U, fewer.scans);
    EXPECT_EQ(2U, fewer.decomposed->wholes.size());
    EXPECT_EQ(first.result.root, fewer.result.root);
    EXPECT_TRUE(get_cached(*_engine, &cache, 7251, 5).decomposed_from_cache);

    config::restore_digest_mor_prefix_boundaries = 2;
    CachedDigest more = get_cached(*_engine, &cache, 7251, 5);
    EXPECT_FALSE(more.decomposed_from_cache);
    EXPECT_TRUE(more.result.from_cache);
    ASSERT_NE(nullptr, more.decomposed);
    EXPECT_EQ(3U, more.decomposed->wholes.size());
}

TEST_F(RestoreDigestTabletTest, FailedDecomposedDigestCachesTheWholeDigestOnly) {
    _next_id = 943000; // not the ids of another test, see the segment cache
    ConfigRestore restore;
    config::restore_digest_cache_decomposed_mb = 256;
    auto tablet = create_tablet(7261, false);
    ASSERT_NE(nullptr, tablet);
    auto schema = tablet->tablet_schema();
    write(tablet, schema, 2, {{{1, 1}, {2, 3}, {3, 4}}}, false, true);
    write_delete(tablet, 3, {cond("v", "=", "3")});
    write(tablet, schema, 4, {{{5, 5}}}, false, true);

    RestoreDigestCache cache(/*capacity=*/16);
    config::restore_digest_prefix_max_scans = 0; // the DELETE condition needs an extra scan
    CachedDigest failed = get_cached(*_engine, &cache, 7261, 4);
    ASSERT_TRUE(failed.status.ok()) << failed.status;
    EXPECT_TRUE(failed.decomposed_status.is<ErrorCode::NOT_IMPLEMENTED_ERROR>())
            << failed.decomposed_status;
    EXPECT_EQ(nullptr, failed.decomposed);
    EXPECT_FALSE(failed.result.from_cache);
    EXPECT_EQ(1U, cache.size());
    EXPECT_EQ(0U, cache.decomposed_count());
    EXPECT_EQ(0, cache.decomposed_bytes());

    // again: the whole digest comes from the cache, so there is no second scan on the failure path
    CachedDigest again = get_cached(*_engine, &cache, 7261, 4);
    ASSERT_TRUE(again.status.ok());
    EXPECT_FALSE(again.decomposed_status.ok());
    EXPECT_TRUE(again.result.from_cache);
    EXPECT_EQ(failed.result.root, again.result.root);
    EXPECT_EQ(1U, again.scans); // the attempt to decompose, which gives up
    EXPECT_EQ(0U, cache.decomposed_count());

    // the failure of a first call scans the tablet twice at most (the attempt and the whole digest)
    RestoreDigestCache fresh_cache(/*capacity=*/16);
    EXPECT_EQ(2U, get_cached(*_engine, &fresh_cache, 7261, 4).scans);

    // the configuration restored: the decomposed digest is produced now, the whole digest is kept
    config::restore_digest_prefix_max_scans = restore.max_scans;
    CachedDigest ok = get_cached(*_engine, &cache, 7261, 4);
    ASSERT_TRUE(ok.status.ok());
    ASSERT_TRUE(ok.decomposed_status.ok()) << ok.decomposed_status;
    ASSERT_NE(nullptr, ok.decomposed);
    EXPECT_TRUE(ok.result.from_cache);
    EXPECT_FALSE(ok.decomposed_from_cache);
    EXPECT_EQ(failed.result.root, ok.result.root);
    EXPECT_EQ(ok.result.root, ok.decomposed->root);
    EXPECT_EQ(1U, cache.decomposed_count());
}

TEST_F(RestoreDigestTabletTest, WithoutAskingForTheDecomposedDigestItIsNotTouched) {
    _next_id = 944000; // not the ids of another test, see the segment cache
    ConfigRestore restore;
    config::restore_digest_cache_decomposed_mb = 256;
    auto tablet = create_tablet(7271, false);
    ASSERT_NE(nullptr, tablet);
    write(tablet, tablet->tablet_schema(), 2, {{{1, 1}, {2, 2}}}, false, true);
    write(tablet, tablet->tablet_schema(), 3, {{{3, 3}}}, false, true);

    RestoreDigestCache cache(/*capacity=*/16);
    CachedDigest warm = get_cached(*_engine, &cache, 7271, 3);
    ASSERT_NE(nullptr, warm.decomposed);
    const int64_t bytes = cache.decomposed_bytes();
    // a call for the whole digest only, on a hit and after a version the cache has not seen
    CachedDigest hit = get_cached(*_engine, &cache, 7271, 3, false);
    ASSERT_TRUE(hit.status.ok());
    EXPECT_TRUE(hit.result.from_cache);
    CachedDigest other = get_cached(*_engine, &cache, 7271, 2, false);
    ASSERT_TRUE(other.status.ok());
    EXPECT_FALSE(other.result.from_cache);
    EXPECT_EQ(1U, other.scans);
    EXPECT_EQ(bytes, cache.decomposed_bytes());
    EXPECT_EQ(1U, cache.decomposed_count());
    EXPECT_EQ(2U, cache.size());
    // and the decomposed digest is still the one from before
    CachedDigest again = get_cached(*_engine, &cache, 7271, 3);
    EXPECT_TRUE(again.decomposed_from_cache);
    EXPECT_EQ(warm.decomposed.get(), again.decomposed.get());
}

TEST_F(RestoreDigestTabletTest, CachedDecomposedDigestWhichDisagreesIsDroppedAndScannedAgain) {
    _next_id = 945000; // not the ids of another test, see the segment cache
    ConfigRestore restore;
    config::restore_digest_cache_decomposed_mb = 256;
    auto tablet = create_tablet(7281, false);
    ASSERT_NE(nullptr, tablet);
    write(tablet, tablet->tablet_schema(), 2, {{{1, 1}, {2, 2}}}, false, true);
    write(tablet, tablet->tablet_schema(), 3, {{{3, 3}}}, false, true);
    RestoreDigest direct = tablet_digest(7281, 3);

    RestoreDigestCache cache(/*capacity=*/16);
    CachedDigest first = get_cached(*_engine, &cache, 7281, 3);
    ASSERT_NE(nullptr, first.decomposed);
    // the cached whole digest says another root than the parts compose to
    const RestoreDigestCache::Key key = tablet_cache_key(*_engine, 7281, 3, direct.schema_sig);
    LogicalDigestResult bogus = first.result;
    bogus.root = std::string(64, 'f');
    bogus.from_cache = false;
    RestoreDigestCache::Hit hit;
    LogicalDigestResult ignored;
    ASSERT_TRUE(cache.lookup(key, &ignored, &hit));
    cache.insert(key, bogus, hit.decomposed, hit.guard);
    ASSERT_EQ(1U, cache.decomposed_count());

    CachedDigest got = get_cached(*_engine, &cache, 7281, 3);
    ASSERT_TRUE(got.status.ok()) << got.status;
    ASSERT_TRUE(got.decomposed_status.ok()) << got.decomposed_status;
    ASSERT_NE(nullptr, got.decomposed);
    EXPECT_EQ(1U, got.scans);
    EXPECT_FALSE(got.decomposed_from_cache);
    EXPECT_FALSE(got.result.from_cache);
    EXPECT_EQ(direct.root, got.result.root);
    EXPECT_EQ(direct.root, got.decomposed->root);
    EXPECT_EQ(1U, cache.stale_count());
    // both were replaced by the new result
    CachedDigest next = get_cached(*_engine, &cache, 7281, 3);
    EXPECT_EQ(0U, next.scans);
    EXPECT_TRUE(next.decomposed_from_cache);
    EXPECT_EQ(direct.root, next.result.root);
}

// ===== the data identity in the key ==========================================================

TEST_F(RestoreDigestTabletTest, RowsetsSigIgnoresTheOrderAndSeesTheRowsets) {
    _next_id = 949000; // not the ids of another test, see the segment cache
    auto tablet = create_tablet(7310, false);
    ASSERT_NE(nullptr, tablet);
    auto schema = tablet->tablet_schema();
    write(tablet, schema, 2, {{{1, 1}}}, false, true);
    write(tablet, schema, 3, {{{2, 2}}}, false, true);
    std::vector<RowsetSharedPtr> rowsets;
    {
        std::shared_lock lock(tablet->get_header_lock());
        auto ret = tablet->capture_consistent_rowsets_unlocked(Version(0, 3), CaptureRowsetOps {});
        ASSERT_TRUE(ret.has_value());
        rowsets = ret->rowsets;
    }
    ASSERT_EQ(3U, rowsets.size());
    const std::string sig = restore_digest_rowsets_sig(rowsets);
    EXPECT_EQ(32U, sig.size());
    std::vector<RowsetSharedPtr> reversed(rowsets.rbegin(), rowsets.rend());
    EXPECT_EQ(sig, restore_digest_rowsets_sig(reversed));
    std::vector<RowsetSharedPtr> fewer(rowsets.begin(), rowsets.end() - 1);
    EXPECT_NE(sig, restore_digest_rowsets_sig(fewer));
    // the same versions with another rowset id
    replace_rowset(tablet, schema, 3, {{2, 2}});
    std::vector<RowsetSharedPtr> replaced;
    {
        std::shared_lock lock(tablet->get_header_lock());
        auto ret = tablet->capture_consistent_rowsets_unlocked(Version(0, 3), CaptureRowsetOps {});
        ASSERT_TRUE(ret.has_value());
        replaced = ret->rowsets;
    }
    ASSERT_EQ(3U, replaced.size());
    EXPECT_NE(sig, restore_digest_rowsets_sig(replaced));
}

// The content of a tablet at the same id, version and schema is replaced: the cached digest of the
// old content must not be returned.
TEST_F(RestoreDigestTabletTest, ReplacedRowsetIsNotACacheHit) {
    _next_id = 950000; // not the ids of another test, see the segment cache
    ConfigRestore restore;
    config::restore_digest_cache_decomposed_mb = 256;
    auto tablet = create_tablet(7311, false);
    ASSERT_NE(nullptr, tablet);
    auto schema = tablet->tablet_schema();
    write(tablet, schema, 2, {{{1, 10}, {2, 20}}}, false, true);
    write(tablet, schema, 3, {{{3, 30}}}, false, true);

    RestoreDigestCache cache(/*capacity=*/16);
    CachedDigest x = get_cached(*_engine, &cache, 7311, 3);
    ASSERT_TRUE(x.status.ok()) << x.status;
    ASSERT_TRUE(x.decomposed_status.ok()) << x.decomposed_status;
    EXPECT_FALSE(x.result.from_cache);
    CachedDigest x_again = get_cached(*_engine, &cache, 7311, 3);
    EXPECT_TRUE(x_again.result.from_cache);
    EXPECT_TRUE(x_again.decomposed_from_cache);
    EXPECT_EQ(0U, x_again.scans);

    // version 3 now holds other rows: same tablet, same version, same schema
    replace_rowset(tablet, schema, 3, {{7, 70}, {8, 80}, {9, 90}});
    RestoreDigest direct = tablet_digest(7311, 3);
    ASSERT_NE(x.result.root, direct.root);

    CachedDigest y = get_cached(*_engine, &cache, 7311, 3);
    ASSERT_TRUE(y.status.ok()) << y.status;
    ASSERT_TRUE(y.decomposed_status.ok()) << y.decomposed_status;
    EXPECT_FALSE(y.result.from_cache);
    EXPECT_FALSE(y.decomposed_from_cache);
    EXPECT_EQ(1U, y.scans);
    EXPECT_EQ(direct.root, y.result.root);
    EXPECT_EQ(5U, y.result.rows);
    EXPECT_EQ(direct.root, y.decomposed->root);
    // not a stale entry: the key did not match, nothing disagreed
    EXPECT_EQ(0U, cache.stale_count());
    // the new content hits now
    CachedDigest y_again = get_cached(*_engine, &cache, 7311, 3);
    EXPECT_TRUE(y_again.result.from_cache);
    EXPECT_EQ(direct.root, y_again.result.root);

    // the same without the decomposed digest (what the RESTORE_DIGEST task of the target does)
    replace_rowset(tablet, schema, 3, {{11, 1}});
    RestoreDigest direct2 = tablet_digest(7311, 3);
    ASSERT_NE(direct.root, direct2.root);
    CachedDigest z = get_cached(*_engine, &cache, 7311, 3, /*want_decomposed=*/false);
    ASSERT_TRUE(z.status.ok()) << z.status;
    EXPECT_FALSE(z.result.from_cache);
    EXPECT_EQ(1U, z.scans);
    EXPECT_EQ(direct2.root, z.result.root);
    EXPECT_TRUE(get_cached(*_engine, &cache, 7311, 3, false).result.from_cache);
}

// The tablet is dropped and created again with the same id, and loaded with other data at the same
// versions (a new tablet_uid and new rowset ids).
TEST_F(RestoreDigestTabletTest, RecreatedTabletIsNotACacheHit) {
    _next_id = 951000; // not the ids of another test, see the segment cache
    ConfigRestore restore;
    config::restore_digest_cache_decomposed_mb = 256;
    auto tablet = create_tablet(7312, false);
    ASSERT_NE(nullptr, tablet);
    write(tablet, tablet->tablet_schema(), 2, {{{1, 10}, {2, 20}}}, false, true);
    write(tablet, tablet->tablet_schema(), 3, {{{3, 30}}}, false, true);

    RestoreDigestCache cache(/*capacity=*/16);
    CachedDigest x = get_cached(*_engine, &cache, 7312, 3);
    ASSERT_TRUE(x.status.ok()) << x.status;
    ASSERT_TRUE(x.decomposed_status.ok()) << x.decomposed_status;
    const TabletUid old_uid = tablet->tablet_uid();
    const int64_t replica_id = tablet->replica_id();
    tablet.reset();
    ASSERT_TRUE(_engine->tablet_manager()->drop_tablet(7312, replica_id, false).ok());

    auto again = create_tablet(7312, false);
    ASSERT_NE(nullptr, again);
    EXPECT_FALSE(old_uid == again->tablet_uid());
    write(again, again->tablet_schema(), 2, {{{5, 50}}}, false, true);
    write(again, again->tablet_schema(), 3, {{{6, 60}, {7, 70}}}, false, true);
    RestoreDigest direct = tablet_digest(7312, 3);
    ASSERT_NE(x.result.root, direct.root);

    CachedDigest y = get_cached(*_engine, &cache, 7312, 3);
    ASSERT_TRUE(y.status.ok()) << y.status;
    EXPECT_FALSE(y.result.from_cache);
    EXPECT_EQ(1U, y.scans);
    EXPECT_EQ(direct.root, y.result.root);
    ASSERT_TRUE(y.decomposed_status.ok()) << y.decomposed_status;
    EXPECT_EQ(direct.root, y.decomposed->root);

    // a plain request (no decomposed digest) on yet another recreation
    auto key_old = tablet_cache_key(*_engine, 7312, 3, direct.schema_sig);
    EXPECT_EQ(again->tablet_uid(), key_old.tablet_uid);
    EXPECT_FALSE(old_uid == key_old.tablet_uid);
    EXPECT_TRUE(get_cached(*_engine, &cache, 7312, 3, false).result.from_cache);
}

// A compaction retires the rowsets and makes another one: the same rows, but the identity of the
// data is different, so the whole digest and the decomposed digest are both computed again, and
// come out the same as a direct computation.
TEST_F(RestoreDigestTabletTest, CompactedRowsetsAreNotACacheHit) {
    _next_id = 952000; // not the ids of another test, see the segment cache
    ConfigRestore restore;
    config::restore_digest_cache_decomposed_mb = 256;
    auto tablet = create_tablet(7313, false);
    ASSERT_NE(nullptr, tablet);
    auto schema = tablet->tablet_schema();
    write(tablet, schema, 2, {{{1, 1}, {2, 2}}}, false, true);
    write(tablet, schema, 3, {{{3, 3}}}, false, true);
    write(tablet, schema, 4, {{{4, 4}}}, false, true);

    RestoreDigestCache cache(/*capacity=*/16);
    CachedDigest before = get_cached(*_engine, &cache, 7313, 4);
    ASSERT_TRUE(before.decomposed_status.ok()) << before.decomposed_status;
    // the whole digest alone, at another version
    CachedDigest plain_before = get_cached(*_engine, &cache, 7313, 3, false);
    ASSERT_TRUE(plain_before.status.ok());

    compact(tablet, schema, 2, 3);
    RestoreDigest direct = tablet_digest(7313, 4);
    EXPECT_EQ(before.result.root, direct.root);

    CachedDigest after = get_cached(*_engine, &cache, 7313, 4);
    ASSERT_TRUE(after.status.ok()) << after.status;
    ASSERT_TRUE(after.decomposed_status.ok()) << after.decomposed_status;
    EXPECT_FALSE(after.result.from_cache);
    EXPECT_FALSE(after.decomposed_from_cache);
    EXPECT_EQ(1U, after.scans);
    EXPECT_EQ(direct.root, after.result.root);
    EXPECT_EQ(direct.root, after.decomposed->root);
    EXPECT_EQ(0U, cache.stale_count());

    // the new entry hits, and a plain request for the same data does too
    EXPECT_TRUE(get_cached(*_engine, &cache, 7313, 4, false).result.from_cache);
}

// The cache holds a wrong root for the key of the current data: the recomputed digest is returned,
// the entry is replaced and counted, and the next request hits the right value.
TEST_F(RestoreDigestTabletTest, PoisonedEntryIsReplacedByTheRecomputedDigest) {
    _next_id = 953000; // not the ids of another test, see the segment cache
    ConfigRestore restore;
    config::restore_digest_cache_decomposed_mb = 256;
    auto tablet = create_tablet(7314, false);
    ASSERT_NE(nullptr, tablet);
    write(tablet, tablet->tablet_schema(), 2, {{{1, 1}, {2, 2}}}, false, true);
    write(tablet, tablet->tablet_schema(), 3, {{{3, 3}}}, false, true);
    RestoreDigest direct = tablet_digest(7314, 3);

    RestoreDigestCache cache(/*capacity=*/16);
    const RestoreDigestCache::Key key = tablet_cache_key(*_engine, 7314, 3, direct.schema_sig);
    LogicalDigestResult bogus;
    bogus.algo_version = RestoreDigest::kAlgoVersion;
    bogus.schema_sig = direct.schema_sig;
    bogus.root = std::string(64, 'e');
    bogus.rows = 3;
    cache.insert(key, bogus); // no decomposed digest in the entry
    EXPECT_EQ(0U, cache.stale_count());

    CachedDigest got = get_cached(*_engine, &cache, 7314, 3);
    ASSERT_TRUE(got.status.ok()) << got.status;
    ASSERT_TRUE(got.decomposed_status.ok()) << got.decomposed_status;
    EXPECT_FALSE(got.result.from_cache);
    EXPECT_EQ(direct.root, got.result.root);
    ASSERT_NE(nullptr, got.decomposed);
    EXPECT_EQ(direct.root, got.decomposed->root);
    EXPECT_EQ(1U, cache.stale_count());
    EXPECT_EQ(1U, cache.size());

    CachedDigest next = get_cached(*_engine, &cache, 7314, 3);
    EXPECT_TRUE(next.result.from_cache);
    EXPECT_TRUE(next.decomposed_from_cache);
    EXPECT_EQ(0U, next.scans);
    EXPECT_EQ(direct.root, next.result.root);
    EXPECT_EQ(1U, cache.stale_count());
    CachedDigest plain = get_cached(*_engine, &cache, 7314, 3, false);
    EXPECT_TRUE(plain.result.from_cache);
    EXPECT_EQ(direct.root, plain.result.root);
}

TEST(RestoreDigestCacheTest, OlderGenerationsOfTheSameVersionAreEvicted) {
    ConfigRestore restore;
    config::restore_digest_cache_decomposed_mb = 256;
    RestoreDigestCache cache(/*capacity=*/16);
    auto value = [](const std::string& root) {
        LogicalDigestResult v;
        v.root = root;
        return v;
    };
    RestoreDigestCache::DecomposedGuard guard;
    LogicalDigestResult out;
    RestoreDigestCache::Hit hit;
    const int64_t big = RestoreDigestCache::estimate_bytes(*fake_decomposed(40));
    const int64_t small = RestoreDigestCache::estimate_bytes(*fake_decomposed(10));
    ASSERT_NE(big, small);

    RestoreDigestCache::Key old_gen = cache_key(1, 5, "a");
    old_gen.rowsets_sig = "old";
    cache.insert(old_gen, value("r_old"), fake_decomposed(40), guard);
    // another version and another schema of the same tablet are not generations of the key
    cache.insert(cache_key(1, 6, "a"), value("v6"), fake_decomposed(10), guard);
    cache.insert(cache_key(1, 5, "b"), value("sb"), nullptr, guard);
    cache.insert(cache_key(2, 5, "a"), value("t2"), nullptr, guard);
    EXPECT_EQ(4U, cache.size());
    EXPECT_EQ(big + small, cache.decomposed_bytes());

    // the same tablet and version with another generation (rowsets, here also the delete bitmap)
    RestoreDigestCache::Key new_gen = cache_key(1, 5, "a");
    new_gen.rowsets_sig = "new";
    new_gen.bitmap_keys = 3;
    cache.insert(new_gen, value("r_new"), fake_decomposed(10), guard);
    EXPECT_EQ(4U, cache.size());
    EXPECT_EQ(2 * small, cache.decomposed_bytes());
    EXPECT_EQ(2U, cache.decomposed_count());
    EXPECT_FALSE(cache.lookup(old_gen, &out));
    ASSERT_TRUE(cache.lookup(new_gen, &out, &hit));
    EXPECT_EQ("r_new", out.root);
    EXPECT_NE(nullptr, hit.decomposed);
    EXPECT_TRUE(cache.lookup(cache_key(1, 6, "a"), &out));
    EXPECT_TRUE(cache.lookup(cache_key(1, 5, "b"), &out));
    EXPECT_TRUE(cache.lookup(cache_key(2, 5, "a"), &out));

    // only the new generation of (1, 5, "a") is left, and it is the only entry the index knows of
    // after an erase: a later insert of the old generation evicts it in turn
    cache.erase(new_gen);
    EXPECT_EQ(3U, cache.size());
    EXPECT_EQ(small, cache.decomposed_bytes());
    cache.insert(old_gen, value("r_old"), fake_decomposed(40), guard);
    cache.insert(new_gen, value("r_new"), fake_decomposed(10), guard);
    EXPECT_EQ(4U, cache.size());
    EXPECT_EQ(2 * small, cache.decomposed_bytes());
    cache.clear();
    EXPECT_EQ(0U, cache.size());
    EXPECT_EQ(0, cache.decomposed_bytes());
    cache.insert(old_gen, value("r_old"), fake_decomposed(40), guard);
    cache.insert(new_gen, value("r_new"), fake_decomposed(10), guard);
    EXPECT_EQ(1U, cache.size());
    EXPECT_EQ(small, cache.decomposed_bytes());
}

TEST(RestoreDigestCacheTest, DecomposedBytesAreLimitedByTheirOwnLru) {
    ConfigRestore restore;
    RestoreDigestCache cache(/*capacity=*/16);
    auto value = [](const std::string& root) {
        LogicalDigestResult v;
        v.root = root;
        return v;
    };
    const int parts = 160; // 160 * 8 KB = 1.25 MB
    const int64_t one = RestoreDigestCache::estimate_bytes(*fake_decomposed(parts));
    ASSERT_GT(one, 1 << 20);
    ASSERT_LT(one, 2 << 20);
    config::restore_digest_cache_decomposed_mb = 2;
    RestoreDigestCache::DecomposedGuard guard;
    LogicalDigestResult out;
    RestoreDigestCache::Hit hit;

    cache.insert(cache_key(1, 5, "a"), value("r1"), fake_decomposed(parts), guard);
    EXPECT_EQ(1U, cache.decomposed_count());
    EXPECT_EQ(one, cache.decomposed_bytes());
    // the second does not fit with the first: the decomposed digest of the older entry is dropped,
    // its whole digest stays
    cache.insert(cache_key(2, 5, "a"), value("r2"), fake_decomposed(parts), guard);
    EXPECT_EQ(1U, cache.decomposed_count());
    EXPECT_EQ(one, cache.decomposed_bytes());
    EXPECT_EQ(2U, cache.size());
    ASSERT_TRUE(cache.lookup(cache_key(1, 5, "a"), &out, &hit));
    EXPECT_EQ("r1", out.root);
    EXPECT_EQ(nullptr, hit.decomposed);
    ASSERT_TRUE(cache.lookup(cache_key(2, 5, "a"), &out, &hit));
    EXPECT_NE(nullptr, hit.decomposed);

    // the byte limited LRU is by use: a lookup of entry 2 protects it from entry 3 ... and entry 3
    // takes the place, since there is room for one only
    cache.insert(cache_key(3, 5, "a"), value("r3"), fake_decomposed(parts), guard);
    EXPECT_EQ(1U, cache.decomposed_count());
    ASSERT_TRUE(cache.lookup(cache_key(2, 5, "a"), &out, &hit));
    EXPECT_EQ(nullptr, hit.decomposed);
    ASSERT_TRUE(cache.lookup(cache_key(3, 5, "a"), &out, &hit));
    EXPECT_NE(nullptr, hit.decomposed);

    // a limit with room for two: the one used least recently goes
    config::restore_digest_cache_decomposed_mb = 3;
    cache.insert(cache_key(4, 5, "a"), value("r4"), fake_decomposed(parts), guard);
    EXPECT_EQ(2U, cache.decomposed_count());
    ASSERT_TRUE(cache.lookup(cache_key(3, 5, "a"), &out, &hit)); // 3 is used again, 4 is older
    EXPECT_NE(nullptr, hit.decomposed);
    cache.insert(cache_key(5, 5, "a"), value("r5"), fake_decomposed(parts), guard);
    EXPECT_EQ(2U, cache.decomposed_count());
    ASSERT_TRUE(cache.lookup(cache_key(4, 5, "a"), &out, &hit));
    EXPECT_EQ(nullptr, hit.decomposed);
    ASSERT_TRUE(cache.lookup(cache_key(3, 5, "a"), &out, &hit));
    EXPECT_NE(nullptr, hit.decomposed);
    ASSERT_TRUE(cache.lookup(cache_key(5, 5, "a"), &out, &hit));
    EXPECT_NE(nullptr, hit.decomposed);
    EXPECT_EQ(5U, cache.size());
    EXPECT_EQ(2 * one, cache.decomposed_bytes());

    // the limit lowered: the next insert brings the cache under it
    config::restore_digest_cache_decomposed_mb = 2;
    cache.insert(cache_key(6, 5, "a"), value("r6"), nullptr, guard);
    EXPECT_EQ(1U, cache.decomposed_count());
    EXPECT_EQ(one, cache.decomposed_bytes());

    // one digest larger than the limit is not cached, and no other digest is dropped for it
    cache.insert(cache_key(7, 5, "a"), value("r7"), fake_decomposed(2 * parts), guard);
    EXPECT_EQ(1U, cache.decomposed_count());
    EXPECT_EQ(one, cache.decomposed_bytes());
    ASSERT_TRUE(cache.lookup(cache_key(7, 5, "a"), &out, &hit));
    EXPECT_EQ("r7", out.root);
    EXPECT_EQ(nullptr, hit.decomposed);
    ASSERT_TRUE(cache.lookup(cache_key(5, 5, "a"), &out, &hit));
    EXPECT_NE(nullptr, hit.decomposed);

    // 0 does not cache them at all
    config::restore_digest_cache_decomposed_mb = 0;
    cache.insert(cache_key(8, 5, "a"), value("r8"), fake_decomposed(1), guard);
    ASSERT_TRUE(cache.lookup(cache_key(8, 5, "a"), &out, &hit));
    EXPECT_EQ("r8", out.root);
    EXPECT_EQ(nullptr, hit.decomposed);
    // (the ones from before are dropped by the next insert, the limit being 0)
    EXPECT_EQ(0U, cache.decomposed_count());
    EXPECT_EQ(0, cache.decomposed_bytes());

    // replacing and erasing keep the byte count right
    config::restore_digest_cache_decomposed_mb = 8;
    cache.insert(cache_key(9, 5, "a"), value("r9"), fake_decomposed(parts), guard);
    cache.insert(cache_key(9, 5, "a"), value("r9"), fake_decomposed(2 * parts), guard);
    EXPECT_EQ(1U, cache.decomposed_count());
    EXPECT_EQ(RestoreDigestCache::estimate_bytes(*fake_decomposed(2 * parts)),
              cache.decomposed_bytes());
    // a whole digest alone with the same root keeps the decomposed one, another root drops it
    cache.insert(cache_key(9, 5, "a"), value("r9"));
    EXPECT_EQ(1U, cache.decomposed_count());
    cache.insert(cache_key(9, 5, "a"), value("other"));
    EXPECT_EQ(0U, cache.decomposed_count());
    EXPECT_EQ(0, cache.decomposed_bytes());
    cache.insert(cache_key(9, 5, "a"), value("other"), fake_decomposed(parts), guard);
    cache.erase(cache_key(9, 5, "a"));
    EXPECT_EQ(0U, cache.decomposed_count());
    EXPECT_EQ(0, cache.decomposed_bytes());

    // an entry evicted by the count limit gives its bytes back
    RestoreDigestCache small(/*capacity=*/2);
    small.insert(cache_key(1, 1, "a"), value("a"), fake_decomposed(parts), guard);
    small.insert(cache_key(2, 1, "a"), value("b"), fake_decomposed(parts), guard);
    small.insert(cache_key(3, 1, "a"), value("c"), nullptr, guard);
    EXPECT_EQ(2U, small.size());
    EXPECT_EQ(1U, small.decomposed_count());
    EXPECT_EQ(one, small.decomposed_bytes());
    small.clear();
    EXPECT_EQ(0, small.decomposed_bytes());
}

TEST_F(RestoreDigestTabletTest, ZeroDecomposedLimitCachesTheWholeDigestOnly) {
    _next_id = 946000; // not the ids of another test, see the segment cache
    ConfigRestore restore;
    config::restore_digest_cache_decomposed_mb = 0;
    auto tablet = create_tablet(7291, false);
    ASSERT_NE(nullptr, tablet);
    write(tablet, tablet->tablet_schema(), 2, {{{1, 1}, {2, 2}}}, false, true);

    RestoreDigestCache cache(/*capacity=*/16);
    CachedDigest first = get_cached(*_engine, &cache, 7291, 2);
    ASSERT_TRUE(first.decomposed_status.ok()) << first.decomposed_status;
    ASSERT_NE(nullptr, first.decomposed);
    EXPECT_EQ(1U, first.scans);
    EXPECT_EQ(0U, cache.decomposed_count());
    CachedDigest second = get_cached(*_engine, &cache, 7291, 2);
    ASSERT_NE(nullptr, second.decomposed);
    EXPECT_TRUE(second.result.from_cache);
    EXPECT_FALSE(second.decomposed_from_cache);
    EXPECT_EQ(1U, second.scans); // one scan, as before this cache existed, and not two
    EXPECT_EQ(first.decomposed->serialize(), second.decomposed->serialize());
    EXPECT_EQ(0U, cache.decomposed_count());
}

TEST_F(RestoreDigestTabletTest, TaskProducesTheDecomposedDigest) {
    _next_id = 930000; // not the ids of another test, see the segment cache
    auto tablet = create_tablet(7221, false);
    ASSERT_NE(nullptr, tablet);
    auto schema = tablet->tablet_schema();
    write(tablet, schema, 2, {{{1, 10}, {2, 20}}}, false, true);
    write(tablet, schema, 3, {{{3, 30}}}, false, true);
    RestoreDigestCache::instance()->clear();
    PrefixDigestOutput prefix;
    TLogicalDigest d = compute_logical_digest_for_task(*_engine, 7221, 3, 0, &prefix);
    EXPECT_EQ("OK", d.status_code);
    EXPECT_TRUE(prefix.produced) << prefix.reason;
    ASSERT_NE(nullptr, prefix.digest);
    EXPECT_EQ(d.root, prefix.digest->root);
    EXPECT_EQ(7221, prefix.digest->tablet_id);
    EXPECT_GE(prefix.digest->rowsets.size(), 2U);

    // merge on read: the decomposed digest is produced, with whole digests
    auto mor = create_tablet(7222, /*mow=*/false, /*unique=*/true);
    ASSERT_NE(nullptr, mor);
    write(mor, mor->tablet_schema(), 2, {{{1, 10}}}, false, true);
    write(mor, mor->tablet_schema(), 3, {{{1, 11}}}, false, true);
    PrefixDigestOutput mor_prefix;
    TLogicalDigest md = compute_logical_digest_for_task(*_engine, 7222, 3, 0, &mor_prefix);
    EXPECT_EQ("OK", md.status_code);
    EXPECT_TRUE(mor_prefix.produced) << mor_prefix.reason;
    ASSERT_NE(nullptr, mor_prefix.digest);
    EXPECT_TRUE(mor_prefix.digest->mor);
    EXPECT_EQ(md.root, mor_prefix.digest->root);

    // turned off: the logical digest is fine, there is no decomposed digest
    const int32_t saved = config::restore_digest_mor_prefix_boundaries;
    config::restore_digest_mor_prefix_boundaries = 0;
    PrefixDigestOutput none;
    TLogicalDigest nd = compute_logical_digest_for_task(*_engine, 7222, 3, 0, &none);
    config::restore_digest_mor_prefix_boundaries = saved;
    EXPECT_EQ("OK", nd.status_code);
    EXPECT_FALSE(none.produced);
    EXPECT_FALSE(none.reason.empty());
}

// ===== 14. incremental restore: append the rowsets after the local version =====================

namespace {
TabletMetaPB make_meta(const std::vector<std::pair<int64_t, int64_t>>& versions,
                       bool remote_last = false) {
    TabletMetaPB pb;
    pb.set_tablet_id(1);
    int n = 0;
    for (auto [s, e] : versions) {
        auto* rs = pb.add_rs_metas();
        rs->set_start_version(s);
        rs->set_end_version(e);
        rs->set_rowset_id_v2(fmt::format("0200000000000000{:016x}", 1000 + n));
        rs->set_num_segments(1);
        if (remote_last && n + 1 == static_cast<int>(versions.size())) {
            rs->set_resource_id("remote");
        }
        ++n;
    }
    pb.add_stale_rs_metas()->set_start_version(5);
    pb.mutable_delete_bitmap()->add_rowset_ids("x");
    return pb;
}
} // namespace

TEST(RestoreIncrementalTest, SelectRowsetsAfterTheBaseVersion) {
    TabletMetaPB inc;
    std::vector<std::string> ids;
    // [0-1] [2-3] [4-4] [5-6]: cut at 3 gives [4-4] [5-6]
    auto pb = make_meta({{0, 1}, {2, 3}, {4, 4}, {5, 6}});
    ASSERT_TRUE(select_incremental_rowsets(pb, 3, 6, &inc, &ids).ok());
    ASSERT_EQ(2, inc.rs_metas_size());
    EXPECT_EQ(4, inc.rs_metas(0).start_version());
    EXPECT_EQ(6, inc.rs_metas(1).end_version());
    EXPECT_EQ(0, inc.stale_rs_metas_size());
    EXPECT_FALSE(inc.has_delete_bitmap());
    EXPECT_EQ(2U, ids.size());
    // a rowset crosses the base version
    EXPECT_FALSE(select_incremental_rowsets(pb, 2, 6, &inc, &ids).ok());
    // nothing after the base version, or the chain does not reach the end version
    EXPECT_FALSE(select_incremental_rowsets(pb, 6, 6, &inc, &ids).ok());
    EXPECT_FALSE(select_incremental_rowsets(pb, 3, 7, &inc, &ids).ok());
    // a gap
    EXPECT_FALSE(select_incremental_rowsets(make_meta({{0, 1}, {2, 3}, {5, 6}}), 3, 6, &inc, &ids).ok());
    // a remote storage rowset
    EXPECT_FALSE(select_incremental_rowsets(make_meta({{0, 3}, {4, 6}}, true), 3, 6, &inc, &ids).ok());
}

TEST(RestoreIncrementalTest, CutManifestKeepsTheFilesOfTheRowsets) {
    auto pb = make_meta({{0, 1}, {2, 3}, {4, 4}});
    TabletMetaPB inc;
    std::vector<std::string> ids;
    ASSERT_TRUE(select_incremental_rowsets(pb, 3, 4, &inc, &ids).ok());
    const std::string id0 = pb.rs_metas(0).rowset_id_v2();
    SnapshotManifest m;
    m.tablet_id = 1;
    for (const std::string& n : {std::string("1.hdr"), id0 + "_0.dat", id0 + "_0.idx", ids[0] + "_0.dat",
                                 ids[0] + "_0_5.idx", ids[0] + "_1.dat"}) {
        SnapshotManifestFile f;
        f.name = n;
        f.size = 10;
        m.files.push_back(f);
    }
    SnapshotManifest cut;
    // one segment file expected, the manifest has two: mismatch
    EXPECT_TRUE(cut_manifest_to_increment(m, inc, ids, &cut).is<ErrorCode::RESTORE_MANIFEST_MISMATCH>());
    m.files.pop_back();
    ASSERT_TRUE(cut_manifest_to_increment(m, inc, ids, &cut).ok());
    ASSERT_EQ(3U, cut.files.size()); // hdr, dat, idx
    EXPECT_TRUE(is_rowset_file(ids[0] + "_0.dat", ids[0]));
    EXPECT_FALSE(is_rowset_file(ids[0] + "x_0.dat", ids[0]));
    // a missing segment file
    m.files.erase(m.files.begin() + 3);
    EXPECT_FALSE(cut_manifest_to_increment(m, inc, ids, &cut).ok());
}

// A source tablet with rowsets of versions 2..6, a local tablet equal to it at version 3 but laid out
// differently, the snapshot dir of the increment as the download makes it, and the append.
class RestoreIncrementalAppendTest : public RestoreDigestTabletTest {
protected:
    // The models of the incremental append; the DELETE variants have a DELETE condition in the increment.
    enum class Kind { DUP, MOW, MOR, MOW_DELETES, MOR_DELETES };
    static bool kind_mow(Kind kind) { return kind == Kind::MOW || kind == Kind::MOW_DELETES; }
    TabletSharedPtr create_kind(int64_t id, Kind kind) {
        switch (kind) {
        case Kind::DUP:
            return create_tablet(id, false);
        case Kind::MOR:
        case Kind::MOR_DELETES:
            return create_tablet(id, /*mow=*/false, /*unique=*/true);
        default:
            return create_tablet(id, true);
        }
    }

    void build_source(const TabletSharedPtr& src, Kind kind) {
        auto schema = src->tablet_schema();
        switch (kind) {
        case Kind::DUP:
            build_source(src, false);
            return;
        case Kind::MOW:
            build_source(src, true);
            return;
        case Kind::MOW_DELETES: {
            auto rs2 = write(src, schema, 2, {{{1, 10}, {2, 20}, {3, 30}, {4, 40}, {5, 50}, {6, 60}}}, false, true);
            write(src, schema, 3, {{{3, 31}, {5, 51}}}, false, true);
            auto& bitmap = src->tablet_meta()->delete_bitmap();
            bitmap.add({rs2->rowset_id(), 0, 3}, 2);
            bitmap.add({rs2->rowset_id(), 0, 3}, 4);
            write(src, schema, 4, {{{1, 0, 1}, {6, 61}}}, false, true);
            bitmap.add({rs2->rowset_id(), 0, 4}, 0);
            bitmap.add({rs2->rowset_id(), 0, 4}, 5);
            // kills key 4 of the local base
            write_delete(src, 5, {cond("v", "=", "40")});
            write(src, schema, 6, {{{2, 21}, {7, 70}}}, false, true);
            bitmap.add({rs2->rowset_id(), 0, 6}, 1);
            return;
        }
        case Kind::MOR:
            write(src, schema, 2, {{{1, 10}, {2, 20}, {3, 30}, {4, 40}, {5, 50}, {6, 60}}}, false, true);
            write(src, schema, 3, {{{3, 31}, {5, 51}}}, false, true);
            write(src, schema, 4, {{{1, 0, 1}, {6, 61}}}, false, true);
            write(src, schema, 5, {{{2, 21}, {7, 70}}}, true, true);
            write(src, schema, 6, {{{2, 0, 1}}}, false, true);
            return;
        case Kind::MOR_DELETES:
            write(src, schema, 2, {{{1, 10}, {2, 20}, {3, 30}, {4, 40}, {5, 50}, {6, 60}}}, false, true);
            write(src, schema, 3, {{{3, 31}, {5, 51}}}, false, true);
            write(src, schema, 4, {{{1, 0, 1}, {6, 61}}}, false, true);
            // kills the row of key 2 of the local base before the merge, key 2 is gone
            write_delete(src, 5, {cond("v", "=", "20")});
            write(src, schema, 6, {{{1, 11}, {8, 80}}}, false, true);
            return;
        }
    }

    void build_local(const TabletSharedPtr& local, Kind kind) {
        // the merged state of the source at version 3 (the same rows for every unique model)
        build_local(local, kind != Kind::DUP);
    }

    // the rowsets after version 3 of the source tablet, and the marks of the merge-on-write bitmap
    void build_source(const TabletSharedPtr& src, bool mow) {
        auto schema = src->tablet_schema();
        if (mow) {
            auto rs2 = write(src, schema, 2, {{{1, 10}, {2, 20}, {3, 30}, {4, 40}, {5, 50}, {6, 60}}}, false, true);
            auto rs3 = write(src, schema, 3, {{{3, 31}, {5, 51}}}, false, true);
            auto& bitmap = src->tablet_meta()->delete_bitmap();
            bitmap.add({rs2->rowset_id(), 0, 3}, 2);
            bitmap.add({rs2->rowset_id(), 0, 3}, 4);
            auto rs4 = write(src, schema, 4, {{{1, 0, 1}, {6, 61}}}, false, true);
            bitmap.add({rs2->rowset_id(), 0, 4}, 0);
            bitmap.add({rs2->rowset_id(), 0, 4}, 5);
            auto rs5 = write(src, schema, 5, {{{2, 21}, {7, 70}}}, false, true);
            bitmap.add({rs2->rowset_id(), 0, 5}, 1);
            auto rs6 = write(src, schema, 6, {{{2, 0, 1}}}, false, true);
            bitmap.add({rs5->rowset_id(), 0, 6}, 0);
            (void)rs3;
            (void)rs4;
            (void)rs6;
        } else {
            write(src, schema, 2, {{{1, 10}, {2, 20}, {3, 30}}}, false, true);
            write(src, schema, 3, {{{4, 40}, {5, 50}}}, false, true);
            write(src, schema, 4, {{{6, 60}, {7, 70}}}, false, true);
            write_delete(src, 5, {cond("v", "=", "20")});
            write(src, schema, 6, {{{1, 11}, {8, 80}}}, false, true);
        }
    }

    // equal to the source at version 3, as one rowset of version 2 and an empty one of version 3
    void build_local(const TabletSharedPtr& local, bool mow) {
        auto schema = local->tablet_schema();
        if (mow) {
            write(local, schema, 2, {{{1, 10}, {2, 20}, {3, 31}, {4, 40}, {5, 51}, {6, 60}}}, false, true);
        } else {
            write(local, schema, 2, {{{1, 10}, {2, 20}, {3, 30}, {4, 40}, {5, 50}}}, false, true);
        }
        write(local, schema, 3, {}, false, true);
    }

    // The snapshot dir of the local tablet with the increment of the source tablet at (base, end], as the
    // download task leaves it.
    std::string make_increment_dir(const TabletSharedPtr& src, const TabletSharedPtr& local,
                                   int64_t base, int64_t end) {
        TSnapshotRequest empty_req;
        empty_req.__set_tablet_id(local->tablet_id());
        empty_req.__set_schema_hash(local->schema_hash());
        empty_req.__set_restore_incremental(true);
        std::string empty_path;
        bool allow = false;
        EXPECT_TRUE(_engine->snapshot_mgr()->make_snapshot(empty_req, &empty_path, &allow).ok());
        std::string dir = fmt::format("{}/{}/{}", empty_path, local->tablet_id(), local->schema_hash());
        // a full snapshot of the source tablet, as the backup keeps it
        TSnapshotRequest req;
        req.__set_tablet_id(src->tablet_id());
        req.__set_schema_hash(src->schema_hash());
        req.__set_version(end);
        std::string src_path;
        EXPECT_TRUE(_engine->snapshot_mgr()->make_snapshot(req, &src_path, &allow).ok());
        std::string src_dir = fmt::format("{}/{}/{}", src_path, src->tablet_id(), src->schema_hash());
        TabletMetaPB remote;
        EXPECT_TRUE(TabletMeta::load_from_file(fmt::format("{}/{}.hdr", src_dir, src->tablet_id()), &remote).ok());
        TabletMetaPB inc;
        std::vector<std::string> ids;
        Status st = select_incremental_rowsets(remote, base, end, &inc, &ids);
        EXPECT_TRUE(st.ok()) << st;
        std::vector<io::FileInfo> files;
        bool exists = false;
        EXPECT_TRUE(io::global_local_filesystem()->list(src_dir, true, &files, &exists).ok());
        for (const auto& f : files) {
            for (const auto& id : ids) {
                if (is_rowset_file(f.file_name, id)) {
                    EXPECT_TRUE(io::global_local_filesystem()
                                        ->link_file(src_dir + "/" + f.file_name, dir + "/" + f.file_name)
                                        .ok());
                }
            }
        }
        EXPECT_TRUE(TabletMeta::save(fmt::format("{}/{}.hdr", dir, local->tablet_id()), inc).ok());
        return dir;
    }

    // The whole download: the full snapshot of the source tablet moved onto a tablet.
    void whole_restore(const TabletSharedPtr& src, const TabletSharedPtr& target, int64_t end) {
        TSnapshotRequest req;
        req.__set_tablet_id(target->tablet_id());
        req.__set_schema_hash(target->schema_hash());
        req.__set_ref_tablet_id(src->tablet_id());
        req.__set_version(end);
        std::string path;
        bool allow = false;
        EXPECT_TRUE(_engine->snapshot_mgr()->make_snapshot(req, &path, &allow).ok());
        std::string dir = fmt::format("{}/{}/{}", path, target->tablet_id(), target->schema_hash());
        // the tablet meta of the snapshot is named by the target tablet
        SnapshotLoader loader(*_engine, ExecEnv::GetInstance(), 1L, 2L);
        Status st = loader.move(dir, target, true);
        EXPECT_TRUE(st.ok()) << st;
    }

    std::vector<std::string> tablet_files(const TabletSharedPtr& tablet) {
        std::vector<io::FileInfo> files;
        bool exists = false;
        EXPECT_TRUE(io::global_local_filesystem()->list(tablet->tablet_path(), true, &files, &exists).ok());
        std::vector<std::string> names;
        for (const auto& f : files) {
            names.push_back(f.file_name);
        }
        std::sort(names.begin(), names.end());
        return names;
    }

    // The evidence that the restore job `job_id` loaded the local source of `base` into the tablet, which the append
    // needs (the load of the local source writes it, these tablets are built by hand).
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

    void run_append(Kind kind, int64_t base_id) {
        _next_id = 940000 + base_id;
        auto src = create_kind(base_id, kind);
        auto local = create_kind(base_id + 1, kind);
        auto whole = create_kind(base_id + 2, kind);
        ASSERT_NE(nullptr, src);
        ASSERT_NE(nullptr, local);
        ASSERT_NE(nullptr, whole);
        build_source(src, kind);
        build_local(local, kind);
        mark_loaded(local, 1, 3);
        // the local data equals the source at the base version, laid out differently
        EXPECT_TRUE(digests_equal(tablet_digest(src->tablet_id(), 3), tablet_digest(local->tablet_id(), 3)));
        RestoreDigest local_before = tablet_digest(local->tablet_id(), 3);

        std::string dir = make_increment_dir(src, local, 3, 6);
        SnapshotLoader loader(*_engine, ExecEnv::GetInstance(), 1L, 2L);
        TRestoreIncrementalRange range;
        range.base_version = 3;
        range.end_version = 6;
        Status st = loader.append_increment(dir, local, range);
        ASSERT_TRUE(st.ok()) << st;
        EXPECT_EQ(6, local->max_version_unlocked());

        whole_restore(src, whole, 6);
        // the move reloads the tablet, the old object is stale
        ASSERT_EQ(6, _engine->tablet_manager()->get_tablet(whole->tablet_id())->max_version_unlocked());
        RestoreDigest appended = tablet_digest(local->tablet_id(), 6);
        // the same as the whole download, and the source
        EXPECT_TRUE(digests_equal(tablet_digest(whole->tablet_id(), 6), appended));
        EXPECT_TRUE(digests_equal(tablet_digest(src->tablet_id(), 6), appended));
        // every version up to the base is still the local data, and the versions in between are right too
        EXPECT_TRUE(digests_equal(local_before, tablet_digest(local->tablet_id(), 3)));
        for (int64_t v = 4; v <= 6; ++v) {
            EXPECT_TRUE(digests_equal(tablet_digest(src->tablet_id(), v), tablet_digest(local->tablet_id(), v)))
                    << v;
        }
        EXPECT_NE(local_before.root, appended.root);
        // idempotent: the same snapshot dir again is a no-op
        EXPECT_TRUE(loader.append_increment(dir, local, range).ok());
        EXPECT_EQ(6, local->max_version_unlocked());
        // a digest of 7 rows (dup: 5 rows deleted by the condition...) is not the point; the rows are there
        EXPECT_GT(appended.rows, 0U);
    }
};

TEST_F(RestoreIncrementalAppendTest, DuplicateEqualsWholeDownload) {
    run_append(Kind::DUP, 7401);
}

TEST_F(RestoreIncrementalAppendTest, MergeOnWriteWithUpdatesAndDeletesEqualsWholeDownload) {
    run_append(Kind::MOW, 7411);
}

// the DELETE condition is in the increment (version 5) and kills rows of the local data
TEST_F(RestoreIncrementalAppendTest, MergeOnReadEqualsWholeDownload) {
    run_append(Kind::MOR, 7441);
}

TEST_F(RestoreIncrementalAppendTest, MergeOnReadWithDeleteConditionEqualsWholeDownload) {
    run_append(Kind::MOR_DELETES, 7451);
}

TEST_F(RestoreIncrementalAppendTest, MergeOnWriteWithDeleteConditionEqualsWholeDownload) {
    run_append(Kind::MOW_DELETES, 7461);
}

// The delete predicate travels in the rowset meta of the increment: it is in the appended rowset, and
// not in the rowsets which were already there.
TEST_F(RestoreIncrementalAppendTest, DeletePredicateIsAppendedWithTheRowset) {
    for (Kind kind : {Kind::DUP, Kind::MOW_DELETES, Kind::MOR_DELETES}) {
        const int64_t base_id = 7470 + 10 * static_cast<int>(kind);
        _next_id = 990000 + base_id;
        auto src = create_kind(base_id, kind);
        auto local = create_kind(base_id + 1, kind);
        build_source(src, kind);
        build_local(local, kind);
        mark_loaded(local, 1, 3);
        std::string dir = make_increment_dir(src, local, 3, 6);
        SnapshotLoader loader(*_engine, ExecEnv::GetInstance(), 1L, 2L);
        TRestoreIncrementalRange range;
        range.base_version = 3;
        range.end_version = 6;
        ASSERT_TRUE(loader.append_increment(dir, local, range).ok());
        int with_predicate = 0;
        for (const auto& [version, rs_meta] : local->tablet_meta()->all_rs_metas()) {
            if (rs_meta->has_delete_predicate()) {
                ++with_predicate;
                EXPECT_EQ(5, version.first);
            }
        }
        EXPECT_EQ(1, with_predicate) << static_cast<int>(kind);
        // the predicate is applied: key 2 (or 4) of the local base is gone at 5, from the local data
        EXPECT_NE(tablet_digest(local->tablet_id(), 4).root, tablet_digest(local->tablet_id(), 5).root);
        EXPECT_TRUE(digests_equal(tablet_digest(src->tablet_id(), 5), tablet_digest(local->tablet_id(), 5)));
    }
}

TEST_F(RestoreIncrementalAppendTest, WrongBaseVersionIsRefusedAndNothingChanges) {
    _next_id = 950000;
    auto src = create_tablet(7421, false);
    auto local = create_tablet(7422, false);
    build_source(src, false);
    build_local(local, false);
    mark_loaded(local, 1, 3);
    std::string dir = make_increment_dir(src, local, 3, 6);
    auto files_before = tablet_files(local);
    SnapshotLoader loader(*_engine, ExecEnv::GetInstance(), 1L, 2L);
    TRestoreIncrementalRange range;
    range.base_version = 2; // the tablet is at 3
    range.end_version = 6;
    EXPECT_FALSE(loader.append_increment(dir, local, range).ok());
    EXPECT_EQ(3, local->max_version_unlocked());
    EXPECT_EQ(files_before, tablet_files(local));
}

TEST_F(RestoreIncrementalAppendTest, FailureLeavesNoHalfAppendedState) {
    _next_id = 960000;
    config::enable_debug_points = true;
    Defer defer([]() {
        DebugPoints::instance()->clear();
        config::enable_debug_points = false;
    });
    auto src = create_tablet(7431, true);
    auto local = create_tablet(7432, true);
    auto whole = create_tablet(7433, true);
    build_source(src, true);
    build_local(local, true);
    mark_loaded(local, 1, 3);
    std::string dir = make_increment_dir(src, local, 3, 6);
    auto files_before = tablet_files(local);
    RestoreDigest before = tablet_digest(7432, 3);
    auto bitmap_count_before = local->tablet_meta()->delete_bitmap().get_delete_bitmap_count();
    auto bitmap_card_before = local->tablet_meta()->delete_bitmap().cardinality();
    size_t rowsets_before = local->tablet_meta()->all_rs_metas().size();

    DebugPoints::instance()->add_with_params("Tablet.revise_tablet_meta_fail", {{"tablet_id", "7432"}});
    SnapshotLoader loader(*_engine, ExecEnv::GetInstance(), 1L, 2L);
    TRestoreIncrementalRange range;
    range.base_version = 3;
    range.end_version = 6;
    EXPECT_FALSE(loader.append_increment(dir, local, range).ok());
    // nothing of the increment is in the tablet: version, rowsets, delete bitmap, files
    EXPECT_EQ(3, local->max_version_unlocked());
    EXPECT_EQ(rowsets_before, local->tablet_meta()->all_rs_metas().size());
    EXPECT_EQ(bitmap_count_before, local->tablet_meta()->delete_bitmap().get_delete_bitmap_count());
    EXPECT_EQ(bitmap_card_before, local->tablet_meta()->delete_bitmap().cardinality());
    EXPECT_EQ(files_before, tablet_files(local));
    EXPECT_TRUE(digests_equal(before, tablet_digest(7432, 3)));
    EXPECT_FALSE(compute_tablet_restore_digest(*_engine, 7432, 4, &before).ok());
    // the evidence is as before too: loaded, not appended
    EXPECT_TRUE(local->tablet_meta()->has_restore_stage());
    EXPECT_EQ(RestoreStagePB::LOCAL_LOADED, local->tablet_meta()->restore_stage().stage());

    // the failure is gone: appending the same dir again works and equals the whole download
    DebugPoints::instance()->clear();
    Status st = loader.append_increment(dir, local, range);
    ASSERT_TRUE(st.ok()) << st;
    whole_restore(src, whole, 6);
    EXPECT_TRUE(digests_equal(tablet_digest(7433, 6), tablet_digest(7432, 6)));
}

// The atomic restore keeps the data of the table being replaced: the snapshot of a tablet made from the local source
// tablet (TSnapshotRequest.restore_local_source).
class RestoreLocalSourceTest : public RestoreIncrementalAppendTest {
protected:
    size_t snapshot_dirs(const TabletSharedPtr& tablet) {
        std::string dir = tablet->data_dir()->path() + "/snapshot";
        if (!std::filesystem::exists(dir)) {
            return 0;
        }
        return std::distance(std::filesystem::directory_iterator(dir),
                             std::filesystem::directory_iterator());
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

    // keep the data of src at `version` in target, as the snapshot and the move of the restore do
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

    void force_copy(bool copy) {
        config::enable_debug_points = true;
        DebugPoints::instance()->clear();
        if (copy) {
            DebugPoints::instance()->add("SnapshotManager.restore_local_source.force_copy");
        }
    }

    int64_t hard_links(const TabletSharedPtr& tablet) {
        std::vector<io::FileInfo> files;
        bool exists = false;
        EXPECT_TRUE(io::global_local_filesystem()
                            ->list(tablet->tablet_path(), true, &files, &exists)
                            .ok());
        int64_t linked = 0;
        for (const auto& f : files) {
            struct stat st;
            std::string path = tablet->tablet_path() + "/" + f.file_name;
            if (f.file_name.ends_with(".dat") && stat(path.c_str(), &st) == 0 && st.st_nlink > 1) {
                ++linked;
            }
        }
        return linked;
    }

    void run_keep(bool mow, int64_t base_id, bool copy) {
        _next_id = 970000 + base_id;
        force_copy(copy);
        Defer defer([]() {
            DebugPoints::instance()->clear();
            config::enable_debug_points = false;
        });
        auto src = create_tablet(base_id, mow);
        auto target = create_tablet(base_id + 1, mow);
        ASSERT_NE(nullptr, src);
        ASSERT_NE(nullptr, target);
        build_source(src, mow);
        auto files_before = tablet_files(src);
        RestoreDigest src_digest = tablet_digest(src->tablet_id(), 6);

        LocalSourceStats stats = keep(src, target, 6);
        EXPECT_EQ(1, stats.tablets);
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
        auto moved = _engine->tablet_manager()->get_tablet(target->tablet_id());
        ASSERT_NE(nullptr, moved);
        EXPECT_EQ(6, moved->max_version_unlocked());
        EXPECT_TRUE(digests_equal(src_digest, tablet_digest(target->tablet_id(), 6)));
        // the source tablet is not changed in any way
        EXPECT_EQ(files_before, tablet_files(src));
        EXPECT_EQ(6, src->max_version_unlocked());
        EXPECT_TRUE(digests_equal(src_digest, tablet_digest(src->tablet_id(), 6)));
        // linked: the segment files of the source are shared with the moved tablet
        if (!copy) {
            EXPECT_GT(hard_links(moved), 0);
        }
    }
};

TEST_F(RestoreLocalSourceTest, DuplicateKeepEqualsSourceAndLinks) {
    run_keep(false, 7501, false);
}

TEST_F(RestoreLocalSourceTest, MergeOnWriteKeepEqualsSourceAndLinks) {
    run_keep(true, 7511, false);
}

// the source tablet is on another disk: the files are copied
TEST_F(RestoreLocalSourceTest, DuplicateKeepCopiesOnAnotherDisk) {
    run_keep(false, 7521, true);
}

TEST_F(RestoreLocalSourceTest, MergeOnWriteKeepCopiesOnAnotherDisk) {
    run_keep(true, 7531, true);
}

// the version is a boundary inside the source tablet: the data of the source at that version, not the latest
TEST_F(RestoreLocalSourceTest, KeepsTheDataAtTheVersionNotTheLatest) {
    _next_id = 980000;
    force_copy(false);
    Defer defer([]() {
        DebugPoints::instance()->clear();
        config::enable_debug_points = false;
    });
    auto src = create_tablet(7541, false);
    auto target = create_tablet(7542, false);
    build_source(src, false);
    RestoreDigest at3 = tablet_digest(src->tablet_id(), 3);
    RestoreDigest at6 = tablet_digest(src->tablet_id(), 6);
    EXPECT_FALSE(digests_equal(at3, at6));
    keep(src, target, 3);
    auto moved = _engine->tablet_manager()->get_tablet(target->tablet_id());
    EXPECT_EQ(3, moved->max_version_unlocked());
    EXPECT_TRUE(digests_equal(at3, tablet_digest(target->tablet_id(), 3)));
    EXPECT_EQ(6, src->max_version_unlocked());
}

// no source tablet, a version the source does not have, a missing version: the task fails and no snapshot is left
TEST_F(RestoreLocalSourceTest, FailureLeavesNoSnapshot) {
    _next_id = 990000;
    auto src = create_tablet(7551, false);
    auto target = create_tablet(7552, false);
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
    missing.__set_ref_tablet_id(7559);
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
}

// The snapshot task of the atomic restore of a partition behind the backup: the local source at the base version
// is loaded into the tablet, the snapshot dir is an empty one for the increment. The increment of the backup at
// (base, end] is downloaded into it, and appended by the move.
class RestoreLocalSourceIncrementalTest : public RestoreLocalSourceTest {
protected:
    // the increment of backup at (base, end], put into the (empty) dir of the target
    void put_increment(const TabletSharedPtr& backup, const TabletSharedPtr& target,
                       const std::string& dir, int64_t base, int64_t end) {
        TSnapshotRequest req;
        req.__set_tablet_id(backup->tablet_id());
        req.__set_schema_hash(backup->schema_hash());
        req.__set_version(end);
        std::string src_path;
        bool allow = false;
        ASSERT_TRUE(_engine->snapshot_mgr()->make_snapshot(req, &src_path, &allow).ok());
        std::string src_dir =
                fmt::format("{}/{}/{}", src_path, backup->tablet_id(), backup->schema_hash());
        TabletMetaPB remote;
        ASSERT_TRUE(TabletMeta::load_from_file(
                            fmt::format("{}/{}.hdr", src_dir, backup->tablet_id()), &remote)
                            .ok());
        TabletMetaPB inc;
        std::vector<std::string> ids;
        ASSERT_TRUE(select_incremental_rowsets(remote, base, end, &inc, &ids).ok());
        std::vector<io::FileInfo> files;
        bool exists = false;
        ASSERT_TRUE(io::global_local_filesystem()->list(src_dir, true, &files, &exists).ok());
        for (const auto& f : files) {
            for (const auto& id : ids) {
                if (is_rowset_file(f.file_name, id)) {
                    ASSERT_TRUE(io::global_local_filesystem()
                                        ->link_file(src_dir + "/" + f.file_name,
                                                    dir + "/" + f.file_name)
                                        .ok());
                }
            }
        }
        ASSERT_TRUE(TabletMeta::save(fmt::format("{}/{}.hdr", dir, target->tablet_id()), inc).ok());
    }

    void run_incremental(bool mow, int64_t base_id, bool copy) {
        run_incremental(mow ? Kind::MOW : Kind::DUP, base_id, copy);
    }

    void run_incremental(Kind kind, int64_t base_id, bool copy) {
        _next_id = 1010000 + base_id;
        force_copy(copy);
        Defer defer([]() {
            DebugPoints::instance()->clear();
            config::enable_debug_points = false;
        });
        // backup: the data at version 6; origin: the tablet being replaced, equal to it at 3
        auto backup = create_kind(base_id, kind);
        auto origin = create_kind(base_id + 1, kind);
        auto target = create_kind(base_id + 2, kind);
        auto whole = create_kind(base_id + 3, kind);
        ASSERT_NE(nullptr, whole);
        build_source(backup, kind);
        build_local(origin, kind);
        EXPECT_TRUE(digests_equal(tablet_digest(backup->tablet_id(), 3),
                                  tablet_digest(origin->tablet_id(), 3)));
        auto origin_files = tablet_files(origin);
        size_t dirs = snapshot_dirs(target);

        TSnapshotRequest req = local_source_request(origin, target, 3);
        req.__set_restore_incremental(true);
        std::string dir_path;
        LocalSourceStats stats;
        SnapshotLoader loader(*_engine, ExecEnv::GetInstance(), 1L, 2L);
        Status st = loader.make_local_source_snapshot_and_load(req, &dir_path, &stats);
        ASSERT_TRUE(st.ok()) << st;
        EXPECT_EQ(1, stats.tablets);
        EXPECT_GT(stats.linked_files + stats.copied_files, 0);
        // the local source snapshot is released, only the empty dir of the increment is left
        EXPECT_EQ(dirs + 1, snapshot_dirs(target));
        std::string dir =
                fmt::format("{}/{}/{}", dir_path, target->tablet_id(), target->schema_hash());
        std::vector<io::FileInfo> files;
        bool exists = false;
        ASSERT_TRUE(io::global_local_filesystem()->list(dir, true, &files, &exists).ok());
        EXPECT_TRUE(files.empty());
        // the tablet is the origin at the base version, as the incremental restore of a local tablet finds it
        auto loaded = _engine->tablet_manager()->get_tablet(target->tablet_id());
        ASSERT_NE(nullptr, loaded);
        EXPECT_EQ(3, loaded->max_version_unlocked());
        EXPECT_TRUE(digests_equal(tablet_digest(origin->tablet_id(), 3),
                                  tablet_digest(target->tablet_id(), 3)));

        put_increment(backup, target, dir, 3, 6);
        TRestoreIncrementalRange range;
        range.base_version = 3;
        range.end_version = 6;
        st = loader.append_increment(dir, loaded, range);
        ASSERT_TRUE(st.ok()) << st;
        EXPECT_EQ(6, loaded->max_version_unlocked());
        whole_restore(backup, whole, 6);
        // the same as the backup and as the whole download, at the end and at every version of the increment
        EXPECT_TRUE(digests_equal(tablet_digest(backup->tablet_id(), 6),
                                  tablet_digest(target->tablet_id(), 6)));
        EXPECT_TRUE(digests_equal(tablet_digest(whole->tablet_id(), 6),
                                  tablet_digest(target->tablet_id(), 6)));
        for (int64_t v = 3; v <= 6; ++v) {
            EXPECT_TRUE(digests_equal(tablet_digest(backup->tablet_id(), v),
                                      tablet_digest(target->tablet_id(), v)))
                    << v;
        }
        // the origin tablet is not changed
        EXPECT_EQ(origin_files, tablet_files(origin));
        EXPECT_EQ(3, origin->max_version_unlocked());
    }
};

TEST_F(RestoreLocalSourceIncrementalTest, DuplicateLocalBasePlusIncrementEqualsBackup) {
    run_incremental(false, 7601, false);
}

TEST_F(RestoreLocalSourceIncrementalTest, MergeOnWriteLocalBasePlusIncrementEqualsBackup) {
    run_incremental(true, 7611, false);
}

TEST_F(RestoreLocalSourceIncrementalTest, MergeOnReadLocalBasePlusIncrementEqualsBackup) {
    run_incremental(Kind::MOR, 7651, false);
}

TEST_F(RestoreLocalSourceIncrementalTest, MergeOnReadWithDeleteConditionLocalBasePlusIncrement) {
    run_incremental(Kind::MOR_DELETES, 7661, false);
}

TEST_F(RestoreLocalSourceIncrementalTest, MergeOnWriteWithDeleteConditionLocalBasePlusIncrement) {
    run_incremental(Kind::MOW_DELETES, 7671, false);
}

TEST_F(RestoreLocalSourceIncrementalTest, MergeOnReadLocalBaseCopiedOnAnotherDisk) {
    run_incremental(Kind::MOR_DELETES, 7681, true);
}

TEST_F(RestoreLocalSourceIncrementalTest, DuplicateLocalBaseCopiedOnAnotherDisk) {
    run_incremental(false, 7621, true);
}

TEST_F(RestoreLocalSourceIncrementalTest, MergeOnWriteLocalBaseCopiedOnAnotherDisk) {
    run_incremental(true, 7631, true);
}

// the local source can not be read: the task fails and no snapshot is left
TEST_F(RestoreLocalSourceIncrementalTest, FailureLeavesNoSnapshot) {
    _next_id = 1040000;
    auto origin = create_tablet(7641, false);
    auto target = create_tablet(7642, false);
    build_local(origin, false);
    size_t dirs = snapshot_dirs(target);
    SnapshotLoader loader(*_engine, ExecEnv::GetInstance(), 1L, 2L);
    std::string path;
    LocalSourceStats stats;
    // the origin has no version 5
    TSnapshotRequest req = local_source_request(origin, target, 5);
    req.__set_restore_incremental(true);
    EXPECT_FALSE(loader.make_local_source_snapshot_and_load(req, &path, &stats).ok());
    EXPECT_EQ(dirs, snapshot_dirs(target));
    // the origin is not on this backend
    TSnapshotRequest missing = local_source_request(origin, target, 3);
    missing.__set_ref_tablet_id(7649);
    missing.__set_restore_incremental(true);
    EXPECT_FALSE(loader.make_local_source_snapshot_and_load(missing, &path, &stats).ok());
    EXPECT_EQ(dirs, snapshot_dirs(target));
}

// The evidence of the restore job in the tablet meta (RestoreStagePB): the judgement of the resent snapshot (load of
// the local source) and move (append of the increment) tasks.
class RestoreStageTest : public RestoreLocalSourceIncrementalTest {
protected:
    struct Prepared {
        TabletSharedPtr backup;
        TabletSharedPtr origin;
        TabletSharedPtr target;
        std::string dir;
        TRestoreIncrementalRange range;
        TRestoreBackupDigest backup_digest;
    };

    static bool not_retryable(const Status& st) {
        return !st.ok() &&
               st.to_string().find(SnapshotLoader::kRestoreNotRetryable) != std::string::npos;
    }

    TRestoreBackupDigest digest_of(const TabletSharedPtr& tablet, int64_t version) {
        LogicalDigestResult result;
        Status st = get_tablet_logical_digest(*_engine, tablet->tablet_id(), version, 1,
                                              RestoreDigestCache::instance(), &result);
        EXPECT_TRUE(st.ok()) << st;
        TRestoreBackupDigest digest;
        digest.__set_algo_version(static_cast<int32_t>(result.algo_version));
        digest.__set_schema_sig(result.schema_sig);
        digest.__set_root(result.root);
        digest.__set_rows(static_cast<int64_t>(result.rows));
        digest.__set_status_code("OK");
        return digest;
    }

    TSnapshotRequest load_request(const Prepared& p, int64_t job_id) {
        TSnapshotRequest req = local_source_request(p.origin, p.target, 3);
        req.__set_restore_incremental(true);
        req.__set_job_id(job_id);
        return req;
    }

    // backup (data at 6), origin (equal to it at 3), and a fresh target
    Prepared prepare(Kind kind, int64_t base_id) {
        _next_id = 1100000 + base_id;
        Prepared p;
        p.backup = create_kind(base_id, kind);
        p.origin = create_kind(base_id + 1, kind);
        p.target = create_kind(base_id + 2, kind);
        build_source(p.backup, kind);
        build_local(p.origin, kind);
        p.range.base_version = 3;
        p.range.end_version = 6;
        p.backup_digest = digest_of(p.backup, 6);
        return p;
    }

    // the snapshot task of the restore job: loads the local source into the target, returns the empty dir
    Status load(Prepared& p, SnapshotLoader& loader, int64_t job_id, LocalSourceStats* stats) {
        std::string dir_path;
        Status st = loader.make_local_source_snapshot_and_load(load_request(p, job_id), &dir_path,
                                                               stats);
        if (st.ok()) {
            p.dir = fmt::format("{}/{}/{}", dir_path, p.target->tablet_id(),
                                p.target->schema_hash());
        }
        return st;
    }

    TabletSharedPtr current(const TabletSharedPtr& tablet) {
        return _engine->tablet_manager()->get_tablet(tablet->tablet_id());
    }

    RestoreStagePB stage_of(const TabletSharedPtr& tablet) {
        auto t = current(tablet);
        EXPECT_TRUE(t->tablet_meta()->has_restore_stage());
        return t->tablet_meta()->restore_stage();
    }

    void debug_on() {
        config::enable_debug_points = true;
        DebugPoints::instance()->clear();
    }
    void debug_off() {
        DebugPoints::instance()->clear();
        config::enable_debug_points = false;
    }
};

// The non atomic incremental restore appends to the tablet being restored, which no restore job loaded from a local
// source: no evidence is needed or written, the judgement is the one before the evidence (base version, loaded tag).
TEST_F(RestoreStageTest, NonAtomicAppendNeedsNoEvidenceAndIsIdempotentByTheLoadedTag) {
    _next_id = 1130000;
    auto src = create_tablet(7841, false);
    auto local = create_tablet(7842, false);
    build_source(src, false);
    build_local(local, false);
    ASSERT_FALSE(local->tablet_meta()->has_restore_stage());
    std::string dir = make_increment_dir(src, local, 3, 6);
    SnapshotLoader loader(*_engine, ExecEnv::GetInstance(), 1L, 2L);
    TRestoreIncrementalRange range;
    range.base_version = 3;
    range.end_version = 6;
    // the wrong base version: a plain error, not the one which cancels the job
    TRestoreIncrementalRange wrong = range;
    wrong.base_version = 2;
    Status st = loader.append_increment(dir, local, wrong, nullptr, false);
    EXPECT_FALSE(st.ok());
    EXPECT_FALSE(not_retryable(st)) << st;
    EXPECT_EQ(3, local->max_version_unlocked());
    st = loader.append_increment(dir, local, range, nullptr, false);
    ASSERT_TRUE(st.ok()) << st;
    EXPECT_EQ(6, local->max_version_unlocked());
    EXPECT_FALSE(local->tablet_meta()->has_restore_stage());
    EXPECT_TRUE(digests_equal(tablet_digest(src->tablet_id(), 6),
                              tablet_digest(local->tablet_id(), 6)));
    // resent: the loaded tag of the snapshot dir says it is done
    auto files = tablet_files(local);
    EXPECT_TRUE(loader.append_increment(dir, local, range, nullptr, false).ok());
    EXPECT_EQ(files, tablet_files(local));
    EXPECT_EQ(6, local->max_version_unlocked());
    // the debug point which appends without the evidence changes nothing without evidence
    EXPECT_FALSE(local->tablet_meta()->has_restore_stage());
}

TEST_F(RestoreStageTest, TabletMetaKeepsTheStageAcrossSerialization) {
    auto tablet = create_tablet(7701, false);
    ASSERT_NE(nullptr, tablet);
    EXPECT_FALSE(tablet->tablet_meta()->has_restore_stage());
    TabletMetaPB pb;
    tablet->tablet_meta()->to_meta_pb(&pb, false);
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
    restored.to_meta_pb(&file_pb, false);
    std::string file = tablet->tablet_path() + "/stage_roundtrip.hdr";
    ASSERT_TRUE(TabletMeta::save(file, file_pb).ok());
    TabletMetaPB loaded;
    ASSERT_TRUE(TabletMeta::load_from_file(file, &loaded).ok());
    EXPECT_EQ(42, loaded.restore_stage().job_id());
    std::filesystem::remove(file);
}

// the snapshot of a tablet which an earlier restore loaded has its evidence, the load drops it; the evidence of this
// job is saved by the same step; a move of the tablet to another disk (convert without reset) keeps it
TEST_F(RestoreStageTest, ConvertRowsetIdsDropsTheSourceEvidenceAndWritesTheOneOfTheJob) {
    _next_id = 1110000;
    auto src = create_tablet(7711, false);
    build_source(src, false);
    mark_loaded(src, 7, 3, RestoreStagePB::APPENDED, 6);
    auto make_dir = [&](const TabletSharedPtr& target) {
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
    std::string dir1 = make_dir(src);
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
    std::string dir2 = make_dir(src);
    auto guards2 = _engine->snapshot_mgr()->convert_rowset_ids(
            dir2, src->tablet_id(), src->replica_id(), src->table_id(), src->partition_id(),
            src->schema_hash(), true);
    ASSERT_TRUE(guards2.has_value());
    hdr_stage(dir2, &has);
    EXPECT_FALSE(has);
    // reset and the evidence of this job
    std::string dir3 = make_dir(src);
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

// the four branches of the load of the local source
TEST_F(RestoreStageTest, LoadWritesTheEvidenceAndTheResentTaskIsSkipped) {
    Prepared p = prepare(Kind::DUP, 7721);
    SnapshotLoader loader(*_engine, ExecEnv::GetInstance(), 1L, 2L);
    TabletUid uid_before = p.target->tablet_uid();
    LocalSourceStats stats;
    // branch 3: no evidence, the empty initial tablet: loaded, with the evidence of the job
    ASSERT_TRUE(load(p, loader, 1, &stats).ok());
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
    // branch 1: resent, the evidence of this job and the version: nothing is loaded again
    LocalSourceStats again;
    ASSERT_TRUE(load(p, loader, 1, &again).ok());
    EXPECT_EQ(0, again.tablets);
    EXPECT_EQ(uid_loaded, current(p.target)->tablet_uid());
    EXPECT_EQ(files, tablet_files(current(p.target)));
    // and the empty dir for the increment is there for the download
    EXPECT_TRUE(std::filesystem::exists(p.dir));
    EXPECT_TRUE(digests_equal(tablet_digest(p.origin->tablet_id(), 3),
                              tablet_digest(p.target->tablet_id(), 3)));
}

TEST_F(RestoreStageTest, LoadRefusesTheEvidenceOfAnotherJobAndTheTabletWhichIsNotClean) {
    Prepared p = prepare(Kind::DUP, 7731);
    SnapshotLoader loader(*_engine, ExecEnv::GetInstance(), 1L, 2L);
    ASSERT_TRUE(load(p, loader, 1, nullptr).ok());
    auto files = tablet_files(current(p.target));
    // branch 2: the evidence of another job
    SnapshotLoader other(*_engine, ExecEnv::GetInstance(), 2L, 2L);
    Status st = load(p, other, 2, nullptr);
    EXPECT_TRUE(not_retryable(st)) << st;
    // branch 1 with a version which is not the evidence: the max version is not the base version
    mark_loaded(current(p.target), 1, 3, RestoreStagePB::APPENDED, 6);
    st = load(p, loader, 1, nullptr);
    EXPECT_TRUE(not_retryable(st)) << st;
    // the request asks for another local source version than the evidence
    mark_loaded(current(p.target), 1, 2);
    st = load(p, loader, 1, nullptr);
    EXPECT_TRUE(not_retryable(st)) << st;
    // nothing is replaced by the refused tasks
    EXPECT_EQ(files, tablet_files(current(p.target)));
    EXPECT_EQ(3, current(p.target)->max_version_unlocked());

    // branch 4: no evidence, the tablet is not the empty one the job created
    Prepared q = prepare(Kind::DUP, 7741);
    write(q.target, q.target->tablet_schema(), 2, {{{1, 10}}}, false, true);
    auto q_files = tablet_files(q.target);
    st = load(q, loader, 1, nullptr);
    EXPECT_TRUE(not_retryable(st)) << st;
    EXPECT_EQ(q_files, tablet_files(q.target));
    EXPECT_EQ(2, q.target->max_version_unlocked());
    // no job id: refused as well
    TSnapshotRequest no_job = load_request(q, 1);
    no_job.__isset.job_id = false;
    SnapshotLoader no_id(*_engine, ExecEnv::GetInstance(), 0L, 2L);
    std::string path;
    st = no_id.make_local_source_snapshot_and_load(no_job, &path, nullptr);
    EXPECT_TRUE(not_retryable(st)) << st;
}

// an empty local source, and the evidence lost: the shape of the loaded tablet (empty rowsets) is not taken for
// the clean tablet of the job, the judgement is by the evidence only
TEST_F(RestoreStageTest, EmptyLoadedTabletIsNotTakenForACleanOneWithoutEvidence) {
    _next_id = 1120000;
    auto origin = create_tablet(7751, false);
    auto target = create_tablet(7752, false);
    write(origin, origin->tablet_schema(), 2, {}, false, true);
    write(origin, origin->tablet_schema(), 3, {}, false, true);
    SnapshotLoader loader(*_engine, ExecEnv::GetInstance(), 1L, 2L);
    TSnapshotRequest req = local_source_request(origin, target, 3);
    req.__set_restore_incremental(true);
    req.__set_job_id(1);
    std::string path;
    ASSERT_TRUE(loader.make_local_source_snapshot_and_load(req, &path, nullptr).ok());
    auto loaded = current(target);
    EXPECT_EQ(3, loaded->max_version_unlocked());
    EXPECT_TRUE(loaded->tablet_meta()->has_restore_stage());
    // resent: skipped by the evidence
    LocalSourceStats again;
    ASSERT_TRUE(loader.make_local_source_snapshot_and_load(req, &path, &again).ok());
    EXPECT_EQ(0, again.tablets);
    // after a compaction of the empty rowsets the evidence is still there
    // the evidence is lost: it is refused, not loaded over
    {
        std::lock_guard lock(loaded->get_header_lock());
        loaded->tablet_meta()->clear_restore_stage();
        loaded->save_meta();
    }
    Status st = loader.make_local_source_snapshot_and_load(req, &path, nullptr);
    EXPECT_TRUE(not_retryable(st)) << st;
}

// compaction keeps the evidence, the append is judged by the max version, not by the rowsets
TEST_F(RestoreStageTest, CompactionBetweenLoadAndAppendDoesNotChangeTheJudgement) {
    Prepared p = prepare(Kind::DUP, 7761);
    SnapshotLoader loader(*_engine, ExecEnv::GetInstance(), 1L, 2L);
    ASSERT_TRUE(load(p, loader, 1, nullptr).ok());
    auto loaded = current(p.target);
    // compact the rowsets of version 2 and 3 into one, as a compaction of the staging tablet would
    compact(loaded, loaded->tablet_schema(), 2, 3);
    EXPECT_EQ(3, loaded->max_version_unlocked());
    EXPECT_TRUE(loaded->tablet_meta()->has_restore_stage());
    put_increment(p.backup, p.target, p.dir, 3, 6);
    Status st = loader.append_increment(p.dir, loaded, p.range, &p.backup_digest);
    ASSERT_TRUE(st.ok()) << st;
    EXPECT_EQ(6, loaded->max_version_unlocked());
    EXPECT_EQ(RestoreStagePB::APPENDED, stage_of(p.target).stage());
    EXPECT_TRUE(digests_equal(tablet_digest(p.backup->tablet_id(), 6),
                              tablet_digest(p.target->tablet_id(), 6)));
}

// the append branches 1 and 2, then the load resent after the append
TEST_F(RestoreStageTest, AppendWritesTheEvidenceWithTheAppendAndTheResentTaskIsOk) {
    Prepared p = prepare(Kind::DUP, 7771);
    SnapshotLoader loader(*_engine, ExecEnv::GetInstance(), 1L, 2L);
    ASSERT_TRUE(load(p, loader, 1, nullptr).ok());
    auto loaded = current(p.target);
    put_increment(p.backup, p.target, p.dir, 3, 6);
    ASSERT_TRUE(loader.append_increment(p.dir, loaded, p.range, &p.backup_digest).ok());
    RestoreStagePB stage = stage_of(p.target);
    EXPECT_EQ(RestoreStagePB::APPENDED, stage.stage());
    EXPECT_EQ(1, stage.job_id());
    EXPECT_EQ(3, stage.base_version());
    EXPECT_EQ(6, stage.end_version());
    // the evidence is in the saved tablet meta, not only in memory
    {
        std::string binary;
        loaded->tablet_meta()->serialize(&binary);
        TabletMeta saved;
        ASSERT_TRUE(saved.deserialize(binary).ok());
        EXPECT_EQ(RestoreStagePB::APPENDED, saved.restore_stage().stage());
    }
    auto files = tablet_files(loaded);
    // branch 1 again (no digest needed)
    EXPECT_TRUE(loader.append_increment(p.dir, loaded, p.range).ok());
    EXPECT_EQ(files, tablet_files(loaded));
    // even if the snapshot dir is gone
    std::filesystem::remove_all(p.dir);
    EXPECT_TRUE(loader.append_increment(p.dir, loaded, p.range).ok());
    // the snapshot task resent after the append: skipped, the tablet is not replaced
    LocalSourceStats again;
    ASSERT_TRUE(load(p, loader, 1, &again).ok());
    EXPECT_EQ(0, again.tablets);
    EXPECT_EQ(6, current(p.target)->max_version_unlocked());
    // the append of another range or by another job is refused
    TRestoreIncrementalRange other = p.range;
    other.end_version = 7;
    EXPECT_TRUE(not_retryable(loader.append_increment(p.dir, loaded, other)));
    SnapshotLoader other_job(*_engine, ExecEnv::GetInstance(), 5L, 2L);
    EXPECT_TRUE(not_retryable(other_job.append_increment(p.dir, loaded, p.range)));
}

TEST_F(RestoreStageTest, AppendWithoutTheEvidenceOfTheJobIsRefusedAndNothingChanges) {
    Prepared p = prepare(Kind::DUP, 7781);
    SnapshotLoader loader(*_engine, ExecEnv::GetInstance(), 1L, 2L);
    ASSERT_TRUE(load(p, loader, 1, nullptr).ok());
    auto loaded = current(p.target);
    put_increment(p.backup, p.target, p.dir, 3, 6);
    auto files = tablet_files(loaded);
    // another job
    SnapshotLoader other(*_engine, ExecEnv::GetInstance(), 2L, 2L);
    EXPECT_TRUE(not_retryable(other.append_increment(p.dir, loaded, p.range, &p.backup_digest)));
    // loaded from another base version than the one the increment starts from
    TRestoreIncrementalRange wrong = p.range;
    wrong.base_version = 2;
    EXPECT_TRUE(not_retryable(loader.append_increment(p.dir, loaded, wrong, &p.backup_digest)));
    // no evidence at all
    {
        std::lock_guard lock(loaded->get_header_lock());
        loaded->tablet_meta()->clear_restore_stage();
        loaded->save_meta();
    }
    EXPECT_TRUE(not_retryable(loader.append_increment(p.dir, loaded, p.range, &p.backup_digest)));
    // the evidence is there, but the snapshot dir does not hold an increment up to the version asked: refused by
    // the check of the dir (it may be a resent task of a dir not downloaded yet), nothing changes
    mark_loaded(loaded, 1, 3);
    TRestoreIncrementalRange later = p.range;
    later.end_version = 9;
    EXPECT_FALSE(loader.append_increment(p.dir, loaded, later, &p.backup_digest).ok());
    EXPECT_EQ(files, tablet_files(loaded));
    EXPECT_EQ(3, loaded->max_version_unlocked());
    // and with the evidence it works
    EXPECT_TRUE(loader.append_increment(p.dir, loaded, p.range, &p.backup_digest).ok());
}

// the increment is in the tablet but the evidence of it is not (debug point: the append is done without it and
// fails): branch 3, the digest of the tablet is checked against the one of the backup
TEST_F(RestoreStageTest, AppendedWithoutEvidenceIsOkWhenTheDigestEqualsTheBackup) {
    Prepared p = prepare(Kind::DUP, 7791);
    debug_on();
    Defer defer([this]() { debug_off(); });
    SnapshotLoader loader(*_engine, ExecEnv::GetInstance(), 1L, 2L);
    ASSERT_TRUE(load(p, loader, 1, nullptr).ok());
    auto loaded = current(p.target);
    put_increment(p.backup, p.target, p.dir, 3, 6);
    DebugPoints::instance()->add_with_params(
            "SnapshotLoader.append_increment.fail_before_stage",
            {{"tablet_id", std::to_string(p.target->tablet_id())}});
    Status st = loader.append_increment(p.dir, loaded, p.range, &p.backup_digest);
    EXPECT_FALSE(st.ok());
    EXPECT_FALSE(not_retryable(st)) << st;
    DebugPoints::instance()->clear();
    // the data is appended, the evidence is still the one of the load
    EXPECT_EQ(6, loaded->max_version_unlocked());
    EXPECT_EQ(RestoreStagePB::LOCAL_LOADED, stage_of(p.target).stage());
    // resent: the digest equals the backup: OK and the evidence is written
    st = loader.append_increment(p.dir, loaded, p.range, &p.backup_digest);
    ASSERT_TRUE(st.ok()) << st;
    EXPECT_EQ(RestoreStagePB::APPENDED, stage_of(p.target).stage());
    EXPECT_EQ(6, stage_of(p.target).end_version());
    EXPECT_EQ(6, loaded->max_version_unlocked());
    // once more: branch 1
    EXPECT_TRUE(loader.append_increment(p.dir, loaded, p.range).ok());
}

TEST_F(RestoreStageTest, AppendedWithoutEvidenceFailsWhenTheDigestDiffersOrIsMissing) {
    Prepared p = prepare(Kind::DUP, 7801);
    debug_on();
    Defer defer([this]() { debug_off(); });
    SnapshotLoader loader(*_engine, ExecEnv::GetInstance(), 1L, 2L);
    ASSERT_TRUE(load(p, loader, 1, nullptr).ok());
    auto loaded = current(p.target);
    put_increment(p.backup, p.target, p.dir, 3, 6);
    DebugPoints::instance()->add("SnapshotLoader.append_increment.fail_before_stage");
    EXPECT_FALSE(loader.append_increment(p.dir, loaded, p.range, &p.backup_digest).ok());
    DebugPoints::instance()->clear();
    ASSERT_EQ(6, loaded->max_version_unlocked());

    // no digest of the backup
    EXPECT_TRUE(not_retryable(loader.append_increment(p.dir, loaded, p.range)));
    // the digest of the backup is another one (another version of it)
    TRestoreBackupDigest other = digest_of(p.backup, 3);
    EXPECT_TRUE(not_retryable(loader.append_increment(p.dir, loaded, p.range, &other)));
    // the algorithm, the schema, the status
    TRestoreBackupDigest bad = p.backup_digest;
    bad.__set_algo_version(bad.algo_version + 1);
    EXPECT_TRUE(not_retryable(loader.append_increment(p.dir, loaded, p.range, &bad)));
    bad = p.backup_digest;
    bad.__set_schema_sig("0000");
    EXPECT_TRUE(not_retryable(loader.append_increment(p.dir, loaded, p.range, &bad)));
    bad = p.backup_digest;
    bad.__set_status_code("ERROR");
    EXPECT_TRUE(not_retryable(loader.append_increment(p.dir, loaded, p.range, &bad)));
    bad = p.backup_digest;
    bad.__isset.root = false;
    EXPECT_TRUE(not_retryable(loader.append_increment(p.dir, loaded, p.range, &bad)));
    // none of them wrote the evidence
    EXPECT_EQ(RestoreStagePB::LOCAL_LOADED, stage_of(p.target).stage());
    // the right digest works
    EXPECT_TRUE(loader.append_increment(p.dir, loaded, p.range, &p.backup_digest).ok());
}

// a unique key tablet: the digest does not prove the hidden state, so it fails even with an equal digest
TEST_F(RestoreStageTest, AppendedWithoutEvidenceFailsForUniqueKeyTablets) {
    Prepared p = prepare(Kind::MOW, 7811);
    debug_on();
    Defer defer([this]() { debug_off(); });
    SnapshotLoader loader(*_engine, ExecEnv::GetInstance(), 1L, 2L);
    ASSERT_TRUE(load(p, loader, 1, nullptr).ok());
    auto loaded = current(p.target);
    put_increment(p.backup, p.target, p.dir, 3, 6);
    DebugPoints::instance()->add("SnapshotLoader.append_increment.fail_before_stage");
    EXPECT_FALSE(loader.append_increment(p.dir, loaded, p.range, &p.backup_digest).ok());
    DebugPoints::instance()->clear();
    ASSERT_EQ(6, loaded->max_version_unlocked());
    EXPECT_TRUE(digests_equal(tablet_digest(p.backup->tablet_id(), 6),
                              tablet_digest(p.target->tablet_id(), 6)));
    Status st = loader.append_increment(p.dir, loaded, p.range, &p.backup_digest);
    EXPECT_TRUE(not_retryable(st)) << st;
    EXPECT_EQ(RestoreStagePB::LOCAL_LOADED, stage_of(p.target).stage());
}

// the failure of the append keeps the evidence of the load, so the retry appends
TEST_F(RestoreStageTest, FailedAppendKeepsTheEvidenceOfTheLoadAndTheRetryAppends) {
    Prepared p = prepare(Kind::DUP, 7821);
    debug_on();
    Defer defer([this]() { debug_off(); });
    SnapshotLoader loader(*_engine, ExecEnv::GetInstance(), 1L, 2L);
    ASSERT_TRUE(load(p, loader, 1, nullptr).ok());
    auto loaded = current(p.target);
    put_increment(p.backup, p.target, p.dir, 3, 6);
    DebugPoints::instance()->add_with_params(
            "Tablet.revise_tablet_meta_fail",
            {{"tablet_id", std::to_string(p.target->tablet_id())}});
    Status st = loader.append_increment(p.dir, loaded, p.range, &p.backup_digest);
    EXPECT_FALSE(st.ok());
    EXPECT_FALSE(not_retryable(st)) << st;
    EXPECT_EQ(3, loaded->max_version_unlocked());
    EXPECT_EQ(RestoreStagePB::LOCAL_LOADED, stage_of(p.target).stage());
    DebugPoints::instance()->clear();
    ASSERT_TRUE(loader.append_increment(p.dir, loaded, p.range, &p.backup_digest).ok());
    EXPECT_EQ(6, loaded->max_version_unlocked());
    EXPECT_EQ(RestoreStagePB::APPENDED, stage_of(p.target).stage());
}

// the delay debug points: for the tablet and the backend they are set for
TEST_F(RestoreStageTest, DelayDebugPointsDelayTheTaskOfTheTabletOnly) {
    Prepared p = prepare(Kind::DUP, 7831);
    debug_on();
    Defer defer([this]() { debug_off(); });
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
    EXPECT_LT(elapsed_ms([&]() { ASSERT_TRUE(load(p, loader, 1, nullptr).ok()); }), 400);
    DebugPoints::instance()->add_with_params("SnapshotLoader.make_local_source.delay",
                                             {{"sleep_ms", "400"}, {"backend", "no.such.host"}});
    EXPECT_LT(elapsed_ms([&]() { ASSERT_TRUE(load(p, loader, 1, nullptr).ok()); }), 400);
    // the tablet
    DebugPoints::instance()->add_with_params(
            "SnapshotLoader.make_local_source.delay",
            {{"sleep_ms", "400"}, {"tablet_id", std::to_string(p.target->tablet_id())}});
    EXPECT_GE(elapsed_ms([&]() { ASSERT_TRUE(load(p, loader, 1, nullptr).ok()); }), 400);
    DebugPoints::instance()->clear();

    auto loaded = current(p.target);
    put_increment(p.backup, p.target, p.dir, 3, 6);
    DebugPoints::instance()->add_with_params(
            "SnapshotLoader.append_increment.delay",
            {{"sleep_ms", "400"}, {"backend", BackendOptions::get_localhost()}});
    EXPECT_GE(
            elapsed_ms([&]() {
                ASSERT_TRUE(loader.append_increment(p.dir, loaded, p.range, &p.backup_digest).ok());
            }),
            400);
}

} // namespace doris
