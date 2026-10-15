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

#include <fmt/format.h>
#include <gen_cpp/olap_file.pb.h>
#include <glog/logging.h>
#include <xxh3.h>

#include <algorithm>
#include <atomic>
#include <bit>
#include <chrono>
#include <cstring>
#include <map>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <string_view>
#include <thread>
#include <tuple>

#include "common/config.h"
#include "common/consts.h"
#include "common/status.h"
#include "core/block/block.h"
#include "core/column/column.h"
#include "core/column/column_array.h"
#include "core/column/column_map.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/column/column_struct.h"
#include "core/value/vdatetime_value.h"
#include "runtime/memory/mem_tracker_limiter.h"
#include "runtime/thread_context.h"
#include "storage/delete/delete_handler.h"
#include "storage/iterator/block_reader.h"
#include "storage/olap_common.h"
#include "storage/rowset/rowset.h"
#include "storage/rowset/rowset_reader.h"
#include "storage/rowset/rowset_reader_context.h"
#include "storage/schema.h"
#include "storage/storage_engine.h"
#include "storage/tablet/tablet.h"
#include "storage/tablet/tablet_manager.h"
#include "storage/tablet/tablet_meta.h"
#include "storage/tablet/tablet_reader.h"
#include "storage/utils.h"
#include "util/sha.h"

namespace doris {

static_assert(std::endian::native == std::endian::little,
              "restore digest encoding assumes a little endian host");
static_assert(sizeof(VecDateTimeValue) == 8, "DATE/DATETIME v1 are 8 bytes in memory");

namespace restore_digest_detail {

enum class ColRole : uint8_t { DIGEST, DELETE_SIGN, EXCLUDED };
enum class ColKind : uint8_t {
    FIXED,
    STRING,
    CHAR,
    DATE_V1,
    DATETIME_V1,
    ARRAY,
    MAP,
    STRUCT,
};

struct ColCodec {
    ColKind kind = ColKind::FIXED;
    uint8_t width = 0; // for FIXED
    // ARRAY: the element, MAP: key and value, STRUCT: the fields
    std::vector<ColCodec> children;
};

// Whether the column type is supported, and how it is encoded. `path` names the column for the
// error message (nested elements are reported as a.b).
Status codec_of(const TabletColumn& col, const std::string& path, ColCodec* codec) {
    auto fixed = [&](uint8_t w) {
        codec->kind = ColKind::FIXED;
        codec->width = w;
        return Status::OK();
    };
    auto nested = [&](size_t expect_min, size_t expect_max) -> Status {
        const auto& subs = col.get_sub_columns();
        if (subs.size() < expect_min || subs.size() > expect_max) {
            return Status::NotSupported("restore digest: column {} has {} sub columns", path,
                                        subs.size());
        }
        codec->children.resize(subs.size());
        for (size_t i = 0; i < subs.size(); ++i) {
            RETURN_IF_ERROR(codec_of(*subs[i], fmt::format("{}.{}", path, i), &codec->children[i]));
        }
        return Status::OK();
    };
    switch (col.type()) {
    case FieldType::OLAP_FIELD_TYPE_BOOL:
    case FieldType::OLAP_FIELD_TYPE_TINYINT:
        return fixed(1);
    case FieldType::OLAP_FIELD_TYPE_SMALLINT:
        return fixed(2);
    case FieldType::OLAP_FIELD_TYPE_INT:
    case FieldType::OLAP_FIELD_TYPE_FLOAT:
    case FieldType::OLAP_FIELD_TYPE_DECIMAL32:
    case FieldType::OLAP_FIELD_TYPE_DATEV2:
    case FieldType::OLAP_FIELD_TYPE_IPV4:
        return fixed(4);
    case FieldType::OLAP_FIELD_TYPE_BIGINT:
    case FieldType::OLAP_FIELD_TYPE_DOUBLE:
    case FieldType::OLAP_FIELD_TYPE_DECIMAL64:
    case FieldType::OLAP_FIELD_TYPE_DATETIMEV2:
        return fixed(8);
    case FieldType::OLAP_FIELD_TYPE_LARGEINT:
    case FieldType::OLAP_FIELD_TYPE_DECIMAL128I:
    case FieldType::OLAP_FIELD_TYPE_DECIMAL: // DECIMALV2, int128 in memory
    case FieldType::OLAP_FIELD_TYPE_IPV6:
        return fixed(16);
    case FieldType::OLAP_FIELD_TYPE_DECIMAL256:
        return fixed(32);
    case FieldType::OLAP_FIELD_TYPE_VARCHAR:
    case FieldType::OLAP_FIELD_TYPE_STRING:
    case FieldType::OLAP_FIELD_TYPE_JSONB:
        codec->kind = ColKind::STRING;
        return Status::OK();
    case FieldType::OLAP_FIELD_TYPE_CHAR:
        codec->kind = ColKind::CHAR;
        return Status::OK();
    case FieldType::OLAP_FIELD_TYPE_DATE:
        codec->kind = ColKind::DATE_V1;
        return Status::OK();
    case FieldType::OLAP_FIELD_TYPE_DATETIME:
        codec->kind = ColKind::DATETIME_V1;
        return Status::OK();
    case FieldType::OLAP_FIELD_TYPE_ARRAY:
        codec->kind = ColKind::ARRAY;
        return nested(1, 1);
    case FieldType::OLAP_FIELD_TYPE_MAP:
        codec->kind = ColKind::MAP;
        return nested(2, 2);
    case FieldType::OLAP_FIELD_TYPE_STRUCT:
        codec->kind = ColKind::STRUCT;
        return nested(1, SIZE_MAX);
    default:
        return Status::NotSupported("restore digest: column {} has unsupported type {}", path,
                                    TabletColumn::get_string_by_field_type(col.type()));
    }
}

// Part of the schema signature: the type, precision and scale, and for nested types the same
// for every sub column. Scalars keep the exact text of the first cut.
void append_type_sig(std::string& sig, const TabletColumn& col) {
    sig += fmt::format("{}:{}:{}", static_cast<int>(col.type()), col.precision(), col.frac());
    const auto& subs = col.get_sub_columns();
    if (!subs.empty()) {
        sig += '<';
        for (size_t i = 0; i < subs.size(); ++i) {
            if (i > 0) {
                sig += ',';
            }
            append_type_sig(sig, *subs[i]);
        }
        sig += '>';
    }
}

// Classifies a schema column; returns NotSupported for columns the algorithm can not digest.
Status classify_column(const TabletColumn& col, KeysType keys_type, ColRole* role,
                       ColCodec* codec) {
    const std::string& name = col.name();
    if (name == DELETE_SIGN && keys_type == UNIQUE_KEYS) {
        *role = ColRole::DELETE_SIGN;
        return Status::OK();
    }
    if (name == VERSION_COL || name == COMMIT_TSO_COL || name == SKIP_BITMAP_COL ||
        name == BeConsts::ROW_STORE_COL) {
        *role = ColRole::EXCLUDED;
        return Status::OK();
    }
    if (name == BINLOG_TSO_COL || name == BINLOG_LSN_COL || name == BINLOG_OP_COL ||
        name == ROW_LSN_COL) {
        return Status::NotSupported("restore digest: row binlog column {}", name);
    }
    if (name != SEQUENCE_COL && name != DELETE_SIGN && name.starts_with("__DORIS_")) {
        return Status::NotSupported("restore digest: unknown hidden column {}", name);
    }
    RETURN_IF_ERROR(codec_of(col, name, codec));
    *role = ColRole::DIGEST;
    return Status::OK();
}

struct ColumnPlan {
    std::vector<uint32_t> digest_ordinals; // ordinals in the schema, schema order
    std::vector<ColCodec> codecs;          // parallel to digest_ordinals
    int32_t delete_sign_ordinal = -1;
    std::string schema_sig;
};

Status build_plan(const TabletSchema& schema, KeysType keys_type, bool enable_mow,
                  ColumnPlan* plan) {
    if (keys_type == DUP_KEYS) {
        // supported
    } else if (keys_type == UNIQUE_KEYS) {
        // MoW and MoR are both supported
    } else {
        return Status::NotSupported("restore digest: keys type {} is not supported",
                                    static_cast<int>(keys_type));
    }
    if (!schema.cluster_key_uids().empty()) {
        return Status::NotSupported("restore digest: cluster key is not supported");
    }
    if (schema.has_seq_map()) {
        // The merge of a sequence mapping table (BlockReader::_replace_key_next_block) replaces
        // value column groups by their own sequence column and does not drop delete sign rows, so
        // neither the checksum reader nor the MoW direct read gives a defined visible row set.
        return Status::NotSupported("restore digest: sequence mapping is not supported");
    }
    if (schema.binlog_tso_col_idx() != -1 || schema.binlog_lsn_col_idx() != -1 ||
        schema.binlog_op_col_idx() != -1) {
        return Status::NotSupported("restore digest: row binlog is not supported");
    }

    SHA256Digest sig;
    std::string sig_data = fmt::format("restore-digest-schema|algo={}|keys_type={}|mow={}",
                                       RestoreDigest::kAlgoVersion, static_cast<int>(keys_type),
                                       enable_mow ? 1 : 0);
    const auto& columns = schema.columns();
    for (uint32_t i = 0; i < columns.size(); ++i) {
        const TabletColumn& col = *columns[i];
        ColRole role = ColRole::DIGEST;
        ColCodec codec;
        RETURN_IF_ERROR(classify_column(col, keys_type, &role, &codec));
        if (role == ColRole::DELETE_SIGN) {
            plan->delete_sign_ordinal = static_cast<int32_t>(i);
        } else if (role == ColRole::DIGEST) {
            plan->digest_ordinals.push_back(i);
            plan->codecs.push_back(std::move(codec));
            sig_data += '|';
            append_type_sig(sig_data, col);
            sig_data += fmt::format(":{}", col.is_key() ? 1 : 0);
        }
    }
    if (keys_type == UNIQUE_KEYS && plan->delete_sign_ordinal < 0) {
        return Status::NotSupported("restore digest: unique table without delete sign column");
    }
    if (plan->digest_ordinals.empty()) {
        return Status::NotSupported("restore digest: no column takes part in the digest");
    }
    sig.reset(sig_data.data(), sig_data.size());
    plan->schema_sig = std::string(sig.digest());
    return Status::OK();
}

// A read-only view of one column of a block.
struct ColView {
    ColKind kind = ColKind::FIXED;
    uint8_t width = 0;
    const uint8_t* null_map = nullptr;
    const char* data = nullptr;
    const ColumnString* str = nullptr;
    const IColumn* col = nullptr;      // the column without its top level Nullable wrapper
    const ColCodec* codec = nullptr;
};

Status make_view(const ColumnPtr& column, const ColCodec& codec, size_t rows, ColView* view) {
    const IColumn* c = column.get();
    view->kind = codec.kind;
    view->width = codec.width;
    view->null_map = nullptr;
    view->codec = &codec;
    if (const auto* nullable = check_and_get_column<ColumnNullable>(c)) {
        view->null_map = nullable->get_null_map_data().data();
        c = &nullable->get_nested_column();
    }
    view->col = c;
    if (c->size() != rows) {
        return Status::InternalError("restore digest: column {} has {} elements for {} rows",
                                     c->get_name(), c->size(), rows);
    }
    if (codec.kind == ColKind::FIXED) {
        // get_raw_data() reports the element count in `size`, not bytes
        StringRef raw = c->get_raw_data();
        if (raw.size != rows) {
            return Status::InternalError(
                    "restore digest: column {} has {} elements for {} rows, expect width {}",
                    c->get_name(), raw.size, rows, codec.width);
        }
        view->data = raw.data;
    } else if (codec.kind == ColKind::STRING || codec.kind == ColKind::CHAR) {
        view->str = check_and_get_column<ColumnString>(c);
        if (view->str == nullptr) {
            return Status::InternalError("restore digest: expect a string column but got {}",
                                         c->get_name());
        }
    } else if (codec.kind == ColKind::DATE_V1 || codec.kind == ColKind::DATETIME_V1) {
        StringRef raw = c->get_raw_data();
        if (raw.size != rows) {
            return Status::InternalError("restore digest: date column {} has {} raw elements",
                                         c->get_name(), raw.size);
        }
        view->data = raw.data;
    }
    return Status::OK();
}

inline void append_u32(std::string& buf, uint32_t v) {
    buf.append(reinterpret_cast<const char*>(&v), sizeof(v));
}
inline void append_u64(std::string& buf, uint64_t v) {
    buf.append(reinterpret_cast<const char*>(&v), sizeof(v));
}

constexpr XXH64_hash_t kHashSeed = RestoreDigest::kAlgoVersion;

bool encode_slot(const IColumn& col, const ColCodec& codec, size_t row, std::string& buf);

inline void encode_date_v1(const char* raw8, bool with_time, std::string& buf) {
    VecDateTimeValue v;
    std::memcpy(&v, raw8, sizeof(v));
    const uint16_t year = v.year();
    buf.append(reinterpret_cast<const char*>(&year), sizeof(year));
    buf.push_back(static_cast<char>(v.month()));
    buf.push_back(static_cast<char>(v.day()));
    if (with_time) {
        buf.push_back(static_cast<char>(v.hour()));
        buf.push_back(static_cast<char>(v.minute()));
        buf.push_back(static_cast<char>(v.second()));
    }
}

// Appends the encoding of a non-NULL value (no flag byte). Returns false if the column object is
// not what the schema promises.
bool encode_value(const IColumn& col, const ColCodec& codec, size_t row, std::string& buf) {
    switch (codec.kind) {
    case ColKind::FIXED: {
        StringRef raw = col.get_raw_data();
        if (raw.size <= row) {
            return false;
        }
        buf.append(raw.data + row * codec.width, codec.width);
        return true;
    }
    case ColKind::STRING:
    case ColKind::CHAR: {
        StringRef s = col.get_data_at(row);
        size_t len = s.size;
        if (codec.kind == ColKind::CHAR) {
            while (len > 0 && s.data[len - 1] == '\0') {
                --len;
            }
        }
        append_u32(buf, static_cast<uint32_t>(len));
        buf.append(s.data, len);
        return true;
    }
    case ColKind::DATE_V1:
    case ColKind::DATETIME_V1: {
        StringRef raw = col.get_raw_data();
        if (raw.size <= row) {
            return false;
        }
        encode_date_v1(raw.data + row * sizeof(VecDateTimeValue), codec.kind == ColKind::DATETIME_V1,
                       buf);
        return true;
    }
    case ColKind::ARRAY: {
        const auto* arr = check_and_get_column<ColumnArray>(&col);
        if (arr == nullptr || codec.children.size() != 1) {
            return false;
        }
        const size_t begin = arr->offset_at(row);
        const size_t n = arr->size_at(row);
        append_u64(buf, n);
        for (size_t i = 0; i < n; ++i) {
            if (!encode_slot(arr->get_data(), codec.children[0], begin + i, buf)) {
                return false;
            }
        }
        return true;
    }
    case ColKind::MAP: {
        const auto* map = check_and_get_column<ColumnMap>(&col);
        if (map == nullptr || codec.children.size() != 2) {
            return false;
        }
        const size_t begin = map->offset_at(row);
        const size_t n = map->size_at(row);
        append_u64(buf, n);
        for (size_t i = 0; i < n; ++i) {
            if (!encode_slot(map->get_keys(), codec.children[0], begin + i, buf) ||
                !encode_slot(map->get_values(), codec.children[1], begin + i, buf)) {
                return false;
            }
        }
        return true;
    }
    case ColKind::STRUCT: {
        const auto* st = check_and_get_column<ColumnStruct>(&col);
        if (st == nullptr || st->tuple_size() != codec.children.size()) {
            return false;
        }
        for (size_t i = 0; i < codec.children.size(); ++i) {
            if (!encode_slot(st->get_column(i), codec.children[i], row, buf)) {
                return false;
            }
        }
        return true;
    }
    }
    return false;
}

// A nested element: a flag byte, then the value if it is not NULL.
bool encode_slot(const IColumn& col, const ColCodec& codec, size_t row, std::string& buf) {
    const IColumn* c = &col;
    if (const auto* nullable = check_and_get_column<ColumnNullable>(c)) {
        if (nullable->is_null_at(row)) {
            buf.push_back('\x00');
            return true;
        }
        c = &nullable->get_nested_column();
    }
    buf.push_back('\x01');
    return encode_value(*c, codec, row, buf);
}

// Encodes one row, appending to `buf`. The encoding is injective for a fixed schema: every field
// is a flag byte followed by either a fixed number of bytes or a length / count prefixed value.
// Returns false if a column does not have the shape the schema promises.
inline bool encode_row(const std::vector<ColView>& views, size_t row, std::string& buf) {
    for (const ColView& v : views) {
        if (v.null_map != nullptr && v.null_map[row] != 0) {
            buf.push_back('\x00');
            continue;
        }
        buf.push_back('\x01');
        if (v.kind == ColKind::FIXED) {
            buf.append(v.data + row * v.width, v.width);
        } else if (v.kind == ColKind::STRING || v.kind == ColKind::CHAR) {
            StringRef s = v.str->get_data_at(row);
            size_t len = s.size;
            if (v.kind == ColKind::CHAR) {
                while (len > 0 && s.data[len - 1] == '\0') {
                    --len;
                }
            }
            append_u32(buf, static_cast<uint32_t>(len));
            buf.append(s.data, len);
        } else if (v.kind == ColKind::DATE_V1 || v.kind == ColKind::DATETIME_V1) {
            encode_date_v1(v.data + row * sizeof(VecDateTimeValue), v.kind == ColKind::DATETIME_V1,
                           buf);
        } else if (!encode_value(*v.col, *v.codec, row, buf)) {
            return false;
        }
    }
    return true;
}

// Hashes the visible rows of blocks into a partial digest. One instance per worker.
class BlockHasher {
public:
    explicit BlockHasher(const ColumnPlan& plan) : _plan(plan), _views(plan.digest_ordinals.size()) {
        _row_buf.reserve(256);
    }

    // The block has the digest columns first, then (when the schema has one) the delete sign.
    Status add_block(const Block& block, RestoreDigest* acc) {
        const size_t n = block.rows();
        if (n == 0) {
            return Status::OK();
        }
        const size_t num_digest = _plan.digest_ordinals.size();
        std::vector<ColumnPtr> keep_alive;
        keep_alive.reserve(num_digest + 1);
        for (size_t j = 0; j < num_digest; ++j) {
            keep_alive.push_back(
                    block.get_by_position(j).column->convert_to_full_column_if_const());
            RETURN_IF_ERROR(make_view(keep_alive.back(), _plan.codecs[j], n, &_views[j]));
        }
        ColView delete_sign_view;
        ColCodec delete_sign_codec;
        delete_sign_codec.kind = ColKind::FIXED;
        delete_sign_codec.width = 1;
        const bool has_delete_sign = _plan.delete_sign_ordinal >= 0;
        if (has_delete_sign) {
            keep_alive.push_back(
                    block.get_by_position(num_digest).column->convert_to_full_column_if_const());
            RETURN_IF_ERROR(make_view(keep_alive.back(), delete_sign_codec, n, &delete_sign_view));
        }
        acc->rows_scanned += n;
        for (size_t row = 0; row < n; ++row) {
            if (has_delete_sign &&
                ((delete_sign_view.null_map != nullptr && delete_sign_view.null_map[row] != 0) ||
                 delete_sign_view.data[row] != 0)) {
                continue;
            }
            _row_buf.clear();
            if (!encode_row(_views, row, _row_buf)) {
                return Status::InternalError(
                        "restore digest: a column does not have the shape its schema type needs");
            }
            XXH128_hash_t h = XXH3_128bits_withSeed(_row_buf.data(), _row_buf.size(), kHashSeed);
            auto& bucket = acc->buckets[h.high64 >> 56];
            bucket.sum += (static_cast<unsigned __int128>(h.high64) << 64) | h.low64;
            bucket.count++;
            acc->rows++;
            acc->encoded_bytes += _row_buf.size();
        }
        return Status::OK();
    }

private:
    const ColumnPlan& _plan;
    std::vector<ColView> _views;
    std::string _row_buf;
};

// Bucket sums are plain sums modulo 2^128, so partial digests just add up.
void merge_partial(const RestoreDigest& part, RestoreDigest* total) {
    for (size_t i = 0; i < RestoreDigest::kNumBuckets; ++i) {
        total->buckets[i].sum += part.buckets[i].sum;
        total->buckets[i].count += part.buckets[i].count;
    }
    total->rows += part.rows;
    total->rows_scanned += part.rows_scanned;
    total->encoded_bytes += part.encoded_bytes;
}

// The read schema: all digest columns, then the delete sign column.
ReadSchemaSPtr make_read_schema(const TabletSchema& schema, const ColumnPlan& plan) {
    std::vector<ColumnId> read_ordinals(plan.digest_ordinals.begin(), plan.digest_ordinals.end());
    if (plan.delete_sign_ordinal >= 0) {
        read_ordinals.push_back(static_cast<ColumnId>(plan.delete_sign_ordinal));
    }
    return std::make_shared<ReadSchema>(project_columns_by_ordinal(schema.columns(), read_ordinals));
}

struct DigestScanTask {
    size_t rowset_idx = 0;
    int64_t seg_begin = 0;
    int64_t seg_end = 0; // [seg_begin, seg_end); the whole rowset when both are 0
};

// What one scan of a part of a rowset sees: the version the delete bitmap and the delete
// conditions are taken at, and which of them.
struct ScanView {
    int64_t version = 0;
    // for MoW, the bitmap whose entries up to `version` apply; null otherwise
    DeleteBitmapPtr delete_bitmap;
    // the DELETE conditions with version <= `version`
    std::vector<RowsetMetaSharedPtr> delete_metas;
};

// Reads the visible rows of a part of one rowset (no merge) and hashes them.
Status run_scan(const RowsetSharedPtr& rowset, const TabletSchemaSPtr& schema, int batch_size,
                bool enable_mow, const ColumnPlan& plan, const ScanView& view,
                int64_t seg_begin, int64_t seg_end, RestoreDigest* acc) {
    // Every task owns its read schema and delete handler: appending the columns of delete
    // conditions mutates the schema, and the handler's predicates are not shared across readers
    // in the query path either.
    auto read_schema = make_read_schema(*schema, plan);
    DeleteHandler delete_handler;
    if (!view.delete_metas.empty()) {
        std::vector<TabletColumn> dropped_columns;
        RETURN_IF_ERROR(delete_handler.init(view.delete_metas, view.version, read_schema,
                                            &dropped_columns));
        read_schema->append_dropped_columns(std::move(dropped_columns));
    }
    RETURN_IF_ERROR(read_schema->init_from_tablet_schema(*schema,
                                                         /*merge_by_sequence_mapping=*/false,
                                                         /*map_row_binlog_columns=*/false));

    RowsetReaderSharedPtr reader;
    RETURN_IF_ERROR(rowset->create_reader(&reader));

    OlapReaderStatistics stats;
    RowsetReaderContext ctx;
    ctx.reader_type = ReaderType::READER_CHECKSUM;
    ctx.version = Version(0, view.version);
    ctx.need_ordered_result = false;
    ctx.read_schema = read_schema;
    ctx.stats = &stats;
    ctx.use_page_cache = false;
    ctx.batch_size = batch_size;
    ctx.enable_unique_key_merge_on_write = enable_mow;
    ctx.delete_bitmap = view.delete_bitmap;
    ctx.delete_handler = delete_handler.empty() ? nullptr : &delete_handler;
    RowSetSplits splits(reader);
    splits.segment_offsets = {seg_begin, seg_end};
    RETURN_IF_ERROR(reader->init(&ctx, splits));

    BlockHasher hasher(plan);
    while (true) {
        Block block = read_schema->create_read_block();
        Status st = reader->next_batch(&block);
        RETURN_IF_ERROR(hasher.add_block(block, acc));
        if (st.is<ErrorCode::END_OF_FILE>()) {
            return Status::OK();
        }
        RETURN_IF_ERROR(st);
    }
}

Status run_direct_task(const RestoreDigestInput& input, const ColumnPlan& plan,
                       const std::vector<RowsetMetaSharedPtr>& delete_metas, const DigestScanTask& task,
                       RestoreDigest* acc) {
    ScanView view;
    view.version = input.version;
    view.delete_bitmap = input.delete_bitmap;
    view.delete_metas = delete_metas;
    return run_scan(input.rowsets[task.rowset_idx], input.schema, input.batch_size,
                    input.enable_mow, plan, view, task.seg_begin, task.seg_end, acc);
}

// Unique MoR: the rows of all rowsets are merged by key with the reader the checksum task uses
// (highest version wins, sequence column first, delete sign rows dropped after the merge, delete
// conditions applied), then hashed. Single threaded: the merge needs all rowsets at once.
Status run_mor(const RestoreDigestInput& input, const ColumnPlan& plan, RestoreDigest* acc) {
    if (input.tablet == nullptr) {
        return Status::InvalidArgument("restore digest: unique MoR needs the tablet");
    }
    if (input.rowsets.empty()) {
        return Status::OK();
    }
    TabletReader::ReaderParams params;
    params.tablet = input.tablet;
    params.reader_type = ReaderType::READER_CHECKSUM;
    params.version =
            Version(input.rowsets.front()->start_version(), input.rowsets.back()->end_version());
    TabletReadSource read_source;
    for (const auto& rowset : input.rowsets) {
        RowsetReaderSharedPtr rs_reader;
        RETURN_IF_ERROR(rowset->create_reader(&rs_reader));
        read_source.rs_splits.emplace_back(std::move(rs_reader));
    }
    read_source.fill_delete_predicates();
    params.set_read_source(std::move(read_source), /*skip_delete_bitmap=*/true);
    params.tablet_schema = input.schema;
    params.read_schema = make_read_schema(*input.schema, plan);

    BlockReader reader;
    reader.set_batch_size(input.batch_size);
    RETURN_IF_ERROR(reader.init(params));
    Block block = params.read_schema->create_read_block();
    BlockHasher hasher(plan);
    bool eof = false;
    while (!eof) {
        RETURN_IF_ERROR(reader.next_block_with_aggregation(&block, &eof));
        RETURN_IF_ERROR(hasher.add_block(block, acc));
        block.clear_column_data();
    }
    return Status::OK();
}

// Runs `fn` for every task on up to `threads` workers. Each worker accumulates into its own
// partial digest; the partials are added to `total`.
template <typename Fn>
Status run_tasks(const std::vector<DigestScanTask>& tasks, int threads, Fn&& fn, RestoreDigest* total) {
    if (tasks.empty()) {
        return Status::OK();
    }
    const size_t workers = std::min<size_t>(static_cast<size_t>(std::max(1, threads)), tasks.size());
    if (workers == 1) {
        RestoreDigest local;
        for (const DigestScanTask& task : tasks) {
            RETURN_IF_ERROR(fn(task, &local));
        }
        merge_partial(local, total);
        return Status::OK();
    }
    std::atomic<size_t> next {0};
    std::atomic<bool> failed {false};
    std::mutex mu;
    Status first_error = Status::OK();
    std::vector<std::unique_ptr<RestoreDigest>> partials(workers);
    std::shared_ptr<MemTrackerLimiter> tracker = MemTrackerLimiter::create_shared(
            MemTrackerLimiter::Type::OTHER, "RestoreDigestWorkers");
    std::vector<std::thread> pool;
    pool.reserve(workers);
    for (size_t w = 0; w < workers; ++w) {
        partials[w] = std::make_unique<RestoreDigest>();
        pool.emplace_back([&, w] {
            SCOPED_ATTACH_TASK(tracker);
            while (!failed.load(std::memory_order_relaxed)) {
                size_t i = next.fetch_add(1);
                if (i >= tasks.size()) {
                    break;
                }
                Status st = fn(tasks[i], partials[w].get());
                if (!st.ok()) {
                    std::lock_guard lock(mu);
                    if (first_error.ok()) {
                        first_error = st;
                    }
                    failed.store(true);
                    break;
                }
            }
        });
    }
    for (auto& t : pool) {
        t.join();
    }
    RETURN_IF_ERROR(first_error);
    for (const auto& part : partials) {
        merge_partial(*part, total);
    }
    return Status::OK();
}


} // namespace restore_digest_detail
Status check_restore_digest_supported(const TabletSchema& schema, KeysType keys_type,
                                      bool enable_mow) {
    using namespace restore_digest_detail;
    ColumnPlan plan;
    return build_plan(schema, keys_type, enable_mow, &plan);
}

void RestoreDigest::finalize() {
    SHA256Digest sha;
    std::string header =
            fmt::format("DORIS-RESTORE-DIGEST|algo={}|buckets={}|", kAlgoVersion, kNumBuckets);
    sha.reset(header.data(), header.size());
    for (const Bucket& b : buckets) {
        uint64_t lo = static_cast<uint64_t>(b.sum);
        uint64_t hi = static_cast<uint64_t>(b.sum >> 64);
        sha.update(&lo, sizeof(lo));
        sha.update(&hi, sizeof(hi));
        sha.update(&b.count, sizeof(b.count));
    }
    root = std::string(sha.digest());
}

std::string RestoreDigest::to_json() const {
    std::string out;
    out.reserve(kNumBuckets * 64 + 512);
    out += fmt::format(
            "{{\"algo_version\":{},\"rows\":{},\"rows_scanned\":{},\"root\":\"{}\","
            "\"schema_sig\":\"{}\",\"rowset_count\":{},\"segment_count\":{},\"bytes_read\":{},"
            "\"encoded_bytes\":{},\"elapsed_ms\":{},\"threads\":{},\"delete_predicates\":{},"
            "\"buckets\":[",
            kAlgoVersion, rows, rows_scanned, root, schema_sig, rowset_count, segment_count,
            bytes_read, encoded_bytes, elapsed_ms, threads, delete_predicates);
    for (size_t i = 0; i < buckets.size(); ++i) {
        const Bucket& b = buckets[i];
        if (i > 0) {
            out += ',';
        }
        out += fmt::format("{{\"sum\":\"{:016x}{:016x}\",\"count\":{}}}",
                           static_cast<uint64_t>(b.sum >> 64), static_cast<uint64_t>(b.sum),
                           b.count);
    }
    out += "]}";
    return out;
}

Status compute_restore_digest(const RestoreDigestInput& input, RestoreDigest* digest) {
    using namespace restore_digest_detail;
    if (input.schema == nullptr || digest == nullptr) {
        return Status::InvalidArgument("restore digest: null schema or output");
    }
    auto start = std::chrono::steady_clock::now();
    ColumnPlan plan;
    RETURN_IF_ERROR(build_plan(*input.schema, input.keys_type, input.enable_mow, &plan));
    if (input.enable_mow && input.delete_bitmap == nullptr) {
        return Status::InvalidArgument("restore digest: MoW needs a delete bitmap snapshot");
    }
    const bool is_mor = input.keys_type == UNIQUE_KEYS && !input.enable_mow;

    RestoreDigest total;
    total.schema_sig = plan.schema_sig;

    std::vector<RowsetMetaSharedPtr> delete_metas;
    for (const RowsetSharedPtr& rowset : input.rowsets) {
        total.rowset_count++;
        total.segment_count += static_cast<uint32_t>(rowset->num_segments());
        total.bytes_read += rowset->data_disk_size();
        if (rowset->rowset_meta()->has_delete_predicate() &&
            rowset->version().first <= input.version) {
            delete_metas.push_back(rowset->rowset_meta());
        }
    }
    total.delete_predicates = static_cast<uint32_t>(delete_metas.size());

    if (is_mor) {
        total.threads = 1;
        RETURN_IF_ERROR(run_mor(input, plan, &total));
    } else {
        const int threads = std::max(1, input.threads);
        std::vector<DigestScanTask> tasks;
        for (size_t i = 0; i < input.rowsets.size(); ++i) {
            const RowsetSharedPtr& rowset = input.rowsets[i];
            if (rowset->num_rows() == 0) {
                continue;
            }
            const int64_t n = static_cast<int64_t>(rowset->num_segments());
            // Cut a rowset into runs of segments. Every task walks the delete bitmap of all the
            // segments of its rowset once, so the runs are kept long: about 4 tasks per worker.
            const int64_t chunk =
                    threads == 1 ? n : std::max<int64_t>(1, (n + threads * 4 - 1) / (threads * 4));
            for (int64_t b = 0; b < n; b += chunk) {
                DigestScanTask task;
                task.rowset_idx = i;
                const int64_t e = std::min(n, b + chunk);
                if (!(b == 0 && e == n)) {
                    task.seg_begin = b;
                    task.seg_end = e;
                }
                tasks.push_back(task);
            }
        }
        total.threads = static_cast<uint32_t>(
                std::min<size_t>(static_cast<size_t>(threads), std::max<size_t>(1, tasks.size())));
        RETURN_IF_ERROR(run_tasks(
                tasks, threads,
                [&](const DigestScanTask& task, RestoreDigest* acc) {
                    return run_direct_task(input, plan, delete_metas, task, acc);
                },
                &total));
    }

    total.finalize();
    total.elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                               std::chrono::steady_clock::now() - start)
                               .count();
    *digest = std::move(total);
    return Status::OK();
}

namespace {

using restore_digest_detail::ScanView;

// The part of a decomposed digest which is a scan: a part of one rowset seen at a version.
struct PrefixScanJob {
    size_t rowset_idx = 0;
    ScanView view;
    int64_t seg_begin = 0; // [seg_begin, seg_end), the whole rowset when both are 0
    int64_t seg_end = 0;
    size_t group = 0;      // the buckets this scan adds to
};

void add_buckets(DigestBuckets* dst, const DigestBuckets& src) {
    for (size_t i = 0; i < dst->size(); ++i) {
        (*dst)[i].sum += src[i].sum;
        (*dst)[i].count += src[i].count;
    }
}

void sub_buckets(DigestBuckets* dst, const DigestBuckets& src) {
    for (size_t i = 0; i < dst->size(); ++i) {
        (*dst)[i].sum -= src[i].sum;
        (*dst)[i].count -= src[i].count;
    }
}

bool buckets_empty(const DigestBuckets& b) {
    return std::all_of(b.begin(), b.end(),
                       [](const RestoreDigest::Bucket& x) { return x.count == 0 && x.sum == 0; });
}

// Cuts the segments of a rowset into runs the same way compute_restore_digest does.
std::vector<std::pair<int64_t, int64_t>> segment_runs(int64_t num_segments, int threads) {
    std::vector<std::pair<int64_t, int64_t>> runs;
    const int64_t chunk = threads == 1 ? num_segments
                                       : std::max<int64_t>(1, (num_segments + threads * 4 - 1) /
                                                                      (threads * 4));
    for (int64_t b = 0; b < num_segments; b += chunk) {
        const int64_t e = std::min(num_segments, b + chunk);
        runs.emplace_back(b == 0 && e == num_segments ? std::pair<int64_t, int64_t> {0, 0}
                                                      : std::pair<int64_t, int64_t> {b, e});
    }
    return runs;
}

// The scans which read a subset of the rows through a bitmap of their own have to stay out of the
// delete bitmap aggregation cache entries of the real tablet, whose key is (tablet id, rowset,
// segment, version). Every such bitmap gets a tablet id of its own.
int64_t next_scratch_bitmap_tablet_id() {
    static std::atomic<int64_t> next {-1};
    return next.fetch_sub(1);
}

} // namespace

namespace {

// Unique MoR: a whole digest at the base version and at the most recent rowset boundaries below it.
// The rowsets of every version are merged by key, nothing of one is reused for another.
Status decompose_mor(const RestoreDigestInput& input, int64_t tablet_id, const std::string& schema_sig,
                     int threads, RestoreDigestDecomposed* out) {
    using namespace restore_digest_detail;
    if (input.tablet == nullptr) {
        return Status::InvalidArgument("restore digest: unique MoR needs the tablet");
    }
    const int64_t boundaries = config::restore_digest_mor_prefix_boundaries;
    if (boundaries <= 0) {
        return Status::NotSupported(
                "restore digest: the decomposed digest of unique merge on read is disabled "
                "(restore_digest_mor_prefix_boundaries = 0)");
    }
    // prefixes[0] is the whole chain, then the shorter ones, the most recent first
    std::vector<size_t> prefixes {input.rowsets.size()};
    for (size_t n = input.rowsets.size(); n > 1 && static_cast<int64_t>(prefixes.size()) <= boundaries;
         --n) {
        prefixes.push_back(n - 1);
    }
    const auto start = std::chrono::steady_clock::now();
    std::vector<RestoreDigest> digests(prefixes.size());
    std::vector<DigestScanTask> tasks(prefixes.size());
    for (size_t i = 0; i < tasks.size(); ++i) {
        tasks[i].rowset_idx = i;
    }
    RestoreDigest unused;
    RETURN_IF_ERROR(run_tasks(
            tasks, std::min<int>(threads, static_cast<int>(tasks.size())),
            [&](const DigestScanTask& task, RestoreDigest*) {
                RestoreDigestInput sub = input;
                sub.rowsets.assign(input.rowsets.begin(),
                                   input.rowsets.begin() + static_cast<long>(prefixes[task.rowset_idx]));
                sub.version = sub.rowsets.back()->end_version();
                sub.threads = 1;
                return compute_restore_digest(sub, &digests[task.rowset_idx]);
            },
            &unused));

    RestoreDigestDecomposed result;
    result.algo_version = RestoreDigest::kAlgoVersion;
    result.schema_sig = schema_sig;
    result.mow = false;
    result.mor = true;
    result.tablet_id = tablet_id;
    result.base_version = input.version;
    for (const RowsetSharedPtr& rowset : input.rowsets) {
        RestoreDigestRowsetPart part;
        part.rowset_id = rowset->rowset_id().to_string();
        part.start_version = rowset->start_version();
        part.end_version = rowset->end_version();
        result.rowsets.push_back(std::move(part));
    }
    for (size_t i = prefixes.size(); i-- > 0;) {
        RestoreDigestWholePart whole;
        whole.version = input.rowsets[prefixes[i] - 1]->end_version();
        whole.buckets = digests[i].buckets;
        result.wholes.push_back(std::move(whole));
    }
    result.root = digests[0].root;
    result.rows = digests[0].rows;
    int64_t scanned = 0;
    for (const auto& d : digests) {
        scanned += static_cast<int64_t>(d.rows_scanned);
    }
    LOG(INFO) << "decomposed digest of the MoR tablet " << tablet_id << " at version " << input.version
              << ": " << prefixes.size() << " whole digests, " << scanned << " rows scanned in total, "
              << std::chrono::duration_cast<std::chrono::milliseconds>(
                         std::chrono::steady_clock::now() - start)
                         .count()
              << " ms, the base version alone took " << digests[0].elapsed_ms << " ms";
    *out = std::move(result);
    return Status::OK();
}

} // namespace

Status decompose_restore_digest(const RestoreDigestInput& input, int64_t tablet_id,
                                RestoreDigestDecomposed* out) {
    using namespace restore_digest_detail;
    if (input.schema == nullptr || out == nullptr) {
        return Status::InvalidArgument("restore digest: null schema or output");
    }
    ColumnPlan plan;
    RETURN_IF_ERROR(build_plan(*input.schema, input.keys_type, input.enable_mow, &plan));
    const bool is_mor = input.keys_type == UNIQUE_KEYS && !input.enable_mow;
    if (input.enable_mow && input.delete_bitmap == nullptr) {
        return Status::InvalidArgument("restore digest: MoW needs a delete bitmap snapshot");
    }
    if (input.rowsets.empty()) {
        return Status::InvalidArgument("restore digest: no rowset");
    }
    for (size_t i = 1; i < input.rowsets.size(); ++i) {
        if (input.rowsets[i]->end_version() <= input.rowsets[i - 1]->end_version()) {
            return Status::InvalidArgument("restore digest: the rowsets are not in version order");
        }
    }
    if (input.rowsets.back()->end_version() != input.version) {
        return Status::InvalidArgument(
                "restore digest: the last rowset ends at {}, not at the version {}",
                input.rowsets.back()->end_version(), input.version);
    }
    std::vector<RowsetMetaSharedPtr> delete_metas;
    for (const RowsetSharedPtr& rowset : input.rowsets) {
        if (rowset->rowset_meta()->has_delete_predicate() &&
            rowset->version().first <= input.version) {
            delete_metas.push_back(rowset->rowset_meta());
        }
    }
    const int threads = std::max(1, input.threads);
    if (is_mor) {
        return decompose_mor(input, tablet_id, plan.schema_sig, threads, out);
    }
    const int64_t max_scans = std::max<int64_t>(0, config::restore_digest_prefix_max_scans);

    // 1. Plan the scans.
    struct RowsetPlan {
        size_t base_group = 0;
        bool scanned = false;
        // (version, group) of the later scans, ascending: the delete conditions (duplicate), or the
        // death versions of rows (MoW)
        std::vector<std::pair<int64_t, size_t>> later;
        // MoW with DELETE conditions above the rowset: (version of the condition, group) of the level
        // scans, the rows alive after the condition. `later` then holds the bitmap deaths only.
        std::vector<std::pair<int64_t, size_t>> levels;
    };
    std::vector<RowsetPlan> plans(input.rowsets.size());
    std::vector<PrefixScanJob> jobs;
    size_t groups = 0;
    int64_t extra_scans = 0;
    auto add_job = [&](size_t rowset_idx, ScanView view, std::pair<int64_t, int64_t> run,
                       size_t group) {
        PrefixScanJob job;
        job.rowset_idx = rowset_idx;
        job.view = std::move(view);
        job.seg_begin = run.first;
        job.seg_end = run.second;
        job.group = group;
        jobs.push_back(std::move(job));
    };
    constexpr uint64_t kMaxSegmentRows = 0xFFFFFFF0ULL;

    for (size_t i = 0; i < input.rowsets.size(); ++i) {
        const RowsetSharedPtr& rowset = input.rowsets[i];
        if (rowset->num_rows() == 0) {
            continue;
        }
        const int64_t num_segments = static_cast<int64_t>(rowset->num_segments());
        const int64_t end = rowset->end_version();
        RowsetPlan& plan_i = plans[i];
        plan_i.scanned = true;

        // The rows alive at the end version of the rowset: no delete condition applies to the
        // rowset yet, the bitmap marks up to the end version do.
        plan_i.base_group = groups++;
        for (const auto& run : segment_runs(num_segments, threads)) {
            ScanView view;
            view.version = end;
            view.delete_bitmap = input.delete_bitmap;
            add_job(i, std::move(view), run, plan_i.base_group);
        }

        if (!input.enable_mow) {
            // Duplicate: the rows alive after each later DELETE condition. The rows removed by
            // the condition of version d are the difference with the previous level.
            for (const auto& meta : delete_metas) {
                const int64_t d = meta->version().first;
                if (d <= end) {
                    continue;
                }
                if (++extra_scans > max_scans) {
                    return Status::NotSupported(
                            "restore digest: the decomposed digest needs more than {} extra scans "
                            "for the delete conditions",
                            max_scans);
                }
                const size_t group = groups++;
                plan_i.later.emplace_back(d, group);
                for (const auto& run : segment_runs(num_segments, threads)) {
                    ScanView view;
                    view.version = d;
                    view.delete_metas = delete_metas;
                    add_job(i, std::move(view), run, group);
                }
            }
            continue;
        }

        // Unique MoW. The DELETE conditions above the rowset kill rows at their own versions: the rows
        // alive after each condition are read with a level scan, the rows killed by the bitmap
        // before a condition are the ones the bitmap scans below read.
        std::vector<int64_t> preds;
        for (const auto& meta : delete_metas) {
            if (meta->version().first > end) {
                preds.push_back(meta->version().first);
            }
        }
        std::sort(preds.begin(), preds.end());
        for (const int64_t d : preds) {
            if (++extra_scans > max_scans) {
                return Status::NotSupported(
                        "restore digest: the decomposed digest needs more than {} extra scans for "
                        "the delete conditions",
                        max_scans);
            }
            const size_t group = groups++;
            plan_i.levels.emplace_back(d, group);
            for (const auto& run : segment_runs(num_segments, threads)) {
                ScanView view;
                view.version = d;
                view.delete_bitmap = input.delete_bitmap;
                view.delete_metas = delete_metas;
                add_job(i, std::move(view), run, group);
            }
        }

        // Unique MoW: the rows of every segment by the first version above the end version at
        // which the delete bitmap marks them.
        std::map<int64_t, std::map<uint32_t, roaring::Roaring>> by_version;
        {
            const RowsetId& rid = rowset->rowset_id();
            std::shared_lock lock(input.delete_bitmap->lock);
            for (int64_t seg = 0; seg < num_segments; ++seg) {
                roaring::Roaring seen;
                const uint32_t seg_id = static_cast<uint32_t>(seg);
                for (auto it = input.delete_bitmap->delete_bitmap.lower_bound(DeleteBitmap::BitmapKey {rid, seg_id, 0});
                     it != input.delete_bitmap->delete_bitmap.end(); ++it) {
                    const auto& [key, bits] = *it;
                    if (std::get<0>(key) != rid || std::get<1>(key) != seg_id) {
                        break;
                    }
                    const int64_t v = static_cast<int64_t>(std::get<2>(key));
                    if (v > input.version) {
                        break;
                    }
                    if (v > end) {
                        roaring::Roaring fresh = bits;
                        fresh -= seen;
                        if (!fresh.isEmpty()) {
                            by_version[v][seg_id] |= fresh;
                        }
                    }
                    seen |= bits;
                }
            }
        }
        if (by_version.empty()) {
            continue;
        }
        std::vector<uint32_t> segment_rows;
        rowset->rowset_meta()->get_num_segment_rows(&segment_rows);
        for (const auto& [k, per_segment] : by_version) {
            // the rows killed by the bitmap at the version of a condition die there anyway
            if (std::binary_search(preds.begin(), preds.end(), k)) {
                continue;
            }
            if (++extra_scans > max_scans) {
                return Status::NotSupported(
                        "restore digest: the decomposed digest needs more than {} extra scans for "
                        "the delete bitmap versions",
                        max_scans);
            }
            // Read only the rows which die at k: the scan skips the rows of its bitmap, so the
            // bitmap is the complement of the set.
            const int64_t lo = static_cast<int64_t>(per_segment.begin()->first);
            const int64_t hi = static_cast<int64_t>(per_segment.rbegin()->first);
            auto bitmap = std::make_shared<DeleteBitmap>(next_scratch_bitmap_tablet_id());
            for (int64_t seg = lo; seg <= hi; ++seg) {
                roaring::Roaring complement;
                complement.addRange(0, segment_rows.size() == static_cast<size_t>(num_segments)
                                               ? segment_rows[static_cast<size_t>(seg)]
                                               : kMaxSegmentRows);
                auto it = per_segment.find(static_cast<uint32_t>(seg));
                if (it != per_segment.end()) {
                    complement -= it->second;
                }
                bitmap->set({rowset->rowset_id(), static_cast<uint32_t>(seg), 1}, complement);
            }
            const size_t group = groups++;
            plan_i.later.emplace_back(k, group);
            ScanView view;
            // the rows which die at k, and are not hit by a condition up to k
            view.version = preds.empty() ? std::max<int64_t>(1, input.version) : k;
            if (!preds.empty()) {
                view.delete_metas = delete_metas;
            }
            view.delete_bitmap = std::move(bitmap);
            add_job(i, std::move(view),
                    lo == 0 && hi + 1 == num_segments ? std::pair<int64_t, int64_t> {0, 0}
                                                      : std::pair<int64_t, int64_t> {lo, hi + 1},
                    group);
        }
    }

    // 2. Run them. Every scan adds to the buckets of its group.
    std::vector<DigestBuckets> group_buckets(groups);
    {
        std::vector<DigestScanTask> tasks(jobs.size());
        for (size_t i = 0; i < jobs.size(); ++i) {
            tasks[i].rowset_idx = i;
        }
        std::mutex mu;
        RestoreDigest unused;
        RETURN_IF_ERROR(run_tasks(
                tasks, threads,
                [&](const DigestScanTask& task, RestoreDigest*) {
                    const PrefixScanJob& job = jobs[task.rowset_idx];
                    RestoreDigest part;
                    RETURN_IF_ERROR(run_scan(input.rowsets[job.rowset_idx], input.schema,
                                             input.batch_size, input.enable_mow, plan, job.view,
                                             job.seg_begin, job.seg_end, &part));
                    std::lock_guard lock(mu);
                    add_buckets(&group_buckets[job.group], part.buckets);
                    return Status::OK();
                },
                &unused));
    }

    // 3. Assemble the parts.
    RestoreDigestDecomposed result;
    result.algo_version = RestoreDigest::kAlgoVersion;
    result.schema_sig = plan.schema_sig;
    result.mow = input.enable_mow;
    result.tablet_id = tablet_id;
    result.base_version = input.version;
    std::map<int64_t, DigestBuckets> marks;
    for (size_t i = 0; i < input.rowsets.size(); ++i) {
        const RowsetSharedPtr& rowset = input.rowsets[i];
        RestoreDigestRowsetPart part;
        part.rowset_id = rowset->rowset_id().to_string();
        part.start_version = rowset->start_version();
        part.end_version = rowset->end_version();
        const RowsetPlan& plan_i = plans[i];
        if (plan_i.scanned) {
            part.buckets = group_buckets[plan_i.base_group];
            if (input.enable_mow) {
                for (const auto& [k, group] : plan_i.later) {
                    add_buckets(&marks[k], group_buckets[group]);
                }
                // The rows alive just before a condition d are the rows alive after the previous
                // one, less the rows the bitmap kills in between (not hit by a condition, so they
                // are the rows of the bitmap scans). Those that d kills die at d.
                size_t next_later = 0;
                DigestBuckets running = part.buckets;
                for (const auto& [d, group] : plan_i.levels) {
                    while (next_later < plan_i.later.size() && plan_i.later[next_later].first < d) {
                        sub_buckets(&running, group_buckets[plan_i.later[next_later].second]);
                        ++next_later;
                    }
                    DigestBuckets removed = running;
                    sub_buckets(&removed, group_buckets[group]);
                    add_buckets(&marks[d], removed);
                    running = group_buckets[group];
                }
            } else {
                const DigestBuckets* previous = &part.buckets;
                for (const auto& [d, group] : plan_i.later) {
                    DigestBuckets removed = *previous;
                    sub_buckets(&removed, group_buckets[group]);
                    add_buckets(&marks[d], removed);
                    previous = &group_buckets[group];
                }
            }
        }
        result.rowsets.push_back(std::move(part));
    }
    for (auto& [k, buckets] : marks) {
        if (buckets_empty(buckets)) {
            continue;
        }
        RestoreDigestMarkPart mark;
        mark.mark_version = k;
        mark.buckets = buckets;
        result.marks.push_back(std::move(mark));
    }
    RestoreDigest total;
    RETURN_IF_ERROR(result.compose(input.version, &total));
    result.root = total.root;
    result.rows = total.rows;
    *out = std::move(result);
    return Status::OK();
}

std::string restore_digest_rowsets_sig(const std::vector<RowsetSharedPtr>& rowsets) {
    struct Item {
        int64_t start;
        int64_t end;
        std::string id;
    };
    std::vector<Item> items;
    items.reserve(rowsets.size());
    for (const auto& rs : rowsets) {
        items.push_back({rs->start_version(), rs->end_version(), rs->rowset_id().to_string()});
    }
    std::sort(items.begin(), items.end(), [](const Item& a, const Item& b) {
        return std::tie(a.start, a.end, a.id) < std::tie(b.start, b.end, b.id);
    });
    std::string buf;
    for (const Item& it : items) {
        // length prefixed fields, so that no two different sets share the same byte string
        buf.append(fmt::format("{}:{}:{}:{};", it.start, it.end, it.id.size(), it.id));
    }
    XXH128_hash_t h = XXH3_128bits(buf.data(), buf.size());
    return fmt::format("{:016x}{:016x}", h.high64, h.low64);
}

namespace {

// Captures the rowsets (and the delete bitmap snapshot for MoW) and the schema of a tablet at
// (0, version].
Status prepare_tablet_input(StorageEngine& engine, int64_t tablet_id, int64_t version, int threads,
                            RestoreDigestInput* input) {
    TabletSharedPtr tablet = engine.tablet_manager()->get_tablet(tablet_id);
    if (tablet == nullptr) {
        return Status::NotFound("restore digest: could not find tablet {}", tablet_id);
    }
    if (tablet->is_row_binlog_tablet()) {
        return Status::NotSupported("restore digest: row binlog tablet is not supported");
    }
    if (tablet->tablet_meta()->cooldown_meta_id().initialized()) {
        return Status::NotSupported("restore digest: tablet with cooldown data is not supported");
    }

    input->version = version;
    input->keys_type = tablet->keys_type();
    input->enable_mow = tablet->enable_unique_key_merge_on_write();
    input->threads = threads;
    input->tablet = tablet;
    input->tablet_uid = tablet->tablet_uid();
    {
        // Rowsets and the delete bitmap snapshot must be taken in the same critical section.
        std::shared_lock rdlock(tablet->get_header_lock());
        auto ret = tablet->capture_consistent_rowsets_unlocked(Version(0, version),
                                                               CaptureRowsetOps {});
        if (!ret) {
            return std::move(ret.error());
        }
        input->rowsets = std::move(ret->rowsets);
        if (input->enable_mow) {
            input->delete_bitmap = std::make_shared<DeleteBitmap>(
                    tablet->tablet_meta()->delete_bitmap().snapshot(version));
        }
    }
    // the data identity, from the very rowsets which will be read (computed outside of the header
    // lock: the rowsets are immutable)
    input->rowsets_sig = restore_digest_rowsets_sig(input->rowsets);
    std::vector<RowsetMetaSharedPtr> metas;
    metas.reserve(input->rowsets.size());
    for (const auto& rs : input->rowsets) {
        if (!rs->is_local()) {
            return Status::NotSupported("restore digest: remote rowset {} is not supported",
                                        rs->rowset_id().to_string());
        }
        metas.push_back(rs->rowset_meta());
    }
    // The newest schema among the rowsets and the tablet itself: after a light schema change the
    // rowsets may all carry an older schema, and the segments just read the default for the
    // columns they do not have.
    input->schema = metas.empty() ? tablet->tablet_schema()
                                  : BaseTablet::tablet_schema_with_merged_max_schema_version(metas);
    if (tablet->tablet_schema()->schema_version() > input->schema->schema_version()) {
        input->schema = tablet->tablet_schema();
    }
    return Status::OK();
}

} // namespace

Status compute_tablet_restore_digest(StorageEngine& engine, int64_t tablet_id, int64_t version,
                                     RestoreDigest* digest, int threads) {
    RestoreDigestInput input;
    RETURN_IF_ERROR(prepare_tablet_input(engine, tablet_id, version, threads, &input));
    return compute_restore_digest(input, digest);
}

Status compute_restore_digest_schema_sig(const TabletSchema& schema, KeysType keys_type,
                                         bool enable_mow, std::string* schema_sig) {
    using namespace restore_digest_detail;
    ColumnPlan plan;
    RETURN_IF_ERROR(build_plan(schema, keys_type, enable_mow, &plan));
    *schema_sig = std::move(plan.schema_sig);
    return Status::OK();
}

size_t RestoreDigestCache::KeyHash::operator()(const Key& k) const {
    size_t h = std::hash<std::string>()(k.schema_sig);
    auto mix = [&h](uint64_t v) {
        h ^= std::hash<uint64_t>()(v) + 0x9e3779b97f4a7c15ULL + (h << 6) + (h >> 2);
    };
    mix(static_cast<uint64_t>(k.tablet_id));
    mix(static_cast<uint64_t>(k.version));
    mix(k.algo_version);
    mix(static_cast<uint64_t>(k.tablet_uid.hi));
    mix(static_cast<uint64_t>(k.tablet_uid.lo));
    mix(std::hash<std::string>()(k.rowsets_sig));
    mix(static_cast<uint64_t>(k.bitmap_keys));
    return h;
}

RestoreDigestCache* RestoreDigestCache::instance() {
    static RestoreDigestCache cache;
    return &cache;
}

int64_t RestoreDigestCache::capacity() const {
    return _capacity > 0 ? _capacity : config::restore_digest_cache_capacity;
}

int64_t RestoreDigestCache::estimate_bytes(const RestoreDigestDecomposed& d) {
    int64_t bytes = static_cast<int64_t>(sizeof(RestoreDigestDecomposed));
    bytes += static_cast<int64_t>(d.schema_sig.capacity() + d.root.capacity());
    bytes += static_cast<int64_t>(d.rowsets.capacity() * sizeof(RestoreDigestRowsetPart));
    for (const auto& r : d.rowsets) {
        bytes += static_cast<int64_t>(r.rowset_id.capacity());
    }
    bytes += static_cast<int64_t>(d.marks.capacity() * sizeof(RestoreDigestMarkPart));
    bytes += static_cast<int64_t>(d.wholes.capacity() * sizeof(RestoreDigestWholePart));
    return bytes;
}

void RestoreDigestCache::drop_decomposed(EntryList::iterator it) {
    if (it->decomposed == nullptr) {
        return;
    }
    _decomposed_bytes -= it->decomposed_bytes;
    _decomposed_lru.erase(it->decomposed_pos);
    it->decomposed.reset();
    it->decomposed_bytes = 0;
    it->guard = DecomposedGuard();
}

void RestoreDigestCache::erase_entry(EntryList::iterator it) {
    drop_decomposed(it);
    auto idx = _by_tablet_version.find({it->key.tablet_id, it->key.version});
    if (idx != _by_tablet_version.end()) {
        auto& entries = idx->second;
        entries.erase(std::remove(entries.begin(), entries.end(), it), entries.end());
        if (entries.empty()) {
            _by_tablet_version.erase(idx);
        }
    }
    _map.erase(it->key);
    _lru.erase(it);
}

void RestoreDigestCache::enforce_decomposed_limit(int64_t limit) {
    while (_decomposed_bytes > limit && !_decomposed_lru.empty()) {
        drop_decomposed(_decomposed_lru.back());
    }
}

bool RestoreDigestCache::lookup(const Key& key, LogicalDigestResult* out, Hit* hit) {
    std::lock_guard lock(_mtx);
    auto it = _map.find(key);
    if (it == _map.end()) {
        ++_misses;
        return false;
    }
    _lru.splice(_lru.begin(), _lru, it->second);
    Entry& entry = *it->second;
    *out = entry.value;
    out->from_cache = true;
    if (hit != nullptr) {
        hit->decomposed = entry.decomposed;
        hit->guard = entry.guard;
        if (entry.decomposed != nullptr) {
            _decomposed_lru.splice(_decomposed_lru.begin(), _decomposed_lru, entry.decomposed_pos);
        }
    }
    ++_hits;
    return true;
}

void RestoreDigestCache::insert(const Key& key, const LogicalDigestResult& value) {
    insert(key, value, nullptr, DecomposedGuard());
}

void RestoreDigestCache::insert(const Key& key, const LogicalDigestResult& value,
                                std::shared_ptr<const RestoreDigestDecomposed> decomposed,
                                const DecomposedGuard& guard) {
    const int64_t cap = capacity();
    const int64_t limit = std::max<int64_t>(0, config::restore_digest_cache_decomposed_mb) << 20;
    const int64_t bytes = decomposed != nullptr ? estimate_bytes(*decomposed) : 0;
    std::lock_guard lock(_mtx);
    auto it = _map.find(key);
    if (it != _map.end()) {
        // keep the existing decomposed digest only when none is given and the root did not change
        if (decomposed != nullptr || it->second->value.root != value.root) {
            drop_decomposed(it->second);
        }
        it->second->value = value;
        _lru.splice(_lru.begin(), _lru, it->second);
    } else if (cap > 0) {
        // older generations of the same data can never hit again: drop them (and the bytes of their
        // decomposed digests) before the new entry is added
        auto idx = _by_tablet_version.find({key.tablet_id, key.version});
        if (idx != _by_tablet_version.end()) {
            std::vector<EntryList::iterator> stale;
            for (const auto& e : idx->second) {
                if (e->key.algo_version == key.algo_version &&
                    e->key.schema_sig == key.schema_sig) {
                    stale.push_back(e);
                }
            }
            for (const auto& e : stale) {
                erase_entry(e);
            }
        }
        _lru.emplace_front();
        _lru.front().key = key;
        _lru.front().value = value;
        _map[key] = _lru.begin();
        _by_tablet_version[std::make_pair(key.tablet_id, key.version)].push_back(_lru.begin());
        it = _map.find(key);
    } else {
        return;
    }
    // a digest larger than the limit is not kept, and no other digest is dropped for it
    if (decomposed != nullptr && limit > 0 && bytes <= limit) {
        Entry& entry = *it->second;
        entry.decomposed = std::move(decomposed);
        entry.guard = guard;
        entry.decomposed_bytes = bytes;
        _decomposed_lru.push_front(it->second);
        entry.decomposed_pos = _decomposed_lru.begin();
        _decomposed_bytes += bytes;
    }
    while (!_lru.empty() && static_cast<int64_t>(_lru.size()) > std::max<int64_t>(cap, 0)) {
        erase_entry(std::prev(_lru.end()));
    }
    enforce_decomposed_limit(limit);
}

void RestoreDigestCache::erase(const Key& key) {
    std::lock_guard lock(_mtx);
    auto it = _map.find(key);
    if (it != _map.end()) {
        erase_entry(it->second);
    }
}

size_t RestoreDigestCache::size() const {
    std::lock_guard lock(_mtx);
    return _lru.size();
}

size_t RestoreDigestCache::decomposed_count() const {
    std::lock_guard lock(_mtx);
    return _decomposed_lru.size();
}

int64_t RestoreDigestCache::decomposed_bytes() const {
    std::lock_guard lock(_mtx);
    return _decomposed_bytes;
}

void RestoreDigestCache::clear() {
    std::lock_guard lock(_mtx);
    _decomposed_lru.clear();
    _decomposed_bytes = 0;
    _lru.clear();
    _map.clear();
    _by_tablet_version.clear();
}

Status compute_tablet_restore_digest_decomposed(StorageEngine& engine, int64_t tablet_id,
                                                int64_t version, int threads,
                                                RestoreDigestDecomposed* out) {
    if (threads <= 0) {
        threads = std::max(1, config::restore_digest_threads);
    }
    RestoreDigestInput input;
    RETURN_IF_ERROR(prepare_tablet_input(engine, tablet_id, version, threads, &input));
    return decompose_restore_digest(input, tablet_id, out);
}

namespace {
std::atomic<uint64_t> g_scan_count {0};

void fill_result(const RestoreDigest& digest, LogicalDigestResult* result) {
    result->algo_version = RestoreDigest::kAlgoVersion;
    result->schema_sig = digest.schema_sig;
    result->root = digest.root;
    result->rows = digest.rows;
    result->from_cache = false;
}

// Whether the decomposed digest of a cache hit was computed under the same settings: the key
// already identifies the rowsets (ids and versions), so only the guard is left to compare.
bool decomposed_matches_input(const RestoreDigestDecomposed& d, const RestoreDigestInput& input,
                              const RestoreDigestCache::DecomposedGuard& cached,
                              const RestoreDigestCache::DecomposedGuard& now) {
    return cached == now && d.rowsets.size() == input.rowsets.size();
}
} // namespace

uint64_t restore_digest_scan_count() {
    return g_scan_count.load();
}

Status get_tablet_logical_digest(StorageEngine& engine, int64_t tablet_id, int64_t version,
                                 int threads, RestoreDigestCache* cache,
                                 LogicalDigestResult* result,
                                 std::shared_ptr<const RestoreDigestDecomposed>* decomposed,
                                 Status* decomposed_status, bool* decomposed_from_cache) {
    if (threads <= 0) {
        threads = std::max(1, config::restore_digest_threads);
    }
    if (decomposed != nullptr) {
        decomposed->reset();
    }
    if (decomposed_from_cache != nullptr) {
        *decomposed_from_cache = false;
    }
    RestoreDigestInput input;
    RETURN_IF_ERROR(prepare_tablet_input(engine, tablet_id, version, threads, &input));
    RestoreDigestCache::DecomposedGuard guard;
    if (input.keys_type == UNIQUE_KEYS && !input.enable_mow) {
        guard.mor_boundaries = config::restore_digest_mor_prefix_boundaries;
    }
    if (input.enable_mow && input.delete_bitmap != nullptr) {
        guard.bitmap_keys = static_cast<int64_t>(input.delete_bitmap->delete_bitmap.size());
    }
    RestoreDigestCache::Key key;
    key.tablet_id = tablet_id;
    key.version = version;
    key.algo_version = RestoreDigest::kAlgoVersion;
    key.tablet_uid = input.tablet_uid;
    key.rowsets_sig = input.rowsets_sig;
    key.bitmap_keys = guard.bitmap_keys;
    // NotSupported schemas are reported here, before anything is scanned or cached.
    RETURN_IF_ERROR(compute_restore_digest_schema_sig(*input.schema, input.keys_type,
                                                      input.enable_mow, &key.schema_sig));
    RestoreDigestCache::Hit hit;
    bool cached = cache != nullptr &&
                  cache->lookup(key, result, decomposed != nullptr ? &hit : nullptr);
    const std::string sig_prefix = input.rowsets_sig.substr(0, 8);
    if (decomposed != nullptr) {
        if (cached && hit.decomposed != nullptr &&
            decomposed_matches_input(*hit.decomposed, input, hit.guard, guard)) {
            // No I/O: the parts compose to the whole digest, which must be the cached one.
            RestoreDigest composed;
            Status st = hit.decomposed->compose(version, &composed);
            if (st.ok() && composed.root == result->root) {
                cache->note_decomposed(true);
                *decomposed = std::move(hit.decomposed);
                if (decomposed_status != nullptr) {
                    *decomposed_status = Status::OK();
                }
                if (decomposed_from_cache != nullptr) {
                    *decomposed_from_cache = true;
                }
                return Status::OK();
            }
            LOG(WARNING) << "restore digest: stale cache entry, tablet_id=" << tablet_id
                         << ", version=" << version << ", rowsets_sig=" << sig_prefix
                         << ", cached root=" << result->root << ", the cached decomposed digest "
                         << "composes to " << (st.ok() ? composed.root : st.to_string())
                         << ", dropping the entry and scanning again";
            cache->note_stale();
            cache->erase(key);
            cached = false;
        }
        if (cache != nullptr) {
            cache->note_decomposed(false);
        }
        // One pass over the tablet gives the parts, and the whole digest is composed from them.
        ++g_scan_count;
        auto fresh = std::make_shared<RestoreDigestDecomposed>();
        Status st = decompose_restore_digest(input, tablet_id, fresh.get());
        RestoreDigest composed;
        if (st.ok()) {
            st = fresh->compose(version, &composed);
        }
        if (st.ok() && cached && composed.root != result->root) {
            // The recomputed digest wins: the cached whole digest is dropped and replaced.
            LOG(WARNING) << "restore digest: stale cache entry, tablet_id=" << tablet_id
                         << ", version=" << version << ", rowsets_sig=" << sig_prefix
                         << ", cached root=" << result->root << ", recomputed root="
                         << composed.root << ", replacing the entry";
            cache->note_stale();
            cache->erase(key);
            cached = false;
        }
        if (decomposed_status != nullptr) {
            *decomposed_status = st;
        }
        if (st.ok()) {
            if (!cached) {
                fill_result(composed, result);
                result->rowsets_sig = input.rowsets_sig;
            }
            if (cache != nullptr) {
                // for a cached whole digest this attaches the decomposed digest to its entry
                cache->insert(key, *result, fresh, guard);
            }
            *decomposed = std::move(fresh);
            return Status::OK();
        }
        // The decomposed digest failed (not supported, over the scan limit, ...): only the whole
        // digest is cached, from the cache if it is there (the key identifies the data, so it
        // describes it).
    }
    result->rowsets_sig = input.rowsets_sig;
    if (cached) {
        return Status::OK();
    }
    ++g_scan_count;
    RestoreDigest digest;
    RETURN_IF_ERROR(compute_restore_digest(input, &digest));
    fill_result(digest, result);
    result->rowsets_sig = input.rowsets_sig;
    if (cache != nullptr) {
        cache->insert(key, *result);
    }
    return Status::OK();
}

} // namespace doris
