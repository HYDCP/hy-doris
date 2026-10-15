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

#include <array>
#include <atomic>
#include <cstdint>
#include <list>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/status.h"
#include "storage/olap_common.h"
#include "storage/rowset/rowset_fwd.h"
#include "storage/tablet/tablet_fwd.h"
#include "storage/tablet/tablet_schema.h"

namespace doris {

class StorageEngine;

// Order-independent logical digest of a tablet at a fixed version (0, V].
//
// algo_version = 1:
//   * Every visible row is encoded into a canonical byte string. For each digest column, in
//     schema order, one null-flag byte is written (0x00 = NULL, 0x01 = not NULL), followed by the
//     value if it is not NULL:
//       BOOLEAN/TINYINT/SMALLINT/INT/BIGINT/LARGEINT: native width, little endian
//       FLOAT/DOUBLE: raw IEEE-754 bits (no normalization of -0 / NaN payload)
//       DECIMAL32/64/128I/256: the unscaled integer at native width, little endian; precision and
//                              scale come from the schema signature, not from the row
//       DATEV2/DATETIMEV2: raw u32/u64 bit pattern; scale comes from the schema signature
//       IPV4/IPV6: u32/u128, little endian
//       CHAR: trailing '\0' padding stripped, then u32 length + bytes
//       VARCHAR/STRING/JSONB: u32 length + bytes (JSONB: the stored binary, byte for byte)
//     Types added after the first cut (they do not change the encoding of any type above, so
//     algo_version stays 1):
//       DECIMALV2: the in-memory int128 (value * 10^9), little endian; precision / scale are in
//                  the schema signature
//       DATE (v1): year u16, month u8, day u8 (fields, never the packed bits)
//       DATETIME (v1): year u16, month u8, day u8, hour u8, minute u8, second u8
//       ARRAY: u64 element count, then per element a null flag byte and the element encoding
//       MAP: u64 entry count, then per entry (key, value), each with a null flag byte, in the
//            stored order (the entries are not sorted)
//       STRUCT: per field in declaration order a null flag byte and the field encoding
//   * The row bytes are hashed with XXH3-128 (seed = algo_version). The high 8 bits of the high
//     64-bit half pick one of 256 buckets; a bucket keeps the sum (mod 2^128) of the row hashes
//     and its row count.
//   * root = SHA-256 over a fixed header and all 256 (sum, count) pairs.
//   * schema_sig = SHA-256 over the keys type and the (type, precision, scale) of every digest
//     column. Two digests are comparable only if algo_version and schema_sig are equal.
//
// Hidden columns: the delete sign is only used to filter rows (MoW and MoR), the sequence column
// takes part in the digest, version / commit TSO / skip bitmap / row store columns are excluded.
//
// Which rows are visible:
//   * Duplicate: every row, minus the rows removed by DELETE conditions (delete predicates) of
//     version <= V. A condition of version d removes matching rows of rowsets with end version < d,
//     the same rule the query path uses, so the digest is the same before and after a compaction
//     has physically dropped those rows.
//   * Unique MoW: the rows the delete bitmap snapshot at V leaves, minus delete-sign rows, minus
//     delete predicates. No merge is needed.
//   * Unique MoR: rows are merged across rowsets by key (highest version wins, sequence column
//     first when the table has one) by the same BlockReader the checksum task uses, then
//     delete-sign rows are dropped.
struct RestoreDigest {
    static constexpr uint32_t kAlgoVersion = 1;
    static constexpr size_t kNumBuckets = 256;

    struct Bucket {
        unsigned __int128 sum = 0;
        uint64_t count = 0;
    };

    std::array<Bucket, kNumBuckets> buckets {};
    uint64_t rows = 0;
    // rows read from the segments, including rows removed by the delete sign
    uint64_t rows_scanned = 0;
    // on-disk size (data part) of the scanned rowsets
    uint64_t bytes_read = 0;
    // size of the canonical row encoding that was hashed
    uint64_t encoded_bytes = 0;
    uint32_t rowset_count = 0;
    uint32_t segment_count = 0;
    int64_t elapsed_ms = 0;
    // worker threads actually used (the unique MoR merge is always single threaded)
    uint32_t threads = 1;
    // number of delete predicates (rowsets with version <= V) applied to the rows
    uint32_t delete_predicates = 0;
    std::string schema_sig; // hex
    std::string root;       // hex

    // Computes `root` from the buckets.
    void finalize();
    std::string to_json() const;
};

struct RestoreDigestInput {
    // The rowsets which make up (0, version]; the caller keeps them alive.
    std::vector<RowsetSharedPtr> rowsets;
    // The schema used for reading, normally the merged max-schema-version schema of the rowsets.
    TabletSchemaSPtr schema;
    KeysType keys_type = DUP_KEYS;
    bool enable_mow = false;
    // Non-null for MoW: an immutable snapshot of the delete bitmap at `version`.
    DeleteBitmapPtr delete_bitmap;
    int64_t version = 0;
    int batch_size = 4096;
    // Needed for unique MoR only, where the rows of the rowsets are merged by a BlockReader.
    BaseTabletSPtr tablet;
    // Worker threads for the rowset / segment parallel read (Duplicate and MoW).
    int threads = 1;
    // The tablet instance and the fingerprint of `rowsets`, captured with them (data identity for
    // the digest cache). Empty rowsets_sig means not captured.
    TabletUid tablet_uid;
    std::string rowsets_sig;
};

// Fingerprint (hex XXH3-128) of a set of rowsets: the rowset id, start and end version of each,
// sorted by (start_version, end_version, rowset_id), so independent of the order given.
std::string restore_digest_rowsets_sig(const std::vector<RowsetSharedPtr>& rowsets);

// Checks that the model and every column are supported by algo_version 1. Returns NotSupported
// with the reason otherwise. Never computes anything.
Status check_restore_digest_supported(const TabletSchema& schema, KeysType keys_type,
                                      bool enable_mow);

// Computes the digest from a given set of rowsets (the layer which unit tests drive directly).
Status compute_restore_digest(const RestoreDigestInput& input, RestoreDigest* digest);

// Computes the digest of tablet `tablet_id` at (0, version]. The rowsets and (for MoW) the delete
// bitmap snapshot are taken under the header lock. Read only.
Status compute_tablet_restore_digest(StorageEngine& engine, int64_t tablet_id, int64_t version,
                                     RestoreDigest* digest, int threads = 1);

// The part of a digest which is reported to the FE and kept in the cache.
struct LogicalDigestResult {
    uint32_t algo_version = RestoreDigest::kAlgoVersion;
    std::string schema_sig; // hex
    std::string root;       // hex
    uint64_t rows = 0;
    bool from_cache = false;
    // Hex of the data identity (the rowsets fingerprint) the digest was computed or looked up
    // with, set by get_tablet_logical_digest; for logs.
    std::string rowsets_sig;
};

struct RestoreDigestDecomposed;

// In-memory LRU cache of tablet digests. The digest of a given set of rowsets never changes, but the
// content of a tablet at a version can be replaced (a non-atomic restore overwrite, a full clone, a
// tablet dropped and recreated with the same id, a compaction), so the key carries the data identity
// besides (tablet_id, version, algo_version, schema_sig): the tablet_uid, and a fingerprint of the
// rowsets of (0, version] (ids and versions, captured in the same critical section as the rowsets
// which are read). Anything which replaces the data creates new rowset ids, so it cannot hit an
// old entry; a restart of the BE keeps the rowset ids but empties the cache anyway. A compaction
// changes the rowsets too, so the whole digest is recomputed after it: correctness first.
//
// An entry may also hold the decomposed digest (the per rowset parts, which is what the source of a
// backup needs besides the whole digest), shared and immutable. The decomposed digest describes the
// rowsets which the tablet had at the time, and a compaction can change those without changing the
// whole digest, so it carries a guard: the caller compares it with the current state before using it.
// The decomposed digests are limited in bytes (config::restore_digest_cache_decomposed_mb, checked at
// every insert) by a second LRU: when over the limit only the decomposed digest of the least recently
// used entries is dropped, their whole digests stay.
class RestoreDigestCache {
public:
    struct Key {
        int64_t tablet_id = 0;
        int64_t version = 0;
        uint32_t algo_version = 0;
        std::string schema_sig;
        // the tablet instance: a tablet recreated with the same id has another uid
        TabletUid tablet_uid;
        // fingerprint of the rowsets of (0, version], see restore_digest_rowsets_sig()
        std::string rowsets_sig;
        // MoW: number of keys of the delete bitmap snapshot at the version (the delete bitmap
        // generation), else -1
        int64_t bitmap_keys = -1;
        bool operator==(const Key& o) const {
            return tablet_id == o.tablet_id && version == o.version &&
                   algo_version == o.algo_version && schema_sig == o.schema_sig &&
                   tablet_uid == o.tablet_uid && rowsets_sig == o.rowsets_sig &&
                   bitmap_keys == o.bitmap_keys;
        }
    };

    // What the decomposed digest depends on besides the key (which identifies the rowsets).
    struct DecomposedGuard {
        // unique MoR: config::restore_digest_mor_prefix_boundaries when it was computed, else -1
        int64_t mor_boundaries = -1;
        // MoW: number of keys of the delete bitmap snapshot at the version, else -1
        int64_t bitmap_keys = -1;
        bool operator==(const DecomposedGuard& o) const {
            return mor_boundaries == o.mor_boundaries && bitmap_keys == o.bitmap_keys;
        }
    };

    struct Hit {
        // null if the entry has no decomposed digest
        std::shared_ptr<const RestoreDigestDecomposed> decomposed;
        DecomposedGuard guard;
    };

    // capacity <= 0 means "use config::restore_digest_cache_capacity (checked at every insert)".
    explicit RestoreDigestCache(int64_t capacity = 0) : _capacity(capacity) {}

    static RestoreDigestCache* instance();

    // `hit` may be null; when it is not, the decomposed digest of the entry (if any) is returned too and
    // counts as recently used by the byte limited LRU.
    bool lookup(const Key& key, LogicalDigestResult* out, Hit* hit = nullptr);
    // Inserting a key which is not in the cache evicts the entries of the same (tablet_id, version,
    // algo_version, schema_sig) with another tablet_uid / rowsets_sig / bitmap_keys: they are older
    // generations of the data which can never hit again.
    //
    // Stores the whole digest. An existing decomposed digest stays if the root is the same, else it
    // is dropped.
    void insert(const Key& key, const LogicalDigestResult& value);
    // Stores the whole digest and its decomposed digest, which replaces any existing one. The
    // decomposed digest is not kept (the whole digest is) if config::restore_digest_cache_decomposed_mb
    // is 0 or the digest alone is larger than that.
    void insert(const Key& key, const LogicalDigestResult& value,
                std::shared_ptr<const RestoreDigestDecomposed> decomposed,
                const DecomposedGuard& guard);
    void erase(const Key& key);
    size_t size() const;
    uint64_t hits() const { return _hits; }
    uint64_t misses() const { return _misses; }
    // How many times a cached whole digest disagreed with the digest recomputed from the data (it
    // was replaced by the recomputed one). Not expected to be non-zero.
    uint64_t stale_count() const { return _stale; }
    void note_stale() { ++_stale; }
    // Statistics of the decomposed digests: count and bytes held now, and how many times a caller
    // that asked for one found it usable / not.
    size_t decomposed_count() const;
    int64_t decomposed_bytes() const;
    uint64_t decomposed_hits() const { return _decomposed_hits; }
    uint64_t decomposed_misses() const { return _decomposed_misses; }
    void note_decomposed(bool hit) { ++(hit ? _decomposed_hits : _decomposed_misses); }
    void clear();

    // Approximate memory of a decomposed digest.
    static int64_t estimate_bytes(const RestoreDigestDecomposed& d);

private:
    struct KeyHash {
        size_t operator()(const Key& k) const;
    };
    struct TabletVersionHash {
        size_t operator()(const std::pair<int64_t, int64_t>& p) const {
            return (std::hash<int64_t>()(p.first) * 1000003ULL) ^ std::hash<int64_t>()(p.second);
        }
    };
    struct Entry;
    using EntryList = std::list<Entry>;
    using DecomposedList = std::list<EntryList::iterator>;
    struct Entry {
        Key key;
        LogicalDigestResult value;
        std::shared_ptr<const RestoreDigestDecomposed> decomposed;
        DecomposedGuard guard;
        int64_t decomposed_bytes = 0;
        DecomposedList::iterator decomposed_pos; // valid iff decomposed != null
    };

    int64_t capacity() const;
    // all with _mtx held
    void drop_decomposed(EntryList::iterator it);
    void erase_entry(EntryList::iterator it);
    void enforce_decomposed_limit(int64_t limit);

    int64_t _capacity;
    mutable std::mutex _mtx;
    EntryList _lru; // front = most recently used
    std::unordered_map<Key, EntryList::iterator, KeyHash> _map;
    // (tablet_id, version) -> the entries of that tablet and version, to find the older generations
    // of a key. Maintained together with _map, under _mtx.
    std::unordered_map<std::pair<int64_t, int64_t>, std::vector<EntryList::iterator>,
                       TabletVersionHash>
            _by_tablet_version;
    DecomposedList _decomposed_lru; // the entries holding a decomposed digest, front = most recent
    int64_t _decomposed_bytes = 0;
    std::atomic<uint64_t> _hits {0};
    std::atomic<uint64_t> _misses {0};
    std::atomic<uint64_t> _stale {0};
    std::atomic<uint64_t> _decomposed_hits {0};
    std::atomic<uint64_t> _decomposed_misses {0};
};

// Computes the schema_sig a digest of this schema would have. NotSupported if the digest does not
// support the model or a column.
Status compute_restore_digest_schema_sig(const TabletSchema& schema, KeysType keys_type,
                                         bool enable_mow, std::string* schema_sig);

using DigestBuckets = std::array<RestoreDigest::Bucket, RestoreDigest::kNumBuckets>;

// The digest of a tablet at its backup version V_b, decomposed so that the digest at any rowset
// boundary version V_l <= V_b can be composed without reading any data (phase 1 of the incremental
// restore, see R2 design section 13).
//
// The visible rows at a version V are the rows of the rowsets with end <= V which are alive at V. A
// row has a death version: the lowest version at which it stops being visible, because
//   * (unique MoW) the delete bitmap marks it at that version, or
//   * (duplicate) a DELETE condition of that version matches it (the condition removes the rows of the
//     rowsets with end < version, the same rule the query path uses).
// Delete sign rows are never visible. So, with
//   rowset_buckets(R)  = buckets of the rows of R which are alive at R.end_version
//   mark_buckets(k)    = buckets of the rows whose death version is k (> the end version of their
//                        rowset), summed over all rowsets
// the digest at a boundary V_l is
//   compose(V_l) = sum(rowset_buckets(R), R.end <= V_l) - sum(mark_buckets(k), k <= V_l)
// (every mark k of a row has k > R.end, so "the marked row's rowset ends <= V_l" holds whenever
// k <= V_l). The sums are modulo 2^128, the counts are plain integers, both are exact, so the result
// equals the digest computed directly at V_l, bucket by bucket.
//
// Unique MoW with DELETE conditions: the death version of a row is the lower of the version at which
// the delete bitmap marks it and the version of the first DELETE condition which matches it and is
// above the end version of its rowset (R2 design section 15). The rows which die at a bitmap version
// are read through a bitmap of their own, the rows which die at a DELETE condition are the difference
// of two levels of the rowset, so the composition formula stays the same.
//
// Unique MoR: the visible rows are decided by merging the rowsets by key, which is not additive. A
// file of a MoR tablet carries, besides the rowset chain (without buckets), the whole digest at the
// `restore_digest_mor_prefix_boundaries` most recent rowset boundaries below the base version and at
// the base version (format version 2). Only those versions are boundaries of such a file.
//
// Not supported: tablets which need more than config::restore_digest_prefix_max_scans extra scans, and
// MoR with the number of boundaries set to 0.
struct RestoreDigestRowsetPart {
    std::string rowset_id; // on the source tablet, informational
    int64_t start_version = 0;
    int64_t end_version = 0;
    DigestBuckets buckets {};
};

struct RestoreDigestMarkPart {
    int64_t mark_version = 0;
    DigestBuckets buckets {};
};

// Unique MoR: the whole digest at one version (format version 2 files).
struct RestoreDigestWholePart {
    int64_t version = 0;
    DigestBuckets buckets {};
};

struct RestoreDigestDecomposed {
    // Files of the models which compose by rowset (duplicate, MoW) keep format version 1, byte for byte as
    // before. A MoR file is format version 2: version 1 plus a trailing section with the whole digests.
    static constexpr uint32_t kFormatVersion = 1;
    static constexpr uint32_t kFormatVersionMor = 2;
    // file name next to the manifest of a local snapshot: <snapshot>/<tablet_id>/rdigest
    static constexpr std::string_view kLocalFileName = "rdigest";

    uint32_t algo_version = RestoreDigest::kAlgoVersion;
    std::string schema_sig; // hex
    bool mow = false;
    int64_t tablet_id = 0;
    // V_b, the version of the backup: the end version of the last rowset
    int64_t base_version = 0;
    // the digest at V_b, hex, and its row count; equals compose(base_version)
    std::string root;
    uint64_t rows = 0;
    // sorted by end_version, the chain of the rowsets of (0, base_version]
    std::vector<RestoreDigestRowsetPart> rowsets;
    // sorted by mark_version, only the versions which have marked rows
    std::vector<RestoreDigestMarkPart> marks;
    // unique MoR only: the rowsets above carry no buckets, the digests are these whole ones, sorted by
    // version, the last one is at base_version
    bool mor = false;
    std::vector<RestoreDigestWholePart> wholes;

    // Binary content of the rdigest file. Sparse: only the buckets with rows are written.
    std::string serialize() const;
    // Parses a file, after checking that its SHA-256 is `expected_root` (RESTORE_MANIFEST_MISMATCH
    // otherwise, also for a malformed file or a file of another tablet).
    static Status parse(std::string_view content, std::string_view expected_root,
                        int64_t expected_tablet_id, RestoreDigestDecomposed* out);
    // Lower case hex SHA-256 of serialize(): the root of the file recorded in the backup job info.
    std::string file_root() const;

    // Whether `version` is the end version of one of the rowsets (MoR: one of the versions with a whole
    // digest).
    bool is_boundary(int64_t version) const;
    // Whether `version` is the end version of one of the rowsets.
    bool is_rowset_end(int64_t version) const;
    // The digest at `version`, with the same buckets, rows and root as compute_restore_digest at
    // `version`. NotSupported if `version` is not a rowset boundary (the other fields of the digest
    // that describe the scan are left at their defaults).
    Status compose(int64_t version, RestoreDigest* digest) const;
};

// Decomposed digest of the rowsets of a snapshot input (the layer which unit tests drive directly).
// `input.version` is V_b and must be the end version of the last rowset. MoR and the other
// unsupported cases return NotSupported with the reason.
Status decompose_restore_digest(const RestoreDigestInput& input, int64_t tablet_id,
                                RestoreDigestDecomposed* out);

// Decomposed digest of tablet `tablet_id` at (0, version].
Status compute_tablet_restore_digest_decomposed(StorageEngine& engine, int64_t tablet_id,
                                                int64_t version, int threads,
                                                RestoreDigestDecomposed* out);

// The digest of tablet `tablet_id` at (0, version] for backup / restore: looks `cache` up first (by
// the schema_sig of the tablet) and computes + caches on a miss. threads <= 0 means
// config::restore_digest_threads. `cache` may be null (no caching).
//
// If `decomposed` is not null, the decomposed digest is produced as well and `*decomposed_status`
// tells whether it is valid (when it is not, `*decomposed` is null and the status says why: the
// tablet is not supported, or a failure). The decomposed digest comes from the cache when the entry
// has one that still describes the rowsets captured now (same rowset ids and versions, same MoR
// boundaries setting, same size of the MoW delete bitmap) and composes to the cached root; then
// nothing is scanned (`*decomposed_from_cache` is true). Otherwise the tablet is scanned once, the
// whole digest is composed from the parts, and both are cached (the decomposed digest only if it
// is valid and fits config::restore_digest_cache_decomposed_mb). If the cache holds another root for
// the same key, the decomposed digest is dropped as inconsistent and the cached value is returned.
// If `decomposed` is null the decomposed part of the cache is neither read nor written.
Status get_tablet_logical_digest(StorageEngine& engine, int64_t tablet_id, int64_t version,
                                 int threads, RestoreDigestCache* cache,
                                 LogicalDigestResult* result,
                                 std::shared_ptr<const RestoreDigestDecomposed>* decomposed = nullptr,
                                 Status* decomposed_status = nullptr,
                                 bool* decomposed_from_cache = nullptr);

// Number of scans (decomposed or whole) get_tablet_logical_digest has started, for tests.
uint64_t restore_digest_scan_count();

} // namespace doris
