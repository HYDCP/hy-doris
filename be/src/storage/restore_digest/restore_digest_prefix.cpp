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

// The file format and the composition of the decomposed restore digest, see
// RestoreDigestDecomposed in restore_digest.h. Nothing here touches tablets or data.

#include <fmt/format.h>

#include <algorithm>
#include <bit>
#include <cstring>

#include "common/status.h"
#include "storage/restore_digest/restore_digest.h"
#include "util/sha.h"

namespace doris {

static_assert(std::endian::native == std::endian::little,
              "the decomposed restore digest file assumes a little endian host");

namespace {

constexpr char kMagic[4] = {'R', 'D', 'G', 'D'};

bool bucket_is_empty(const RestoreDigest::Bucket& b) {
    return b.count == 0 && b.sum == 0;
}

class Writer {
public:
    void u8(uint8_t v) { _buf.push_back(static_cast<char>(v)); }
    void u16(uint16_t v) { append(&v, sizeof(v)); }
    void u32(uint32_t v) { append(&v, sizeof(v)); }
    void u64(uint64_t v) { append(&v, sizeof(v)); }
    void i64(int64_t v) { append(&v, sizeof(v)); }
    void varint(uint64_t v) {
        while (v >= 0x80) {
            u8(static_cast<uint8_t>(v | 0x80));
            v >>= 7;
        }
        u8(static_cast<uint8_t>(v));
    }
    void str(const std::string& s) {
        varint(s.size());
        _buf.append(s);
    }
    void bytes(const void* p, size_t n) { append(p, n); }
    // Sparse buckets: the number of non-empty buckets, then (index, sum, count) of each.
    void buckets(const DigestBuckets& buckets) {
        uint16_t n = 0;
        for (const auto& b : buckets) {
            n += bucket_is_empty(b) ? 0 : 1;
        }
        u16(n);
        for (size_t i = 0; i < buckets.size(); ++i) {
            if (bucket_is_empty(buckets[i])) {
                continue;
            }
            u8(static_cast<uint8_t>(i));
            u64(static_cast<uint64_t>(buckets[i].sum));
            u64(static_cast<uint64_t>(buckets[i].sum >> 64));
            varint(buckets[i].count);
        }
    }
    std::string take() { return std::move(_buf); }

private:
    void append(const void* p, size_t n) { _buf.append(static_cast<const char*>(p), n); }
    std::string _buf;
};

class Reader {
public:
    explicit Reader(std::string_view data) : _data(data) {}
    bool u8(uint8_t* v) { return read(v, sizeof(*v)); }
    bool u16(uint16_t* v) { return read(v, sizeof(*v)); }
    bool u32(uint32_t* v) { return read(v, sizeof(*v)); }
    bool u64(uint64_t* v) { return read(v, sizeof(*v)); }
    bool i64(int64_t* v) { return read(v, sizeof(*v)); }
    bool varint(uint64_t* v) {
        *v = 0;
        for (int shift = 0; shift < 64; shift += 7) {
            uint8_t b = 0;
            if (!u8(&b)) {
                return false;
            }
            *v |= static_cast<uint64_t>(b & 0x7F) << shift;
            if ((b & 0x80) == 0) {
                return true;
            }
        }
        return false;
    }
    bool str(std::string* s) {
        uint64_t n = 0;
        if (!varint(&n) || n > _data.size() - _pos) {
            return false;
        }
        s->assign(_data.data() + _pos, n);
        _pos += n;
        return true;
    }
    bool buckets(DigestBuckets* out) {
        *out = DigestBuckets {};
        uint16_t n = 0;
        if (!u16(&n) || n > RestoreDigest::kNumBuckets) {
            return false;
        }
        int last = -1;
        for (uint16_t i = 0; i < n; ++i) {
            uint8_t idx = 0;
            uint64_t lo = 0;
            uint64_t hi = 0;
            uint64_t count = 0;
            if (!u8(&idx) || !u64(&lo) || !u64(&hi) || !varint(&count) || idx <= last) {
                return false;
            }
            last = idx;
            (*out)[idx].sum = (static_cast<unsigned __int128>(hi) << 64) | lo;
            (*out)[idx].count = count;
        }
        return true;
    }
    bool at_end() const { return _pos == _data.size(); }

private:
    bool read(void* v, size_t n) {
        if (n > _data.size() - _pos) {
            return false;
        }
        std::memcpy(v, _data.data() + _pos, n);
        _pos += n;
        return true;
    }
    std::string_view _data;
    size_t _pos = 0;
};

std::string sha256_of(std::string_view data) {
    SHA256Digest sha;
    sha.reset(data.data(), data.size());
    return std::string(sha.digest());
}

} // namespace

std::string RestoreDigestDecomposed::serialize() const {
    Writer w;
    w.bytes(kMagic, sizeof(kMagic));
    w.u32(mor ? kFormatVersionMor : kFormatVersion);
    w.u32(algo_version);
    w.u8(mow ? 1 : 0);
    w.i64(tablet_id);
    w.i64(base_version);
    w.u64(rows);
    w.str(schema_sig);
    w.str(root);
    w.u32(static_cast<uint32_t>(rowsets.size()));
    for (const auto& rs : rowsets) {
        w.i64(rs.start_version);
        w.i64(rs.end_version);
        w.str(rs.rowset_id);
        w.buckets(rs.buckets);
    }
    w.u32(static_cast<uint32_t>(marks.size()));
    for (const auto& mark : marks) {
        w.i64(mark.mark_version);
        w.buckets(mark.buckets);
    }
    if (mor) {
        // format version 2: the whole digests
        w.u32(static_cast<uint32_t>(wholes.size()));
        for (const auto& whole : wholes) {
            w.i64(whole.version);
            w.buckets(whole.buckets);
        }
    }
    return w.take();
}

std::string RestoreDigestDecomposed::file_root() const {
    return sha256_of(serialize());
}

Status RestoreDigestDecomposed::parse(std::string_view content, std::string_view expected_root,
                                      int64_t expected_tablet_id, RestoreDigestDecomposed* out) {
    std::string root = sha256_of(content);
    if (root != expected_root) {
        return Status::Error<ErrorCode::RESTORE_MANIFEST_MISMATCH, false>(
                "decomposed digest of tablet {} does not match the root, expected {}, actual {}",
                expected_tablet_id, expected_root, root);
    }
    auto invalid = [&](std::string_view reason) {
        return Status::Error<ErrorCode::RESTORE_MANIFEST_MISMATCH, false>(
                "invalid decomposed digest of tablet {}: {}", expected_tablet_id, reason);
    };
    Reader r(content);
    char magic[4] = {};
    for (char& c : magic) {
        uint8_t b = 0;
        if (!r.u8(&b)) {
            return invalid("truncated header");
        }
        c = static_cast<char>(b);
    }
    if (std::memcmp(magic, kMagic, sizeof(kMagic)) != 0) {
        return invalid("bad magic");
    }
    RestoreDigestDecomposed d;
    uint32_t format = 0;
    uint8_t mow = 0;
    if (!r.u32(&format) || (format != kFormatVersion && format != kFormatVersionMor)) {
        return invalid("unknown format version");
    }
    if (!r.u32(&d.algo_version) || !r.u8(&mow) || !r.i64(&d.tablet_id) || !r.i64(&d.base_version) ||
        !r.u64(&d.rows) || !r.str(&d.schema_sig) || !r.str(&d.root)) {
        return invalid("truncated header");
    }
    d.mow = mow != 0;
    if (d.tablet_id != expected_tablet_id) {
        return invalid(fmt::format("tablet id mismatch, found {}", d.tablet_id));
    }
    uint32_t n = 0;
    if (!r.u32(&n)) {
        return invalid("truncated rowsets");
    }
    for (uint32_t i = 0; i < n; ++i) {
        RestoreDigestRowsetPart rs;
        if (!r.i64(&rs.start_version) || !r.i64(&rs.end_version) || !r.str(&rs.rowset_id) ||
            !r.buckets(&rs.buckets)) {
            return invalid("bad rowset part");
        }
        if (rs.end_version < rs.start_version ||
            (!d.rowsets.empty() && rs.end_version <= d.rowsets.back().end_version)) {
            return invalid("rowset versions are not ascending");
        }
        d.rowsets.push_back(std::move(rs));
    }
    if (!r.u32(&n)) {
        return invalid("truncated marks");
    }
    for (uint32_t i = 0; i < n; ++i) {
        RestoreDigestMarkPart mark;
        if (!r.i64(&mark.mark_version) || !r.buckets(&mark.buckets)) {
            return invalid("bad mark part");
        }
        if (!d.marks.empty() && mark.mark_version <= d.marks.back().mark_version) {
            return invalid("mark versions are not ascending");
        }
        d.marks.push_back(std::move(mark));
    }
    if (format == kFormatVersionMor) {
        d.mor = true;
        if (d.mow || !d.marks.empty()) {
            return invalid("a merge on read file has marks");
        }
        if (!r.u32(&n)) {
            return invalid("truncated whole digests");
        }
        for (uint32_t i = 0; i < n; ++i) {
            RestoreDigestWholePart whole;
            if (!r.i64(&whole.version) || !r.buckets(&whole.buckets)) {
                return invalid("bad whole digest");
            }
            if (!d.wholes.empty() && whole.version <= d.wholes.back().version) {
                return invalid("whole digest versions are not ascending");
            }
            d.wholes.push_back(std::move(whole));
        }
        if (d.wholes.empty() || d.wholes.back().version != d.base_version) {
            return invalid("the last whole digest is not at the base version");
        }
        for (const auto& whole : d.wholes) {
            if (!d.is_rowset_end(whole.version)) {
                return invalid("a whole digest is not at the end of a rowset");
            }
        }
    }
    if (!r.at_end()) {
        return invalid("trailing bytes");
    }
    if (d.rowsets.empty() || d.rowsets.back().end_version != d.base_version) {
        return invalid("the last rowset does not end at the base version");
    }
    *out = std::move(d);
    return Status::OK();
}

bool RestoreDigestDecomposed::is_rowset_end(int64_t version) const {
    auto it = std::lower_bound(
            rowsets.begin(), rowsets.end(), version,
            [](const RestoreDigestRowsetPart& rs, int64_t v) { return rs.end_version < v; });
    return it != rowsets.end() && it->end_version == version;
}

bool RestoreDigestDecomposed::is_boundary(int64_t version) const {
    if (mor) {
        return std::any_of(wholes.begin(), wholes.end(),
                           [version](const RestoreDigestWholePart& w) { return w.version == version; });
    }
    return is_rowset_end(version);
}

Status RestoreDigestDecomposed::compose(int64_t version, RestoreDigest* digest) const {
    if (!is_boundary(version)) {
        return Status::NotSupported(
                "decomposed digest: version {} is not a rowset boundary of the tablet {} (base "
                "version {})",
                version, tablet_id, base_version);
    }
    RestoreDigest total;
    total.schema_sig = schema_sig;
    if (mor) {
        // the whole digest of that version
        for (const auto& whole : wholes) {
            if (whole.version == version) {
                total.buckets = whole.buckets;
            }
        }
        for (const auto& rs : rowsets) {
            if (rs.end_version > version) {
                break;
            }
            total.rowset_count++;
        }
    } else {
        for (const auto& rs : rowsets) {
            if (rs.end_version > version) {
                break;
            }
            total.rowset_count++;
            for (size_t i = 0; i < RestoreDigest::kNumBuckets; ++i) {
                total.buckets[i].sum += rs.buckets[i].sum;
                total.buckets[i].count += rs.buckets[i].count;
            }
        }
        for (const auto& mark : marks) {
            if (mark.mark_version > version) {
                break;
            }
            for (size_t i = 0; i < RestoreDigest::kNumBuckets; ++i) {
                total.buckets[i].sum -= mark.buckets[i].sum;
                total.buckets[i].count -= mark.buckets[i].count;
            }
        }
    }
    for (const auto& b : total.buckets) {
        total.rows += b.count;
    }
    total.finalize();
    *digest = std::move(total);
    return Status::OK();
}

} // namespace doris
