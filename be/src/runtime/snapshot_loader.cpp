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

// IWYU pragma: no_include <bthread/errno.h>
#include <absl/strings/str_split.h>
#include <errno.h> // IWYU pragma: keep
#include <fmt/format.h>
#include <gen_cpp/AgentService_types.h>
#include <gen_cpp/FrontendService.h>
#include <gen_cpp/FrontendService_types.h>
#include <gen_cpp/HeartbeatService_types.h>
#include <gen_cpp/PlanNodes_types.h>
#include <gen_cpp/Status_types.h>
#include <gen_cpp/Types_types.h>
#include <rapidjson/document.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <cstring>
#include <filesystem>
#include <istream>
#include <set>
#include <shared_mutex>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "common/cast_set.h"
#include "common/config.h"
#include "common/logging.h"
#include "io/fs/broker_file_system.h"
#include "io/fs/file_reader.h"
#include "io/fs/file_system.h"
#include "io/fs/file_writer.h"
#include "io/fs/hdfs_file_system.h"
#include "io/fs/local_file_system.h"
#include "io/fs/path.h"
#include "io/fs/remote_file_system.h"
#include "io/fs/s3_file_system.h"
#include "io/hdfs_builder.h"
#include "runtime/cluster_info.h"
#include "runtime/exec_env.h"
#include "service/backend_options.h"
#include "service/http/http_client.h"
#include "storage/data_dir.h"
#include "storage/restore_digest/restore_digest.h"
#include "storage/snapshot/snapshot_manager.h"
#include "storage/storage_engine.h"
#include "storage/tablet/tablet.h"
#include "storage/tablet/tablet_manager.h"
#include "util/client_cache.h"
#include "util/debug_points.h"
#include "util/defer_op.h"
#include "util/md5.h"
#include "util/s3_uri.h"
#include "util/s3_util.h"
#include "util/sha.h"
#include "util/slice.h"
#include "util/thrift_rpc_helper.h"

namespace doris {

namespace {

Status restore_not_retryable(const std::string& msg) {
    return Status::InternalError("{} {}", SnapshotLoader::kRestoreNotRetryable, msg);
}

// Sleeps for the debug point `name` (param sleep_ms), if it is for this tablet (param tablet_id, any if unset)
// and this backend (param backend, the host of the backend, any if unset).
void restore_debug_delay(const char* name, int64_t tablet_id) {
    DBUG_EXECUTE_IF(name, {
        auto ptablet = dp->param<int64_t>("tablet_id", 0);
        auto pbackend = dp->param<std::string>("backend", "");
        if ((ptablet == 0 || ptablet == tablet_id) &&
            (pbackend.empty() || pbackend == BackendOptions::get_localhost())) {
            int64_t ms = dp->param<int64_t>("sleep_ms", 1000);
            LOG(INFO) << "debug point " << name << ": sleep " << ms << " ms, tablet " << tablet_id;
            std::this_thread::sleep_for(std::chrono::milliseconds(ms));
        }
    });
}

// What the restore job judges the staging tablet by: the evidence in the tablet meta, the max version, and whether
// the tablet is still the one created by the job (only the empty initial rowset).
struct RestoreTabletState {
    bool has_stage = false;
    RestoreStagePB stage;
    int64_t max_version = -1;
    bool initial_only = false;
};

RestoreTabletState read_restore_tablet_state(const TabletSharedPtr& tablet) {
    RestoreTabletState state;
    std::shared_lock rdlock(tablet->get_header_lock());
    state.has_stage = tablet->tablet_meta()->has_restore_stage();
    if (state.has_stage) {
        state.stage = tablet->tablet_meta()->restore_stage();
    }
    state.max_version = tablet->max_version_unlocked();
    const auto& metas = tablet->tablet_meta()->all_rs_metas();
    if (metas.size() == 1) {
        const auto& rs_meta = metas.begin()->second;
        state.initial_only = rs_meta->start_version() == 0 && rs_meta->num_rows() == 0 &&
                             rs_meta->num_segments() == 0 && !rs_meta->has_delete_predicate();
    }
    return state;
}

} // namespace

struct LocalFileStat {
    uint64_t size;
    std::string md5;
};

struct RemoteFileStat {
    std::string url;
    std::string md5;
    uint64_t size;
};

class SnapshotHttpDownloader {
public:
    SnapshotHttpDownloader(const TRemoteTabletSnapshot& remote_tablet_snapshot,
                           TabletSharedPtr tablet, SnapshotLoader& snapshot_loader)
            : _tablet(std::move(tablet)),
              _snapshot_loader(snapshot_loader),
              _remote_tablet_snapshot(remote_tablet_snapshot),
              _local_tablet_id(remote_tablet_snapshot.local_tablet_id),
              _remote_tablet_id(remote_tablet_snapshot.remote_tablet_id),
              _local_path(remote_tablet_snapshot.local_snapshot_path),
              _remote_path(remote_tablet_snapshot.remote_snapshot_path),
              _remote_be_addr(remote_tablet_snapshot.remote_be_addr) {
        _incremental = remote_tablet_snapshot.__isset.incremental;
        auto& token = remote_tablet_snapshot.remote_token;
        auto& remote_be_addr = remote_tablet_snapshot.remote_be_addr;

        // HEAD http://172.16.0.14:6781/api/_tablet/_download?token=e804dd27-86da-4072-af58-70724075d2a4&file=/home/ubuntu/doris_master/output/be/storage/snapshot/20230410102306.9.180/
        _base_url = fmt::format("http://{}:{}/api/_tablet/_download?token={}&channel=ingest_binlog",
                                remote_be_addr.hostname, remote_be_addr.port, token);
    }
    ~SnapshotHttpDownloader() = default;
    SnapshotHttpDownloader(const SnapshotHttpDownloader&) = delete;
    SnapshotHttpDownloader& operator=(const SnapshotHttpDownloader&) = delete;

    void set_report_progress_callback(std::function<Status()> report_progress) {
        _report_progress_callback = std::move(report_progress);
    }

    size_t get_download_file_num() { return _need_download_files.size(); }

    // Delete all the local files first and download all the remote files, without reusing any
    // local file. Used to download a tablet again after the manifest check failed.
    void set_disable_reuse(bool disable_reuse) { _disable_reuse = disable_reuse; }

    // Whether download() failed because the downloaded files do not match the manifest.
    bool manifest_mismatch() const { return _manifest_mismatch; }

    const ManifestCheckResult& manifest_check_result() const { return _manifest_check_result; }

    // The files reused and downloaded of the tablet, valid after download().
    const SnapshotDownloadStats& stats() const { return _stats; }

    Status download();

private:
    constexpr static int kDownloadFileMaxRetry = 3;

    // Load existing files from local snapshot path, compute the md5sum of the files
    // if enable_download_md5sum_check is true
    Status _load_existing_files();

    // List remote files from remote be, and find the hdr file
    Status _list_remote_files();

    // Download hdr file from remote be to a tmp file
    Status _download_hdr_file();

    // Link same rowset files by compare local hdr file and remote hdr file
    // if the local files are copied from the remote rowset, link them as the
    // remote rowset files, to avoid the duplicated downloading.
    Status _link_same_rowset_files();

    // Get all remote file stats, excluding the hdr file.
    Status _get_remote_file_stats();

    // Compute the need download files according to the local files md5sum (if enable_download_md5sum_check is true)
    void _get_need_download_files();

    // Download all need download files
    Status _download_files();

    // Install remote hdr file to local snapshot path from the tmp file
    Status _install_remote_hdr_file();

    // Delete orphan files, which are not in remote
    Status _delete_orphan_files();

    // Download a file from remote be to local path with the file stat
    Status _download_http_file(DataDir* data_dir, const std::string& remote_file_url,
                               const std::string& local_file_path,
                               const RemoteFileStat& remote_filestat);

    // Get the file stat from remote be
    Status _get_http_file_stat(const std::string& remote_file_url, RemoteFileStat* file_stat);

    // Whether the manifest root is given and the check is enabled.
    bool _use_manifest() const;

    // Fetch the manifest from the remote BE and check it against the root, then take the files to
    // download from it.
    Status _fetch_manifest();

    // Check the local snapshot against the manifest, if the manifest is used.
    Status _check_manifest();

    // The incremental restore: cut the remote files, the manifest and the tablet meta to the increment.
    Status _cut_to_increment();
    // Write the tablet meta of the increment to the snapshot dir, after the check against the manifest.
    Status _install_increment_hdr();

    TabletSharedPtr _tablet;
    SnapshotLoader& _snapshot_loader;
    const TRemoteTabletSnapshot& _remote_tablet_snapshot;
    std::function<Status()> _report_progress_callback;
    bool _disable_reuse = false;
    bool _manifest_mismatch = false;
    SnapshotManifest _manifest;
    ManifestCheckResult _manifest_check_result;
    SnapshotDownloadStats _stats;
    // The remote file names linked from the local files of the same lineage, with the file sizes.
    std::unordered_map<std::string, int64_t> _linked_files;

    std::string _base_url;
    int64_t _local_tablet_id;
    int64_t _remote_tablet_id;
    const std::string& _local_path;
    const std::string& _remote_path;
    const TNetworkAddress& _remote_be_addr;

    std::string _local_hdr_filename;
    std::string _remote_hdr_filename;
    std::vector<std::string> _remote_file_list;
    std::unordered_map<std::string, LocalFileStat> _local_files;
    std::unordered_map<std::string, RemoteFileStat> _remote_files;

    bool _incremental = false;
    TabletMetaPB _increment;

    std::string _tmp_hdr_file;
    RemoteFileStat _remote_hdr_filestat;
    std::vector<std::string> _need_download_files;
};

static std::string get_loaded_tag_path(const std::string& snapshot_path) {
    return snapshot_path + "/LOADED";
}

static Status write_loaded_tag(const std::string& snapshot_path, int64_t tablet_id) {
    std::unique_ptr<io::FileWriter> writer;
    std::string file = get_loaded_tag_path(snapshot_path);
    RETURN_IF_ERROR(io::global_local_filesystem()->create_file(file, &writer));
    return writer->close();
}

Status upload_with_checksum(io::RemoteFileSystem& fs, std::string_view local_path,
                            std::string_view remote_path, std::string_view checksum) {
    auto full_remote_path = fmt::format("{}.{}", remote_path, checksum);
    switch (fs.type()) {
    case io::FileSystemType::HDFS:
    case io::FileSystemType::BROKER: {
        std::string temp = fmt::format("{}.part", remote_path);
        RETURN_IF_ERROR(fs.upload(local_path, temp));
        RETURN_IF_ERROR(fs.rename(temp, full_remote_path));
        break;
    }
    case io::FileSystemType::S3:
        RETURN_IF_ERROR(fs.upload(local_path, full_remote_path));
        break;
    default:
        throw doris::Exception(
                Status::FatalError("unknown fs type: {}", static_cast<int>(fs.type())));
    }
    return Status::OK();
}

bool _end_with(std::string_view str, std::string_view match) {
    return str.size() >= match.size() &&
           str.compare(str.size() - match.size(), match.size(), match) == 0;
}

Status compute_file_digests(const std::string& path, std::string* md5, std::string* sha256,
                            int64_t* size) {
    io::FileReaderSPtr reader;
    RETURN_IF_ERROR(io::global_local_filesystem()->open_file(path, &reader));

    constexpr size_t kBufferSize = 1024 * 1024;
    std::vector<char> buffer(kBufferSize);
    Md5Digest md5_digest;
    SHA256Digest sha256_digest;
    sha256_digest.reset(nullptr, 0);

    size_t file_size = reader->size();
    size_t offset = 0;
    while (offset < file_size) {
        size_t bytes_to_read = std::min(kBufferSize, file_size - offset);
        size_t bytes_read = 0;
        Status st = reader->read_at(offset, Slice(buffer.data(), bytes_to_read), &bytes_read);
        if (!st.ok()) {
            static_cast<void>(reader->close());
            return st;
        }
        if (bytes_read == 0) {
            static_cast<void>(reader->close());
            return Status::IOError("unexpected end of file {}, read {} of {} bytes", path, offset,
                                   file_size);
        }
        if (md5 != nullptr) {
            md5_digest.update(buffer.data(), bytes_read);
        }
        if (sha256 != nullptr) {
            sha256_digest.update(buffer.data(), bytes_read);
        }
        offset += bytes_read;
    }
    RETURN_IF_ERROR(reader->close());

    if (md5 != nullptr) {
        md5_digest.digest();
        *md5 = md5_digest.hex();
    }
    if (sha256 != nullptr) {
        *sha256 = std::string(sha256_digest.digest());
    }
    if (size != nullptr) {
        *size = static_cast<int64_t>(offset);
    }
    return Status::OK();
}

std::string sha256_hex(std::string_view data) {
    SHA256Digest digest;
    digest.reset(data.data(), data.size());
    return std::string(digest.digest());
}

Status parent_path_of(const std::string& path, std::string* parent) {
    std::string_view p = path;
    while (p.size() > 1 && p.back() == '/') {
        p.remove_suffix(1);
    }
    size_t pos = p.find_last_of('/');
    if (pos == std::string_view::npos || pos == 0) {
        return Status::InternalError("no parent dir of path {}", path);
    }
    *parent = std::string(p.substr(0, pos));
    return Status::OK();
}

std::string SnapshotManifest::serialize() {
    std::sort(files.begin(), files.end(),
              [](const auto& a, const auto& b) { return a.name < b.name; });
    rapidjson::StringBuffer buffer;
    rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
    writer.StartObject();
    writer.Key("version");
    writer.Int(kVersion);
    writer.Key("tablet_id");
    writer.Int64(tablet_id);
    writer.Key("files");
    writer.StartArray();
    for (const auto& file : files) {
        writer.StartObject();
        writer.Key("n");
        writer.String(file.name.data(), static_cast<rapidjson::SizeType>(file.name.size()));
        writer.Key("s");
        writer.Int64(file.size);
        if (!file.md5.empty()) {
            writer.Key("m");
            writer.String(file.md5.data(), static_cast<rapidjson::SizeType>(file.md5.size()));
        }
        if (!file.sha256.empty()) {
            writer.Key("d");
            writer.String(file.sha256.data(), static_cast<rapidjson::SizeType>(file.sha256.size()));
        }
        writer.EndObject();
    }
    writer.EndArray();
    writer.EndObject();
    return std::string(buffer.GetString(), buffer.GetSize());
}

Status SnapshotManifest::parse(std::string_view content, std::string_view expected_root,
                               int64_t expected_tablet_id, SnapshotManifest* manifest) {
    std::string root = sha256_hex(content);
    if (root != expected_root) {
        return Status::Error<ErrorCode::RESTORE_MANIFEST_MISMATCH, false>(
                "manifest of tablet {} does not match the root, expected {}, actual {}",
                expected_tablet_id, expected_root, root);
    }
    auto invalid = [&](std::string_view reason) {
        return Status::Error<ErrorCode::RESTORE_MANIFEST_MISMATCH, false>(
                "invalid manifest of tablet {}: {}", expected_tablet_id, reason);
    };
    rapidjson::Document doc;
    doc.Parse(content.data(), content.size());
    if (doc.HasParseError() || !doc.IsObject()) {
        return invalid("not a json object");
    }
    if (!doc.HasMember("version") || !doc["version"].IsInt() ||
        doc["version"].GetInt() != kVersion) {
        return invalid("unknown version");
    }
    if (!doc.HasMember("tablet_id") || !doc["tablet_id"].IsInt64() ||
        doc["tablet_id"].GetInt64() != expected_tablet_id) {
        return invalid("tablet id mismatch");
    }
    if (!doc.HasMember("files") || !doc["files"].IsArray()) {
        return invalid("no files");
    }
    manifest->tablet_id = expected_tablet_id;
    manifest->files.clear();
    std::set<std::string> names;
    for (const auto& item : doc["files"].GetArray()) {
        if (!item.IsObject() || !item.HasMember("n") || !item["n"].IsString() ||
            !item.HasMember("s") || !item["s"].IsInt64() || item["s"].GetInt64() < 0) {
            return invalid("a file has no name or size");
        }
        SnapshotManifestFile file;
        file.name = std::string(item["n"].GetString(), item["n"].GetStringLength());
        file.size = item["s"].GetInt64();
        if (file.name.empty() || file.name.find('/') != std::string::npos ||
            !names.insert(file.name).second) {
            return invalid(fmt::format("bad or duplicated file name {}", file.name));
        }
        if (item.HasMember("m")) {
            if (!item["m"].IsString()) {
                return invalid("bad md5");
            }
            file.md5 = std::string(item["m"].GetString(), item["m"].GetStringLength());
        }
        if (item.HasMember("d")) {
            if (!item["d"].IsString()) {
                return invalid("bad digest");
            }
            file.sha256 = std::string(item["d"].GetString(), item["d"].GetStringLength());
        }
        manifest->files.push_back(std::move(file));
    }
    return Status::OK();
}

std::string SnapshotManifest::remote_file_name(int64_t tablet_id, std::string_view root) {
    return fmt::format("__manifest__{}.{}", tablet_id, root.substr(0, 32));
}

std::string prefix_digest_remote_file_name(int64_t tablet_id, std::string_view root) {
    return fmt::format("__rdigest__{}.{}", tablet_id, root.substr(0, 32));
}

Status write_prefix_digest_file(const std::string& content, const std::string& path,
                                std::string* root) {
    io::FileWriterPtr writer;
    RETURN_IF_ERROR(io::global_local_filesystem()->create_file(path, &writer));
    RETURN_IF_ERROR(writer->append(Slice(content)));
    RETURN_IF_ERROR(writer->close());
    *root = sha256_hex(content);
    return Status::OK();
}

Status write_snapshot_manifest(SnapshotManifest& manifest, const std::string& path,
                               std::string* root) {
    std::string content = manifest.serialize();
    io::FileWriterPtr writer;
    RETURN_IF_ERROR(io::global_local_filesystem()->create_file(path, &writer));
    RETURN_IF_ERROR(writer->append(Slice(content)));
    RETURN_IF_ERROR(writer->close());
    *root = sha256_hex(content);
    return Status::OK();
}

static Status read_local_file(const std::string& path, std::string* content) {
    io::FileReaderSPtr reader;
    RETURN_IF_ERROR(io::global_local_filesystem()->open_file(path, &reader));
    content->resize(reader->size());
    size_t bytes_read = 0;
    Status st = reader->read_at(0, Slice(content->data(), content->size()), &bytes_read);
    static_cast<void>(reader->close());
    RETURN_IF_ERROR(st);
    if (bytes_read != content->size()) {
        return Status::IOError("failed to read {}, read {} of {} bytes", path, bytes_read,
                               content->size());
    }
    return Status::OK();
}

Status fetch_prefix_digest_from_remote_be(const TRemoteTabletSnapshot& remote_tablet_snapshot,
                                          const std::string& root, RestoreDigestDecomposed* digest) {
    // <storage_root>/snapshot/<time>.<seq>.<timeout>/<tablet_id>/rdigest, next to the manifest
    std::string remote_parent;
    RETURN_IF_ERROR(parent_path_of(remote_tablet_snapshot.remote_snapshot_path, &remote_parent));
    const auto& addr = remote_tablet_snapshot.remote_be_addr;
    std::string url = fmt::format(
            "http://{}:{}/api/_tablet/_download?token={}&channel=ingest_binlog&file={}/{}",
            addr.hostname, addr.port, remote_tablet_snapshot.remote_token, remote_parent,
            RestoreDigestDecomposed::kLocalFileName);
    std::string content;
    auto fetch_cb = [&url, &content](HttpClient* client) {
        content.clear();
        RETURN_IF_ERROR(client->init(url));
        client->set_timeout_ms(config::download_binlog_meta_timeout_ms);
        return client->execute(&content);
    };
    RETURN_IF_ERROR(HttpClient::execute_with_retry(3, 1, fetch_cb));
    return RestoreDigestDecomposed::parse(content, root, remote_tablet_snapshot.remote_tablet_id,
                                          digest);
}

void SnapshotDownloadStats::classify_tablet() {
    int64_t reused = linked_files + skipped_files;
    if (reused == 0 && downloaded_files == 0) {
        return;
    }
    if (tablets_incremental > 0) {
        // an increment is not a reuse of the local files
        return;
    }
    if (downloaded_files == 0) {
        ++tablets_full_reuse;
    } else if (reused == 0) {
        ++tablets_no_reuse;
    } else {
        ++tablets_partial_reuse;
    }
}

void SnapshotDownloadStats::merge(const SnapshotDownloadStats& other) {
    linked_files += other.linked_files;
    linked_bytes += other.linked_bytes;
    skipped_files += other.skipped_files;
    skipped_bytes += other.skipped_bytes;
    downloaded_files += other.downloaded_files;
    downloaded_bytes += other.downloaded_bytes;
    tablets_full_reuse += other.tablets_full_reuse;
    tablets_partial_reuse += other.tablets_partial_reuse;
    tablets_no_reuse += other.tablets_no_reuse;
    unmatched_rowsets += other.unmatched_rowsets;
    unmatched_no_source_rowset_id += other.unmatched_no_source_rowset_id;
    unmatched_source_not_in_snapshot += other.unmatched_source_not_in_snapshot;
    unmatched_version_mismatch += other.unmatched_version_mismatch;
    tablets_incremental += other.tablets_incremental;
    incremental_files += other.incremental_files;
    incremental_bytes += other.incremental_bytes;
}

std::string SnapshotDownloadStats::to_string() const {
    return fmt::format(
            "linked: {} files {} bytes, skipped: {} files {} bytes, downloaded: {} files {} bytes, "
            "tablets full/partial/no reuse: {}/{}/{}, unmatched rowsets: {} (no_source_rowset_id "
            "{}, source_not_in_snapshot {}, version_mismatch {})",
            linked_files, linked_bytes, skipped_files, skipped_bytes, downloaded_files,
            downloaded_bytes, tablets_full_reuse, tablets_partial_reuse, tablets_no_reuse,
            unmatched_rowsets, unmatched_no_source_rowset_id, unmatched_source_not_in_snapshot,
            unmatched_version_mismatch) +
           (tablets_incremental > 0
                    ? fmt::format(", incremental tablets: {} ({} files {} bytes)", tablets_incremental,
                                  incremental_files, incremental_bytes)
                    : std::string());
}

TDownloadStats SnapshotDownloadStats::to_thrift() const {
    TDownloadStats result;
    result.__set_linked_files(linked_files);
    result.__set_linked_bytes(linked_bytes);
    result.__set_skipped_files(skipped_files);
    result.__set_skipped_bytes(skipped_bytes);
    result.__set_downloaded_files(downloaded_files);
    result.__set_downloaded_bytes(downloaded_bytes);
    result.__set_tablets_full_reuse(tablets_full_reuse);
    result.__set_tablets_partial_reuse(tablets_partial_reuse);
    result.__set_tablets_no_reuse(tablets_no_reuse);
    result.__set_unmatched_rowsets(unmatched_rowsets);
    result.__set_unmatched_no_source_rowset_id(unmatched_no_source_rowset_id);
    result.__set_unmatched_source_not_in_snapshot(unmatched_source_not_in_snapshot);
    result.__set_unmatched_version_mismatch(unmatched_version_mismatch);
    result.__set_tablets_incremental(tablets_incremental);
    result.__set_incremental_files(incremental_files);
    result.__set_incremental_bytes(incremental_bytes);
    return result;
}

void count_unmatched_rowsets(const TabletMetaPB& local_meta, const TabletMetaPB& remote_meta,
                             SnapshotDownloadStats* stats) {
    std::vector<const RowsetMetaPB*> local_rowsets;
    std::unordered_map<std::string, const RowsetMetaPB*> local_by_id;
    for (const auto& meta : local_meta.rs_metas()) {
        if (meta.has_resource_id() || meta.num_segments() == 0) {
            continue;
        }
        local_rowsets.push_back(&meta);
        local_by_id.emplace(meta.rowset_id_v2(), &meta);
    }
    // the source rowset id of the local rowsets -> the local rowsets
    std::unordered_multimap<std::string, const RowsetMetaPB*> local_by_source;
    for (const auto* meta : local_rowsets) {
        if (meta->has_source_rowset_id()) {
            local_by_source.emplace(meta->source_rowset_id(), meta);
        }
    }

    auto same_version = [](const RowsetMetaPB& a, const RowsetMetaPB& b) {
        return a.start_version() == b.start_version() && a.end_version() == b.end_version();
    };
    for (const auto& remote : remote_meta.rs_metas()) {
        if (remote.has_resource_id() || remote.num_segments() == 0) {
            continue;
        }
        bool matched = false;
        bool version_mismatch = false;
        auto sources = local_by_source.equal_range(remote.rowset_id_v2());
        for (auto it = sources.first; it != sources.second; ++it) {
            (same_version(*it->second, remote) ? matched : version_mismatch) = true;
        }
        if (remote.has_source_rowset_id()) {
            auto it = local_by_id.find(remote.source_rowset_id());
            if (it != local_by_id.end()) {
                (same_version(*it->second, remote) ? matched : version_mismatch) = true;
            }
        }
        if (matched) {
            continue;
        }

        ++stats->unmatched_rowsets;
        if (version_mismatch) {
            ++stats->unmatched_version_mismatch;
            continue;
        }
        bool overlapped = false;
        bool overlapped_without_source = false;
        for (const auto* local : local_rowsets) {
            if (local->start_version() > remote.end_version() ||
                local->end_version() < remote.start_version()) {
                continue;
            }
            overlapped = true;
            overlapped_without_source |= !local->has_source_rowset_id();
        }
        if (overlapped && !overlapped_without_source) {
            ++stats->unmatched_source_not_in_snapshot;
        } else {
            ++stats->unmatched_no_source_rowset_id;
        }
    }
}

// The file which marks a tablet snapshot as moved to the tablet dir, see SnapshotLoader::move().

Status select_incremental_rowsets(const TabletMetaPB& remote, int64_t base_version,
                                  int64_t end_version, TabletMetaPB* increment,
                                  std::vector<std::string>* rowset_ids) {
    std::vector<const RowsetMetaPB*> selected;
    for (const auto& rs : remote.rs_metas()) {
        if (rs.start_version() > base_version) {
            selected.push_back(&rs);
        } else if (rs.end_version() > base_version) {
            return Status::InternalError(
                    "the remote tablet {} can not be cut at version {}: rowset [{}-{}] crosses it",
                    remote.tablet_id(), base_version, rs.start_version(), rs.end_version());
        }
    }
    std::sort(selected.begin(), selected.end(), [](const RowsetMetaPB* a, const RowsetMetaPB* b) {
        return a->start_version() < b->start_version();
    });
    int64_t expected_start = base_version + 1;
    for (const auto* rs : selected) {
        if (rs->start_version() != expected_start || rs->end_version() < rs->start_version()) {
            return Status::InternalError(
                    "the rowsets of the remote tablet {} after version {} are not a chain: "
                    "expected a rowset starting at {}, got [{}-{}]",
                    remote.tablet_id(), base_version, expected_start, rs->start_version(),
                    rs->end_version());
        }
        if (rs->has_resource_id()) {
            return Status::InternalError("rowset [{}-{}] of the remote tablet {} is in a remote storage",
                                         rs->start_version(), rs->end_version(),
                                         remote.tablet_id());
        }
        if (rs->rowset_id_v2().empty()) {
            return Status::InternalError("rowset [{}-{}] of the remote tablet {} has no rowset id v2",
                                         rs->start_version(), rs->end_version(),
                                         remote.tablet_id());
        }
        expected_start = rs->end_version() + 1;
    }
    if (selected.empty() || expected_start != end_version + 1) {
        return Status::InternalError(
                "the rowsets of the remote tablet {} after version {} end at version {}, expected {}",
                remote.tablet_id(), base_version, expected_start - 1, end_version);
    }
    *increment = remote;
    increment->clear_rs_metas();
    increment->clear_inc_rs_metas();
    increment->clear_stale_rs_metas();
    increment->clear_delete_bitmap();
    rowset_ids->clear();
    for (const auto* rs : selected) {
        *increment->add_rs_metas() = *rs;
        rowset_ids->push_back(rs->rowset_id_v2());
    }
    return Status::OK();
}

bool is_rowset_file(const std::string& file_name, const std::string& rowset_id) {
    return file_name.size() > rowset_id.size() + 1 &&
           file_name.compare(0, rowset_id.size(), rowset_id) == 0 &&
           file_name[rowset_id.size()] == '_';
}

Status cut_manifest_to_increment(const SnapshotManifest& manifest, const TabletMetaPB& increment,
                                 const std::vector<std::string>& rowset_ids,
                                 SnapshotManifest* cut) {
    cut->tablet_id = manifest.tablet_id;
    cut->files.clear();
    std::map<std::string, int64_t> dat_files;
    for (const auto& id : rowset_ids) {
        dat_files[id] = 0;
    }
    for (const auto& file : manifest.files) {
        if (file.name.ends_with(".hdr")) {
            cut->files.push_back(file);
            continue;
        }
        for (const auto& id : rowset_ids) {
            if (is_rowset_file(file.name, id)) {
                cut->files.push_back(file);
                if (file.name.ends_with(".dat")) {
                    ++dat_files[id];
                }
                break;
            }
        }
    }
    for (const auto& rs : increment.rs_metas()) {
        if (dat_files[rs.rowset_id_v2()] != rs.num_segments()) {
            return Status::Error<ErrorCode::RESTORE_MANIFEST_MISMATCH, false>(
                    "the manifest of tablet {} has {} segment files of rowset {} [{}-{}], expected {}",
                    manifest.tablet_id, dat_files[rs.rowset_id_v2()], rs.rowset_id_v2(),
                    rs.start_version(), rs.end_version(), rs.num_segments());
        }
    }
    return Status::OK();
}

static constexpr std::string_view kLoadedTagFileName = "LOADED";

Status check_tablet_snapshot_manifest(const std::string& local_path, int64_t local_tablet_id,
                                      const SnapshotManifest& manifest, bool check_digest,
                                      ManifestCheckResult* result) {
    result->checked = false;
    result->digest_checked = false;

    // expected local file name -> the file in the manifest
    std::map<std::string, const SnapshotManifestFile*> expected_files;
    const std::string local_hdr_name = fmt::format("{}.hdr", local_tablet_id);
    for (const auto& file : manifest.files) {
        // the tablet meta file is renamed to the local tablet id when downloading.
        std::string local_name = _end_with(file.name, ".hdr") ? local_hdr_name : file.name;
        if (!expected_files.emplace(std::move(local_name), &file).second) {
            return Status::Error<ErrorCode::RESTORE_MANIFEST_MISMATCH, false>(
                    "invalid manifest of tablet {}, duplicated file {}", local_tablet_id,
                    file.name);
        }
    }

    // local file name -> file size
    std::map<std::string, int64_t> local_files;
    bool exists = true;
    std::vector<io::FileInfo> files;
    RETURN_IF_ERROR(io::global_local_filesystem()->list(local_path, true, &files, &exists));
    if (!exists) {
        return Status::Error<ErrorCode::RESTORE_MANIFEST_MISMATCH, false>(
                "restore manifest check failed, tablet {}, path {}: the snapshot dir does not "
                "exist",
                local_tablet_id, local_path);
    }
    for (const auto& file : files) {
        if (file.file_name == kLoadedTagFileName) {
            continue;
        }
        local_files.emplace(file.file_name, file.file_size);
    }

    // 1. the set of files and the size of each file, report the first mismatch in name order.
    std::set<std::string> all_names;
    for (const auto& entry : expected_files) {
        all_names.insert(entry.first);
    }
    for (const auto& entry : local_files) {
        all_names.insert(entry.first);
    }
    for (const auto& name : all_names) {
        auto expected = expected_files.find(name);
        auto local = local_files.find(name);
        if (local == local_files.end()) {
            return Status::Error<ErrorCode::RESTORE_MANIFEST_MISMATCH, false>(
                    "restore manifest check failed, tablet {}, path {}: missing file {}, "
                    "expected size {}, actual: not exist",
                    local_tablet_id, local_path, name, expected->second->size);
        }
        if (expected == expected_files.end()) {
            return Status::Error<ErrorCode::RESTORE_MANIFEST_MISMATCH, false>(
                    "restore manifest check failed, tablet {}, path {}: unexpected file {}, "
                    "expected: not exist, actual size {}",
                    local_tablet_id, local_path, name, local->second);
        }
        if (expected->second->size != local->second) {
            return Status::Error<ErrorCode::RESTORE_MANIFEST_MISMATCH, false>(
                    "restore manifest check failed, tablet {}, path {}: size mismatch of file {}, "
                    "expected {}, actual {}",
                    local_tablet_id, local_path, name, expected->second->size, local->second);
        }
    }

    // 2. the digest of each file, if required and recorded.
    bool all_digest_checked = true;
    if (check_digest) {
        for (const auto& [name, expected] : expected_files) {
            if (name == local_hdr_name) {
                // the tablet meta file is rewritten in restore, it has no digest to check.
                continue;
            }
            if (expected->sha256.empty()) {
                all_digest_checked = false;
                continue;
            }
            std::string sha256;
            RETURN_IF_ERROR(compute_file_digests(local_path + "/" + name, nullptr, &sha256));
            if (sha256 != expected->sha256) {
                return Status::Error<ErrorCode::RESTORE_MANIFEST_MISMATCH, false>(
                        "restore manifest check failed, tablet {}, path {}: sha256 mismatch of "
                        "file {}, expected {}, actual {}",
                        local_tablet_id, local_path, name, expected->sha256, sha256);
            }
        }
    }

    result->checked = true;
    result->digest_checked = check_digest && all_digest_checked;
    return Status::OK();
}

Status SnapshotHttpDownloader::_get_http_file_stat(const std::string& remote_file_url,
                                                   RemoteFileStat* file_stat) {
    uint64_t file_size = 0;
    std::string file_md5;
    auto get_file_stat_cb = [&remote_file_url, &file_size, &file_md5](HttpClient* client) {
        int64_t timeout_ms = config::download_binlog_meta_timeout_ms;
        std::string url = remote_file_url;
        if (config::enable_download_md5sum_check) {
            // compute md5sum is time-consuming, so we set a longer timeout
            timeout_ms = config::download_binlog_meta_timeout_ms * 3;
            url = fmt::format("{}&acquire_md5=true", remote_file_url);
        }
        RETURN_IF_ERROR(client->init(url));
        client->set_timeout_ms(timeout_ms);
        RETURN_IF_ERROR(client->head());
        RETURN_IF_ERROR(client->get_content_length(&file_size));
        if (config::enable_download_md5sum_check) {
            RETURN_IF_ERROR(client->get_content_md5(&file_md5));
        }
        return Status::OK();
    };
    RETURN_IF_ERROR(HttpClient::execute_with_retry(kDownloadFileMaxRetry, 1, get_file_stat_cb));
    file_stat->url = remote_file_url;
    file_stat->size = file_size;
    file_stat->md5 = std::move(file_md5);
    return Status::OK();
}

Status SnapshotHttpDownloader::_download_http_file(DataDir* data_dir,
                                                   const std::string& remote_file_url,
                                                   const std::string& local_file_path,
                                                   const RemoteFileStat& remote_filestat) {
    auto file_size = remote_filestat.size;
    const auto& remote_file_md5 = remote_filestat.md5;

    // check disk capacity
    if (data_dir->reach_capacity_limit(file_size)) {
        return Status::Error<ErrorCode::EXCEEDED_LIMIT>(
                "reach the capacity limit of path {}, file_size={}", data_dir->path(), file_size);
    }

    uint64_t estimate_timeout = file_size / config::download_low_speed_limit_kbps / 1024;
    if (estimate_timeout < config::download_low_speed_time) {
        estimate_timeout = config::download_low_speed_time;
    }

    LOG(INFO) << "clone begin to download file from: " << remote_file_url
              << " to: " << local_file_path << ". size(B): " << file_size
              << ", timeout(s): " << estimate_timeout;

    auto download_cb = [&remote_file_url, &remote_file_md5, estimate_timeout, &local_file_path,
                        file_size](HttpClient* client) {
        RETURN_IF_ERROR(client->init(remote_file_url));
        client->set_timeout_ms(estimate_timeout * 1000);
        RETURN_IF_ERROR(client->download(local_file_path));

        std::error_code ec;
        // Check file length
        uint64_t local_file_size = std::filesystem::file_size(local_file_path, ec);
        if (ec) {
            LOG(WARNING) << "download file error" << ec.message();
            return Status::IOError("can't retrive file_size of {}, due to {}", local_file_path,
                                   ec.message());
        }
        if (local_file_size != file_size) {
            LOG(WARNING) << "download file length error"
                         << ", remote_path=" << remote_file_url << ", file_size=" << file_size
                         << ", local_file_size=" << local_file_size;
            return Status::InternalError("downloaded file size is not equal");
        }

        if (!remote_file_md5.empty()) { // keep compatibility
            std::string local_file_md5;
            RETURN_IF_ERROR(
                    io::global_local_filesystem()->md5sum(local_file_path, &local_file_md5));
            if (local_file_md5 != remote_file_md5) {
                LOG(WARNING) << "download file md5 error"
                             << ", remote_file_url=" << remote_file_url
                             << ", local_file_path=" << local_file_path
                             << ", remote_file_md5=" << remote_file_md5
                             << ", local_file_md5=" << local_file_md5;
                return Status::RuntimeError(
                        "download file {} md5 is not equal, local={}, remote={}", remote_file_url,
                        local_file_md5, remote_file_md5);
            }
        }

        return io::global_local_filesystem()->permission(local_file_path,
                                                         io::LocalFileSystem::PERMS_OWNER_RW);
    };
    auto status = HttpClient::execute_with_retry(kDownloadFileMaxRetry, 1, download_cb);
    if (!status.ok()) {
        LOG(WARNING) << "failed to download file from " << remote_file_url
                     << ", status: " << status.to_string();
        return status;
    }

    return Status::OK();
}

Status SnapshotHttpDownloader::_load_existing_files() {
    std::vector<std::string> existing_files;
    RETURN_IF_ERROR(_snapshot_loader._get_existing_files_from_local(_local_path, &existing_files));
    for (auto& local_file : existing_files) {
        // add file size
        std::string local_file_path = _local_path + "/" + local_file;
        std::error_code ec;
        uint64_t local_file_size = std::filesystem::file_size(local_file_path, ec);
        if (ec) {
            LOG(WARNING) << "download file error, can't retrive file_size of " << local_file_path
                         << ", due to " << ec.message();
            return Status::IOError("can't retrive file_size of {}, due to {}", local_file_path,
                                   ec.message());
        }

        // get md5sum
        std::string md5;
        if (config::enable_download_md5sum_check) {
            auto status = io::global_local_filesystem()->md5sum(local_file_path, &md5);
            if (!status.ok()) {
                LOG(WARNING) << "download file error, local file " << local_file_path
                             << " md5sum: " << status.to_string();
                return status;
            }
        }
        _local_files[local_file] = {local_file_size, md5};

        // get hdr file
        if (local_file.ends_with(".hdr")) {
            _local_hdr_filename = local_file;
        }
    }

    return Status::OK();
}

Status SnapshotHttpDownloader::_list_remote_files() {
    // get all these use http download action
    // http://172.16.0.14:6781/api/_tablet/_download?token=e804dd27-86da-4072-af58-70724075d2a4&file=/home/ubuntu/doris_master/output/be/storage/snapshot/20230410102306.9.180//2774718/217609978/2774718.hdr
    std::string remote_url_prefix = fmt::format("{}&file={}", _base_url, _remote_path);

    LOG(INFO) << "list remote files: " << remote_url_prefix << ", job: " << _snapshot_loader._job_id
              << ", task id: " << _snapshot_loader._task_id << ", remote be: " << _remote_be_addr;

    std::string remote_file_list_str;
    auto list_files_cb = [&remote_url_prefix, &remote_file_list_str](HttpClient* client) {
        RETURN_IF_ERROR(client->init(remote_url_prefix));
        client->set_timeout_ms(config::download_binlog_meta_timeout_ms);
        return client->execute(&remote_file_list_str);
    };
    RETURN_IF_ERROR(HttpClient::execute_with_retry(kDownloadFileMaxRetry, 1, list_files_cb));

    _remote_file_list = absl::StrSplit(remote_file_list_str, "\n", absl::SkipWhitespace());

    // find hdr file
    auto hdr_file =
            std::find_if(_remote_file_list.begin(), _remote_file_list.end(),
                         [](const std::string& filename) { return _end_with(filename, ".hdr"); });
    if (hdr_file == _remote_file_list.end()) {
        std::string msg =
                fmt::format("can't find hdr file in remote snapshot path: {}", _remote_path);
        LOG(WARNING) << msg;
        return Status::RuntimeError(std::move(msg));
    }
    _remote_hdr_filename = *hdr_file;
    _remote_file_list.erase(hdr_file);

    return Status::OK();
}

Status SnapshotHttpDownloader::_download_hdr_file() {
    RemoteFileStat remote_hdr_stat;
    std::string remote_hdr_file_url =
            fmt::format("{}&file={}/{}", _base_url, _remote_path, _remote_hdr_filename);
    auto status = _get_http_file_stat(remote_hdr_file_url, &remote_hdr_stat);
    if (!status.ok()) {
        LOG(WARNING) << "failed to get remote hdr file stat: " << remote_hdr_file_url
                     << ", error: " << status.to_string();
        return status;
    }

    std::string hdr_filename = _remote_hdr_filename + ".tmp";
    std::string hdr_file = _local_path + "/" + hdr_filename;
    status = _download_http_file(_tablet->data_dir(), remote_hdr_file_url, hdr_file,
                                 remote_hdr_stat);
    if (!status.ok()) {
        LOG(WARNING) << "failed to download remote hdr file: " << remote_hdr_file_url
                     << ", error: " << status.to_string();
        return status;
    }
    _tmp_hdr_file = hdr_file;
    _remote_hdr_filestat = remote_hdr_stat;
    return Status::OK();
}

Status SnapshotHttpDownloader::_link_same_rowset_files() {
    std::string local_hdr_file_path = _local_path + "/" + _local_hdr_filename;

    // load local tablet meta
    TabletMetaPB local_tablet_meta;
    auto status = TabletMeta::load_from_file(local_hdr_file_path, &local_tablet_meta);
    if (!status.ok()) {
        // This file might broken because of the partial downloading.
        LOG(WARNING) << "failed to load local tablet meta: " << local_hdr_file_path
                     << ", skip link same rowset files, error: " << status.to_string();
        return Status::OK();
    }

    // load remote tablet meta
    TabletMetaPB remote_tablet_meta;
    status = TabletMeta::load_from_file(_tmp_hdr_file, &remote_tablet_meta);
    if (!status.ok()) {
        LOG(WARNING) << "failed to load remote tablet meta: " << _tmp_hdr_file
                     << ", error: " << status.to_string();
        return status;
    }

    count_unmatched_rowsets(local_tablet_meta, remote_tablet_meta, &_stats);

    LOG(INFO) << "link rowset files by compare " << _local_hdr_filename << " and "
              << _remote_hdr_filename;

    std::unordered_map<std::string, const RowsetMetaPB&> remote_rowset_metas;
    for (const auto& rowset_meta : remote_tablet_meta.rs_metas()) {
        if (rowset_meta.has_resource_id()) { // skip remote rowset
            continue;
        }
        remote_rowset_metas.insert({rowset_meta.rowset_id_v2(), rowset_meta});
    }

    std::unordered_map<std::string, const RowsetMetaPB&> local_rowset_metas;
    for (const auto& rowset_meta : local_tablet_meta.rs_metas()) {
        if (rowset_meta.has_resource_id()) {
            continue;
        }
        local_rowset_metas.insert({rowset_meta.rowset_id_v2(), rowset_meta});
    }

    for (const auto& local_rowset_meta : local_tablet_meta.rs_metas()) {
        if (local_rowset_meta.has_resource_id() || !local_rowset_meta.has_source_rowset_id()) {
            continue;
        }

        auto remote_rowset_meta = remote_rowset_metas.find(local_rowset_meta.source_rowset_id());
        if (remote_rowset_meta == remote_rowset_metas.end()) {
            continue;
        }

        const auto& remote_rowset_id = remote_rowset_meta->first;
        const auto& remote_rowset_meta_pb = remote_rowset_meta->second;
        const auto& local_rowset_id = local_rowset_meta.rowset_id_v2();
        auto remote_tablet_id = remote_rowset_meta_pb.tablet_id();
        if (local_rowset_meta.start_version() != remote_rowset_meta_pb.start_version() ||
            local_rowset_meta.end_version() != remote_rowset_meta_pb.end_version()) {
            continue;
        }

        LOG(INFO) << "rowset " << local_rowset_id << " was downloaded from remote tablet "
                  << remote_tablet_id << " rowset " << remote_rowset_id
                  << ", directly link files instead of downloading";

        // Since the rowset meta are the same, we can link the local rowset files as
        // the downloaded remote rowset files.
        for (const auto& [local_file, local_filestat] : _local_files) {
            if (!local_file.starts_with(local_rowset_id)) {
                continue;
            }

            std::string remote_file = local_file;
            remote_file.replace(0, local_rowset_id.size(), remote_rowset_id);
            std::string local_file_path = _local_path + "/" + local_file;
            std::string remote_file_path = _local_path + "/" + remote_file;

            bool exist = true;
            RETURN_IF_ERROR(io::global_local_filesystem()->exists(remote_file_path, &exist));
            if (exist) {
                continue;
            }

            LOG(INFO) << "link file from " << local_file_path << " to " << remote_file_path;
            if (!io::global_local_filesystem()->link_file(local_file_path, remote_file_path)) {
                std::string msg = fmt::format("link file failed from {} to {}, err: {}",
                                              local_file_path, remote_file_path, strerror(errno));
                LOG(WARNING) << msg;
                return Status::InternalError(std::move(msg));
            }

            _local_files[remote_file] = local_filestat;
            _linked_files[remote_file] = static_cast<int64_t>(local_filestat.size);
        }
    }

    for (const auto& remote_rowset_meta : remote_tablet_meta.rs_metas()) {
        if (remote_rowset_meta.has_resource_id() || !remote_rowset_meta.has_source_rowset_id()) {
            continue;
        }

        auto local_rowset_meta = local_rowset_metas.find(remote_rowset_meta.source_rowset_id());
        if (local_rowset_meta == local_rowset_metas.end()) {
            continue;
        }

        const auto& local_rowset_id = local_rowset_meta->first;
        const auto& local_rowset_meta_pb = local_rowset_meta->second;
        const auto& remote_rowset_id = remote_rowset_meta.rowset_id_v2();
        auto local_tablet_id = local_rowset_meta_pb.tablet_id();

        if (remote_rowset_meta.start_version() != local_rowset_meta_pb.start_version() ||
            remote_rowset_meta.end_version() != local_rowset_meta_pb.end_version()) {
            continue;
        }

        LOG(INFO) << "remote rowset " << remote_rowset_id << " was derived from local tablet "
                  << local_tablet_id << " rowset " << local_rowset_id
                  << ", skip downloading these files";

        for (const auto& remote_file : _remote_file_list) {
            if (!remote_file.starts_with(remote_rowset_id)) {
                continue;
            }

            std::string local_file = remote_file;
            local_file.replace(0, remote_rowset_id.size(), local_rowset_id);
            std::string local_file_path = _local_path + "/" + local_file;
            std::string remote_file_path = _local_path + "/" + remote_file;

            bool exist = false;
            RETURN_IF_ERROR(io::global_local_filesystem()->exists(remote_file_path, &exist));
            if (exist) {
                continue;
            }

            LOG(INFO) << "link file from " << local_file_path << " to " << remote_file_path;
            if (!io::global_local_filesystem()->link_file(local_file_path, remote_file_path)) {
                std::string msg = fmt::format("link file failed from {} to {}, err: {}",
                                              local_file_path, remote_file_path, strerror(errno));
                LOG(WARNING) << msg;
                return Status::InternalError(std::move(msg));
            } else {
                auto it = _local_files.find(local_file);
                if (it != _local_files.end()) {
                    _local_files[remote_file] = it->second;
                    _linked_files[remote_file] = static_cast<int64_t>(it->second.size);
                } else {
                    std::string msg =
                            fmt::format("local file {} don't exist in _local_files, err: {}",
                                        local_file, strerror(errno));
                    LOG(WARNING) << msg;
                    return Status::InternalError(std::move(msg));
                }
            }
        }
    }
    return Status::OK();
}

Status SnapshotHttpDownloader::_get_remote_file_stats() {
    for (const auto& filename : _remote_file_list) {
        if (_report_progress_callback) {
            RETURN_IF_ERROR(_report_progress_callback());
        }

        std::string remote_file_url =
                fmt::format("{}&file={}/{}", _base_url, _remote_path, filename);

        RemoteFileStat remote_filestat;
        RETURN_IF_ERROR(_get_http_file_stat(remote_file_url, &remote_filestat));
        _remote_files[filename] = remote_filestat;
    }

    return Status::OK();
}

void SnapshotHttpDownloader::_get_need_download_files() {
    for (const auto& [remote_file, remote_filestat] : _remote_files) {
        LOG(INFO) << "remote file: " << remote_file << ", size: " << remote_filestat.size
                  << ", md5: " << remote_filestat.md5;
        auto it = _local_files.find(remote_file);
        if (it == _local_files.end()) {
            _need_download_files.emplace_back(remote_file);
            continue;
        }

        if (auto& local_filestat = it->second; local_filestat.size != remote_filestat.size) {
            _need_download_files.emplace_back(remote_file);
            continue;
        }

        if (auto& local_filestat = it->second; local_filestat.md5 != remote_filestat.md5) {
            _need_download_files.emplace_back(remote_file);
            continue;
        }

        LOG(INFO) << fmt::format("file {} already exists, skip download url {}", remote_file,
                                 remote_filestat.url);
        if (_linked_files.contains(remote_file)) {
            // counted as linked below
            continue;
        }
        ++_stats.skipped_files;
        _stats.skipped_bytes += static_cast<int64_t>(remote_filestat.size);
    }
    // A linked file which differs from the remote one is downloaded again, it is not reused.
    std::unordered_set<std::string> need_download(_need_download_files.begin(),
                                                  _need_download_files.end());
    for (const auto& [linked_file, linked_size] : _linked_files) {
        if (_remote_files.contains(linked_file) && !need_download.contains(linked_file)) {
            ++_stats.linked_files;
            _stats.linked_bytes += linked_size;
        }
    }
}

Status SnapshotHttpDownloader::_download_files() {
    DataDir* data_dir = _tablet->data_dir();

    uint64_t total_file_size = 0;
    MonotonicStopWatch watch(true);
    for (auto& filename : _need_download_files) {
        if (_report_progress_callback) {
            RETURN_IF_ERROR(_report_progress_callback());
        }

        auto& remote_filestat = _remote_files[filename];
        auto file_size = remote_filestat.size;
        auto& remote_file_url = remote_filestat.url;
        auto& remote_file_md5 = remote_filestat.md5;

        std::string local_filename;
        RETURN_IF_ERROR(
                _snapshot_loader._replace_tablet_id(filename, _local_tablet_id, &local_filename));
        std::string local_file_path = _local_path + "/" + local_filename;

        RETURN_IF_ERROR(
                _download_http_file(data_dir, remote_file_url, local_file_path, remote_filestat));
        total_file_size += file_size;
        ++_stats.downloaded_files;
        _stats.downloaded_bytes += static_cast<int64_t>(file_size);

        // local_files always keep the updated local files
        _local_files[filename] = LocalFileStat {file_size, remote_file_md5};
    }

    uint64_t total_time_ms = watch.elapsed_time() / 1000 / 1000;
    total_time_ms = total_time_ms > 0 ? total_time_ms : 0;
    double copy_rate = 0.0;
    if (total_time_ms > 0) {
        copy_rate =
                static_cast<double>(total_file_size) / static_cast<double>(total_time_ms) / 1000.0;
    }
    LOG(INFO) << fmt::format(
            "succeed to copy remote tablet {} to local tablet {}, total downloading {} files, "
            "total file size: {} B, cost: {} ms, rate: {} MB/s",
            _remote_tablet_id, _local_tablet_id, _need_download_files.size(), total_file_size,
            total_time_ms, copy_rate);

    return Status::OK();
}

Status SnapshotHttpDownloader::_install_remote_hdr_file() {
    std::string local_hdr_filename;
    RETURN_IF_ERROR(_snapshot_loader._replace_tablet_id(_remote_hdr_filename, _local_tablet_id,
                                                        &local_hdr_filename));
    std::string local_hdr_file_path = _local_path + "/" + local_hdr_filename;

    auto status = io::global_local_filesystem()->rename(_tmp_hdr_file, local_hdr_file_path);
    if (!status.ok()) {
        LOG(WARNING) << "failed to install remote hdr file from: " << _tmp_hdr_file << " to"
                     << local_hdr_file_path << ", error: " << status.to_string();
        return Status::RuntimeError("failed install remote hdr file {} from tmp {}, error: {}",
                                    local_hdr_file_path, _tmp_hdr_file, status.to_string());
    }

    // also save the hdr file into remote files.
    _remote_files[_remote_hdr_filename] = _remote_hdr_filestat;

    return Status::OK();
}

Status SnapshotHttpDownloader::_delete_orphan_files() {
    // local_files: contain all remote files and local files
    // finally, delete local files which are not in remote
    for (const auto& [local_file, local_filestat] : _local_files) {
        // replace the tablet id in local file name with the remote tablet id,
        // in order to compare the file name.
        std::string new_name;
        Status st = _snapshot_loader._replace_tablet_id(local_file, _remote_tablet_id, &new_name);
        if (!st.ok()) {
            LOG(WARNING) << "failed to replace tablet id. unknown local file: " << st
                         << ". ignore it";
            continue;
        }
        VLOG_CRITICAL << "new file name after replace tablet id: " << new_name;
        const auto& find = _remote_files.find(new_name);
        if (find != _remote_files.end()) {
            continue;
        }

        // delete
        std::string full_local_file = _local_path + "/" + local_file;
        LOG(INFO) << "begin to delete local snapshot file: " << full_local_file
                  << ", it does not exist in remote";
        if (remove(full_local_file.c_str()) != 0) {
            LOG(WARNING) << "failed to delete unknown local file: " << full_local_file
                         << ", error: " << strerror(errno) << ", file size: " << local_filestat.size
                         << ", ignore it";
        }
    }
    return Status::OK();
}

bool SnapshotHttpDownloader::_use_manifest() const {
    return config::restore_manifest_check && _remote_tablet_snapshot.__isset.manifest_root;
}

Status SnapshotHttpDownloader::_fetch_manifest() {
    // The manifest is next to the remote tablet snapshot dir, one level up:
    // <storage_root>/snapshot/<time>.<seq>.<timeout>/<tablet_id>/manifest
    std::string remote_parent;
    RETURN_IF_ERROR(parent_path_of(_remote_path, &remote_parent));
    std::string url = fmt::format("{}&file={}/{}", _base_url, remote_parent,
                                  SnapshotManifest::kLocalFileName);
    std::string content;
    auto fetch_cb = [&url, &content](HttpClient* client) {
        content.clear();
        RETURN_IF_ERROR(client->init(url));
        client->set_timeout_ms(config::download_binlog_meta_timeout_ms);
        return client->execute(&content);
    };
    RETURN_IF_ERROR(HttpClient::execute_with_retry(kDownloadFileMaxRetry, 1, fetch_cb));

    auto status = SnapshotManifest::parse(content, _remote_tablet_snapshot.manifest_root,
                                          _remote_tablet_id, &_manifest);
    if (!status.ok()) {
        _manifest_mismatch = status.is<ErrorCode::RESTORE_MANIFEST_MISMATCH>();
        LOG(WARNING) << "failed to parse the manifest of remote tablet " << _remote_tablet_id
                     << " from " << url << ", error: " << status;
        return status;
    }

    // Download the files in the manifest, instead of the files listed by the remote BE.
    _remote_file_list.clear();
    _remote_hdr_filename.clear();
    for (const auto& file : _manifest.files) {
        if (!_end_with(file.name, ".hdr")) {
            _remote_file_list.push_back(file.name);
            continue;
        }
        if (!_remote_hdr_filename.empty()) {
            _manifest_mismatch = true;
            return Status::Error<ErrorCode::RESTORE_MANIFEST_MISMATCH, false>(
                    "invalid manifest of remote tablet {}: more than one tablet meta file",
                    _remote_tablet_id);
        }
        _remote_hdr_filename = file.name;
    }
    if (_remote_hdr_filename.empty()) {
        _manifest_mismatch = true;
        return Status::Error<ErrorCode::RESTORE_MANIFEST_MISMATCH, false>(
                "invalid manifest of remote tablet {}: no tablet meta file", _remote_tablet_id);
    }
    return Status::OK();
}

Status SnapshotHttpDownloader::_check_manifest() {
    if (!_use_manifest()) {
        return Status::OK();
    }
    auto status = check_tablet_snapshot_manifest(_local_path, _local_tablet_id, _manifest,
                                                 config::restore_manifest_digest_check,
                                                 &_manifest_check_result);
    if (!status.ok()) {
        _manifest_mismatch = status.is<ErrorCode::RESTORE_MANIFEST_MISMATCH>();
        LOG(WARNING) << "failed to check the downloaded snapshot of tablet " << _local_tablet_id
                     << " against the manifest, remote tablet: " << _remote_tablet_id
                     << ", reuse local files: " << !_disable_reuse << ", error: " << status;
    }
    return status;
}

Status SnapshotHttpDownloader::_cut_to_increment() {
    if (!_use_manifest()) {
        return Status::InternalError("the incremental restore of tablet {} needs the manifest",
                                     _local_tablet_id);
    }
    TabletMetaPB remote_meta;
    RETURN_IF_ERROR(TabletMeta::load_from_file(_tmp_hdr_file, &remote_meta));
    const auto& range = _remote_tablet_snapshot.incremental;
    std::vector<std::string> rowset_ids;
    RETURN_IF_ERROR(select_incremental_rowsets(remote_meta, range.base_version, range.end_version,
                                               &_increment, &rowset_ids));
    SnapshotManifest cut;
    Status st = cut_manifest_to_increment(_manifest, _increment, rowset_ids, &cut);
    if (!st.ok()) {
        _manifest_mismatch = st.is<ErrorCode::RESTORE_MANIFEST_MISMATCH>();
        return st;
    }
    _manifest = std::move(cut);
    std::vector<std::string> files;
    for (const auto& file : _manifest.files) {
        if (!file.name.ends_with(".hdr")) {
            files.push_back(file.name);
        }
    }
    _remote_file_list = std::move(files);
    return Status::OK();
}

Status SnapshotHttpDownloader::_install_increment_hdr() {
    std::string local_hdr = fmt::format("{}/{}.hdr", _local_path, _local_tablet_id);
    RETURN_IF_ERROR(TabletMeta::save(local_hdr, _increment));
    _stats.tablets_incremental = 1;
    _stats.incremental_files = _stats.downloaded_files;
    _stats.incremental_bytes = _stats.downloaded_bytes;
    return Status::OK();
}

Status SnapshotHttpDownloader::download() {
    // Take a lock to protect the local snapshot path.
    auto local_snapshot_guard = LocalSnapshotLock::instance().acquire(_local_path);

    // Step 1: Validate local tablet snapshot paths
    bool res = true;
    RETURN_IF_ERROR(io::global_local_filesystem()->is_directory(_local_path, &res));
    if (!res) {
        std::string msg =
                fmt::format("snapshot path is not directory or does not exist: {}", _local_path);
        LOG(WARNING) << msg;
        return Status::RuntimeError(std::move(msg));
    }

    // Step 2: get all local files
    if (_disable_reuse) {
        // Download all the files again, without reusing any local file.
        RETURN_IF_ERROR(_snapshot_loader._clear_local_snapshot_files(_local_path));
    }
    RETURN_IF_ERROR(_load_existing_files());

    // Step 3: Validate remote tablet snapshot paths && remote files map. With a manifest, the files
    // to download are those in the manifest.
    if (_use_manifest()) {
        RETURN_IF_ERROR(_fetch_manifest());
    } else {
        RETURN_IF_ERROR(_list_remote_files());
    }

    // Step 4: download hdr file to a tmp file
    RETURN_IF_ERROR(_download_hdr_file());

    // The increment: only the files of the rowsets after the base version, nothing is linked.
    if (_incremental) {
        RETURN_IF_ERROR(_cut_to_increment());
    }

    // Step 5: link same rowset files, if local tablet meta file exists
    if (!_incremental && !_disable_reuse && !_local_hdr_filename.empty()) {
        RETURN_IF_ERROR(_link_same_rowset_files());
    }

    // Step 6: get all remote file stats
    RETURN_IF_ERROR(_get_remote_file_stats());

    // Step 7: get all need download files & download them
    _get_need_download_files();
    if (!_need_download_files.empty()) {
        RETURN_IF_ERROR(_download_files());
    }

    // Step 8: install remote hdr file from tmp file
    RETURN_IF_ERROR(_install_remote_hdr_file());

    // Step 9: delete orphan files
    RETURN_IF_ERROR(_delete_orphan_files());

    // Step 10: check the local snapshot, including the linked files, against the manifest
    RETURN_IF_ERROR(_check_manifest());

    if (_incremental) {
        RETURN_IF_ERROR(_install_increment_hdr());
    }

    return Status::OK();
}

BaseSnapshotLoader::BaseSnapshotLoader(ExecEnv* env, int64_t job_id, int64_t task_id,
                                       const TNetworkAddress& broker_addr,
                                       const std::map<std::string, std::string>& prop)
        : _env(env), _job_id(job_id), _task_id(task_id), _broker_addr(broker_addr), _prop(prop) {
    _resource_ctx = ResourceContext::create_shared();
    TUniqueId tid;
    tid.hi = _job_id;
    tid.lo = _task_id;
    _resource_ctx->task_controller()->set_task_id(tid);
    std::shared_ptr<MemTrackerLimiter> mem_tracker = MemTrackerLimiter::create_shared(
            MemTrackerLimiter::Type::OTHER,
            fmt::format("SnapshotLoader#Id={}", ((UniqueId)tid).to_string()));
    _resource_ctx->memory_context()->set_mem_tracker(mem_tracker);
}

Status BaseSnapshotLoader::init(TStorageBackendType::type type, const std::string& location) {
    if (TStorageBackendType::type::S3 == type || TStorageBackendType::type::AZURE == type) {
        S3Conf s3_conf;
        S3URI s3_uri(location);
        RETURN_IF_ERROR(s3_uri.parse());
        RETURN_IF_ERROR(S3ClientFactory::convert_properties_to_s3_conf(_prop, s3_uri, &s3_conf));
        _remote_fs =
                DORIS_TRY(io::S3FileSystem::create(std::move(s3_conf), io::FileSystem::TMP_FS_ID));
    } else if (TStorageBackendType::type::HDFS == type) {
        THdfsParams hdfs_params = parse_properties(_prop);
        _remote_fs = DORIS_TRY(io::HdfsFileSystem::create(hdfs_params, hdfs_params.fs_name,
                                                          io::FileSystem::TMP_FS_ID));
    } else if (TStorageBackendType::type::BROKER == type) {
        std::shared_ptr<io::BrokerFileSystem> fs;
        _remote_fs = DORIS_TRY(
                io::BrokerFileSystem::create(_broker_addr, _prop, io::FileSystem::TMP_FS_ID));
    } else {
        return Status::InternalError("Unknown storage type: {}", type);
    }
    return Status::OK();
}

SnapshotLoader::SnapshotLoader(StorageEngine& engine, ExecEnv* env, int64_t job_id, int64_t task_id,
                               const TNetworkAddress& broker_addr,
                               const std::map<std::string, std::string>& prop)
        : BaseSnapshotLoader(env, job_id, task_id, broker_addr, prop), _engine(engine) {}

Status SnapshotLoader::upload(const std::map<std::string, std::string>& src_to_dest_path,
                              std::map<int64_t, std::vector<std::string>>* tablet_files) {
    if (!_remote_fs) {
        return Status::InternalError("Storage backend not initialized.");
    }
    LOG(INFO) << "begin to upload snapshot files. num: " << src_to_dest_path.size()
              << ", broker addr: " << _broker_addr << ", job: " << _job_id << ", task" << _task_id;

    // check if job has already been cancelled
    int tmp_counter = 1;
    RETURN_IF_ERROR(_report_every(0, &tmp_counter, 0, 0, TTaskType::type::UPLOAD));

    Status status = Status::OK();
    // 1. validate local tablet snapshot paths
    RETURN_IF_ERROR(_check_local_snapshot_paths(src_to_dest_path, true));

    // 2. for each src path, upload it to remote storage
    // we report to frontend for every 10 files, and we will cancel the job if
    // the job has already been cancelled in frontend.
    int report_counter = 0;
    int total_num = doris::cast_set<int>(src_to_dest_path.size());
    int finished_num = 0;
    for (const auto& iter : src_to_dest_path) {
        const std::string& src_path = iter.first;
        const std::string& dest_path = iter.second;

        // Take a lock to protect the local snapshot path.
        auto local_snapshot_guard = LocalSnapshotLock::instance().acquire(src_path);

        int64_t tablet_id = 0;
        int32_t schema_hash = 0;
        RETURN_IF_ERROR(
                _get_tablet_id_and_schema_hash_from_file_path(src_path, &tablet_id, &schema_hash));

        // 2.1 get existing files from remote path
        std::map<std::string, FileStat> remote_files;
        RETURN_IF_ERROR(_list_with_checksum(dest_path, &remote_files));

        for (auto& tmp : remote_files) {
            VLOG_CRITICAL << "get remote file: " << tmp.first << ", checksum: " << tmp.second.md5;
        }

        // 2.2 list local files
        std::vector<std::string> local_files;
        std::vector<std::string> local_files_with_checksum;
        std::vector<SnapshotManifestFile> manifest_files;
        RETURN_IF_ERROR(_get_existing_files_from_local(src_path, &local_files));

        // 2.3 iterate local files
        for (auto& local_file : local_files) {
            RETURN_IF_ERROR(_report_every(10, &report_counter, finished_num, total_num,
                                          TTaskType::type::UPLOAD));

            // calc md5sum of localfile, and the size and sha256 for the manifest in the same read
            std::string md5sum;
            std::string sha256;
            int64_t file_size = 0;
            RETURN_IF_ERROR(compute_file_digests(src_path + "/" + local_file, &md5sum, &sha256,
                                                 &file_size));
            VLOG_CRITICAL << "get file checksum: " << local_file << ": " << md5sum;
            local_files_with_checksum.push_back(local_file + "." + md5sum);
            // the tablet meta file is rewritten in restore, only its name and size are recorded,
            // and the md5 to locate it.
            manifest_files.push_back(SnapshotManifestFile {
                    .name = local_file,
                    .size = file_size,
                    .md5 = md5sum,
                    .sha256 = _end_with(local_file, ".hdr") ? std::string() : sha256});

            // check if this local file need upload
            bool need_upload = false;
            auto find = remote_files.find(local_file);
            if (find != remote_files.end()) {
                if (md5sum != find->second.md5) {
                    // remote storage file exist, but with different checksum
                    LOG(WARNING) << "remote file checksum is invalid. remote: " << find->first
                                 << ", local: " << md5sum;
                    // TODO(cmy): save these files and delete them later
                    need_upload = true;
                }
            } else {
                need_upload = true;
            }

            if (!need_upload) {
                VLOG_CRITICAL << "file exist in remote path, no need to upload: " << local_file;
                continue;
            }

            // upload
            std::string remote_path = dest_path + '/' + local_file;
            std::string local_path = src_path + '/' + local_file;
            RETURN_IF_ERROR(upload_with_checksum(*_remote_fs, local_path, remote_path, md5sum));
        } // end for each tablet's local files

        // 2.4 upload the manifest after all the data files of the tablet
        std::string manifest_root;
        RETURN_IF_ERROR(_upload_manifest(src_path, dest_path, tablet_id, std::move(manifest_files),
                                         &manifest_root));

        // 2.5 and the decomposed digest, if the snapshot has one
        std::string prefix_root;
        RETURN_IF_ERROR(_upload_prefix_digest(src_path, dest_path, tablet_id, &prefix_root));

        tablet_files->emplace(tablet_id, local_files_with_checksum);
        _uploaded_manifest_roots.emplace(tablet_id, std::move(manifest_root));
        if (!prefix_root.empty()) {
            _uploaded_prefix_digest_roots.emplace(tablet_id, std::move(prefix_root));
        }
        finished_num++;
        LOG(INFO) << "finished to write tablet to remote. local path: " << src_path
                  << ", remote path: " << dest_path;
    } // end for each tablet path

    LOG(INFO) << "finished to upload snapshots. job: " << _job_id << ", task id: " << _task_id;
    return status;
}

/*
 * Download snapshot files from remote.
 * After downloaded, the local dir should contains all files existing in remote,
 * may also contains several useless files.
 */
Status SnapshotLoader::download(const std::map<std::string, std::string>& src_to_dest_path,
                                std::vector<int64_t>* downloaded_tablet_ids) {
    if (!_remote_fs) {
        return Status::InternalError("Storage backend not initialized.");
    }
    LOG(INFO) << "begin to download snapshot files. num: " << src_to_dest_path.size()
              << ", broker addr: " << _broker_addr << ", job: " << _job_id
              << ", task id: " << _task_id;

    // check if job has already been cancelled
    int tmp_counter = 1;
    RETURN_IF_ERROR(_report_every(0, &tmp_counter, 0, 0, TTaskType::type::DOWNLOAD));

    Status status = Status::OK();
    // 1. validate local tablet snapshot paths
    RETURN_IF_ERROR(_check_local_snapshot_paths(src_to_dest_path, false));

    // 2. for each src path, download it to local storage
    int report_counter = 0;
    int total_num = doris::cast_set<int>(src_to_dest_path.size());
    int finished_num = 0;
    for (const auto& iter : src_to_dest_path) {
        const std::string& remote_path = iter.first;
        const std::string& local_path = iter.second;

        // Take a lock to protect the local snapshot path.
        auto local_snapshot_guard = LocalSnapshotLock::instance().acquire(local_path);

        int64_t local_tablet_id = 0;
        int32_t schema_hash = 0;
        RETURN_IF_ERROR(_get_tablet_id_and_schema_hash_from_file_path(local_path, &local_tablet_id,
                                                                      &schema_hash));
        if (downloaded_tablet_ids != nullptr) {
            downloaded_tablet_ids->push_back(local_tablet_id);
        }

        int64_t remote_tablet_id;
        RETURN_IF_ERROR(_get_tablet_id_from_remote_path(remote_path, &remote_tablet_id));
        VLOG_CRITICAL << "get local tablet id: " << local_tablet_id
                      << ", schema hash: " << schema_hash
                      << ", remote tablet id: " << remote_tablet_id;

        SnapshotDownloadStats tablet_stats;
        auto manifest_root = _manifest_roots.find(remote_path);
        auto increment = _incremental_ranges.find(remote_path);
        if (increment != _incremental_ranges.end()) {
            if (!config::restore_manifest_check || manifest_root == _manifest_roots.end()) {
                return Status::InternalError(
                        "the incremental restore of tablet {} needs the manifest of the backup",
                        local_tablet_id);
            }
            RETURN_IF_ERROR(_download_tablet_incremental(
                    remote_path, local_path, local_tablet_id, remote_tablet_id,
                    manifest_root->second, increment->second, &report_counter, finished_num,
                    total_num, &tablet_stats));
        } else if (!config::restore_manifest_check || manifest_root == _manifest_roots.end()) {
            // no manifest (old backup, old FE) or the check is disabled: download by listing the
            // remote path, as before.
            RETURN_IF_ERROR(_download_tablet_from_remote(remote_path, local_path, local_tablet_id,
                                                         remote_tablet_id, &report_counter,
                                                         finished_num, total_num, &tablet_stats));
        } else {
            ManifestCheckResult result;
            // Fetch the manifest, download the files in it, then check the local snapshot.
            auto download_by_manifest = [&]() -> Status {
                // the stats of the last attempt only
                tablet_stats = SnapshotDownloadStats();
                SnapshotManifest manifest;
                RETURN_IF_ERROR(_fetch_remote_manifest(remote_path, local_path, remote_tablet_id,
                                                       manifest_root->second, &manifest));
                RETURN_IF_ERROR(_download_tablet_by_manifest(
                        remote_path, local_path, local_tablet_id, manifest, &report_counter,
                        finished_num, total_num, &tablet_stats));
                return check_tablet_snapshot_manifest(local_path, local_tablet_id, manifest,
                                                      config::restore_manifest_digest_check,
                                                      &result);
            };
            Status st = download_by_manifest();
            if (st.is<ErrorCode::RESTORE_MANIFEST_MISMATCH>()) {
                // Download all the files of the tablet again, without reusing any local file.
                LOG(WARNING) << "downloaded snapshot of tablet " << local_tablet_id
                             << " does not match the manifest, download it again. job: " << _job_id
                             << ", task id: " << _task_id << ", error: " << st;
                RETURN_IF_ERROR(_clear_local_snapshot_files(local_path));
                st = download_by_manifest();
            }
            if (!st.ok()) {
                LOG(WARNING) << "failed to check the downloaded snapshot of tablet "
                             << local_tablet_id << " against the manifest. job: " << _job_id
                             << ", task id: " << _task_id << ", error: " << st;
                return st;
            }
            if (result.checked) {
                _add_manifest_verified_tablet(local_tablet_id, result.digest_checked);
            }
        }

        _add_tablet_download_stats(local_tablet_id, remote_tablet_id, std::move(tablet_stats));
        finished_num++;
    } // end for src_to_dest_path

    LOG(INFO) << "finished to download snapshots. job: " << _job_id << ", task id: " << _task_id;
    return status;
}

Status SnapshotLoader::_download_tablet_from_remote(const std::string& remote_path,
                                                    const std::string& local_path,
                                                    int64_t local_tablet_id,
                                                    int64_t remote_tablet_id, int* report_counter,
                                                    int finished_num, int total_num,
                                                    SnapshotDownloadStats* stats) {
    // 2.1. get local files
    std::vector<std::string> local_files;
    RETURN_IF_ERROR(_get_existing_files_from_local(local_path, &local_files));

    // 2.2. get remote files
    std::map<std::string, FileStat> remote_files;
    RETURN_IF_ERROR(_list_with_checksum(remote_path, &remote_files));
    if (remote_files.empty()) {
        std::stringstream ss;
        ss << "get nothing from remote path: " << remote_path;
        LOG(WARNING) << ss.str();
        return Status::InternalError(ss.str());
    }

    TabletSharedPtr tablet = _engine.tablet_manager()->get_tablet(local_tablet_id);
    if (tablet == nullptr) {
        std::stringstream ss;
        ss << "failed to get local tablet: " << local_tablet_id;
        LOG(WARNING) << ss.str();
        return Status::InternalError(ss.str());
    }
    DataDir* data_dir = tablet->data_dir();

    for (auto& remote_iter : remote_files) {
        RETURN_IF_ERROR(_report_every(10, report_counter, finished_num, total_num,
                                      TTaskType::type::DOWNLOAD));

        bool need_download = false;
        const std::string& remote_file = remote_iter.first;
        const FileStat& file_stat = remote_iter.second;
        auto find = std::find(local_files.begin(), local_files.end(), remote_file);
        if (find == local_files.end()) {
            // remote file does not exist in local, download it
            need_download = true;
        } else {
            if (_end_with(remote_file, ".hdr")) {
                // this is a header file, download it.
                need_download = true;
            } else {
                // check checksum
                std::string local_md5sum;
                Status st = io::global_local_filesystem()->md5sum(local_path + "/" + remote_file,
                                                                  &local_md5sum);
                if (!st.ok()) {
                    LOG(WARNING) << "failed to get md5sum of local file: " << remote_file
                                 << ". msg: " << st << ". download it";
                    need_download = true;
                } else {
                    VLOG_CRITICAL << "get local file checksum: " << remote_file << ": "
                                  << local_md5sum;
                    if (file_stat.md5 != local_md5sum) {
                        // file's checksum does not equal, download it.
                        need_download = true;
                    }
                }
            }
        }

        if (!need_download) {
            LOG(INFO) << "remote file already exist in local, no need to download."
                      << ", file: " << remote_file;
            if (!_end_with(remote_file, ".hdr")) {
                ++stats->skipped_files;
                stats->skipped_bytes += static_cast<int64_t>(file_stat.size);
            }
            continue;
        }

        // begin to download
        std::string full_remote_file = remote_path + "/" + remote_file + "." + file_stat.md5;
        std::string local_file_name;
        // we need to replace the tablet_id in remote file name with local tablet id
        RETURN_IF_ERROR(_replace_tablet_id(remote_file, local_tablet_id, &local_file_name));
        std::string full_local_file = local_path + "/" + local_file_name;
        LOG(INFO) << "begin to download from " << full_remote_file << " to " << full_local_file;
        size_t file_len = file_stat.size;

        // check disk capacity
        if (data_dir->reach_capacity_limit(file_len)) {
            return Status::Error<ErrorCode::EXCEEDED_LIMIT>(
                    "reach the capacity limit of path {}, file_size={}", data_dir->path(),
                    file_len);
        }
        // remove file which will be downloaded now.
        // this file will be added to local_files if it be downloaded successfully.
        if (find != local_files.end()) {
            local_files.erase(find);
        }
        RETURN_IF_ERROR(_remote_fs->download(full_remote_file, full_local_file));

        // 3. check md5 of the downloaded file
        std::string downloaded_md5sum;
        RETURN_IF_ERROR(io::global_local_filesystem()->md5sum(full_local_file, &downloaded_md5sum));
        VLOG_CRITICAL << "get downloaded file checksum: " << full_local_file << ": "
                      << downloaded_md5sum;
        if (downloaded_md5sum != file_stat.md5) {
            std::stringstream ss;
            ss << "invalid md5 of downloaded file: " << full_local_file
               << ", expected: " << file_stat.md5 << ", get: " << downloaded_md5sum;
            LOG(WARNING) << ss.str();
            return Status::InternalError(ss.str());
        }

        if (!_end_with(remote_file, ".hdr")) {
            ++stats->downloaded_files;
            stats->downloaded_bytes += static_cast<int64_t>(file_len);
        }

        // local_files always keep the updated local files
        local_files.push_back(local_file_name);
        LOG(INFO) << "finished to download file via broker. file: " << full_local_file
                  << ", length: " << file_len;
    } // end for all remote files

    // finally, delete local files which are not in remote
    for (const auto& local_file : local_files) {
        // replace the tablet id in local file name with the remote tablet id,
        // in order to compare the file name.
        std::string new_name;
        Status st = _replace_tablet_id(local_file, remote_tablet_id, &new_name);
        if (!st.ok()) {
            LOG(WARNING) << "failed to replace tablet id. unknown local file: " << st
                         << ". ignore it";
            continue;
        }
        VLOG_CRITICAL << "new file name after replace tablet id: " << new_name;
        const auto& find = remote_files.find(new_name);
        if (find != remote_files.end()) {
            continue;
        }

        // delete
        std::string full_local_file = local_path + "/" + local_file;
        VLOG_CRITICAL << "begin to delete local snapshot file: " << full_local_file
                      << ", it does not exist in remote";
        if (remove(full_local_file.c_str()) != 0) {
            LOG(WARNING) << "failed to delete unknown local file: " << full_local_file
                         << ", ignore it";
        }
    }

    return Status::OK();
}

Status SnapshotLoader::_download_tablet_incremental(
        const std::string& remote_path, const std::string& local_path, int64_t local_tablet_id,
        int64_t remote_tablet_id, const std::string& manifest_root,
        const TRestoreIncrementalRange& range, int* report_counter, int finished_num,
        int total_num, SnapshotDownloadStats* stats) {
    auto attempt = [&]() -> Status {
        *stats = SnapshotDownloadStats();
        SnapshotManifest manifest;
        RETURN_IF_ERROR(_fetch_remote_manifest(remote_path, local_path, remote_tablet_id,
                                               manifest_root, &manifest));
        // 1. the tablet meta file only, to know the rowsets of the increment
        SnapshotManifest hdr_manifest;
        hdr_manifest.tablet_id = manifest.tablet_id;
        for (const auto& file : manifest.files) {
            if (file.name.ends_with(".hdr")) {
                hdr_manifest.files.push_back(file);
            }
        }
        if (hdr_manifest.files.size() != 1) {
            return Status::Error<ErrorCode::RESTORE_MANIFEST_MISMATCH, false>(
                    "invalid manifest of remote tablet {}: {} tablet meta files", remote_tablet_id,
                    hdr_manifest.files.size());
        }
        SnapshotDownloadStats hdr_stats;
        RETURN_IF_ERROR(_download_tablet_by_manifest(remote_path, local_path, local_tablet_id,
                                                     hdr_manifest, report_counter, finished_num,
                                                     total_num, &hdr_stats));
        std::string local_hdr = fmt::format("{}/{}.hdr", local_path, local_tablet_id);
        TabletMetaPB remote_meta;
        RETURN_IF_ERROR(TabletMeta::load_from_file(local_hdr, &remote_meta));
        TabletMetaPB increment;
        std::vector<std::string> rowset_ids;
        RETURN_IF_ERROR(select_incremental_rowsets(remote_meta, range.base_version,
                                                   range.end_version, &increment, &rowset_ids));
        // 2. the files of these rowsets
        SnapshotManifest cut;
        RETURN_IF_ERROR(cut_manifest_to_increment(manifest, increment, rowset_ids, &cut));
        RETURN_IF_ERROR(_download_tablet_by_manifest(remote_path, local_path, local_tablet_id, cut,
                                                     report_counter, finished_num, total_num,
                                                     stats));
        ManifestCheckResult result;
        RETURN_IF_ERROR(check_tablet_snapshot_manifest(
                local_path, local_tablet_id, cut, config::restore_manifest_digest_check, &result));
        if (result.checked) {
            _add_manifest_verified_tablet(local_tablet_id, result.digest_checked);
        }
        // 3. the tablet meta of the snapshot is the increment only
        RETURN_IF_ERROR(TabletMeta::save(local_hdr, increment));
        stats->tablets_incremental = 1;
        stats->incremental_files = stats->downloaded_files;
        stats->incremental_bytes = stats->downloaded_bytes;
        return Status::OK();
    };
    Status st = attempt();
    if (st.is<ErrorCode::RESTORE_MANIFEST_MISMATCH>()) {
        LOG(WARNING) << "downloaded increment of tablet " << local_tablet_id
                     << " does not match the manifest, download it again. job: " << _job_id
                     << ", task id: " << _task_id << ", error: " << st;
        RETURN_IF_ERROR(_clear_local_snapshot_files(local_path));
        st = attempt();
    }
    return st;
}

Status SnapshotLoader::_upload_manifest(const std::string& src_path, const std::string& dest_path,
                                        int64_t tablet_id, std::vector<SnapshotManifestFile> files,
                                        std::string* root) {
    // local: <snapshot>/<tablet_id>/<schema_hash> -> <snapshot>/<tablet_id>/manifest.upload
    // remote: .../__idx_<id>/__<tablet_id> -> .../__idx_<id>/__manifest__<tablet_id>.<root prefix>
    std::string local_parent;
    std::string remote_parent;
    RETURN_IF_ERROR(parent_path_of(src_path, &local_parent));
    RETURN_IF_ERROR(parent_path_of(dest_path, &remote_parent));
    SnapshotManifest manifest;
    manifest.tablet_id = tablet_id;
    manifest.files = std::move(files);
    std::string local_file = local_parent + "/manifest.upload";
    RETURN_IF_ERROR(write_snapshot_manifest(manifest, local_file, root));
    std::string remote_name = SnapshotManifest::remote_file_name(tablet_id, *root);
    size_t dot = remote_name.find_last_of('.');
    Status st = upload_with_checksum(*_remote_fs, local_file,
                                     remote_parent + "/" + remote_name.substr(0, dot),
                                     remote_name.substr(dot + 1));
    static_cast<void>(io::global_local_filesystem()->delete_file(local_file));
    RETURN_IF_ERROR(st);
    LOG(INFO) << "uploaded the manifest of tablet " << tablet_id << " to " << remote_parent << "/"
              << remote_name << ", root: " << *root;
    return Status::OK();
}

Status SnapshotLoader::_upload_prefix_digest(const std::string& src_path,
                                             const std::string& dest_path, int64_t tablet_id,
                                             std::string* root) {
    // local: <snapshot>/<tablet_id>/<schema_hash> -> <snapshot>/<tablet_id>/rdigest
    // remote: .../__idx_<id>/__<tablet_id> -> .../__idx_<id>/__rdigest__<tablet_id>.<root prefix>
    std::string local_parent;
    std::string remote_parent;
    RETURN_IF_ERROR(parent_path_of(src_path, &local_parent));
    RETURN_IF_ERROR(parent_path_of(dest_path, &remote_parent));
    std::string local_file =
            fmt::format("{}/{}", local_parent, RestoreDigestDecomposed::kLocalFileName);
    bool exists = false;
    RETURN_IF_ERROR(io::global_local_filesystem()->exists(local_file, &exists));
    if (!exists) {
        root->clear();
        return Status::OK();
    }
    std::string content;
    RETURN_IF_ERROR(read_local_file(local_file, &content));
    *root = sha256_hex(content);
    std::string remote_name = prefix_digest_remote_file_name(tablet_id, *root);
    size_t dot = remote_name.find_last_of('.');
    RETURN_IF_ERROR(upload_with_checksum(*_remote_fs, local_file,
                                         remote_parent + "/" + remote_name.substr(0, dot),
                                         remote_name.substr(dot + 1)));
    LOG(INFO) << "uploaded the decomposed digest of tablet " << tablet_id << " to " << remote_parent
              << "/" << remote_name << ", root: " << *root;
    return Status::OK();
}

Status SnapshotLoader::fetch_prefix_digest(const std::string& remote_path,
                                           const std::string& local_path,
                                           int64_t remote_tablet_id, const std::string& root,
                                           RestoreDigestDecomposed* digest) {
    if (!_remote_fs) {
        return Status::InternalError("Storage backend not initialized.");
    }
    std::string local_parent;
    RETURN_IF_ERROR(parent_path_of(local_path, &local_parent));
    return fetch_prefix_digest_to_dir(remote_path, local_parent, remote_tablet_id, root, digest);
}

Status SnapshotLoader::fetch_prefix_digest_to_dir(const std::string& remote_path,
                                                  const std::string& tmp_dir,
                                                  int64_t remote_tablet_id,
                                                  const std::string& root,
                                                  RestoreDigestDecomposed* digest) {
    if (!_remote_fs) {
        return Status::InternalError("Storage backend not initialized.");
    }
    std::string remote_parent;
    const std::string& local_parent = tmp_dir;
    RETURN_IF_ERROR(parent_path_of(remote_path, &remote_parent));
    std::string remote_file =
            remote_parent + "/" + prefix_digest_remote_file_name(remote_tablet_id, root);
    // download to a tmp file next to the local snapshot dir, never into it.
    std::string local_file = fmt::format("{}/rdigest.{}.download", local_parent, _task_id);
    RETURN_IF_ERROR(_remote_fs->download(remote_file, local_file));
    std::string content;
    Status st = read_local_file(local_file, &content);
    static_cast<void>(io::global_local_filesystem()->delete_file(local_file));
    RETURN_IF_ERROR(st);
    return RestoreDigestDecomposed::parse(content, root, remote_tablet_id, digest);
}

Status SnapshotLoader::_fetch_remote_manifest(const std::string& remote_path,
                                              const std::string& local_path,
                                              int64_t remote_tablet_id, const std::string& root,
                                              SnapshotManifest* manifest) {
    std::string remote_parent;
    std::string local_parent;
    RETURN_IF_ERROR(parent_path_of(remote_path, &remote_parent));
    RETURN_IF_ERROR(parent_path_of(local_path, &local_parent));
    std::string remote_file =
            remote_parent + "/" + SnapshotManifest::remote_file_name(remote_tablet_id, root);
    // download to a tmp file next to the local snapshot dir, never into it.
    std::string local_file = fmt::format("{}/manifest.{}.download", local_parent, _task_id);
    RETURN_IF_ERROR(_remote_fs->download(remote_file, local_file));
    std::string content;
    Status st = read_local_file(local_file, &content);
    static_cast<void>(io::global_local_filesystem()->delete_file(local_file));
    RETURN_IF_ERROR(st);
    return SnapshotManifest::parse(content, root, remote_tablet_id, manifest);
}

Status SnapshotLoader::_download_tablet_by_manifest(const std::string& remote_path,
                                                    const std::string& local_path,
                                                    int64_t local_tablet_id,
                                                    const SnapshotManifest& manifest,
                                                    int* report_counter, int finished_num,
                                                    int total_num, SnapshotDownloadStats* stats) {
    std::vector<std::string> existing_files;
    RETURN_IF_ERROR(_get_existing_files_from_local(local_path, &existing_files));
    std::set<std::string> local_files(existing_files.begin(), existing_files.end());

    TabletSharedPtr tablet = _engine.tablet_manager()->get_tablet(local_tablet_id);
    if (tablet == nullptr) {
        return Status::InternalError("failed to get local tablet: {}", local_tablet_id);
    }
    DataDir* data_dir = tablet->data_dir();

    std::set<std::string> expected_local_files;
    for (const auto& file : manifest.files) {
        RETURN_IF_ERROR(_report_every(10, report_counter, finished_num, total_num,
                                      TTaskType::type::DOWNLOAD));
        if (file.md5.empty()) {
            return Status::Error<ErrorCode::RESTORE_MANIFEST_MISMATCH, false>(
                    "invalid manifest of remote path {}: no md5 of file {}", remote_path,
                    file.name);
        }
        std::string local_file_name;
        RETURN_IF_ERROR(_replace_tablet_id(file.name, local_tablet_id, &local_file_name));
        expected_local_files.insert(local_file_name);
        std::string full_local_file = local_path + "/" + local_file_name;

        // the tablet meta file is always downloaded, the others are skipped if the same md5.
        if (!_end_with(file.name, ".hdr") && local_files.contains(local_file_name)) {
            std::string local_md5sum;
            Status st = io::global_local_filesystem()->md5sum(full_local_file, &local_md5sum);
            if (st.ok() && local_md5sum == file.md5) {
                LOG(INFO) << "remote file already exist in local, no need to download. file: "
                          << file.name;
                ++stats->skipped_files;
                stats->skipped_bytes += file.size;
                continue;
            }
        }

        // locate the object by the md5 in the manifest, never by listing the remote path.
        std::string full_remote_file = remote_path + "/" + file.name + "." + file.md5;
        if (data_dir->reach_capacity_limit(file.size)) {
            return Status::Error<ErrorCode::EXCEEDED_LIMIT>(
                    "reach the capacity limit of path {}, file_size={}", data_dir->path(),
                    file.size);
        }
        LOG(INFO) << "begin to download from " << full_remote_file << " to " << full_local_file;
        RETURN_IF_ERROR(_remote_fs->download(full_remote_file, full_local_file));
        std::string downloaded_md5sum;
        RETURN_IF_ERROR(io::global_local_filesystem()->md5sum(full_local_file, &downloaded_md5sum));
        if (downloaded_md5sum != file.md5) {
            return Status::Error<ErrorCode::RESTORE_MANIFEST_MISMATCH, false>(
                    "restore manifest check failed, tablet {}, path {}: md5 mismatch of "
                    "downloaded file {}, expected {}, actual {}",
                    local_tablet_id, local_path, full_remote_file, file.md5, downloaded_md5sum);
        }
        if (!_end_with(file.name, ".hdr")) {
            ++stats->downloaded_files;
            stats->downloaded_bytes += file.size;
        }
        local_files.insert(local_file_name);
    }

    // delete the local files not in the manifest
    for (const auto& local_file : local_files) {
        if (expected_local_files.contains(local_file)) {
            continue;
        }
        std::string full_local_file = local_path + "/" + local_file;
        LOG(INFO) << "delete local snapshot file not in the manifest: " << full_local_file;
        RETURN_IF_ERROR(io::global_local_filesystem()->delete_file(full_local_file));
    }
    return Status::OK();
}

Status SnapshotLoader::_clear_local_snapshot_files(const std::string& local_path) {
    std::vector<std::string> local_files;
    RETURN_IF_ERROR(_get_existing_files_from_local(local_path, &local_files));
    for (const auto& local_file : local_files) {
        RETURN_IF_ERROR(io::global_local_filesystem()->delete_file(local_path + "/" + local_file));
    }
    LOG(INFO) << "cleared " << local_files.size() << " files in local snapshot path " << local_path
              << ", job: " << _job_id << ", task id: " << _task_id;
    return Status::OK();
}

void SnapshotLoader::_add_tablet_download_stats(int64_t local_tablet_id, int64_t remote_tablet_id,
                                                SnapshotDownloadStats stats) {
    stats.classify_tablet();
    LOG(INFO) << "download stats of local tablet " << local_tablet_id << ", remote tablet "
              << remote_tablet_id << ", job: " << _job_id << ", task id: " << _task_id << ", "
              << stats.to_string();
    _download_stats.merge(stats);
}

void SnapshotLoader::_add_manifest_verified_tablet(int64_t tablet_id, bool digest_checked) {
    _manifest_verified_tablets.push_back(tablet_id);
    if (digest_checked) {
        ++_manifest_digest_checked_num;
    }
}

Status SnapshotLoader::remote_http_download(
        const std::vector<TRemoteTabletSnapshot>& remote_tablet_snapshots,
        std::vector<int64_t>* downloaded_tablet_ids) {
    // check if job has already been cancelled

#ifndef BE_TEST
    int tmp_counter = 1;
    RETURN_IF_ERROR(_report_every(0, &tmp_counter, 0, 0, TTaskType::type::DOWNLOAD));
#endif
    Status status = Status::OK();

    for (const auto& remote_tablet_snapshot : remote_tablet_snapshots) {
        auto local_tablet_id = remote_tablet_snapshot.local_tablet_id;
        const auto& local_path = remote_tablet_snapshot.local_snapshot_path;
        const auto& remote_path = remote_tablet_snapshot.remote_snapshot_path;

        LOG(INFO) << fmt::format(
                "download snapshots via http. job: {}, task id: {}, local dir: {}, remote dir: {}",
                _job_id, _task_id, local_path, remote_path);

        TabletSharedPtr tablet = _engine.tablet_manager()->get_tablet(local_tablet_id);
        if (tablet == nullptr) {
            std::string msg = fmt::format("failed to get local tablet: {}", local_tablet_id);
            LOG(WARNING) << msg;
            return Status::RuntimeError(std::move(msg));
        }

        if (downloaded_tablet_ids != nullptr) {
            downloaded_tablet_ids->push_back(local_tablet_id);
        }

#ifndef BE_TEST
        int report_counter = 0;
        int finished_num = 0;
        int total_num = doris::cast_set<int>(remote_tablet_snapshots.size());
#endif
        SnapshotDownloadStats tablet_stats;
        auto download_tablet = [&](bool disable_reuse, bool* manifest_mismatch,
                                   ManifestCheckResult* manifest_check_result) {
            SnapshotHttpDownloader downloader(remote_tablet_snapshot, tablet, *this);
            downloader.set_disable_reuse(disable_reuse);
#ifndef BE_TEST
            downloader.set_report_progress_callback(
                    [this, &report_counter, &finished_num, &total_num]() {
                        return _report_every(10, &report_counter, finished_num, total_num,
                                             TTaskType::type::DOWNLOAD);
                    });
#endif
            Status st = downloader.download();
            _set_http_download_files_num(downloader.get_download_file_num());
            *manifest_mismatch = downloader.manifest_mismatch();
            *manifest_check_result = downloader.manifest_check_result();
            // the stats of the last attempt only
            tablet_stats = downloader.stats();
            return st;
        };

        bool manifest_mismatch = false;
        ManifestCheckResult manifest_check_result;
        Status st = download_tablet(false, &manifest_mismatch, &manifest_check_result);
        if (!st.ok() && manifest_mismatch) {
            // The files reused locally or downloaded do not match the manifest, download all the
            // files of the tablet again without reusing any local file.
            LOG(WARNING) << "downloaded snapshot of tablet " << local_tablet_id
                         << " does not match the manifest, download it again without reusing "
                            "local files. job: "
                         << _job_id << ", task id: " << _task_id << ", error: " << st;
            st = download_tablet(true, &manifest_mismatch, &manifest_check_result);
        }
        RETURN_IF_ERROR(st);
        if (manifest_check_result.checked) {
            _add_manifest_verified_tablet(local_tablet_id, manifest_check_result.digest_checked);
        }
        _add_tablet_download_stats(local_tablet_id, remote_tablet_snapshot.remote_tablet_id,
                                   std::move(tablet_stats));

#ifndef BE_TEST
        ++finished_num;
#endif
    }

    LOG(INFO) << "finished to download snapshots. job: " << _job_id << ", task id: " << _task_id;
    return status;
}

// move the snapshot files in snapshot_path
// to tablet_path
// If overwrite, just replace the tablet_path with snapshot_path,
// else: (TODO)
//
// MUST hold tablet's header lock, push lock, cumulative lock and base compaction lock
Status SnapshotLoader::move(const std::string& snapshot_path, TabletSharedPtr tablet,
                            bool overwrite) {
    return move_with_restore_stage(snapshot_path, tablet, overwrite, nullptr);
}

Status SnapshotLoader::move_with_restore_stage(const std::string& snapshot_path,
                                               TabletSharedPtr tablet, bool overwrite,
                                               const RestoreStagePB* restore_stage) {
    // Debug point: the move fails (params: tablet_id, any if unset), for the failure of the whole move.
    DBUG_EXECUTE_IF("SnapshotLoader.move.fail", {
        auto ptablet = dp->param<int64_t>("tablet_id", 0);
        if (ptablet == 0 || ptablet == tablet->tablet_id()) {
            return Status::InternalError("injected failure of the move of tablet {}",
                                         tablet->tablet_id());
        }
    });
    // Take a lock to protect the local snapshot path.
    auto local_snapshot_guard = LocalSnapshotLock::instance().acquire(snapshot_path);

    auto tablet_path = tablet->tablet_path();
    auto store_path = tablet->data_dir()->path();
    LOG(INFO) << "begin to move snapshot files. from: " << snapshot_path << ", to: " << tablet_path
              << ", store: " << store_path << ", job: " << _job_id << ", task id: " << _task_id;

    Status status = Status::OK();

    // validate snapshot_path and tablet_path
    int64_t snapshot_tablet_id = 0;
    int32_t snapshot_schema_hash = 0;
    RETURN_IF_ERROR(_get_tablet_id_and_schema_hash_from_file_path(
            snapshot_path, &snapshot_tablet_id, &snapshot_schema_hash));

    int64_t tablet_id = 0;
    int32_t schema_hash = 0;
    RETURN_IF_ERROR(
            _get_tablet_id_and_schema_hash_from_file_path(tablet_path, &tablet_id, &schema_hash));

    if (tablet_id != snapshot_tablet_id || schema_hash != snapshot_schema_hash) {
        std::stringstream ss;
        ss << "path does not match. snapshot: " << snapshot_path
           << ", tablet path: " << tablet_path;
        LOG(WARNING) << ss.str();
        return Status::InternalError(ss.str());
    }

    DataDir* store = _engine.get_store(store_path);
    if (store == nullptr) {
        std::stringstream ss;
        ss << "failed to get store by path: " << store_path;
        LOG(WARNING) << ss.str();
        return Status::InternalError(ss.str());
    }

    if (!std::filesystem::exists(tablet_path)) {
        std::stringstream ss;
        ss << "tablet path does not exist: " << tablet_path;
        LOG(WARNING) << ss.str();
        return Status::InternalError(ss.str());
    }

    if (!std::filesystem::exists(snapshot_path)) {
        std::stringstream ss;
        ss << "snapshot path does not exist: " << snapshot_path;
        LOG(WARNING) << ss.str();
        return Status::InternalError(ss.str());
    }

    std::string loaded_tag_path = get_loaded_tag_path(snapshot_path);
    bool already_loaded = false;
    RETURN_IF_ERROR(io::global_local_filesystem()->exists(loaded_tag_path, &already_loaded));
    if (already_loaded) {
        LOG(INFO) << "snapshot path already moved: " << snapshot_path;
        return Status::OK();
    }

    // rename the rowset ids and tabletid info in rowset meta
    // The restore stage of the source is dropped (the snapshot of a tablet which an earlier restore
    // loaded has it), the one of this restore, if any, is saved with the tablet meta by the reload.
    auto res = _engine.snapshot_mgr()->convert_rowset_ids(
            snapshot_path, tablet_id, tablet->replica_id(), tablet->table_id(),
            tablet->partition_id(), schema_hash, true, restore_stage);
    if (!res.has_value()) [[unlikely]] {
        auto err_msg =
                fmt::format("failed to convert rowsetids in snapshot: {}, tablet path: {}, err: {}",
                            snapshot_path, tablet_path, res.error());
        LOG(WARNING) << err_msg;
        return Status::InternalError(err_msg);
    }

    if (!overwrite) {
        throw Exception(Status::FatalError("only support overwrite now"));
    }

    // Medium migration/clone/checkpoint/compaction may change or check the
    // files and tablet meta, so we need to take these locks.
    std::unique_lock migration_lock(tablet->get_migration_lock(), std::try_to_lock);
    std::unique_lock base_compact_lock(tablet->get_base_compaction_lock(), std::try_to_lock);
    std::unique_lock cumu_compact_lock(tablet->get_cumulative_compaction_lock(), std::try_to_lock);
    std::unique_lock cold_compact_lock(tablet->get_cold_compaction_lock(), std::try_to_lock);
    std::unique_lock build_idx_lock(tablet->get_build_inverted_index_lock(), std::try_to_lock);
    std::unique_lock meta_store_lock(tablet->get_meta_store_lock(), std::try_to_lock);
    if (!migration_lock.owns_lock() || !base_compact_lock.owns_lock() ||
        !cumu_compact_lock.owns_lock() || !cold_compact_lock.owns_lock() ||
        !build_idx_lock.owns_lock() || !meta_store_lock.owns_lock()) {
        // This error should be retryable
        auto obtain_lock_status =
                Status::ObtainLockFailed("failed to get tablet locks, tablet: {}", tablet_id);
        LOG(WARNING) << obtain_lock_status << ", snapshot path: " << snapshot_path
                     << ", tablet path: " << tablet_path;
        return obtain_lock_status;
    }

    std::vector<std::string> snapshot_files;
    RETURN_IF_ERROR(_get_existing_files_from_local(snapshot_path, &snapshot_files));

    // FIXME: the below logic will demage the tablet files if failed in the middle.

    // 1. simply delete the old dir and replace it with the snapshot dir
    try {
        // This remove seems soft enough, because we already get
        // tablet id and schema hash from this path, which
        // means this path is a valid path.
        std::filesystem::remove_all(tablet_path);
        VLOG_CRITICAL << "remove dir: " << tablet_path;
        std::filesystem::create_directory(tablet_path);
        VLOG_CRITICAL << "re-create dir: " << tablet_path;
    } catch (const std::filesystem::filesystem_error& e) {
        std::stringstream ss;
        ss << "failed to move tablet path: " << tablet_path << ". err: " << e.what();
        LOG(WARNING) << ss.str();
        return Status::InternalError(ss.str());
    }

    // link files one by one
    // files in snapshot dir will be moved in snapshot clean process
    std::vector<std::string> linked_files;
    for (auto& file : snapshot_files) {
        auto full_src_path = fmt::format("{}/{}", snapshot_path, file);
        auto full_dest_path = fmt::format("{}/{}", tablet_path, file);
        if (link(full_src_path.c_str(), full_dest_path.c_str()) != 0) {
            LOG(WARNING) << "failed to link file from " << full_src_path << " to " << full_dest_path
                         << ", err: " << std::strerror(errno);

            // clean the already linked files
            for (auto& linked_file : linked_files) {
                remove(linked_file.c_str());
            }

            return Status::InternalError("move tablet failed");
        }
        linked_files.push_back(full_dest_path);
        VLOG_CRITICAL << "link file from " << full_src_path << " to " << full_dest_path;
    }

    // snapshot loader not need to change tablet uid
    // fixme: there is no header now and can not call load_one_tablet here
    // reload header
    Status ost = _engine.tablet_manager()->load_tablet_from_dir(store, tablet_id, schema_hash,
                                                                tablet_path, true);
    if (!ost.ok()) {
        std::stringstream ss;
        ss << "failed to reload header of tablet: " << tablet_id;
        LOG(WARNING) << ss.str();
        return Status::InternalError(ss.str());
    }

    // mark the snapshot path as loaded
    RETURN_IF_ERROR(write_loaded_tag(snapshot_path, tablet_id));

    LOG(INFO) << "finished to reload header of tablet: " << tablet_id;

    return status;
}

Status SnapshotLoader::make_local_source_snapshot_and_load(const TSnapshotRequest& request,
                                                           std::string* snapshot_path,
                                                           LocalSourceStats* stats) {
    auto tablet = _engine.tablet_manager()->get_tablet(request.tablet_id);
    if (tablet == nullptr) {
        return Status::Error<ErrorCode::TABLE_NOT_FOUND>("failed to get tablet. tablet={}",
                                                         request.tablet_id);
    }
    restore_debug_delay("SnapshotLoader.make_local_source.delay", request.tablet_id);
    const int64_t job_id = request.__isset.job_id ? request.job_id : _job_id;
    if (job_id <= 0 || !request.__isset.version) {
        return restore_not_retryable(
                fmt::format("the snapshot task of tablet {} has no restore job id or no version",
                            request.tablet_id));
    }
    const int64_t base_version = request.version;
    bool already_loaded = false;
    {
        // A resent task: judge the tablet by the evidence of this job, never by the shape of the tablet.
        RestoreTabletState state = read_restore_tablet_state(tablet);
        if (state.has_stage) {
            if (state.stage.job_id() != job_id) {
                return restore_not_retryable(
                        fmt::format("tablet {} was loaded by the restore job {}, not by job {}",
                                    request.tablet_id, state.stage.job_id(), job_id));
            }
            const bool version_ok = state.stage.base_version() == base_version &&
                                    ((state.stage.stage() == RestoreStagePB::LOCAL_LOADED &&
                                      state.max_version == base_version) ||
                                     (state.stage.stage() == RestoreStagePB::APPENDED &&
                                      state.max_version == state.stage.end_version()));
            if (!version_ok) {
                return restore_not_retryable(fmt::format(
                        "tablet {} has the evidence of job {} (stage {}, base version {}, end "
                        "version {}) but its max version is {}, the local source of version {} "
                        "can not be taken as loaded",
                        request.tablet_id, job_id, static_cast<int>(state.stage.stage()),
                        state.stage.base_version(), state.stage.end_version(), state.max_version,
                        base_version));
            }
            already_loaded = true;
        } else if (!state.initial_only) {
            // overwriting is only allowed for the clean tablet the job created
            return restore_not_retryable(fmt::format(
                    "tablet {} has no evidence of the restore job {} and is not the empty tablet "
                    "created by it, max version {}",
                    request.tablet_id, job_id, state.max_version));
        }
    }
    if (already_loaded) {
        LOG(INFO) << "the local source of tablet " << request.tablet_id
                  << " is already loaded by job " << job_id << ", skip loading it";
    } else {
        TSnapshotRequest source_request = request;
        source_request.__set_restore_incremental(false);
        std::string source_path;
        bool allow_incremental_clone = false; // not used
        RETURN_IF_ERROR(_engine.snapshot_mgr()->make_snapshot(source_request, &source_path,
                                                              &allow_incremental_clone, stats));
        Defer release_source {[&]() {
            Status st = _engine.snapshot_mgr()->release_snapshot(source_path);
            if (!st.ok()) {
                LOG(WARNING) << "failed to release the local source snapshot " << source_path
                             << ": " << st;
            }
        }};
        // saved with the tablet meta of the load, so the evidence and the data are one step
        RestoreStagePB stage;
        stage.set_job_id(job_id);
        stage.set_stage(RestoreStagePB::LOCAL_LOADED);
        stage.set_base_version(base_version);
        RETURN_IF_ERROR(move_with_restore_stage(
                fmt::format("{}/{}/{}", source_path, request.tablet_id, request.schema_hash),
                tablet, true, &stage));
    }
    TSnapshotRequest empty_request = request;
    empty_request.__set_restore_local_source(false);
    bool allow_incremental_clone = false; // not used
    return _engine.snapshot_mgr()->make_snapshot(empty_request, snapshot_path,
                                                 &allow_incremental_clone, nullptr);
}

Status SnapshotLoader::append_increment(const std::string& snapshot_path, TabletSharedPtr tablet,
                                        const TRestoreIncrementalRange& range,
                                        const TRestoreBackupDigest* backup_digest,
                                        bool local_source_loaded) {
    restore_debug_delay("SnapshotLoader.append_increment.delay", tablet->tablet_id());
    auto local_snapshot_guard = LocalSnapshotLock::instance().acquire(snapshot_path);

    auto tablet_path = tablet->tablet_path();
    LOG(INFO) << "begin to append the increment (" << range.base_version << ", "
              << range.end_version << "] of snapshot " << snapshot_path << " to tablet "
              << tablet->tablet_id() << ", job: " << _job_id << ", task id: " << _task_id;

    int64_t snapshot_tablet_id = 0;
    int32_t snapshot_schema_hash = 0;
    RETURN_IF_ERROR(_get_tablet_id_and_schema_hash_from_file_path(
            snapshot_path, &snapshot_tablet_id, &snapshot_schema_hash));
    int64_t tablet_id = 0;
    int32_t schema_hash = 0;
    RETURN_IF_ERROR(
            _get_tablet_id_and_schema_hash_from_file_path(tablet_path, &tablet_id, &schema_hash));
    if (tablet_id != snapshot_tablet_id || schema_hash != snapshot_schema_hash) {
        return Status::InternalError("path does not match. snapshot: {}, tablet path: {}",
                                     snapshot_path, tablet_path);
    }
    if (!local_source_loaded) {
        // Not loaded from a local source by a restore job (the non atomic incremental restore appends to the
        // tablet being restored): no evidence, as before.
        bool legacy_exists = false;
        RETURN_IF_ERROR(io::global_local_filesystem()->exists(snapshot_path, &legacy_exists));
        if (!legacy_exists) {
            return Status::InternalError("snapshot path does not exist: {}", snapshot_path);
        }
        bool already_loaded = false;
        RETURN_IF_ERROR(io::global_local_filesystem()->exists(get_loaded_tag_path(snapshot_path),
                                                              &already_loaded));
        if (already_loaded) {
            LOG(INFO) << "snapshot path already appended: " << snapshot_path;
            return Status::OK();
        }
    }
    // The same locks as the full / incremental clone and the move of the restore. They are taken before the
    // judgement of a resent task, so that no compaction or push changes the tablet meanwhile; the header lock is
    // not held by the judgement, as the digest it may compute takes the shared header lock.
    std::unique_lock migration_lock(tablet->get_migration_lock(), std::try_to_lock);
    std::unique_lock base_compact_lock(tablet->get_base_compaction_lock(), std::try_to_lock);
    std::unique_lock cumu_compact_lock(tablet->get_cumulative_compaction_lock(), std::try_to_lock);
    std::unique_lock cold_compact_lock(tablet->get_cold_compaction_lock(), std::try_to_lock);
    std::unique_lock build_idx_lock(tablet->get_build_inverted_index_lock(), std::try_to_lock);
    std::unique_lock meta_store_lock(tablet->get_meta_store_lock(), std::try_to_lock);
    std::unique_lock push_lock(tablet->get_push_lock(), std::try_to_lock);
    std::unique_lock rowset_update_lock(tablet->get_rowset_update_lock(), std::try_to_lock);
    if (!migration_lock.owns_lock() || !base_compact_lock.owns_lock() ||
        !cumu_compact_lock.owns_lock() || !cold_compact_lock.owns_lock() ||
        !build_idx_lock.owns_lock() || !meta_store_lock.owns_lock() || !push_lock.owns_lock() ||
        !rowset_update_lock.owns_lock()) {
        // retryable, nothing is changed
        return Status::ObtainLockFailed("failed to get tablet locks, tablet: {}", tablet_id);
    }

    // Judge by the evidence of this job in the tablet meta.
    RestoreTabletState state;
    if (local_source_loaded) {
        state = read_restore_tablet_state(tablet);
        if (!state.has_stage || state.stage.job_id() != _job_id) {
            return restore_not_retryable(
                    fmt::format("tablet {} has no evidence that the restore job {} loaded it "
                                "(evidence of job {}), "
                                "the increment ({}, {}] can not be appended",
                                tablet_id, _job_id, state.has_stage ? state.stage.job_id() : -1,
                                range.base_version, range.end_version));
        }
        if (state.stage.stage() == RestoreStagePB::APPENDED) {
            if (state.max_version == range.end_version &&
                state.stage.end_version() == range.end_version) {
                LOG(INFO) << "the increment (" << range.base_version << ", " << range.end_version
                          << "] is already appended to tablet " << tablet_id << " by job "
                          << _job_id;
                return Status::OK();
            }
            return restore_not_retryable(
                    fmt::format("tablet {} has the evidence of the append up to version {} by job "
                                "{}, but its max "
                                "version is {}, not the requested {}",
                                tablet_id, state.stage.end_version(), _job_id, state.max_version,
                                range.end_version));
        }
        if (state.stage.base_version() != range.base_version) {
            return restore_not_retryable(fmt::format(
                    "tablet {} was loaded to version {} by job {}, not to the base version {} of "
                    "the "
                    "increment",
                    tablet_id, state.stage.base_version(), _job_id, range.base_version));
        }
        if (state.max_version == range.end_version) {
            // The increment is in the tablet but the evidence of it is not. The protection of the scheduler and the
            // single save of the append make it impossible; if it happens check the data itself against the backup.
            RETURN_IF_ERROR(_check_appended_tablet_equals_backup(tablet, range, backup_digest));
            std::lock_guard wrlock(tablet->get_header_lock());
            RestoreStagePB appended = state.stage;
            appended.set_stage(RestoreStagePB::APPENDED);
            appended.set_end_version(range.end_version);
            tablet->tablet_meta()->set_restore_stage(appended);
            tablet->save_meta();
            LOG(INFO) << "the tablet " << tablet_id << " is at version " << range.end_version
                      << " and equals the backup, the evidence of the append of job " << _job_id
                      << " is written";
            return Status::OK();
        }
        if (state.max_version != range.base_version) {
            return restore_not_retryable(fmt::format(
                    "the max version of tablet {} is {}, neither the base version {} nor the end "
                    "version {} of the increment",
                    tablet_id, state.max_version, range.base_version, range.end_version));
        }
    }

    bool exists = false;
    RETURN_IF_ERROR(io::global_local_filesystem()->exists(snapshot_path, &exists));
    if (!exists) {
        return Status::InternalError("snapshot path does not exist: {}", snapshot_path);
    }

    // rename the rowset ids and tablet id info in the rowset metas (and the segment files)
    auto converted = _engine.snapshot_mgr()->convert_rowset_ids(
            snapshot_path, tablet_id, tablet->replica_id(), tablet->table_id(),
            tablet->partition_id(), schema_hash);
    if (!converted.has_value()) [[unlikely]] {
        return Status::InternalError("failed to convert rowset ids in snapshot {}: {}",
                                     snapshot_path, converted.error());
    }
    // keeps the new rowset ids pending until the rowsets are in the tablet
    auto pending_guards = std::move(converted.value());

    auto cloned_meta = std::make_shared<TabletMeta>();
    RETURN_IF_ERROR(
            cloned_meta->create_from_file(fmt::format("{}/{}.hdr", snapshot_path, tablet_id)));
    std::vector<RowsetMetaSharedPtr> rs_metas;
    for (const auto& [version, rs_meta] : cloned_meta->all_rs_metas()) {
        rs_metas.push_back(rs_meta);
    }
    std::sort(rs_metas.begin(), rs_metas.end(),
              [](const RowsetMetaSharedPtr& a, const RowsetMetaSharedPtr& b) {
                  return a->start_version() < b->start_version();
              });
    int64_t expected_start = range.base_version + 1;
    for (const auto& rs_meta : rs_metas) {
        if (rs_meta->start_version() != expected_start) {
            return Status::InternalError("the increment in {} is not a chain from version {}",
                                         snapshot_path, range.base_version + 1);
        }
        expected_start = rs_meta->end_version() + 1;
    }
    if (rs_metas.empty() || expected_start != range.end_version + 1) {
        return Status::InternalError("the increment in {} does not end at version {}",
                                     snapshot_path, range.end_version);
    }

    std::lock_guard wrlock(tablet->get_header_lock());

    // The local tablet must be what the increment was verified against.
    if (tablet->max_version_unlocked() != range.base_version) {
        return Status::InternalError(
                "the max version of tablet {} is {}, not {}, the increment can not be appended",
                tablet_id, tablet->max_version_unlocked(), range.base_version);
    }
    {
        auto captured = tablet->capture_consistent_rowsets_unlocked(
                Version(0, range.base_version), CaptureRowsetOps {});
        if (!captured) {
            return Status::InternalError("tablet {} has no consistent rowsets up to version {}: {}",
                                         tablet_id, range.base_version, captured.error());
        }
    }

    // link the segment files into the tablet dir
    std::vector<std::string> snapshot_files;
    RETURN_IF_ERROR(_get_existing_files_from_local(snapshot_path, &snapshot_files));
    std::vector<std::string> linked_files;
    auto remove_linked = [&linked_files]() {
        for (const auto& file : linked_files) {
            remove(file.c_str());
        }
    };
    for (const auto& file : snapshot_files) {
        if (file.ends_with(".hdr")) {
            continue;
        }
        if (!file.ends_with(".dat") && !file.ends_with(".idx")) {
            continue;
        }
        auto src = fmt::format("{}/{}", snapshot_path, file);
        auto dest = fmt::format("{}/{}", tablet_path, file);
        bool dest_exists = false;
        Status st = io::global_local_filesystem()->exists(dest, &dest_exists);
        if (st.ok() && dest_exists) {
            st = Status::InternalError("file {} already exists in tablet {}", file, tablet_id);
        }
        if (st.ok() && link(src.c_str(), dest.c_str()) != 0) {
            st = Status::InternalError("failed to link {} to {}: {}", src, dest,
                                       std::strerror(errno));
        }
        if (!st.ok()) {
            remove_linked();
            return st;
        }
        linked_files.push_back(dest);
    }

    std::vector<RowsetSharedPtr> rowsets;
    for (const auto& rs_meta : rs_metas) {
        RowsetSharedPtr rs;
        Status st = tablet->create_rowset(rs_meta, &rs);
        if (!st.ok()) {
            remove_linked();
            return st;
        }
        rowsets.push_back(std::move(rs));
    }

    // revise_tablet_meta adds the rowsets and, for merge-on-write, calculates the delete bitmap of
    // them. It removes the rowsets again if that fails, but not the marks it has made on the old
    // rowsets and the bitmap of the new ones: keep the bitmap to put it back.
    const bool mow = tablet->keys_type() == UNIQUE_KEYS && tablet->enable_unique_key_merge_on_write();
    std::unique_ptr<DeleteBitmap> bitmap_before;
    if (mow) {
        bitmap_before = std::make_unique<DeleteBitmap>(tablet->tablet_meta()->delete_bitmap().snapshot());
    }
    // The evidence of the append is saved by the same save_meta as the rowsets. With the debug point the append is
    // done without it and fails afterwards: a tablet appended with no evidence, as a crash between the two
    // steps of an implementation with separate saves would leave it.
    bool skip_stage = false;
    DBUG_EXECUTE_IF("SnapshotLoader.append_increment.fail_before_stage", {
        auto ptablet = dp->param<int64_t>("tablet_id", 0);
        if (ptablet == 0 || ptablet == tablet_id) {
            skip_stage = true;
        }
    });
    const RestoreStagePB stage_before = state.stage;
    if (!local_source_loaded) {
        skip_stage = true;
    }
    if (!skip_stage) {
        RestoreStagePB appended = stage_before;
        appended.set_stage(RestoreStagePB::APPENDED);
        appended.set_end_version(range.end_version);
        tablet->tablet_meta()->set_restore_stage(appended);
    }
    const bool inject_failure = skip_stage && local_source_loaded;
    Status st = tablet->revise_tablet_meta(rowsets, {}, true);
    if (!st.ok()) {
        if (local_source_loaded) {
            tablet->tablet_meta()->set_restore_stage(stage_before);
        }
        if (mow) {
            tablet->tablet_meta()->delete_bitmap() = std::move(*bitmap_before);
        }
        // the files of the rowsets are removed with the rowsets, the ones not known to the tablet
        // are removed here
        remove_linked();
        LOG(WARNING) << "failed to append the increment to tablet " << tablet_id
                     << ", the tablet is unchanged: " << st;
        return st;
    }
    if (inject_failure) {
        return Status::InternalError(
                "injected failure after the append of tablet {} and before the evidence is "
                "written",
                tablet_id);
    }
    RETURN_IF_ERROR(write_loaded_tag(snapshot_path, tablet_id));
    LOG(INFO) << "finished to append the increment (" << range.base_version << ", "
              << range.end_version << "] to tablet " << tablet_id << ", " << rowsets.size()
              << " rowsets, job: " << _job_id << ", task id: " << _task_id;
    return Status::OK();
}

Status SnapshotLoader::_check_appended_tablet_equals_backup(
        const TabletSharedPtr& tablet, const TRestoreIncrementalRange& range,
        const TRestoreBackupDigest* backup_digest) {
    const int64_t tablet_id = tablet->tablet_id();
    if (tablet->keys_type() == UNIQUE_KEYS) {
        return restore_not_retryable(fmt::format(
                "tablet {} is at version {} with no evidence of the append, and the digest can not "
                "prove the hidden state of a unique key tablet",
                tablet_id, range.end_version));
    }
    if (backup_digest == nullptr || !backup_digest->__isset.root ||
        !backup_digest->__isset.schema_sig || !backup_digest->__isset.algo_version ||
        !backup_digest->__isset.status_code || backup_digest->status_code != "OK") {
        return restore_not_retryable(fmt::format(
                "tablet {} is at version {} with no evidence of the append, and the request has no "
                "usable digest of the backup to check it against",
                tablet_id, range.end_version));
    }
    if (backup_digest->algo_version != static_cast<int32_t>(RestoreDigest::kAlgoVersion)) {
        return restore_not_retryable(
                fmt::format("the algorithm version {} of the digest of the backup is not {}",
                            backup_digest->algo_version, RestoreDigest::kAlgoVersion));
    }
    LogicalDigestResult local;
    Status st = get_tablet_logical_digest(_engine, tablet_id, range.end_version, 1,
                                          RestoreDigestCache::instance(), &local);
    if (!st.ok()) {
        return restore_not_retryable(
                fmt::format("failed to compute the digest of tablet {} at version {}: {}",
                            tablet_id, range.end_version, st.to_string()));
    }
    if (local.algo_version != static_cast<uint32_t>(backup_digest->algo_version) ||
        local.schema_sig != backup_digest->schema_sig || local.root != backup_digest->root ||
        (backup_digest->__isset.rows && local.rows != static_cast<uint64_t>(backup_digest->rows))) {
        return restore_not_retryable(fmt::format(
                "tablet {} is at version {} with no evidence of the append and its digest differs "
                "from the backup (local {}, backup {})",
                tablet_id, range.end_version, local.root, backup_digest->root));
    }
    return Status::OK();
}

Status SnapshotLoader::_get_tablet_id_and_schema_hash_from_file_path(const std::string& src_path,
                                                                     int64_t* tablet_id,
                                                                     int32_t* schema_hash) {
    // path should be like: /path/.../tablet_id/schema_hash
    // we try to extract tablet_id from path
    size_t pos = src_path.find_last_of("/");
    if (pos == std::string::npos || pos == src_path.length() - 1) {
        return Status::InternalError("failed to get tablet id from path: {}", src_path);
    }

    std::string schema_hash_str = src_path.substr(pos + 1);
    std::stringstream ss1;
    ss1 << schema_hash_str;
    ss1 >> *schema_hash;

    // skip schema hash part
    size_t pos2 = src_path.find_last_of("/", pos - 1);
    if (pos2 == std::string::npos) {
        return Status::InternalError("failed to get tablet id from path: {}", src_path);
    }

    std::string tablet_str = src_path.substr(pos2 + 1, pos - pos2);
    std::stringstream ss2;
    ss2 << tablet_str;
    ss2 >> *tablet_id;

    VLOG_CRITICAL << "get tablet id " << *tablet_id << ", schema hash: " << *schema_hash
                  << " from path: " << src_path;
    return Status::OK();
}

Status SnapshotLoader::_check_local_snapshot_paths(
        const std::map<std::string, std::string>& src_to_dest_path, bool check_src) {
    bool res = true;
    for (const auto& pair : src_to_dest_path) {
        std::string path;
        if (check_src) {
            path = pair.first;
        } else {
            path = pair.second;
        }

        RETURN_IF_ERROR(io::global_local_filesystem()->is_directory(path, &res));
        if (!res) {
            std::stringstream ss;
            ss << "snapshot path is not directory or does not exist: " << path;
            LOG(WARNING) << ss.str();
            return Status::RuntimeError(ss.str());
        }
        if (check_src) {
            RETURN_IF_ERROR(_check_snapshot_path_on_broken_storage(path));
        }
    }
    LOG(INFO) << "all local snapshot paths are existing. num: " << src_to_dest_path.size();
    return Status::OK();
}

Status SnapshotLoader::_check_snapshot_path_on_broken_storage(const std::string& path) {
    std::string canonical_path;
    RETURN_IF_ERROR(io::global_local_filesystem()->canonicalize(path, &canonical_path));

    auto broken_paths = _engine.get_broken_paths();
    for (auto* store : _engine.get_stores(true)) {
        std::string canonical_store_path;
        RETURN_IF_ERROR(
                io::global_local_filesystem()->canonicalize(store->path(), &canonical_store_path));
        if (!io::LocalFileSystem::equal_or_sub_path(canonical_store_path, canonical_path)) {
            continue;
        }
        if (!store->is_used() || broken_paths.contains(store->path()) ||
            broken_paths.contains(canonical_store_path)) {
            return Status::IOError(
                    "snapshot path is on broken storage path, snapshot_path={}, "
                    "storage_path={}",
                    canonical_path, canonical_store_path);
        }
        break;
    }

    return Status::OK();
}

Status SnapshotLoader::_get_existing_files_from_local(const std::string& local_path,
                                                      std::vector<std::string>* local_files) {
    bool exists = true;
    std::vector<io::FileInfo> files;
    RETURN_IF_ERROR(io::global_local_filesystem()->list(local_path, true, &files, &exists));
    for (auto& file : files) {
        local_files->push_back(file.file_name);
    }
    LOG(INFO) << "finished to list files in local path: " << local_path
              << ", file num: " << local_files->size();
    return Status::OK();
}

Status SnapshotLoader::_replace_tablet_id(const std::string& file_name, int64_t tablet_id,
                                          std::string* new_file_name) {
    // eg:
    // 10007.hdr
    // 10007_2_2_0_0.idx
    // 10007_2_2_0_0.dat
    if (_end_with(file_name, ".hdr")) {
        std::stringstream ss;
        ss << tablet_id << ".hdr";
        *new_file_name = ss.str();
        return Status::OK();
    } else if (_end_with(file_name, ".idx") || _end_with(file_name, ".dat")) {
        *new_file_name = file_name;
        return Status::OK();
    } else {
        return Status::InternalError("invalid tablet file name: {}", file_name);
    }
}

Status BaseSnapshotLoader::_get_tablet_id_from_remote_path(const std::string& remote_path,
                                                           int64_t* tablet_id) {
    // eg:
    // bos://xxx/../__tbl_10004/__part_10003/__idx_10004/__10005
    size_t pos = remote_path.find_last_of("_");
    if (pos == std::string::npos) {
        return Status::InternalError("invalid remove file path: {}", remote_path);
    }

    std::string tablet_id_str = remote_path.substr(pos + 1);
    std::stringstream ss;
    ss << tablet_id_str;
    ss >> *tablet_id;

    return Status::OK();
}

// only return CANCELLED if FE return that job is cancelled.
// otherwise, return OK
Status BaseSnapshotLoader::_report_every(int report_threshold, int* counter, int32_t finished_num,
                                         int32_t total_num, TTaskType::type type) {
    ++*counter;
    if (*counter <= report_threshold) {
        return Status::OK();
    }

    LOG(INFO) << "report to frontend. job id: " << _job_id << ", task id: " << _task_id
              << ", finished num: " << finished_num << ", total num:" << total_num;

    TNetworkAddress master_addr = _env->cluster_info()->master_fe_addr;

    TSnapshotLoaderReportRequest request;
    request.job_id = _job_id;
    request.task_id = _task_id;
    request.task_type = type;
    request.__set_finished_num(finished_num);
    request.__set_total_num(total_num);
    TStatus report_st;

    Status rpcStatus = ThriftRpcHelper::rpc<FrontendServiceClient>(
            master_addr.hostname, master_addr.port,
            [&request, &report_st](FrontendServiceConnection& client) {
                client->snapshotLoaderReport(report_st, request);
            },
            10000);

    if (!rpcStatus.ok()) {
        // rpc failed, ignore
        return Status::OK();
    }

    // reset
    *counter = 0;
    if (report_st.status_code == TStatusCode::CANCELLED) {
        LOG(INFO) << "job is cancelled. job id: " << _job_id << ", task id: " << _task_id;
        return Status::Cancelled("Cancelled");
    }
    return Status::OK();
}

Status BaseSnapshotLoader::_list_with_checksum(const std::string& dir,
                                               std::map<std::string, FileStat>* md5_files) {
    bool exists = true;
    std::vector<io::FileInfo> files;
    RETURN_IF_ERROR(_remote_fs->list(dir, true, &files, &exists));
    for (auto& tmp_file : files) {
        io::Path path(tmp_file.file_name);
        std::string file_name = path.filename();
        size_t pos = file_name.find_last_of(".");
        if (pos == std::string::npos || pos == file_name.size() - 1) {
            // Not found checksum separator, ignore this file
            continue;
        }
        FileStat stat = {std::string(file_name, 0, pos), std::string(file_name, pos + 1),
                         tmp_file.file_size};
        md5_files->emplace(std::string(file_name, 0, pos), stat);
    }

    return Status::OK();
}

} // end namespace doris
