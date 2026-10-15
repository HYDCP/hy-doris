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

#include <gen_cpp/AgentService_types.h>
#include <gen_cpp/MasterService_types.h>
#include <gen_cpp/Types_types.h>
#include <gen_cpp/olap_file.pb.h>

#include <cstdint>
#include <map>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "common/status.h"
#include "runtime/workload_management/resource_context.h"
#include "storage/restore_digest/restore_digest.h"
#include "storage/tablet/tablet_fwd.h"

namespace doris {
namespace io {
class RemoteFileSystem;
} // namespace io

class DataDir;
class TRemoteTabletSnapshot;
struct LocalSourceStats;
class StorageEngine;

struct FileStat {
    std::string name;
    std::string md5;
    int64_t size;
};
class ExecEnv;

// Compute the md5 and the SHA-256 of a local file by reading it once. Both are lower case hex strings.
// Pass nullptr to skip one of them. If size is not nullptr, it is set to the number of bytes read.
Status compute_file_digests(const std::string& path, std::string* md5, std::string* sha256,
                            int64_t* size = nullptr);

// Lower case hex SHA-256 of the data.
std::string sha256_hex(std::string_view data);

// A file in the manifest of a tablet snapshot.
struct SnapshotManifestFile {
    // file name in the source tablet snapshot dir
    std::string name;
    int64_t size = 0;
    // the md5 suffix of the object name in a repository ("<name>.<md5>"), empty in local mode
    std::string md5;
    // lower case hex SHA-256 of the content, empty if not computed; never set for the tablet meta file
    std::string sha256;
};

// The manifest of a tablet snapshot: the complete list of its files. It is written as a JSON file next to
// the tablet snapshot dir, one level up, so that an old BE never sees it:
//   {"version":1,"tablet_id":<id>,"files":[{"n":<name>,"s":<size>,"m":<md5>,"d":<sha256>}, ...]}
// The files are sorted by name, "m" and "d" are omitted if empty. The SHA-256 of the file content is the
// manifest root, recorded by FE in the backup job info.
struct SnapshotManifest {
    static constexpr int kVersion = 1;
    // the file name in a local snapshot: <storage_root>/snapshot/<time>.<seq>.<timeout>/<tablet_id>/manifest
    static constexpr std::string_view kLocalFileName = "manifest";

    int64_t tablet_id = 0;
    std::vector<SnapshotManifestFile> files;

    // Serialize to the JSON content, the files are sorted by name first.
    std::string serialize();

    // Parse the content of a manifest file, after checking its SHA-256 is the expected root. Returns a
    // RESTORE_MANIFEST_MISMATCH error if the root does not match, or the content is not a valid manifest of
    // the expected tablet.
    static Status parse(std::string_view content, std::string_view expected_root,
                        int64_t expected_tablet_id, SnapshotManifest* manifest);

    // The object name of the manifest in a repository, in the parent dir of the tablet dir. The suffix is the
    // first 32 hex chars of the root instead of the md5 used by the data files, so that it can be located by
    // the root alone: listing the parent dir on an object storage would list all the tablets under it.
    static std::string remote_file_name(int64_t tablet_id, std::string_view root);
};

// Write a manifest to a local file, returns the root (SHA-256 of the content).
Status write_snapshot_manifest(SnapshotManifest& manifest, const std::string& path,
                               std::string* root);

// The decomposed digest file of a tablet snapshot (see RestoreDigestDecomposed) is kept the same way as
// the manifest: next to the tablet snapshot dir, one level up, so that an old BE never sees it. In a
// repository its object name is "__rdigest__<tablet_id>.<first 32 hex chars of the root>".
std::string prefix_digest_remote_file_name(int64_t tablet_id, std::string_view root);

// Write the content of a decomposed digest file to a local file, returns its root (SHA-256).
Status write_prefix_digest_file(const std::string& content, const std::string& path,
                                std::string* root);

// Fetch the decomposed digest file of a tablet of a snapshot kept on a remote BE (a backup kept on
// local), over the BE http download of the snapshot, then check it against `root` and parse it.
// remote_tablet_snapshot.remote_snapshot_path is the tablet snapshot dir, the file is next to it.
Status fetch_prefix_digest_from_remote_be(const TRemoteTabletSnapshot& remote_tablet_snapshot,
                                          const std::string& root, RestoreDigestDecomposed* digest);

// The decomposed digest file of a tablet snapshot (see RestoreDigestDecomposed) is kept the same way as
// the manifest: next to the tablet snapshot dir, one level up, so that an old BE never sees it. In a
// repository its object name is "__rdigest__<tablet_id>.<first 32 hex chars of the root>".
std::string prefix_digest_remote_file_name(int64_t tablet_id, std::string_view root);

// Write the content of a decomposed digest file to a local file, returns its root (SHA-256).
Status write_prefix_digest_file(const std::string& content, const std::string& path,
                                std::string* root);

// Fetch the decomposed digest file of a tablet of a snapshot kept on a remote BE (a backup kept on
// local), over the BE http download of the snapshot, then check it against `root` and parse it.
// remote_tablet_snapshot.remote_snapshot_path is the tablet snapshot dir, the file is next to it.
Status fetch_prefix_digest_from_remote_be(const TRemoteTabletSnapshot& remote_tablet_snapshot,
                                          const std::string& root, RestoreDigestDecomposed* digest);

// Returns the parent dir of a path (without the trailing '/'), or an error if it has none.
Status parent_path_of(const std::string& path, std::string* parent);

struct ManifestCheckResult {
    // The local tablet snapshot matches the manifest.
    bool checked = false;
    // The SHA-256 of all the files except the tablet meta file are checked.
    bool digest_checked = false;
};

// Check the files of a downloaded local tablet snapshot against the manifest of the source tablet
// snapshot:
//  - the set of files must be the same, no more and no less;
//  - the size of each file must be the same;
//  - if check_digest is true, the SHA-256 of each file must be the same, if the manifest has it. The
//    tablet meta file is excluded, it is rewritten in restore.
// The names in the manifest are those of the source tablet, the tablet meta file "<source tablet id>.hdr"
// is expected as "<local_tablet_id>.hdr", as it is renamed when downloading.
//
// Returns a RESTORE_MANIFEST_MISMATCH error with the first mismatched file, the expected and the actual
// value, if the local tablet snapshot does not match.
Status check_tablet_snapshot_manifest(const std::string& local_path, int64_t local_tablet_id,
                                      const SnapshotManifest& manifest, bool check_digest,
                                      ManifestCheckResult* result);

// Statistics of the data reused locally and downloaded by a download task, the tablet meta files excluded.
struct SnapshotDownloadStats {
    int64_t linked_files = 0;
    int64_t linked_bytes = 0;
    int64_t skipped_files = 0;
    int64_t skipped_bytes = 0;
    int64_t downloaded_files = 0;
    int64_t downloaded_bytes = 0;
    int64_t tablets_full_reuse = 0;
    int64_t tablets_partial_reuse = 0;
    int64_t tablets_no_reuse = 0;
    int64_t unmatched_rowsets = 0;
    int64_t unmatched_no_source_rowset_id = 0;
    int64_t unmatched_source_not_in_snapshot = 0;
    int64_t unmatched_version_mismatch = 0;
    // tablets downloaded as an increment (see select_incremental_rowsets), and the files and bytes of them,
    // which are also in downloaded_files / downloaded_bytes
    int64_t tablets_incremental = 0;
    int64_t incremental_files = 0;
    int64_t incremental_bytes = 0;

    // Count the stats of one tablet as full / partial / no reuse by its files, nothing if it has no file.
    void classify_tablet();
    void merge(const SnapshotDownloadStats& other);
    std::string to_string() const;
    TDownloadStats to_thrift() const;
};

// The increment (base_version, end_version] of a remote tablet snapshot, to append to a local tablet at
// base_version (the incremental restore). The remote tablet meta is cut to the rowsets of the increment: the
// rowsets with a start version after base_version, which must form a chain from base_version + 1 to end_version
// (otherwise the backup can not be cut at base_version). The stale rowsets, the incremental rowsets (deprecated)
// and the delete bitmap are dropped, the delete bitmap of a merge-on-write tablet is calculated again when the
// rowsets are appended. Rowsets in a remote storage are not supported.
//
// rowset_ids receives the id (as in the names of the segment files) of the selected rowsets, in version order.
Status select_incremental_rowsets(const TabletMetaPB& remote, int64_t base_version,
                                  int64_t end_version, TabletMetaPB* increment,
                                  std::vector<std::string>* rowset_ids);

// Whether a file of a tablet snapshot dir is a data file of the rowset: "<rowset_id>_<...>".
bool is_rowset_file(const std::string& file_name, const std::string& rowset_id);

// Cut the manifest of the remote tablet snapshot to the increment: the tablet meta file and the files of the
// selected rowsets, and check that every rowset has all its segments (the number of ".dat" files is
// num_segments).
Status cut_manifest_to_increment(const SnapshotManifest& manifest, const TabletMetaPB& increment,
                                 const std::vector<std::string>& rowset_ids,
                                 SnapshotManifest* cut);

// Count the rowsets of the remote tablet which have no lineage match in the local tablet, with the reason.
// A remote rowset matches if a local rowset has it as source_rowset_id, or it has a local rowset as
// source_rowset_id, with the same version range. Rowsets without segments and remote storage rowsets
// have nothing to reuse and are not counted. The reason of a remote rowset without match is:
//  - version_mismatch: a local rowset has the lineage with it, but a different version range;
//  - source_not_in_snapshot: the local rowsets overlapping its version range all have a source, but not
//    this rowset (e.g. the source rowset was compacted in the snapshot);
//  - no_source_rowset_id: otherwise, a local rowset overlapping its version range has no source, or no
//    local rowset overlaps it.
void count_unmatched_rowsets(const TabletMetaPB& local_meta, const TabletMetaPB& remote_meta,
                             SnapshotDownloadStats* stats);

class BaseSnapshotLoader {
public:
    BaseSnapshotLoader(ExecEnv* env, int64_t job_id, int64_t task_id,
                       const TNetworkAddress& broker_addr = {},
                       const std::map<std::string, std::string>& broker_prop = {});

    virtual ~BaseSnapshotLoader() = default;

    Status init(TStorageBackendType::type type, const std::string& location);

    virtual Status upload(const std::map<std::string, std::string>& src_to_dest_path,
                          std::map<int64_t, std::vector<std::string>>* tablet_files) = 0;

    virtual Status download(const std::map<std::string, std::string>& src_to_dest_path,
                            std::vector<int64_t>* downloaded_tablet_ids) = 0;

    std::shared_ptr<ResourceContext> resource_ctx() { return _resource_ctx; }

protected:
    Status _get_tablet_id_from_remote_path(const std::string& remote_path, int64_t* tablet_id);

    Status _report_every(int report_threshold, int* counter, int finished_num, int total_num,
                         TTaskType::type type);

    Status _list_with_checksum(const std::string& dir, std::map<std::string, FileStat>* md5_files);

protected:
    ExecEnv* _env = nullptr;
    int64_t _job_id;
    int64_t _task_id;
    const TNetworkAddress _broker_addr;
    const std::map<std::string, std::string> _prop;
    std::shared_ptr<io::RemoteFileSystem> _remote_fs;
    std::shared_ptr<ResourceContext> _resource_ctx;
};

/*
 * Upload:
 * upload() will upload the specified snapshot
 * to remote storage via broker.
 * Each call of upload() is responsible for several tablet snapshots.
 *
 * It will try to get the existing files in remote storage,
 * and only upload the incremental part of files.
 *
 * Download:
 * download() will download the remote tablet snapshot files
 * to local snapshot dir via broker.
 * It will also only download files which does not exist in local dir.
 *
 * Move:
 * move() is the final step of restore process. it will replace the
 * old tablet data dir with the newly downloaded snapshot dir.
 * and reload the tablet header to take this tablet on line.
 *
 */
class SnapshotLoader : public BaseSnapshotLoader {
    friend class SnapshotHttpDownloader;

public:
    SnapshotLoader(StorageEngine& engine, ExecEnv* env, int64_t job_id, int64_t task_id,
                   const TNetworkAddress& broker_addr = {},
                   const std::map<std::string, std::string>& broker_prop = {});
    ~SnapshotLoader() override {};

    Status upload(const std::map<std::string, std::string>& src_to_dest_path,
                  std::map<int64_t, std::vector<std::string>>* tablet_files) override;

    Status download(const std::map<std::string, std::string>& src_to_dest_path,
                    std::vector<int64_t>* downloaded_tablet_ids) override;

    Status remote_http_download(const std::vector<TRemoteTabletSnapshot>& remote_tablets,
                                std::vector<int64_t>* downloaded_tablet_ids);

    Status move(const std::string& snapshot_path, TabletSharedPtr tablet, bool overwrite);

    // move(), the tablet meta saved with the loaded tablet has restore_stage as the evidence of the restore job
    // (none if null). The restore stage of the source snapshot is always dropped.
    Status move_with_restore_stage(const std::string& snapshot_path, TabletSharedPtr tablet,
                                   bool overwrite, const RestoreStagePB* restore_stage);

    // Atomic restore with partition level reuse, a tablet whose increment is to be downloaded: the tablet being
    // replaced (request.ref_tablet_id) is the source of the data up to request.version, the version the increment
    // starts from. Make the snapshot of it (see TSnapshotRequest.restore_local_source), load it into the tablet of the
    // restore at once as the move does, release it, and make an empty snapshot dir (snapshot_path) for the increment
    // to be downloaded into. The tablet then looks like the local tablet of the incremental restore. On failure
    // no snapshot is left, the tablet may have been loaded.
    Status make_local_source_snapshot_and_load(const TSnapshotRequest& request,
                                               std::string* snapshot_path, LocalSourceStats* stats);

    int64_t get_http_download_files_num() const { return _http_download_files_num; }

    // The manifest root of each tablet to download, the key is the remote path, the same as the key
    // of src_to_dest_path of download().
    void set_manifest_roots(std::map<std::string, std::string> manifest_roots) {
        _manifest_roots = std::move(manifest_roots);
    }

    // The increment to download of each tablet, by the remote path (same key as src_to_dest_path of
    // download()); the other tablets are downloaded as a whole.
    void set_incremental_ranges(std::map<std::string, TRestoreIncrementalRange> ranges) {
        _incremental_ranges = std::move(ranges);
    }

    // Append the rowsets of the increment, downloaded into the snapshot dir, to the tablet whose max version is
    // range.base_version, which then has the max version range.end_version. All or nothing: on failure the
    // tablet (rowsets, delete bitmap, files) is as before. Idempotent per snapshot dir.
    //
    // local_source_loaded: the tablet was loaded from a local source by the atomic restore. Only then the judgement
    // below by the evidence is done; otherwise (the non atomic incremental restore) the tablet is checked for the
    // base version only and a snapshot dir already appended is skipped by its loaded tag, as before.
    //
    // The restore job (the job id of the loader) must have loaded the local source into the tablet (the evidence
    // RestoreStagePB of the tablet meta), and the evidence of the append is saved with the append. A resent task is
    // judged by the evidence: appended -> OK; loaded and still at the base version -> append; loaded and already at
    // the end version (the append was done without the evidence) -> OK if the digest of the tablet at the end
    // version equals backup_digest (the digest of the backup of the tablet), else an error. Anything else is an
    // error whose message has the prefix kRestoreNotRetryable, the FE cancels the job on it.
    Status append_increment(const std::string& snapshot_path, TabletSharedPtr tablet,
                            const TRestoreIncrementalRange& range,
                            const TRestoreBackupDigest* backup_digest = nullptr,
                            bool local_source_loaded = true);

    // The prefix of the error messages which make the FE cancel the restore job instead of resending the task.
    static constexpr const char* kRestoreNotRetryable = "[RESTORE_NOT_RETRYABLE]";

    // The manifest root of each tablet uploaded by upload().
    const std::map<int64_t, std::string>& uploaded_manifest_roots() const {
        return _uploaded_manifest_roots;
    }

    // The local tablets whose downloaded snapshot has been checked against the manifest.
    const std::vector<int64_t>& manifest_verified_tablets() const {
        return _manifest_verified_tablets;
    }

    // The decomposed digest (rdigest) root of each tablet uploaded by upload(), only the tablets
    // which have one.
    const std::map<int64_t, std::string>& uploaded_prefix_digest_roots() const {
        return _uploaded_prefix_digest_roots;
    }

    // Fetch the decomposed digest file of a tablet from the repository, check it against the root
    // recorded in the job info and parse it. The incremental restore composes the digest of the
    // remote tablet at a version from it: digest->compose(version, &d). remote_path and local_path
    // are the same as the ones of download(); local_path only decides where the temporary file is.
    Status fetch_prefix_digest(const std::string& remote_path, const std::string& local_path,
                               int64_t remote_tablet_id, const std::string& root,
                               RestoreDigestDecomposed* digest);

    // Same as fetch_prefix_digest, the temporary file is put in tmp_dir.
    Status fetch_prefix_digest_to_dir(const std::string& remote_path, const std::string& tmp_dir,
                                      int64_t remote_tablet_id, const std::string& root,
                                      RestoreDigestDecomposed* digest);

    // The data reused and downloaded by the tablets downloaded so far.
    const SnapshotDownloadStats& download_stats() const { return _download_stats; }

    // Whether the digests are checked for all the tablets in manifest_verified_tablets().
    bool manifest_digest_checked() const {
        return !_manifest_verified_tablets.empty() &&
               _manifest_digest_checked_num == _manifest_verified_tablets.size();
    }

private:
    // OK if the digest of the tablet at range.end_version equals the one of the backup (algorithm, schema and
    // status checked), else an error of kRestoreNotRetryable. Not for a unique key tablet.
    Status _check_appended_tablet_equals_backup(const TabletSharedPtr& tablet,
                                                const TRestoreIncrementalRange& range,
                                                const TRestoreBackupDigest* backup_digest);

    // Download the files of a tablet from the remote path to the local path, see download().
    Status _download_tablet_from_remote(const std::string& remote_path,
                                        const std::string& local_path, int64_t local_tablet_id,
                                        int64_t remote_tablet_id, int* report_counter,
                                        int finished_num, int total_num,
                                        SnapshotDownloadStats* stats);

    // Fetch the manifest of a tablet from the repository and check it against the root.
    Status _fetch_remote_manifest(const std::string& remote_path, const std::string& local_path,
                                  int64_t remote_tablet_id, const std::string& root,
                                  SnapshotManifest* manifest);

    // Download the files in the manifest of a tablet from the remote path to the local path, then
    // delete the local files not in the manifest. Files not in the manifest in the remote path are
    // ignored.
    Status _download_tablet_by_manifest(const std::string& remote_path,
                                        const std::string& local_path, int64_t local_tablet_id,
                                        const SnapshotManifest& manifest, int* report_counter,
                                        int finished_num, int total_num,
                                        SnapshotDownloadStats* stats);

    // Upload the manifest of a tablet next to its dir in the repository, returns the root.
    Status _upload_manifest(const std::string& src_path, const std::string& dest_path,
                            int64_t tablet_id, std::vector<SnapshotManifestFile> files,
                            std::string* root);

    // Upload the decomposed digest file of a tablet next to its dir in the repository, if the
    // snapshot has one (<snapshot>/<tablet_id>/rdigest), returns its root; empty if there is none.
    Status _upload_prefix_digest(const std::string& src_path, const std::string& dest_path,
                                 int64_t tablet_id, std::string* root);

    // Download only the increment of the tablet, see select_incremental_rowsets.
    Status _download_tablet_incremental(const std::string& remote_path,
                                        const std::string& local_path, int64_t local_tablet_id,
                                        int64_t remote_tablet_id, const std::string& manifest_root,
                                        const TRestoreIncrementalRange& range, int* report_counter,
                                        int finished_num, int total_num,
                                        SnapshotDownloadStats* stats);

    // Delete all the files in a local tablet snapshot dir, so that it can be downloaded again
    // without reusing any local file.
    Status _clear_local_snapshot_files(const std::string& local_path);

    void _add_manifest_verified_tablet(int64_t tablet_id, bool digest_checked);

    // Add the stats of a finished tablet to the total and log them.
    void _add_tablet_download_stats(int64_t local_tablet_id, int64_t remote_tablet_id,
                                    SnapshotDownloadStats stats);

    Status _replace_tablet_id(const std::string& file_name, int64_t tablet_id,
                              std::string* new_file_name);

    Status _check_snapshot_path_on_broken_storage(const std::string& path);

    Status _check_local_snapshot_paths(const std::map<std::string, std::string>& src_to_dest_path,
                                       bool check_src);

    Status _get_tablet_id_and_schema_hash_from_file_path(const std::string& src_path,
                                                         int64_t* tablet_id, int32_t* schema_hash);

    Status _get_existing_files_from_local(const std::string& local_path,
                                          std::vector<std::string>* local_files);

    void _set_http_download_files_num(int64_t num) { _http_download_files_num = num; }

private:
    StorageEngine& _engine;
    // for test remote_http_download
    size_t _http_download_files_num;

    std::map<std::string, std::string> _manifest_roots;
    std::map<std::string, TRestoreIncrementalRange> _incremental_ranges;
    std::map<int64_t, std::string> _uploaded_manifest_roots;
    std::map<int64_t, std::string> _uploaded_prefix_digest_roots;
    std::vector<int64_t> _manifest_verified_tablets;
    size_t _manifest_digest_checked_num = 0;
    SnapshotDownloadStats _download_stats;
};

} // end namespace doris
