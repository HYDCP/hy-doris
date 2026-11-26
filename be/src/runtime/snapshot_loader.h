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
#include <gen_cpp/Types_types.h>
#include <gen_cpp/olap_file.pb.h>

#include <cstdint>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include "common/status.h"
#include "olap/tablet_fwd.h"

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

    // An empty snapshot dir (no tablet meta, no data file) is the one of a tablet which the snapshot task loaded
    // from the local source itself (TSnapshotRequest.restore_kept_load): nothing is moved, the evidence of the load
    // of this job in the tablet meta is checked instead, an error with kRestoreNotRetryable if it is not there.
    Status move(const std::string& snapshot_path, TabletSharedPtr tablet, bool overwrite);

    // move(), the tablet meta saved with the loaded tablet has restore_stage as the evidence of the restore job
    // (none if null). The restore stage of the source snapshot is always dropped.
    Status move_with_restore_stage(const std::string& snapshot_path, TabletSharedPtr tablet,
                                   bool overwrite, const RestoreStagePB* restore_stage);

    // Atomic restore with partition level reuse, a kept tablet (request.restore_kept_load): the tablet being
    // replaced (request.ref_tablet_id) is the source of the data up to request.version, the version kept. Make the
    // snapshot of it (see TSnapshotRequest.restore_local_source), load it into the tablet of the restore at once as
    // the move does, release it, and make an empty snapshot dir (snapshot_path), so that the move has nothing to
    // move. On failure no snapshot is left, the tablet may have been loaded.
    Status make_local_source_snapshot_and_load(const TSnapshotRequest& request,
                                               std::string* snapshot_path, LocalSourceStats* stats);

    int64_t get_http_download_files_num() const { return _http_download_files_num; }

    // The prefix of the error messages which make the FE cancel the restore job instead of resending the task.
    static constexpr const char* kRestoreNotRetryable = "[RESTORE_NOT_RETRYABLE]";

private:
    // The move of an empty snapshot: the tablet must have the evidence of the load of the local source by this job
    // (LOCAL_LOADED) and be at the version loaded, else an error of kRestoreNotRetryable. Reads the tablet only.
    Status _confirm_local_source_loaded(const TabletSharedPtr& tablet);

    Status _replace_tablet_id(const std::string& file_name, int64_t tablet_id,
                              std::string* new_file_name);

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
};

} // end namespace doris
