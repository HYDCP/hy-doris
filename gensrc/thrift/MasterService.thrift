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

namespace cpp doris
namespace java org.apache.doris.thrift

include "AgentService.thrift"
include "PaloInternalService.thrift"
include "Types.thrift"
include "Status.thrift"

struct TTabletInfo {
    1: required Types.TTabletId tablet_id
    2: required Types.TSchemaHash schema_hash
    3: required Types.TVersion version
    4: required Types.TVersionHash version_hash
    5: required Types.TCount row_count
    // data size on local disk
    6: required Types.TSize data_size
    7: optional Types.TStorageMedium storage_medium
    8: optional list<Types.TTransactionId> transaction_ids
    9: optional i64 total_version_count
    10: optional i64 path_hash
    11: optional bool version_miss
    12: optional bool used
    13: optional Types.TPartitionId partition_id
    14: optional bool is_in_memory
    15: optional Types.TReplicaId replica_id
    // data size on remote storage
    16: optional Types.TSize remote_data_size
    // 17: optional Types.TReplicaId cooldown_replica_id
    // 18: optional bool is_cooldown
    19: optional i64 cooldown_term
    20: optional Types.TUniqueId cooldown_meta_id
    21: optional i64 visible_version_count
    22: optional i64 local_index_size = 0      // .idx
    23: optional i64 local_segment_size = 0    // .dat
    24: optional i64 remote_index_size = 0     // .idx
    25: optional i64 remote_segment_size = 0   // .dat
    
    // For cloud
    1000: optional bool is_persistent
}

// Statistics of a download task, summed over all the tablets (replicas) of the task. The bytes and files
// exclude the tablet meta files.
struct TDownloadStats {
    // reused by hard linking the local files of the rowsets with the same lineage (http snapshot only)
    1: optional i64 linked_files
    2: optional i64 linked_bytes
    // already exist locally with the same name and size (and md5), not downloaded
    3: optional i64 skipped_files
    4: optional i64 skipped_bytes
    5: optional i64 downloaded_files
    6: optional i64 downloaded_bytes
    // tablets whose data files are all / partially / not reused
    7: optional i64 tablets_full_reuse
    8: optional i64 tablets_partial_reuse
    9: optional i64 tablets_no_reuse
    // rowsets of the remote tablets which have no lineage match in the local tablets, and why
    10: optional i64 unmatched_rowsets
    11: optional i64 unmatched_no_source_rowset_id
    12: optional i64 unmatched_source_not_in_snapshot
    13: optional i64 unmatched_version_mismatch
    // tablets restored incrementally (only the rowsets of the increment are downloaded), and the files and bytes
    // downloaded for them, which are also counted in downloaded_files / downloaded_bytes
    14: optional i64 tablets_incremental
    15: optional i64 incremental_files
    16: optional i64 incremental_bytes
    // snapshot task with restore_local_source (atomic restore keeps the data of the table being replaced): the
    // tablets made from the local tablet, and their files hard linked / copied (the other disk) from it
    17: optional i64 local_source_tablets
    18: optional i64 local_source_linked_files
    19: optional i64 local_source_linked_bytes
    20: optional i64 local_source_copied_files
    21: optional i64 local_source_copied_bytes
}

// Result of the logical digest computation of a tablet, see TSnapshotRequest.compute_logical_digest and
// TRestoreDigestReq. root is empty unless status_code is OK.
struct TLogicalDigest {
    1: optional i32 algo_version
    2: optional string schema_sig
    // hex of the root digest
    3: optional string root
    4: optional i64 rows
    // OK / NOT_SUPPORTED / ERROR
    5: optional string status_code
    6: optional string status_msg
    // snapshot task with compute_logical_digest: SHA-256 of the decomposed digest file (rdigest) written next
    // to the manifest of the snapshot, from which the digest at any rowset boundary version of the snapshot
    // can be composed. Unset if the file was not written, prefix_msg then says why (it is not an error:
    // unique MoR and a few other models have no decomposed digest).
    7: optional string prefix_root
    8: optional string prefix_msg
    9: optional i64 prefix_bytes
    // restore digest task with prefix_source: OK if the digest of the tablet at the version equals the one composed
    // from the decomposed digest file of the backup and the backup can be cut at the version; otherwise
    // NOT_BOUNDARY / MISMATCH / SCHEMA_MISMATCH / ERROR (prefix_msg says why)
    10: optional string prefix_verdict
}

struct TFinishTaskRequest {
    1: required Types.TBackend backend
    2: required Types.TTaskType task_type
    3: required i64 signature
    4: required Status.TStatus task_status
    5: optional i64 report_version
    6: optional list<TTabletInfo> finish_tablet_infos
    7: optional i64 tablet_checksum
    8: optional i64 request_version
    9: optional i64 request_version_hash
    10: optional string snapshot_path
    11: optional list<Types.TTabletId> error_tablet_ids
    12: optional list<string> snapshot_files
    13: optional map<Types.TTabletId, list<string>> tablet_files
    14: optional list<Types.TTabletId> downloaded_tablet_ids
    15: optional i64 copy_size
    16: optional i64 copy_time_ms
    17: optional map<Types.TTabletId, Types.TVersion> succ_tablets
    18: optional map<i64, i64> table_id_to_delta_num_rows
    19: optional map<i64, map<i64, i64>> table_id_to_tablet_id_to_delta_num_rows
    // for Cloud mow table only, used by FE to check if the response is for the latest request
    20: optional list<AgentService.TCalcDeleteBitmapPartitionInfo> resp_partitions;
    // upload task: SHA-256 of the manifest file uploaded next to the files of each tablet
    21: optional map<Types.TTabletId, string> manifest_roots
    // snapshot task: SHA-256 of the manifest file written next to the snapshot files
    22: optional string manifest_root
    // download task: the tablets whose downloaded snapshot has been checked against the manifest
    23: optional list<Types.TTabletId> manifest_verified_tablets
    // download task: whether the digests of the files of all manifest_verified_tablets are checked
    24: optional bool manifest_digest_checked
    // download task: how much of the data was reused locally instead of downloaded
    25: optional TDownloadStats download_stats
    // snapshot task (compute_logical_digest) and restore digest task: the logical digest of the tablet
    26: optional TLogicalDigest logical_digest
    // upload task: SHA-256 of the decomposed digest file (rdigest) uploaded next to the files of each tablet,
    // only the tablets which have one
    27: optional map<Types.TTabletId, string> prefix_digest_roots
}

struct TTablet {
    1: required list<TTabletInfo> tablet_infos
}

struct TDisk {
    1: required string root_path
    2: required Types.TSize disk_total_capacity
    // local used capacity
    3: required Types.TSize data_used_capacity
    4: required bool used
    5: optional Types.TSize disk_available_capacity
    6: optional i64 path_hash
    7: optional Types.TStorageMedium storage_medium
    8: optional Types.TSize remote_used_capacity
    9: optional Types.TSize trash_used_capacity
}

struct TPluginInfo {
    1: required string plugin_name
    2: required i32 type
}

struct TReportRequest {
    1: required Types.TBackend backend
    2: optional i64 report_version
    3: optional map<Types.TTaskType, set<i64>> tasks // string signature
    4: optional map<Types.TTabletId, TTablet> tablets
    5: optional map<string, TDisk> disks // string root_path
    6: optional bool force_recovery
    7: optional list<TTablet> tablet_list
    // the max compaction score of all tablets on a backend,
    // this field should be set along with tablet report
    8: optional i64 tablet_max_compaction_score
    9: optional list<AgentService.TStoragePolicy> storage_policy // only id and version
    10: optional list<AgentService.TStorageResource> resource // only id and version
    11: i32 num_cores
    12: i32 pipeline_executor_size
    13: optional map<Types.TPartitionId, Types.TVersion> partitions_version
    // tablet num in be, in cloud num_tablets may not eq tablet_list.size()
    14: optional i64 num_tablets
    15: optional list<AgentService.TIndexPolicy> index_policy
    // Running query/loading tasks
    16: optional i64 running_tasks
}

struct TMasterResult {
    // required in V1
    1: required Status.TStatus status
}

// Deprecated
enum TResourceType {
    TRESOURCE_CPU_SHARE = 0,
    TRESOURCE_IO_SHARE = 1,
    TRESOURCE_SSD_READ_IOPS = 2,
    TRESOURCE_SSD_WRITE_IOPS = 3,
    TRESOURCE_SSD_READ_MBPS = 4,
    TRESOURCE_SSD_WRITE_MBPS = 5,
    TRESOURCE_HDD_READ_IOPS = 6,
    TRESOURCE_HDD_WRITE_IOPS = 7,
    TRESOURCE_HDD_READ_MBPS = 8,
    TRESOURCE_HDD_WRITE_MBPS = 9
}

// Deprecated
struct TResourceGroup {
    1: required map<TResourceType, i32> resourceByType
}

// Deprecated
struct TUserResource {
    1: required TResourceGroup resource

    // Share in this user quota; Now only support High, Normal, Low
    2: required map<string, i32> shareByGroup
}

// Deprecated
struct TFetchResourceResult {
    // Master service not find protocol version, so using agent service version
    1: required AgentService.TAgentServiceVersion protocolVersion
    2: required i64 resourceVersion
    3: required map<string, TUserResource> resourceByUser
}
