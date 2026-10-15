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

package org.apache.doris.backup;

import org.apache.doris.common.Config;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.thrift.TDownloadStats;

import com.google.gson.annotations.SerializedName;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * How much of the data a restore job reused locally instead of downloading, summed over the download tasks
 * reported by the backends, shown in the DownloadStats column of SHOW RESTORE.
 *
 * <p>The counts are by replica: a download task covers some replicas of tablets, so the sum is by
 * "replica x tablet". Bytes and files exclude the tablet meta files. A replica is "not reported" if its backend
 * does not report the stats (an old version). The stats never affect the job.
 */
public class RestoreDownloadStats {
    @SerializedName("lf")
    private long linkedFiles;
    @SerializedName("lb")
    private long linkedBytes;
    @SerializedName("sf")
    private long skippedFiles;
    @SerializedName("sb")
    private long skippedBytes;
    @SerializedName("df")
    private long downloadedFiles;
    @SerializedName("db")
    private long downloadedBytes;
    @SerializedName("full")
    private long tabletsFullReuse;
    @SerializedName("part")
    private long tabletsPartialReuse;
    @SerializedName("none")
    private long tabletsNoReuse;
    // the tablets restored by appending the increment, and the files and bytes downloaded for them (they are also in
    // the downloaded counts)
    @SerializedName("it")
    private long incrementalTablets;
    @SerializedName("if")
    private long incrementalFiles;
    @SerializedName("ib")
    private long incrementalBytes;
    // Atomic restore, the tablets made from the local tablets of the table being replaced by a snapshot task
    // (kept partitions and the local base of incremental partitions), and the files and bytes hard linked / copied
    // (another disk) from them. Reported by the snapshot tasks, not in the counts of the download.
    @SerializedName("lst")
    private long localSourceTablets;
    @SerializedName("llf")
    private long localSourceLinkedFiles;
    @SerializedName("llb")
    private long localSourceLinkedBytes;
    @SerializedName("lcf")
    private long localSourceCopiedFiles;
    @SerializedName("lcb")
    private long localSourceCopiedBytes;
    @SerializedName("um")
    private long unmatchedRowsets;
    @SerializedName("ums")
    private long unmatchedNoSourceRowsetId;
    @SerializedName("umn")
    private long unmatchedSourceNotInSnapshot;
    @SerializedName("umv")
    private long unmatchedVersionMismatch;
    // the replicas whose backend reported the stats
    @SerializedName("rep")
    private long reportedReplicas;
    // the replicas to download, fixed when the download finishes or the job is cancelled, -1 if not fixed yet
    @SerializedName("tot")
    private long totalReplicas = -1;

    public RestoreDownloadStats() {
        // for persist
    }

    // Add the stats reported by a download task which downloaded the given number of replicas.
    public void add(TDownloadStats stats, long replicas) {
        linkedFiles += stats.getLinkedFiles();
        linkedBytes += stats.getLinkedBytes();
        skippedFiles += stats.getSkippedFiles();
        skippedBytes += stats.getSkippedBytes();
        downloadedFiles += stats.getDownloadedFiles();
        downloadedBytes += stats.getDownloadedBytes();
        tabletsFullReuse += stats.getTabletsFullReuse();
        tabletsPartialReuse += stats.getTabletsPartialReuse();
        tabletsNoReuse += stats.getTabletsNoReuse();
        incrementalTablets += stats.getTabletsIncremental();
        incrementalFiles += stats.getIncrementalFiles();
        incrementalBytes += stats.getIncrementalBytes();
        unmatchedRowsets += stats.getUnmatchedRowsets();
        unmatchedNoSourceRowsetId += stats.getUnmatchedNoSourceRowsetId();
        unmatchedSourceNotInSnapshot += stats.getUnmatchedSourceNotInSnapshot();
        unmatchedVersionMismatch += stats.getUnmatchedVersionMismatch();
        reportedReplicas += replicas;
    }

    // Add the stats reported by a snapshot task made from a local tablet (restore_local_source).
    public void addLocalSource(TDownloadStats stats) {
        localSourceTablets += stats.getLocalSourceTablets();
        localSourceLinkedFiles += stats.getLocalSourceLinkedFiles();
        localSourceLinkedBytes += stats.getLocalSourceLinkedBytes();
        localSourceCopiedFiles += stats.getLocalSourceCopiedFiles();
        localSourceCopiedBytes += stats.getLocalSourceCopiedBytes();
    }

    // The counts of the local snapshots are made before the download starts, which resets the stats.
    public void carryLocalSource(RestoreDownloadStats other) {
        localSourceTablets += other.localSourceTablets;
        localSourceLinkedFiles += other.localSourceLinkedFiles;
        localSourceLinkedBytes += other.localSourceLinkedBytes;
        localSourceCopiedFiles += other.localSourceCopiedFiles;
        localSourceCopiedBytes += other.localSourceCopiedBytes;
    }

    public long getLocalSourceTablets() {
        return localSourceTablets;
    }

    public long getLocalSourceLinkedBytes() {
        return localSourceLinkedBytes;
    }

    public long getLocalSourceCopiedBytes() {
        return localSourceCopiedBytes;
    }

    public boolean isFixed() {
        return totalReplicas >= 0;
    }

    public void fix(long total) {
        totalReplicas = total;
    }

    public long getLinkedBytes() {
        return linkedBytes;
    }

    public long getSkippedBytes() {
        return skippedBytes;
    }

    public long getDownloadedBytes() {
        return downloadedBytes;
    }

    public long getIncrementalBytes() {
        return incrementalBytes;
    }

    public long getIncrementalTablets() {
        return incrementalTablets;
    }

    public long getReportedReplicas() {
        return reportedReplicas;
    }

    // (linked + skipped) / (linked + skipped + downloaded) by bytes, 0 if nothing to download at all.
    public double getReuseRatio() {
        return getReuseRatio(0);
    }

    /**
     * (linked + skipped + kept) / (linked + skipped + kept + downloaded) by bytes, 0 if there is no data at all.
     *
     * @param keptBytes the local data size of the partitions kept by partition level reuse, by replica
     */
    public double getReuseRatio(long keptBytes) {
        return getReuseRatio(keptBytes, 0);
    }

    /**
     * (linked + skipped + kept + incrementalLocal) / (linked + skipped + kept + incrementalLocal + downloaded) by
     * bytes, 0 if there is no data at all.
     *
     * @param incrementalLocalBytes the local data size (0, V_l] that the incremental partitions keep, by replica
     */
    public double getReuseRatio(long keptBytes, long incrementalLocalBytes) {
        long reused = linkedBytes + skippedBytes + keptBytes + incrementalLocalBytes;
        long total = reused + downloadedBytes;
        if (total <= 0) {
            return 0.0;
        }
        return Math.round((double) reused / total * 1000) / 1000.0;
    }

    public String toJson(long currentTotalReplicas) {
        return toJson(currentTotalReplicas, 0);
    }

    /**
     * @param currentTotalReplicas the replicas to download if the stats are not fixed yet
     * @param keptBytes the local data size of the partitions kept by partition level reuse, by replica. It is not
     *         downloaded and not in the other counts, but is data that needed no download.
     */
    public String toJson(long currentTotalReplicas, long keptBytes) {
        return toJson(currentTotalReplicas, keptBytes, 0);
    }

    /**
     * @param keptAtomicBytes the part of keptBytes kept by an atomic restore (the local snapshots of the table being
     *         replaced), it is also in keptBytes
     */
    public String toJson(long currentTotalReplicas, long keptBytes, long keptAtomicBytes) {
        return toJson(currentTotalReplicas, keptBytes, keptAtomicBytes, 0);
    }

    /**
     * @param incrementalLocalBytes the local data size (0, V_l] that the incremental partitions (of an atomic restore
     *         too) keep and only append the increment to, by replica. Not in keptBytes, counts in the reuse ratio.
     */
    public String toJson(long currentTotalReplicas, long keptBytes, long keptAtomicBytes,
            long incrementalLocalBytes) {
        return toJson(currentTotalReplicas, keptBytes, keptAtomicBytes, incrementalLocalBytes, null);
    }

    /**
     * @param checkLevel the check levels of partition level reuse (requested, effective, kept without proof), see
     *         {@link RestoreReuseResult#levelSummary()}, not shown if null
     */
    public String toJson(long currentTotalReplicas, long keptBytes, long keptAtomicBytes,
            long incrementalLocalBytes, Map<String, Object> checkLevel) {
        long total = isFixed() ? totalReplicas : currentTotalReplicas;
        Map<String, Object> json = new LinkedHashMap<>();
        json.put("linked_bytes", linkedBytes);
        json.put("skipped_bytes", skippedBytes);
        json.put("kept_bytes", keptBytes);
        json.put("downloaded_bytes", downloadedBytes);
        if (Config.enable_restore_incremental_append || incrementalTablets > 0 || incrementalLocalBytes > 0) {
            json.put("incremental_tablets", incrementalTablets);
            json.put("incremental_bytes", incrementalBytes);
            json.put("incremental_local_bytes", incrementalLocalBytes);
        }
        if (Config.enable_restore_atomic_reuse || localSourceTablets > 0 || keptAtomicBytes > 0) {
            // atomic restore: what the table being replaced kept, by the local snapshots, not downloaded
            json.put("kept_atomic_bytes", keptAtomicBytes);
            Map<String, Object> atomic = new LinkedHashMap<>();
            atomic.put("local_tablets", localSourceTablets);
            atomic.put("linked_bytes", localSourceLinkedBytes);
            atomic.put("linked_files", localSourceLinkedFiles);
            atomic.put("copied_bytes", localSourceCopiedBytes);
            atomic.put("copied_files", localSourceCopiedFiles);
            json.put("atomic_local", atomic);
        }
        if (checkLevel != null) {
            json.put("check_level", checkLevel);
        }
        json.put("reuse_ratio", getReuseRatio(keptBytes, incrementalLocalBytes));
        json.put("linked_files", linkedFiles);
        json.put("skipped_files", skippedFiles);
        json.put("downloaded_files", downloadedFiles);
        Map<String, Object> replicas = new LinkedHashMap<>();
        replicas.put("full_reuse", tabletsFullReuse);
        replicas.put("partial_reuse", tabletsPartialReuse);
        replicas.put("no_reuse", tabletsNoReuse);
        replicas.put("not_reported", Math.max(0, total - reportedReplicas));
        json.put("replicas", replicas);
        json.put("unmatched_rowsets", unmatchedRowsets);
        Map<String, Object> reasons = new LinkedHashMap<>();
        reasons.put("no_source_rowset_id", unmatchedNoSourceRowsetId);
        reasons.put("source_not_in_snapshot", unmatchedSourceNotInSnapshot);
        reasons.put("version_mismatch", unmatchedVersionMismatch);
        json.put("unmatched_reason", reasons);
        return GsonUtils.GSON.toJson(json);
    }

    @Override
    public String toString() {
        return toJson(totalReplicas);
    }
}
