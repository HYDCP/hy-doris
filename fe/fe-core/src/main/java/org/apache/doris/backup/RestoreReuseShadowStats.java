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

import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.MaterializedIndex.IndexExtState;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.Replica;
import org.apache.doris.catalog.Replica.ReplicaState;
import org.apache.doris.catalog.RestoreLineage;
import org.apache.doris.catalog.Tablet;
import org.apache.doris.common.Config;
import org.apache.doris.persist.gson.GsonUtils;

import com.google.gson.JsonObject;
import com.google.gson.annotations.SerializedName;

/**
 * Shadow statistics of a restore job: how many partitions (and bytes) the restore job could have
 * kept locally instead of downloading, if partition level reuse were enabled.
 *
 * <p>A partition is counted as reusable only if the lineage check (L0) passes for it, and it is not in a
 * case that the first version of partition level reuse does not cover (see {@link Unsupported}); a partition
 * that passes L0 but is in such a case is counted separately. L0 is necessary but not sufficient for reuse,
 * so the numbers are an upper bound. Computing the statistics never changes the behavior of the restore job.
 */
public class RestoreReuseShadowStats {

    /**
     * The result of the L0 check of a partition, in the order the conditions are checked.
     */
    public enum L0Verdict {
        // All conditions are satisfied.
        REUSABLE,
        // The partition does not exist locally, nothing to reuse.
        NO_LOCAL_PARTITION,
        // The local partition has no restore lineage.
        NO_LINEAGE,
        // The lineage of the local partition points to another source partition.
        LINEAGE_MISMATCH,
        // The local partition was written after it was restored.
        LOCAL_VERSION_CHANGED,
        // The source partition was written after the backup that the local partition was restored from.
        SOURCE_VERSION_CHANGED,
        // The commit seq of the source table is unknown, or goes backwards.
        COMMIT_SEQ_MISMATCH,
        // The reverse and table level checks are not allowed at the check level off and disable.
        LEVEL_NOT_ALLOWED,
        // The source partition in the backup has no restore lineage (reverse check).
        NO_BACKUP_LINEAGE,
        // The local table and the backup table have no restore source relation (table level check).
        NO_TABLE_RELATION,
        // The partition name or range differs from the backup (table level check).
        PARTITION_MISMATCH,
        // The version of the backup differs from the visible version of the local partition.
        VERSION_MISMATCH,
        // Both the lineage and the backup know the cluster id of the source, and they differ.
        CLUSTER_MISMATCH
    }

    /**
     * The cases not covered by the first version of partition level reuse. Only checked for the partitions
     * that pass L0, in this order.
     */
    public enum Unsupported {
        // Covered.
        NONE,
        // Atomic restore replaces the whole table, keeping local partitions there is not supported yet.
        ATOMIC_RESTORE,
        // The logical equivalence of an aggregate table can not be proved (e.g. SUM of floating point values).
        AGGREGATE_TABLE,
        // Part of the data is cooled down to remote storage, reading it costs about the same as downloading.
        REMOTE_STORAGE,
        // A unique merge-on-write table: the rows with a delete sign (and their sequence values) and the rows
        // deleted by a delete predicate are hidden state. The digest skips them, but the partial column update and
        // the sequence check of the later writes read them, so two replicas with the same digest may still behave
        // differently. Not reused unless restore_reuse_allow_hidden_state_models.
        HIDDEN_STATE_MOW,
        // A unique merge-on-read table: the overwritten versions of a key are hidden state, a later delete predicate
        // makes them visible again. Not reused unless restore_reuse_allow_hidden_state_models.
        HIDDEN_STATE_MOR
    }

    @SerializedName("partitions")
    private long partitions = 0;
    @SerializedName("reusable")
    private long reusable = 0;
    @SerializedName("reusable_bytes_single_replica")
    private long reusableBytesSingleReplica = 0;
    // reusable, by the L0 check that passed: a forward, b reverse, c table level
    @SerializedName("reusable_a")
    private long reusableForward = 0;
    @SerializedName("reusable_b")
    private long reusableReverse = 0;
    @SerializedName("reusable_c")
    private long reusableTable = 0;
    @SerializedName("l0_passed_but_atomic_restore")
    private long l0PassedButAtomicRestore = 0;
    @SerializedName("l0_passed_but_aggregate_table")
    private long l0PassedButAggregateTable = 0;
    @SerializedName("l0_passed_but_remote_storage")
    private long l0PassedButRemoteStorage = 0;
    @SerializedName("l0_passed_but_hidden_state_mow")
    private long l0PassedButHiddenStateMow = 0;
    @SerializedName("l0_passed_but_hidden_state_mor")
    private long l0PassedButHiddenStateMor = 0;
    @SerializedName("cluster_mismatch")
    private long clusterMismatch = 0;
    @SerializedName("no_local")
    private long noLocalPartition = 0;
    @SerializedName("no_lineage")
    private long noLineage = 0;
    @SerializedName("lineage_mismatch")
    private long lineageMismatch = 0;
    @SerializedName("local_version_changed")
    private long localVersionChanged = 0;
    @SerializedName("source_version_changed")
    private long sourceVersionChanged = 0;
    @SerializedName("commit_seq_mismatch")
    private long commitSeqMismatch = 0;

    /**
     * Check whether the local partition holds the same data as the source partition in the backup,
     * judged by the restore lineage and the versions only (L0).
     *
     * @param localPart the local partition with the same name, null if absent
     * @param srcDbId the source db id of the backup
     * @param srcTableId the source table id of the backup
     * @param srcPartitionId the source partition id of the backup
     * @param srcVersion the version of the source partition in the backup
     * @param srcCommitSeq the commit seq of the source table in the backup, or
     *         {@link RestoreLineage#UNKNOWN_COMMIT_SEQ}
     */
    public static L0Verdict checkL0(Partition localPart, long srcDbId, long srcTableId, long srcPartitionId,
            long srcVersion, long srcCommitSeq) {
        return checkL0(localPart, srcDbId, srcTableId, srcPartitionId, srcVersion, srcCommitSeq,
                RestoreLineage.UNKNOWN_CLUSTER_ID);
    }

    /**
     * @param srcClusterId the cluster id of the source of the backup, {@link RestoreLineage#UNKNOWN_CLUSTER_ID} if
     *         unknown. If both it and the one of the lineage are known they must be equal.
     */
    public static L0Verdict checkL0(Partition localPart, long srcDbId, long srcTableId, long srcPartitionId,
            long srcVersion, long srcCommitSeq, long srcClusterId) {
        if (localPart == null) {
            return L0Verdict.NO_LOCAL_PARTITION;
        }
        RestoreLineage lineage = localPart.getRestoreLineage();
        // 1. the local partition was restored from the same source partition.
        if (lineage == null) {
            return L0Verdict.NO_LINEAGE;
        }
        if (!lineage.isSameSource(srcDbId, srcTableId, srcPartitionId)) {
            return L0Verdict.LINEAGE_MISMATCH;
        }
        if (RestoreReuseJudge.clusterDiffers(lineage.getSrcClusterId(), srcClusterId)) {
            return L0Verdict.CLUSTER_MISMATCH;
        }
        // 2. no local write since the restore.
        if (localPart.getVisibleVersion() != lineage.getSrcVersion()) {
            return L0Verdict.LOCAL_VERSION_CHANGED;
        }
        // 3. no source write since the backup that the local partition was restored from.
        if (srcVersion != lineage.getSrcVersion()) {
            return L0Verdict.SOURCE_VERSION_CHANGED;
        }
        // 4. both backups come from the same monotonic commit seq of the source table.
        if (lineage.getSrcCommitSeq() == RestoreLineage.UNKNOWN_COMMIT_SEQ
                || srcCommitSeq == RestoreLineage.UNKNOWN_COMMIT_SEQ
                || srcCommitSeq < lineage.getSrcCommitSeq()) {
            return L0Verdict.COMMIT_SEQ_MISMATCH;
        }
        return L0Verdict.REUSABLE;
    }

    /**
     * The forward L0 check without the versions: the local partition was restored from the same source partition, and
     * the commit seq does not go backwards. Used by the incremental append, where the versions differ.
     */
    public static L0Verdict checkL0Relation(Partition localPart, long srcDbId, long srcTableId, long srcPartitionId,
            long srcCommitSeq) {
        return checkL0Relation(localPart, srcDbId, srcTableId, srcPartitionId, srcCommitSeq,
                RestoreLineage.UNKNOWN_CLUSTER_ID);
    }

    public static L0Verdict checkL0Relation(Partition localPart, long srcDbId, long srcTableId, long srcPartitionId,
            long srcCommitSeq, long srcClusterId) {
        if (localPart == null) {
            return L0Verdict.NO_LOCAL_PARTITION;
        }
        RestoreLineage lineage = localPart.getRestoreLineage();
        if (lineage == null) {
            return L0Verdict.NO_LINEAGE;
        }
        if (!lineage.isSameSource(srcDbId, srcTableId, srcPartitionId)) {
            return L0Verdict.LINEAGE_MISMATCH;
        }
        if (RestoreReuseJudge.clusterDiffers(lineage.getSrcClusterId(), srcClusterId)) {
            return L0Verdict.CLUSTER_MISMATCH;
        }
        if (lineage.getSrcCommitSeq() == RestoreLineage.UNKNOWN_COMMIT_SEQ
                || srcCommitSeq == RestoreLineage.UNKNOWN_COMMIT_SEQ
                || srcCommitSeq < lineage.getSrcCommitSeq()) {
            return L0Verdict.COMMIT_SEQ_MISMATCH;
        }
        return L0Verdict.REUSABLE;
    }

    /**
     * Whether the local partition is in a case not covered by the first version of partition level reuse.
     */
    public static Unsupported checkUnsupported(boolean isAtomicRestore, OlapTable localTbl, Partition localPart) {
        if (isAtomicRestore && !Config.enable_restore_atomic_reuse) {
            return Unsupported.ATOMIC_RESTORE;
        }
        if (localTbl.getKeysType() == KeysType.AGG_KEYS) {
            return Unsupported.AGGREGATE_TABLE;
        }
        Unsupported hidden = checkHiddenState(localTbl);
        if (hidden != Unsupported.NONE) {
            return hidden;
        }
        if (localPart.getRemoteDataSize() > 0) {
            return Unsupported.REMOTE_STORAGE;
        }
        return Unsupported.NONE;
    }

    /**
     * Whether the table has a hidden state that the digest does not cover, so that the digest proves the visible rows
     * only, not the behavior of the later writes. Only duplicate tables are free of it. The check is lifted by
     * Config.restore_reuse_allow_hidden_state_models.
     */
    public static Unsupported checkHiddenState(OlapTable localTbl) {
        if (Config.restore_reuse_allow_hidden_state_models || localTbl.getKeysType() != KeysType.UNIQUE_KEYS) {
            return Unsupported.NONE;
        }
        return localTbl.getEnableUniqueKeyMergeOnWrite() ? Unsupported.HIDDEN_STATE_MOW
                : Unsupported.HIDDEN_STATE_MOR;
    }

    /**
     * The local data size of one replica of the partition: for each tablet of each visible index, the local
     * data size of one NORMAL replica. The replica with the largest size is chosen, so that a replica whose
     * size has not been reported yet (0) is not picked; all replicas of a tablet hold the same logical data.
     */
    public static long getSingleReplicaLocalDataSize(Partition partition) {
        long dataSize = 0;
        for (MaterializedIndex index : partition.getMaterializedIndices(IndexExtState.VISIBLE)) {
            for (Tablet tablet : index.getTablets()) {
                long replicaDataSize = 0;
                for (Replica replica : tablet.getReplicas()) {
                    if (replica.getState() == ReplicaState.NORMAL) {
                        replicaDataSize = Math.max(replicaDataSize, replica.getDataSize());
                    }
                }
                dataSize += replicaDataSize;
            }
        }
        return dataSize;
    }

    /** The local data size of all replicas of the partition (what a download of it would write). */
    public static long getAllReplicasLocalDataSize(Partition partition) {
        long dataSize = 0;
        for (MaterializedIndex index : partition.getMaterializedIndices(IndexExtState.VISIBLE)) {
            for (Tablet tablet : index.getTablets()) {
                for (Replica replica : tablet.getReplicas()) {
                    if (replica.getState() == ReplicaState.NORMAL) {
                        dataSize += replica.getDataSize();
                    }
                }
            }
        }
        return dataSize;
    }

    /**
     * Add a partition.
     *
     * @param verdict the result of the L0 check
     * @param unsupported whether the partition is in a case not covered, only used if L0 passes
     * @param singleReplicaBytes the local data size of one replica of the partition, only used if the
     *         partition is reusable
     */
    public void add(L0Verdict verdict, Unsupported unsupported, long singleReplicaBytes) {
        add(verdict, unsupported, singleReplicaBytes, RestoreReuseJudge.L0_FORWARD);
    }

    /**
     * @param l0Path the L0 check that passed ({@link RestoreReuseJudge#L0_FORWARD} etc), only used if L0 passes
     */
    public void add(L0Verdict verdict, Unsupported unsupported, long singleReplicaBytes, String l0Path) {
        partitions++;
        switch (verdict) {
            case REUSABLE:
                switch (unsupported) {
                    case ATOMIC_RESTORE:
                        l0PassedButAtomicRestore++;
                        break;
                    case AGGREGATE_TABLE:
                        l0PassedButAggregateTable++;
                        break;
                    case REMOTE_STORAGE:
                        l0PassedButRemoteStorage++;
                        break;
                    case HIDDEN_STATE_MOW:
                        l0PassedButHiddenStateMow++;
                        break;
                    case HIDDEN_STATE_MOR:
                        l0PassedButHiddenStateMor++;
                        break;
                    default:
                        reusable++;
                        reusableBytesSingleReplica += singleReplicaBytes;
                        if (RestoreReuseJudge.L0_REVERSE.equals(l0Path)) {
                            reusableReverse++;
                        } else if (RestoreReuseJudge.L0_TABLE.equals(l0Path)) {
                            reusableTable++;
                        } else {
                            reusableForward++;
                        }
                        break;
                }
                break;
            case NO_LOCAL_PARTITION:
                noLocalPartition++;
                break;
            case NO_LINEAGE:
                noLineage++;
                break;
            case LINEAGE_MISMATCH:
                lineageMismatch++;
                break;
            case LOCAL_VERSION_CHANGED:
                localVersionChanged++;
                break;
            case SOURCE_VERSION_CHANGED:
                sourceVersionChanged++;
                break;
            case COMMIT_SEQ_MISMATCH:
                commitSeqMismatch++;
                break;
            case CLUSTER_MISMATCH:
                clusterMismatch++;
                break;
            default:
                break;
        }
    }

    public long getPartitions() {
        return partitions;
    }

    public long getReusable() {
        return reusable;
    }

    public long getReusableBytesSingleReplica() {
        return reusableBytesSingleReplica;
    }

    public long getReusableForward() {
        return reusableForward;
    }

    public long getReusableReverse() {
        return reusableReverse;
    }

    public long getReusableTable() {
        return reusableTable;
    }

    public long getL0PassedButAtomicRestore() {
        return l0PassedButAtomicRestore;
    }

    public long getL0PassedButAggregateTable() {
        return l0PassedButAggregateTable;
    }

    public long getL0PassedButRemoteStorage() {
        return l0PassedButRemoteStorage;
    }

    public long getL0PassedButHiddenStateMow() {
        return l0PassedButHiddenStateMow;
    }

    public long getL0PassedButHiddenStateMor() {
        return l0PassedButHiddenStateMor;
    }

    public long getClusterMismatch() {
        return clusterMismatch;
    }

    public long getNoLocalPartition() {
        return noLocalPartition;
    }

    public long getNoLineage() {
        return noLineage;
    }

    public long getLineageMismatch() {
        return lineageMismatch;
    }

    public long getLocalVersionChanged() {
        return localVersionChanged;
    }

    public long getSourceVersionChanged() {
        return sourceVersionChanged;
    }

    public long getCommitSeqMismatch() {
        return commitSeqMismatch;
    }

    @Override
    public String toString() {
        return GsonUtils.GSON.toJson(this);
    }

    /**
     * The estimate with the actual result of partition level reuse of the job added, for the ReuseEstimate column:
     * the partitions kept (by the L0 check that let them in) and the bytes of a single replica, and the partitions
     * that were candidates but are downloaded, by reason.
     *
     * @param actual the result of the job, null if there is none (the feature is off, or not decided yet)
     */
    public String toJson(RestoreReuseResult actual) {
        if (actual == null) {
            return toString();
        }
        JsonObject json = GsonUtils.GSON.toJsonTree(this).getAsJsonObject();
        json.addProperty("kept_partitions", actual.getKeptPartitions());
        json.addProperty("kept_bytes_single_replica", actual.getKeptBytesSingleReplica());
        json.addProperty("kept_a", actual.getKeptByPath(RestoreReuseJudge.L0_FORWARD));
        json.addProperty("kept_b", actual.getKeptByPath(RestoreReuseJudge.L0_REVERSE));
        json.addProperty("kept_c", actual.getKeptByPath(RestoreReuseJudge.L0_TABLE));
        // the check level (requested and effective), and the kept partitions not proved by a digest
        if (!actual.isEmpty()) {
            json.add("check_level", GsonUtils.GSON.toJsonTree(actual.levelSummary()));
        }
        json.addProperty("download_digest_mismatch", actual.countReasons(
                RestoreReuseResult.DOWNLOAD_ROOT_MISMATCH, RestoreReuseResult.DOWNLOAD_SCHEMA_MISMATCH));
        json.addProperty("download_timeout", actual.countReasons(RestoreReuseResult.DOWNLOAD_TIMEOUT));
        json.addProperty("download_not_supported", actual.countReasons(RestoreReuseResult.DOWNLOAD_DIGEST_ERROR));
        json.addProperty("download_sample_failed", actual.countReasons(RestoreReuseResult.DOWNLOAD_SAMPLE_FAILED));
        json.addProperty("download_other", actual.countReasons(RestoreReuseResult.DOWNLOAD_CHANGED,
                RestoreReuseResult.DOWNLOAD_VERIFY_ERROR));
        if (Config.enable_restore_atomic_reuse || actual.getKeptAtomicPartitions() > 0
                || actual.getIncrementalAtomicPartitions() > 0) {
            // the partitions of an atomic restore made from the local snapshot of the table being replaced
            json.addProperty("kept_atomic", actual.getKeptAtomicPartitions());
            json.addProperty("incremental_atomic", actual.getIncrementalAtomicPartitions());
            json.addProperty("kept_atomic_bytes_all_replicas", actual.getKeptAtomicBytesAllReplicas());
        }
        if (Config.enable_restore_incremental_append || actual.getIncrementalPartitions() > 0) {
            // the partitions that download only the rowsets after the local version, and the local data they keep
            json.addProperty("incremental_partitions", actual.getIncrementalPartitions());
            json.addProperty("incremental_local_bytes_all_replicas", actual.getIncrementalLocalBytesAllReplicas());
            json.addProperty("download_not_boundary", actual.countReasons(RestoreReuseResult.DOWNLOAD_NOT_BOUNDARY));
            json.addProperty("download_prefix_error", actual.countReasons(RestoreReuseResult.DOWNLOAD_PREFIX_ERROR));
        }
        return GsonUtils.GSON.toJson(json);
    }
}
