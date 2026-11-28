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
import org.apache.doris.catalog.Tablet;
import org.apache.doris.common.Config;
import org.apache.doris.persist.gson.GsonUtils;

import com.google.gson.JsonObject;

/**
 * The cases that partition level reuse does not cover (see {@link Unsupported}), the local data sizes of a
 * partition, and the ReuseEstimate column of SHOW RESTORE: the actual result of partition level reuse of a job.
 */
public class RestoreReuseShadowStats {

    /**
     * The cases not covered by the first version of partition level reuse, in the order they are checked.
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
        // A unique merge-on-write table, and restore_reuse_mow is off (the default). With it on, the table without a
        // sequence column is reused. restore_reuse_allow_hidden_state_models does not lift it.
        HIDDEN_STATE_MOW,
        // A unique merge-on-write table with a sequence column (or a sequence mapping): the rows that the delete
        // bitmap marks still decide the result of a later write (a larger sequence value makes the new row a loser).
        // Never reused.
        HIDDEN_STATE_MOW_SEQUENCE,
        // A unique merge-on-read table: the overwritten versions of a key are hidden state, a later delete predicate
        // makes them visible again. Not reused unless restore_reuse_allow_hidden_state_models.
        HIDDEN_STATE_MOR,
        // A frontend or a backend of the cluster does not report the capability of partition level reuse (an old
        // version, a rolling upgrade or rollback), the job downloads all the data. Only the reason is counted.
        MIXED_VERSIONS
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
     * Whether the table has a hidden state, which decides the behavior of the later writes beside the visible rows.
     * Duplicate tables are free of it. A unique merge-on-write table without a sequence column is reused only when
     * Config.restore_reuse_mow is on (it takes precedence over restore_reuse_allow_hidden_state_models, which does
     * not apply to it); one with a sequence column never is. A unique merge-on-read table is reused only when
     * Config.restore_reuse_allow_hidden_state_models is on.
     */
    public static Unsupported checkHiddenState(OlapTable localTbl) {
        if (localTbl.getKeysType() != KeysType.UNIQUE_KEYS) {
            return Unsupported.NONE;
        }
        if (!localTbl.getEnableUniqueKeyMergeOnWrite()) {
            return Config.restore_reuse_allow_hidden_state_models ? Unsupported.NONE : Unsupported.HIDDEN_STATE_MOR;
        }
        if (!Config.restore_reuse_mow) {
            return Unsupported.HIDDEN_STATE_MOW;
        }
        if (localTbl.hasSequenceCol() || localTbl.getSequenceMapCol() != null) {
            return Unsupported.HIDDEN_STATE_MOW_SEQUENCE;
        }
        return Unsupported.NONE;
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
     * The actual result of partition level reuse of the job, for the ReuseEstimate column: the partitions kept and the
     * bytes of a single replica, and the partitions that were candidates but are downloaded, by reason.
     *
     * @param actual the result of the job
     */
    public String toJson(RestoreReuseResult actual) {
        JsonObject json = new JsonObject();
        json.addProperty("kept_partitions", actual.getKeptPartitions());
        json.addProperty("kept_bytes_single_replica", actual.getKeptBytesSingleReplica());
        json.addProperty("kept_token", actual.countKeptReason(RestoreReuseResult.KEPT_STATE_TOKEN));
        json.addProperty("token_equal", actual.getTokenEqualPartitions());
        long mixedVersions = actual.getRejected().getOrDefault(RestoreJob.MIXED_VERSIONS, 0L);
        if (mixedVersions > 0) {
            // the job did not decide the reuse, a node does not have the capability of it
            json.addProperty("mixed_versions", mixedVersions);
        }
        json.addProperty("download_other", actual.countReasons(RestoreReuseResult.DOWNLOAD_CHANGED,
                RestoreReuseResult.DOWNLOAD_VERIFY_ERROR));
        if (Config.enable_restore_atomic_reuse || actual.getKeptAtomicPartitions() > 0
                || actual.getIncrementalAtomicPartitions() > 0) {
            // the partitions of an atomic restore made from the local snapshot of the table being replaced
            json.addProperty("kept_atomic", actual.getKeptAtomicPartitions());
            json.addProperty("kept_atomic_bytes_all_replicas", actual.getKeptAtomicBytesAllReplicas());
        }
        return GsonUtils.GSON.toJson(json);
    }
}
