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

import org.apache.doris.backup.BackupJobInfo.BackupIndexInfo;
import org.apache.doris.backup.BackupJobInfo.BackupOlapTableInfo;
import org.apache.doris.backup.BackupJobInfo.BackupPartitionInfo;
import org.apache.doris.backup.BackupJobInfo.BackupTabletInfo;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.MaterializedIndex.IndexExtState;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.OlapTable.OlapTableState;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.PartitionInfo;
import org.apache.doris.catalog.PartitionItem;
import org.apache.doris.catalog.Replica;
import org.apache.doris.catalog.Replica.ReplicaState;
import org.apache.doris.catalog.RestoreLineage;
import org.apache.doris.catalog.RestoreSource;
import org.apache.doris.catalog.Tablet;
import org.apache.doris.common.Config;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import java.util.Collections;
import java.util.List;
import java.util.Random;

/**
 * The decisions of partition level reuse that need no catalog changes: the check level, the chain of conditions that
 * select the candidate partitions, the choice of the sampled partitions, and the comparison with the digests of the
 * backup. All static and pure, so each of them can be tested alone.
 *
 * <p>The conditions are an ordered chain, cheap ones first. The first condition that is not satisfied gives the
 * reason that the partition is downloaded. Other ways to pass L0 (see {@link #L0_CHECKS}) and other conditions can be
 * added to the chains without changing the callers.
 */
public final class RestoreReuseJudge {

    private RestoreReuseJudge() {
    }

    /** The check level of partition level reuse, from the restore property reuse_check_level. */
    public enum CheckLevel {
        // Judge by L0 only, no digest. Not allowed for unique tables.
        OFF,
        // Compute the digest of a sample of the candidates, and of all of them if the sample finds a mismatch.
        SAMPLE,
        // Compute the digest of all candidates.
        FULL,
        // Never keep a local partition.
        DISABLE;

        /** Parses the property, an absent or invalid value gives the default, or SAMPLE if that is invalid. */
        public static CheckLevel parse(String value, String defaultValue) {
            CheckLevel level = parseOrNull(value);
            if (level == null) {
                level = parseOrNull(defaultValue);
            }
            return level == null ? SAMPLE : level;
        }

        private static CheckLevel parseOrNull(String value) {
            if (value == null) {
                return null;
            }
            try {
                return valueOf(value.trim().toUpperCase());
            } catch (IllegalArgumentException e) {
                return null;
            }
        }

        public String lower() {
            return name().toLowerCase();
        }
    }

    /**
     * The effective check level that the partitions of a table are judged with, from the level requested by the job
     * (the property or the default). Every caller (the selection of the candidates, the estimate of the shadow
     * statistics) gets the level here, so the downstream code only sees the effective one.
     *
     * <p>Unless Config.restore_reuse_allow_unverified_keep is true, OFF and SAMPLE are taken as FULL: a partition is
     * kept or appended only if the digest of every replica proves it, the lineage stamp and L0 only select the
     * candidates. If it is true, the requested level takes effect as before, and a unique table does not allow OFF
     * (it is upgraded to SAMPLE). DISABLE is never changed.
     */
    public static CheckLevel levelOfTable(CheckLevel jobLevel, KeysType keysType) {
        if (jobLevel == CheckLevel.DISABLE) {
            return jobLevel;
        }
        if (!Config.restore_reuse_allow_unverified_keep) {
            return CheckLevel.FULL;
        }
        if (jobLevel == CheckLevel.OFF && keysType == KeysType.UNIQUE_KEYS) {
            return CheckLevel.SAMPLE;
        }
        return jobLevel;
    }

    // ---- the cluster id of the source ----

    private static boolean clusterKnown(long clusterId) {
        return clusterId != RestoreLineage.UNKNOWN_CLUSTER_ID;
    }

    /** Both cluster ids are known and differ. An unknown one (an old stamp or backup) does not differ. */
    public static boolean clusterDiffers(long a, long b) {
        return clusterKnown(a) && clusterKnown(b) && a != b;
    }

    private static boolean clusterKnownEqual(long a, long b) {
        return clusterKnown(a) && clusterKnown(b) && a == b;
    }

    /**
     * Whether the L0 check that passed also compared the cluster id of the source on both sides, and they are equal.
     * If not (an old stamp or backup has no cluster id) the partition is only a candidate for the digest, and must
     * not be kept without one (check level off with Config.restore_reuse_allow_unverified_keep).
     *
     * <p>Note: only the check level off consumes it, and the reverse (b) and table level (c) checks return
     * LEVEL_NOT_ALLOWED at off, so the b and c branches are not reached in that use today. They are kept so that the
     * answer stays right if another caller (or a later level) asks for a b or c path.
     */
    public static boolean clusterProven(Input in, String l0Path) {
        if (L0_FORWARD.equals(l0Path)) {
            RestoreLineage lineage = in.localPartition == null ? null : in.localPartition.getRestoreLineage();
            return lineage != null
                    && clusterKnownEqual(lineage.getSrcClusterId(), in.jobInfo.getSrcClusterIdOrUnknown());
        } else if (L0_REVERSE.equals(l0Path)) {
            return in.backupLineage != null
                    && clusterKnownEqual(in.backupLineage.getSrcClusterId(), in.localClusterId);
        } else if (L0_TABLE.equals(l0Path)) {
            RestoreSource localSource = in.localTable.getRestoreSource();
            if (localSource != null && localSource.isSameSource(in.jobInfo.dbId, in.backupTable.id)
                    && clusterKnownEqual(localSource.getSrcClusterId(), in.jobInfo.getSrcClusterIdOrUnknown())) {
                return true;
            }
            return in.backupTableSource != null
                    && in.backupTableSource.isSameSource(in.localDbId, in.localTable.getId())
                    && clusterKnownEqual(in.backupTableSource.getSrcClusterId(), in.localClusterId);
        }
        return false;
    }

    /** What a condition needs to know about a partition to restore. */
    public static class Input {
        public boolean atomicRestore;
        // Atomic restore: why the data of the partition can not be made from the local tablets of the table being
        // replaced (the schema is changed, the staging tablets are not bound to the local ones, see
        // RestoreJob#checkAtomicBinding), null if it can.
        public String atomicReject;
        public boolean allowLoad;
        public boolean cloudMode;
        public long minPartitionBytes;
        public CheckLevel level;
        public BackupJobInfo jobInfo;
        public BackupOlapTableInfo backupTable;
        public BackupPartitionInfo backupPartition;
        public long srcCommitSeq;
        public long localDbId;
        // The cluster id of this cluster (Env#getClusterId), for the reverse checks.
        public long localClusterId = RestoreLineage.UNKNOWN_CLUSTER_ID;
        public OlapTable localTable;
        public Partition localPartition;
        // What the backup meta carried before it was cleared (see RestoreJob#clearRestoreLineageInBackupMeta), for
        // the reverse L0 checks. The source partition's own restore lineage, the source table's own restore source,
        // and the source table itself for the partition range. Null if the backup has none.
        public RestoreLineage backupLineage;
        public RestoreSource backupTableSource;
        public OlapTable backupOlapTable;
        // Set by checkL0: the L0 check that passed, null if none did.
        public String l0Path;
        // Set with l0Path: the cluster id of the source was compared on both sides and equal, see clusterProven.
        public boolean clusterProven;
        // The incremental append is allowed (the FE configs enable_restore_incremental_append and
        // enable_restore_partition_reuse), see firstRejectIncremental.
        public boolean incrementalEnabled;
        // The L0 checks look at the relation of the partitions only, not the versions: the local partition is
        // behind the backup. Only set while the incremental conditions are judged.
        public boolean relationOnly;
        private long singleReplicaBytes = -1;

        /** The local data size of a single replica of the partition, computed once. */
        public long getSingleReplicaBytes() {
            if (singleReplicaBytes < 0) {
                singleReplicaBytes = RestoreReuseShadowStats.getSingleReplicaLocalDataSize(localPartition);
            }
            return singleReplicaBytes;
        }
    }

    /** A condition of the chain: returns the reason that the partition is not a candidate, or null if satisfied. */
    public interface Condition {
        String check(Input in);
    }

    /** A way to pass L0, the partition passes if any of them does. */
    public abstract static class L0Check {
        final String name;

        L0Check(String name) {
            this.name = name;
        }

        abstract RestoreReuseShadowStats.L0Verdict check(Input in);
    }

    public static final String L0_FORWARD = "a";
    public static final String L0_REVERSE = "b";
    public static final String L0_TABLE = "c";

    /** a. forward: the local partition has the lineage of the source partition of this backup. */
    public static final L0Check FORWARD_L0 = new L0Check(L0_FORWARD) {
        @Override
        RestoreReuseShadowStats.L0Verdict check(Input in) {
            if (in.relationOnly) {
                return RestoreReuseShadowStats.checkL0Relation(in.localPartition, in.jobInfo.dbId,
                        in.backupTable.id, in.backupPartition.id, in.srcCommitSeq,
                        in.jobInfo.getSrcClusterIdOrUnknown());
            }
            return RestoreReuseShadowStats.checkL0(in.localPartition, in.jobInfo.dbId, in.backupTable.id,
                    in.backupPartition.id, in.backupPartition.version, in.srcCommitSeq,
                    in.jobInfo.getSrcClusterIdOrUnknown());
        }
    };

    /**
     * b. reverse: the source partition in the backup carries the lineage that points to this local partition, and
     * the versions are equal. Only used when a digest is computed.
     */
    public static final L0Check REVERSE_L0 = new L0Check(L0_REVERSE) {
        @Override
        RestoreReuseShadowStats.L0Verdict check(Input in) {
            if (in.level == CheckLevel.OFF || in.level == CheckLevel.DISABLE) {
                return RestoreReuseShadowStats.L0Verdict.LEVEL_NOT_ALLOWED;
            }
            if (in.localPartition == null) {
                return RestoreReuseShadowStats.L0Verdict.NO_LOCAL_PARTITION;
            }
            if (in.backupLineage == null) {
                return RestoreReuseShadowStats.L0Verdict.NO_BACKUP_LINEAGE;
            }
            if (!in.backupLineage.isSameSource(in.localDbId, in.localTable.getId(), in.localPartition.getId())) {
                return RestoreReuseShadowStats.L0Verdict.LINEAGE_MISMATCH;
            }
            if (clusterDiffers(in.backupLineage.getSrcClusterId(), in.localClusterId)) {
                return RestoreReuseShadowStats.L0Verdict.CLUSTER_MISMATCH;
            }
            if (!in.relationOnly && in.backupPartition.version != in.localPartition.getVisibleVersion()) {
                return RestoreReuseShadowStats.L0Verdict.VERSION_MISMATCH;
            }
            return RestoreReuseShadowStats.L0Verdict.REUSABLE;
        }
    };

    /**
     * c. table level relation: the local table and the backup table are the same replicated table (the source of one
     * is the other, in either direction), the partitions have the same name and range, and the versions are equal.
     * Only used when a digest is computed.
     */
    public static final L0Check TABLE_L0 = new L0Check(L0_TABLE) {
        @Override
        RestoreReuseShadowStats.L0Verdict check(Input in) {
            if (in.level == CheckLevel.OFF || in.level == CheckLevel.DISABLE) {
                return RestoreReuseShadowStats.L0Verdict.LEVEL_NOT_ALLOWED;
            }
            if (in.localPartition == null) {
                return RestoreReuseShadowStats.L0Verdict.NO_LOCAL_PARTITION;
            }
            RestoreSource localSource = in.localTable.getRestoreSource();
            boolean forward = localSource != null && localSource.isSameSource(in.jobInfo.dbId, in.backupTable.id);
            boolean reverse = in.backupTableSource != null
                    && in.backupTableSource.isSameSource(in.localDbId, in.localTable.getId());
            if (!forward && !reverse) {
                return RestoreReuseShadowStats.L0Verdict.NO_TABLE_RELATION;
            }
            // the source cluster: forward, the one the local table was restored from against the one of the backup;
            // reverse, the one the backup table was restored from against this cluster. Unknown is not a mismatch.
            forward = forward && !clusterDiffers(localSource.getSrcClusterId(),
                    in.jobInfo.getSrcClusterIdOrUnknown());
            reverse = reverse && !clusterDiffers(in.backupTableSource.getSrcClusterId(), in.localClusterId);
            if (!forward && !reverse) {
                return RestoreReuseShadowStats.L0Verdict.CLUSTER_MISMATCH;
            }
            if (!sameRange(in)) {
                return RestoreReuseShadowStats.L0Verdict.PARTITION_MISMATCH;
            }
            if (!in.relationOnly && in.backupPartition.version != in.localPartition.getVisibleVersion()) {
                return RestoreReuseShadowStats.L0Verdict.VERSION_MISMATCH;
            }
            return RestoreReuseShadowStats.L0Verdict.REUSABLE;
        }
    };

    // The partition name is the same by the way the local partition is found, compare the partition types and ranges
    // of the two tables. Unknown is not the same.
    private static boolean sameRange(Input in) {
        if (in.backupOlapTable == null) {
            return false;
        }
        Partition backupPart = in.backupOlapTable.getPartition(in.localPartition.getName(), false);
        if (backupPart == null) {
            return false;
        }
        PartitionInfo localInfo = in.localTable.getPartitionInfo();
        PartitionInfo backupInfo = in.backupOlapTable.getPartitionInfo();
        if (localInfo == null || backupInfo == null || localInfo.getType() != backupInfo.getType()) {
            return false;
        }
        PartitionItem localItem = localInfo.getItem(in.localPartition.getId());
        PartitionItem backupItem = backupInfo.getItem(backupPart.getId());
        return localItem != null && localItem.equals(backupItem);
    }

    /**
     * Whether the candidate of the L0 path must have its digest computed, whatever the sample says: the partitions
     * that came in by the reverse or the table level relation are only proved by the relation and the version,
     * which is weaker than the lineage stamp of the forward path.
     */
    public static boolean needsFullDigest(String l0Path, boolean forceFullForRelation) {
        return forceFullForRelation && (L0_REVERSE.equals(l0Path) || L0_TABLE.equals(l0Path));
    }

    public static final List<L0Check> L0_CHECKS = ImmutableList.of(FORWARD_L0, REVERSE_L0, TABLE_L0);

    /**
     * The first L0 check that passes, null if none does. At the check level off only the forward check is used.
     *
     * @param first if not null, [0] receives the verdict of the forward check
     */
    static String passedL0(Input in, RestoreReuseShadowStats.L0Verdict[] first) {
        for (L0Check check : L0_CHECKS) {
            RestoreReuseShadowStats.L0Verdict verdict = check.check(in);
            if (first != null && first[0] == null) {
                first[0] = verdict;
            }
            if (verdict == RestoreReuseShadowStats.L0Verdict.REUSABLE) {
                return check.name;
            }
        }
        return null;
    }

    public static final List<Condition> CONDITIONS = ImmutableList.of(
            RestoreReuseJudge::checkMode,
            RestoreReuseJudge::checkL0,
            RestoreReuseJudge::checkSupported,
            RestoreReuseJudge::checkDigestPresent,
            RestoreReuseJudge::checkSize,
            RestoreReuseJudge::checkReplicas);

    /** Returns the reason that the partition is not a candidate, null if it is. */
    public static String firstReject(Input in) {
        for (Condition condition : CONDITIONS) {
            String reason = condition.check(in);
            if (reason != null) {
                return reason;
            }
        }
        return null;
    }

    // 1. non-atomic restore into an existing table that is forbidden to write, or an atomic restore whose staging
    // tablets are bound to the local ones, to a table without schema change.
    static String checkMode(Input in) {
        if (in.atomicRestore) {
            if (!Config.enable_restore_atomic_reuse) {
                return "ATOMIC_RESTORE";
            }
            if (in.allowLoad) {
                return "ALLOW_LOAD";
            }
            if (in.atomicReject != null) {
                return in.atomicReject;
            }
            // the table being replaced is not forbidden to write, the version is checked before it is replaced
            if (in.localTable.getState() != OlapTableState.NORMAL) {
                return "TABLE_STATE_" + in.localTable.getState().name();
            }
            return null;
        }
        if (in.allowLoad) {
            // the version may change after the judgement
            return "ALLOW_LOAD";
        }
        if (in.localTable.getState() != OlapTableState.RESTORE) {
            return "TABLE_STATE_" + in.localTable.getState().name();
        }
        return null;
    }

    // 2. L0: the lineage and the versions.
    static String checkL0(Input in) {
        RestoreReuseShadowStats.L0Verdict[] first = new RestoreReuseShadowStats.L0Verdict[1];
        in.l0Path = passedL0(in, first);
        if (in.l0Path != null) {
            in.clusterProven = clusterProven(in, in.l0Path);
            return null;
        }
        return "L0_" + (first[0] == null ? "FAILED" : first[0].name());
    }

    // 3. cases that the first version does not cover.
    static String checkSupported(Input in) {
        if (in.cloudMode) {
            return "CLOUD_MODE";
        }
        RestoreReuseShadowStats.Unsupported unsupported = RestoreReuseShadowStats.checkUnsupported(
                in.atomicRestore, in.localTable, in.localPartition);
        return unsupported == RestoreReuseShadowStats.Unsupported.NONE ? null : unsupported.name();
    }

    // 4. every tablet of the backup partition has a digest, matching the local tablets one to one. Not needed
    // if no digest is computed.
    static String checkDigestPresent(Input in) {
        if (in.level == CheckLevel.OFF) {
            return null;
        }
        for (MaterializedIndex localIdx : in.localPartition.getMaterializedIndices(IndexExtState.VISIBLE)) {
            BackupIndexInfo backupIdx = in.backupPartition.getIdx(in.localTable.getIndexNameById(localIdx.getId()));
            if (backupIdx == null || backupIdx.sortedTabletInfoList.size() != localIdx.getTablets().size()) {
                return "INDEX_MISMATCH";
            }
            for (BackupTabletInfo backupTablet : backupIdx.sortedTabletInfoList) {
                LogicalDigestInfo digest = in.jobInfo.getLogicalDigest(backupTablet.id);
                if (digest == null || !digest.hasDigest()) {
                    return "NO_DIGEST";
                }
            }
        }
        return null;
    }

    // 5. small partitions are cheaper to download.
    static String checkSize(Input in) {
        return in.getSingleReplicaBytes() >= in.minPartitionBytes ? null : "TOO_SMALL";
    }

    // 6. every replica is healthy and has the version of the backup, so the digest of each is computed on that
    // version, and the replicas are the same as after a download.
    static String checkReplicas(Input in) {
        return replicasUnhealthyAt(in.localPartition, in.backupPartition.version);
    }

    private static String replicasUnhealthyAt(Partition localPartition, long version) {
        for (MaterializedIndex index : localPartition.getMaterializedIndices(IndexExtState.VISIBLE)) {
            for (Tablet tablet : index.getTablets()) {
                if (tablet.getReplicas().isEmpty()) {
                    return "REPLICA_UNHEALTHY";
                }
                for (Replica replica : tablet.getReplicas()) {
                    if (!isReplicaHealthyAt(replica, version)) {
                        return "REPLICA_UNHEALTHY";
                    }
                }
            }
        }
        return null;
    }

    // ---- incremental append ----

    /**
     * The conditions of the incremental append, cheap ones first: the local partition is behind the partition in the
     * backup, is related to it (any of the L0 checks, versions aside), and every replica is at the same version, so
     * the digest of the local data at that version can be compared with the backup. Whether the data is the same
     * and the backup can be cut there is proved by the digest tasks, not here.
     */
    public static final List<Condition> INCREMENTAL_CONDITIONS = ImmutableList.of(
            RestoreReuseJudge::checkIncrementalEnabled,
            RestoreReuseJudge::checkMode,
            RestoreReuseJudge::checkIncrementalLevel,
            RestoreReuseJudge::checkIncrementalRelation,
            RestoreReuseJudge::checkSupported,
            RestoreReuseJudge::checkIncrementalModel,
            RestoreReuseJudge::checkDigestPresent,
            RestoreReuseJudge::checkIncrementalSources,
            RestoreReuseJudge::checkSize,
            RestoreReuseJudge::checkReplicasAtLocalVersion);

    /** Returns the reason that the partition is not a candidate of the incremental append, null if it is. */
    public static String firstRejectIncremental(Input in) {
        boolean saved = in.relationOnly;
        in.relationOnly = true;
        try {
            for (Condition condition : INCREMENTAL_CONDITIONS) {
                String reason = condition.check(in);
                if (reason != null) {
                    return reason;
                }
            }
            return null;
        } finally {
            in.relationOnly = saved;
        }
    }

    static String checkIncrementalEnabled(Input in) {
        return in.incrementalEnabled ? null : "INCREMENTAL_DISABLED";
    }

    // The digest is the only proof of the local data, so it is not for the level off and disable.
    static String checkIncrementalLevel(Input in) {
        if (in.level == CheckLevel.OFF || in.level == CheckLevel.DISABLE) {
            return "INCREMENTAL_LEVEL_" + in.level.name();
        }
        return null;
    }

    // The relation by L0 (versions aside), and the local partition is behind the backup.
    static String checkIncrementalRelation(Input in) {
        RestoreReuseShadowStats.L0Verdict[] first = new RestoreReuseShadowStats.L0Verdict[1];
        in.l0Path = passedL0(in, first);
        if (in.l0Path == null) {
            return "L0_" + (first[0] == null ? "FAILED" : first[0].name());
        }
        in.clusterProven = clusterProven(in, in.l0Path);
        long localVersion = in.localPartition.getVisibleVersion();
        if (localVersion >= in.backupPartition.version) {
            return localVersion == in.backupPartition.version ? "INCREMENTAL_SAME_VERSION"
                    : "INCREMENTAL_LOCAL_AHEAD";
        }
        return null;
    }

    // Duplicate and unique (merge-on-write and merge-on-read): a duplicate or merge-on-write digest is composed by
    // rowset, a merge-on-read one is carried by the whole digests the backup keeps at its most recent rowset
    // boundaries (restore_digest_mor_prefix_boundaries), so a local version outside them is no boundary and the
    // partition is downloaded whole (the BE reports NOT_BOUNDARY). Whether the backup has a decomposed digest at all
    // is checkDigestPresent / checkIncrementalSources.
    static String checkIncrementalModel(Input in) {
        KeysType keysType = in.localTable.getKeysType();
        // the unique tables have a hidden state that the digest does not cover, see checkSupported
        RestoreReuseShadowStats.Unsupported hidden = RestoreReuseShadowStats.checkHiddenState(in.localTable);
        if (hidden != RestoreReuseShadowStats.Unsupported.NONE) {
            return hidden.name();
        }
        if (keysType == KeysType.DUP_KEYS || keysType == KeysType.UNIQUE_KEYS) {
            // A table with binlog is fine: a restore never carries the binlog of the snapshot, the whole download
            // does not either.
            return null;
        }
        return "INCREMENTAL_MODEL_" + keysType.name();
    }

    // The backup has the manifest (to download only some files and check them) and the decomposed digest file
    // (to compose the digest at the local version) of every tablet.
    static String checkIncrementalSources(Input in) {
        if (!in.jobInfo.hasManifest()) {
            return "NO_MANIFEST";
        }
        for (MaterializedIndex localIdx : in.localPartition.getMaterializedIndices(IndexExtState.VISIBLE)) {
            BackupIndexInfo backupIdx = in.backupPartition.getIdx(in.localTable.getIndexNameById(localIdx.getId()));
            if (backupIdx == null) {
                return "INDEX_MISMATCH";
            }
            for (BackupTabletInfo backupTablet : backupIdx.sortedTabletInfoList) {
                if (backupTablet.manifestRoot == null || backupTablet.manifestRoot.isEmpty()) {
                    return "NO_MANIFEST";
                }
                if (backupTablet.prefixDigestRoot == null || backupTablet.prefixDigestRoot.isEmpty()) {
                    return "NO_PREFIX_DIGEST";
                }
            }
        }
        return null;
    }

    // Every replica is healthy and at the visible version of the local partition, the version of the increment.
    static String checkReplicasAtLocalVersion(Input in) {
        return replicasUnhealthyAt(in.localPartition, in.localPartition.getVisibleVersion());
    }

    public static boolean isReplicaHealthyAt(Replica replica, long version) {
        return replica.getState() == ReplicaState.NORMAL && !replica.isBad() && replica.getVersion() == version
                && replica.getLastFailedVersion() < 0;
    }

    /**
     * Pick the partitions to compute the digest for in the sample check level: the ratio of them, at least one.
     *
     * @return a new list of the picked elements, empty if the pool is empty
     */
    public static <T> List<T> pickSample(List<T> pool, double ratio, Random random) {
        if (pool.isEmpty()) {
            return Lists.newArrayList();
        }
        int num = (int) Math.ceil(pool.size() * Math.max(0, Math.min(1, ratio)));
        num = Math.max(1, Math.min(pool.size(), num));
        List<T> shuffled = Lists.newArrayList(pool);
        Collections.shuffle(shuffled, random);
        return Lists.newArrayList(shuffled.subList(0, num));
    }

    /**
     * Compare the digest of a tablet in the backup with the digests of its local replicas. Consistent only if the
     * algorithm version and schema signature are the same as the backup, and the root of every replica is the same
     * as the backup.
     *
     * @param expected the digest in the backup
     * @param replicaDigests the digest of each local replica, null for a replica without a result
     * @return null if consistent, otherwise the reason (a DOWNLOAD_ constant of {@link RestoreReuseResult})
     */
    public static String compareTablet(LogicalDigestInfo expected, List<LogicalDigestInfo> replicaDigests) {
        if (expected == null || !expected.hasDigest() || replicaDigests.isEmpty()) {
            return RestoreReuseResult.DOWNLOAD_DIGEST_ERROR;
        }
        String reason = null;
        for (LogicalDigestInfo actual : replicaDigests) {
            if (actual == null || !actual.hasDigest()) {
                return RestoreReuseResult.DOWNLOAD_DIGEST_ERROR;
            }
            if (!expected.algoVersion.equals(actual.algoVersion) || !expected.schemaSig.equals(actual.schemaSig)) {
                reason = RestoreReuseResult.DOWNLOAD_SCHEMA_MISMATCH;
            } else if (!expected.root.equalsIgnoreCase(actual.root) && reason == null) {
                reason = RestoreReuseResult.DOWNLOAD_ROOT_MISMATCH;
            }
        }
        return reason;
    }

    /**
     * Judge the digest tasks of a tablet of the incremental append: every local replica must report a digest and the
     * verdict OK of the comparison with the decomposed digest of the backup at its version.
     *
     * @param replicaDigests the digest reported by each local replica, null for a replica without a result
     * @return null if every replica passed, otherwise the reason (a DOWNLOAD_ constant of {@link RestoreReuseResult})
     */
    public static String compareIncrementalTablet(List<LogicalDigestInfo> replicaDigests) {
        if (replicaDigests.isEmpty()) {
            return RestoreReuseResult.DOWNLOAD_DIGEST_ERROR;
        }
        String reason = null;
        for (LogicalDigestInfo actual : replicaDigests) {
            if (actual == null || !actual.hasDigest() || actual.prefixVerdict == null) {
                // an old backend, a failed task, an unsupported tablet
                return RestoreReuseResult.DOWNLOAD_DIGEST_ERROR;
            }
            String one;
            switch (actual.prefixVerdict) {
                case LogicalDigestInfo.PREFIX_OK:
                    one = null;
                    break;
                case LogicalDigestInfo.PREFIX_NOT_BOUNDARY:
                    one = RestoreReuseResult.DOWNLOAD_NOT_BOUNDARY;
                    break;
                case LogicalDigestInfo.PREFIX_MISMATCH:
                    one = RestoreReuseResult.DOWNLOAD_ROOT_MISMATCH;
                    break;
                case LogicalDigestInfo.PREFIX_SCHEMA_MISMATCH:
                    one = RestoreReuseResult.DOWNLOAD_SCHEMA_MISMATCH;
                    break;
                default:
                    one = RestoreReuseResult.DOWNLOAD_PREFIX_ERROR;
                    break;
            }
            if (one != null && reason == null) {
                reason = one;
            }
        }
        return reason;
    }
}
