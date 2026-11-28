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
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Index;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.MaterializedIndex.IndexExtState;
import org.apache.doris.catalog.MaterializedIndexMeta;
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
import com.google.common.collect.Maps;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.Set;

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
        // The first node which does not report the capability that the reuse of a unique merge-on-write table needs
        // (RestoreJob#RESTORE_REUSE_CAPABILITY_MOW), null if every node does. Only read for such a table.
        public String mowIncapableNode;
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
        // The effective state token of the local partition and the one the backup recorded for the partition, see
        // PartitionStateToken. Null if there is none.
        public String localStateToken;
        public String backupStateToken;
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

    // The algorithm version of the logical digest of a unique merge-on-write tablet (BE restore_digest.h).
    public static final int MOW_DIGEST_ALGO_VERSION = 2;

    public static final String L0_FORWARD = "a";
    public static final String L0_REVERSE = "b";
    public static final String L0_TABLE = "c";
    // Not an L0 check: the partition is kept because its state token equals the one of the backup partition.
    public static final String L0_TOKEN = "t";

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

    /**
     * The conditions of a keep proved by the state token instead of a digest (Config#restore_reuse_trust_state_token):
     * the same as the chain above with the state token in place of L0 and no digest needed.
     */
    public static final List<Condition> TOKEN_CONDITIONS = ImmutableList.of(
            RestoreReuseJudge::checkMode,
            RestoreReuseJudge::checkToken,
            RestoreReuseJudge::checkSupported,
            RestoreReuseJudge::checkSize,
            RestoreReuseJudge::checkReplicas);

    /** Returns the reason that the partition can not be kept by its state token, null if it can. */
    public static String firstRejectByToken(Input in) {
        for (Condition condition : TOKEN_CONDITIONS) {
            String reason = condition.check(in);
            if (reason != null) {
                return reason;
            }
        }
        return null;
    }

    /** Whether the local partition and the backup partition have the same effective state token. */
    public static boolean tokenEqual(Input in) {
        return in.localStateToken != null && in.localStateToken.equals(in.backupStateToken);
    }

    // the state token, the version and the structure of the partition are the same as the backup
    static String checkToken(Input in) {
        if (!tokenEqual(in)) {
            if (in.backupStateToken == null) {
                return "TOKEN_ABSENT_BACKUP";
            }
            return in.localStateToken == null ? "TOKEN_ABSENT_LOCAL" : "TOKEN_MISMATCH";
        }
        if (in.backupPartition.version != in.localPartition.getVisibleVersion()) {
            return "TOKEN_VERSION_MISMATCH";
        }
        String structure = checkSameStructure(in);
        if (structure != null) {
            return structure;
        }
        in.l0Path = L0_TOKEN;
        in.clusterProven = true;
        return null;
    }

    /**
     * Without a digest the schema signature in it does not compare the schemas, so compare them here. The state
     * token says that the data of the two partitions is the same; this is a second line against a path that changed
     * the data or what it is read as and missed the invalidation, and it covers the changes that are not meant to
     * invalidate (a light schema change). The kept files are read by the metadata of the backup after an atomic
     * restore, and keep the local metadata after a non-atomic one, so everything the reading of the files depends
     * on must be the same on both sides:
     * <ul>
     * <li>the model (keys type, merge-on-write, sequence column): how the rows of the files are merged;</li>
     * <li>the range: which rows the partition stands for;</li>
     * <li>the distribution (type, bucket columns, bucket number): which rows a tablet holds, the bucket pruning of
     * a query relies on it;</li>
     * <li>the visible indexes (by name) and the tablets of each index;</li>
     * <li>the short key column count of each index: the short key index of the files is built with it, and a seek
     * by the short keys reads it;</li>
     * <li>the definition of each materialized index (its define statement, which holds the define expressions of
     * its columns and its WHERE): the index has the rows the definition made, and a query rewritten to the index
     * trusts the definition;</li>
     * <li>every column of each index, see sameColumn;</li>
     * <li>the indexes of the table (inverted, ngram bloom filter, ...), see sameIndexes;</li>
     * <li>the bloom filter columns and their fpp: the bloom filter in the files is built by them. A bloom filter
     * stores its own parameters, so a difference is not known to change a result: it is compared to keep the kept
     * files the same as the files a download would make, at the cost of a download after a change of them.</li>
     * </ul>
     */
    static String checkSameStructure(Input in) {
        OlapTable local = in.localTable;
        OlapTable backup = in.backupOlapTable;
        if (backup == null) {
            return "STRUCTURE_UNKNOWN";
        }
        if (local.getKeysType() != backup.getKeysType() || isMow(local) != isMow(backup)
                || local.hasSequenceCol().booleanValue() != backup.hasSequenceCol().booleanValue()) {
            return "STRUCTURE_MODEL";
        }
        if (!sameRange(in)) {
            return "STRUCTURE_RANGE";
        }
        // sameRange found the partition of the same name in the backup
        Partition backupPart = backup.getPartition(in.localPartition.getName(), false);
        if (!in.localPartition.getDistributionInfo().equals(backupPart.getDistributionInfo())) {
            return "STRUCTURE_DISTRIBUTION";
        }
        if (!sameIndexes(local.getIndexes(), backup.getIndexes())) {
            return "STRUCTURE_TABLE_INDEX";
        }
        if (!sameBloomFilter(local, backup)) {
            return "STRUCTURE_BLOOM_FILTER";
        }
        // the indexes and the tablets of the backup are those of the job info: the partitions of the backup meta
        // table are reset by the restore and do not tell them
        List<MaterializedIndex> localIndexes = in.localPartition.getMaterializedIndices(IndexExtState.VISIBLE);
        if (localIndexes.size() != in.backupPartition.indexes.size()) {
            return "STRUCTURE_INDEX_COUNT";
        }
        for (MaterializedIndex localIdx : localIndexes) {
            String name = local.getIndexNameById(localIdx.getId());
            // the base index is named by the table, which an atomic restore renames to the temp name
            Long backupIdxId = localIdx.getId() == local.getBaseIndexId() ? Long.valueOf(backup.getBaseIndexId())
                    : backup.getIndexIdByName(name);
            BackupIndexInfo backupIdx = in.backupPartition.getIdx(name);
            if (backupIdxId == null || backupIdx == null) {
                return "STRUCTURE_INDEX_NAME";
            }
            if (backupIdx.sortedTabletInfoList.size() != localIdx.getTablets().size()) {
                return "STRUCTURE_INDEX_TABLETS";
            }
            MaterializedIndexMeta localMeta = local.getIndexMetaByIndexId(localIdx.getId());
            MaterializedIndexMeta backupMeta = backup.getIndexMetaByIndexId(backupIdxId);
            if (localMeta.getShortKeyColumnCount() != backupMeta.getShortKeyColumnCount()
                    || !Objects.equals(defineStmtOf(localMeta), defineStmtOf(backupMeta))) {
                return "STRUCTURE_INDEX_DEFINE";
            }
            List<Column> localCols = local.getSchemaByIndexId(localIdx.getId(), true);
            List<Column> backupCols = backup.getSchemaByIndexId(backupIdxId, true);
            if (localCols == null || backupCols == null || localCols.size() != backupCols.size()) {
                return "STRUCTURE_COLUMN";
            }
            for (int i = 0; i < localCols.size(); i++) {
                if (!sameColumn(localCols.get(i), backupCols.get(i))) {
                    return "STRUCTURE_COLUMN";
                }
            }
        }
        return null;
    }

    /**
     * Column#equals, but the comment, with the unique id and the exact name. Column#equals compares what the data
     * of the files is read as: the type, key, nullable, aggregation, the default value and the real default value
     * (a file written before the column was added by a light schema change is read with it), auto increment, the
     * visibility (a hidden column), the sub columns of a complex type and the cluster key id (the sort order of a
     * merge-on-write table). The comment is metadata only, a change of it must not break the reuse. The unique id is
     * how the files name the column. Not compared, as they only matter to the rows written later, which the restore
     * does not keep: the expression of a generated column and the ON UPDATE default.
     */
    static boolean sameColumn(Column local, Column backup) {
        Column backupWithLocalComment = new Column(backup);
        backupWithLocalComment.setComment(local.getComment());
        return local.getName().equals(backup.getName()) && local.getUniqueId() == backup.getUniqueId()
                && local.equals(backupWithLocalComment);
    }

    /**
     * The indexes of the table by name: the id (an inverted index file is named by it, and the BE finds the index
     * of a kept file by the id of the restored metadata), the type, the columns and the properties (the parser of an
     * inverted index decides what a MATCH finds, the gram size of an ngram bloom filter what it filters). Not the
     * comment.
     */
    static boolean sameIndexes(List<Index> local, List<Index> backup) {
        if (local.size() != backup.size()) {
            return false;
        }
        Map<String, Index> backupByName = Maps.newHashMap();
        for (Index index : backup) {
            backupByName.put(index.getIndexName(), index);
        }
        for (Index a : local) {
            Index b = backupByName.get(a.getIndexName());
            if (b == null || a.getIndexId() != b.getIndexId() || a.getIndexType() != b.getIndexType()
                    || !a.getColumns().equals(b.getColumns()) || !a.getProperties().equals(b.getProperties())) {
                return false;
            }
        }
        return true;
    }

    // the bloom filter columns, and the fpp if there are any
    private static boolean sameBloomFilter(OlapTable local, OlapTable backup) {
        Set<String> localBf = local.getCopiedBfColumns();
        Set<String> backupBf = backup.getCopiedBfColumns();
        boolean localHas = localBf != null && !localBf.isEmpty();
        boolean backupHas = backupBf != null && !backupBf.isEmpty();
        if (localHas != backupHas) {
            return false;
        }
        return !localHas || (localBf.equals(backupBf) && local.getBfFpp() == backup.getBfFpp());
    }

    // the define statement of a materialized index, null for the base index and an index without one
    private static String defineStmtOf(MaterializedIndexMeta meta) {
        return meta.getDefineStmt() == null ? null : meta.getDefineStmt().originStmt;
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
        if (unsupported != RestoreReuseShadowStats.Unsupported.NONE) {
            return unsupported.name();
        }
        // a node of version 1 would report the digest of algorithm version 1 for a merge-on-write table, which is not
        // comparable: only that table is kept off, the duplicate tables are not affected
        if (isMow(in.localTable) && in.mowIncapableNode != null) {
            return RestoreJob.MIXED_VERSIONS;
        }
        return null;
    }

    static boolean isMow(OlapTable table) {
        return table.getKeysType() == KeysType.UNIQUE_KEYS && table.getEnableUniqueKeyMergeOnWrite();
    }

    /**
     * Whether the digest of a tablet of a unique merge-on-write table in the backup is of an algorithm version below
     * 2. It skips the rows with a delete sign, so it does not prove the behavior of the later writes, and is never
     * trusted. Whether a digest is there at all is checkDigestPresent.
     */
    static boolean hasMowDigestV1(Input in) {
        if (!isMow(in.localTable)) {
            return false;
        }
        for (MaterializedIndex localIdx : in.localPartition.getMaterializedIndices(IndexExtState.VISIBLE)) {
            BackupIndexInfo backupIdx = in.backupPartition.getIdx(in.localTable.getIndexNameById(localIdx.getId()));
            if (backupIdx == null) {
                continue;
            }
            for (int i = 0; i < backupIdx.sortedTabletInfoList.size(); i++) {
                LogicalDigestInfo digest = backupIdx.getLogicalDigestAt(i);
                if (digest != null && digest.hasDigest()
                        && (digest.algoVersion == null || digest.algoVersion < MOW_DIGEST_ALGO_VERSION)) {
                    return true;
                }
            }
        }
        return false;
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
            for (int i = 0; i < backupIdx.sortedTabletInfoList.size(); i++) {
                LogicalDigestInfo digest = backupIdx.getLogicalDigestAt(i);
                if (digest == null || !digest.hasDigest()) {
                    String skipReason = in.jobInfo.logicalDigestSkipReason;
                    if (digest == null && skipReason != null && skipReason.startsWith(RestoreJob.TOO_MANY_TABLETS)) {
                        // the backup did not record the digest because it has too many tablets
                        return "NO_DIGEST_TOO_MANY_TABLETS";
                    }
                    return "NO_DIGEST";
                }
            }
        }
        if (hasMowDigestV1(in)) {
            return "HIDDEN_STATE_MOW_DIGEST_V1";
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
            for (int i = 0; i < backupIdx.sortedTabletInfoList.size(); i++) {
                if (backupIdx.getManifestRootAt(i) == null) {
                    return "NO_MANIFEST";
                }
                if (!backupIdx.hasPrefixDigestAt(i)) {
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
