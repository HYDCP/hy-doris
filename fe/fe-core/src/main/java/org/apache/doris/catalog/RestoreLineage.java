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

package org.apache.doris.catalog;

import com.google.gson.annotations.SerializedName;

import java.util.Objects;

/**
 * The lineage of the data in a partition that was written by a RESTORE job.
 *
 * <p>It records which backup the partition data came from: the partition in the source cluster
 * (db id, table id, partition id), the version the partition was restored to (which is the
 * visible version of the source partition when the backup was taken), the commit seq of the
 * source table when the backup was taken, the backup time, and the time the restore job committed.
 *
 * <p>It is always rebuilt from the {@code BackupJobInfo} of the restore job that writes it, and is
 * never inherited from the backup meta: the backup meta is a deep copy of the source table, so it
 * carries the lineage of the source partitions, which describes the source's own upstream.
 *
 * <p>The object is immutable. The serialized names are part of the persisted metadata and must
 * not be changed.
 */
public class RestoreLineage {
    public static final long UNKNOWN_COMMIT_SEQ = -1L;
    // The cluster id of the source cluster is unknown (a stamp written before it was recorded, or an old backup).
    public static final long UNKNOWN_CLUSTER_ID = -1L;

    // The db id of the source partition.
    @SerializedName("db")
    private long srcDbId;
    // The table id of the source partition.
    @SerializedName("tbl")
    private long srcTableId;
    // The id of the source partition.
    @SerializedName("part")
    private long srcPartitionId;
    // The version the partition was restored to, equals the source partition's visible version in the backup.
    @SerializedName("ver")
    private long srcVersion;
    // The commit seq of the source table when the backup was taken, UNKNOWN_COMMIT_SEQ if unknown.
    @SerializedName("seq")
    private long srcCommitSeq = UNKNOWN_COMMIT_SEQ;
    // The backup time (ms) of the backup, which together with the label identifies the snapshot.
    @SerializedName("bt")
    private long backupTime;
    // The time (ms) the restore job committed the partition. Unlike the visible version time, which is inherited
    // from the source for newly created partitions, it always means the time of the restore in this cluster.
    @SerializedName("rt")
    private long restoreTime;
    // The cluster id of the source cluster of the backup, null (not written) if unknown. It is no proof (a cluster id
    // is not unique), it only narrows the candidates.
    @SerializedName("cid")
    private Long srcClusterId;

    // For gson.
    private RestoreLineage() {
    }

    public RestoreLineage(long srcDbId, long srcTableId, long srcPartitionId, long srcVersion,
            long srcCommitSeq, long backupTime, long restoreTime) {
        this(srcDbId, srcTableId, srcPartitionId, srcVersion, srcCommitSeq, backupTime, restoreTime,
                UNKNOWN_CLUSTER_ID);
    }

    public RestoreLineage(long srcDbId, long srcTableId, long srcPartitionId, long srcVersion,
            long srcCommitSeq, long backupTime, long restoreTime, long srcClusterId) {
        this.srcClusterId = srcClusterId == UNKNOWN_CLUSTER_ID ? null : srcClusterId;
        this.srcDbId = srcDbId;
        this.srcTableId = srcTableId;
        this.srcPartitionId = srcPartitionId;
        this.srcVersion = srcVersion;
        this.srcCommitSeq = srcCommitSeq;
        this.backupTime = backupTime;
        this.restoreTime = restoreTime;
    }

    public long getSrcDbId() {
        return srcDbId;
    }

    public long getSrcTableId() {
        return srcTableId;
    }

    public long getSrcPartitionId() {
        return srcPartitionId;
    }

    public long getSrcVersion() {
        return srcVersion;
    }

    public long getSrcCommitSeq() {
        return srcCommitSeq;
    }

    public long getBackupTime() {
        return backupTime;
    }

    public long getRestoreTime() {
        return restoreTime;
    }

    /** The cluster id of the source cluster, {@link #UNKNOWN_CLUSTER_ID} if unknown. */
    public long getSrcClusterId() {
        return srcClusterId == null ? UNKNOWN_CLUSTER_ID : srcClusterId;
    }

    /**
     * Whether this lineage points to the given source partition.
     */
    public boolean isSameSource(long dbId, long tableId, long partitionId) {
        return srcDbId == dbId && srcTableId == tableId && srcPartitionId == partitionId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof RestoreLineage)) {
            return false;
        }
        RestoreLineage that = (RestoreLineage) o;
        return srcDbId == that.srcDbId && srcTableId == that.srcTableId && srcPartitionId == that.srcPartitionId
                && srcVersion == that.srcVersion && srcCommitSeq == that.srcCommitSeq
                && backupTime == that.backupTime && restoreTime == that.restoreTime
                && Objects.equals(srcClusterId, that.srcClusterId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(srcDbId, srcTableId, srcPartitionId, srcVersion, srcCommitSeq, backupTime,
                restoreTime, srcClusterId);
    }

    @Override
    public String toString() {
        return "RestoreLineage{srcDbId=" + srcDbId + ", srcTableId=" + srcTableId
                + ", srcPartitionId=" + srcPartitionId + ", srcVersion=" + srcVersion
                + ", srcCommitSeq=" + srcCommitSeq + ", backupTime=" + backupTime
                + ", restoreTime=" + restoreTime + ", srcClusterId=" + getSrcClusterId() + "}";
    }
}
