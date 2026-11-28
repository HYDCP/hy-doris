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
 * The table level source of a table that a RESTORE job wrote: the db id and the table id in the source cluster of the
 * backup. It tells that the table is a replica of the source table (as with a table synchronized by ccr-syncer), so
 * that partitions of the two with the same name and range may be the same data even though the partition lineage
 * (see {@link RestoreLineage}) can not tell, e.g. after the increments of a synchronization.
 *
 * <p>Like the lineage, it is always rebuilt from the {@code BackupJobInfo} of the restore job that writes it, and is
 * never inherited from the backup meta, which carries the source of the source table. The object is immutable. The
 * serialized names are part of the persisted metadata and must not be changed.
 */
public class RestoreSource {
    @SerializedName("db")
    private long srcDbId;
    @SerializedName("tbl")
    private long srcTableId;
    // The cluster id of the source cluster, null (not written) if unknown. No proof, see RestoreLineage.
    @SerializedName("cid")
    private Long srcClusterId;

    // For gson.
    private RestoreSource() {
    }

    public RestoreSource(long srcDbId, long srcTableId) {
        this(srcDbId, srcTableId, RestoreLineage.UNKNOWN_CLUSTER_ID);
    }

    public RestoreSource(long srcDbId, long srcTableId, long srcClusterId) {
        this.srcDbId = srcDbId;
        this.srcTableId = srcTableId;
        this.srcClusterId = srcClusterId == RestoreLineage.UNKNOWN_CLUSTER_ID ? null : srcClusterId;
    }

    public long getSrcClusterId() {
        return srcClusterId == null ? RestoreLineage.UNKNOWN_CLUSTER_ID : srcClusterId;
    }

    public long getSrcDbId() {
        return srcDbId;
    }

    public long getSrcTableId() {
        return srcTableId;
    }

    public boolean isSameSource(long dbId, long tableId) {
        return srcDbId == dbId && srcTableId == tableId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof RestoreSource)) {
            return false;
        }
        RestoreSource that = (RestoreSource) o;
        return srcDbId == that.srcDbId && srcTableId == that.srcTableId
                && Objects.equals(srcClusterId, that.srcClusterId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(srcDbId, srcTableId, srcClusterId);
    }

    @Override
    public String toString() {
        return "RestoreSource{srcDbId=" + srcDbId + ", srcTableId=" + srcTableId
                + ", srcClusterId=" + getSrcClusterId() + "}";
    }
}
