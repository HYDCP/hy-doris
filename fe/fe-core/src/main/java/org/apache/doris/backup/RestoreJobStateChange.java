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

import org.apache.doris.backup.RestoreJob.RestoreJobState;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.Resource;
import org.apache.doris.catalog.Table;
import org.apache.doris.common.Config;
import org.apache.doris.common.Pair;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.persist.gson.GsonUtils;

import com.google.gson.annotations.JsonAdapter;
import com.google.gson.annotations.SerializedName;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * The journal of a restore job entering a persisted state, carrying only what changed since the
 * previous journal of the job.
 *
 * OP_RESTORE_JOB writes the whole job on every persisted state, so the job info is written three
 * times and the restored tables, snapshot infos and file mapping twice. With this journal:
 *
 *   creation   OP_RESTORE_JOB        the whole job (job info, backup meta)
 *   DOWNLOAD   OP_RESTORE_JOB_STATE  what checkAndPrepareMeta and the snapshot tasks produced
 *   COMMIT     OP_RESTORE_JOB_STATE  the state and timestamps only
 *   FINISHED / CANCELLED  OP_RESTORE_JOB  the whole job, which has released its metadata by then
 *
 * A follower applies it to the job it already holds, see RestoreJob.replayStateChange.
 */
public class RestoreJobStateChange implements Writable {
    // dbId and label locate the job. jobId can't, it is only assigned in checkAndPrepareMeta.
    @SerializedName("dbid")
    long dbId;
    @SerializedName("l")
    String label;

    @SerializedName("st")
    RestoreJobState state;

    @SerializedName("jid")
    long jobId;
    @SerializedName("mpt")
    long metaPreparedTime;
    @SerializedName("sft")
    long snapshotFinishedTime;
    @SerializedName("dft")
    long downloadFinishedTime;
    @SerializedName("msg")
    Map<Long, String> taskErrMsg;

    // The fields below are produced between the creation and the DOWNLOAD state, and are only set for DOWNLOAD.
    // They are declared as in RestoreJob, to be written and read by the same adapters.
    @SerializedName("meta")
    BackupMeta backupMeta;
    @SerializedName("fm")
    RestoreFileMapping fileMapping;
    @SerializedName("rp")
    List<Pair<String, Partition>> restoredPartitions;
    @SerializedName("rt")
    List<Table> restoredTbls;
    @SerializedName("rr")
    List<Resource> restoredResources;
    @SerializedName("rvi")
    @JsonAdapter(GsonUtils.GuavaTableTypeAdapterFactory.class)
    com.google.common.collect.Table<Long, Long, Long> restoredVersionInfo;
    @SerializedName("si")
    @JsonAdapter(GsonUtils.GuavaTableTypeAdapterFactory.class)
    com.google.common.collect.Table<Long, Long, SnapshotInfo> snapshotInfos;

    RestoreJobStateChange(long dbId, String label, RestoreJobState state, long jobId, long metaPreparedTime,
            long snapshotFinishedTime, long downloadFinishedTime, Map<Long, String> taskErrMsg) {
        this.dbId = dbId;
        this.label = label;
        this.state = state;
        this.jobId = jobId;
        this.metaPreparedTime = metaPreparedTime;
        this.snapshotFinishedTime = snapshotFinishedTime;
        this.downloadFinishedTime = downloadFinishedTime;
        this.taskErrMsg = taskErrMsg;
    }

    public long getDbId() {
        return dbId;
    }

    public String getLabel() {
        return label;
    }

    public RestoreJobState getState() {
        return state;
    }

    public static RestoreJobStateChange read(DataInput in) throws IOException {
        byte[] jsonBytes = GsonUtils.readJsonBytes(in);
        if (AbstractJob.isCompressedJobMarker(jsonBytes)) {
            return GsonUtils.fromJsonCompressed(in, RestoreJobStateChange.class);
        } else {
            return GsonUtils.fromJsonBytes(jsonBytes, RestoreJobStateChange.class);
        }
    }

    // The same framing as AbstractJob.write
    @Override
    public void write(DataOutput out) throws IOException {
        if (Config.restore_job_compressed_serialization) {
            Text.writeString(out, AbstractJob.COMPRESSED_JOB_ID);
            GsonUtils.toJsonCompressed(out, this);
        } else {
            GsonUtils.toJsonAsText(out, this);
        }
    }

    @Override
    public String toString() {
        return "restore job state change, db id: " + dbId + ", label: " + label + ", job id: " + jobId
                + ", state: " + state;
    }
}
