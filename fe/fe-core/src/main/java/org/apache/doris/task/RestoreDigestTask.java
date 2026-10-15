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


package org.apache.doris.task;

import org.apache.doris.thrift.TResourceInfo;
import org.apache.doris.thrift.TRestoreDigestPrefixSource;
import org.apache.doris.thrift.TRestoreDigestReq;
import org.apache.doris.thrift.TTaskType;

/**
 * Asks a backend to compute the logical digest of one replica of a tablet at a version, see
 * TRestoreDigestReq. The backend reports a TLogicalDigest in the finish task request. One task per replica, so the
 * signature (not the tablet id) identifies the task.
 */
public class RestoreDigestTask extends AgentTask {
    private final long jobId;
    private final int schemaHash;
    private final long version;
    // worker threads of the backend for this task, <= 0 means the backend config restore_digest_threads
    private final int threads;
    // Where the backend reads the decomposed digest of the backup, to compare it with the digest of the replica at
    // the version (the incremental restore). Null for the plain digest.
    private TRestoreDigestPrefixSource prefixSource;

    public RestoreDigestTask(long backendId, long signature, long jobId, long dbId, long tableId, long partitionId,
            long indexId, long tabletId, int schemaHash, long version, int threads) {
        super((TResourceInfo) null, backendId, TTaskType.RESTORE_DIGEST, dbId, tableId, partitionId, indexId,
                tabletId, signature);
        this.jobId = jobId;
        this.schemaHash = schemaHash;
        this.version = version;
        this.threads = threads;
    }

    public long getJobId() {
        return jobId;
    }

    public int getSchemaHash() {
        return schemaHash;
    }

    public long getVersion() {
        return version;
    }

    public int getThreads() {
        return threads;
    }

    public void setPrefixSource(TRestoreDigestPrefixSource prefixSource) {
        this.prefixSource = prefixSource;
    }

    public TRestoreDigestPrefixSource getPrefixSource() {
        return prefixSource;
    }

    public TRestoreDigestReq toThrift() {
        TRestoreDigestReq request = new TRestoreDigestReq(tabletId, schemaHash, version);
        if (threads > 0) {
            request.setThreads(threads);
        }
        if (prefixSource != null) {
            request.setPrefixSource(prefixSource);
        }
        return request;
    }

    @Override
    public String toString() {
        return "RestoreDigestTask: job " + jobId + ", signature " + signature + ", backend " + backendId
                + ", tablet " + tabletId + ", version " + version;
    }
}
