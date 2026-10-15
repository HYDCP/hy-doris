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

import org.apache.doris.thrift.TMoveDirReq;
import org.apache.doris.thrift.TResourceInfo;
import org.apache.doris.thrift.TRestoreBackupDigest;
import org.apache.doris.thrift.TRestoreIncrementalRange;
import org.apache.doris.thrift.TTaskType;

public class DirMoveTask extends AgentTask {

    private long jobId;
    private String src;
    private int schemaHash;
    private boolean overwrite;
    // Append the rowsets of the range to the tablet instead of replacing it, null for the plain restore.
    private TRestoreIncrementalRange incrementalRange;
    private TRestoreBackupDigest backupLogicalDigest;
    // The tablet was loaded from a local source by the atomic restore (the backend judges the task by the evidence).
    private boolean localSourceLoaded = false;

    public DirMoveTask(TResourceInfo resourceInfo, long backendId, long signature, long jobId, long dbId,
            long tableId, long partitionId, long indexId, long tabletId, String src, int schemaHash,
            boolean overwrite) {
        super(resourceInfo, backendId, TTaskType.MOVE, dbId, tableId, partitionId, indexId, tabletId, signature);
        this.jobId = jobId;
        this.src = src;
        this.schemaHash = schemaHash;
        this.overwrite = overwrite;
    }

    public long getJobId() {
        return jobId;
    }

    public String getSrc() {
        return src;
    }

    public int getSchemaHash() {
        return schemaHash;
    }

    public boolean isOverwrite() {
        return overwrite;
    }

    public void setIncrementalRange(long baseVersion, long endVersion) {
        this.incrementalRange = new TRestoreIncrementalRange(baseVersion, endVersion);
    }

    public TRestoreIncrementalRange getIncrementalRange() {
        return incrementalRange;
    }

    // The logical digest of the backup of the tablet, for an incremental append: lets the backend tell that the
    // tablet is already the backup. Null if the backup has none.
    public void setBackupLogicalDigest(TRestoreBackupDigest backupLogicalDigest) {
        this.backupLogicalDigest = backupLogicalDigest;
    }

    public void setLocalSourceLoaded(boolean localSourceLoaded) {
        this.localSourceLoaded = localSourceLoaded;
    }

    public boolean isLocalSourceLoaded() {
        return localSourceLoaded;
    }

    public TRestoreBackupDigest getBackupLogicalDigest() {
        return backupLogicalDigest;
    }

    public TMoveDirReq toThrift() {
        TMoveDirReq req = new TMoveDirReq(tabletId, schemaHash, src, jobId, overwrite);
        if (incrementalRange != null) {
            req.setIncremental(incrementalRange);
            if (localSourceLoaded) {
                req.setLocalSourceLoaded(true);
            }
            if (backupLogicalDigest != null) {
                req.setBackupLogicalDigest(backupLogicalDigest);
            }
        }
        return req;
    }

}
