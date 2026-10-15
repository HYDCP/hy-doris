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

import org.apache.doris.thrift.TLogicalDigest;

import com.google.gson.annotations.SerializedName;

/**
 * The logical digest of a tablet snapshot, see TLogicalDigest. Persisted in the job info of a backup (per tablet,
 * BackupIndexInfo.logicalDigests) and in the SnapshotInfo, so the gson names are short and must not change.
 * Without a digest (root is empty) the reason code says why.
 */
public class LogicalDigestInfo {
    // the backend failed or did not report
    public static final String REASON_ERROR = "ERROR";
    // the digest does not support the model or a column type of the tablet
    public static final String REASON_NOT_SUPPORTED = "NOT_SUPPORTED";
    // the backend is an old version and did not report a digest
    public static final String REASON_NO_REPORT = "NO_REPORT";

    @SerializedName("a")
    public Integer algoVersion;
    @SerializedName("s")
    public String schemaSig;
    // lower case hex, empty if there is no digest
    @SerializedName("r")
    public String root;
    // reason code if root is empty, absent otherwise
    @SerializedName("e")
    public String reason;

    // The verdict of a restore digest task with a prefix source (see TLogicalDigest.prefix_verdict), not persisted.
    public static final String PREFIX_OK = "OK";
    public static final String PREFIX_NOT_BOUNDARY = "NOT_BOUNDARY";
    public static final String PREFIX_MISMATCH = "MISMATCH";
    public static final String PREFIX_SCHEMA_MISMATCH = "SCHEMA_MISMATCH";
    public static final String PREFIX_ERROR = "ERROR";

    // null if the digest was not compared with the decomposed digest of a backup
    public transient String prefixVerdict;
    public transient String prefixMsg;

    public LogicalDigestInfo() {
        // for persist
    }

    public static LogicalDigestInfo of(int algoVersion, String schemaSig, String root) {
        LogicalDigestInfo info = new LogicalDigestInfo();
        info.algoVersion = algoVersion;
        info.schemaSig = schemaSig;
        info.root = root;
        return info;
    }

    public static LogicalDigestInfo none(String reason) {
        LogicalDigestInfo info = new LogicalDigestInfo();
        info.root = "";
        info.reason = reason;
        return info;
    }

    /** Converts the digest reported by a backend. A null (not reported) is NO_REPORT. */
    public static LogicalDigestInfo fromThrift(TLogicalDigest digest) {
        if (digest == null) {
            return none(REASON_NO_REPORT);
        }
        String code = digest.isSetStatusCode() ? digest.getStatusCode() : "";
        boolean hasDigest = "OK".equals(code) && digest.isSetRoot() && !digest.getRoot().isEmpty()
                && digest.isSetSchemaSig() && !digest.getSchemaSig().isEmpty() && digest.isSetAlgoVersion();
        LogicalDigestInfo info;
        if (hasDigest) {
            info = of(digest.getAlgoVersion(), digest.getSchemaSig(), digest.getRoot().toLowerCase());
        } else {
            info = none("NOT_SUPPORTED".equals(code) ? REASON_NOT_SUPPORTED : REASON_ERROR);
        }
        if (digest.isSetPrefixVerdict()) {
            info.prefixVerdict = digest.getPrefixVerdict();
        }
        if (digest.isSetPrefixMsg()) {
            info.prefixMsg = digest.getPrefixMsg();
        }
        return info;
    }

    public boolean hasDigest() {
        return root != null && !root.isEmpty();
    }

    @Override
    public String toString() {
        return hasDigest() ? "algo=" + algoVersion + ", sig=" + schemaSig + ", root=" + root : "none(" + reason + ")";
    }
}
