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

import org.apache.doris.persist.gson.GsonUtils;

import com.google.gson.annotations.SerializedName;

/**
 * The result of checking the downloaded tablet snapshots of a restore job against the manifest of the backup,
 * shown in the ManifestCheck column of SHOW RESTORE.
 *
 * <p>The tablets are counted by replica: each downloaded replica is checked by its backend. A replica is
 * unverified if its backend does not support the manifest check (an old version), the check is disabled on
 * the backend, or the manifest of the tablet is missing. An unverified replica never fails the job.
 */
public class RestoreManifestCheck {
    @SerializedName("version")
    private int version;
    @SerializedName("algo")
    private String algo;
    @SerializedName("verified_tablets")
    private long verifiedTablets;
    @SerializedName("unverified_tablets")
    private long unverifiedTablets;
    // whether the digests of the files are checked for all the verified tablets
    @SerializedName("digest_checked")
    private boolean digestChecked;

    public RestoreManifestCheck() {
        // for persist
    }

    public RestoreManifestCheck(int version, String algo, long verifiedTablets, long unverifiedTablets,
            boolean digestChecked) {
        this.version = version;
        this.algo = algo;
        this.verifiedTablets = verifiedTablets;
        this.unverifiedTablets = unverifiedTablets;
        this.digestChecked = digestChecked;
    }

    public int getVersion() {
        return version;
    }

    public String getAlgo() {
        return algo;
    }

    public long getVerifiedTablets() {
        return verifiedTablets;
    }

    public long getUnverifiedTablets() {
        return unverifiedTablets;
    }

    public boolean isDigestChecked() {
        return digestChecked;
    }

    @Override
    public String toString() {
        return GsonUtils.GSON.toJson(this);
    }
}
