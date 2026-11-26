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

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;

/**
 * The state token of a partition: the name of the logical content of the partition at its visible version. Two
 * partitions with the same effective token are the same initial state after the same sequence of writes, so their
 * content is the same as long as the replicas of a tablet agree and CCR applies a version of the source as it is.
 *
 * <p>A local write gives the partition a new token with a random part. A CCR increment adopts the token of the source
 * version when the local token is the token the source had before the write. A RESTORE adopts the token of the backup.
 *
 * <p>The effective token is one of three things, see {@link #effective}: the explicit token while its binding holds,
 * {@link #EMPTY} for a partition that never had a visible transaction and no explicit token, or none (null). A name
 * that is not stored is never made up from the cluster id, the partition id or the version: a cold copy of the
 * metadata has the same cluster id, and a metadata rollback makes a version again with other data.
 *
 * <p>The explicit token is bound to the (visible version, visible version time) of the partition it was set at. A
 * path that changes the version without knowing the token (an older FE as the master, the tracking switched off, a
 * path not registered) breaks the binding, and the partition has no token. So such a path only makes the partition
 * not reused.
 *
 * <p>EMPTY: a partition at {@link Partition#PARTITION_INIT_VERSION} has no visible data (every load and delete is a
 * version after it, a data rowset is never at the initial version), so two such partitions are the same on any two
 * clusters, and the first CCR increment of a partition that both sides created (ADD PARTITION replayed as a DDL)
 * continues the name. Checked: TRUNCATE and a dynamic or auto partition make new partitions at the initial version
 * with no data; INSERT OVERWRITE replaces the partitions by temporary partitions whose loads moved their version
 * past it (a temporary partition left at the initial version is empty); a RESTORE sets the version of the backup on
 * the partitions it restores (one restored at the initial version is the empty partition of the backup) and adopts
 * the token of the backup, also when that is none; ADMIN SET PARTITION VERSION invalidates the token, so a partition
 * set back to the initial version is not EMPTY. A schema change, rollup or finished inverted index job invalidates,
 * also at the initial version.
 *
 * <p>A path that changes the data and keeps the version (a schema change job rewrites the data in the new schema, a
 * rollup job builds the data of a new index, an inverted index job of BUILD INDEX or DROP INDEX that finishes wrote or
 * deleted the index files of the rowsets of its partition, an admin operation sets the version, a restore overwrites
 * the data), or that changes the data an index name stands for (a rollup is renamed), must invalidate the token: the
 * binding still holds there, and nothing else tells that the token no longer names the data. A column type changed and
 * changed back, a materialized view dropped and created again under its name with another WHERE, or two rollups of the
 * same columns that swap their names, leave the same structure and the same version with other data. A change of the
 * metadata that leaves the data files and the names of the indexes as they are (a light schema change) does not
 * invalidate the token: the schemas are compared, and the kept files are read by the metadata of the backup.
 *
 * <p>A partition without a token gets a new one at its next write. A backup also gives it one before it takes the
 * versions (logged as OP_SET_PARTITION_STATE_TOKEN), so that the partition restored from the backup has the token of
 * the source and is kept the next time.
 *
 * <p>A tainted partition (see {@link #taint}) had an operation on one of its replicas outside of the versions (ADMIN
 * SET REPLICA VERSION, ADMIN SET REPLICA STATUS to BAD or OK), a consistency check that failed, a lost replica made
 * empty (Config#recover_with_empty_tablet), or a cancelled inverted index job (its tasks may have changed the index
 * files of some of its replicas, in any state of the job): its replicas may not agree any more or lost data, so no name
 * may be given to it, for another cluster would adopt a name of content this cluster does not have on every replica. A
 * write keeps the taint (and names nothing), a backup names nothing, an invalidation keeps it. Only a restore that
 * adopts the token of the backup for the partition clears it: a restore keeps the local data of a partition by its
 * token, and a tainted partition has no token, so it is downloaded as a whole.
 *
 * <p>The object is immutable. The serialized names are part of the persisted metadata and must not be changed.
 */
public class PartitionStateToken {
    private static final SecureRandom RANDOM = new SecureRandom();

    /**
     * The name of a partition that never had a visible transaction, see the class comment. Not a hex string, so no
     * token made by {@link #fresh} equals it.
     */
    public static final String EMPTY = "empty";

    @SerializedName("tk")
    private String token;
    // the visible version and the visible version time of the partition the token is bound to
    @SerializedName("bv")
    private long boundVersion;
    @SerializedName("bt")
    private long boundVersionTime;
    // the partition was changed outside of the versions (a schema change or rollup job, an admin operation, an
    // overwrite by a restore): no token
    @SerializedName("bad")
    private boolean bad;
    // the replicas of the partition may not agree, see the class comment: no token until a restore downloads it
    @SerializedName("tnt")
    private boolean tainted;

    private PartitionStateToken() {
    }

    public PartitionStateToken(String token, long boundVersion, long boundVersionTime) {
        this.token = token;
        this.boundVersion = boundVersion;
        this.boundVersionTime = boundVersionTime;
        this.bad = false;
    }

    /** A token that makes the partition have no effective token, until the next write, restore or backup. */
    public static PartitionStateToken invalid() {
        PartitionStateToken t = new PartitionStateToken();
        t.bad = true;
        return t;
    }

    /**
     * Invalidates the state token of the partition: no token until the next write, restore or backup. A tainted
     * partition stays tainted. The caller holds the write lock of the table, on the master and on replay.
     */
    public static void invalidate(Partition partition) {
        if (!isTainted(partition)) {
            partition.setStateToken(invalid());
        }
    }

    /**
     * Taints the partition, see the class comment. The caller holds the write lock of the table, on the master and
     * on replay.
     */
    public static void taint(Partition partition) {
        PartitionStateToken t = new PartitionStateToken();
        t.tainted = true;
        partition.setStateToken(t);
    }

    public static boolean isTainted(Partition partition) {
        PartitionStateToken explicit = partition.getStateToken();
        return explicit != null && explicit.tainted;
    }

    public String getToken() {
        return token;
    }

    public long getBoundVersion() {
        return boundVersion;
    }

    public long getBoundVersionTime() {
        return boundVersionTime;
    }

    public boolean isBad() {
        return bad;
    }

    public boolean isTainted() {
        return tainted;
    }

    /**
     * The token as a follower applies a logged one to its partition (Env#replaySetPartitionStateToken). The master
     * binds a token to the version time of its partition, and the version time of a partition is not the same on
     * every frontend: a restore (Partition#updateVersionForRestore) and ADMIN SET PARTITION VERSION take the current
     * time of each frontend. The log is replayed in order, so a partition at the bound version is the partition the
     * master named: the token is bound to its own version time. At another version it gets none. An invalid or a
     * tainted token is applied as it is.
     *
     * <p>The version time in the binding tells a version written again (after a metadata rollback) from the one that
     * was named, on the frontend that binds it; it is not part of the name, which is random (P1). A follower that
     * rebinds at the same version only makes its binding match its own clock, as the master's matches the master's.
     */
    public PartitionStateToken boundTo(Partition partition) {
        if (bad || tainted) {
            return this;
        }
        if (boundVersion != partition.getVisibleVersion()) {
            return invalid();
        }
        return new PartitionStateToken(token, boundVersion, partition.getVisibleVersionTime());
    }

    /**
     * The effective token of the partition, null if it has none. Null never equals any token: the explicit token
     * while it is bound to the visible version and version time of the partition, EMPTY if the partition has no
     * explicit token and is at the initial version, otherwise null. An invalidated or tainted partition has none.
     */
    public static String effective(Partition partition) {
        PartitionStateToken explicit = partition.getStateToken();
        if (explicit == null) {
            return partition.getVisibleVersion() == Partition.PARTITION_INIT_VERSION ? EMPTY : null;
        }
        if (explicit.bad || explicit.tainted) {
            return null;
        }
        if (explicit.boundVersion == partition.getVisibleVersion()
                && explicit.boundVersionTime == partition.getVisibleVersionTime()) {
            return explicit.token;
        }
        return null;
    }

    /** A new token for a local write: never equal to another token. */
    public static String fresh(String prev, long clusterId, long partitionId, long version) {
        byte[] nonce = new byte[16];
        RANDOM.nextBytes(nonce);
        return hash("t|" + prev + "|" + clusterId + "|" + partitionId + "|" + version + "|" + toHex(nonce));
    }

    private static String hash(String s) {
        try {
            byte[] d = MessageDigest.getInstance("SHA-256").digest(s.getBytes(StandardCharsets.UTF_8));
            byte[] head = new byte[16];
            System.arraycopy(d, 0, head, 0, 16);
            return toHex(head);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    private static String toHex(byte[] bytes) {
        StringBuilder sb = new StringBuilder(bytes.length * 2);
        for (byte b : bytes) {
            sb.append(Character.forDigit((b >> 4) & 0xF, 16)).append(Character.forDigit(b & 0xF, 16));
        }
        return sb.toString();
    }

    @Override
    public String toString() {
        return tainted ? "tainted" : bad ? "invalid" : token + "@" + boundVersion + "/" + boundVersionTime;
    }
}
