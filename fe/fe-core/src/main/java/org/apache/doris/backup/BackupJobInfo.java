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

import org.apache.doris.analysis.BackupStmt.BackupContent;
import org.apache.doris.analysis.PartitionNames;
import org.apache.doris.analysis.TableRef;
import org.apache.doris.backup.RestoreFileMapping.IdChain;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.MaterializedIndex.IndexExtState;
import org.apache.doris.catalog.OdbcCatalogResource;
import org.apache.doris.catalog.OdbcTable;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.PartitionStateToken;
import org.apache.doris.catalog.Resource;
import org.apache.doris.catalog.RestoreLineage;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.TableIf.TableType;
import org.apache.doris.catalog.Tablet;
import org.apache.doris.catalog.View;
import org.apache.doris.common.Config;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.FeMetaVersion;
import org.apache.doris.common.Version;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.persist.gson.GsonPostProcessable;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.thrift.TNetworkAddress;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.gson.Gson;
import com.google.gson.annotations.SerializedName;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedWriter;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/*
 * This is a memory structure mapping the job info file in repository.
 * It contains all content of a job info file.
 * It also be used to save the info of a restore job, such as alias of table and meta info file path
 */
public class BackupJobInfo implements Writable, GsonPostProcessable {
    private static final Logger LOG = LogManager.getLogger(BackupJobInfo.class);

    // The version of the manifest this version writes. Version 1 (a version before the capacity controls): the roots
    // are maps with a tablet id key, and the decomposed digest file of a tablet (rdigest) is next to its manifest, its
    // SHA-256 is in the job info. Version 2: the roots are arrays, the rdigest is a file of the manifest (so
    // the manifest root locates and checks it), only whether a tablet has one is in the job info. A version which
    // only knows version 1 takes a backup of version 2 as one without manifest, it neither checks the downloads
    // nor reuses data, and does not fail.
    public static final int MANIFEST_VERSION = 2;
    public static final int MANIFEST_VERSION_V1 = 1;
    public static final String DIGEST_SHA256 = "sha256";
    public static final String DIGEST_NONE = "none";

    @SerializedName("name")
    public String name;
    @SerializedName("database")
    public String dbName;
    @SerializedName("id")
    public long dbId;
    @SerializedName("backup_time")
    public long backupTime;
    @SerializedName("content")
    public BackupContent content;
    // only include olap table
    @SerializedName("backup_objects")
    public Map<String, BackupOlapTableInfo> backupOlapTableObjects = Maps.newHashMap();
    // include other objects: view, external table
    @SerializedName("new_backup_objects")
    public NewBackupObjects newBackupObjects = new NewBackupObjects();
    public boolean success;
    @SerializedName("backup_result")
    public String successJson = "succeed";

    @SerializedName("meta_version")
    public int metaVersion;
    @SerializedName("major_version")
    public int majorVersion;
    @SerializedName("minor_version")
    public int minorVersion;
    @SerializedName("patch_version")
    public int patchVersion;
    @SerializedName("is_force_replication_allocation")
    public boolean isForceReplicationAllocation;

    @SerializedName("tablet_be_map")
    public Map<Long, Long> tabletBeMap = Maps.newHashMap();

    @SerializedName("tablet_snapshot_path_map")
    public Map<Long, String> tabletSnapshotPathMap = Maps.newHashMap();

    @SerializedName("table_commit_seq_map")
    public Map<Long, Long> tableCommitSeqMap;

    // The cluster id of the cluster that took the backup (Env#getClusterId). Null (absent in the json) for a backup by
    // an old version. It is not unique (a random int made when the cluster is initialized, and a standby cluster
    // built from a cold copy of the image shares it), so it only narrows the candidates of partition level reuse.
    @SerializedName("scid")
    public Long srcClusterId;

    // The version of the manifest. The manifest of each tablet snapshot (the complete list of its files) is a file
    // next to the tablet data, one level up; the job info only records its SHA-256, the manifest root, see
    // BackupIndexInfo.manifestRoots. Null (absent in the json) means there is no manifest, e.g. a backup by an old
    // version, or some backend did not report the root. Not written if null, so a job info without manifest is
    // the same as before.
    @SerializedName("manifest_version")
    public Integer manifestVersion;
    // The digest algorithm of the files in the manifest, DIGEST_SHA256 or DIGEST_NONE.
    @SerializedName("digest_algorithm")
    public String digestAlgorithm;

    // Why the backup did not record the logical digest although it was asked to (e.g. TOO_MANY_TABLETS:<n>><limit>),
    // see Config.backup_logical_digest_max_tablets. Null (absent in the json) if it did, or was not asked to.
    @SerializedName("ldskip")
    public String logicalDigestSkipReason;

    // partition id -> its indexes, built on demand, not persisted. The manifest root, the logical digest and the
    // decomposed digest of a tablet are read from its index by position, so no map with a tablet id key is kept.
    private transient Map<Long, List<BackupIndexInfo>> partitionIndexes;

    public static class ExtraInfo {
        public static class NetworkAddrss {
            @SerializedName("ip")
            public String ip;
            @SerializedName("port")
            public int port;
        }

        @SerializedName("be_network_map")
        public Map<Long, NetworkAddrss> beNetworkMap = Maps.newHashMap();

        @SerializedName("token")
        public String token;
    }

    @SerializedName("extra_info")
    public ExtraInfo extraInfo;


    // This map is used to save the table alias mapping info when processing a restore job.
    // origin -> alias
    @SerializedName("tblalias")
    public Map<String, String> tblAlias = Maps.newHashMap();

    public long getBackupTime() {
        return backupTime;
    }

    public void initBackupJobInfoAfterDeserialize() {
        // transform success
        if (successJson.equals("succeed")) {
            success = true;
        } else {
            success = false;
        }

        // init meta version
        if (metaVersion == 0) {
            // meta_version does not exist
            metaVersion = FeConstants.meta_version;
        }

        // init olap table info
        for (BackupOlapTableInfo backupOlapTableInfo : backupOlapTableObjects.values()) {
            for (BackupPartitionInfo backupPartitionInfo : backupOlapTableInfo.partitions.values()) {
                for (BackupIndexInfo backupIndexInfo : backupPartitionInfo.indexes.values()) {
                    backupIndexInfo.upgradeLegacyFormat();
                    // a job info read from the edit log has the list already
                    backupIndexInfo.sortedTabletInfoList.clear();
                    List<Long> sortedTabletIds = backupIndexInfo.getSortedTabletIds();
                    boolean skipped = false;
                    for (Long tabletId : sortedTabletIds) {
                        List<String> files = backupIndexInfo.getTabletFiles(tabletId);
                        if (files == null) {
                            skipped = true;
                            continue;
                        }
                        backupIndexInfo.sortedTabletInfoList.add(new BackupTabletInfo(tabletId, files));
                    }
                    if (skipped) {
                        // The position of a tablet in the arrays is not its position in sortedTabletInfoList any
                        // more, never mix them up: the index has no manifest and no digest.
                        LOG.warn("backup {} index {} has tablets in the tablets order without files, ignore its "
                                + "manifest roots and digests", name, backupIndexInfo.id);
                        backupIndexInfo.clearManifestRoots();
                        backupIndexInfo.clearLogicalDigests();
                        backupIndexInfo.clearPrefixDigests();
                    }
                }
            }
        }
    }

    public Table.TableType getTypeByTblName(String tblName) {
        if (backupOlapTableObjects.containsKey(tblName)) {
            return Table.TableType.OLAP;
        }
        for (BackupViewInfo backupViewInfo : newBackupObjects.views) {
            if (backupViewInfo.name.equals(tblName)) {
                return Table.TableType.VIEW;
            }
        }
        for (BackupOdbcTableInfo backupOdbcTableInfo : newBackupObjects.odbcTables) {
            if (backupOdbcTableInfo.dorisTableName.equals(tblName)) {
                return Table.TableType.ODBC;
            }
        }
        return null;
    }

    public BackupOlapTableInfo getOlapTableInfo(String tblName) {
        return backupOlapTableObjects.get(tblName);
    }

    public void removeTable(TableRef tableRef, TableType tableType) {
        switch (tableType) {
            case OLAP:
                removeOlapTable(tableRef);
                break;
            case VIEW:
                removeView(tableRef);
                break;
            case ODBC:
                removeOdbcTable(tableRef);
                break;
            default:
                break;
        }
    }

    public void removeOlapTable(TableRef tableRef) {
        String tblName = tableRef.getName().getTbl();
        BackupOlapTableInfo tblInfo = backupOlapTableObjects.get(tblName);
        if (tblInfo == null) {
            LOG.info("Ignore error: exclude table " + tblName + " does not exist in snapshot " + name);
            return;
        }
        PartitionNames partitionNames = tableRef.getPartitionNames();
        if (partitionNames == null) {
            backupOlapTableObjects.remove(tblInfo);
            return;
        }
        // check the selected partitions
        for (String partName : partitionNames.getPartitionNames()) {
            if (tblInfo.containsPart(partName)) {
                tblInfo.partitions.remove(partName);
            } else {
                LOG.info("Ignore error: exclude partition " + partName + " of table " + tblName
                        + " does not exist in snapshot");
            }
        }
    }

    public void removeView(TableRef tableRef) {
        Iterator<BackupViewInfo> iter = newBackupObjects.views.listIterator();
        while (iter.hasNext()) {
            if (iter.next().name.equals(tableRef.getName().getTbl())) {
                iter.remove();
                return;
            }
        }
    }

    public void removeOdbcTable(TableRef tableRef) {
        Iterator<BackupOdbcTableInfo> iter = newBackupObjects.odbcTables.listIterator();
        while (iter.hasNext()) {
            BackupOdbcTableInfo backupOdbcTableInfo = iter.next();
            if (backupOdbcTableInfo.dorisTableName.equals(tableRef.getName().getTbl())) {
                if (backupOdbcTableInfo.resourceName != null) {
                    Iterator<BackupOdbcResourceInfo> resourceIter = newBackupObjects.odbcResources.listIterator();
                    while (resourceIter.hasNext()) {
                        if (resourceIter.next().name.equals(backupOdbcTableInfo.resourceName)) {
                            resourceIter.remove();
                        }
                    }
                }
                iter.remove();
                return;
            }
        }
    }

    public void retainOlapTables(Set<String> tblNames) {
        Iterator<Map.Entry<String, BackupOlapTableInfo>> iter = backupOlapTableObjects.entrySet().iterator();
        while (iter.hasNext()) {
            if (!tblNames.contains(iter.next().getKey())) {
                iter.remove();
            }
        }
    }

    public void retainView(Set<String> viewNames) {
        Iterator<BackupViewInfo> iter = newBackupObjects.views.listIterator();
        while (iter.hasNext()) {
            if (!viewNames.contains(iter.next().name)) {
                iter.remove();
            }
        }
    }

    public void retainOdbcTables(Set<String> odbcTableNames) {
        Iterator<BackupOdbcTableInfo> odbcIter = newBackupObjects.odbcTables.listIterator();
        Set<String> retainOdbcResourceNames = Sets.newHashSet();
        while (odbcIter.hasNext()) {
            BackupOdbcTableInfo backupOdbcTableInfo = odbcIter.next();
            if (!odbcTableNames.contains(backupOdbcTableInfo.dorisTableName)) {
                odbcIter.remove();
            } else {
                retainOdbcResourceNames.add(backupOdbcTableInfo.resourceName);
            }
        }
        Iterator<BackupOdbcResourceInfo> resourceIter = newBackupObjects.odbcResources.listIterator();
        while (resourceIter.hasNext()) {
            if (!retainOdbcResourceNames.contains(resourceIter.next().name)) {
                resourceIter.remove();
            }
        }
    }

    public void setAlias(String orig, String alias) {
        tblAlias.put(orig, alias);
    }

    public String getAliasByOriginNameIfSet(String orig) {
        return tblAlias.containsKey(orig) ? tblAlias.get(orig) : orig;
    }

    public String getOrginNameByAlias(String alias) {
        for (Map.Entry<String, String> entry : tblAlias.entrySet()) {
            if (entry.getValue().equals(alias)) {
                return entry.getKey();
            }
        }
        return alias;
    }

    public static class BriefBackupJobInfo {
        @SerializedName("name")
        public String name;
        @SerializedName("database")
        public String database;
        @SerializedName("backup_time")
        public long backupTime;
        @SerializedName("content")
        public BackupContent content;
        @SerializedName("olap_table_list")
        public List<BriefBackupOlapTable> olapTableList = Lists.newArrayList();
        @SerializedName("view_list")
        public List<BackupViewInfo> viewList = Lists.newArrayList();
        @SerializedName("odbc_table_list")
        public List<BackupOdbcTableInfo> odbcTableList = Lists.newArrayList();
        @SerializedName("odbc_resource_list")
        public List<BackupOdbcResourceInfo> odbcResourceList = Lists.newArrayList();

        public static BriefBackupJobInfo fromBackupJobInfo(BackupJobInfo backupJobInfo) {
            BriefBackupJobInfo briefBackupJobInfo = new BriefBackupJobInfo();
            briefBackupJobInfo.name = backupJobInfo.name;
            briefBackupJobInfo.database = backupJobInfo.dbName;
            briefBackupJobInfo.backupTime = backupJobInfo.backupTime;
            briefBackupJobInfo.content = backupJobInfo.content;
            for (Map.Entry<String, BackupOlapTableInfo> olapTableEntry :
                    backupJobInfo.backupOlapTableObjects.entrySet()) {
                BriefBackupOlapTable briefBackupOlapTable = new BriefBackupOlapTable();
                briefBackupOlapTable.name = olapTableEntry.getKey();
                briefBackupOlapTable.partitionNames = Lists.newArrayList(olapTableEntry.getValue().partitions.keySet());
                briefBackupJobInfo.olapTableList.add(briefBackupOlapTable);
            }
            briefBackupJobInfo.viewList = backupJobInfo.newBackupObjects.views;
            briefBackupJobInfo.odbcTableList = backupJobInfo.newBackupObjects.odbcTables;
            briefBackupJobInfo.odbcResourceList = backupJobInfo.newBackupObjects.odbcResources;
            return briefBackupJobInfo;
        }
    }

    public static class BriefBackupOlapTable {
        @SerializedName("name")
        public String name;
        @SerializedName("partition_names")
        public List<String> partitionNames;
    }

    public static class NewBackupObjects {
        @SerializedName("views")
        public List<BackupViewInfo> views = Lists.newArrayList();
        @SerializedName("odbc_tables")
        public List<BackupOdbcTableInfo> odbcTables = Lists.newArrayList();
        @SerializedName("odbc_resources")
        public List<BackupOdbcResourceInfo> odbcResources = Lists.newArrayList();
    }

    public static class BackupOlapTableInfo {
        @SerializedName("id")
        public long id;
        @SerializedName("partitions")
        public Map<String, BackupPartitionInfo> partitions = Maps.newHashMap();

        public boolean containsPart(String partName) {
            return partitions.containsKey(partName);
        }

        public BackupPartitionInfo getPartInfo(String partName) {
            return partitions.get(partName);
        }

        public void retainPartitions(Collection<String> partNames) {
            if (partNames == null || partNames.isEmpty()) {
                // retain all
                return;
            }
            Iterator<Map.Entry<String, BackupPartitionInfo>> iter = partitions.entrySet().iterator();
            while (iter.hasNext()) {
                if (!partNames.contains(iter.next().getKey())) {
                    iter.remove();
                }
            }
        }
    }

    public static class BackupPartitionInfo {
        @SerializedName("id")
        public long id;
        @SerializedName("version")
        public long version;
        // the effective state token of the partition at the backup version, see PartitionStateToken; null if it
        // has none, or the source does not track the tokens
        @SerializedName("stk")
        public String stateToken;
        @SerializedName("indexes")
        public Map<String, BackupIndexInfo> indexes = Maps.newHashMap();

        public BackupIndexInfo getIdx(String idxName) {
            return indexes.get(idxName);
        }
    }

    public static class BackupIndexInfo {
        @SerializedName("id")
        public long id;
        @SerializedName("schema_hash")
        public int schemaHash;
        @SerializedName("tablets")
        public Map<Long, List<String>> tablets = Maps.newHashMap();
        @SerializedName("tablets_order")
        public List<Long> tabletsOrder = Lists.newArrayList();

        // The per tablet data below is kept in arrays aligned with the tablets (position i is the i-th tablet of
        // tablets_order, or of the tablet ids sorted if there is no tablets_order, see getSortedTabletIds()), so
        // that neither the json nor the memory has a map with a tablet id key for each. An empty string means
        // the tablet has none.
        //
        // The manifest root of the tablet snapshot: the SHA-256 (lower case hex) of its manifest file. Null if the
        // backup has no manifest.
        @SerializedName("mra")
        public List<String> manifestRootArray;
        // The logical digest of the tablet snapshots, see LogicalDigestInfo. The schema signature and the algorithm
        // version are the same for the tablets of an index, so they are recorded once for the index, and each
        // tablet only records its root. Null if the backup has no logical digest (the logical_digest property is
        // off, an old version, or the backup has too many tablets).
        @SerializedName("lra")
        public List<String> logicalDigestRootArray;
        // The reason (LogicalDigestInfo.REASON_*) of the tablets which have no digest. A tablet with neither a root
        // nor a reason was not asked for a digest (e.g. a unique table). Null if every tablet that was asked
        // has a digest.
        @SerializedName("lna")
        public List<String> logicalDigestNoneArray;
        // the schema signature and the algorithm version of the digests in logicalDigestRootArray
        @SerializedName("lds")
        public String logicalDigestSchemaSig;
        @SerializedName("lda")
        public Integer logicalDigestAlgo;
        // Which tablet snapshots have a decomposed digest file (rdigest), from which the logical digest at any rowset
        // boundary version can be composed: one char for each tablet, '1' if it has one. The file is recorded in the
        // manifest of the tablet (manifest_version 2), and located by it, so there is no root here. Null if no
        // tablet has one.
        @SerializedName("rdp")
        public String prefixDigestPresent;
        // Only read, for a backup of a version before the decomposed digest was recorded in the manifest
        // (manifest_version 1): the SHA-256 of the decomposed digest file of each tablet, which was kept next to the
        // manifest. Also converted from "rd".
        @SerializedName("rra")
        public List<String> prefixDigestRootArray;

        // Only read, for a backup of a version before the arrays: tablet id -> the value. They are converted to the
        // arrays after reading (upgradeLegacyFormat), then dropped, and never written.
        // (A version which does not know the new names does not use the digests of the new format, and a version
        // which knows "ld" only fails on a changed "ld", so the names of the old fields are not reused.)
        @SerializedName("manifest_roots")
        public Map<Long, String> manifestRoots;
        @SerializedName("ldr")
        public Map<Long, String> logicalDigestRoots;
        @SerializedName("ldn")
        public Map<Long, String> logicalDigestNone;
        @SerializedName("ld")
        public Map<Long, LogicalDigestInfo> logicalDigests;
        @SerializedName("rd")
        public Map<Long, String> prefixDigestRoots;

        public List<BackupTabletInfo> sortedTabletInfoList = Lists.newArrayList();

        // tablet id -> position, built on demand, not persisted, see positionOf().
        private transient volatile TabletPositions positions;

        public List<String> getTabletFiles(long tabletId) {
            return tablets.get(tabletId);
        }

        private static String at(List<String> array, int pos) {
            if (array == null || pos < 0 || pos >= array.size()) {
                return null;
            }
            String value = array.get(pos);
            return value == null || value.isEmpty() ? null : value;
        }

        /** The manifest root of the pos-th tablet, null if there is none. */
        public String getManifestRootAt(int pos) {
            return at(manifestRootArray, pos);
        }

        /**
         * The logical digest of the pos-th tablet put together from the index and the tablet, null if the backup
         * has none for the tablet. It is made when asked, no object is kept for the tablet.
         */
        public LogicalDigestInfo getLogicalDigestAt(int pos) {
            String root = at(logicalDigestRootArray, pos);
            if (root != null) {
                if (logicalDigestSchemaSig == null || logicalDigestAlgo == null) {
                    return LogicalDigestInfo.none(LogicalDigestInfo.REASON_ERROR);
                }
                return LogicalDigestInfo.of(logicalDigestAlgo, logicalDigestSchemaSig, root);
            }
            String reason = at(logicalDigestNoneArray, pos);
            return reason == null ? null : LogicalDigestInfo.none(reason);
        }

        /** Whether the pos-th tablet snapshot has a decomposed digest file. */
        public boolean hasPrefixDigestAt(int pos) {
            if (at(prefixDigestRootArray, pos) != null) {
                return true;
            }
            return prefixDigestPresent != null && pos >= 0 && pos < prefixDigestPresent.length()
                    && prefixDigestPresent.charAt(pos) == '1';
        }

        /**
         * The SHA-256 of the decomposed digest file of the pos-th tablet snapshot, only for a backup which has it in
         * the job info (manifest_version 1). Null otherwise, for a newer backup the file is located by the manifest.
         */
        public String getPrefixDigestRootAt(int pos) {
            return at(prefixDigestRootArray, pos);
        }

        public int getTabletCount() {
            return tabletsOrder == null || tabletsOrder.isEmpty() ? tablets.size() : tabletsOrder.size();
        }

        /** The position of the tablet, -1 if the index does not have it. Not used for the per tablet loops. */
        public int positionOf(long tabletId) {
            TabletPositions p = positions;
            if (p == null) {
                p = TabletPositions.build(getSortedTabletIds());
                positions = p;
            }
            return p.find(tabletId);
        }

        public LogicalDigestInfo getLogicalDigest(long tabletId) {
            return getLogicalDigestAt(positionOf(tabletId));
        }

        public String getManifestRoot(long tabletId) {
            return getManifestRootAt(positionOf(tabletId));
        }

        public boolean hasPrefixDigest(long tabletId) {
            return hasPrefixDigestAt(positionOf(tabletId));
        }

        // Record the manifest roots of all the tablets.
        void setManifestRoots(List<String> roots) {
            manifestRootArray = roots;
        }

        // Record the logical digest of the pos-th tablet. A digest which does not have the schema signature or the
        // algorithm version of the others of the index can not be recorded once for the index, it is recorded as a
        // tablet without digest (MIXED_SCHEMA).
        void putLogicalDigestAt(int pos, LogicalDigestInfo digest) {
            if (digest.hasDigest() && digest.schemaSig != null && digest.algoVersion != null) {
                if (logicalDigestSchemaSig == null) {
                    logicalDigestSchemaSig = digest.schemaSig;
                    logicalDigestAlgo = digest.algoVersion;
                }
                if (logicalDigestSchemaSig.equals(digest.schemaSig) && logicalDigestAlgo.equals(digest.algoVersion)) {
                    logicalDigestRootArray = put(logicalDigestRootArray, pos, digest.root);
                    return;
                }
                digest = LogicalDigestInfo.none(LogicalDigestInfo.REASON_MIXED_SCHEMA);
            } else if (digest.hasDigest()) {
                digest = LogicalDigestInfo.none(LogicalDigestInfo.REASON_ERROR);
            }
            logicalDigestNoneArray = put(logicalDigestNoneArray, pos,
                    digest.reason == null || digest.reason.isEmpty() ? LogicalDigestInfo.REASON_ERROR : digest.reason);
        }

        // Set the pos-th value, the array is made if absent.
        private List<String> put(List<String> array, int pos, String value) {
            if (array == null) {
                int count = getTabletCount();
                array = Lists.newArrayList(Collections.nCopies(count, ""));
            }
            array.set(pos, value);
            return array;
        }

        /** The number of tablets which have a logical digest. */
        int countLogicalDigests() {
            return count(logicalDigestRootArray);
        }

        private static int count(List<String> array) {
            int num = 0;
            if (array != null) {
                for (String value : array) {
                    if (value != null && !value.isEmpty()) {
                        num++;
                    }
                }
            }
            return num;
        }

        void clearLogicalDigests() {
            logicalDigestRootArray = null;
            logicalDigestNoneArray = null;
            logicalDigestSchemaSig = null;
            logicalDigestAlgo = null;
            logicalDigests = null;
            logicalDigestRoots = null;
            logicalDigestNone = null;
        }

        void clearPrefixDigests() {
            prefixDigestPresent = null;
            prefixDigestRootArray = null;
            prefixDigestRoots = null;
        }

        void clearManifestRoots() {
            manifestRootArray = null;
            manifestRoots = null;
        }

        // Keep the logical digests of the given tablets only. The arrays are dropped if none is kept, a kept array
        // does not shrink.
        void retainLogicalDigests(Set<Long> tabletIds) {
            List<Long> ids = getSortedTabletIds();
            for (int i = 0; i < ids.size(); i++) {
                if (tabletIds.contains(ids.get(i))) {
                    continue;
                }
                if (logicalDigestRootArray != null && i < logicalDigestRootArray.size()) {
                    logicalDigestRootArray.set(i, "");
                }
                if (logicalDigestNoneArray != null && i < logicalDigestNoneArray.size()) {
                    logicalDigestNoneArray.set(i, "");
                }
            }
            if (count(logicalDigestRootArray) == 0) {
                logicalDigestRootArray = null;
                logicalDigestSchemaSig = null;
                logicalDigestAlgo = null;
            }
            if (count(logicalDigestNoneArray) == 0) {
                logicalDigestNoneArray = null;
            }
            logicalDigests = null;
            logicalDigestRoots = null;
            logicalDigestNone = null;
        }

        /**
         * Read the data of a backup of a version before the arrays (the maps with a tablet id key) into the arrays,
         * and drop the maps. The new fields win if both are there. For the logical digest the order of the old
         * reading is kept: ldr, then ldn, then ld.
         */
        void upgradeLegacyFormat() {
            if (manifestRoots == null && logicalDigestRoots == null && logicalDigestNone == null
                    && logicalDigests == null && prefixDigestRoots == null) {
                return;
            }
            List<Long> ids = getSortedTabletIds();
            int count = ids.size();
            if (manifestRoots != null) {
                if (manifestRootArray == null) {
                    manifestRootArray = toArray(manifestRoots, ids);
                }
                manifestRoots = null;
            }
            if (prefixDigestRoots != null) {
                if (prefixDigestRootArray == null && prefixDigestPresent == null) {
                    prefixDigestRootArray = toArray(prefixDigestRoots, ids);
                }
                prefixDigestRoots = null;
            }
            if (logicalDigestRoots != null || logicalDigestNone != null || logicalDigests != null) {
                Map<Long, String> oldRoots = logicalDigestRoots;
                Map<Long, String> oldNone = logicalDigestNone;
                Map<Long, LogicalDigestInfo> oldDigests = logicalDigests;
                logicalDigestRoots = null;
                logicalDigestNone = null;
                logicalDigests = null;
                for (int i = 0; i < count; i++) {
                    if (at(logicalDigestRootArray, i) != null || at(logicalDigestNoneArray, i) != null) {
                        continue;
                    }
                    long tabletId = ids.get(i);
                    String root = oldRoots == null ? null : oldRoots.get(tabletId);
                    if (root != null) {
                        if (logicalDigestSchemaSig == null || logicalDigestAlgo == null) {
                            putLogicalDigestAt(i, LogicalDigestInfo.none(LogicalDigestInfo.REASON_ERROR));
                        } else {
                            putLogicalDigestAt(i,
                                    LogicalDigestInfo.of(logicalDigestAlgo, logicalDigestSchemaSig, root));
                        }
                        continue;
                    }
                    String reason = oldNone == null ? null : oldNone.get(tabletId);
                    if (reason != null) {
                        putLogicalDigestAt(i, LogicalDigestInfo.none(reason));
                        continue;
                    }
                    LogicalDigestInfo digest = oldDigests == null ? null : oldDigests.get(tabletId);
                    if (digest != null) {
                        putLogicalDigestAt(i, digest);
                    }
                }
            }
        }

        private static List<String> toArray(Map<Long, String> map, List<Long> ids) {
            List<String> array = Lists.newArrayListWithCapacity(ids.size());
            boolean any = false;
            for (Long id : ids) {
                String value = map.get(id);
                any |= value != null && !value.isEmpty();
                array.add(value == null ? "" : value);
            }
            return any ? array : null;
        }

        List<Long> getSortedTabletIds() {
            if (tabletsOrder == null || tabletsOrder.isEmpty()) {
                // in previous version, we are not saving tablets order(which was a BUG),
                // so we have to sort the tabletIds to restore the original order of tablets.
                List<Long> tmpList = Lists.newArrayList(tablets.keySet());
                tmpList.sort((o1, o2) -> Long.valueOf(o1).compareTo(Long.valueOf(o2)));
                return tmpList;
            } else {
                return tabletsOrder;
            }
        }
    }

    // tablet id -> position in the tablet order of an index: the tablet ids sorted, and the position of each if the
    // tablet order is not the order of the ids. Two primitive arrays instead of a map with a key for each tablet.
    private static class TabletPositions {
        private final long[] keys;
        // null if the position is the index in keys
        private final int[] positions;

        private TabletPositions(long[] keys, int[] positions) {
            this.keys = keys;
            this.positions = positions;
        }

        static TabletPositions build(List<Long> ids) {
            int count = ids.size();
            long[] keys = new long[count];
            boolean ascending = true;
            for (int i = 0; i < count; i++) {
                keys[i] = ids.get(i);
                if (i > 0 && keys[i] <= keys[i - 1]) {
                    ascending = false;
                }
            }
            if (ascending) {
                return new TabletPositions(keys, null);
            }
            Integer[] order = new Integer[count];
            for (int i = 0; i < count; i++) {
                order[i] = i;
            }
            Arrays.sort(order, (a, b) -> Long.compare(keys[a], keys[b]));
            long[] sortedKeys = new long[count];
            int[] sortedPositions = new int[count];
            for (int i = 0; i < count; i++) {
                sortedKeys[i] = keys[order[i]];
                sortedPositions[i] = order[i];
            }
            return new TabletPositions(sortedKeys, sortedPositions);
        }

        int find(long tabletId) {
            int i = Arrays.binarySearch(keys, tabletId);
            if (i < 0) {
                return -1;
            }
            return positions == null ? i : positions[i];
        }
    }

    public static class BackupTabletInfo {
        @SerializedName("id")
        public long id;
        @SerializedName("files")
        public List<String> files;

        public BackupTabletInfo(long id, List<String> files) {
            this.id = id;
            this.files = files;
        }
    }

    public static class BackupViewInfo {
        @SerializedName("id")
        public long id;
        @SerializedName("name")
        public String name;
    }

    public static class BackupOdbcTableInfo {
        @SerializedName("id")
        public long id;
        @SerializedName("doris_table_name")
        public String dorisTableName;
        @SerializedName("linked_odbc_database_name")
        public String linkedOdbcDatabaseName;
        @SerializedName("linked_odbc_table_name")
        public String linkedOdbcTableName;
        @SerializedName("resource_name")
        public String resourceName;
        @SerializedName("host")
        public String host;
        @SerializedName("port")
        public String port;
        @SerializedName("user")
        public String user;
        @SerializedName("driver")
        public String driver;
        @SerializedName("odbc_type")
        public String odbcType;
    }

    public static class BackupOdbcResourceInfo {
        @SerializedName("name")
        public String name;
    }

    // eg: __db_10001/__tbl_10002/__part_10003/__idx_10002/__10004
    public String getFilePath(String db, String tbl, String part, String idx, long tabletId) {
        if (!db.equalsIgnoreCase(dbName)) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("db name does not equal: {}-{}", dbName, db);
            }
            return null;
        }

        BackupOlapTableInfo tblInfo = backupOlapTableObjects.get(tbl);
        if (tblInfo == null) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("tbl {} does not exist", tbl);
            }
            return null;
        }

        BackupPartitionInfo partInfo = tblInfo.getPartInfo(part);
        if (partInfo == null) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("part {} does not exist", part);
            }
            return null;
        }

        BackupIndexInfo idxInfo = partInfo.getIdx(idx);
        if (idxInfo == null) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("idx {} does not exist", idx);
            }
            return null;
        }

        List<String> pathSeg = Lists.newArrayList();
        pathSeg.add(Repository.PREFIX_DB + dbId);
        pathSeg.add(Repository.PREFIX_TBL + tblInfo.id);
        pathSeg.add(Repository.PREFIX_PART + partInfo.id);
        pathSeg.add(Repository.PREFIX_IDX + idxInfo.id);
        pathSeg.add(Repository.PREFIX_COMMON + tabletId);

        return Joiner.on("/").join(pathSeg);
    }

    // eg: __db_10001/__tbl_10002/__part_10003/__idx_10002/__10004
    public String getFilePath(IdChain ids) {
        List<String> pathSeg = Lists.newArrayList();
        pathSeg.add(Repository.PREFIX_DB + dbId);
        pathSeg.add(Repository.PREFIX_TBL + ids.getTblId());
        pathSeg.add(Repository.PREFIX_PART + ids.getPartId());
        pathSeg.add(Repository.PREFIX_IDX + ids.getIdxId());
        pathSeg.add(Repository.PREFIX_COMMON + ids.getTabletId());

        return Joiner.on("/").join(pathSeg);
    }

    // struct TRemoteTabletSnapshot {
    //     1: optional i64 local_tablet_id
    //     2: optional string local_snapshot_path
    //     3: optional i64 remote_tablet_id
    //     4: optional i64 remote_be_id
    //     5: optional Types.TSchemaHash schema_hash
    //     6: optional Types.TNetworkAddress remote_be_addr
    //     7: optional string remote_snapshot_path
    //     8: optional string token
    // }

    public String getTabletSnapshotPath(Long tabletId) {
        return tabletSnapshotPathMap.get(tabletId);
    }

    public Long getBeId(Long tabletId) {
        return tabletBeMap.get(tabletId);
    }

    public String getToken() {
        return extraInfo.token;
    }

    public TNetworkAddress getBeAddr(Long beId) {
        ExtraInfo.NetworkAddrss addr = extraInfo.beNetworkMap.get(beId);
        if (addr == null) {
            return null;
        }

        return new TNetworkAddress(addr.ip, addr.port);
    }

    // TODO(Drogon): improve this find perfermance
    public Long getSchemaHash(long tableId, long partitionId, long indexId) {
        for (BackupOlapTableInfo backupOlapTableInfo : backupOlapTableObjects.values()) {
            if (backupOlapTableInfo.id != tableId) {
                continue;
            }

            for (BackupPartitionInfo backupPartitionInfo : backupOlapTableInfo.partitions.values()) {
                if (backupPartitionInfo.id != partitionId) {
                    continue;
                }

                for (BackupIndexInfo backupIndexInfo : backupPartitionInfo.indexes.values()) {
                    if (backupIndexInfo.id != indexId) {
                        continue;
                    }

                    return Long.valueOf(backupIndexInfo.schemaHash);
                }
            }
        }
        return null;
    }

    public static BackupJobInfo fromCatalog(long backupTime, String label, String dbName, long dbId,
                                            BackupContent content, BackupMeta backupMeta,
                                            Map<Long, SnapshotInfo> snapshotInfos, Map<Long, Long> tableCommitSeqMap) {

        BackupJobInfo jobInfo = new BackupJobInfo();
        jobInfo.backupTime = backupTime;
        jobInfo.name = label;
        jobInfo.dbName = dbName;
        jobInfo.dbId = dbId;
        jobInfo.metaVersion = FeConstants.meta_version;
        jobInfo.content = content;
        jobInfo.tableCommitSeqMap = tableCommitSeqMap;
        jobInfo.majorVersion = Version.DORIS_BUILD_VERSION_MAJOR;
        jobInfo.minorVersion = Version.DORIS_BUILD_VERSION_MINOR;
        jobInfo.patchVersion = Version.DORIS_BUILD_VERSION_PATCH;
        jobInfo.isForceReplicationAllocation = !Config.force_olap_table_replication_allocation.isEmpty();

        Collection<Table> tbls = backupMeta.getTables().values();
        // tbls
        for (Table tbl : tbls) {
            if (tbl instanceof OlapTable) {
                OlapTable olapTbl = (OlapTable) tbl;
                BackupOlapTableInfo tableInfo = new BackupOlapTableInfo();
                tableInfo.id = tbl.getId();
                jobInfo.backupOlapTableObjects.put(tbl.getName(), tableInfo);
                // partitions
                for (Partition partition : olapTbl.getPartitions()) {
                    BackupPartitionInfo partitionInfo = new BackupPartitionInfo();
                    partitionInfo.id = partition.getId();
                    partitionInfo.version = partition.getVisibleVersion();
                    // a cluster that does not track the tokens records none (master only, the job info is logged),
                    // nor does a backup of the metadata only: it has no data the token could name
                    partitionInfo.stateToken = Config.restore_state_token_track && content == BackupContent.ALL
                            ? PartitionStateToken.effective(partition) : null;
                    tableInfo.partitions.put(partition.getName(), partitionInfo);
                    // indexes
                    for (MaterializedIndex index : partition.getMaterializedIndices(IndexExtState.VISIBLE)) {
                        BackupIndexInfo idxInfo = new BackupIndexInfo();
                        idxInfo.id = index.getId();
                        idxInfo.schemaHash = olapTbl.getSchemaHashByIndexId(index.getId());
                        partitionInfo.indexes.put(olapTbl.getIndexNameById(index.getId()), idxInfo);
                        // tablets
                        if (content == BackupContent.METADATA_ONLY) {
                            for (Tablet tablet : index.getTablets()) {
                                idxInfo.tablets.put(tablet.getId(), Lists.newArrayList());
                            }
                        } else {
                            for (Tablet tablet : index.getTablets()) {
                                SnapshotInfo snapshotInfo = snapshotInfos.get(tablet.getId());
                                idxInfo.tablets.put(tablet.getId(),
                                        Lists.newArrayList(snapshotInfo.getFiles()));
                                jobInfo.tabletBeMap.put(tablet.getId(), snapshotInfo.getBeId());
                                jobInfo.tabletSnapshotPathMap.put(tablet.getId(), snapshotInfo.getPath());
                            }
                        }
                        idxInfo.tabletsOrder.addAll(index.getTabletIdsInOrder());
                    }
                }
            } else if (tbl instanceof View) {
                View view = (View) tbl;
                BackupViewInfo backupViewInfo = new BackupViewInfo();
                backupViewInfo.id = view.getId();
                backupViewInfo.name = view.getName();
                jobInfo.newBackupObjects.views.add(backupViewInfo);
            } else if (tbl instanceof OdbcTable) {
                OdbcTable odbcTable = (OdbcTable) tbl;
                BackupOdbcTableInfo backupOdbcTableInfo = new BackupOdbcTableInfo();
                backupOdbcTableInfo.id = odbcTable.getId();
                backupOdbcTableInfo.dorisTableName = odbcTable.getName();
                backupOdbcTableInfo.linkedOdbcDatabaseName = odbcTable.getOdbcDatabaseName();
                backupOdbcTableInfo.linkedOdbcTableName = odbcTable.getOdbcTableName();
                if (odbcTable.getOdbcCatalogResourceName() != null) {
                    backupOdbcTableInfo.resourceName = odbcTable.getOdbcCatalogResourceName();
                } else {
                    backupOdbcTableInfo.host = odbcTable.getHost();
                    backupOdbcTableInfo.port = odbcTable.getPort();
                    backupOdbcTableInfo.user = odbcTable.getUserName();
                    backupOdbcTableInfo.driver = odbcTable.getOdbcDriver();
                    backupOdbcTableInfo.odbcType = odbcTable.getOdbcTableTypeName();
                }
                jobInfo.newBackupObjects.odbcTables.add(backupOdbcTableInfo);
            }
        }
        // resources
        Collection<Resource> resources = backupMeta.getResourceNameMap().values();
        for (Resource resource : resources) {
            if (resource instanceof OdbcCatalogResource) {
                OdbcCatalogResource odbcCatalogResource = (OdbcCatalogResource) resource;
                BackupOdbcResourceInfo backupOdbcResourceInfo = new BackupOdbcResourceInfo();
                backupOdbcResourceInfo.name = odbcCatalogResource.getName();
                jobInfo.newBackupObjects.odbcResources.add(backupOdbcResourceInfo);
            }
        }

        return jobInfo;
    }

    /**
     * Record the manifest roots of all the tablets reported by the backends, see SnapshotInfo.getManifestRoot().
     * All or nothing: if any tablet has no manifest root (e.g. an old backend), no manifest is written for the whole
     * job, the same as a backup of an old version.
     *
     * @param snapshotInfos tablet id -> snapshot info
     * @param digestAlgorithm the digest algorithm of the files in the manifests, DIGEST_SHA256 or DIGEST_NONE
     * @return true if the manifest is written
     */
    public boolean buildManifest(Map<Long, SnapshotInfo> snapshotInfos, String digestAlgorithm) {
        clearManifest();
        if (content == BackupContent.METADATA_ONLY) {
            return false;
        }

        Map<BackupIndexInfo, List<String>> built = Maps.newHashMap();
        for (BackupOlapTableInfo tblInfo : backupOlapTableObjects.values()) {
            for (BackupPartitionInfo partInfo : tblInfo.partitions.values()) {
                for (BackupIndexInfo idxInfo : partInfo.indexes.values()) {
                    List<Long> tabletIds = idxInfo.getSortedTabletIds();
                    List<String> roots = Lists.newArrayListWithCapacity(tabletIds.size());
                    built.put(idxInfo, roots);
                    for (Long tabletId : tabletIds) {
                        SnapshotInfo info = snapshotInfos.get(tabletId);
                        String root = info == null ? null : info.getManifestRoot();
                        if (!isValidManifestRoot(root)) {
                            LOG.info("no manifest for backup {}, tablet {} has no valid manifest root: {}",
                                    name, tabletId, root);
                            return false;
                        }
                        roots.add(root.toLowerCase());
                    }
                }
            }
        }
        for (Map.Entry<BackupIndexInfo, List<String>> entry : built.entrySet()) {
            entry.getKey().setManifestRoots(entry.getValue().isEmpty() ? null : entry.getValue());
        }
        manifestVersion = MANIFEST_VERSION;
        this.digestAlgorithm = digestAlgorithm;
        return true;
    }

    /**
     * Record the logical digest of every tablet that was asked for one, see TLogicalDigest. Unlike the manifest, it
     * is per tablet: a tablet without a digest (NOT_SUPPORTED, failed, or reported by an old backend) records the
     * reason code, the other tablets are not affected. A tablet which was not asked for a digest (the snapshot info
     * has none, e.g. a unique table) records nothing. The schema signature and the algorithm version are recorded
     * once for an index.
     *
     * @param snapshotInfos tablet id -> snapshot info
     * @return the number of tablets with a digest
     */
    public int buildLogicalDigests(Map<Long, SnapshotInfo> snapshotInfos) {
        clearLogicalDigests();
        if (content == BackupContent.METADATA_ONLY) {
            return 0;
        }
        int withDigest = 0;
        for (BackupOlapTableInfo tblInfo : backupOlapTableObjects.values()) {
            for (BackupPartitionInfo partInfo : tblInfo.partitions.values()) {
                for (BackupIndexInfo idxInfo : partInfo.indexes.values()) {
                    List<Long> tabletIds = idxInfo.getSortedTabletIds();
                    for (int pos = 0; pos < tabletIds.size(); pos++) {
                        SnapshotInfo info = snapshotInfos.get(tabletIds.get(pos));
                        LogicalDigestInfo digest;
                        if (info == null) {
                            digest = LogicalDigestInfo.none(LogicalDigestInfo.REASON_NO_REPORT);
                        } else {
                            digest = info.getLogicalDigest();
                            if (digest == null) {
                                // not asked for a digest
                                continue;
                            }
                        }
                        idxInfo.putLogicalDigestAt(pos, digest);
                    }
                    withDigest += idxInfo.countLogicalDigests();
                }
            }
        }
        return withDigest;
    }

    /**
     * Record which tablets have a decomposed digest file, see SnapshotInfo.getPrefixDigestRoot(). The file is a
     * file of the manifest of the tablet, so it is only recorded if the job has a manifest: without it there is
     * nowhere to locate and check the file. Per tablet and optional: a tablet without a file records nothing, and
     * the other tablets are not affected.
     *
     * @param snapshotInfos tablet id -> snapshot info
     * @return the number of tablets with a decomposed digest file
     */
    public int buildPrefixDigests(Map<Long, SnapshotInfo> snapshotInfos) {
        clearPrefixDigests();
        if (content == BackupContent.METADATA_ONLY || !hasManifest()) {
            return 0;
        }
        int count = 0;
        for (BackupOlapTableInfo tblInfo : backupOlapTableObjects.values()) {
            for (BackupPartitionInfo partInfo : tblInfo.partitions.values()) {
                for (BackupIndexInfo idxInfo : partInfo.indexes.values()) {
                    List<Long> tabletIds = idxInfo.getSortedTabletIds();
                    StringBuilder present = new StringBuilder(tabletIds.size());
                    int num = 0;
                    for (Long tabletId : tabletIds) {
                        SnapshotInfo info = snapshotInfos.get(tabletId);
                        if (info != null && isValidManifestRoot(info.getPrefixDigestRoot())) {
                            present.append('1');
                            num++;
                        } else {
                            present.append('0');
                        }
                    }
                    count += num;
                    idxInfo.prefixDigestPresent = num == 0 ? null : present.toString();
                }
            }
        }
        return count;
    }

    /** The number of tablets of the backup. */
    public long getTabletNum() {
        long num = 0;
        for (BackupOlapTableInfo tblInfo : backupOlapTableObjects.values()) {
            if (tblInfo == null) {
                continue;
            }
            for (BackupPartitionInfo partInfo : tblInfo.partitions.values()) {
                for (BackupIndexInfo idxInfo : partInfo.indexes.values()) {
                    num += idxInfo.tablets.size();
                }
            }
        }
        return num;
    }

    // The index of the partition, null if it is not in the backup.
    public BackupIndexInfo getIndexInfo(long partitionId, long indexId) {
        Map<Long, List<BackupIndexInfo>> lookup = partitionIndexes;
        if (lookup == null) {
            lookup = Maps.newHashMap();
            for (BackupOlapTableInfo tblInfo : backupOlapTableObjects.values()) {
                if (tblInfo == null) {
                    continue;
                }
                for (BackupPartitionInfo partInfo : tblInfo.partitions.values()) {
                    lookup.put(partInfo.id, Lists.newArrayList(partInfo.indexes.values()));
                }
            }
            partitionIndexes = lookup;
        }
        List<BackupIndexInfo> indexes = lookup.get(partitionId);
        if (indexes != null) {
            for (BackupIndexInfo idxInfo : indexes) {
                if (idxInfo.id == indexId) {
                    return idxInfo;
                }
            }
        }
        return null;
    }

    // Whether the decomposed digest file of a tablet in the backup can be located by its manifest (the manifest of
    // the backup has it), otherwise its SHA-256 is in the job info (BackupIndexInfo.getPrefixDigestRootAt).
    public boolean isPrefixDigestInManifest() {
        return manifestVersion != null && manifestVersion >= MANIFEST_VERSION;
    }

    private void clearPrefixDigests() {
        for (BackupOlapTableInfo tblInfo : backupOlapTableObjects.values()) {
            for (BackupPartitionInfo partInfo : tblInfo.partitions.values()) {
                for (BackupIndexInfo idxInfo : partInfo.indexes.values()) {
                    idxInfo.clearPrefixDigests();
                }
            }
        }
    }

    // Returns the logical digest of a tablet in the backup, put together from its index, null if the backup has none
    // for the tablet.
    public LogicalDigestInfo getLogicalDigest(long partitionId, long indexId, long tabletId) {
        BackupIndexInfo idxInfo = getIndexInfo(partitionId, indexId);
        return idxInfo == null ? null : idxInfo.getLogicalDigest(tabletId);
    }

    private void clearLogicalDigests() {
        logicalDigestSkipReason = null;
        for (BackupOlapTableInfo tblInfo : backupOlapTableObjects.values()) {
            for (BackupPartitionInfo partInfo : tblInfo.partitions.values()) {
                for (BackupIndexInfo idxInfo : partInfo.indexes.values()) {
                    idxInfo.clearLogicalDigests();
                }
            }
        }
    }

    // a lower or upper case hex SHA-256
    private static boolean isValidManifestRoot(String root) {
        return root != null && root.length() == 64 && root.chars().allMatch(c -> Character.digit(c, 16) >= 0);
    }

    /**
     * Whether this backup has a manifest that this version can check. A manifest of an unknown (newer) version
     * is not checked, never fails a restore.
     */
    public boolean hasManifest() {
        return manifestVersion != null
                && (manifestVersion == MANIFEST_VERSION || manifestVersion == MANIFEST_VERSION_V1);
    }

    /** The cluster id of the source cluster, {@link RestoreLineage#UNKNOWN_CLUSTER_ID} for an old backup. */
    public long getSrcClusterIdOrUnknown() {
        return srcClusterId == null ? RestoreLineage.UNKNOWN_CLUSTER_ID : srcClusterId;
    }

    // Returns the manifest root of a tablet in the backup, null if absent.
    public String getManifestRoot(long partitionId, long indexId, long tabletId) {
        if (!hasManifest()) {
            return null;
        }
        BackupIndexInfo idxInfo = getIndexInfo(partitionId, indexId);
        return idxInfo == null ? null : idxInfo.getManifestRoot(tabletId);
    }

    private void clearManifest() {
        manifestVersion = null;
        digestAlgorithm = null;
        for (BackupOlapTableInfo tblInfo : backupOlapTableObjects.values()) {
            for (BackupPartitionInfo partInfo : tblInfo.partitions.values()) {
                for (BackupIndexInfo idxInfo : partInfo.indexes.values()) {
                    idxInfo.clearManifestRoots();
                }
            }
        }
    }

    public static BackupJobInfo fromFile(String path) throws IOException {
        try (InputStream inputStream = Files.newInputStream(Paths.get(path))) {
            return fromInputStream(inputStream);
        }
    }

    public static BackupJobInfo genFromJson(String json) {
        /* parse the json string:
         * {
         *   "backup_time": 1522231864000,
         *   "name": "snapshot1",
         *   "database": "db1"
         *   "id": 10000
         *   "backup_result": "succeed",
         *   "meta_version" : 40 // this is optional
         *   "backup_objects": {
         *       "table1": {
         *           "partitions": {
         *               "partition2": {
         *                   "indexes": {
         *                       "rollup1": {
         *                           "id": 10009,
         *                           "schema_hash": 3473401,
         *                           "tablets": {
         *                               "10008": ["__10029_seg1.dat", "__10029_seg2.dat"],
         *                               "10007": ["__10030_seg1.dat", "__10030_seg2.dat"]
         *                           },
         *                           "tablets_order": ["10007", "10008"]
         *                       },
         *                       "table1": {
         *                           "id": 10008,
         *                           "schema_hash": 9845021,
         *                           "tablets": {
         *                               "10004": ["__10027_seg1.dat", "__10027_seg2.dat"],
         *                               "10005": ["__10028_seg1.dat", "__10028_seg2.dat"]
         *                           },
         *                           "tablets_order": ["10004, "10005"]
         *                       }
         *                   },
         *                   "id": 10007
         *                   "version": 10
         *                   "version_hash": 1273047329538
         *               },
         *           },
         *           "id": 10001
         *       }
         *   },
         *   "new_backup_objects": {
         *       "views": [
         *           {"id": 1,
         *            "name": "view1"
         *           }
         *       ],
         *       "odbc_tables": [
         *           {"id": 2,
         *            "doris_table_name": "oracle1",
         *            "linked_odbc_database_name": "external_db1",
         *            "linked_odbc_table_name": "external_table1",
         *            "resource_name": "bj_oracle"
         *           }
         *       ],
         *       "odbc_resources": [
         *           {"name": "bj_oracle"}
         *       ]
         *   }
         * }
         */
        BackupJobInfo jobInfo = GsonUtils.GSON.fromJson(json, BackupJobInfo.class);
        return jobInfo;
    }

    public static BackupJobInfo fromInputStream(InputStream inputStream) throws IOException {
        try (InputStreamReader reader = new InputStreamReader(inputStream, StandardCharsets.UTF_8)) {
            return GsonUtils.GSON.fromJson(reader, BackupJobInfo.class);
        }
    }

    public void writeToFile(File jobInfoFile) throws IOException {
        // stream the json directly to the file instead of materializing the whole json
        // String of a huge job info in memory; the file content is unchanged
        // Buffer the json writer: JsonWriter emits per-token fragments, and an unbuffered
        // StreamEncoder allocates a scratch char[]/CharBuffer on every single write call.
        try (Writer writer = new BufferedWriter(
                new OutputStreamWriter(new FileOutputStream(jobInfoFile), StandardCharsets.UTF_8))) {
            GsonUtils.GSON.toJson(this, writer);
        }
    }

    // Only return basic info, table and partitions
    public String getBrief() {
        BriefBackupJobInfo briefBackupJobInfo = BriefBackupJobInfo.fromBackupJobInfo(this);
        Gson gson = GsonUtils.GSON_PRETTY_PRINTING;
        return gson.toJson(briefBackupJobInfo);
    }

    public String toJson(boolean prettyPrinting) {
        Gson gson;
        if (prettyPrinting) {
            gson = GsonUtils.GSON_PRETTY_PRINTING;
        } else {
            gson = GsonUtils.GSON;
        }
        return gson.toJson(this);
    }

    public String getInfo() {
        return getBrief();
    }

    public void releaseSnapshotInfo() {
        tabletBeMap.clear();
        tabletSnapshotPathMap.clear();
        for (BackupOlapTableInfo tableInfo : backupOlapTableObjects.values()) {
            for (BackupPartitionInfo partInfo : tableInfo.partitions.values()) {
                for (BackupIndexInfo indexInfo : partInfo.indexes.values()) {
                    for (BackupTabletInfo tabletInfo : indexInfo.sortedTabletInfoList) {
                        tabletInfo.files.clear();
                    }
                    // The manifest roots are only needed for downloading. Keep manifest_version and
                    // digest_algorithm, they are shown in SHOW RESTORE.
                    indexInfo.clearManifestRoots();
                    indexInfo.clearLogicalDigests();
                    indexInfo.clearPrefixDigests();
                    indexInfo.positions = null;
                }
            }
        }
        partitionIndexes = null;
    }

    /**
     * Release the logical digests and the decomposed digest marks, which a restore only needs to decide the reuse of
     * partitions, except the logical digests of the given tablets (those that are sent with the incremental append,
     * see RestoreJob#getBackupLogicalDigest). The manifest roots are kept, the download needs them.
     */
    public void releaseDigests(Set<Long> keepLogicalDigestTablets) {
        for (BackupOlapTableInfo tableInfo : backupOlapTableObjects.values()) {
            for (BackupPartitionInfo partInfo : tableInfo.partitions.values()) {
                for (BackupIndexInfo indexInfo : partInfo.indexes.values()) {
                    indexInfo.clearPrefixDigests();
                    if (keepLogicalDigestTablets.isEmpty()) {
                        indexInfo.clearLogicalDigests();
                    } else {
                        indexInfo.retainLogicalDigests(keepLogicalDigestTablets);
                    }
                }
            }
        }
    }

    /**
     * Release the manifest roots, which are only needed to download. manifest_version and digest_algorithm are kept,
     * they are shown in SHOW RESTORE.
     */
    public void releaseManifestRoots() {
        for (BackupOlapTableInfo tableInfo : backupOlapTableObjects.values()) {
            for (BackupPartitionInfo partInfo : tableInfo.partitions.values()) {
                for (BackupIndexInfo indexInfo : partInfo.indexes.values()) {
                    indexInfo.clearManifestRoots();
                }
            }
        }
    }

    public static BackupJobInfo read(DataInput in) throws IOException {
        if (Env.getCurrentEnvJournalVersion() < FeMetaVersion.VERSION_135) {
            return BackupJobInfo.readFields(in);
        }
        String json = Text.readString(in);
        return genFromJson(json);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, toJson(false));
    }

    @Override
    public void gsonPostProcess() throws IOException {
        initBackupJobInfoAfterDeserialize();
    }

    public static BackupJobInfo readFields(DataInput in) throws IOException {
        String json = Text.readString(in);
        BackupJobInfo backupJobInfo = genFromJson(json);
        int size = in.readInt();
        for (int i = 0; i < size; i++) {
            String tbl = Text.readString(in);
            String alias = Text.readString(in);
            backupJobInfo.tblAlias.put(tbl, alias);
        }
        return backupJobInfo;
    }

    public String toString() {
        return toJson(true);
    }
}
