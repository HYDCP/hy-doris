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
import org.apache.doris.common.FeConstants;
import org.apache.doris.nereids.trees.plans.commands.BackupCommand.BackupContent;
import org.apache.doris.task.RestoreDigestTask;
import org.apache.doris.task.SnapshotTask;
import org.apache.doris.thrift.TLogicalDigest;
import org.apache.doris.thrift.TRestoreDigestReq;
import org.apache.doris.thrift.TSnapshotRequest;
import org.apache.doris.thrift.TTaskType;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Map;

public class LogicalDigestTest {
    private static final long TABLET_1 = 101;
    private static final long TABLET_2 = 102;
    private static final long TABLET_3 = 103;
    private static final String MD5 = "4f158689243a3d6030352fec3cfd3798";

    private static String hex(char c) {
        return String.valueOf(c).repeat(64);
    }

    private static BackupJobInfo newJobInfo(long... tabletIds) {
        BackupJobInfo jobInfo = new BackupJobInfo();
        jobInfo.name = "snapshot_1";
        jobInfo.dbName = "src_db";
        jobInfo.dbId = 1;
        jobInfo.backupTime = 1700000000000L;
        jobInfo.success = true;
        jobInfo.content = BackupContent.ALL;
        jobInfo.metaVersion = FeConstants.meta_version;
        BackupOlapTableInfo tblInfo = new BackupOlapTableInfo();
        tblInfo.id = 2;
        BackupPartitionInfo partInfo = new BackupPartitionInfo();
        partInfo.id = 3;
        partInfo.version = 10;
        BackupIndexInfo idxInfo = new BackupIndexInfo();
        idxInfo.id = 4;
        idxInfo.schemaHash = 5;
        for (long tabletId : tabletIds) {
            idxInfo.tablets.put(tabletId, Lists.newArrayList("1.dat." + MD5, tabletId + ".hdr." + MD5));
            idxInfo.tabletsOrder.add(tabletId);
        }
        partInfo.indexes.put("tbl", idxInfo);
        tblInfo.partitions.put("p1", partInfo);
        jobInfo.backupOlapTableObjects.put("tbl", tblInfo);
        return jobInfo;
    }

    private static SnapshotInfo snapshotInfo(long tabletId, LogicalDigestInfo digest) {
        SnapshotInfo info = new SnapshotInfo(1, 2, 3, 4, tabletId, 10001, 5, "/path",
                Lists.newArrayList("1.dat." + MD5, tabletId + ".hdr." + MD5));
        info.setLogicalDigest(digest);
        return info;
    }

    private static BackupIndexInfo index(BackupJobInfo jobInfo) {
        return jobInfo.backupOlapTableObjects.get("tbl").partitions.get("p1").indexes.get("tbl");
    }

    private static BackupJobInfo jobInfoWithDigests() {
        BackupJobInfo jobInfo = newJobInfo(TABLET_1, TABLET_2, TABLET_3);
        Map<Long, SnapshotInfo> infos = Maps.newHashMap();
        infos.put(TABLET_1, snapshotInfo(TABLET_1, LogicalDigestInfo.of(1, hex('5'), hex('a'))));
        infos.put(TABLET_2, snapshotInfo(TABLET_2, LogicalDigestInfo.none(LogicalDigestInfo.REASON_NOT_SUPPORTED)));
        // TABLET_3 has no snapshot info digest at all
        infos.put(TABLET_3, snapshotInfo(TABLET_3, null));
        Assertions.assertEquals(1, jobInfo.buildLogicalDigests(infos));
        return jobInfo;
    }

    @Test
    public void testFromThrift() {
        TLogicalDigest ok = new TLogicalDigest();
        ok.setAlgoVersion(1);
        ok.setSchemaSig(hex('5'));
        ok.setRoot(hex('A'));
        ok.setRows(7);
        ok.setStatusCode("OK");
        LogicalDigestInfo info = LogicalDigestInfo.fromThrift(ok);
        Assertions.assertTrue(info.hasDigest());
        Assertions.assertEquals(1, (int) info.algoVersion);
        Assertions.assertEquals(hex('5'), info.schemaSig);
        // normalized to lower case
        Assertions.assertEquals(hex('a'), info.root);
        Assertions.assertNull(info.reason);

        TLogicalDigest notSupported = new TLogicalDigest();
        notSupported.setStatusCode("NOT_SUPPORTED");
        notSupported.setStatusMsg("restore digest: keys type 2 is not supported");
        info = LogicalDigestInfo.fromThrift(notSupported);
        Assertions.assertFalse(info.hasDigest());
        Assertions.assertEquals("", info.root);
        Assertions.assertEquals("NOT_SUPPORTED", info.reason);

        TLogicalDigest failed = new TLogicalDigest();
        failed.setStatusCode("ERROR");
        Assertions.assertEquals("ERROR", LogicalDigestInfo.fromThrift(failed).reason);
        // OK without a root is not a digest
        TLogicalDigest broken = new TLogicalDigest();
        broken.setStatusCode("OK");
        Assertions.assertFalse(LogicalDigestInfo.fromThrift(broken).hasDigest());
        // not reported (an old backend)
        Assertions.assertEquals("NO_REPORT", LogicalDigestInfo.fromThrift(null).reason);
    }

    @Test
    public void testBuildLogicalDigests() {
        BackupJobInfo jobInfo = jobInfoWithDigests();
        LogicalDigestInfo d1 = jobInfo.getLogicalDigest(TABLET_1);
        Assertions.assertTrue(d1.hasDigest());
        Assertions.assertEquals(hex('a'), d1.root);
        LogicalDigestInfo d2 = jobInfo.getLogicalDigest(TABLET_2);
        Assertions.assertFalse(d2.hasDigest());
        Assertions.assertEquals("NOT_SUPPORTED", d2.reason);
        // an entry per tablet, the missing report is recorded as such
        Assertions.assertEquals("NO_REPORT", jobInfo.getLogicalDigest(TABLET_3).reason);
        Assertions.assertNull(jobInfo.getLogicalDigest(999));

        // metadata only: no files, no digests
        jobInfo.content = BackupContent.METADATA_ONLY;
        Assertions.assertEquals(0, jobInfo.buildLogicalDigests(Maps.newHashMap()));
        Assertions.assertNull(jobInfo.getLogicalDigest(TABLET_1));
    }

    @Test
    public void testGsonRoundTrip() {
        BackupJobInfo jobInfo = jobInfoWithDigests();
        String json = jobInfo.toJson(false);
        // short and stable names
        Assertions.assertTrue(json.contains("\"ld\":{"), json);
        Assertions.assertTrue(json.contains("\"101\":{\"a\":1,\"s\":\"" + hex('5') + "\",\"r\":\"" + hex('a')
                + "\"}"), json);
        Assertions.assertTrue(json.contains("\"102\":{\"r\":\"\",\"e\":\"NOT_SUPPORTED\"}"), json);
        Assertions.assertTrue(json.contains("\"103\":{\"r\":\"\",\"e\":\"NO_REPORT\"}"), json);
        // the index is a cache, not persisted
        Assertions.assertFalse(json.contains("logicalDigestIndex"), json);

        BackupJobInfo read = BackupJobInfo.genFromJson(json);
        Assertions.assertEquals(hex('a'), read.getLogicalDigest(TABLET_1).root);
        Assertions.assertEquals(hex('5'), read.getLogicalDigest(TABLET_1).schemaSig);
        Assertions.assertEquals(1, (int) read.getLogicalDigest(TABLET_1).algoVersion);
        Assertions.assertEquals("NOT_SUPPORTED", read.getLogicalDigest(TABLET_2).reason);
        // the BackupTabletInfo view
        BackupIndexInfo idxInfo = index(read);
        Assertions.assertEquals(3, idxInfo.sortedTabletInfoList.size());
        Assertions.assertEquals(hex('a'), idxInfo.sortedTabletInfoList.get(0).logicalDigest.root);
        // written again, the same
        Assertions.assertEquals(json, read.toJson(false));

        // released after the restore
        read.releaseSnapshotInfo();
        Assertions.assertNull(read.getLogicalDigest(TABLET_1));
        Assertions.assertNull(idxInfo.sortedTabletInfoList.get(0).logicalDigest);
        Assertions.assertFalse(read.toJson(false).contains("\"ld\""));
    }

    @Test
    public void testReadOldJobInfo() {
        // a job info written by an old version, without any digest field.
        String json = "{\"name\":\"snapshot_1\",\"database\":\"src_db\",\"id\":1,\"backup_time\":1700000000000,"
                + "\"content\":\"ALL\",\"backup_objects\":{\"tbl\":{\"id\":2,\"partitions\":{\"p1\":{\"id\":3,"
                + "\"version\":10,\"indexes\":{\"tbl\":{\"id\":4,\"schema_hash\":5,\"tablets\":{\"101\":"
                + "[\"1.dat." + MD5 + "\",\"101.hdr." + MD5 + "\"]},\"tablets_order\":[101]}}}}}},"
                + "\"new_backup_objects\":{},\"backup_result\":\"succeed\",\"meta_version\":0,"
                + "\"tablet_be_map\":{},\"tablet_snapshot_path_map\":{}}";
        BackupJobInfo jobInfo = BackupJobInfo.genFromJson(json);
        Assertions.assertNull(jobInfo.getLogicalDigest(101));
        BackupIndexInfo idxInfo = index(jobInfo);
        Assertions.assertNull(idxInfo.logicalDigests);
        Assertions.assertNull(idxInfo.sortedTabletInfoList.get(0).logicalDigest);
        Assertions.assertEquals(2, idxInfo.sortedTabletInfoList.get(0).files.size());
        // written without new keys
        Assertions.assertFalse(jobInfo.toJson(false).contains("\"ld\""));
    }

    @Test
    public void testSnapshotInfoRoundTripAndOldFormat() {
        SnapshotInfo info = snapshotInfo(TABLET_1, LogicalDigestInfo.of(1, hex('5'), hex('a')));
        String json = com.google.gson.JsonParser.parseString(
                org.apache.doris.persist.gson.GsonUtils.GSON.toJson(info)).toString();
        Assertions.assertTrue(json.contains("\"ld\":{\"a\":1"), json);
        SnapshotInfo read = org.apache.doris.persist.gson.GsonUtils.GSON.fromJson(json, SnapshotInfo.class);
        Assertions.assertEquals(hex('a'), read.getLogicalDigest().root);
        // a SnapshotInfo written by an old version
        SnapshotInfo old = org.apache.doris.persist.gson.GsonUtils.GSON.fromJson(
                "{\"tab\":101,\"be\":10001,\"sh\":5,\"path\":\"/path\",\"f\":[\"1.dat\"]}",
                SnapshotInfo.class);
        Assertions.assertNull(old.getLogicalDigest());
        Assertions.assertEquals(101, old.getTabletId());
    }

    @Test
    public void testRestoreDigestTask() {
        RestoreDigestTask task = new RestoreDigestTask(10001, 777, 55, 1, 2, 3, 4, TABLET_1, 5, 10, 8);
        Assertions.assertEquals(TTaskType.RESTORE_DIGEST, task.getTaskType());
        Assertions.assertEquals(10001, task.getBackendId());
        // the signature identifies the task, replicas of one tablet have different ones
        Assertions.assertEquals(777, task.getSignature());
        Assertions.assertEquals(55, task.getJobId());
        Assertions.assertEquals(TABLET_1, task.getTabletId());
        Assertions.assertEquals(5, task.getSchemaHash());
        Assertions.assertEquals(10, task.getVersion());
        TRestoreDigestReq req = task.toThrift();
        Assertions.assertEquals(TABLET_1, req.getTabletId());
        Assertions.assertEquals(5, req.getSchemaHash());
        Assertions.assertEquals(10, req.getVersion());
        Assertions.assertTrue(req.isSetThreads());
        Assertions.assertEquals(8, req.getThreads());

        // no threads: the backend decides
        RestoreDigestTask defaultThreads = new RestoreDigestTask(10002, 778, 55, 1, 2, 3, 4, TABLET_1, 5, 10, 0);
        Assertions.assertFalse(defaultThreads.toThrift().isSetThreads());
        Assertions.assertEquals(TABLET_1, defaultThreads.getTabletId());
        Assertions.assertNotEquals(task.getSignature(), defaultThreads.getSignature());
    }

    @Test
    public void testSnapshotTaskFlag() {
        SnapshotTask task = new SnapshotTask(null, 10001, 1, 55, 1, 2, 3, 4, TABLET_1, 10, 5, 1000, false);
        Assertions.assertFalse(task.isComputeLogicalDigest());
        TSnapshotRequest req = task.toThrift();
        Assertions.assertFalse(req.isSetComputeLogicalDigest());
        task.setComputeLogicalDigest(true);
        req = task.toThrift();
        Assertions.assertTrue(req.isSetComputeLogicalDigest());
        Assertions.assertTrue(req.isComputeLogicalDigest());
        // independent of the manifest digest
        Assertions.assertFalse(req.isSetComputeDigest());
    }

    @Test
    public void testPrefixDigestRoots() {
        BackupJobInfo jobInfo = newJobInfo(TABLET_1, TABLET_2, TABLET_3);
        Map<Long, SnapshotInfo> infos = Maps.newHashMap();
        SnapshotInfo i1 = snapshotInfo(TABLET_1, LogicalDigestInfo.of(1, hex('5'), hex('a')));
        i1.setPrefixDigestRoot(hex('B'));
        infos.put(TABLET_1, i1);
        // no decomposed digest (unique MoR, ...)
        infos.put(TABLET_2, snapshotInfo(TABLET_2, LogicalDigestInfo.of(1, hex('5'), hex('c'))));
        // a root which is not a SHA-256 is not recorded
        SnapshotInfo i3 = snapshotInfo(TABLET_3, LogicalDigestInfo.of(1, hex('5'), hex('d')));
        i3.setPrefixDigestRoot("abc");
        infos.put(TABLET_3, i3);
        Assertions.assertEquals(1, jobInfo.buildPrefixDigests(infos));
        // lower case
        Assertions.assertEquals(hex('b'), jobInfo.getPrefixDigestRoot(TABLET_1));
        Assertions.assertNull(jobInfo.getPrefixDigestRoot(TABLET_2));
        Assertions.assertNull(jobInfo.getPrefixDigestRoot(TABLET_3));

        String json = jobInfo.toJson(false);
        // short and stable name, next to "ld"
        Assertions.assertTrue(json.contains("\"rd\":{\"101\":\"" + hex('b') + "\"}"), json);
        BackupJobInfo read = BackupJobInfo.genFromJson(json);
        Assertions.assertEquals(hex('b'), read.getPrefixDigestRoot(TABLET_1));
        Assertions.assertEquals(hex('b'), index(read).sortedTabletInfoList.get(0).prefixDigestRoot);
        Assertions.assertNull(index(read).sortedTabletInfoList.get(1).prefixDigestRoot);
        Assertions.assertEquals(json, read.toJson(false));
        read.releaseSnapshotInfo();
        Assertions.assertNull(read.getPrefixDigestRoot(TABLET_1));
        Assertions.assertFalse(read.toJson(false).contains("\"rd\""));

        // nobody has one: nothing is written
        Assertions.assertEquals(0, jobInfo.buildPrefixDigests(Maps.newHashMap()));
        Assertions.assertFalse(jobInfo.toJson(false).contains("\"rd\""));
        // metadata only: no files, no digests
        jobInfo.content = BackupContent.METADATA_ONLY;
        Assertions.assertEquals(0, jobInfo.buildPrefixDigests(infos));

        // the snapshot info keeps it in the journal, an old one has none
        SnapshotInfo withRoot = snapshotInfo(TABLET_1, null);
        withRoot.setPrefixDigestRoot(hex('b'));
        String infoJson = com.google.gson.JsonParser.parseString(
                org.apache.doris.persist.gson.GsonUtils.GSON.toJson(withRoot)).toString();
        Assertions.assertTrue(infoJson.contains("\"rd\":\"" + hex('b') + "\""), infoJson);
        Assertions.assertEquals(hex('b'),
                org.apache.doris.persist.gson.GsonUtils.GSON.fromJson(infoJson, SnapshotInfo.class)
                        .getPrefixDigestRoot());
        Assertions.assertNull(org.apache.doris.persist.gson.GsonUtils.GSON.fromJson(
                "{\"tab\":101,\"be\":10001,\"sh\":5,\"path\":\"/path\",\"f\":[\"1.dat\"]}",
                SnapshotInfo.class).getPrefixDigestRoot());
    }
}
