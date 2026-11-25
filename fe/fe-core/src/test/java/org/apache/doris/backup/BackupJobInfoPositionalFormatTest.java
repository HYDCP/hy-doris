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
import org.apache.doris.backup.BackupJobInfo.BackupIndexInfo;
import org.apache.doris.backup.BackupJobInfo.BackupOlapTableInfo;
import org.apache.doris.backup.BackupJobInfo.BackupPartitionInfo;
import org.apache.doris.backup.BackupJobInfo.BackupTabletInfo;
import org.apache.doris.common.FeConstants;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The per tablet data of the job info (manifest root, logical digest, decomposed digest) is kept in arrays aligned
 * with the tablets of the index, see BackupIndexInfo: the format that is written, the formats that are still read,
 * and what the memory structure does not have any more.
 */
public class BackupJobInfoPositionalFormatTest {
    private static final String MD5 = "4f158689243a3d6030352fec3cfd3798";
    private static final long PART = 3;
    private static final long IDX = 4;

    private static String hex(char c) {
        return Strings.repeat(String.valueOf(c), 64);
    }

    private static String idHex(long id) {
        return String.format("%064x", id);
    }

    // The tablets are in the given order, which may not be the order of the ids.
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
        partInfo.id = PART;
        partInfo.version = 10;
        BackupIndexInfo idxInfo = new BackupIndexInfo();
        idxInfo.id = IDX;
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

    private static BackupIndexInfo index(BackupJobInfo jobInfo) {
        return jobInfo.backupOlapTableObjects.get("tbl").partitions.get("p1").indexes.get("tbl");
    }

    private static SnapshotInfo snapshotInfo(long tabletId, String manifestRoot, LogicalDigestInfo digest,
            String prefixRoot) {
        SnapshotInfo info = new SnapshotInfo(1, 2, 3, 4, tabletId, 10001, 5, "/path",
                Lists.newArrayList("1.dat", tabletId + ".hdr"));
        info.setManifestRoot(manifestRoot);
        info.setLogicalDigest(digest);
        info.setPrefixDigestRoot(prefixRoot);
        return info;
    }

    // tablets 104, 102, 103, 101 in this order: 104 has a digest and a decomposed digest, 102 has no digest
    // (NOT_SUPPORTED), 103 was not asked for a digest, 101 has a digest and no decomposed digest.
    private static BackupJobInfo mixedJobInfo() {
        BackupJobInfo jobInfo = newJobInfo(104, 102, 103, 101);
        Map<Long, SnapshotInfo> infos = Maps.newHashMap();
        infos.put(104L, snapshotInfo(104, hex('4'), LogicalDigestInfo.of(1, hex('5'), hex('a')), hex('d')));
        infos.put(102L, snapshotInfo(102, hex('2'),
                LogicalDigestInfo.none(LogicalDigestInfo.REASON_NOT_SUPPORTED), null));
        infos.put(103L, snapshotInfo(103, hex('3'), null, null));
        infos.put(101L, snapshotInfo(101, hex('1'), LogicalDigestInfo.of(1, hex('5'), hex('b')), null));
        Assertions.assertTrue(jobInfo.buildManifest(infos, BackupJobInfo.DIGEST_SHA256));
        Assertions.assertEquals(2, jobInfo.buildLogicalDigests(infos));
        Assertions.assertEquals(1, jobInfo.buildPrefixDigests(infos));
        return jobInfo;
    }

    @Test
    public void testWriteAndReadAlignedWithTabletsOrder() {
        BackupJobInfo jobInfo = mixedJobInfo();
        Assertions.assertEquals(BackupJobInfo.MANIFEST_VERSION, (int) jobInfo.manifestVersion);
        String json = jobInfo.toJson(false);
        // position i is the i-th tablet of tablets_order
        Assertions.assertTrue(json.contains("\"tablets_order\":[104,102,103,101]"), json);
        Assertions.assertTrue(json.contains("\"mra\":[\"" + hex('4') + "\",\"" + hex('2') + "\",\"" + hex('3')
                + "\",\"" + hex('1') + "\"]"), json);
        Assertions.assertTrue(json.contains("\"lra\":[\"" + hex('a') + "\",\"\",\"\",\"" + hex('b') + "\"]"), json);
        Assertions.assertTrue(json.contains("\"lna\":[\"\",\"NOT_SUPPORTED\",\"\",\"\"]"), json);
        Assertions.assertTrue(json.contains("\"rdp\":\"1000\""), json);
        Assertions.assertTrue(json.contains("\"lds\":\"" + hex('5') + "\""), json);
        // none of the maps with a tablet id key, none of the names of the previous formats
        for (String old : new String[] {"manifest_roots", "\"ldr\"", "\"ldn\"", "\"ld\"", "\"rd\"", "\"rra\""}) {
            Assertions.assertFalse(json.contains(old), old + ": " + json);
        }

        BackupJobInfo read = BackupJobInfo.genFromJson(json);
        BackupIndexInfo idxInfo = index(read);
        Assertions.assertEquals(4, idxInfo.sortedTabletInfoList.size());
        Assertions.assertEquals(104, idxInfo.sortedTabletInfoList.get(0).id);
        Assertions.assertEquals(102, idxInfo.sortedTabletInfoList.get(1).id);
        Assertions.assertEquals(103, idxInfo.sortedTabletInfoList.get(2).id);
        Assertions.assertEquals(101, idxInfo.sortedTabletInfoList.get(3).id);
        // by position
        Assertions.assertEquals(hex('4'), idxInfo.getManifestRootAt(0));
        Assertions.assertEquals(hex('1'), idxInfo.getManifestRootAt(3));
        Assertions.assertNull(idxInfo.getManifestRootAt(4));
        Assertions.assertNull(idxInfo.getManifestRootAt(-1));
        Assertions.assertEquals(hex('a'), idxInfo.getLogicalDigestAt(0).root);
        Assertions.assertEquals(hex('5'), idxInfo.getLogicalDigestAt(0).schemaSig);
        Assertions.assertEquals("NOT_SUPPORTED", idxInfo.getLogicalDigestAt(1).reason);
        Assertions.assertNull(idxInfo.getLogicalDigestAt(2));
        Assertions.assertEquals(hex('b'), idxInfo.getLogicalDigestAt(3).root);
        Assertions.assertTrue(idxInfo.hasPrefixDigestAt(0));
        Assertions.assertFalse(idxInfo.hasPrefixDigestAt(1));
        Assertions.assertFalse(idxInfo.hasPrefixDigestAt(3));
        // by tablet id, which is not in the order of the ids
        Assertions.assertEquals(0, idxInfo.positionOf(104));
        Assertions.assertEquals(1, idxInfo.positionOf(102));
        Assertions.assertEquals(2, idxInfo.positionOf(103));
        Assertions.assertEquals(3, idxInfo.positionOf(101));
        Assertions.assertEquals(-1, idxInfo.positionOf(999));
        Assertions.assertEquals(hex('2'), idxInfo.getManifestRoot(102));
        Assertions.assertEquals(hex('b'), idxInfo.getLogicalDigest(101).root);
        Assertions.assertNull(idxInfo.getLogicalDigest(103));
        Assertions.assertNull(idxInfo.getLogicalDigest(999));
        Assertions.assertTrue(idxInfo.hasPrefixDigest(104));
        Assertions.assertFalse(idxInfo.hasPrefixDigest(101));
        // by the ids of the partition and the index
        Assertions.assertSame(idxInfo, read.getIndexInfo(PART, IDX));
        Assertions.assertNull(read.getIndexInfo(PART, IDX + 1));
        Assertions.assertNull(read.getIndexInfo(PART + 1, IDX));
        Assertions.assertEquals(hex('3'), read.getManifestRoot(PART, IDX, 103));
        Assertions.assertNull(read.getManifestRoot(PART, IDX, 999));
        Assertions.assertNull(read.getManifestRoot(PART + 1, IDX, 103));
        Assertions.assertEquals(hex('a'), read.getLogicalDigest(PART, IDX, 104).root);
        // written again, the same
        Assertions.assertEquals(json, read.toJson(false));
    }

    @Test
    public void testTabletsInTheOrderOfTheIds() {
        // the common case: the order of the tablets is the order of the ids
        BackupJobInfo jobInfo = newJobInfo(101, 102, 103);
        Map<Long, SnapshotInfo> infos = Maps.newHashMap();
        for (long id = 101; id <= 103; id++) {
            infos.put(id, snapshotInfo(id, idHex(id), null, null));
        }
        Assertions.assertTrue(jobInfo.buildManifest(infos, BackupJobInfo.DIGEST_NONE));
        BackupJobInfo read = BackupJobInfo.genFromJson(jobInfo.toJson(false));
        for (long id = 101; id <= 103; id++) {
            Assertions.assertEquals(idHex(id), read.getManifestRoot(PART, IDX, id));
            Assertions.assertEquals((int) (id - 101), index(read).positionOf(id));
        }
        Assertions.assertEquals(-1, index(read).positionOf(100));
        Assertions.assertEquals(-1, index(read).positionOf(104));
    }

    @Test
    public void testNoTabletsOrderUsesTheSortedIds() {
        // a backup of the version which did not save tablets_order: the tablets are in the order of the ids
        BackupJobInfo jobInfo = newJobInfo(103, 101, 102);
        BackupIndexInfo written = index(jobInfo);
        written.tabletsOrder = Lists.newArrayList();
        written.manifestRoots = Maps.newHashMap();
        for (long id = 101; id <= 103; id++) {
            written.manifestRoots.put(id, idHex(id));
        }
        jobInfo.manifestVersion = BackupJobInfo.MANIFEST_VERSION_V1;
        BackupJobInfo read = BackupJobInfo.genFromJson(jobInfo.toJson(false));
        BackupIndexInfo idxInfo = index(read);
        Assertions.assertEquals(101, idxInfo.sortedTabletInfoList.get(0).id);
        Assertions.assertEquals(idHex(101), idxInfo.getManifestRootAt(0));
        Assertions.assertEquals(idHex(103), idxInfo.getManifestRootAt(2));
        Assertions.assertEquals(2, idxInfo.positionOf(103));
        Assertions.assertEquals(idHex(102), read.getManifestRoot(PART, IDX, 102));
    }

    @Test
    public void testReadTheFormatOfThePreviousVersion() {
        // manifest_roots, ldr/ldn/lds/lda and rd, all maps with a tablet id key, written with manifest_version 1.
        // 104: digest, rdigest. 102: no digest. 103: not asked. 101: digest, no rdigest.
        BackupJobInfo jobInfo = newJobInfo(104, 102, 103, 101);
        BackupIndexInfo idxInfo = index(jobInfo);
        jobInfo.manifestVersion = BackupJobInfo.MANIFEST_VERSION_V1;
        jobInfo.digestAlgorithm = BackupJobInfo.DIGEST_SHA256;
        idxInfo.manifestRoots = Maps.newHashMap();
        for (long id : new long[] {104, 102, 103, 101}) {
            idxInfo.manifestRoots.put(id, idHex(id));
        }
        idxInfo.logicalDigestRoots = Maps.newHashMap();
        idxInfo.logicalDigestRoots.put(104L, hex('a'));
        idxInfo.logicalDigestRoots.put(101L, hex('b'));
        idxInfo.logicalDigestNone = Maps.newHashMap();
        idxInfo.logicalDigestNone.put(102L, LogicalDigestInfo.REASON_NOT_SUPPORTED);
        idxInfo.logicalDigestSchemaSig = hex('5');
        idxInfo.logicalDigestAlgo = 1;
        idxInfo.prefixDigestRoots = Maps.newHashMap();
        idxInfo.prefixDigestRoots.put(104L, hex('d'));
        String json = jobInfo.toJson(false);
        Assertions.assertTrue(json.contains("\"manifest_roots\":{"), json);
        Assertions.assertTrue(json.contains("\"ldr\":{"), json);
        Assertions.assertTrue(json.contains("\"rd\":{\"104\":\"" + hex('d') + "\"}"), json);
        Assertions.assertFalse(json.contains("\"mra\""), json);

        BackupJobInfo read = BackupJobInfo.genFromJson(json);
        Assertions.assertTrue(read.hasManifest());
        Assertions.assertFalse(read.isPrefixDigestInManifest());
        BackupIndexInfo readIdx = index(read);
        Assertions.assertEquals(idHex(102), read.getManifestRoot(PART, IDX, 102));
        Assertions.assertEquals(idHex(104), readIdx.getManifestRootAt(0));
        Assertions.assertEquals(hex('a'), read.getLogicalDigest(PART, IDX, 104).root);
        Assertions.assertEquals(hex('5'), read.getLogicalDigest(PART, IDX, 104).schemaSig);
        Assertions.assertEquals("NOT_SUPPORTED", read.getLogicalDigest(PART, IDX, 102).reason);
        Assertions.assertNull(read.getLogicalDigest(PART, IDX, 103));
        Assertions.assertEquals(hex('b'), read.getLogicalDigest(PART, IDX, 101).root);
        // the roots of the decomposed digest are in the job info for this version, only some tablets have one
        Assertions.assertTrue(readIdx.hasPrefixDigestAt(0));
        Assertions.assertEquals(hex('d'), readIdx.getPrefixDigestRootAt(0));
        Assertions.assertFalse(readIdx.hasPrefixDigestAt(1));
        Assertions.assertFalse(readIdx.hasPrefixDigestAt(3));
        Assertions.assertNull(readIdx.getPrefixDigestRootAt(3));
        // the maps are gone, the arrays are what is kept, and what is written
        Assertions.assertNull(readIdx.manifestRoots);
        Assertions.assertNull(readIdx.logicalDigestRoots);
        Assertions.assertNull(readIdx.logicalDigestNone);
        Assertions.assertNull(readIdx.logicalDigests);
        Assertions.assertNull(readIdx.prefixDigestRoots);
        Assertions.assertNotNull(readIdx.manifestRootArray);
        String again = read.toJson(false);
        Assertions.assertTrue(again.contains("\"mra\":["), again);
        Assertions.assertTrue(again.contains("\"lra\":["), again);
        Assertions.assertTrue(again.contains("\"rra\":[\"" + hex('d') + "\",\"\",\"\",\"\"]"), again);
        Assertions.assertTrue(again.contains("\"manifest_version\":1"), again);
        for (String old : new String[] {"manifest_roots", "\"ldr\"", "\"ldn\"", "\"rd\""}) {
            Assertions.assertFalse(again.contains(old), old + ": " + again);
        }
        // and read back the same (a restore job logs its job info and replays it)
        BackupJobInfo replayed = BackupJobInfo.genFromJson(again);
        Assertions.assertEquals(again, replayed.toJson(false));
        Assertions.assertEquals(hex('d'), index(replayed).getPrefixDigestRootAt(0));
        Assertions.assertEquals(hex('a'), replayed.getLogicalDigest(PART, IDX, 104).root);
        Assertions.assertEquals(4, index(replayed).sortedTabletInfoList.size());
    }

    @Test
    public void testReadMixedOfLdAndLdr() {
        // "ldr"/"ldn" win over the oldest "ld", tablet by tablet
        BackupJobInfo jobInfo = newJobInfo(101, 102, 103);
        BackupIndexInfo idxInfo = index(jobInfo);
        idxInfo.logicalDigestSchemaSig = hex('5');
        idxInfo.logicalDigestAlgo = 1;
        idxInfo.logicalDigestRoots = Maps.newHashMap();
        idxInfo.logicalDigestRoots.put(101L, hex('a'));
        idxInfo.logicalDigests = Maps.newHashMap();
        idxInfo.logicalDigests.put(101L, LogicalDigestInfo.of(1, hex('5'), hex('c')));
        idxInfo.logicalDigests.put(102L, LogicalDigestInfo.of(1, hex('5'), hex('d')));
        // another schema than the index: not put together with the index
        idxInfo.logicalDigests.put(103L, LogicalDigestInfo.of(1, hex('6'), hex('e')));
        BackupJobInfo read = BackupJobInfo.genFromJson(jobInfo.toJson(false));
        Assertions.assertEquals(hex('a'), read.getLogicalDigest(PART, IDX, 101).root);
        Assertions.assertEquals(hex('d'), read.getLogicalDigest(PART, IDX, 102).root);
        Assertions.assertEquals(LogicalDigestInfo.REASON_MIXED_SCHEMA, read.getLogicalDigest(PART, IDX, 103).reason);
    }

    @Test
    public void testManifestVersions() {
        BackupJobInfo jobInfo = newJobInfo(101);
        jobInfo.manifestVersion = null;
        Assertions.assertFalse(jobInfo.hasManifest());
        Assertions.assertFalse(jobInfo.isPrefixDigestInManifest());
        jobInfo.manifestVersion = BackupJobInfo.MANIFEST_VERSION_V1;
        Assertions.assertTrue(jobInfo.hasManifest());
        Assertions.assertFalse(jobInfo.isPrefixDigestInManifest());
        jobInfo.manifestVersion = BackupJobInfo.MANIFEST_VERSION;
        Assertions.assertTrue(jobInfo.hasManifest());
        Assertions.assertTrue(jobInfo.isPrefixDigestInManifest());
        // a version this one does not know is not checked and does not fail
        jobInfo.manifestVersion = BackupJobInfo.MANIFEST_VERSION + 1;
        Assertions.assertFalse(jobInfo.hasManifest());
        Assertions.assertNull(jobInfo.getManifestRoot(PART, IDX, 101));
        // a version before the arrays only takes manifest_version 1 for a manifest that it can check: it takes
        // version 2 for a manifest of a newer version, and none of "manifest_roots", "rd", "ldr" is written for it
        // to take as roots.
        Assertions.assertEquals(2, BackupJobInfo.MANIFEST_VERSION);
        String json = mixedJobInfo().toJson(false);
        Assertions.assertTrue(json.contains("\"manifest_version\":2"), json);
        for (String old : new String[] {"manifest_roots", "\"ldr\"", "\"ldn\"", "\"rd\""}) {
            Assertions.assertFalse(json.contains(old), old);
        }
    }

    @Test
    public void testNoMapWithATabletIdKeyInMemory() throws Exception {
        int count = 3000;
        long[] ids = new long[count];
        for (int i = 0; i < count; i++) {
            ids[i] = 1000000L + (i * 7919L) % count;
        }
        BackupJobInfo jobInfo = newJobInfo(ids);
        Map<Long, SnapshotInfo> infos = Maps.newHashMap();
        for (long id : ids) {
            infos.put(id, snapshotInfo(id, idHex(id), LogicalDigestInfo.of(1, hex('5'), "e" + idHex(id).substring(1)),
                    "f" + idHex(id).substring(1)));
        }
        Assertions.assertTrue(jobInfo.buildManifest(infos, BackupJobInfo.DIGEST_SHA256));
        Assertions.assertEquals(count, jobInfo.buildLogicalDigests(infos));
        Assertions.assertEquals(count, jobInfo.buildPrefixDigests(infos));
        String json = jobInfo.toJson(false);
        BackupJobInfo read = BackupJobInfo.genFromJson(json);
        // use all the lookups by tablet id
        for (long id : ids) {
            Assertions.assertEquals(idHex(id), read.getManifestRoot(PART, IDX, id));
            Assertions.assertEquals("e" + idHex(id).substring(1), read.getLogicalDigest(PART, IDX, id).root);
        }

        // the index has the arrays and no map of the per tablet data
        BackupIndexInfo idxInfo = index(read);
        Assertions.assertEquals(count, idxInfo.manifestRootArray.size());
        Assertions.assertEquals(count, idxInfo.logicalDigestRootArray.size());
        Assertions.assertEquals(count, idxInfo.prefixDigestPresent.length());
        for (String name : new String[] {"manifestRoots", "logicalDigestRoots", "logicalDigestNone", "logicalDigests",
                "prefixDigestRoots"}) {
            Field field = BackupIndexInfo.class.getDeclaredField(name);
            field.setAccessible(true);
            Assertions.assertNull(field.get(idxInfo), name);
        }
        // there is no object for a tablet other than the id and the files of the tablet
        List<String> tabletFields = Lists.newArrayList();
        for (Field field : BackupTabletInfo.class.getDeclaredFields()) {
            if (!Modifier.isStatic(field.getModifiers())) {
                tabletFields.add(field.getName());
            }
        }
        Collections.sort(tabletFields);
        Assertions.assertEquals(Arrays.asList("files", "id"), tabletFields);
        // and the job info does not have a map from a tablet id to its index any more (the lookup is by partition)
        for (Field field : BackupJobInfo.class.getDeclaredFields()) {
            Assertions.assertNotEquals("java.util.Map<java.lang.Long, " + BackupIndexInfo.class.getName() + ">",
                    field.getGenericType().getTypeName(), field.getName());
        }
        // the lookup that is kept has an entry for each partition, not for each tablet
        Field lookup = BackupJobInfo.class.getDeclaredField("partitionIndexes");
        lookup.setAccessible(true);
        Assertions.assertEquals(1, ((Map<?, ?>) lookup.get(read)).size());

        // the json is much smaller than the one of the previous format, which repeats the tablet id for each value
        // and has the root of the decomposed digest
        BackupJobInfo previous = newJobInfo(ids);
        BackupIndexInfo previousIdx = index(previous);
        previous.manifestVersion = BackupJobInfo.MANIFEST_VERSION_V1;
        previousIdx.manifestRoots = Maps.newHashMap();
        previousIdx.logicalDigestRoots = Maps.newHashMap();
        previousIdx.prefixDigestRoots = Maps.newHashMap();
        previousIdx.logicalDigestSchemaSig = hex('5');
        previousIdx.logicalDigestAlgo = 1;
        for (long id : ids) {
            previousIdx.manifestRoots.put(id, idHex(id));
            previousIdx.logicalDigestRoots.put(id, "e" + idHex(id).substring(1));
            previousIdx.prefixDigestRoots.put(id, "f" + idHex(id).substring(1));
        }
        int previousSize = previous.toJson(false).length();
        Assertions.assertTrue(json.length() < previousSize, json.length() + " vs " + previousSize);
        // the 13 digits of the id, the quotes and the colon for 3 values, and the root of the decomposed digest
        Assertions.assertTrue(previousSize - json.length() > count * 90, json.length() + " vs " + previousSize);
        // the decomposed digest is not in the job info: its root is not
        Assertions.assertFalse(json.contains("f" + idHex(ids[0]).substring(1)), "the root of rdigest");
    }

    @Test
    public void testReleaseKeepsWhatIsAskedForByPosition() {
        BackupJobInfo read = BackupJobInfo.genFromJson(mixedJobInfo().toJson(false));
        // the digest of 101 is kept (the incremental append sends it), the others and the rdigest marks are gone
        Set<Long> keep = Collections.singleton(101L);
        read.releaseDigests(keep);
        BackupIndexInfo idxInfo = index(read);
        Assertions.assertEquals(hex('b'), read.getLogicalDigest(PART, IDX, 101).root);
        Assertions.assertEquals(hex('5'), read.getLogicalDigest(PART, IDX, 101).schemaSig);
        Assertions.assertNull(read.getLogicalDigest(PART, IDX, 104));
        Assertions.assertNull(read.getLogicalDigest(PART, IDX, 102));
        Assertions.assertNull(idxInfo.prefixDigestPresent);
        Assertions.assertFalse(idxInfo.hasPrefixDigestAt(0));
        // the manifest roots are kept for the download
        Assertions.assertEquals(hex('4'), read.getManifestRoot(PART, IDX, 104));
        String json = read.toJson(false);
        Assertions.assertTrue(json.contains("\"lra\":[\"\",\"\",\"\",\"" + hex('b') + "\"]"), json);
        Assertions.assertFalse(json.contains("\"lna\""), json);
        Assertions.assertFalse(json.contains("\"rdp\""), json);
        Assertions.assertFalse(json.contains(hex('a')), json);

        // a kept tablet without a digest: nothing is kept
        read.releaseDigests(Collections.singleton(103L));
        Assertions.assertNull(idxInfo.logicalDigestRootArray);
        Assertions.assertNull(idxInfo.logicalDigestSchemaSig);
        Assertions.assertNull(idxInfo.logicalDigestAlgo);

        // the manifest roots are released after the download, the version is kept
        read.releaseManifestRoots();
        Assertions.assertNull(idxInfo.manifestRootArray);
        Assertions.assertNull(read.getManifestRoot(PART, IDX, 104));
        Assertions.assertTrue(read.hasManifest());
        Assertions.assertFalse(read.toJson(false).contains("\"mra\""));
    }

    @Test
    public void testReleaseSnapshotInfoReleasesEverythingPerTablet() {
        BackupJobInfo read = BackupJobInfo.genFromJson(mixedJobInfo().toJson(false));
        // the lookups build their caches
        Assertions.assertNotNull(read.getManifestRoot(PART, IDX, 102));
        Assertions.assertEquals(1, index(read).positionOf(102));
        read.releaseSnapshotInfo();
        BackupIndexInfo idxInfo = index(read);
        Assertions.assertNull(idxInfo.manifestRootArray);
        Assertions.assertNull(idxInfo.logicalDigestRootArray);
        Assertions.assertNull(idxInfo.logicalDigestNoneArray);
        Assertions.assertNull(idxInfo.prefixDigestPresent);
        Assertions.assertNull(idxInfo.prefixDigestRootArray);
        Assertions.assertNull(read.getManifestRoot(PART, IDX, 102));
        Assertions.assertNull(read.getLogicalDigest(PART, IDX, 104));
        Assertions.assertTrue(read.hasManifest());
        for (BackupTabletInfo tablet : idxInfo.sortedTabletInfoList) {
            Assertions.assertTrue(tablet.files.isEmpty());
        }
        String json = read.toJson(false);
        for (String key : new String[] {"\"mra\"", "\"lra\"", "\"lna\"", "\"lds\"", "\"lda\"", "\"rdp\""}) {
            Assertions.assertFalse(json.contains(key), key + ": " + json);
        }
    }

    @Test
    public void testTabletWithoutFilesIgnoresThePerTabletData() {
        // tablets_order has a tablet which is not in tablets: the positions of the arrays are not the ones of
        // sortedTabletInfoList any more, the index has no manifest root and no digest
        BackupJobInfo jobInfo = mixedJobInfo();
        index(jobInfo).tablets.remove(102L);
        BackupJobInfo read = BackupJobInfo.genFromJson(jobInfo.toJson(false));
        BackupIndexInfo idxInfo = index(read);
        Assertions.assertEquals(3, idxInfo.sortedTabletInfoList.size());
        Assertions.assertNull(idxInfo.manifestRootArray);
        Assertions.assertNull(idxInfo.logicalDigestRootArray);
        Assertions.assertNull(idxInfo.prefixDigestPresent);
        Assertions.assertNull(read.getLogicalDigest(PART, IDX, 104));
        Assertions.assertNull(read.getManifestRoot(PART, IDX, 104));
    }

    @Test
    public void testSeveralIndexesAndPartitions() {
        BackupJobInfo jobInfo = newJobInfo(101, 102);
        BackupPartitionInfo second = new BackupPartitionInfo();
        second.id = 30;
        second.version = 10;
        for (long idxId : new long[] {40, 41}) {
            BackupIndexInfo idxInfo = new BackupIndexInfo();
            idxInfo.id = idxId;
            idxInfo.schemaHash = 5;
            long tabletId = 200 + idxId;
            idxInfo.tablets.put(tabletId, Lists.newArrayList("1.dat." + MD5));
            idxInfo.tabletsOrder.add(tabletId);
            second.indexes.put("idx" + idxId, idxInfo);
        }
        jobInfo.backupOlapTableObjects.get("tbl").partitions.put("p2", second);
        Map<Long, SnapshotInfo> infos = Maps.newHashMap();
        for (long id : new long[] {101, 102, 240, 241}) {
            infos.put(id, snapshotInfo(id, idHex(id), null, null));
        }
        Assertions.assertTrue(jobInfo.buildManifest(infos, BackupJobInfo.DIGEST_SHA256));
        BackupJobInfo read = BackupJobInfo.genFromJson(jobInfo.toJson(false));
        Assertions.assertEquals(idHex(102), read.getManifestRoot(PART, IDX, 102));
        Assertions.assertEquals(idHex(240), read.getManifestRoot(30, 40, 240));
        Assertions.assertEquals(idHex(241), read.getManifestRoot(30, 41, 241));
        // a tablet of another index
        Assertions.assertNull(read.getManifestRoot(30, 41, 240));
        Assertions.assertNull(read.getManifestRoot(PART, IDX, 240));
        Assertions.assertEquals(4, read.getTabletNum());
    }
}
