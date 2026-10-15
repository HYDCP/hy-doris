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

import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.ReplicaAllocation;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.nereids.trees.plans.commands.BackupCommand.BackupContent;
import org.apache.doris.nereids.trees.plans.commands.ShowRestoreCommand;
import org.apache.doris.persist.EditLog;
import org.apache.doris.thrift.TBackend;
import org.apache.doris.thrift.TDownloadStats;
import org.apache.doris.thrift.TFinishTaskRequest;
import org.apache.doris.thrift.TStatus;
import org.apache.doris.thrift.TStatusCode;
import org.apache.doris.thrift.TTaskType;

import com.google.common.collect.Lists;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

public class RestoreDownloadStatsTest {
    private final Env env = Mockito.mock(Env.class);
    private final InternalCatalog catalog = Mockito.mock(InternalCatalog.class);
    private final EditLog editLog = Mockito.mock(EditLog.class);
    private MockedStatic<Env> mockedEnvStatic;
    private Database db;

    @BeforeEach
    public void setUp() throws Exception {
        db = CatalogMocker.mockDb();
        mockedEnvStatic = Mockito.mockStatic(Env.class);
        mockedEnvStatic.when(Env::getCurrentEnvJournalVersion).thenReturn(FeConstants.meta_version);
        mockedEnvStatic.when(Env::getCurrentEnv).thenReturn(env);
        Mockito.when(env.getInternalCatalog()).thenReturn(catalog);
        Mockito.when(env.getEditLog()).thenReturn(editLog);
        Mockito.when(catalog.getDbNullable(Mockito.anyLong())).thenReturn(db);
    }

    @AfterEach
    public void tearDown() {
        if (mockedEnvStatic != null) {
            mockedEnvStatic.close();
        }
    }

    private RestoreJob newRestoreJob() {
        BackupJobInfo jobInfo = new BackupJobInfo();
        jobInfo.name = "snapshot_1";
        jobInfo.dbName = "src_db";
        jobInfo.dbId = 1;
        jobInfo.success = true;
        jobInfo.content = BackupContent.ALL;
        jobInfo.metaVersion = FeConstants.meta_version;
        return new RestoreJob("restore_label", "2024-01-01 00:00:00", db.getId(), db.getFullName(), jobInfo, false,
                new ReplicaAllocation((short) 3), 100000, -1, false, false, false, false, false, false,
                false, false, env, 20000);
    }

    private static String downloadStatsColumn(RestoreJob job) {
        List<String> fullInfo = job.getFullInfo();
        Assertions.assertEquals(ShowRestoreCommand.TITLE_NAMES.size(), fullInfo.size());
        return fullInfo.get(ShowRestoreCommand.TITLE_NAMES.indexOf("DownloadStats"));
    }

    private static TFinishTaskRequest downloadFinished(TDownloadStats stats, Long... tabletIds) {
        TFinishTaskRequest request = new TFinishTaskRequest(new TBackend("", 0, 1), TTaskType.DOWNLOAD, 1,
                new TStatus(TStatusCode.OK));
        request.setDownloadedTabletIds(Lists.newArrayList(tabletIds));
        if (stats != null) {
            request.setDownloadStats(stats);
        }
        return request;
    }

    private static TDownloadStats stats(long linkedFiles, long linkedBytes, long skippedFiles, long skippedBytes,
            long downloadedFiles, long downloadedBytes, long full, long partial, long none, long unmatched,
            long noSource, long notInSnapshot, long versionMismatch) {
        TDownloadStats stats = new TDownloadStats();
        stats.setLinkedFiles(linkedFiles);
        stats.setLinkedBytes(linkedBytes);
        stats.setSkippedFiles(skippedFiles);
        stats.setSkippedBytes(skippedBytes);
        stats.setDownloadedFiles(downloadedFiles);
        stats.setDownloadedBytes(downloadedBytes);
        stats.setTabletsFullReuse(full);
        stats.setTabletsPartialReuse(partial);
        stats.setTabletsNoReuse(none);
        stats.setUnmatchedRowsets(unmatched);
        stats.setUnmatchedNoSourceRowsetId(noSource);
        stats.setUnmatchedSourceNotInSnapshot(notInSnapshot);
        stats.setUnmatchedVersionMismatch(versionMismatch);
        return stats;
    }

    private static void addReplicas(RestoreJob job, long... tabletIds) {
        for (long beId : new long[] {1, 2}) {
            for (long tabletId : tabletIds) {
                job.snapshotInfos.put(tabletId, beId, new SnapshotInfo(1, 2, 3, 4, tabletId, beId, 5, "/p",
                        Lists.newArrayList()));
            }
        }
    }

    @Test
    public void testShowRestoreColumn() {
        int column = ShowRestoreCommand.TITLE_NAMES.indexOf("DownloadStats");
        Assertions.assertEquals(ShowRestoreCommand.TITLE_NAMES.size() - 1, column);
        Assertions.assertEquals(column - 1, ShowRestoreCommand.TITLE_NAMES.indexOf("ManifestCheck"));
        Assertions.assertEquals(column - 2, ShowRestoreCommand.TITLE_NAMES.indexOf("ReuseEstimate"));
        Assertions.assertFalse(ShowRestoreCommand.BRIEF_TITLE_NAMES.contains("DownloadStats"));

        // not downloaded yet
        RestoreJob job = newRestoreJob();
        Assertions.assertEquals(FeConstants.null_string, downloadStatsColumn(job));
        Assertions.assertEquals(ShowRestoreCommand.BRIEF_TITLE_NAMES.size(), job.getBriefInfo().size());
    }

    @Test
    public void testAccumulate() {
        RestoreJob job = newRestoreJob();
        // 4 replicas: tablets 201 and 202 on backends 1 and 2
        addReplicas(job, 201, 202);
        resetDownloadStats(job);
        Assertions.assertEquals("{\"linked_bytes\":0,\"skipped_bytes\":0,\"kept_bytes\":0,\"downloaded_bytes\":0,\"reuse_ratio\":0.0,"
                + "\"linked_files\":0,\"skipped_files\":0,\"downloaded_files\":0,"
                + "\"replicas\":{\"full_reuse\":0,\"partial_reuse\":0,\"no_reuse\":0,\"not_reported\":4},"
                + "\"unmatched_rowsets\":0,"
                + "\"unmatched_reason\":{\"no_source_rowset_id\":0,\"source_not_in_snapshot\":0,"
                + "\"version_mismatch\":0}}", downloadStatsColumn(job));

        // backend 1: tablet 201 fully linked, tablet 202 linked with 1 file downloaded
        job.recordDownloadStats(1, downloadFinished(stats(8, 800, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0), 201L));
        job.recordDownloadStats(1, downloadFinished(stats(2, 200, 1, 50, 1, 50, 0, 1, 0, 2, 0, 2, 0), 202L));
        JsonObject json = JsonParser.parseString(downloadStatsColumn(job)).getAsJsonObject();
        Assertions.assertEquals(1000, json.get("linked_bytes").getAsLong());
        Assertions.assertEquals(50, json.get("skipped_bytes").getAsLong());
        Assertions.assertEquals(50, json.get("downloaded_bytes").getAsLong());
        // (1000 + 50) / 1100
        Assertions.assertEquals(0.955, json.get("reuse_ratio").getAsDouble(), 1e-9);
        Assertions.assertEquals(10, json.get("linked_files").getAsLong());
        Assertions.assertEquals(1, json.get("skipped_files").getAsLong());
        Assertions.assertEquals(1, json.get("downloaded_files").getAsLong());
        JsonObject replicas = json.getAsJsonObject("replicas");
        Assertions.assertEquals(1, replicas.get("full_reuse").getAsLong());
        Assertions.assertEquals(1, replicas.get("partial_reuse").getAsLong());
        Assertions.assertEquals(0, replicas.get("no_reuse").getAsLong());
        // backend 2 has not reported yet
        Assertions.assertEquals(2, replicas.get("not_reported").getAsLong());
        Assertions.assertEquals(2, json.get("unmatched_rowsets").getAsLong());
        Assertions.assertEquals(2, json.getAsJsonObject("unmatched_reason").get("source_not_in_snapshot")
                .getAsLong());

        // backend 2 is old, reports nothing: not reported, the job is not affected
        job.recordDownloadStats(2, downloadFinished(null, 201L, 202L));
        json = JsonParser.parseString(downloadStatsColumn(job)).getAsJsonObject();
        Assertions.assertEquals(2, json.getAsJsonObject("replicas").get("not_reported").getAsLong());
        Assertions.assertEquals(1000, json.get("linked_bytes").getAsLong());
    }

    @Test
    public void testNothingDownloadedRatioIsZero() {
        RestoreJob job = newRestoreJob();
        addReplicas(job, 201);
        resetDownloadStats(job);
        job.recordDownloadStats(1, downloadFinished(stats(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0), 201L));
        Assertions.assertEquals(0.0, job.getDownloadStats().getReuseRatio());
        // all files downloaded: the ratio is 0 too
        job.recordDownloadStats(2, downloadFinished(stats(0, 0, 0, 0, 3, 300, 0, 0, 1, 0, 0, 0, 0), 201L));
        Assertions.assertEquals(0.0, job.getDownloadStats().getReuseRatio());
        Assertions.assertEquals(2, job.getDownloadStats().getReportedReplicas());
    }

    @Test
    public void testFixAndPersist() throws Exception {
        RestoreJob job = newRestoreJob();
        addReplicas(job, 201, 202);
        resetDownloadStats(job);
        job.recordDownloadStats(1, downloadFinished(stats(8, 800, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0), 201L, 202L));
        // fixed when the download finishes, then the snapshot infos are released
        job.fixDownloadStats();
        String shown = downloadStatsColumn(job);
        job.snapshotInfos.clear();
        Assertions.assertEquals(shown, downloadStatsColumn(job));
        Assertions.assertEquals(2, JsonParser.parseString(shown).getAsJsonObject().getAsJsonObject("replicas")
                .get("not_reported").getAsLong());

        RestoreJob readJob = writeAndRead(job);
        Assertions.assertEquals(shown, downloadStatsColumn(readJob));
        Assertions.assertTrue(readJob.getDownloadStats().isFixed());
        // fixed once
        job.fixDownloadStats();
        Assertions.assertEquals(shown, downloadStatsColumn(job));
    }

    @Test
    public void testOldPersistedJobHasNoDownloadStats() throws Exception {
        // as persisted by an old version: no "dls"
        RestoreJob job = newRestoreJob();
        RestoreJob readJob = writeAndRead(job);
        Assertions.assertNull(readJob.getDownloadStats());
        Assertions.assertEquals(FeConstants.null_string, downloadStatsColumn(readJob));
        // an old backend reports nothing before any stats exists: still nothing, no error
        readJob.recordDownloadStats(1, downloadFinished(null, 201L));
        Assertions.assertNull(readJob.getDownloadStats());
    }

    private static void resetDownloadStats(RestoreJob job) {
        Deencapsulation.invoke(job, "resetDownloadStats");
    }

    private static RestoreJob writeAndRead(RestoreJob job) throws Exception {
        Path path = Files.createTempFile("restoreDownloadStats", "tmp");
        try {
            try (DataOutputStream out = new DataOutputStream(Files.newOutputStream(path))) {
                job.write(out);
            }
            try (DataInputStream in = new DataInputStream(Files.newInputStream(path))) {
                return RestoreJob.read(in);
            }
        } finally {
            Files.deleteIfExists(path);
        }
    }
}
