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

import org.apache.doris.analysis.BackupStmt;
import org.apache.doris.catalog.ReplicaAllocation;
import org.apache.doris.common.Config;
import org.apache.doris.common.io.Text;
import org.apache.doris.persist.gson.GsonUtils;

import com.google.common.collect.Lists;
import com.google.gson.JsonParseException;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class BackupRestoreJobStreamingJsonConfigTest {
    @Test
    public void testBackupJobRoundTripWithGlobalSwitch() throws Exception {
        BackupJob job = newBackupJob("backup_label");

        assertRoundTrip(job, true, false, false);
        assertRoundTrip(job, false, false, false);
    }

    @Test
    public void testRestoreJobRoundTripWithGlobalSwitch() throws Exception {
        RestoreJob job = newRestoreJob("restore_label");

        assertRoundTrip(job, true, false, false);
        assertRoundTrip(job, false, false, false);
    }

    @Test
    public void testCompressedJobRoundTripWithGlobalSwitch() throws Exception {
        assertRoundTrip(newBackupJob("backup_compressed"), true, true, false);
        assertRoundTrip(newBackupJob("backup_compressed_tree"), false, true, false);
        assertRoundTrip(newRestoreJob("restore_compressed"), true, false, true);
        assertRoundTrip(newRestoreJob("restore_compressed_tree"), false, false, true);
    }

    @Test
    public void testReadPathUsesGlobalSwitch() throws Exception {
        BackupJob job = newBackupJob("backup_read_mode");
        String json = GsonUtils.GSON.toJson(job, AbstractJob.class);
        String typeFieldSecondJson = moveClazzFieldAfterFirstField(json, "BackupJob");

        boolean oldStreaming = Config.enable_backup_restore_job_streaming_json;
        try {
            Config.enable_backup_restore_job_streaming_json = false;
            AbstractJob treeReadJob = readJson(typeFieldSecondJson);
            Assert.assertEquals(AbstractJob.JobType.BACKUP, treeReadJob.getType());
            Assert.assertEquals(job.getLabel(), treeReadJob.getLabel());

            Config.enable_backup_restore_job_streaming_json = true;
            try {
                readJson(typeFieldSecondJson);
                Assert.fail("expect JsonParseException");
            } catch (JsonParseException e) {
                Assert.assertTrue(e.getMessage(), e.getMessage().contains("instead of 'clazz'"));
            }
        } finally {
            Config.enable_backup_restore_job_streaming_json = oldStreaming;
        }
    }

    private static void assertRoundTrip(AbstractJob job, boolean streaming, boolean backupCompressed,
            boolean restoreCompressed) throws IOException {
        boolean oldStreaming = Config.enable_backup_restore_job_streaming_json;
        boolean oldBackupCompressed = Config.backup_job_compressed_serialization;
        boolean oldRestoreCompressed = Config.restore_job_compressed_serialization;
        try {
            Config.enable_backup_restore_job_streaming_json = streaming;
            Config.backup_job_compressed_serialization = backupCompressed;
            Config.restore_job_compressed_serialization = restoreCompressed;

            AbstractJob restored = writeAndRead(job);
            Assert.assertEquals(job.getType(), restored.getType());
            Assert.assertEquals(job.getLabel(), restored.getLabel());
            Assert.assertEquals(job.getDbId(), restored.getDbId());
            Assert.assertEquals(job.getDbName(), restored.getDbName());
        } finally {
            Config.enable_backup_restore_job_streaming_json = oldStreaming;
            Config.backup_job_compressed_serialization = oldBackupCompressed;
            Config.restore_job_compressed_serialization = oldRestoreCompressed;
        }
    }

    private static BackupJob newBackupJob(String label) {
        return new BackupJob(label, 1L, "db1", Lists.newArrayList(), 1000L,
                BackupStmt.BackupContent.ALL, null, 2L, 0L);
    }

    private static RestoreJob newRestoreJob(String label) {
        BackupJobInfo jobInfo = new BackupJobInfo();
        jobInfo.isForceReplicationAllocation = false;
        return new RestoreJob(label, "2026-06-17 09:00:00", 1L, "db1", jobInfo, false,
                new ReplicaAllocation((short) 1), 1000L, -1, false, false, false,
                false, false, false, false, null, 2L);
    }

    private static AbstractJob writeAndRead(AbstractJob job) throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(bytes);
        job.write(out);
        out.flush();
        return AbstractJob.read(new DataInputStream(new ByteArrayInputStream(bytes.toByteArray())));
    }

    private static AbstractJob readJson(String json) throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(bytes);
        Text.writeString(out, json);
        out.flush();
        return AbstractJob.read(new DataInputStream(new ByteArrayInputStream(bytes.toByteArray())));
    }

    private static String moveClazzFieldAfterFirstField(String json, String clazz) {
        String prefix = "{\"clazz\":\"" + clazz + "\",";
        Assert.assertTrue(json, json.startsWith(prefix));
        String withoutClazz = "{" + json.substring(prefix.length());
        int firstComma = withoutClazz.indexOf(',');
        Assert.assertTrue(withoutClazz, firstComma > 0);
        return withoutClazz.substring(0, firstComma + 1) + "\"clazz\":\"" + clazz + "\","
                + withoutClazz.substring(firstComma + 1);
    }
}
