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

package org.apache.doris.persist;

import org.apache.doris.backup.BackupJob;
import org.apache.doris.backup.BackupJobInfo;
import org.apache.doris.backup.RestoreJob;
import org.apache.doris.catalog.ReplicaAllocation;
import org.apache.doris.common.Config;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.io.DataOutputBuffer;
import org.apache.doris.common.io.Writable;
import org.apache.doris.journal.JournalBatch;
import org.apache.doris.journal.JournalEntity;
import org.apache.doris.journal.SerializedJournalEntity;
import org.apache.doris.journal.bdbje.BDBJEJournal;
import org.apache.doris.meta.MetaContext;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Uninterruptibles;
import mockit.Mock;
import mockit.MockUp;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

public class EditLogTest {
    private String meta = "editLogTestDir/";

    /**
     * Holds a job in the middle of its serialization until the test releases it.
     */
    private static class BlockingSerialization {
        private final CountDownLatch started = new CountDownLatch(1);
        private final CountDownLatch finish = new CountDownLatch(1);
        private volatile Thread serializeThread;

        void write(DataOutput out) throws IOException {
            serializeThread = Thread.currentThread();
            started.countDown();
            Uninterruptibles.awaitUninterruptibly(finish);
            out.writeInt(1);
        }
    }

    public void mkdir() {
        File dir = new File(meta);
        if (!dir.exists()) {
            dir.mkdir();
        } else {
            File[] files = dir.listFiles();
            for (File file : files) {
                if (file.isFile()) {
                    file.delete();
                }
            }
        }
    }

    public void addFiles(int image, int edit) {
        File imageFile = new File(meta + "image." + image);
        try {
            imageFile.createNewFile();
        } catch (IOException e) {
            e.printStackTrace();
        }

        for (int i = 1; i <= edit; i++) {
            File editFile = new File(meta + "edits." + i);
            try {
                editFile.createNewFile();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        File current = new File(meta + "edits");
        try {
            current.createNewFile();
        } catch (IOException e) {
            e.printStackTrace();
        }

        File version = new File(meta + "VERSION");
        try {
            version.createNewFile();
            String line1 = "#Mon Feb 02 13:59:54 CST 2015\n";
            String line2 = "clusterId=966271669";
            FileWriter fw = new FileWriter(version);
            fw.write(line1);
            fw.write(line2);
            fw.flush();
            fw.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void deleteDir() {
        File dir = new File(meta);
        if (dir.exists()) {
            File[] files = dir.listFiles();
            for (File file : files) {
                if (file.isFile()) {
                    file.delete();
                }
            }

            dir.delete();
        }
    }

    @Test
    public void testWriteLog() throws IOException {

    }

    @Test
    public void test() {

    }

    private static byte[] journalBytes(Writable writable) throws IOException {
        JournalBatch batch = new JournalBatch();
        batch.addJournal(OperationType.OP_RESTORE_JOB, writable);
        JournalBatch.Entity entity = batch.getJournalEntities().get(0);
        return Arrays.copyOf(entity.getBinaryData(), entity.getBinaryDataLength());
    }

    @Test
    public void testSerializedJournalEntityKeepsJournalBytes() throws IOException {
        MetaContext metaContext = new MetaContext();
        metaContext.setMetaVersion(FeConstants.meta_version);
        metaContext.setThreadLocalInfo();

        boolean originalCompression = Config.restore_job_compressed_serialization;
        try {
            for (boolean compression : new boolean[] {false, true}) {
                Config.restore_job_compressed_serialization = compression;
                RestoreJob job = new RestoreJob("test_label", "2026-11-09 00:00:00", 1L, "test_db", new BackupJobInfo(),
                        false, new ReplicaAllocation((short) 3), 1000L, -1, false, false, false, false, false, false,
                        false, null, 1L);

                SerializedJournalEntity serialized =
                        SerializedJournalEntity.serialize(OperationType.OP_RESTORE_JOB, job);
                byte[] bytes = journalBytes(serialized);
                Assert.assertArrayEquals(journalBytes(job), bytes);

                // journal paths that write the op code themselves get the data only
                DataOutputBuffer data = new DataOutputBuffer();
                serialized.write(data);
                Assert.assertArrayEquals(Arrays.copyOfRange(bytes, Short.BYTES, bytes.length),
                        Arrays.copyOf(data.getData(), data.getLength()));

                JournalEntity entity = new JournalEntity();
                entity.readFields(new DataInputStream(new ByteArrayInputStream(bytes)));
                Assert.assertEquals(OperationType.OP_RESTORE_JOB, entity.getOpCode());
                Assert.assertEquals("test_label", ((RestoreJob) entity.getData()).getLabel());
            }
        } finally {
            Config.restore_job_compressed_serialization = originalCompression;
            MetaContext.remove();
        }
    }

    private EditLog newEditLog(List<Short> writtenOps) {
        new MockUp<BDBJEJournal>() {
            @Mock
            public void $init(String nodeName) { // CHECKSTYLE IGNORE THIS LINE
            }

            @Mock
            public long write(JournalBatch batch) {
                batch.getJournalEntities().forEach(entity -> writtenOps.add(entity.getOpCode()));
                return writtenOps.size();
            }
        };

        String originalEditLogType = Config.edit_log_type;
        Config.edit_log_type = "bdb";
        try {
            return new EditLog("test_node");
        } finally {
            Config.edit_log_type = originalEditLogType;
        }
    }

    private void checkSerializationDoesNotBlockOtherJournals(short jobOp, BlockingSerialization serialization,
            Consumer<EditLog> logJob) throws Exception {
        List<Short> writtenOps = Collections.synchronizedList(Lists.newArrayList());
        EditLog editLog = newEditLog(writtenOps);
        Thread submitter = new Thread(() -> logJob.accept(editLog));
        submitter.setDaemon(true);
        submitter.start();
        try {
            Assert.assertTrue(serialization.started.await(30, TimeUnit.SECONDS));

            // the flusher is free while the job is being serialized, so another journal can still be written
            CountDownLatch otherJournalWritten = new CountDownLatch(1);
            Thread otherWriter = new Thread(() -> {
                editLog.logSaveNextId(1L);
                otherJournalWritten.countDown();
            });
            otherWriter.setDaemon(true);
            otherWriter.start();
            Assert.assertTrue(otherJournalWritten.await(30, TimeUnit.SECONDS));
            Assert.assertEquals(Lists.newArrayList(OperationType.OP_SAVE_NEXTID), writtenOps);
        } finally {
            serialization.finish.countDown();
        }

        submitter.join(30000);
        Assert.assertFalse(submitter.isAlive());
        Assert.assertSame(submitter, serialization.serializeThread);
        Assert.assertEquals(Lists.newArrayList(OperationType.OP_SAVE_NEXTID, jobOp), writtenOps);
    }

    private void checkDoneJobIsSerializedByFlusher(short jobOp, AtomicReference<Thread> serializeThread,
            Consumer<EditLog> logJob) {
        List<Short> writtenOps = Collections.synchronizedList(Lists.newArrayList());
        logJob.accept(newEditLog(writtenOps));
        Assert.assertEquals("EditLog-Flusher", serializeThread.get().getName());
        Assert.assertEquals(Lists.newArrayList(jobOp), writtenOps);
    }

    @Test
    public void testDoneRestoreJobIsSerializedByFlusher() {
        AtomicReference<Thread> serializeThread = new AtomicReference<>();
        RestoreJob job = new RestoreJob() {
            @Override
            public boolean isDone() {
                return true;
            }

            @Override
            public void write(DataOutput out) {
                serializeThread.set(Thread.currentThread());
            }
        };
        checkDoneJobIsSerializedByFlusher(OperationType.OP_RESTORE_JOB, serializeThread,
                editLog -> editLog.logRestoreJob(job));
    }

    @Test
    public void testDoneBackupJobIsSerializedByFlusher() {
        AtomicReference<Thread> serializeThread = new AtomicReference<>();
        BackupJob job = new BackupJob() {
            @Override
            public boolean isDone() {
                return true;
            }

            @Override
            public void write(DataOutput out) {
                serializeThread.set(Thread.currentThread());
            }
        };
        checkDoneJobIsSerializedByFlusher(OperationType.OP_BACKUP_JOB, serializeThread,
                editLog -> editLog.logBackupJob(job));
    }

    @Test
    public void testSerializingRestoreJobDoesNotBlockOtherJournals() throws Exception {
        BlockingSerialization serialization = new BlockingSerialization();
        RestoreJob job = new RestoreJob() {
            @Override
            public void write(DataOutput out) throws IOException {
                serialization.write(out);
            }
        };
        checkSerializationDoesNotBlockOtherJournals(OperationType.OP_RESTORE_JOB, serialization,
                editLog -> editLog.logRestoreJob(job));
    }

    @Test
    public void testSerializingBackupJobDoesNotBlockOtherJournals() throws Exception {
        BlockingSerialization serialization = new BlockingSerialization();
        BackupJob job = new BackupJob() {
            @Override
            public void write(DataOutput out) throws IOException {
                serialization.write(out);
            }
        };
        checkSerializationDoesNotBlockOtherJournals(OperationType.OP_BACKUP_JOB, serialization,
                editLog -> editLog.logBackupJob(job));
    }

    @Test
    public void testSerializedJournalEntityMustBeAddedWithItsOp() {
        SerializedJournalEntity serialized = SerializedJournalEntity.serialize(OperationType.OP_SAVE_NEXTID,
                out -> out.writeLong(1L));
        Assert.assertThrows(IllegalStateException.class,
                () -> new JournalBatch().addJournal(OperationType.OP_RESTORE_JOB, serialized));
    }
}
