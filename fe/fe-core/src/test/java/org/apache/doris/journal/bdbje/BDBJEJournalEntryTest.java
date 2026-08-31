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

package org.apache.doris.journal.bdbje;

import org.apache.doris.common.io.DataOutputBuffer;
import org.apache.doris.common.io.Writable;
import org.apache.doris.journal.JournalBatch;
import org.apache.doris.journal.JournalEntity;
import org.apache.doris.persist.OperationType;
import org.apache.doris.persist.gson.GsonUtils;

import com.sleepycat.je.DatabaseEntry;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.Arrays;

public class BDBJEJournalEntryTest { // CHECKSTYLE IGNORE THIS LINE: BDBJE should use uppercase
    private static final Logger LOG = LogManager.getLogger(BDBJEJournalEntryTest.class);
    private static final int OUTPUT_BUFFER_INIT_SIZE = 128;
    private static final String JSON_PAYLOAD = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
            + "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

    @Test
    public void testCompactEntryMatchesStreamingSerializedSize() throws Exception {
        Writable writable = out -> GsonUtils.toJsonAsText(out, JSON_PAYLOAD);
        DataOutputBuffer buffer = serialize(OperationType.OP_LOCAL_EOF, writable);

        Assertions.assertTrue(buffer.getLength() > OUTPUT_BUFFER_INIT_SIZE);
        Assertions.assertTrue(buffer.getData().length > buffer.getLength());

        DatabaseEntry entry = BDBJEJournal.createDatabaseEntry(buffer);
        Assertions.assertEquals(buffer.getLength(), entry.getSize());
        long serializedSize = BDBJEJournal.countJournalSize(OperationType.OP_LOCAL_EOF, writable);
        Assertions.assertEquals(buffer.getLength(), serializedSize);
        Assertions.assertTrue(BDBJEJournal.exceedMaxJournalSize(
                OperationType.OP_LOCAL_EOF, writable, serializedSize - 1));
        Assertions.assertFalse(BDBJEJournal.exceedMaxJournalSize(
                OperationType.OP_LOCAL_EOF, writable, serializedSize));
        Assertions.assertFalse(BDBJEJournal.exceedMaxJournalSize(
                OperationType.OP_LOCAL_EOF, writable, serializedSize + 1));
    }

    @Test
    public void testPaddedAndCompactEntriesAreReadable() throws Exception {
        Timestamp timestamp = new Timestamp();
        DataOutputBuffer buffer = serialize(OperationType.OP_TIMESTAMP, timestamp);
        Assertions.assertTrue(buffer.getData().length > buffer.getLength());

        DatabaseEntry paddedEntry = new DatabaseEntry(buffer.getData());
        DatabaseEntry compactEntry = BDBJEJournal.createDatabaseEntry(buffer);

        JournalEntity paddedEntity = readEntry(paddedEntry);
        JournalEntity compactEntity = readEntry(compactEntry);
        Assertions.assertEquals(OperationType.OP_TIMESTAMP, paddedEntity.getOpCode());
        Assertions.assertEquals(OperationType.OP_TIMESTAMP, compactEntity.getOpCode());
        Assertions.assertEquals(((Timestamp) paddedEntity.getData()).getTimestamp(),
                ((Timestamp) compactEntity.getData()).getTimestamp());
    }

    @Test
    public void testBatchEntityValidDataLength() throws Exception {
        String payload = buildLargeAsciiString(2 * 1024 * 1024);
        // the payload object is reusable, so the same writable can be serialized twice
        // (once by countJournalSize, once by JournalBatch.addJournal)
        Writable writable = out -> GsonUtils.toJsonAsText(out, payload);

        JournalBatch batch = new JournalBatch();
        batch.addJournal(OperationType.OP_LOCAL_EOF, writable);
        JournalBatch.Entity entity = batch.getJournalEntities().get(0);

        LOG.info("batch entity valid length {}, backing array capacity {}",
                entity.getBinaryDataLength(), entity.getBinaryData().length);
        // the backing array grows past the valid bytes, so padding must exist
        Assertions.assertTrue(entity.getBinaryData().length > entity.getBinaryDataLength());
        Assertions.assertEquals(entity.getBinaryDataLength(), batch.getSize());

        long serializedSize = BDBJEJournal.countJournalSize(OperationType.OP_LOCAL_EOF, writable);
        Assertions.assertEquals(entity.getBinaryDataLength(), serializedSize);
        Assertions.assertTrue(BDBJEJournal.exceedMaxJournalSize(
                OperationType.OP_LOCAL_EOF, writable, serializedSize - 1));
        Assertions.assertFalse(BDBJEJournal.exceedMaxJournalSize(
                OperationType.OP_LOCAL_EOF, writable, serializedSize));
        Assertions.assertFalse(BDBJEJournal.exceedMaxJournalSize(
                OperationType.OP_LOCAL_EOF, writable, serializedSize + 1));
    }

    private static String buildLargeAsciiString(int length) {
        char[] chars = new char[length];
        Arrays.fill(chars, 'a');
        return new String(chars);
    }

    private static DataOutputBuffer serialize(short op, Writable writable) throws IOException {
        JournalEntity entity = new JournalEntity();
        entity.setOpCode(op);
        entity.setData(writable);
        DataOutputBuffer buffer = new DataOutputBuffer(OUTPUT_BUFFER_INIT_SIZE);
        entity.write(buffer);
        return buffer;
    }

    private static JournalEntity readEntry(DatabaseEntry entry) throws IOException {
        byte[] bytes = Arrays.copyOfRange(entry.getData(), entry.getOffset(), entry.getOffset() + entry.getSize());
        JournalEntity entity = new JournalEntity();
        entity.readFields(new DataInputStream(new ByteArrayInputStream(bytes)));
        return entity;
    }
}
