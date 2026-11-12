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

package org.apache.doris.journal;

import org.apache.doris.common.io.DataOutputBuffer;
import org.apache.doris.common.io.Writable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataOutput;
import java.io.IOException;

/**
 * A journal entity serialized in the calling thread, so that the EditLog-Flusher thread does not have to.
 *
 * The buffer holds the whole entity in the format of {@link JournalEntity#write}, the op code followed by the data.
 * {@link JournalBatch#addJournal} takes the buffer as it is, so the bytes are not copied again in the flusher.
 */
public class SerializedJournalEntity implements Writable {
    private static final Logger LOG = LogManager.getLogger(SerializedJournalEntity.class);

    private final short opCode;
    private final DataOutputBuffer buffer;

    private SerializedJournalEntity(short opCode, DataOutputBuffer buffer) {
        this.opCode = opCode;
        this.buffer = buffer;
    }

    public static SerializedJournalEntity serialize(short opCode, Writable data) {
        JournalEntity entity = new JournalEntity();
        entity.setOpCode(opCode);
        entity.setData(data);
        DataOutputBuffer buffer = new DataOutputBuffer();
        try {
            entity.write(buffer);
        } catch (Throwable t) {
            // Same as a serialization failure in the flusher: the journal can't be written,
            // while the in-memory state is already ahead of it.
            LOG.error("Fatal Error : serialize journal failed", t);
            System.exit(-1);
        }
        return new SerializedJournalEntity(opCode, buffer);
    }

    public short getOpCode() {
        return opCode;
    }

    DataOutputBuffer getBuffer() {
        return buffer;
    }

    // Writes the data only, for the journal paths that write the op code themselves.
    @Override
    public void write(DataOutput out) throws IOException {
        out.write(buffer.getData(), Short.BYTES, buffer.getLength() - Short.BYTES);
    }
}
