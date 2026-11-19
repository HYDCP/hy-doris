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

import org.apache.doris.persist.OperationType;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

public class JournalBatchTest {
    @Test
    public void testSerializedJournalEntityIsNotCopied() throws IOException {
        SerializedJournalEntity serialized = SerializedJournalEntity.serialize(OperationType.OP_SAVE_NEXTID,
                out -> out.writeLong(1L));
        JournalBatch batch = new JournalBatch();
        batch.addJournal(OperationType.OP_SAVE_NEXTID, serialized);

        JournalBatch.Entity entity = batch.getJournalEntities().get(0);
        Assert.assertSame(serialized.getBuffer().getData(), entity.getBinaryData());
        Assert.assertEquals(Short.BYTES + Long.BYTES, entity.getBinaryDataLength());
        Assert.assertEquals(Short.BYTES + Long.BYTES, batch.getSize());
    }
}
