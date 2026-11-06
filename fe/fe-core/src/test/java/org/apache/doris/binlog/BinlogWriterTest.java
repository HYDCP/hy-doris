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

package org.apache.doris.binlog;

import org.apache.doris.common.Config;
import org.apache.doris.thrift.TBinlog;
import org.apache.doris.thrift.TBinlogType;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TMemoryBuffer;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.List;

public class BinlogWriterTest {
    private static TBinlog binlog(long sequence, int dataSize) {
        TBinlog result = BinlogTestUtils.newBinlog(1L, 2L, sequence, sequence);
        result.setType(TBinlogType.CREATE_TABLE);
        char[] data = new char[dataSize];
        Arrays.fill(data, 'x');
        result.setData(new String(data));
        return result;
    }

    private static byte[] legacyRecord(TBinlog binlog, boolean padded) throws TException, IOException {
        TMemoryBuffer buffer = new TMemoryBuffer(16 * 1024);
        binlog.write(new TBinaryProtocol(buffer));
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(bytes);
        int length = padded ? buffer.getArray().length : buffer.length();
        out.writeInt(length);
        out.write(buffer.getArray(), 0, length);
        return bytes.toByteArray();
    }

    @Test
    public void testMixedRecordsAreByteIdenticalAndReadable() throws Exception {
        List<TBinlog> binlogs = Arrays.asList(binlog(1, 0), binlog(2, 64), binlog(3, 20 * 1024),
                binlog(4, 2 * 1024 * 1024), binlog(5, 16), binlog(6, 0));
        BinlogManager.BinlogWriter writer = new BinlogManager.BinlogWriter();
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(bytes);
        ByteArrayOutputStream expected = new ByteArrayOutputStream();
        for (TBinlog binlog : binlogs) {
            writer.write(out, binlog);
            expected.write(legacyRecord(binlog, false));
        }
        Assert.assertArrayEquals(expected.toByteArray(), bytes.toByteArray());

        BinlogManager reader = new BinlogManager();
        DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes.toByteArray()));
        for (TBinlog binlog : binlogs) {
            Assert.assertEquals(binlog, reader.readTBinlogFromStream(in));
        }
        Assert.assertEquals(-1, in.read());
    }

    @Test
    public void testSmallRecordsReuseBufferAndOversizedRecordReleasesIt() throws Exception {
        BinlogManager.BinlogWriter writer = new BinlogManager.BinlogWriter();
        Field bufferField = BinlogManager.BinlogWriter.class.getDeclaredField("buffer");
        bufferField.setAccessible(true);
        Object initialBuffer = bufferField.get(writer);
        DataOutputStream out = new DataOutputStream(new ByteArrayOutputStream());
        writer.write(out, binlog(1, 128));
        writer.write(out, binlog(2, 32 * 1024));
        Assert.assertSame(initialBuffer, bufferField.get(writer));
        writer.write(out, binlog(3, 2 * 1024 * 1024));
        Object replacement = bufferField.get(writer);
        Assert.assertNotSame(initialBuffer, replacement);
        writer.write(out, binlog(4, 16));
        Assert.assertSame(replacement, bufferField.get(writer));
    }

    @Test
    public void testReaderAcceptsLegacyPaddedRecords() throws Exception {
        TBinlog first = binlog(1, 64);
        TBinlog second = binlog(2, 128);
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        bytes.write(legacyRecord(first, true));
        bytes.write(legacyRecord(second, true));
        DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes.toByteArray()));
        BinlogManager reader = new BinlogManager();
        Assert.assertEquals(first, reader.readTBinlogFromStream(in));
        Assert.assertEquals(second, reader.readTBinlogFromStream(in));
        Assert.assertEquals(-1, in.read());
    }

    @Test
    public void testReaderRejectsTruncatedRecord() throws Exception {
        byte[] record = legacyRecord(binlog(1, 128), false);
        DataInputStream in = new DataInputStream(new ByteArrayInputStream(Arrays.copyOf(record, record.length - 1)));
        Assert.assertThrows(EOFException.class, () -> new BinlogManager().readTBinlogFromStream(in));
    }

    @Test
    public void testEmptyAndDisabledCheckpointPreserveChecksum() throws Exception {
        boolean enabled = Config.enable_feature_binlog;
        try {
            BinlogManager manager = new BinlogManager();
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            DataOutputStream out = new DataOutputStream(bytes);
            Config.enable_feature_binlog = true;
            Assert.assertEquals(12345L, manager.write(out, 12345L));
            Assert.assertEquals(4, bytes.size());
            Assert.assertEquals(0, new DataInputStream(new ByteArrayInputStream(bytes.toByteArray())).readInt());
            Config.enable_feature_binlog = false;
            bytes.reset();
            Assert.assertEquals(12345L, manager.write(out, 12345L));
            Assert.assertEquals(0, bytes.size());
        } finally {
            Config.enable_feature_binlog = enabled;
        }
    }
}
