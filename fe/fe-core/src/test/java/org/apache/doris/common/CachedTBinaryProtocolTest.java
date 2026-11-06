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

package org.apache.doris.common;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TField;
import org.apache.thrift.protocol.TList;
import org.apache.thrift.protocol.TMap;
import org.apache.thrift.protocol.TMessage;
import org.apache.thrift.protocol.TMessageType;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.protocol.TProtocolUtil;
import org.apache.thrift.protocol.TStruct;
import org.apache.thrift.protocol.TType;
import org.apache.thrift.transport.TIOStreamTransport;
import org.apache.thrift.transport.TMemoryBuffer;
import org.apache.thrift.transport.TMemoryInputTransport;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class CachedTBinaryProtocolTest {
    private static byte[] header(byte type, short id) {
        return type == TType.STOP ? new byte[] {type} : new byte[] {type, (byte) (id >>> 8), (byte) id};
    }

    @Test
    public void testAllWireTypesAndFieldIdBoundaries() throws Exception {
        short[] ids = {Short.MIN_VALUE, -1, 0, 1, 63, 64, 127, Short.MAX_VALUE};
        for (int type = Byte.MIN_VALUE; type <= Byte.MAX_VALUE; type++) {
            for (short id : ids) {
                byte[] bytes = header((byte) type, id);
                TMemoryInputTransport originalInput = new TMemoryInputTransport(bytes);
                TMemoryInputTransport cachedInput = new TMemoryInputTransport(bytes);
                TField original = new TBinaryProtocol(originalInput).readFieldBegin();
                TField cached = new CachedTBinaryProtocol(cachedInput).readFieldBegin();
                Assert.assertEquals(original.name, cached.name);
                Assert.assertEquals(original.type, cached.type);
                Assert.assertEquals(original.id, cached.id);
                Assert.assertEquals(0, originalInput.getBytesRemainingInBuffer());
                Assert.assertEquals(0, cachedInput.getBytesRemainingInBuffer());
            }
        }
    }

    @Test
    public void testDescriptorsSharedAcrossProtocolInstances() throws Exception {
        for (short id : new short[] {0, 1, 63}) {
            byte[] bytes = header(TType.I64, id);
            TField first = new CachedTBinaryProtocol(new TMemoryInputTransport(bytes)).readFieldBegin();
            TField second = new CachedTBinaryProtocol(new TMemoryInputTransport(bytes)).readFieldBegin();
            Assert.assertSame(first, second);
        }
        TField stop1 = new CachedTBinaryProtocol(new TMemoryInputTransport(header(TType.STOP, (short) 0)))
                .readFieldBegin();
        TField stop2 = new CachedTBinaryProtocol(new TMemoryInputTransport(header(TType.STOP, (short) 0)))
                .readFieldBegin();
        Assert.assertSame(stop1, stop2);
    }

    @Test
    public void testUncachedFieldsKeepStandardValuesWithoutGrowingCache() throws Exception {
        for (byte[] bytes : new byte[][] {header(TType.I64, (short) -1), header(TType.I64, (short) 64),
                header(Byte.MAX_VALUE, (short) 1), header(Byte.MIN_VALUE, (short) 1)}) {
            TField first = new CachedTBinaryProtocol(new TMemoryInputTransport(bytes)).readFieldBegin();
            TField second = new CachedTBinaryProtocol(new TMemoryInputTransport(bytes)).readFieldBegin();
            Assert.assertEquals(first, second);
            Assert.assertNotSame(first, second);
        }
    }

    @Test
    public void testStopDoesNotConsumeFollowingData() throws Exception {
        TMemoryInputTransport input = new TMemoryInputTransport(new byte[] {TType.STOP, 42, 43});
        TProtocol protocol = new CachedTBinaryProtocol(input);
        Assert.assertEquals(TType.STOP, protocol.readFieldBegin().type);
        Assert.assertEquals(2, input.getBytesRemainingInBuffer());
        Assert.assertEquals(42, protocol.readByte());
        Assert.assertEquals(43, protocol.readByte());
    }

    @Test
    public void testStopOnStreamLeavesNextFieldIntact() throws Exception {
        byte[] bytes = {TType.STOP, TType.I64, 0, 1, TType.STOP};
        ByteArrayInputStream input = new ByteArrayInputStream(bytes);
        TProtocol protocol = new CachedTBinaryProtocol(new TIOStreamTransport(input));
        TField firstStop = protocol.readFieldBegin();
        Assert.assertEquals(TType.STOP, firstStop.type);
        Assert.assertEquals(0, firstStop.id);
        Assert.assertEquals("", firstStop.name);
        Assert.assertEquals(bytes.length - 1, input.available());
        TField next = protocol.readFieldBegin();
        Assert.assertEquals(TType.I64, next.type);
        Assert.assertEquals(1, next.id);
        Assert.assertEquals(1, input.available());
        Assert.assertSame(firstStop, protocol.readFieldBegin());
        Assert.assertEquals(0, input.available());
    }

    @Test
    public void testTruncatedHeadersPreserveExceptionsAndConsumption() throws Exception {
        for (byte[] bytes : new byte[][] {new byte[0], {TType.I32}, {TType.I32, 1}}) {
            TMemoryInputTransport originalInput = new TMemoryInputTransport(bytes);
            TMemoryInputTransport cachedInput = new TMemoryInputTransport(bytes);
            TException original = Assert.assertThrows(TException.class,
                    () -> new TBinaryProtocol(originalInput).readFieldBegin());
            TException cached = Assert.assertThrows(TException.class,
                    () -> new CachedTBinaryProtocol(cachedInput).readFieldBegin());
            Assert.assertEquals(original.getClass(), cached.getClass());
            Assert.assertEquals(original.getMessage(), cached.getMessage());
            Assert.assertEquals(originalInput.getBytesRemainingInBuffer(), cachedInput.getBytesRemainingInBuffer());
        }
    }

    @Test
    public void testUnsupportedTypeStillFailsWhenSkipped() throws Exception {
        byte[] bytes = header((byte) 127, (short) 1);
        TProtocol original = new TBinaryProtocol(new TMemoryInputTransport(bytes));
        TProtocol cached = new CachedTBinaryProtocol(new TMemoryInputTransport(bytes));
        TField originalField = original.readFieldBegin();
        TField cachedField = cached.readFieldBegin();
        TException originalError = Assert.assertThrows(TException.class,
                () -> TProtocolUtil.skip(original, originalField.type));
        TException cachedError = Assert.assertThrows(TException.class,
                () -> TProtocolUtil.skip(cached, cachedField.type));
        Assert.assertEquals(originalError.getClass(), cachedError.getClass());
        Assert.assertEquals(originalError.getMessage(), cachedError.getMessage());
    }

    @Test
    public void testNestedConsecutiveMessagesAndWireCompatibility() throws Exception {
        TProtocolFactory original = new TBinaryProtocol.Factory();
        TProtocolFactory cached = new CachedTBinaryProtocol.Factory();
        byte[] bytes = encodeMessages(original);
        Assert.assertArrayEquals(bytes, encodeMessages(cached));
        decodeMessages(cached, bytes);
        decodeMessages(original, encodeMessages(cached));
    }

    @Test
    public void testFactoryPreservesStrictnessAndLengthLimits() throws Exception {
        TProtocolFactory original = new TBinaryProtocol.Factory(true, false, 3, 2);
        TProtocolFactory cached = new CachedTBinaryProtocol.Factory(true, false, 3, 2);
        Assert.assertTrue(cached.getProtocol(new TMemoryBuffer(16)) instanceof CachedTBinaryProtocol);
        Assert.assertArrayEquals(encodeMessages(original), encodeMessages(cached));
        byte[] legacyMessages = encodeMessages(new TBinaryProtocol.Factory(false, false));
        assertSameReadFailure(original, cached, legacyMessages, TProtocol::readMessageBegin);
        assertSameReadFailure(original, cached, new byte[] {0, 0, 0, 4, 'l', 'o', 'n', 'g'}, TProtocol::readString);
        byte[] list = {TType.I32, 0, 0, 0, 3};
        assertSameReadFailure(original, cached, list, TProtocol::readListBegin);
    }

    @Test
    public void testConcurrentReadersOwnStateAndShareOnlyDescriptors() throws Exception {
        TProtocolFactory factory = new CachedTBinaryProtocol.Factory();
        byte[] messages = encodeMessages(new TBinaryProtocol.Factory());
        CyclicBarrier start = new CyclicBarrier(4);
        ExecutorService executor = Executors.newFixedThreadPool(4);
        try {
            Callable<TField> task = () -> {
                start.await(10, TimeUnit.SECONDS);
                for (int i = 0; i < 64; i++) {
                    decodeMessages(factory, messages);
                }
                return factory.getProtocol(new TMemoryInputTransport(header(TType.I64, (short) 1))).readFieldBegin();
            };
            List<Future<TField>> futures = executor.invokeAll(Arrays.asList(task, task, task, task));
            TField expected = futures.get(0).get(10, TimeUnit.SECONDS);
            for (Future<TField> future : futures) {
                Assert.assertSame(expected, future.get(10, TimeUnit.SECONDS));
            }
        } finally {
            executor.shutdownNow();
            Assert.assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
        }
    }

    private interface ReadOperation {
        void read(TProtocol protocol) throws TException;
    }

    private static void assertSameReadFailure(TProtocolFactory originalFactory, TProtocolFactory cachedFactory,
            byte[] bytes, ReadOperation operation) throws Exception {
        TProtocol original = originalFactory.getProtocol(new TIOStreamTransport(new ByteArrayInputStream(bytes)));
        TProtocol cached = cachedFactory.getProtocol(new TIOStreamTransport(new ByteArrayInputStream(bytes)));
        TException first = Assert.assertThrows(TException.class, () -> operation.read(original));
        TException second = Assert.assertThrows(TException.class, () -> operation.read(cached));
        Assert.assertEquals(first.getClass(), second.getClass());
        Assert.assertEquals(first.getMessage(), second.getMessage());
    }

    private static byte[] encodeMessages(TProtocolFactory factory) throws Exception {
        TMemoryBuffer output = new TMemoryBuffer(256);
        TProtocol protocol = factory.getProtocol(output);
        for (int seq = 0; seq < 2; seq++) {
            protocol.writeMessageBegin(new TMessage("report", TMessageType.CALL, seq));
            protocol.writeStructBegin(new TStruct("request"));
            protocol.writeFieldBegin(new TField("id", TType.I64, (short) 1));
            protocol.writeI64(1000000L + seq);
            protocol.writeFieldEnd();
            protocol.writeFieldBegin(new TField("nested", TType.MAP, (short) 2));
            protocol.writeMapBegin(new TMap(TType.STRING, TType.LIST, 1));
            protocol.writeString("副本");
            protocol.writeListBegin(new TList(TType.I32, 2));
            protocol.writeI32(11);
            protocol.writeI32(22);
            protocol.writeListEnd();
            protocol.writeMapEnd();
            protocol.writeFieldEnd();
            protocol.writeFieldBegin(new TField("future", TType.STRING, (short) 64));
            protocol.writeString("unknown field");
            protocol.writeFieldEnd();
            protocol.writeFieldBegin(new TField("legacy", TType.I32, (short) -1));
            protocol.writeI32(42);
            protocol.writeFieldEnd();
            protocol.writeFieldStop();
            protocol.writeStructEnd();
            protocol.writeMessageEnd();
        }
        return Arrays.copyOf(output.getArray(), output.length());
    }

    private static void decodeMessages(TProtocolFactory factory, byte[] bytes) throws Exception {
        TMemoryInputTransport input = new TMemoryInputTransport(bytes);
        TProtocol protocol = factory.getProtocol(input);
        for (int seq = 0; seq < 2; seq++) {
            TMessage message = protocol.readMessageBegin();
            Assert.assertEquals("report", message.name);
            Assert.assertEquals(TMessageType.CALL, message.type);
            Assert.assertEquals(seq, message.seqid);
            protocol.readStructBegin();
            Assert.assertEquals(1, protocol.readFieldBegin().id);
            Assert.assertEquals(1000000L + seq, protocol.readI64());
            protocol.readFieldEnd();
            Assert.assertEquals(2, protocol.readFieldBegin().id);
            TMap map = protocol.readMapBegin();
            Assert.assertEquals(TType.STRING, map.keyType);
            Assert.assertEquals(TType.LIST, map.valueType);
            Assert.assertEquals(1, map.size);
            Assert.assertEquals("副本", protocol.readString());
            TList list = protocol.readListBegin();
            Assert.assertEquals(TType.I32, list.elemType);
            Assert.assertEquals(2, list.size);
            Assert.assertEquals(11, protocol.readI32());
            Assert.assertEquals(22, protocol.readI32());
            protocol.readListEnd();
            protocol.readMapEnd();
            protocol.readFieldEnd();
            TField future = protocol.readFieldBegin();
            Assert.assertEquals(64, future.id);
            TProtocolUtil.skip(protocol, future.type);
            protocol.readFieldEnd();
            TField legacy = protocol.readFieldBegin();
            Assert.assertEquals(-1, legacy.id);
            TProtocolUtil.skip(protocol, legacy.type);
            protocol.readFieldEnd();
            Assert.assertEquals(TType.STOP, protocol.readFieldBegin().type);
            protocol.readStructEnd();
            protocol.readMessageEnd();
        }
        Assert.assertEquals(0, input.getBytesRemainingInBuffer());
    }
}
