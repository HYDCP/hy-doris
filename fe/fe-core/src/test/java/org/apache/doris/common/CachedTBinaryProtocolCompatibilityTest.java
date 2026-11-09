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

import org.apache.doris.thrift.TBackend;
import org.apache.doris.thrift.TReportRequest;
import org.apache.doris.thrift.TStorageMedium;
import org.apache.doris.thrift.TTablet;
import org.apache.doris.thrift.TTabletInfo;
import org.apache.doris.thrift.TTabletStat;
import org.apache.doris.thrift.TTabletStatResult;
import org.apache.doris.thrift.TUniqueId;

import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TField;
import org.apache.thrift.protocol.TList;
import org.apache.thrift.protocol.TMap;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.protocol.TType;
import org.apache.thrift.transport.TIOStreamTransport;
import org.apache.thrift.transport.TMemoryBuffer;
import org.apache.thrift.transport.TMemoryInputTransport;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.layered.TFramedTransport;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Supplier;

public class CachedTBinaryProtocolCompatibilityTest {
    private static final TProtocolFactory[] FACTORIES = {
            new TBinaryProtocol.Factory(), new CachedTBinaryProtocol.Factory()};

    @Test
    public void testTabletReportOnBufferedAndUnbufferedTransports() throws Exception {
        assertRoundTrips(report(), TReportRequest::new);
    }

    @Test
    public void testTabletStatisticsMapResponse() throws Exception {
        assertRoundTrips(statistics(false), TTabletStatResult::new);
    }

    @Test
    public void testTabletStatisticsListResponse() throws Exception {
        assertRoundTrips(statistics(true), TTabletStatResult::new);
    }

    @Test
    public void testFramedReportsAndStatistics() throws Exception {
        assertFramedRoundTrips(report(), TReportRequest::new);
        assertFramedRoundTrips(statistics(false), TTabletStatResult::new);
        assertFramedRoundTrips(statistics(true), TTabletStatResult::new);
    }

    @Test
    public void testUnknownNestedFieldAndConsecutiveReports() throws Exception {
        TReportRequest expected = report();
        byte[] original = encode(expected, FACTORIES[0]);
        TMemoryBuffer output = new TMemoryBuffer(original.length * 2 + 128);
        output.write(original, 0, original.length - 1);
        TProtocol writer = FACTORIES[0].getProtocol(output);
        writer.writeFieldBegin(new TField("future", TType.MAP, (short) 30000));
        writer.writeMapBegin(new TMap(TType.STRING, TType.LIST, 1));
        writer.writeString("future tablets");
        writer.writeListBegin(new TList(TType.I64, 2));
        writer.writeI64(Long.MIN_VALUE);
        writer.writeI64(Long.MAX_VALUE);
        writer.writeListEnd();
        writer.writeMapEnd();
        writer.writeFieldEnd();
        writer.writeFieldStop();
        expected.write(writer);
        byte[] messages = Arrays.copyOf(output.getArray(), output.length());
        for (TProtocolFactory factory : FACTORIES) {
            TMemoryInputTransport input = new TMemoryInputTransport(messages);
            TProtocol protocol = factory.getProtocol(input);
            TReportRequest first = new TReportRequest();
            first.read(protocol);
            TReportRequest second = new TReportRequest();
            second.read(protocol);
            Assert.assertEquals(expected, first);
            Assert.assertEquals(expected, second);
            Assert.assertEquals(0, input.getBytesRemainingInBuffer());
        }
    }

    @Test
    public void testTruncatedReportPreservesReadFailure() throws Exception {
        byte[] bytes = encode(report(), FACTORIES[0]);
        byte[] truncated = Arrays.copyOf(bytes, bytes.length / 2);
        TException original = Assert.assertThrows(TException.class,
                () -> new TReportRequest().read(FACTORIES[0].getProtocol(new TMemoryInputTransport(truncated))));
        TException cached = Assert.assertThrows(TException.class,
                () -> new TReportRequest().read(FACTORIES[1].getProtocol(new TMemoryInputTransport(truncated))));
        Assert.assertEquals(original.getClass(), cached.getClass());
        Assert.assertEquals(original.getMessage(), cached.getMessage());
    }

    private static void assertRoundTrips(TBase<?, ?> expected, Supplier<? extends TBase<?, ?>> create)
            throws Exception {
        Assert.assertArrayEquals(encode(expected, FACTORIES[0]), encode(expected, FACTORIES[1]));
        for (TProtocolFactory writer : FACTORIES) {
            byte[] bytes = encode(expected, writer);
            for (TProtocolFactory reader : FACTORIES) {
                TMemoryInputTransport buffered = new TMemoryInputTransport(bytes);
                TBase<?, ?> decoded = create.get();
                decoded.read(reader.getProtocol(buffered));
                Assert.assertEquals(expected, decoded);
                Assert.assertEquals(0, buffered.getBytesRemainingInBuffer());
                ByteArrayInputStream stream = new ByteArrayInputStream(bytes);
                decoded = create.get();
                decoded.read(reader.getProtocol(new TIOStreamTransport(stream)));
                Assert.assertEquals(expected, decoded);
                Assert.assertEquals(0, stream.available());
            }
        }
    }

    private static void assertFramedRoundTrips(TBase<?, ?> expected, Supplier<? extends TBase<?, ?>> create)
            throws Exception {
        for (TProtocolFactory writer : FACTORIES) {
            TMemoryBuffer output = new TMemoryBuffer(1024);
            TTransport framedOutput = new TFramedTransport(output);
            expected.write(writer.getProtocol(framedOutput));
            framedOutput.flush();
            byte[] bytes = Arrays.copyOf(output.getArray(), output.length());
            for (TProtocolFactory reader : FACTORIES) {
                // Use a stream under framing, matching the socket-based client pool.
                ByteArrayInputStream input = new ByteArrayInputStream(bytes);
                TBase<?, ?> decoded = create.get();
                decoded.read(reader.getProtocol(new TFramedTransport(new TIOStreamTransport(input))));
                Assert.assertEquals(expected, decoded);
                Assert.assertEquals(0, input.available());
            }
        }
    }

    private static byte[] encode(TBase<?, ?> value, TProtocolFactory factory) throws Exception {
        TMemoryBuffer output = new TMemoryBuffer(1024);
        value.write(factory.getProtocol(output));
        return Arrays.copyOf(output.getArray(), output.length());
    }

    private static TReportRequest report() {
        TTabletInfo info = new TTabletInfo();
        info.setTabletId(1000001L).setSchemaHash(1024).setVersion(77).setVersionHash(0)
                .setRowCount(100).setDataSize(4096).setStorageMedium(TStorageMedium.SSD)
                .setTransactionIds(Arrays.asList(9001L, 9002L)).setTotalVersionCount(9).setVisibleVersionCount(8)
                .setPathHash(880001L).setVersionMiss(false).setUsed(true).setPartitionId(300001L)
                .setIsInMemory(false).setReplicaId(400001L).setRemoteDataSize(512).setCooldownTerm(5)
                .setCooldownMetaId(new TUniqueId(1, 2)).setLocalIndexSize(256).setLocalSegmentSize(3840)
                .setRemoteIndexSize(32).setRemoteSegmentSize(480).setIsPersistent(true);
        Map<Long, TTablet> tablets = new LinkedHashMap<>();
        tablets.put(info.getTabletId(), new TTablet(Collections.singletonList(info)));
        TReportRequest request = new TReportRequest().setBackend(new TBackend("127.0.0.1", 9060, 8040));
        request.setTablets(tablets).setTabletList(Collections.singletonList(new TTablet(
                Collections.singletonList(info.deepCopy())))).setReportVersion(99).setForceRecovery(false)
                .setPartitionsVersion(Collections.singletonMap(300001L, 77L)).setNumCores(32)
                .setPipelineExecutorSize(8).setNumTablets(1).setRunningTasks(2);
        return request;
    }

    private static TTabletStatResult statistics(boolean listResponse) {
        TTabletStat stat = new TTabletStat(1000001L);
        stat.setDataSize(4096).setRowCount(100).setTotalVersionCount(9).setVisibleVersionCount(8)
                .setVisibleVersion(77).setRemoteDataSize(512).setLocalIndexSize(256).setLocalSegmentSize(3840)
                .setRemoteIndexSize(32).setRemoteSegmentSize(480);
        TTabletStatResult result = new TTabletStatResult(new LinkedHashMap<>());
        if (listResponse) {
            result.setTabletStatList(Collections.singletonList(stat));
        } else {
            result.putToTabletsStats(stat.getTabletId(), stat);
        }
        return result;
    }
}
