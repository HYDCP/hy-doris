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

import com.sun.management.ThreadMXBean;
import org.apache.thrift.TBase;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TIOStreamTransport;
import org.apache.thrift.transport.TMemoryBuffer;
import org.apache.thrift.transport.TMemoryInputTransport;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import java.io.ByteArrayInputStream;
import java.lang.management.CompilationMXBean;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Manual allocation and decode-time comparison using generated Thrift classes.
 * Arguments: baseline|cached report|stat-map|stat-list memory|stream.
 * Run each combination in separate JVMs, in reversed orders, with the intended JVM options.
 * Properties: probe.warmup (minimum decoded messages), probe.operations (messages per batch),
 * probe.samples, and probe.tablets (tablets per message, default 512).
 * Warmup and measurement use the same batch size and decode call site.
 * Check compilation logs for convergence; a fixed warmup count cannot guarantee it on every JVM.
 * Stream mode exercises TIOStreamTransport over an in-memory stream, not network throughput.
 * This is not a performance assertion in the unit test suite.
 */
public class CachedTBinaryProtocolBenchmark {
    private static volatile Object blackhole;
    private static final int TABLETS = Integer.getInteger("probe.tablets", 512);

    private static TBase<?, ?> fixture(String scenario) {
        if (scenario.equals("report")) {
            TReportRequest request = new TReportRequest().setBackend(new TBackend("127.0.0.1", 9060, 8040));
            Map<Long, TTablet> tablets = new LinkedHashMap<>();
            for (int i = 0; i < TABLETS; i++) {
                long id = 1000000L + i;
                TTabletInfo info = new TTabletInfo();
                info.setTabletId(id).setSchemaHash(1024).setVersion(77).setVersionHash(0)
                        .setRowCount(100).setDataSize(4096).setStorageMedium(TStorageMedium.SSD)
                        .setTotalVersionCount(9).setVisibleVersionCount(8).setPathHash(880001L)
                        .setVersionMiss(false).setUsed(true).setPartitionId(300001L).setReplicaId(400001L + i)
                        .setRemoteDataSize(512).setLocalIndexSize(256).setLocalSegmentSize(3840)
                        .setRemoteIndexSize(32).setRemoteSegmentSize(480);
                tablets.put(id, new TTablet(Collections.singletonList(info)));
            }
            return request.setTablets(tablets).setReportVersion(99).setNumCores(32)
                    .setPipelineExecutorSize(8).setNumTablets(TABLETS);
        }
        Map<Long, TTabletStat> map = new LinkedHashMap<>();
        List<TTabletStat> list = new ArrayList<>();
        for (int i = 0; i < TABLETS; i++) {
            TTabletStat stat = new TTabletStat(1000000L + i);
            stat.setDataSize(4096).setRowCount(100).setTotalVersionCount(9).setVisibleVersionCount(8)
                    .setVisibleVersion(77).setRemoteDataSize(512).setLocalIndexSize(256).setLocalSegmentSize(3840)
                    .setRemoteIndexSize(32).setRemoteSegmentSize(480);
            if (scenario.equals("stat-map")) {
                map.put(stat.getTabletId(), stat);
            } else {
                list.add(stat);
            }
        }
        TTabletStatResult result = new TTabletStatResult(map);
        if (scenario.equals("stat-list")) {
            result.setTabletStatList(list);
        }
        return result;
    }

    private static TBase<?, ?> empty(String scenario) {
        return scenario.equals("report") ? new TReportRequest() : new TTabletStatResult();
    }

    private static long checksum(TBase<?, ?> decoded) {
        if (decoded instanceof TReportRequest) {
            TReportRequest report = (TReportRequest) decoded;
            TTablet tablet = report.getTablets().values().iterator().next();
            return report.getTabletsSize() + report.getReportVersion()
                    + tablet.getTabletInfos().get(0).getVersion();
        }
        TTabletStatResult result = (TTabletStatResult) decoded;
        return result.isSetTabletStatList() ? result.getTabletStatListSize() : result.getTabletsStatsSize();
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            throw new IllegalArgumentException(
                    "Expected: baseline|cached report|stat-map|stat-list memory|stream");
        }
        String variant = args[0];
        String scenario = args[1];
        String transportName = args[2];
        if (!(variant.equals("baseline") || variant.equals("cached"))
                || !(scenario.equals("report") || scenario.equals("stat-map") || scenario.equals("stat-list"))
                || !(transportName.equals("memory") || transportName.equals("stream"))) {
            throw new IllegalArgumentException("Unsupported variant, scenario, or transport");
        }
        int warmup = Integer.getInteger("probe.warmup", 262144);
        int operations = Integer.getInteger("probe.operations", 1024);
        int samples = Integer.getInteger("probe.samples", 12);
        if (warmup <= 0 || operations <= 0 || samples <= 0 || TABLETS <= 0) {
            throw new IllegalArgumentException("Warmup, operations, samples, and tablets must be positive");
        }
        int warmupBatches = (warmup - 1) / operations + 1;
        TBase<?, ?> expected = fixture(scenario);
        TMemoryBuffer encoded = new TMemoryBuffer(1024);
        expected.write(new TBinaryProtocol(encoded));
        byte[] payload = Arrays.copyOf(encoded.getArray(), encoded.length());
        ResettableInput input = transportName.equals("memory")
                ? new ResettableMemoryInput(payload) : new ResettableStreamTransport(payload);
        TTransport transport = input.transport();
        TProtocol protocol = variant.equals("baseline")
                ? new TBinaryProtocol(transport) : new CachedTBinaryProtocol(transport);
        TBase<?, ?> check = empty(scenario);
        check.read(protocol);
        if (!expected.equals(check) || input.remaining() != 0) {
            throw new AssertionError("decoded fixture differs");
        }
        ThreadMXBean allocation = (ThreadMXBean) ManagementFactory.getThreadMXBean();
        if (!allocation.isThreadAllocatedMemorySupported() || !allocation.isCurrentThreadCpuTimeSupported()) {
            throw new IllegalStateException("Thread allocation and CPU metrics are required");
        }
        if (!allocation.isThreadAllocatedMemoryEnabled()) {
            allocation.setThreadAllocatedMemoryEnabled(true);
        }
        if (!allocation.isThreadCpuTimeEnabled()) {
            allocation.setThreadCpuTimeEnabled(true);
        }
        long threadId = Thread.currentThread().getId();
        CompilationMXBean compiler = ManagementFactory.getCompilationMXBean();
        RuntimeMXBean runtime = ManagementFactory.getRuntimeMXBean();
        List<GarbageCollectorMXBean> collectors = ManagementFactory.getGarbageCollectorMXBeans();
        Sample[] records = new Sample[samples];
        for (int i = 0; i < samples; i++) {
            records[i] = new Sample();
        }
        for (int batch = 0; batch < warmupBatches; batch++) {
            decodeBatch(protocol, input, scenario, operations);
        }
        for (Sample record : records) {
            long compileBefore = compiler.getTotalCompilationTime();
            long gcBefore = gcTime(collectors);
            record.startMillis = runtime.getUptime();
            long bytesBefore = allocation.getThreadAllocatedBytes(threadId);
            long cpuBefore = allocation.getCurrentThreadCpuTime();
            long timeBefore = System.nanoTime();
            record.checksum = decodeBatch(protocol, input, scenario, operations);
            record.elapsedNanos = System.nanoTime() - timeBefore;
            record.cpuNanos = allocation.getCurrentThreadCpuTime() - cpuBefore;
            record.allocatedBytes = allocation.getThreadAllocatedBytes(threadId) - bytesBefore;
            record.endMillis = runtime.getUptime();
            record.compileMillis = compiler.getTotalCompilationTime() - compileBefore;
            record.gcMillis = gcTime(collectors) - gcBefore;
        }
        // Print after all measurements to keep formatting and console I/O out of the samples.
        System.out.printf(Locale.ROOT,
                "SETUP variant=%s scenario=%s transport=%s tablets=%d payload=%d warmup=%d warmup_batches=%d jdk=%s%n",
                variant, scenario, transportName, TABLETS, payload.length, (long) warmupBatches * operations,
                warmupBatches, System.getProperty("java.runtime.version"));
        for (int sample = 0; sample < records.length; sample++) {
            Sample record = records[sample];
            System.out.printf(Locale.ROOT,
                    "SAMPLE variant=%s scenario=%s transport=%s sample=%d operations=%d bytes=%d"
                            + " B_per_decode=%.3f ns_per_decode=%.3f cpu_ns=%.3f compile_ms=%d gc_ms=%d"
                            + " checksum=%d start_ms=%d end_ms=%d%n",
                    variant, scenario, transportName, sample, operations, record.allocatedBytes,
                    (double) record.allocatedBytes / operations, (double) record.elapsedNanos / operations,
                    (double) record.cpuNanos / operations, record.compileMillis, record.gcMillis,
                    record.checksum, record.startMillis, record.endMillis);
        }
        blackhole = null;
    }

    private static long decodeBatch(TProtocol protocol, ResettableInput input, String scenario, int operations)
            throws Exception {
        long sum = 0;
        for (int i = 0; i < operations; i++) {
            input.resetMessage();
            TBase<?, ?> decoded = empty(scenario);
            decoded.read(protocol);
            blackhole = decoded;
            sum += checksum(decoded);
        }
        return sum;
    }

    private static long gcTime(List<GarbageCollectorMXBean> collectors) {
        long total = 0;
        for (GarbageCollectorMXBean collector : collectors) {
            total += collector.getCollectionTime();
        }
        return total;
    }

    private static class Sample {
        private long allocatedBytes;
        private long elapsedNanos;
        private long cpuNanos;
        private long compileMillis;
        private long gcMillis;
        private long checksum;
        private long startMillis;
        private long endMillis;
    }

    private interface ResettableInput {
        TTransport transport();

        void resetMessage() throws TTransportException;

        int remaining();
    }

    private static class ResettableMemoryInput implements ResettableInput {
        private final byte[] payload;
        private final TMemoryInputTransport transport;

        private ResettableMemoryInput(byte[] payload) throws TTransportException {
            this.payload = payload;
            this.transport = new TMemoryInputTransport(payload);
        }

        @Override
        public TTransport transport() {
            return transport;
        }

        @Override
        public void resetMessage() {
            transport.reset(payload);
        }

        @Override
        public int remaining() {
            return transport.getBytesRemainingInBuffer();
        }
    }

    private static class ResettableStreamTransport extends TIOStreamTransport implements ResettableInput {
        private final ByteArrayInputStream source;
        private final int messageSize;

        private ResettableStreamTransport(byte[] payload) throws TTransportException {
            this(new ByteArrayInputStream(payload), payload.length);
        }

        private ResettableStreamTransport(ByteArrayInputStream source, int messageSize) throws TTransportException {
            super(source);
            this.source = source;
            this.messageSize = messageSize;
            resetMessage();
        }

        @Override
        public TTransport transport() {
            return this;
        }

        @Override
        public void resetMessage() throws TTransportException {
            source.reset();
            // Reset each independent message against the transport's configured maximum.
            resetConsumedMessageSize(messageSize);
        }

        @Override
        public int remaining() {
            return source.available();
        }
    }
}
