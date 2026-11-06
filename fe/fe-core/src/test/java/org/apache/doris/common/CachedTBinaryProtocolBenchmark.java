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
import org.apache.thrift.transport.TMemoryBuffer;
import org.apache.thrift.transport.TMemoryInputTransport;

import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Manual allocation and decode-time comparison using generated Thrift classes.
 * Run baseline/cached with report/stat-map/stat-list in separate JVMs, in reversed orders.
 * This is not a performance assertion in the unit test suite.
 */
public class CachedTBinaryProtocolBenchmark {
    private static volatile Object blackhole;
    private static final int TABLETS = 512;

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
        String variant = args[0];
        String scenario = args[1];
        TBase<?, ?> expected = fixture(scenario);
        TMemoryBuffer encoded = new TMemoryBuffer(1024);
        expected.write(new TBinaryProtocol(encoded));
        byte[] payload = Arrays.copyOf(encoded.getArray(), encoded.length());
        TMemoryInputTransport input = new TMemoryInputTransport(payload);
        TProtocol protocol = variant.equals("baseline")
                ? new TBinaryProtocol(input) : new CachedTBinaryProtocol(input);
        TBase<?, ?> check = empty(scenario);
        check.read(protocol);
        if (!expected.equals(check) || input.getBytesRemainingInBuffer() != 0) {
            throw new AssertionError("decoded fixture differs");
        }
        ThreadMXBean allocation = (ThreadMXBean) ManagementFactory.getThreadMXBean();
        if (!allocation.isThreadAllocatedMemoryEnabled()) {
            allocation.setThreadAllocatedMemoryEnabled(true);
        }
        int warmup = Integer.getInteger("probe.warmup", 4096);
        int operations = Integer.getInteger("probe.operations", 128);
        for (int i = 0; i < warmup; i++) {
            input.reset(payload);
            TBase<?, ?> decoded = empty(scenario);
            decoded.read(protocol);
            blackhole = decoded;
        }
        System.out.printf(Locale.ROOT, "SETUP variant=%s scenario=%s tablets=%d payload=%d warmup=%d jdk=%s%n",
                variant, scenario, TABLETS, payload.length, warmup, System.getProperty("java.runtime.version"));
        for (int sample = 0; sample < 8; sample++) {
            long bytesBefore = allocation.getThreadAllocatedBytes(Thread.currentThread().getId());
            long timeBefore = System.nanoTime();
            long sum = 0;
            for (int i = 0; i < operations; i++) {
                input.reset(payload);
                TBase<?, ?> decoded = empty(scenario);
                decoded.read(protocol);
                blackhole = decoded;
                sum += checksum(decoded);
            }
            long elapsed = System.nanoTime() - timeBefore;
            long bytes = allocation.getThreadAllocatedBytes(Thread.currentThread().getId()) - bytesBefore;
            System.out.printf(Locale.ROOT,
                    "SAMPLE variant=%s scenario=%s sample=%d operations=%d bytes=%d B_per_decode=%.3f ns_per_decode=%.3f checksum=%d%n",
                    variant, scenario, sample, operations, bytes, (double) bytes / operations,
                    (double) elapsed / operations, sum);
        }
        blackhole = null;
    }
}
