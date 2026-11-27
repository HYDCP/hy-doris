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

package org.apache.doris.resource.workloadschedpolicy;

import org.apache.doris.common.Config;
import org.apache.doris.thrift.TQueryStatistics;
import org.apache.doris.thrift.TReportWorkloadRuntimeStatusParams;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class WorkloadQueryProgressTest {
    @Test
    void sumsProgressAcrossReportingBackends() {
        WorkloadRuntimeStatusMgr manager = new WorkloadRuntimeStatusMgr();
        report(manager, 1L, new TQueryStatistics().setTotalTasksNum(10).setFinishedTasksNum(8));
        report(manager, 2L, new TQueryStatistics().setTotalTasksNum(20).setFinishedTasksNum(4));

        manager.aggregateQueryStatistics(true);
        WorkloadRuntimeStatusMgr.QueryStatisticsSnapshot snapshot = manager.getQueryStatisticsSnapshot();
        Assertions.assertEquals(2, snapshot.getReportingBackends().get("query").intValue());
        Assertions.assertEquals(30, snapshot.getStatistics().get("query").getTotalTasksNum());
        Assertions.assertEquals(12, snapshot.getStatistics().get("query").getFinishedTasksNum());
    }

    @Test
    void mixedBackendProgressIsUnknownRegardlessOfReportOrder() {
        for (long oldBackendId : new long[] {1L, 2L}) {
            WorkloadRuntimeStatusMgr manager = new WorkloadRuntimeStatusMgr();
            report(manager, oldBackendId, new TQueryStatistics().setScanBytes(3));
            report(manager, 3L - oldBackendId,
                    new TQueryStatistics().setTotalTasksNum(10).setFinishedTasksNum(10).setScanBytes(6));

            TQueryStatistics merged = manager.aggregateQueryStatistics(true).get("query");
            Assertions.assertFalse(merged.isSetTotalTasksNum());
            Assertions.assertFalse(merged.isSetFinishedTasksNum());
            Assertions.assertEquals(9L, merged.getScanBytes());
        }
    }

    @Test
    void progressWaitsForTasksOnEveryBackend() {
        for (long preparingBackendId : new long[] {1L, 2L}) {
            WorkloadRuntimeStatusMgr manager = new WorkloadRuntimeStatusMgr();
            report(manager, preparingBackendId, new TQueryStatistics().setTotalTasksNum(0).setFinishedTasksNum(0));
            report(manager, 3L - preparingBackendId,
                    new TQueryStatistics().setTotalTasksNum(5).setFinishedTasksNum(5));

            TQueryStatistics merged = manager.aggregateQueryStatistics(true).get("query");
            Assertions.assertFalse(merged.isSetTotalTasksNum());
            Assertions.assertFalse(merged.isSetFinishedTasksNum());

            report(manager, preparingBackendId, new TQueryStatistics().setTotalTasksNum(10).setFinishedTasksNum(0));
            merged = manager.aggregateQueryStatistics(true).get("query");
            Assertions.assertEquals(15, merged.getTotalTasksNum());
            Assertions.assertEquals(5, merged.getFinishedTasksNum());
        }
    }

    @Test
    void disablingExtendedStatsReleasesSnapshotAndPreservesAudit() {
        WorkloadRuntimeStatusMgr manager = new WorkloadRuntimeStatusMgr();
        report(manager, 1L, new TQueryStatistics().setScanBytes(15).setTotalTasksNum(10).setFinishedTasksNum(2));
        boolean previous = Config.enable_extended_current_query_stats;
        try {
            Config.enable_extended_current_query_stats = true;
            manager.runAfterCatalogReady();
            Assertions.assertEquals(10, manager.getQueryStatisticsSnapshot().getStatistics().get("query").total_tasks_num);

            Config.enable_extended_current_query_stats = false;
            manager.runAfterCatalogReady();
            Assertions.assertTrue(manager.getQueryStatisticsSnapshot().getStatistics().isEmpty());
            Assertions.assertTrue(manager.getQueryStatisticsSnapshot().getReportingBackends().isEmpty());
            TQueryStatistics audit = manager.aggregateQueryStatistics(false).get("query");
            Assertions.assertEquals(15, audit.scan_bytes);
            Assertions.assertFalse(audit.isSetTotalTasksNum());

            Config.enable_extended_current_query_stats = true;
            manager.runAfterCatalogReady();
            Assertions.assertEquals(2, manager.getQueryStatisticsSnapshot().getStatistics().get("query").finished_tasks_num);
        } finally {
            Config.enable_extended_current_query_stats = previous;
        }
    }

    private void report(WorkloadRuntimeStatusMgr manager, long backendId, TQueryStatistics statistics) {
        manager.updateBeQueryStats(new TReportWorkloadRuntimeStatusParams()
                .setBackendId(backendId).setQueryStatisticsMap(ImmutableMap.of("query", statistics)));
    }
}
