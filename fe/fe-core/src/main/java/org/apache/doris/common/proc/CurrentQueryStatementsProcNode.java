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

package org.apache.doris.common.proc;

import org.apache.doris.catalog.Env;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.util.QueryStatisticsFormatter;
import org.apache.doris.qe.QeProcessorImpl;
import org.apache.doris.qe.QueryStatisticsItem;
import org.apache.doris.resource.workloadschedpolicy.WorkloadRuntimeStatusMgr.QueryStatisticsSnapshot;
import org.apache.doris.thrift.TQueryStatistics;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.commons.codec.digest.DigestUtils;

import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/*
 * show proc "/current_query_stmts"
 */
public class CurrentQueryStatementsProcNode implements ProcNodeInterface {
    public static final ImmutableList<String> TITLE_NAMES = new ImmutableList.Builder<String>()
            .add("QueryId").add("ConnectionId").add("Catalog").add("Database").add("User")
            .add("ExecTime").add("SqlHash").add("Statement").build();

    public static final ImmutableList<String> EXTENDED_TITLE_NAMES = new ImmutableList.Builder<String>()
            .addAll(TITLE_NAMES)
            .add("ScanRows").add("ScanBytes").add("ProcessRows").add("CpuMs")
            .add("MaxPeakMemoryBytes").add("CurrentUsedMemoryBytes").add("WorkloadGroupId")
            .add("ShuffleSendBytes").add("ShuffleSendRows")
            .add("ScanBytesFromLocalStorage").add("ScanBytesFromRemoteStorage")
            .add("TotalTasks").add("FinishedTasks").add("Progress").build();
    private static final int EXEC_TIME_INDEX = 5;

    @Override
    public ProcResult fetchResult() throws AnalysisException {
        return fetchResult(Config.enable_extended_current_query_stats);
    }

    public ProcResult fetchResult(boolean extended) throws AnalysisException {
        final BaseProcResult result = new BaseProcResult();
        final Map<String, QueryStatisticsItem> statistic =
                QeProcessorImpl.INSTANCE.getQueryStatistics();
        result.setNames(extended ? EXTENDED_TITLE_NAMES : TITLE_NAMES);
        QueryStatisticsSnapshot snapshot = extended
                ? Env.getCurrentEnv().getWorkloadRuntimeStatusMgr().getQueryStatisticsSnapshot() : null;
        final List<List<String>> sortedRowData = Lists.newArrayList();

        for (QueryStatisticsItem item : statistic.values()) {
            final List<String> values = Lists.newArrayList();
            values.add(item.getQueryId());
            values.add(item.getConnId());
            values.add(item.getCatalog());
            values.add(item.getDb());
            values.add(item.getUser());
            values.add(item.getQueryExecTime());
            values.add(DigestUtils.md5Hex(item.getSql()));
            values.add(item.getSql());
            if (extended) {
                appendStatistics(values, snapshot, item);
            }
            sortedRowData.add(values);
        }

        // sort according to ExecTime
        sortedRowData.sort((l1, l2) -> {
            final long execTime1 = Long.parseLong(l1.get(EXEC_TIME_INDEX));
            final long execTime2 = Long.parseLong(l2.get(EXEC_TIME_INDEX));
            return Long.compare(execTime2, execTime1);
        });
        result.setRows(sortedRowData);
        return result;
    }

    private void appendStatistics(List<String> row, QueryStatisticsSnapshot snapshot, QueryStatisticsItem item) {
        TQueryStatistics statistics = snapshot.getStatistics().get(item.getQueryId());
        if (statistics == null) {
            row.addAll(Collections.nCopies(EXTENDED_TITLE_NAMES.size() - TITLE_NAMES.size(), "N/A"));
            return;
        }
        row.add(QueryStatisticsFormatter.getRowsReturned(statistics.getScanRows()));
        row.add(QueryStatisticsFormatter.getScanBytes(statistics.getScanBytes()));
        row.add(statistics.isSetProcessRows()
                ? QueryStatisticsFormatter.getRowsReturned(statistics.getProcessRows()) : "N/A");
        row.add(String.valueOf(statistics.getCpuMs()));
        row.add(QueryStatisticsFormatter.getScanBytes(statistics.getMaxPeakMemoryBytes()));
        row.add(QueryStatisticsFormatter.getScanBytes(statistics.getCurrentUsedMemoryBytes()));
        row.add(String.valueOf(statistics.getWorkloadGroupId()));
        row.add(QueryStatisticsFormatter.getScanBytes(statistics.getShuffleSendBytes()));
        row.add(QueryStatisticsFormatter.getRowsReturned(statistics.getShuffleSendRows()));
        row.add(QueryStatisticsFormatter.getScanBytes(statistics.getScanBytesFromLocalStorage()));
        row.add(QueryStatisticsFormatter.getScanBytes(statistics.getScanBytesFromRemoteStorage()));

        long expectedBackends = item.getFragmentInstanceInfos().stream()
                .map(instance -> instance.getAddress()).distinct().count();
        int reportingBackends = snapshot.getReportingBackends().getOrDefault(item.getQueryId(), 0);
        if (expectedBackends == 0 || reportingBackends < expectedBackends || !statistics.isSetTotalTasksNum()
                || statistics.getTotalTasksNum() <= 0
                || statistics.getFinishedTasksNum() > statistics.getTotalTasksNum()) {
            row.addAll(Collections.nCopies(3, "N/A"));
        } else {
            row.add(String.valueOf(statistics.getTotalTasksNum()));
            row.add(String.valueOf(statistics.getFinishedTasksNum()));
            double progress = 100.0 * statistics.getFinishedTasksNum() / statistics.getTotalTasksNum();
            row.add(String.format(Locale.ROOT, "%.1f%%", progress));
        }
    }
}
