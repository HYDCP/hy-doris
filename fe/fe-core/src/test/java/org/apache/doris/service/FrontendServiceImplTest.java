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

package org.apache.doris.service;


import org.apache.doris.analysis.CreateDbStmt;
import org.apache.doris.analysis.CreateTableStmt;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.common.Config;
import org.apache.doris.common.ConfigBase;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.ha.FrontendNodeType;
import org.apache.doris.ha.MasterInfo;
import org.apache.doris.insertoverwrite.InsertOverwriteManager;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ShowResultSet;
import org.apache.doris.tablefunction.BackendsTableValuedFunction;
import org.apache.doris.thrift.TBackendsMetadataParams;
import org.apache.doris.thrift.TBeginTxnRequest;
import org.apache.doris.thrift.TBeginTxnResult;
import org.apache.doris.thrift.TCommitTxnRequest;
import org.apache.doris.thrift.TCommitTxnResult;
import org.apache.doris.thrift.TCreatePartitionRequest;
import org.apache.doris.thrift.TCreatePartitionResult;
import org.apache.doris.thrift.TFetchSchemaTableDataRequest;
import org.apache.doris.thrift.TFetchSchemaTableDataResult;
import org.apache.doris.thrift.TGetBinlogLagResult;
import org.apache.doris.thrift.TGetBinlogRequest;
import org.apache.doris.thrift.TGetBinlogResult;
import org.apache.doris.thrift.TGetDbsParams;
import org.apache.doris.thrift.TGetDbsResult;
import org.apache.doris.thrift.TGetMasterTokenRequest;
import org.apache.doris.thrift.TGetMasterTokenResult;
import org.apache.doris.thrift.TGetSnapshotRequest;
import org.apache.doris.thrift.TGetSnapshotResult;
import org.apache.doris.thrift.TLockBinlogRequest;
import org.apache.doris.thrift.TLockBinlogResult;
import org.apache.doris.thrift.TMetadataTableRequestParams;
import org.apache.doris.thrift.TMetadataType;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TNullableStringLiteral;
import org.apache.doris.thrift.TReplacePartitionRequest;
import org.apache.doris.thrift.TReplacePartitionResult;
import org.apache.doris.thrift.TRestoreSnapshotRequest;
import org.apache.doris.thrift.TRestoreSnapshotResult;
import org.apache.doris.thrift.TRollbackTxnRequest;
import org.apache.doris.thrift.TRollbackTxnResult;
import org.apache.doris.thrift.TSchemaTableName;
import org.apache.doris.thrift.TShowUserRequest;
import org.apache.doris.thrift.TShowUserResult;
import org.apache.doris.thrift.TStatusCode;
import org.apache.doris.utframe.UtFrameUtils;

import mockit.Mocked;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

public class FrontendServiceImplTest {
    private static String runningDir = "fe/mocked/FrontendServiceImplTest/" + UUID.randomUUID().toString() + "/";
    private static ConnectContext connectContext;
    @Rule
    public ExpectedException expectedException = ExpectedException.none();
    @Mocked
    ExecuteEnv exeEnv;

    @BeforeClass
    public static void beforeClass() throws Exception {
        FeConstants.runningUnitTest = true;
        FeConstants.default_scheduler_interval_millisecond = 100;
        Config.dynamic_partition_enable = true;
        Config.dynamic_partition_check_interval_seconds = 1;
        UtFrameUtils.createDorisCluster(runningDir, 2);
        // create connect context
        connectContext = UtFrameUtils.createDefaultCtx();
        // create database
        String createDbStmtStr = "create database test;";
        CreateDbStmt createDbStmt = (CreateDbStmt) UtFrameUtils.parseAndAnalyzeStmt(createDbStmtStr, connectContext);
        Env.getCurrentEnv().createDb(createDbStmt);
    }

    @AfterClass
    public static void tearDown() {
        UtFrameUtils.cleanDorisFeDir(runningDir);
    }

    private static void createTable(String sql) throws Exception {
        CreateTableStmt createTableStmt = (CreateTableStmt) UtFrameUtils.parseAndAnalyzeStmt(sql, connectContext);
        Env.getCurrentEnv().createTable(createTableStmt);
    }

    private static class NotMasterEnvState implements AutoCloseable {
        private final Env env;
        private final FrontendNodeType oldFeType;
        private final MasterInfo oldMasterInfo;
        private final AtomicBoolean isReady;
        private final boolean oldReady;

        NotMasterEnvState(String masterHost, int masterRpcPort) {
            env = Env.getCurrentEnv();
            oldFeType = Deencapsulation.getField(env, "feType");
            oldMasterInfo = Deencapsulation.getField(env, "masterInfo");
            isReady = Deencapsulation.getField(env, "isReady");
            oldReady = isReady.get();

            Deencapsulation.setField(env, "feType", FrontendNodeType.FOLLOWER);
            Deencapsulation.setField(env, "masterInfo", new MasterInfo(masterHost, 0, masterRpcPort));
            isReady.set(masterRpcPort > 0);
        }

        @Override
        public void close() {
            Deencapsulation.setField(env, "feType", oldFeType);
            Deencapsulation.setField(env, "masterInfo", oldMasterInfo);
            isReady.set(oldReady);
        }
    }

    private void assertNotMasterWithAddress(TNetworkAddress address) {
        Assert.assertNotNull(address);
        Assert.assertEquals("master-fe", address.getHostname());
        Assert.assertEquals(9020, address.getPort());
    }

    @Test
    public void testNotMasterCcrApisReturnValidMasterAddress() throws Exception {
        try (NotMasterEnvState ignored = new NotMasterEnvState("master-fe", 9020)) {
            FrontendServiceImpl impl = new FrontendServiceImpl(exeEnv);

            TBeginTxnResult beginTxnResult = impl.beginTxn(new TBeginTxnRequest());
            Assert.assertEquals(TStatusCode.NOT_MASTER, beginTxnResult.getStatus().getStatusCode());
            assertNotMasterWithAddress(beginTxnResult.getMasterAddress());

            TCommitTxnResult commitTxnResult = impl.commitTxn(new TCommitTxnRequest());
            Assert.assertEquals(TStatusCode.NOT_MASTER, commitTxnResult.getStatus().getStatusCode());
            assertNotMasterWithAddress(commitTxnResult.getMasterAddress());

            TRollbackTxnResult rollbackTxnResult = impl.rollbackTxn(new TRollbackTxnRequest());
            Assert.assertEquals(TStatusCode.NOT_MASTER, rollbackTxnResult.getStatus().getStatusCode());
            assertNotMasterWithAddress(rollbackTxnResult.getMasterAddress());

            TGetMasterTokenResult masterTokenResult = impl.getMasterToken(new TGetMasterTokenRequest());
            Assert.assertEquals(TStatusCode.NOT_MASTER, masterTokenResult.getStatus().getStatusCode());
            assertNotMasterWithAddress(masterTokenResult.getMasterAddress());

            TGetBinlogRequest binlogRequest = new TGetBinlogRequest();
            TGetBinlogResult binlogResult = impl.getBinlog(binlogRequest);
            Assert.assertEquals(TStatusCode.NOT_MASTER, binlogResult.getStatus().getStatusCode());
            assertNotMasterWithAddress(binlogResult.getMasterAddress());

            TGetBinlogLagResult binlogLagResult = impl.getBinlogLag(binlogRequest);
            Assert.assertEquals(TStatusCode.NOT_MASTER, binlogLagResult.getStatus().getStatusCode());
            assertNotMasterWithAddress(binlogLagResult.getMasterAddress());

            TLockBinlogResult lockBinlogResult = impl.lockBinlog(new TLockBinlogRequest());
            Assert.assertEquals(TStatusCode.NOT_MASTER, lockBinlogResult.getStatus().getStatusCode());
            assertNotMasterWithAddress(lockBinlogResult.getMasterAddress());

            TGetSnapshotResult getSnapshotResult = impl.getSnapshot(new TGetSnapshotRequest());
            Assert.assertEquals(TStatusCode.NOT_MASTER, getSnapshotResult.getStatus().getStatusCode());
            assertNotMasterWithAddress(getSnapshotResult.getMasterAddress());

            TRestoreSnapshotResult restoreSnapshotResult = impl.restoreSnapshot(new TRestoreSnapshotRequest());
            Assert.assertEquals(TStatusCode.NOT_MASTER, restoreSnapshotResult.getStatus().getStatusCode());
            assertNotMasterWithAddress(restoreSnapshotResult.getMasterAddress());
        }
    }

    @Test
    public void testNotMasterCcrApisSkipInvalidMasterAddress() throws Exception {
        try (NotMasterEnvState ignored = new NotMasterEnvState("", 0)) {
            FrontendServiceImpl impl = new FrontendServiceImpl(exeEnv);

            TGetBinlogResult binlogResult = impl.getBinlog(new TGetBinlogRequest());
            Assert.assertEquals(TStatusCode.NOT_MASTER, binlogResult.getStatus().getStatusCode());
            Assert.assertFalse(binlogResult.isSetMasterAddress());
        }
    }


    @Test
    public void testCreatePartitionRange() throws Exception {
        String createOlapTblStmt = new String("CREATE TABLE test.partition_range(\n"
                + "    event_day DATETIME NOT NULL,\n"
                + "    site_id INT DEFAULT '10',\n"
                + "    city_code VARCHAR(100)\n"
                + ")\n"
                + "DUPLICATE KEY(event_day, site_id, city_code)\n"
                + "AUTO PARTITION BY range (date_trunc( event_day,'day')) (\n"
                + "\n"
                + ")\n"
                + "DISTRIBUTED BY HASH(event_day, site_id) BUCKETS 2\n"
                + "PROPERTIES(\"replication_num\" = \"1\");");

        createTable(createOlapTblStmt);
        Database db = Env.getCurrentInternalCatalog().getDbOrAnalysisException("test");
        OlapTable table = (OlapTable) db.getTableOrAnalysisException("partition_range");

        List<List<TNullableStringLiteral>> partitionValues = new ArrayList<>();
        List<TNullableStringLiteral> values = new ArrayList<>();

        TNullableStringLiteral start = new TNullableStringLiteral();
        start.setValue("2023-08-07 00:00:00");
        values.add(start);

        partitionValues.add(values);

        FrontendServiceImpl impl = new FrontendServiceImpl(exeEnv);
        TCreatePartitionRequest request = new TCreatePartitionRequest();
        request.setDbId(db.getId());
        request.setTableId(table.getId());
        request.setPartitionValues(partitionValues);
        TCreatePartitionResult partition = impl.createPartition(request);

        Assert.assertEquals(partition.getStatus().getStatusCode(), TStatusCode.OK);
        Partition p20230807 = table.getPartition("p20230807000000");
        Assert.assertNotNull(p20230807);
    }

    @Test
    public void testIncrementalPartitionReplicaRequirements() throws Exception {
        boolean originalAllowReplicaOnSameHost = Config.allow_replica_on_same_host;
        boolean originalForceMajorityLoad = Config.force_majority_load_for_two_replica;
        Config.allow_replica_on_same_host = true;
        Config.force_majority_load_for_two_replica = true;
        try {
            createTable("CREATE TABLE test.incremental_partition_replica_requirements(\n"
                    + "    event_day DATETIME NOT NULL,\n"
                    + "    site_id INT\n"
                    + ")\n"
                    + "DUPLICATE KEY(event_day, site_id)\n"
                    + "AUTO PARTITION BY range (date_trunc(event_day, 'day')) ()\n"
                    + "DISTRIBUTED BY HASH(event_day) BUCKETS 1\n"
                    + "PROPERTIES(\n"
                    + "    'replication_num' = '2',\n"
                    + "    'min_load_replica_num' = '1'\n"
                    + ");");

            Database db = Env.getCurrentInternalCatalog().getDbOrAnalysisException("test");
            OlapTable table = (OlapTable) db.getTableOrAnalysisException(
                    "incremental_partition_replica_requirements");
            List<TNullableStringLiteral> values = new ArrayList<>();
            values.add(new TNullableStringLiteral().setValue("2023-08-08 00:00:00"));

            TCreatePartitionRequest createRequest = new TCreatePartitionRequest();
            createRequest.setDbId(db.getId());
            createRequest.setTableId(table.getId());
            createRequest.setPartitionValues(Arrays.asList(values));
            FrontendServiceImpl impl = new FrontendServiceImpl(exeEnv);
            TCreatePartitionResult createResult = impl.createPartition(createRequest);

            Assert.assertEquals(TStatusCode.OK, createResult.getStatus().getStatusCode());
            Assert.assertEquals(1, createResult.getPartitionsSize());
            Assert.assertEquals(2, createResult.getPartitions().get(0).getTotalReplicaNum());
            Assert.assertEquals(2, createResult.getPartitions().get(0).getLoadRequiredReplicaNum());

            Partition partition = table.getPartition("p20230808000000");
            Assert.assertNotNull(partition);
            InsertOverwriteManager overwriteManager = Env.getCurrentEnv().getInsertOverwriteManager();
            long taskGroupId = overwriteManager.registerTaskGroup(table.getId());
            try {
                TReplacePartitionRequest replaceRequest = new TReplacePartitionRequest();
                replaceRequest.setOverwriteGroupId(taskGroupId);
                replaceRequest.setDbId(db.getId());
                replaceRequest.setTableId(table.getId());
                replaceRequest.setPartitionIds(Arrays.asList(partition.getId()));
                TReplacePartitionResult replaceResult = impl.replacePartition(replaceRequest);

                Assert.assertEquals(TStatusCode.OK, replaceResult.getStatus().getStatusCode());
                Assert.assertEquals(1, replaceResult.getPartitionsSize());
                Assert.assertEquals(2, replaceResult.getPartitions().get(0).getTotalReplicaNum());
                Assert.assertEquals(2, replaceResult.getPartitions().get(0).getLoadRequiredReplicaNum());
            } finally {
                overwriteManager.taskGroupFail(taskGroupId);
            }
        } finally {
            Config.allow_replica_on_same_host = originalAllowReplicaOnSameHost;
            Config.force_majority_load_for_two_replica = originalForceMajorityLoad;
        }
    }

    @Test
    public void testCreatePartitionRangeMedium() throws Exception {
        ConfigBase.setMutableConfig("disable_storage_medium_check", "true");
        String createOlapTblStmt = new String("CREATE TABLE test.partition_range2(\n"
                + "    event_day DATETIME NOT NULL,\n"
                + "    site_id INT DEFAULT '10',\n"
                + "    city_code VARCHAR(100)\n"
                + ")\n"
                + "DUPLICATE KEY(event_day, site_id, city_code)\n"
                + "AUTO PARTITION BY range (date_trunc( event_day,'day')) (\n"
                + "\n"
                + ")\n"
                + "DISTRIBUTED BY HASH(event_day, site_id) BUCKETS 2\n"
                + "PROPERTIES(\"storage_medium\" = \"ssd\",\"replication_num\" = \"1\");");

        createTable(createOlapTblStmt);
        Database db = Env.getCurrentInternalCatalog().getDbOrAnalysisException("test");
        OlapTable table = (OlapTable) db.getTableOrAnalysisException("partition_range2");

        List<List<TNullableStringLiteral>> partitionValues = new ArrayList<>();
        List<TNullableStringLiteral> values = new ArrayList<>();

        TNullableStringLiteral start = new TNullableStringLiteral();
        start.setValue("2023-08-07 00:00:00");
        values.add(start);

        partitionValues.add(values);

        FrontendServiceImpl impl = new FrontendServiceImpl(exeEnv);
        TCreatePartitionRequest request = new TCreatePartitionRequest();
        request.setDbId(db.getId());
        request.setTableId(table.getId());
        request.setPartitionValues(partitionValues);
        TCreatePartitionResult partition = impl.createPartition(request);

        Assert.assertEquals(partition.getStatus().getStatusCode(), TStatusCode.OK);
        Partition p20230807 = table.getPartition("p20230807000000");
        Assert.assertNotNull(p20230807);

        ShowResultSet result = UtFrameUtils.showPartitionsByName(connectContext, "test.partition_range2");
        String showCreateTableResultSql = result.getResultRows().get(0).get(10);
        System.out.println(showCreateTableResultSql);
        Assert.assertEquals(showCreateTableResultSql, "SSD");
    }

    @Test
    public void testCreatePartitionList() throws Exception {
        String createOlapTblStmt = new String("CREATE TABLE test.partition_list(\n"
                + "    event_day DATETIME,\n"
                + "    site_id INT DEFAULT '10',\n"
                + "    city_code VARCHAR(100) NOT NULL\n"
                + ")\n"
                + "DUPLICATE KEY(event_day, site_id, city_code)\n"
                + "AUTO PARTITION BY list (city_code) (\n"
                + "\n"
                + ")\n"
                + "DISTRIBUTED BY HASH(event_day, site_id) BUCKETS 2\n"
                + "PROPERTIES(\"replication_num\" = \"1\");");

        createTable(createOlapTblStmt);
        Database db = Env.getCurrentInternalCatalog().getDbOrAnalysisException("test");
        OlapTable table = (OlapTable) db.getTableOrAnalysisException("partition_list");

        List<List<TNullableStringLiteral>> partitionValues = new ArrayList<>();
        List<TNullableStringLiteral> values = new ArrayList<>();

        TNullableStringLiteral start = new TNullableStringLiteral();
        start.setValue("BEIJING");
        values.add(start);

        partitionValues.add(values);

        FrontendServiceImpl impl = new FrontendServiceImpl(exeEnv);
        TCreatePartitionRequest request = new TCreatePartitionRequest();
        request.setDbId(db.getId());
        request.setTableId(table.getId());
        request.setPartitionValues(partitionValues);
        TCreatePartitionResult partition = impl.createPartition(request);

        Assert.assertEquals(partition.getStatus().getStatusCode(), TStatusCode.OK);
        List<Partition> pbs = (List<Partition>) table.getAllPartitions();
        Assert.assertEquals(pbs.size(), 1);
    }

    @Test
    public void testGetDBNames() throws Exception {
        // create database
        String createDbStmtStr = "create database `test_`;";
        CreateDbStmt createDbStmt = (CreateDbStmt) UtFrameUtils.parseAndAnalyzeStmt(createDbStmtStr, connectContext);
        Env.getCurrentEnv().createDb(createDbStmt);

        FrontendServiceImpl impl = new FrontendServiceImpl(exeEnv);
        TGetDbsParams params = new TGetDbsParams();
        params.setPattern("tes%");
        params.setCurrentUserIdent(connectContext.getCurrentUserIdentity().toThrift());
        TGetDbsResult dbNames = impl.getDbNames(params);

        Assert.assertEquals(dbNames.getDbs().size(), 2);
        List<String> expected = Arrays.asList("test", "test_");
        dbNames.getDbs().sort(String::compareTo);
        expected.sort(String::compareTo);
        Assert.assertEquals(dbNames.getDbs(), expected);
    }

    @Test
    public void fetchSchemaTableData() throws Exception {
        FrontendServiceImpl impl = new FrontendServiceImpl(exeEnv);

        TFetchSchemaTableDataRequest request = new TFetchSchemaTableDataRequest();
        request.setSchemaTableName(TSchemaTableName.METADATA_TABLE);

        TFetchSchemaTableDataResult result = impl.fetchSchemaTableData(request);
        Assert.assertEquals(result.getStatus().getStatusCode(), TStatusCode.INTERNAL_ERROR);
        Assert.assertEquals(result.getStatus().getErrorMsgs().get(0), "Metadata table params is not set. ");

        TMetadataTableRequestParams params = new TMetadataTableRequestParams();
        request.setMetadaTableParams(params);
        result = impl.fetchSchemaTableData(request);
        Assert.assertEquals(result.getStatus().getStatusCode(), TStatusCode.INTERNAL_ERROR);
        Assert.assertEquals(result.getStatus().getErrorMsgs().get(0), "Metadata table params is not set. ");

        params.setMetadataType(TMetadataType.BACKENDS);
        request.setMetadaTableParams(params);
        result = impl.fetchSchemaTableData(request);
        Assert.assertEquals(result.getStatus().getStatusCode(), TStatusCode.INTERNAL_ERROR);
        Assert.assertEquals(result.getStatus().getErrorMsgs().get(0), "backends metadata param is not set.");

        params.setMetadataType(TMetadataType.BACKENDS);
        TBackendsMetadataParams backendsMetadataParams = new TBackendsMetadataParams();
        backendsMetadataParams.setClusterName("");
        params.setBackendsMetadataParams(backendsMetadataParams);
        params.setColumnsName((new BackendsTableValuedFunction(new HashMap<String, String>())).getTableColumns()
                .stream().map(c -> c.getName()).collect(Collectors.toList()));
        request.setMetadaTableParams(params);
        result = impl.fetchSchemaTableData(request);
        Assert.assertEquals(result.getStatus().getStatusCode(), TStatusCode.OK);
        Assert.assertEquals(Env.getCurrentSystemInfo().getAllBackendIds(false).size(), result.getDataBatchSize());
    }

    @Test
    public void testShowUser() {
        FrontendServiceImpl impl = new FrontendServiceImpl(exeEnv);
        TShowUserRequest request = new TShowUserRequest();
        TShowUserResult result = impl.showUser(request);
        System.out.println(result);
    }
}
