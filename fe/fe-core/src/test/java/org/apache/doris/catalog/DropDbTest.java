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

package org.apache.doris.catalog;

import org.apache.doris.analysis.CreateDbStmt;
import org.apache.doris.analysis.CreateTableStmt;
import org.apache.doris.analysis.DropDbStmt;
import org.apache.doris.analysis.RecoverDbStmt;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.ExceptionChecker;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.journal.JournalEntity;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.plans.commands.CreateTableCommand;
import org.apache.doris.persist.DropInfo;
import org.apache.doris.persist.EditLog;
import org.apache.doris.persist.OperationType;
import org.apache.doris.persist.RecoverInfo;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.utframe.UtFrameUtils;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.util.List;
import java.util.UUID;

public class DropDbTest {
    private static String runningDir = "fe/mocked/DropDbTest/" + UUID.randomUUID().toString() + "/";

    private static ConnectContext connectContext;

    @BeforeClass
    public static void beforeClass() throws Exception {
        UtFrameUtils.createDorisCluster(runningDir);

        // create connect context
        connectContext = UtFrameUtils.createDefaultCtx();
        // create database
        String createDbStmtStr1 = "create database test1;";
        String createDbStmtStr2 = "create database test2;";
        String createDbStmtStr3 = "create database test3;";
        String createTablleStr1 = "create table test1.tbl1(k1 int, k2 bigint) duplicate key(k1) "
                + "distributed by hash(k2) buckets 1" + " properties('replication_num' = '1');";
        String createTablleStr2 = "create table test2.tbl1" + "(k1 int, k2 bigint)" + " duplicate key(k1) "
                + "distributed by hash(k2) buckets 1 " + "properties('replication_num' = '1');";
        createDb(createDbStmtStr1);
        createDb(createDbStmtStr2);
        createDb(createDbStmtStr3);
        createTable(createTablleStr1);
        createTable(createTablleStr2);
    }

    @Test
    public void legacyDatabaseRecoveryDropsOnlyTemporaryTables() throws Exception {
        String name = "legacy_temp_recover";
        createDb("CREATE DATABASE " + name);
        String definition = " (id INT) DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES ('replication_num'='1')";
        createTable("CREATE TABLE " + name + ".shadow" + definition);
        createTemporaryTable("CREATE TEMPORARY TABLE " + name + ".shadow" + definition);
        InternalCatalog catalog = Env.getCurrentInternalCatalog();
        Database db = catalog.getDbOrDdlException(name);
        Table permanent = db.getNonTempTableNullable("shadow");
        Table temp = db.getTemporaryTableNullable("shadow");
        long dbId = db.getId();

        // Older DROP DATABASE records put both tables in the recovery manifest.
        catalog.replayDropDb(name, false, System.currentTimeMillis());
        RecoverInfo oldRecover = RecoverInfo.fromJson("{\"dbId\":" + dbId + "}");
        catalog.replayRecoverDatabase(oldRecover);
        Assert.assertSame(temp, catalog.getDbOrDdlException(name).getTableNullable(temp.getId()));

        catalog.replayDropDb(name, false, System.currentTimeMillis());
        RecoverDbStmt recover = (RecoverDbStmt) UtFrameUtils.parseAndAnalyzeStmt("RECOVER DATABASE " + name,
                connectContext);
        Env.getCurrentEnv().recoverDatabase(recover);
        Assert.assertNull(db.getTableNullable(temp.getId()));
        Assert.assertSame(permanent, db.getTableNullable("shadow"));
        Assert.assertThrows(DdlException.class, () -> Env.getCurrentRecycleBin().recoverTable(
                db, temp.getName(), temp.getId(), ""));

        createTemporaryTable("CREATE TEMPORARY TABLE " + name + ".shadow" + definition);
        Table next = db.getTemporaryTableNullable("shadow");
        catalog.replayDropDb(name, false, System.currentTimeMillis());
        String renamed = name + "_renamed";
        RecoverInfo newRecover = new RecoverInfo(dbId, -1, -1, renamed, "", "", "", "");
        newRecover.markDiscardTemporaryTables();
        catalog.replayRecoverDatabase(RecoverInfo.fromJson(newRecover.toJson()));
        Assert.assertSame(db, catalog.getDbOrDdlException(renamed));
        Assert.assertEquals(renamed, next.getQualifiedDbName());
        // Promotion here must be safe even if only the RECOVER record reached the replica.
        Assert.assertNull(db.getTableNullable(next.getId()));
        Assert.assertSame(permanent, db.getTableNullable("shadow"));
        JournalEntity compatDrop = new JournalEntity();
        compatDrop.setOpCode(OperationType.OP_DROP_TABLE);
        compatDrop.setData(DropInfo.forRecoveredTemporaryTable(dbId, next.getId(), next.getName()));
        EditLog.loadJournal(Env.getCurrentEnv(), 1L, compatDrop);
        Assert.assertSame(permanent, db.getTableNullable("shadow"));
        Assert.assertTrue(Env.getCurrentRecycleBin().isRecycleTable(dbId, next.getId()));
    }

    private static void createTemporaryTable(String sql) throws Exception {
        CreateTableCommand command = (CreateTableCommand) new NereidsParser().parseSingle(sql);
        command.run(connectContext, new StmtExecutor(connectContext, sql));
    }

    @AfterClass
    public static void tearDown() {
        File file = new File(runningDir);
        file.delete();
    }

    private static void createDb(String sql) throws Exception {
        CreateDbStmt createDbStmt = (CreateDbStmt) UtFrameUtils.parseAndAnalyzeStmt(sql, connectContext);
        Env.getCurrentEnv().createDb(createDbStmt);
    }

    private static void dropDb(String sql) throws Exception {
        DropDbStmt dropDbStmt = (DropDbStmt) UtFrameUtils.parseAndAnalyzeStmt(sql, connectContext);
        Env.getCurrentEnv().dropDb(dropDbStmt);
    }

    private static void createTable(String sql) throws Exception {
        CreateTableStmt createTableStmt = (CreateTableStmt) UtFrameUtils.parseAndAnalyzeStmt(sql, connectContext);
        Env.getCurrentEnv().createTable(createTableStmt);
    }

    @Test
    public void testNormalDropDb() throws Exception {
        Database db = Env.getCurrentInternalCatalog().getDbOrMetaException("test1");
        OlapTable table = (OlapTable) db.getTableOrMetaException("tbl1");
        Partition partition = table.getAllPartitions().iterator().next();
        long tabletId = partition.getBaseIndex().getTablets().get(0).getId();
        String dropDbSql = "drop database test1";
        dropDb(dropDbSql);
        db = Env.getCurrentInternalCatalog().getDbNullable("test1");
        Assert.assertNull(db);
        List<Replica> replicaList = Env.getCurrentEnv().getTabletInvertedIndex().getReplicasByTabletId(tabletId);
        Assert.assertEquals(1, replicaList.size());
        String recoverDbSql = "recover database test1";
        RecoverDbStmt recoverDbStmt = (RecoverDbStmt) UtFrameUtils.parseAndAnalyzeStmt(recoverDbSql, connectContext);
        Env.getCurrentEnv().recoverDatabase(recoverDbStmt);
        db = Env.getCurrentInternalCatalog().getDbNullable("test1");
        Assert.assertNotNull(db);
        Assert.assertEquals("test1", db.getFullName());
        table = (OlapTable) db.getTableOrMetaException("tbl1");
        Assert.assertNotNull(table);
        Assert.assertEquals("tbl1", table.getName());

        dropDbSql = "drop schema test1";
        dropDb(dropDbSql);
        db = Env.getCurrentInternalCatalog().getDbNullable("test1");
        Assert.assertNull(db);
        Env.getCurrentEnv().recoverDatabase(recoverDbStmt);
        db = Env.getCurrentInternalCatalog().getDbNullable("test1");
        Assert.assertNotNull(db);

        dropDbSql = "drop schema if exists test1";
        dropDb(dropDbSql);
        db = Env.getCurrentInternalCatalog().getDbNullable("test1");
        Assert.assertNull(db);
    }

    @Test
    public void testForceDropDb() throws Exception {
        String dropDbSql = "drop database test2 force";
        dropDb(dropDbSql);
        Database db = Env.getCurrentInternalCatalog().getDbNullable("test2");
        Assert.assertNull(db);
        // After unify force and non-force drop db, the replicas will be recycled eventually.
        //
        // Database db = Env.getCurrentInternalCatalog().getDbOrMetaException("test2");
        // OlapTable table = (OlapTable) db.getTableOrMetaException("tbl1");
        // Partition partition = table.getAllPartitions().iterator().next();
        // long tabletId = partition.getBaseIndex().getTablets().get(0).getId();
        // ...
        // List<Replica> replicaList = Env.getCurrentEnv().getTabletInvertedIndex().getReplicasByTabletId(tabletId);
        // Assert.assertTrue(replicaList.isEmpty());
        String recoverDbSql = "recover database test2";
        RecoverDbStmt recoverDbStmt = (RecoverDbStmt) UtFrameUtils.parseAndAnalyzeStmt(recoverDbSql, connectContext);
        ExceptionChecker.expectThrowsWithMsg(DdlException.class,
                "Unknown database 'test2' or database id '-1'",
                () -> Env.getCurrentEnv().recoverDatabase(recoverDbStmt));

        dropDbSql = "drop schema test3 force";
        db = Env.getCurrentInternalCatalog().getDbOrMetaException("test3");
        Assert.assertNotNull(db);
        dropDb(dropDbSql);
        db = Env.getCurrentInternalCatalog().getDbNullable("test3");
        Assert.assertNull(db);
        recoverDbSql = "recover database test3";
        RecoverDbStmt recoverDbStmt2 = (RecoverDbStmt) UtFrameUtils.parseAndAnalyzeStmt(recoverDbSql, connectContext);
        ExceptionChecker.expectThrowsWithMsg(DdlException.class,
                "Unknown database 'test3'",
                () -> Env.getCurrentEnv().recoverDatabase(recoverDbStmt2));

        dropDbSql = "drop schema if exists test3 force";
        dropDb(dropDbSql);
    }
}
