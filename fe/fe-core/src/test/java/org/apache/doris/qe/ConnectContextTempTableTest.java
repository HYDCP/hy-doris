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

package org.apache.doris.qe;

import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.Table;
import org.apache.doris.datasource.InternalCatalog;

import mockit.Delegate;
import mockit.Expectations;
import mockit.Invocation;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import mockit.Verifications;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class ConnectContextTempTableTest {
    @Mocked
    private Env env;

    @Test
    public void testMasterContinuesAfterMissingDatabaseAndTable(@Mocked InternalCatalog catalog,
            @Mocked Database db, @Mocked Table table) throws Exception {
        ConnectContext ctx = new ConnectContext();
        ctx.addTempTableToDB("empty_db", "removed");
        ctx.removeTempTableFromDB("empty_db", "removed");
        ctx.addTempTableToDB("dropped_db", "stale");
        ctx.addTempTableToDB("live_db", "missing");
        ctx.addTempTableToDB("live_db", "remaining");

        new Expectations() {
            {
                Env.getCurrentEnv();
                result = env;
                env.isMaster();
                result = true;
                env.getInternalCatalog();
                result = catalog;
                catalog.getDb("dropped_db");
                result = Optional.empty();
                catalog.getDb("live_db");
                result = Optional.of(db);
                db.getTable("missing");
                result = Optional.empty();
                db.getTable("remaining");
                result = Optional.of(table);
            }
        };

        ctx.cleanup();

        new Verifications() {
            {
                catalog.getDb("empty_db");
                times = 0;
                catalog.dropTableWithoutCheck(db, table, false, true);
                times = 1;
                env.unregisterSessionInfo(ctx.getSessionId());
                times = 1;
            }
        };
    }

    @Test
    public void testDropCanUnregisterTablesDuringCleanup(@Mocked InternalCatalog catalog,
            @Mocked Database db, @Mocked Table table) throws Exception {
        ConnectContext ctx = new ConnectContext();
        ctx.addTempTableToDB("db", "first");
        ctx.addTempTableToDB("db", "second");
        List<String> dropped = new ArrayList<>();
        new Expectations() {
            {
                Env.getCurrentEnv();
                result = env;
                env.isMaster();
                result = true;
                env.getInternalCatalog();
                result = catalog;
                catalog.getDb("db");
                result = Optional.of(db);
                db.getTable(anyString);
                result = new Delegate<Object>() {
                    Optional<Table> lookup(String name) {
                        dropped.add(name);
                        return Optional.of(table);
                    }
                };
                catalog.dropTableWithoutCheck(db, table, false, true);
                result = new Delegate<Object>() {
                    void drop() {
                        ctx.removeTempTableFromDB("db", dropped.get(dropped.size() - 1));
                    }
                };
                times = 2;
            }
        };

        ctx.killConnection();

        Assert.assertTrue(dropped.contains("first"));
        Assert.assertTrue(dropped.contains("second"));
    }

    @Test
    public void testFollowerForwardsQualifiedNames() {
        ConnectContext ctx = new ConnectContext();
        ctx.setEnv(env);
        ctx.addTempTableToDB("db_a", "tmp");
        ctx.addTempTableToDB("db_b", "tmp");
        new Expectations() {
            {
                Env.getCurrentEnv();
                result = env;
                env.isMaster();
                result = false;
            }
        };

        List<String> sql = new ArrayList<>();
        new MockUp<MasterOpExecutor>() {
            @Mock
            public void execute(Invocation invocation) {
                MasterOpExecutor executor = invocation.getInvokedInstance();
                sql.add(executor.originStmt.originStmt);
            }
        };

        ctx.cleanup();

        Assert.assertEquals(2, sql.size());
        Assert.assertTrue(sql.contains("drop table `db_a`.`tmp`"));
        Assert.assertTrue(sql.contains("drop table `db_b`.`tmp`"));
    }

    @Test
    public void testProxyLeavesTablesToCreatingSession() {
        ConnectContext proxy = new ConnectContext(null, true);
        proxy.addTempTableToDB("db", "tmp");

        proxy.deleteTempTable();

        new Verifications() {
            {
                env.isMaster();
                times = 0;
            }
        };
    }
}
