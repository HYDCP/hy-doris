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

import org.apache.doris.catalog.BinlogConfig;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.Config;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.persist.ReplaceTableOperationLog;
import org.apache.doris.thrift.TBinlog;
import org.apache.doris.thrift.TBinlogType;

import mockit.Mock;
import mockit.MockUp;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ReplaceTableBinlogTest {
    private static final long DB_ID = 1L;
    private static final long TABLE_ID = 2L;

    @Before
    public void setUp() {
        Config.enable_feature_binlog = true;

        Database database = new Database();
        new MockUp<Env>() {
            @Mock
            public InternalCatalog getCurrentInternalCatalog() {
                return new InternalCatalog();
            }
        };
        new MockUp<InternalCatalog>() {
            @Mock
            public Database getDbNullable(long dbId) {
                return database;
            }
        };
        new MockUp<Database>() {
            @Mock
            public BinlogConfig getBinlogConfig() {
                return new BinlogConfig();
            }
        };
    }

    @Test
    public void testTableBinlogReplaceUsesPersistedOriginConfig() throws Exception {
        BinlogManager manager = new BinlogManager();
        ReplaceTableOperationLog replaceLog = new ReplaceTableOperationLog(DB_ID, TABLE_ID, "origin", 3L,
                "replacement", false, new BinlogConfig(true, 3600L, Long.MAX_VALUE, Long.MAX_VALUE));

        manager.addReplaceTable(replaceLog, 10L);

        Field dbBinlogMapField = BinlogManager.class.getDeclaredField("dbBinlogMap");
        dbBinlogMapField.setAccessible(true);
        Map<Long, DBBinlog> dbBinlogMap = (Map<Long, DBBinlog>) dbBinlogMapField.get(manager);
        List<TBinlog> binlogs = new ArrayList<>();
        dbBinlogMap.get(DB_ID).getAllBinlogs(binlogs);

        Assert.assertTrue(binlogs.stream().anyMatch(binlog -> binlog.getType() == TBinlogType.REPLACE_TABLE
                && binlog.getCommitSeq() == 10L));
    }
}
