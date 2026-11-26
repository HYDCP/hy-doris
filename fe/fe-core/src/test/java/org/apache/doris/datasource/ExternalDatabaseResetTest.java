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

package org.apache.doris.datasource;

import org.apache.doris.catalog.Env;
import org.apache.doris.common.util.Util;
import org.apache.doris.datasource.metacache.MetaCache;
import org.apache.doris.datasource.test.TestExternalCatalog;
import org.apache.doris.datasource.test.TestExternalDatabase;
import org.apache.doris.datasource.test.TestExternalTable;

import com.google.common.util.concurrent.MoreExecutors;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;

public class ExternalDatabaseResetTest {
    @Test
    public void testResetPreservesNamesForLookupsUsingTheRetiredCache() {
        TestExternalCatalog catalog = Mockito.mock(TestExternalCatalog.class);
        catalog.useMetaCache = Optional.of(true);
        Mockito.when(catalog.getUseMetaCache()).thenCallRealMethod();
        Mockito.when(catalog.getId()).thenReturn(7L);
        Mockito.when(catalog.getName()).thenReturn("catalog");
        Mockito.when(catalog.getLowerCaseMetaNames()).thenReturn("true");
        Mockito.when(catalog.listTableNames(null, "db")).thenReturn(Collections.singletonList("OldOrders"))
                .thenReturn(Collections.singletonList("NewOrders"));
        Mockito.when(catalog.fromRemoteTableName(Mockito.eq("db"), Mockito.anyString())).thenReturn("orders");
        TestExternalDatabase db = new TestExternalDatabase(catalog, 1L, "db", "db");
        long tableId = Util.genIdByName("catalog", "db", "orders");

        Env env = Mockito.mock(Env.class);
        ExternalMetaCacheMgr manager = Mockito.mock(ExternalMetaCacheMgr.class);
        Mockito.when(env.getExtMetaCacheMgr()).thenReturn(manager);
        ExecutorService executor = MoreExecutors.newDirectExecutorService();
        try (MockedStatic<Env> currentEnv = Mockito.mockStatic(Env.class)) {
            currentEnv.when(Env::getCurrentEnv).thenReturn(env);
            List<MetaCache<TestExternalTable>> caches = new ArrayList<>();
            Mockito.when(manager.<TestExternalTable>buildMetaCache(Mockito.eq("db"), Mockito.any(), Mockito.any(),
                    Mockito.anyLong(), Mockito.any(), Mockito.any(), Mockito.isNull())).thenAnswer(call -> {
                        MetaCache<TestExternalTable> cache = new MetaCache<>("db", executor,
                                call.getArgument(1), call.getArgument(2), call.getArgument(3),
                                call.getArgument(4), call.getArgument(5), null);
                        caches.add(cache);
                        return cache;
                    });

            Assert.assertEquals(Collections.singleton("orders"), db.getTableNamesWithLock());
            MetaCache<TestExternalTable> retired = caches.get(0);

            Mockito.doAnswer(call -> {
                Assert.assertFalse(Thread.holdsLock(db));
                Assert.assertEquals("NewOrders", db.getTableNullable("orders").getRemoteName());
                return null;
            }).when(manager).invalidateDbCache(7L, "db");

            db.resetMetaToUninitialized();

            Mockito.verify(manager).invalidateDbCache(7L, "db");
            // Finish a lookup that captured the old cache before reset and still needs its remote name.
            TestExternalTable oldTable = retired.getMetaObj("orders", tableId).orElseThrow(AssertionError::new);
            Assert.assertEquals("OldOrders", oldTable.getRemoteName());
            Assert.assertEquals("NewOrders", db.getTableNullable(tableId).getRemoteName());
        } finally {
            executor.shutdown();
        }
    }
}
