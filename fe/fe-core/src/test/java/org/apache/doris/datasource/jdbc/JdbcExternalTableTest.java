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

package org.apache.doris.datasource.jdbc;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.datasource.ExternalMetaCacheMgr;
import org.apache.doris.datasource.ExternalSchemaCache;
import org.apache.doris.datasource.SchemaCacheValue;
import org.apache.doris.datasource.jdbc.client.JdbcClientException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

public class JdbcExternalTableTest {

    @Test
    public void testInitSchemaReturnsNegativeResultForEmptyColumns() {
        JdbcExternalCatalog catalog = Mockito.mock(JdbcExternalCatalog.class);
        JdbcExternalDatabase database = Mockito.mock(JdbcExternalDatabase.class);
        mockTableIdentity(catalog, database);
        Mockito.when(database.getRemoteName()).thenReturn("remote_db");
        Mockito.when(catalog.listColumns("remote_db", "remote_tbl")).thenReturn(Collections.emptyList());
        JdbcExternalTable table = new JdbcExternalTable(
                1L, "local_tbl", "remote_tbl", catalog, database);

        Assertions.assertFalse(table.initSchema().isPresent());
        Mockito.verify(catalog).listColumns("remote_db", "remote_tbl");
    }

    @Test
    public void testCachedEmptySchemaFailsAtJdbcTableBoundary() {
        JdbcExternalCatalog catalog = Mockito.mock(JdbcExternalCatalog.class);
        JdbcExternalDatabase database = Mockito.mock(JdbcExternalDatabase.class);
        mockTableIdentity(catalog, database);
        JdbcExternalTable table = new JdbcExternalTable(
                1L, "local_tbl", "remote_tbl", catalog, database);

        ExternalSchemaCache schemaCache = Mockito.mock(ExternalSchemaCache.class);
        Mockito.when(schemaCache.getSchemaValue(Mockito.any())).thenReturn(Optional.empty());

        try (MockedStatic<Env> mockedEnv = mockSchemaCache(catalog, schemaCache)) {
            JdbcClientException fullSchemaException = Assertions.assertThrows(
                    JdbcClientException.class, table::getFullSchema);
            JdbcClientException baseSchemaException = Assertions.assertThrows(
                    JdbcClientException.class, table::getBaseSchema);

            Assertions.assertEquals("failed to get jdbc columns info for remote table `remote_db.remote_tbl` "
                            + "in catalog `jdbc_ctl`: no columns returned",
                    fullSchemaException.getMessage());
            Assertions.assertEquals(fullSchemaException.getMessage(), baseSchemaException.getMessage());
            Mockito.verify(schemaCache, Mockito.times(2)).getSchemaValue(Mockito.any());
            Mockito.verify(catalog, Mockito.never()).listColumns(Mockito.anyString(), Mockito.anyString());
        }
    }

    @Test
    public void testGetFullSchemaReturnsCachedColumns() {
        JdbcExternalCatalog catalog = Mockito.mock(JdbcExternalCatalog.class);
        JdbcExternalDatabase database = Mockito.mock(JdbcExternalDatabase.class);
        mockTableIdentity(catalog, database);
        JdbcExternalTable table = new JdbcExternalTable(
                1L, "local_tbl", "remote_tbl", catalog, database);

        List<Column> columns = Collections.singletonList(Mockito.mock(Column.class));
        ExternalSchemaCache schemaCache = Mockito.mock(ExternalSchemaCache.class);
        Mockito.when(schemaCache.getSchemaValue(Mockito.any()))
                .thenReturn(Optional.of(new SchemaCacheValue(columns)));

        try (MockedStatic<Env> mockedEnv = mockSchemaCache(catalog, schemaCache)) {
            Assertions.assertSame(columns, table.getFullSchema());
        }
    }

    private void mockTableIdentity(JdbcExternalCatalog catalog, JdbcExternalDatabase database) {
        Mockito.when(catalog.getId()).thenReturn(10L);
        Mockito.when(catalog.getName()).thenReturn("jdbc_ctl");
        Mockito.when(database.getFullName()).thenReturn("local_db");
        Mockito.when(database.getRemoteName()).thenReturn("remote_db");
    }

    private MockedStatic<Env> mockSchemaCache(JdbcExternalCatalog catalog, ExternalSchemaCache schemaCache) {
        Env env = Mockito.mock(Env.class);
        ExternalMetaCacheMgr cacheMgr = Mockito.mock(ExternalMetaCacheMgr.class);
        MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class);
        mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
        Mockito.when(env.getExtMetaCacheMgr()).thenReturn(cacheMgr);
        Mockito.when(cacheMgr.getSchemaCache(catalog)).thenReturn(schemaCache);
        return mockedEnv;
    }
}
