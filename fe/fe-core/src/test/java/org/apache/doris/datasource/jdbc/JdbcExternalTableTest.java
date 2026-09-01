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

import org.apache.doris.datasource.jdbc.client.JdbcClientException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Collections;

public class JdbcExternalTableTest {

    @Test
    public void testInitSchemaRejectsEmptyColumns() {
        JdbcExternalCatalog catalog = Mockito.mock(JdbcExternalCatalog.class);
        JdbcExternalDatabase database = Mockito.mock(JdbcExternalDatabase.class);
        Mockito.when(catalog.getName()).thenReturn("jdbc_ctl");
        Mockito.when(database.getRemoteName()).thenReturn("remote_db");
        Mockito.when(catalog.listColumns("remote_db", "remote_tbl")).thenReturn(Collections.emptyList());
        JdbcExternalTable table = new JdbcExternalTable(
                1L, "local_tbl", "remote_tbl", catalog, database);

        JdbcClientException exception = Assertions.assertThrows(JdbcClientException.class, table::initSchema);

        Assertions.assertEquals("failed to get jdbc columns info for remote table `remote_db.remote_tbl` "
                        + "in catalog `jdbc_ctl`: no columns returned",
                exception.getMessage());
    }
}
