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

import org.apache.doris.analysis.DropTableStmt;
import org.apache.doris.analysis.TableName;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.datasource.CatalogMgr;
import org.apache.doris.datasource.ExternalCatalog;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.nereids.analyzer.UnboundTableSink;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.commands.insert.InsertUtils;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class TemporaryTableDropTest {
    @Test
    void bindsCtasSinkByIdAfterCopyingThePlan() {
        Env env = Mockito.mock(Env.class);
        InternalCatalog catalog = Mockito.mock(InternalCatalog.class);
        Database db = new Database(10L, "db");
        OlapTable created = new OlapTable(12L, "target", ImmutableList.of(), KeysType.DUP_KEYS,
                new SinglePartitionInfo(), null);
        db.getIdToTableRef().put(created.getId(), created);
        Mockito.when(env.getInternalCatalog()).thenReturn(catalog);
        Mockito.when(catalog.getName()).thenReturn(InternalCatalog.INTERNAL_CATALOG_NAME);
        Mockito.when(catalog.getDbNullable("db")).thenReturn(db);
        ConnectContext ctx = new ConnectContext();
        ctx.setEnv(env);

        Plan child = Mockito.mock(Plan.class);
        Plan sink = new UnboundTableSink<>(ImmutableList.of("internal", "db", "target"),
                ImmutableList.of(), ImmutableList.of(), ImmutableList.of(), child)
                .withTargetTableId(created.getId()).withChildren(ImmutableList.of(child));

        Assertions.assertSame(created, InsertUtils.getTargetTable(sink, ctx));
        db.getIdToTableRef().remove(created.getId());
        Assertions.assertThrows(AnalysisException.class,
                () -> InsertUtils.getTargetTable(sink, ctx));
    }

    @Test
    void neverDelegatesTemporaryDropToExternalCatalog() throws Exception {
        Env env = Mockito.mock(Env.class, Mockito.CALLS_REAL_METHODS);
        CatalogMgr catalogMgr = Mockito.mock(CatalogMgr.class);
        ExternalCatalog external = Mockito.mock(ExternalCatalog.class);
        Deencapsulation.setField(env, "catalogMgr", catalogMgr);
        Mockito.when(catalogMgr.getCatalogOrException(Mockito.eq("ext"), Mockito.any())).thenReturn(external);

        TableName target = new TableName("ext", "db", "permanent");
        env.dropTable(new DropTableStmt(true, true, target, false));
        DdlException e = Assertions.assertThrows(DdlException.class,
                () -> env.dropTable(new DropTableStmt(false, true, target, false)));
        Assertions.assertTrue(e.getMessage().contains("Unknown table"));
        Mockito.verifyNoInteractions(external);
    }
}
