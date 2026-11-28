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

package org.apache.doris.analysis;

import org.apache.doris.catalog.Env;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.mysql.privilege.MockedAuth;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Map;

public class BackupTest {

    private Analyzer analyzer;

    private Env env;

    @Mocked
    private ConnectContext ctx;

    @Before
    public void setUp() {
        MockedAuth.mockedConnectContext(ctx, "root", "192.188.3.1");

        analyzer = AccessTestUtil.fetchAdminAnalyzer(true);
        env = AccessTestUtil.fetchAdminCatalog();
        new MockUp<Env>() {
            @Mock
            public Env getCurrentEnv() {
                return env;
            }
        };
    }

    public BackupStmt createStmt(boolean caseSensitive) {
        return createStmt(caseSensitive, null);
    }

    public BackupStmt createStmt(boolean caseSensitive, Map<String, String> properties) {
        BackupStmt stmt;
        List<TableRef> tblRefs = Lists.newArrayList();
        String testDB = "test_db";
        tblRefs.add(new TableRef(new TableName(InternalCatalog.INTERNAL_CATALOG_NAME, null, "table1"), null));
        if (caseSensitive) {
            // case sensitive
            tblRefs.add(new TableRef(new TableName(InternalCatalog.INTERNAL_CATALOG_NAME, null, "Table1"), null));
        }
        AbstractBackupTableRefClause tableRefClause = new AbstractBackupTableRefClause(false, tblRefs);
        stmt = new BackupStmt(new LabelName(testDB, "label1"), "repo",
                tableRefClause, properties);
        return stmt;
    }

    @Test
    public void caseSensitiveTest() throws Exception {
        BackupStmt stmt = createStmt(true);
        stmt.analyze(analyzer);
    }

    @Test
    public void testManifestDigestProperty() throws Exception {
        // default false
        BackupStmt stmt = createStmt(false);
        stmt.analyze(analyzer);
        Assert.assertFalse(stmt.isManifestDigest());

        for (String value : new String[] {"true", "TRUE", "false"}) {
            Map<String, String> properties = Maps.newHashMap();
            properties.put(BackupStmt.PROP_MANIFEST_DIGEST, value);
            BackupStmt digestStmt = createStmt(false, properties);
            digestStmt.analyze(analyzer);
            Assert.assertEquals(Boolean.parseBoolean(value), digestStmt.isManifestDigest());
        }

        Map<String, String> invalid = Maps.newHashMap();
        invalid.put(BackupStmt.PROP_MANIFEST_DIGEST, "yes");
        BackupStmt invalidStmt = createStmt(false, invalid);
        Assert.assertThrows(AnalysisException.class, () -> invalidStmt.analyze(analyzer));
    }

    @Test
    public void testLogicalDigestProperty() throws Exception {
        // default false
        BackupStmt stmt = createStmt(false);
        stmt.analyze(analyzer);
        Assert.assertFalse(stmt.isLogicalDigest());

        for (String value : new String[] {"true", "TRUE", "false"}) {
            Map<String, String> properties = Maps.newHashMap();
            properties.put(BackupStmt.PROP_LOGICAL_DIGEST, value);
            BackupStmt digestStmt = createStmt(false, properties);
            digestStmt.analyze(analyzer);
            Assert.assertEquals(Boolean.parseBoolean(value), digestStmt.isLogicalDigest());
            // independent of manifest_digest
            Assert.assertFalse(digestStmt.isManifestDigest());
        }

        Map<String, String> invalid = Maps.newHashMap();
        invalid.put(BackupStmt.PROP_LOGICAL_DIGEST, "yes");
        BackupStmt invalidStmt = createStmt(false, invalid);
        Assert.assertThrows(AnalysisException.class, () -> invalidStmt.analyze(analyzer));
    }
}
