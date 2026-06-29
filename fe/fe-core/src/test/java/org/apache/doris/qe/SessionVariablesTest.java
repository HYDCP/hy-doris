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

import org.apache.doris.analysis.SetStmt;
import org.apache.doris.analysis.ShowVariablesStmt;
import org.apache.doris.common.CaseSensibility;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.ExceptionChecker;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.PatternMatcher;
import org.apache.doris.common.PatternMatcherWrapper;
import org.apache.doris.common.VariableAnnotation;
import org.apache.doris.thrift.TQueryOptions;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;

public class SessionVariablesTest extends TestWithFeService {

    private SessionVariable sessionVariable;
    private int numOfForwardVars;

    @Override
    protected void runBeforeAll() throws Exception {
        FeConstants.runningUnitTest = true;
        createDatabase("test_d");
        useDatabase("test_d");
        createTable("create table test_t1 \n" + "(k1 int, k2 int) distributed by hash(k1) buckets 1\n"
                + "properties(\"replication_num\" = \"1\");");

        sessionVariable = new SessionVariable();
        Field[] fields = SessionVariable.class.getDeclaredFields();
        for (Field f : fields) {
            VariableMgr.VarAttr varAttr = f.getAnnotation(VariableMgr.VarAttr.class);
            if (varAttr == null || !varAttr.needForward()) {
                continue;
            }
            numOfForwardVars++;
        }
    }

    @Test
    public void testExperimentalSessionVariables() throws Exception {
        connectContext.setThreadLocalInfo();
        // 1. set without experimental
        SessionVariable sessionVar = connectContext.getSessionVariable();
        boolean enableShareScan = sessionVar.getEnableSharedScan();
        String sql = "set enable_shared_scan=" + (enableShareScan ? "false" : "true");
        SetStmt setStmt = (SetStmt) parseAndAnalyzeStmt(sql, connectContext);
        SetExecutor setExecutor = new SetExecutor(connectContext, setStmt);
        setExecutor.execute();
        Assertions.assertNotEquals(sessionVar.getEnableSharedScan(), enableShareScan);
        // 2. set with experimental
        enableShareScan = sessionVar.getEnableSharedScan();
        sql = "set experimental_enable_shared_scan=" + (enableShareScan ? "false" : "true");
        setStmt = (SetStmt) parseAndAnalyzeStmt(sql, connectContext);
        setExecutor = new SetExecutor(connectContext, setStmt);
        setExecutor.execute();
        Assertions.assertNotEquals(sessionVar.getEnableSharedScan(), enableShareScan);
        // 3. set global without experimental
        enableShareScan = sessionVar.getEnableSharedScan();
        sql = "set global enable_shared_scan=" + (enableShareScan ? "false" : "true");
        setStmt = (SetStmt) parseAndAnalyzeStmt(sql, connectContext);
        setExecutor = new SetExecutor(connectContext, setStmt);
        setExecutor.execute();
        Assertions.assertNotEquals(sessionVar.getEnableSharedScan(), enableShareScan);
        // 4. set global with experimental
        enableShareScan = sessionVar.getEnableSharedScan();
        sql = "set global experimental_enable_shared_scan=" + (enableShareScan ? "false" : "true");
        setStmt = (SetStmt) parseAndAnalyzeStmt(sql, connectContext);
        setExecutor = new SetExecutor(connectContext, setStmt);
        setExecutor.execute();
        Assertions.assertNotEquals(sessionVar.getEnableSharedScan(), enableShareScan);

        // 5. set experimental for EXPERIMENTAL_ONLINE var
        boolean bucketShuffle = sessionVar.isEnableBucketShuffleJoin();
        sql = "set global experimental_enable_bucket_shuffle_join=" + (bucketShuffle ? "false" : "true");
        setStmt = (SetStmt) parseAndAnalyzeStmt(sql, connectContext);
        setExecutor = new SetExecutor(connectContext, setStmt);
        setExecutor.execute();
        Assertions.assertNotEquals(sessionVar.isEnableBucketShuffleJoin(), bucketShuffle);

        // 6. set non experimental for EXPERIMENTAL_ONLINE var
        bucketShuffle = sessionVar.isEnableBucketShuffleJoin();
        sql = "set global enable_bucket_shuffle_join=" + (bucketShuffle ? "false" : "true");
        setStmt = (SetStmt) parseAndAnalyzeStmt(sql, connectContext);
        setExecutor = new SetExecutor(connectContext, setStmt);
        setExecutor.execute();
        Assertions.assertNotEquals(sessionVar.isEnableBucketShuffleJoin(), bucketShuffle);

        // 4. set experimental for none experimental var
        sql = "set experimental_group_concat_max_len=5";
        setStmt = (SetStmt) parseAndAnalyzeStmt(sql, connectContext);
        SetExecutor setExecutor2 = new SetExecutor(connectContext, setStmt);
        ExceptionChecker.expectThrowsWithMsg(DdlException.class, "Unknown system variable",
                setExecutor2::execute);

        // 5. show variables
        String showSql = "show variables like '%experimental%'";
        ShowVariablesStmt showStmt = (ShowVariablesStmt) parseAndAnalyzeStmt(showSql, connectContext);
        PatternMatcher matcher = null;
        if (showStmt.getPattern() != null) {
            matcher = PatternMatcherWrapper.createMysqlPattern(showStmt.getPattern(),
                    CaseSensibility.VARIABLES.getCaseSensibility());
        }
        int num = sessionVar.getVariableNumByVariableAnnotation(VariableAnnotation.EXPERIMENTAL);
        List<List<String>> result = VariableMgr.dump(showStmt.getType(), sessionVar, matcher);
        Assertions.assertEquals(num, result.size());
    }

    @Test
    public void testForwardSessionVariables() {
        Map<String, String> vars = sessionVariable.getForwardVariables();
        Assertions.assertTrue(numOfForwardVars >= 6);
        Assertions.assertEquals(numOfForwardVars, vars.size());

        vars.put(SessionVariable.ENABLE_PROFILE, "true");
        vars.put(SessionVariable.ENABLE_PRELOAD_EXTERNAL_METADATA, "true");
        vars.put(SessionVariable.INSERT_VISIBLE_TIMEOUT_RETURN_MODE, "ERROR");
        sessionVariable.setForwardedSessionVariables(vars);
        Assertions.assertTrue(sessionVariable.enableProfile);
        Assertions.assertEquals(SessionVariable.INSERT_VISIBLE_TIMEOUT_RETURN_MODE_ERROR,
                sessionVariable.getInsertVisibleTimeoutReturnMode());
        Assertions.assertEquals(SessionVariable.InsertVisibleTimeoutReturnMode.ERROR,
                sessionVariable.getInsertVisibleTimeoutReturnModeEnum());
        // Forwarded string enums should be normalized because the forward path bypasses session setters.
        vars.put(SessionVariable.EXTERNAL_TABLE_DML_RETURN_STATUS, "COMMITTED");
        sessionVariable.setForwardedSessionVariables(vars);
        Assertions.assertTrue(sessionVariable.enableProfile);
        Assertions.assertTrue(sessionVariable.isEnablePreloadExternalMetadata());
        Assertions.assertEquals(SessionVariable.EXTERNAL_TABLE_DML_RETURN_STATUS_COMMITTED,
                sessionVariable.getExternalTableDmlReturnStatus());
    }

    @Test
    public void testExternalTableDmlReturnStatus() throws Exception {
        connectContext.setThreadLocalInfo();
        SessionVariable sessionVar = connectContext.getSessionVariable();

        // Explicit session settings should keep a normalized value that can be forwarded to master FE.
        String sql = "set external_table_dml_return_status='COMMITTED'";
        SetStmt setStmt = (SetStmt) parseAndAnalyzeStmt(sql, connectContext);
        SetExecutor setExecutor = new SetExecutor(connectContext, setStmt);
        setExecutor.execute();
        Assertions.assertEquals(SessionVariable.EXTERNAL_TABLE_DML_RETURN_STATUS_COMMITTED,
                sessionVar.getExternalTableDmlReturnStatus());
        Assertions.assertEquals(SessionVariable.EXTERNAL_TABLE_DML_RETURN_STATUS_COMMITTED,
                sessionVar.getForwardVariables().get(SessionVariable.EXTERNAL_TABLE_DML_RETURN_STATUS));

        sql = "set external_table_dml_return_status='invalid'";
        setStmt = (SetStmt) parseAndAnalyzeStmt(sql, connectContext);
        SetExecutor invalidExecutor = new SetExecutor(connectContext, setStmt);
        ExceptionChecker.expectThrowsWithMsg(DdlException.class, "externalTableDmlReturnStatus value is invalid",
                invalidExecutor::execute);
    }

    @Test
    public void testForwardQueryOptions() {
        TQueryOptions queryOptions = sessionVariable.getQueryOptionVariables();
        Assertions.assertTrue(queryOptions.isSetMemLimit());
        Assertions.assertFalse(queryOptions.isSetLoadMemLimit());
        Assertions.assertTrue(queryOptions.isSetQueryTimeout());

        queryOptions.setQueryTimeout(123);
        queryOptions.setInsertTimeout(123);
        sessionVariable.setForwardedSessionVariables(queryOptions);
        Assertions.assertEquals(123, sessionVariable.getQueryTimeoutS());
        Assertions.assertEquals(123, sessionVariable.getInsertTimeoutS());
    }

    @Test
    public void testCloneSessionVariablesWithSessionOriginValueNotEmpty() throws NoSuchFieldException {
        Field txIsolation = SessionVariable.class.getField("txIsolation");
        SessionVariableField txIsolationSessionVariableField = new SessionVariableField(txIsolation);
        sessionVariable.addSessionOriginValue(txIsolationSessionVariableField, "test");

        SessionVariable sessionVariableClone = VariableMgr.cloneSessionVariable(sessionVariable);

        Assertions.assertEquals("test",
                sessionVariableClone.getSessionOriginValue().get(txIsolationSessionVariableField));
    }

    @Test
    public void testInsertVisibleTimeoutReturnMode() throws Exception {
        connectContext.setThreadLocalInfo();
        SessionVariable sessionVar = connectContext.getSessionVariable();

        String sql = "set insert_visible_timeout_return_mode='ERROR'";
        SetStmt setStmt = (SetStmt) parseAndAnalyzeStmt(sql, connectContext);
        SetExecutor setExecutor = new SetExecutor(connectContext, setStmt);
        setExecutor.execute();
        Assertions.assertEquals(SessionVariable.INSERT_VISIBLE_TIMEOUT_RETURN_MODE_ERROR,
                sessionVar.getInsertVisibleTimeoutReturnMode());
        Assertions.assertEquals(SessionVariable.InsertVisibleTimeoutReturnMode.ERROR,
                sessionVar.getInsertVisibleTimeoutReturnModeEnum());
        Assertions.assertEquals(SessionVariable.INSERT_VISIBLE_TIMEOUT_RETURN_MODE_ERROR,
                sessionVar.getForwardVariables().get(SessionVariable.INSERT_VISIBLE_TIMEOUT_RETURN_MODE));

        SessionVariable restored = new SessionVariable();
        restored.readFromJson("{\"insert_visible_timeout_return_mode\":\"ERROR\"}");
        Assertions.assertEquals(SessionVariable.INSERT_VISIBLE_TIMEOUT_RETURN_MODE_ERROR,
                restored.getInsertVisibleTimeoutReturnMode());
        Assertions.assertEquals(SessionVariable.InsertVisibleTimeoutReturnMode.ERROR,
                restored.getInsertVisibleTimeoutReturnModeEnum());

        Field field = SessionVariable.class.getDeclaredField("insertVisibleTimeoutReturnMode");
        VariableMgr.VarAttr varAttr = field.getAnnotation(VariableMgr.VarAttr.class);
        Assertions.assertArrayEquals(new String[] {
                "控制普通内表 INSERT 在 publish timeout 时返回给客户端的状态。",
                "Controls the status returned to the client when a normal internal-table INSERT times out "
                        + "while waiting for publish visibility."
        }, varAttr.description());
        Assertions.assertArrayEquals(new String[] {
                SessionVariable.INSERT_VISIBLE_TIMEOUT_RETURN_MODE_COMMITTED,
                SessionVariable.INSERT_VISIBLE_TIMEOUT_RETURN_MODE_ERROR
        }, varAttr.options());

        sql = "set insert_visible_timeout_return_mode='unexpected'";
        setStmt = (SetStmt) parseAndAnalyzeStmt(sql, connectContext);
        SetExecutor invalidSetExecutor = new SetExecutor(connectContext, setStmt);
        ExceptionChecker.expectThrowsWithMsg(DdlException.class,
                "insertVisibleTimeoutReturnMode value is invalid", invalidSetExecutor::execute);
    }

    @Test
    public void testInsertVisibleTimeoutReturnModeDefaultsAndCheckerBranches() {
        // Cover the default branch and the helper methods used by setter/checker paths.
        SessionVariable sessionVar = new SessionVariable();
        Assertions.assertEquals(SessionVariable.INSERT_VISIBLE_TIMEOUT_RETURN_MODE_COMMITTED,
                sessionVar.getInsertVisibleTimeoutReturnMode());
        Assertions.assertEquals(SessionVariable.InsertVisibleTimeoutReturnMode.COMMITTED,
                sessionVar.getInsertVisibleTimeoutReturnModeEnum());
        Assertions.assertFalse(sessionVar.isInsertVisibleTimeoutReturnError());

        // Verify setter parsing is case-insensitive and stores the canonical lowercase value.
        sessionVar.setInsertVisibleTimeoutReturnMode("ErRoR");
        Assertions.assertEquals(SessionVariable.INSERT_VISIBLE_TIMEOUT_RETURN_MODE_ERROR,
                sessionVar.getInsertVisibleTimeoutReturnMode());
        Assertions.assertEquals(SessionVariable.InsertVisibleTimeoutReturnMode.ERROR,
                sessionVar.getInsertVisibleTimeoutReturnModeEnum());
        Assertions.assertTrue(sessionVar.isInsertVisibleTimeoutReturnError());

        ExceptionChecker.expectThrowsWithMsg(UnsupportedOperationException.class,
                "insertVisibleTimeoutReturnMode value is empty",
                () -> sessionVar.checkInsertVisibleTimeoutReturnMode(""));
    }
}
