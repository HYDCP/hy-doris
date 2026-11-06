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

import org.apache.doris.analysis.EmptyStmt;
import org.apache.doris.analysis.Queriable;
import org.apache.doris.analysis.QueryStmt;
import org.apache.doris.analysis.SqlParser;
import org.apache.doris.analysis.SqlScanner;
import org.apache.doris.analysis.StatementBase;
import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.common.Config;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.util.SqlParserUtils;
import org.apache.doris.nereids.glue.LogicalPlanAdapter;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.plans.commands.Command;
import org.apache.doris.plugin.AuditEvent;
import org.apache.doris.plugin.AuditEvent.AuditEventBuilder;
import org.apache.doris.qe.QueryState.ErrType;
import org.apache.doris.qe.QueryState.MysqlStateType;

import org.apache.commons.codec.digest.DigestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.StringReader;

public class AuditLogHelperTest {
    private static final String QUERY_DIGEST = "SELECT * FROM audit_test WHERE k = ?";

    private ConnectContext ctx;
    private ConnectContext previousContext;
    private long previousThreshold;
    private boolean previousRunningUnitTest;

    @BeforeEach
    public void setUp() {
        previousContext = ConnectContext.get();
        previousThreshold = Config.sql_digest_generation_threshold_ms;
        previousRunningUnitTest = FeConstants.runningUnitTest;
        FeConstants.runningUnitTest = true;
        Config.sql_digest_generation_threshold_ms = 5000;
        ctx = new ConnectContext();
        ctx.setCurrentUserIdentity(UserIdentity.ROOT);
        ctx.setThreadLocalInfo();
        ctx.getState().setIsQuery(true);
    }

    @AfterEach
    public void tearDown() {
        Config.sql_digest_generation_threshold_ms = previousThreshold;
        FeConstants.runningUnitTest = previousRunningUnitTest;
        if (previousContext == null) {
            ConnectContext.remove();
        } else {
            previousContext.setThreadLocalInfo();
        }
    }

    @Test
    public void testFailedQueriesIgnoreThreshold() {
        StatementBase stmt = parseStatement("select * from audit_test where k = 1");
        ctx.getState().setError(ErrorCode.ERR_UNKNOWN_ERROR, "query execution failed");

        for (long elapseMs : new long[] {0, 1, 5000, 5001}) {
            Assertions.assertEquals(DigestUtils.md5Hex(QUERY_DIGEST),
                    AuditLogHelper.getSqlDigest(event(elapseMs), stmt));
        }
        Config.sql_digest_generation_threshold_ms = Long.MAX_VALUE;
        Assertions.assertEquals(DigestUtils.md5Hex(QUERY_DIGEST), AuditLogHelper.getSqlDigest(event(0), stmt));
        Assertions.assertEquals(MysqlStateType.ERR, ctx.getState().getStateType());
        Assertions.assertEquals("query execution failed", ctx.getState().getErrorMessage());
    }

    @Test
    public void testParsedQueryWithAnalysisErrorHasDigest() {
        // The parser can provide a digest before a missing table is rejected during analysis.
        StatementBase stmt = parseStatement("select * from missing_table where k = 1");
        ctx.getState().setError(ErrorCode.ERR_UNKNOWN_ERROR, "table does not exist");
        ctx.getState().setErrType(ErrType.ANALYSIS_ERR);

        Assertions.assertEquals(DigestUtils.md5Hex("SELECT * FROM missing_table WHERE k = ?"),
                AuditLogHelper.getSqlDigest(event(0), stmt));
        Assertions.assertEquals(ErrType.ANALYSIS_ERR, ctx.getState().getErrType());
        Assertions.assertEquals("table does not exist", ctx.getState().getErrorMessage());
    }

    @Test
    public void testForwardedFailureUsesFinalAuditState() {
        StatementBase stmt = parseStatement("select * from audit_test where k = 1");
        Assertions.assertEquals(MysqlStateType.OK, ctx.getState().getStateType());
        // A forwarded response can replace the final audit state while the local context remains OK.
        AuditEvent forwardedEvent = new AuditEventBuilder()
                .setState(MysqlStateType.ERR.toString())
                .setIsQuery(ctx.getState().isQuery())
                .setQueryTime(0)
                .setErrorMessage("forwarded query failed")
                .build();

        Assertions.assertEquals(DigestUtils.md5Hex(QUERY_DIGEST), AuditLogHelper.getSqlDigest(forwardedEvent, stmt));
        Assertions.assertEquals(MysqlStateType.OK, ctx.getState().getStateType());
        Assertions.assertEquals("ERR", forwardedEvent.state);
        Assertions.assertEquals("forwarded query failed", forwardedEvent.errorMessage);
    }

    @Test
    public void testParsedQueryFailureBeforeQueryClassification() throws Exception {
        StatementBase stmt = parseStatement("select * from audit_test where k = 1");
        ctx.getState().setIsQuery(false);
        ctx.getState().setError("blocked by SQL rule before query classification");
        AuditEvent failedEvent = event(0);
        Assertions.assertFalse(failedEvent.isQuery);
        Assertions.assertEquals(DigestUtils.md5Hex(QUERY_DIGEST), AuditLogHelper.getSqlDigest(failedEvent, stmt));

        StatementBase legacyStmt = SqlParserUtils.getFirstStmt(
                new SqlParser(new SqlScanner(new StringReader("select 1"))));
        Assertions.assertTrue(legacyStmt instanceof QueryStmt);
        String legacyDigest = ((Queriable) legacyStmt).toDigest();
        Assertions.assertFalse(legacyDigest.isEmpty());
        Assertions.assertEquals(DigestUtils.md5Hex(legacyDigest), AuditLogHelper.getSqlDigest(failedEvent, legacyStmt));
    }

    @Test
    public void testFailedNonQueryCommandStaysEmpty() {
        StatementBase stmt = parseStatement("insert into audit_test values (1)");
        LogicalPlanAdapter adapter = (LogicalPlanAdapter) stmt;
        Assertions.assertTrue(adapter.getLogicalPlan() instanceof Command);
        Assertions.assertFalse(adapter.toDigest().isEmpty());
        ctx.getState().setIsQuery(false);
        ctx.getState().setError("insert failed");

        Assertions.assertEquals("", AuditLogHelper.getSqlDigest(event(0), stmt));
        Assertions.assertEquals("", AuditLogHelper.getSqlDigest(event(5001), stmt));
    }

    @Test
    public void testSuccessfulQueriesPreserveStrictThreshold() {
        StatementBase stmt = parseStatement("select * from audit_test where k = 1");
        for (MysqlStateType state : new MysqlStateType[] {MysqlStateType.OK, MysqlStateType.EOF}) {
            if (state == MysqlStateType.OK) {
                ctx.getState().setOk();
            } else {
                ctx.getState().setEof();
            }

            Config.sql_digest_generation_threshold_ms = 5000;
            Assertions.assertEquals("", AuditLogHelper.getSqlDigest(event(0), stmt));
            Assertions.assertEquals("", AuditLogHelper.getSqlDigest(event(5000), stmt));
            Assertions.assertEquals(DigestUtils.md5Hex(QUERY_DIGEST), AuditLogHelper.getSqlDigest(event(5001), stmt));

            Config.sql_digest_generation_threshold_ms = 0;
            Assertions.assertEquals("", AuditLogHelper.getSqlDigest(event(0), stmt));
            Assertions.assertEquals(DigestUtils.md5Hex(QUERY_DIGEST), AuditLogHelper.getSqlDigest(event(1), stmt));

            Config.sql_digest_generation_threshold_ms = -1;
            Assertions.assertEquals(DigestUtils.md5Hex(QUERY_DIGEST), AuditLogHelper.getSqlDigest(event(0), stmt));
        }
    }

    @Test
    public void testMissingStatementsAndDigestsStayEmpty() {
        ctx.getState().setError("query failed");
        Assertions.assertEquals("", AuditLogHelper.getSqlDigest(event(0), null));
        Assertions.assertEquals("", AuditLogHelper.getSqlDigest(event(0), new EmptyStmt()));
        // Proxy/unavailable contexts deliberately leave the adapter's cached digest empty.
        Assertions.assertEquals("", AuditLogHelper.getSqlDigest(event(0), new LogicalPlanAdapter(null, null)));
        StatementBase nullDigestStmt = new LogicalPlanAdapter(null, null) {
            @Override
            public String toDigest() {
                return null;
            }
        };
        Assertions.assertEquals("", AuditLogHelper.getSqlDigest(event(0), nullDigestStmt));
    }

    @Test
    public void testInternalQueriesAndUnclassifiedSuccessStayEmpty() {
        StatementBase stmt = parseStatement("select * from audit_test where k = 1");
        ctx.getState().setError("internal query failed");
        ctx.getState().setInternal(true);
        Assertions.assertEquals("", AuditLogHelper.getSqlDigest(event(5001), stmt));
        ctx.getState().setOk();
        Assertions.assertEquals("", AuditLogHelper.getSqlDigest(event(5001), stmt));

        ctx.getState().setInternal(false);
        ctx.getState().setIsQuery(false);
        ctx.getState().setOk();
        Assertions.assertEquals("", AuditLogHelper.getSqlDigest(event(5001), stmt));
    }

    @Test
    public void testNonFinalQueryStateStaysEmpty() {
        StatementBase stmt = parseStatement("select * from audit_test where k = 1");
        ctx.getState().setNoop();
        Assertions.assertEquals("", AuditLogHelper.getSqlDigest(event(5001), stmt));
    }

    @Test
    public void testDigestFailurePreservesQueryError() {
        StatementBase stmt = new LogicalPlanAdapter(null, null) {
            @Override
            public String toDigest() {
                throw new IllegalStateException("digest unavailable");
            }
        };
        ctx.getState().setError(ErrorCode.ERR_UNKNOWN_ERROR, "original analysis failure");
        ctx.getState().setErrType(ErrType.ANALYSIS_ERR);
        AuditEvent failedEvent = event(0);

        Assertions.assertEquals("",
                Assertions.assertDoesNotThrow(() -> AuditLogHelper.getSqlDigest(failedEvent, stmt)));
        Assertions.assertEquals("ERR", failedEvent.state);
        Assertions.assertEquals("original analysis failure", failedEvent.errorMessage);
        Assertions.assertEquals(MysqlStateType.ERR, ctx.getState().getStateType());
        Assertions.assertEquals(ErrorCode.ERR_UNKNOWN_ERROR, ctx.getState().getErrorCode());
        Assertions.assertEquals(ErrType.ANALYSIS_ERR, ctx.getState().getErrType());
        Assertions.assertEquals("original analysis failure", ctx.getState().getErrorMessage());
    }

    @Test
    public void testDifferentLiteralsGroupWithSuccessfulQuery() {
        StatementBase failedStmt = parseStatement("select * from audit_test where k = 1");
        StatementBase successfulStmt = parseStatement("select * from audit_test where k = 2");
        ctx.getState().setError("query failed");
        String failedDigest = AuditLogHelper.getSqlDigest(event(0), failedStmt);
        ctx.getState().setEof();
        String successfulDigest = AuditLogHelper.getSqlDigest(event(5001), successfulStmt);

        Assertions.assertEquals(DigestUtils.md5Hex(QUERY_DIGEST), failedDigest);
        Assertions.assertEquals(failedDigest, successfulDigest);
    }

    @Test
    public void testReusedConnectionDoesNotReusePreviousDigest() {
        StatementBase firstStmt = parseStatement("select * from audit_test where k = 1");
        ctx.getState().setError("first query failed");
        Assertions.assertEquals(DigestUtils.md5Hex(QUERY_DIGEST), AuditLogHelper.getSqlDigest(event(0), firstStmt));

        ctx.getState().reset();
        ctx.getState().setIsQuery(true);
        StatementBase secondStmt = parseStatement("select * from another_table where k = 2");
        Assertions.assertEquals("", AuditLogHelper.getSqlDigest(event(0), secondStmt));
        ctx.getState().setError("second query failed");
        Assertions.assertEquals(DigestUtils.md5Hex("SELECT * FROM another_table WHERE k = ?"),
                AuditLogHelper.getSqlDigest(event(0), secondStmt));

        ctx.getState().reset();
        ctx.getState().setIsQuery(true);
        ctx.getState().setError("parsing failed");
        Assertions.assertEquals("", AuditLogHelper.getSqlDigest(event(0), null));
    }

    private AuditEvent event(long elapseMs) {
        return new AuditEventBuilder()
                .setEventType(AuditEvent.EventType.AFTER_QUERY)
                .setState(ctx.getState().toString())
                .setIsQuery(ctx.getState().isQuery())
                .setisInternal(ctx.getState().isInternal())
                .setQueryTime(elapseMs)
                .setErrorCode(ctx.getState().getErrorCode() == null ? 0 : ctx.getState().getErrorCode().getCode())
                .setErrorMessage(ctx.getState().getErrorMessage())
                .build();
    }

    private StatementBase parseStatement(String sql) {
        return new NereidsParser().parseSQL(sql).get(0);
    }
}
