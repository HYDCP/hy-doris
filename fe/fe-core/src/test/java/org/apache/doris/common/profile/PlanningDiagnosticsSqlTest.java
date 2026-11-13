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

package org.apache.doris.common.profile;

import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.mysql.authenticate.TestLogAppender;
import org.apache.doris.qe.QueryState;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.utframe.TestWithFeService;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.LogEvent;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class PlanningDiagnosticsSqlTest extends TestWithFeService {
    private static Stream<Arguments> timeoutHints() {
        return Stream.of(
                Arguments.of("query_timeout=2", "Unknown column", 2000L),
                Arguments.of("query_timeout=2, nonexistent_planning_diagnostics_variable=1",
                        "Can not set session variable", 2000L),
                Arguments.of("max_execution_time=3000", "Unknown column", 3000L),
                Arguments.of("query_timeout='invalid'", "Can not set session variable", 20000L));
    }

    @ParameterizedTest
    @MethodSource("timeoutHints")
    public void testSqlTimeoutSnapshotAndRestoration(String hint, String expectedError, long expectedTimeout)
            throws Exception {
        connectContext.setThreadLocalInfo();
        SessionVariable variables = connectContext.getSessionVariable();
        int originalTimeout = variables.getQueryTimeoutS();
        int originalMaxExecutionTime = variables.getMaxExecutionTimeMS();
        try {
            executeSql("set max_execution_time=20000");
            executeSql("set query_timeout=20");
            // Capture the logger event itself: no FE routing, filesystem, console or log-tail assumptions.
            try (TestLogAppender appender = TestLogAppender.attach(PlanningDiagnostics.class, Level.WARN)) {
                connectContext.getState().reset();
                new StmtExecutor(connectContext, "select /*+ SET_VAR(" + hint
                        + ") */ missing_planning_diagnostics_column").execute();
                Assertions.assertEquals(QueryState.MysqlStateType.ERR, connectContext.getState().getStateType());
                Assertions.assertTrue(connectContext.getState().getErrorMessage().contains(expectedError));
                List<LogEvent> events = Deencapsulation.getField(appender, "events");
                List<LogEvent> finished = events.stream().filter(event -> event.getMessage().getFormattedMessage()
                        .startsWith("Planning finished: ")).collect(Collectors.toList());
                Assertions.assertEquals(1, finished.size());
                String message = finished.get(0).getMessage().getFormattedMessage();
                JsonObject diagnostic = JsonParser.parseString(message.substring("Planning finished: ".length()))
                        .getAsJsonObject();
                Assertions.assertEquals("failed", diagnostic.get("status").getAsString());
                Assertions.assertEquals("analyze", diagnostic.get("failed_step").getAsString());
                Assertions.assertEquals(expectedTimeout, diagnostic.get("query_timeout_ms").getAsLong());
                Assertions.assertEquals(20, connectContext.getExecTimeout());
                Assertions.assertNull(connectContext.getPlanningDiagnostics());
            }
        } finally {
            executeSql("set max_execution_time=" + originalMaxExecutionTime);
            executeSql("set query_timeout=" + originalTimeout);
        }
    }
}
