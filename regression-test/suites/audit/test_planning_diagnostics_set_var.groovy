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

import groovy.json.JsonSlurper
import org.apache.doris.regression.util.Http

suite("test_planning_diagnostics_set_var", "nonConcurrent") {
    def originalTimeout = sql("show variables like 'query_timeout'")[0][1]
    def originalMaxExecutionTime = sql("show variables like 'max_execution_time'")[0][1]
    def checkFailedPlanningTimeout = { String hint, String expectedError, long expectedTimeout ->
        test {
            sql """select /*+ SET_VAR(${hint}) */ missing_planning_diagnostics_column
                   from planning_diagnostics_set_var_test"""
            exception expectedError
        }
        def queryId = sql("select last_query_id()")[0][0]
        def event = null
        for (int attempt = 0; attempt < 20; attempt++) {
            // Use the existing bounded warning-log view and keep unrelated log content out of test output.
            def response = Http.GET("http://${context.config.feHttpAddress}/rest/v1/log", true, false)
            assertEquals(0, response.code)
            def line = response.data.LogContents.log.split("</br>").find {
                it.contains("Planning finished: ") && it.contains('"query_id":"' + queryId + '"')
            }
            if (line != null) {
                event = new JsonSlurper().parseText(line.substring(line.indexOf("Planning finished: ")
                        + "Planning finished: ".length()))
                break
            }
            sleep(500)
        }
        assertTrue(event != null, "Missing planning diagnostic for query ${queryId}")
        assertEquals("failed", event.status)
        assertEquals(expectedTimeout, event.query_timeout_ms as long)
        assertEquals(20, sql("select @@query_timeout")[0][0] as int)
    }
    try {
        sql "set max_execution_time = 20000"
        sql "set query_timeout = 20"
        sql "drop table if exists planning_diagnostics_set_var_test"
        sql """create table planning_diagnostics_set_var_test (id int)
               distributed by hash(id) buckets 1 properties ("replication_num"="1")"""
        sql "insert into planning_diagnostics_set_var_test values (1), (2)"
        def hintedResult = sql("""select /*+ SET_VAR(query_timeout=2) */ @@query_timeout, sum(id)
                                 from planning_diagnostics_set_var_test""")
        assertEquals(2, hintedResult[0][0] as int)
        assertEquals(3L, hintedResult[0][1] as long)
        assertEquals(20, sql("select @@query_timeout")[0][0] as int)
        checkFailedPlanningTimeout("query_timeout=2", "Unknown column", 2000L)
        checkFailedPlanningTimeout("query_timeout=2, nonexistent_planning_diagnostics_variable=1",
                "Can not set session variable", 2000L)
        // Other timeout setters must also refresh the effective execution budget.
        checkFailedPlanningTimeout("max_execution_time=3000", "Unknown column", 3000L)
        checkFailedPlanningTimeout("query_timeout='invalid'", "Can not set session variable", 20000L)
        assertEquals(3L, sql("select sum(id) from planning_diagnostics_set_var_test")[0][0] as long)
    } finally {
        sql "set max_execution_time = ${originalMaxExecutionTime}"
        sql "set query_timeout = ${originalTimeout}"
    }
}
