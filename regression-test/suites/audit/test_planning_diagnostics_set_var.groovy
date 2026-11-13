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

suite("test_planning_diagnostics_set_var", "nonConcurrent") {
    // PlanningDiagnosticsSqlTest checks the actual query_timeout_ms logger payload through SQL execution.
    // This cluster regression must not depend on FE routing, log sinks or a bounded warning-log tail.
    def originalTimeout = sql("show variables like 'query_timeout'")[0][1]
    def originalMaxExecutionTime = sql("show variables like 'max_execution_time'")[0][1]
    def checkFailedPlanning = { String hint, String expectedError ->
        test {
            sql """select /*+ SET_VAR(${hint}) */ missing_planning_diagnostics_column
                   from planning_diagnostics_set_var_test"""
            exception expectedError
        }
        assertEquals(20, sql("select @@query_timeout")[0][0] as int)
    }
    try {
        sql "set max_execution_time = 20000"
        sql "set query_timeout = 20"
        sql "drop table if exists planning_diagnostics_set_var_test"
        sql """create table planning_diagnostics_set_var_test (id int)
               distributed by hash(id) buckets 1 properties ("replication_num"="1")"""
        sql "insert into planning_diagnostics_set_var_test values (1), (2)"
        order_qt_hint_timeout """select /*+ SET_VAR(query_timeout=2) */ @@query_timeout, sum(id)
                                from planning_diagnostics_set_var_test"""
        assertEquals(20, sql("select @@query_timeout")[0][0] as int)
        checkFailedPlanning("query_timeout=2", "Unknown column")
        checkFailedPlanning("query_timeout=2, nonexistent_planning_diagnostics_variable=1",
                "Can not set session variable")
        checkFailedPlanning("max_execution_time=3000", "Unknown column")
        checkFailedPlanning("query_timeout='invalid'", "Can not set session variable")
        order_qt_restored_timeout "select @@query_timeout, sum(id) from planning_diagnostics_set_var_test"
    } finally {
        sql "set max_execution_time = ${originalMaxExecutionTime}"
        sql "set query_timeout = ${originalTimeout}"
    }
}
