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

import org.apache.doris.regression.suite.ClusterOptions

suite("test_permanent_table_ctas", "p0,docker") {
    def options = new ClusterOptions()
    options.enableDebugPoints()
    options.setFeNum(1)
    options.setBeNum(1)
    docker(options) {
        sql "CREATE DATABASE IF NOT EXISTS permanent_table_ctas_db"
        sql "USE permanent_table_ctas_db"
        sql "DROP TABLE IF EXISTS permanent_ctas_success"
        sql "DROP TABLE IF EXISTS permanent_ctas_failed"

        sql """
            CREATE TABLE permanent_ctas_success PROPERTIES ("replication_num" = "1")
            AS SELECT 20 AS id
        """
        assertEquals([[20]], sql("SELECT id FROM permanent_ctas_success ORDER BY id"))

        sql """
            CREATE TABLE IF NOT EXISTS permanent_ctas_success PROPERTIES ("replication_num" = "1")
            AS SELECT 40 AS id
        """
        assertEquals([[20]], sql("SELECT id FROM permanent_ctas_success ORDER BY id"))

        GetDebugPoint().enableDebugPointForAllFEs("CreateTableCommand.beforeCtasInsert")
        try {
            test {
                sql """
                    CREATE TABLE permanent_ctas_failed PROPERTIES ("replication_num" = "1")
                    AS SELECT 60 AS id
                """
                exception "injected CTAS insert failure"
            }
        } finally {
            GetDebugPoint().disableDebugPointForAllFEs("CreateTableCommand.beforeCtasInsert")
        }
        test {
            sql "SELECT id FROM permanent_ctas_failed"
            exception "does not exist"
        }
        assertEquals([[20]], sql("SELECT id FROM permanent_ctas_success ORDER BY id"))
    }
}
