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

suite("test_temp_table_ctas_identity", "p0,docker") {
    def options = new ClusterOptions()
    options.enableDebugPoints()
    options.setFeNum(1)
    options.setBeNum(1)
    options.feConfigs.add("lower_case_table_names=2")
    docker(options) {
        sql "CREATE DATABASE IF NOT EXISTS temp_table_ctas_identity_db"
        sql "USE temp_table_ctas_identity_db"
        sql "DROP TABLE IF EXISTS TEMP_TABLE_CTAS_OK"
        sql "DROP TABLE IF EXISTS TEMP_TABLE_CTAS_FAIL"
        sql """
            CREATE TEMPORARY TABLE Temp_Table_Ctas_Ok (id INT)
            DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES ("replication_num" = "1")
        """
        sql "INSERT INTO Temp_Table_Ctas_Ok VALUES (10)"
        sql """
            CREATE TABLE TEMP_TABLE_CTAS_OK PROPERTIES ("replication_num" = "1")
            AS SELECT 20 AS id
        """
        assertEquals([[10]], sql("SELECT id FROM TEMP_TABLE_CTAS_OK ORDER BY id"))
        connectInDocker(context.config.jdbcUser, context.config.jdbcPassword) {
            assertEquals([[20]], sql("SELECT id FROM temp_table_ctas_identity_db.TEMP_TABLE_CTAS_OK ORDER BY id"))
        }

        sql """
            CREATE TEMPORARY TABLE Temp_Table_Ctas_Fail (id INT)
            DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES ("replication_num" = "1")
        """
        sql "INSERT INTO Temp_Table_Ctas_Fail VALUES (30)"
        GetDebugPoint().enableDebugPointForAllFEs("CreateTableCommand.beforeCtasInsert")
        try {
            test {
                sql """
                    CREATE TABLE TEMP_TABLE_CTAS_FAIL PROPERTIES ("replication_num" = "1")
                    AS SELECT 40 AS id
                """
                exception "injected CTAS insert failure"
            }
        } finally {
            GetDebugPoint().disableDebugPointForAllFEs("CreateTableCommand.beforeCtasInsert")
        }
        assertEquals([[30]], sql("SELECT id FROM TEMP_TABLE_CTAS_FAIL ORDER BY id"))
        connectInDocker(context.config.jdbcUser, context.config.jdbcPassword) {
            test {
                sql "SELECT id FROM temp_table_ctas_identity_db.TEMP_TABLE_CTAS_FAIL"
                exception "does not exist"
            }
        }
    }
}
