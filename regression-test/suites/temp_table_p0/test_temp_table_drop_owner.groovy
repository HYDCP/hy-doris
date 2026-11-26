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

suite("test_temp_table_drop_owner", "p0,docker") {
    def options = new ClusterOptions()
    options.setFeNum(1)
    options.setBeNum(1)
    options.feConfigs.add("lower_case_table_names=1")
    docker(options) {
        sql "CREATE DATABASE IF NOT EXISTS temp_table_drop_owner_db"
        sql "USE temp_table_drop_owner_db"
        sql "CREATE USER IF NOT EXISTS temp_drop_non_owner IDENTIFIED BY 'drop_test_password'"
        sql "GRANT DROP_PRIV ON temp_table_drop_owner_db.* TO temp_drop_non_owner"
        sql """
            CREATE TEMPORARY TABLE test_temp_table_drop_owner (id INT)
            DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES ("replication_num" = "1")
        """
        sql "INSERT INTO test_temp_table_drop_owner VALUES (10)"
        def row = sql_return_maparray("SHOW DATA").find {
            it.TableName.contains("_#TEMP#_test_temp_table_drop_owner")
        }
        assertNotNull(row)
        String target = "temp_table_drop_owner_db.`" + row.TableName + "`"
        connectInDocker("temp_drop_non_owner", "drop_test_password") {
            test {
                sql "DROP TABLE " + target
                exception "Unknown table"
            }
        }
        assertEquals([[10]], sql("SELECT id FROM test_temp_table_drop_owner ORDER BY id"))
    }
}
