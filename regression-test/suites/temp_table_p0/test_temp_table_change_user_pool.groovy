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

import com.mysql.cj.jdbc.MysqlConnectionPoolDataSource

suite("test_temp_table_change_user_pool", "p0") {
    def database = getCurDbName()
    def borrower = "temp_pool_${UUID.randomUUID().toString().replace('-', '')}"
    def recoveryDb = "${borrower}_db"
    sql "CREATE USER ${borrower} IDENTIFIED BY 'temp_pool_password'"
    try {
        sql "GRANT SELECT_PRIV, CREATE_PRIV ON `${database}`.* TO ${borrower}"
        sql "CREATE DATABASE `${recoveryDb}`"
        sql "GRANT SELECT_PRIV, CREATE_PRIV ON `${recoveryDb}`.* TO ${borrower}"
        sql """CREATE TABLE `${recoveryDb}`.pool_shadow (permanent_id INT)
            DISTRIBUTED BY HASH(permanent_id) BUCKETS 1 PROPERTIES ("replication_num" = "1")"""
        def dbId = sql("SHOW PROC '/dbs'").find { it[1] == recoveryDb }[0]
        def tableIds = { sql("SHOW PROC '/dbs/${dbId}'").collect { it[0] } }
        def permanentIds = tableIds()
        assertEquals(1, permanentIds.size())
        def source = new MysqlConnectionPoolDataSource()
        source.setURL(getCurDbConnectUrl())
        def pool = source.getPooledConnection(borrower, "temp_pool_password")
        try {
            def physicalId
            pool.getConnection().withCloseable { first ->
                first.createStatement().withCloseable { stmt ->
                    stmt.executeQuery("SELECT CONNECTION_ID()").withCloseable { rows ->
                        assertTrue(rows.next())
                        physicalId = rows.getLong(1)
                    }
                    stmt.execute("""CREATE TEMPORARY TABLE `${recoveryDb}`.pool_shadow (previous_id INT)
                        DISTRIBUTED BY HASH(previous_id) BUCKETS 1 PROPERTIES ("replication_num" = "1")""")
                    stmt.execute("""CREATE TEMPORARY TABLE test_temp_table_change_user_pool (id INT)
                        DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES ("replication_num" = "1")""")
                }
            }
            assertEquals(2, tableIds().size())
            sql "DROP DATABASE `${recoveryDb}`"
            pool.getConnection().withCloseable { second ->
                sql "RECOVER DATABASE `${recoveryDb}`"
                assertEquals(permanentIds, tableIds())
                second.createStatement().withCloseable { stmt ->
                    stmt.executeQuery("SELECT CONNECTION_ID()").withCloseable { rows ->
                        assertTrue(rows.next())
                        assertEquals(physicalId, rows.getLong(1))
                    }
                    stmt.executeQuery("SHOW CREATE TABLE `${recoveryDb}`.pool_shadow").withCloseable { rows ->
                        assertTrue(rows.next())
                        assertTrue(rows.getString(2).contains("permanent_id"))
                    }
                    // Recreating the name without IF NOT EXISTS fails if the first borrower's table survived.
                    stmt.execute("""CREATE TEMPORARY TABLE test_temp_table_change_user_pool (id INT)
                        DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES ("replication_num" = "1")""")
                }
            }
        } finally {
            pool.close()
        }
    } finally {
        sql "DROP USER ${borrower}"
        sql "DROP DATABASE IF EXISTS `${recoveryDb}` FORCE"
    }
}
