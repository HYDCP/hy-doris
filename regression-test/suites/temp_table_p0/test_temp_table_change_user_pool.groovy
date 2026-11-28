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
    sql "CREATE USER ${borrower} IDENTIFIED BY 'temp_pool_password'"
    try {
        sql "GRANT SELECT_PRIV, CREATE_PRIV ON `${database}`.* TO ${borrower}"
        sql "DROP TABLE IF EXISTS test_temp_table_change_user_pool"
        sql """CREATE TABLE test_temp_table_change_user_pool (permanent_id INT)
            DISTRIBUTED BY HASH(permanent_id) BUCKETS 1 PROPERTIES ("replication_num" = "1")"""
        sql "INSERT INTO test_temp_table_change_user_pool VALUES (17)"
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
                    stmt.execute("""CREATE TEMPORARY TABLE test_temp_table_change_user_pool (id INT)
                        DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES ("replication_num" = "1")""")
                }
            }
            2.times {
                pool.getConnection().withCloseable { nextBorrow ->
                    nextBorrow.createStatement().withCloseable { stmt ->
                        stmt.executeQuery("SELECT CONNECTION_ID()").withCloseable { rows ->
                            assertTrue(rows.next())
                            assertEquals(physicalId, rows.getLong(1))
                        }
                        stmt.executeQuery("SELECT permanent_id FROM test_temp_table_change_user_pool ORDER BY permanent_id")
                                .withCloseable { rows ->
                                    assertTrue(rows.next())
                                    assertEquals(17, rows.getInt(1))
                                    assertFalse(rows.next())
                                }
                    }
                }
            }
            pool.getConnection().withCloseable { third ->
                third.createStatement().withCloseable { stmt ->
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
    }
}
