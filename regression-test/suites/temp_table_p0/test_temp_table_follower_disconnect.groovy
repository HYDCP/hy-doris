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

// create temporary table on a follower fe, then close the session normally or let it
// time out, the temporary table should be dropped on master fe in both cases.
suite('test_temp_table_follower_disconnect', 'p0,docker') {
    def options = new ClusterOptions()
    options.setFeNum(2)
    options.setBeNum(1)
    options.connectToFollower = true
    docker(options) {
        sql "create database if not exists test_temp_table_follower_disconnect_db"
        sql "use test_temp_table_follower_disconnect_db"

        def assertTempTableDropped = { String tbl ->
            awaitUntil(60) {
                def showResult = sql_return_maparray("show data")
                showResult.find { it.TableName.contains(tbl) } == null
            }
            def showResult = sql_return_maparray("show data")
            assert showResult.find { it.TableName.contains(tbl) } == null
        }

        // case 1: session is closed normally, the temporary table should be dropped
        connectInDocker(context.config.jdbcUser, context.config.jdbcPassword) {
            sql "use test_temp_table_follower_disconnect_db"
            sql """create temporary table t_test_temp_table_follower_disconnect_1(id int)
                    properties("replication_num" = "1") """
            def showResult = sql_return_maparray("show data")
            assert showResult.find { it.TableName.contains("t_test_temp_table_follower_disconnect_1") } != null
        }
        assertTempTableDropped("t_test_temp_table_follower_disconnect_1")

        // case 2: session times out, the temporary table should be dropped
        connectInDocker(context.config.jdbcUser, context.config.jdbcPassword) {
            sql "use test_temp_table_follower_disconnect_db"
            sql """create temporary table t_test_temp_table_follower_disconnect_2(id int)
                    properties("replication_num" = "1") """
            def showResult = sql_return_maparray("show data")
            assert showResult.find { it.TableName.contains("t_test_temp_table_follower_disconnect_2") } != null

            // set session variables for a short connection timeout
            sql "set interactive_timeout=5"
            sql "set wait_timeout=5"
            sleep(10 * 1000)
        }
        assertTempTableDropped("t_test_temp_table_follower_disconnect_2")
    }
}
