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

suite("test_temp_table_explicit_drop", "p0") {
    sql "DROP TABLE IF EXISTS test_temp_table_explicit_drop"
    sql """
        CREATE TABLE test_temp_table_explicit_drop (id INT)
        DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES ("replication_num" = "1")
    """
    sql "INSERT INTO test_temp_table_explicit_drop VALUES (1)"

    sql "DROP TEMPORARY TABLE IF EXISTS test_temp_table_explicit_drop"
    test {
        sql "DROP TEMPORARY TABLE test_temp_table_explicit_drop"
        exception "Unknown table"
    }
    assertEquals([[1]], sql("SELECT id FROM test_temp_table_explicit_drop ORDER BY id"))

    sql """
        CREATE TEMPORARY TABLE test_temp_table_explicit_drop PROPERTIES ("replication_num" = "1")
        AS SELECT 2 AS id
    """
    assertEquals([[2]], sql("SELECT id FROM test_temp_table_explicit_drop ORDER BY id"))
    sql "DROP TEMPORARY TABLE test_temp_table_explicit_drop"
    assertEquals([[1]], sql("SELECT id FROM test_temp_table_explicit_drop ORDER BY id"))
}
