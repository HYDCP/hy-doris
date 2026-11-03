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

// The minimal signed number divided by -1 overflows; it must return NULL instead of raising SIGFPE.
suite("test_int_div_min_value") {
    sql "DROP TABLE IF EXISTS test_int_div_min_value"
    sql """
    CREATE TABLE test_int_div_min_value (
        id INT,
        i32 INT,
        i64 BIGINT,
        i128 LARGEINT,
        d BIGINT
    ) ENGINE=OLAP
    DUPLICATE KEY(id)
    DISTRIBUTED BY HASH(id) BUCKETS 1
    PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
    )
    """
    sql """
    INSERT INTO test_int_div_min_value VALUES
        (1, -2147483648, -9223372036854775808, -170141183460469231731687303715884105728, -1),
        (2, -2147483648, -9223372036854775808, -170141183460469231731687303715884105728, 1),
        (3, 7, 7, 7, -1),
        (4, 7, 7, 7, 0),
        (5, -2147483647, -9223372036854775807, -170141183460469231731687303715884105727, -1)
    """

    // vector DIV vector
    order_qt_vector_vector """
    SELECT id, CAST(i32 AS INT) DIV CAST(d AS INT), i64 DIV d, i128 DIV CAST(d AS LARGEINT)
    FROM test_int_div_min_value
    """
    // vector DIV constant -1
    order_qt_vector_const """
    SELECT id, i32 DIV -1, i64 DIV -1, i128 DIV CAST(-1 AS LARGEINT) FROM test_int_div_min_value
    """
    // constant DIV vector
    order_qt_const_vector """
    SELECT id, CAST(-9223372036854775808 AS BIGINT) DIV d FROM test_int_div_min_value
    """
}
