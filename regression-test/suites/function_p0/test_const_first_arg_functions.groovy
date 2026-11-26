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

// The first argument of these functions is a constant while another argument varies by row,
// so BE receives a one-row ColumnConst for it and must not index it with the row number.
suite("test_const_first_arg_functions") {
    sql "DROP TABLE IF EXISTS test_const_first_arg_functions"
    sql """
    CREATE TABLE test_const_first_arg_functions (
        id INT,
        path VARCHAR(32),
        len INT
    ) ENGINE=OLAP
    DUPLICATE KEY(id)
    DISTRIBUTED BY HASH(id) BUCKETS 1
    PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
    )
    """
    sql """
    INSERT INTO test_const_first_arg_functions VALUES
        (1, '\$.a', 256),
        (2, '\$.b', 256),
        (3, '\$', 256),
        (4, '\$.a', 256),
        (5, '\$.c', 256)
    """

    order_qt_json_length """
    SELECT id, json_length('{"a":[1,2,3],"b":{"x":1}}', path) FROM test_const_first_arg_functions
    """
    order_qt_jsonb_extract_string """
    SELECT id, jsonb_extract_string('{"a":"x","b":{"y":2}}', path) FROM test_const_first_arg_functions
    """
    order_qt_json_keys """
    SELECT id, json_keys('{"a":{"k1":1,"k2":2},"b":{"k3":3}}', path) FROM test_const_first_arg_functions
    """

    order_qt_format_round_double """
    SELECT id, format_round(CAST(1234.5678 AS DOUBLE), id % 3) FROM test_const_first_arg_functions
    """
    order_qt_format_round_bigint """
    SELECT id, format_round(CAST(1234567 AS BIGINT), id % 3) FROM test_const_first_arg_functions
    """
    order_qt_format_round_largeint """
    SELECT id, format_round(CAST(1234567 AS LARGEINT), id % 3) FROM test_const_first_arg_functions
    """
    order_qt_format_round_decimal """
    SELECT id, format_round(CAST(1234.5678 AS DECIMAL(10, 4)), id % 3) FROM test_const_first_arg_functions
    """

    order_qt_sha2_const_length """
    SELECT id, sha2('abc', 256) FROM test_const_first_arg_functions
    """
    order_qt_sha2_column_length """
    SELECT id, sha2('abc', len) FROM test_const_first_arg_functions
    """
}
