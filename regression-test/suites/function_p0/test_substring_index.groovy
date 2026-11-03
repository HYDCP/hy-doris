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

suite("test_substring_index") {
    sql "set enable_nereids_planner=true"
    sql "set enable_fallback_to_original_planner=false"

    sql "DROP TABLE IF EXISTS test_substring_index"
    sql """
    CREATE TABLE test_substring_index (
        id INT,
        str VARCHAR(100),
        delimiter VARCHAR(10),
        count INT
    ) ENGINE=OLAP
    DUPLICATE KEY(id)
    DISTRIBUTED BY HASH(id) BUCKETS 1
    PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
    )
    """

    sql """
    INSERT INTO test_substring_index VALUES
        (1, 'AAA_01|BBB_02|CCC_03|DDD_04|EEE_05|FFF_06', 'BBB', -1),
        (2, 'zyz_01|zyz_02|CCC_03|qwe_04|qwe_05|qwe_06', 'ccc', -1),
        (3, 'AAA_01|BBB_02|CCC_03|DDD_04|EEE_05|FFF_06', 'DDD', -1),
        (4, 'sgr_01|wsc_02|CCC_03|DDD_04|rfv_05|rgb_06', 'DDD', -1),
        (5, 'cdr_01|vfr_02|dfc_03|DDD_04|EEE_05|FFF_06', 'eee', -1),
        (6, 'AAA_01|dsd_02|ert_03|bgt_04|fgh_05|hyb_06', 'A_01', -1),
        (7, 'AAA_01|BBB_02|CCC_03|DDD_04|EEE_05|FFF_06', 'BBB', 1),
        (8, 'AAA_01|BBB_02|CCC_03|DDD_04|EEE_05|FFF_06', '|', 2),
        (9, 'AAA_01|BBB_02|CCC_03|DDD_04|EEE_05|FFF_06', '|', -2),
        (10, 'ABC', '|', 1),
        (11, 'ABC|DEF', '|', 0),
        (12, '', 'ABC', 1),
        (13, 'ABC|DEF|GHI', '', 1)
    """

    sql """
    INSERT INTO test_substring_index VALUES
        (101, '北京市|上海市|广州市|深圳市|成都市', '|', 2),
        (102, '北京市|上海市|广州市|深圳市|成都市', '|', -2),
        (103, '北京市|上海市|广州市|深圳市|成都市', '上海', -1),
        (104, '中国人民共和国', '人民', 1),
        (105, '中国人民共和国', '人民', -1),
        (106, '你好，世界！你好，朋友！', '你好', 1),
        (107, '你好，世界！你好，朋友！', '你好', -1),
        (108, '你好，世界！你好，朋友！', '世界', -1),
        (109, '中文|测试|数据', '测试', 1),
        (110, '中文|测试|数据', '测试', -1)
    """

    sql """
    INSERT INTO test_substring_index VALUES
        (201, 'hello😀world😀example', '😀', 1),
        (202, 'hello😀world😀example', '😀', 2),
        (203, 'hello😀world😀example', '😀', -1),
        (204, '👋👋hello👋world👋', '👋', 2),
        (205, '👋👋hello👋world👋', '👋', -2)
    """

    qt_sql """
    SELECT
        id,
        str,
        delimiter,
        count,
        substring_index(str, delimiter, count) as result
    FROM test_substring_index
    WHERE id BETWEEN 1 AND 13
    ORDER BY id
    """

    qt_sql """
    SELECT
        id,
        str,
        delimiter,
        count,
        substring_index(str, delimiter, count) as result
    FROM test_substring_index
    WHERE id BETWEEN 101 AND 110
    ORDER BY id
    """

    qt_sql """
    SELECT
        id,
        str,
        delimiter,
        count,
        substring_index(str, delimiter, count) as result
    FROM test_substring_index
    WHERE id BETWEEN 201 AND 205
    ORDER BY id
    """

    qt_sql """
    SELECT
        a.id,
        a.str,
        a.delimiter,
        b.count,
        substring_index(a.str, a.delimiter, b.count) as result
    FROM test_substring_index a
    JOIN test_substring_index b ON a.id = b.id
    WHERE a.id IN (1, 3, 7, 8, 101, 103, 201, 203)
    ORDER BY a.id
    """

    qt_sql """
    SELECT
        substring_index('', '', 1) as empty_all,
        substring_index('test', '', 1) as empty_delimiter,
        substring_index('', 'test', 1) as empty_string,
        substring_index('test', 'test', 0) as zero_count,
        substring_index('test|test', '|', 999) as large_count,
        substring_index('test|test', '|', -999) as large_negative_count
    """

    sql "DROP TABLE IF EXISTS test_substring_index"

    sql "DROP TABLE IF EXISTS test_substring_index2"
    sql """
    CREATE TABLE test_substring_index2 (
        id INT,
        str VARCHAR(100),
        delimiter VARCHAR(10)
    ) ENGINE=OLAP
    DUPLICATE KEY(id)
    DISTRIBUTED BY HASH(id) BUCKETS 1
    PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
    )
    """

    sql """
    INSERT INTO test_substring_index2 VALUES
        (1, NULL, NULL),
        (2, NULL, NULL)
    """
    qt_sql """
    SELECT SUBSTRING_INDEX(IFNULL(a.str, ''), ',', a.id)
    FROM test_substring_index2 a
    ORDER BY a.id
    """
}
