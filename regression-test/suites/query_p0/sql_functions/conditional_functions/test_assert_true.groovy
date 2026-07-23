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

suite("test_assert_true") {
    // Prepare a stable table for validating non-literal message binding.
    sql """ DROP TABLE IF EXISTS test_assert_true """
    sql """
        CREATE TABLE test_assert_true (
            id INT
        )
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1"
        )
    """
    sql """ INSERT INTO test_assert_true VALUES (1) """

    // Validate public SQL behavior through the original planner path.
    sql """ SET enable_nereids_planner=false """
    sql """ SET enable_fallback_to_original_planner=true """

    test {
        sql """ SELECT assert_true(TRUE, 'assert_true should be available') """
        result([[true]])
    }

    test {
        sql """ SELECT assert_true(FALSE, 'assert_true false message') """
        exception "assert_true false message"
    }

    test {
        sql """ SELECT assert_true(NULL, 'assert_true null message') """
        exception "assert_true null message"
    }

    test {
        sql """ SELECT assert_true(TRUE, CAST(id AS STRING)) FROM test_assert_true """
        exception "assert_true only accept constant for 2nd argument"
    }

    // Validate the same public SQL behavior through the Nereids path.
    sql """ SET enable_nereids_planner=true """
    sql """ SET enable_fallback_to_original_planner=false """

    test {
        sql """ SELECT assert_true(TRUE, 'assert_true should be available') """
        result([[true]])
    }

    test {
        sql """ SELECT assert_true(FALSE, 'assert_true false message') """
        exception "assert_true false message"
    }

    test {
        sql """ SELECT assert_true(NULL, 'assert_true null message') """
        exception "assert_true null message"
    }

    test {
        sql """ SELECT assert_true(TRUE, CAST(id AS STRING)) FROM test_assert_true """
        exception "assert_true only accept constant for 2nd argument"
    }
}
