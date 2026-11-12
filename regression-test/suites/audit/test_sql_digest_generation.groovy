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

import java.nio.charset.StandardCharsets
import java.security.MessageDigest

suite("test_sql_digest_generation", "nonConcurrent") {
    def originalAuditPlugin = sql "show global variables like 'enable_audit_plugin'"
    def marker = "digest_" + UUID.randomUUID().toString().replace("-", "")
    // Analysis errors replace stmt with the error message, so match this run's unique SQL hash.
    def waitForAudit = { String statement ->
        def sqlHash = MessageDigest.getInstance("MD5")
                .digest(statement.getBytes(StandardCharsets.UTF_8)).encodeHex().toString()
        def rows = []
        long deadline = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(60)
        while (System.nanoTime() < deadline) {
            sql "call flush_audit_log()"
            rows = sql """SELECT sql_digest, state, error_code, query_time
                          FROM __internal_schema.audit_log
                          WHERE sql_hash = '${sqlHash}' ORDER BY time DESC LIMIT 1"""
            if (!rows.isEmpty()) {
                return rows[0]
            }
            Thread.sleep(1000)
        }
        assertTrue(false, "Audit record not found for: " + statement)
    }
    def waitForDigest = { String statement -> waitForAudit(statement)[0] }
    try {
        sql "set global enable_audit_plugin = true"
        sql "DROP TABLE IF EXISTS test_sql_digest_generation"
        sql """CREATE TABLE test_sql_digest_generation (id INT)
               DISTRIBUTED BY HASH(id) BUCKETS 1
               PROPERTIES ("replication_num" = "1")"""
        sql "INSERT INTO test_sql_digest_generation VALUES (1)"
        // The production comparison is strictly greater than the threshold, including 0 ms queries.
        setFeConfigTemporary([sql_digest_generation_threshold_ms:-1]) {
            def sameShapeSql1 = "select 1 as ${marker}"
            def sameShapeSql2 = "select 2 as ${marker}"
            def diffShapeSql = "select 1 + 1 as ${marker}"
            sql sameShapeSql1
            sql sameShapeSql2
            sql diffShapeSql
            def digest1 = waitForDigest(sameShapeSql1)
            def digest2 = waitForDigest(sameShapeSql2)
            def differentDigest = waitForDigest(diffShapeSql)
            [digest1, digest2, differentDigest].each {
                assertTrue(it != null && !it.toString().isEmpty(), "Expected a nonempty SQL digest")
            }
            assertEquals(digest1, digest2)
            assertTrue(digest1 != differentDigest, "Different shapes must have different digests")
        }
        setFeConfigTemporary([sql_digest_generation_threshold_ms:100000]) {
            def statement = "select 3 as ${marker}_threshold"
            // Keep aliases identical so only literals differ between these failed query shapes.
            def analysisSql1 = "select missing_column + 1 as ${marker}_analysis " +
                    "from test_sql_digest_generation"
            def analysisSql2 = "select missing_column + 2 as ${marker}_analysis " +
                    "from test_sql_digest_generation"
            // A table column prevents constant folding and reaches the BE assert_true implementation.
            def runtimeSql = "select assert_true(id = 0, '${marker}_runtime_error') " +
                    "as ${marker}_runtime from test_sql_digest_generation"
            def syntaxSql = "select 1 as ${marker}_syntax from"

            // Execute consecutively on the same JDBC connection before audit polling can reset it.
            test {
                sql analysisSql1
                exception "Unknown column 'missing_column'"
            }
            jdbc_sql statement
            test {
                sql analysisSql2
                exception "Unknown column 'missing_column'"
            }
            test {
                sql runtimeSql
                exception "${marker}_runtime_error"
            }
            test {
                sql syntaxSql
                exception "(line 1, pos "
            }

            def analysisAudit1 = waitForAudit(analysisSql1)
            def analysisAudit2 = waitForAudit(analysisSql2)
            def runtimeAudit = waitForAudit(runtimeSql)
            def syntaxAudit = waitForAudit(syntaxSql)
            [analysisAudit1, analysisAudit2, runtimeAudit, syntaxAudit].each {
                assertEquals("ERR", it[1])
                assertTrue((it[2] as int) != 0, "Expected the original query error to remain in audit")
                assertTrue((it[3] as long) <= 100000, "Failure should be below the success digest threshold")
            }
            [analysisAudit1[0], analysisAudit2[0], runtimeAudit[0]].each {
                assertTrue(it != null && !it.toString().isEmpty(), "Expected a nonempty failed-query digest")
            }
            assertEquals(analysisAudit1[0], analysisAudit2[0])
            assertTrue(analysisAudit2[0] != runtimeAudit[0], "Different failed shapes must have different digests")
            assertEquals("", waitForDigest(statement))
            assertEquals("", syntaxAudit[0])
        }
    } finally {
        sql "set global enable_audit_plugin = ${originalAuditPlugin[0][1]}"
    }
}
