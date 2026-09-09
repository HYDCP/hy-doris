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

suite("test_sql_digest_generation", "nonConcurrent") {
    def originalAuditPlugin = sql "show global variables like 'enable_audit_plugin'"
    def marker = "digest_" + UUID.randomUUID().toString().replace("-", "")
    // Match this run's exact statement, rather than counts affected by other queries.
    def waitForDigest = { String statement ->
        def rows = []
        long deadline = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(60)
        while (System.nanoTime() < deadline) {
            sql "call flush_audit_log()"
            rows = sql """SELECT sql_digest FROM __internal_schema.audit_log
                          WHERE stmt = '${statement}' ORDER BY time DESC LIMIT 1"""
            if (!rows.isEmpty()) {
                return rows[0][0]
            }
            Thread.sleep(1000)
        }
        assertTrue(false, "Audit record not found for: " + statement)
    }
    try {
        sql "set global enable_audit_plugin = true"
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
            sql statement
            assertEquals("", waitForDigest(statement))
        }
    } finally {
        sql "set global enable_audit_plugin = ${originalAuditPlugin[0][1]}"
    }
}
