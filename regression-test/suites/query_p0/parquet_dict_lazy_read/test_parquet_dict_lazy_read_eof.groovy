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

// Covers the parquet lazy-read EOF path with dictionary-encoded predicates:
// when a predicate on a dictionary-encoded string column filters out all
// remaining rows, the reader returns an empty batch directly at EOF and the
// dictionary-coded predicate column must be converted back to plain string
// columns before the block is reused, otherwise stale dictionary codes leak
// into later batches and downstream operators.
import org.codehaus.groovy.runtime.IOGroovyMethods

suite("test_parquet_dict_lazy_read_eof", "p0") {
    // check whether the FE config 'enable_outfile_to_local' is true
    StringBuilder strBuilder = new StringBuilder()
    strBuilder.append("curl --location-trusted -u " + context.config.jdbcUser + ":" + context.config.jdbcPassword)
    strBuilder.append(" http://" + context.config.feHttpAddress + "/rest/v1/config/fe")
    def process = strBuilder.toString().execute()
    def code = process.waitFor()
    def out = process.getText()
    logger.info("Request FE Config: code=" + code + ", out=" + out)
    assertEquals(code, 0)
    def response = parseJson(out.trim())
    assertEquals(response.code, 0)
    assertEquals(response.msg, "success")
    boolean enableOutfileToLocal = false
    for (Object conf: response.data.rows) {
        assert conf instanceof Map
        if (((Map<String, String>) conf).get("Name").toLowerCase() == "enable_outfile_to_local") {
            enableOutfileToLocal = ((Map<String, String>) conf).get("Value").toLowerCase() == "true"
        }
    }
    if (!enableOutfileToLocal) {
        logger.warn("Please set enable_outfile_to_local to true to run test_parquet_dict_lazy_read_eof")
        return
    }

    def tableName = "parquet_dict_lazy_read_src"
    // The local() tvf resolves file paths against the backend's DORIS_HOME,
    // so write the parquet file under <repo>/output/be/tmp which is the
    // backend home in the regression environment.
    def dataPath = context.config.dataPath
    def repoRoot = new File(dataPath).getParentFile().getParentFile().getAbsolutePath()
    def beHome = "${repoRoot}/output/be"
    def beTmpDir = new File("${beHome}/tmp")
    if (!beTmpDir.exists()) {
        beTmpDir.mkdirs()
    }
    def uuid = UUID.randomUUID().toString()
    def outfilePrefix = "parquet_dict_lazy_read_eof_${uuid}"
    def outFilePath = "${beHome}/tmp/${outfilePrefix}"

    try {
        sql """ DROP TABLE IF EXISTS ${tableName} """
        sql """
            CREATE TABLE ${tableName} (
                k INT,
                s VARCHAR(16),
                pad VARCHAR(64)
            ) DUPLICATE KEY(k)
            DISTRIBUTED BY HASH(k) BUCKETS 1
            PROPERTIES ("replication_num" = "1");
        """
        // Low-cardinality string column so that the parquet writer picks
        // dictionary encoding for it.
        sql """
            INSERT INTO ${tableName}
            SELECT cast(number % 3 as int),
                   concat('str_', cast(number % 3 as string)),
                   concat('pad_', cast(number as string))
            FROM numbers("number" = "5000");
        """
        def srcTotal = sql "select count(*) from ${tableName}"
        def srcStr1 = sql "select count(*) from ${tableName} where s = 'str_1'"
        def srcStr2 = sql "select count(*) from ${tableName} where s = 'str_2'"
        assertEquals(5000, srcTotal[0][0])
        assertEquals(1667, srcStr1[0][0])

        sql """
            SELECT * FROM ${tableName} INTO OUTFILE "file://${outFilePath}"
            FORMAT AS PARQUET;
        """

        def ipList = [:]
        def portList = [:]
        getBackendIpHeartbeatPort(ipList, portList)
        ipList.each { beid, ip ->
            def tvf = """
                select * from local(
                "file_path" = "tmp/${outfilePrefix}*",
                "backend_id" = "${beid}",
                "format" = "parquet")
            """

            // Predicate on the dictionary-encoded column matches nothing:
            // the reader hits the lazy-read EOF path with all rows filtered.
            def emptyResult = sql "select count(*) from (${tvf}) t where s = 'not_exist'"
            assertEquals(0, emptyResult[0][0])

            // Predicate matches a subset while other columns are lazily read.
            def str1Count = sql "select count(*) from (${tvf}) t where s = 'str_1'"
            assertEquals(srcStr1[0][0], str1Count[0][0])

            // Predicate column is also projected, mixed with lazy columns.
            def str2Count = sql "select count(*) from (${tvf}) t where s = 'str_2'"
            assertEquals(srcStr2[0][0], str2Count[0][0])

            // Value check after dictionary decoding, with a filter that
            // passes every row.
            def sample = sql "select s, count(*) from (${tvf}) t group by s order by s"
            assertEquals(3, sample.size())
            assertEquals("str_0", sample[0][0])
            assertEquals("str_1", sample[1][0])
            assertEquals("str_2", sample[2][0])
        }
    } finally {
        beTmpDir.listFiles().findAll { it.name.startsWith(outfilePrefix) }.each { it.delete() }
        sql """ DROP TABLE IF EXISTS ${tableName} """
    }
}
