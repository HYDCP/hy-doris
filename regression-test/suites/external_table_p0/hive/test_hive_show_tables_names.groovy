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

suite("test_hive_show_tables_names", "p0,external,hive,external_docker,external_docker_hive") {
    String enabled = context.config.otherConfigs.get("enableHiveTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable Hive test.")
        return
    }

    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String hmsPort = context.config.otherConfigs.get("hive2HmsPort")

    for (boolean useMetaCache : [true, false]) {
        String catalog = "hive_show_tables_names_${useMetaCache}"
        sql """drop catalog if exists ${catalog}"""
        sql """create catalog ${catalog} properties (
            'type' = 'hms',
            'hive.metastore.uris' = 'thrift://${externalEnvIp}:${hmsPort}',
            'use_meta_cache' = '${useMetaCache}'
        )"""
        sql """create database if not exists ${catalog}.show_tables_names"""
        sql """drop table if exists ${catalog}.show_tables_names.show_tables_a"""
        sql """drop table if exists ${catalog}.show_tables_names.show_tables_b"""
        sql """drop table if exists ${catalog}.show_tables_names.show_tables_denied"""
        sql """drop table if exists ${catalog}.show_tables_names.show_tables_added"""
        sql """create table ${catalog}.show_tables_names.show_tables_b (k1 int)"""
        sql """create table ${catalog}.show_tables_names.show_tables_a (k1 int)"""
        sql """create table ${catalog}.show_tables_names.show_tables_denied (k1 int)"""
        sql """refresh catalog ${catalog}"""

        // The first call follows a cold catalog refresh; the second uses the same name cache.
        for (int i = 0; i < 2; i++) {
            assertEquals([["show_tables_a"], ["show_tables_b"], ["show_tables_denied"]],
                    sql("show tables from ${catalog}.show_tables_names"))
            assertEquals([["show_tables_a"], ["show_tables_b"]],
                    sql("show tables from ${catalog}.show_tables_names like 'show_tables__'"))
            assertEquals([], sql("show tables from ${catalog}.show_tables_names like 'missing%'"))
        }

        def fullRows = sql """show full tables from ${catalog}.show_tables_names"""
        assertEquals(3, fullRows.size())
        assertTrue(fullRows.every { it.size() == 4 })
        assertEquals(["show_tables_a", "show_tables_b", "show_tables_denied"], fullRows.collect { it[0] })
        assertTrue(fullRows.every { it[1] == "BASE TABLE" && it[2] == "NONE" && it[3] == "NONE" })
        assertEquals([], sql("show views from ${catalog}.show_tables_names"))
        assertEquals([["show_tables_a"]], sql("""show tables from ${catalog}.show_tables_names
                where Tables_in_show_tables_names = 'show_tables_a'"""))

        sql """drop user if exists show_tables_names_user"""
        sql """create user show_tables_names_user identified by 'show_tables_names_password'"""
        sql """grant select_priv on internal.${context.config.defaultDb}.* to show_tables_names_user"""
        sql """grant select_priv on ${catalog}.show_tables_names.show_tables_a to show_tables_names_user"""
        connect("show_tables_names_user", "show_tables_names_password", context.config.jdbcUrl) {
            assertEquals([["show_tables_a"]], sql("show tables from ${catalog}.show_tables_names"))
            assertEquals([], sql("show tables from ${catalog}.show_tables_names like '%denied'"))
        }

        sql """drop table if exists ${catalog}.show_tables_names.show_tables_added"""
        sql """create table ${catalog}.show_tables_names.show_tables_added (k1 int)"""
        sql """refresh database ${catalog}.show_tables_names"""
        assertEquals([["show_tables_added"]],
                sql("show tables from ${catalog}.show_tables_names like '%added'"))
    }

}
