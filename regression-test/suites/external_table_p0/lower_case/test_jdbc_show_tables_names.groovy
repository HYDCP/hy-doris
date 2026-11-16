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

suite("test_jdbc_show_tables_names", "p0,external,doris,meta_names_mapping,external_docker") {
    String jdbcUrl = context.config.jdbcUrl
    String driverUrl = context.config.otherConfigs.get("showTablesJdbcDriverUrl")
    if (driverUrl == null) {
        driverUrl = "https://${getS3BucketName()}.${getS3Endpoint()}/regression/jdbc_driver/mysql-connector-j-8.4.0.jar"
    }

    sql """create database if not exists internal.show_tables_names_jdbc"""
    sql """drop table if exists internal.show_tables_names_jdbc.source_a"""
    sql """drop table if exists internal.show_tables_names_jdbc.source_b"""
    sql """create table internal.show_tables_names_jdbc.source_a (k1 int)
        distributed by hash(k1) buckets 1 properties ('replication_num' = '1')"""
    sql """create table internal.show_tables_names_jdbc.source_b (k1 int)
        distributed by hash(k1) buckets 1 properties ('replication_num' = '1')"""
    sql """drop user if exists show_tables_names_jdbc_source"""
    sql """create user show_tables_names_jdbc_source identified by 'show_tables_names_password'"""
    sql """grant all on internal.show_tables_names_jdbc.* to show_tables_names_jdbc_source"""
    sql """grant select_priv on internal.${context.config.defaultDb}.* to show_tables_names_jdbc_source"""
    if (isCloudMode()) {
        def clusters = sql "show clusters"
        assertTrue(!clusters.isEmpty())
        sql """grant usage_priv on cluster `${clusters[0][0]}` to show_tables_names_jdbc_source"""
    }

    for (boolean useMetaCache : [true, false]) {
        String catalog = "jdbc_show_tables_names_${useMetaCache}"
        sql """drop catalog if exists ${catalog}"""
        sql """create catalog ${catalog} properties (
            'type' = 'jdbc',
            'user' = 'show_tables_names_jdbc_source',
            'password' = 'show_tables_names_password',
            'jdbc_url' = '${jdbcUrl}',
            'driver_url' = '${driverUrl}',
            'driver_class' = 'com.mysql.cj.jdbc.Driver',
            'only_specified_database' = 'true',
            'include_database_list' = 'show_tables_names_jdbc',
            'lower_case_meta_names' = 'true',
            'use_meta_cache' = '${useMetaCache}',
            'meta_names_mapping' = '{"tables":[{"remoteDatabase":"show_tables_names_jdbc",
                "remoteTable":"source_a","mapping":"mapped_a"}]}'
        )"""
        for (int i = 0; i < 2; i++) {
            assertEquals([["mapped_a"], ["source_b"]],
                    sql("show tables from ${catalog}.show_tables_names_jdbc"))
            assertEquals([["mapped_a"]],
                    sql("show tables from ${catalog}.show_tables_names_jdbc like 'mapped_%'"))
        }
        def fullRows = sql "show full tables from ${catalog}.show_tables_names_jdbc"
        assertEquals(["mapped_a", "source_b"], fullRows.collect { it[0] })
        assertTrue(fullRows.every { it.size() == 4 })

        sql """drop user if exists show_tables_names_jdbc_viewer"""
        sql """create user show_tables_names_jdbc_viewer identified by 'show_tables_names_password'"""
        sql """grant select_priv on internal.${context.config.defaultDb}.* to show_tables_names_jdbc_viewer"""
        sql """grant select_priv on ${catalog}.show_tables_names_jdbc.mapped_a to show_tables_names_jdbc_viewer"""
        connect("show_tables_names_jdbc_viewer", "show_tables_names_password", context.config.jdbcUrl) {
            assertEquals([["mapped_a"]], sql("show tables from ${catalog}.show_tables_names_jdbc"))
            assertEquals([], sql("show tables from ${catalog}.show_tables_names_jdbc like 'source_%'"))
        }
    }

    // The mapping collides with the unmapped source_a name when names are listed.
    sql """drop catalog if exists jdbc_show_tables_names_conflict"""
    sql """create catalog jdbc_show_tables_names_conflict properties (
        'type' = 'jdbc',
        'user' = 'show_tables_names_jdbc_source',
        'password' = 'show_tables_names_password',
        'jdbc_url' = '${jdbcUrl}',
        'driver_url' = '${driverUrl}',
        'driver_class' = 'com.mysql.cj.jdbc.Driver',
        'only_specified_database' = 'true',
        'include_database_list' = 'show_tables_names_jdbc',
        'lower_case_meta_names' = 'true',
        'use_meta_cache' = 'true',
        'meta_names_mapping' = '{"tables":[{"remoteDatabase":"show_tables_names_jdbc",
            "remoteTable":"source_b","mapping":"source_a"}]}'
    )"""
    test {
        sql """show tables from jdbc_show_tables_names_conflict.show_tables_names_jdbc"""
        exception "Found conflicting"
    }
}
