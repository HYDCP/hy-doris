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

import groovy.json.JsonSlurper

def getProfileList = { masterHTTPAddr ->
    def dst = 'http://' + masterHTTPAddr
    def conn = new URL(dst + "/rest/v1/query_profile").openConnection()
    conn.setRequestMethod("GET")
    def encoding = Base64.getEncoder().encodeToString(
            (context.config.feHttpUser + ":" + (context.config.feHttpPassword == null
                    ? "" : context.config.feHttpPassword)).getBytes("UTF-8"))
    conn.setRequestProperty("Authorization", "Basic ${encoding}")
    return conn.getInputStream().getText()
}

suite('profile_safe') {
    sql """set enable_profile = true;"""

    // Verify that SHOW QUERY PROFILE does not create profile entries for itself.
    for (int i = 0; i < 10; i++) {
        sql """show query profile;"""
    }
    def res1 = sql "show query profile;"
    def profileCounts = res1.size()
    for (int i = 0; i < profileCounts; i++) {
        def stmt = res1[i][-1]
        assert !stmt.contains("show query profile")
    }

    def allFrontends = sql """show frontends;"""
    logger.info("allFrontends: " + allFrontends)
    def frontendCounts = allFrontends.size()
    def masterIP = ""
    def masterHTTPPort = ""

    for (def i = 0; i < frontendCounts; i++) {
        def currentFrontend = allFrontends[i]
        def isMaster = currentFrontend[8]
        if (isMaster == "true") {
            masterIP = allFrontends[i][1]
            masterHTTPPort = allFrontends[i][3]
            break
        }
    }
    def masterAddress = masterIP + ":" + masterHTTPPort
    logger.info("masterIP:masterHTTPPort is:${masterAddress}")

    sql """drop table if exists profile_safe;"""
    sql """create table profile_safe (k1 INT, v1 VARCHAR)
            distributed by hash(k1) buckets 8 properties("replication_num"="1");"""
    // Verify that INSERT INTO ... VALUES does not create profile entries.
    sql """ INSERT INTO profile_safe VALUES (1, 'test_profile_safe'),(2, 'test_profile_safe');"""
    Thread.sleep(200)
    def wholeString = getProfileList(masterAddress)
    def profileListData = new JsonSlurper().parseText(wholeString).data.rows
    for (def profileList : profileListData) {
        def stmt = profileList["Sql Statement"].toString()
        if (stmt.contains("INSERT INTO profile_safe VALUES")) {
            logger.info("stmt is: ${stmt}")
        }
        assert !stmt.contains("INSERT INTO profile_safe VALUES")
    }

    // Verify that INSERT INTO ... SELECT still keeps its LOAD profile.
    sql """ INSERT INTO profile_safe SELECT * FROM profile_safe;"""
    boolean hasInsertSelectProfile = false
    for (int i = 0; i < 10; i++) {
        Thread.sleep(500)
        wholeString = getProfileList(masterAddress)
        profileListData = new JsonSlurper().parseText(wholeString).data.rows
        for (def profileList : profileListData) {
            def taskType = profileList["Task Type"].toString()
            def stmt = profileList["Sql Statement"].toString()
            if (taskType == "LOAD" && stmt.contains("INSERT INTO profile_safe SELECT * FROM profile_safe")) {
                hasInsertSelectProfile = true
                break
            }
        }
        if (hasInsertSelectProfile == true) {
            break
        }
    }
    assertTrue(hasInsertSelectProfile)
}
