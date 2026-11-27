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

import com.mysql.cj.NativeSession
import com.mysql.cj.jdbc.JdbcConnection
import com.mysql.cj.protocol.a.NativeConstants
import com.mysql.cj.protocol.a.NativePacketPayload

import java.sql.DriverManager
import java.util.concurrent.TimeUnit

def resetConnection = { connection ->
    NativeSession session = (NativeSession) connection.unwrap(JdbcConnection.class).getSession()
    NativePacketPayload packet = new NativePacketPayload(1)
    packet.writeInteger(NativeConstants.IntegerDataType.INT1, 0x1f)
    session.sendCommand(packet, false, 0)
}

suite("test_temp_table_reset_connection", "p0") {
    def database = sql("SELECT DATABASE()")[0][0]
    def connectionId = sql("SELECT CONNECTION_ID()")[0][0]

    sql """
        CREATE TEMPORARY TABLE test_temp_table_reset_connection (id INT)
        DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES ("replication_num" = "1")
    """
    sql "INSERT INTO test_temp_table_reset_connection VALUES (9)"
    assertEquals([[9]], sql("SELECT id FROM test_temp_table_reset_connection ORDER BY id"))

    resetConnection(context.getConnection())

    assertEquals(connectionId, sql("SELECT CONNECTION_ID()")[0][0])
    test {
        sql "SELECT id FROM `${database}`.test_temp_table_reset_connection"
        exception "does not exist"
    }
    sql """
        CREATE TEMPORARY TABLE `${database}`.test_temp_table_reset_connection (id INT)
        DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES ("replication_num" = "1")
    """
}

suite("test_temp_table_duplicate_create_reset", "p0") {
    def database = sql("SELECT DATABASE()")[0][0]
    def connectionId = sql("SELECT CONNECTION_ID()")[0][0]
    def create = """CREATE TEMPORARY TABLE `${database}`.test_temp_table_duplicate_create_reset (id INT)
        DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES ("replication_num" = "1")"""
    sql create
    test {
        sql create
        exception "already exist"
    }
    2.times {
        resetConnection(context.getConnection())
        assertEquals(connectionId, sql("SELECT CONNECTION_ID()")[0][0])
        test {
            sql "SELECT id FROM `${database}`.test_temp_table_duplicate_create_reset ORDER BY id"
            exception "does not exist"
        }
    }
    sql create
}

suite("test_permanent_table_transaction_reset", "p0") {
    if (isCloudMode()) {
        return
    }
    def database = getCurDbName()
    sql "DROP TABLE IF EXISTS test_permanent_table_transaction_reset"
    sql """
        CREATE TABLE test_permanent_table_transaction_reset (id INT)
        DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES ("replication_num" = "1")
    """
    sql "INSERT INTO test_permanent_table_transaction_reset VALUES (17)"

    [false, true].each { timeout ->
        def label = "permanent_reset_${UUID.randomUUID().toString().replace('-', '')}"
        def conn = context.getConnection().unwrap(JdbcConnection.class)
        def connectionId = conn.getId()
        sql "SET insert_timeout = ${timeout ? 5 : 300}"
        sql "SET query_timeout = ${timeout ? 5 : 300}"
        sql "BEGIN WITH LABEL ${label}"
        try {
            sql "INSERT INTO `${database}`.test_permanent_table_transaction_reset SELECT 29"
            def expectedStatus = timeout ? "ABORTED" : "PREPARE"
            def deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(90)
            def status
            DriverManager.getConnection(getCurDbConnectUrl(), context.config.jdbcUser, context.config.jdbcPassword)
                    .withCloseable { observer ->
                        observer.createStatement().withCloseable { stmt ->
                            while (System.nanoTime() < deadline) {
                                stmt.executeQuery("SHOW TRANSACTION FROM `${database}` WHERE LABEL = '${label}'")
                                        .withCloseable { rows ->
                                            status = rows.next() ? rows.getString("TransactionStatus") : null
                                        }
                                if (status == expectedStatus) {
                                    break
                                }
                                sleep(500)
                            }
                        }
                    }
            assertEquals(expectedStatus, status)
            2.times {
                resetConnection(context.getConnection())
                assertEquals(connectionId, conn.getId())
            }
        } finally {
            if (timeout) {
                test {
                    sql "ROLLBACK"
                    exception "transaction not found"
                }
            } else {
                sql "ROLLBACK"
            }
        }
        assertEquals(connectionId, ((Number) sql("SELECT CONNECTION_ID()")[0][0]).longValue())
        assertEquals([[17]], sql("SELECT id FROM `${database}`.test_permanent_table_transaction_reset ORDER BY id"))
    }
}

suite("test_temporary_table_reset_after_aborted_transaction", "p0") {
    if (isCloudMode()) {
        return
    }
    def database = getCurDbName()
    sql "DROP TABLE IF EXISTS temp_reset_aborted_txn_permanent"
    sql """
        CREATE TABLE temp_reset_aborted_txn_permanent (id INT)
        DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES ("replication_num" = "1")
    """
    sql "INSERT INTO temp_reset_aborted_txn_permanent VALUES (17)"
    sql """
        CREATE TEMPORARY TABLE temp_reset_aborted_txn_table (id INT)
        DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES ("replication_num" = "1")
    """
    sql "INSERT INTO temp_reset_aborted_txn_table VALUES (29)"
    def connectionId = sql("SELECT CONNECTION_ID()")[0][0]
    def label = "temporary_reset_${UUID.randomUUID().toString().replace('-', '')}"
    sql "SET insert_timeout = 5"
    sql "SET query_timeout = 5"
    sql "BEGIN WITH LABEL ${label}"
    sql "INSERT INTO temp_reset_aborted_txn_permanent SELECT 41"

    def status
    def deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(90)
    DriverManager.getConnection(getCurDbConnectUrl(), context.config.jdbcUser, context.config.jdbcPassword)
            .withCloseable { observer ->
                observer.createStatement().withCloseable { stmt ->
                    while (System.nanoTime() < deadline) {
                        stmt.executeQuery("SHOW TRANSACTION WHERE LABEL = '${label}'").withCloseable { rows ->
                            status = rows.next() ? rows.getString("TransactionStatus") : null
                        }
                        if (status == "ABORTED") {
                            break
                        }
                        sleep(500)
                    }
                }
            }
    assertEquals("ABORTED", status)
    2.times {
        resetConnection(context.getConnection())
        assertEquals(connectionId, sql("SELECT CONNECTION_ID()")[0][0])
        test {
            sql "SELECT id FROM `${database}`.temp_reset_aborted_txn_table"
            exception "does not exist"
        }
        assertEquals([[17]], sql("SELECT id FROM `${database}`.temp_reset_aborted_txn_permanent ORDER BY id"))
    }
}
