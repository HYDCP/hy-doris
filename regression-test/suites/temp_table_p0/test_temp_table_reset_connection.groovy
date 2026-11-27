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
