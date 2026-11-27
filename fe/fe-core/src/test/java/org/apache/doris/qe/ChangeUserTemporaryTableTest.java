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

package org.apache.doris.qe;

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.AuthenticationException;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.mysql.MysqlHandshakePacket;
import org.apache.doris.mysql.privilege.Auth;
import org.apache.doris.qe.QueryState.MysqlStateType;
import org.apache.doris.transaction.TransactionEntry;
import org.apache.doris.transaction.TransactionNotFoundException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;

class ChangeUserTemporaryTableTest {
    private void changeUser(ConnectContext ctx, String username) {
        ByteBuffer payload = ByteBuffer.allocate(128);
        payload.put(username.getBytes(StandardCharsets.UTF_8)).put((byte) 0).put((byte) 0);
        payload.put((byte) 0).putShort((short) 33);
        payload.put(MysqlHandshakePacket.AUTH_PLUGIN_NAME.getBytes(StandardCharsets.UTF_8)).put((byte) 0).flip();
        MysqlConnectProcessor processor = new MysqlConnectProcessor(ctx);
        Deencapsulation.setField(processor, "packetBuf", payload);
        Deencapsulation.invoke(processor, "handleChangeUser");
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void ordinaryConnectionsResetTwiceWithoutTouchingTransaction(boolean timedOut) throws Exception {
        Env env = Mockito.mock(Env.class, Mockito.RETURNS_DEEP_STUBS);
        UserIdentity user = UserIdentity.createAnalyzedUserIdentWithIp("pool_user", "%");
        try (MockedStatic<Env> mocked = Mockito.mockStatic(Env.class)) {
            mocked.when(Env::getCurrentEnv).thenReturn(env);
            ConnectContext ctx = Mockito.spy(new ConnectContext());
            ctx.setEnv(env);
            ctx.setCurrentUserIdentity(user);
            ctx.setQualifiedUser(user.getQualifiedUser());
            ctx.setRemoteIP("localhost");
            Mockito.doReturn(new byte[20]).when(ctx).getAuthPluginData();
            Auth auth = env.getAuth();
            Mockito.doAnswer(call -> {
                call.<List<UserIdentity>>getArgument(4).add(user);
                return null;
            }).when(auth).checkPassword(Mockito.eq("pool_user"), Mockito.anyString(),
                    Mockito.any(byte[].class), Mockito.any(byte[].class), Mockito.anyList());

            TransactionEntry transaction = Mockito.mock(TransactionEntry.class);
            Mockito.when(transaction.isTxnModel()).thenReturn(true);
            if (timedOut) {
                TransactionNotFoundException error = new TransactionNotFoundException("transaction timed out", 11L);
                Mockito.when(transaction.abortTransaction()).thenThrow(error);
                Mockito.when(transaction.abortTransactionForTemporaryTableReset()).thenThrow(error);
            }
            ctx.setTxnEntry(transaction);
            String sessionId = ctx.getSessionId();
            MysqlConnectProcessor processor = new MysqlConnectProcessor(ctx);
            for (int reset = 0; reset < 2; reset++) {
                processor.handleResetConnection();
                Assertions.assertEquals(MysqlStateType.OK, ctx.getState().getStateType());
                Assertions.assertSame(transaction, ctx.getTxnEntry());
                Assertions.assertEquals(sessionId, ctx.getSessionId());
            }
            for (int reset = 0; reset < 2; reset++) {
                changeUser(ctx, "pool_user");
                Assertions.assertEquals(MysqlStateType.OK, ctx.getState().getStateType());
                Assertions.assertSame(transaction, ctx.getTxnEntry());
                Assertions.assertEquals(sessionId, ctx.getSessionId());
            }
            Mockito.verifyNoInteractions(transaction);
        }
    }

    @Test
    void authenticationAndCleanupMustSucceedBeforeChangingIdentity() throws Exception {
        Env env = Mockito.mock(Env.class, Mockito.RETURNS_DEEP_STUBS);
        UserIdentity oldUser = UserIdentity.createAnalyzedUserIdentWithIp("old_user", "%");
        UserIdentity newUser = UserIdentity.createAnalyzedUserIdentWithIp("new_user", "%");
        try (MockedStatic<Env> mocked = Mockito.mockStatic(Env.class)) {
            mocked.when(Env::getCurrentEnv).thenReturn(env);
            ConnectContext ctx = Mockito.spy(new ConnectContext());
            ctx.setEnv(env);
            ctx.setCurrentUserIdentity(oldUser);
            ctx.setQualifiedUser(oldUser.getQualifiedUser());
            ctx.setRemoteIP("localhost");
            Mockito.doReturn(new byte[20]).when(ctx).getAuthPluginData();
            Auth auth = env.getAuth();
            Mockito.doThrow(new AuthenticationException("bad credentials")).doAnswer(call -> {
                call.<List<UserIdentity>>getArgument(4).add(newUser);
                return null;
            }).when(auth).checkPassword(Mockito.eq("new_user"), Mockito.anyString(),
                    Mockito.any(byte[].class), Mockito.any(byte[].class), Mockito.anyList());

            changeUser(ctx, "new_user");
            Assertions.assertEquals(MysqlStateType.ERR, ctx.getState().getStateType());
            Assertions.assertSame(oldUser, ctx.getCurrentUserIdentity());
            Mockito.verify(ctx, Mockito.never()).resetTemporaryTables();

            Mockito.doThrow(new DdlException("cleanup failed")).doAnswer(call -> {
                Assertions.assertSame(oldUser, ctx.getCurrentUserIdentity());
                Assertions.assertEquals(oldUser.getQualifiedUser(), ctx.getQualifiedUser());
                return call.callRealMethod();
            }).when(ctx).resetTemporaryTables();
            changeUser(ctx, "new_user");
            Assertions.assertEquals(MysqlStateType.ERR, ctx.getState().getStateType());
            Assertions.assertSame(oldUser, ctx.getCurrentUserIdentity());
            changeUser(ctx, "new_user");
            Assertions.assertEquals(MysqlStateType.OK, ctx.getState().getStateType());
            Assertions.assertSame(newUser, ctx.getCurrentUserIdentity());
        }
    }
}
