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

package org.apache.doris.service.arrowflight;

import org.apache.doris.common.Config;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.QueryLogContext;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.service.arrowflight.results.FlightSqlChannel;
import org.apache.doris.service.arrowflight.sessions.FlightSessionsManager;
import org.apache.doris.service.arrowflight.sessions.FlightSqlConnectContext;
import org.apache.doris.thrift.TUniqueId;

import com.google.protobuf.ByteString;
import org.apache.arrow.flight.FlightProducer.CallContext;
import org.apache.arrow.flight.FlightProducer.ServerStreamListener;
import org.apache.arrow.flight.FlightProducer.StreamListener;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.Result;
import org.apache.arrow.flight.sql.impl.FlightSql.ActionCreatePreparedStatementRequest;
import org.apache.arrow.flight.sql.impl.FlightSql.TicketStatementQuery;
import org.apache.logging.log4j.ThreadContext;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class DorisFlightSqlProducerTest {

    private boolean prevRunningUnitTest;

    @Before
    public void setUp() {
        // FlightSqlConnectContext.init() only reaches Env when this is false; keep it true so the
        // context can be built without a running FE.
        prevRunningUnitTest = FeConstants.runningUnitTest;
        FeConstants.runningUnitTest = true;
    }

    @After
    public void tearDown() {
        FeConstants.runningUnitTest = prevRunningUnitTest;
    }

    /**
     * Regression test for the FE direct-memory leak in {@code createPreparedStatement}
     * (issue apache/doris#65305, fixed by PR #65311).
     *
     * <p>Before the fix, each prepare allocated a {@link org.apache.arrow.vector.VectorSchemaRoot}
     * via {@code FlightSqlChannel.createOneOneSchemaRoot("ResultMeta", ...)} and read only its
     * {@code Schema}, never closing the root, leaking one off-heap {@code VarCharVector} buffer per
     * prepare. Repeated prepare calls can exhaust {@code MaxDirectMemorySize}; queries executed
     * without preparing do not exercise this allocation path.
     *
     * <p>This test drives {@code createPreparedStatement} against a real {@link FlightSqlChannel} many
     * times and asserts the channel's Arrow allocator holds zero bytes afterwards. It goes red if the
     * try-with-resources that closes the temporary roots is removed.
     */
    @Test
    public void createPreparedStatementDoesNotLeakChannelAllocator() throws Exception {
        // A real flight session context owns a real FlightSqlChannel (and thus a real Arrow allocator),
        // so allocator bookkeeping is exercised for real instead of mocked away.
        FlightSqlConnectContext connectContext = new FlightSqlConnectContext("test-peer-identity");
        FlightSqlChannel channel = connectContext.getFlightSqlChannel();
        Assert.assertEquals("channel allocator should start empty", 0L, channel.getAllocatedMemory());

        FlightSessionsManager sessionsManager = new FlightSessionsManager() {
            @Override
            public ConnectContext getConnectContext(String peerIdentity) {
                return connectContext;
            }

            @Override
            public ConnectContext createConnectContext(String peerIdentity) {
                return connectContext;
            }

            @Override
            public void closeConnectContext(String peerIdentity) {
                // not exercised by this test
            }
        };
        DorisFlightSqlProducer producer =
                new DorisFlightSqlProducer(Location.forGrpcInsecure("127.0.0.1", 9090), sessionsManager);

        CallContext callContext = Mockito.mock(CallContext.class);
        Mockito.when(callContext.peerIdentity()).thenReturn("test-peer-identity");

        final int rounds = 100;
        AtomicInteger errors = new AtomicInteger(0);
        try {
            // createPreparedStatement runs asynchronously and mutates a shared, non-thread-safe
            // ConnectContext, so drive it serially: each prepare completes before the next is issued.
            // The leak, if any, still accumulates on the single channel allocator across rounds.
            for (int i = 0; i < rounds; i++) {
                CountDownLatch finished = new CountDownLatch(1);
                StreamListener<Result> listener = new StreamListener<Result>() {
                    @Override
                    public void onNext(Result val) {
                        // discard the placeholder prepared-statement result
                    }

                    @Override
                    public void onError(Throwable t) {
                        errors.incrementAndGet();
                        finished.countDown();
                    }

                    @Override
                    public void onCompleted() {
                        finished.countDown();
                    }
                };
                ActionCreatePreparedStatementRequest request = ActionCreatePreparedStatementRequest.newBuilder()
                        .setQuery("select * from t where id = " + i).build();
                producer.createPreparedStatement(request, callContext, listener);
                Assert.assertTrue("createPreparedStatement #" + i + " did not finish in time",
                        finished.await(30, TimeUnit.SECONDS));
            }

            // Guard against a false pass: if a prepare failed before reaching the allocation, no buffer
            // would be leaked and the memory assertion below could not detect a regression.
            Assert.assertEquals("no createPreparedStatement call should fail", 0, errors.get());
            // Every temporary VectorSchemaRoot must have been closed, so the channel's Arrow allocator
            // is back to zero. Reverting the fix leaves `rounds` ResultMeta buffers allocated here.
            Assert.assertEquals("createPreparedStatement leaked off-heap memory in the channel allocator",
                    0L, channel.getAllocatedMemory());
        } finally {
            // Producer.close() does not stop its executor, so release the test's worker threads here.
            ExecutorService executor = Deencapsulation.getField(producer, "executorService");
            executor.shutdownNow();
            try {
                Assert.assertTrue("prepare workers did not stop", executor.awaitTermination(30, TimeUnit.SECONDS));
            } finally {
                producer.close();
            }
        }
    }

    @Test
    public void streamUsesRetainedQueryAndRestoresWorker() throws Exception {
        verifyStreamIdentity(false, false);
    }

    @Test
    public void failedStreamKeepsRetainedQueryThroughCleanup() throws Exception {
        verifyStreamIdentity(true, false);
    }

    @Test
    public void expiredStreamDoesNotUseTheSessionsPreviousQuery() throws Exception {
        verifyStreamIdentity(false, true);
    }

    private void verifyStreamIdentity(boolean fail, boolean missing) throws Exception {
        boolean savedEnabled = Config.sys_log_enable_query_id;
        String savedIdentity = ThreadContext.get(QueryLogContext.QUERY_ID);
        try {
            for (boolean enabled : new boolean[] {true, false}) {
                Config.sys_log_enable_query_id = enabled;
                FlightSqlConnectContext context = new FlightSqlConnectContext("peer");
                FlightSqlChannel channel = context.getFlightSqlChannel();
                FlightSessionsManager sessions = Mockito.mock(FlightSessionsManager.class);
                Mockito.when(sessions.getConnectContext("peer")).thenReturn(context);
                DorisFlightSqlProducer producer =
                        new DorisFlightSqlProducer(Location.forGrpcInsecure("127.0.0.1", 9090), sessions);
                try {
                    TUniqueId queryId = new TUniqueId(1, 2);
                    if (!missing) {
                        channel.addOKResult("independent-ticket", "SET query_timeout=10", queryId);
                    }
                    queryId.setLo(3);
                    context.setQueryId(new TUniqueId(9, 9));
                    ThreadContext.put(QueryLogContext.QUERY_ID, "worker");
                    List<String> observed = new ArrayList<>();
                    ServerStreamListener listener = Mockito.mock(ServerStreamListener.class);
                    Mockito.doAnswer(invocation -> {
                        observed.add(ThreadContext.get(QueryLogContext.QUERY_ID));
                        if (fail) {
                            throw new IllegalStateException("stream failure");
                        }
                        return null;
                    }).when(listener).start(Mockito.any());
                    Mockito.doAnswer(invocation -> {
                        observed.add(ThreadContext.get(QueryLogContext.QUERY_ID));
                        return null;
                    }).when(listener).error(Mockito.any());
                    Mockito.doAnswer(invocation -> {
                        observed.add(ThreadContext.get(QueryLogContext.QUERY_ID));
                        return null;
                    }).when(listener).completed();
                    TicketStatementQuery ticket = TicketStatementQuery.newBuilder()
                            .setStatementHandle(ByteString.copyFromUtf8("peer:independent-ticket")).build();
                    if (fail || missing) {
                        Assert.assertThrows(RuntimeException.class,
                                () -> producer.getStreamStatement(ticket, null, listener));
                    } else {
                        producer.getStreamStatement(ticket, null, listener);
                    }
                    Assert.assertEquals(missing ? 2 : fail ? 3 : 2, observed.size());
                    String expected = enabled ? missing ? null : "1-2" : "worker";
                    for (String identity : observed) {
                        Assert.assertEquals(expected, identity);
                    }
                    Assert.assertEquals("worker", ThreadContext.get(QueryLogContext.QUERY_ID));
                    Assert.assertNull(channel.getResult("independent-ticket"));
                    Assert.assertEquals(0, channel.getAllocatedMemory());
                } finally {
                    ExecutorService executor = Deencapsulation.getField(producer, "executorService");
                    executor.shutdownNow();
                    Assert.assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
                    producer.close();
                    channel.close();
                }
            }
        } finally {
            Config.sys_log_enable_query_id = savedEnabled;
            if (savedIdentity == null) {
                ThreadContext.remove(QueryLogContext.QUERY_ID);
            } else {
                ThreadContext.put(QueryLogContext.QUERY_ID, savedIdentity);
            }
        }
    }

}
