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

package org.apache.doris.common;

import org.apache.doris.thrift.TBackend;
import org.apache.doris.thrift.TReportRequest;
import org.apache.doris.thrift.TTablet;
import org.apache.doris.thrift.TTabletInfo;
import org.apache.doris.utframe.UtFrameUtils;

import org.apache.thrift.TProcessor;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TMessage;
import org.apache.thrift.protocol.TMessageType;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.layered.TFramedTransport;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class ThriftServerProtocolTest {
    @Test(timeout = 30000)
    public void testThreadPoolUsesCachedProtocol() throws Exception {
        assertServerProtocol(ThriftServer.THREAD_POOL, CachedTBinaryProtocol.class);
    }

    @Test(timeout = 30000)
    public void testSimpleUsesStandardProtocol() throws Exception {
        assertServerProtocol(ThriftServer.SIMPLE, TBinaryProtocol.class);
    }

    @Test(timeout = 30000)
    public void testThreadedSelectorUsesStandardProtocol() throws Exception {
        assertServerProtocol(ThriftServer.THREADED_SELECTOR, TBinaryProtocol.class);
    }

    // Exercise the actual server factory and generated payload, not FE report business logic.
    private static void assertServerProtocol(String mode, Class<? extends TProtocol> expectedProtocol)
            throws Exception {
        String previousMode = Config.thrift_server_type;
        BlockingQueue<ObservedRequest> observedRequests = new LinkedBlockingQueue<>();
        TProcessor processor = (input, output) -> {
            TMessage message = input.readMessageBegin();
            TReportRequest request = new TReportRequest();
            request.read(input);
            input.readMessageEnd();
            observedRequests.add(new ObservedRequest(input, output, message, request));
            output.writeMessageBegin(new TMessage(message.name, TMessageType.REPLY, message.seqid));
            request.write(output);
            output.writeMessageEnd();
            output.getTransport().flush();
        };
        ThriftServer server = null;
        boolean started = false;
        try {
            Config.thrift_server_type = mode;
            int port = UtFrameUtils.findValidPort();
            server = new ThriftServer(port, processor);
            server.start();
            started = true;
            boolean framed = ThriftServer.THREADED_SELECTOR.equals(mode);
            try (TTransport transport = framed
                    ? new TFramedTransport(new TSocket("127.0.0.1", port, 5000))
                    : new TSocket("127.0.0.1", port, 5000)) {
                transport.open();
                TProtocol client = new TBinaryProtocol(transport);
                for (int sequence = 1; sequence <= 2; sequence++) {
                    TReportRequest expected = report(sequence);
                    client.writeMessageBegin(new TMessage("echoReport", TMessageType.CALL, sequence));
                    expected.write(client);
                    client.writeMessageEnd();
                    transport.flush();

                    TMessage reply = client.readMessageBegin();
                    Assert.assertEquals("echoReport", reply.name);
                    Assert.assertEquals(TMessageType.REPLY, reply.type);
                    Assert.assertEquals(sequence, reply.seqid);
                    TReportRequest actual = new TReportRequest();
                    actual.read(client);
                    client.readMessageEnd();
                    Assert.assertEquals(expected, actual);

                    ObservedRequest observed = observedRequests.poll(5, TimeUnit.SECONDS);
                    Assert.assertNotNull(observed);
                    Assert.assertEquals(expectedProtocol, observed.inputProtocol);
                    Assert.assertEquals(expectedProtocol, observed.outputProtocol);
                    Assert.assertEquals("echoReport", observed.message.name);
                    Assert.assertEquals(TMessageType.CALL, observed.message.type);
                    Assert.assertEquals(sequence, observed.message.seqid);
                    Assert.assertEquals(expected, observed.request);
                    if (!framed) {
                        Assert.assertEquals(Config.thrift_max_message_size, observed.messageLimit);
                        Assert.assertEquals(Config.thrift_max_frame_size, observed.frameLimit);
                    }
                }
            }
        } finally {
            try {
                if (started) {
                    // Close the client first so SIMPLE can leave its blocking request loop.
                    server.stop();
                    server.join();
                }
            } finally {
                Config.thrift_server_type = previousMode;
            }
        }
    }

    private static TReportRequest report(int sequence) {
        TTabletInfo tabletInfo = new TTabletInfo();
        tabletInfo.setTabletId(1000001L).setSchemaHash(1024).setVersion(sequence).setVersionHash(0)
                .setRowCount(100).setDataSize(4096).setTransactionIds(Arrays.asList(9001L, 9002L));
        TTablet tablet = new TTablet(Collections.singletonList(tabletInfo));
        return new TReportRequest().setBackend(new TBackend("127.0.0.1", 9060, 8040))
                .setTablets(Collections.singletonMap(tabletInfo.getTabletId(), tablet)).setReportVersion(sequence);
    }

    private static final class ObservedRequest {
        private final Class<?> inputProtocol;
        private final Class<?> outputProtocol;
        private final int messageLimit;
        private final int frameLimit;
        private final TMessage message;
        private final TReportRequest request;

        private ObservedRequest(TProtocol input, TProtocol output, TMessage message, TReportRequest request) {
            inputProtocol = input.getClass();
            outputProtocol = output.getClass();
            messageLimit = input.getTransport().getConfiguration().getMaxMessageSize();
            frameLimit = input.getTransport().getConfiguration().getMaxFrameSize();
            this.message = message;
            this.request = request;
        }
    }
}
