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

package org.apache.doris.rpc;

import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TType;
import org.apache.thrift.transport.TMemoryInputTransport;
import org.junit.Assert;
import org.junit.Test;

public class TCustomProtocolFactoryTest {
    @Test
    public void testMessageLimitAndTransportArePreserved() throws Exception {
        TMemoryInputTransport input = new TMemoryInputTransport(new byte[] {TType.I32, 0, 1, 0, 0, 0, 42});
        TProtocol protocol = new TCustomProtocolFactory(123456).getProtocol(input);
        Assert.assertEquals(TBinaryProtocol.class, protocol.getClass());
        Assert.assertSame(input, protocol.getTransport());
        Assert.assertEquals(123456, input.getConfiguration().getMaxMessageSize());
        Assert.assertEquals(1, protocol.readFieldBegin().id);
        Assert.assertEquals(42, protocol.readI32());
        Assert.assertEquals(0, input.getBytesRemainingInBuffer());
    }
}
