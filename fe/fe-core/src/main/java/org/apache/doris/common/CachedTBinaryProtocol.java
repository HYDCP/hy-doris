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

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TField;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TType;
import org.apache.thrift.transport.TTransport;

/** Binary protocol with bounded reuse of immutable field descriptors on the read path. */
public final class CachedTBinaryProtocol extends TBinaryProtocol {
    private static final int CACHED_FIELD_ID_COUNT = 64;
    private static final TField STOP_FIELD = new TField("", TType.STOP, (short) 0);
    private static final TField[] CACHED_FIELDS = createCachedFields();

    public CachedTBinaryProtocol(TTransport transport) {
        super(transport);
    }

    private CachedTBinaryProtocol(TTransport transport, long stringLengthLimit, long containerLengthLimit,
            boolean strictRead, boolean strictWrite) {
        super(transport, stringLengthLimit, containerLengthLimit, strictRead, strictWrite);
    }

    private static TField[] createCachedFields() {
        TField[] fields = new TField[(TType.ENUM + 1) * CACHED_FIELD_ID_COUNT];
        // STOP is handled separately and never indexes the table.
        for (byte type = TType.VOID; type <= TType.ENUM; type++) {
            for (short id = 0; id < CACHED_FIELD_ID_COUNT; id++) {
                fields[type * CACHED_FIELD_ID_COUNT + id] = new TField("", type, id);
            }
        }
        return fields;
    }

    @Override
    public TField readFieldBegin() throws TException {
        byte type = readByte();
        // STOP has no field ID on the wire, as in Thrift 0.16.0's TBinaryProtocol.
        if (type == TType.STOP) {
            return STOP_FIELD;
        }
        short id = readI16();
        if (type >= TType.STOP && type <= TType.ENUM && id >= 0 && id < CACHED_FIELD_ID_COUNT) {
            return CACHED_FIELDS[type * CACHED_FIELD_ID_COUNT + id];
        }
        // Unknown types and IDs retain the standard protocol's parsing and validation behavior.
        return new TField("", type, id);
    }

    public static final class Factory extends TBinaryProtocol.Factory {
        public Factory() {
        }

        public Factory(boolean strictRead, boolean strictWrite, long stringLengthLimit, long containerLengthLimit) {
            super(strictRead, strictWrite, stringLengthLimit, containerLengthLimit);
        }

        @Override
        public TProtocol getProtocol(TTransport transport) {
            return new CachedTBinaryProtocol(transport, stringLengthLimit_, containerLengthLimit_,
                    strictRead_, strictWrite_);
        }
    }
}
