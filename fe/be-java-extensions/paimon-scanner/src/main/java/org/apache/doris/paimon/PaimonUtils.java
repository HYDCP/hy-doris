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

package org.apache.doris.paimon;

import org.apache.doris.common.classloader.ThreadClassLoaderContext;

import org.apache.paimon.fs.FileIO;
import org.apache.paimon.table.AppendOnlyFileStoreTable;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.PrimaryKeyFileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.InstantiationUtil;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

public class PaimonUtils {
    private static final Base64.Decoder DECODER = Base64.getUrlDecoder();

    public static List<String> getFieldNames(RowType rowType) {
        return rowType.getFields().stream()
                .map(DataField::name)
                .map(String::toLowerCase)
                .collect(Collectors.toList());
    }

    // Fallback-branch tables contain two serialized data tables. Install the same scan counter
    // on both while preserving the schema and branch selected by the FE.
    static Table deserializeTable(String encoded, AtomicLong bytes) throws IOException, ClassNotFoundException {
        ClassLoader loader = PaimonUtils.class.getClassLoader();
        try (ThreadClassLoaderContext ignored = new ThreadClassLoaderContext(loader);
                CountingTableInput input = new CountingTableInput(DECODER.decode(encoded), loader, bytes)) {
            return (Table) input.readObject();
        }
    }

    private static final class CountingTableInput extends InstantiationUtil.ClassLoaderObjectInputStream {
        private final AtomicLong bytes;

        private CountingTableInput(byte[] serialized, ClassLoader loader, AtomicLong bytes) throws IOException {
            super(new ByteArrayInputStream(serialized), loader);
            this.bytes = bytes;
            enableResolveObject(true);
        }

        @Override
        protected Object resolveObject(Object value) {
            if (value == null || (value.getClass() != AppendOnlyFileStoreTable.class
                    && value.getClass() != PrimaryKeyFileStoreTable.class)) {
                return value;
            }
            FileStoreTable table = (FileStoreTable) value;
            FileIO io = new RemoteReadCountingFileIO(table.fileIO(), table.location(), bytes);
            if (value instanceof AppendOnlyFileStoreTable) {
                return new AppendOnlyFileStoreTable(io, table.location(), table.schema(), table.catalogEnvironment());
            }
            return new PrimaryKeyFileStoreTable(io, table.location(), table.schema(), table.catalogEnvironment());
        }
    }

    public static <T> T deserialize(String encodedStr) {
        try {
            return InstantiationUtil.deserializeObject(
                    DECODER.decode(encodedStr.getBytes(java.nio.charset.StandardCharsets.UTF_8)),
                    PaimonUtils.class.getClassLoader());
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }
}
