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

import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.fs.FileStatus;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.AppendOnlyFileStoreTable;
import org.apache.paimon.table.CatalogEnvironment;
import org.apache.paimon.table.FallbackReadFileStoreTable;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.PrimaryKeyFileStoreTable;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.utils.InstantiationUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

class RemoteReadCountingFileIOTest {
    @TempDir
    java.nio.file.Path directory;

    // Use a real seekable file stream with a storage URI local to this test.
    static class TestFileIO extends LocalFileIO {
        @Override
        public SeekableInputStream newInputStream(Path path) throws IOException {
            return super.newInputStream(new Path(path.toUri().getPath()));
        }

        @Override
        public FileStatus getFileStatus(Path path) throws IOException {
            return super.getFileStatus(new Path(path.toUri().getPath()));
        }

        @Override
        public boolean exists(Path path) throws IOException {
            return super.exists(new Path(path.toUri().getPath()));
        }
    }

    @Test
    void countsSuccessfulReadsPerScannerAndSkipsLocalFiles() throws Exception {
        java.nio.file.Path file = directory.resolve("data");
        Files.write(file, new byte[] {11, 12, 13, 14});
        Path remote = new Path("hdfs://storage" + file.toUri().getPath());
        Path local = new Path(file.toUri());
        for (boolean primaryKey : new boolean[] {false, true}) {
            Schema.Builder builder = Schema.newBuilder().column("id", DataTypes.INT());
            if (primaryKey) {
                builder.primaryKey("id");
            }
            TableSchema schema = TableSchema.create(7, builder.build());
            CatalogEnvironment env = CatalogEnvironment.empty();
            FileStoreTable original = primaryKey
                    ? new PrimaryKeyFileStoreTable(new TestFileIO(), remote, schema, env)
                    : new AppendOnlyFileStoreTable(new TestFileIO(), remote, schema, env);
            String encoded = encode(original);
            AtomicLong bytes = new AtomicLong();
            FileStoreTable table = (FileStoreTable) PaimonUtils.deserializeTable(encoded, bytes);
            Assertions.assertEquals(original.schema(), table.schema());
            Assertions.assertEquals(original.getClass(), table.getClass());
            try (SeekableInputStream input = table.fileIO().newInputStream(remote)) {
                Assertions.assertEquals(11, input.read());
                Assertions.assertEquals(3, input.read(new byte[6]));
                Assertions.assertEquals(-1, input.read());
                input.seek(1);
                Assertions.assertEquals(12, input.read());
                Assertions.assertEquals(1, input.skip(1));
            }
            Assertions.assertEquals(6L, bytes.get());

            AtomicLong second = new AtomicLong();
            FileStoreTable other = (FileStoreTable) PaimonUtils.deserializeTable(encoded, second);
            try (SeekableInputStream input = other.fileIO().newInputStream(local)) {
                Assertions.assertEquals(11, input.read());
            }
            Assertions.assertEquals(0, second.get());
            Assertions.assertEquals(6L, bytes.get());
        }
    }

    @Test
    void parquetAndOrcReturnRowsAndCountStorageReads() throws Exception {
        for (String format : Arrays.asList("parquet", "orc")) {
            Path local = new Path(directory.resolve(format).toUri());
            Schema schema = Schema.newBuilder().column("id", DataTypes.INT()).option("file.format", format).build();
            TableSchema stored = new SchemaManager(LocalFileIO.create(), local).createTable(schema);
            FileStoreTable writer = new AppendOnlyFileStoreTable(LocalFileIO.create(), local, stored,
                    CatalogEnvironment.empty());
            BatchWriteBuilder builder = writer.newBatchWriteBuilder();
            try (BatchTableWrite write = builder.newWrite(); BatchTableCommit commit = builder.newCommit()) {
                write.write(GenericRow.of(11));
                write.write(GenericRow.of(12));
                commit.commit(write.prepareCommit());
            }
            DataSplit split = (DataSplit) writer.newReadBuilder().newScan().plan().splits().get(0);
            Path remote = new Path("hdfs://storage" + local.toUri().getPath());
            String bucketPath = new Path(split.bucketPath()).toUri().getPath();
            DataSplit remoteSplit = DataSplit.builder().withSnapshot(split.snapshotId()).withPartition(split.partition())
                    .withBucket(split.bucket()).withBucketPath("hdfs://storage" + bucketPath)
                    .withDataFiles(split.dataFiles()).build();
            Map<String, String> options = new HashMap<>(stored.options());
            options.put("path", remote.toString());
            FileStoreTable source = new AppendOnlyFileStoreTable(new TestFileIO(), remote, stored.copy(options),
                    CatalogEnvironment.empty());
            AtomicLong bytes = new AtomicLong();
            FileStoreTable table = (FileStoreTable) PaimonUtils.deserializeTable(encode(source), bytes);
            List<Integer> rows = new ArrayList<>();
            try (RecordReader<InternalRow> reader = table.newReadBuilder().newRead().createReader(remoteSplit)) {
                reader.forEachRemaining(row -> rows.add(row.getInt(0)));
            }
            Assertions.assertEquals(Arrays.asList(11, 12), rows);
            Assertions.assertTrue(bytes.get() > 0, format);
        }
    }

    @Test
    void metricsFollowTheReaderCapability() {
        Map<String, String> params = new HashMap<>();
        params.put("required_fields", "id");
        params.put("columns_types", "int");
        Assertions.assertTrue(new PaimonJniScanner(16, params).getStatistics().isEmpty());
        params.put("collect_remote_scan_bytes", "true");
        Assertions.assertEquals("0", new PaimonJniScanner(16, params).getStatistics().get("bytes:RemoteScanBytes"));
    }

    @Test
    void preservesFallbackBranchAndItsSelectedSchema() throws Exception {
        Path location = new Path("s3://warehouse/table");
        Schema.Builder builder = Schema.newBuilder().column("id", DataTypes.INT());
        FileStoreTable main = new AppendOnlyFileStoreTable(new TestFileIO(), location,
                TableSchema.create(7, builder.build()), CatalogEnvironment.empty());
        FileStoreTable fallback = new AppendOnlyFileStoreTable(new TestFileIO(), location,
                TableSchema.create(3, builder.option("branch", "previous").build()), CatalogEnvironment.empty());
        String encoded = encode(new FallbackReadFileStoreTable(main, fallback));
        FallbackReadFileStoreTable restored =
                (FallbackReadFileStoreTable) PaimonUtils.deserializeTable(encoded, new AtomicLong());
        Assertions.assertEquals(main.schema(), restored.wrapped().schema());
        Assertions.assertTrue(restored.wrapped().fileIO() instanceof RemoteReadCountingFileIO);
        java.lang.reflect.Field fallbackField = FallbackReadFileStoreTable.class.getDeclaredField("fallback");
        fallbackField.setAccessible(true);
        FileStoreTable nested = (FileStoreTable) fallbackField.get(restored);
        Assertions.assertEquals(fallback.schema(), nested.schema());
        Assertions.assertTrue(nested.fileIO() instanceof RemoteReadCountingFileIO);
    }

    private static String encode(FileStoreTable table) throws IOException {
        return Base64.getUrlEncoder().encodeToString(InstantiationUtil.serializeObject(table));
    }
}
