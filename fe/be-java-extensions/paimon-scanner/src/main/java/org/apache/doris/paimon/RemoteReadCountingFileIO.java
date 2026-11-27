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

import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.FileStatus;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.fs.SeekableInputStreamWrapper;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

/** Counts bytes returned by storage streams, below Paimon's format readers and buffers. */
final class RemoteReadCountingFileIO implements FileIO {
    private static final long serialVersionUID = 1L;

    private final FileIO delegate;
    private final String tableScheme;
    private final AtomicLong bytes;

    RemoteReadCountingFileIO(FileIO delegate, Path location, AtomicLong bytes) {
        this.delegate = delegate;
        this.tableScheme = location.toUri().getScheme();
        this.bytes = bytes;
    }

    @Override
    public SeekableInputStream newInputStream(Path path) throws IOException {
        SeekableInputStream stream = delegate.newInputStream(path);
        String scheme = path.toUri().getScheme();
        if (scheme == null) {
            scheme = tableScheme;
        }
        if (scheme == null || "file".equalsIgnoreCase(scheme)) {
            return stream;
        }
        return new SeekableInputStreamWrapper(stream) {
            @Override
            public int read() throws IOException {
                int value = in.read();
                if (value >= 0) {
                    bytes.incrementAndGet();
                }
                return value;
            }

            @Override
            public int read(byte[] buffer, int offset, int length) throws IOException {
                int count = in.read(buffer, offset, length);
                if (count > 0) {
                    bytes.addAndGet(count);
                }
                return count;
            }

            @Override
            public int available() throws IOException {
                return in.available();
            }
        };
    }

    @Override
    public boolean isObjectStore() {
        return delegate.isObjectStore();
    }

    @Override
    public void configure(CatalogContext context) {
        delegate.configure(context);
    }

    @Override
    public PositionOutputStream newOutputStream(Path path, boolean overwrite) throws IOException {
        return delegate.newOutputStream(path, overwrite);
    }

    @Override
    public FileStatus getFileStatus(Path path) throws IOException {
        return delegate.getFileStatus(path);
    }

    @Override
    public FileStatus[] listStatus(Path path) throws IOException {
        return delegate.listStatus(path);
    }

    @Override
    public boolean exists(Path path) throws IOException {
        return delegate.exists(path);
    }

    @Override
    public boolean delete(Path path, boolean recursive) throws IOException {
        return delegate.delete(path, recursive);
    }

    @Override
    public boolean mkdirs(Path path) throws IOException {
        return delegate.mkdirs(path);
    }

    @Override
    public boolean rename(Path source, Path target) throws IOException {
        return delegate.rename(source, target);
    }

    @Override
    public void close() throws IOException {
        delegate.close();
    }
}
