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

#pragma once

#include <gen_cpp/Types_types.h>

#include "io/io_common.h"

namespace doris::vectorized {

struct FileScanBytes {
    int64_t local;
    int64_t remote;
};

inline FileScanBytes classify_file_scan_bytes(TFileType::type file_type, int64_t read_bytes,
                                              const io::FileCacheStatistics& cache,
                                              int64_t peer_cache_bytes) {
    if (cache.bytes_read_from_local || cache.bytes_read_from_remote || peer_cache_bytes) {
        return {cache.bytes_read_from_local, cache.bytes_read_from_remote};
    }
    // Direct file reads have no file-cache statistics.
    switch (file_type) {
    case TFileType::FILE_LOCAL:
        return {read_bytes, 0};
    case TFileType::FILE_BROKER:
    case TFileType::FILE_S3:
    case TFileType::FILE_HDFS:
    case TFileType::FILE_NET:
        return {0, read_bytes};
    case TFileType::FILE_STREAM:
        return {0, 0};
    }
    return {0, 0};
}

} // namespace doris::vectorized
