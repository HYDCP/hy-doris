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

#include <cstdint>
#include <string>
#include <vector>

#include "common/status.h"
#include "service/http/http_handler_with_auth.h"

namespace doris {

class HttpRequest;
class StorageEngine;

// Debug-only, read-only endpoint of the restore reuse prototype:
//   GET /api/_restore_digest?tablet_id=<id>&version=<v>
// returns the order-independent logical digest of the tablet at (0, v] as JSON.
// Debug only, off unless the BE config enable_restore_digest_http_api is true.
class RestoreDigestAction final : public HttpHandlerWithAuth {
public:
    RestoreDigestAction(ExecEnv* exec_env, StorageEngine& engine, TPrivilegeHier::type hier,
                        TPrivilegeType::type type);

    ~RestoreDigestAction() override = default;

    void handle(HttpRequest* req) override;

    // The largest decomposed digest file the prefix_file request reads (it is read into memory whole).
    static constexpr int64_t kMaxPrefixFileBytes = 256LL << 20;

    // The prefix_file request reads only a regular file under <store>/snapshot/ of one of `store_paths`, of at
    // most `max_bytes`: the debug API must not read any other local file. On success `*real_path` is the
    // canonical path, `*size` its size. INVALID_ARGUMENT otherwise.
    static Status check_prefix_file(const std::vector<std::string>& store_paths,
                                    const std::string& path, int64_t max_bytes,
                                    std::string* real_path, int64_t* size);

private:
    void handle_prefix_file(HttpRequest* req, int64_t tablet_id, int64_t version,
                            const std::string& path, const std::string& root);

    StorageEngine& _engine;
};

} // namespace doris
