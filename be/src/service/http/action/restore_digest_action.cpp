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

#include "service/http/action/restore_digest_action.h"

#include <fmt/format.h>

#include <algorithm>
#include <string>

#include "common/logging.h"
#include "common/status.h"
#include "io/fs/file_reader.h"
#include "io/fs/local_file_system.h"
#include "runtime/memory/mem_tracker_limiter.h"
#include "runtime/thread_context.h"
#include "service/http/action/action_constants.h"
#include "service/http/http_channel.h"
#include "service/http/http_headers.h"
#include "service/http/http_request.h"
#include "service/http/http_status.h"
#include "storage/restore_digest/restore_digest.h"
#include "storage/storage_engine.h"

namespace doris {

RestoreDigestAction::RestoreDigestAction(ExecEnv* exec_env, StorageEngine& engine,
                                         TPrivilegeHier::type hier, TPrivilegeType::type type)
        : HttpHandlerWithAuth(exec_env, hier, type), _engine(engine) {}

// Debug: composes the digest at `version` from a decomposed digest file (rdigest) on this BE,
// after checking the file against `root`. version = -1 lists the boundaries instead.
void RestoreDigestAction::handle_prefix_file(HttpRequest* req, int64_t tablet_id, int64_t version,
                                             const std::string& path, const std::string& root) {
    std::string content;
    Status st;
    {
        io::FileReaderSPtr reader;
        st = io::global_local_filesystem()->open_file(path, &reader);
        if (st.ok()) {
            content.resize(reader->size());
            size_t bytes_read = 0;
            st = reader->read_at(0, Slice(content.data(), content.size()), &bytes_read);
            static_cast<void>(reader->close());
        }
    }
    RestoreDigestDecomposed decomposed;
    if (st.ok()) {
        st = RestoreDigestDecomposed::parse(content, root, tablet_id, &decomposed);
    }
    RestoreDigest digest;
    if (st.ok() && version >= 0) {
        st = decomposed.compose(version, &digest);
    }
    req->add_output_header(HttpHeaders::CONTENT_TYPE, HEADER_JSON.c_str());
    if (!st.ok()) {
        std::string msg = st.to_string();
        std::string escaped;
        for (char c : msg) {
            if (c == '"' || c == '\\') {
                escaped.push_back('\\');
            }
            escaped.push_back(c == '\n' ? ' ' : c);
        }
        HttpChannel::send_reply(
                req,
                st.is<ErrorCode::NOT_IMPLEMENTED_ERROR>() ? HttpStatus::NOT_IMPLEMENTED
                                                          : HttpStatus::INTERNAL_SERVER_ERROR,
                fmt::format("{{\"status\":\"{}\",\"msg\":\"{}\"}}",
                            st.is<ErrorCode::NOT_IMPLEMENTED_ERROR>() ? "NOT_COMPOSABLE" : "ERROR",
                            escaped));
        return;
    }
    if (version >= 0) {
        HttpChannel::send_reply(req, HttpStatus::OK, digest.to_json());
        return;
    }
    std::string boundaries;
    for (const auto& rs : decomposed.rowsets) {
        boundaries += fmt::format("{}[{},{}]", boundaries.empty() ? "" : ",", rs.start_version,
                                  rs.end_version);
    }
    std::string marks;
    for (const auto& mark : decomposed.marks) {
        marks += fmt::format("{}{}", marks.empty() ? "" : ",", mark.mark_version);
    }
    HttpChannel::send_reply(
            req, HttpStatus::OK,
            fmt::format("{{\"base_version\":{},\"root\":\"{}\",\"rows\":{},\"mow\":{},"
                        "\"bytes\":{},\"rowsets\":[{}],\"mark_versions\":[{}]}}",
                        decomposed.base_version, decomposed.root, decomposed.rows,
                        decomposed.mow ? "true" : "false", content.size(), boundaries, marks));
}

void RestoreDigestAction::handle(HttpRequest* req) {
    const std::string& tablet_id_str = req->param(TABLET_ID);
    const std::string& version_str = req->param("version");
    int64_t tablet_id = 0;
    int64_t version = 0;
    try {
        tablet_id = std::stoll(tablet_id_str);
        version = std::stoll(version_str);
    } catch (...) {
        HttpChannel::send_reply(req, HttpStatus::BAD_REQUEST,
                                "parameters tablet_id and version are required integers");
        return;
    }
    const std::string& prefix_file = req->param("prefix_file");
    if (!prefix_file.empty()) {
        handle_prefix_file(req, tablet_id, version, prefix_file, req->param("root"));
        return;
    }
    int threads = 1;
    const std::string& threads_str = req->param("threads");
    if (!threads_str.empty()) {
        try {
            threads = std::stoi(threads_str);
        } catch (...) {
            HttpChannel::send_reply(req, HttpStatus::BAD_REQUEST,
                                    "parameter threads must be an integer");
            return;
        }
        threads = std::clamp(threads, 1, 64);
    }
    LOG(INFO) << "restore digest begin, tablet_id=" << tablet_id << ", version=" << version
              << ", threads=" << threads;

    RestoreDigest digest;
    Status st;
    {
        auto mem_tracker = MemTrackerLimiter::create_shared(
                MemTrackerLimiter::Type::OTHER,
                "RestoreDigest#tabletId=" + std::to_string(tablet_id));
        SCOPED_ATTACH_TASK(mem_tracker);
        st = compute_tablet_restore_digest(_engine, tablet_id, version, &digest, threads);
    }
    if (st.ok()) {
        LOG(INFO) << "restore digest done, tablet_id=" << tablet_id << ", version=" << version
                  << ", rows=" << digest.rows << ", root=" << digest.root
                  << ", elapsed_ms=" << digest.elapsed_ms;
        req->add_output_header(HttpHeaders::CONTENT_TYPE, HEADER_JSON.c_str());
        HttpChannel::send_reply(req, HttpStatus::OK, digest.to_json());
        return;
    }
    LOG(WARNING) << "restore digest failed, tablet_id=" << tablet_id << ", version=" << version
                 << ", status=" << st;
    HttpStatus code = st.is<ErrorCode::NOT_FOUND>() ? HttpStatus::NOT_FOUND
                      : st.is<ErrorCode::NOT_IMPLEMENTED_ERROR>()
                              ? HttpStatus::NOT_IMPLEMENTED
                              : HttpStatus::INTERNAL_SERVER_ERROR;
    std::string code_name = st.is<ErrorCode::NOT_IMPLEMENTED_ERROR>() ? "NOT_SUPPORTED" : "ERROR";
    std::string msg = st.to_string();
    // minimal JSON escaping of the message
    std::string escaped;
    for (char c : msg) {
        if (c == '"' || c == '\\') {
            escaped.push_back('\\');
            escaped.push_back(c);
        } else if (c == '\n') {
            escaped += "\\n";
        } else {
            escaped.push_back(c);
        }
    }
    req->add_output_header(HttpHeaders::CONTENT_TYPE, HEADER_JSON.c_str());
    HttpChannel::send_reply(
            req, code, fmt::format("{{\"status\":\"{}\",\"msg\":\"{}\"}}", code_name, escaped));
}

} // namespace doris
