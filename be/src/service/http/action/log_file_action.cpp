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

#include "service/http/action/log_file_action.h"

#include <atomic>
#include <chrono>
#include <cstring>
#include <cstdio>
#include <cstdlib>
#include <fcntl.h>
#include <fstream>
#include <limits>
#include <sstream>
#include <string>
#include <sys/stat.h>
#include <utility>
#include <unistd.h>
#include <vector>
#include <zlib.h>

#include <fmt/format.h>

#include "common/config.h"
#include "common/logging.h"
#include "io/fs/local_file_system.h"
#include "runtime/exec_env.h"
#include "service/http/http_channel.h"
#include "service/http/http_headers.h"
#include "service/http/http_method.h"
#include "service/http/http_request.h"
#include "service/http/http_status.h"
#include "util/defer_op.h"
#include "util/easy_json.h"
#include "util/path_util.h"
#include "util/string_util.h"

namespace doris {

namespace {

std::atomic<bool> g_log_download_in_progress {false};

class GzipFileWriter {
public:
    GzipFileWriter(FILE* fp, int level) : _fp(fp) {
        memset(&_zs, 0, sizeof(_zs));
        _ok = (deflateInit2(&_zs, level, Z_DEFLATED, 15 + 16, 8, Z_DEFAULT_STRATEGY) == Z_OK);
    }

    ~GzipFileWriter() {
        if (_ok) {
            deflateEnd(&_zs);
        }
    }

    bool ok() const { return _ok; }

    Status write(const void* data, size_t len) {
        if (!_ok) {
            return Status::InternalError("gzip init failed");
        }
        _zs.next_in = (Bytef*)data;
        _zs.avail_in = static_cast<uInt>(len);
        unsigned char out[64 * 1024];
        while (_zs.avail_in > 0) {
            _zs.next_out = out;
            _zs.avail_out = sizeof(out);
            int rc = deflate(&_zs, Z_NO_FLUSH);
            if (rc != Z_OK) {
                return Status::InternalError("gzip deflate failed: {}", rc);
            }
            size_t produced = sizeof(out) - _zs.avail_out;
            if (produced > 0 && fwrite(out, 1, produced, _fp) != produced) {
                return Status::IOError("write gzip output failed");
            }
        }
        return Status::OK();
    }

    Status finish() {
        if (!_ok) {
            return Status::InternalError("gzip init failed");
        }
        unsigned char out[64 * 1024];
        int rc;
        do {
            _zs.next_out = out;
            _zs.avail_out = sizeof(out);
            rc = deflate(&_zs, Z_FINISH);
            if (rc != Z_OK && rc != Z_STREAM_END) {
                return Status::InternalError("gzip finish failed: {}", rc);
            }
            size_t produced = sizeof(out) - _zs.avail_out;
            if (produced > 0 && fwrite(out, 1, produced, _fp) != produced) {
                return Status::IOError("write gzip output failed");
            }
        } while (rc != Z_STREAM_END);
        return Status::OK();
    }

private:
    FILE* _fp = nullptr;
    z_stream _zs;
    bool _ok = false;
};

struct TarHeader {
    char name[100];
    char mode[8];
    char uid[8];
    char gid[8];
    char size[12];
    char mtime[12];
    char chksum[8];
    char typeflag;
    char linkname[100];
    char magic[6];
    char version[2];
    char uname[32];
    char gname[32];
    char devmajor[8];
    char devminor[8];
    char prefix[155];
    char pad[12];
};

void write_octal(char* buf, size_t len, uint64_t value) {
    snprintf(buf, len, "%0*lo", static_cast<int>(len - 1), static_cast<unsigned long>(value));
}

Status build_tar_header(TarHeader* h, const std::string& entry_name, int64_t file_size,
                        int64_t mtime_sec) {
    memset(h, 0, sizeof(TarHeader));
    std::string name = entry_name;
    if (name.size() <= 100) {
        memcpy(h->name, name.data(), name.size());
    } else if (name.size() <= 255) {
        size_t split_pos = name.rfind('/');
        if (split_pos == std::string::npos || split_pos == 0) {
            return Status::InvalidArgument("tar entry name too long");
        }
        std::string prefix = name.substr(0, split_pos);
        std::string base = name.substr(split_pos + 1);
        if (prefix.size() > 155 || base.size() > 100) {
            return Status::InvalidArgument("tar entry name too long");
        }
        memcpy(h->prefix, prefix.data(), prefix.size());
        memcpy(h->name, base.data(), base.size());
    } else {
        return Status::InvalidArgument("tar entry name too long");
    }

    write_octal(h->mode, sizeof(h->mode), 0644);
    write_octal(h->uid, sizeof(h->uid), 0);
    write_octal(h->gid, sizeof(h->gid), 0);
    write_octal(h->size, sizeof(h->size), static_cast<uint64_t>(file_size));
    write_octal(h->mtime, sizeof(h->mtime), static_cast<uint64_t>(mtime_sec));
    memset(h->chksum, ' ', sizeof(h->chksum));
    h->typeflag = '0';
    memcpy(h->magic, "ustar", 5);
    memcpy(h->version, "00", 2);

    unsigned int sum = 0;
    const unsigned char* p = reinterpret_cast<const unsigned char*>(h);
    for (size_t i = 0; i < 512; ++i) {
        sum += p[i];
    }
    snprintf(h->chksum, sizeof(h->chksum), "%06o", sum);
    h->chksum[6] = '\0';
    h->chksum[7] = ' ';
    return Status::OK();
}

std::string get_base_log_dir() {
    std::string dir = config::sys_log_dir;
    if (dir.empty()) {
        const char* env = std::getenv("LOG_DIR");
        if (env != nullptr) {
            dir = env;
        }
    }
    return dir;
}

Status resolve_in_base(const std::string& base_dir, const std::string& input_path, std::string* out) {
    if (base_dir.empty()) {
        return Status::InternalError("LOG_DIR is empty");
    }
    std::string rel = input_path;
    if (rel.empty()) {
        rel = "/";
    }
    if (rel[0] == '/') {
        rel = rel.substr(1);
    }
    std::string joined = fmt::format("{}/{}", base_dir, rel);
    std::string canonical;
    RETURN_IF_ERROR(io::global_local_filesystem()->canonicalize(joined, &canonical));
    std::string canonical_base;
    RETURN_IF_ERROR(io::global_local_filesystem()->canonicalize(base_dir, &canonical_base));
    if (!io::LocalFileSystem::contain_path(canonical_base, canonical)) {
        return Status::InvalidArgument("invalid path");
    }
    *out = canonical;
    return Status::OK();
}

int64_t get_tail_bytes(HttpRequest* req) {
    const auto& p = req->param("tail_bytes");
    if (p.empty()) {
        return 1024 * 1024;
    }
    auto v = safe_stoi(p, "tail_bytes");
    if (!v.has_value()) {
        return 1024 * 1024;
    }
    int64_t n = v.value();
    if (n < 64 * 1024) {
        n = 64 * 1024;
    }
    if (n > 4 * 1024 * 1024) {
        n = 4 * 1024 * 1024;
    }
    return n;
}

std::string sanitize_file_name(std::string name) {
    for (char& c : name) {
        if (c == '/' || c == '\\' || c == '\r' || c == '\n') {
            c = '_';
        }
    }
    if (name.empty()) {
        name = "download";
    }
    return name;
}

Status send_file_attachment(HttpRequest* req, const std::string& abs_file, const std::string& download_name,
                            bufferevent_rate_limit_group* rate_limit_group, const std::string& content_type) {
    int fd = open(abs_file.c_str(), O_RDONLY);
    if (fd < 0) {
        return Status::NotFound("failed to open file");
    }
    struct stat st;
    if (fstat(fd, &st) < 0) {
        close(fd);
        return Status::NotFound("failed to stat file");
    }
    int64_t file_size = st.st_size;
    req->add_output_header(HttpHeaders::CONTENT_TYPE, content_type.c_str());
    req->add_output_header("Content-Disposition",
                           fmt::format("attachment; filename={}", download_name).c_str());

    if (req->method() == HttpMethod::HEAD) {
        close(fd);
        req->add_output_header(HttpHeaders::CONTENT_LENGTH, std::to_string(file_size).c_str());
        HttpChannel::send_reply(req);
        return Status::OK();
    }

    HttpChannel::send_file(req, fd, 0, file_size, rate_limit_group);
    return Status::OK();
}

} // namespace

LogFilesAction::LogFilesAction(ExecEnv* exec_env) : HttpHandlerWithAuth(exec_env) {
    _base_log_dir = get_base_log_dir();
}

void LogFilesAction::handle(HttpRequest* req) {
    std::string abs_dir;
    auto st = resolve_in_base(_base_log_dir, req->param("path"), &abs_dir);
    if (!st.ok()) {
        HttpChannel::send_reply(req, HttpStatus::BAD_REQUEST, st.to_string_no_stack());
        return;
    }

    bool exists = false;
    st = io::global_local_filesystem()->exists(abs_dir, &exists);
    if (!st.ok() || !exists) {
        HttpChannel::send_reply(req, HttpStatus::NOT_FOUND, "path not exist");
        return;
    }

    bool is_dir = false;
    st = io::global_local_filesystem()->is_directory(abs_dir, &is_dir);
    if (!st.ok() || !is_dir) {
        HttpChannel::send_reply(req, HttpStatus::BAD_REQUEST, "path is not directory");
        return;
    }

    std::vector<io::FileInfo> files;
    st = io::global_local_filesystem()->list(abs_dir, false, &files, &exists);
    if (!st.ok()) {
        HttpChannel::send_reply(req, HttpStatus::INTERNAL_SERVER_ERROR, st.to_string_no_stack());
        return;
    }

    int max_entries = 2000;
    const auto& max_entries_param = req->param("max_entries");
    if (!max_entries_param.empty()) {
        auto v = safe_stoi(max_entries_param, "max_entries");
        if (v.has_value() && v.value() > 0) {
            max_entries = v.value();
        }
    }

    std::string input_path = req->param("path");
    if (input_path.empty()) {
        input_path = "/";
    }
    if (input_path.back() != '/') {
        input_path.push_back('/');
    }

    EasyJson ej;
    ej["status"] = "OK";
    ej["base_log_dir"] = _base_log_dir;
    ej["path"] = input_path;
    EasyJson entries = ej.Set("entries", EasyJson::kArray);
    int count = 0;
    for (const auto& f : files) {
        if (count >= max_entries) {
            break;
        }
        EasyJson item = entries.PushBack(EasyJson::kObject);
        item["name"] = f.file_name;
        item["is_dir"] = !f.is_file;
        item["size"] = f.file_size;
        item["path"] = input_path + f.file_name + (f.is_file ? "" : "/");
        count++;
    }
    req->add_output_header(HttpHeaders::CONTENT_TYPE, "application/json; charset=utf-8");
    HttpChannel::send_reply(req, HttpStatus::OK, ej.ToString());
}

LogFileViewAction::LogFileViewAction(ExecEnv* exec_env) : HttpHandlerWithAuth(exec_env) {
    _base_log_dir = get_base_log_dir();
}

void LogFileViewAction::handle(HttpRequest* req) {
    std::string abs_file;
    auto st = resolve_in_base(_base_log_dir, req->param("path"), &abs_file);
    if (!st.ok()) {
        HttpChannel::send_reply(req, HttpStatus::BAD_REQUEST, st.to_string_no_stack());
        return;
    }

    bool exists = false;
    st = io::global_local_filesystem()->exists(abs_file, &exists);
    if (!st.ok() || !exists) {
        HttpChannel::send_reply(req, HttpStatus::NOT_FOUND, "file not exist");
        return;
    }

    bool is_dir = false;
    st = io::global_local_filesystem()->is_directory(abs_file, &is_dir);
    if (!st.ok() || is_dir) {
        HttpChannel::send_reply(req, HttpStatus::BAD_REQUEST, "path is directory");
        return;
    }

    std::ifstream ifs(abs_file, std::ios::binary);
    if (!ifs.is_open()) {
        HttpChannel::send_reply(req, HttpStatus::NOT_FOUND, "failed to open file");
        return;
    }
    ifs.seekg(0, std::ios::end);
    std::streamoff file_size = ifs.tellg();
    int64_t n = get_tail_bytes(req);
    std::streamoff start = file_size > n ? (file_size - n) : 0;
    ifs.seekg(start, std::ios::beg);
    std::string content;
    content.assign(std::istreambuf_iterator<char>(ifs), std::istreambuf_iterator<char>());
    req->add_output_header(HttpHeaders::CONTENT_TYPE, "text/plain; charset=utf-8");
    HttpChannel::send_reply(req, HttpStatus::OK, content);
}

LogFileDownloadAction::LogFileDownloadAction(ExecEnv* exec_env,
                                             std::shared_ptr<bufferevent_rate_limit_group> rate_limit_group)
        : HttpHandlerWithAuth(exec_env), _rate_limit_group(std::move(rate_limit_group)) {
    _base_log_dir = get_base_log_dir();
}

void LogFileDownloadAction::handle(HttpRequest* req) {
    bool expected = false;
    if (!g_log_download_in_progress.compare_exchange_strong(expected, true)) {
        HttpChannel::send_reply(req, HttpStatus::SERVICE_UNAVAILABLE, "another download is in progress");
        return;
    }
    Defer defer([&]() { g_log_download_in_progress.store(false); });

    std::string abs_file;
    auto st = resolve_in_base(_base_log_dir, req->param("path"), &abs_file);
    if (!st.ok()) {
        HttpChannel::send_reply(req, HttpStatus::BAD_REQUEST, st.to_string_no_stack());
        return;
    }

    bool exists = false;
    st = io::global_local_filesystem()->exists(abs_file, &exists);
    if (!st.ok() || !exists) {
        HttpChannel::send_reply(req, HttpStatus::NOT_FOUND, "file not exist");
        return;
    }
    bool is_dir = false;
    st = io::global_local_filesystem()->is_directory(abs_file, &is_dir);
    if (!st.ok() || is_dir) {
        HttpChannel::send_reply(req, HttpStatus::BAD_REQUEST, "path is directory");
        return;
    }

    std::string name = req->param("download_name");
    if (name.empty()) {
        name = path_util::base_name(abs_file);
    }
    name = sanitize_file_name(name);
    st = send_file_attachment(req, abs_file, name, _rate_limit_group.get(), "application/octet-stream");
    if (!st.ok()) {
        HttpChannel::send_reply(req, HttpStatus::NOT_FOUND, st.to_string_no_stack());
        return;
    }
}

LogFileArchiveAction::LogFileArchiveAction(ExecEnv* exec_env,
                                           std::shared_ptr<bufferevent_rate_limit_group> rate_limit_group)
        : HttpHandlerWithAuth(exec_env), _rate_limit_group(std::move(rate_limit_group)) {
    _base_log_dir = get_base_log_dir();
}

void LogFileArchiveAction::handle(HttpRequest* req) {
    bool expected = false;
    if (!g_log_download_in_progress.compare_exchange_strong(expected, true)) {
        HttpChannel::send_reply(req, HttpStatus::SERVICE_UNAVAILABLE, "another download is in progress");
        return;
    }
    Defer defer([&]() { g_log_download_in_progress.store(false); });

    std::string body;
    try {
        body = req->get_request_body();
    } catch (...) {
        HttpChannel::send_reply(req, HttpStatus::BAD_REQUEST, "invalid request body");
        return;
    }
    if (body.empty()) {
        HttpChannel::send_reply(req, HttpStatus::BAD_REQUEST, "paths is empty");
        return;
    }

    if (starts_with(body, "paths=")) {
        body = body.substr(strlen("paths="));
    }

    std::vector<std::string> raw_paths = split(body, "\n");
    std::vector<std::pair<std::string, std::string>> files;
    files.reserve(raw_paths.size());
    for (auto& p : raw_paths) {
        std::string_view sv = trim(p);
        if (sv.empty()) {
            continue;
        }
        std::string abs_file;
        auto st = resolve_in_base(_base_log_dir, std::string(sv), &abs_file);
        if (!st.ok()) {
            HttpChannel::send_reply(req, HttpStatus::BAD_REQUEST, st.to_string_no_stack());
            return;
        }
        bool exists = false;
        st = io::global_local_filesystem()->exists(abs_file, &exists);
        if (!st.ok() || !exists) {
            HttpChannel::send_reply(req, HttpStatus::NOT_FOUND, "file not exist");
            return;
        }
        bool is_dir = false;
        st = io::global_local_filesystem()->is_directory(abs_file, &is_dir);
        if (!st.ok() || is_dir) {
            HttpChannel::send_reply(req, HttpStatus::BAD_REQUEST, "path is directory");
            return;
        }
        std::string entry_name = path_util::base_name(abs_file);
        files.emplace_back(abs_file, entry_name);
        if (files.size() >= 64) {
            break;
        }
    }
    if (files.empty()) {
        HttpChannel::send_reply(req, HttpStatus::BAD_REQUEST, "paths is empty");
        return;
    }

    auto ts = std::chrono::duration_cast<std::chrono::milliseconds>(
                      std::chrono::system_clock::now().time_since_epoch())
                      .count();
    std::string tmp_file = fmt::format("{}/be_logs_{}.tar.gz", _base_log_dir, ts);
    FILE* fp = fopen(tmp_file.c_str(), "wb");
    if (fp == nullptr) {
        HttpChannel::send_reply(req, HttpStatus::INTERNAL_SERVER_ERROR, "failed to create temp file");
        return;
    }

    Status st;
    {
        GzipFileWriter gzip(fp, 1);
        if (!gzip.ok()) {
            fclose(fp);
            HttpChannel::send_reply(req, HttpStatus::INTERNAL_SERVER_ERROR, "gzip init failed");
            return;
        }

        unsigned char zero_block[512];
        memset(zero_block, 0, sizeof(zero_block));
        unsigned char buf[64 * 1024];

        for (const auto& it : files) {
            const std::string& abs_file = it.first;
            const std::string& entry_name = it.second;
            struct stat st_buf;
            if (stat(abs_file.c_str(), &st_buf) != 0) {
                st = Status::NotFound("failed to stat file");
                break;
            }
            TarHeader header;
            st = build_tar_header(&header, entry_name, st_buf.st_size, st_buf.st_mtime);
            if (!st.ok()) {
                break;
            }
            st = gzip.write(&header, 512);
            if (!st.ok()) {
                break;
            }

            int fd = open(abs_file.c_str(), O_RDONLY);
            if (fd < 0) {
                st = Status::NotFound("failed to open file");
                break;
            }
            int64_t remaining = st_buf.st_size;
            while (remaining > 0) {
                ssize_t r = read(fd, buf, sizeof(buf));
                if (r < 0) {
                    close(fd);
                    st = Status::IOError("failed to read file");
                    break;
                }
                if (r == 0) {
                    break;
                }
                remaining -= r;
                st = gzip.write(buf, r);
                if (!st.ok()) {
                    close(fd);
                    break;
                }
            }
            close(fd);
            if (!st.ok()) {
                break;
            }

            int64_t pad = (512 - (st_buf.st_size % 512)) % 512;
            if (pad > 0) {
                st = gzip.write(zero_block, pad);
                if (!st.ok()) {
                    break;
                }
            }
        }

        if (st.ok()) {
            st = gzip.write(zero_block, 512);
        }
        if (st.ok()) {
            st = gzip.write(zero_block, 512);
        }
        if (st.ok()) {
            st = gzip.finish();
        }
    }
    fclose(fp);

    if (!st.ok()) {
        unlink(tmp_file.c_str());
        HttpChannel::send_reply(req, HttpStatus::INTERNAL_SERVER_ERROR, st.to_string_no_stack());
        return;
    }

    std::string name = req->param("download_name");
    if (name.empty()) {
        name = "be_logs.tar.gz";
    }
    name = sanitize_file_name(name);
    if (!ends_with(to_lower(name), ".tar.gz")) {
        name = name + ".tar.gz";
    }
    st = send_file_attachment(req, tmp_file, name, _rate_limit_group.get(), "application/gzip");
    unlink(tmp_file.c_str());
    if (!st.ok()) {
        HttpChannel::send_reply(req, HttpStatus::INTERNAL_SERVER_ERROR, st.to_string_no_stack());
        return;
    }
}

} // namespace doris
