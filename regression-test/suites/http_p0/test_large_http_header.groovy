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

suite("test_large_http_header", "p0") {
    def largeHeaderValue = "x" * (100 * 1024)

    httpTest {
        endpoint context.config.feHttpAddress
        uri "/api/health"
        op "get"
        header "X-Large-Header", largeHeaderValue
        check { code, body ->
            assertEquals(200, code)
        }
    }

    // A header larger than jetty_server_max_http_header_size (default 1 MiB)
    // must be rejected before reaching the servlet, with 431 Request Header
    // Fields Too Large.
    def oversizedHeaderValue = "x" * (1024 * 1024 + 64 * 1024)
    httpTest {
        endpoint context.config.feHttpAddress
        uri "/api/health"
        op "get"
        header "X-Large-Header", oversizedHeaderValue
        check { code, body ->
            assertEquals(431, code)
        }
    }
}
