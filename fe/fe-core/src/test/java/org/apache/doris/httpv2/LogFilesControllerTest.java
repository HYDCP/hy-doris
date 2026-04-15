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

package org.apache.doris.httpv2;

import org.apache.doris.common.Config;
import org.apache.doris.httpv2.controller.LogFilesController;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.lang.reflect.Method;
import java.nio.file.Path;

public class LogFilesControllerTest {

    @TempDir
    public File tempDir;

    @Test
    public void testResolveInBase() throws Exception {
        Config.sys_log_dir = tempDir.getAbsolutePath();
        Path base = tempDir.toPath().normalize();
        Method method = LogFilesController.class.getDeclaredMethod("resolveInBase", Path.class, String.class);
        method.setAccessible(true);

        Object resolved = method.invoke(null, base, "/a.log");
        Assertions.assertEquals(base.resolve("a.log").normalize(), resolved);

        Assertions.assertThrows(Exception.class, () -> method.invoke(null, base, "/../etc/passwd"));
        Assertions.assertThrows(Exception.class, () -> method.invoke(null, base, "../etc/passwd"));
    }
}

