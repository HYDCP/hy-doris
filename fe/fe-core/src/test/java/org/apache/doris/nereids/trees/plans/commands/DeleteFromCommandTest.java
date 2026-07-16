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

package org.apache.doris.nereids.trees.plans.commands;

import org.apache.doris.nereids.exceptions.AnalysisException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class DeleteFromCommandTest {

    @Test
    public void testBuildDeleteFallbackException() {
        // Verify that the merged exception exposes both failure causes and keeps the fallback cause.
        Exception initialException = new Exception("Where clause only supports compound predicate");
        Exception fallbackException = new Exception("disk path exceeds limit");

        AnalysisException mergedException =
                DeleteFromCommand.buildDeleteFallbackException(initialException, fallbackException);

        Assertions.assertEquals(
                "Delete failed with 2 causes. Primary cause: disk path exceeds limit."
                        + " Initial predicate-check failure: Where clause only supports compound predicate.",
                mergedException.getMessage());
        Assertions.assertSame(fallbackException, mergedException.getCause());
        Assertions.assertEquals(1, mergedException.getSuppressed().length);
        Assertions.assertSame(initialException, mergedException.getSuppressed()[0]);
    }

    @Test
    public void testBuildDeleteFallbackExceptionUsesThrowableToStringWhenMessageIsNull() {
        // Verify that null messages still produce a usable merged error.
        Exception initialException = new Exception((String) null);
        Exception fallbackException = new Exception((String) null);

        AnalysisException mergedException =
                DeleteFromCommand.buildDeleteFallbackException(initialException, fallbackException);

        Assertions.assertTrue(mergedException.getMessage().contains("Primary cause: java.lang.Exception"));
        Assertions.assertTrue(
                mergedException.getMessage().contains("Initial predicate-check failure: java.lang.Exception"));
    }
}
