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

package org.apache.doris.common.profile;

import org.apache.doris.common.Config;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.common.util.DocGenerator;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.lang.reflect.Field;

public class PlanningDiagnosticsConfigTest {
    @ParameterizedTest
    @CsvSource({
            "nereids_planning_log_threshold_ms, 慢速, slow Nereids planning passes",
            "nereids_planning_log_interval_ms, 最小间隔, Minimum interval",
            "nereids_planning_operation_log_threshold_ms, 锁等待, lock wait/hold"})
    public void testConfigDocumentationInBothLanguages(String name, String chinese, String english) throws Exception {
        Field field = Config.class.getField(name);
        DocGenerator generator = new DocGenerator("", "", "", "", "", "", "", "");
        Class<?> language = Class.forName(DocGenerator.class.getName() + "$Lang");
        for (Object value : language.getEnumConstants()) {
            String documentation = Deencapsulation.invoke(generator, "genSingleConfFieldDoc", field, value);
            Assertions.assertTrue(documentation.contains("### `" + name + "`"));
            if (value.toString().equals("CN")) {
                Assertions.assertTrue(documentation.contains(chinese));
                Assertions.assertTrue(documentation.contains("毫秒"));
            } else {
                Assertions.assertTrue(documentation.contains(english));
                Assertions.assertTrue(documentation.contains("milliseconds"));
            }
        }
    }
}
