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

package org.apache.doris.common;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Map;

public class ConfigBaseTest {
    private static final String HOST_SET_CAPACITY = "tablet_health_check_host_set_initial_capacity";

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    private int savedCapacity;
    private Class<? extends ConfigBase> savedConfClass;
    private Map<String, Field> savedConfFields;
    private Field confFileField;
    private Field customConfFileField;
    private Object savedConfFile;
    private Object savedCustomConfFile;

    @Before
    public void setUp() throws Exception {
        savedCapacity = Config.tablet_health_check_host_set_initial_capacity;
        savedConfClass = ConfigBase.confClass;
        savedConfFields = ConfigBase.confFields;
        confFileField = ConfigBase.class.getDeclaredField("confFile");
        confFileField.setAccessible(true);
        savedConfFile = confFileField.get(null);
        customConfFileField = ConfigBase.class.getDeclaredField("customConfFile");
        customConfFileField.setAccessible(true);
        savedCustomConfFile = customConfFileField.get(null);
        new Config().init(temporaryFolder.newFile("empty.conf").getAbsolutePath());
    }

    @After
    public void tearDown() throws Exception {
        Config.tablet_health_check_host_set_initial_capacity = savedCapacity;
        ConfigBase.confClass = savedConfClass;
        ConfigBase.confFields = savedConfFields;
        confFileField.set(null, savedConfFile);
        customConfFileField.set(null, savedCustomConfFile);
    }

    @Test
    public void testHostSetCapacityDefaultAndMetadata() throws Exception {
        Assert.assertEquals(4, Config.tablet_health_check_host_set_initial_capacity);
        Field field = Config.class.getField(HOST_SET_CAPACITY);
        ConfigBase.ConfField annotation = field.getAnnotation(ConfigBase.ConfField.class);
        Assert.assertTrue(annotation.mutable());
        Assert.assertTrue(annotation.masterOnly());
        Assert.assertEquals(1, annotation.minValue());
        Assert.assertEquals(16, annotation.maxValue());
        Assert.assertTrue(Modifier.isVolatile(field.getModifiers()));
    }

    @Test
    public void testHostSetCapacityLoadedFromConfig() throws Exception {
        for (int capacity : new int[] {1, 4, 6, 8, 16}) {
            new Config().init(writeCapacity("startup-" + capacity + ".conf", Integer.toString(capacity)));
            Assert.assertEquals(capacity, Config.tablet_health_check_host_set_initial_capacity);
        }
    }

    @Test
    public void testHostSetCapacityLoadedFromCustomConfig() throws Exception {
        new Config().initCustom(writeCapacity("fe_custom.conf", "16"));
        Assert.assertEquals(16, Config.tablet_health_check_host_set_initial_capacity);
    }

    @Test
    public void testHostSetCapacityRejectsInvalidStartupConfig() throws Exception {
        Config.tablet_health_check_host_set_initial_capacity = 8;
        for (String value : new String[] {"0", "-1", "17", "2147483647", "invalid", "2147483648"}) {
            String path = writeCapacity("startup-invalid-" + value + ".conf", value);
            Assert.assertThrows(IllegalArgumentException.class, () -> new Config().init(path));
            Assert.assertEquals(8, Config.tablet_health_check_host_set_initial_capacity);
        }
        for (String value : new String[] {"0", "17"}) {
            String path = writeCapacity("custom-invalid-" + value + ".conf", value);
            Assert.assertThrows(IllegalArgumentException.class, () -> new Config().initCustom(path));
            Assert.assertEquals(8, Config.tablet_health_check_host_set_initial_capacity);
        }
    }

    @Test
    public void testHostSetCapacityDynamicUpdates() throws Exception {
        for (String value : new String[] {"1", "4", "6", "8", "16", " 6 "}) {
            ConfigBase.setMutableConfig(HOST_SET_CAPACITY, value);
            Assert.assertEquals(Integer.parseInt(value.trim()), Config.tablet_health_check_host_set_initial_capacity);
        }
    }

    @Test
    public void testHostSetCapacityRejectsInvalidDynamicUpdates() {
        Config.tablet_health_check_host_set_initial_capacity = 8;
        for (String value : new String[] {"0", "-1", "17", "2147483647", "invalid", "2147483648"}) {
            Assert.assertThrows(ConfigException.class, () -> ConfigBase.setMutableConfig(HOST_SET_CAPACITY, value));
            Assert.assertEquals(8, Config.tablet_health_check_host_set_initial_capacity);
        }
    }

    @Test
    public void testExistingIntConfigKeepsUnboundedRange() throws Exception {
        int previous = Config.max_backup_tablets_per_job;
        try {
            ConfigBase.setMutableConfig("max_backup_tablets_per_job", "-1");
            Assert.assertEquals(-1, Config.max_backup_tablets_per_job);
            ConfigBase.setMutableConfig("max_backup_tablets_per_job", Integer.toString(Integer.MAX_VALUE));
            Assert.assertEquals(Integer.MAX_VALUE, Config.max_backup_tablets_per_job);
        } finally {
            Config.max_backup_tablets_per_job = previous;
        }
    }

    private String writeCapacity(String name, String value) throws Exception {
        File file = temporaryFolder.newFile(name);
        Files.write(file.toPath(), (HOST_SET_CAPACITY + "=" + value + "\n").getBytes(StandardCharsets.UTF_8));
        return file.getAbsolutePath();
    }
}
