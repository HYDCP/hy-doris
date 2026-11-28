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

package org.apache.doris.backup;

import org.apache.doris.common.Config;
import org.apache.doris.common.ConfigBase.ConfField;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * The defaults of the configs of the local data reuse of a restore on branch-3.1 (section 6.2 of the plan): whatever
 * changes the original path of a backup or restore is off, so is the reuse. Read the declared defaults, the tests which
 * need a switch set it themselves.
 */
public class RestoreReuseDefaultsTest {

    @Test
    public void testSwitchesWhichChangeTheOriginalPathAreOff() {
        Assertions.assertFalse(Config.enable_backup_manifest);
        Assertions.assertFalse(Config.backup_restore_merge_idle_ticks);
        Assertions.assertFalse(Config.backup_restore_run_on_task_finish);
    }

    @Test
    public void testReuseIsOff() {
        Assertions.assertFalse(Config.enable_restore_partition_reuse);
        Assertions.assertFalse(Config.enable_restore_incremental_append);
        Assertions.assertFalse(Config.enable_restore_atomic_reuse);
        Assertions.assertFalse(Config.restore_reuse_mow);
        Assertions.assertFalse(Config.restore_reuse_allow_unverified_keep);
        Assertions.assertFalse(Config.restore_reuse_allow_hidden_state_models);
        // the state token: no new metadata is written, and no partition is kept by it
        Assertions.assertFalse(Config.restore_state_token_track);
        Assertions.assertFalse(Config.restore_reuse_trust_state_token);
    }

    @Test
    public void testSafeDefaultsOfTheReuse() {
        Assertions.assertEquals("full", Config.restore_reuse_default_check_level);
        Assertions.assertTrue(Config.restore_reuse_require_capability);
        // only in effect once the atomic reuse is on
        Assertions.assertTrue(Config.restore_reuse_kept_load_at_snapshot);
        Assertions.assertEquals(0L, Config.restore_reuse_min_partition_bytes);
        Assertions.assertEquals(0.1, Config.restore_reuse_sample_ratio, 0.0);
        Assertions.assertTrue(Config.restore_reuse_force_full_for_relation);
        Assertions.assertEquals(3600, Config.restore_digest_timeout_s);
        Assertions.assertEquals(10, Config.restore_local_source_task_max_retry);
        Assertions.assertEquals(0L, Config.backup_logical_digest_max_tablets);
        Assertions.assertEquals(0L, Config.restore_reuse_max_tablets);
    }

    // branch-3.1 keeps a description of each config in Chinese and in English ({"中文", "English"}): the doc
    // generator reads the one of a language by its index
    @Test
    public void testTheConfigsOfTheReuseHaveTheTwoDescriptions() throws Exception {
        String[] names = {"enable_backup_manifest", "backup_restore_merge_idle_ticks",
                "backup_restore_run_on_task_finish", "enable_restore_partition_reuse",
                "restore_reuse_default_check_level", "restore_reuse_allow_unverified_keep",
                "restore_reuse_allow_hidden_state_models", "restore_reuse_mow", "restore_local_source_task_max_retry",
                "restore_reuse_sample_ratio", "restore_reuse_force_full_for_relation",
                "restore_reuse_min_partition_bytes", "restore_digest_timeout_s", "enable_restore_incremental_append",
                "enable_restore_atomic_reuse", "restore_reuse_kept_load_at_snapshot",
                "restore_reuse_require_capability", "backup_logical_digest_max_tablets", "restore_reuse_max_tablets",
                "restore_state_token_track", "restore_reuse_trust_state_token"};
        for (String name : names) {
            String[] description = Config.class.getField(name).getAnnotation(ConfField.class).description();
            Assertions.assertEquals(2, description.length, name);
            Assertions.assertTrue(description[0].codePoints().anyMatch(c -> c >= 0x4e00 && c <= 0x9fff),
                    name + ": the first one is in Chinese");
            Assertions.assertTrue(description[1].codePoints().allMatch(c -> c < 0x2e80),
                    name + ": the second one is in English");
        }
    }
}
