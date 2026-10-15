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

package org.apache.doris.clone;

import org.apache.doris.backup.AbstractJob;
import org.apache.doris.backup.BackupHandler;
import org.apache.doris.backup.BackupJob;
import org.apache.doris.backup.RestoreJob;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

// The check of the tablet scheduler for the staging partitions which a restore job loads from a local source.
public class TabletSchedulerLocalSourceTest {
    private static final long DB_ID = 1;
    private static final long PROTECTED_PARTITION = 100;
    private static final long OTHER_PARTITION = 101;

    private RestoreJob runningJob(boolean done) {
        RestoreJob job = Mockito.mock(RestoreJob.class);
        Mockito.when(job.isDone()).thenReturn(done);
        Mockito.when(job.isLocalSourceStagingPartition(PROTECTED_PARTITION)).thenReturn(true);
        return job;
    }

    @Test
    public void testHitIsRejectedAndOthersAreNot() {
        BackupHandler handler = Mockito.mock(BackupHandler.class);
        Mockito.when(handler.hasJobsWithLocalSource()).thenReturn(true);
        RestoreJob running = runningJob(false);
        Mockito.when(handler.getJob(DB_ID)).thenReturn(running);
        Assertions.assertTrue(TabletScheduler.isLocalSourceStagingPartition(handler, DB_ID, PROTECTED_PARTITION));
        // another partition of the same table
        Assertions.assertFalse(TabletScheduler.isLocalSourceStagingPartition(handler, DB_ID, OTHER_PARTITION));
    }

    @Test
    public void testNothingIsLookedUpWhenNoJobHasLocalSourcePartitions() {
        BackupHandler handler = Mockito.mock(BackupHandler.class);
        Mockito.when(handler.hasJobsWithLocalSource()).thenReturn(false);
        Assertions.assertFalse(TabletScheduler.isLocalSourceStagingPartition(handler, DB_ID, PROTECTED_PARTITION));
        // not even the job of the database is asked for
        Mockito.verify(handler, Mockito.never()).getJob(Mockito.anyLong());
        // no handler (not started)
        Assertions.assertFalse(TabletScheduler.isLocalSourceStagingPartition(null, DB_ID, PROTECTED_PARTITION));
    }

    @Test
    public void testDoneJobAndOtherJobsAreNotAHit() {
        BackupHandler handler = Mockito.mock(BackupHandler.class);
        Mockito.when(handler.hasJobsWithLocalSource()).thenReturn(true);
        // the job is done (the set may be stale for a moment)
        RestoreJob done = runningJob(true);
        Mockito.when(handler.getJob(DB_ID)).thenReturn(done);
        Assertions.assertFalse(TabletScheduler.isLocalSourceStagingPartition(handler, DB_ID, PROTECTED_PARTITION));
        // no job in the database, or a backup job
        Mockito.when(handler.getJob(DB_ID)).thenReturn(null);
        Assertions.assertFalse(TabletScheduler.isLocalSourceStagingPartition(handler, DB_ID, PROTECTED_PARTITION));
        AbstractJob backup = Mockito.mock(BackupJob.class);
        Mockito.when(backup.isDone()).thenReturn(false);
        Mockito.when(handler.getJob(DB_ID)).thenReturn(backup);
        Assertions.assertFalse(TabletScheduler.isLocalSourceStagingPartition(handler, DB_ID, PROTECTED_PARTITION));
        // a job of another database is not asked about this one
        RestoreJob other = runningJob(false);
        Mockito.when(handler.getJob(DB_ID + 1)).thenReturn(other);
        Assertions.assertFalse(TabletScheduler.isLocalSourceStagingPartition(handler, DB_ID, PROTECTED_PARTITION));
    }
}
