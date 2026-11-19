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

package org.apache.doris.task;

import org.apache.doris.catalog.Env;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.GenericPool;
import org.apache.doris.metric.MetricRepo;
import org.apache.doris.system.Backend;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.thrift.BackendService;
import org.apache.doris.thrift.TAgentResult;
import org.apache.doris.thrift.TAgentTaskRequest;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TStatus;
import org.apache.doris.thrift.TStatusCode;
import org.apache.doris.thrift.TTaskType;

import com.google.common.collect.Lists;
import mockit.Delegate;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class AgentBoundedBatchTaskTest {
    private static final long BACKEND_ID = 10001;
    private static final long JOB_ID = 20001;
    private static final long DB_ID = 30001;

    @Mocked
    private BackendService.Client client;

    private List<AgentTask> tasks;

    @Before
    public void setUp() {
        FeConstants.runningUnitTest = true;
        MetricRepo.init();

        SystemInfoService systemInfo = new SystemInfoService();
        Backend backend = new Backend(BACKEND_ID, "127.0.0.1", 9050);
        backend.setAlive(true);
        systemInfo.addBackend(backend);
        new MockUp<Env>() {
            @Mock
            public SystemInfoService getCurrentSystemInfo() {
                return systemInfo;
            }
        };

        new MockUp<GenericPool<BackendService.Client>>() {
            @Mock
            public BackendService.Client borrowObject(TNetworkAddress address) {
                return client;
            }

            @Mock
            public void returnObject(TNetworkAddress address, BackendService.Client object) {
            }

            @Mock
            public void invalidateObject(TNetworkAddress address, BackendService.Client object) {
            }
        };

        tasks = Lists.newArrayList();
        for (long signature = 1; signature <= 3; signature++) {
            tasks.add(new DownloadTask(null, BACKEND_ID, signature, JOB_ID, DB_ID, Lists.newArrayList()));
        }
    }

    @After
    public void tearDown() {
        for (AgentTask task : tasks) {
            AgentTaskQueue.removeTaskOfType(TTaskType.DOWNLOAD, task.getSignature());
        }
    }

    @Test
    public void testSubmitAllTasks() throws Exception {
        AtomicInteger submitted = new AtomicInteger();
        new Expectations() {
            {
                client.submitTasks((List<TAgentTaskRequest>) any);
                minTimes = 0;
                result = new Delegate() {
                    TAgentResult submitTasks(List<TAgentTaskRequest> requests) {
                        submitted.addAndGet(requests.size());
                        return new TAgentResult(new TStatus(TStatusCode.OK));
                    }
                };
            }
        };

        AgentBoundedBatchTask batchTask = new AgentBoundedBatchTask(10, 3);
        tasks.forEach(batchTask::addTask);
        Assert.assertFalse(batchTask.isStopped());
        batchTask.run();

        Assert.assertTrue(batchTask.isStopped());
        Assert.assertEquals(3, submitted.get());
        for (AgentTask task : tasks) {
            Assert.assertTrue(AgentTaskQueue.contains(task));
        }
    }

    @Test
    public void testNoTaskRegisteredAfterCancel() throws Exception {
        CountDownLatch firstSubmitted = new CountDownLatch(1);
        CountDownLatch jobCancelled = new CountDownLatch(1);
        AtomicInteger submitted = new AtomicInteger();
        new Expectations() {
            {
                client.submitTasks((List<TAgentTaskRequest>) any);
                minTimes = 0;
                result = new Delegate() {
                    TAgentResult submitTasks(List<TAgentTaskRequest> requests) throws InterruptedException {
                        submitted.addAndGet(requests.size());
                        firstSubmitted.countDown();
                        Assert.assertTrue(jobCancelled.await(10, TimeUnit.SECONDS));
                        return new TAgentResult(new TStatus(TStatusCode.OK));
                    }
                };
            }
        };

        // the concurrency is 1, so the 2nd task can only be registered after the 1st task is finished
        AgentBoundedBatchTask batchTask = new AgentBoundedBatchTask(10, 1);
        tasks.forEach(batchTask::addTask);
        Thread sender = new Thread(batchTask);
        sender.start();
        Assert.assertTrue(firstSubmitted.await(10, TimeUnit.SECONDS));

        // The job is cancelled the same way as RestoreJob.cancelInternal(): cancel the batch task, then remove
        // the registered tasks from AgentTaskQueue. The 1st task is finished, which frees the concurrency slot.
        batchTask.cancel();
        for (AgentTask task : tasks) {
            AgentTaskQueue.removeTaskOfType(TTaskType.DOWNLOAD, task.getSignature());
        }
        tasks.get(0).setFinished(true);
        jobCancelled.countDown();

        sender.join(TimeUnit.SECONDS.toMillis(10));
        Assert.assertFalse(sender.isAlive());
        Assert.assertTrue(batchTask.isStopped());
        Assert.assertEquals(1, submitted.get());
        for (AgentTask task : tasks) {
            Assert.assertFalse(AgentTaskQueue.contains(task));
        }
    }
}
