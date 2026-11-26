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

package org.apache.doris.resource.workloadschedpolicy;

import org.apache.doris.common.UserException;

/**
 * Workload condition for matching query remote scan bytes collected on BE.
 */
public class WorkloadConditionBeScanBytesFromRemoteStorage implements WorkloadCondition {

    @Override
    public boolean eval(String strValue) {
        // Currently this metric is evaluated only on BE, so FE-side matching always returns false.
        return false;
    }

    public static WorkloadConditionBeScanBytesFromRemoteStorage createWorkloadCondition(
            String value) throws UserException {
        try {
            if (Long.parseLong(value) < 0) {
                throw new NumberFormatException();
            }
        } catch (NumberFormatException e) {
            throw new UserException("invalid remote scan bytes value: " + value + ", it requires >= 0");
        }
        return new WorkloadConditionBeScanBytesFromRemoteStorage();
    }

    @Override
    public WorkloadMetricType getMetricType() {
        return WorkloadMetricType.BE_SCAN_BYTES_FROM_REMOTE_STORAGE;
    }
}
