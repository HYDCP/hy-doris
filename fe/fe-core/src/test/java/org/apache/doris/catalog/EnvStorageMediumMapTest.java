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

package org.apache.doris.catalog;

import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.thrift.TStorageMedium;

import mockit.Expectations;
import mockit.Injectable;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.Map;

public class EnvStorageMediumMapTest {
    @Injectable
    private InternalCatalog catalog;
    @Injectable
    private Database db;
    @Injectable
    private OlapTable table;
    @Injectable
    private Partition partition;

    private Env preparePartition(DataProperty dataProperty) {
        Env env = Env.getCurrentEnv();
        PartitionInfo partitionInfo = new PartitionInfo(PartitionType.UNPARTITIONED);
        if (dataProperty != null) {
            partitionInfo.setDataProperty(30001L, dataProperty);
        }
        new Expectations(env) {
            {
                env.getInternalCatalog();
                result = catalog;
                catalog.getDbIds();
                result = Collections.singletonList(10001L);
                catalog.getDbNullable(10001L);
                result = db;
                db.getTables();
                result = Collections.singletonList(table);
                table.isManagedTable();
                result = true;
                table.getId();
                result = 20001L;
                table.getPartitionInfo();
                result = partitionInfo;
                table.getAllPartitions();
                result = Collections.singletonList(partition);
                partition.getId();
                result = 30001L;
                partition.getName();
                result = "p_missing";
                minTimes = 0;
            }
        };
        return env;
    }

    @Test
    public void testHddPartition() {
        Env env = preparePartition(new DataProperty(TStorageMedium.HDD));
        Map<Long, TStorageMedium> result = env.getPartitionIdToStorageMediumMap();
        Assert.assertEquals(Collections.singletonMap(30001L, TStorageMedium.HDD), result);
    }

    @Test
    public void testSsdPartitionBeforeCooldown() {
        Env env = preparePartition(new DataProperty(TStorageMedium.SSD, Long.MAX_VALUE, ""));
        Map<Long, TStorageMedium> result = env.getPartitionIdToStorageMediumMap();
        Assert.assertEquals(Collections.singletonMap(30001L, TStorageMedium.SSD), result);
    }

    @Test
    public void testMissingDataPropertyMessage() {
        Env env = preparePartition(null);
        NullPointerException exception = Assert.assertThrows(NullPointerException.class,
                env::getPartitionIdToStorageMediumMap);
        Assert.assertEquals("p_missing, pId:30001, db: 10001, tbl: 20001", exception.getMessage());
    }
}
