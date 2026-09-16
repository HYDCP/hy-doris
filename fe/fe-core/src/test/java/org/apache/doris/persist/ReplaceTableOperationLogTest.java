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

package org.apache.doris.persist;

import org.apache.doris.catalog.BinlogConfig;

import org.junit.Assert;
import org.junit.Test;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;

public class ReplaceTableOperationLogTest {
    @Test
    public void testSerialization() throws Exception {
        File file = new File("./ReplaceTableOperationLogTest");
        file.createNewFile();
        DataOutputStream dos = new DataOutputStream(new FileOutputStream(file));

        BinlogConfig binlogConfig = new BinlogConfig(true, 3600L, 1024L, 10L);
        ReplaceTableOperationLog log = new ReplaceTableOperationLog(1L, 2L, "origin", 3L, "replacement",
                false, binlogConfig);
        log.write(dos);

        dos.flush();
        dos.close();

        DataInputStream dis = new DataInputStream(new FileInputStream(file));
        ReplaceTableOperationLog restored = ReplaceTableOperationLog.read(dis);

        Assert.assertEquals(log.getDbId(), restored.getDbId());
        Assert.assertEquals(log.getNewTblId(), restored.getNewTblId());
        Assert.assertEquals(log.getOrigTblId(), restored.getOrigTblId());
        Assert.assertEquals(log.isSwapTable(), restored.isSwapTable());
        Assert.assertEquals(log.getOrigTblName(), restored.getOrigTblName());
        Assert.assertEquals(log.getNewTblName(), restored.getNewTblName());
        Assert.assertEquals(binlogConfig, restored.getOrigTblBinlogConfig());

        dis.close();
        file.delete();
    }

    @Test
    public void testOriginTableBinlogConfigIsPersistedInJson() {
        BinlogConfig binlogConfig = new BinlogConfig(true, 3600L, 1024L, 10L);
        ReplaceTableOperationLog log = new ReplaceTableOperationLog(1L, 2L, "origin", 3L, "replacement",
                false, binlogConfig);

        ReplaceTableOperationLog restored = ReplaceTableOperationLog.fromJson(log.toJson());

        Assert.assertEquals(binlogConfig, restored.getOrigTblBinlogConfig());
    }
}
