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

package org.apache.doris.binlog;

import org.apache.doris.common.Pair;
import org.apache.doris.persist.RecoverInfo;
import org.apache.doris.persist.ReplaceTableOperationLog;
import org.apache.doris.thrift.TBinlog;
import org.apache.doris.thrift.TBinlogType;
import org.apache.doris.thrift.TStatus;
import org.apache.doris.thrift.TStatusCode;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import mockit.Mock;
import mockit.MockUp;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

public class DbBinlogTest {
    private long dbId = 10000L;
    private long baseTableId = 20000L;
    private int tableNum = 5;
    private int gcTableNum = 2;
    private List<Long> tableIds;

    private int totalBinlogNum = 10;
    private int expiredBinlogNum = 3;
    private long baseNum = 30000L;

    @Before
    public void setUp() {
        // check args valid
        Assert.assertTrue(totalBinlogNum > 0);
        Assert.assertTrue(gcTableNum <= tableNum);
        Assert.assertTrue(expiredBinlogNum <= totalBinlogNum);

        // gen tableIds
        tableIds = Lists.newArrayList();
        for (int i = 0; i < tableNum; ++i) {
            tableIds.add(baseTableId + i);
        }

        new MockUp<BinlogUtils>() {
            @Mock
            public long getExpiredMs(long direct) {
                return direct;
            }
        };
    }

    @Test
    public void testTableTtlGcCommonCase() {
        // init base data
        long expiredTime = baseNum + expiredBinlogNum;
        Map<String, Long> ttlMap = Maps.newHashMap();
        for (int i = 0; i < tableNum; ++i) {
            String key = String.format("%d_%d", dbId, baseTableId + i);
            if (i <= gcTableNum) {
                ttlMap.put(key, expiredTime);
            } else {
                ttlMap.put(key, 0L);
            }
        }
        MockBinlogConfigCache binlogConfigCache = BinlogTestUtils.newMockBinlogConfigCache(ttlMap);
        binlogConfigCache.addDbBinlogConfig(dbId, false, 0L);

        // init & add binlogs
        List<TBinlog> testBinlogs = Lists.newArrayList();
        Long[] tableLastCommitInfo = new Long[tableNum];
        long maxGcTableId = baseTableId + gcTableNum;
        long expiredCommitSeq = -1;
        for (int i = 0; i < totalBinlogNum; ++i) {
            long tableId = baseTableId + (i / tableNum);
            long commitSeq = baseNum + i;
            if (tableId <= maxGcTableId) {
                expiredCommitSeq = commitSeq;
            }
            tableLastCommitInfo[i / tableNum] = commitSeq;
            TBinlog binlog = BinlogTestUtils.newBinlog(dbId, tableId, commitSeq, baseNum);
            testBinlogs.add(binlog);
        }

        // init DbBinlog
        DBBinlog dbBinlog = null;

        // insert binlogs
        for (int i = 0; i < totalBinlogNum; ++i) {
            if (dbBinlog == null) {
                dbBinlog = new DBBinlog(binlogConfigCache, testBinlogs.get(i));
            }
            dbBinlog.addBinlog(testBinlogs.get(i), null);
        }

        // trigger gc
        BinlogTombstone tombstone = dbBinlog.gc();

        // check binlog status
        for (TBinlog binlog : testBinlogs) {
            if (binlog.getTableIds().get(0) <= baseTableId + gcTableNum) {
                Assert.assertEquals(0, binlog.getTableRef());
            } else {
                Assert.assertEquals(1, binlog.getTableRef());
            }
        }

        // check dummy binlog
        List<TBinlog> allBinlogs = Lists.newArrayList();
        dbBinlog.getAllBinlogs(allBinlogs);
        for (TBinlog binlog : allBinlogs) {
            if (binlog.getType() != TBinlogType.DUMMY) {
                break;
            }
            long belong = binlog.getBelong();
            if (belong < 0) {
                Assert.assertEquals(expiredCommitSeq, binlog.getCommitSeq());
            } else if (belong <= maxGcTableId) {
                int offset = (int) (belong - baseTableId);
                Assert.assertEquals((long) tableLastCommitInfo[offset], binlog.getCommitSeq());
            } else {
                Assert.assertEquals(-1, binlog.getCommitSeq());
            }
        }

        // check tombstone
        Assert.assertFalse(tombstone.isDbBinlogTomstone());
        Assert.assertEquals(expiredCommitSeq, tombstone.getCommitSeq());
    }

    @Test
    public void testTableTtlGcBinlogMultiRefCase() {
        // init base data
        long expiredTime = baseNum + expiredBinlogNum;
        Map<String, Long> ttlMap = Maps.newHashMap();
        for (int i = 0; i < tableNum; ++i) {
            String key = String.format("%d_%d", dbId, baseTableId + i);
            if (i < tableNum - 1) {
                ttlMap.put(key, expiredTime);
            } else {
                ttlMap.put(key, 0L);
            }
        }
        MockBinlogConfigCache binlogConfigCache = BinlogTestUtils.newMockBinlogConfigCache(ttlMap);
        binlogConfigCache.addDbBinlogConfig(dbId, false, 0L);

        // init & add binlogs
        List<TBinlog> testBinlogs = Lists.newArrayList();
        for (int i = 0; i < totalBinlogNum; ++i) {
            // generate tableIds
            long tableId = baseTableId + (i / (tableNum - 1));
            long additionalTableId = (long) (Math.random() * tableNum) + baseTableId;
            while (tableId == additionalTableId) {
                additionalTableId = (long) (Math.random() * tableNum) + baseTableId;
            }
            List<Long> tableIds = Lists.newArrayList(tableId, additionalTableId);
            // init commitSeq
            long commitSeq = baseNum + i;

            TBinlog binlog = BinlogTestUtils.newBinlog(dbId, tableIds, commitSeq, baseNum);
            testBinlogs.add(binlog);
        }

        // init dbBinlog
        DBBinlog dbBinlog = null;

        // ad additional ref & add to dbBinlog
        for (int i = 0; i < totalBinlogNum; ++i) {
            TBinlog binlog = testBinlogs.get(i);
            if (dbBinlog == null) {
                dbBinlog = new DBBinlog(binlogConfigCache, binlog);
            }
            dbBinlog.addBinlog(binlog, null);
        }

        // trigger gc
        dbBinlog.gc();

        // check binlog status
        long unGcTableId = baseTableId + tableNum - 1;
        for (TBinlog binlog : testBinlogs) {
            if (binlog.getTableIds().contains(unGcTableId)) {
                Assert.assertEquals(1, binlog.getTableRef());
            } else {
                Assert.assertEquals(0, binlog.getTableRef());
            }
        }
    }

    @Test
    public void testTableCommitSeqGc() {
        // init base data
        long expiredTime = baseNum + expiredBinlogNum;
        Map<String, Long> ttlMap = Maps.newHashMap();
        MockBinlogConfigCache binlogConfigCache = BinlogTestUtils.newMockBinlogConfigCache(ttlMap);
        binlogConfigCache.addDbBinlogConfig(dbId, true, expiredTime);

        // init & add binlogs
        List<TBinlog> testBinlogs = Lists.newArrayList();
        for (int i = 0; i < totalBinlogNum; ++i) {
            // generate tableIds
            long tableId = baseTableId + (i / (tableNum - 1));
            long additionalTableId = (long) (Math.random() * tableNum) + baseTableId;
            while (tableId == additionalTableId) {
                additionalTableId = (long) (Math.random() * tableNum) + baseTableId;
            }
            List<Long> tableIds = Lists.newArrayList(tableId, additionalTableId);
            // init stamp
            long stamp = baseNum + i;

            TBinlog binlog = BinlogTestUtils.newBinlog(dbId, tableIds, stamp, stamp);
            testBinlogs.add(binlog);
        }

        // init dbBinlog
        DBBinlog dbBinlog = null;

        // ad additional ref & add to dbBinlog
        for (int i = 0; i < totalBinlogNum; ++i) {
            TBinlog binlog = testBinlogs.get(i);
            if (dbBinlog == null) {
                dbBinlog = new DBBinlog(binlogConfigCache, binlog);
            }
            dbBinlog.addBinlog(binlog, null);
        }

        // trigger gc
        dbBinlog.gc();

        // check binlog status
        for (TBinlog binlog : testBinlogs) {
            if (binlog.getTimestamp() <= expiredTime) {
                Assert.assertEquals(0, binlog.getTableRef());
            } else {
                Assert.assertTrue(binlog.getTableRef() != 0);
            }
        }
    }

    @Test
    public void testAddBinlog() throws NoSuchFieldException, IllegalAccessException {
        // set max value num
        int maxValue = 12;

        // mock up
        new MockUp<BinlogConfigCache>() {
            @Mock
            boolean isEnableDB(long dbId) {
                return true;
            }

            @Mock
            boolean isEnableTable(long dbId, long tableId) {
                return true;
            }
        };

        // reflect field
        Field allBinlogsField = DBBinlog.class.getDeclaredField("allBinlogs");
        allBinlogsField.setAccessible(true);
        Field tableBinlogMapField = DBBinlog.class.getDeclaredField("tableBinlogMap");
        tableBinlogMapField.setAccessible(true);


        for (int i = 0; i <= maxValue; ++i) {
            TBinlogType type = TBinlogType.findByValue(i);
            if (type == TBinlogType.DUMMY) {
                continue;
            }
            TBinlog binlog = BinlogTestUtils.newBinlog(dbId, baseTableId, 1, 1);
            binlog.setType(type);
            DBBinlog dbBinlog = new DBBinlog(new BinlogConfigCache(), binlog);

            dbBinlog.addBinlog(binlog, null);

            TreeSet<TBinlog> allbinlogs = (TreeSet<TBinlog>) allBinlogsField.get(dbBinlog);
            Map<Long, TableBinlog> tableBinlogMap = (Map<Long, TableBinlog>) tableBinlogMapField.get(dbBinlog);
            Assert.assertTrue(allbinlogs.contains(binlog));
            switch (type) {
                case CREATE_TABLE:
                case DROP_TABLE: {
                    Assert.assertTrue(tableBinlogMap.isEmpty());
                    break;
                }
                default: {
                    Assert.assertTrue(tableBinlogMap.containsKey(baseTableId));
                    break;
                }
            }
        }
    }

    @Test
    public void testDbAndTableGcWithDisable() {
        // init base data
        long expiredTime = baseNum + expiredBinlogNum;
        Map<String, Long> ttlMap = Maps.newHashMap();
        for (int i = 0; i < tableNum; ++i) {
            String key = String.format("%d_%d", dbId, baseTableId + i);
            ttlMap.put(key, expiredTime);
        }
        MockBinlogConfigCache binlogConfigCache = BinlogTestUtils.newMockBinlogConfigCache(ttlMap);
        // disable db binlog
        binlogConfigCache.addDbBinlogConfig(dbId, false, 0L);
        // disable some table binlog
        for (int i = 0; i <= gcTableNum; i++) {
            binlogConfigCache.addTableBinlogConfig(dbId, baseTableId + i, false, expiredTime);
        }

        // init & add binlogs
        List<TBinlog> testBinlogs = Lists.newArrayList();
        Long[] tableLastCommitInfo = new Long[tableNum];
        long maxGcTableId = baseTableId + gcTableNum;
        for (int i = 0; i < totalBinlogNum; ++i) {
            long tableId = baseTableId + (i / tableNum);
            long commitSeq = baseNum + i;
            tableLastCommitInfo[i / tableNum] = commitSeq;
            TBinlog binlog = BinlogTestUtils.newBinlog(dbId, tableId, commitSeq, baseNum);
            testBinlogs.add(binlog);
        }

        // init DbBinlog
        DBBinlog dbBinlog = null;

        // insert binlogs
        for (int i = 0; i < totalBinlogNum; ++i) {
            if (dbBinlog == null) {
                dbBinlog = new DBBinlog(binlogConfigCache, testBinlogs.get(i));
            }
            dbBinlog.addBinlog(testBinlogs.get(i), null);
        }

        // trigger gc
        BinlogTombstone tombstone = dbBinlog.gc();

        // check binlog status - all binlogs should be cleared for disabled tables
        for (TBinlog binlog : testBinlogs) {
            long tableId = binlog.getTableIds().get(0);
            if (tableId <= maxGcTableId) {
                // For disabled tables, all binlogs should be cleared
                Assert.assertEquals(0, binlog.getTableRef());
            } else {
                // For enabled tables, only expired binlogs should be cleared
                if (binlog.getTimestamp() <= expiredTime) {
                    Assert.assertEquals(0, binlog.getTableRef());
                } else {
                    Assert.assertEquals(1, binlog.getTableRef());
                }
            }
        }

        // check tombstone
        Assert.assertFalse(tombstone.isDbBinlogTomstone());
        Assert.assertEquals(baseNum + totalBinlogNum - 1, tombstone.getCommitSeq());
    }

    @Test
    public void testDbAndTableGcWithEnable() {
        // init base data
        long expiredTime = baseNum + expiredBinlogNum;
        Map<String, Long> ttlMap = Maps.newHashMap();
        for (int i = 0; i < tableNum; ++i) {
            String key = String.format("%d_%d", dbId, baseTableId + i);
            ttlMap.put(key, expiredTime);
        }
        MockBinlogConfigCache binlogConfigCache = BinlogTestUtils.newMockBinlogConfigCache(ttlMap);
        // enable db binlog
        binlogConfigCache.addDbBinlogConfig(dbId, true, expiredTime);
        // enable all table binlog
        for (int i = 0; i < tableNum; i++) {
            binlogConfigCache.addTableBinlogConfig(dbId, baseTableId + i, true, expiredTime);
        }

        // init & add binlogs
        List<TBinlog> testBinlogs = Lists.newArrayList();
        for (int i = 0; i < totalBinlogNum; ++i) {
            long tableId = baseTableId + (i / tableNum);
            long commitSeq = baseNum + i;
            TBinlog binlog = BinlogTestUtils.newBinlog(dbId, tableId, commitSeq, baseNum + i);
            testBinlogs.add(binlog);
        }

        // init DbBinlog
        DBBinlog dbBinlog = null;

        // insert binlogs
        for (int i = 0; i < totalBinlogNum; ++i) {
            if (dbBinlog == null) {
                dbBinlog = new DBBinlog(binlogConfigCache, testBinlogs.get(i));
            }
            dbBinlog.addBinlog(testBinlogs.get(i), null);
        }

        // trigger gc
        BinlogTombstone tombstone = dbBinlog.gc();

        // check binlog status - only expired binlogs should be cleared
        for (TBinlog binlog : testBinlogs) {
            if (binlog.getTimestamp() <= expiredTime) {
                Assert.assertEquals(0, binlog.getTableRef());
            } else {
                Assert.assertEquals(1, binlog.getTableRef());
            }
        }

        // check tombstone
        Assert.assertTrue(tombstone.isDbBinlogTomstone());
        Assert.assertEquals(expiredTime, tombstone.getCommitSeq());
    }

    private DBBinlog newEnabledDbBinlog(long tableId) {
        new MockUp<BinlogConfigCache>() {
            @Mock
            boolean isEnableDB(long dbId) {
                return true;
            }

            @Mock
            boolean isEnableTable(long dbId, long tableId) {
                return true;
            }
        };

        DBBinlog dbBinlog = new DBBinlog(new BinlogConfigCache(),
                BinlogTestUtils.newBinlog(dbId, tableId, 1, 1));
        // add a normal binlog so that tableBinlogMap contains the table entry
        dbBinlog.addBinlog(BinlogTestUtils.newBinlog(dbId, tableId, 2, 2), null);
        return dbBinlog;
    }

    private void addDropTableBinlog(DBBinlog dbBinlog, long tableId, long commitSeq) {
        TBinlog binlog = BinlogTestUtils.newBinlog(dbId, tableId, commitSeq, commitSeq);
        binlog.setType(TBinlogType.DROP_TABLE);
        DropTableRecord record = DropTableRecord.fromJson("{\"tableId\":" + tableId + "}");
        dbBinlog.addBinlog(binlog, record);
    }

    @Test
    public void testGetBinlogAfterDropTable() {
        DBBinlog dbBinlog = newEnabledDbBinlog(baseTableId);

        // before drop: all table level APIs work
        Assert.assertEquals(TStatusCode.OK,
                dbBinlog.getBinlog(baseTableId, -1, 10).first.getStatusCode());
        Assert.assertEquals(TStatusCode.OK,
                dbBinlog.getBinlogLag(baseTableId, -1).first.getStatusCode());
        Assert.assertEquals(TStatusCode.OK,
                dbBinlog.lockBinlog(baseTableId, "job1", -1).first.getStatusCode());

        addDropTableBinlog(dbBinlog, baseTableId, 3);

        // after drop: all table level APIs return BINLOG_NOT_FOUND_TABLE immediately
        Pair<TStatus, List<TBinlog>> binlogResult = dbBinlog.getBinlog(baseTableId, -1, 10);
        Assert.assertEquals(TStatusCode.BINLOG_NOT_FOUND_TABLE, binlogResult.first.getStatusCode());
        Pair<TStatus, BinlogLagInfo> lagResult = dbBinlog.getBinlogLag(baseTableId, -1);
        Assert.assertEquals(TStatusCode.BINLOG_NOT_FOUND_TABLE, lagResult.first.getStatusCode());
        Pair<TStatus, Long> lockResult = dbBinlog.lockBinlog(baseTableId, "job1", -1);
        Assert.assertEquals(TStatusCode.BINLOG_NOT_FOUND_TABLE, lockResult.first.getStatusCode());
        Assert.assertEquals(-1L, (long) lockResult.second);

        // db level binlog (tableId = -1) is not affected by dropped tables
        Assert.assertNotEquals(TStatusCode.BINLOG_NOT_FOUND_TABLE,
                dbBinlog.getBinlog(-1, -1, 10).first.getStatusCode());
        Assert.assertNotEquals(TStatusCode.BINLOG_NOT_FOUND_TABLE,
                dbBinlog.getBinlogLag(-1, -1).first.getStatusCode());
        Assert.assertNotEquals(TStatusCode.BINLOG_NOT_FOUND_TABLE,
                dbBinlog.lockBinlog(-1, "job2", -1).first.getStatusCode());
    }

    @Test
    public void testGetBinlogAfterRecoverTable() {
        DBBinlog dbBinlog = newEnabledDbBinlog(baseTableId);
        addDropTableBinlog(dbBinlog, baseTableId, 3);
        Assert.assertEquals(TStatusCode.BINLOG_NOT_FOUND_TABLE,
                dbBinlog.getBinlog(baseTableId, -1, 10).first.getStatusCode());

        // recover the table, the dropped signal should be cleared automatically
        TBinlog binlog = BinlogTestUtils.newBinlog(dbId, baseTableId, 4, 4);
        binlog.setType(TBinlogType.RECOVER_INFO);
        RecoverInfo recoverInfo = RecoverInfo.fromJson("{\"tableId\":" + baseTableId + "}");
        dbBinlog.addBinlog(binlog, recoverInfo);

        Assert.assertEquals(TStatusCode.OK,
                dbBinlog.getBinlog(baseTableId, -1, 10).first.getStatusCode());
        Assert.assertEquals(TStatusCode.OK,
                dbBinlog.getBinlogLag(baseTableId, -1).first.getStatusCode());
        Assert.assertEquals(TStatusCode.OK,
                dbBinlog.lockBinlog(baseTableId, "job1", -1).first.getStatusCode());
    }

    @Test
    public void testDropAndRecreateTableWithSameName() {
        DBBinlog dbBinlog = newEnabledDbBinlog(baseTableId);
        addDropTableBinlog(dbBinlog, baseTableId, 3);

        // recreate a table with the same name, the new table gets a new table id
        long newTableId = baseTableId + 100;
        dbBinlog.addBinlog(BinlogTestUtils.newBinlog(dbId, newTableId, 4, 4), null);

        // the old id keeps reporting BINLOG_NOT_FOUND_TABLE, the new id works
        Assert.assertEquals(TStatusCode.BINLOG_NOT_FOUND_TABLE,
                dbBinlog.getBinlog(baseTableId, -1, 10).first.getStatusCode());
        Assert.assertEquals(TStatusCode.OK,
                dbBinlog.getBinlog(newTableId, -1, 10).first.getStatusCode());
    }

    @Test
    public void testReplaceTableDroppedSignal() {
        DBBinlog dbBinlog = newEnabledDbBinlog(baseTableId);

        // replace table with swap: neither table is treated as dropped
        TBinlog swapBinlog = BinlogTestUtils.newBinlog(dbId, baseTableId, 3, 3);
        swapBinlog.setType(TBinlogType.REPLACE_TABLE);
        ReplaceTableOperationLog swapLog = ReplaceTableOperationLog.fromJson(
                "{\"origTblId\":" + baseTableId + ",\"swapTable\":true}");
        dbBinlog.addBinlog(swapBinlog, swapLog);
        Assert.assertEquals(TStatusCode.OK,
                dbBinlog.getBinlog(baseTableId, -1, 10).first.getStatusCode());

        // replace table without swap: the orig table is treated as dropped
        TBinlog replaceBinlog = BinlogTestUtils.newBinlog(dbId, baseTableId, 4, 4);
        replaceBinlog.setType(TBinlogType.REPLACE_TABLE);
        ReplaceTableOperationLog replaceLog = ReplaceTableOperationLog.fromJson(
                "{\"origTblId\":" + baseTableId + ",\"swapTable\":false}");
        dbBinlog.addBinlog(replaceBinlog, replaceLog);
        Assert.assertEquals(TStatusCode.BINLOG_NOT_FOUND_TABLE,
                dbBinlog.getBinlog(baseTableId, -1, 10).first.getStatusCode());
    }

    @Test
    public void testDroppedTableSignalClearedByGc() {
        // db binlog enabled, all binlogs with timestamp <= expiredTime will be gc'd
        long expiredTime = 10;
        Map<String, Long> ttlMap = Maps.newHashMap();
        MockBinlogConfigCache binlogConfigCache = BinlogTestUtils.newMockBinlogConfigCache(ttlMap);
        binlogConfigCache.addDbBinlogConfig(dbId, true, expiredTime);
        binlogConfigCache.addTableBinlogConfig(dbId, baseTableId, true, expiredTime);

        DBBinlog dbBinlog = new DBBinlog(binlogConfigCache,
                BinlogTestUtils.newBinlog(dbId, baseTableId, 1, 1));
        dbBinlog.addBinlog(BinlogTestUtils.newBinlog(dbId, baseTableId, 1, 1), null);
        addDropTableBinlog(dbBinlog, baseTableId, 2);
        // one more binlog after the drop, so that gcDroppedResources can expire the drop record
        dbBinlog.addBinlog(BinlogTestUtils.newBinlog(dbId, baseTableId, 3, 3), null);

        Assert.assertEquals(TStatusCode.BINLOG_NOT_FOUND_TABLE,
                dbBinlog.getBinlog(baseTableId, -1, 10).first.getStatusCode());

        // the drop record (commitSeq 2) is expired by gc, the signal is cleared
        dbBinlog.gc();
        Assert.assertTrue(dbBinlog.getDroppedTables().isEmpty());
        Assert.assertNotEquals(TStatusCode.BINLOG_NOT_FOUND_TABLE,
                dbBinlog.getBinlog(baseTableId, -1, 10).first.getStatusCode());
    }
}
