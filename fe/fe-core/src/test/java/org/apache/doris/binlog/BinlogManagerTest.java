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

import org.apache.doris.catalog.BinlogConfig;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.EnvFactory;
import org.apache.doris.common.Config;
import org.apache.doris.common.Pair;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.persist.BinlogGcInfo;
import org.apache.doris.thrift.TBinlog;
import org.apache.doris.thrift.TBinlogType;
import org.apache.doris.thrift.TStatus;
import org.apache.doris.thrift.TStatusCode;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import mockit.Mock;
import mockit.MockUp;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TMemoryBuffer;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class BinlogManagerTest {
    private Map<Long, List<Long>> frameWork;

    private int dbNum = 2;
    private int tableNumPerDb = 3;

    private long dbBaseId = 10000;
    private long tableBaseId = 100;
    private long baseNum = 10000;
    private long timeNow = baseNum;
    private long ttl = 3;

    private boolean enableDbBinlog = false;
    private boolean enableTableBinlog = true;

    @BeforeClass
    public static void beforeClass() {
        Config.enable_feature_binlog = true;
    }

    @Before
    public void setUp() {
        Assert.assertTrue(tableNumPerDb < 100);
        enableDbBinlog = false;
        enableTableBinlog = true;
        frameWork = Maps.newHashMap();
        for (int dbOff = 1; dbOff <= dbNum; ++dbOff) {
            long dbId = dbOff * dbBaseId;
            List<Long> tableIds = Lists.newArrayList();
            for (int tblOff = 1; tblOff <= tableNumPerDb; ++tblOff) {
                tableIds.add(tableBaseId * tblOff + dbId);
            }
            frameWork.put(dbId, tableIds);
        }

        new MockUp<BinlogConfigCache>() {
            @Mock
            public BinlogConfig getDBBinlogConfig(long dbId) {
                return new BinlogConfig();
            }

            @Mock
            public BinlogConfig getTableBinlogConfig(long dbId, long tableId) {
                return new BinlogConfig();
            }

            @Mock
            public boolean isEnableTable(long dbId, long tableId) {
                return enableTableBinlog;
            }

            @Mock
            public boolean isEnableDB(long dbId) {
                return enableDbBinlog;
            }
        };

        new MockUp<BinlogConfig>() {
            @Mock
            public long getTtlSeconds() {
                return ttl;
            }

            @Mock
            public boolean isEnable() {
                return enableDbBinlog;
            }
        };

        new MockUp<Env>() {
            @Mock
            public InternalCatalog getCurrentInternalCatalog() {
                return EnvFactory.getInstance().createInternalCatalog();
            }
        };

        new MockUp<InternalCatalog>() {
            @Mock
            public Database getDbNullable(long dbId) {
                return new Database();
            }
        };

        new MockUp<Database>() {
            @Mock
            public BinlogConfig getBinlogConfig() {
                return new BinlogConfig();
            }
        };
    }

    @Test
    public void testAddSingleTableBinlogWhenOnlyDbBinlogEnabled() throws NoSuchMethodException,
            InvocationTargetException, IllegalAccessException, NoSuchFieldException {
        enableDbBinlog = true;
        enableTableBinlog = false;

        Method addBinlog = BinlogManager.class.getDeclaredMethod("addBinlog", long.class, List.class,
                long.class, long.class, TBinlogType.class, String.class, boolean.class, Object.class);
        addBinlog.setAccessible(true);
        Field dbBinlogMapField = BinlogManager.class.getDeclaredField("dbBinlogMap");
        dbBinlogMapField.setAccessible(true);

        BinlogManager manager = new BinlogManager();
        long commitSeq = 1L;
        addBinlog.invoke(manager, dbBaseId, Lists.newArrayList(tableBaseId), commitSeq, timeNow,
                TBinlogType.UPSERT, "", false, null);

        Map<Long, DBBinlog> dbBinlogMap = (Map<Long, DBBinlog>) dbBinlogMapField.get(manager);
        Assert.assertTrue(dbBinlogMap.containsKey(dbBaseId));
        List<TBinlog> binlogs = Lists.newArrayList();
        dbBinlogMap.get(dbBaseId).getAllBinlogs(binlogs);
        boolean found = false;
        for (TBinlog binlog : binlogs) {
            if (binlog.getType() == TBinlogType.UPSERT && binlog.getCommitSeq() == commitSeq) {
                found = true;
                break;
            }
        }
        Assert.assertTrue(found);
    }

    @Test
    public void testGetBinlog()
            throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        // reflect BinlogManager
        Method addBinlog = BinlogManager.class.getDeclaredMethod("addBinlog", TBinlog.class, Object.class);
        addBinlog.setAccessible(true);

        // init binlog manager & addBinlog
        BinlogManager manager = new BinlogManager();

        // insert table binlogs
        int binlogNum = 10;
        for (int i = 1; i <= binlogNum; ++i) {
            TBinlog binlog = BinlogTestUtils.newBinlog(dbBaseId, tableBaseId, i, i);
            if (i % 2 == 0) {
                binlog.setType(TBinlogType.CREATE_TABLE);
            }
            addBinlog.invoke(manager, binlog, null);

        }

        // test get
        Pair<TStatus, TBinlog> pair;

        // get too old
        pair = manager.getBinlog(dbBaseId, tableBaseId, -99);
        Assert.assertEquals(TStatusCode.BINLOG_TOO_OLD_COMMIT_SEQ, pair.first.getStatusCode());
        Assert.assertEquals(TBinlogType.DUMMY, pair.second.getType());

        // get odd commit seq in table level ok
        pair = manager.getBinlog(dbBaseId, tableBaseId, 5);
        Assert.assertEquals(TStatusCode.OK, pair.first.getStatusCode());
        Assert.assertEquals(5 + 2, pair.second.getCommitSeq());

        // get even commit seq in table level ok
        pair = manager.getBinlog(dbBaseId, tableBaseId, 6);
        Assert.assertEquals(TStatusCode.OK, pair.first.getStatusCode());
        Assert.assertEquals(6 + 1, pair.second.getCommitSeq());

        // get odd commit seq in db level ok
        pair = manager.getBinlog(dbBaseId, -1, 5);
        Assert.assertEquals(TStatusCode.OK, pair.first.getStatusCode());
        Assert.assertEquals(5 + 1, pair.second.getCommitSeq());

        // get even commit seq in db level ok
        pair = manager.getBinlog(dbBaseId, -1, 6);
        Assert.assertEquals(TStatusCode.OK, pair.first.getStatusCode());
        Assert.assertEquals(6 + 1, pair.second.getCommitSeq());

        // get too new
        pair = manager.getBinlog(dbBaseId, tableBaseId, 999);
        Assert.assertEquals(TStatusCode.BINLOG_TOO_NEW_COMMIT_SEQ, pair.first.getStatusCode());
        Assert.assertNull(pair.second);
    }

    // A reader waiting for a busy DBBinlog (e.g. a long gc holds its write lock) must not hold the lock
    // of BinlogManager. Otherwise a writer of BinlogManager (addBinlog of any db) waits for the reader,
    // and all readers of all dbs queue behind the writer.
    @Test
    public void testReaderWaitingDbBinlogNotHoldManagerLock() throws Exception {
        Method addBinlog = BinlogManager.class.getDeclaredMethod("addBinlog", TBinlog.class, Object.class);
        addBinlog.setAccessible(true);
        Field dbBinlogMapField = BinlogManager.class.getDeclaredField("dbBinlogMap");
        dbBinlogMapField.setAccessible(true);
        Field managerLockField = BinlogManager.class.getDeclaredField("lock");
        managerLockField.setAccessible(true);
        Field dbLockField = DBBinlog.class.getDeclaredField("lock");
        dbLockField.setAccessible(true);

        BinlogManager manager = new BinlogManager();
        addBinlog.invoke(manager, BinlogTestUtils.newBinlog(dbBaseId, tableBaseId, 1, 1), null);
        DBBinlog dbBinlog = ((Map<Long, DBBinlog>) dbBinlogMapField.get(manager)).get(dbBaseId);
        ReentrantReadWriteLock managerLock = (ReentrantReadWriteLock) managerLockField.get(manager);
        ReentrantReadWriteLock dbLock = (ReentrantReadWriteLock) dbLockField.get(dbBinlog);

        List<Runnable> readers = Lists.newArrayList(
                () -> manager.getBinlog(dbBaseId, tableBaseId, 0, 1),
                () -> manager.getBinlogLag(dbBaseId, tableBaseId, 0),
                () -> manager.getDroppedPartitions(dbBaseId),
                () -> manager.getDroppedTables(dbBaseId),
                () -> manager.getDroppedIndexes(dbBaseId),
                () -> manager.getBinlogInfo());
        for (Runnable reader : readers) {
            AtomicReference<Throwable> readerError = new AtomicReference<>();
            Thread readerThread = new Thread(reader);
            readerThread.setUncaughtExceptionHandler((thread, e) -> readerError.set(e));
            dbLock.writeLock().lock();
            try {
                readerThread.start();
                long deadline = System.currentTimeMillis() + 10000;
                while (!dbLock.hasQueuedThread(readerThread)) {
                    Assert.assertNull(readerError.get());
                    Assert.assertTrue("reader does not wait for the db binlog lock",
                            System.currentTimeMillis() < deadline);
                    Thread.sleep(10);
                }
                Assert.assertTrue(managerLock.writeLock().tryLock(5, TimeUnit.SECONDS));
                managerLock.writeLock().unlock();
            } finally {
                dbLock.writeLock().unlock();
            }
            readerThread.join(10000);
            Assert.assertFalse(readerThread.isAlive());
            Assert.assertNull(readerError.get());
        }
    }

    @Test
    public void testPersist() throws NoSuchMethodException, InvocationTargetException, IllegalAccessException,
            IOException, NoSuchFieldException {
        // reflect BinlogManager
        // addBinlog method
        Method addBinlog = BinlogManager.class.getDeclaredMethod("addBinlog", TBinlog.class, Object.class);
        addBinlog.setAccessible(true);
        // dbBinlogMap
        Field dbBinlogMapField = BinlogManager.class.getDeclaredField("dbBinlogMap");
        dbBinlogMapField.setAccessible(true);

        // init binlog manager & addBinlog
        BinlogManager originManager = new BinlogManager();

        // insert binlogs
        long commitSeq = baseNum;
        for (Map.Entry<Long, List<Long>> dbEntry : frameWork.entrySet()) {
            long dbId = dbEntry.getKey();
            for (long tableId : dbEntry.getValue()) {
                addBinlog.invoke(originManager, BinlogTestUtils.newBinlog(dbId, tableId, commitSeq, commitSeq), null);
                ++commitSeq;
            }
        }

        // init output stream
        ByteArrayOutputStream arrayOutputStream = new ByteArrayOutputStream();
        DataOutputStream outputStream = new DataOutputStream(arrayOutputStream);

        // serialize binlogs
        originManager.write(outputStream, 0L);

        // init another binlog manager
        BinlogManager newManager = new BinlogManager();

        // deserialize binlogs
        ByteArrayInputStream arrayInputStream = new ByteArrayInputStream(arrayOutputStream.toByteArray());
        DataInputStream inputStream = new DataInputStream(arrayInputStream);
        newManager.read(inputStream, 0L);

        // get origin & new dbbinlog's allbinlogs
        Map<Long, DBBinlog> originDbBinlogMap = (Map<Long, DBBinlog>) dbBinlogMapField.get(originManager);
        Map<Long, DBBinlog> newDbBinlogMap = (Map<Long, DBBinlog>) dbBinlogMapField.get(newManager);
        Assert.assertEquals(originDbBinlogMap.size(), newDbBinlogMap.size());
        for (long dbId : frameWork.keySet()) {
            List<TBinlog> originBinlogList = Lists.newArrayList();
            List<TBinlog> newBinlogList = Lists.newArrayList();
            originDbBinlogMap.get(dbId).getAllBinlogs(originBinlogList);
            newDbBinlogMap.get(dbId).getAllBinlogs(newBinlogList);
            Assert.assertEquals(originBinlogList.size(), newBinlogList.size());
            for (int i = 0; i < originBinlogList.size(); ++i) {
                Assert.assertEquals(originBinlogList.get(i).getCommitSeq(),
                        newBinlogList.get(i).getCommitSeq());
            }
        }
    }

    @Test
    public void testWriteTBinlogWithActualLength() throws NoSuchMethodException, InvocationTargetException,
            IllegalAccessException, IOException, NoSuchFieldException, TException {
        // reflect BinlogManager
        // addBinlog method
        Method addBinlog = BinlogManager.class.getDeclaredMethod("addBinlog", TBinlog.class, Object.class);
        addBinlog.setAccessible(true);
        // dbBinlogMap
        Field dbBinlogMapField = BinlogManager.class.getDeclaredField("dbBinlogMap");
        dbBinlogMapField.setAccessible(true);

        // init binlog manager & addBinlog
        BinlogManager originManager = new BinlogManager();

        // one small binlog whose serialized size is far less than BUFFER_SIZE(16KB),
        // one large binlog whose serialized size is larger than BUFFER_SIZE
        TBinlog smallBinlog = BinlogTestUtils.newBinlog(dbBaseId, tableBaseId, baseNum + 1, baseNum + 1);
        TBinlog largeBinlog = BinlogTestUtils.newBinlog(dbBaseId, tableBaseId, baseNum + 2, baseNum + 2);
        // CREATE_TABLE binlog data is not parsed as json when recovering
        largeBinlog.setType(TBinlogType.CREATE_TABLE);
        char[] largeData = new char[20 * 1024];
        Arrays.fill(largeData, 'a');
        largeBinlog.setData(new String(largeData));
        addBinlog.invoke(originManager, smallBinlog, null);
        addBinlog.invoke(originManager, largeBinlog, null);

        // serialize binlogs
        ByteArrayOutputStream arrayOutputStream = new ByteArrayOutputStream();
        DataOutputStream outputStream = new DataOutputStream(arrayOutputStream);
        originManager.write(outputStream, 0L);
        byte[] rawData = arrayOutputStream.toByteArray();

        // expected actual thrift serialized length of each binlog
        TMemoryBuffer smallBuffer = new TMemoryBuffer(32);
        smallBinlog.write(new TBinaryProtocol(smallBuffer));
        Assert.assertTrue(smallBuffer.length() < 16 * 1024);
        TMemoryBuffer largeBuffer = new TMemoryBuffer(32);
        largeBinlog.write(new TBinaryProtocol(largeBuffer));
        Assert.assertTrue(largeBuffer.length() > 16 * 1024);

        // the binlogs written to the stream, in stream order (table/db dummies included)
        Map<Long, DBBinlog> originDbBinlogMap = (Map<Long, DBBinlog>) dbBinlogMapField.get(originManager);
        List<TBinlog> expectedBinlogs = Lists.newArrayList();
        for (DBBinlog dbBinlog : originDbBinlogMap.values()) {
            dbBinlog.getAllBinlogs(expectedBinlogs);
        }

        // each record length in the stream must equal the actual serialized length,
        // not the padded TMemoryBuffer capacity
        DataInputStream checkStream = new DataInputStream(new ByteArrayInputStream(rawData));
        int binlogCount = checkStream.readInt();
        Assert.assertEquals(expectedBinlogs.size(), binlogCount);
        int totalRecordBytes = 0;
        for (int i = 0; i < binlogCount; ++i) {
            TMemoryBuffer buffer = new TMemoryBuffer(32);
            expectedBinlogs.get(i).write(new TBinaryProtocol(buffer));
            int recordLength = checkStream.readInt();
            Assert.assertEquals(buffer.length(), recordLength);
            totalRecordBytes += 4 + recordLength;
            checkStream.skipBytes(recordLength);
        }
        Assert.assertEquals(4 + totalRecordBytes, rawData.length);

        // deserialize binlogs & compare with origin
        BinlogManager newManager = new BinlogManager();
        newManager.read(new DataInputStream(new ByteArrayInputStream(rawData)), 0L);
        Map<Long, DBBinlog> newDbBinlogMap = (Map<Long, DBBinlog>) dbBinlogMapField.get(newManager);
        Assert.assertEquals(originDbBinlogMap.size(), newDbBinlogMap.size());
        List<TBinlog> originBinlogList = Lists.newArrayList();
        List<TBinlog> newBinlogList = Lists.newArrayList();
        originDbBinlogMap.get(dbBaseId).getAllBinlogs(originBinlogList);
        newDbBinlogMap.get(dbBaseId).getAllBinlogs(newBinlogList);
        Assert.assertEquals(originBinlogList.size(), newBinlogList.size());
        for (int i = 0; i < originBinlogList.size(); ++i) {
            Assert.assertEquals(originBinlogList.get(i).getCommitSeq(), newBinlogList.get(i).getCommitSeq());
            Assert.assertEquals(originBinlogList.get(i).getData(), newBinlogList.get(i).getData());
        }
    }

    @Test
    public void testReplayGcFromTableLevel() throws NoSuchMethodException, InvocationTargetException,
            IllegalAccessException, NoSuchFieldException {
        // MockUp
        new MockUp<BinlogUtils>() {
            @Mock
            public long getExpiredMs(long ttl) {
                return timeNow - ttl;
            }
        };

        // reflect BinlogManager
        // addBinlog method
        Method addBinlog = BinlogManager.class.getDeclaredMethod("addBinlog", TBinlog.class, Object.class);
        addBinlog.setAccessible(true);
        // dbBinlogMap
        Field dbBinlogMapField = BinlogManager.class.getDeclaredField("dbBinlogMap");
        dbBinlogMapField.setAccessible(true);

        // init binlog origin & new manager
        BinlogManager originManager = new BinlogManager();
        BinlogManager newManager = new BinlogManager();

        // insert binlogs
        long commitSeq = 0;
        for (Map.Entry<Long, List<Long>> dbEntry : frameWork.entrySet()) {
            long dbId = dbEntry.getKey();
            for (long tableId : dbEntry.getValue()) {
                addBinlog.invoke(originManager, BinlogTestUtils.newBinlog(dbId, tableId, commitSeq, timeNow), null);
                addBinlog.invoke(newManager, BinlogTestUtils.newBinlog(dbId, tableId, commitSeq, timeNow), null);
                ++commitSeq;
            }
        }

        // origin manager gc & get BinlogGcInfo
        BinlogGcInfo info = new BinlogGcInfo(originManager.gc());

        // new manager replay gc
        newManager.replayGc(info);

        // get origin & new dbbinlog's allbinlogs
        Map<Long, DBBinlog> originDbBinlogMap = (Map<Long, DBBinlog>) dbBinlogMapField.get(originManager);
        Map<Long, DBBinlog> newDbBinlogMap = (Map<Long, DBBinlog>) dbBinlogMapField.get(newManager);
        Assert.assertEquals(originDbBinlogMap.size(), newDbBinlogMap.size());
        for (long dbId : frameWork.keySet()) {
            List<TBinlog> originBinlogList = Lists.newArrayList();
            List<TBinlog> newBinlogList = Lists.newArrayList();
            originDbBinlogMap.get(dbId).getAllBinlogs(originBinlogList);
            newDbBinlogMap.get(dbId).getAllBinlogs(newBinlogList);
            Assert.assertEquals(originBinlogList.size(), newBinlogList.size());
            for (int i = 0; i < originBinlogList.size(); ++i) {
                TBinlog originBinlog = originBinlogList.get(i);
                TBinlog newBinlog = newBinlogList.get(i);
                Assert.assertEquals(originBinlog.getCommitSeq(), newBinlog.getCommitSeq());
                if (newBinlog.getType() != TBinlogType.DUMMY) {
                    Assert.assertTrue(newBinlog.getTimestamp() > timeNow - ttl);
                }
            }
        }
    }

    @Test
    public void testReplayGcFromDbLevel() throws NoSuchMethodException, InvocationTargetException,
            IllegalAccessException, NoSuchFieldException {
        // MockUp
        new MockUp<BinlogUtils>() {
            @Mock
            public long getExpiredMs(long ttl) {
                return timeNow - ttl;
            }
        };

        // set dbBinlogEnable
        enableDbBinlog = true;

        // reflect BinlogManager
        // addBinlog method
        Method addBinlog = BinlogManager.class.getDeclaredMethod("addBinlog", TBinlog.class, Object.class);
        addBinlog.setAccessible(true);
        // dbBinlogMap
        Field dbBinlogMapField = BinlogManager.class.getDeclaredField("dbBinlogMap");
        dbBinlogMapField.setAccessible(true);

        // init binlog origin & new manager
        BinlogManager originManager = new BinlogManager();
        BinlogManager newManager = new BinlogManager();

        // insert binlogs
        long commitSeq = baseNum;
        for (Map.Entry<Long, List<Long>> dbEntry : frameWork.entrySet()) {
            long dbId = dbEntry.getKey();
            for (long tableId : dbEntry.getValue()) {
                ++commitSeq;
                addBinlog.invoke(originManager, BinlogTestUtils.newBinlog(dbId, tableId, commitSeq, commitSeq), null);
                addBinlog.invoke(newManager, BinlogTestUtils.newBinlog(dbId, tableId, commitSeq, commitSeq), null);
            }
        }
        timeNow = commitSeq;

        // origin manager gc & get BinlogGcInfo
        BinlogGcInfo info = new BinlogGcInfo(originManager.gc());

        // new manager replay gc
        newManager.replayGc(info);

        // get origin & new dbbinlog's allbinlogs
        Map<Long, DBBinlog> originDbBinlogMap = (Map<Long, DBBinlog>) dbBinlogMapField.get(originManager);
        Map<Long, DBBinlog> newDbBinlogMap = (Map<Long, DBBinlog>) dbBinlogMapField.get(newManager);
        Assert.assertEquals(originDbBinlogMap.size(), newDbBinlogMap.size());
        for (Map.Entry<Long, List<Long>> dbEntry : frameWork.entrySet()) {
            long dbId = dbEntry.getKey();
            List<TBinlog> originBinlogList = Lists.newArrayList();
            List<TBinlog> newBinlogList = Lists.newArrayList();
            originDbBinlogMap.get(dbId).getAllBinlogs(originBinlogList);
            newDbBinlogMap.get(dbId).getAllBinlogs(newBinlogList);
            Assert.assertEquals(originBinlogList.size(), newBinlogList.size());
            for (int i = 0; i < originBinlogList.size(); ++i) {
                TBinlog originBinlog = originBinlogList.get(i);
                TBinlog newBinlog = newBinlogList.get(i);
                Assert.assertEquals(originBinlog.getCommitSeq(), newBinlog.getCommitSeq());
                if (newBinlog.getType() != TBinlogType.DUMMY) {
                    Assert.assertTrue(newBinlog.getCommitSeq() > timeNow - ttl);
                }
            }
        }
    }
}
