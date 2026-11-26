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

import org.apache.doris.analysis.AdminSetReplicaStatusStmt;
import org.apache.doris.analysis.RollupRenameClause;
import org.apache.doris.catalog.Replica.ReplicaStatus;
import org.apache.doris.common.Config;
import org.apache.doris.common.FeMetaVersion;
import org.apache.doris.consistency.ConsistencyChecker;
import org.apache.doris.meta.MetaContext;
import org.apache.doris.persist.ConsistencyCheckInfo;
import org.apache.doris.persist.EditLog;
import org.apache.doris.persist.PartitionStateTokenLog;
import org.apache.doris.persist.SetReplicaStatusOperationLog;
import org.apache.doris.persist.SetReplicaVersionOperationLog;
import org.apache.doris.persist.TableInfo;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.thrift.TStorageType;
import org.apache.doris.thrift.TUniqueId;
import org.apache.doris.transaction.FakeTransactionIDGenerator;
import org.apache.doris.transaction.PartitionCommitInfo;
import org.apache.doris.transaction.TableCommitInfo;
import org.apache.doris.transaction.TransactionState;
import org.apache.doris.transaction.TransactionState.LoadJobSourceType;
import org.apache.doris.transaction.TransactionState.TxnCoordinator;
import org.apache.doris.transaction.TransactionState.TxnSourceType;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.util.Map;

/** The changes of a table outside of a transaction that invalidate the state tokens, and the backup that names. */
public class PartitionStateTokenInvalidateTest {
    private static final long ROLLUP_ID = 90001L;

    private FakeEnv fakeEnv;
    private FakeEditLog fakeEditLog;
    private FakeTransactionIDGenerator fakeTransactionIDGenerator;
    private Env env;
    private Database db;
    private OlapTable table;
    private Partition partition;

    @BeforeEach
    public void setUp() throws Exception {
        fakeEnv = new FakeEnv();
        fakeEditLog = new FakeEditLog();
        fakeTransactionIDGenerator = new FakeTransactionIDGenerator();
        env = CatalogTestUtil.createTestCatalog();
        // the logs written are verified on a mock (FakeEditLog only makes the edit log of the test catalog)
        env.setEditLog(Mockito.mock(EditLog.class));
        MetaContext metaContext = new MetaContext();
        metaContext.setMetaVersion(FeMetaVersion.VERSION_CURRENT);
        metaContext.setThreadLocalInfo();
        FakeEnv.setEnv(env);
        db = env.getInternalCatalog().getDbOrDdlException(CatalogTestUtil.testDbId1);
        table = (OlapTable) db.getTableOrDdlException(CatalogTestUtil.testTableId1);
        partition = table.getPartition(CatalogTestUtil.testPartitionId1);
    }

    @AfterEach
    public void tearDown() {
        Config.restore_state_token_track = false;
    }

    private void name(String token) {
        partition.setStateToken(new PartitionStateToken(token, partition.getVisibleVersion(),
                partition.getVisibleVersionTime()));
        Assertions.assertEquals(token, PartitionStateToken.effective(partition));
    }

    @Test
    public void testRenameRollupInvalidates() throws Exception {
        table.setIndexMeta(ROLLUP_ID, "r1", table.getBaseSchema(), 0, 1, (short) 1, TStorageType.COLUMN,
                table.getKeysType());
        // two rollups of the same columns may swap their names: the name then stands for other data
        name("aaaa");
        env.renameRollup(db, table, new RollupRenameClause("r1", "r2"));
        Assertions.assertEquals(ROLLUP_ID, table.getIndexIdByName("r2").longValue());
        Assertions.assertNull(PartitionStateToken.effective(partition));
        // on replay
        name("bbbb");
        env.replayRenameRollup(TableInfo.createForRollupRename(db.getId(), table.getId(), ROLLUP_ID, "r2", "r3"));
        Assertions.assertEquals(ROLLUP_ID, table.getIndexIdByName("r3").longValue());
        Assertions.assertNull(PartitionStateToken.effective(partition));
    }

    @Test
    public void testBackupNamesThePartitionsWithoutToken() {
        partition.setStateToken(PartitionStateToken.invalid());
        // the tokens are not tracked: nothing is named
        env.namePartitionsWithoutStateToken(db, table);
        Assertions.assertNull(PartitionStateToken.effective(partition));

        Config.restore_state_token_track = true;
        env.namePartitionsWithoutStateToken(db, table);
        String named = PartitionStateToken.effective(partition);
        Assertions.assertNotNull(named);
        Assertions.assertEquals(partition.getVisibleVersion(), partition.getStateToken().getBoundVersion());
        // a partition that has a token keeps it
        env.namePartitionsWithoutStateToken(db, table);
        Assertions.assertEquals(named, PartitionStateToken.effective(partition));
        // a partition that never had a token and has a visible version is named
        partition.setStateToken(null);
        Assertions.assertNull(PartitionStateToken.effective(partition));
        env.namePartitionsWithoutStateToken(db, table);
        Assertions.assertNotNull(PartitionStateToken.effective(partition));
        Assertions.assertNotEquals(named, PartitionStateToken.effective(partition));
        // a partition at the initial version is EMPTY and is not named
        Partition empty = addPartition(80001L);
        env.namePartitionsWithoutStateToken(db, table);
        Assertions.assertNull(empty.getStateToken());
        Assertions.assertEquals(PartitionStateToken.EMPTY, PartitionStateToken.effective(empty));
    }

    @Test
    public void testBackupNamesInLogsOfBoundedSize() {
        Config.restore_state_token_track = true;
        int added = 2 * Env.MAX_PARTITIONS_PER_STATE_TOKEN_LOG + 500;
        for (int i = 0; i < added; i++) {
            addPartition(80001L + i).updateVisibleVersionAndTime(2, 1000);
        }
        env.namePartitionsWithoutStateToken(db, table);
        ArgumentCaptor<PartitionStateTokenLog> logs = ArgumentCaptor.forClass(PartitionStateTokenLog.class);
        Mockito.verify(env.getEditLog(), Mockito.times(3)).logSetPartitionStateToken(logs.capture());
        int named = 0;
        for (PartitionStateTokenLog log : logs.getAllValues()) {
            Assertions.assertTrue(log.getTokens().size() <= Env.MAX_PARTITIONS_PER_STATE_TOKEN_LOG);
            named += log.getTokens().size();
        }
        // the added ones and the partition of the test table, which has a version and no token
        Assertions.assertEquals(added + 1, named);
        for (Partition p : table.getPartitions()) {
            Assertions.assertNotNull(PartitionStateToken.effective(p));
        }
    }

    @Test
    public void testTaintIsKeptUntilARestore() throws Exception {
        name("aaaa");
        PartitionStateToken.taint(partition);
        Assertions.assertTrue(PartitionStateToken.isTainted(partition));
        Assertions.assertNull(PartitionStateToken.effective(partition));
        // an invalidation keeps it: a schema change or rollup job, ADMIN SET PARTITION VERSION
        table.invalidatePartitionStateTokens();
        Assertions.assertTrue(PartitionStateToken.isTainted(partition));
        env.setPartitionVersionInternal(db.getFullName(), table.getName(), partition.getId(),
                partition.getVisibleVersion(), true);
        Assertions.assertTrue(PartitionStateToken.isTainted(partition));
        // a backup does not name it
        Config.restore_state_token_track = true;
        env.namePartitionsWithoutStateToken(db, table);
        Assertions.assertTrue(PartitionStateToken.isTainted(partition));
        Mockito.verify(env.getEditLog(), Mockito.never()).logSetPartitionStateToken(Mockito.any());
        // persisted
        Partition q = GsonUtils.GSON.fromJson(GsonUtils.GSON.toJson(partition), Partition.class);
        Assertions.assertTrue(PartitionStateToken.isTainted(q));
        Assertions.assertNull(PartitionStateToken.effective(q));
        // a restore stamps the token of the backup
        partition.setStateToken(new PartitionStateToken("bbbb", partition.getVisibleVersion(),
                partition.getVisibleVersionTime()));
        Assertions.assertFalse(PartitionStateToken.isTainted(partition));
    }

    @Test
    public void testAdminReplicaOperationsTaint() throws Exception {
        long backendId = CatalogTestUtil.testBackendId1;
        long tabletId = CatalogTestUtil.testTabletId1;
        // ADMIN SET REPLICA VERSION, as the follower replays it (the master runs the same code before it logs)
        name("aaaa");
        env.replaySetReplicaVersion(new SetReplicaVersionOperationLog(backendId, tabletId,
                partition.getVisibleVersion(), null, null, 1000L));
        Assertions.assertTrue(PartitionStateToken.isTainted(partition));
        // ADMIN SET REPLICA STATUS to BAD and back to OK
        name("bbbb");
        env.replaySetReplicaStatus(new SetReplicaStatusOperationLog(backendId, tabletId, ReplicaStatus.BAD, -1L));
        Assertions.assertTrue(PartitionStateToken.isTainted(partition));
        name("cccc");
        env.replaySetReplicaStatus(new SetReplicaStatusOperationLog(backendId, tabletId, ReplicaStatus.OK, -1L));
        Assertions.assertTrue(PartitionStateToken.isTainted(partition));
        // DROP only asks the scheduler to drop the replica first, which repairs from the others
        name("dddd");
        env.replaySetReplicaStatus(new SetReplicaStatusOperationLog(backendId, tabletId, ReplicaStatus.DROP,
                2000L));
        Assertions.assertFalse(PartitionStateToken.isTainted(partition));
        Assertions.assertEquals("dddd", PartitionStateToken.effective(partition));
    }

    @Test
    public void testLostReplicaTaintsAndLogsAReplicaStatus() throws Exception {
        // recover_with_empty_tablet: a lost replica is made empty at the same version
        long backendId = CatalogTestUtil.testBackendId1;
        long tabletId = CatalogTestUtil.testTabletId1;
        Replica replica = Env.getCurrentInvertedIndex().getReplica(tabletId, backendId);
        // a partition that never had a name (the tokens were never tracked): nothing is logged
        Partition unnamed = addPartition(80002L);
        unnamed.updateVisibleVersionAndTime(2, 1000);
        env.taintPartitionOfLostReplica(unnamed, tabletId, backendId, replica);
        Assertions.assertNull(unnamed.getStateToken());
        // EMPTY has no data to lose
        Partition empty = addPartition(80001L);
        env.taintPartitionOfLostReplica(empty, tabletId, backendId, replica);
        Assertions.assertEquals(PartitionStateToken.EMPTY, PartitionStateToken.effective(empty));
        Mockito.verify(env.getEditLog(), Mockito.never()).logSetReplicaStatus(Mockito.any());

        // a name given while the tokens were tracked is tainted, also with the tracking off, by a log that a
        // frontend that does not know the state tokens reads
        name("aaaa");
        replica.setBad(true);
        env.taintPartitionOfLostReplica(partition, tabletId, backendId, replica);
        Assertions.assertTrue(PartitionStateToken.isTainted(partition));
        // the replica keeps its status until the empty one is created
        Assertions.assertTrue(replica.isBad());
        ArgumentCaptor<SetReplicaStatusOperationLog> logs = ArgumentCaptor.forClass(
                SetReplicaStatusOperationLog.class);
        Mockito.verify(env.getEditLog(), Mockito.times(1)).logSetReplicaStatus(logs.capture());
        Assertions.assertEquals(ReplicaStatus.BAD, logs.getValue().getReplicaStatus());
        Assertions.assertEquals(tabletId, logs.getValue().getTabletId());
        Assertions.assertEquals(backendId, logs.getValue().getBackendId());
        Mockito.verify(env.getEditLog(), Mockito.never()).logSetPartitionStateToken(Mockito.any());
        // tainted already: no name to lose, nothing logged
        env.taintPartitionOfLostReplica(partition, tabletId, backendId, replica);
        Mockito.verify(env.getEditLog(), Mockito.times(1)).logSetReplicaStatus(Mockito.any());

        // the follower replays the log on a replica that has the status already, and taints
        name("bbbb");
        env.replaySetReplicaStatus(logs.getValue());
        Assertions.assertTrue(PartitionStateToken.isTainted(partition));
        Assertions.assertTrue(replica.isBad());

        // a lost replica that is good is logged as good, and the replay taints all the same
        replica.setBad(false);
        name("cccc");
        env.taintPartitionOfLostReplica(partition, tabletId, backendId, replica);
        Assertions.assertTrue(PartitionStateToken.isTainted(partition));
        Mockito.verify(env.getEditLog(), Mockito.times(2)).logSetReplicaStatus(logs.capture());
        Assertions.assertEquals(ReplicaStatus.OK, logs.getValue().getReplicaStatus());
        name("dddd");
        env.replaySetReplicaStatus(logs.getValue());
        Assertions.assertTrue(PartitionStateToken.isTainted(partition));
        Assertions.assertFalse(replica.isBad());
    }

    @Test
    public void testAdminReplicaStatusNotChangedOnTheMasterDoesNotTaint() throws Exception {
        // the master logs and taints a change only; the replay taints whatever the replica is, so the master
        // must not log a status that is not a change
        long backendId = CatalogTestUtil.testBackendId1;
        long tabletId = CatalogTestUtil.testTabletId1;
        Replica replica = Env.getCurrentInvertedIndex().getReplica(tabletId, backendId);
        replica.setBad(false);
        AdminSetReplicaStatusStmt command = Mockito.mock(AdminSetReplicaStatusStmt.class);
        Mockito.when(command.getTabletId()).thenReturn(tabletId);
        Mockito.when(command.getBackendId()).thenReturn(backendId);
        Mockito.when(command.getStatus()).thenReturn(ReplicaStatus.OK);
        name("aaaa");
        env.setReplicaStatus(command);
        Assertions.assertEquals("aaaa", PartitionStateToken.effective(partition));
        Mockito.verify(env.getEditLog(), Mockito.never()).logSetReplicaStatus(Mockito.any());
        Mockito.when(command.getStatus()).thenReturn(ReplicaStatus.BAD);
        env.setReplicaStatus(command);
        Assertions.assertTrue(PartitionStateToken.isTainted(partition));
        Mockito.verify(env.getEditLog(), Mockito.times(1)).logSetReplicaStatus(Mockito.any());
        replica.setBad(false);
    }

    @Test
    public void testRecoverPartitionInvalidates() throws Exception {
        // RECOVER PARTITION, as the follower replays it: while the partition was in the recycle bin its replicas
        // could be changed and its indexes renamed with nothing to invalidate it
        CatalogRecycleBin recycleBin = new CatalogRecycleBin();
        name("aaaa");
        Assertions.assertTrue(recycleBin.recyclePartition(db.getId(), table.getId(), table.getName(), partition,
                null, null, new DataProperty(DataProperty.DEFAULT_STORAGE_MEDIUM),
                new ReplicaAllocation((short) 3), false, true));
        recycleBin.replayRecoverPartition(table, partition.getId(), null);
        Assertions.assertNull(PartitionStateToken.effective(partition));
        Assertions.assertTrue(partition.getStateToken().isBad());
        // a tainted one stays tainted
        PartitionStateToken.taint(partition);
        Assertions.assertTrue(recycleBin.recyclePartition(db.getId(), table.getId(), table.getName(), partition,
                null, null, new DataProperty(DataProperty.DEFAULT_STORAGE_MEDIUM),
                new ReplicaAllocation((short) 3), false, true));
        recycleBin.replayRecoverPartition(table, partition.getId(), null);
        Assertions.assertTrue(PartitionStateToken.isTainted(partition));
    }

    @Test
    public void testInconsistentChecksumTaints() throws Exception {
        // the replay of a finished consistency check (CheckConsistencyJob#tryFinishJob taints the same way)
        ConsistencyChecker checker = new ConsistencyChecker();
        name("aaaa");
        checker.replayFinishConsistencyCheck(new ConsistencyCheckInfo(db.getId(), table.getId(), partition.getId(),
                CatalogTestUtil.testIndexId1, CatalogTestUtil.testTabletId1, 1000L, partition.getVisibleVersion(),
                true), env);
        Assertions.assertEquals("aaaa", PartitionStateToken.effective(partition));
        checker.replayFinishConsistencyCheck(new ConsistencyCheckInfo(db.getId(), table.getId(), partition.getId(),
                CatalogTestUtil.testIndexId1, CatalogTestUtil.testTabletId1, 2000L, partition.getVisibleVersion(),
                false), env);
        Assertions.assertTrue(PartitionStateToken.isTainted(partition));
    }

    @Test
    public void testReplayBindsToTheLocalVersionTime() throws Exception {
        // the master named the partition at its version time; this frontend has another one for the same version
        // (a restore and ADMIN SET PARTITION VERSION take the current time of each frontend)
        long version = partition.getVisibleVersion();
        partition.updateVisibleVersionAndTime(version, 7777L);
        Map<Long, PartitionStateToken> tokens = Maps.newHashMap();
        tokens.put(partition.getId(), new PartitionStateToken("dddd", version, 1234L));
        env.replaySetPartitionStateToken(new PartitionStateTokenLog(db.getId(), table.getId(), tokens));
        Assertions.assertEquals("dddd", PartitionStateToken.effective(partition));
        Assertions.assertEquals(7777L, partition.getStateToken().getBoundVersionTime());
        // at another version: no name
        tokens.put(partition.getId(), new PartitionStateToken("eeee", version - 1, 1234L));
        env.replaySetPartitionStateToken(new PartitionStateTokenLog(db.getId(), table.getId(), tokens));
        Assertions.assertNull(PartitionStateToken.effective(partition));
        // an invalid or tainted token as it is
        PartitionStateToken.taint(partition);
        PartitionStateToken tainted = partition.getStateToken();
        name("ffff");
        tokens.put(partition.getId(), tainted);
        env.replaySetPartitionStateToken(new PartitionStateTokenLog(db.getId(), table.getId(), tokens));
        Assertions.assertTrue(PartitionStateToken.isTainted(partition));
        // a partition that does not exist is skipped, not the others
        tokens.clear();
        tokens.put(99999L, new PartitionStateToken("gggg", version, 1234L));
        tokens.put(partition.getId(), new PartitionStateToken("hhhh", version, 1234L));
        env.replaySetPartitionStateToken(new PartitionStateTokenLog(db.getId(), table.getId(), tokens));
        Assertions.assertEquals("hhhh", PartitionStateToken.effective(partition));
    }

    // a new partition of the test table, at the initial version
    private Partition addPartition(long id) {
        Partition p = new Partition(id, "p" + id, new MaterializedIndex(id + 100000L,
                MaterializedIndex.IndexState.NORMAL), new RandomDistributionInfo(1));
        table.addPartition(p);
        return p;
    }

    @Test
    public void testReplayOfTheNames() throws Exception {
        Map<Long, PartitionStateToken> tokens = Maps.newHashMap();
        tokens.put(partition.getId(), new PartitionStateToken("cccc", partition.getVisibleVersion(),
                partition.getVisibleVersionTime()));
        // through the serialized form, as a follower reads it
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        new PartitionStateTokenLog(db.getId(), table.getId(), tokens).write(new DataOutputStream(bytes));
        PartitionStateTokenLog log = PartitionStateTokenLog.read(
                new DataInputStream(new ByteArrayInputStream(bytes.toByteArray())));
        Assertions.assertEquals(db.getId(), log.getDbId());
        Assertions.assertEquals(table.getId(), log.getTableId());

        partition.setStateToken(PartitionStateToken.invalid());
        env.replaySetPartitionStateToken(log);
        Assertions.assertEquals("cccc", PartitionStateToken.effective(partition));
    }

    @Test
    public void testCcrNotContinuousIsToldBeforeTheTransactionIsVisible() {
        // ccr-syncer commits with only_commit: the commit returns before the transaction is visible
        name("src4");
        long next = partition.getVisibleVersion() + 1;
        TransactionState transactionState = ccrCommitted(next);
        // not a CCR increment
        Assertions.assertTrue(transactionState.getCcrNotContinuousPartitionIdsBeforeVisible(db).isEmpty());
        // the partition has the token the source had before the write: it will adopt
        Map<Long, TransactionState.CcrPartitionSource> sources = Maps.newHashMap();
        sources.put(partition.getId(), new TransactionState.CcrPartitionSource(101L, next, "src4", "src5"));
        transactionState.setCcrPartitionSources(sources);
        Assertions.assertTrue(transactionState.getCcrNotContinuousPartitionIdsBeforeVisible(db).isEmpty());
        // it has another one
        name("local4");
        Assertions.assertEquals(Lists.newArrayList(partition.getId()),
                transactionState.getCcrNotContinuousPartitionIdsBeforeVisible(db));
        // a transaction before this one is not visible yet and may change the token: not told this time
        Assertions.assertTrue(ccrCommittedFrom(next + 1, sources).getCcrNotContinuousPartitionIdsBeforeVisible(db)
                .isEmpty());
        // tainted: no token to continue
        name("src4");
        PartitionStateToken.taint(partition);
        Assertions.assertEquals(Lists.newArrayList(partition.getId()),
                transactionState.getCcrNotContinuousPartitionIdsBeforeVisible(db));
    }

    private TransactionState ccrCommittedFrom(long version, Map<Long, TransactionState.CcrPartitionSource> sources) {
        TransactionState transactionState = ccrCommitted(version);
        transactionState.setCcrPartitionSources(sources);
        return transactionState;
    }

    // a committed transaction that writes the version into the partition of the test table
    private TransactionState ccrCommitted(long version) {
        TransactionState transactionState = new TransactionState(db.getId(), Lists.newArrayList(table.getId()),
                3000, "label_ccr", new TUniqueId(1, 2), LoadJobSourceType.BACKEND_STREAMING,
                new TxnCoordinator(TxnSourceType.BE, 0, "127.0.0.1", System.currentTimeMillis()), 50000L,
                60 * 1000L);
        TableCommitInfo tableCommitInfo = new TableCommitInfo(table.getId());
        tableCommitInfo.addPartitionCommitInfo(new PartitionCommitInfo(partition.getId(), "", version, 1000L,
                false));
        transactionState.putIdToTableCommitInfo(table.getId(), tableCommitInfo);
        return transactionState;
    }
}
