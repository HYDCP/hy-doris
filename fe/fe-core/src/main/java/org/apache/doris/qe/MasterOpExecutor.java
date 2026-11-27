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

package org.apache.doris.qe;

import org.apache.doris.analysis.RedirectStatus;
import org.apache.doris.analysis.TableName;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ClientPool;
import org.apache.doris.common.DdlException;
import org.apache.doris.thrift.FrontendService;
import org.apache.doris.thrift.TDropSessionTemporaryTableRequest;
import org.apache.doris.thrift.TDropSessionTemporaryTableResult;
import org.apache.doris.thrift.TGroupCommitInfo;
import org.apache.doris.thrift.TMasterOpRequest;
import org.apache.doris.thrift.TMasterOpResult;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TStatusCode;
import org.apache.doris.thrift.TWaitingTxnStatusRequest;
import org.apache.doris.thrift.TWaitingTxnStatusResult;
import org.apache.doris.transaction.TransactionStatus;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.thrift.TApplicationException;

import java.util.concurrent.TimeUnit;

/**
 * MasterOpExecutor is used to send request to Master FE.
 * It is inherited from FEOpExecutor. The difference is that MasterOpExecutor may need to wait the journal being
 * synced before returning.
 */
public class MasterOpExecutor extends FEOpExecutor {
    private static final Logger LOG = LogManager.getLogger(MasterOpExecutor.class);
    private final int journalWaitTimeoutMs;
    private boolean trackTemporaryTableCreation;
    private long temporaryTableResetDeadline;

    public MasterOpExecutor(OriginStatement originStmt, ConnectContext ctx, RedirectStatus status, boolean isQuery) {
        super(new TNetworkAddress(ctx.getEnv().getMasterHost(), ctx.getEnv().getMasterRpcPort()),
                originStmt, ctx, isQuery);
        if (status.isNeedToWaitJournalSync()) {
            this.journalWaitTimeoutMs = (int) (ctx.getExecTimeout() * 1000 * RPC_TIMEOUT_COEFFICIENT);
        } else {
            this.journalWaitTimeoutMs = 0;
        }
    }

    /**
     * used for simply syncing journal with master under strong consistency mode
     */
    public MasterOpExecutor(ConnectContext ctx) {
        this(null, ctx, RedirectStatus.FORWARD_WITH_SYNC, true);
    }

    static long temporaryTableResetDeadline(ConnectContext ctx) {
        return System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(
                (int) (ctx.getExecTimeout() * 1000 * RPC_TIMEOUT_COEFFICIENT));
    }

    static void checkTemporaryTableResetDeadline(long deadline) throws DdlException {
        limitToTemporaryTableResetDeadline(Integer.MAX_VALUE, deadline);
    }

    private static int limitToTemporaryTableResetDeadline(int timeoutMs, long deadline) throws DdlException {
        if (deadline == 0 || timeoutMs == 0) {
            return timeoutMs;
        }
        long remaining = deadline - System.nanoTime();
        if (remaining <= 0) {
            throw new DdlException("Timed out cleaning temporary tables on connection reset");
        }
        return (int) Math.min(timeoutMs, Math.max(1, TimeUnit.NANOSECONDS.toMillis(remaining)));
    }

    public MasterOpExecutor withTemporaryTableResetDeadline(long deadline) {
        temporaryTableResetDeadline = deadline;
        return this;
    }

    @Override
    protected int getThriftTimeoutMs() throws DdlException {
        return limitToTemporaryTableResetDeadline(super.getThriftTimeoutMs(), temporaryTableResetDeadline);
    }

    @Override
    public void execute() throws Exception {
        super.execute();
        waitOnReplaying();
    }

    public void executeCheckingSqlStatus() throws Exception {
        super.execute(true, true);
        waitOnReplaying();
    }

    public void executeTemporaryTableDrop() throws Exception {
        super.execute(true, false);
        waitOnReplaying();
    }

    public boolean canResetTemporaryTableTransaction(long dbId, long txnId, boolean checkExpired) throws Exception {
        TWaitingTxnStatusRequest request = new TWaitingTxnStatusRequest().setDbId(dbId).setTxnId(txnId);
        if (checkExpired) {
            request.setCheckExpiredTxnForTemporaryTableReset(true);
        }
        FrontendService.Client client = ClientPool.frontendPool.borrowObject(feAddr, getThriftTimeoutMs());
        boolean transportOk = false;
        try {
            TWaitingTxnStatusResult reply = client.waitingTxnStatus(request);
            transportOk = true;
            TStatusCode status = reply.getStatus().getStatusCode();
            return (checkExpired && status == TStatusCode.NOT_FOUND)
                    || (status == TStatusCode.OK && reply.isSetTxnStatusId()
                            && reply.getTxnStatusId() == TransactionStatus.ABORTED.value());
        } finally {
            if (transportOk) {
                ClientPool.frontendPool.returnObject(feAddr, client);
            } else {
                ClientPool.frontendPool.invalidateObject(feAddr, client);
            }
        }
    }

    // Only UNKNOWN_METHOD permits a fallback to SQL on an older Master.
    boolean tryDropTemporaryTableById(long dbId, long tableId, boolean waitForReplay) throws Exception {
        Env env = ctx.getEnv();
        TDropSessionTemporaryTableRequest request = new TDropSessionTemporaryTableRequest(env.getClusterId(),
                env.getToken(), ctx.getSessionId(), dbId, tableId);
        FrontendService.Client client = ClientPool.frontendPool.borrowObject(feAddr, getThriftTimeoutMs());
        boolean transportOk = false;
        TDropSessionTemporaryTableResult reply;
        try {
            reply = client.dropSessionTemporaryTable(request);
            transportOk = true;
        } catch (TApplicationException e) {
            if (e.getType() == TApplicationException.UNKNOWN_METHOD) {
                transportOk = true;
                return false;
            }
            throw e;
        } finally {
            if (transportOk) {
                ClientPool.frontendPool.returnObject(feAddr, client);
            } else {
                ClientPool.frontendPool.invalidateObject(feAddr, client);
            }
        }
        if (reply.getStatus().getStatusCode() != TStatusCode.OK) {
            throw new DdlException("Master rejected cleanup of temporary table " + tableId + ": "
                    + reply.getStatus().getStatusCode() + " " + reply.getStatus().getErrorMsgs());
        }
        if (waitForReplay) {
            ctx.recordTemporaryTableDropJournal(reply.getMaxJournalId());
        }
        return true;
    }

    long getAcknowledgedCreateJournalId() {
        if (result == null || !result.isSetStatusCode() || !result.isSetMaxJournalId()) {
            return -1L;
        }
        // A legacy Master also reports its journal position when the CREATE finishes with a SQL error.
        return result.getMaxJournalId();
    }

    boolean recordCreatedTemporaryTables(TableName awaitingReplay) {
        if (result == null || !result.isSetCreatedTemporaryTableDbIds()) {
            return false;
        }
        ctx.addForwardedTemporaryTables(result.getCreatedTemporaryTableDbIds(), result.getMaxJournalId(),
                awaitingReplay);
        return true;
    }

    public void trackTemporaryTableCreation() {
        trackTemporaryTableCreation = true;
    }

    @Override
    protected TMasterOpRequest buildStmtForwardParams() throws AnalysisException {
        TMasterOpRequest params = super.buildStmtForwardParams();
        if (trackTemporaryTableCreation && ctx.getConnectType() == ConnectContext.ConnectType.MYSQL
                && ctx.getConnectScheduler() != null) {
            params.setTempTableOriginConnectionId(ctx.getConnectionId());
        }
        return params;
    }

    @Override
    public void cancel() throws Exception {
        TMasterOpResult cancelResult = sendCancel();
        if (cancelResult != null) {
            ctx.getEnv().getJournalObservable().waitOn(cancelResult.getMaxJournalId(), journalWaitTimeoutMs);
        }
    }

    void waitOnReplaying() throws DdlException {
        LOG.info("forwarding to master get result max journal id: {}", result.maxJournalId);
        waitForTemporaryTableResetReplay(result.maxJournalId);
    }

    void waitForTemporaryTableResetReplay(long journalId) throws DdlException {
        ctx.getEnv().getJournalObservable().waitOn(journalId,
                limitToTemporaryTableResetDeadline(journalWaitTimeoutMs, temporaryTableResetDeadline));
    }

    public void syncJournal() throws Exception {
        result = forward(buildSyncJournalParams());
        waitOnReplaying();
    }

    public long getGroupCommitLoadBeId(long tableId, String cluster) throws Exception {
        result = forward(buildGetGroupCommitLoadBeIdParmas(tableId, cluster));
        waitOnReplaying();
        return result.groupCommitLoadBeId;
    }

    public void updateLoadData(long tableId, long receiveData) throws Exception {
        result = forward(buildUpdateLoadDataParams(tableId, receiveData));
        waitOnReplaying();
    }

    private TMasterOpRequest buildSyncJournalParams() {
        final TMasterOpRequest params = new TMasterOpRequest();
        // node ident
        params.setClientNodeHost(Env.getCurrentEnv().getSelfNode().getHost());
        params.setClientNodePort(Env.getCurrentEnv().getSelfNode().getPort());
        params.setSyncJournalOnly(true);
        params.setDb(ctx.getDatabase());
        params.setUser(ctx.getQualifiedUser());
        // just make the protocol happy
        params.setSql("");
        return params;
    }

    private TMasterOpRequest buildGetGroupCommitLoadBeIdParmas(long tableId, String cluster) {
        final TGroupCommitInfo groupCommitParams = new TGroupCommitInfo();
        groupCommitParams.setGetGroupCommitLoadBeId(true);
        groupCommitParams.setGroupCommitLoadTableId(tableId);
        groupCommitParams.setCluster(cluster);
        return getMasterOpRequestForGroupCommit(groupCommitParams);
    }

    private TMasterOpRequest buildUpdateLoadDataParams(long tableId, long receiveData) {
        final TGroupCommitInfo groupCommitParams = new TGroupCommitInfo();
        groupCommitParams.setUpdateLoadData(true);
        groupCommitParams.setTableId(tableId);
        groupCommitParams.setReceiveData(receiveData);
        return getMasterOpRequestForGroupCommit(groupCommitParams);
    }

    private TMasterOpRequest getMasterOpRequestForGroupCommit(TGroupCommitInfo groupCommitParams) {
        final TMasterOpRequest params = new TMasterOpRequest();
        // node ident
        params.setClientNodeHost(Env.getCurrentEnv().getSelfNode().getHost());
        params.setClientNodePort(Env.getCurrentEnv().getSelfNode().getPort());
        params.setGroupCommitInfo(groupCommitParams);
        params.setDb(ctx.getDatabase());
        params.setUser(ctx.getQualifiedUser());
        // just make the protocol happy
        params.setSql("");
        return params;
    }

}
