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

import org.apache.doris.analysis.BackupStmt.BackupContent;
import org.apache.doris.analysis.RestoreStmt;
import org.apache.doris.backup.BackupJobInfo.BackupIndexInfo;
import org.apache.doris.backup.BackupJobInfo.BackupOlapTableInfo;
import org.apache.doris.backup.BackupJobInfo.BackupPartitionInfo;
import org.apache.doris.backup.BackupJobInfo.BackupTabletInfo;
import org.apache.doris.backup.RestoreFileMapping.IdChain;
import org.apache.doris.backup.Status.ErrCode;
import org.apache.doris.catalog.BinlogConfig;
import org.apache.doris.catalog.DataProperty;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.EnvFactory;
import org.apache.doris.catalog.FsBroker;
import org.apache.doris.catalog.Index;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.MaterializedIndex.IndexExtState;
import org.apache.doris.catalog.MaterializedIndexMeta;
import org.apache.doris.catalog.OdbcCatalogResource;
import org.apache.doris.catalog.OdbcTable;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.OlapTable.OlapTableState;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.Partition.PartitionState;
import org.apache.doris.catalog.PartitionInfo;
import org.apache.doris.catalog.PartitionItem;
import org.apache.doris.catalog.PartitionStateToken;
import org.apache.doris.catalog.PartitionType;
import org.apache.doris.catalog.Replica;
import org.apache.doris.catalog.Replica.ReplicaState;
import org.apache.doris.catalog.ReplicaAllocation;
import org.apache.doris.catalog.Resource;
import org.apache.doris.catalog.ResourceMgr;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.TableIf.TableType;
import org.apache.doris.catalog.Tablet;
import org.apache.doris.catalog.TabletMeta;
import org.apache.doris.catalog.View;
import org.apache.doris.clone.DynamicPartitionScheduler;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.FeMetaVersion;
import org.apache.doris.common.MarkedCountDownLatch;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.Pair;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.util.DbUtil;
import org.apache.doris.common.util.DebugPointUtil;
import org.apache.doris.common.util.DynamicPartitionUtil;
import org.apache.doris.common.util.PropertyAnalyzer;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.ha.FrontendNodeType;
import org.apache.doris.persist.gson.GsonPostProcessable;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.resource.Tag;
import org.apache.doris.system.Backend;
import org.apache.doris.system.Frontend;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.task.AgentBatchTask;
import org.apache.doris.task.AgentBoundedBatchTask;
import org.apache.doris.task.AgentTask;
import org.apache.doris.task.AgentTaskExecutor;
import org.apache.doris.task.AgentTaskQueue;
import org.apache.doris.task.CreateReplicaTask;
import org.apache.doris.task.DirMoveTask;
import org.apache.doris.task.DownloadTask;
import org.apache.doris.task.ReleaseSnapshotTask;
import org.apache.doris.task.SnapshotTask;
import org.apache.doris.thrift.TFinishTaskRequest;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TRemoteTabletSnapshot;
import org.apache.doris.thrift.TStatusCode;
import org.apache.doris.thrift.TStorageMedium;
import org.apache.doris.thrift.TStorageType;
import org.apache.doris.thrift.TTaskType;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import com.google.common.collect.Table.Cell;
import com.google.gson.annotations.JsonAdapter;
import com.google.gson.annotations.SerializedName;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.zip.GZIPInputStream;

public class RestoreJob extends AbstractJob implements GsonPostProcessable {
    private static final String PROP_RESERVE_REPLICA = RestoreStmt.PROP_RESERVE_REPLICA;
    private static final String PROP_RESERVE_DYNAMIC_PARTITION_ENABLE =
            RestoreStmt.PROP_RESERVE_DYNAMIC_PARTITION_ENABLE;
    private static final String PROP_IS_BEING_SYNCED = PropertyAnalyzer.PROPERTIES_IS_BEING_SYNCED;
    private static final String PROP_CLEAN_TABLES = RestoreStmt.PROP_CLEAN_TABLES;
    private static final String PROP_CLEAN_PARTITIONS = RestoreStmt.PROP_CLEAN_PARTITIONS;
    private static final String PROP_ATOMIC_RESTORE = RestoreStmt.PROP_ATOMIC_RESTORE;
    private static final String PROP_FORCE_REPLACE = RestoreStmt.PROP_FORCE_REPLACE;
    private static final String ATOMIC_RESTORE_TABLE_PREFIX = "__doris_atomic_restore_prefix__";

    private static final Logger LOG = LogManager.getLogger(RestoreJob.class);

    // CHECKSTYLE OFF
    public enum RestoreJobState {
        PENDING, // Job is newly created. Check and prepare meta in catalog. Create replica if necessary.
                 // Waiting for replica creation finished synchronously, then sending snapshot tasks.
                 // then transfer to CREATING.
        CREATING, // Creating replica on BE. Transfer to SNAPSHOTING after all replicas created.
        SNAPSHOTING, // Waiting for snapshot finished. Than transfer to DOWNLOAD.
        DOWNLOAD, // Send download tasks.
        DOWNLOADING, // Waiting for download finished.
        COMMIT, // After download finished, all data is ready for taking effect.
                    // Send movement tasks to BE, than transfer to COMMITTING
        COMMITTING, // wait all tasks finished. Transfer to FINISHED
        FINISHED,
        CANCELLED
    }
    // CHECKSTYLE ON

    @SerializedName("bts")
    private String backupTimestamp;

    @SerializedName("j")
    protected BackupJobInfo jobInfo;
    @SerializedName("al")
    private boolean allowLoad;

    @SerializedName("st")
    protected volatile RestoreJobState state;

    @SerializedName("meta")
    private BackupMeta backupMeta;

    @SerializedName("fm")
    protected RestoreFileMapping fileMapping = new RestoreFileMapping();

    @SerializedName("mpt")
    protected long metaPreparedTime = -1;
    @SerializedName("sft")
    protected long snapshotFinishedTime = -1;
    @SerializedName("dft")
    protected long downloadFinishedTime = -1;

    @SerializedName("ra")
    private ReplicaAllocation replicaAlloc;

    private boolean reserveReplica = false;
    private boolean reserveDynamicPartitionEnable = false;
    private long createReplicasTimeStamp = -1;
    // this 2 members is to save all newly restored objs
    // tbl name -> part
    @SerializedName("rp")
    protected List<Pair<String, Partition>> restoredPartitions = Lists.newArrayList();
    @SerializedName("rt")
    protected List<Table> restoredTbls = Lists.newArrayList();
    @SerializedName("rr")
    protected List<Resource> restoredResources = Lists.newArrayList();

    // save all restored partitions' version info which are already exist in catalog
    // table id -> partition id -> (version, version hash)
    // use the streaming adapter to avoid materializing the huge JsonElement DOM tree
    // when persisting a job with a large number of tablets
    @SerializedName("rvi")
    @JsonAdapter(GsonUtils.GuavaTableTypeAdapterFactory.class)
    private com.google.common.collect.Table<Long, Long, Long> restoredVersionInfo = HashBasedTable.create();
    // tablet id->(be id -> snapshot info)
    @SerializedName("si")
    @JsonAdapter(GsonUtils.GuavaTableTypeAdapterFactory.class)
    protected com.google.common.collect.Table<Long, Long, SnapshotInfo> snapshotInfos = HashBasedTable.create();

    // The outcome of partition level reuse: the partitions that keep their local data, and the reason of each
    // decision. Null if partition level reuse is not enabled. Fixed when the replicas are created (it is in the edit
    // log written when entering DOWNLOAD), kept after the job finishes.
    @SerializedName("rru")
    private RestoreReuseResult reuseResult;
    // whether the master tracked the state tokens when it committed the job: the restored partitions adopt the
    // tokens of the backup only then, see stampStateToken. Set on the master before the stamp, persisted with the
    // finished job for the replay.
    @SerializedName("stt")
    private boolean stampStateTokens;

    // The ids of the staging partitions of an atomic restore whose data is loaded from a local source (kept or
    // incrementally appended partitions): the tablet scheduler must not repair their replicas by clone while the job
    // runs, a clone would replace the data the job loaded. Immutable, only replaced as a whole, so the tablet
    // scheduler reads it without any lock. Built in finishVerifying (before the snapshot tasks are sent), empty
    // before that and when the job is done, rebuilt from reuseResult when the job is replayed.
    private volatile Set<Long> localSourceStagingPartitions = Collections.emptySet();

    // The prefix of the error message of a backend which says that retrying the task cannot succeed.
    public static final String RESTORE_NOT_RETRYABLE = "[RESTORE_NOT_RETRYABLE]";
    // The version of the restore reuse protocol of this build, reported in the heartbeat of the frontend and the
    // backend. A node of an old version reports none (0). 1: the local source snapshot and the load of a kept
    // partition by the snapshot task (TSnapshotRequest.restore_local_source, restore_kept_load).
    public static final int RESTORE_REUSE_CAPABILITY_REPORTED = 1;
    // What partition level reuse needs of every node: 1, the local source snapshot, which an old node does not
    // understand.
    public static final int RESTORE_REUSE_CAPABILITY_MIN_REQUIRED = 1;
    // The reason of downloading all the data when a node does not have the capability.
    public static final String MIXED_VERSIONS = "MIXED_VERSIONS";
    // The reason of downloading a partition whose state token equals the one of the backup, when the token is not
    // trusted (restore_reuse_trust_state_token is false): only counted, see selectReuseCandidates.
    private static final String TOKEN_NOT_TRUSTED = "TOKEN_NOT_TRUSTED";
    // The working state of partition level reuse while PENDING and CREATING, never persisted: the job is redone
    // from PENDING after a restart or a master switch.
    private transient List<ReuseCandidate> reuseCandidates = Lists.newArrayList();
    // What the backup meta carried before it was cleared: backup table name -> its table, for the structure checks.
    private transient Map<String, BackupStamps> backupStamps = Maps.newHashMap();

    // the meta version is used when reading backup meta from file.
    // we do not persist this field, because this is just a temporary solution.
    // the true meta version should be get from backup job info, which is saved when doing backup job.
    // But the earlier version of Doris do not save the meta version in backup job info, so we allow user to
    // set this 'metaVersion' in restore stmt.
    // NOTICE: because we do not persist it, this info may be lost if Frontend restart,
    // and if you don't want to losing it, backup your data again by using latest Doris version.
    private int metaVersion = -1;

    private boolean isBeingSynced = false;

    // Whether to delete existing tables that are not involved in the restore.
    private boolean isCleanTables = false;
    // Whether to delete existing partitions that are not involved in the restore.
    private boolean isCleanPartitions = false;
    // Whether to restore the data into a temp table, and then replace the origin one.
    protected boolean isAtomicRestore = false;
    // Whether to restore the table by replacing the exists but conflicted table.
    protected boolean isForceReplace = false;

    // restore properties
    @SerializedName("prop")
    protected Map<String, String> properties = Maps.newHashMap();

    private Map<Long, AgentBoundedBatchTask> batchTaskPerTable = new HashMap<>();

    private MarkedCountDownLatch<Long, Long> createReplicaTasksLatch = null;

    public RestoreJob() {
        super(JobType.RESTORE);
    }

    public RestoreJob(JobType jobType) {
        super(jobType);
    }

    public RestoreJob(String label, String backupTs, long dbId, String dbName, BackupJobInfo jobInfo, boolean allowLoad,
            ReplicaAllocation replicaAlloc, long timeoutMs, int metaVersion, boolean reserveReplica,
            boolean reserveDynamicPartitionEnable, boolean isBeingSynced, boolean isCleanTables,
            boolean isCleanPartitions, boolean isAtomicRestore, boolean isForceReplace, Env env, long repoId) {
        super(JobType.RESTORE, label, dbId, dbName, timeoutMs, env, repoId);
        this.backupTimestamp = backupTs;
        this.jobInfo = jobInfo;
        this.allowLoad = allowLoad;
        this.replicaAlloc = replicaAlloc;
        this.state = RestoreJobState.PENDING;
        this.metaVersion = metaVersion;
        this.reserveReplica = reserveReplica;
        // if backup snapshot is come from a cluster with force replication allocation,
        // ignore the origin allocation
        if (jobInfo.isForceReplicationAllocation) {
            this.reserveReplica = false;
        }
        this.reserveDynamicPartitionEnable = reserveDynamicPartitionEnable;
        this.isBeingSynced = isBeingSynced;
        this.isCleanTables = isCleanTables;
        this.isCleanPartitions = isCleanPartitions;
        this.isAtomicRestore = isAtomicRestore;
        if (this.isAtomicRestore) {
            this.isForceReplace = isForceReplace;
        }
        properties.put(PROP_RESERVE_REPLICA, String.valueOf(reserveReplica));
        properties.put(PROP_RESERVE_DYNAMIC_PARTITION_ENABLE, String.valueOf(reserveDynamicPartitionEnable));
        properties.put(PROP_IS_BEING_SYNCED, String.valueOf(isBeingSynced));
        properties.put(PROP_CLEAN_TABLES, String.valueOf(isCleanTables));
        properties.put(PROP_CLEAN_PARTITIONS, String.valueOf(isCleanPartitions));
        properties.put(PROP_ATOMIC_RESTORE, String.valueOf(isAtomicRestore));
        properties.put(PROP_FORCE_REPLACE, String.valueOf(isForceReplace));
    }

    public RestoreJob(String label, String backupTs, long dbId, String dbName, BackupJobInfo jobInfo, boolean allowLoad,
            ReplicaAllocation replicaAlloc, long timeoutMs, int metaVersion, boolean reserveReplica,
            boolean reserveDynamicPartitionEnable, boolean isBeingSynced, boolean isCleanTables,
            boolean isCleanPartitions, boolean isAtomicRestore, boolean isForceReplace, Env env, long repoId,
            BackupMeta backupMeta) {
        this(label, backupTs, dbId, dbName, jobInfo, allowLoad, replicaAlloc, timeoutMs, metaVersion, reserveReplica,
                reserveDynamicPartitionEnable, isBeingSynced, isCleanTables, isCleanPartitions, isAtomicRestore,
                isForceReplace, env,
                repoId);
        this.backupMeta = backupMeta;
    }

    public boolean isFromLocalSnapshot() {
        return repoId == Repository.KEEP_ON_LOCAL_REPO_ID;
    }

    public RestoreJobState getState() {
        return state;
    }

    public int getMetaVersion() {
        return metaVersion;
    }

    public boolean isBeingSynced() {
        return isBeingSynced;
    }

    public synchronized boolean finishTabletSnapshotTask(SnapshotTask task, TFinishTaskRequest request) {
        if (task.isRestoreLocalSource() && task.getJobId() == jobId
                && cancelOnLocalSourceTaskFailure(task, request, RestoreJobState.SNAPSHOTING)) {
            return false;
        }
        if (checkTaskStatus(task, task.getJobId(), request)) {
            return false;
        }

        Preconditions.checkState(request.isSetSnapshotPath());

        // snapshot path does not contains last 'tablet_id' and 'schema_hash' dir
        // eg:
        // /path/to/your/be/data/snapshot/20180410102311.0/
        // Full path will look like:
        // /path/to/your/be/data/snapshot/20180410102311.0/10006/352781111/
        SnapshotInfo info = new SnapshotInfo(task.getDbId(), task.getTableId(), task.getPartitionId(),
                task.getIndexId(), task.getTabletId(), task.getBackendId(),
                task.getSchemaHash(), request.getSnapshotPath(), Lists.newArrayList());

        snapshotInfos.put(task.getTabletId(), task.getBackendId(), info);
        taskProgress.remove(task.getSignature());
        Long removedTabletId = unfinishedSignatureToId.remove(task.getSignature());
        if (removedTabletId != null) {
            taskErrMsg.remove(task.getSignature());
            Preconditions.checkState(task.getTabletId() == removedTabletId, removedTabletId);
            if (LOG.isDebugEnabled()) {
                LOG.debug("get finished snapshot info: {}, unfinished tasks num: {}, remove result: {}. {}",
                          info, unfinishedSignatureToId.size(), this, removedTabletId);
            }
            nudgeIfIdle(unfinishedSignatureToId.isEmpty());
            return true;
        }
        return false;
    }


    public synchronized boolean finishTabletDownloadTask(DownloadTask task, TFinishTaskRequest request) {
        if (checkTaskStatus(task, task.getJobId(), request)) {
            return false;
        }

        Preconditions.checkState(request.isSetDownloadedTabletIds());

        for (Long tabletId : request.getDownloadedTabletIds()) {
            SnapshotInfo info = snapshotInfos.get(tabletId, task.getBackendId());
            if (info == null) {
                LOG.warn("failed to find snapshot infos of tablet {} in be {}, {}",
                          tabletId, task.getBackendId(), this);
                return false;
            }
        }

        taskProgress.remove(task.getSignature());
        Long beId = unfinishedSignatureToId.remove(task.getSignature());
        if (beId == null || beId != task.getBackendId()) {
            LOG.warn("invalid download task: {}. {}", task, this);
            return false;
        }

        taskErrMsg.remove(task.getSignature());
        nudgeIfIdle(unfinishedSignatureToId.isEmpty());
        return true;
    }

    public synchronized boolean finishDirMoveTask(DirMoveTask task, TFinishTaskRequest request) {
        if ((task.getIncrementalRange() != null || task.isLocalSourceKept()) && task.isLocalSourceLoaded()
                && task.getJobId() == jobId
                && cancelOnLocalSourceTaskFailure(task, request, RestoreJobState.COMMITTING)) {
            return false;
        }
        if (checkTaskStatus(task, task.getJobId(), request)) {
            return false;
        }

        taskProgress.remove(task.getSignature());
        Long tabletId = unfinishedSignatureToId.remove(task.getSignature());
        if (tabletId == null || tabletId != task.getTabletId()) {
            LOG.warn("invalid dir move task: {}. {}", task, this);
            return false;
        }

        taskErrMsg.remove(task.getSignature());
        nudgeIfIdle(unfinishedSignatureToId.isEmpty());
        return true;
    }

    /**
     * The failure of a task of the local source (the snapshot task which loads the local source into a staging
     * tablet, the move task which appends the increment or confirms a kept tablet): other tasks are resent without
     * end, but these cannot
     * succeed forever if the tablet is not what the job expects, so the job is cancelled (an atomic restore leaves
     * the original table alone) when
     *  - the backend says that retrying is useless (the error has RESTORE_NOT_RETRYABLE), or
     *  - the task failed more than Config.restore_local_source_task_max_retry times. A failure to obtain the tablet
     *    locks is not counted: it is retried at once by design.
     * The failure of the other tasks (whole snapshot, download, move) is not touched.
     *
     * @return true if the job is cancelled
     */
    private boolean cancelOnLocalSourceTaskFailure(AgentTask task, TFinishTaskRequest request,
                                                   RestoreJobState expectedState) {
        if (request.getTaskStatus().getStatusCode() == TStatusCode.OK) {
            return false;
        }
        String errMsg = request.getTaskStatus().isSetErrorMsgs()
                ? Joiner.on(",").join(request.getTaskStatus().getErrorMsgs()) : "";
        if (request.getTaskStatus().getStatusCode() == TStatusCode.OBTAIN_LOCK_FAILED) {
            // not counted
            task.revertFailed();
            return false;
        }
        boolean notRetryable = errMsg.contains(RESTORE_NOT_RETRYABLE);
        if (!notRetryable && task.getFailedTimes() <= Config.restore_local_source_task_max_retry) {
            return false;
        }
        taskErrMsg.put(task.getSignature(), errMsg);
        if (state != expectedState) {
            LOG.warn("ignore the failure of {} task {} in state {}: {}. {}", task.getTaskType(), task.getSignature(),
                    state, errMsg, this);
            return false;
        }
        LOG.warn("cancel restore job, {} task {} of tablet {} failed {} times on backend {}{}: {}. {}",
                task.getTaskType(), task.getSignature(), task.getTabletId(), task.getFailedTimes(),
                task.getBackendId(), notRetryable ? ", retrying is useless" : "", errMsg, this);
        status = new Status(ErrCode.COMMON_ERROR, (notRetryable
                ? "the backend " + task.getBackendId() + " cannot go on with tablet " + task.getTabletId()
                : task.getTaskType() + " task of tablet " + task.getTabletId() + " failed "
                        + task.getFailedTimes() + " times on backend " + task.getBackendId())
                + ": " + errMsg);
        cancelInternal(false);
        return true;
    }

    private boolean checkTaskStatus(AgentTask task, long jobId, TFinishTaskRequest request) {
        Preconditions.checkState(jobId == this.jobId);
        Preconditions.checkState(dbId == task.getDbId());

        if (request.getTaskStatus().getStatusCode() != TStatusCode.OK) {
            taskErrMsg.put(task.getSignature(), Joiner.on(",").join(request.getTaskStatus().getErrorMsgs()));
            return true;
        }
        return false;
    }

    @Override
    public synchronized void replayRun() {
        LOG.info("replay run restore job: {}", this);
        switch (state) {
            case DOWNLOAD:
                replayCheckAndPrepareMeta();
                break;
            case FINISHED:
                replayWaitingAllTabletsCommitted();
                break;
            default:
                break;
        }
    }

    @Override
    public synchronized void replayCancel() {
        cancelInternal(true /* is replay */);
    }

    @Override
    public boolean isPending() {
        return state == RestoreJobState.PENDING;
    }

    @Override
    public boolean isCancelled() {
        return state == RestoreJobState.CANCELLED;
    }

    @Override
    public boolean isFinished() {
        return state == RestoreJobState.FINISHED;
    }

    @Override
    public synchronized Status updateRepo(Repository repo) {
        this.repo = repo;

        if (this.state == RestoreJobState.DOWNLOADING) {
            for (Map.Entry<Long, Long> entry : unfinishedSignatureToId.entrySet()) {
                long signature = entry.getKey();
                long beId = entry.getValue();
                AgentTask task = AgentTaskQueue.getTask(beId, TTaskType.DOWNLOAD, signature);
                if (task == null || task.getTaskType() != TTaskType.DOWNLOAD) {
                    continue;
                }
                ((DownloadTask) task).updateBrokerProperties(
                        repo.getRemoteFileSystem().getStorageProperties().getBackendConfigProperties());
                AgentTaskQueue.updateTask(beId, TTaskType.DOWNLOAD, signature, task);
            }
            LOG.info("finished to update download job properties. {}", this);
        }
        LOG.info("finished to update repo of job. {}", this);
        return Status.OK;
    }

    @Override
    public synchronized void run() {
        if (state == RestoreJobState.FINISHED || state == RestoreJobState.CANCELLED) {
            return;
        }

        if (System.currentTimeMillis() - createTime > timeoutMs) {
            status = new Status(ErrCode.TIMEOUT, "restore job with label: " + label + "  timeout.");
            cancelInternal(false);
            return;
        }

        // get repo if not set
        if (repo == null && !isFromLocalSnapshot()) {
            repo = env.getBackupHandler().getRepoMgr().getRepo(repoId);
            if (repo == null) {
                status = new Status(ErrCode.COMMON_ERROR, "failed to get repository: " + repoId);
                cancelInternal(false);
                return;
            }
        }

        LOG.info("run restore job: {}", this);

        // With backup_restore_merge_idle_ticks the next state is run in this round as well when the state reached
        // waits for nothing (the cancel check and the pause of the debug point are done before every state).
        int steps = 0;
        do {
            RestoreJobState before = state;
            checkIfNeedCancel();

            if (status.ok()) {
                if (state != RestoreJobState.PENDING && state != RestoreJobState.CREATING
                        && label.equals(DebugPointUtil.getDebugParamOrDefault("FE.PAUSE_NON_PENDING_RESTORE_JOB",
                                ""))) {
                    LOG.info("pause restore job by debug point: {}", this);
                    return;
                }

                switch (state) {
                    case PENDING:
                        checkAndPrepareMeta();
                        break;
                    case CREATING:
                        waitingAllReplicasCreated();
                        break;
                    case SNAPSHOTING:
                        waitingAllSnapshotsFinished();
                        break;
                    case DOWNLOAD:
                        downloadSnapshots();
                        break;
                    case DOWNLOADING:
                        waitingAllDownloadFinished();
                        break;
                    case COMMIT:
                        commit();
                        break;
                    case COMMITTING:
                        waitingAllTabletsCommitted();
                        break;
                    default:
                        break;
                }
            }
            if (!Config.backup_restore_merge_idle_ticks || !status.ok() || state == before || isWaitingState()
                    || ++steps >= MAX_MERGED_STEPS_RESTORE) {
                break;
            }
            LOG.info("go on with state {} in the same round. {}", state, this);
        } while (true);

        if (!status.ok()) {
            cancelInternal(false);
        }
    }

    // Whether the state waits for the backends (or is done): the next round looks again, nothing to run now.
    // CREATING polls the create replica tasks every round as before.
    private boolean isWaitingState() {
        switch (state) {
            case CREATING:
            case FINISHED:
            case CANCELLED:
                return true;
            case SNAPSHOTING:
            case DOWNLOADING:
            case COMMITTING:
                return !unfinishedSignatureToId.isEmpty();
            default:
                return false;
        }
    }

    /**
     * return true if some restored objs have been dropped.
     */
    protected void checkIfNeedCancel() {
        if (state == RestoreJobState.PENDING || state == RestoreJobState.CREATING) {
            return;
        }

        Database db = env.getInternalCatalog().getDbNullable(dbId);
        if (db == null) {
            status = new Status(ErrCode.NOT_FOUND, "database " + dbId + " has been dropped");
            return;
        }

        for (IdChain idChain : fileMapping.getMapping().keySet()) {
            OlapTable tbl = (OlapTable) db.getTableNullable(idChain.getTblId());
            if (tbl == null) {
                status = new Status(ErrCode.NOT_FOUND, "table " + idChain.getTblId() + " has been dropped");
                return;
            }
            tbl.readLock();
            try {
                Partition part = tbl.getPartition(idChain.getPartId());
                if (part == null) {
                    status = new Status(ErrCode.NOT_FOUND, "partition " + idChain.getPartId() + " has been dropped");
                    return;
                }

                MaterializedIndex index = part.getIndex(idChain.getIdxId());
                if (index == null) {
                    status = new Status(ErrCode.NOT_FOUND, "index " + idChain.getIdxId() + " has been dropped");
                    return;
                }
            } finally {
                tbl.readUnlock();
            }
        }
    }

    /**
     * Restore rules as follow:
     * OlapTable
     * A. Table already exist
     *      A1. Partition already exist, generate file mapping
     *      A2. Partition does not exist, add restored partition to the table.
     *          Reset all index/tablet/replica id, and create replica on BE outside the table lock.
     * B. Table does not exist
     *      B1. Add table to the db, reset all table/index/tablet/replica id,
     *          and create replica on BE outside the db lock.
     * View
     *      * A. View already exist. The same signature is allowed.
     *      * B. View does not exist.
     * All newly created table/partition/index/tablet/replica should be saved for rolling back.
     *
     * Step:
     * 1. download and deserialize backup meta from repository.
     * 2. set all existing restored table's state to RESTORE.
     * 3. check if the expected restore objs are valid.
     * 4. create replicas if necessary.
     * 5. add restored objs to catalog.
     * 6. make snapshot for all replicas for incremental download later.
     */
    private void checkAndPrepareMeta() {
        Database db = env.getInternalCatalog().getDbNullable(dbId);
        if (db == null) {
            status = new Status(ErrCode.NOT_FOUND, "database " + dbId + " does not exist");
            return;
        }

        // generate job id
        jobId = env.getNextId();

        // deserialize meta
        if (!downloadAndDeserializeMetaInfo()) {
            return;
        }
        Preconditions.checkNotNull(backupMeta);

        // Must be done before any local partition is modified by this job.
        captureBackupStamps();
        // The backup meta carries the state token of the source partitions, which must not be inherited.
        clearRestoreLineageInBackupMeta();

        // Check the olap table state.
        //
        // If isAtomicRestore is not set, set all restored tbls' state to RESTORE,
        // the table's origin state must be NORMAL and does not have unfinished load job.
        for (String tableName : jobInfo.backupOlapTableObjects.keySet()) {
            Table tbl = db.getTableNullable(jobInfo.getAliasByOriginNameIfSet(tableName));
            if (tbl == null) {
                continue;
            }

            if (tbl.getType() != TableType.OLAP) {
                status = new Status(ErrCode.COMMON_ERROR, "Only support restore OLAP table: " + tbl.getName());
                return;
            }

            OlapTable olapTbl = (OlapTable) tbl;
            if (!olapTbl.writeLockIfExist()) {
                continue;
            }
            try {
                if (olapTbl.getState() != OlapTableState.NORMAL) {
                    status = new Status(ErrCode.COMMON_ERROR,
                            "Table " + tbl.getName() + "'s state is not NORMAL: " + olapTbl.getState().name());
                    return;
                }

                if (olapTbl.existTempPartitions()) {
                    status = new Status(ErrCode.COMMON_ERROR, "Do not support restoring table with temp partitions");
                    return;
                }

                if (isAtomicRestore) {
                    // We will create new OlapTable in atomic restore, so does not set the RESTORE state.
                    // Instead, set table in atomic restore state, to forbid the alter table operation.
                    olapTbl.setInAtomicRestore();
                    continue;
                }

                olapTbl.setState(OlapTableState.RESTORE);
                // set restore status for partitions
                BackupOlapTableInfo tblInfo = jobInfo.backupOlapTableObjects.get(tableName);
                for (Map.Entry<String, BackupPartitionInfo> partitionEntry : tblInfo.partitions.entrySet()) {
                    String partitionName = partitionEntry.getKey();
                    Partition partition = olapTbl.getPartition(partitionName);
                    if (partition == null) {
                        continue;
                    }
                    partition.setState(PartitionState.RESTORE);
                }
            } finally {
                olapTbl.writeUnlock();
            }
        }

        for (BackupJobInfo.BackupViewInfo backupViewInfo : jobInfo.newBackupObjects.views) {
            Table tbl = db.getTableNullable(jobInfo.getAliasByOriginNameIfSet(backupViewInfo.name));
            if (tbl == null) {
                continue;
            }
            if (tbl.getType() != TableType.VIEW) {
                status = new Status(ErrCode.COMMON_ERROR,
                        "The local table " + tbl.getName()
                                + " with the same name but a different type of backup meta.");
                return;
            }
        }
        for (BackupJobInfo.BackupOdbcTableInfo backupOdbcTableInfo : jobInfo.newBackupObjects.odbcTables) {
            Table tbl = db.getTableNullable(jobInfo.getAliasByOriginNameIfSet(backupOdbcTableInfo.dorisTableName));
            if (tbl == null) {
                continue;
            }
            if (tbl.getType() != TableType.ODBC) {
                status = new Status(ErrCode.COMMON_ERROR,
                        "The local table " + tbl.getName()
                                + " with the same name but a different type of backup meta.");
                return;
            }
        }
        for (BackupJobInfo.BackupOdbcResourceInfo backupOdbcResourceInfo : jobInfo.newBackupObjects.odbcResources) {
            Resource resource = Env.getCurrentEnv().getResourceMgr().getResource(backupOdbcResourceInfo.name);
            if (resource == null) {
                continue;
            }
            if (resource.getType() != Resource.ResourceType.ODBC_CATALOG) {
                status = new Status(ErrCode.COMMON_ERROR,
                        "The local resource " + resource.getName()
                                + " with the same name but a different type of backup meta.");
                return;
            }
        }
        // the new tablets -> { local tablet, schema hash, storage medium }, used in atomic restore.
        Map<Long, TabletRef> tabletBases = new HashMap<>();
        // the staging tables of atomic restore whose tablets are bound to the tablets of the table being replaced,
        // by the name of the table in the backup
        Map<String, OlapTable> atomicStaging = new HashMap<>();

        // Check and prepare meta objects.

        // The tables that are restored but not committed, because the table name may be changed.
        List<Table> stagingRestoreTables = Lists.newArrayList();
        db.readLock();
        try {
            for (Map.Entry<String, BackupOlapTableInfo> olapTableEntry : jobInfo.backupOlapTableObjects.entrySet()) {
                String tableName = olapTableEntry.getKey();
                BackupOlapTableInfo tblInfo = olapTableEntry.getValue();
                Table remoteTbl = backupMeta.getTable(tableName);
                Preconditions.checkNotNull(remoteTbl);
                Table localTbl = db.getTableNullable(jobInfo.getAliasByOriginNameIfSet(tableName));
                boolean isSchemaChanged = false;
                if (localTbl != null && localTbl.getType() != TableType.OLAP) {
                    // table already exist, but is not OLAP
                    status = new Status(ErrCode.COMMON_ERROR,
                            "The type of local table should be same as type of remote table: "
                                    + remoteTbl.getName());
                    return;
                }

                if (localTbl != null) {
                    OlapTable localOlapTbl = (OlapTable) localTbl;
                    OlapTable remoteOlapTbl = (OlapTable) remoteTbl;

                    localOlapTbl.readLock();
                    try {
                        List<String> intersectPartNames = Lists.newArrayList();
                        Status st = localOlapTbl.getIntersectPartNamesWith(remoteOlapTbl, intersectPartNames);
                        if (!st.ok()) {
                            if (isForceReplace) {
                                LOG.info("{}, will force replace, job: {}",
                                        st.getErrMsg(), this);
                                isSchemaChanged = true;
                            } else {
                                status = st;
                                return;
                            }
                        }
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("get intersect part names: {}, job: {}", intersectPartNames, this);
                        }
                        String localTblSignature = localOlapTbl.getSignature(
                                BackupHandler.SIGNATURE_VERSION, intersectPartNames);
                        String remoteTblSignature = remoteOlapTbl.getSignature(
                                BackupHandler.SIGNATURE_VERSION, intersectPartNames);
                        if (!localTblSignature.equals(remoteTblSignature)) {
                            if (isForceReplace) {
                                LOG.info("Table {} already exists but with different schema, will force replace, "
                                        + "local table: {}, remote table: {}",
                                        tableName, localTblSignature, remoteTblSignature);
                                isSchemaChanged = true;
                            } else {
                                String alias = jobInfo.getAliasByOriginNameIfSet(tableName);
                                LOG.warn("Table {} already exists but with different schema, "
                                        + "local table: {}, remote table: {}",
                                        alias, localTblSignature, remoteTblSignature);
                                status = new Status(ErrCode.COMMON_ERROR, "Table "
                                        + alias + " already exist but with different schema");
                                return;
                            }
                        }

                        checkStorageVault(localOlapTbl);
                        if (!status.ok()) {
                            return;
                        }

                        // Table with same name and has same schema. Check partition
                        for (Map.Entry<String, BackupPartitionInfo> partitionEntry : tblInfo.partitions.entrySet()) {
                            String partitionName = partitionEntry.getKey();
                            BackupPartitionInfo backupPartInfo = partitionEntry.getValue();
                            Partition localPartition = localOlapTbl.getPartition(partitionName);
                            Partition remotePartition = remoteOlapTbl.getPartition(partitionName);
                            if (localPartition != null) {
                                // Partition already exist.
                                if (Config.isCloudMode()) {
                                    // cloud mode don't allow restoring on existing partition now.
                                    status = new Status(ErrCode.COMMON_ERROR, "cloud mode don't allow restore"
                                            + " on a exising partition now. local table: " + localTbl.getName()
                                            + " restore partition: " + partitionName);
                                    return;
                                }
                                PartitionInfo localPartInfo = localOlapTbl.getPartitionInfo();
                                PartitionInfo remotePartInfo = remoteOlapTbl.getPartitionInfo();
                                ReplicaAllocation remoteReplicaAlloc = remotePartInfo.getReplicaAllocation(
                                        remotePartition.getId());
                                if (localPartInfo.getType() == PartitionType.RANGE
                                        || localPartInfo.getType() == PartitionType.LIST) {
                                    PartitionItem localItem = localPartInfo.getItem(localPartition.getId());
                                    PartitionItem remoteItem = remoteOlapTbl
                                            .getPartitionInfo().getItem(backupPartInfo.id);
                                    if (!localItem.equals(remoteItem)) {
                                        // Same partition name, different range
                                        if (isForceReplace) {
                                            isSchemaChanged = true;
                                        } else {
                                            status = new Status(ErrCode.COMMON_ERROR, "Partition " + partitionName
                                            + " in table " + localTbl.getName()
                                            + " has different partition item with partition in repository");
                                            return;
                                        }
                                    }
                                }

                                if (isAtomicRestore) {
                                    // skip gen file mapping for atomic restore.
                                    continue;
                                }

                                // Same partition, same range or a single partitioned table.
                                if (genFileMappingWhenBackupReplicasEqual(localPartInfo, localPartition,
                                        localTbl, backupPartInfo, partitionName, tblInfo, remoteReplicaAlloc)) {
                                    return;
                                }
                            } else if (!isAtomicRestore) {
                                // partitions does not exist
                                PartitionInfo localPartitionInfo = localOlapTbl.getPartitionInfo();
                                if (localPartitionInfo.getType() == PartitionType.RANGE
                                        || localPartitionInfo.getType() == PartitionType.LIST) {
                                    PartitionItem remoteItem = remoteOlapTbl.getPartitionInfo()
                                            .getItem(backupPartInfo.id);
                                    if (localPartitionInfo.getAnyIntersectItem(remoteItem, false) != null) {
                                        status = new Status(ErrCode.COMMON_ERROR, "Partition " + partitionName
                                                + " in table " + localTbl.getName()
                                                + " has conflict partition item with existing items");
                                        return;
                                    } else {
                                        // this partition can be added to this table, set ids
                                        ReplicaAllocation restoreReplicaAlloc = replicaAlloc;
                                        if (reserveReplica) {
                                            PartitionInfo remotePartInfo = remoteOlapTbl.getPartitionInfo();
                                            restoreReplicaAlloc = remotePartInfo.getReplicaAllocation(
                                                remotePartition.getId());
                                        }
                                        Partition restorePart = resetPartitionForRestore(localOlapTbl, remoteOlapTbl,
                                                partitionName,
                                                restoreReplicaAlloc);
                                        if (restorePart == null) {
                                            return;
                                        }
                                        restoredPartitions.add(Pair.of(localOlapTbl.getName(), restorePart));
                                    }
                                } else {
                                    // It is impossible that a single partitioned table exist
                                    // without any existing partition
                                    status = new Status(ErrCode.COMMON_ERROR,
                                            "No partition exist in single partitioned table " + localOlapTbl.getName());
                                    return;
                                }
                            }
                        }
                    } finally {
                        localOlapTbl.readUnlock();
                    }
                }

                // Table does not exist or atomic restore
                if (localTbl == null || isAtomicRestore) {
                    OlapTable remoteOlapTbl = (OlapTable) remoteTbl;
                    // Retain only expected restore partitions in this table;
                    Set<String> allPartNames = remoteOlapTbl.getPartitionNames();
                    for (String partName : allPartNames) {
                        if (!tblInfo.containsPart(partName)) {
                            remoteOlapTbl.dropPartitionAndReserveTablet(partName);
                        }
                    }

                    // reset all ids in this table
                    String srcDbName = jobInfo.dbName;
                    Status st = remoteOlapTbl.resetIdsForRestore(env, db, replicaAlloc, reserveReplica, srcDbName);
                    if (!st.ok()) {
                        status = st;
                        return;
                    }

                    // reset next version to visible version + 1 for all partitions
                    remoteOlapTbl.resetVersionForRestore();

                    // Reset properties to correct values.
                    remoteOlapTbl.resetPropertiesForRestore(reserveDynamicPartitionEnable, reserveReplica,
                                                            replicaAlloc, isBeingSynced);

                    // DO NOT set remote table's new name here, cause we will still need the origin name later
                    // remoteOlapTbl.setName(jobInfo.getAliasByOriginNameIfSet(tblInfo.name));
                    remoteOlapTbl.setState(allowLoad ? OlapTableState.RESTORE_WITH_LOAD : OlapTableState.RESTORE);

                    if (isAtomicRestore && localTbl != null && !isSchemaChanged) {
                        // bind the backends and base tablets from local tbl.
                        status = bindLocalAndRemoteOlapTableReplicas((OlapTable) localTbl, remoteOlapTbl, tabletBases);
                        if (!status.ok()) {
                            return;
                        }
                        atomicStaging.put(tableName, remoteOlapTbl);
                    }
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("put remote table {} to restoredTbls", remoteOlapTbl.getName());
                    }
                    stagingRestoreTables.add(remoteOlapTbl);
                }
            } // end of all restore olap tables

            // restore views
            for (BackupJobInfo.BackupViewInfo backupViewInfo : jobInfo.newBackupObjects.views) {
                String backupViewName = backupViewInfo.name;
                Table localTbl = db.getTableNullable(jobInfo.getAliasByOriginNameIfSet(backupViewName));
                View remoteView = (View) backupMeta.getTable(backupViewName);
                if (localTbl != null) {
                    Preconditions.checkState(localTbl.getType() == TableType.VIEW);
                    View localView = (View) localTbl;
                    String localViewSignature = localView.getSignature(BackupHandler.SIGNATURE_VERSION);
                    // keep compatible with old version, compare the signature without reset view def
                    if (!localViewSignature.equals(remoteView.getSignature(BackupHandler.SIGNATURE_VERSION))) {
                        // reset view def to dest db name and compare signature again
                        String srcDbName = jobInfo.dbName;
                        remoteView.resetViewDefForRestore(srcDbName, db.getName());
                        if (!localViewSignature.equals(remoteView.getSignature(BackupHandler.SIGNATURE_VERSION))) {
                            if (isForceReplace) {
                                LOG.info("View {} already exist but with different schema, will force replace, "
                                        + "local view: {}, remote view: {}",
                                        backupViewName, localViewSignature,
                                        remoteView.getSignature(BackupHandler.SIGNATURE_VERSION));
                            } else {
                                LOG.warn("View {} already exist but with different schema, will force replace, "
                                        + "local view: {}, remote view: {}",
                                        backupViewName, localViewSignature,
                                        remoteView.getSignature(BackupHandler.SIGNATURE_VERSION));
                                status = new Status(ErrCode.COMMON_ERROR, "View "
                                        + jobInfo.getAliasByOriginNameIfSet(backupViewName)
                                        + " already exist but with different schema");
                                return;
                            }
                        }
                    }
                }
                if (localTbl == null || isAtomicRestore) {
                    String srcDbName = jobInfo.dbName;
                    remoteView.resetViewDefForRestore(srcDbName, db.getName());
                    remoteView.resetIdsForRestore(env);
                    stagingRestoreTables.add(remoteView);
                }
            }

            // restore odbc external table
            for (BackupJobInfo.BackupOdbcTableInfo backupOdbcTableInfo : jobInfo.newBackupObjects.odbcTables) {
                String backupOdbcTableName = backupOdbcTableInfo.dorisTableName;
                Table localTbl = db.getTableNullable(jobInfo.getAliasByOriginNameIfSet(backupOdbcTableName));
                OdbcTable remoteOdbcTable = (OdbcTable) backupMeta.getTable(backupOdbcTableName);
                if (localTbl != null) {
                    Preconditions.checkState(localTbl.getType() == TableType.ODBC);
                    OdbcTable localOdbcTable = (OdbcTable) localTbl;
                    if (!localOdbcTable.getSignature(BackupHandler.SIGNATURE_VERSION)
                            .equals(remoteOdbcTable.getSignature(BackupHandler.SIGNATURE_VERSION))) {
                        status = new Status(ErrCode.COMMON_ERROR, "Odbc table "
                                + jobInfo.getAliasByOriginNameIfSet(backupOdbcTableName)
                                + " already exist but with different schema");
                        return;
                    }
                } else {
                    remoteOdbcTable.resetIdsForRestore(env);
                    stagingRestoreTables.add(remoteOdbcTable);
                }
            }

            if (LOG.isDebugEnabled()) {
                LOG.debug("finished to prepare restored partitions and tables. {}", this);
            }
            // for now, nothing is modified in catalog

            if (isAtomicRestore && !restoredPartitions.isEmpty()) {
                throw new RuntimeException("atomic restore is set, but the restored partitions is not empty");
            }

            // generate create replica tasks for all restored partitions
            for (Pair<String, Partition> entry : restoredPartitions) {
                OlapTable localTbl = (OlapTable) db.getTableNullable(entry.first);
                Preconditions.checkNotNull(localTbl, localTbl.getName());
                Partition restorePart = entry.second;
                OlapTable remoteTbl = (OlapTable) backupMeta.getTable(entry.first);
                BackupPartitionInfo backupPartitionInfo
                        = jobInfo.getOlapTableInfo(entry.first).getPartInfo(restorePart.getName());
                createReplicas(db, localTbl, restorePart);
                genFileMapping(localTbl, restorePart, remoteTbl.getId(), backupPartitionInfo,
                        !allowLoad /* if allow load, do not overwrite when commit */);
            }

            // generate create replica task for all restored tables
            for (Table restoreTbl : stagingRestoreTables) {
                if (restoreTbl.getType() == TableType.OLAP) {
                    OlapTable restoreOlapTable = (OlapTable) restoreTbl;
                    for (Partition restorePart : restoreOlapTable.getPartitions()) {
                        createReplicas(db, restoreOlapTable, restorePart, tabletBases);
                        BackupOlapTableInfo backupOlapTableInfo = jobInfo.getOlapTableInfo(restoreOlapTable.getName());
                        genFileMapping(restoreOlapTable, restorePart, backupOlapTableInfo.id,
                                backupOlapTableInfo.getPartInfo(restorePart.getName()),
                                !allowLoad /* if allow load, do not overwrite when commit */,
                                tabletBases);
                    }
                }
                // set restored table's new name after all 'genFileMapping'
                String tableName = jobInfo.getAliasByOriginNameIfSet(restoreTbl.getName());
                if (Env.isStoredTableNamesLowerCase()) {
                    tableName = tableName.toLowerCase();
                }
                if ((restoreTbl.getType() == TableType.OLAP || restoreTbl
                        .getType() == TableType.VIEW) && isAtomicRestore) {
                    tableName = tableAliasWithAtomicRestore(tableName);
                }
                restoreTbl.setName(tableName);
                restoredTbls.add(restoreTbl);
            }

            if (LOG.isDebugEnabled()) {
                LOG.debug("finished to generate create replica tasks. {}", this);
            }
        } finally {
            db.readUnlock();
        }

        if (!status.ok()) {
            return;
        }

        // The data of the existing partitions to overwrite is about to change. Their state token must not outlive
        // the data it stands for, e.g. if the job is cancelled after some tablets are moved in COMMITTING.
        // This is persisted with the DOWNLOAD edit log (replayed by replayCheckAndPrepareMeta), which is
        // written before any tablet data is moved, and the token is written again in allTabletCommitted.
        // The candidates of partition level reuse keep their token until they are decided when the replicas are
        // created, nothing local is changed before that.
        Set<Pair<Long, Long>> candidateKeys = selectReuseCandidates(db, atomicStaging, tabletBases);
        invalidateRestoreLineageOfOverwrittenPartitions(db, candidateKeys);

        // check and restore resources
        checkAndRestoreResources();
        if (!status.ok()) {
            return;
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("finished to restore resources. {}", this.jobId);
        }

        doCreateReplicas();
        if (!status.ok()) {
            return;
        }

        // No log here, PENDING state restore job will redo this method
        state = RestoreJobState.CREATING;
    }

    protected void doCreateReplicas() {
        // Send create replica task to BE outside the db lock
        int numBatchTasks = batchTaskPerTable.values()
                .stream()
                .mapToInt(AgentBatchTask::getTaskNum)
                .sum();
        createReplicaTasksLatch = new MarkedCountDownLatch<>(numBatchTasks);
        if (numBatchTasks > 0) {
            LOG.info("begin to send create replica tasks to BE for restore. total {} tasks. {}",
                    numBatchTasks, this);
            for (AgentBatchTask batchTask : batchTaskPerTable.values()) {
                for (AgentTask task : batchTask.getAllTasks()) {
                    createReplicaTasksLatch.addMark(task.getBackendId(), task.getTabletId());
                    ((CreateReplicaTask) task).setLatch(createReplicaTasksLatch);
                }
                AgentTaskExecutor.submit(batchTask);
            }
        }

        // No log here, PENDING state restore job will redo this method
        state = RestoreJobState.CREATING;
        createReplicasTimeStamp = System.currentTimeMillis();
        batchTaskPerTable.clear();
    }

    protected void checkStorageVault(OlapTable localTable) {
        // do nothing
    }

    protected void waitingAllReplicasCreated() {
        boolean ok = true;
        try {
            if (!createReplicaTasksLatch.await(0, TimeUnit.SECONDS)) {
                LOG.info("waiting {} create replica tasks for restore to finish. {}",
                        createReplicaTasksLatch.getCount(), this);
                long createReplicasTimeOut = DbUtil.getCreateReplicasTimeoutMs(createReplicaTasksLatch.getMarkCount());
                long tryCreateTime = System.currentTimeMillis() - createReplicasTimeStamp;
                if (tryCreateTime > createReplicasTimeOut) {
                    status = new Status(ErrCode.TIMEOUT,
                            "restore job with create replicas timeout: " + tryCreateTime + " with label: " + label);
                    cancelInternal(false);
                    LOG.warn("restore job {} create replicas timeout, cancel {}", jobId, this);
                }
                return;
            }
        } catch (InterruptedException e) {
            LOG.warn("InterruptedException, {}", this, e);
            ok = false;
        }

        if (!(ok && createReplicaTasksLatch.getStatus().ok())) {
            // only show at most 10 results
            List<String> subList = createReplicaTasksLatch.getLeftMarks().stream().limit(10)
                    .map(item -> "(backendId = " + item.getKey() + ", tabletId = "  + item.getValue() + ")")
                    .collect(Collectors.toList());
            String idStr = Joiner.on(", ").join(subList);
            String reason = "TIMEDOUT";
            if (!createReplicaTasksLatch.getStatus().ok()) {
                reason = createReplicaTasksLatch.getStatus().getErrorMsg();
            }
            String errMsg = String.format(
                    "Failed to create replicas for restore: %s, unfinished marks: %s", reason, idStr);
            status = new Status(ErrCode.COMMON_ERROR, errMsg);
            return;
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("finished to create all restored replicas. {}", this);
        }
        allReplicasCreated();
    }

    protected void allReplicasCreated() {
        Database db = env.getInternalCatalog().getDbNullable(dbId);
        if (db == null) {
            status = new Status(ErrCode.NOT_FOUND, "database " + dbId + " does not exist");
            return;
        }

        // add restored partitions.
        // table should be in State RESTORE, so no other partitions can be
        // added to or removed from this table during the restore process.
        for (Pair<String, Partition> entry : restoredPartitions) {
            OlapTable localTbl = (OlapTable) db.getTableNullable(entry.first);
            localTbl.writeLock();
            try {
                Partition restoredPart = entry.second;
                OlapTable remoteTbl = (OlapTable) backupMeta.getTable(entry.first);
                if (localTbl.getPartitionInfo().getType() == PartitionType.RANGE
                        || localTbl.getPartitionInfo().getType() == PartitionType.LIST) {

                    PartitionInfo remotePartitionInfo = remoteTbl.getPartitionInfo();
                    PartitionInfo localPartitionInfo = localTbl.getPartitionInfo();
                    BackupPartitionInfo backupPartitionInfo
                            = jobInfo.getOlapTableInfo(entry.first).getPartInfo(restoredPart.getName());
                    long remotePartId = backupPartitionInfo.id;
                    PartitionItem remoteItem = remoteTbl.getPartitionInfo().getItem(remotePartId);
                    DataProperty remoteDataProperty = remotePartitionInfo.getDataProperty(remotePartId);
                    ReplicaAllocation restoreReplicaAlloc = replicaAlloc;
                    if (reserveReplica) {
                        restoreReplicaAlloc = remotePartitionInfo.getReplicaAllocation(remotePartId);
                    }
                    localPartitionInfo.addPartition(restoredPart.getId(), false, remoteItem,
                            remoteDataProperty, restoreReplicaAlloc,
                            remotePartitionInfo.getIsInMemory(remotePartId),
                            remotePartitionInfo.getIsMutable(remotePartId));
                }
                localTbl.addPartition(restoredPart);
            } finally {
                localTbl.writeUnlock();
            }
        }

        // add restored tables
        for (Table tbl : restoredTbls) {
            if (!db.writeLockIfExist()) {
                status = new Status(ErrCode.COMMON_ERROR, "Database " + db.getFullName() + " has been dropped");
                return;
            }
            tbl.writeLock();
            try {
                if (!db.registerTable(tbl)) {
                    status = new Status(ErrCode.COMMON_ERROR, "Table " + tbl.getName()
                            + " already exist in db: " + db.getFullName());
                    return;
                }
            } finally {
                tbl.writeUnlock();
                db.writeUnlock();
            }
        }

        LOG.info("finished to prepare meta. {}", this);

        if (jobInfo.content == null || jobInfo.content == BackupContent.ALL) {
            if (!reuseCandidates.isEmpty()) {
                // The snapshots of the partitions that keep their data are not needed, decide them first.
                metaPreparedTime = System.currentTimeMillis();
                finishVerifying(db);
                return;
            }
            prepareAndSendSnapshotTaskForOlapTable(db);
        }

        metaPreparedTime = System.currentTimeMillis();
        state = RestoreJobState.SNAPSHOTING;
        // No log here, PENDING state restore job will redo this method
    }

    private Status bindLocalAndRemoteOlapTableReplicas(
            OlapTable localOlapTbl, OlapTable remoteOlapTbl,
            Map<Long, TabletRef> tabletBases) {
        localOlapTbl.readLock();
        try {
            // The storage medium of the remote olap table's storage is HDD, because we want to
            // restore the tables in another cluster might without SSD.
            //
            // Keep the storage medium of the new olap table the same as the old one, so that
            // the replicas in the new olap table will not be migrated to other storage mediums.
            remoteOlapTbl.setStorageMedium(localOlapTbl.getStorageMedium());
            for (Partition partition : remoteOlapTbl.getPartitions()) {
                Partition localPartition = localOlapTbl.getPartition(partition.getName());
                if (localPartition == null) {
                    continue;
                }
                // Since the replicas are bound to the same disk, the storage medium must be the same
                // to avoid media migration.
                TStorageMedium storageMedium = localOlapTbl.getPartitionInfo()
                        .getDataProperty(localPartition.getId()).getStorageMedium();
                remoteOlapTbl.getPartitionInfo().getDataProperty(partition.getId())
                        .setStorageMedium(storageMedium);
                if (LOG.isDebugEnabled()) {
                    LOG.debug("bind local partition {} and remote partition {} with same storage medium {}, name: {}",
                            localPartition.getId(), partition.getId(), storageMedium, partition.getName());
                }
                for (MaterializedIndex index : partition.getMaterializedIndices(IndexExtState.VISIBLE)) {
                    String indexName = remoteOlapTbl.getIndexNameById(index.getId());
                    Long localIndexId = localOlapTbl.getIndexIdByName(indexName);
                    MaterializedIndex localIndex = localIndexId == null ? null : localPartition.getIndex(localIndexId);
                    if (localIndex == null) {
                        continue;
                    }
                    int schemaHash = localOlapTbl.getSchemaHashByIndexId(localIndexId);
                    if (schemaHash == -1) {
                        return new Status(ErrCode.COMMON_ERROR, String.format(
                                "schema hash of local index %d is not found, remote table=%d, remote index=%d, "
                                + "local table=%d, local index=%d", localIndexId, remoteOlapTbl.getId(), index.getId(),
                                localOlapTbl.getId(), localIndexId));
                    }

                    List<Tablet> localTablets = localIndex.getTablets();
                    List<Tablet> remoteTablets = index.getTablets();
                    if (localTablets.size() != remoteTablets.size()) {
                        LOG.warn("skip bind replicas because the size of local tablet {} is not equals to "
                                + "the remote {}, is_atomic_restore=true, remote table={}, remote index={}, "
                                + "local table={}, local index={}", localTablets.size(), remoteTablets.size(),
                                remoteOlapTbl.getId(), index.getId(), localOlapTbl.getId(), localIndexId);
                        continue;
                    }
                    for (int i = 0; i < remoteTablets.size(); i++) {
                        Tablet localTablet = localTablets.get(i);
                        Tablet remoteTablet = remoteTablets.get(i);
                        List<Replica> localReplicas = localTablet.getReplicas();
                        List<Replica> remoteReplicas = remoteTablet.getReplicas();
                        if (localReplicas.size() != remoteReplicas.size()) {
                            LOG.warn("skip bind replicas because the size of local replicas {} is not equals to "
                                    + "the remote {}, is_atomic_restore=true, remote table={}, remote index={}, "
                                    + "local table={}, local index={}, local tablet={}, remote tablet={}",
                                    localReplicas.size(), remoteReplicas.size(), remoteOlapTbl.getId(),
                                    index.getId(), localOlapTbl.getId(), localIndexId, localTablet.getId(),
                                    remoteTablet.getId());
                            continue;
                        }
                        for (int j = 0; j < remoteReplicas.size(); j++) {
                            long backendId = localReplicas.get(j).getBackendIdWithoutException();
                            remoteReplicas.get(j).setBackendId(backendId);
                            if (LOG.isDebugEnabled()) {
                                LOG.debug("bind local replica {} and remote replica {} with same backend {}, table={}",
                                        localReplicas.get(j).getId(), remoteReplicas.get(j).getId(), backendId,
                                        localOlapTbl.getName());
                            }
                        }
                        tabletBases.put(remoteTablet.getId(), new TabletRef(localTablet.getId(), schemaHash));
                    }
                }
            }
        } finally {
            localOlapTbl.readUnlock();
        }
        return Status.OK;
    }

    protected void prepareAndSendSnapshotTaskForOlapTable(Database db) {
        LOG.info("begin to make snapshot. {} when restore content is ALL", this);
        // begin to make snapshots for all replicas
        // snapshot is for incremental download
        unfinishedSignatureToId.clear();
        taskProgress.clear();
        taskErrMsg.clear();
        Multimap<Long, Long> bePathsMap = HashMultimap.create();
        AgentBoundedBatchTask batchTask = new AgentBoundedBatchTask(
                Config.backup_restore_batch_task_num_per_rpc, Config.restore_task_concurrency_per_be);
        db.readLock();
        try {
            for (Map.Entry<IdChain, IdChain> entry : fileMapping.getMapping().entrySet()) {
                IdChain idChain = entry.getKey();
                OlapTable tbl = (OlapTable) db.getTableNullable(idChain.getTblId());
                tbl.readLock();
                try {
                    Partition part = tbl.getPartition(idChain.getPartId());
                    MaterializedIndex index = part.getIndex(idChain.getIdxId());
                    Tablet tablet = index.getTablet(idChain.getTabletId());
                    Replica replica = tablet.getReplicaById(idChain.getReplicaId());
                    long signature = env.getNextId();
                    boolean isRestoreTask = true;
                    // We don't care the visible version in restore job, the end version is used.
                    long visibleVersion = -1L;
                    // Atomic restore, a partition that keeps the data of the table being replaced, or its base: the
                    // snapshot is made from the local tablet at this version (the one of the proof).
                    RestoreReuseResult.Decision atomicDecision = getAtomicLocalDecision(tbl.getId(), part.getId());
                    if (atomicDecision != null) {
                        if (!entry.getValue().hasRefTabletId()) {
                            status = new Status(ErrCode.COMMON_ERROR, "the tablet " + tablet.getId()
                                    + " of the partition " + atomicDecision.tableName + "."
                                    + atomicDecision.partitionName + " is not bound to a local tablet");
                            return;
                        }
                        visibleVersion = atomicDecision.version;
                    }
                    long beId = replica.getBackendIdWithoutException();
                    SnapshotTask task = new SnapshotTask(null, beId,
                            signature, jobId, db.getId(),
                            tbl.getId(), part.getId(), index.getId(), tablet.getId(), visibleVersion,
                            tbl.getSchemaHashByIndexId(index.getId()), timeoutMs, isRestoreTask);
                    if (entry.getValue().hasRefTabletId()) {
                        task.setRefTabletId(entry.getValue().getRefTabletId());
                    }
                    if (atomicDecision != null) {
                        task.setRestoreLocalSource(true);
                        if (atomicDecision.kept && Config.restore_reuse_kept_load_at_snapshot) {
                            // loaded into the staging tablet by the snapshot task, the move only confirms it
                            task.setRestoreKeptLoad(true);
                        }
                    }
                    batchTask.addTask(task);
                    unfinishedSignatureToId.put(signature, tablet.getId());
                    bePathsMap.put(beId, replica.getPathHash());
                } finally {
                    tbl.readUnlock();
                }
            }
        } finally {
            db.readUnlock();
        }

        // check disk capacity
        org.apache.doris.common.Status st = Env.getCurrentSystemInfo().checkExceedDiskCapacityLimit(bePathsMap, true);
        if (!st.ok()) {
            status = new Status(ErrCode.COMMON_ERROR, st.getErrorMsg());
            return;
        }

        AgentTaskExecutor.submit(batchTask);
        LOG.info("finished to send snapshot tasks, num: {}. {}", batchTask.getTaskNum(), this);
    }

    private void checkAndRestoreResources() {
        ResourceMgr resourceMgr = Env.getCurrentEnv().getResourceMgr();
        for (BackupJobInfo.BackupOdbcResourceInfo backupOdbcResourceInfo : jobInfo.newBackupObjects.odbcResources) {
            String backupResourceName = backupOdbcResourceInfo.name;
            Resource localResource = resourceMgr.getResource(backupResourceName);
            OdbcCatalogResource remoteOdbcResource = (OdbcCatalogResource) backupMeta.getResource(backupResourceName);
            if (localResource != null) {
                if (localResource.getType() != Resource.ResourceType.ODBC_CATALOG) {
                    status = new Status(ErrCode.COMMON_ERROR, "The type of local resource "
                            + backupResourceName + " is not same as restored resource");
                    return;
                }
                OdbcCatalogResource localOdbcResource = (OdbcCatalogResource) localResource;
                if (localOdbcResource.getSignature(BackupHandler.SIGNATURE_VERSION)
                        != remoteOdbcResource.getSignature(BackupHandler.SIGNATURE_VERSION)) {
                    status = new Status(ErrCode.COMMON_ERROR, "Odbc resource "
                            + jobInfo.getAliasByOriginNameIfSet(backupResourceName)
                            + " already exist but with different properties");
                    return;
                }
            } else {
                try {
                    // restore resource
                    resourceMgr.createResource(remoteOdbcResource, false);
                } catch (DdlException e) {
                    status = new Status(ErrCode.COMMON_ERROR, e.getMessage());
                    return;
                }
                restoredResources.add(remoteOdbcResource);
            }
        }
    }

    private boolean genFileMappingWhenBackupReplicasEqual(PartitionInfo localPartInfo, Partition localPartition,
            Table localTbl, BackupPartitionInfo backupPartInfo, String partitionName, BackupOlapTableInfo tblInfo,
            ReplicaAllocation remoteReplicaAlloc) {
        short restoreReplicaNum;
        short localReplicaNum = localPartInfo.getReplicaAllocation(localPartition.getId()).getTotalReplicaNum();
        if (!reserveReplica) {
            restoreReplicaNum = replicaAlloc.getTotalReplicaNum();
        } else {
            restoreReplicaNum = remoteReplicaAlloc.getTotalReplicaNum();
        }
        if (localReplicaNum != restoreReplicaNum) {
            status = new Status(ErrCode.COMMON_ERROR, "Partition " + partitionName
                    + " in table " + localTbl.getName()
                    + " has different replication num '"
                    + localReplicaNum
                    + "' with partition in repository, which is " + restoreReplicaNum);
            return true;
        }

        // No need to check range, just generate file mapping
        OlapTable localOlapTbl = (OlapTable) localTbl;
        genFileMapping(localOlapTbl, localPartition, tblInfo.id, backupPartInfo,
                true /* overwrite when commit */);
        restoredVersionInfo.put(localOlapTbl.getId(), localPartition.getId(), backupPartInfo.version);
        return false;
    }

    protected void createReplicas(Database db, OlapTable localTbl, Partition restorePart) {
        createReplicas(db, localTbl, restorePart, null);
    }

    protected void createReplicas(Database db, OlapTable localTbl, Partition restorePart,
            Map<Long, TabletRef> tabletBases) {
        Set<String> bfColumns = localTbl.getCopiedBfColumns();
        double bfFpp = localTbl.getBfFpp();

        AgentBoundedBatchTask batchTask = batchTaskPerTable.get(localTbl.getId());
        if (batchTask == null) {
            batchTask = new AgentBoundedBatchTask(
                    Config.backup_restore_batch_task_num_per_rpc, Config.restore_task_concurrency_per_be);
            batchTaskPerTable.put(localTbl.getId(), batchTask);
        }

        BinlogConfig binlogConfig;
        localTbl.readLock();
        try {
            binlogConfig = new BinlogConfig(localTbl.getBinlogConfig());
        } finally {
            localTbl.readUnlock();
        }
        Map<Object, Object> objectPool = new HashMap<Object, Object>();
        List<String> rowStoreColumns = localTbl.getTableProperty().getCopiedRowStoreColumns();
        for (MaterializedIndex restoredIdx : restorePart.getMaterializedIndices(IndexExtState.VISIBLE)) {
            MaterializedIndexMeta indexMeta = localTbl.getIndexMetaByIndexId(restoredIdx.getId());
            List<Index> indexes = restoredIdx.getId() == localTbl.getBaseIndexId()
                                    ? localTbl.getCopiedIndexes() : null;
            List<Integer> clusterKeyIndexes = null;
            if (indexMeta.getIndexId() == localTbl.getBaseIndexId() || localTbl.isShadowIndex(indexMeta.getIndexId())) {
                clusterKeyIndexes = OlapTable.getClusterKeyIndexes(indexMeta.getSchema());
            }
            for (Tablet restoreTablet : restoredIdx.getTablets()) {
                TabletRef baseTabletRef = tabletBases == null ? null : tabletBases.get(restoreTablet.getId());
                // All restored replicas will be saved to HDD by default.
                TStorageMedium storageMedium = TStorageMedium.HDD;
                if (tabletBases != null) {
                    // ensure this tablet is bound to the same backend disk as the origin table's tablet.
                    storageMedium = localTbl.getPartitionInfo().getDataProperty(restorePart.getId()).getStorageMedium();
                }
                TabletMeta tabletMeta = new TabletMeta(db.getId(), localTbl.getId(), restorePart.getId(),
                        restoredIdx.getId(), indexMeta.getSchemaHash(), storageMedium);
                Env.getCurrentInvertedIndex().addTablet(restoreTablet.getId(), tabletMeta);
                for (Replica restoreReplica : restoreTablet.getReplicas()) {
                    Env.getCurrentInvertedIndex().addReplica(restoreTablet.getId(), restoreReplica);
                    CreateReplicaTask task = new CreateReplicaTask(restoreReplica.getBackendIdWithoutException(), dbId,
                            localTbl.getId(), restorePart.getId(), restoredIdx.getId(),
                            restoreTablet.getId(), restoreReplica.getId(), indexMeta.getShortKeyColumnCount(),
                            indexMeta.getSchemaHash(), restoreReplica.getVersion(),
                            indexMeta.getKeysType(), TStorageType.COLUMN,
                            storageMedium,
                            indexMeta.getSchema(), bfColumns, bfFpp, null,
                            indexes,
                            localTbl.isInMemory(),
                            localTbl.getPartitionInfo().getTabletType(restorePart.getId()),
                            null,
                            localTbl.getCompressionType(),
                            localTbl.getEnableUniqueKeyMergeOnWrite(), localTbl.getStoragePolicy(),
                            localTbl.disableAutoCompaction(),
                            localTbl.enableSingleReplicaCompaction(),
                            localTbl.skipWriteIndexOnLoad(),
                            localTbl.getCompactionPolicy(),
                            localTbl.getTimeSeriesCompactionGoalSizeMbytes(),
                            localTbl.getTimeSeriesCompactionFileCountThreshold(),
                            localTbl.getTimeSeriesCompactionTimeThresholdSeconds(),
                            localTbl.getTimeSeriesCompactionEmptyRowsetsThreshold(),
                            localTbl.getTimeSeriesCompactionLevelThreshold(),
                            localTbl.storeRowColumn(),
                            binlogConfig,
                            localTbl.getRowStoreColumnsUniqueIds(rowStoreColumns),
                            objectPool,
                            localTbl.rowStorePageSize(),
                            localTbl.variantEnableFlattenNested(),
                            localTbl.storagePageSize(), localTbl.getTDEAlgorithm(),
                            localTbl.storageDictPageSize());
                    task.setInvertedIndexFileStorageFormat(localTbl.getInvertedIndexFileStorageFormat());
                    task.setInRestoreMode(true);
                    if (baseTabletRef != null) {
                        // ensure this replica is bound to the same backend disk as the origin table's replica.
                        task.setBaseTablet(baseTabletRef.tabletId, baseTabletRef.schemaHash);
                        LOG.info("set base tablet {} for replica {} in restore job {}, tablet id={}",
                                baseTabletRef.tabletId, restoreReplica.getId(), jobId, restoreTablet.getId());
                    }
                    if (!CollectionUtils.isEmpty(clusterKeyIndexes)) {
                        task.setClusterKeyIndexes(clusterKeyIndexes);
                        LOG.info("table: {}, partition: {}, index: {}, tablet: {}, cluster key indexes: {}",
                                localTbl.getId(), restorePart.getId(), restoredIdx.getId(), restoreTablet.getId(),
                                clusterKeyIndexes);
                    }
                    batchTask.addTask(task);
                }
            }
        }
    }

    // reset remote partition.
    // reset all id in remote partition, but DO NOT modify any exist catalog objects.
    @VisibleForTesting
    protected Partition resetPartitionForRestore(OlapTable localTbl, OlapTable remoteTbl, String partName,
                                                 ReplicaAllocation replicaAlloc) {
        Preconditions.checkState(localTbl.getPartition(partName) == null);
        Partition remotePart = remoteTbl.getPartition(partName);
        Preconditions.checkNotNull(remotePart);
        PartitionInfo localPartitionInfo = localTbl.getPartitionInfo();
        Preconditions.checkState(localPartitionInfo.getType() == PartitionType.RANGE
                || localPartitionInfo.getType() == PartitionType.LIST);

        // generate new partition id
        long newPartId = env.getNextId();
        long oldPartId = remotePart.getId();
        remotePart.setIdForRestore(newPartId);

        // indexes
        Map<String, Long> localIdxNameToId = localTbl.getIndexNameToId();
        for (String localIdxName : localIdxNameToId.keySet()) {
            // set ids of indexes in remote partition to the local index ids
            long remoteIdxId = remoteTbl.getIndexIdByName(localIdxName);
            MaterializedIndex remoteIdx = remotePart.getIndex(remoteIdxId);
            long localIdxId = localIdxNameToId.get(localIdxName);
            remoteIdx.setIdForRestore(localIdxId);
            if (localIdxId != localTbl.getBaseIndexId()) {
                // not base table, reset
                remotePart.deleteRollupIndex(remoteIdxId);
                remotePart.createRollupIndex(remoteIdx);
            }
        }

        // save version info for creating replicas
        long visibleVersion = remotePart.getVisibleVersion();
        remotePart.setNextVersion(visibleVersion + 1);
        LOG.info("reset partition {} for restore, visible version: {}, old partition id: {}",
                newPartId, visibleVersion, oldPartId);

        return resetTabletForRestore(localTbl, remoteTbl, remotePart, replicaAlloc);
    }

    protected Partition resetTabletForRestore(OlapTable localTbl, OlapTable remoteTbl, Partition remotePart,
                                         ReplicaAllocation replicaAlloc) {
        // tablets
        long visibleVersion = remotePart.getVisibleVersion();
        Map<Tag, Integer> nextIndexes = Maps.newHashMap();
        for (MaterializedIndex remoteIdx : remotePart.getMaterializedIndices(IndexExtState.VISIBLE)) {
            int schemaHash = remoteTbl.getSchemaHashByIndexId(remoteIdx.getId());
            int remotetabletSize = remoteIdx.getTablets().size();
            remoteIdx.clearTabletsForRestore();
            for (int i = 0; i < remotetabletSize; i++) {
                // generate new tablet id
                long newTabletId = env.getNextId();
                Tablet newTablet = EnvFactory.getInstance().createTablet(newTabletId);
                // add tablet to index, but not add to TabletInvertedIndex
                remoteIdx.addTablet(newTablet, null /* tablet meta */, true /* is restore */);
                // replicas
                try {
                    Pair<Map<Tag, List<Long>>, TStorageMedium> beIdsAndMedium = Env.getCurrentSystemInfo()
                            .selectBackendIdsForReplicaCreation(replicaAlloc, nextIndexes, null, false, false);
                    Map<Tag, List<Long>> beIds = beIdsAndMedium.first;
                    for (Map.Entry<Tag, List<Long>> entry : beIds.entrySet()) {
                        for (Long beId : entry.getValue()) {
                            long newReplicaId = env.getNextId();
                            Replica newReplica = new Replica(newReplicaId, beId, ReplicaState.NORMAL, visibleVersion,
                                    schemaHash);
                            newTablet.addReplica(newReplica, true /* is restore */);
                        }
                    }
                } catch (DdlException e) {
                    status = new Status(ErrCode.COMMON_ERROR, e.getMessage());
                    return null;
                }
            }
        }
        return remotePart;
    }

    // files in repo to files in local
    protected void genFileMapping(OlapTable localTbl, Partition localPartition, Long remoteTblId,
            BackupPartitionInfo backupPartInfo, boolean overwrite) {
        genFileMapping(localTbl, localPartition, remoteTblId, backupPartInfo, overwrite, null);
    }

    protected void genFileMapping(OlapTable localTbl, Partition localPartition, Long remoteTblId,
                                  BackupPartitionInfo backupPartInfo, boolean overwrite,
                                  Map<Long, TabletRef> tabletBases) {
        for (MaterializedIndex localIdx : localPartition.getMaterializedIndices(IndexExtState.VISIBLE)) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("get index id: {}, index name: {}", localIdx.getId(),
                        localTbl.getIndexNameById(localIdx.getId()));
            }
            BackupIndexInfo backupIdxInfo = backupPartInfo.getIdx(localTbl.getIndexNameById(localIdx.getId()));
            Preconditions.checkState(backupIdxInfo.sortedTabletInfoList.size() == localIdx.getTablets().size());
            for (int i = 0; i < localIdx.getTablets().size(); i++) {
                Tablet localTablet = localIdx.getTablets().get(i);
                BackupTabletInfo backupTabletInfo = backupIdxInfo.sortedTabletInfoList.get(i);
                if (LOG.isDebugEnabled()) {
                    LOG.debug("get tablet mapping: {} to {}, index {}", backupTabletInfo.id, localTablet.getId(), i);
                }
                for (Replica localReplica : localTablet.getReplicas()) {
                    long refTabletId = -1L;
                    if (tabletBases != null && tabletBases.containsKey(localTablet.getId())) {
                        refTabletId = tabletBases.get(localTablet.getId()).tabletId;
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("restored tablet {} is based on exists tablet {}",
                                    localTablet.getId(), refTabletId);
                        }
                    }

                    long noReplicaId = -1L;
                    long noRefTabletId = -1L;
                    IdChain src = new IdChain(remoteTblId, backupPartInfo.id, backupIdxInfo.id,
                            backupTabletInfo.id, noReplicaId, refTabletId);
                    IdChain dest = new IdChain(localTbl.getId(), localPartition.getId(), localIdx.getId(),
                            localTablet.getId(), localReplica.getId(), noRefTabletId);
                    fileMapping.putMapping(dest, src, overwrite);
                }
            }
        }
    }

    private boolean downloadAndDeserializeMetaInfo() {
        if (isFromLocalSnapshot()) {
            if (backupMeta != null) {
                return true;
            }

            status = new Status(ErrCode.COMMON_ERROR, "backupMeta is null");
            return false;
        }

        List<BackupMeta> backupMetas = Lists.newArrayList();
        Status st = repo.getSnapshotMetaFile(jobInfo.name, backupMetas,
                this.metaVersion == -1 ? jobInfo.metaVersion : this.metaVersion);
        if (!st.ok()) {
            status = st;
            return false;
        }
        Preconditions.checkState(backupMetas.size() == 1);
        backupMeta = backupMetas.get(0);
        return true;
    }

    private void replayCheckAndPrepareMeta() {
        Database db;
        try {
            db = env.getInternalCatalog().getDbOrMetaException(dbId);
        } catch (MetaNotFoundException e) {
            LOG.warn("[INCONSISTENT META] replayCheckAndPrepareMeta failed", e);
            return;
        }

        invalidateRestoreLineageOfOverwrittenPartitions(db);
        clearTaintOfDownloadedPartitions(db);

        // replay set all existing tables's state to RESTORE
        for (String tableName : jobInfo.backupOlapTableObjects.keySet()) {
            Table tbl = db.getTableNullable(jobInfo.getAliasByOriginNameIfSet(tableName));
            if (tbl == null) {
                continue;
            }
            OlapTable olapTbl = (OlapTable) tbl;
            tbl.writeLock();
            try {
                if (isAtomicRestore) {
                    // Atomic restore will creates new replica of the OlapTable.
                    olapTbl.setInAtomicRestore();
                    continue;
                }

                olapTbl.setState(OlapTableState.RESTORE);
                // set restore status for partitions
                BackupOlapTableInfo tblInfo = jobInfo.backupOlapTableObjects.get(tableName);
                for (Map.Entry<String, BackupPartitionInfo> partitionEntry : tblInfo.partitions.entrySet()) {
                    String partitionName = partitionEntry.getKey();
                    Partition partition = olapTbl.getPartition(partitionName);
                    if (partition == null) {
                        continue;
                    }
                    partition.setState(PartitionState.RESTORE);
                }
            } finally {
                tbl.writeUnlock();
            }
        }

        // restored partitions
        for (Pair<String, Partition> entry : restoredPartitions) {
            OlapTable localTbl = (OlapTable) db.getTableNullable(entry.first);
            Partition restorePart = entry.second;
            OlapTable remoteTbl = (OlapTable) backupMeta.getTable(entry.first);
            PartitionInfo localPartitionInfo = localTbl.getPartitionInfo();
            PartitionInfo remotePartitionInfo = remoteTbl.getPartitionInfo();
            BackupPartitionInfo backupPartitionInfo = jobInfo.getOlapTableInfo(entry.first)
                    .getPartInfo(restorePart.getName());
            long remotePartId = backupPartitionInfo.id;
            DataProperty remoteDataProperty = remotePartitionInfo.getDataProperty(remotePartId);
            ReplicaAllocation restoreReplicaAlloc = replicaAlloc;
            if (reserveReplica) {
                restoreReplicaAlloc = remotePartitionInfo.getReplicaAllocation(remotePartId);
            }
            localPartitionInfo.addPartition(restorePart.getId(), false, remotePartitionInfo.getItem(remotePartId),
                    remoteDataProperty, restoreReplicaAlloc,
                    remotePartitionInfo.getIsInMemory(remotePartId),
                    remotePartitionInfo.getIsMutable(remotePartId));
            localTbl.addPartition(restorePart);

            // modify tablet inverted index
            for (MaterializedIndex restoreIdx : restorePart.getMaterializedIndices(IndexExtState.VISIBLE)) {
                int schemaHash = localTbl.getSchemaHashByIndexId(restoreIdx.getId());
                for (Tablet restoreTablet : restoreIdx.getTablets()) {
                    TabletMeta tabletMeta = new TabletMeta(db.getId(), localTbl.getId(), restorePart.getId(),
                            restoreIdx.getId(), schemaHash, TStorageMedium.HDD);
                    Env.getCurrentInvertedIndex().addTablet(restoreTablet.getId(), tabletMeta);
                    for (Replica restoreReplica : restoreTablet.getReplicas()) {
                        Env.getCurrentInvertedIndex().addReplica(restoreTablet.getId(), restoreReplica);
                    }
                }
            }
        }


        // restored tables
        for (Table restoreTbl : restoredTbls) {
            db.writeLock();
            restoreTbl.writeLock();
            try {
                db.registerTable(restoreTbl);
            } finally {
                restoreTbl.writeUnlock();
                db.writeUnlock();
            }
            if (restoreTbl.getType() != TableType.OLAP) {
                continue;
            }
            OlapTable olapRestoreTbl = (OlapTable) restoreTbl;
            olapRestoreTbl.writeLock();
            try {
                // modify tablet inverted index
                for (Partition restorePart : olapRestoreTbl.getPartitions()) {
                    for (MaterializedIndex restoreIdx : restorePart.getMaterializedIndices(IndexExtState.VISIBLE)) {
                        int schemaHash = olapRestoreTbl.getSchemaHashByIndexId(restoreIdx.getId());
                        for (Tablet restoreTablet : restoreIdx.getTablets()) {
                            TabletMeta tabletMeta = new TabletMeta(db.getId(), restoreTbl.getId(), restorePart.getId(),
                                    restoreIdx.getId(), schemaHash, TStorageMedium.HDD);
                            Env.getCurrentInvertedIndex().addTablet(restoreTablet.getId(), tabletMeta);
                            for (Replica restoreReplica : restoreTablet.getReplicas()) {
                                Env.getCurrentInvertedIndex().addReplica(restoreTablet.getId(), restoreReplica);
                            }
                        }
                    }
                }
            } finally {
                olapRestoreTbl.writeUnlock();
            }
        }

        // restored resource
        ResourceMgr resourceMgr = Env.getCurrentEnv().getResourceMgr();
        for (Resource resource : restoredResources) {
            resourceMgr.replayCreateResource(resource);
        }

        LOG.info("replay check and prepare meta. {}", this);
    }

    protected void waitingAllSnapshotsFinished() {
        if (unfinishedSignatureToId.isEmpty()) {
            snapshotFinishedTime = System.currentTimeMillis();
            state = RestoreJobState.DOWNLOAD;

            // the taint of the partitions to download is cleared with the log, under the locks of their tables
            Database db = env.getInternalCatalog().getDbNullable(dbId);
            List<Table> locked = Lists.newArrayList();
            if (db != null) {
                for (long tblId : Sets.newTreeSet(restoredVersionInfo.rowKeySet())) {
                    Table tbl = db.getTableNullable(tblId);
                    if (tbl != null && tbl.writeLockIfExist()) {
                        locked.add(tbl);
                    }
                }
            }
            try {
                if (db != null) {
                    clearTaintOfDownloadedPartitions(db);
                }
                env.getEditLog().logRestoreJob(this);
            } finally {
                for (int i = locked.size() - 1; i >= 0; i--) {
                    locked.get(i).writeUnlock();
                }
            }
            LOG.info("finished making snapshots. {}", this);
            return;
        }

        LOG.info("waiting {} replicas to make snapshot. {}", unfinishedSignatureToId.size(), this);
        return;
    }

    private void downloadSnapshots() {
        if (isFromLocalSnapshot()) {
            downloadLocalSnapshots();
        } else {
            downloadRemoteSnapshots();
        }
    }

    protected void downloadRemoteSnapshots() {
        // Categorize snapshot infos by db id.
        ArrayListMultimap<Long, SnapshotInfo> dbToSnapshotInfos = ArrayListMultimap.create();
        for (SnapshotInfo info : snapshotInfos.values()) {
            dbToSnapshotInfos.put(info.getDbId(), info);
        }

        // Send download tasks
        unfinishedSignatureToId.clear();
        taskProgress.clear();
        taskErrMsg.clear();
        AgentBoundedBatchTask batchTask = new AgentBoundedBatchTask(
                Config.backup_restore_batch_task_num_per_rpc, Config.restore_task_concurrency_per_be);
        for (long dbId : dbToSnapshotInfos.keySet()) {
            List<SnapshotInfo> infos = dbToSnapshotInfos.get(dbId);

            Database db = env.getInternalCatalog().getDbNullable(dbId);
            if (db == null) {
                status = new Status(ErrCode.NOT_FOUND, "db " + dbId + " does not exist");
                return;
            }

            // We classify the snapshot info by backend
            ArrayListMultimap<Long, SnapshotInfo> beToSnapshots = ArrayListMultimap.create();
            for (SnapshotInfo info : infos) {
                if (isKeptPartition(info.getTblId(), info.getPartitionId())) {
                    // made from the local tablets by the snapshot task
                    continue;
                }
                beToSnapshots.put(info.getBeId(), info);
            }

            db.readLock();
            try {
                for (Long beId : beToSnapshots.keySet()) {
                    List<SnapshotInfo> beSnapshotInfos = beToSnapshots.get(beId);
                    int totalNum = beSnapshotInfos.size();
                    // each task contains several upload sub tasks
                    int taskNumPerBatch = Config.restore_download_snapshot_batch_size;
                    LOG.info("backend {} has total {} snapshots, per task batch size {}, {}",
                            beId, totalNum, taskNumPerBatch, this);

                    List<FsBroker> brokerAddrs = null;
                    brokerAddrs = Lists.newArrayList();
                    Status st = repo.getBrokerAddress(beId, env, brokerAddrs);
                    if (!st.ok()) {
                        status = st;
                        return;
                    }
                    Preconditions.checkState(brokerAddrs.size() == 1);

                    // allot tasks
                    for (int index = 0; index < totalNum; index += taskNumPerBatch) {
                        Map<String, String> srcToDest = Maps.newHashMap();
                        for (int j = 0; j < taskNumPerBatch && index + j < totalNum; j++) {
                            SnapshotInfo info = beSnapshotInfos.get(index + j);
                            Table tbl = db.getTableNullable(info.getTblId());
                            if (tbl == null) {
                                status = new Status(ErrCode.NOT_FOUND, "restored table "
                                        + info.getTabletId() + " does not exist");
                                return;
                            }
                            OlapTable olapTbl = (OlapTable) tbl;
                            olapTbl.readLock();
                            try {
                                Pair<IdChain, IdChain> result = getFileMappingForSnapshots(olapTbl, info);
                                if (!status.ok() || result == null) {
                                    return;
                                }
                                String repoTabletPath = jobInfo.getFilePath(result.second);
                                // eg:
                                // bos://location/__palo_repository_my_repo/_ss_my_ss/_ss_content/__db_10000/
                                // __tbl_10001/__part_10002/_idx_10001/__10003
                                String src = repo.getRepoPath(label, repoTabletPath);
                                if (src == null) {
                                    status = new Status(ErrCode.COMMON_ERROR, "invalid src path: " + repoTabletPath);
                                    return;
                                }
                                SnapshotInfo snapshotInfo = snapshotInfos.get(info.getTabletId(), info.getBeId());
                                Preconditions.checkNotNull(snapshotInfo, info.getTabletId() + "-" + info.getBeId());
                                // download to previous exist snapshot dir
                                String dest = snapshotInfo.getTabletPath();
                                srcToDest.put(src, dest);
                                if (LOG.isDebugEnabled()) {
                                    LOG.debug("create download src path: {}, dest path: {}", src, dest);
                                }

                            } finally {
                                olapTbl.readUnlock();
                            }
                        }
                        long signature = env.getNextId();
                        DownloadTask task = createDownloadTask(beId, signature, jobId, dbId, srcToDest,
                                brokerAddrs.get(0));
                        batchTask.addTask(task);
                        unfinishedSignatureToId.put(signature, beId);
                    }
                }
            } finally {
                db.readUnlock();
            }
        }

        AgentTaskExecutor.submit(batchTask);

        state = RestoreJobState.DOWNLOADING;

        // No edit log here
        LOG.info("finished to send download tasks to BE. num: {}. {}", batchTask.getTaskNum(), this);
    }

    protected void downloadLocalSnapshots() {
        // Categorize snapshot infos by db id.
        ArrayListMultimap<Long, SnapshotInfo> dbToSnapshotInfos = ArrayListMultimap.create();
        for (SnapshotInfo info : snapshotInfos.values()) {
            dbToSnapshotInfos.put(info.getDbId(), info);
        }

        // Send download tasks
        unfinishedSignatureToId.clear();
        taskProgress.clear();
        taskErrMsg.clear();
        AgentBoundedBatchTask batchTask = new AgentBoundedBatchTask(
                Config.backup_restore_batch_task_num_per_rpc, Config.restore_task_concurrency_per_be);
        for (long dbId : dbToSnapshotInfos.keySet()) {
            List<SnapshotInfo> infos = dbToSnapshotInfos.get(dbId);

            Database db = env.getInternalCatalog().getDbNullable(dbId);
            if (db == null) {
                status = new Status(ErrCode.NOT_FOUND, "db " + dbId + " does not exist");
                return;
            }

            // We classify the snapshot info by backend
            ArrayListMultimap<Long, SnapshotInfo> beToSnapshots = ArrayListMultimap.create();
            for (SnapshotInfo info : infos) {
                if (isKeptPartition(info.getTblId(), info.getPartitionId())) {
                    // made from the local tablets by the snapshot task
                    continue;
                }
                beToSnapshots.put(info.getBeId(), info);
            }

            db.readLock();
            try {
                for (Long beId : beToSnapshots.keySet()) {
                    List<SnapshotInfo> beSnapshotInfos = beToSnapshots.get(beId);
                    int totalNum = beSnapshotInfos.size();
                    // each task contains several upload sub tasks
                    int taskNumPerBatch = Config.restore_download_snapshot_batch_size;
                    LOG.info("backend {} has total {} snapshots, per task batch size {}, {}",
                            beId, totalNum, taskNumPerBatch, this);

                    // allot tasks
                    for (int index = 0; index < totalNum; index += taskNumPerBatch) {
                        List<TRemoteTabletSnapshot> remoteTabletSnapshots = Lists.newArrayList();
                        for (int j = 0; j < taskNumPerBatch && index + j < totalNum; j++) {
                            TRemoteTabletSnapshot remoteTabletSnapshot = new TRemoteTabletSnapshot();
                            SnapshotInfo info = beSnapshotInfos.get(index + j);
                            Table tbl = db.getTableNullable(info.getTblId());
                            if (tbl == null) {
                                status = new Status(ErrCode.NOT_FOUND, "restored table "
                                        + info.getTabletId() + " does not exist");
                                return;
                            }
                            OlapTable olapTbl = (OlapTable) tbl;
                            olapTbl.readLock();
                            try {
                                Pair<IdChain, IdChain> result = getFileMappingForSnapshots(olapTbl, info);
                                if (!status.ok() || result == null) {
                                    return;
                                }
                                SnapshotInfo snapshotInfo = snapshotInfos.get(info.getTabletId(), info.getBeId());
                                Preconditions.checkNotNull(snapshotInfo, info.getTabletId() + "-" + info.getBeId());
                                // download to previous exist snapshot dir
                                String dest = snapshotInfo.getTabletPath();

                                Long localTabletId = info.getTabletId();
                                String localSnapshotPath = dest;
                                IdChain repoIds = result.second;
                                Long remoteTabletId = repoIds.getTabletId();
                                Long remoteBeId = jobInfo.getBeId(remoteTabletId);
                                String remoteSnapshotPath = jobInfo.getTabletSnapshotPath(remoteTabletId);
                                if (remoteSnapshotPath == null) {
                                    status = new Status(ErrCode.NOT_FOUND,
                                            "failed to get remote snapshot path of tablet: " + remoteTabletId);
                                    return;
                                }
                                Long schemaHash = jobInfo.getSchemaHash(
                                        repoIds.getTblId(), repoIds.getPartId(), repoIds.getIdxId());
                                if (schemaHash == null) {
                                    status = new Status(ErrCode.NOT_FOUND,
                                            "failed to get schema hash of table: " + repoIds.getTblId()
                                                    + ", partition: " + repoIds.getPartId()
                                                    + ", index: " + repoIds.getIdxId());
                                    return;
                                }
                                // remoteSnapshotPath = "${remoteSnapshotPath}/${remoteTabletId}/${schemaHash}"
                                remoteSnapshotPath =
                                        String.format("%s/%d/%d", remoteSnapshotPath, remoteTabletId, schemaHash);
                                TNetworkAddress remoteBeAddr = jobInfo.getBeAddr(remoteBeId);
                                if (remoteBeAddr == null) {
                                    status = new Status(ErrCode.NOT_FOUND,
                                            "failed to get remote be address of be: " + remoteBeId);
                                    return;
                                }
                                String remoteToken = jobInfo.getToken();

                                remoteTabletSnapshot.setLocalTabletId(localTabletId);
                                remoteTabletSnapshot.setLocalSnapshotPath(localSnapshotPath);
                                remoteTabletSnapshot.setRemoteTabletId(remoteTabletId);
                                remoteTabletSnapshot.setRemoteBeId(remoteBeId);
                                remoteTabletSnapshot.setRemoteBeAddr(remoteBeAddr);
                                remoteTabletSnapshot.setRemoteSnapshotPath(remoteSnapshotPath);
                                remoteTabletSnapshot.setRemoteToken(remoteToken);

                                remoteTabletSnapshots.add(remoteTabletSnapshot);
                            } finally {
                                olapTbl.readUnlock();
                            }
                        }
                        long signature = env.getNextId();
                        DownloadTask task = new DownloadTask(null, beId, signature, jobId, dbId, remoteTabletSnapshots);
                        batchTask.addTask(task);
                        unfinishedSignatureToId.put(signature, beId);
                    }
                }
            } finally {
                db.readUnlock();
            }
        }

        AgentTaskExecutor.submit(batchTask);

        state = RestoreJobState.DOWNLOADING;

        // No edit log here
        LOG.info("finished to send download tasks to BE. num: {}. {}", batchTask.getTaskNum(), this);
    }

    /** The decision of the partition if it is restored by appending the increment, otherwise null. */
    private RestoreReuseResult.Decision getIncrementalDecision(long tblId, long partId) {
        return reuseResult == null ? null : reuseResult.getIncremental(tblId, partId);
    }

    private boolean isIncrementalPartition(long tblId, long partId) {
        return getIncrementalDecision(tblId, partId) != null;
    }

    /** The partition (of the staging table) keeps the data of the table being replaced: nothing to download. */
    private boolean isKeptPartition(long tblId, long partId) {
        return reuseResult != null && reuseResult.getKept(tblId, partId) != null;
    }

    /**
     * Atomic restore: the decision of the staging partition if its data is made from the local tablets of the table
     * being replaced (kept, or the base of an incremental partition), otherwise null.
     */
    private RestoreReuseResult.Decision getAtomicLocalDecision(long tblId, long partId) {
        if (reuseResult == null) {
            return null;
        }
        RestoreReuseResult.Decision decision = reuseResult.getKept(tblId, partId);
        if (decision == null) {
            decision = reuseResult.getIncremental(tblId, partId);
        }
        return decision != null && decision.atomic ? decision : null;
    }

    protected DownloadTask createDownloadTask(long beId, long signature, long jobId, long dbId,
                                              Map<String, String> srcToDest, FsBroker brokerAddr) {
        return new DownloadTask(null, beId, signature, jobId, dbId, srcToDest,
            brokerAddr, repo.getRemoteFileSystem().getStorageProperties().getBackendConfigProperties(),
            repo.getRemoteFileSystem().getStorageType(), repo.getLocation(), "");
    }

    // Get the id mapping for snapshot, user should hold the lock of table.
    protected Pair<IdChain, IdChain> getFileMappingForSnapshots(OlapTable olapTbl, SnapshotInfo info) {
        Partition part = olapTbl.getPartition(info.getPartitionId());
        if (part == null) {
            status = new Status(ErrCode.NOT_FOUND, "partition " + info.getPartitionId()
                    + " does not exist in restored table: " + olapTbl.getName());
            return null;
        }

        MaterializedIndex idx = part.getIndex(info.getIndexId());
        if (idx == null) {
            status = new Status(ErrCode.NOT_FOUND, "index " + info.getIndexId() + " does not exist in partion "
                    + part.getName() + "of restored table " + olapTbl.getName());
            return null;
        }

        Tablet tablet = idx.getTablet(info.getTabletId());
        if (tablet == null) {
            status = new Status(ErrCode.NOT_FOUND,
                    "tablet " + info.getTabletId() + " does not exist in restored table "
                    + olapTbl.getName());
            return null;
        }

        Replica replica = tablet.getReplicaByBackendId(info.getBeId());
        if (replica == null) {
            status = new Status(ErrCode.NOT_FOUND,
                    "replica in be " + info.getBeId() + " of tablet " + tablet.getId()
                    + " does not exist in restored table " + olapTbl.getName());
            return null;
        }

        long refTabletId = -1L;  // no ref tablet id
        IdChain catalogIds = new IdChain(olapTbl.getId(), part.getId(), idx.getId(),
                info.getTabletId(), replica.getId(), refTabletId);
        IdChain repoIds = fileMapping.get(catalogIds);
        if (repoIds == null) {
            status = new Status(ErrCode.NOT_FOUND, "failed to get id mapping of catalog ids: " + catalogIds);
            LOG.info("current file mapping: {}", fileMapping);
            return null;
        }
        return Pair.of(catalogIds, repoIds);
    }

    private void waitingAllDownloadFinished() {
        if (unfinishedSignatureToId.isEmpty()) {
            downloadFinishedTime = System.currentTimeMillis();
            state = RestoreJobState.COMMIT;

            // backupMeta is useless now
            backupMeta = null;

            env.getEditLog().logRestoreJob(this);
            LOG.info("finished to download. {}", this);
        }

        LOG.info("waiting {} tasks to finish downloading from repo. {}", unfinishedSignatureToId.size(), this);
    }

    protected void commit() {
        // Fail before any tablet is moved if a partition that keeps its data was written.
        Status keptStatus = checkKeptPartitions();
        if (!keptStatus.ok()) {
            status = keptStatus;
            return;
        }

        // Send task to move the download dir
        unfinishedSignatureToId.clear();
        taskProgress.clear();
        taskErrMsg.clear();
        AgentBoundedBatchTask batchTask = new AgentBoundedBatchTask(
                Config.backup_restore_batch_task_num_per_rpc, Config.restore_task_concurrency_per_be);
        // tablet id->(be id -> download info)
        for (Cell<Long, Long, SnapshotInfo> cell : snapshotInfos.cellSet()) {
            SnapshotInfo info = cell.getValue();
            long signature = env.getNextId();
            DirMoveTask task = new DirMoveTask(null, cell.getColumnKey(), signature, jobId, dbId, info.getTblId(),
                    info.getPartitionId(), info.getTabletId(), cell.getRowKey(), info.getTabletPath(),
                    info.getSchemaHash(), true /* need reload tablet header */);
            RestoreReuseResult.Decision kept = getAtomicLocalDecision(info.getTblId(), info.getPartitionId());
            if (kept != null && kept.kept) {
                // The snapshot task may have loaded the tablet (restore_reuse_kept_load_at_snapshot), then the
                // backend judges the move by the evidence of the load: its failure is converged. The request of the
                // move is the same either way.
                task.setLocalSourceKept(true);
                task.setLocalSourceLoaded(true);
            }
            batchTask.addTask(task);
            unfinishedSignatureToId.put(signature, info.getTabletId());
        }

        AgentTaskExecutor.submit(batchTask);

        state = RestoreJobState.COMMITTING;

        // No log here
        LOG.info("finished to send move dir tasks. num: {}. {}", batchTask.getTaskNum(), this);
        return;
    }

    protected void waitingAllTabletsCommitted() {
        if (unfinishedSignatureToId.isEmpty()) {
            LOG.info("finished to commit all tablet. {}", this);
            Status st = allTabletCommitted(false /* not replay */);
            if (!st.ok()) {
                status = st;
            }
            return;
        }
        LOG.info("waiting {} tablets to commit. {}", unfinishedSignatureToId.size(), this);
    }

    protected Status allTabletCommitted(boolean isReplay) {
        Database db = env.getInternalCatalog().getDbNullable(dbId);
        if (db == null) {
            return new Status(ErrCode.NOT_FOUND, "database " + dbId + " does not exist");
        }

        // The partitions that keep their local data must not have been written, or the restored data is not
        // consistent with the backup. Never commit silently.
        if (!isReplay) {
            Status keptStatus = checkKeptPartitions();
            if (!keptStatus.ok()) {
                return keptStatus;
            }
        }

        // replace the origin tables in atomic.
        if (isAtomicRestore) {
            Status st = atomicReplaceOlapTables(db, isReplay);
            if (!st.ok()) {
                return st;
            }
        }

        // set all restored partition version and version hash
        // set all tables' state to NORMAL
        setTableStateToNormalAndUpdateProperties(db, true, isReplay);
        for (long tblId : restoredVersionInfo.rowKeySet()) {
            Table tbl = db.getTableNullable(tblId);
            if (tbl == null) {
                continue;
            }
            OlapTable olapTbl = (OlapTable) tbl;
            if (!tbl.writeLockIfExist()) {
                continue;
            }
            try {
                Map<Long, Long> map = restoredVersionInfo.rowMap().get(tblId);
                for (Map.Entry<Long, Long> entry : map.entrySet()) {
                    long partId = entry.getKey();
                    Partition part = olapTbl.getPartition(partId);
                    if (part == null) {
                        continue;
                    }

                    // update partition visible version
                    part.updateVersionForRestore(entry.getValue());
                    long visibleVersion = part.getVisibleVersion();

                    // we also need to update the replica version of these overwritten restored partitions
                    for (MaterializedIndex idx : part.getMaterializedIndices(IndexExtState.VISIBLE)) {
                        for (Tablet tablet : idx.getTablets()) {
                            for (Replica replica : tablet.getReplicas()) {
                                replica.updateVersionForRestore(visibleVersion);
                            }
                        }
                    }

                    if (LOG.isDebugEnabled()) {
                        LOG.debug("restore set partition {} version in table {}, version: {}",
                                partId, tblId, entry.getValue());
                    }
                }
            } finally {
                tbl.writeUnlock();
            }
        }

        settleLocalSourceReplicaVersions(db);

        if (!isReplay) {
            // persisted with the job finished, see stampStateToken
            stampStateTokens = Config.restore_state_token_track;
        }
        stampRestoreLineage(db, System.currentTimeMillis());

        // Drop the exists but non-restored table/partitions.
        if (isCleanTables || isCleanPartitions) {
            Status st = dropAllNonRestoredTableAndPartitions(db);
            if (!st.ok()) {
                return st;
            }
        }

        if (!isReplay) {
            restoredPartitions.clear();
            restoredTbls.clear();
            restoredResources.clear();

            com.google.common.collect.Table<Long, Long, SnapshotInfo> savedSnapshotInfos = snapshotInfos;
            snapshotInfos = HashBasedTable.create();

            snapshotInfos.clear();
            fileMapping.clear();
            jobInfo.releaseSnapshotInfo();

            finishedTime = System.currentTimeMillis();
            state = RestoreJobState.FINISHED;
            clearLocalSourceStagingPartitions();
            env.getEditLog().logRestoreJob(this);

            // Only send release snapshot tasks after the job is finished.
            releaseSnapshots(savedSnapshotInfos, true);
        }
        if (isReplay) {
            clearLocalSourceStagingPartitions();
        }

        LOG.info("job is finished. is replay: {}. {}", isReplay, this);
        return Status.OK;
    }

    /**
     * The tablets of the atomic local source partitions (kept or incremental) are loaded from the local source, and the
     * backend may have reported a lower version than the visible version of the staging partition while the job ran, so
     * the replica carries a last failed version. All their loads and move tasks succeeded when the job gets here, so
     * the replicas are at the visible version: set them there and clear the failed version, or the tablet scheduler
     * repairs them (a clone) as soon as the protection of the job is gone. Done on replay as well, to keep the
     * followers the same. The staging tables are the formal tables now, they are found by the recorded ids.
     * The partitions to download and the non-atomic restore are not touched.
     */
    @VisibleForTesting
    int settleLocalSourceReplicaVersions(Database db) {
        if (reuseResult == null) {
            return 0;
        }
        Map<Long, Set<Long>> partitionsByTable = Maps.newHashMap();
        for (RestoreReuseResult.Decision decision : reuseResult.getDecisions()) {
            if (decision.atomic && (decision.kept || decision.incremental)) {
                partitionsByTable.computeIfAbsent(decision.stagingTableId, k -> Sets.newHashSet())
                        .add(decision.stagingPartitionId);
            }
        }
        int fixed = 0;
        for (Map.Entry<Long, Set<Long>> entry : partitionsByTable.entrySet()) {
            Table tbl = db.getTableNullable(entry.getKey());
            if (!(tbl instanceof OlapTable) || !tbl.writeLockIfExist()) {
                continue;
            }
            try {
                OlapTable olapTbl = (OlapTable) tbl;
                for (long partId : entry.getValue()) {
                    Partition part = olapTbl.getPartition(partId);
                    if (part == null) {
                        continue;
                    }
                    long visibleVersion = part.getVisibleVersion();
                    for (MaterializedIndex idx : part.getMaterializedIndices(IndexExtState.VISIBLE)) {
                        for (Tablet tablet : idx.getTablets()) {
                            for (Replica replica : tablet.getReplicas()) {
                                if (replica.getVersion() < visibleVersion || replica.getLastFailedVersion() > 0) {
                                    replica.updateVersionWithFailed(visibleVersion, -1L, visibleVersion);
                                    fixed++;
                                }
                            }
                        }
                    }
                }
            } finally {
                tbl.writeUnlock();
            }
        }
        LOG.info("restore reuse: settled the version of {} replicas of the local source partitions, job: {}",
                fixed, jobId);
        return fixed;
    }

    private Status dropAllNonRestoredTableAndPartitions(Database db) {
        Set<String> restoredViews = jobInfo.newBackupObjects.views.stream()
                .map(view -> view.name).collect(Collectors.toSet());

        try {
            for (Table table : db.getTables()) {
                long tableId = table.getId();
                String tableName = table.getName();
                TableType tableType = table.getType();
                if (tableType == TableType.OLAP) {
                    BackupOlapTableInfo backupTableInfo = jobInfo.backupOlapTableObjects.get(tableName);
                    if (tableType == TableType.OLAP && backupTableInfo != null) {
                        // drop the non restored partitions.
                        dropNonRestoredPartitions(db, (OlapTable) table, backupTableInfo);
                    } else if (isCleanTables) {
                        // otherwise drop the entire table.
                        LOG.info("drop non restored table {}, table id: {}. {}", tableName, tableId, this);
                        boolean isView = false;
                        boolean isForceDrop = false; // move this table into recyclebin.
                        env.getInternalCatalog().dropTableWithoutCheck(db, table, isView, isForceDrop);
                    }
                } else if (tableType == TableType.VIEW && isCleanTables && !restoredViews.contains(tableName)) {
                    LOG.info("drop non restored view {}, table id: {}. {}", tableName, tableId, this);
                    boolean isView = false;
                    boolean isForceDrop = false; // move this view into recyclebin.
                    env.getInternalCatalog().dropTableWithoutCheck(db, table, isView, isForceDrop);
                }
            }
            return Status.OK;
        } catch (Exception e) {
            LOG.warn("drop all non restored table and partitions failed. {}", this, e);
            return new Status(ErrCode.COMMON_ERROR, e.getMessage());
        }
    }

    private void dropNonRestoredPartitions(
            Database db, OlapTable table, BackupOlapTableInfo backupTableInfo) throws DdlException {
        if (!isCleanPartitions || !table.writeLockIfExist()) {
            return;
        }

        try {
            long tableId = table.getId();
            String tableName = table.getQualifiedName();
            InternalCatalog catalog = env.getInternalCatalog();
            for (String partitionName : table.getPartitionNames()) {
                if (backupTableInfo.containsPart(partitionName)) {
                    continue;
                }

                LOG.info("drop non restored partition {} of table {}({}). {}",
                        partitionName, tableName, tableId, this);
                boolean isTempPartition = false;
                boolean isForceDrop = false; // move this partition into recyclebin.
                catalog.dropPartitionWithoutCheck(db, table, partitionName, isTempPartition, isForceDrop);
            }
        } finally {
            table.writeUnlock();
        }
    }

    private void releaseSnapshots(com.google.common.collect.Table<Long, Long, SnapshotInfo> snapshotInfos,
                                  boolean isJobCompleted) {
        if (snapshotInfos.isEmpty()) {
            return;
        }
        // we do not care about the release snapshot tasks' success or failure,
        // the GC thread on BE will sweep the snapshot, finally.
        AgentBatchTask batchTask = new AgentBatchTask(Config.backup_restore_batch_task_num_per_rpc);
        for (SnapshotInfo info : snapshotInfos.values()) {
            ReleaseSnapshotTask releaseTask = new ReleaseSnapshotTask(null, info.getBeId(), info.getDbId(),
                    info.getTabletId(), info.getPath(), isJobCompleted);
            batchTask.addTask(releaseTask);
        }
        AgentTaskExecutor.submit(batchTask);
        LOG.info("send {} release snapshot tasks, job: {}", snapshotInfos.size(), this);
    }

    private void replayWaitingAllTabletsCommitted() {
        allTabletCommitted(true /* is replay */);
    }

    public List<String> getBriefInfo() {
        return getInfo(true);
    }

    public List<String> getFullInfo() {
        return getInfo(false);
    }

    public synchronized List<String> getInfo(boolean isBrief) {
        String unfinishedTaskIdsStr = unfinishedSignatureToId.entrySet().stream()
                .map(e -> "[" + e.getKey() + "=" + e.getValue() + "]")
                .limit(100)
                .collect(Collectors.joining(", "));

        List<String> info = Lists.newArrayList();
        info.add(String.valueOf(jobId));
        info.add(label);
        info.add(backupTimestamp);
        info.add(dbName);
        info.add(state.name());
        info.add(String.valueOf(allowLoad));
        info.add(String.valueOf(replicaAlloc.getTotalReplicaNum()));
        info.add(replicaAlloc.toCreateStmt());
        info.add(String.valueOf(reserveReplica));
        info.add(String.valueOf(reserveDynamicPartitionEnable));
        if (!isBrief) {
            info.add(getRestoreObjs());
        }
        info.add(TimeUtils.longToTimeString(createTime));
        info.add(TimeUtils.longToTimeString(metaPreparedTime));
        info.add(TimeUtils.longToTimeString(snapshotFinishedTime));
        info.add(TimeUtils.longToTimeString(downloadFinishedTime));
        info.add(TimeUtils.longToTimeString(finishedTime));
        info.add(unfinishedTaskIdsStr);
        if (!isBrief) {
            String taskProgressStr = taskProgress.entrySet().stream()
                    .map(e -> "[" + e.getKey() + ": " + e.getValue().first + "/" + e.getValue().second + "]")
                    .limit(100)
                    .collect(Collectors.joining(", "));
            String taskErrMsgStr = taskErrMsg.entrySet().stream()
                    .map(n -> "[" + n.getKey() + ": " + n.getValue() + "]")
                    .limit(100)
                    .collect(Collectors.joining(", "));
            info.add(taskProgressStr);
            info.add(taskErrMsgStr);
        }
        info.add(status.toString());
        info.add(String.valueOf(timeoutMs / 1000));
        if (!isBrief) {
            // the reuse is off or its selection failed: nothing is kept, all the data is downloaded
            info.add(Config.isCloudMode() ? FeConstants.null_string : new RestoreReuseShadowStats().toJson(
                    reuseResult == null ? new RestoreReuseResult() : reuseResult));
        }
        return info;
    }

    public RestoreReuseResult getReuseResult() {
        return reuseResult;
    }

    private void clearReuseWorkingState() {
        reuseCandidates.clear();
    }

    /**
     * Whether every node which the reuse of this job depends on understands it: every follower frontend (the master
     * included, the observers excluded: they never take over a job) and every alive backend must report
     * the capability in the heartbeat. A node which has not been heard from since the master started counts as not
     * having it. Nothing is checked if restore_reuse_require_capability is false.
     *
     * @return the first node without the capability, null if there is none
     */
    @VisibleForTesting
    static String findNodeWithoutReuseCapability() {
        return findNodeWithoutReuseCapability(RESTORE_REUSE_CAPABILITY_MIN_REQUIRED, true);
    }

    /**
     * @param required the capability that every node must report
     * @param unknownIsIncapable whether the failure to read the nodes is taken as a node without the capability (the
     *         decision of the job), or as none (the estimate, which only records the reason)
     */
    private static String findNodeWithoutReuseCapability(int required, boolean unknownIsIncapable) {
        if (!Config.restore_reuse_require_capability) {
            return null;
        }
        if (DebugPointUtil.isEnable("RestoreJob.restoreReuseCapability.mixedVersions")) {
            // a node of an old version, for the regression test
            return "debug point";
        }
        try {
            Env env = Env.getCurrentEnv();
            SystemInfoService.HostInfo self = env.getSelfNode();
            for (Frontend fe : env.getFrontends(FrontendNodeType.FOLLOWER)) {
                // the host and the port: several frontends may run on a host
                boolean isSelf = self != null && fe.getHost().equals(self.getHost())
                        && fe.getEditLogPort() == self.getPort();
                int capability = isSelf ? RESTORE_REUSE_CAPABILITY_REPORTED : fe.getRestoreReuseCapability();
                if (capability < required) {
                    return "frontend " + fe.getHost() + " (capability " + capability + ")";
                }
            }
            for (Backend be : Env.getCurrentSystemInfo().getAllClusterBackendsNoException().values()) {
                if (!be.isAlive()) {
                    continue;
                }
                if (be.getRestoreReuseCapability() < required) {
                    return "backend " + be.getHost() + " (capability " + be.getRestoreReuseCapability() + ")";
                }
            }
            return null;
        } catch (Exception e) {
            LOG.warn("failed to check the restore reuse capability of the nodes", e);
            return unknownIsIncapable ? "unknown nodes" : null;
        }
    }

    /**
     * Select the existing partitions that may keep their local data, by the chain of conditions in
     * {@link RestoreReuseJudge}. Only the partitions of the A1 branch (existing partition, see restoredVersionInfo)
     * are looked at, so it must be called after the meta is prepared and before the state token of the overwritten
     * partitions is invalidated. The candidates are kept in reuseCandidates, each decided here by its state token.
     * Never fails the job: any problem selects nothing.
     *
     * @return the (table id, partition id) of the candidates
     */
    @VisibleForTesting
    Set<Pair<Long, Long>> selectReuseCandidates(Database db) {
        return selectReuseCandidates(db, Collections.emptyMap(), Collections.emptyMap());
    }

    /**
     * @param atomicStaging atomic restore: the staging tables bound to the table being replaced, by the name of the
     *         table in the backup. The candidates are the partitions of the table being replaced then.
     * @param tabletBases atomic restore: the staging tablet to the tablet of the table being replaced
     */
    @VisibleForTesting
    Set<Pair<Long, Long>> selectReuseCandidates(Database db, Map<String, OlapTable> atomicStaging,
            Map<Long, TabletRef> tabletBases) {
        reuseCandidates.clear();
        reuseResult = null;
        Set<Pair<Long, Long>> keys = Sets.newHashSet();
        if (!Config.enable_restore_partition_reuse || Config.isCloudMode()
                || (isAtomicRestore && !Config.enable_restore_atomic_reuse)
                || !(jobInfo.content == null || jobInfo.content == BackupContent.ALL)) {
            return keys;
        }
        String incapableNode = findNodeWithoutReuseCapability();
        if (incapableNode != null) {
            // Download all the data as before: no local source snapshot is sent.
            RestoreReuseResult result = new RestoreReuseResult();
            for (BackupOlapTableInfo tblInfo : jobInfo.backupOlapTableObjects.values()) {
                for (int i = 0; i < tblInfo.partitions.size(); i++) {
                    result.addRejected(MIXED_VERSIONS);
                }
            }
            reuseResult = result;
            LOG.info("restore reuse: {} does not have the capability of it, download all. job: {}", incapableNode,
                    jobId);
            return keys;
        }
        try {
            RestoreReuseResult result = new RestoreReuseResult();
            List<ReuseCandidate> candidates = Lists.newArrayList();
            for (Map.Entry<String, BackupOlapTableInfo> tblEntry : jobInfo.backupOlapTableObjects.entrySet()) {
                BackupOlapTableInfo tblInfo = tblEntry.getValue();
                Table localTbl = db.getTableNullable(jobInfo.getAliasByOriginNameIfSet(tblEntry.getKey()));
                if (localTbl == null || localTbl.getType() != TableType.OLAP) {
                    continue;
                }
                OlapTable localOlapTbl = (OlapTable) localTbl;
                OlapTable stagingTbl = isAtomicRestore ? atomicStaging.get(tblEntry.getKey()) : null;
                localOlapTbl.readLock();
                try {
                    for (Map.Entry<String, BackupPartitionInfo> partEntry : tblInfo.partitions.entrySet()) {
                        Partition localPart = localOlapTbl.getPartition(partEntry.getKey(), false);
                        if (localPart == null) {
                            continue;
                        }
                        // Atomic restore: the staging partition of the same name is the one to restore, and its
                        // tablets must be bound to the local ones. Otherwise the partition of the restored
                        // partitions (the A1 branch).
                        Partition stagingPart = null;
                        String atomicReject = null;
                        if (isAtomicRestore) {
                            atomicReject = stagingTbl == null ? "ATOMIC_NOT_BOUND_TABLE"
                                    : checkAtomicBinding(localOlapTbl, localPart, stagingTbl, tabletBases);
                            if (stagingTbl != null) {
                                stagingPart = stagingTbl.getPartition(partEntry.getKey(), false);
                            }
                        } else if (!restoredVersionInfo.contains(localOlapTbl.getId(), localPart.getId())) {
                            continue;
                        }
                        RestoreReuseJudge.Input in = buildReuseInput(tblEntry.getKey(), partEntry.getValue(),
                                localOlapTbl, localPart);
                        in.atomicReject = atomicReject;
                        boolean tokenEqual = RestoreReuseJudge.tokenEqual(in);
                        String tokenReject = RestoreReuseJudge.firstRejectByToken(in);
                        if (tokenReject != null) {
                            result.addRejected(tokenReject);
                            if (tokenEqual) {
                                LOG.info("restore reuse: partition {}.{} has the state token of the backup but is "
                                        + "not kept by it: {}, job: {}", localOlapTbl.getName(), partEntry.getKey(),
                                        tokenReject, jobId);
                            }
                            continue;
                        }
                        ReuseCandidate candidate = new ReuseCandidate();
                        candidate.tableId = localOlapTbl.getId();
                        candidate.partitionId = localPart.getId();
                        candidate.tableName = localOlapTbl.getName();
                        candidate.partitionName = partEntry.getKey();
                        if (isAtomicRestore) {
                            candidate.atomic = true;
                            candidate.stagingTableId = stagingTbl.getId();
                            candidate.stagingPartitionId = stagingPart.getId();
                        }
                        candidate.version = in.backupPartition.version;
                        // the version isStillAtVersion checks the partition at
                        candidate.digestVersion = in.backupPartition.version;
                        candidate.bytes = in.getSingleReplicaBytes();
                        candidate.bytesAllReplicas = RestoreReuseShadowStats.getAllReplicasLocalDataSize(localPart);
                        candidate.tokenEqual = tokenEqual;
                        if (Config.restore_reuse_trust_state_token) {
                            candidate.decide(true, RestoreReuseResult.KEPT_STATE_TOKEN);
                        } else {
                            // Observation only: the partition would be kept if the state token were trusted, it is
                            // downloaded, and counted in the token_equal of ReuseEstimate.
                            candidate.decide(false, TOKEN_NOT_TRUSTED);
                        }
                        candidates.add(candidate);
                    }
                } finally {
                    localOlapTbl.readUnlock();
                }
            }
            reuseResult = result;
            reuseCandidates = candidates;
            for (ReuseCandidate candidate : candidates) {
                keys.add(Pair.of(candidate.tableId, candidate.partitionId));
            }
            LOG.info("restore reuse: {} candidate partitions, trust the state token {}, not candidates: {}, job: {}",
                    candidates.size(), Config.restore_reuse_trust_state_token, result.getRejected(), jobId);
        } catch (Exception e) {
            LOG.warn("failed to select the candidate partitions of restore reuse, download all. job: {}", jobId, e);
            reuseCandidates.clear();
            reuseResult = null;
            keys.clear();
        }
        return keys;
    }

    /**
     * Atomic restore: whether the staging partition of the same name is bound to the local partition, tablet by tablet
     * and replica by replica, as bindLocalAndRemoteOlapTableReplicas does: the staging tablet has its local tablet
     * as the base tablet, and each of its replicas is on the backend of the local replica at the same position.
     * The data of the partition can be made from the local tablets only then. The caller holds the read lock of the
     * local table.
     *
     * @return null if bound, otherwise the reason
     */
    @VisibleForTesting
    static String checkAtomicBinding(OlapTable localTbl, Partition localPart, OlapTable stagingTbl,
            Map<Long, TabletRef> tabletBases) {
        Partition stagingPart = stagingTbl.getPartition(localPart.getName(), false);
        if (stagingPart == null) {
            return "ATOMIC_NOT_BOUND_PARTITION";
        }
        for (MaterializedIndex localIdx : localPart.getMaterializedIndices(IndexExtState.VISIBLE)) {
            MaterializedIndex stagingIdx;
            if (localIdx.getId() == localTbl.getBaseIndexId()) {
                // the base index is named by the table, which is renamed to the temp name
                stagingIdx = stagingPart.getBaseIndex();
            } else {
                Long stagingIdxId = stagingTbl.getIndexIdByName(localTbl.getIndexNameById(localIdx.getId()));
                stagingIdx = stagingIdxId == null ? null : stagingPart.getIndex(stagingIdxId);
            }
            if (stagingIdx == null || stagingIdx.getTablets().size() != localIdx.getTablets().size()) {
                return "ATOMIC_NOT_BOUND_INDEX";
            }
            for (int i = 0; i < localIdx.getTablets().size(); i++) {
                Tablet localTablet = localIdx.getTablets().get(i);
                Tablet stagingTablet = stagingIdx.getTablets().get(i);
                TabletRef ref = tabletBases.get(stagingTablet.getId());
                if (ref == null || ref.tabletId != localTablet.getId()) {
                    return "ATOMIC_NOT_BOUND_TABLET";
                }
                List<Replica> localReplicas = localTablet.getReplicas();
                List<Replica> stagingReplicas = stagingTablet.getReplicas();
                if (localReplicas.isEmpty() || localReplicas.size() != stagingReplicas.size()) {
                    return "ATOMIC_NOT_BOUND_REPLICA";
                }
                for (int j = 0; j < localReplicas.size(); j++) {
                    if (localReplicas.get(j).getBackendIdWithoutException()
                            != stagingReplicas.get(j).getBackendIdWithoutException()) {
                        return "ATOMIC_NOT_BOUND_REPLICA";
                    }
                }
            }
        }
        return null;
    }

    /**
     * Fix the decisions: the partitions that keep their data are taken out of the restore (no file mapping, no
     * restoredVersionInfo, so no snapshot, download or move task, and the state token is kept), and the state token
     * of the other overwritten partitions is invalidated, then go on to SNAPSHOTING.
     */
    @VisibleForTesting
    void finishVerifying(Database db) {
        RestoreReuseResult result = reuseResult != null ? reuseResult : new RestoreReuseResult();
        Set<Pair<Long, Long>> kept = Sets.newHashSet();
        for (ReuseCandidate candidate : reuseCandidates) {
            if ((candidate.kept || candidate.incremental) && !isStillAtVersion(db, candidate)) {
                candidate.decide(false, RestoreReuseResult.DOWNLOAD_CHANGED);
            }
            result.addDecision(candidate.toDecision());
            LOG.info("restore reuse: partition {}, job: {}", candidate.toDecision(), jobId);
            if (candidate.kept && !candidate.atomic) {
                kept.add(Pair.of(candidate.tableId, candidate.partitionId));
                restoredVersionInfo.remove(candidate.tableId, candidate.partitionId);
            }
            // An atomic restore keeps the file mapping of a kept partition: its snapshot task makes the local snapshot
            // of the staging tablets, only the download is skipped.
        }
        fileMapping.removePartitions(kept);
        reuseResult = result;
        // From now on the staging replicas are loaded from the local source, the tablet scheduler leaves them alone
        // until the job is done.
        setLocalSourceStagingPartitions(computeLocalSourceStagingPartitions(result));
        // The data of the partitions to download is about to change.
        invalidateRestoreLineageOfOverwrittenPartitions(db);
        clearReuseWorkingState();
        LOG.info("restore reuse: keep {} partitions ({} bytes of a single replica), append the increment to {}, "
                + "download {} of the candidates, job: {}", result.getKeptPartitions(),
                result.getKeptBytesSingleReplica(), result.getIncrementalPartitions(),
                result.getDownloadedPartitions(), jobId);

        prepareAndSendSnapshotTaskForOlapTable(db);
        state = RestoreJobState.SNAPSHOTING;
    }

    /** The staging partitions whose data is loaded from a local source, see localSourceStagingPartitions. */
    @VisibleForTesting
    static Set<Long> computeLocalSourceStagingPartitions(RestoreReuseResult result) {
        if (result == null) {
            return Collections.emptySet();
        }
        Set<Long> partitions = Sets.newHashSet();
        for (RestoreReuseResult.Decision decision : result.getDecisions()) {
            if (decision.atomic && (decision.kept || decision.incremental)) {
                partitions.add(decision.stagingPartitionId);
            }
        }
        return partitions.isEmpty() ? Collections.emptySet() : Collections.unmodifiableSet(partitions);
    }

    /** Whether the replicas of the partition are protected from repair by the tablet scheduler. Takes no lock. */
    public boolean isLocalSourceStagingPartition(long partitionId) {
        return localSourceStagingPartitions.contains(partitionId);
    }

    @VisibleForTesting
    Set<Long> getLocalSourceStagingPartitions() {
        return localSourceStagingPartitions;
    }

    /** Replaces the set and tells the backup handler, which keeps the ids of the jobs with a non-empty set. */
    private void setLocalSourceStagingPartitions(Set<Long> partitions) {
        this.localSourceStagingPartitions = partitions;
        BackupHandler handler = env == null ? null : env.getBackupHandler();
        if (handler != null) {
            handler.syncLocalSourceJob(this);
        }
    }

    /** The job is done: the staging partitions are no longer protected. */
    private void clearLocalSourceStagingPartitions() {
        setLocalSourceStagingPartitions(Collections.emptySet());
    }

    private boolean isStillAtVersion(Database db, ReuseCandidate candidate) {
        OlapTable tbl = (OlapTable) db.getTableNullable(candidate.tableId);
        if (tbl == null) {
            return false;
        }
        tbl.readLock();
        try {
            Partition part = tbl.getPartition(candidate.partitionId);
            if (part == null || part.getVisibleVersion() != candidate.digestVersion) {
                return false;
            }
            for (MaterializedIndex index : part.getMaterializedIndices(IndexExtState.VISIBLE)) {
                for (Tablet tablet : index.getTablets()) {
                    for (Replica replica : tablet.getReplicas()) {
                        if (!RestoreReuseJudge.isReplicaHealthyAt(replica, candidate.digestVersion)) {
                            return false;
                        }
                    }
                }
            }
            return true;
        } finally {
            tbl.readUnlock();
        }
    }

    /**
     * The partitions that keep their local data must still be at the version of the backup, which no write is
     * allowed to change while the table is in the RESTORE state. A different version means a write that should not
     * have happened, the job fails with the reason rather than commit data that is not the backup.
     */
    @VisibleForTesting
    Status checkKeptPartitions() {
        if (reuseResult == null || (reuseResult.getKeptPartitions() == 0
                && reuseResult.getIncrementalPartitions() == 0)) {
            return Status.OK;
        }
        Database db = env.getInternalCatalog().getDbNullable(dbId);
        if (db == null) {
            return new Status(ErrCode.NOT_FOUND, "database " + dbId + " does not exist");
        }
        List<RestoreReuseResult.Decision> toCheck = Lists.newArrayList(reuseResult.getKeptDecisions());
        toCheck.addAll(reuseResult.getIncrementalDecisions());
        for (RestoreReuseResult.Decision decision : toCheck) {
            Table tbl = db.getTableNullable(decision.tableId);
            if (!(tbl instanceof OlapTable) || !tbl.writeLockIfExist()) {
                return new Status(ErrCode.NOT_FOUND, "table " + decision.tableName + " of the kept partition "
                        + decision.partitionName + " has been dropped");
            }
            try {
                Partition part = ((OlapTable) tbl).getPartition(decision.partitionId);
                if (part == null) {
                    return new Status(ErrCode.NOT_FOUND, "the kept partition " + decision.tableName + "."
                            + decision.partitionName + " has been dropped");
                }
                if (part.getVisibleVersion() != decision.version) {
                    return new Status(ErrCode.COMMON_ERROR, "the visible version of the partition "
                            + decision.tableName + "." + decision.partitionName + " that "
                            + (decision.incremental ? "is restored by appending the increment" : "keeps its local data")
                            + " changed from " + decision.version + " to " + part.getVisibleVersion()
                            + " during the restore, the local data is not what was verified any more");
                }
                if (!stillHasKeptStateToken(db, decision, part)) {
                    return new Status(ErrCode.COMMON_ERROR, "the state token of the partition "
                            + decision.tableName + "." + decision.partitionName + " that keeps its local data by it"
                            + " changed during the restore (" + PartitionStateToken.effective(part) + ", a change"
                            + " that keeps the version), the local data is not what was verified any more");
                }
            } finally {
                tbl.writeUnlock();
            }
        }
        return Status.OK;
    }

    /**
     * Whether a partition kept by its state token (KEPT_STATE_TOKEN) still has the token of the backup partition, as
     * when it was judged: a change that keeps the version (a schema change or rollup job that finished, an admin
     * operation on a replica) invalidates or taints the token, which the check of the version does not see. True for
     * a decision of another reason. The caller holds the write lock of the table, and the backup table of the
     * decision is found as the candidate was, by the name the restore gives it.
     */
    private boolean stillHasKeptStateToken(Database db, RestoreReuseResult.Decision decision, Partition part) {
        if (!RestoreReuseResult.KEPT_STATE_TOKEN.equals(decision.reason)) {
            return true;
        }
        String backupToken = null;
        for (Map.Entry<String, BackupOlapTableInfo> tblEntry : jobInfo.backupOlapTableObjects.entrySet()) {
            Table tbl = db.getTableNullable(jobInfo.getAliasByOriginNameIfSet(tblEntry.getKey()));
            if (tbl != null && tbl.getId() == decision.tableId) {
                backupToken = tblEntry.getValue().getPartInfo(decision.partitionName).stateToken;
                break;
            }
        }
        String localToken = PartitionStateToken.effective(part);
        return localToken != null && localToken.equals(backupToken);
    }

    // A table in the backup meta, as the backup meta carried it.
    static class BackupStamps {
        OlapTable table;
    }

    /**
     * Read the tables that the backup meta carries, before their state tokens are cleared, for the structure checks
     * of the keep by the state token (RestoreReuseJudge#checkSameStructure). The restore source and the restore
     * lineage of the backup meta are added by the logical verification.
     */
    @VisibleForTesting
    void captureBackupStamps() {
        backupStamps = Maps.newHashMap();
        if (backupMeta == null) {
            return;
        }
        for (String tableName : jobInfo.backupOlapTableObjects.keySet()) {
            Table remoteTbl = backupMeta.getTable(tableName);
            if (!(remoteTbl instanceof OlapTable)) {
                continue;
            }
            OlapTable remoteOlap = (OlapTable) remoteTbl;
            BackupStamps stamps = new BackupStamps();
            stamps.table = remoteOlap;
            backupStamps.put(tableName, stamps);
        }
    }

    private RestoreReuseJudge.Input buildReuseInput(String backupTableName, BackupPartitionInfo partInfo,
            OlapTable localTbl, Partition localPart) {
        RestoreReuseJudge.Input in = new RestoreReuseJudge.Input();
        in.atomicRestore = isAtomicRestore;
        in.allowLoad = allowLoad;
        in.cloudMode = Config.isCloudMode();
        in.minPartitionBytes = Config.restore_reuse_min_partition_bytes;
        in.backupPartition = partInfo;
        in.localTable = localTbl;
        in.localPartition = localPart;
        in.localStateToken = PartitionStateToken.effective(localPart);
        in.backupStateToken = partInfo.stateToken;
        BackupStamps stamps = backupStamps.get(backupTableName);
        if (stamps != null) {
            in.backupOlapTable = stamps.table;
        }
        return in;
    }

    // A candidate partition of partition level reuse, not persisted.
    @VisibleForTesting
    static class ReuseCandidate {
        long tableId;
        long partitionId;
        String tableName;
        String partitionName;
        // the version of the backup
        long version;
        long bytes;
        long bytesAllReplicas;
        // the version the partition must still be at when the decisions are fixed, see isStillAtVersion
        long digestVersion;
        // atomic restore: the staging table and partition the data goes to
        boolean atomic = false;
        long stagingTableId;
        long stagingPartitionId;
        // the outcome: restored incrementally, never in this version
        boolean incremental = false;
        // the local partition has the state token of the backup partition (whether or not it was kept by it)
        boolean tokenEqual = false;
        boolean decided = false;
        boolean kept = false;
        String reason;

        void decide(boolean kept, String reason) {
            this.decided = true;
            this.kept = kept;
            this.reason = reason;
            this.incremental = false;
        }

        void decideIncremental() {
            this.decided = true;
            this.kept = false;
            this.incremental = true;
            this.reason = RestoreReuseResult.INCREMENTAL_VERIFIED;
        }

        RestoreReuseResult.Decision toDecision() {
            RestoreReuseResult.Decision decision = new RestoreReuseResult.Decision();
            decision.tableId = tableId;
            decision.partitionId = partitionId;
            decision.tableName = tableName;
            decision.partitionName = partitionName;
            // an incremental partition: the local version, and the backup in targetVersion
            decision.version = incremental ? digestVersion : version;
            decision.targetVersion = incremental ? version : 0;
            decision.bytes = bytes;
            decision.bytesAllReplicas = bytesAllReplicas;
            decision.kept = kept;
            decision.incremental = incremental;
            decision.reason = reason;
            decision.atomic = atomic;
            decision.stagingTableId = stagingTableId;
            decision.stagingPartitionId = stagingPartitionId;
            decision.tokenEqual = tokenEqual;
            return decision;
        }
    }

    /**
     * Invalidate the state token of the existing partitions that this job overwrites in place (non-atomic
     * restore, see restoredVersionInfo). Partitions and tables created by this job are dropped if the job is
     * cancelled, and atomic restore does not touch the origin table before it is replaced, so they are not
     * invalidated here. Only the state token in this version, the restore lineage is added by the logical
     * verification, which keeps the name of the method.
     */
    @VisibleForTesting
    void invalidateRestoreLineageOfOverwrittenPartitions(Database db) {
        invalidateRestoreLineageOfOverwrittenPartitions(db, Collections.emptySet());
    }

    /**
     * @param skipped the (table id, partition id) of the partitions whose state token is kept, because they are
     *         candidates of partition level reuse that are not decided yet
     */
    @VisibleForTesting
    void invalidateRestoreLineageOfOverwrittenPartitions(Database db, Set<Pair<Long, Long>> skipped) {
        for (long tblId : restoredVersionInfo.rowKeySet()) {
            Table tbl = db.getTableNullable(tblId);
            if (!(tbl instanceof OlapTable)) {
                continue;
            }
            OlapTable olapTbl = (OlapTable) tbl;
            if (!olapTbl.writeLockIfExist()) {
                continue;
            }
            try {
                for (long partId : restoredVersionInfo.row(tblId).keySet()) {
                    if (skipped.contains(Pair.of(tblId, partId))) {
                        continue;
                    }
                    Partition part = olapTbl.getPartition(partId);
                    if (part != null) {
                        // the data of the partition is being replaced: it has no state until the job stamps it.
                        // A taint is kept until the download is logged, see clearTaintOfDownloadedPartitions.
                        PartitionStateToken.invalidate(part);
                    }
                }
            } finally {
                olapTbl.writeUnlock();
            }
        }
    }

    /**
     * Clear the state tokens the backup meta carries, which must not be inherited: the restored partitions adopt
     * the tokens of the job info (stampStateToken). Only the state token in this version, the restore source and the
     * restore lineage are added by the logical verification, which keeps the name of the method.
     */
    @VisibleForTesting
    void clearRestoreLineageInBackupMeta() {
        for (String tableName : jobInfo.backupOlapTableObjects.keySet()) {
            Table remoteTbl = backupMeta.getTable(tableName);
            if (!(remoteTbl instanceof OlapTable)) {
                continue;
            }
            for (Partition remotePart : ((OlapTable) remoteTbl).getAllPartitions()) {
                remotePart.setStateToken(null);
            }
        }
    }

    /**
     * Write the state token of all partitions restored by this job, both the existing partitions that
     * are overwritten and the newly created partitions and tables (stampStateToken). The token is always taken from
     * the job info of this job, and never inherited from the backup meta. Called on both master and replay. Only the
     * state token in this version, restoreTime is not read: the restore lineage is added by the logical verification,
     * which keeps the name and the signature of the method.
     */
    @VisibleForTesting
    void stampRestoreLineage(Database db, long restoreTime) {
        for (Map.Entry<String, BackupOlapTableInfo> tblEntry : jobInfo.backupOlapTableObjects.entrySet()) {
            Table tbl = db.getTableNullable(jobInfo.getAliasByOriginNameIfSet(tblEntry.getKey()));
            if (tbl == null || tbl.getType() != TableType.OLAP) {
                continue;
            }
            OlapTable olapTbl = (OlapTable) tbl;
            if (!olapTbl.writeLockIfExist()) {
                continue;
            }
            try {
                BackupOlapTableInfo tblInfo = tblEntry.getValue();
                for (Map.Entry<String, BackupPartitionInfo> partEntry : tblInfo.partitions.entrySet()) {
                    Partition part = olapTbl.getPartition(partEntry.getKey(), false);
                    if (part == null) {
                        continue;
                    }
                    stampStateToken(olapTbl, part, partEntry.getValue());
                }
            } finally {
                olapTbl.writeUnlock();
            }
        }
    }

    /**
     * The state token of a partition this job restored, see PartitionStateToken. Called on the master and on replay
     * with the write lock of the table; everything it reads is persisted with the job or replayed before it.
     * <ul>
     * <li>If the master did not track the tokens (restore_state_token_track) when it committed the job, no name
     * is given: the partitions are invalidated. A cluster that does not track the tokens gives no names.</li>
     * <li>A tainted partition stays tainted. The taint of a partition that is downloaded as a whole was cleared when
     * the download was logged (clearTaintOfDownloadedPartitions), so a taint here came later, or is the one of a
     * partition that keeps its local data (kept, or the base of an incremental append), whose replicas the restore
     * did not replace. Taint wins in any order of the taint and of this, so the master, which may run this before
     * the operation that taints and log it after, and a follower, which replays the operation first, agree.</li>
     * <li>A partition kept without a digest (an unverified keep, restore_reuse_allow_unverified_keep) is not proved
     * to be the backup partition: invalidated, no name.</li>
     * <li>A partition that is not at the version of the backup, which is the version the job set (a load made
     * visible in between): invalidated, the name of the backup would be bound to other data.</li>
     * <li>Otherwise it is the backup partition and adopts its token, bound to the version this frontend has for it.
     * A backup partition without a token leaves none, and the partition is EMPTY only if it was restored at the
     * initial version, as the empty partition of the backup.</li>
     * </ul>
     */
    private void stampStateToken(OlapTable olapTbl, Partition part, BackupPartitionInfo partInfo) {
        if (PartitionStateToken.isTainted(part)) {
            LOG.info("restore: partition {} is tainted, it keeps the taint, job: {}", part.getId(), jobId);
            return;
        }
        if (!stampStateTokens) {
            // the master did not track the tokens when it committed the job: no name
            PartitionStateToken.invalidate(part);
            return;
        }
        if (!restoresData()) {
            // a restore of the metadata only creates empty replicas at the version of the backup: the token of the
            // source would name data the partition does not have
            PartitionStateToken.invalidate(part);
            return;
        }
        RestoreReuseResult.Decision kept = reuseResult == null ? null
                : reuseResult.getKept(olapTbl.getId(), part.getId());
        if (kept != null && kept.isUnverifiedKeep()) {
            PartitionStateToken.invalidate(part);
            return;
        }
        if (part.getVisibleVersion() != partInfo.version) {
            LOG.info("restore: partition {} is at version {}, not at the version {} of the backup, no state token, "
                    + "job: {}", part.getId(), part.getVisibleVersion(), partInfo.version, jobId);
            PartitionStateToken.invalidate(part);
            return;
        }
        String token = partInfo.stateToken;
        part.setStateToken(token == null ? null : new PartitionStateToken(token, part.getVisibleVersion(),
                part.getVisibleVersionTime()));
    }

    // whether the backup has the data of the tablets, not only the metadata (BackupContent.METADATA_ONLY)
    private boolean restoresData() {
        return jobInfo.content == null || jobInfo.content == BackupContent.ALL;
    }

    /**
     * Non-atomic restore: the partitions in restoredVersionInfo that are downloaded as a whole (not an incremental
     * append, whose base is the local data) get all their replicas from the backup, so their taint is cleared
     * (invalid until stampStateToken). Done where the DOWNLOAD state is logged, with the write locks of the tables
     * held until it is logged on the master, and on the replay of that log: an operation that taints is ordered
     * before or after it the same way on every frontend. If the job is cancelled from then on, the partitions are
     * tainted again (taintOverwrittenPartitions), as their tablets may be in part moved.
     */
    @VisibleForTesting
    void clearTaintOfDownloadedPartitions(Database db) {
        if (!restoresData()) {
            // nothing is downloaded: the replicas are not made from the backup
            return;
        }
        for (long tblId : restoredVersionInfo.rowKeySet()) {
            Table tbl = db.getTableNullable(tblId);
            if (!(tbl instanceof OlapTable) || !tbl.writeLockIfExist()) {
                continue;
            }
            try {
                for (long partId : restoredVersionInfo.row(tblId).keySet()) {
                    Partition part = ((OlapTable) tbl).getPartition(partId);
                    if (part != null && PartitionStateToken.isTainted(part)
                            && !isIncrementalPartition(tblId, partId)) {
                        part.setStateToken(PartitionStateToken.invalid());
                    }
                }
            } finally {
                tbl.writeUnlock();
            }
        }
    }

    /**
     * Non-atomic restore cancelled after the DOWNLOAD state was logged: in COMMITTING a part of the tablets of a
     * partition may be moved to the backup data (or appended) and the others not, so the replicas of the partitions
     * the job overwrites do not agree: taint them (see PartitionStateToken). From DOWNLOAD on, the master and the
     * followers have the same restoredVersionInfo, and a follower has the job in DOWNLOAD or COMMIT (COMMITTING is
     * not logged), so both taint the same partitions. An atomic restore does not touch the table it replaces, and
     * its restoredVersionInfo is empty.
     */
    @VisibleForTesting
    void taintOverwrittenPartitions(Database db) {
        for (long tblId : restoredVersionInfo.rowKeySet()) {
            Table tbl = db.getTableNullable(tblId);
            if (!(tbl instanceof OlapTable) || !tbl.writeLockIfExist()) {
                continue;
            }
            try {
                for (long partId : restoredVersionInfo.row(tblId).keySet()) {
                    Partition part = ((OlapTable) tbl).getPartition(partId);
                    if (part != null) {
                        PartitionStateToken.taint(part);
                    }
                }
            } finally {
                tbl.writeUnlock();
            }
        }
        LOG.info("restore job {} is cancelled after the download began, taint the partitions it overwrites: {}",
                jobId, restoredVersionInfo.cellSet().size());
    }

    private static boolean isAfterDownloadLogged(RestoreJobState state) {
        return state == RestoreJobState.DOWNLOAD || state == RestoreJobState.DOWNLOADING
                || state == RestoreJobState.COMMIT || state == RestoreJobState.COMMITTING;
    }

    private String getRestoreObjs() {
        Preconditions.checkState(jobInfo != null);
        return jobInfo.getInfo();
    }

    @Override
    public boolean isDone() {
        if (state == RestoreJobState.FINISHED || state == RestoreJobState.CANCELLED) {
            return true;
        }
        return false;
    }

    // cancel by user
    @Override
    public synchronized Status cancel() {
        if (isDone()) {
            return new Status(ErrCode.COMMON_ERROR,
                    "Job with label " + label + " can not be cancelled. state: " + state);
        }

        status = new Status(ErrCode.COMMON_ERROR, "user cancelled, current state: " + state.name());
        cancelInternal(false);
        return Status.OK;
    }

    private void cancelInternal(boolean isReplay) {
        // We need to clean the residual due to current state
        if (!isReplay) {
            switch (state) {
                case SNAPSHOTING:
                    // remove all snapshot tasks in AgentTaskQueue
                    for (Long taskId : unfinishedSignatureToId.keySet()) {
                        AgentTaskQueue.removeTaskOfType(TTaskType.MAKE_SNAPSHOT, taskId);
                    }
                    break;
                case DOWNLOADING:
                    // remove all down tasks in AgentTaskQueue
                    for (Long taskId : unfinishedSignatureToId.keySet()) {
                        AgentTaskQueue.removeTaskOfType(TTaskType.DOWNLOAD, taskId);
                    }
                    break;
                case COMMITTING:
                    // remove all dir move tasks in AgentTaskQueue
                    for (Long taskId : unfinishedSignatureToId.keySet()) {
                        AgentTaskQueue.removeTaskOfType(TTaskType.MOVE, taskId);
                    }
                    break;
                default:
                    break;
            }
        }

        if (isAfterDownloadLogged(state)) {
            Database db = env.getInternalCatalog().getDbNullable(dbId);
            if (db != null) {
                taintOverwrittenPartitions(db);
            }
        }

        cleanMetaObjects(isReplay);

        if (!isReplay) {
            restoredPartitions.clear();
            restoredTbls.clear();
            restoredResources.clear();
            clearReuseWorkingState();

            // backupMeta is useless
            backupMeta = null;

            com.google.common.collect.Table<Long, Long, SnapshotInfo> savedSnapshotInfos = snapshotInfos;
            snapshotInfos = HashBasedTable.create();
            fileMapping.clear();
            jobInfo.releaseSnapshotInfo();
            createReplicasTimeStamp = -1;

            RestoreJobState curState = state;
            finishedTime = System.currentTimeMillis();
            state = RestoreJobState.CANCELLED;
            clearLocalSourceStagingPartitions();
            // log
            env.getEditLog().logRestoreJob(this);

            LOG.info("finished to cancel restore job. current state: {}. is replay: {}. {}",
                    curState.name(), isReplay, this);

            // Send release snapshot tasks after log restore job, so that the snapshot won't be released
            // before the cancelled restore job is persisted.
            releaseSnapshots(savedSnapshotInfos, false);
            return;
        }

        // replay
        clearLocalSourceStagingPartitions();
        LOG.info("finished to cancel restore job. is replay: {}. {}", isReplay, this);
    }

    protected void cleanMetaObjects(boolean isReplay) {

        // clean restored objs
        Database db = env.getInternalCatalog().getDbNullable(dbId);
        if (db != null) {
            // rollback table's state to NORMAL
            setTableStateToNormalAndUpdateProperties(db, false, isReplay);

            // remove restored tbls
            for (Table restoreTbl : restoredTbls) {
                if (isAtomicRestore
                        && (restoreTbl.getType() == TableType.OLAP || restoreTbl.getType() == TableType.VIEW)
                        && !restoreTbl.getName().startsWith(ATOMIC_RESTORE_TABLE_PREFIX)) {
                    // In atomic restore, a table registered to db must have a name with the prefix,
                    // otherwise, it has not been registered and can be ignored here.
                    continue;
                }
                LOG.info("remove restored table when cancelled: {}", restoreTbl.getName());
                if (db.writeLockIfExist()) {
                    try {
                        if (restoreTbl.getType() == TableType.OLAP) {
                            OlapTable restoreOlapTable = (OlapTable) restoreTbl;
                            restoreOlapTable.writeLock();
                            try {
                                for (Partition part : restoreOlapTable.getPartitions()) {
                                    for (MaterializedIndex idx : part.getMaterializedIndices(IndexExtState.VISIBLE)) {
                                        for (Tablet tablet : idx.getTablets()) {
                                            Env.getCurrentInvertedIndex().deleteTablet(tablet.getId());
                                        }
                                    }
                                }
                                db.unregisterTable(restoreTbl.getName());
                            } finally {
                                restoreTbl.writeUnlock();
                            }
                        } else if (restoreTbl.getType() == TableType.VIEW) {
                            restoreTbl.writeLock();
                            try {
                                db.unregisterTable(restoreTbl.getName());
                            } finally {
                                restoreTbl.writeUnlock();
                            }
                        }
                    } finally {
                        db.writeUnlock();
                    }
                }

            }

            // remove restored partitions
            for (Pair<String, Partition> entry : restoredPartitions) {
                OlapTable restoreTbl = (OlapTable) db.getTableNullable(entry.first);
                if (restoreTbl == null) {
                    continue;
                }
                if (!restoreTbl.writeLockIfExist()) {
                    continue;
                }
                LOG.info("remove restored partition in table {} when cancelled: {}",
                        restoreTbl.getName(), entry.second.getName());
                try {
                    restoreTbl.dropPartitionAndReserveTablet(entry.second.getName());
                    for (MaterializedIndex index : entry.second.getMaterializedIndices(IndexExtState.ALL)) {
                        for (Tablet tablet : index.getTablets()) {
                            Env.getCurrentInvertedIndex().deleteTablet(tablet.getId());
                        }
                    }
                    if (Config.isNotCloudMode()) {
                        env.getInternalCatalog().erasePartitionDropBackendReplicas(Lists.newArrayList(entry.second));
                    }
                } finally {
                    restoreTbl.writeUnlock();
                }
            }

            // remove restored resource
            ResourceMgr resourceMgr = Env.getCurrentEnv().getResourceMgr();
            for (Resource resource : restoredResources) {
                LOG.info("remove restored resource when cancelled: {}", resource.getName());
                resourceMgr.dropResource(resource);
            }
        }
    }

    // The decisions of the partitions of the origin table that keep its data (or its base), atomic restore.
    private List<RestoreReuseResult.Decision> getAtomicLocalDecisions(long originTableId) {
        List<RestoreReuseResult.Decision> result = Lists.newArrayList();
        if (reuseResult != null) {
            for (RestoreReuseResult.Decision decision : reuseResult.getDecisions()) {
                if (decision.atomic && (decision.kept || decision.incremental) && decision.tableId == originTableId) {
                    result.add(decision);
                }
            }
        }
        return result;
    }

    private boolean hasAtomicLocalDecision(long originTableId) {
        return !getAtomicLocalDecisions(originTableId).isEmpty();
    }

    /**
     * Fail if a partition of the origin table that the staging table is made from has been written since it was proved
     * equal to the backup (its version changed, or a load is committed and not published yet). The caller holds the
     * write lock of the origin table.
     */
    @VisibleForTesting
    Status checkAtomicLocalVersions(OlapTable originTbl) {
        Database db = env.getInternalCatalog().getDbNullable(dbId);
        for (RestoreReuseResult.Decision decision : getAtomicLocalDecisions(originTbl.getId())) {
            Partition part = originTbl.getPartition(decision.partitionId);
            if (part == null) {
                return new Status(ErrCode.NOT_FOUND, "the partition " + decision.tableName + "."
                        + decision.partitionName + " that the restored data is made from has been dropped");
            }
            if (part.getVisibleVersion() != decision.version || part.getNextVersion() != decision.version + 1) {
                return new Status(ErrCode.COMMON_ERROR, "the visible version of the partition "
                        + decision.tableName + "." + decision.partitionName + " that the restored data is made from"
                        + " changed from " + decision.version + " to " + part.getVisibleVersion() + " (next version "
                        + part.getNextVersion() + ") during the restore, the local data is not what was verified any"
                        + " more. The table is not replaced, restore again");
            }
            if (!stillHasKeptStateToken(db, decision, part)) {
                return new Status(ErrCode.COMMON_ERROR, "the state token of the partition "
                        + decision.tableName + "." + decision.partitionName + " that the restored data is made from"
                        + " by it changed during the restore (" + PartitionStateToken.effective(part) + ", a change"
                        + " that keeps the version), the local data is not what was verified any more. The table is"
                        + " not replaced, restore again");
            }
        }
        return Status.OK;
    }

    /**
     * If a table keeps local data, lock all the origin tables that are replaced (db lock first, then the tables by
     * ascending id, the order of a query that reads several of them), and check the ones that have a local decision,
     * before any table is replaced: either all of them are at the version they were proved at, or none is touched. The
     * replace locks an origin table again (the lock is reentrant): if only the tables with a local decision were held,
     * the replace of another one with a lower id would wait for it while holding a higher one, against a query that
     * holds the lower one and waits for the higher. The caller holds the write lock of the database, and unlocks the
     * tables added to the locked list.
     */
    private Status lockAndCheckAtomicOriginTables(Database db, List<OlapTable> locked) {
        Map<Long, OlapTable> origins = Maps.newTreeMap();
        boolean anyLocalDecision = false;
        for (String tableName : jobInfo.backupOlapTableObjects.keySet()) {
            String originName = jobInfo.getAliasByOriginNameIfSet(tableName);
            if (Env.isStoredTableNamesLowerCase()) {
                originName = originName.toLowerCase();
            }
            Table originTbl = db.getTableNullable(originName);
            if (originTbl instanceof OlapTable) {
                origins.put(originTbl.getId(), (OlapTable) originTbl);
                anyLocalDecision |= hasAtomicLocalDecision(originTbl.getId());
            }
        }
        if (!anyLocalDecision) {
            // nothing to check: the tables are replaced one by one as without the reuse
            return Status.OK;
        }
        for (OlapTable originTbl : origins.values()) {
            originTbl.writeLock();
            locked.add(originTbl);
        }
        for (OlapTable originTbl : origins.values()) {
            if (!hasAtomicLocalDecision(originTbl.getId())) {
                continue;
            }
            Status st = checkAtomicLocalVersions(originTbl);
            if (!st.ok()) {
                return st;
            }
        }
        return Status.OK;
    }

    private Status atomicReplaceOlapTables(Database db, boolean isReplay) {
        // The database lock is held until all the tables are replaced (it is reentrant), and so are the write locks
        // of the origin tables that keep local data: no write is allowed to be in between the check of their
        // versions and the replace.
        if (!db.writeLockIfExist()) {
            return Status.OK;
        }
        List<OlapTable> lockedOrigins = Lists.newArrayList();
        try {
            if (!isReplay) {
                Status st = lockAndCheckAtomicOriginTables(db, lockedOrigins);
                if (!st.ok()) {
                    return st;
                }
            }
            return replaceOlapTablesAndViews(db, isReplay);
        } finally {
            for (int i = lockedOrigins.size() - 1; i >= 0; i--) {
                lockedOrigins.get(i).writeUnlock();
            }
            db.writeUnlock();
        }
    }

    /**
     * Atomic restore: a staging partition made from the local data of the table being replaced (kept, or the base of
     * an incremental append) has the replicas of that data, so it takes over the taint of the local partition (see
     * PartitionStateToken). The local partition may have been tainted after it was judged. Run on the master and on
     * replay, as the tables are replaced; the caller holds the write lock of the origin table.
     */
    private void carryTaintToStagingPartitions(OlapTable originTbl, OlapTable stagingTbl) {
        stagingTbl.writeLock();
        try {
            for (RestoreReuseResult.Decision decision : getAtomicLocalDecisions(originTbl.getId())) {
                Partition originPart = originTbl.getPartition(decision.partitionId);
                if (originPart != null && PartitionStateToken.isTainted(originPart)) {
                    PartitionStateToken.taint(stagingTbl.getPartition(decision.stagingPartitionId));
                    LOG.info("restore: the staging partition {} takes over the taint of partition {}, job: {}",
                            decision.stagingPartitionId, decision.partitionId, jobId);
                }
            }
        } finally {
            stagingTbl.writeUnlock();
        }
    }

    private Status replaceOlapTablesAndViews(Database db, boolean isReplay) {
        for (String tableName : jobInfo.backupOlapTableObjects.keySet()) {
            String originName = jobInfo.getAliasByOriginNameIfSet(tableName);
            if (Env.isStoredTableNamesLowerCase()) {
                originName = originName.toLowerCase();
            }
            String aliasName = tableAliasWithAtomicRestore(originName);

            if (!db.writeLockIfExist()) {
                return Status.OK;
            }
            try {
                Table newTbl = db.getTableNullable(aliasName);
                if (newTbl == null) {
                    LOG.warn("replace table from {} to {}, but the temp table is not found" + " isAtomicRestore: {}",
                            aliasName, originName, isAtomicRestore);
                    return new Status(ErrCode.COMMON_ERROR, "replace table failed, the temp table "
                            + aliasName + " is not found");
                }
                if (newTbl.getType() != TableType.OLAP) {
                    LOG.warn(
                            "replace table from {} to {}, but the temp table is not OLAP, it type is {}"
                                    + " isAtomicRestore: {}",
                            aliasName, originName, newTbl.getType(), isAtomicRestore);
                    return new Status(ErrCode.COMMON_ERROR, "replace table failed, the temp table " + aliasName
                            + " is not OLAP table, it is " + newTbl.getType());
                }

                OlapTable originOlapTbl = null;
                Table originTbl = db.getTableNullable(originName);
                if (originTbl != null) {
                    if (originTbl.getType() != TableType.OLAP) {
                        LOG.warn(
                                "replace table from {} to {}, but the origin table is not OLAP, it type is {}"
                                        + " isAtomicRestore: {}",
                                aliasName, originName, originTbl.getType(), isAtomicRestore);
                        return new Status(ErrCode.COMMON_ERROR, "replace table failed, the origin table "
                                + originName + " is not OLAP table, it is " + originTbl.getType());
                    }
                    originOlapTbl = (OlapTable) originTbl; // save the origin olap table, then drop it.
                }

                // replace the table.
                OlapTable newOlapTbl = (OlapTable) newTbl;
                newOlapTbl.writeLock();
                try {
                    // rename new table name to origin table name and add the new table to database.
                    db.unregisterTable(aliasName);
                    newOlapTbl.checkAndSetName(originName, false);
                    db.unregisterTable(originName);
                    db.registerTable(newOlapTbl);

                    // set the olap table state to normal immediately for querying
                    newOlapTbl.setState(OlapTableState.NORMAL);
                    LOG.info(
                            "restore with replace table {} name to {}, and set state to normal, origin table={}"
                                    + " isAtomicRestore: {}",
                            newOlapTbl.getId(), originName, originOlapTbl == null ? -1L : originOlapTbl.getId(),
                            isAtomicRestore);
                } catch (DdlException e) {
                    LOG.warn("restore with replace table {} name from {} to {}, isAtomicRestore: {}",
                            newOlapTbl.getId(), aliasName, originName, isAtomicRestore, e);
                    return new Status(ErrCode.COMMON_ERROR, "replace table from " + aliasName + " to " + originName
                            + " failed, reason=" + e.getMessage());
                } finally {
                    newOlapTbl.writeUnlock();
                }

                if (originOlapTbl != null) {
                    // The origin table is not used anymore, need to drop all its tablets.
                    originOlapTbl.writeLock();
                    try {
                        carryTaintToStagingPartitions(originOlapTbl, newOlapTbl);
                        LOG.info("drop the origin olap table {}. table={}" + " isAtomicRestore: {}",
                                originOlapTbl.getName(), originOlapTbl.getId(), isAtomicRestore);
                        Env.getCurrentEnv().onEraseOlapTable(originOlapTbl, isReplay);
                    } finally {
                        originOlapTbl.writeUnlock();
                    }
                }
            } finally {
                db.writeUnlock();
            }
        }
        for (BackupJobInfo.BackupViewInfo backupViewInfo : jobInfo.newBackupObjects.views) {
            String originName = jobInfo.getAliasByOriginNameIfSet(backupViewInfo.name);
            if (Env.isStoredTableNamesLowerCase()) {
                originName = originName.toLowerCase();
            }
            String aliasName = tableAliasWithAtomicRestore(originName);

            if (!db.writeLockIfExist()) {
                return Status.OK;
            }
            try {
                Table newTbl = db.getTableNullable(aliasName);
                if (newTbl == null) {
                    LOG.warn("replace view from {} to {}, but the temp view is not found" + " isAtomicRestore: {}",
                            aliasName, originName, isAtomicRestore);
                    return new Status(ErrCode.COMMON_ERROR, "replace view failed, the temp view "
                            + aliasName + " is not found");
                }
                if (newTbl.getType() != TableType.VIEW) {
                    LOG.warn(
                            "replace view from {} to {}, but the temp view is not VIEW, it type is {}"
                                    + " isAtomicRestore: {}",
                            aliasName, originName, newTbl.getType(), isAtomicRestore);
                    return new Status(ErrCode.COMMON_ERROR, "replace view failed, the temp view " + aliasName
                            + " is not OLAP, it is " + newTbl.getType());
                }

                View originViewTbl = null;
                Table originTbl = db.getTableNullable(originName);
                if (originTbl != null) {
                    if (originTbl.getType() != TableType.VIEW) {
                        LOG.warn(
                                "replace view from {} to {}, but the origin view is not VIEW, it type is {}"
                                        + " isAtomicRestore: {}",
                                aliasName, originName, originTbl.getType(), isAtomicRestore);
                        return new Status(ErrCode.COMMON_ERROR, "replace view failed, the origin view "
                                + originName + " is not VIEW, it is " + originTbl.getType());
                    }
                    originViewTbl = (View) originTbl; // save the origin view, then drop it.
                }

                // replace the view.
                View newViewTbl = (View) newTbl;
                newViewTbl.writeLock();
                try {
                    // rename new view name to origin view name and add the new view to database.
                    db.unregisterTable(aliasName);
                    db.unregisterTable(originName);
                    newViewTbl.setName(originName);
                    db.registerTable(newViewTbl);

                    LOG.info(
                            "restore with replace view {} name to {}, origin view={}"
                                    + " isAtomicRestore: {}",
                            newViewTbl.getId(), originName,
                            originViewTbl == null ? -1L : originViewTbl.getId(),
                            isAtomicRestore);
                } finally {
                    newViewTbl.writeUnlock();
                }
            } finally {
                db.writeUnlock();
            }
        }

        return Status.OK;
    }

    protected void setTableStateToNormalAndUpdateProperties(Database db, boolean committed, boolean isReplay) {
        for (String tableName : jobInfo.backupOlapTableObjects.keySet()) {
            Table tbl = db.getTableNullable(jobInfo.getAliasByOriginNameIfSet(tableName));
            if (tbl == null) {
                LOG.warn("table {} is not found and skip set state to normal", tableName);
                continue;
            }

            if (tbl.getType() != TableType.OLAP) {
                continue;
            }

            OlapTable olapTbl = (OlapTable) tbl;
            if (!tbl.writeLockIfExist()) {
                continue;
            }
            try {
                if (olapTbl.getState() == OlapTableState.RESTORE
                        || olapTbl.getState() == OlapTableState.RESTORE_WITH_LOAD) {
                    LOG.info("table {} set state from {} to normal", tableName, olapTbl.getState());
                    olapTbl.setState(OlapTableState.NORMAL);
                }
                if (olapTbl.isInAtomicRestore()) {
                    olapTbl.clearInAtomicRestore();
                    LOG.info("table {} set state from atomic restore to normal", tableName);
                }

                BackupOlapTableInfo tblInfo = jobInfo.backupOlapTableObjects.get(tableName);
                for (Map.Entry<String, BackupPartitionInfo> partitionEntry : tblInfo.partitions.entrySet()) {
                    String partitionName = partitionEntry.getKey();
                    Partition partition = olapTbl.getPartition(partitionName);
                    if (partition == null) {
                        LOG.warn("table {} partition {} is not found and skip set state to normal",
                                tableName, partitionName);
                        continue;
                    }
                    if (partition.getState() == PartitionState.RESTORE) {
                        partition.setState(PartitionState.NORMAL);
                    }
                }
                if (committed && reserveDynamicPartitionEnable) {
                    if (DynamicPartitionUtil.isDynamicPartitionTable(tbl)) {
                        DynamicPartitionUtil.registerOrRemoveDynamicPartitionTable(db.getId(), olapTbl, isReplay);
                        Env.getCurrentEnv().getDynamicPartitionScheduler().createOrUpdateRuntimeInfo(tbl.getId(),
                                DynamicPartitionScheduler.LAST_UPDATE_TIME, TimeUtils.getCurrentFormatTime());
                    }
                }
                if (committed && isBeingSynced) {
                    olapTbl.setBeingSyncedProperties();
                }
                if (committed) {
                    olapTbl.analyze(db.getName());
                }
            } finally {
                tbl.writeUnlock();
            }
        }
    }

    public static RestoreJob read(DataInput in) throws IOException {
        if (Env.getCurrentEnvJournalVersion() < FeMetaVersion.VERSION_136) {
            RestoreJob job = new RestoreJob();
            job.readFields(in);
            return job;
        } else {
            byte[] jsonBytes = GsonUtils.readJsonBytes(in);
            if (AbstractJob.isCompressedJobMarker(jsonBytes)) {
                return GsonUtils.fromJsonCompressed(in, RestoreJob.class);
            } else {
                return GsonUtils.fromJsonBytes(jsonBytes, RestoreJob.class);
            }
        }
    }

    @Deprecated
    @Override
    public void readFields(DataInput in) throws IOException {
        super.readFields(in);
        if (type == JobType.RESTORE_COMPRESSED) {
            type = JobType.RESTORE;

            Text text = new Text();
            text.readFields(in);
            if (LOG.isDebugEnabled() || text.getLength() > (100 << 20)) {
                LOG.info("read restore job compressed size {}", text.getLength());
            }

            ByteArrayInputStream bytesStream = new ByteArrayInputStream(text.getBytes());
            try (GZIPInputStream gzipStream = new GZIPInputStream(bytesStream)) {
                try (DataInputStream stream = new DataInputStream(gzipStream)) {
                    readOthers(stream);
                }
            }
        } else {
            readOthers(in);
        }
    }

    private void readOthers(DataInput in) throws IOException {
        backupTimestamp = Text.readString(in);
        jobInfo = BackupJobInfo.read(in);
        allowLoad = in.readBoolean();

        state = RestoreJobState.valueOf(Text.readString(in));

        if (in.readBoolean()) {
            backupMeta = BackupMeta.read(in);
        }

        fileMapping = RestoreFileMapping.read(in);

        metaPreparedTime = in.readLong();
        snapshotFinishedTime = in.readLong();
        downloadFinishedTime = in.readLong();

        if (Env.getCurrentEnvJournalVersion() < FeMetaVersion.VERSION_105) {
            int restoreReplicationNum = in.readInt();
            replicaAlloc = new ReplicaAllocation((short) restoreReplicationNum);
        } else {
            replicaAlloc = ReplicaAllocation.read(in);
        }

        int size = in.readInt();
        for (int i = 0; i < size; i++) {
            String tblName = Text.readString(in);
            Partition part = Partition.read(in);
            restoredPartitions.add(Pair.of(tblName, part));
        }

        size = in.readInt();
        for (int i = 0; i < size; i++) {
            restoredTbls.add(Table.read(in));
        }

        size = in.readInt();
        for (int i = 0; i < size; i++) {
            long tblId = in.readLong();
            int innerSize = in.readInt();
            for (int j = 0; j < innerSize; j++) {
                long partId = in.readLong();
                long version = in.readLong();
                // Useless but read it to compatible with meta
                long versionHash = in.readLong(); // CHECKSTYLE IGNORE THIS LINE
                restoredVersionInfo.put(tblId, partId, version);
            }
        }

        size = in.readInt();
        for (int i = 0; i < size; i++) {
            long tabletId = in.readLong();
            int innerSize = in.readInt();
            for (int j = 0; j < innerSize; j++) {
                long beId = in.readLong();
                SnapshotInfo info = SnapshotInfo.read(in);
                snapshotInfos.put(tabletId, beId, info);
            }
        }

        // restored resource
        size = in.readInt();
        for (int i = 0; i < size; i++) {
            restoredResources.add(Resource.read(in));
        }

        // read properties
        size = in.readInt();
        for (int i = 0; i < size; i++) {
            String key = Text.readString(in);
            String value = Text.readString(in);
            properties.put(key, value);
        }
        reserveReplica = Boolean.parseBoolean(properties.get(PROP_RESERVE_REPLICA));
        reserveDynamicPartitionEnable = Boolean.parseBoolean(properties.get(PROP_RESERVE_DYNAMIC_PARTITION_ENABLE));
        isBeingSynced = Boolean.parseBoolean(properties.get(PROP_IS_BEING_SYNCED));
        isCleanTables = Boolean.parseBoolean(properties.get(PROP_CLEAN_TABLES));
        isCleanPartitions = Boolean.parseBoolean(properties.get(PROP_CLEAN_PARTITIONS));
        isAtomicRestore = Boolean.parseBoolean(properties.get(PROP_ATOMIC_RESTORE));
        isForceReplace = Boolean.parseBoolean(properties.get(PROP_FORCE_REPLACE));
    }

    @Override
    public void gsonPostProcess() throws IOException {
        reserveReplica = Boolean.parseBoolean(properties.get(PROP_RESERVE_REPLICA));
        reserveDynamicPartitionEnable = Boolean.parseBoolean(properties.get(PROP_RESERVE_DYNAMIC_PARTITION_ENABLE));
        isBeingSynced = Boolean.parseBoolean(properties.get(PROP_IS_BEING_SYNCED));
        isCleanTables = Boolean.parseBoolean(properties.get(PROP_CLEAN_TABLES));
        isCleanPartitions = Boolean.parseBoolean(properties.get(PROP_CLEAN_PARTITIONS));
        isAtomicRestore = Boolean.parseBoolean(properties.get(PROP_ATOMIC_RESTORE));
        isForceReplace = Boolean.parseBoolean(properties.get(PROP_FORCE_REPLACE));
        // The set is not persisted: rebuild it from the persisted decisions of a job which is not done. The backup
        // handler learns about it when it adds the job (BackupHandler.addBackupOrRestoreJob).
        if (state != RestoreJobState.FINISHED && state != RestoreJobState.CANCELLED) {
            localSourceStagingPartitions = computeLocalSourceStagingPartitions(reuseResult);
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder(super.toString());
        sb.append(", backup ts: ").append(backupTimestamp);
        sb.append(", state: ").append(state.name());
        return sb.toString();
    }

    public static String tableAliasWithAtomicRestore(String tableName) {
        return ATOMIC_RESTORE_TABLE_PREFIX + tableName;
    }

    protected static class TabletRef {
        public long tabletId;
        public int schemaHash;

        TabletRef(long tabletId, int schemaHash) {
            this.tabletId = tabletId;
            this.schemaHash = schemaHash;
        }
    }
}
