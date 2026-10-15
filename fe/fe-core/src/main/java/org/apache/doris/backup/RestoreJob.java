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

import org.apache.doris.backup.BackupJobInfo.BackupIndexInfo;
import org.apache.doris.backup.BackupJobInfo.BackupOlapTableInfo;
import org.apache.doris.backup.BackupJobInfo.BackupPartitionInfo;
import org.apache.doris.backup.BackupJobInfo.BackupTabletInfo;
import org.apache.doris.backup.RestoreFileMapping.IdChain;
import org.apache.doris.backup.RestoreReuseJudge.CheckLevel;
import org.apache.doris.backup.Status.ErrCode;
import org.apache.doris.catalog.BinlogConfig;
import org.apache.doris.catalog.DataProperty;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.EnvFactory;
import org.apache.doris.catalog.FsBroker;
import org.apache.doris.catalog.Index;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.LocalReplica;
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
import org.apache.doris.catalog.PartitionType;
import org.apache.doris.catalog.Replica;
import org.apache.doris.catalog.Replica.ReplicaState;
import org.apache.doris.catalog.ReplicaAllocation;
import org.apache.doris.catalog.Resource;
import org.apache.doris.catalog.ResourceMgr;
import org.apache.doris.catalog.RestoreLineage;
import org.apache.doris.catalog.RestoreSource;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.TableIf.TableType;
import org.apache.doris.catalog.Tablet;
import org.apache.doris.catalog.TabletMeta;
import org.apache.doris.catalog.View;
import org.apache.doris.clone.DynamicPartitionScheduler;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.FeConstants;
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
import org.apache.doris.nereids.trees.plans.commands.BackupCommand;
import org.apache.doris.nereids.trees.plans.commands.RestoreCommand;
import org.apache.doris.persist.ColocatePersistInfo;
import org.apache.doris.persist.gson.GsonPostProcessable;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.persist.gson.GsonUtilsBase;
import org.apache.doris.qe.GlobalVariable;
import org.apache.doris.resource.Tag;
import org.apache.doris.system.Backend;
import org.apache.doris.task.AgentBatchTask;
import org.apache.doris.task.AgentBoundedBatchTask;
import org.apache.doris.task.AgentTask;
import org.apache.doris.task.AgentTaskExecutor;
import org.apache.doris.task.AgentTaskQueue;
import org.apache.doris.task.CreateReplicaTask;
import org.apache.doris.task.DirMoveTask;
import org.apache.doris.task.DownloadTask;
import org.apache.doris.task.ReleaseSnapshotTask;
import org.apache.doris.task.RestoreDigestTask;
import org.apache.doris.task.SnapshotTask;
import org.apache.doris.thrift.TFinishTaskRequest;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TRemoteTabletSnapshot;
import org.apache.doris.thrift.TRestoreBackupDigest;
import org.apache.doris.thrift.TRestoreDigestPrefixSource;
import org.apache.doris.thrift.TRestoreIncrementalRange;
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
import com.google.gson.annotations.SerializedName;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class RestoreJob extends AbstractJob implements GsonPostProcessable {
    private static final String PROP_RESERVE_REPLICA = RestoreCommand.PROP_RESERVE_REPLICA;
    private static final String PROP_RESERVE_COLOCATE = RestoreCommand.PROP_RESERVE_COLOCATE;
    private static final String PROP_RESERVE_DYNAMIC_PARTITION_ENABLE =
            RestoreCommand.PROP_RESERVE_DYNAMIC_PARTITION_ENABLE;
    private static final String PROP_IS_BEING_SYNCED = PropertyAnalyzer.PROPERTIES_IS_BEING_SYNCED;
    private static final String PROP_CLEAN_TABLES = RestoreCommand.PROP_CLEAN_TABLES;
    private static final String PROP_CLEAN_PARTITIONS = RestoreCommand.PROP_CLEAN_PARTITIONS;
    private static final String PROP_ATOMIC_RESTORE = RestoreCommand.PROP_ATOMIC_RESTORE;
    private static final String PROP_REUSE_CHECK_LEVEL = RestoreCommand.PROP_REUSE_CHECK_LEVEL;
    private static final String PROP_FORCE_REPLACE = RestoreCommand.PROP_FORCE_REPLACE;
    private static final String ATOMIC_RESTORE_TABLE_PREFIX = "__doris_atomic_restore_prefix__";

    private static final Logger LOG = LogManager.getLogger(RestoreJob.class);

    // CHECKSTYLE OFF
    public enum RestoreJobState {
        PENDING, // Job is newly created. Check and prepare meta in catalog. Create replica if necessary.
                 // Waiting for replica creation finished synchronously, then sending snapshot tasks.
                 // then transfer to CREATING.
        CREATING, // Creating replica on BE. Transfer to VERIFYING (if some existing partitions are candidates of
                  // partition level reuse) or SNAPSHOTING after all replicas created.
        VERIFYING, // Computing the logical digest of the local replicas of the candidate partitions, to decide which
                   // partitions keep their local data. Nothing local is changed. Not persisted, a job restarted
                   // in this state is redone from PENDING. Transfer to SNAPSHOTING.
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

    // For show restore command, master fe change the job state to FINISHED before writing edit log.
    // We set showState to FINISHED after writing edit log. otherwise, query to non master fe may read
    // data before restore finished.
    protected volatile RestoreJobState showState;

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
    private boolean reserveColocate = false;
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
    @SerializedName("rvi")
    private com.google.common.collect.Table<Long, Long, Long> restoredVersionInfo = HashBasedTable.create();
    // tablet id->(be id -> snapshot info)
    @SerializedName("si")
    protected com.google.common.collect.Table<Long, Long, SnapshotInfo> snapshotInfos = HashBasedTable.create();

    // How many partitions (and bytes) could be kept locally instead of downloading if partition level reuse
    // were enabled, computed before the job modifies anything. Only for display, null if not computed.
    @SerializedName("rss")
    private RestoreReuseShadowStats reuseShadowStats;

    // The result of the manifest check of the downloaded snapshots, fixed when the download finishes or the job is
    // cancelled while downloading. Null if the backup has no manifest, or not fixed yet.
    @SerializedName("mck")
    private RestoreManifestCheck manifestCheck;
    // (tablet id, backend id) of the replicas whose downloaded snapshot is checked against the manifest by the
    // backend, and those whose file digests are checked too. Only while downloading, not persisted: download tasks
    // are sent again after a restart.
    private Set<Pair<Long, Long>> manifestVerifiedReplicas = Sets.newHashSet();

    // How much of the data was reused locally instead of downloaded, summed over the download tasks. Null if the job
    // has not started downloading, or was restarted from an old version.
    @SerializedName("dls")
    private RestoreDownloadStats downloadStats;
    private Set<Pair<Long, Long>> manifestDigestCheckedReplicas = Sets.newHashSet();

    // The outcome of partition level reuse: the partitions that keep their local data, and the reason of each
    // decision. Null if partition level reuse is not enabled. Fixed when VERIFYING ends (it is in the edit log
    // written when entering DOWNLOAD), kept after the job finishes.
    @SerializedName("rru")
    private RestoreReuseResult reuseResult;

    // The ids of the staging partitions of an atomic restore whose data is loaded from a local source (kept or
    // incrementally appended partitions): the tablet scheduler must not repair their replicas by clone while the job
    // runs, a clone would replace the data the job loaded. Immutable, only replaced as a whole, so the tablet
    // scheduler reads it without any lock. Built in finishVerifying (before the snapshot tasks are sent), empty
    // before that and when the job is done, rebuilt from reuseResult when the job is replayed.
    private volatile Set<Long> localSourceStagingPartitions = Collections.emptySet();

    // The prefix of the error message of a backend which says that retrying the task cannot succeed.
    public static final String RESTORE_NOT_RETRYABLE = "[RESTORE_NOT_RETRYABLE]";
    // The working state of partition level reuse while PENDING..VERIFYING, never persisted: the job is redone
    // from PENDING after a restart or a master switch.
    private transient List<ReuseCandidate> reuseCandidates = Lists.newArrayList();
    // signature of a digest task -> the digest reported, or none() if the task failed
    private transient Map<Long, LogicalDigestInfo> digestResults = Maps.newHashMap();
    private transient long verifyRoundStartMs = 0;
    // 1: the sampled partitions (or all, if there is no sampling), 2: the others, after the sample found a mismatch
    private transient int verifyRound = 0;
    private transient Random sampleRandom = new Random();
    // What the backup meta carried before it was cleared: backup table name -> its restore source and the restore
    // lineage of its partitions (by partition name), for the reverse L0 checks.
    private transient Map<String, BackupStamps> backupStamps = Maps.newHashMap();

    private List<ColocatePersistInfo> colocatePersistInfos = Lists.newArrayList();

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
            boolean reserveColocate, boolean reserveDynamicPartitionEnable, boolean isBeingSynced,
            boolean isCleanTables, boolean isCleanPartitions, boolean isAtomicRestore, boolean isForceReplace, Env env,
            long repoId) {
        super(JobType.RESTORE, label, dbId, dbName, timeoutMs, env, repoId);
        this.backupTimestamp = backupTs;
        this.jobInfo = jobInfo;
        this.allowLoad = allowLoad;
        this.replicaAlloc = replicaAlloc;
        setState(RestoreJobState.PENDING);
        this.metaVersion = metaVersion;
        this.reserveReplica = reserveReplica;
        this.reserveColocate = reserveColocate;
        // if is cloud mode or backup snapshot is come from a cluster with force replication allocation,
        // ignore the origin allocation
        if (Config.isCloudMode() || jobInfo.isForceReplicationAllocation) {
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
        properties.put(PROP_RESERVE_COLOCATE, String.valueOf(reserveColocate));
        properties.put(PROP_RESERVE_DYNAMIC_PARTITION_ENABLE, String.valueOf(reserveDynamicPartitionEnable));
        properties.put(PROP_IS_BEING_SYNCED, String.valueOf(isBeingSynced));
        properties.put(PROP_CLEAN_TABLES, String.valueOf(isCleanTables));
        properties.put(PROP_CLEAN_PARTITIONS, String.valueOf(isCleanPartitions));
        properties.put(PROP_ATOMIC_RESTORE, String.valueOf(isAtomicRestore));
        properties.put(PROP_FORCE_REPLACE, String.valueOf(isForceReplace));
    }

    public RestoreJob(String label, String backupTs, long dbId, String dbName, BackupJobInfo jobInfo, boolean allowLoad,
            ReplicaAllocation replicaAlloc, long timeoutMs, int metaVersion, boolean reserveReplica,
            boolean reserveColocate, boolean reserveDynamicPartitionEnable, boolean isBeingSynced,
            boolean isCleanTables, boolean isCleanPartitions, boolean isAtomicRestore, boolean isForeReplace, Env env,
            long repoId,
            BackupMeta backupMeta) {
        this(label, backupTs, dbId, dbName, jobInfo, allowLoad, replicaAlloc, timeoutMs, metaVersion, reserveReplica,
                reserveColocate, reserveDynamicPartitionEnable, isBeingSynced, isCleanTables, isCleanPartitions,
                isAtomicRestore, isForeReplace, env, repoId);

        this.backupMeta = backupMeta;
    }

    /** The check level of partition level reuse, null means the default of the FE config. */
    public void setReuseCheckLevel(String level) {
        if (level != null) {
            properties.put(PROP_REUSE_CHECK_LEVEL, level);
        }
    }

    public boolean isFromLocalSnapshot() {
        return repoId == Repository.KEEP_ON_LOCAL_REPO_ID;
    }

    public RestoreJobState getState() {
        return state;
    }

    private void setState(RestoreJobState state) {
        this.state = state;
        this.showState = state;
    }

    public int getMetaVersion() {
        return metaVersion;
    }

    public boolean isBeingSynced() {
        return isBeingSynced;
    }

    public List<ColocatePersistInfo> getColocatePersistInfos() {
        return colocatePersistInfos;
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
            recordLocalSourceStats(request);
            taskErrMsg.remove(task.getSignature());
            Preconditions.checkState(task.getTabletId() == removedTabletId, removedTabletId);
            if (LOG.isDebugEnabled()) {
                LOG.debug("get finished snapshot info: {}, unfinished tasks num: {}, remove result: {}. {}",
                          info, unfinishedSignatureToId.size(), this, removedTabletId);
            }
            return true;
        }
        return false;
    }


    // The finish report of a RESTORE_DIGEST task: the logical digest of one local replica, see
    // TFinishTaskRequest.logical_digest. A failed task, or a report without a valid digest, is recorded as a replica
    // without digest, and the partition is downloaded. Returns true to remove the task from AgentTaskQueue.
    public synchronized boolean finishRestoreDigestTask(RestoreDigestTask task, TFinishTaskRequest request) {
        if (state != RestoreJobState.VERIFYING || task.getJobId() != jobId) {
            // the job moved on (e.g. the task timed out), or a task of another job
            return true;
        }
        Long tabletId = unfinishedSignatureToId.remove(task.getSignature());
        if (tabletId == null || tabletId != task.getTabletId()) {
            LOG.warn("unknown restore digest task: {}. {}", task, this);
            return true;
        }
        LogicalDigestInfo digest;
        if (request.getTaskStatus().getStatusCode() != TStatusCode.OK) {
            LOG.warn("restore digest task failed: {}, status: {}. {}", task, request.getTaskStatus(), this);
            digest = LogicalDigestInfo.none(LogicalDigestInfo.REASON_ERROR);
        } else {
            digest = LogicalDigestInfo.fromThrift(request.isSetLogicalDigest() ? request.getLogicalDigest() : null);
        }
        digestResults.put(task.getSignature(), digest);
        return true;
    }

    public synchronized boolean finishTabletDownloadTask(DownloadTask task, TFinishTaskRequest request) {
        if (request.getTaskStatus().getStatusCode() == TStatusCode.RESTORE_MANIFEST_MISMATCH
                && task.getJobId() == jobId) {
            // The downloaded snapshot does not match the manifest of the backup even after the backend downloaded
            // it again: the backup is broken, waiting until the timeout does not help. Cancel the job at once.
            handleManifestMismatch(task, request);
            return false;
        }
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
        recordManifestCheck(task.getBackendId(), request);
        recordDownloadStats(task.getBackendId(), request);
        return true;
    }

    // Add the stats of a snapshot made from the local tablet of the table being replaced (atomic restore). It never
    // fails the job.
    @VisibleForTesting
    void recordLocalSourceStats(TFinishTaskRequest request) {
        try {
            if (!request.isSetDownloadStats() || !request.getDownloadStats().isSetLocalSourceTablets()) {
                return;
            }
            if (downloadStats == null) {
                downloadStats = new RestoreDownloadStats();
            }
            downloadStats.addLocalSource(request.getDownloadStats());
        } catch (Exception e) {
            LOG.warn("failed to record the local source stats, ignore it. {}", this, e);
        }
    }

    // Add the download stats reported by the backend. An old backend does not report it, its replicas are counted as
    // not reported. It never fails the job.
    @VisibleForTesting
    void recordDownloadStats(long beId, TFinishTaskRequest request) {
        try {
            if (!request.isSetDownloadStats()) {
                return;
            }
            if (downloadStats == null) {
                downloadStats = new RestoreDownloadStats();
            }
            downloadStats.add(request.getDownloadStats(), request.getDownloadedTabletIds().size());
        } catch (Exception e) {
            LOG.warn("failed to record the download stats of backend {}, ignore it. {}", beId, this, e);
        }
    }

    public RestoreDownloadStats getDownloadStats() {
        return downloadStats;
    }

    private void resetDownloadStats() {
        RestoreDownloadStats before = downloadStats;
        downloadStats = new RestoreDownloadStats();
        if (before != null) {
            // what the snapshot tasks made from the local tablets, which is before the download
            downloadStats.carryLocalSource(before);
        }
    }

    // Fix the replicas to download in the stats, when the download finishes or the job is cancelled.
    @VisibleForTesting
    void fixDownloadStats() {
        if (downloadStats != null && !downloadStats.isFixed()) {
            downloadStats.fix(countSnapshotsToDownload());
            LOG.info("download stats: {}. {}", downloadStats, this);
        }
    }

    private void handleManifestMismatch(DownloadTask task, TFinishTaskRequest request) {
        String errMsg = request.getTaskStatus().isSetErrorMsgs()
                ? Joiner.on(",").join(request.getTaskStatus().getErrorMsgs()) : "";
        taskErrMsg.put(task.getSignature(), errMsg);
        if (state != RestoreJobState.DOWNLOADING) {
            LOG.warn("ignore manifest mismatch of download task {} in state {}: {}. {}",
                    task.getSignature(), state, errMsg, this);
            return;
        }
        LOG.warn("cancel restore job, the downloaded snapshot does not match the manifest, backend {}: {}. {}",
                task.getBackendId(), errMsg, this);
        status = new Status(ErrCode.COMMON_ERROR, "restore manifest check failed on backend "
                + task.getBackendId() + ": " + errMsg);
        cancelInternal(false);
    }

    // Record the tablets checked against the manifest by the backend. An old backend does not report it, its
    // tablets are counted as unverified.
    @VisibleForTesting
    void recordManifestCheck(long beId, TFinishTaskRequest request) {
        if (!request.isSetManifestVerifiedTablets()) {
            return;
        }
        boolean digestChecked = request.isSetManifestDigestChecked() && request.isManifestDigestChecked();
        for (Long tabletId : request.getManifestVerifiedTablets()) {
            Pair<Long, Long> replica = Pair.of(tabletId, beId);
            manifestVerifiedReplicas.add(replica);
            if (digestChecked) {
                manifestDigestCheckedReplicas.add(replica);
            }
        }
    }

    // The manifest check result so far, null if the backup has no manifest.
    @VisibleForTesting
    RestoreManifestCheck computeManifestCheck() {
        if (jobInfo == null || !jobInfo.hasManifest()) {
            return null;
        }
        long total = Math.max(countSnapshotsToDownload(), manifestVerifiedReplicas.size());
        long verified = manifestVerifiedReplicas.size();
        boolean digestChecked = verified > 0 && manifestDigestCheckedReplicas.size() == verified;
        return new RestoreManifestCheck(jobInfo.manifestVersion, jobInfo.digestAlgorithm, verified,
                total - verified, digestChecked);
    }

    public RestoreManifestCheck getManifestCheck() {
        return manifestCheck != null ? manifestCheck : computeManifestCheck();
    }

    // The manifest root of a tablet in the backup for the backend to download by the manifest and check the
    // downloaded snapshot, null if the backup has no manifest.
    @VisibleForTesting
    String getExpectedManifestRoot(long repoTabletId) {
        if (Config.isCloudMode() || jobInfo == null || !jobInfo.hasManifest()) {
            return null;
        }
        return jobInfo.getManifestRoot(repoTabletId);
    }

    private void resetManifestCheck() {
        manifestCheck = null;
        manifestVerifiedReplicas.clear();
        manifestDigestCheckedReplicas.clear();
    }

    public synchronized boolean finishDirMoveTask(DirMoveTask task, TFinishTaskRequest request) {
        if (task.getIncrementalRange() != null && task.isLocalSourceLoaded() && task.getJobId() == jobId
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
        return true;
    }

    /**
     * The failure of a task of the local source (the snapshot task which loads the local source into a staging
     * tablet, the move task which appends the increment): other tasks are resent without end, but these two cannot
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
                        repo.getFileSystemDescriptor().getBackendConfigProperties());
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

        checkIfNeedCancel();

        if (status.ok()) {
            if (state != RestoreJobState.PENDING && state != RestoreJobState.CREATING
                    && label.equals(DebugPointUtil.getDebugParamOrDefault("FE.PAUSE_NON_PENDING_RESTORE_JOB", ""))) {
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
                case VERIFYING:
                    waitingAllDigestsFinished();
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

        if (!status.ok()) {
            cancelInternal(false);
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
        computeReuseShadowStats(db);
        // The backup meta carries the restore lineage of the source partitions, which must not be inherited.
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
                OlapTable remoteOlapTbl = (OlapTable) remoteTbl;
                if (remoteOlapTbl.needRowBinlog()) {
                    status = new Status(ErrCode.COMMON_ERROR,
                            "Do not support restore table with binlog<Row> enabled: " + remoteOlapTbl.getName());
                    return;
                }
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
                    if (localOlapTbl.needRowBinlog()) {
                        status = new Status(ErrCode.COMMON_ERROR,
                                "Do not support restore into local table with binlog<Row> enabled: "
                                        + localOlapTbl.getName());
                        return;
                    }
                    if (localOlapTbl.isColocateTable() || (reserveColocate && remoteOlapTbl.isColocateTable())) {
                        status = new Status(ErrCode.COMMON_ERROR, "Not support to restore to local table "
                                + tableName + " with colocate group.");
                        return;
                    }

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

                        st = remoteOlapTbl.checkPropertiesForRestore();
                        if (!st.ok()) {
                            status = st;
                            return;
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
                    // Retain only expected restore partitions in this table;
                    Set<String> allPartNames = remoteOlapTbl.getPartitionNames();
                    for (String partName : allPartNames) {
                        if (!tblInfo.containsPart(partName)) {
                            remoteOlapTbl.dropPartitionAndReserveTablet(partName);
                        }
                    }

                    Status st = remoteOlapTbl.checkPropertiesForRestore();
                    if (!st.ok()) {
                        status = st;
                        return;
                    }

                    // reset all ids in this table
                    String srcDbName = jobInfo.dbName;
                    st = remoteOlapTbl.resetIdsForRestore(env, db, replicaAlloc, reserveReplica,
                            reserveColocate, colocatePersistInfos, srcDbName);
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
                    // ODBC tables are deprecated. We can no longer reset IDs for restore.
                    // Just add the table as-is for metadata compatibility.
                    LOG.warn("Skipping resetIdsForRestore for deprecated ODBC table: {}", backupOdbcTableName);
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
                if (GlobalVariable.isStoredTableNamesLowerCase()) {
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

        // The data of the existing partitions to overwrite is about to change. Their lineage must not outlive
        // the data it describes, e.g. if the job is cancelled after some tablets are moved in COMMITTING.
        // This is persisted with the DOWNLOAD edit log (replayed by replayCheckAndPrepareMeta), which is
        // written before any tablet data is moved, and the lineage is written again in allTabletCommitted.
        // The candidates of partition level reuse keep their lineage until they are decided in VERIFYING, nothing
        // local is changed before that.
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
        setState(RestoreJobState.CREATING);
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
        setState(RestoreJobState.CREATING);
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
                    boolean isInMemory = remotePartitionInfo.getIsInMemory(remotePartId);
                    if (Config.isCloudMode()) {
                        // In cloud mode, storage_medium, cooldown_time, storage_policy and in_memory
                        // from the source cluster are not applicable. Reset them to defaults.
                        remoteDataProperty = new DataProperty(
                                DataProperty.DEFAULT_STORAGE_MEDIUM,
                                DataProperty.MAX_COOLDOWN_TIME_MS,
                                "",
                                remoteDataProperty.isMutable());
                        isInMemory = false;
                    }
                    localPartitionInfo.addPartition(restoredPart.getId(), false, remoteItem,
                            remoteDataProperty, restoreReplicaAlloc,
                            isInMemory,
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

        if (jobInfo.content == null || jobInfo.content == BackupCommand.BackupContent.ALL) {
            if (!reuseCandidates.isEmpty()) {
                // The snapshots of the partitions that keep their data are not needed, decide them first.
                metaPreparedTime = System.currentTimeMillis();
                startVerifying(db);
                return;
            }
            prepareAndSendSnapshotTaskForOlapTable(db);
        }

        metaPreparedTime = System.currentTimeMillis();
        setState(RestoreJobState.SNAPSHOTING);
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
                    }
                    if (isIncrementalPartition(tbl.getId(), part.getId())) {
                        // an empty dir, only the increment is downloaded into it
                        task.setRestoreIncremental(true);
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
            List<Integer> clusterKeyUids = null;
            if (indexMeta.getIndexId() == localTbl.getBaseIndexId() || localTbl.isShadowIndex(indexMeta.getIndexId())) {
                clusterKeyUids = OlapTable.getClusterKeyUids(indexMeta.getSchema());
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
                        restoredIdx.getId(), indexMeta.getSchemaHash(), storageMedium,
                        indexMeta.isRowBinlogIndex());
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
                            localTbl.storageDictPageSize(),
                            localTbl.getColumnSeqMapping(),
                            localTbl.getVerticalCompactionNumColumnsPerGroup());
                    task.setInvertedIndexFileStorageFormat(localTbl.getInvertedIndexFileStorageFormat());
                    task.setInRestoreMode(true);
                    if (baseTabletRef != null) {
                        // ensure this replica is bound to the same backend disk as the origin table's replica.
                        task.setBaseTablet(baseTabletRef.tabletId, baseTabletRef.schemaHash);
                        LOG.info("set base tablet {} for replica {} in restore job {}, tablet id={}",
                                baseTabletRef.tabletId, restoreReplica.getId(), jobId, restoreTablet.getId());
                    }
                    if (!CollectionUtils.isEmpty(clusterKeyUids)) {
                        task.setClusterKeyUids(clusterKeyUids);
                        LOG.info("table: {}, partition: {}, index: {}, tablet: {}, cluster key uids: {}",
                                localTbl.getId(), restorePart.getId(), restoredIdx.getId(), restoreTablet.getId(),
                                clusterKeyUids);
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
            // Collect locally and bulk-publish to keep copy-on-write O(n) for the whole index.
            List<Tablet> newTablets = new ArrayList<>(remotetabletSize);
            for (int i = 0; i < remotetabletSize; i++) {
                // generate new tablet id
                long newTabletId = env.getNextId();
                Tablet newTablet = EnvFactory.getInstance().createTablet(newTabletId);
                newTablets.add(newTablet);
                // replicas
                try {
                    Pair<Map<Tag, List<Long>>, TStorageMedium> beIdsAndMedium = Env.getCurrentSystemInfo()
                            .selectBackendIdsForReplicaCreation(replicaAlloc, nextIndexes, null, false, false);
                    Map<Tag, List<Long>> beIds = beIdsAndMedium.first;
                    for (Map.Entry<Tag, List<Long>> entry : beIds.entrySet()) {
                        for (Long beId : entry.getValue()) {
                            long newReplicaId = env.getNextId();
                            Replica newReplica = new LocalReplica(newReplicaId, beId, ReplicaState.NORMAL,
                                    visibleVersion, schemaHash);
                            newTablet.addReplica(newReplica, true /* is restore */);
                        }
                    }
                } catch (DdlException e) {
                    status = new Status(ErrCode.COMMON_ERROR, e.getMessage());
                    return null;
                }
            }
            // add tablets to index in one batch; TabletInvertedIndex registration
            // is intentionally skipped on the restore path (rebuilt separately).
            remoteIdx.appendTablets(newTablets);
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
            boolean isInMemory = remotePartitionInfo.getIsInMemory(remotePartId);
            if (Config.isCloudMode()) {
                // In cloud mode, storage_medium, cooldown_time, storage_policy and in_memory
                // from the source cluster are not applicable. Reset them to defaults.
                remoteDataProperty = new DataProperty(
                        DataProperty.DEFAULT_STORAGE_MEDIUM,
                        DataProperty.MAX_COOLDOWN_TIME_MS,
                        "",
                        remoteDataProperty.isMutable());
                isInMemory = false;
            }
            localPartitionInfo.addPartition(restorePart.getId(), false, remotePartitionInfo.getItem(remotePartId),
                    remoteDataProperty, restoreReplicaAlloc,
                    isInMemory,
                    remotePartitionInfo.getIsMutable(remotePartId));
            localTbl.addPartition(restorePart);

            // modify tablet inverted index
            for (MaterializedIndex restoreIdx : restorePart.getMaterializedIndices(IndexExtState.VISIBLE)) {
                int schemaHash = localTbl.getSchemaHashByIndexId(restoreIdx.getId());
                for (Tablet restoreTablet : restoreIdx.getTablets()) {
                    TabletMeta tabletMeta = new TabletMeta(db.getId(), localTbl.getId(), restorePart.getId(),
                            restoreIdx.getId(), schemaHash, TStorageMedium.HDD, restoreIdx.isRowBinlog());
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
                                    restoreIdx.getId(), schemaHash, TStorageMedium.HDD, restoreIdx.isRowBinlog());
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
            setState(RestoreJobState.DOWNLOAD);

            env.getEditLog().logRestoreJob(this);
            for (ColocatePersistInfo info : colocatePersistInfos) {
                env.getEditLog().logColocateAddTable(info);
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
        resetManifestCheck();
        resetDownloadStats();
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
                        Map<String, String> manifestRoots = Maps.newHashMap();
                        Map<String, TRestoreIncrementalRange> incrementalRanges = Maps.newHashMap();
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
                                String manifestRoot = getExpectedManifestRoot(result.second.getTabletId());
                                if (manifestRoot != null) {
                                    manifestRoots.put(src, manifestRoot);
                                }
                                RestoreReuseResult.Decision incremental = getIncrementalDecision(info.getTblId(),
                                        info.getPartitionId());
                                if (incremental != null) {
                                    if (manifestRoot == null) {
                                        status = new Status(ErrCode.COMMON_ERROR, "the manifest of tablet "
                                                + result.second.getTabletId() + " is required by the incremental "
                                                + "restore");
                                        return;
                                    }
                                    incrementalRanges.put(src, new TRestoreIncrementalRange(incremental.version,
                                            incremental.targetVersion));
                                }
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
                        if (!manifestRoots.isEmpty()) {
                            task.setManifestRoots(manifestRoots);
                        }
                        if (!incrementalRanges.isEmpty()) {
                            task.setIncrementalRanges(incrementalRanges);
                        }
                        batchTask.addTask(task);
                        unfinishedSignatureToId.put(signature, beId);
                    }
                }
            } finally {
                db.readUnlock();
            }
        }

        AgentTaskExecutor.submit(batchTask);

        setState(RestoreJobState.DOWNLOADING);

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
        resetManifestCheck();
        resetDownloadStats();
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
                                String manifestRoot = getExpectedManifestRoot(remoteTabletId);
                                if (manifestRoot != null) {
                                    remoteTabletSnapshot.setManifestRoot(manifestRoot);
                                }
                                RestoreReuseResult.Decision incremental = getIncrementalDecision(info.getTblId(),
                                        info.getPartitionId());
                                if (incremental != null) {
                                    if (manifestRoot == null) {
                                        status = new Status(ErrCode.COMMON_ERROR, "the manifest of tablet "
                                                + remoteTabletId + " is required by the incremental restore");
                                        return;
                                    }
                                    remoteTabletSnapshot.setIncremental(new TRestoreIncrementalRange(
                                            incremental.version, incremental.targetVersion));
                                }

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

        setState(RestoreJobState.DOWNLOADING);

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

    /** The snapshots the download works on: those of the partitions that keep their data are not downloaded. */
    private long countSnapshotsToDownload() {
        long count = 0;
        for (SnapshotInfo info : snapshotInfos.values()) {
            if (!isKeptPartition(info.getTblId(), info.getPartitionId())) {
                count++;
            }
        }
        return count;
    }

    protected DownloadTask createDownloadTask(long beId, long signature, long jobId, long dbId,
                                              Map<String, String> srcToDest, FsBroker brokerAddr) {
        return new DownloadTask(null, beId, signature, jobId, dbId, srcToDest,
            brokerAddr, repo.getFileSystemDescriptor().getBackendConfigProperties(),
            repo.getFileSystemDescriptor().getThriftStorageType(), repo.getLocation(), "");
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
            setState(RestoreJobState.COMMIT);

            // backupMeta is useless now
            backupMeta = null;
            manifestCheck = computeManifestCheck();
            fixDownloadStats();

            env.getEditLog().logRestoreJob(this);
            LOG.info("finished to download, manifest check: {}. {}", manifestCheck, this);
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
        // the tablet in the backup of each tablet of the restore, for the digest sent with an incremental append
        Map<Long, Long> backupTabletIds = null;
        // tablet id->(be id -> download info)
        for (Cell<Long, Long, SnapshotInfo> cell : snapshotInfos.cellSet()) {
            SnapshotInfo info = cell.getValue();
            long signature = env.getNextId();
            DirMoveTask task = new DirMoveTask(null, cell.getColumnKey(), signature, jobId, dbId, info.getTblId(),
                    info.getPartitionId(), info.getTabletId(), cell.getRowKey(), info.getTabletPath(),
                    info.getSchemaHash(), true /* need reload tablet header */);
            RestoreReuseResult.Decision incremental = getIncrementalDecision(info.getTblId(), info.getPartitionId());
            if (incremental != null) {
                task.setIncrementalRange(incremental.version, incremental.targetVersion);
                // only the atomic restore loads the tablet from a local source (with the evidence)
                task.setLocalSourceLoaded(incremental.atomic);
                if (backupTabletIds == null) {
                    backupTabletIds = buildBackupTabletIds();
                }
                task.setBackupLogicalDigest(getBackupLogicalDigest(backupTabletIds, info.getTabletId()));
            }
            batchTask.addTask(task);
            unfinishedSignatureToId.put(signature, info.getTabletId());
        }

        AgentTaskExecutor.submit(batchTask);

        setState(RestoreJobState.COMMITTING);

        // No log here
        LOG.info("finished to send move dir tasks. num: {}. {}", batchTask.getTaskNum(), this);
        return;
    }

    // The tablet of the backup for each tablet of the restore, by the file mapping.
    private Map<Long, Long> buildBackupTabletIds() {
        Map<Long, Long> ids = Maps.newHashMap();
        for (Map.Entry<IdChain, IdChain> entry : fileMapping.getMapping().entrySet()) {
            ids.put(entry.getKey().getTabletId(), entry.getValue().getTabletId());
        }
        return ids;
    }

    /**
     * The logical digest of the backup of a tablet to send with its incremental append, null if the backup has none.
     * With it the backend can tell that a tablet at the end version without the evidence of the append equals the
     * backup, otherwise it fails the task.
     */
    @VisibleForTesting
    TRestoreBackupDigest getBackupLogicalDigest(Map<Long, Long> backupTabletIds, long tabletId) {
        Long backupTabletId = backupTabletIds.get(tabletId);
        LogicalDigestInfo digest = backupTabletId == null || jobInfo == null ? null
                : jobInfo.getLogicalDigest(backupTabletId);
        if (digest == null || !digest.hasDigest() || digest.algoVersion == null || digest.schemaSig == null) {
            return null;
        }
        TRestoreBackupDigest thrift = new TRestoreBackupDigest();
        thrift.setAlgoVersion(digest.algoVersion);
        thrift.setSchemaSig(digest.schemaSig);
        thrift.setRoot(digest.root);
        thrift.setStatusCode("OK");
        return thrift;
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

        // Use the same time on replay, it is persisted as the finished time of the job.
        long commitTime = isReplay ? finishedTime : System.currentTimeMillis();
        stampRestoreLineage(db, commitTime);

        // Drop the exists but non-restored table/partitions.
        if (isCleanTables || isCleanPartitions) {
            Status st = dropAllNonRestoredTableAndPartitions(db);
            if (!st.ok()) {
                return st;
            }
        }

        updateOlapTablesVersion(db, isReplay);

        if (!isReplay) {
            restoredPartitions.clear();
            restoredTbls.clear();
            restoredResources.clear();

            com.google.common.collect.Table<Long, Long, SnapshotInfo> savedSnapshotInfos = snapshotInfos;
            snapshotInfos = HashBasedTable.create();

            snapshotInfos.clear();
            fileMapping.clear();
            jobInfo.releaseSnapshotInfo();

            finishedTime = commitTime;
            state = RestoreJobState.FINISHED;
            clearLocalSourceStagingPartitions();
            env.getEditLog().logRestoreJob(this);

            // Only send release snapshot tasks after the job is finished.
            releaseSnapshots(savedSnapshotInfos, true);
        }
        showState = RestoreJobState.FINISHED;
        if (isReplay) {
            clearLocalSourceStagingPartitions();
        }

        LOG.info("job is finished. is replay: {}. {}", isReplay, this);
        return Status.OK;
    }

    protected void updateOlapTablesVersion(Database db, boolean isReplay) {
        if (Env.getCurrentEnv().invalidCacheForCloud()) {
            return;
        }

        for (String tableName : jobInfo.backupOlapTableObjects.keySet()) {
            Table tbl = db.getTableNullable(jobInfo.getAliasByOriginNameIfSet(tableName));
            if (tbl == null) {
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
                long nextVersion = olapTbl.getNextVersion();
                olapTbl.updateVisibleVersionAndTime(nextVersion, System.currentTimeMillis());
            } finally {
                olapTbl.writeUnlock();
            }
        }
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
        info.add(showState.name());
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
            info.add(reuseShadowStats == null ? FeConstants.null_string : reuseShadowStats.toJson(reuseResult));
            RestoreManifestCheck check = getManifestCheck();
            info.add(check == null ? FeConstants.null_string : check.toString());
            long keptBytes = reuseResult == null ? 0 : reuseResult.getKeptBytesAllReplicas();
            long keptAtomicBytes = reuseResult == null ? 0 : reuseResult.getKeptAtomicBytesAllReplicas();
            long incrementalLocalBytes = reuseResult == null ? 0 : reuseResult.getIncrementalLocalBytesAllReplicas();
            if (downloadStats != null) {
                info.add(downloadStats.toJson(countSnapshotsToDownload(), keptBytes, keptAtomicBytes,
                        incrementalLocalBytes, reuseLevelSummary()));
            } else if (keptBytes > 0 || incrementalLocalBytes > 0) {
                // everything was kept, nothing was downloaded
                info.add(new RestoreDownloadStats().toJson(countSnapshotsToDownload(), keptBytes, keptAtomicBytes,
                        incrementalLocalBytes, reuseLevelSummary()));
            } else {
                info.add(FeConstants.null_string);
            }
        }
        return info;
    }

    // The requested and the effective check levels, and the kept partitions not proved, null if reuse did not decide.
    private Map<String, Object> reuseLevelSummary() {
        return reuseResult == null || reuseResult.isEmpty() ? null : reuseResult.levelSummary();
    }

    public RestoreReuseShadowStats getReuseShadowStats() {
        return reuseShadowStats;
    }

    static long getSrcCommitSeq(BackupJobInfo jobInfo, long srcTableId) {
        if (jobInfo.tableCommitSeqMap == null) {
            return RestoreLineage.UNKNOWN_COMMIT_SEQ;
        }
        Long commitSeq = jobInfo.tableCommitSeqMap.get(srcTableId);
        return commitSeq == null ? RestoreLineage.UNKNOWN_COMMIT_SEQ : commitSeq;
    }

    static RestoreLineage buildRestoreLineage(BackupJobInfo jobInfo, BackupOlapTableInfo tblInfo,
            BackupPartitionInfo partInfo, long restoreTime) {
        return new RestoreLineage(jobInfo.dbId, tblInfo.id, partInfo.id, partInfo.version,
                getSrcCommitSeq(jobInfo, tblInfo.id), jobInfo.backupTime, restoreTime,
                jobInfo.getSrcClusterIdOrUnknown());
    }

    public RestoreReuseResult getReuseResult() {
        return reuseResult;
    }

    private void clearReuseWorkingState() {
        reuseCandidates.clear();
        digestResults.clear();
        verifyRound = 0;
    }

    /**
     * Select the existing partitions that may keep their local data, by the chain of conditions in
     * {@link RestoreReuseJudge}. Only the partitions of the A1 branch (existing partition, see restoredVersionInfo)
     * are looked at, so it must be called after the meta is prepared and before the lineage of the overwritten
     * partitions is invalidated. The candidates are kept in reuseCandidates, those that need no digest (check level
     * off) are decided here. Never fails the job: any problem selects nothing.
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
                || !(jobInfo.content == null || jobInfo.content == BackupCommand.BackupContent.ALL)) {
            return keys;
        }
        CheckLevel jobLevel = CheckLevel.parse(properties.get(PROP_REUSE_CHECK_LEVEL),
                Config.restore_reuse_default_check_level);
        if (jobLevel == CheckLevel.DISABLE) {
            LOG.info("partition level reuse is disabled by the property of the job {}", jobId);
            return keys;
        }
        CheckLevel effectiveJobLevel = RestoreReuseJudge.levelOfTable(jobLevel, KeysType.DUP_KEYS);
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
                CheckLevel level = RestoreReuseJudge.levelOfTable(jobLevel, localOlapTbl.getKeysType());
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
                        RestoreReuseJudge.Input in = buildReuseInput(tblEntry.getKey(), tblInfo,
                                partEntry.getKey(), partEntry.getValue(), localOlapTbl, localPart, level);
                        in.atomicReject = atomicReject;
                        String reject = RestoreReuseJudge.firstReject(in);
                        boolean incremental = false;
                        if (reject != null) {
                            // a partition that is behind the backup may download only the rowsets after its version
                            String incrementalReject = in.incrementalEnabled
                                    ? RestoreReuseJudge.firstRejectIncremental(in) : reject;
                            if (incrementalReject != null) {
                                result.addRejected(reject);
                                if (!reject.startsWith("L0_") || (in.incrementalEnabled
                                        && !incrementalReject.startsWith("L0_")
                                        && !incrementalReject.startsWith("INCREMENTAL_SAME_VERSION"))) {
                                    LOG.info("restore reuse: partition {}.{} is not a candidate: {}, incremental: {}, "
                                            + "job: {}", localOlapTbl.getName(), partEntry.getKey(), reject,
                                            incrementalReject, jobId);
                                }
                                continue;
                            }
                            incremental = true;
                        }
                        ReuseCandidate candidate = new ReuseCandidate();
                        candidate.tableId = localOlapTbl.getId();
                        candidate.partitionId = localPart.getId();
                        candidate.tableName = localOlapTbl.getName();
                        candidate.partitionName = partEntry.getKey();
                        candidate.incrementalCandidate = incremental;
                        candidate.backupTableId = tblInfo.id;
                        if (isAtomicRestore) {
                            candidate.atomic = true;
                            candidate.stagingTableId = stagingTbl.getId();
                            candidate.stagingPartitionId = stagingPart.getId();
                        }
                        candidate.version = in.backupPartition.version;
                        // the version the digests are computed at: the local one for the incremental append
                        candidate.digestVersion = incremental ? localPart.getVisibleVersion()
                                : in.backupPartition.version;
                        candidate.bytes = in.getSingleReplicaBytes();
                        candidate.bytesAllReplicas = RestoreReuseShadowStats.getAllReplicasLocalDataSize(localPart);
                        candidate.l0Path = in.l0Path;
                        candidate.requestedLevel = jobLevel;
                        candidate.level = level;
                        candidate.clusterProven = in.clusterProven;
                        candidate.backupPartition = in.backupPartition;
                        if (level == CheckLevel.OFF && !incremental && !candidate.clusterProven) {
                            // (only with restore_reuse_allow_unverified_keep) the lineage stamp is the only proof of
                            // a keep at the level off, and it is no proof if the cluster id of the source was not
                            // compared on both sides (an old stamp or backup): verify it by the digest.
                            candidate.level = CheckLevel.FULL;
                        }
                        if (candidate.level == CheckLevel.OFF && !incremental) {
                            candidate.decide(true, RestoreReuseResult.KEPT_L0_ONLY);
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
            LOG.info("restore reuse: {} candidate partitions, check level {} (requested {}, allow unverified keep "
                    + "{}), not candidates: {}, job: {}", candidates.size(), effectiveJobLevel.lower(),
                    jobLevel.lower(), Config.restore_reuse_allow_unverified_keep, result.getRejected(), jobId);
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
     * Start deciding the candidates: send the digest tasks of the sampled partitions (all partitions of the check
     * level full), and enter VERIFYING. Without any digest to compute, decide at once.
     */
    private void startVerifying(Database db) {
        digestResults.clear();
        unfinishedSignatureToId.clear();
        taskProgress.clear();
        taskErrMsg.clear();
        List<ReuseCandidate> toDigest = Lists.newArrayList();
        List<ReuseCandidate> samplePool = Lists.newArrayList();
        for (ReuseCandidate candidate : reuseCandidates) {
            if (candidate.decided) {
                continue;
            }
            if (candidate.incrementalCandidate) {
                // the digest is the only proof of the local data, always computed
                toDigest.add(candidate);
            } else if (candidate.level == CheckLevel.FULL || RestoreReuseJudge.needsFullDigest(candidate.l0Path,
                    Config.restore_reuse_force_full_for_relation)) {
                toDigest.add(candidate);
            } else {
                samplePool.add(candidate);
            }
        }
        for (ReuseCandidate candidate : RestoreReuseJudge.pickSample(samplePool,
                Config.restore_reuse_sample_ratio, sampleRandom)) {
            candidate.sampled = true;
            toDigest.add(candidate);
        }
        verifyRound = 1;
        LOG.info("restore reuse: verify {} of {} candidate partitions ({} sampled). {}", toDigest.size(),
                reuseCandidates.size(), (int) toDigest.stream().filter(c -> c.sampled).count(), this);
        if (toDigest.isEmpty()) {
            finishVerifying(db);
            return;
        }
        sendDigestRound(db, toDigest);
    }

    private void sendDigestRound(Database db, List<ReuseCandidate> candidates) {
        AgentBoundedBatchTask batchTask = new AgentBoundedBatchTask(
                Config.backup_restore_batch_task_num_per_rpc, Config.restore_task_concurrency_per_be);
        for (ReuseCandidate candidate : candidates) {
            try {
                prepareDigestTasks(db, candidate, batchTask);
            } catch (Exception e) {
                LOG.warn("failed to prepare the digest tasks of partition {}.{}, download it. {}",
                        candidate.tableName, candidate.partitionName, this, e);
                candidate.decide(false, RestoreReuseResult.DOWNLOAD_VERIFY_ERROR);
            }
        }
        AgentTaskExecutor.submit(batchTask);
        verifyRoundStartMs = System.currentTimeMillis();
        setState(RestoreJobState.VERIFYING);
        // No log here, a restart redoes the job from PENDING
        LOG.info("restore reuse: sent {} digest tasks of round {}. {}", batchTask.getTaskNum(), verifyRound, this);
    }

    // One digest task for each local replica of the partition, at the visible version, which is the version of the
    // backup. All or none of the tasks of the partition are added.
    private void prepareDigestTasks(Database db, ReuseCandidate candidate, AgentBoundedBatchTask batchTask) {
        OlapTable tbl = (OlapTable) db.getTableNullable(candidate.tableId);
        if (tbl == null) {
            candidate.decide(false, RestoreReuseResult.DOWNLOAD_CHANGED);
            return;
        }
        List<ReuseTabletCheck> checks = Lists.newArrayList();
        List<RestoreDigestTask> tasks = Lists.newArrayList();
        Map<Long, LogicalDigestInfo> immediateResults = Maps.newHashMap();
        tbl.readLock();
        try {
            Partition part = tbl.getPartition(candidate.partitionId);
            if (part == null || part.getVisibleVersion() != candidate.digestVersion) {
                candidate.decide(false, RestoreReuseResult.DOWNLOAD_CHANGED);
                return;
            }
            for (MaterializedIndex index : part.getMaterializedIndices(IndexExtState.VISIBLE)) {
                BackupIndexInfo backupIdx = candidate.backupPartition.getIdx(tbl.getIndexNameById(index.getId()));
                if (backupIdx == null || backupIdx.sortedTabletInfoList.size() != index.getTablets().size()) {
                    candidate.decide(false, RestoreReuseResult.DOWNLOAD_VERIFY_ERROR);
                    return;
                }
                int schemaHash = tbl.getSchemaHashByIndexId(index.getId());
                for (int i = 0; i < index.getTablets().size(); i++) {
                    Tablet tablet = index.getTablets().get(i);
                    BackupTabletInfo backupTablet = backupIdx.sortedTabletInfoList.get(i);
                    ReuseTabletCheck check = new ReuseTabletCheck(jobInfo.getLogicalDigest(backupTablet.id));
                    for (Replica replica : tablet.getReplicas()) {
                        if (!RestoreReuseJudge.isReplicaHealthyAt(replica, candidate.digestVersion)) {
                            candidate.decide(false, RestoreReuseResult.DOWNLOAD_CHANGED);
                            return;
                        }
                        long signature = env.getNextId();
                        long beId = replica.getBackendIdWithoutException();
                        check.signatures.add(signature);
                        Backend backend = Env.getCurrentSystemInfo().getBackend(beId);
                        if (backend == null || !backend.isAlive()) {
                            immediateResults.put(signature, LogicalDigestInfo.none(LogicalDigestInfo.REASON_ERROR));
                            continue;
                        }
                        RestoreDigestTask task = new RestoreDigestTask(beId, signature, jobId, dbId, tbl.getId(),
                                part.getId(), index.getId(), tablet.getId(), schemaHash, candidate.digestVersion, 0);
                        if (candidate.incrementalCandidate) {
                            TRestoreDigestPrefixSource source = buildPrefixSource(candidate, backupIdx, backupTablet,
                                    beId);
                            if (source == null) {
                                immediateResults.put(signature, LogicalDigestInfo.none(LogicalDigestInfo.REASON_ERROR));
                                continue;
                            }
                            task.setPrefixSource(source);
                        }
                        tasks.add(task);
                    }
                    checks.add(check);
                }
            }
        } finally {
            tbl.readUnlock();
        }
        candidate.tablets = checks;
        digestResults.putAll(immediateResults);
        for (RestoreDigestTask task : tasks) {
            batchTask.addTask(task);
            unfinishedSignatureToId.put(task.getSignature(), task.getTabletId());
        }
    }

    /**
     * Where a backend reads the decomposed digest of a tablet of the backup: the repository path, or the remote tablet
     * snapshot kept on a backend. Null if it can not be told, the digest of that replica is then an error and the
     * partition is downloaded.
     */
    private TRestoreDigestPrefixSource buildPrefixSource(ReuseCandidate candidate, BackupIndexInfo backupIdx,
            BackupTabletInfo backupTablet, long beId) {
        try {
            String root = backupTablet.prefixDigestRoot;
            if (root == null || root.isEmpty()) {
                return null;
            }
            TRestoreDigestPrefixSource source = new TRestoreDigestPrefixSource(backupTablet.id, root,
                    candidate.version);
            if (isFromLocalSnapshot()) {
                TRemoteTabletSnapshot remote = buildRemoteTabletSnapshot(backupTablet.id, candidate.backupTableId,
                        candidate.backupPartition.id, backupIdx.id);
                if (remote == null) {
                    return null;
                }
                source.setRemoteTabletSnapshot(remote);
                return source;
            }
            List<FsBroker> brokerAddrs = Lists.newArrayList();
            Status st = repo.getBrokerAddress(beId, env, brokerAddrs);
            if (!st.ok() || brokerAddrs.size() != 1) {
                LOG.warn("failed to get the broker of backend {} for the incremental restore: {}", beId, st);
                return null;
            }
            IdChain ids = new IdChain(candidate.backupTableId, candidate.backupPartition.id, backupIdx.id,
                    backupTablet.id, -1L, -1L);
            String src = repo.getRepoPath(label, jobInfo.getFilePath(ids));
            if (src == null) {
                return null;
            }
            source.setBrokerAddr(new TNetworkAddress(brokerAddrs.get(0).host, brokerAddrs.get(0).port));
            source.setBrokerProp(repo.getFileSystemDescriptor().getBackendConfigProperties());
            source.setStorageBackend(repo.getFileSystemDescriptor().getThriftStorageType().toThrift());
            source.setLocation(repo.getLocation());
            source.setRemotePath(src);
            return source;
        } catch (Exception e) {
            LOG.warn("failed to build the prefix digest source of tablet {} of the backup. {}", backupTablet.id,
                    this, e);
            return null;
        }
    }

    /**
     * The remote tablet snapshot of a tablet of a backup kept on a backend (the fields of the local side are not
     * set), null if the job info does not tell where it is.
     */
    private TRemoteTabletSnapshot buildRemoteTabletSnapshot(long remoteTabletId, long remoteTblId,
            long remotePartId, long remoteIdxId) {
        Long remoteBeId = jobInfo.getBeId(remoteTabletId);
        String remoteSnapshotPath = jobInfo.getTabletSnapshotPath(remoteTabletId);
        Long schemaHash = jobInfo.getSchemaHash(remoteTblId, remotePartId, remoteIdxId);
        if (remoteBeId == null || remoteSnapshotPath == null || schemaHash == null) {
            return null;
        }
        TNetworkAddress remoteBeAddr = jobInfo.getBeAddr(remoteBeId);
        if (remoteBeAddr == null) {
            return null;
        }
        TRemoteTabletSnapshot remote = new TRemoteTabletSnapshot();
        remote.setRemoteTabletId(remoteTabletId);
        remote.setRemoteBeId(remoteBeId);
        remote.setRemoteBeAddr(remoteBeAddr);
        remote.setRemoteSnapshotPath(String.format("%s/%d/%d", remoteSnapshotPath, remoteTabletId, schemaHash));
        remote.setRemoteToken(jobInfo.getToken());
        String manifestRoot = getExpectedManifestRoot(remoteTabletId);
        if (manifestRoot != null) {
            remote.setManifestRoot(manifestRoot);
        }
        return remote;
    }

    protected void waitingAllDigestsFinished() {
        Database db = env.getInternalCatalog().getDbNullable(dbId);
        if (db == null) {
            status = new Status(ErrCode.NOT_FOUND, "database " + dbId + " does not exist");
            return;
        }
        if (!unfinishedSignatureToId.isEmpty()) {
            boolean timedOut = System.currentTimeMillis() - verifyRoundStartMs
                    > Config.restore_digest_timeout_s * 1000L;
            if (!timedOut) {
                LOG.info("waiting {} digest tasks to finish. {}", unfinishedSignatureToId.size(), this);
                return;
            }
            LOG.warn("{} digest tasks timed out after {} s, download the partitions of them. {}",
                    unfinishedSignatureToId.size(), Config.restore_digest_timeout_s, this);
            for (Long signature : unfinishedSignatureToId.keySet()) {
                AgentTaskQueue.removeTaskOfType(TTaskType.RESTORE_DIGEST, signature);
            }
            unfinishedSignatureToId.clear();
        }

        judgeDigestRound();

        if (verifyRound == 1) {
            // The sample decides the rest: a mismatch means the local partitions are not what the lineage says,
            // so every other candidate is verified too; an uncertain sample (a failed task, a timeout) proves
            // nothing, so the rest is downloaded.
            boolean mismatch = false;
            boolean uncertain = false;
            for (ReuseCandidate candidate : reuseCandidates) {
                if (!candidate.sampled || !candidate.decided || candidate.kept) {
                    continue;
                }
                if (RestoreReuseResult.DOWNLOAD_ROOT_MISMATCH.equals(candidate.reason)
                        || RestoreReuseResult.DOWNLOAD_SCHEMA_MISMATCH.equals(candidate.reason)) {
                    mismatch = true;
                } else {
                    uncertain = true;
                }
            }
            List<ReuseCandidate> rest = Lists.newArrayList();
            for (ReuseCandidate candidate : reuseCandidates) {
                if (!candidate.decided) {
                    rest.add(candidate);
                }
            }
            if (!rest.isEmpty() && (mismatch || uncertain)) {
                if (mismatch) {
                    LOG.info("restore reuse: a sampled partition is not consistent, verify the other {} "
                            + "candidates too. {}", rest.size(), this);
                    verifyRound = 2;
                    for (ReuseCandidate candidate : rest) {
                        candidate.level = CheckLevel.FULL;
                    }
                    sendDigestRound(db, rest);
                    return;
                }
                for (ReuseCandidate candidate : rest) {
                    candidate.decide(false, RestoreReuseResult.DOWNLOAD_SAMPLE_FAILED);
                }
            }
        }
        // The sampled partitions are all consistent: the partitions that were not sampled are kept.
        for (ReuseCandidate candidate : reuseCandidates) {
            if (!candidate.decided) {
                candidate.decide(true, RestoreReuseResult.KEPT_SAMPLE_PASSED);
            }
        }
        finishVerifying(db);
    }

    // Judge the partitions of the round that are not decided: all digests of a partition are in, or the round timed
    // out.
    private void judgeDigestRound() {
        for (ReuseCandidate candidate : reuseCandidates) {
            if (candidate.decided || candidate.tablets == null) {
                continue;
            }
            String reason = null;
            boolean complete = true;
            for (ReuseTabletCheck check : candidate.tablets) {
                List<LogicalDigestInfo> actual = Lists.newArrayList();
                for (long signature : check.signatures) {
                    LogicalDigestInfo digest = digestResults.get(signature);
                    if (digest == null) {
                        complete = false;
                        break;
                    }
                    actual.add(digest);
                }
                if (!complete) {
                    break;
                }
                reason = candidate.incrementalCandidate ? RestoreReuseJudge.compareIncrementalTablet(actual)
                        : RestoreReuseJudge.compareTablet(check.expected, actual);
                if (reason != null) {
                    break;
                }
            }
            if (!complete) {
                candidate.decide(false, RestoreReuseResult.DOWNLOAD_TIMEOUT);
            } else if (reason == null && candidate.incrementalCandidate) {
                candidate.decideIncremental();
            } else if (reason == null) {
                candidate.decide(true, RestoreReuseResult.KEPT_DIGEST_VERIFIED);
            } else {
                candidate.decide(false, reason);
            }
        }
    }

    /**
     * Fix the decisions: the partitions that keep their data are taken out of the restore (no file mapping, no
     * restoredVersionInfo, so no snapshot, download or move task, and the lineage is kept), and the lineage of the
     * other overwritten partitions is invalidated, then go on to SNAPSHOTING.
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
        setState(RestoreJobState.SNAPSHOTING);
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
            } finally {
                tbl.writeUnlock();
            }
        }
        return Status.OK;
    }

    // The restore source of a table in the backup meta and the restore lineage of its partitions, as the backup
    // meta carried them.
    static class BackupStamps {
        RestoreSource tableSource;
        OlapTable table;
        final Map<String, RestoreLineage> partitionLineage = Maps.newHashMap();
    }

    /**
     * Read the restore source and the restore lineage that the backup meta carries, before they are cleared. They
     * describe the source cluster's own upstream, so they are only used by the reverse L0 checks, and are never
     * written to the local metadata.
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
            stamps.tableSource = remoteOlap.getRestoreSource();
            stamps.table = remoteOlap;
            for (Partition remotePart : remoteOlap.getAllPartitions()) {
                if (remotePart.getRestoreLineage() != null) {
                    stamps.partitionLineage.put(remotePart.getName(), remotePart.getRestoreLineage());
                }
            }
            backupStamps.put(tableName, stamps);
        }
    }

    private RestoreReuseJudge.Input buildReuseInput(String backupTableName, BackupOlapTableInfo tblInfo,
            String partitionName, BackupPartitionInfo partInfo, OlapTable localTbl, Partition localPart,
            CheckLevel level) {
        RestoreReuseJudge.Input in = new RestoreReuseJudge.Input();
        in.atomicRestore = isAtomicRestore;
        in.allowLoad = allowLoad;
        in.cloudMode = Config.isCloudMode();
        in.minPartitionBytes = Config.restore_reuse_min_partition_bytes;
        in.incrementalEnabled = Config.enable_restore_incremental_append && Config.enable_restore_partition_reuse;
        in.level = level;
        in.jobInfo = jobInfo;
        in.backupTable = tblInfo;
        in.backupPartition = partInfo;
        in.srcCommitSeq = getSrcCommitSeq(jobInfo, tblInfo.id);
        in.localDbId = dbId;
        in.localClusterId = getLocalClusterId();
        in.localTable = localTbl;
        in.localPartition = localPart;
        BackupStamps stamps = backupStamps.get(backupTableName);
        if (stamps != null) {
            in.backupTableSource = stamps.tableSource;
            in.backupOlapTable = stamps.table;
            in.backupLineage = stamps.partitionLineage.get(partitionName);
        }
        return in;
    }

    // The cluster id of this cluster, UNKNOWN_CLUSTER_ID if it can not be told.
    private static long getLocalClusterId() {
        try {
            return Env.getCurrentEnv().getClusterId();
        } catch (Exception e) {
            return RestoreLineage.UNKNOWN_CLUSTER_ID;
        }
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
        String l0Path;
        // The effective check level (may be raised later), and the one the job requested.
        CheckLevel level;
        CheckLevel requestedLevel;
        // The cluster id of the source was compared on both sides and equal, see RestoreReuseJudge#clusterProven.
        boolean clusterProven = false;
        BackupPartitionInfo backupPartition;
        // The local partition is behind the backup: if the digest tasks pass, it is restored by appending the rowsets
        // of the versions after digestVersion (the local version) up to version (the backup).
        boolean incrementalCandidate = false;
        long digestVersion;
        long backupTableId;
        // atomic restore: the staging table and partition the data goes to
        boolean atomic = false;
        long stagingTableId;
        long stagingPartitionId;
        // the outcome: restored incrementally
        boolean incremental = false;
        // in the sample of the check level sample
        boolean sampled = false;
        boolean decided = false;
        boolean kept = false;
        String reason;
        // the digest tasks of the partition, null before they are sent
        List<ReuseTabletCheck> tablets;

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
            decision.l0Path = l0Path;
            decision.level = level.lower();
            decision.requestedLevel = (requestedLevel == null ? level : requestedLevel).lower();
            decision.kept = kept;
            decision.incremental = incremental;
            decision.reason = reason;
            decision.atomic = atomic;
            decision.stagingTableId = stagingTableId;
            decision.stagingPartitionId = stagingPartitionId;
            return decision;
        }
    }

    // The digest of a tablet in the backup, and the signatures of the digest tasks of its local replicas.
    static class ReuseTabletCheck {
        final LogicalDigestInfo expected;
        final List<Long> signatures = Lists.newArrayList();

        ReuseTabletCheck(LogicalDigestInfo expected) {
            this.expected = expected;
        }
    }

    // The check level the estimate is computed with: the level of the job, as for the actual judgement. If the
    // feature is off or disabled for the job, the estimate follows the default level, as if it were enabled.
    // Note: DISABLE is taken as OFF, which levelOfTable raises to FULL unless
    // restore_reuse_allow_unverified_keep, so the estimate counts the L0 paths that need a digest too (reverse, table
    // level), as the actual judgement with the default level does. Before the narrowing the default was SAMPLE, which
    // allowed them as well, but DISABLE as OFF counted the forward path only.
    private CheckLevel estimateLevel(OlapTable localTbl) {
        CheckLevel level = CheckLevel.parse(properties.get(PROP_REUSE_CHECK_LEVEL),
                Config.restore_reuse_default_check_level);
        if (level == CheckLevel.DISABLE) {
            level = CheckLevel.OFF;
        }
        return RestoreReuseJudge.levelOfTable(level, localTbl.getKeysType());
    }

    /**
     * Judge each partition to restore by the lineage check (L0), and count the partitions and bytes that
     * could be kept locally instead of downloading. It only records the result, and never fails the job.
     */
    @VisibleForTesting
    void computeReuseShadowStats(Database db) {
        if (Config.isCloudMode()) {
            // Storage is shared in cloud mode, there is nothing to download or reuse locally. Besides,
            // CloudPartition.getVisibleVersion() may call the meta service, which must not be done for every
            // partition while holding the table lock.
            reuseShadowStats = null;
            return;
        }
        try {
            RestoreReuseShadowStats stats = new RestoreReuseShadowStats();
            for (Map.Entry<String, BackupOlapTableInfo> tblEntry : jobInfo.backupOlapTableObjects.entrySet()) {
                BackupOlapTableInfo tblInfo = tblEntry.getValue();
                Table localTbl = db.getTableNullable(jobInfo.getAliasByOriginNameIfSet(tblEntry.getKey()));
                if (localTbl == null || localTbl.getType() != TableType.OLAP) {
                    for (int i = 0; i < tblInfo.partitions.size(); i++) {
                        stats.add(RestoreReuseShadowStats.L0Verdict.NO_LOCAL_PARTITION,
                                RestoreReuseShadowStats.Unsupported.NONE, 0);
                    }
                    continue;
                }
                OlapTable localOlapTbl = (OlapTable) localTbl;
                localOlapTbl.readLock();
                try {
                    for (Map.Entry<String, BackupPartitionInfo> partEntry : tblInfo.partitions.entrySet()) {
                        BackupPartitionInfo partInfo = partEntry.getValue();
                        Partition localPart = localOlapTbl.getPartition(partEntry.getKey(), false);
                        RestoreReuseShadowStats.L0Verdict verdict;
                        String l0Path = RestoreReuseJudge.L0_FORWARD;
                        if (localPart == null) {
                            verdict = RestoreReuseShadowStats.L0Verdict.NO_LOCAL_PARTITION;
                        } else {
                            // The estimate follows the L0 checks of the check level of the job, the reverse and
                            // table level checks count only where a digest would be computed.
                            RestoreReuseJudge.Input in = buildReuseInput(tblEntry.getKey(), tblInfo,
                                    partEntry.getKey(), partInfo, localOlapTbl, localPart, estimateLevel(localOlapTbl));
                            RestoreReuseShadowStats.L0Verdict[] first = new RestoreReuseShadowStats.L0Verdict[1];
                            String path = RestoreReuseJudge.passedL0(in, first);
                            verdict = path != null ? RestoreReuseShadowStats.L0Verdict.REUSABLE : first[0];
                            l0Path = path;
                        }
                        RestoreReuseShadowStats.Unsupported unsupported = RestoreReuseShadowStats.Unsupported.NONE;
                        long bytes = 0;
                        if (verdict == RestoreReuseShadowStats.L0Verdict.REUSABLE) {
                            unsupported = RestoreReuseShadowStats.checkUnsupported(isAtomicRestore, localOlapTbl,
                                    localPart);
                            bytes = RestoreReuseShadowStats.getSingleReplicaLocalDataSize(localPart);
                        }
                        stats.add(verdict, unsupported, bytes, l0Path);
                    }
                } finally {
                    localOlapTbl.readUnlock();
                }
            }
            reuseShadowStats = stats;
            LOG.info("restore reuse shadow stats: {}, job: {}", stats, jobId);
        } catch (Exception e) {
            reuseShadowStats = null;
            LOG.warn("failed to compute restore reuse shadow stats, ignore it. job: {}", jobId, e);
        }
    }

    /**
     * Clear the restore lineage of the existing partitions that this job overwrites in place (non-atomic
     * restore, see restoredVersionInfo). Partitions and tables created by this job are dropped if the job is
     * cancelled, and atomic restore does not touch the origin table before it is replaced, so they are not
     * cleared here.
     */
    @VisibleForTesting
    void invalidateRestoreLineageOfOverwrittenPartitions(Database db) {
        invalidateRestoreLineageOfOverwrittenPartitions(db, Collections.emptySet());
    }

    /**
     * @param skipped the (table id, partition id) of the partitions whose lineage is kept, because they are
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
                        part.setRestoreLineage(null);
                    }
                }
            } finally {
                olapTbl.writeUnlock();
            }
        }
    }

    @VisibleForTesting
    void clearRestoreLineageInBackupMeta() {
        for (String tableName : jobInfo.backupOlapTableObjects.keySet()) {
            Table remoteTbl = backupMeta.getTable(tableName);
            if (!(remoteTbl instanceof OlapTable)) {
                continue;
            }
            ((OlapTable) remoteTbl).setRestoreSource(null);
            for (Partition remotePart : ((OlapTable) remoteTbl).getAllPartitions()) {
                remotePart.setRestoreLineage(null);
            }
        }
    }

    /**
     * Write the restore lineage of all partitions restored by this job, both the existing partitions that
     * are overwritten and the newly created partitions and tables. The lineage is always rebuilt from the
     * job info of this job, and never inherited from the backup meta. Called on both master and replay.
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
                olapTbl.setRestoreSource(new RestoreSource(jobInfo.dbId, tblInfo.id,
                        jobInfo.getSrcClusterIdOrUnknown()));
                for (Map.Entry<String, BackupPartitionInfo> partEntry : tblInfo.partitions.entrySet()) {
                    Partition part = olapTbl.getPartition(partEntry.getKey(), false);
                    if (part == null) {
                        continue;
                    }
                    part.setRestoreLineage(buildRestoreLineage(jobInfo, tblInfo, partEntry.getValue(), restoreTime));
                }
            } finally {
                olapTbl.writeUnlock();
            }
        }
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
                case VERIFYING:
                    // remove all digest tasks in AgentTaskQueue. Nothing local was changed, nothing else to undo.
                    for (Long signature : unfinishedSignatureToId.keySet()) {
                        AgentTaskQueue.removeTaskOfType(TTaskType.RESTORE_DIGEST, signature);
                    }
                    break;
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
                    if (reuseResult != null && reuseResult.getIncrementalPartitions()
                            > reuseResult.getIncrementalAtomicPartitions()) {
                        // The append of a tablet is all or nothing, but the tablets of a partition are not. Say so,
                        // restoring again downloads the partition as a whole, since its replicas differ.
                        LOG.warn("restore job {} is cancelled while appending the increments, the tablets of the "
                                + "incremental partitions {} may be appended in part; restore again to repair them",
                                jobId, reuseResult.getIncrementalDecisions());
                        status = new Status(status.getErrCode(), status.getErrMsg()
                                + " (incremental partitions may be partially appended, restore again to repair)");
                    }
                    break;
                default:
                    break;
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

            if (state == RestoreJobState.DOWNLOADING && manifestCheck == null) {
                // keep the partial result of the manifest check for SHOW RESTORE
                manifestCheck = computeManifestCheck();
            }
            if (state == RestoreJobState.DOWNLOADING) {
                fixDownloadStats();
            }
            com.google.common.collect.Table<Long, Long, SnapshotInfo> savedSnapshotInfos = snapshotInfos;
            snapshotInfos = HashBasedTable.create();
            fileMapping.clear();
            jobInfo.releaseSnapshotInfo();
            createReplicasTimeStamp = -1;

            RestoreJobState curState = state;
            finishedTime = System.currentTimeMillis();
            setState(RestoreJobState.CANCELLED);
            clearLocalSourceStagingPartitions();
            // log
            env.getEditLog().logRestoreJob(this);
            for (ColocatePersistInfo info : colocatePersistInfos) {
                Env.getCurrentColocateIndex().removeTable(info.getTableId());
                env.getEditLog().logColocateRemoveTable(info);
            }
            colocatePersistInfos.clear();

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
                                    for (MaterializedIndex idx
                                            : part.getMaterializedIndices(IndexExtState.VISIBLE, true)) {
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
                    for (MaterializedIndex index : entry.second.getMaterializedIndices(IndexExtState.ALL, true)) {
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
        }
        return Status.OK;
    }

    private Status atomicReplaceOlapTables(Database db, boolean isReplay) {
        for (String tableName : jobInfo.backupOlapTableObjects.keySet()) {
            String originName = jobInfo.getAliasByOriginNameIfSet(tableName);
            if (GlobalVariable.isStoredTableNamesLowerCase()) {
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

                // The partitions made from the local tablets of the origin table must be at the version they were
                // proved at, until the table is replaced: no write is allowed to be in between (the check and the
                // replace are under the same lock of the origin table). The origin table is not touched if not.
                boolean originLocked = false;
                if (!isReplay && originOlapTbl != null && hasAtomicLocalDecision(originOlapTbl.getId())) {
                    originOlapTbl.writeLock();
                    originLocked = true;
                    Status st = checkAtomicLocalVersions(originOlapTbl);
                    if (!st.ok()) {
                        originOlapTbl.writeUnlock();
                        return st;
                    }
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
                    if (originLocked) {
                        originOlapTbl.writeUnlock();
                    }
                    return new Status(ErrCode.COMMON_ERROR, "replace table from " + aliasName + " to " + originName
                            + " failed, reason=" + e.getMessage());
                } finally {
                    newOlapTbl.writeUnlock();
                }

                if (originOlapTbl != null) {
                    // The origin table is not used anymore, need to drop all its tablets.
                    originOlapTbl.writeLock();
                    try {
                        LOG.info("drop the origin olap table {}. table={}" + " isAtomicRestore: {}",
                                originOlapTbl.getName(), originOlapTbl.getId(), isAtomicRestore);
                        Env.getCurrentEnv().onEraseOlapTable(db.getId(), originOlapTbl, isReplay);
                    } finally {
                        originOlapTbl.writeUnlock();
                    }
                }
                if (originLocked) {
                    originOlapTbl.writeUnlock();
                }
            } finally {
                db.writeUnlock();
            }
        }
        for (BackupJobInfo.BackupViewInfo backupViewInfo : jobInfo.newBackupObjects.views) {
            String originName = jobInfo.getAliasByOriginNameIfSet(backupViewInfo.name);
            if (GlobalVariable.isStoredTableNamesLowerCase()) {
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
                // auto partition with retention_count also need to be registered to dynamic scheduler
                if (committed && olapTbl.getPartitionRetentionCount() > 0) {
                    DynamicPartitionUtil.registerOrRemoveDynamicPartitionTable(db.getId(), olapTbl, isReplay);
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
        String json = Text.readString(in);
        if (AbstractJob.COMPRESSED_JOB_ID.equals(json)) {
            return GsonUtilsBase.fromJsonCompressed(in, RestoreJob.class, GsonUtils.GSON);
        } else {
            return GsonUtils.GSON.fromJson(json, RestoreJob.class);
        }
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
        showState = state;
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
