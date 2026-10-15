# 增量追加卡住问题分析（测试环境，3 万 tablet 规模的原子恢复）

分析对象：测试环境中的一次恢复作业（原子恢复，`reuse_check_level=full`，3 万 tablet 规模），其中一个 tablet 在 BE1 上的 MOVE 任务。代码基线：分支 `restore-r2`，HEAD `9b9149a12ff`。本文只读代码与日志，未改代码、未动集群。

## 0. 结论

**根因（已证实）**：版本 4 是 **FE 的 TabletScheduler 下发的 REPAIR clone** 写进去的，不是追加重复执行，也不是第一次追加其实成功。

链条如下：

1. 原子恢复为增量分区建的暂存 tablet，FE 侧副本版本在创建时就等于**备份版本 V_b = 4**（`RestoreJob.resetTabletForRestore`，`RestoreJob.java:1845`）；而 BE 侧在 SNAPSHOTING 阶段用"本地来源快照"把该 tablet 装成了原表的 **V_l = 3**（`SnapshotLoader::make_local_source_snapshot_and_load`，`snapshot_loader.cpp:2454-2482`；日志 `add duplicated tablet ... old_version=4, new_version=3`）。
2. 从 22:59 到 23:18 的近 20 分钟里，FE 认为副本在 4，BE 上报 3。`ReportHandler.handleRecoverTablet` 把三个副本标为缺版本（`lastFailedVersion=5`），tablet checker 把该 tablet 放进 REPAIR 队列，状态 VERSION_INCOMPLETE。
3. 表处于 RESTORE 状态，但 `TabletScheduler` 对 **VERSION_INCOMPLETE 例外放行**（`TabletScheduler.java:646-656`："VERSION_INCOMPLETE will repair the replica in place, which is allowed"）。之前之所以没修，只是因为三个副本都在 3，找不到版本完整的源副本（23:12:15 `UNRECOVERABLE` 被移出）。
4. 23:18:44 BE2、BE3 的增量追加先完成（版本到 4）；23:18:45 BE2 上报，FE 同步到 4；23:18:45.463 checker 重新入队；23:18:46.541 **向 BE1 下发 clone**（源 BE2，`missed_versions=[4-4]`）；23:18:46.667 clone 完成，BE1 的 tablet 已是 4。
5. BE1 的 MOVE 任务 23:15:18 就已提交，但 MOVE 线程池只有 1 个线程（`agent_server.cpp:142`），排队 3.5 分钟后 **23:18:47.003** 才开始执行，比 clone 晚 0.3 秒，`append_increment` 的版本前置检查失败（`snapshot_loader.cpp:2572-2576`）。
6. 之后每 ~12 秒重试一次，共 308 次，直到 23:57 人工取消。

**为什么无限重试（已证实）**：`append_increment` 返回的是 `InternalError`，FE 的 `finishDirMoveTask` 对任何非 OK 状态只是把错误信息记到 `taskErrMsg`、返回 false（`RestoreJob.java:593-618`），`MasterImpl.finishMoveDirTask` 因此**不把任务从 AgentTaskQueue 移除**（`MasterImpl.java:657-664`）；BE 每 10 秒上报运行中任务（`report_task_interval_seconds=10`），`ReportHandler.taskReport` 把队列里 BE 没在跑的任务重发（`ReportHandler.java:747-767`，`agent_task_resend_wait_time_ms=5000`）。RestoreJob 自身没有任何"错误次数 / 不可恢复错误 → 作业失败"的判断，只有 24 小时的作业超时（`timeout: 86400000`）会终止它。

**不稳定复现的原因（已证实）**：需要同时满足 (a) 该 tablet 的某个副本的追加先完成并被上报，(b) 另一个副本的 MOVE 任务还在单线程队列里排队，(c) 期间恰好过了一轮 tablet checker（20 秒）并且 clone 在该 MOVE 执行之前完成。3 万 tablet 时 COMMITTING 要跑 20 分钟、队列深度上万，窄窗口被放大；小规模回归里基本碰不到。

**clone 写入的内容是否正确（已证实 + 推断）**：clone 从 BE2 拷来的 (3,4] rowset 就是 BE2 追加进去的、从同一份备份转换而来的 rowset，内容与 BE1 本该追加的完全相同（`clone get tablet info success ... version:4`，FE 同步后 dataSize 3504/rowCount 26，与 BE2 一致）。也就是说，卡住时 BE1 的 tablet 已经处于**正确的最终状态**，追加只要能识别"已经到目标版本且内容一致"就可以直接成功。此为推断：没有逐行比对过 BE1 与 BE2 的数据（作业已取消、暂存表已清理，无法补做）。

## 1. 证据时间线

来源：测试机上事故现场保存的 FE 与 BE1～BE3 的 tablet 相关日志，以及完整的 FE、BE 日志。下表时间为当次运行的时钟时间。

| 时间 | 端 | 事件 | 日志位置 |
|---|---|---|---|
| 22:54:00 | FE | 为该 tablet 的三个副本设 base tablet（原表 tablet） | `RestoreJob.createReplicas():1765` |
| 22:54:06~11 | BE1/2/3 | 创建 tablet（`create tablet use the base tablet data dir`） | `tablet_manager.cpp:328` |
| 22:59:32~40 | BE1/2/3 | 快照任务：先对原表 tablet 做本地来源快照 version=3，再 `move` 到暂存 tablet 目录并重载表头：`add duplicated tablet. force=1, old_version=4, new_version=3`；随后为增量下载建空快照目录 | `snapshot_manager.cpp:154`、`snapshot_loader.cpp:2303`、`tablet_manager.cpp:189` |
| 23:02:22 | BE1/2/3 | 下载增量 (3,4]：1 个 .dat（1073 B）+ hdr；`incremental tablets: 1` | `snapshot_loader.cpp:2192` |
| 23:02:52 | FE | `finished to send move dir tasks. num: 90000`，进入 COMMITTING | `RestoreJob.commit():2511` |
| 23:05:08~51 | FE | 三个副本：`set missing version ... version in fe 4, version in be 3`，`lastFailedVersion=5` | `ReportHandler.handleRecoverTablet():1408` |
| 23:05:32 | FE | checker 入队：`VERSION_INCOMPLETE, REPAIR, VERY_HIGH` | `TabletScheduler.addTablet():307` |
| 23:12:15 | FE | 调度器移出：`table is in alter process, but tablet status is UNRECOVERABLE`（三副本都在 3，无源副本） | `TabletScheduler.removeTabletCtx():2213` |
| 23:15:18 | BE1 | MOVE <signature> 提交到 BE1 任务队列（`successfully submit task\|type=MOVE`） | `task_worker_pool.cpp:437` |
| 23:18:44.23~.27 | BE2 | 追加 (3,4] 成功，`successfully move dir\|signature=<signature>` | `snapshot_loader.cpp:2489/2652` |
| 23:18:44.56~.59 | BE3 | 同上，signature <signature> | 同上 |
| 23:18:45.308 | FE | 收到 BE2 上报，`sync replica ... version=4 ... last failed version change to -1` | `ReportHandler.sync():896` |
| 23:18:45.463 | FE | checker 再次入队 VERSION_INCOMPLETE | `TabletScheduler.addTablet():307` |
| 23:18:46.541 | FE | `create clone task to repair replica ... replica=<replica_id>`（BE1） | `TabletSchedCtx.createCloneReplicaAndTask():1175` |
| 23:18:46.571 | BE1 | `get clone task ... clone to existed tablet. missed_versions_size=1 ... visible_version=4` | `engine_clone_task.cpp:246` |
| 23:18:46.667 | BE1 | `finish incremental clone`，`revise tablet`，`clone get tablet info success ... version:4` | `engine_clone_task.cpp:893/351` |
| **23:18:47.003** | BE1 | `get move dir task. signature=<signature>`，`begin to append the increment (3, 4]` | `task_worker_pool.cpp:1860`、`snapshot_loader.cpp:2489` |
| 23:18:47.193 | BE1 | `failed to move dir ... the max version of tablet <tablet_id> is 4, not 3` | `task_worker_pool.cpp:1877` |
| 23:18:47.193 | FE | `finish task reports bad. ... task_type:MOVE, signature:<signature>` | `MasterImpl.finishTaskInternal():116` |
| 23:18:50 起 | BE1 | 每 ~12 秒 `successfully submit task\|type=MOVE\|signature=<signature>` → 同样失败，共 308 次 | be.WARNING |
| 23:23:39 起 | FE | `waiting 1 tablets to commit`（695 次） | `RestoreJob.waitingAllTabletsCommitted():2524` |
| 23:57:08 | FE | 人工取消，`status: user cancelled, current state: COMMITTING` | `RestoreJob.cancelInternal():3861` |

两个关键差值：clone 完成 (23:18:46.667) 与追加开始 (23:18:47.003) 相差 **336 ms**；MOVE 任务提交 (23:15:18) 到执行 (23:18:47) 排队 **3 分 29 秒**。

## 2. 候选原因逐项排除

| 候选 | 结论 | 依据 |
|---|---|---|
| 同一个追加任务重复执行、第二次撞到第一次的结果 | 否 | BE1 第一次 `begin to append` 就是 23:18:47.003，此前没有任何该 tablet 的 append 日志；第一次就失败。`append_increment` 也有 loaded tag 保护（`snapshot_loader.cpp:2510-2516`），同一快照目录第二次会直接返回 OK |
| 任务重试时第一次其实已成功 | 否 | 同上；且成功会写 loaded tag，之后重试会打 `snapshot path already appended` 而不是版本错误 |
| FE 版本对账 / clone 写入 | **是** | 23:18:46 的 clone 日志链完整（FE 下发、BE 执行、revise、上报版本 4） |
| 原子恢复的暂存 tablet 被其他路径写入（load、compaction、另一恢复作业） | 否 | 该 tablet 在 BE1 日志里 22:54～23:18 之间只有 create、本地来源快照、下载、clone、append；表在 RESTORE 状态不接受导入；暂存表名带前缀，无人访问 |
| FE 重启导致任务重发错乱 | 否 | 事故期间 FE 没有重启，FE 日志连续 |

## 3. 代码路径

### 3.1 FE 把暂存副本版本设成 V_b（原子路径特有）

- `RestoreJob.resetTabletForRestore`（`RestoreJob.java:1821-1860`）：`new LocalReplica(newReplicaId, beId, ReplicaState.NORMAL, visibleVersion, schemaHash)`，`visibleVersion = remotePart.getVisibleVersion()` 即备份版本 4；分区的 visible version 也是 4（`resetPartitionForRestore`，`:1813-1814`）。
- 非原子路径的增量分区复用的是**已有**分区和副本（A1 情况，`genFileMapping(..., overwrite)`），FE 副本版本就是本地 V_l=3，与 BE 一致，不会触发修复；版本在提交时才改到 4（`allTabletCommitted`，`RestoreJob.java:2572-2580`）。所以这个竞争**只影响原子恢复 + 增量分区**。
- 原子路径下的"保留"分区：本地来源快照版本 = V_b，FE 与 BE 一致，不受影响。全量下载的 tablet：BE 创建时就带版本 4 的空 rowset，上报 4，也不受影响。

### 3.2 BE 把暂存 tablet 装成 V_l

- `make_snapshot_callback`（`task_worker_pool.cpp:1677-1690`）：`restore_local_source && restore_incremental` 时走 `make_local_source_snapshot_and_load`。
- `SnapshotLoader::make_local_source_snapshot_and_load`（`snapshot_loader.cpp:2454-2482`）：对原表 tablet 做版本 3 的快照 → `move(..., overwrite=true)` 替换暂存 tablet 目录并 `load_tablet_from_dir(force=true)` → 再为增量下载建一个空快照目录。此后 BE 上该 tablet 的 max version = 3。

### 3.3 FE 对账与修复

- `ReportHandler.handleRecoverTablet`（`ReportHandler.java:1337-1410`）：`replica.checkVersionRegressive(beVersion)` 为真（FE 4 > BE 3）→ `lastFailedVersion = 5`。
- `Tablet.getHealth`（`Tablet.java:461-467`）：有版本完整的副本且有不完整副本 → `VERSION_INCOMPLETE, VERY_HIGH`；一个都没有 → `UNRECOVERABLE`。
- `TabletScheduler.scheduleTablet`（`TabletScheduler.java:646-656`）：表非 NORMAL 时只拦截非 VERSION_INCOMPLETE 的状态，VERSION_INCOMPLETE 放行并"原地修复"。这是上游为 schema change 的影子副本设计的例外，对 RESTORE 状态的表同样生效。
- clone 在 BE 侧：`EngineCloneTask::_finish_incremental_clone`（`engine_clone_task.cpp:890-920`）→ `revise_tablet_meta(rowsets, {}, true)`，与 `append_increment` 用的是同一个函数（`snapshot_loader.cpp:2639`），MoW 的 delete bitmap 计算逻辑相同。

### 3.4 BE 追加的前置检查

`SnapshotLoader::append_increment`（`snapshot_loader.cpp:2484-2656`）顺序：loaded tag 检查 → `convert_rowset_ids`（会把快照目录里的段文件改名、重写 hdr，`_rename_rowset_id` 在 `snapshot_manager.cpp:359-410`，这就是日志里 `deleting .../snapshot/.../*.dat` 的来源，每次重试都会再转一遍，可重入）→ 校验 rowset 链 (base, end] → 取锁 → **`max_version_unlocked() != range.base_version` 则 InternalError**（`:2572-2576`）→ 链接文件 → `revise_tablet_meta` → 写 loaded tag。失败发生在取锁之后、改动文件之前，tablet 未被破坏。

### 3.5 FE 的失败处理与重发

- `RestoreJob.finishDirMoveTask`（`RestoreJob.java:593-607`）→ `checkTaskStatus`（`:609-618`）：非 OK 只记 `taskErrMsg`，返回 false。
- `MasterImpl.finishMoveDirTask`（`MasterImpl.java:657-664`）：返回 false 时不 `removeTask`。
- `ReportHandler.taskReport`（`ReportHandler.java:719-767`）：BE 每 `report_task_interval_seconds`(10s) 上报，队列里 BE 没在跑且创建超过 `agent_task_resend_wait_time_ms`(5s) 的任务重发。观察到的 12~15 秒周期与之吻合。
- `RestoreJob.run`（`:678-700`）只有 `createTime + timeoutMs` 超时一条退出路径；`checkIfNeedCancel`（`:748-782`）只查库表分区是否被删；`waitingAllTabletsCommitted`（`:2515-2525`）只等 `unfinishedSignatureToId` 清空。`taskErrMsg` 仅用于 SHOW RESTORE 展示（`:2782-2787`）。
- 这个"失败任务留在队列里由上报重发"是 Doris 上游 MOVE/DOWNLOAD/MAKE_SNAPSHOT 任务的既有机制，不是 R2 引入的；R2 引入的是一类**确定性失败**（版本前置检查），让既有机制的缺点暴露出来。

## 4. 修复方案

三条并行，建议都做；(c) 消除竞争是治本，(a) 幂等是兜底，(b) 让"真失败"快速收敛。

### (a) BE：追加可识别"已追加"，视为成功

位置：`SnapshotLoader::append_increment`，在 `:2572` 的版本检查处改为三分支：

1. `max_version == range.base_version`：照旧追加。
2. `max_version == range.end_version`：检查本地 (base, end] 区间的 rowset 与快照 hdr 里的 rowset **一一对应**：`capture_consistent_rowsets_unlocked(Version(base+1, end))` 得到本地 rowset 列表，与 `rs_metas` 按 `(start_version, end_version)` 比较，且每对的 `num_rows`、`num_segments`、`data_disk_size`（可选 `delete_predicate` 是否存在）相等。全部相等 → 写 loaded tag、记一条 `increment already present` 日志、返回 OK；否则返回 **不可重试错误**（见 (b) 的错误码约定）。
3. 其他 → 不可重试错误。

正确性论证：
- 本地 (0, base] 在 VERIFYING 已用摘要证明与备份一致，提交前又有 `isStillAtVersion`/`checkKeptPartitions` 复查；(base, end] 这段若版本边界、行数、段数、字节数都与备份 rowset 一致，唯一可能的来源就是同一份备份数据经 clone/追加写入（RESTORE 状态下不接受导入，compaction 不会产生恰好等于备份 rowset 边界与字节数的结果——同一数据不同写法字节数通常不同，这里要求字节数相等是保守条件）。rowset id 不比（clone 和追加转换出的 id 不同）。
- 不能只比版本：如果本地被别的东西写到 end 版本但内容不同，必须失败。
- MoW：clone 路径和追加路径都调 `revise_tablet_meta(..., is_incremental_clone=true)`，对新 rowset 用本地数据重算 delete bitmap（`tablet.cpp` `revise_tablet_meta` 的 incremental 分支），结果相同；因此"已到版本且 rowset 一致"时 bitmap 也一致，不需要再算。
- MoR、带 DELETE 条件：rowset meta 里的 delete predicate 随 rowset 一起 clone，视同一致；比较时加上 `has_delete_predicate` 相等即可。
- Dup：无额外状态。
- 非原子路径：不会出现这个竞争（FE 版本 = V_l），分支 2 不会命中，无影响；但同样受益于幂等（例如 FE 重发时 BE 恰好刚成功但 loaded tag 还没写——现在 `write_loaded_tag` 在 revise 成功之后，窗口极小，但存在）。
- 原子路径：命中时直接成功，FE 随后按 `unfinishedSignatureToId` 清空提交，暂存表与原表互换，结果与正常路径相同。

单测：`be/test/storage/restore_digest/restore_digest_test.cpp` 已有 append_increment 的 Dup 与 MoW 用例（提交 `dcc367161da`），新增：
- `AppendIncrementAlreadyPresentIsIdempotent`：先用 clone 的方式（或直接 `revise_tablet_meta`）把同一个增量 rowset 加进 tablet，再调 `append_increment`，期望 OK、rowset 数不变、loaded tag 写出；Dup 与 MoW 各一个，MoW 校验 delete bitmap 不变。
- `AppendIncrementSameVersionDifferentContentFails`：本地到 end 版本但行数不同 → 不可重试错误，tablet 不变。
- `AppendIncrementRetryAfterConvertIsReentrant`：模拟第一次在版本检查处失败后再调一次 → 行为一致（覆盖 `convert_rowset_ids` 的重入）。

### (b) FE：不可恢复时让作业明确失败，而不是无限重试

1. BE 侧错误码约定：`append_increment`/`move` 把"状态不对"类错误用一个新的或既有的非重试错误码（例如 `Status::Error<ErrorCode::VERSION_ALREADY_MERGED>` 不合适，建议新增 `RESTORE_TABLET_STATE_MISMATCH`，或在错误信息前加固定前缀 `[NOT_RETRYABLE]`），`ObtainLockFailed` 等瞬时错误保持可重试。
2. `RestoreJob.finishDirMoveTask` / `checkTaskStatus`（已有同类先例可照抄：DOWNLOAD 任务收到 `RESTORE_MANIFEST_MISMATCH` 时 `finishTabletDownloadTask` 直接走 `handleManifestMismatch` → `cancelInternal`，`RestoreJob.java:437-441, 527-541`）：
   - 维护 `Map<Long signature, Integer> taskFailCount`；非 OK 时 +1。
   - 若错误被标为不可重试，或 `taskFailCount >= Config.restore_task_max_retry`（新增，默认 5），则 `status = new Status(COMMON_ERROR, "dir move of tablet X on backend Y failed N times: msg")`，返回 true（让 MasterImpl 从队列移除），下一轮 `run()` 走 `cancelInternal`。
   - DOWNLOAD、MAKE_SNAPSHOT 任务可用同一套计数（见第 5 节）。
3. 对原子路径：COMMITTING 失败 → `cancelInternal` → `cleanMetaObjects` 删暂存表，原表未被触碰，这正是原子恢复的保证；日志里的 `(incremental partitions may be partially appended, restore again to repair)` 提示仅对非原子路径有意义（`:3810-3819` 已区分 `getIncrementalAtomicPartitions`）。
4. 对非原子路径：COMMITTING 中途失败本来就会留下部分替换的 tablet（上游 FIXME，`snapshot_loader.cpp:2395`），R2 在 cancel 消息里已说明"再恢复一次修复"；(b) 不改变这一点，只是把等 24 小时变成几分钟。

单测：`fe/fe-core/src/test/java/org/apache/doris/backup/RestoreJobTest.java` / `RestoreReuseTest.java`：
- 构造 COMMITTING 作业，对一个 DirMoveTask 连续 `finishDirMoveTask` 返回带不可重试错误码的 `TFinishTaskRequest`，期望 `status` 非 OK、下一轮 `run()` 后 `state == CANCELLED`，且原子路径下原表仍在、暂存表被清理。
- 可重试错误（ObtainLockFailed）N-1 次后成功 → 作业继续。
- 计数超过上限 → 取消。

### (c) 消除竞争

有两种做法，推荐 c1，c2 作为可选加固。

**c1（推荐）：RESTORE 状态的表一律不做副本修复。**
`TabletScheduler.scheduleTablet`（`TabletScheduler.java:646`）增加：`tableState == OlapTableState.RESTORE || tableState == RESTORE_WITH_LOAD` 时无论 health 状态都抛 `UNRECOVERABLE/DIAGNOSE_IGNORE`（"table is being restored"）。
- 论证：RESTORE 期间 tablet 的数据由恢复作业全权负责（创建、快照、下载、移动、提交），修复 clone 对它只会制造冲突；作业结束（FINISHED 或 CANCELLED）后表回到 NORMAL，正常修复恢复。schema change 的影子副本不在 RESTORE 状态，上游例外的本意不受影响。`RESTORE_WITH_LOAD` 允许导入的那条路径里，副本缺版本本来也应等恢复结束再修。
- 影响：所有表模型、原子与非原子一样；只多了一条调度拒绝。
- 单测：`TabletSchedulerTest`/`TabletHealthTest` 里构造 RESTORE 状态的表 + VERSION_INCOMPLETE tablet，期望被拒绝并带原因；NORMAL 状态行为不变。

**c2（可选）：让 FE 的暂存副本版本与 BE 实际一致。**
对增量分区，在原子路径里把暂存副本和暂存分区的 visible version 初始化为 V_l，提交时再由 `allTabletCommitted` 的 `updateVersionForRestore(V_b)` 抬到 V_b。
- 风险：暂存分区 `visibleVersion`/`nextVersion` 牵涉 `resetPartitionForRestore`、`restoredVersionInfo`、提交后的 `updateOlapTablesVersion`，还要确认 BE 追加到 4 之后 FE 从 3 同步到 4 的 `ReportHandler.sync` 路径（`metaVersion < backendVersion → needSync`）不会把 `lastFailedVersion` 等置成怪值；改动面比 c1 大，收益与 c1 重叠。建议只在 c1 因某种原因不能接受时再做。

**不建议**的做法：靠 BE 侧加锁串行化。clone 和 append 已经取同一组 tablet 锁，它们本来就是串行的，问题在于先后顺序，不在并发。

### (d) 复现方法（回归用例）

要稳定触发需要让"一个副本先追加成功并上报"与"另一个副本的 MOVE 延后"同时发生，再让 tablet checker 跑一轮：
1. BE 新增调试点 `SnapshotLoader.append_increment.delay`（参数 `backend_id`/`tablet_id`、`sleep_ms`），在 `append_increment` 取锁之前 sleep；FE 侧用 `tablet_checker_interval_ms` 小值（回归 FE 配置通常已缩短）和 BE `report_tablet_interval_seconds` 小值。
2. 用例流程（在 `test_backup_restore_atomic_reuse.groovy` 旁新增 `test_backup_restore_atomic_incremental_race.groovy`）：建 3 副本表 → 备份 → 原子恢复一次（建立血缘）→ 源端写一批（版本 +1）→ 再备份 → 对目标 BE1 打开延迟调试点（sleep 90 s）→ 原子恢复 → 期望：修复前作业卡在 COMMITTING 并在 BE1 日志出现 `max version ... not ...`（用于验证复现）；修复后作业 FINISHED，三副本数据与源端一致，并且（若做了 c1）FE 日志里没有对该表的 clone。
3. 同一个用例去掉延迟调试点、改为 `Tablet.revise_tablet_meta_fail` 注入（已有调试点，`tablet.cpp`）验证 (b)：作业在有限次重试后 CANCELLED，原表数据不变。
4. MoW 和 MoR+DELETE 各跑一遍（现有 `test_backup_restore_incremental_append_unique.groovy` 的表定义可复用）。

### (e) 改动项与验证

- (a) BE 幂等 + 3 个单测。
- (b) FE 失败收敛 + 错误码 + 单测。
- (c1) 调度器一处判断 + 单测。
- (d) 调试点 + 回归用例（含 MoW/MoR 变体）。
- 修复后需要在测试机上重跑 3 万 tablet 的 full 档一次验证（约 1 小时）。

## 5. 同类风险排查

| 位置 | 风险 | 判断 |
|---|---|---|
| `SnapshotLoader::move`（全量替换，`snapshot_loader.cpp:2296-2452`） | loaded tag 保证同一快照重入幂等；但 `remove_all(tablet_path)` 之后任何失败都会损坏 tablet（上游 FIXME `:2395`），且失败同样走无限重发 | 上游既有问题；(b) 的计数能把它从"24 小时"收敛成"几分钟后作业失败"。原子路径下暂存表损坏无害；非原子路径与上游一致 |
| `make_local_source_snapshot_and_load`（`:2454-2482`） | MAKE_SNAPSHOT 被重发时会再做一次本地来源快照并再次替换暂存 tablet 目录。SNAPSHOTING 阶段内重放内容相同、无害，但会多留一个快照目录（86400 s 后清理）。若 FE 在 DOWNLOADING/COMMITTING 之后还能重发旧的 MAKE_SNAPSHOT（`unfinishedSignatureToId.clear()` 后队列里残留），会把已下载/已追加的内容冲掉 | 推测：阶段切换时 `unfinishedSignatureToId.clear()` 不会从 AgentTaskQueue 移除任务，只有成功回调才移除；需要确认是否存在"FE 已切换阶段、队列里仍有旧 MAKE_SNAPSHOT 被重发"的窗口。建议在 `make_local_source_snapshot_and_load` 里加一道检查：暂存 tablet 已非空且版本 ≥ V_l 时拒绝再替换 |
| `append_increment` 非原子路径 | 若 VERIFYING 与 COMMITTING 之间本地分区被写（版本 3→4），追加同样命中版本检查并无限重试 | RESTORE 状态拒绝导入，正常不会发生；`checkKeptPartitions`（`:2536`）在提交时只查"保留"分区的版本，可以顺带把增量分区的 V_l 也查一遍，不一致直接失败而不是让 BE 报错 |
| DOWNLOAD 任务（`:2231-2360`） | 非瞬时错误（如仓库文件缺失）同样无限重发直到 24 小时超时；只有 `RESTORE_MANIFEST_MISMATCH` 一种已被识别为立即取消（`:437-441`） | 上游既有；(b) 的计数应覆盖 DOWNLOAD |
| `finishSnapshotTask`（`:396-409`）/`finishDirMoveTask`（`:599-603`）收到**已不在 `unfinishedSignatureToId` 里**的完成回报时返回 false | 返回 false 意味着 MasterImpl 不从队列移除；任务会被上报机制每 10 秒重发一次直到作业取消或超时。触发条件：BE 执行成功、FE 处理完成回报之前 BE 的任务上报恰好显示"未运行"而被重发（通用的双执行窗口）。对 MOVE，重发后 BE 靠 loaded tag 返回 OK（无害但刷日志）；对带本地来源的 MAKE_SNAPSHOT，重发会再做一次本地来源快照并替换暂存 tablet 目录——若此时作业已到 COMMITTING 且增量已追加，会把追加结果冲掉，再触发本次事故同类的版本错误 | 推测，未在日志里观察到。建议 "未知 signature 的完成回报" 一律返回 true 让队列移除（RESTORE_DIGEST 的 `finishRestoreDigestTask` 已这样做，`:415-422`），并在 `make_local_source_snapshot_and_load` 里拒绝替换已非空且版本 ≥ V_l 的暂存 tablet |
| RESTORE_DIGEST 任务 | 任务总是返回 OK，判定结果放在载荷里；另有 `restore_digest_timeout_s` | 无此风险 |
| `TabletScheduler` 对 RESTORE 表的 VERSION_INCOMPLETE 修复 | 除本次事故外，对"保留"分区、全量下载 tablet 版本一致，不触发；但 FE 重启后副本 `lastFailedVersion` 被持久化，恢复完成后 `sync` 会清掉（日志 23:18:45 已见） | c1 一并消除 |
| MOVE 线程池 1 线程（`agent_server.cpp:142`） | 不是 bug，但把竞争窗口放大到分钟级，也是 COMMITTING 占 40% 耗时的原因 | 若允许改配置，提升到可配并发会同时缩短 COMMITTING 和缩小窗口；需确认 `move`/`append_increment` 的锁范围允许不同 tablet 并行（都是 tablet 级锁，应当可以） |
| `handleRecoverTablet` 把 `lastFailedVersion` 设为 5 | 作业提交后分区到 4，`sync` 判 `lastFailedVersion > committedVersion` 重置为 -1 | 已观察到自愈，无需处理 |

## 6. 待决问题

1. 修复范围：是否接受 c1（RESTORE 表一律不修副本）这个对调度器的改动，还是只做 (a)+(b)。c1 改在上游通用代码上，回合到 branch-3.x 时需要单独评估。
2. (b) 的重试上限与错误码形式：新增错误码（改 thrift/`status.h`）还是用消息前缀；默认重试次数（建议 5 次，约 1 分钟）。
3. 是否顺带把 MOVE 线程数做成可配置（性能与窗口双收益，但属性能改动，不在这个 bug 范围内）。
4. 第 5 节第 2 行的"旧 MAKE_SNAPSHOT 重发冲掉已追加内容"是推测，需要读 `AgentTaskQueue` 在阶段切换时的清理逻辑确认；是否现在排查。

## 7. 本次未能做到的事

- 无法逐行比对 BE1 与 BE2 在卡住时的 tablet 内容（作业取消后暂存表与快照目录已清理，快照目录已不存在）；"clone 写入的内容正确"是基于同源 rowset 与一致的 dataSize/rowCount 的推断。
- 未在集群上做任何写操作或复现尝试（按约束）。
