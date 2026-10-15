# 增量追加卡住：修复方案（第 3 版，已两轮审查）

> 根因分析见[增量追加卡住问题分析](./stuck-append-analysis.md)。代码基于 `restore-r2` HEAD `9b9149a12ff`。经两轮审查后修订：第 1 轮见第 5 节，第 2 轮（对抗式审查）见第 6 节。**第 6 节的修订优先于正文中与之冲突的内容。**

## 0. 原则

**功能关闭时，Doris 原有行为完全不变。** 这是硬性要求，也是审查和测试的第一项。

- 只改我们新增的代码路径：增量追加、本地来源快照、复用判断。
- 必须改 Doris 原有代码时（调度器），只加一个判断。这个判断只在表处于 RESTORE 状态、并且命中本次恢复的增量决定时才生效。
- Doris 原有任务的失败和重试行为不变，包括整体下载、整体移动和普通快照。

## 1. 根因回顾

1. 原子恢复为增量分区建暂存副本时，FE 把副本版本记成备份版本 V_b（`RestoreJob.java:1845`）。BE 先用本地来源快照装到 V_l，等追加完成才到 V_b。
2. 调度器认为这些副本"缺版本"。RESTORE 状态下，调度器对 `VERSION_INCOMPLETE` 例外放行修复（`TabletScheduler.java:646-656`），于是向 BE1 发起克隆，把 BE1 补到 V_b。
3. BE1 排队中的追加任务随后才执行。前置检查（`snapshot_loader.cpp:2572`）发现本地已是 V_b，于是报错。
4. FE 对 DirMove 失败只记录错误、返回 false（`RestoreJob.java:593-618`），任务每 10 秒重发一次，直到 24 小时后作业超时。

## 2. 修复内容

### (a) BE：追加可以安全重做（只改我们的代码）

位置在 `SnapshotLoader::append_increment`。新的检查放在 `convert_rowset_ids`（`:2518`）**之前**，避免每次重试都重写快照 hdr、给文件改名。

1. `max_version == V_l`：照旧追加。
2. `max_version == V_b`：判断本地数据是否已经等于备份。
   - **快路径**：本地 (V_l, V_b] 的 rowset 与快照 hdr 一一对应，比对起止版本、行数、段数、数据字节数，以及是否带 delete predicate。全部相同就视为已追加：写 loaded tag，记 `increment already present` 日志，返回 OK。
   - **慢路径**：rowset 对不上时不直接判失败。暂存 tablet 在 BE 上是普通 tablet，克隆之后可能被 compaction 合并，rowset 的划分会变，但数据并没有错。这时用 `compute_tablet_restore_digest(engine, tablet_id, V_b)`（`restore_digest.h:144`）算出本地在 V_b 的逻辑摘要，与备份记录的该 tablet 整体摘要比较：
     - 摘要相等：视为已追加，返回 OK；
     - 摘要不等、备份没有摘要、或表结构不支持摘要：返回**不可重试错误**。
   - 备份的整体摘要由 FE 下发：在 `commit()`（`RestoreJob.java:2500` 附近）构造 DirMoveTask 时，把 `BackupTabletInfo.logicalDigest`（`BackupJobInfo.java:453`）填进 `TMoveDirReq` 新增的 optional 字段。旧 FE 不填这个字段，BE 收到后只走快路径，快路径不通过就返回不可重试错误。
3. 其他版本：返回不可重试错误。

**为什么摘要比对足够**：逻辑摘要描述的是可见的行，与 rowset 怎么划分无关。MoW 的 delete bitmap、MoR 的合并、DELETE 条件、sequence 列，最终都体现在可见行上，所以都能覆盖。错误复用的风险与整个 R2 复用判断相同，靠的是同一套算法。

**单测**：
- 快路径：已到 V_b 且 rowset 相同时返回 OK。Duplicate、MoW 各一例，MoW 还要校验 bitmap 不变。
- 慢路径：已到 V_b，但 rowset 被合并过，摘要相等时返回 OK；行数不同、摘要不等时返回不可重试错误，tablet 保持不变。
- 没有备份摘要，且快路径不通过时，返回不可重试错误。
- 第一次在检查处失败后重入，行为一致，hdr 和文件没有被改动。

### (b) FE：只对增量追加任务设失败上限

**范围**：只在 `DirMoveTask.getIncrementalRange() != null` 时生效（`DirMoveTask.java:60-66`）。这个字段只在增量分区下发时设置（`RestoreJob.java:2491-2494`），任务对象在任务队列中复用，重发不会丢。整体移动任务（这个字段为 null）的处理完全不变。

1. **错误码**：新增 `RESTORE_INCREMENT_STATE_MISMATCH`，照 `RESTORE_MANIFEST_MISMATCH` 的做法加到 `Status.thrift`（`:118` 附近）和 `status.h`（`:82` 附近）。`to_thrift` 对大于 0 的错误码会原样传递（`status.cpp:69`）。
2. **FE 处理**（`RestoreJob.finishDirMoveTask`）：
   - 收到 `RESTORE_INCREMENT_STATE_MISMATCH`：设置作业 status，立即 `cancelInternal(false)`，与现有的 `handleManifestMismatch`（`:527-541`）一致。`cancelInternal` 在 COMMITTING 分支会清掉所有 MOVE 任务（`:3805-3808`）。
   - 其他错误：直接用 `task.getFailedTimes()`（`AgentTask.java:120`；`MasterImpl.finishTask` 每次收到非 OK 回报都会调用 `task.failed()`，`MasterImpl.java:158`），不再另建计数。超过 `restore_incremental_append_max_retry`（新增 FE 配置，mutable，默认 10）就取消，未超过则保持原有的重发行为。
   - 注意：MOVE 线程池只有一个线程（`agent_server.cpp:142`），每次重试都要排队。3 万 tablet 规模下，重试 10 次可能要几十分钟，而不是 100 秒。这仍然远好于卡 24 小时，提交说明里要写明。
3. **取消后的效果**：
   - 原子路径：只删除暂存表，原表不受影响。
   - 非原子路径：与现有行为一致，已有的取消信息会提示"再恢复一次即可修复"。
4. **单测**：
   - 收到不可重试错误后，作业立即 CANCELLED；原子路径下原表仍在，暂存表已清理。
   - 可重试错误出现若干次后成功，作业继续。
   - 超过重试上限后取消。
   - **整体移动任务连续失败超过上限，仍不取消、继续重发**，以此证明原有行为没有改变。

### (c) 窄版：调度器跳过等待追加的暂存分区（Doris 原有代码，只加一个判断）

**不另建登记集合**，直接查询当前的恢复作业：

- 在 `TabletScheduler.scheduleTablet`（`:646` 附近），只有当 `tableState == RESTORE` 时才进入新判断。
- 判断内容：`BackupHandler.getCurrentJob(dbId)` 是一个未结束的 RestoreJob，并且它的 `reuseResult` 里有这样一个决定：`atomic && incremental && stagingTableId == tbl.getId() && stagingPartitionId == partition.getId()`。满足时抛出 `UNRECOVERABLE/DIAGNOSE_IGNORE`，原因写 "partition is waiting for the restore increment"。

这样做的好处：

- `reuseResult` 本来就随作业持久化（`RestoreJob.java:243` 的 gson 字段 `rru`）。作业 FINISHED 或 CANCELLED 后，`isDone()` 为真，判断自然失效。
- FE 重启、切主、回放取消、回放到终态，都不需要额外维护，不存在"tablet 永远不被修复"的风险。
- 功能关闭，或者表不在 RESTORE 状态时，不进入判断，调度器行为与原来一致。
- 放在 `scheduleTablet` 即可。`TabletChecker` 只看分区状态（`TabletChecker.java:360`），不需要改。

需要确认的点：Decision 里是否已有 `stagingTableId` 和 `stagingPartitionId`。如果没有，就在原子路径建暂存表时补上，作为持久化字段。

**单测**：
- 命中的暂存分区，`VERSION_INCOMPLETE` 修复被拒绝；
- 同一张表里的其他分区、其他表，行为不变；
- 作业结束后不再拒绝；
- 回放后判断照样生效。

### (d) 防止重发的本地来源快照冲掉已装载的内容（只改我们的代码）

第 1 版"按版本判断已装载"的做法是错的。暂存 tablet 刚创建时，BE 会先建一个空的 `[0, V_b]` 初始 rowset（FE 传入的版本就是 V_b，`RestoreJob.java:1732`、`tablet.cpp:1968`）。所以首次装载前，tablet 就已经"有内容且最大版本是 V_b"，按版本判断会跳过首次装载，恢复出一张空表。

**改为用标记文件判断**：

- `make_local_source_snapshot_and_load`（`snapshot_loader.cpp:2454`）在 `move(...)` 成功后，在 tablet 目录写入标记文件 `.restore_local_source.<job_id>`。
- 入口先检查标记文件：
  - 有标记：说明已经装载过，不再替换，只重做后续需要的快照步骤，返回 OK。
  - 没有标记：照旧替换。
- 重复回报的 OK 会在 `MasterImpl.finishTask` 里因为找不到任务而被丢弃，不会掩盖问题。

这个重发窗口是真实存在的：BE 上报某个任务"没在运行"，FE 就重发；第二份任务排进 BE 的长队列，很晚才执行。标记文件可以封住这个窗口。

### (e) 提交前复查：已经实现，删除

`checkKeptPartitions`（`RestoreJob.java:3412-3489`）已经把增量分区（`getIncrementalDecisions()`）纳入复查并校验版本，`commit()` 和 `allTabletCommitted()` 都会调用它（`:2479`、`:2536`）。

## 3. 回归用例

1. **新用例 `test_backup_restore_atomic_incremental_race`**：
   - 必须 3 副本、3 个 BE。现有流水线默认单副本（`force_olap_table_replication_allocation=tag.location.default:1`），所以用例里显式设 `replication_num=3`，BE 少于 3 个时跳过。
   - 新增 BE 调试点 `SnapshotLoader.append_increment.delay`：按 tablet 或 BE 延迟，放在追加取锁之前。`snapshot_loader.cpp` 目前没有调试点，需要新增。
   - BE 的 `report_tablet_interval_seconds` 可以在用例里调小（mutable）。FE 的 `tablet_checker_interval_ms` 不可在运行时修改，只能改流水线的 `fe.conf`，或者沿用默认值。
   - 延迟时间必须大于"检查器间隔 + 上报间隔"：MOVE 完成后 BE 不会主动上报 tablet，FE 要等周期上报才能同步到 V_b。
   - 期望：修复后作业 FINISHED，三副本数据与源端一致，恢复期间该表没有发生克隆；去掉 (c) 后可以复现，作业依靠 (a) 仍能 FINISHED。
   - Duplicate、MoW、MoR + DELETE 条件各跑一遍。
2. **注入追加失败**（已有调试点 `Tablet.revise_tablet_meta_fail`，`tablet.cpp:431`）：作业在有限次重试后 CANCELLED，原表数据不变。
3. **原有行为不变**：
   - 复用开关全关，跑 `backup_restore` 目录全部 64 个用例，结果与修改前一致；
   - 对整体移动任务注入失败，确认仍按原方式重发、不会被取消。
4. **测试机验证**：重跑 3 万 tablet 的 full 档，确认不再卡住。

## 4. 原有行为核对（审查确认）

- (a) 和 (d) 只在增量追加、本地来源快照这两个分支里生效；
- (b) 只在 `getIncrementalRange() != null` 时生效；
- (c) 只在 `tableState == RESTORE` 并且命中本次恢复的增量决定时生效；
- 整体下载、整体移动、普通快照的失败重发路径都没有改动。

## 5. 第 1 版到第 2 版的修订（审查）

| 级别 | 问题 | 修订 |
| --- | --- | --- |
| 严重 | (d) 按版本判断"已装载"会跳过首次装载，恢复出空表 | 改为用标记文件判断 |
| 严重 | (e) 已经实现 | 删除 |
| 严重 | (a) 一一比对 rowset 经不起 compaction，数据正确也会被判失败 | 增加逻辑摘要慢路径；检查移到 `convert_rowset_ids` 之前 |
| 中等 | (c) 登记集合的生命周期难维护，可能导致 tablet 永远不被修复 | 改为直接查询当前作业的 `reuseResult` |
| 中等 | (b) 识别方式和计数 | 用 `getIncrementalRange()` 识别，用 `getFailedTimes()` 计数，并立即取消 |
| 低 | 回归环境的约束 | 3 副本 / 3 BE；检查器间隔不能在运行时修改；延迟时间要足够长 |

## 6. 第 2 版到第 3 版的修订（第 2 轮对抗式审查）

关键前提都已对照代码核实。

### 严重

1. **(a) 慢路径会自死锁（已核实）。** `append_increment` 在版本检查处已持有 `std::lock_guard wrlock(tablet->get_header_lock())`（`snapshot_loader.cpp:2569`），而 `compute_tablet_restore_digest` 内部的 `prepare_tablet_input` 会再取同一把锁的共享锁（`restore_digest.cpp:1311`）。shared_mutex 不可重入，唯一的 MOVE 线程会永久挂起。
   - **改法**：版本判断和摘要计算都放在取 header 写锁**之前**。这时已经通过 try-lock 拿到了 compaction、push、rowset_update 等锁（`:2560-2567`），rowset 集合不会再变。快路径和慢路径都在这里完成：判定为"已追加"就直接返回 OK，不再取写锁；只有 `max_version == V_l` 时才进入原有的写锁区。
   - 写锁区里原有的版本检查保留，作为最后一道防线。

2. **(c) 是必需项，并且必须覆盖整个 SNAPSHOTING 阶段（推测，代码链条完整）。** 各 BE 执行本地来源装载的时间不同。假如 BE1 已装到 V_l，BE2 还没装载，仍上报空的初始 rowset `[0, V_b]`，FE 会把 BE2 当作版本齐全的克隆源（`TabletSchedCtx.java:716-731`）。BE2 没有 `[V_b, V_b]`，快照退化为全量（`snapshot_manager.cpp:623-632`），全量克隆会删掉 BE1 已装载的数据（`engine_clone_task.cpp:926-990`），暂存副本变成空的 V_b。之后 (a) 判定不通过，作业被取消。不会提交错误数据，但作业会失败。
   - **改法**：
     - (c) 从"建议"改为**必需**，删掉回归用例中"去掉 (c) 后依靠 (a) 仍能 FINISHED"这一条期望；
     - (c) 从 VERIFYING 产生决定起就生效，`reuseResult` 在 SNAPSHOTING 之前已一次性赋值（`RestoreJob.java:3367`），满足这个要求；
     - 回归用例增加"MAKE_SNAPSHOT 跨 BE 延迟"变体。

### 中等

3. **新错误码与旧 FE 混跑会退化成原来的卡死。** 旧 FE 不认识新的状态码，Java thrift 反序列化得到 null，`MasterImpl.java:160` 处会抛 NPE，任务留在队列里被无限重发。现有的 `RESTORE_MANIFEST_MISMATCH` 也有同样的问题。
   - **改法**：BE 只有在 `TMoveDirReq` 带了新的摘要字段时，才返回新错误码，因为带这个字段说明 FE 是新版本；否则返回普通的 InternalError。

4. **(d) 改用 meta 判据，不再用标记文件。** 标记文件在存储迁移时会丢失，也识别不出"已装载后又被全量克隆冲掉"的情况。
   - **改法**：只有当暂存 tablet **只有一个 0 段的初始 rowset `[0, V_b]`** 时才允许替换，否则跳过并返回 OK。这个判据在 BE 重启、迁移、GC 之后都成立，也不会留下残留文件。
   - 前面第 2 条那种"装载后被冲成空的 V_b"，克隆出来的是带数据段的 rowset，不会被误认为初始 rowset，所以不会再次替换。这种情况交给 (a) 判定，作业失败。

5. **FE 在 COMMITTING 阶段重启：增量分区可能少一段数据（已核实）。** `unfinishedSignatureToId` 不持久化（`AbstractJob.java:89`）。`replayRun` 只处理 DOWNLOAD 和 FINISHED（`RestoreJob.java:621-633`）。COMMITTING 不写编辑日志，重启后（或切主后）作业回放到的最后状态是 COMMIT，会重新下发移动任务；所以要求重发的移动任务可以安全重做。如果仍然在 COMMITTING 状态下运行而 `unfinishedSignatureToId` 为空，`waitingAllTabletsCommitted`（`:2515-2523`）会立即提交，这是需要避免的路径，以下修复针对它。Doris 原有的整体移动也有这个问题，后果是"目录没有替换"；增量路径的后果是"少追加一段数据"。
   - **改法（只对有增量决定的作业）**：用一个不持久化的标记，记录"本进程是否已下发过 COMMITTING 的移动任务"。如果作业处于 COMMITTING、有增量决定、但本进程还没下发过，就退回 COMMIT，重新下发移动任务。重发是安全的：已完成的追加由 loaded tag 和 (a) 判定为已存在，直接返回 OK。
   - 没有增量决定的作业保持原有行为。

6. **(c) 的并发与代价。** 调度器在表写锁内调用 `getCurrentJob`；`RestoreJob.run()` 是 synchronized，并且会在内部取表锁（`:678`）。
   - **约束**：调度器**不得调用 RestoreJob 的任何 synchronized 方法**，只读 `reuseResult` 字段并调用 `getIncrementalDecisions()`（`RestoreReuseResult.java:211`，不是 synchronized）。`reuseResult` 加上 volatile。
   - **代价**：只在 `tabletHealth.status == VERSION_INCOMPLETE && tableState == RESTORE` 时才查，平时不进入。
   - `RESTORE_WITH_LOAD` 不用处理：允许导入时，复用判断会直接拒绝（`RestoreReuseJudge.java:312-326`），不会产生增量决定。

### 低

7. **(a) 比较时必须同时核对** `algo_version`、`schema_sig`，以及 `status_code == OK`。缺任何一项都按"没有备份摘要"处理。MoR 的 boundaries 配置只影响分解摘要，不影响整体摘要。
8. **(a) 读快照 hdr 必须在 `convert_rowset_ids` 之前**，用原始 hdr。
9. **(b) 计数不包含 `ObtainLockFailed`。** 拿不到锁是正常的竞争，不计入重试次数。取消前还要再校验一次 `state == COMMITTING`。
10. 已核对、无需修改的点：
    - FE 能在 `commit()` 里通过 `fileMapping` 拿到对应备份 tablet 的摘要，并且 jobInfo 随作业持久化，FE 重启后仍可用；
    - MoW bitmap 一致的论证成立；
    - 副本之间 rowset id 不同是 Doris 的常态，没有影响；
    - 非原子路径取消后部分分区已追加，这是既有行为，(b) 只是让它更快结束，不新增风险。
