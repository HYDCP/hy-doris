# Restore / CCR 全量同步的本地数据复用

目标是让 Restore 和 CCR 全量同步时，目标端已有且内容一致的数据不再重复下载。证明不了一致就照常下载，结果与完整下载相同；复用相关的能力默认关闭。注意"三个复用开关全关"不等于没有任何新行为：备份文件清单（Manifest）默认会生成，文件集合与大小校验默认开启。只有三个复用开关（`enable_restore_partition_reuse`、`enable_restore_incremental_append`、`enable_restore_atomic_reuse`）和 `enable_backup_manifest` 全部关闭，才回到旧行为（仅余常数字段和心跳能力位）。Manifest 默认开启的代价：BE 每个 tablet 多写一个小文件（数 KB），对象存储仓库每个 tablet 多一个对象，jobInfo 每个 tablet 多约 83 字节（`manifest_roots`），详见 [manifest-design.md](./manifest-design.md)。FE 配置 `enable_backup_manifest`（默认 true，可动态修改，仅 Master 生效）设为 false 后，BE 不写 Manifest，jobInfo 不写 `manifest_roots`，这样的备份在恢复时按"没有 Manifest"处理（不做文件集合校验，也不能增量补齐）。BE 只在 FE 明确要求时（快照与上传任务带 `write_manifest`）才写 Manifest、才算文件的 SHA-256；旧版本 FE 不带这个字段，新版本 BE 在它下面和原来完全一样，只列文件、只算 md5。新增的 thrift/proto 字段都用私有号段 7601 起，见 [field-numbers.md](./field-numbers.md)。

## 代码位置

本分支 `restore-r2`，基于 apache/doris master `14c957e048b`。

## 文档

| 文档 | 内容 |
| --- | --- |
| [overview.md](./overview.md) | 总体方案：目标与原则、地基 / R0 / R2 三部分的架构、决策门、路线与风险 |
| [manifest-design.md](./manifest-design.md) | 地基：版本化 Manifest、强摘要与备份完整性校验的设计 |
| [r0-observation-design.md](./r0-observation-design.md) | R0 观测：统计既有血缘复用的硬链接数、下载数与未匹配原因 |
| [r2-prototype-research.md](./r2-prototype-research.md) | R2 原型调研：现有校验和读取路径的代码核对，以及逻辑摘要的实现选型 |
| [r2-detailed-design.md](./r2-detailed-design.md) | R2 详细设计：分区级逻辑摘要复用、增量补齐（含 MoR 与带 DELETE 条件的 MoW）、原子恢复路径 |
| [incremental-digest-research.md](./incremental-digest-research.md) | 摘要增量化调研：按 rowset 缓存摘要并在 compaction 时继承，使摘要开销与变化量成正比 |
| [timing-experiment.md](./timing-experiment.md) | 端到端耗时实验设计，以及测试环境下已完成的摸底与补充实验结果 |
| [stuck-append-analysis.md](./stuck-append-analysis.md) | 原子恢复 + 增量补齐卡在 COMMITTING 的根因分析 |
| [stuck-append-fix-plan.md](./stuck-append-fix-plan.md) | 上述问题的修复方案（已两轮审查） |
| [speedup-design.md](./speedup-design.md) | 提速第一批：保留分区由快照任务装载到位（提交只做零 IO 确认）、状态机合并空拍、任务完成即运行 |
| [field-numbers.md](./field-numbers.md) | 新增 thrift/proto 字段的私有号段（7601 起）登记表 |

## 复用范围（首版）

- **复用 Duplicate 表；无 sequence 列的 MoW 表在 `restore_reuse_mow=true` 时也可复用。** MoW 表用算法版本 2 的摘要：对没有被 delete bitmap 标记的全部物理行求摘要，删除标记行也算（删除标记列按普通摘要列编码）。后续写入对已有数据只通过按 key 查找感知这些行，所以两端摘要相同，后续同一笔写入的结果（包括 `partial_update_new_key_behavior=ERROR` 的部分列更新是否报错）也相同。代价：两端 base compaction 进度不同、且分区里有删除标记行时，摘要不同，该分区整体下载。`restore_reuse_mow`（FE 配置，mutable，masterOnly）首版默认 false，原因记为 `HIDDEN_STATE_MOW`；备份侧不受它影响，始终为无 sequence 列的 MoW 表计算并记录版本 2 摘要。下面这些情况一律不复用：带 sequence 列（含 sequence mapping）的 MoW 表（`HIDDEN_STATE_MOW_SEQUENCE`，被 bitmap 标记但 sequence 值更大的行仍会让之后的写入落败，而它不在摘要里）；tablet 内存在带 DELETE 条件的 rowset（light delete，BE 判 NOT_SUPPORTED，base compaction 合并掉该 rowset 后恢复）；备份里算法版本 1 的 MoW 摘要（`HIDDEN_STATE_MOW_DIGEST_V1`，不受 `restore_reuse_allow_hidden_state_models` 影响，`restore_reuse_mow=false` 优先）。MoR 表仍整体下载（`HIDDEN_STATE_MOR`），`restore_reuse_allow_hidden_state_models`（默认 false）只对 MoR 有效，仅供实验，不保证一致。Duplicate 摘要保持版本 1，不变。
- **复用后的元数据是本地值。** 复用保留的是本地的数据文件，不是备份里的字节，所以下面这些元数据在复用后是本地集群自己的值，整体下载则带来源端的值，Duplicate 表同样如此：每行提交 TSO（`__DORIS_COMMIT_TSO_COL__`，被视为各集群自己的提交时间，按 TSO 范围做流式读取的结果可能因此不同）、rowset ID、版本列（`__DORIS_VERSION_COL__`）、binlog 与 CCR 相关的元数据。摘要不包含这些列。
- **等价性不含之后的 compaction 时机。** 复用保证的是：复用完成时两端状态相同，之后执行同样的写入序列，结果也相同。它不保证两端之后的 compaction 时机相同：复用保留的是本地的 rowset 布局，与整体下载得到的布局不同，同样的 compaction 策略覆盖的版本范围也可能不同。MoW 表的 base compaction 会清理删除标记行，所以复用之后，若两端 base compaction 覆盖的范围不同，部分列更新在 `partial_update_new_key_behavior=ERROR` 下可能一端报错、一端成功。这与原生多副本之间因 compaction 时机不同而出现的差异属于同一类。
- **默认每个复用的分区都有摘要证明。** `restore_reuse_default_check_level` 默认 `full`；`off`、`sample` 默认也按 `full` 执行，除非打开 `restore_reuse_allow_unverified_keep`（默认 false，仅供实验和观测，不提供一致性保证）。`SHOW RESTORE` 里同时显示请求的档位和实际生效的档位。
- **谱系戳带来源集群 ID。** 备份记录源端集群 ID，谱系戳随之记录，两边都有且不相等时不作为候选。集群 ID 是初始化时生成的随机整数，不是全局唯一标识，只用来缩小候选范围，不当作证明；任何一边缺失（旧备份、旧谱系戳）时仍可作为候选，由摘要兜底。
- **"查询结果相同"的含义。** 摘要证明的是在备份版本上的可见数据相同，不含隐藏列（如 `__DORIS_VERSION_COL__`，它随写入历史而不同，`show_hidden_columns=true` 时可见）。

## 容量边界

jobInfo 里每个 tablet 的数据（manifest root、逻辑摘要 root、是否有分解摘要）都存成与 `tablets_order` 对齐的数组，内存里也是数组，没有以 tablet id 为 key 的 map，也不为每个 tablet 常驻一个视图对象：`mra`（manifest root）、`lra`（逻辑摘要 root）、`lna`（无摘要原因）、`rdp`（每个 tablet 一个字符，`1` 表示它的 manifest 里有分解摘要），空串表示该 tablet 没有；schema_sig 与算法版本放在索引级的 `lds`、`lda`。分解摘要文件（rdigest）是 manifest 的一条记录（名字 `__rdigest__`，带大小和 SHA-256），BE 按 manifest 定位并校验它，jobInfo 不再记它的 root；`manifest_version` 由 1 升为 2，只认 1 的旧 FE 会把新备份当作没有 Manifest，自动不复用，不报错。新 FE 仍能读版本 1 的备份（`manifest_roots`、`ldr`、`ldn`、`rd`，以及更早的 `ld`），读入后转成数组；这类备份的分解摘要仍按 jobInfo 里的 root 取。没有 Manifest（`enable_backup_manifest=false`）时分解摘要无处记录，不计算，增量补齐不可用。

大规模下的控制项：

- `backup_logical_digest_max_tablets`（默认 0，不限制）：备份涉及的 tablet 数超过它时，备份不要求 BE 计算逻辑摘要，jobInfo 不写摘要，备份照常成功；原因（`TOO_MANY_TABLETS:<n>><limit>`）显示在 `SHOW BACKUP` 末尾的 `LogicalDigest` 列。
- `restore_reuse_max_tablets`（默认 0，不限制）：恢复所用备份的 tablet 数超过它时，本作业不复用任何分区，整体下载，`SHOW RESTORE` 的 `ReuseEstimate` 里记 `too_many_tablets`。
- 备份为 Duplicate 表和无 sequence 列的 MoW 表计算并记录逻辑摘要；带 sequence 列的 MoW 表没有摘要；MoR 表默认不计算（源端 FE 的 `restore_reuse_allow_hidden_state_models` 为 false 时），只有该配置在备份前打开才有。
- 恢复作业在摘要比对结束后释放摘要，下载完成后释放 Manifest 校验的中间集合与 Manifest root；分区的全部副本摘要到齐就立即判定并释放该分区的结果。

## 提速

设计与收益估算见 [speedup-design.md](./speedup-design.md)。三项都有开关：

- **保留分区由快照任务装载到位**（`restore_reuse_kept_load_at_snapshot`，FE，mutable，masterOnly，默认 true；只在 `enable_restore_atomic_reuse` 打开后有意义）。原子恢复里判为保留的分区，其暂存 tablet 原来由提交阶段的 MOVE 任务装载（改 rowset id、链接、重载），而 BE 的 MOVE 线程池每个 BE 只有一个线程。打开后，快照任务带 `restore_kept_load`，BE 像处理增量分区的本地底座一样，在快照阶段就把本地数据装进暂存 tablet（证据 `LOCAL_LOADED` 随 tablet meta 落盘），留下一个空的快照目录；提交阶段的 MOVE 请求不变，BE 看到空目录（目录里没有任何文件，普通恢复的快照目录一定有 tablet meta）就只核对本作业的装载证据，不做任何 IO；核对不过返回 `[RESTORE_NOT_RETRYABLE]`，FE 立即取消。旧版本 BE 忽略该字段，仍做带 tablet meta 的完整快照、由 MOVE 整体替换，结果正确只是慢；旧版本 FE 接管后发的普通 MOVE 在新 BE 上同样按目录内容处理。`SHOW RESTORE` 的 `DownloadStats.atomic_local.loaded_tablets` 是由快照任务装载的副本数。BE 配置 `restore_local_source_snapshot_timeout_s`（默认 86400，与恢复作业的默认超时相同）：本地来源快照目录的存活时间，BE 在装载后、释放前崩溃时由清扫线程按它清理；实际取它与作业超时中的较大者，清扫线程不知道装载是否在进行，这样跨盘的慢装载不会被中途清掉。保留分区的 MOVE 失败无论开关取值都按 `restore_local_source_task_max_retry` 收敛（超过次数取消作业），不再像原路径那样重发到作业超时。
- **状态机合并空拍**（`backup_restore_merge_idle_ticks`，FE，mutable，masterOnly，默认 true）。备份恢复作业每一拍（`backup_handler_update_interval_millis`，默认 3 秒）只推进一个状态；打开后，到达一个不等待 BE 的状态时在同一拍继续：keep_on_local 备份从快照完成一拍走到 FINISHED（省 4 拍），恢复在快照完成的那一拍发下载任务、在下载完成的那一拍发 MOVE（没有要下载的东西时从快照完成一拍走到 COMMITTING）。状态函数和它们写的编辑日志不变，顺序不变，FOLLOWER 回放不受影响；一拍最多走 5（备份）/4（恢复）个状态；失败仍在当拍结束。改的是 Doris 原有路径，默认关闭时逐字节等价于原行为。
- **任务完成即运行**（`backup_restore_run_on_task_finish`，FE，mutable，masterOnly，默认 true）。某个状态等待的最后一个 BE 任务回报时，作业请求 BackupHandler 立刻跑一次 `run()`（由 BackupHandler 的一个线程执行），省掉等下一拍的平均半拍。请求只记 (dbId, jobId)，执行前重新查找作业，只在本 FE 仍是 Master、该作业仍是库里当前作业且未结束时才运行。

## 当前状态

已实现：

- 谱系戳；
- 备份完整性校验；
- 复用观测；
- 分区级复用（逻辑摘要）；
- 增量补齐（实现上含 MoR，但首版按上面的范围，只对 Duplicate 表和无 sequence 列的 MoW 表生效，后者需要 `restore_reuse_mow=true`）；MoW 逻辑摘要版本 2；
- 原子恢复路径；
- 摘要缓存修复；
- 提速第一批（见下文"提速"一节）：保留分区由快照任务装载到位、状态机合并空拍、任务完成即运行。

**已修复**：原子恢复 + 增量补齐在多副本下，曾因副本修复竞争卡在 COMMITTING。根因和修复方案见 [stuck-append-analysis.md](./stuck-append-analysis.md) 和 [stuck-append-fix-plan.md](./stuck-append-fix-plan.md)。修复要点：

- 装载完成的证据写进 tablet meta（`restore_stage`），和作业绑定；
- 运行中的作业登记了本地来源分区时，调度器不对这些分区做副本修复，与表的状态无关；
- 提交时校正本地来源副本的版本；
- 本地来源任务的失败按次数收敛，不可重试的错误带前缀 `[RESTORE_NOT_RETRYABLE]`，FE 收到后立即取消作业。

3 万 tablet 规模的验证中，没有作业卡住。

测试环境的实测结论：灾备来回切换演练中，下载量减少约 71%，错误复用为 0；在下载较慢的单机测试环境、数据量较大时，端到端耗时加速约 2～3 倍，数据量较小（约 2 GB）时因固定开销占大头只有约 1.2～1.6 倍。目标规模和万兆网络下的收益仍需复测。

## 部署与回滚

先说明保护机制，再说步骤。

**自动避开混跑。** FE 和 BE 在心跳里上报复用能力（`restore_reuse_capability`，本版本上报 3，旧版本不带这个字段，按 0 处理；3 只表示支持保留分区由快照任务装载，不被任何判定要求，能力为 2 的 BE 由 MOVE 装载，结果相同）。恢复作业开始判定复用时，检查所有 FOLLOWER FE（Master 自己算，Observer 不会接管作业，不检查）和所有存活的 BE：Duplicate 表要求能力至少为 1，MoW 表要求至少为 2（版本 2 摘要）。任何一个节点能力低于要求，或者主节点还没收到它的心跳，相应的表（能力低于 1 时是整个作业）就不复用，整体下载，不下发摘要任务、本地来源快照和增量补齐，并在 `SHOW RESTORE` 的 `ReuseEstimate` 里记录原因 `MIXED_VERSIONS`。开关是 FE 配置 `restore_reuse_require_capability`（可动态修改，默认 true）；关掉它等于不检查，只用于实验。复用开关关闭时，`ReuseEstimate` 里也可能显示 `MIXED_VERSIONS`，那只是估算，作业本身没有走复用。FE 切主或重启后，首轮心跳（约 5 秒）之前开始的作业，因为还没收到各节点的能力，会整体下载。这个检查只在作业开始时做一次，作业进行中发生的升级、降级或切主，由下面的文件层保护和步骤兜底。

**文件层保护。**

- 只含增量的快照，其元数据文件命名为 `<tablet_id>.hdr.increment`，目录里没有 `<tablet_id>.hdr`。旧版本 BE 的整体替换找不到 `.hdr` 会直接失败，不会把增量当成完整快照去替换 tablet；新版本 BE 的整体替换发现 `.hdr.increment` 同样拒绝，返回不可重试错误，tablet 不变。
- 快照与备份清单不符，BE 以 `INTERNAL_ERROR` 回报，错误信息带前缀 `[RESTORE_MANIFEST_MISMATCH]`，FE 按前缀识别并立即取消作业。没有新增 thrift 状态码，旧 FE 收到的是它认识的错误，不会出错。

**升级。**

1. 先升级所有 BE，再升级所有 FE。MoW 表的复用需要所有节点能力都不小于 2：升级期间有旧节点时，MoW 表整体下载，Duplicate 表照常复用。旧版本备份里没有 MoW 的版本 2 摘要，新版本恢复时同样整体下载，需要用新版本重新备份。升级期间保持复用开关关闭（`enable_restore_partition_reuse`、`enable_restore_incremental_append`、`enable_restore_atomic_reuse`）。
2. 全部节点升级完成后，确认心跳已经收到（`restore_reuse_require_capability` 为 true 时，作业会自己判断），再打开复用开关。
3. 升级和回滚期间，不要用 `logical_digest=true` 做备份。新版本 BE 为这样的备份写出版本 2 的 manifest（多一条分解摘要），旧版本 BE 不认识版本 2，按 manifest 不符处理，连普通恢复也会失败。默认属性（`logical_digest=false`）的备份不受影响。如果已经有这样的备份、又必须在旧版本 BE 上恢复，可以临时把 BE 配置 `restore_manifest_check` 设为 false。
4. 新 FE 加旧 BE 时，带 `logical_digest=true` 的备份会被 FE 当作 manifest 里有分解摘要，而旧 BE 并没有写进去；恢复时这类分区的增量补齐会报错并回退为整体下载，结果正确，只是少了复用。全部 BE 升级后不再出现。
5. 保留分区的快照装载（`restore_reuse_kept_load_at_snapshot`）不需要等所有节点升级：能力低于 3 的 BE 忽略该字段，仍由 MOVE 装载。作业进行中降级 BE 的情况和其他本地来源任务一样：先按回滚步骤排空在途作业。`backup_restore_merge_idle_ticks` 与 `backup_restore_run_on_task_finish` 只影响 Master FE 的调度，随时可开关，不涉及持久化。

**回滚。**

1. 先关闭上面三个复用开关。
2. 等待或取消所有在途的恢复作业：用 `SHOW RESTORE` 检查，确认没有处于 PENDING 到 COMMITTING 之间的作业，必要时 `CANCEL RESTORE`。关闭开关不会撤销进行中作业已经做出的复用决定，所以这一步不能省。
3. 先回滚 FE，再回滚 BE。
4. 回滚可能丢失谱系戳：旧版本 FE 写镜像或者重写包含分区、表的编辑日志时，会丢掉它不认识的字段（谱系戳、来源等）。丢失是安全的，只是失去复用依据；再次升级后的第一个恢复作业会整体下载，之后重新建立谱系戳。BE 上的 `restore_stage` 可能被旧版本保留，也可能丢失，二者都没有害处：判定时会核对作业 ID 和基础版本，证据丢失时作业被取消。

**作业期间意外切到旧 FE。** 旧 FE 不认识复用相关的字段，会把增量作业当作普通恢复继续下发整体替换。此时由文件层保护兜底：BE 找不到 `.hdr` 会让任务失败，不会冲掉本地数据。旧 FE 会一直重发任务直到作业超时，所以发现后应该手动 `CANCEL RESTORE`；原子恢复下原表不受影响，非原子恢复按“重新恢复一次”处理。保留分区那一侧不会出现数据错误，只是没有按预期保留。升级后重新恢复一次即可。
