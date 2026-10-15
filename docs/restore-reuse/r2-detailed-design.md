# R2 详细设计：分区级逻辑等价复用接入恢复流程

> 原型结论见内部实测记录，判定分层见[总体方案](./overview.md)第 5 章。**目标场景是灾备的来回切换**（A→B 同步，切到 B，再切回 A），见第 11 节。

## 1. 目标与边界

恢复时，本地已有某个分区、并且能证明它和备份里的这个分区逻辑上相同，就**整个分区保留本地数据**，跳过这个分区的快照、下载和文件移动；证明不了就照常下载。

首版边界（不变）：
- 只走**非原子恢复**中"分区已存在、range 一致"的路径（A1 分支）；原子恢复放到二期；
- 支持 Duplicate、Unique MoW、Unique MoR（含 sequence 列）；Aggregate 和摘要返回 NOT_SUPPORTED 的情况一律下载；
- **2026-10-14 收窄：首版只复用 Duplicate 表，Unique 表默认整体下载**，见第 16 节，以第 16 节为准；
- 不改变恢复的最终结果：保留下来的分区，数据、版本、tablet meta 都要和完整下载一致（总体方案 2.2 的 I6）。

## 2. 总体流程

```text
备份侧                                       恢复侧（非原子、A1 分支）
────────                                     ──────────────────────────
做快照（版本 V）                               checkAndPrepareMeta：
  └─ 按备份属性，同时计算逻辑摘要                 ① 表状态置为 RESTORE（已有逻辑，禁写）
     结果随快照报告带回 FE                         ② 对每个分区判 L0（已有：谱系戳 + 版本）
jobInfo 每个 tablet 记：                          ③ 按启用条件和档位选出候选分区
  algo_version、schema_sig、root                  ④ 对候选分区的每个本地副本下发摘要任务（新状态 VERIFYING）
                                                  ⑤ 逐 tablet 比对：本地每个副本的 root 都等于备份的 root
                                                     → 整个分区判为"保留"，否则"下载"
                                                  ⑥ 保留的分区：不建快照任务、不下载、不移动，
                                                     也不进 restoredVersionInfo
                                               提交前：持表写锁复查保留分区的 visibleVersion
```

## 3. 备份侧：摘要的产生

- **触发**：备份属性 `logical_digest = "true"` 时才算（默认关）。ccr-syncer 可以给全量同步的备份加这个属性。
- **在哪算**：快照任务里，快照做完后，对 live tablet 的 (0, V] 计算。快照本身就是这些 rowset 的硬链接，读的是同一份数据。复用 `TSnapshotRequest`，新增可选字段 `compute_logical_digest`，不新增任务类型。
- **报告**：快照报告新增 `logical_digest {algo_version, schema_sig, root, rows, status}`。NOT_SUPPORTED 或计算失败时，status 带原因，root 为空。
- **记录**：jobInfo 按 tablet 记 `algo_version`、`schema_sig`、`root`，约 150 字节/tablet，10 万个 tablet 约 15 MB。
- **缓存**：同一个 tablet 的同一个版本，摘要不会变。BE 按 `(tablet_id, version, algo_version, schema_sig)` 缓存摘要，冷分区只需要算一次。首版用内存 LRU，BE 重启后失效；以后视需要再持久化。
- **成本**：首次全量要把整个快照扫一遍，单 BE 多线程约 1.1～1.4 GB/s（冷盘），MoR 表只能单线程，约 130 MB/s。因为有缓存，稳态下只有新写入的分区需要算。

## 4. 恢复侧：判定

### 4.1 候选分区（便宜的条件先判）

1. 走非原子 A1 分支，并且表状态为 `RESTORE`。表状态是 `RESTORE_WITH_LOAD`（恢复期间允许导入）时不启用 R2，因为版本可能在判定后变化；
2. L0 通过：现有的 `checkL0`，即谱系戳与这次备份同源、本地版本 == 戳版本 == 备份版本、commitSeq 不倒退；
3. 不属于不支持的情况：Aggregate 表、数据冷却到远端存储、云模式；
4. 备份里这个分区的每个 tablet 都有摘要；
5. 分区大小 ≥ `restore_reuse_min_partition_bytes`（FE 配置）。原来默认 1 GB，理由是小分区直接下载更省事。2026-10-10 改为默认 0，即不按大小过滤（提交 `03019208ed5`）。原因有两条：一是按天分区的表，单个分区常常不到 1 GB，用 1 GB 的默认值几乎不会复用；二是小分区的摘要校验开销很小，600 个几 MB 的分区合计只多 10～33 秒（见耗时实验设计 2.4 节）。

### 4.2 校验档位

恢复属性 `reuse_check_level`，FE 配置 `restore_reuse_default_check_level` 提供默认值。

| 档位 | 做法 | 适用 |
| --- | --- | --- |
| `off` | 只凭 L0 判定，不算摘要 | 只允许非 Unique 表，并且分区是由 Restore 建出来的（谱系戳存在即满足）。Unique 表一律不允许 |
| `sample` | 候选分区里抽一部分（`restore_reuse_sample_ratio`，默认 10%，至少 1 个）算摘要。只要有一个抽中的分区不一致，其余候选分区全部升级为 `full` | 原默认；2026-10-14 起默认按 `full` 执行（第 16 节） |
| `full` | 所有候选分区都算摘要 | **默认**（2026-10-14 起） |

### 4.3 摘要任务

- 新增 agent task 类型 `RESTORE_DIGEST`（不复用一致性检查的任务，原因见 `R2原型调研.md` 第 4 节），请求是 `(tablet_id, version, threads)`，回报的内容和备份侧一样。
- 对候选分区里每个 tablet 的**每个本地副本**都要算。MoR + sequence 表的副本之间本来就可能不一致（内部实测记录），只核一个副本不够。
- 并发和限流：每个 BE 同时运行的摘要任务数由 `restore_digest_task_concurrency` 控制（默认 2），每个任务的线程数由 `restore_digest_threads` 控制（默认 4）。MoR 只能单线程，排在最后。
- 超时：整体受恢复任务的超时约束；另设 `restore_digest_timeout_s`（默认 3600）。超时的分区按"下载"处理，不让整个恢复失败。

### 4.4 比对

- 一个 tablet 判为一致的条件：`algo_version`、`schema_sig` 与备份相同，并且本地**每个**副本的 root 都等于备份的 root；
- 分区里**所有** tablet 一致，分区才判为"保留"；有一个不一致，整个分区下载；
- tablet 的对应关系沿用现有恢复的绑定方式（按 tablet 在 index 里的顺序）；
- 结果持久化在 RestoreJob 里（保留的分区列表），随 VERIFYING 结束时的编辑日志写出，回放时照此执行。

## 5. 状态机

在 `PENDING` 与 `SNAPSHOTING` 之间新增 `VERIFYING`：

```text
PENDING ──checkAndPrepareMeta──► VERIFYING ──全部摘要返回或超时──► SNAPSHOTING ──► ...（原流程）
                                    │
                                    └─ 没有候选分区时直接跳过
```

- 进入 VERIFYING 时，表已经是 RESTORE 状态，禁止写入，所以版本是稳定的；
- 保留的分区不生成快照任务、下载任务和移动任务；
- 谱系戳：保留的分区在提交时**仍然重写戳**，等价于这个分区刚刚被这次恢复写过，下一次恢复照常判 L0。覆盖前先作废戳的逻辑只作用于真正要覆盖的分区；
- 取消：VERIFYING 阶段取消时，本地数据一点没动，直接结束即可。

## 6. 提交前复查

在 `allTabletCommitted` 里，持表写锁，复查每个保留分区的 `visibleVersion` 仍然等于备份版本。不等就说明有不该发生的写入（理论上 RESTORE 状态已经禁写），恢复任务失败并给出明确原因，**不能静默提交**。

## 7. 可观测

- `ReuseEstimate` 列：在影子统计的基础上，增加实际结果：`kept_partitions`、`kept_bytes_single_replica`，以及因摘要不一致、超时、不支持而改为下载的分区数；
- `DownloadStats` 列：增加 `kept_bytes`（保留分区的本地数据量，按副本计），`reuse_ratio` 改为 (linked + skipped + kept) / 总量；
- FE 日志：每个分区的判定结果和原因。

## 8. 开关与兼容

- FE 配置 `enable_restore_partition_reuse`，**默认 false**。关闭时行为和今天完全一样，影子统计照旧；
- 恢复属性 `reuse_check_level` 可以按任务覆盖默认档位，也可以设为 `disable`；
- 新 FE + 旧 BE：摘要任务失败或不支持，按"下载"处理；
- 旧备份（没有摘要）：只能用 `off` 档，所以 Unique 表一律下载；
- 新备份被旧 FE 恢复：旧 FE 忽略摘要字段，照常下载。

## 9. 改动范围

| 部分 | 内容 |
| --- | --- |
| BE | 快照任务内算摘要并上报；摘要缓存；`RESTORE_DIGEST` 任务与线程池；限流 |
| thrift | 快照请求和报告的新字段；新任务类型 |
| FE 备份侧 | `logical_digest` 属性；jobInfo 记录摘要 |
| FE 恢复侧 | 候选筛选、档位、VERIFYING 状态、任务下发与汇总、比对、保留分区跳过三类任务、提交前复查、回放 |
| 可观测 | `ReuseEstimate`、`DownloadStats` 扩展，日志 |
| 测试 | 单测；回归（保留分区与完整下载结果一致、改一行后必须下载、各档位、取消、FE 切主、BE 重启）；故障注入 |


## 10. 已定（2026-10-03）

1. 备份侧：备份属性 `logical_digest = "true"` 时才算摘要，默认关，由 syncer 或用户开启。
2. 摘要缓存：首版只放内存，BE 重启后重算。
3. 默认档位原为 `sample`（抽 10%，至少 1 个）；Unique 表不允许 `off`。**2026-10-14 改为默认 `full`，见第 16 节。**
4. 提交前复查版本失败：恢复任务失败，给出明确原因。
5. 最小分区阈值：1 GB。

## 11. 灾备来回切换（2026-10-03 补充）

目标：A→B 同步、切到 B、再切回 A（B→A 全量同步）的整个过程中，没有变化的分区都不重新下载。按第 4.1 节的 L0（只认"本地分区上有指向这次备份来源的谱系戳"），以下两种情况会被直接挡掉，所以要扩展。

### 11.1 两个缺口

1. **切回（反向）**：A 的分区是 A 自己的原始数据，没有谱系戳；B 的分区才带着指向 A 的戳。B→A 同步时，A 本地分区 L0 判为 `no_lineage`。
2. **增量同步之后的全量**：A→B 增量同步期间，B 的分区版本随增量推进，而戳停在上一次全量的版本，于是 L0 判为 `local_version_changed`（内部实测记录）。灾备的常态正是"增量一直在跑，偶尔全量"。

### 11.2 扩展后的 L0（只在 `sample` / `full` 档位下生效）

L0 是挑候选的便宜过滤，真正的证明是摘要。所以在要算摘要的档位下，把候选条件放宽为满足下面任意一条：

| 条件 | 覆盖的场景 |
| --- | --- |
| a. 正向：本地分区的戳指向这次备份的源分区，且版本相等（现有 L0） | 重复全量、compaction 之后的全量 |
| b. 反向：**备份里的源分区自己带着戳**，戳指向本地这个分区（库、表、分区 id 都等于本地的），且备份版本 == 本地 visibleVersion | 切回，且切过去以后 B 上这个分区没有写入 |
| c. 表级关联：本地表和备份表之间有同步关系（表级来源描述符，见下），分区名和 range 相同，且备份版本 == 本地 visibleVersion | 增量同步之后的全量，正反两个方向都适用 |

- **b 的数据来源**：备份元数据本来就是源表的深拷贝，戳会跟着进来（这正是"透传陷阱"）。现在恢复时先把它读出来用于判定，再按原逻辑清掉，不会被继承。
- **c 的表级来源描述符**：恢复任务提交时，在表上记下"这张表的数据来自哪个源库、源表"，即 OlapTable 新增一个 gson 字段（源 db id、源表 id）。这个字段同样随备份带到对端，所以反向时也能认出对端的表来自本地这张表。
- **这三条都只用来挑候选**：放宽以后，`off` 档位不允许使用这三条中的 b 和 c，必须算摘要。
- **版本对齐已实测确认**（2026-10-03）：用 ccr-syncer 做表级同步，全量之后分多轮增量写入（insert、MoW 的 update/delete），再做 compaction，每一轮上下游同名分区的 VisibleVersion 都完全相同。原因是下游每个分区按各自的 nextVersion 递增，而上下游的事务一一对应（FE `DatabaseTransactionMgr` 用 `partition.getNextVersion()` 分配版本）。所以只要同步没有丢事务或多出事务，版本号就对齐；万一不对齐，b、c 判不通过，只是不复用，不影响正确性。

### 11.3 与既有复用的关系

既有复用本来就能处理"物理 rowset 没变"的情况，包括切回时的反向匹配，以及增量同步写入的 1/3 副本。R2 补的是"物理变了、逻辑没变"的那部分：compaction、换副本、Clone，以及另外 2/3 的副本。两者叠加后，来回切换时没有变化的分区才能做到基本不下载。

## 12. 首版之后的决定（2026-10-03）

1. **由 b、c 进来的分区默认强制 full**：b、c 只要求"有同步关系且版本相等"，证据比 a 弱；在 sample 档位下，没被抽中的分区不算摘要，可能把"关系成立、版本相等、内容不同"的分区错误保留。新增 FE 配置 `restore_reuse_force_full_for_relation`（默认 true），关闭后 b、c 也按 sample 抽样。
2. **保留之后再变化只能整体下载**：作为增强项纳入开发，设计见第 13 节。

## 13. 增强：保留之后再变化的分区只补增量（草稿，2026-10-03）

### 13.1 问题

一个分区被 R2 保留以后，本地的 rowset 是本地自己的，上面没有指向对端 rowset 的来源记录。之后对端这个分区再有写入，版本前进到 V_b，而本地还停在 V_l，判定就会落到"版本不等"而整体下载。演练 s6 中，R2 为此下载了 84 MB，而只靠既有复用的对照组只下载了 5.7 MB（内部实测记录）。

### 13.2 思路：本地做底，只下载 (V_l, V_b] 的增量

条件全部满足时，不整体下载，而是**保留本地 (0, V_l] 的数据，只从对端取 (V_l, V_b] 这一段 rowset 追加上去**：

1. 本地分区和备份分区之间存在 a、b、c 中的某种关系（复用第 11 节的判定，只是不再要求版本相等），且 V_l < V_b；
2. **证明本地 (0, V_l] 与对端 (0, V_l] 逻辑相同**：比较两边在 V_l 这个版本上的摘要（做法见 13.3）；
3. 对端在 V_l 处有 rowset 边界，即对端没有把跨越 V_l 的版本合并进同一个 rowset，所以能单独取出 (V_l, V_b] 的增量快照；
4. 摘要支持这个表模型（首版只做 Duplicate 和 Unique MoW，MoR 的摘要不可加，退回整体下载）。

任何一条不满足，就退回现有的整体下载。

### 13.3 关键：对端在 V_l 上的摘要从哪来

对端做备份时并不知道接收端停在哪个版本，所以要让对端的摘要**可以按版本前缀拼出来**：
- 逻辑摘要是按桶求和（模 2^128），可加可减；
- 备份侧除了整体摘要，再**按 rowset 上报各自的桶和**；MoW 表还要按删除标记所在的版本，上报被标记行的桶和；
- 接收端要得到 V_l 的摘要，就把版本 ≤ V_l 的 rowset 的桶和加起来，再减去版本 ≤ V_l 的删除标记对应的桶和，与本地在 V_l 上的摘要比较。

这样备份侧只需要扫描一遍，任意前缀版本的摘要都能拼出来。代价是 jobInfo 中每个 tablet 的摘要数据变大（每个 rowset 一组桶和），需要按地基的做法，把它放到数据旁边的独立文件里，jobInfo 只存根摘要。

### 13.4 执行

- FE：对这类分区，不生成整段快照，而是对 (V_l, V_b] 生成增量快照任务（`start_version`/`end_version`）；下载照旧；提交时走"增量追加"，不是整体替换；
- BE：新增"恢复的增量追加"，参照增量 clone 的 `_finish_incremental_clone`：把下载下来的 rowset 转换 rowset id 后，用 `revise_tablet_meta` 加到本地 tablet 上；MoW 表要按本地数据为新 rowset 计算 delete bitmap（增量 clone 已有这套逻辑）；
- 版本：追加后本地版本 = V_b，与整体下载的结果一致；
- 谱系戳：照常重写为这次备份的来源。

### 13.5 局限与风险

- **对端在 V_l 处要有 rowset 边界**：对端的 cumulative compaction 可能把切换前后的 rowset 合并到一起，边界就没了，只能整体下载。命中率取决于 compaction 的时机，要在演练中实测。
- MoR 不支持；MoW 的增量追加要重新计算 delete bitmap，这是最容易出错的地方，要重点测试"追加后与整体下载逐行一致"。
- 改动面大：BE 新增按 rowset 的摘要上报、增量追加；FE 新增增量快照与提交路径。

### 13.6 备选（更简单，收益更小）

接收端在 VERIFYING 时，对本地已有的 rowset 按版本区间匹配对端的 rowset。对端 rowset 与本地 rowset 版本区间相同、单个 rowset 的摘要也相同时，**只跳过这一个 rowset 的下载**（但仍需对端的文件字节，因为本地文件和对端文件的物理布局不同，不能直接替换）。这一点决定了它行不通：不同字节的文件不能冒名顶替，所以按 rowset 粒度的复用必须字节相同。**因此本节只记录，不采用。**

### 13.7 进度

- **第一阶段完成**（2026-10-03，`3b2314e65fe`、`41de09b341b`、`d3f35e32d3f`）：按版本前缀可拼接的摘要。
  - 备份时，BE 在快照任务里产出分解摘要文件 `rdigest`：每个 rowset 的桶和，另加按"死亡版本"分组的删除标记桶和。远端仓库放在 `__idx_<id>/__rdigest__<tablet_id>.<根前32位>`，local 模式放在 `snapshot/<ts>/<tablet_id>/rdigest`。jobInfo 里记录根摘要（gson `rd`）。
  - 组合公式：`compose(V) = Σ_{R.end≤V} rowset_buckets − Σ_{k≤V} mark_buckets`；V 不是 rowset 边界时不可组合。
  - 验证：BE 单测 63 个通过；集群上两种模式共 144 个边界版本，拼出的结果与直接计算全部一致。
  - 文件每个 tablet 约 10～20 KB。
  - 不支持：MoR、MoW 带 DELETE 条件，以及额外扫描次数超过上限（`restore_digest_prefix_max_scans`，默认 4096）的 tablet。
- **第二阶段完成**（2026-10-04，HEAD `628cc491903`）：判定类型 INCREMENTAL、扩展 `RESTORE_DIGEST` 做前缀校验、只下载 (V_l, V_b] 的 rowset、BE 追加、提交与回滚；开关 `enable_restore_incremental_append`，默认关闭。演练中 s6 从 84 MB 降到 5.7 MB，总下载 312 MB（内部实测记录）。

## 14. 二期：原子恢复路径（2026-10-04）

### 14.1 为什么要做

ccr-syncer 默认用原子恢复做全量同步（总体方案第 9 章风险）。首版只覆盖非原子路径，所以按默认配置，R2 在 CCR 全量同步中用不上。

### 14.2 原子恢复现在怎么做

- 恢复到一张隐藏的暂存表（`__doris_atomic_restore_prefix__<表名>`），全部完成后，与原表整体互换（`atomicReplaceOlapTables`）；在互换之前，原表一点都不动；
- schema 没变时，`bindLocalAndRemoteOlapTableReplicas` 把暂存表每个 tablet 的副本放到原表对应副本所在的同一个 BE 上，并把原表 tablet 记作它的 base / ref tablet（`RestoreJob.java:1114`、`:1843`）。下载时 BE 拿 ref tablet 去做血缘比对，这就是既有复用在原子恢复下也生效的原因（内部实测记录）。

### 14.3 做法：保留 = 从原表 tablet 在本机建一份快照

- **判定**：和非原子路径相同（第 4、11、12、13 节），只是比较的对象是**原表**的分区和副本：L0（a/b/c）、档位、对原表每个副本做摘要校验、提交前复查。增加两个条件：这张表 schema 没变，并且暂存 tablet 已与原表 tablet 绑定（副本在同一个 BE 上）。
- **执行**：判为保留的分区，不再从远端下载，而是：
  1. 在原表 tablet 所在的 BE 上，对原表 tablet 的版本 V 做一次本地快照（硬链接，不复制数据，也不改动原表）；
  2. 把这份快照作为暂存 tablet 的"下载结果"放进它的快照目录：同一块盘就硬链接，不在同一块盘就本地复制；
  3. 之后照常走移动、提交流程。暂存表的这个分区版本 = V，与备份一致。
- **增量补齐**：第 13 节的增量追加，在原子路径下变成"原表本地快照 (0, V_l] + 从远端下载 (V_l, V_b]"，合并成暂存 tablet 的内容。在保留做完并测通之后再做。
- **安全性**：原表在互换之前不受任何影响，本地快照只是硬链接；失败或取消时，暂存表按现有逻辑清理即可。这比非原子路径更安全。

### 14.4 局限

- schema 变过的表，暂存 tablet 和原表没有绑定，一律整体下载；
- 跨盘时要做一次本地复制，比硬链接慢，但仍然省掉了跨机房的下载；
- 暂存 tablet 和原表 tablet 的副本必须在同一个 BE 上，副本分布不一致时，那个 tablet 退回下载。

### 14.5 进度

**完成**（2026-10-04，HEAD `5238412312d`，内部实测记录）：开关 `enable_restore_atomic_reuse`；本地来源快照（快照任务字段 `restore_local_source`，没有新增下载任务类型）；保留与增量补齐都支持原子路径。原子模式演练总下载 312.2 MB，与非原子模式相同，错误复用 0。

### 14.6 统计补充（2026-10-08）

`DownloadStats` 新增 `incremental_local_bytes`（增量分区沿用的本地数据量，按副本计，原子与非原子都计入），`reuse_ratio` 改为 (linked + skipped + kept + incremental_local) / (同上 + downloaded)。提交 `6a08e131e76`。回归中，增量分区的 `reuse_ratio` 从 0 变为 0.98～0.99。

## 15. 增量补齐扩展到 MoR 与 MoW 带 DELETE 条件（2026-10-08）

第 13 节的增量补齐目前只支持 Duplicate 和不带 DELETE 条件的 MoW（内部实测记录）。扩展如下：

- **MoW 带 DELETE 条件**：每一行的"失效版本"取两者中的较小值：delete bitmap 把它标记删除的版本，以及第一个命中它、且版本大于它所在 rowset 结束版本的 DELETE 条件的版本。按失效版本把这一行计入删除贡献，组合公式不变（`compose(V) = Σ rowset − Σ_{k≤V} 删除贡献`）。扫描次数仍受 `restore_digest_prefix_max_scans` 约束。
- **MoR**：可见行要按主键归并决定，摘要不可加，所以不做分解。备份侧在对端**最近的 K 个 rowset 边界版本**上，各算一次完整的 MoR 摘要（按版本序归并，含 sequence 列和 delete sign），K 由 BE 配置 `restore_digest_mor_prefix_boundaries` 控制，默认 3。结果写进同一个 `rdigest` 文件，作为"按版本的整体摘要"一类条目。接收端只有当 V_l 正好是这几个边界之一时才能补增量，否则整体下载。代价：MoR 只能单线程（约 130 MB/s），备份侧要多扫 K 遍，缓存可以抵消一部分重复计算。
- **增量追加**：MoR 的增量 rowset 追加后，读的时候照常归并，不需要额外处理；带 DELETE 条件的表，增量里如果含有 delete predicate 的 rowset，要连同 rowset meta 里的删除条件一起追加。两者都要验证"追加后与整体下载逐行一致"。
- **开关**：沿用 `enable_restore_incremental_append`，不新增开关。

**进度**：完成（2026-10-08，HEAD `cfe1c1d889b`，内部实测记录）。

## 16. 复用范围收窄（2026-10-14）

目标：每个实际复用的分区都有独立证明，证明不了就整体下载。摘要证明的是**在备份版本上查询结果相同**（不含隐藏列，如 `__DORIS_VERSION_COL__`）；复用还要求**底层状态足以让之后的写入行为相同**。只有对两者等价的表模型才复用。本节优先于前文。

### 16.1 默认每个复用分区都做摘要校验

- `restore_reuse_default_check_level` 默认值从 `sample` 改为 `full`。
- 新增 FE 配置 `restore_reuse_allow_unverified_keep`（mutable，masterOnly，默认 false）。为 false 时，`sample` 和 `off` 都按 `full` 生效：提升放在 `RestoreReuseJudge.levelOfTable` 里，选候选和影子统计的估算都经过它。`disable` 不变。
- 为 true 时保持原行为（`sample` 没抽中的分区、`off` 档的分区不算摘要就保留），只供实验和观测，不提供一致性保证。这类保留在 `SHOW RESTORE` 中标为未证明（`KEPT_L0_ONLY`、`KEPT_SAMPLE_PASSED`，`check_level.kept_unverified` 计数）。
- `SHOW RESTORE` 的 `ReuseEstimate` 和 `DownloadStats` 里有 `check_level`：请求的档位（`requested`）、实际生效的档位（`effective`）、未证明的保留数。
- 估算口径的变化：`DISABLE` 在估算时当作 `OFF`，再被提升为 `FULL`，所以默认配置下估算会把需要摘要的反向、表级路径也算上；原来 `DISABLE` 当 `OFF` 只算正向路径。
- 校验开销增大，开启复用后需要在发布说明里写明。

### 16.2 谱系戳带来源集群身份

- 备份：`BackupJobInfo` 新增可选字段，json 名 `scid`（代码字段 `srcClusterId`），值为源端集群 ID；旧备份没有这个字段。
- 谱系戳 `RestoreLineage` 新增 `cid`，表级关联 `RestoreSource` 同样新增 `cid`，取自备份的 `scid`，缺失为 -1（未知）。新增字段都是可选的，旧版本读到会忽略。
- 比较：正向，谱系戳的 `cid` 对备份的 `scid`；反向（b），备份里的谱系戳的 `cid` 对本地集群 ID；表级（c），正向用本地表来源的 `cid` 对备份的 `scid`，反向用备份表来源的 `cid` 对本地集群 ID。两边都已知且不相等，不作为候选；任一边未知，仍可作为候选，由摘要兜底。
- `restore_reuse_allow_unverified_keep=true` 且档位为 `off` 时，如果集群 ID 没有在两边都比较过（缺失），不能免校验保留，该分区改为做摘要。
- 局限：集群 ID 是初始化时生成的随机整数，不是全局唯一标识；运维可能把两套集群配成同一个值；用源端镜像冷备搭建的灾备集群，集群 ID 相同。所以它只用来缩小候选，不当作证明，这也是默认必须 `full` 的原因。

### 16.3 只复用 Duplicate 表

| 表模型 | 结论 | 原因 |
| --- | --- | --- |
| Duplicate（含 DELETE 条件） | 复用 | 没有被覆盖的旧版本；DELETE 条件删掉的行永久删除，之后的写入不会让它们重现 |
| Unique MoW（无论有无 sequence 列） | 拒绝：`HIDDEN_STATE_MOW` | 删除标记行是隐藏状态：部分列更新查找旧行时只看 delete bitmap、不看删除标记，"最新一行是删除标记"和"key 不存在"行为不同；摘要跳过删除标记行，两端摘要相同时，同一笔写入（如 `partial_update_new_key_behavior=ERROR` 的部分列更新）可能一边成功一边失败，两端 compaction 进度不同就会触发。带 sequence 列时，删除标记行的 sequence 值还影响之后的写入是否被接受 |
| Unique MoR | 拒绝：`HIDDEN_STATE_MOR` | 被覆盖的旧版本是隐藏状态，之后的 DELETE 条件会让它们重现 |
| Aggregate | 拒绝 | 逻辑等价无法证明 |

- 整分区保留和增量补齐两条路径都拒绝。影子统计新增 `l0_passed_but_hidden_state_mow`、`l0_passed_but_hidden_state_mor` 两个计数，不把它们算作可复用。
- FE 配置 `restore_reuse_allow_hidden_state_models`（mutable，masterOnly，默认 false）：为 true 时恢复收窄前的行为，仅供实验，不保证一致。
- BE 和摘要算法不改，MoR 的前缀摘要等代码保留，默认用不到。
- 以后放开 MoW 的方向：摘要算法 v2，对没被 delete bitmap 标记的全部物理行算摘要（包括删除标记行和被 light delete 删除条件命中的行），每行带标志位和 sequence 值，证明的是写入路径看到的状态；两端 compaction 进度不同时摘要会不同，退回整体下载（保守）。需要升级 `algo_version` 并重新审查。MoR 同理，代价更高。

### 16.4 测试

- FE 单测：默认档位；`allow_unverified_keep` 开关下 sample、off 的有效档位；集群 ID 在正向、反向、表级路径上相同、不同、缺失的组合；MoW、MoR 的拒绝原因与开关；影子统计；新字段缺失时可读。
- 回归：`test_backup_restore_reuse_scope`：Duplicate 整分区保留后与整体下载逐行一致；MoW 一端有删除标记行、另一端该 key 不存在，可见结果相同时必须被拒绝并整体下载，之后部分列更新两端一致；MoR 被拒绝；默认生效档位为 `full`。涉及 Unique 表复用机制的既有用例显式打开 `restore_reuse_allow_hidden_state_models`，结束时恢复。
