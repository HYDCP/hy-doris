# 摘要增量化调研：让逻辑摘要的开销与变化量成正比

> 2026-10-12。只调研、设计、少量核实，不改代码。基于 `restore-r2` 分支 HEAD `03019208ed5`。文中行号以该提交为准。引用的实测数字来自 [耗时实验设计](./timing-experiment.md) 第 2 节和内部耗时实验报告。

## 0. 结论

1. **推荐：按 rowset 的持久化摘要缓存 + compaction 时继承。** 每个 rowset 的"桶和"存进 BE 本地 RocksDB（与 rowset meta 同库、独立前缀），tablet 在版本 V 的根摘要和 `rdigest` 文件都由各 rowset 的缓存条目相加得到；只有缓存里没有的 rowset 才读盘。compaction 的输出 rowset 的条目由输入条目相加得出，不读数据。Duplicate、MoW（含 DELETE 条件、sequence 列）都成立，MoR 不成立（归并不可加），MoR 只做 tablet 级持久化缓存。
2. **预计能省多少**：源端和目标端的摘要读盘量从"全量"降到"上次以来新写入的 rowset"。测试机实测里三张表合计源端 +28～34 s、目标端 +30～37 s；按场景估算，0% 变化时两端读盘都接近 0，30% 分区各写一个 5～10% 的新版本时读盘约为全量的 3%，compaction 之后（有继承）仍接近 0，原子恢复之后（做了条目搬运）接近 0。剩下的是每 tablet 的固定开销（打开 tablet、取 rowset 列表、组合 256 个桶、写 rdigest），估计三张表 1000 个 tablet 合计在几秒以内，**需要在测试机上实测确认**。
3. **一个现状缺陷（代码已证实）**：`restore_digest_prefix_enabled=true`（默认）时，源端快照里即使 tablet 级缓存命中，也照样完整扫一遍去产出分解摘要（`restore_digest.cpp:1456-1460`，`decompose_restore_digest` 不看 `cached`）。耗时实验里"全部 `from_cache=1` 仍多 14～19 s"的现象，原因更可能是这个，而不是报告里猜的"元数据与 rowset 打开"。所以现有缓存对源端基本没有作用。
4. **主要风险**是继承算错导致两端得到"相同但都错"的摘要，从而错误保留。防护：首发带"交叉核对"模式（继承的同时完整重算并比对，只记日志），稳定后改为抽样核对；缓存条目带 `algo_version`、`schema_sig`、rowset 版本区间与行数，不匹配只会重算、不会复用。
5. MoR 的单遍多边界优化可选。
6. 待决问题见第 8 节。

## 1. 现状核实

### 1.1 摘要在哪里算、算几遍

| 环节 | 入口 | 算什么 | 读几遍 | 证据 |
| --- | --- | --- | --- | --- |
| 源端：快照任务 | `task_worker_pool.cpp:1728-1740` `make_snapshot_callback` → `compute_logical_digest_for_task` → `get_tablet_logical_digest` | 快照做完后，对**live tablet**（不是快照文件，同一份数据的硬链接）在 (0, V] 算整体摘要；`restore_digest_prefix_enabled` 为真时同时产出分解摘要 `rdigest` | 见 1.3 | 已证实 |
| 源端：缓存 | `restore_digest.cpp:1434-1497` | 键 `(tablet_id, version, algo_version, schema_sig)`，内存 LRU，容量 `restore_digest_cache_capacity`（默认 10 万），重启丢失 | — | 已证实 |
| 源端：缓存失效的真实情况 | `restore_digest.cpp:1456-1460` | `if (decomposed != nullptr) { decompose_restore_digest(...) }` 在 `cached` 判断之前无条件执行；命中只省掉最后那次 `compute_restore_digest`，而分解摘要本身就是一次完整扫描 | **命中也要扫全量** | 已证实 |
| 源端：快照与摘要之间的 compaction | `task_worker_pool.cpp:1516-1530` | 摘要是对 live tablet 算的；若此时 rowset 集合与快照里的不同，就不写 rdigest（整体摘要仍上报） | — | 已证实 |
| 目标端：RESTORE_DIGEST 任务 | `task_worker_pool.cpp:1636-1660` `restore_digest_callback` | 对候选分区**每个本地副本**算整体摘要；增量候选还要取对端 rdigest 并 `compose(V_l)` 比对（`verify_prefix_digest`，不读数据） | 每副本 1 遍 | 已证实 |
| 目标端：谁进候选、谁全算 | `RestoreJob.java:3036-3058`，`RestoreReuseJudge.java:262` | `incrementalCandidate` 一律算；`FULL` 档或 `needsFullDigest`（b 反向 / c 表级关联 且 `restore_reuse_force_full_for_relation=true`）一律算；其余按 10% 抽样 | — | 已证实 |
| 目标端：缓存 | 同一个 `RestoreDigestCache` | 键同上。原子恢复后暂存 tablet 是新 tablet_id，所以下一次校验必然 miss | — | 已证实（键结构）；"基本不命中"是耗时报告的观察，未逐次核对 |

两点澄清：
- 摘要是 (tablet, version) 的函数，**与 compaction 无关**。compaction 不改变 version，所以"compaction 后缓存失效"对现有 tablet 级缓存并不成立；真正让它失效的是：BE 重启、版本前进（增量同步是常态）、原子恢复换了 tablet_id，以及上面那条"命中也要扫"。
- 源端每个 tablet 只在**一个副本**上算（备份只选一个副本做快照）；目标端对**每个副本**算，集群总读盘是单副本数据量的 3 倍，但分布在不同 BE 上并行。

### 1.2 一次扫描的成本构成

| 项 | 数值 | 来源 |
| --- | --- | --- |
| 单线程、页缓存热 | Duplicate 279 MB/s、MoW 308 MB/s、MoR 144 MB/s | 内部实测记录 |
| 12 线程，热 / 冷 | Duplicate 1595 / 1100 MB/s，MoW 1592 / 1435 MB/s | 同上 |
|测试机三 BE 合计、4 线程 | 约 0.7 GB/s（约 230 MB/s/BE），盘读 250 MB/s、盘利用率 70%、每 BE 0.8～1 核 | timing-probe.md 3.3 |
| 编码 + 哈希 | 750～850 MB/s（不是瓶颈） | 内部实测记录 |

推论（推测）：单线程下解码占主导（热 279 vs 冷 234，盘只贡献约 16%）；多线程下盘读成为瓶颈（热 1595 vs 冷 1100，盘贡献约 30%；测试机单盘共用时盘利用率 70%）。所以"少读"比"读得快"更有效：**不扫就是最大的优化**。

### 1.3 读放大：分解摘要的额外扫描

分解摘要每个 rowset 需要"在自己 end 版本上的存活行"（1 遍），再加：

| 模型 | 额外扫描 | 代码 | 性质 |
| --- | --- | --- | --- |
| Duplicate，无 DELETE | 0 | `restore_digest.cpp:1046-1056` | 已证实 |
| Duplicate，有 DELETE 条件 | 每个 (rowset, 版本 > rowset.end 的 DELETE 条件 d) **各 1 遍完整扫描**，取两层之差得到死于 d 的行 | `:1058-1082` | 已证实；条件多的表是 O(rowset 数 × 条件数) |
| MoW，无 DELETE | 每个 (rowset, 死亡版本 k) 1 次扫描，但用补集 bitmap 只读死于 k 的行，实际只触碰含这些行的页 | `:1105-1180` | 已证实；成本与被删行的分布有关，不是整遍 |
| MoW + DELETE | 每个 (rowset, 条件 d) 1 遍 level 完整扫描 + 上面的 bitmap 死亡扫描 | `:1085-1104` | 已证实 |
| MoR | 基准版本 + 最近 K 个边界各做一次**完整归并**，K=`restore_digest_mor_prefix_boundaries`（默认 3），并行度 ≤ threads | `:906-979 decompose_mor` | 已证实；读 K+1 倍，占 K+1 个核 |
| 上限 | 额外扫描 > `restore_digest_prefix_max_scans`（4096）→ 不产出 rdigest，退回只报整体摘要 | `:1067,1091,1143` | 已证实 |

### 1.4 "开销来自哪里"分解表（测试机，三张表，每副本 10～18 GB）

| 来源 | 占比 / 数值 | 依据 |
| --- | --- | --- |
| 源端：读全部 tablet 一遍（Dup、MoW） | 约 16 GB / 0.7 GB/s ≈ 23 s，占源端增量的大头 | timing-probe 3.3：最长 BE 23 s |
| 源端：MoR 的 K+1 遍 | 墙钟不变（并行），多占 3 核、3 份归并内存 | 内部实测记录 |
| 源端：MoW 死亡行扫描、DELETE 条件 level 扫描 | 实验表无 DELETE，MoW 覆盖 5% 键，影响小 | 推测 |
| 源端：FE 调度 | 快照阶段按 3 s 轮询；摘要让快照任务变慢即推迟整体 | 已证实（测试环境阶段耗时都是 3 s 的倍数） |
| 目标端：候选分区每副本读一遍 | 反向关联强制 full，200 分区 × 3 副本 全读 | 已证实（timing-probe 3.3：sample 与 full 相同） |
| 目标端：增量候选 compose 对端 rdigest | 不读数据，可忽略 | 已证实 |
| 两端缓存命中带来的节省 | 源端 ≈ 0（1.1 节第 3 行）；目标端原子恢复后 ≈ 0 | 已证实 / 观察 |

## 2. 方案调研

统一的估算口径：`d_read` = 一次全量同步两端摘要相关的读盘量，以"今天 = 100%"计。场景：A 0% 变化；B 30% 分区各写一个新版本（新版本约为分区的 5～10%）；C 两端刚做过 full compaction 再同步；D 上一次是原子恢复，这次再同步。

### 2.1 方向 1：按 rowset 缓存摘要

原理：rowset 不可变；分解摘要已经是"每个 rowset 一组桶和 + 按死亡版本分组的标记桶和"（`RestoreDigestDecomposed`，`restore_digest.h:238-300`），把它拆成"每 rowset 一个条目"存起来，下次只算新 rowset，再相加。

**存哪里**（三选一）：

| 位置 | 优点 | 缺点 | 判断 |
| --- | --- | --- | --- |
| 内存 LRU（现状的扩展） | 最简单 | 重启丢；BE 滚动升级、故障后首轮全量重算 | 作为前置的热缓存可以，不能单靠它 |
| **RocksDB meta（与 `rst_` 同库、新前缀）** | 持久；按 tablet_uid 前缀范围删除；写入是 KB 级，走已有 `OlapMeta::put/get/iterate/remove`（`olap_meta.h:52-68`）；不碰任何文件枚举逻辑 | 要自己做 GC（挂在 `RowsetMetaManager::remove` 的四个调用点：`tablet.cpp:678,1574,1590`、`storage_engine.cpp:1019`，以及 drop tablet） | **推荐** |
| rowset 旁的文件（`<rowset_id>.rdigest`） | 快照硬链接、clone、本地来源快照自动带走 | 要进 manifest、上传、下载、GC、`convert_rowset_ids` 改名等所有文件枚举路径；任何漏网之处都是坏文件或孤儿文件 | 不推荐 |
| 写进 `RowsetMetaPB` 新字段 | 随 meta 走 | 每个 rowset meta 多 1～6 KB 常驻内存（百万 rowset 即 GB 级）；改核心 proto | 不推荐 |

**各模型是否成立**：
- Duplicate（含 DELETE 条件）：成立。条目 = 存活于 R.end 的桶和 + 各 (d, 死于 d 的桶和)。
- MoW（含 sequence 列、DELETE 条件）：成立。条目 = 存活于 R.end 的桶和（bitmap 标记 ≤ R.end 在发布时就定了，之后不变）+ 各 (k, 死于 k 的桶和)。**死亡列表会增长**：后来的版本会给旧 rowset 加新标记。所以条目要记 `marks_upto`（已覆盖到哪个版本）；用到 V > marks_upto 时，只扫 (marks_upto, V] 的新标记行（bitmap 补集扫描，只碰相关页），再把条目补全。没有新标记时只查 bitmap，不读数据。
- MoR：不成立，归并不可加。只能缓存"tablet 在版本 V 的整体摘要"。

**能省多少**（`d_read`）：A ≈ 0%；B ≈ 3%（60/200 分区 × 10% 新行）；C：不带继承时 compacted 的 tablet 全部重算，源端 = 100%，目标端 = 100%（full compaction 场景与今天相同；只是 cumulative 时小得多）；D：不搬运条目时，暂存 tablet 的 rowset 都是新 id，100%。

**正确性风险**：条目用错（schema 变了、算法变了、rowset 行数不同）。防护：条目里存 `algo_version`、`schema_sig`、start/end version、num_rows，取用时逐项核对，不符即重算。因为摘要是单射性很强的 256 桶模 2^128 和，两端只有"内容相同"才会相等；缓存只是把"读盘"换成"查表"，不改变比较逻辑。

**兼容**：不改 thrift、不改 FE、不改 rdigest 文件格式、`algo_version` 不变。新旧 BE 混跑：各 BE 独立，旧 BE 照常全算。升级后首轮全量算一遍把缓存填满。


### 2.2 方向 2：compaction 时继承

在 `Compaction::do_compaction_impl` 构建完输出 rowset 后，如果所有输入 rowset 都有有效条目：

```text
out.base       = Σ in_i.base − Σ_{k ≤ out.end} in_i.deaths(k)
out.deaths(k)  = Σ in_i.deaths(k)            对 k > out.end
```

**成立条件逐项核对**（已证实的代码依据）：
- Duplicate：compaction 读取时 `read_source.fill_delete_predicates()` 并以 `reader_params.version = 输出版本` 过滤（`merger.cpp:136-139, 330-333`），即版本 ≤ out.end、且 > 输入 rowset.end 的条件在输出里被物理删除；更高版本的条件不在输入里，不影响。输出的 `delete_predicate` 是输入的并集（`compaction.cpp:411-430`），其生效版本 = out.start，只作用于更老的 rowset，与"输出 rowset 自身存活于 out.end"一致。公式成立。
- MoW：普通 MoW 的 compaction 读取**不带** bitmap（`merger.cpp:144-149` 只给 cluster key 和 row binlog 的 tablet 传 bitmap），旧行是在按 key 归并时被新行顶掉的；输入 rowset 上 `[0, max_version+1)` 全部版本的标记按行号映射到输出（`compaction.cpp:1831`，`base_tablet.cpp:1850-1897`），找不到目标行的就是被归并掉的行（`missed_rows`）。所以公式不依赖"怎么删"，只依赖 Doris 自己的 compaction 不变量：输出 rowset 在 out.end 的可见行 == 输入 rowset 在 out.end 的可见行之并（P1/P2 已在集群上验证 compaction 前后摘要相同，内部实测记录）。前半句就是 `Σ in.base − Σ_{k≤out.end} in.deaths(k)`；k > out.end 的标记映射后仍落在同一批行上，哈希相同，所以 out.deaths(k) = Σ in.deaths(k)。若有某行在 k ≤ out.end 被标记却没被归并掉（映射后输出带 ≤ out.end 的标记），公式同样把它减掉了，与"输出在 out.end 的存活行"定义一致。公式成立。
- MoW + DELETE 条件、sequence 列：同上两条叠加；sequence 列只是参与编码的一列，谁胜出由 bitmap 决定，compaction 不改变胜者。成立。
- delete sign 行：摘要两边都不计入（过滤后再哈希），cumulative 保留、base 删除它都不影响。成立。
- MoR：不成立，不做。
- 边界：输入条目的 `marks_upto` 必须都 ≥ out.end，否则先补标记再继承，或放弃继承。

**能省多少**：场景 C 从 100% 降到 ≈ 0%（compaction 时不读任何数据，只做 256 桶加减）。日常 cumulative compaction 也不再触发重算。

**正确性风险**（最大的一条）：继承公式若有系统性错误，两端用同一份代码会得到"相同但都错"的摘要。例如漏掉某类死亡标记，两端都漏，目标端若恰恰缺这次删除，摘要仍相等 → 错误保留。防护：
- `restore_digest_inherit_mode = off | verify | on`，首发默认 `verify`：继承之后仍完整重算一遍并比对，不一致记 WARNING + bvar 计数并以重算为准。`verify` 等价于今天的开销，不会更慢；跑过一轮灾备演练与一段线上观察后再切 `on`。
- `on` 模式下保留抽样核对 `restore_digest_inherit_verify_ratio`（默认 1%）。
- 弱校验：Duplicate 的 Σcount 必须等于输出 rowset 的 num_rows；MoW 因 delete sign 行取 ≤。
- 单测：Dup/MoW/MoW+DELETE/sequence，各种 compaction 切分（cumulative、base、含 delete 的 base）前后，继承结果 == 直接扫描结果，逐桶比对。

**兼容**：BE 内部改动，不改协议。

### 2.3 方向 3：目标端"没变就不重读"

现状目标端的 tablet 级缓存键已经是精确的（内容由 version 决定），缺的只是：持久化、以及 tablet_id 在原子恢复后变了。

做法：
- (a) tablet 级整体摘要也进 RocksDB（`rdgt_<tablet_uid>_<version>`），顺手覆盖 MoR 和重启。
- (b) **原子恢复的条目搬运**：`restore_local_source` 快照把原表 tablet 的 rowset 硬链接给暂存 tablet（`snapshot_loader.cpp:2454-2482`），随后 `convert_rowset_ids` 改 rowset id（`snapshot_manager.cpp:205`，映射在 `rowset_id_mapping`）。在这里把原 rowset 的条目按映射复制到新 tablet_uid / 新 rowset_id 下。零读盘。
- (c) **下载的 rowset 从对端 rdigest 播种**：对端的 rdigest 里本来就有每个 rowset 的桶和（`RestoreDigestRowsetPart`）。下载并通过清单核对（大小 + SHA-256，`snapshot_loader.cpp:792-816`）后，这些 rowset 的字节与对端相同，摘要也相同，可以直接按新 rowset_id 落成本地条目（标记 `origin=imported`）。这样全量下载之后下一次校验也不用读盘。前提是清单带了 SHA-256（`restore_manifest_digest_check`），否则不播种。MoW 的死亡标记在本地要重算 bitmap，这里只播种 base 桶和，死亡列表留空、`marks_upto = R.end`，让方向 1 的增量标记扫描去补。
- (d) FE 侧"随谱系戳持久化摘要"：不建议。摘要是每 tablet 每副本的，放 FE 是 150 B × tablet × 副本的编辑日志开销，而且 FE 只能证明"FE 记得"，不能证明 BE 上没变；BE 本地持久化缓存已经达到同样效果且键更准。

**能省多少**：D 从 100% 降到 ≈ 0%（保留分区靠 (b)，下载分区靠 (c)）。MoR 在"版本没变"时也不再重算。

**风险**：(c) 相当于相信对端算的摘要代表本地文件，前提是字节级一致由清单保证；若清单没有 SHA-256（只有大小），不播种。(b) 只是搬运，没有新假设。


### 2.4 方向 4（对照）：降低校验强度

- `restore_reuse_force_full_for_relation=false`：反向、表级关联也按 10% 抽样。d_read 目标端立即降到 10%。代价：b、c 只凭"有同步关系 + 版本相等"，没被抽中的分区若内容不同会被错误保留。灾备切回场景里，B 上被写过的分区版本一定变化（版本对齐已验证），所以"版本相等却内容不同"只会来自异常：丢事务、手工改数据、副本不一致（内部实测记录 MoR+sequence 的 Doris 自身 bug）。这正是摘要要防的事。
- 按数据量自适应（分区大的全算、小的抽样）：小分区读得快，省不了多少；大分区才贵又不能放过。意义不大。

结论：作为对照保留，不作为优化手段。做了方向 1～3 以后 full 校验本身就便宜，没有必要降强度。

### 2.5 补充方向

- **MoR 单遍多边界**：现在 K+1 个边界各做一次归并（读 K+1 倍）。可以改成一次按 key 归并、每个 key 组内同时判定 K+1 个前缀的胜者（版本 ≤ 边界的最大 (sequence, version)），一遍读完。收益只在 MoR 表。可选。
- **MoW 死亡标记在发布时顺手算**：发布删除/更新时已经定位到旧行位置，若此时读出旧行算哈希，就不用事后扫描。但把读盘加进写路径，不值得；不做。

### 2.6 社区与其他系统

- **Doris 自身**：`ADMIN CHECK TABLET` 的一致性检查每次从 rowset 完整读一遍算 SipHash，没有缓存和增量（见 [engine_checksum_task.cpp](https://apache.googlesource.com/doris/+/HEAD/be/src/olap/task/engine_checksum_task.cpp)、[ADMIN CHECK TABLET](https://doris.apache.org/docs/sql-manual/sql-statements/Database-Administration-Statements/ADMIN-CHECK-TABLET)）。社区没有"按 rowset 缓存逻辑摘要"的做法，查不到相关讨论。
- **Cassandra 增量修复**：只对未修复的 SSTable 建 Merkle 树，用 SSTable 元数据里的 `repairedAt` 标记跳过已修复数据，思路是"不可变文件 + 标记"（[DataStax: More Efficient Repairs in 2.1](https://datastax.com/blog/more-efficient-repairs-21)，[Incremental Repair Improvements in Cassandra 4](https://thelastpickle.com/blog/2018/09/10/incremental-repair-improvements-in-cassandra-4.html)）。它的教训直接适用：2.1 版有 compaction 在标记前吃掉 SSTable 的竞态，4.0 用"pending repair"状态并限制 compaction 才修好。我们的对应点是"快照之后、摘要之前发生 compaction"（现已通过 `write_snapshot_prefix_digest` 的 rowset 集合比对处理）和"继承时输入条目的 `marks_upto` 不够新"。
- **可加集合哈希**：Bitcoin Core 用 MuHash（模乘的可加集合哈希）维护 UTXO 集合哈希，每次变更增量更新而不重扫全集，与本方案"桶内模 2^128 求和、可加可减"同一思路。ClickHouse 每个不可变 part 带 `checksums.txt`，副本间按 part 比对，属于物理校验，不做逻辑等价。

## 3. 推荐方案设计

### 3.1 数据结构

每个 rowset 一个条目（序列化沿用 `rdigest` 的稀疏桶写法）：

```text
RowsetDigestEntry {
  uint32 format = 1
  uint32 algo_version          // = RestoreDigest::kAlgoVersion
  bytes  schema_sig            // 计算时 tablet 的 schema_sig
  uint8  model                 // DUP / MOW
  int64  start_version, end_version
  int64  num_rows              // rowset meta 的行数，用于弱校验
  int64  marks_upto            // 死亡列表已覆盖到的版本（Dup：已考虑的 DELETE 条件版本上限）
  sparse buckets base          // 存活于 end_version 的行
  list<(int64 k, sparse buckets)> deaths   // 死于 k (> end_version) 的行
  uint8  origin                // scanned / inherited / imported / propagated
  int64  created_ms
}
```

tablet 级整体摘要条目（所有模型，MoR 唯一依赖）：`{algo_version, schema_sig, root, rows, origin}`。

**键**（RocksDB `META` 列族）：
- `rdg1_<tablet_uid>_<rowset_id>`：rowset 条目。用 tablet_uid 不用 tablet_id，原因与 `rst_` 相同（同 id 不同 uid 的旧 tablet 残留）。
- `rdgt1_<tablet_uid>_<version>`：tablet 级条目。
- 前缀里的 `1` 是条目格式版本。`algo_version`、`schema_sig` 放在值里，取用时核对；核对不过只会重算。

**内存**：现有 `RestoreDigestCache` 之上加一层 rowset 条目 LRU（按字节限额，默认 256 MB），RocksDB 是真相。

### 3.2 读路径（两端共用）

`get_tablet_logical_digest(tablet, V)` 改为：

1. 取 rowset 列表与（MoW）bitmap 快照（现有 `prepare_tablet_input`）。要求 V = 最后一个 rowset 的 end（visible version 必然满足）。
2. MoR：查 `rdgt1_`，命中返回；否则按现状计算并写入。
3. Dup/MoW：对每个 rowset 查条目并核对（algo、schema_sig、版本区间、行数）。缺失或不符的 rowset 加入"待扫"；MoW 条目 `marks_upto < V` 的，查 bitmap 中 (marks_upto, V] 是否有新标记，有则加入"补标记扫描"（只读死亡行）；Dup 条目若存在 d ∈ (marks_upto, V] 的 DELETE 条件，加入"补条件扫描"。
4. 只对待扫 rowset 跑现有 `decompose_restore_digest` 的扫描逻辑（它已经按 rowset 分组产出 base 与 deaths），写回条目。
5. 组合：`root = Σ base − Σ_{k ≤ V} deaths(k)`，同时拼出 `RestoreDigestDecomposed`（rowsets、marks），写 rdigest 的逻辑不变。写 `rdgt1_`。

命中率为 100% 时整条路径不读数据文件。源端 1.1 节的缺陷由此自然消失（分解摘要来自条目，不再触发扫描）。

### 3.3 写入与失效时机

| 时机 | 动作 |
| --- | --- |
| 摘要计算扫描到某 rowset | 写 `rdg1_` 条目（origin=scanned） |
| compaction 完成，`modify_rowsets` 之前 | 输入全有条目 → 按 2.2 公式写输出条目（origin=inherited）；`inherit_mode=verify` 时另起后台任务完整扫描输出 rowset 比对 |
| `restore_local_source` 快照 → `convert_rowset_ids` | 按 `rowset_id_mapping` 把原 tablet 条目复制到新 tablet_uid（origin=propagated） |
| 下载完成、清单 SHA-256 核对通过、`convert_rowset_ids` 之后 | 用对端 rdigest 的 `RestoreDigestRowsetPart` 播种（origin=imported，MoW 只有 base，`marks_upto=end`） |
| `RowsetMetaManager::remove` | 同时删 `rdg1_` |
| drop tablet / `TabletMetaManager::remove` | 范围删 `rdg1_<uid>_*`、`rdgt1_<uid>_*` |
| schema change 完成（新 tablet） | 新 tablet 无条目，首次全算；旧条目随旧 tablet 删除 |
| 后台清理 | 复用 `_clean_unused_rowset_metas` 的遍历：条目对应的 rowset meta 不存在即删 |

不需要"失效"的情况：compaction 后旧输入变 stale，条目留到 GC；version 前进不影响已存在的 rowset 条目。

### 3.4 各表模型

| 模型 | rowset 条目 | 继承 | 说明 |
| --- | --- | --- | --- |
| Duplicate | base | 是 | 最简单 |
| Duplicate + DELETE | base + deaths(d) | 是 | 补条件扫描仍是整遍（两层之差），条件多的表退化；受 `max_scans` 保护，超限只报整体摘要 |
| MoW（含 sequence） | base + deaths(k) + marks_upto | 是 | 补标记扫描只读死亡行 |
| MoW + DELETE | 同上 + level 扫描 | 是 | 死亡版本取 min(bitmap, 条件)，与现状一致 |
| MoR（含 sequence） | 无，只有 `rdgt1_` | 否 | 版本不变时命中；变了全算 K+1 遍；可选做单遍多边界 |
| Aggregate、cluster key、VARIANT 等 | NOT_SUPPORTED，不缓存 | — | 与现状一致 |

### 3.5 与现有机制的衔接

- **分解摘要 / rdigest**：文件格式不变（v1、v2），内容由条目拼出；`write_snapshot_prefix_digest` 的"快照 rowset 集合 == 摘要 rowset 集合"检查保留。
- **增量补齐**：目标端在 V_l 的摘要 = 条目组合（V_l 是本地最后 rowset 的 end）；对端在 V_l 的摘要 = 对端 rdigest `compose(V_l)`。不变。
- **原子路径**：3.3 的搬运与播种让暂存表第一天就有热缓存；提交互换后暂存 tablet 成为正式表，条目随 tablet_uid 走。
- **谱系戳 / L0 / 档位**：不变。force_full 可以继续开着，因为 full 变便宜了。
- **云模式、冷却数据**：不支持，与现状一致。

### 3.6 可观测

- bvar：`restore_digest_rowset_cache_{hit,miss,inherited,propagated,imported,evicted}`、`restore_digest_bytes_scanned`、`restore_digest_bytes_skipped`（命中 rowset 的 `data_disk_size` 之和）、`restore_digest_inherit_verify_{ok,mismatch}`。
- 日志：`logical digest done` 增加 `rowsets_total/rowsets_scanned/bytes_scanned/bytes_skipped`。
- `TLogicalDigest` 可选增加 `bytes_scanned`（thrift 新可选字段，旧 FE 忽略），FE 在 `ReuseEstimate` 汇总"校验读盘字节"，耗时实验直接用。可放第二阶段。
- 调试接口 `/api/_restore_digest` 加 `?cache=list` 列出 tablet 的条目（origin、marks_upto），便于演练核对。

### 3.7 配置

| 配置（BE） | 默认 | 说明 |
| --- | --- | --- |
| `restore_digest_rowset_cache_enabled` | true | 总开关；关掉即现状 |
| `restore_digest_rowset_cache_min_rowset_bytes` | 8 MB | 小于此的 rowset 不缓存（重扫便宜，省 RocksDB 空间） |
| `restore_digest_rowset_cache_mem_bytes` | 256 MB | 内存层限额 |
| `restore_digest_inherit_mode` | verify | off / verify / on |
| `restore_digest_inherit_verify_ratio` | 0.01 | on 模式的抽样核对比例 |
| `restore_digest_import_from_backup` | true | 下载后用对端 rdigest 播种（需要清单 SHA-256） |

现有 `restore_digest_cache_capacity`（tablet 级内存缓存）保留。

### 3.8 测试计划

- **单测**（BE）：条目序列化往返；组合 == 直接计算（Dup / Dup+DELETE / MoW / MoW+sequence / MoW+DELETE，各 1、3、20 个 rowset）；marks_upto 补标记后 == 直接计算；继承：对同一批数据做 cumulative、base、含 delete 的 base，继承条目 == 扫描条目逐桶相等；schema_sig / algo_version / 行数不符时必须重算；GC：删 rowset 后条目消失。
- **回归**（`backup_restore` 目录）：现有 64 个用例全部通过；新增：开启缓存两次备份第二次 `bytes_scanned=0`；compaction 后备份 `bytes_scanned=0` 且摘要不变；原子恢复后再校验 `bytes_scanned=0`；`inherit_mode=verify` 下人为注入错误继承（debug point）必须报 mismatch 并以重算为准。
- **测试机端到端**（沿用耗时实验的测试脚本，改前改后同一数据）：四个点 0% / 30% / compaction 后 / 原子恢复后，各 3 对交替；指标：备份快照阶段、恢复快照阶段的增量秒数，`bytes_scanned`，正确性逐分区比对；再跑一轮 `inherit_mode=verify` 看 mismatch 为 0。
- **故障注入**：BE 在条目写入途中重启（RocksDB 原子写，最多丢一个条目）；缓存条目手工改坏（行数不符）必须重算。

### 3.9 分阶段

| 阶段 | 内容 |
| --- | --- |
| 0 | 在测试机测清两件事：关掉 `restore_digest_prefix_enabled` 后缓存命中的源端开销（证实 1.1 第 3 行）；单 tablet 冷/热页缓存的摘要耗时 |
| 1 | rowset 条目 + RocksDB 持久化 + 组合读路径 + tablet 级持久化（MoR）+ GC + bvar |
| 2 | compaction 继承 + verify 模式 + 单测 |
| 3 | 原子恢复搬运 + 下载播种 + 回归用例 +测试机实测 |
| 4（可选） | MoR 单遍多边界 |

阶段 1 单独上线就能解决增量同步常态下的大部分开销；阶段 2、3 分别解决 compaction 和原子恢复两个特例。

## 4. 测量

计划在测试机上做两项小测量（单 tablet 冷/热页缓存耗时；关闭 prefix 后缓存命中的快照耗时），不造新数据，只用现有库或 ≤ 2 GB 的临时表。调研时测试机无法连接，**未能执行**。已有数据能支撑的结论写在 1.2、1.4 节，并明确标为推测的部分需要阶段 0 补测。

## 5. 预计节省汇总

以测试机三张表、每副本 10～18 GB 为例（今天：源端 +28～34 s，目标端 +30～37 s）：

| 场景 | 源端读盘 | 目标端读盘 | 预计两端摘要增量 | 备注 |
| --- | --- | --- | --- | --- |
| A 0% | ≈ 0 | ≈ 0 | 几秒（固定开销，待测） | 阶段 1 即可 |
| B 30% 分区各写 5～10% | ≈ 3% | ≈ 3% | 约 2～4 s | 阶段 1 |
| C full compaction 后 | ≈ 0（继承） | ≈ 0 | 几秒 | 需阶段 2；无阶段 2 时 = 今天 |
| D 原子恢复后 | ≈ 0 | ≈ 0（搬运 + 播种） | 几秒 | 需阶段 3；无阶段 3 时目标端 = 今天 |
| MoR 表 | 版本不变时 0，变了 K+1 遍 | 同 | 视 MoR 占比 | 阶段 4 可降到 1 遍 |

外推到万兆网络的目标规模：摘要开销从"与数据量同阶"降为"与变化量同阶"，加速比上限回到 (F+D)/(F+变化量下载)，不再被摘要吃掉。

## 6. 风险清单

| 风险 | 影响 | 防护 |
| --- | --- | --- |
| 继承公式系统性错误 | 两端"相同但都错"→ 错误保留 | verify 模式首发、抽样核对、行数弱校验、逐桶单测 |
| 条目被错误复用（schema 变、算法变） | 同上 | 值内核对 algo/schema_sig/版本/行数，不符重算 |
| 播种信任了对端摘要 | 下载文件若与对端不同则条目错 | 只在清单 SHA-256 通过后播种；MoW 死亡列表本地重算 |
| RocksDB 膨胀 | 每条 ≤ 6 KB，百万 rowset 数 GB | 小 rowset 不缓存、GC、可关 |
| DELETE 条件很多的表 | 补条件扫描仍是整遍 | `max_scans` 兜底，退回整体摘要 |
| 补标记扫描与并发 compaction | 条目 marks_upto 落后 | 取 bitmap 快照与 rowset 列表在同一临界区（现状已如此）；继承前检查 marks_upto |

## 7. 兼容与升级

- 不改 `algo_version`、rdigest 格式、thrift 必填字段；可选新增 `bytes_scanned`。
- 新旧 BE 混跑：互不影响；旧 BE 全算，新 BE 用缓存，根摘要相同。
- 升级后首轮全算填缓存；回滚到旧 BE 时 `rdg1_`/`rdgt1_` 键无人读取，留在 RocksDB 里无害，可用 `_clean_unused_rowset_metas` 式清理。
- 回合 3.1：RocksDB meta、compaction 钩子、`convert_rowset_ids` 在 3.1 都存在，改动点一致；`beta_rowset_reader` 的 bitmap 版本截断在 3.1 同样成立（需在回合时复核行号）。

## 8. 待决问题

1. **继承首发模式**：`verify`（开销与今天相同，但零风险）还是直接 `on`（立刻省，靠单测与抽样）？建议 verify 跑完一轮演练再切。
2. **是否做下载播种（3.3 imported）**：它相信对端摘要代表本地字节一致的文件；若不接受，原子恢复后下载过的分区下一次仍要全算一次。
3. **阶段 4（MoR 单遍多边界）是否做**：取决于 MoR 表的占比。
4. **阶段 0 的测量何时做**：测试机当时连不上；需要确认机器状态，或改在其他环境测。
5. **存储位置是否接受 RocksDB meta**：这是对 BE 核心存储的新增键前缀，需要和主线维护者（回合时）确认命名与 GC 挂点。
6. **`restore_reuse_force_full_for_relation` 保持 true**：建议保持；本方案让 full 变便宜，没有必要降强度。
