# Restore / CCR 全量同步的本地数据复用

目标是让 Restore 和 CCR 全量同步时，目标端已有且内容一致的数据不再重复下载。证明不了一致就照常下载，结果与完整下载相同；复用相关的能力默认关闭。注意"复用开关全关"不等于没有任何新行为：备份文件清单（Manifest）默认会生成，文件集合与大小校验默认开启，备份侧会多一次文件清单的计算和存储，开销见 [manifest-design.md](./manifest-design.md)。

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

## 复用范围（首版）

- **只复用 Duplicate 表。** Unique 表（MoW、MoR）的删除标记行、被覆盖的旧版本等隐藏状态不在摘要里，摘要相同的两个分区，之后同一笔写入的结果可能不同，所以默认整体下载，原因记为 `HIDDEN_STATE_MOW`、`HIDDEN_STATE_MOR`。FE 配置 `restore_reuse_allow_hidden_state_models`（默认 false）可以恢复旧行为，仅供实验，不保证一致。
- **默认每个复用的分区都有摘要证明。** `restore_reuse_default_check_level` 默认 `full`；`off`、`sample` 默认也按 `full` 执行，除非打开 `restore_reuse_allow_unverified_keep`（默认 false，仅供实验和观测，不提供一致性保证）。`SHOW RESTORE` 里同时显示请求的档位和实际生效的档位。
- **谱系戳带来源集群 ID。** 备份记录源端集群 ID，谱系戳随之记录，两边都有且不相等时不作为候选。集群 ID 是初始化时生成的随机整数，不是全局唯一标识，只用来缩小候选范围，不当作证明；任何一边缺失（旧备份、旧谱系戳）时仍可作为候选，由摘要兜底。
- **"查询结果相同"的含义。** 摘要证明的是在备份版本上的可见数据相同，不含隐藏列（如 `__DORIS_VERSION_COL__`，它随写入历史而不同，`show_hidden_columns=true` 时可见）。

## 当前状态

已实现：

- 谱系戳；
- 备份完整性校验；
- 复用观测；
- 分区级复用（逻辑摘要）；
- 增量补齐（实现上含 MoR、带 DELETE 条件的 MoW，但首版按上面的范围，默认只对 Duplicate 表生效）；
- 原子恢复路径；
- 摘要缓存修复。

**已知未修复**：原子恢复 + 增量补齐在多副本下，可能因副本修复竞争卡在 COMMITTING。根因和修复方案见 [stuck-append-analysis.md](./stuck-append-analysis.md) 和 [stuck-append-fix-plan.md](./stuck-append-fix-plan.md)，修复方案已审查、尚未实现。

测试环境的实测结论：灾备来回切换演练中，下载量减少约 71%，错误复用为 0；在下载较慢的单机测试环境、数据量较大时，端到端耗时加速约 2～3 倍，数据量较小（约 2 GB）时因固定开销占大头只有约 1.2～1.6 倍。目标规模和万兆网络下的收益仍需复测。

## 部署与回滚

- **升级顺序。** 先把所有 FE 和所有 BE 升级完成，再打开复用开关（`enable_restore_partition_reuse`、`enable_restore_incremental_append`、`enable_restore_atomic_reuse`）。原子恢复的增量补齐依赖 FE 下发的作业 ID 和备份摘要，以及 BE 写在 tablet 元数据里的完成证据（`restore_stage`）；FE 和 BE 版本混跑时不要打开开关。
- **降级前。** 先关掉上面的复用开关，等进行中的恢复作业全部结束（FINISHED 或 CANCELLED）再降级。旧版本 BE 读写 tablet 元数据时会丢掉 `restore_stage`，之后它不能证明恢复进度。
- **旧 FE 接管进行中的作业。** 主节点切换到没有该功能的旧 FE 时，进行中的增量恢复作业会失败，原子恢复下原表不受影响，不会写错数据；升级后重新恢复一次即可。
