# R0 观测设计：既有复用的统计与展示

> 为什么只做观测见[总体方案](./overview.md) 4.3 和内部实测记录；本文只写怎么做。

## 1. 目标

每次恢复结束后，能直接看到既有复用（按 `source_rowset_id` 硬链接）**实际省了多少**、没省下的原因是什么，口径和 R2 的影子统计一致（按字节计）。用途：
- DBA 能看出某次恢复为什么突然变慢（血缘断了）；
- 判门 1 时，算 R2 的增量价值要扣掉既有复用已经省下的部分，需要这个数。

不改变任何恢复行为。

## 2. 统计什么

按**下载任务**统计。一个下载任务对应一个 BE 上的若干副本，所以汇总后自然就是"副本 × tablet"的口径。

| 计数 | 含义 | 来源 |
| --- | --- | --- |
| `linked_files` / `linked_bytes` | 按血缘硬链接复用的文件和字节 | HTTP 路径 `_link_same_rowset_files` |
| `skipped_files` / `skipped_bytes` | 本地已有、同名同大小（及 md5）而跳过下载的文件 | 两条下载路径里"不需要下载"的分支 |
| `downloaded_files` / `downloaded_bytes` | 实际下载的文件和字节 | 两条下载路径的下载分支 |
| `tablets_full_reuse` / `tablets_partial_reuse` / `tablets_no_reuse` | 副本级：数据文件全部复用 / 部分复用 / 完全没复用 | 每个副本下载结束时判定（`.hdr` 不计） |
| `unmatched_rowsets` | 远端有、本地找不到血缘匹配的 rowset 数 | `_link_same_rowset_files` |
| `unmatched_reason` | 按原因计数：`no_source_rowset_id`（本地 rowset 没有来源）、`source_not_in_snapshot`（来源 rowset 不在这次快照里，常见于 compaction 或上游换副本）、`version_mismatch`（来源对上但版本区间不同） | 同上 |

远端仓库路径没有血缘复用，只会有 `skipped` 和 `downloaded`。

## 3. 怎么上报和展示

- **BE → FE**：`TFinishTaskRequest` 新增一个可选字段 `download_stats`（结构体，字段同上表），下载任务完成时带回。旧 BE 不带，FE 就不计入（记为"未上报"的副本数）。
- **FE**：RestoreJob 累加各下载任务的统计，持久化为一个新的 gson 字段，随 job 写编辑日志。
- **展示**：`SHOW RESTORE` 完整输出在 `ManifestCheck` 之后新增一列 `DownloadStats`（JSON），BRIEF 不变。示例：

```json
{"linked_bytes":169345024,"skipped_bytes":0,"downloaded_bytes":3145728,"reuse_ratio":0.982,
 "linked_files":176,"skipped_files":0,"downloaded_files":64,
 "replicas":{"full_reuse":16,"partial_reuse":32,"no_reuse":0,"not_reported":0},
 "unmatched_rowsets":32,"unmatched_reason":{"no_source_rowset_id":0,"source_not_in_snapshot":32,"version_mismatch":0}}
```

  `reuse_ratio` = (linked + skipped) / (linked + skipped + downloaded)，按字节计。
- **日志**：BE 在每个副本下载结束时打一行汇总日志，FE 在恢复结束时打一行整体汇总。
- **指标**：第一版不加 Prometheus 指标，先用 `SHOW RESTORE` 和日志；以后需要监控大盘时再加。

## 4. 兼容

- 新 FE + 旧 BE：没有统计，计入 `not_reported`。
- 旧 FE + 新 BE：旧 FE 忽略新字段。
- 不改变任何下载、复用、校验的行为；统计出错只打日志，不影响恢复。

## 5. 改动范围

| 部分 | 内容 |
| --- | --- |
| thrift | `TDownloadStats` 结构体与 `TFinishTaskRequest.download_stats` |
| BE | 两条下载路径计数；`_link_same_rowset_files` 统计未匹配原因；任务完成时上报 |
| FE | 累加、持久化、`SHOW RESTORE` 新列、日志 |
| 测试 | BE 单测（HTTP 路径的链接、下载、跳过、各种未匹配原因）；FE 单测（累加、列、兼容）；回归用例（同一份快照恢复两次，第二次 `reuse_ratio` 接近 1） |


## 6. 已定（2026-10-02）

1. `SHOW RESTORE` 只给汇总；tablet 级明细在 BE 日志里按 tablet 打印，便于排查。
2. 第一版不加 Prometheus 指标。
3. **未匹配原因的判定**（实现时定）：以未匹配的远端 rowset 为单位，空 rowset 和在远端存储上的 rowset 不计。本地有血缘对应、但版本区间不同，记 `version_mismatch`；与它版本重叠的本地 rowset 都有来源、但来源不是它，记 `source_not_in_snapshot`；其余（重叠的本地 rowset 里有没来源的，或者根本没有重叠）记 `no_source_rowset_id`，所以首次恢复的未匹配全部记在这一类。

实现：`restore-r0-stats` 分支 `c9ddf187813`；验证见内部实测记录。gson 字段 `dls`，thrift 字段 `TFinishTaskRequest.download_stats`（25）。
