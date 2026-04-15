# Doris WebUI 日志查看与下载（tar.gz）设计文档
 
**状态**：Draft  
**目标读者**：Doris 内核开发（master）  
**决策目标**：明确 FE/BE 两侧对 WebUI 暴露的接口与语义，便于后续实现与联调  
**交付范围**：查看 + 打包下载（tar.gz）  
**参考实现**：FE QueryProfile 的查看/下载交互与接口  
- UI：[/workspace/ui/src/pages/query-profile/index.tsx](file:///workspace/ui/src/pages/query-profile/index.tsx)  
- FE：[/workspace/fe/fe-core/src/main/java/org/apache/doris/httpv2/controller/QueryProfileController.java](file:///workspace/fe/fe-core/src/main/java/org/apache/doris/httpv2/controller/QueryProfileController.java)  
 
---
 
## 1. 背景与目标
 
当前 Doris WebUI 已支持部分日志在线查看（FE 侧 Log 页面展示 `fe.warn.log` 末尾内容），但缺少通用的“日志文件列表 + 查看 + 下载/打包下载”能力，排障时仍需要登录机器手工拷贝文件。
 
本设计目标：
 
- 在 **FE WebUI（8030）** 新增 Tab：对 **当前 FE 进程** 的 `sys_log_dir` 日志目录提供文件列表、预览（tail）、下载与 tar.gz 打包下载。
- 在 **BE WebUI（8040）** 新增 Tab：对 **当前 BE 进程** 的 `sys_log_dir` 日志目录提供同等能力。
- 大文件（~1GB）场景下提供**限流**能力，并保证同一时刻**仅允许一个下载任务**，避免对 FE/BE 服务造成影响。
 
---
 
## 2. 非目标（Non-Goals）
 
- 不新增/改动鉴权与权限模型（假设进入 WebUI 后已具备访问权限）。
- 不支持在 FE 页面直接下载任意 BE 节点日志（FE 页仅覆盖当前 FE；BE 页仅覆盖当前 BE）。
- 不做断点续传（Range）与下载任务队列（忙时直接返回 busy）。
- 不保证对已压缩日志（如 `.gz`）的在线预览体验（下载/打包仍支持）。
 
---
 
## 3. 日志目录定位规则
 
FE/BE 日志根目录由配置项 `sys_log_dir` 决定；当其为空时回退到环境变量 `LOG_DIR`（现有代码已采用该规则）：
 
- FE：[/workspace/fe/fe-core/src/main/java/org/apache/doris/httpv2/controller/LogController.java](file:///workspace/fe/fe-core/src/main/java/org/apache/doris/httpv2/controller/LogController.java)  
- FE：[/workspace/fe/fe-core/src/main/java/org/apache/doris/common/Log4jConfig.java](file:///workspace/fe/fe-core/src/main/java/org/apache/doris/common/Log4jConfig.java)  
- BE：[/workspace/be/src/common/logconfig.cpp](file:///workspace/be/src/common/logconfig.cpp)  
 
本功能统一定义：
 
- `baseLogDir = sys_log_dir != "" ? sys_log_dir : getenv("LOG_DIR")`
- 仅允许访问 `baseLogDir` 下的文件/子目录（强制路径规范化，防止目录穿越）。
 
---
 
## 4. WebUI 设计
 
### 4.1 FE WebUI（React，8030）
 
新增路由与菜单 Tab（建议命名：`Log Files`），页面结构参考 QueryProfile：
 
- 路径选择（默认 `/` 表示 `baseLogDir` 根）
- 文件名过滤（search）、刷新按钮
- 文件列表 Table（复用现有 Table 组件的 filter/sort 能力）
- 预览区域（`<pre>`），展示 tail 内容（默认 1MB，可选 256KB/1MB/4MB）
 
表格列建议：
 
- Name
- Size
- Last Modified
- Type（file/dir）
- Action：View / Download / Add to Pack
 
下载交互：
 
- 单文件 Download：使用浏览器原生下载（`<a href=...>` 或 `window.location = ...`），避免 `fetch()->blob()` 将 1GB 文件加载到 JS 内存（QueryProfile 的 blob 下载仅适用于小文件，不直接复用）。
- 打包下载（tar.gz）：勾选多个文件后点击 `Download as tar.gz`，通过表单 POST 触发下载（避免 URL 过长）。
 
### 4.2 BE WebUI（Mustache + Bootstrap，8040）
 
BE WebUI 是内置页面框架（Mustache 模板 + navbar）。新增一个 navbar Tab（建议：`Log Files`），提供与 FE 相同的交互能力：
 
- 页面注册方式参考：[/workspace/be/src/service/http/web_page_handler.cpp](file:///workspace/be/src/service/http/web_page_handler.cpp)
- 文件下载能力可复用 BE 侧文件响应与限流机制（见第 6 节）。
 
---
 
## 5. 接口设计（核心）
 
### 5.1 FE 接口（Spring，`/rest/v1`）
 
接口风格对齐 QueryProfile（`/rest/v1/...`），并复用 FE 侧已有文件输出工具能力：
 
- 文件输出：[/workspace/fe/fe-core/src/main/java/org/apache/doris/httpv2/rest/RestBaseController.java](file:///workspace/fe/fe-core/src/main/java/org/apache/doris/httpv2/rest/RestBaseController.java)
 
#### 5.1.1 列目录
 
`GET /rest/v1/log_files`
 
Query 参数：
 
- `path`：相对 `baseLogDir` 的路径，默认空或 `/`
- `include_dir`：是否返回子目录（默认 true）
- `include_file`：是否返回文件（默认 true）
- `max_entries`：最大返回条目（默认 2000，超出截断并返回 `truncated=true`）
 
返回（JSON）：
 
```json
{
  "code": 0,
  "msg": "success",
  "data": {
    "base_log_dir": "/path/to/sys_log_dir",
    "path": "/",
    "truncated": false,
    "entries": [
      {"name":"fe.log","path":"/fe.log","is_dir":false,"size":123,"mtime_ms":1710000000000},
      {"name":"pipe_tracing","path":"/pipe_tracing","is_dir":true,"size":0,"mtime_ms":1710000000000}
    ]
  }
}
```
 
#### 5.1.2 查看（tail 预览）
 
`GET /rest/v1/log_file/view`
 
Query 参数：
 
- `path`：文件相对路径（必须是 file）
- `tail_bytes`：默认 `1048576`（1MB），允许范围建议 `64KB ~ 4MB`
 
返回：
 
- `Content-Type: text/plain; charset=utf-8`
- Body：tail 文本
 
错误码建议：
 
- 404：文件不存在
- 400：path 指向目录或参数非法
 
#### 5.1.3 单文件下载
 
`GET /rest/v1/log_file/download`
 
Query 参数：
 
- `path`：文件相对路径
- `download_name`：可选，默认使用原始文件名
 
返回：
 
- `Content-Disposition: attachment; filename="..."`
- 流式输出文件内容
- 触发“单下载并发限制 + 限流”
 
错误码建议：
 
- 429：当前已有下载任务进行中（Only one download at a time）
- 404/400 同上
 
#### 5.1.4 打包下载（tar.gz）
 
`POST /rest/v1/log_file/archive`
 
请求方式：
 
- `Content-Type: application/json` 或 `application/x-www-form-urlencoded`（前端用 form POST 触发下载更友好）
 
请求体（示例 JSON）：
 
```json
{
  "paths": ["/fe.log", "/fe.warn.log", "/audit.log"],
  "format": "tar.gz",
  "download_name": "fe_logs_2026-04-15.tar.gz"
}
```
 
约束建议：
 
- `paths` 仅允许 file；最大文件数默认 64
- 可配置最大总大小上限（例如 10GB）以防误操作
 
返回：
 
- `Content-Type: application/gzip`
- `Content-Disposition: attachment; filename="...tar.gz"`
- Body：tar.gz 数据流
 
错误码建议：
 
- 429：已有下载任务进行中
- 400：paths 为空、超过限制、包含目录或非法路径
 
### 5.2 BE 接口（HTTP action，`/api`）+ 页面（`/log_files`）
 
BE 侧已有高性能文件响应工具 `do_file_response()`，并支持传入 `bufferevent_rate_limit_group` 做链路限流：
 
- `do_file_response`：[/workspace/be/src/service/http/utils.cpp](file:///workspace/be/src/service/http/utils.cpp)  
- 限流组用法参考：[/workspace/be/src/service/http/action/download_binlog_action.cpp](file:///workspace/be/src/service/http/action/download_binlog_action.cpp)  
 
#### 5.2.1 列目录
 
`GET /api/log_files?path=/&max_entries=2000`
 
返回（JSON，建议）：
 
```json
{
  "status": "OK",
  "base_log_dir": "/path/to/sys_log_dir",
  "path": "/",
  "truncated": false,
  "entries": [
    {"name":"be.INFO","path":"/be.INFO","is_dir":false,"size":123,"mtime_ms":1710000000000}
  ]
}
```
 
#### 5.2.2 查看（tail 预览）
 
`GET /api/log_file/view?path=/be.INFO&tail_bytes=1048576`
 
返回：
 
- `Content-Type: text/plain; charset=utf-8`
- Body：tail 文本
 
#### 5.2.3 单文件下载
 
`GET /api/log_file/download?path=/be.INFO`
 
返回：
 
- `Content-Disposition: attachment; filename="be.INFO"`
- 使用 `do_file_response(abs_path, req, rate_limit_group)` 发送文件
 
#### 5.2.4 打包下载（tar.gz）
 
`POST /api/log_file/archive`（body 携带 paths）
 
返回 tar.gz 附件流，约束与 FE 同步。
 
#### 5.2.5 页面入口（BE WebUI Tab）
 
`GET /log_files`（mustache 模板渲染页面），页面内调用上述 `/api/...` 接口完成列表/预览；下载链接直连 `/api/log_file/download` 与 `/api/log_file/archive`。
 
---
 
## 6. 资源控制：单并发 + 限流
 
### 6.1 单下载并发（必须）
 
- FE：进程级全局 semaphore（容量=1），覆盖：
  - `/rest/v1/log_file/download`
  - `/rest/v1/log_file/archive`
- BE：同样的进程级全局 semaphore（容量=1），覆盖：
  - `/api/log_file/download`
  - `/api/log_file/archive`
 
返回策略：
 
- 无排队；获取不到 semaphore 则直接返回 429（可带 `Retry-After: 3`）。
 
### 6.2 下载限流（必须）
 
建议新增配置项（FE/BE 各自独立）：
 
- `web_log_download_rate_limit_mb_per_sec`（默认例如 20MB/s，0 表示不限流但仍保留单并发）
 
实现建议：
 
- FE：流式写出时按字节数做 token-bucket 或 RateLimiter 节流，避免一次性写满 socket buffer。
- BE：优先复用 `bufferevent_rate_limit_group`；打包下载若不走 `send_file`，也需在写出环节做同等节流。
 
---
 
## 7. 路径与输入校验（即使不做鉴权也必须做）
 
必须防止目录穿越（`../`、绝对路径、符号链接绕过等）。
 
建议规则（FE/BE 一致）：
 
- 仅接受 `path` 为相对 `baseLogDir` 的逻辑路径（如 `/fe.log`、`/subdir/x.log`）
- 服务端将 `baseLogDir + path` 做规范化（normalize/canonicalize），并校验最终路径仍位于 `baseLogDir` 内
- 对打包 `paths[]` 每个元素逐一校验，并限制数量
 
参考实现：
 
- FE 侧目录穿越防护样例：[/workspace/fe/fe-core/src/main/java/org/apache/doris/httpv2/rest/GetLogFileAction.java](file:///workspace/fe/fe-core/src/main/java/org/apache/doris/httpv2/rest/GetLogFileAction.java)  
- BE 侧 allowlist/canonicalize 防护样例：[/workspace/be/src/service/http/action/download_action.cpp](file:///workspace/be/src/service/http/action/download_action.cpp)  
 
---
 
## 8. 测试方案（建议）
 
### 8.1 FE
 
- Controller 单测：
  - 列目录返回结构、max_entries 截断
  - view tail_bytes 上下限
  - download/archive 路径穿越被拒绝
  - 单并发返回 429
- 参考测试风格：[/workspace/fe/fe-core/src/test/java/org/apache/doris/httpv2/GetLogFileActionTest.java](file:///workspace/fe/fe-core/src/test/java/org/apache/doris/httpv2/GetLogFileActionTest.java)
 
### 8.2 BE
 
- 单测/集成测试：
  - path 校验、目录穿越
  - list/view 基本正确性
  - download 返回 header 正确（Content-Disposition/Content-Length）
  - 429 busy 语义
 
---
 
## 9. 与现有功能的关系
 
- FE 现有 Log 页（UI：[/workspace/ui/src/pages/logs/index.tsx](file:///workspace/ui/src/pages/logs/index.tsx)，后端：[/workspace/fe/fe-core/src/main/java/org/apache/doris/httpv2/controller/LogController.java](file:///workspace/fe/fe-core/src/main/java/org/apache/doris/httpv2/controller/LogController.java)）保留不变：该页偏“日志配置/动态 verbose + 展示固定 warn log 尾部”。
- 新增的 Log Files 页专注“文件级日志浏览 + 预览 + 下载/打包”，两者互补。
 
---
 
## 10. 待确认点
 
- 接口路径命名是否保持本设计：
  - FE：`/rest/v1/log_files`、`/rest/v1/log_file/*`
  - BE：`/api/log_files`、`/api/log_file/*` + 页面 `/log_files`
- 预览默认 `tail_bytes=1MB` 是否合适，或希望改为“按行数”预览（需要定义 `tail_lines` 语义与实现成本）。
