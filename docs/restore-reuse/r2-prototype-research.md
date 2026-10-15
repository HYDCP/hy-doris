# R2 原型调研：L2 顺序无关逻辑摘要

> R2 原型阶段的前期调研。只读核对，没有改代码。
>
> - 代码：本仓库。BE 与 gensrc 相对基线 `14c957e048b` 无改动，BE 行号就是基线行号；FE 的 `RestoreJob.java` 在开发分支上有改动，涉及它的行号按基线（`git show 14c957e048b:...`）给出。
> - 路径简写：`be/` = `be/src/`，`fe/` = `fe/fe-core/src/main/java/org/apache/doris/`。
> - 调研日期：2026-09-28。

## 0. 结论速览

| 问题 | 结论 |
| --- | --- |
| 1 读取路径 | checksum 任务的读路径**不能直接复用**。它**没有把 MoW 的 delete bitmap 传给 reader**，靠 key 归并近似可见行，在部分列更新冲突重写、cluster key 等场景下与查询可见行不一致。它还会读出全部隐藏列，并且对 Duplicate 表也做了多路归并。能复用的是 capture 与加锁方式、READER_CHECKSUM 对 MoR 的 REPLACE、sequence 列与 delete sign 处理，以及删除条件的过滤。 |
| 2 规范化编码 | 现有列级哈希**不是单射**：NULL 与 0 哈希相同；ARRAY 不带长度；BITMAP/HLL/QUANTILE_STATE 的哈希是空实现；VARBINARY 调用会抛异常。必须自写逐类型编码器，可参考 `serialize_impl`（有 null 标志字节和长度前缀）的格式并把规范钉死。VARIANT 可复用现成的 `canonical_hash`。 |
| 3 哈希 | BE 已有 XXH3（xxHash 0.8.3，含 `XXH3_128bits`）、SipHash-2-4（可输出 128 位）、OpenSSL 1.1.1s（SHA-256，已有封装 `SHA256Digest`）。thirdparty 里**没有 BLAKE3 的 C 库**。原型建议用 XXH3-128，SHA-256 留作 `algo_version=2`。 |
| 4 挂载点 | 新增一种 agent task 类型（建议叫 `TABLET_LOGICAL_DIGEST`），不要复用 CHECK_CONSISTENCY。备份侧在 SNAPSHOTING 完成后、UPLOAD 前对 live tablet 的 (0,V] 计算；恢复侧对本地 tablet 在 visibleVersion V 上计算。读固定版本是安全的，前提是在头锁内抓到 rowset 并复制一份 delete bitmap 快照。唯一的时间窗是 stale rowset 的清理（默认 600 秒）。 |
| 5 成本 | 必须读全部列。主要开销是解压和解码，MoR 还有堆归并和逐行物化。Duplicate 与 MoW 可以完全免归并，并且能按 rowset/segment 并行。现成可用的只有"线程数"这一种限流手段；本地读路径**没有 IO 限速**，但有可以复用的 `IOThrottle` 类。 |
| 6 风险与范围 | 原型先做 Duplicate + MoW（带 bitmap、直读）和标量类型，再做 MoR 与 sequence 列，最后做复杂类型。VARIANT、BITMAP/HLL/QUANTILE_STATE/AGG_STATE、cluster key、sequence mapping、row binlog、冷却数据、云模式、Aggregate（包括 Duplicate 基表上的聚合型同步物化视图）一律延后或直接判为不支持并降级下载。 |

---

## 1. 读取路径

### 1.1 结论

1. **构造方式**：`EngineChecksumTask` 先持 tablet 头锁的读锁，用 `capture_consistent_rowsets_unlocked((0,V])` 抓一致的 rowset 集合，自己拼 `TabletReadSource`（只填 `rs_splits` 和 `delete_predicates`），`reader_type=READER_CHECKSUM`，schema 用 `tablet_schema_with_merged_max_schema_version`（全部列）。出锁后再 `BlockReader::init`，然后循环 `next_block_with_aggregation`，逐行把所有列喂给同一个 SipHash，最后折叠成 32 位。
2. **各模型的输出**（READER_CHECKSUM）：

| 模型 | BlockReader 分支 | 归并 | 删除条件（DELETE WHERE） | delete sign | sequence 列 | delete bitmap |
| --- | --- | --- | --- | --- | --- | --- |
| Duplicate | `_direct_next_block` | rowset 有重叠时仍走**堆归并**（只有 READER_QUERY 才关归并） | 生效 | 不适用 | 不适用 | 不适用 |
| Unique MoR | `_unique_key_next_block` | 堆归并，同 key 取最高版本；有 sequence 列时先比较 sequence 列 | 生效 | **生效**（归并后过滤） | 生效 | 不适用 |
| Unique MoR + sequence mapping（`has_seq_map`） | `_replace_key_next_block` | 按列组替换 | 生效 | **不过滤**（这个分支没有 delete sign 过滤） | 按映射生效 | 不适用 |
| Unique MoW | 同 MoR（`_unique_key_next_block`） | 堆归并 | 生效 | 生效 | 生效（在比较器里） | **不生效**：没有传 bitmap |
| Aggregate | `_agg_key_next_block` | 聚合 | 生效 | — | — | — |

3. **能否直接复用拿到"可见行"**：不能，差距有 5 处。
   - **MoW 没有应用 delete bitmap。** `capture_consistent_rowsets_unlocked` 其实已经返回了 `delete_bitmap`，但 checksum 任务只取了 `rowsets`，自己拼的 `TabletReadSource.delete_bitmap` 是空的，`set_read_source` 把空指针传了下去。于是 MoW 退化成"按 key 归并取最高版本"。这在大多数场景下与可见行等价，但**不是定义上的等价**：
     - 部分列更新在发布阶段的冲突重写，会把原行标删，并在同一 rowset 里新生成一行，同 key 同版本出现两行，只有 bitmap 能区分哪一行可见；
     - cluster key 表的 segment 按 cluster key 排序，按 unique key 堆归并的前提不成立（这是推断，需要 UT 证实；compaction 对 cluster key 表会显式带 bitmap，而且禁止水平合并，可以作为旁证）；
     - MoW 本来不需要归并，堆归并是纯额外开销。
   - **隐藏列全部读出**：`__DORIS_DELETE_SIGN__`、`__DORIS_VERSION_COL__`、`__DORIS_COMMIT_TSO_COL__`、`__DORIS_SKIP_BITMAP_COL__`、`__DORIS_ROW_STORE_COL__` 等都会进 Block，需要按第 2 节的规则挑选。
   - **Duplicate 做了多余的归并**：顺序无关的摘要不需要归并。
   - **sequence mapping 分支不过滤 delete sign**，首版不支持这类表。
   - **哈希与输出**：流式、顺序相关，只有 32 位，结果放在 `TFinishTaskRequest.tablet_checksum`（i64），没有分桶结构。
4. **MoW 的 delete bitmap 在读路径上从哪里生效**：`ReaderParams.delete_bitmap` → `RowsetReaderContext.delete_bitmap` → `BetaRowsetReader` 初始化时对每个 segment 调 `get_agg({rowset_id, seg_id, version.second})`，只聚合版本 ≤ V 的标记 → `SegmentIterator` 执行 `_row_bitmap -= delete_bitmap`。查询路径靠 `capture_read_source` 自动带上 bitmap（前提是 `need_read_delete_bitmap()`），并对 MoW 走 direct 模式，不归并；delete sign 由 FE 下推谓词过滤。
5. **原型读路径建议**：

| 模型 | 建议读法 |
| --- | --- |
| Duplicate | 不归并，逐 rowset/segment 直读；删除条件照常下推。 |
| Unique MoW | 在头锁内抓 rowset，同时取 `delete_bitmap().snapshot(V)`（与 make_snapshot 的做法一致），传给 reader；不归并直读；由摘要代码自己过滤 `__DORIS_DELETE_SIGN__ != 0` 的行。 |
| Unique MoR | 沿用 READER_CHECKSUM 的 `_unique_key_next_block`（归并 + sequence 列 + 归并后过滤 delete sign + 删除条件）。 |

   实现上可以新增一个 `ReaderType`（例如 `READER_LOGICAL_DIGEST`），在 `VCollectIterator::init` 和 `BlockReader::init` 两处让 Duplicate/MoW 走不归并的分支，MoR 保留归并分支。也可以绕开 BlockReader，直接用 RowsetReader 读 Duplicate/MoW。

### 1.2 证据

- checksum 构造 reader：`be/storage/task/engine_checksum_task.cpp:48-74`。其中 :56-63 自己拼 `TabletReadSource`，只调 `fill_delete_predicates()`，没有 bitmap；:68-70 用 merged max schema 生成全列的 read_schema。
- 头锁内 capture、出锁后 init、流式哈希、32 位折叠：`engine_checksum_task.cpp:117-129`、`:135`、`:141-152`。
- `set_read_source` 只会从 read_source 里 move bitmap：`be/storage/tablet/tablet_reader.h:119-127`。非查询读取默认不用 page cache：`tablet_reader.h:137-139`。
- capture 其实已经返回了 bitmap：`be/storage/rowset_version_mgr.cpp:143-145`。查询用的 `capture_read_source` 会带上 bitmap：`rowset_version_mgr.cpp:185-208`。`need_read_delete_bitmap()` 的判断：`be/storage/tablet/base_tablet.h:96-99`。`CaptureRowsetOps.include_stale_rowsets` 默认为 true：`base_tablet.h:476-480`。`TabletReadSource` 定义：`base_tablet.h:503-509`。
- 只有 READER_QUERY 会对 Duplicate 和 MoW 关闭有序读：`be/storage/tablet/tablet_reader.cpp:131-157`。bitmap 透传：`tablet_reader.cpp:183`。
- READER_CHECKSUM 打开 `_delete_sign_available`：`tablet_reader.cpp:447-456`。删除条件初始化：`tablet_reader.cpp:458-460`。
- 删除条件在 rowset reader 中无条件生效：`be/storage/rowset/beta_rowset_reader.cpp:134-135`。bitmap 按版本 V 聚合：`beta_rowset_reader.cpp:160-177`。在 segment 上生效：`be/storage/segment/segment_iterator.cpp:558-561`。
- BlockReader 分支选择：`be/storage/iterator/block_reader.cpp:606-630`。sequence mapping 只对非 MoW-query 启用：`block_reader.cpp:560-564`。
- `_unique_key_next_block` 与归并后的 delete sign 过滤：`block_reader.cpp:802-888`，其中过滤在 :850-886，:852 用 `DORIS_CHECK_GT(delete_sign_idx, 0)` 断言表里存在 delete sign 列。
- `_replace_key_next_block` 不过滤 delete sign：`block_reader.cpp:652-711`、`:718-743`。
- VCollectIterator 只对 READER_QUERY 关闭 Duplicate/MoW 的归并：`be/storage/iterator/vcollect_iterator.cpp:76-92`。`_skip_same`：`vcollect_iterator.cpp:135-136`。比较器依次比较 key、sequence 列、版本：`vcollect_iterator.cpp:215-246`、`:689-701`。
- 查询路径对 MoW 用 direct 模式：`be/exec/scan/olap_scanner.cpp:371-377`。查询路径 capture：`be/exec/operator/olap_scan_operator.cpp:1088`。
- compaction 只在 cluster key 或 row binlog 时带 bitmap：`be/storage/merger.cpp:144-149`、`:338-345`。cluster key 表禁止水平合并：`merger.cpp:106-109`。
- MoW 发布阶段的冲突重写（原行和新行都标删，并生成新行）：`be/storage/tablet/base_tablet.cpp:796-844`。sequence 列较小的新行被标删：`base_tablet.cpp:775-791`。
- 快照对 MoW 取 `delete_bitmap().snapshot(version)`：`be/storage/snapshot/snapshot_manager.cpp:657-663`。

---

## 2. 规范化编码

### 2.1 结论

**原则**：编码的第一要求是**单射**，也就是不同的可见值必须编出不同的字节。编码冲突会导致误判"相同"，这是不安全的方向。第二要求才是**规范**，也就是同一个值只有一种编码；做不到只会导致误判"不同"然后降级下载，属于安全方向。因此，凡是规范化没有把握的类型，宁可按存储字节严格比较。

**行编码框架（`algo_version=1`，草案）**：

- 列顺序：按 schema 顺序，只取"参与摘要的列"（见 2.3）。schema 签名（列 unique id、类型、精度/scale、长度、是否可空、参与的隐藏列集合）不逐行编码，放进摘要头，两端不等就不命中。
- 每列先写 1 字节标记：`0x00` 表示 NULL，`0x01` 表示非空。非空再写值的编码。
- 定长值用固定字节序（小端）。变长值先写 `u32` 长度再写字节。容器先写 `u64` 元素个数，元素递归按上面的规则编码。
- 行字节流喂给 XXH3-128（见第 3 节）。

**现有实现的问题**（不能直接当规范用）：

| 问题 | 现象 |
| --- | --- |
| Nullable 的 SipHash | NULL 写入 `update(0)`（4 字节 0），非空值不带标记。`Nullable(Int32)` 的 NULL 与 0 **完全相同**。 |
| Nullable 的 xxHash | NULL 等于 `XXH3(int 0, seed)`，与非空 Int32 值 0 **完全相同**。 |
| ARRAY 的 SipHash | 不写元素个数，`([1],[2,3])` 与 `([1,2],[3])` 冲突。 |
| BITMAP/HLL/QUANTILE_STATE | `update_hash(es)_with_value` 是空实现，**这几列对现有 checksum 不可见**。 |
| VARBINARY | 没有实现哈希，调用会抛 NOT_IMPLEMENTED。 |
| `serialize_impl`（聚合 key 用的序列化） | 有 null 标志字节、字符串 u32 长度、数组/Map 的 size_t 个数，**满足单射**，可以作为参考或原型的起点。但它是内部格式，可能随版本变化，也不覆盖复杂类型，正式规范要自己钉死。 |

**逐类型**：

| 类型 | 内存表示 | 同值多表示或冲突风险 | 建议规范 |
| --- | --- | --- | --- |
| BOOLEAN | `ColumnUInt8` | 存储里只有 0/1 | 1 字节 |
| TINY/SMALL/INT/BIGINT/LARGEINT | `ColumnVector` int8~int128 | 无 | 原宽度，小端 |
| FLOAT/DOUBLE | `ColumnFloat32/64` | -0/+0、NaN 的载荷位 | **按 IEEE 原始位编码，不做规范化**。查询能看到 -0 与 0 的区别；存储原样保留位模式，两端同源所以位一致。规范化反而会让两个查询可见值不同的数据被判为相同。 |
| DECIMALV2 | `ColumnDecimal128V2`，int128，内部 scale 固定为 9 | 列的精度/scale 只影响展示 | int128 小端；类型参数进 schema 签名 |
| DECIMAL32/64/128I/256 | `ColumnDecimal`，存非缩放整数，scale 在类型里 | 值本身没有 scale 歧义；scale 不同的两列比较没有意义 | 按类型原宽度写补码小端；精度和 scale 进 schema 签名 |
| DATE/DATETIME（v1） | `ColumnDate/ColumnDateTime` = int64 位打包的 `VecDateTimeValue`，含 `_neg`、`_type` 等位 | 原始 8 字节里有 `_type` 和 `_neg`，来自不同构造路径（存储读取、轻量 schema change 的默认值）时可能不一致 | **按字段编码** `(year u16, month, day, hour, minute, second)`，不用原始位 |
| DATEV2/DATETIMEV2 | `UInt32`/`UInt64` 位域，微秒 20 位；scale 在类型里 | 写入时已按 scale 截断 | 原始 u32/u64；scale 进 schema 签名。原型阶段加 UT 确认"scale<6 时低位微秒恒为 0" |
| TIMESTAMPTZ / TIMESTAMP_NS | `ColumnVector` u64/i64 | 无 | 原宽度 |
| TIMEV2 | Float64 | 只出现在计算里，不能作存储列 | 不涉及 |
| IPv4 / IPv6 | `ColumnVector` u32 / u128 | 无 | 原宽度 |
| CHAR | `ColumnString`；落盘补 `\0` 到定长，读取时各种编码的 pre-decoder 用 `strnlen` 去掉 | Doris 的 CHAR **不补空格**，补的是 `\0`；值里若本来就含 `\0` 会被截断，但两端一致 | 读出后再做一次"去掉结尾 `\0`"（幂等、防御性），**不去空格**；然后 u32 长度 + 字节 |
| VARCHAR/STRING | `ColumnString` | 无 | u32 长度 + 字节 |
| VARBINARY | `ColumnVarbinary` | 没有哈希实现 | u32 长度 + 字节（要自己实现） |
| JSONB | `ColumnString` 装二进制 JSONB | 键序是写入顺序，不同 JSON 文本、不同写入器版本可能编出不同字节 | **按原始字节严格比较**（u32 长度 + 字节）。不做键排序：compaction 只是拷贝值，两端同源时字节一致；不一致会降级下载，属安全方向 |
| ARRAY | `ColumnArray`（offsets + 嵌套列） | 现有 SipHash 不写长度 | `u64 个数` + 逐元素（元素也带 null 标记） |
| MAP | `ColumnMap`（offsets + keys + values） | 顺序是存储顺序，查询可见 | `u64 个数` + 按存储顺序写 (k, v)，不排序 |
| STRUCT | `ColumnStruct` | 无 | 按字段顺序逐字段编码（带 null 标记） |
| VARIANT | `ColumnVariantV2`，三种状态之一：编码字节（带元数据字典）、typed 标量列、shredded 树；拆列方式随 segment 和 compaction 变化 | **最高风险**：物理形态随布局变化。主线已有 `canonical_hash`，是与状态无关的逻辑 token 流，并且支持插拔 Sink。代价是数值会被规范化：整数域内的 float/double 会与整数判为相等，例如 `1` 与 `1.0`，这属于轻微的"不安全"方向 | 复用 `canonical_hash`，自己写一个 Sink 把 token 喂给 XXH3-128 流式状态。原型阶段**延后**，要单独做"compaction 前后与稀疏/typed 不同形态"的 UT，并评估数值规范化是否可以接受 |
| BITMAP | `ColumnBitmap`（`BitmapValue` 对象） | 内部有 EMPTY/SINGLE/SET/BITMAP32/64 以及 V2 变体；序列化受 `bitmap_serialize_version`、`enable_set_in_bitmap_value` 和 runOptimize 影响；现有哈希是空实现 | 规范化方案：基数 + 按升序遍历的全部元素。成本与基数成正比。**原型延后**，表里有这类列时 L2 判为不支持 |
| HLL | `ColumnHLL` | EMPTY/EXPLICIT/SPARSE/FULL 四种形态 | 规范化方案：展开成全量寄存器（每行 16KB，很贵）。**延后** |
| QUANTILE_STATE | TDigest | 合并顺序会影响内部状态，无法规范化 | **不支持** |
| AGG_STATE | 序列化的中间状态 | 只出现在 Aggregate 表 | **不支持**（Aggregate 本来就不在范围内） |
| NULL（TYPE_NULL） | `IColumnDummy` | 不是存储类型 | 不涉及 |

### 2.2 其他必须钉死的点

- **轻量 schema change 加的列**：老 rowset 里没有这列，读的时候按 schema 默认值填；compaction 之后变成物化值。两者逻辑一致，但要用 UT 覆盖"默认值是函数"或"日期类默认值"的情况。
- **按 bucket 对 tablet**：摘要按 tablet 比较（备份的第 i 个 tablet 对应本地的第 i 个）。因为桶和是可加的，分区摘要就是各 tablet 摘要之和，也可以只按分区比较。**建议按 tablet 比较**：行落到错误的 tablet 会让分桶裁剪的查询漏数，只按分区求和发现不了。
- **物化索引**：分区里的每个可见 index（rollup、同步物化视图）都要算摘要。如果 Duplicate 基表上挂了聚合型同步物化视图，那个 index 的 KeysType 是 AGG_KEYS，按 5.4 的结论不可证，整个分区就不能复用。

### 2.3 隐藏列取舍

| 隐藏列 | 读出时的值 | 建议 |
| --- | --- | --- |
| `__DORIS_DELETE_SIGN__` | 过滤之后恒为 0 | 用来过滤，不参与摘要 |
| `__DORIS_SEQUENCE_COL__` | 真实值，决定以后写入时谁胜出 | **参与摘要**：目标分区复用后还要继续接受写入 |
| `__DORIS_VERSION_COL__` | 单版本 rowset 在盘上存 0，读取时替换成 rowset 版本；compaction 之后是物化值 | 不参与（这是版本元数据，版本对齐由 L0 负责）；是否纳入作为待决项 |
| `__DORIS_COMMIT_TSO_COL__` | 读取时替换成 rowset 的 commit_tso，跨集群一定不同 | **必须排除** |
| `__DORIS_SKIP_BITMAP_COL__` | 灵活部分列更新的写入元数据，类型是 BITMAP | 排除 |
| `__DORIS_ROW_STORE_COL__` | 整行的 JSONB 冗余副本，编码依赖写入器 | 排除 |
| row binlog 相关列（TSO/LSN/OP） | — | 带 row binlog 的 tablet 首版不支持 |

### 2.4 证据

- 类型枚举：`be/core/data_type/define_primitive_type.h:29-78`。类型到列类的映射：`be/core/data_type/primitive_type.h:285-545`。
- IColumn 的哈希与序列化接口：`be/core/column/column.h:324-391`（`serialize_value_into_arena`、`serialize_impl`、`update_hash_with_value`、`update_hashes_with_value`），基类默认实现是抛 NOT_IMPLEMENTED。
- Nullable 的 SipHash，NULL 写 `update(0)`：`be/core/column/column_nullable.cpp:155-161`。`SipHash::update(const T&)` 按 sizeof 写入：`be/exec/common/sip_hash.h:174-178`。
- Nullable 的 xxHash：`column_nullable.cpp:125-137`、`:218-233`。`xxHash64NullWithSeed` 等于哈希 int 0：`be/util/hash_util.hpp:445-453`。
- ColumnVector 按原始字节哈希：`be/core/column/column_vector.cpp:139-160`。Decimal：`be/core/column/column_decimal.cpp:164-166`。
- ColumnString 的 SipHash 写 size_t 长度，xxHash 不写长度：`be/core/column/column_string.h:492-575`。
- ARRAY 不写长度：`be/core/column/column_array.cpp:361-366`。MAP 写 kv 个数：`be/core/column/column_map.cpp:335-344`。
- 复杂类型的哈希是空实现，注释说明是给 checksum 留的：`be/core/column/column_complex.h:195-205`。另见 `be/test/core/column/common_column_test.h:2090`。
- `serialize_impl` 满足单射的格式：Nullable 在 `column_nullable.cpp:337-345`，String 在 `be/core/column/column_string.cpp:489-496`，Array 在 `column_array.cpp:227-240`，Map 在 `column_map.cpp:250-262`。
- ColumnVarbinary 没有 `update_hash*`：`be/core/column/column_varbinary.h` 里只有 `serialize_impl`（:175）。
- `Block::update_hash` 逐行逐列虚调用：`be/core/block/block.cpp:905-911`。
- `VecDateTimeValue` 的位布局（`_neg`、`_type`）：`be/core/value/vdatetime_value.h:799-807`。DateV2/DateTimeV2 位域：`vdatetime_value.h:230-262`。
- CHAR 落盘补 `\0`：`be/storage/iterator/olap_data_convertor.cpp:554`。读时剥离：`be/storage/segment/binary_plain_page_char_strip_pre_decoder.h:31-35`、`be/storage/segment/encoding_info.cpp:480-510`、`be/storage/segment/column_reader.cpp:2531-2550`。
- VARIANT 的 canonical 哈希：`be/core/column/variant_v2/column_variant_v2.cpp:1618-1654`；接口、Sink 与数值规范化说明在 `be/core/value/variant/variant_canonical.h:36-95`；状态说明在 `be/core/column/variant_v2/column_variant_v2.h:106-113`、`:266-283`。checksum 读取 variant 走 compaction 同款路径：`be/storage/segment/variant/variant_column_reader.cpp:64-67`。
- BITMAP 的多种内部类型与受配置影响的序列化：`be/core/value/bitmap_value.h:59-90`、`:1452-1540`、`:2421-2466`。HLL 四种形态：`be/core/value/hll.h:52-73`。QUANTILE_STATE 用 TDigest：`be/core/value/quantile_state.h:45`、`:74`。
- 隐藏列名：`be/storage/utils.h:37-45`，`be/common/consts.h:27-28`。ReadSchema 对隐藏列的识别：`be/storage/schema.h:186-214`。
- VERSION_COL 在读取时替换：`be/storage/segment/segment_iterator.cpp:2461-2485`。COMMIT_TSO 在读取时替换成 rowset 的 commit_tso：`be/storage/segment/segment.cpp:157-162`、`:449-456`。

---

## 3. 哈希与向量化

### 3.1 结论

- **现成实现**：
  - 列级批量 64 位：`update_hashes_with_value`，用 XXH3-64 按列逐次以上一列结果为种子链式计算，原本用于 shuffle 分区；
  - 区间：`update_xxHash_with_value`；
  - CRC32/CRC32C 批量；
  - 单行 SipHash：`update_hash_with_value`，SipHash-2-4，可以 `get128` 取 128 位。
  
  这些实现都有第 2 节列出的单射问题，而且只有 64 位（SipHash 路径是逐行虚调用）。**不能直接拿来当摘要**。
- **能否得到每行 ≥128 位**：可以，但要自己组装。建议做法：
  1. 按列批量把每行的规范编码追加到每行的缓冲区里（向量化方式与 `IColumn::serialize(StringRef* keys, num_rows)` 相同：按列循环、按行追加，避免逐行虚调用）；
  2. 对每行缓冲调用 `XXH3_128bits_withSeed`，种子由 `algo_version` 固定；
  3. 用高 k 位定桶（k = 8～12），桶内对 128 位做模 2^128 的加法，同时累计行数（u64）。
  
  注意：定桶用的高 k 位在同一个桶内全都相同，所以桶和的有效熵约为 (128−k) 位。对非对抗场景足够。如果评审要求更高，可以改用两个不同种子的 XXH3-128 拼成 256 位，或者走 SHA-256 档。
- **性能判断**：XXH3-128 单核吞吐是 GB/s 以上量级，比解压和解码快一个数量级以上，所以哈希不会是瓶颈。逐行 SHA-256 在小行上开销明显（每次调用有固定开销，一行几十字节），只作为合规档。
- **thirdparty**：
  - xxHash 0.8.3：已链接，头文件 `xxh3.h` 已被 `hash_util.hpp` 引入；`XXH128` 已经在 `uniq` 聚合里用过；
  - OpenSSL 1.1.1s：提供 SHA-256，BE 已有封装 `SHA256Digest`；1.1.1 的 EVP 也提供 BLAKE2b-512 和 BLAKE2s-256，不过 BE 里没有封装；
  - **BLAKE3 没有 C/C++ 库**：只在 lance-c 补丁的 Rust Cargo 依赖里出现，不能直接用。如果要用，需要新增 thirdparty，不建议。
  - 另外还有 CityHash、MurmurHash3（`util/hash/`），都不需要。

### 3.2 证据

- `update_hashes_with_value`、`update_xxHash_with_value`、CRC 接口：`be/core/column/column.h:373-441`。各列的实现清单见第 2.4 节。
- `xxHash64WithSeed` 实际调用的是 `XXH3_64bits_withSeed`：`be/util/hash_util.hpp:445-447`；头文件引入：`hash_util.hpp:23-35`。
- SipHash 的 `get128`、`get64`：`be/exec/common/sip_hash.h:183-207`。
- 已有 XXH128 用法：`be/exprs/aggregate/aggregate_function_uniq.h:72`。
- thirdparty：xxHash 在 `thirdparty/vars.sh:496-500`；OpenSSL 在 `vars.sh:64-68`；CRC32C 在 `vars.sh:119-123`。BLAKE3 只出现在 `thirdparty/patches/lance-c-0.1.9-pr-77.patch:16`、`:127`、`:198`（Cargo 依赖）。
- SHA-256 封装：`be/util/sha.h:46`，`be/util/sha.cpp:65`。MD5 封装：`be/util/md5.h:29`。

---

## 4. 挂载点

### 4.1 结论

- **任务类型**：**新增** `TTaskType`（例如 `TABLET_LOGICAL_DIGEST = 36`），配独立的 worker pool 和配置项（例如 `logical_digest_worker_count`，默认 1～2）。请求里带 `(tablet_id, schema_hash, version, algo_version, 分桶参数)`；回包在 `TFinishTaskRequest` 里新增一个可选字段（例如 `binary logical_digest`，或者结构化的 `list<TDigestBucket>`，外加行数和算法版本）。**不复用 CHECK_CONSISTENCY** 的理由：
  - FE 收到 CHECK_CONSISTENCY 的完成回报后直接路由给一致性检查逻辑，要改的是它的分派；
  - 它的线程池默认只有 1 个线程，和夜间的一致性巡检抢资源；
  - 回包字段只有 i64。
  
  BE 侧的计算函数写成独立函数 `compute_logical_digest(tablet, V, params)`，备份和恢复两侧，以及 UT 都能调用。
- **备份侧**：
  - 时机：BackupJob 在 `waitingAllSnapshotsFinished` 转到 UPLOAD_SNAPSHOT 之前（或并行于上传），对 `snapshotInfos` 里每个 tablet 所在的那个 BE 下发摘要任务，对 **live tablet 的 (0,V]** 计算，V 是 PENDING 阶段固定的版本；
  - 为什么不直接读快照目录：快照目录不是已注册的 tablet，要从 header 和文件重建一个 Tablet 对象才能读，工作量大；
  - 快照和 live tablet 在 (0,V] 上是同一组 rowset（快照是硬链接），对 MoW 取的也是同一个 `delete_bitmap().snapshot(V)`，所以两者逻辑等价；
  - 另一个方案是把摘要计算合进 `make_snapshot` 回调：rowset 引用天然一致，但会拉长 SNAPSHOTING 阶段，而且 CCR 走 HTTP 快照（`KEEP_ON_LOCAL_REPO_ID`）时也会被迫付出这笔成本。**原型阶段先做独立任务**。
- **恢复侧**：非原子 A1 分支（分区已存在）上，对本地每个副本的 tablet 在 **visibleVersion V（等于备份版本）** 上计算，按 tablet 下标与备份摘要配对。建议对**每个保留的副本**都算：各副本分别被保留，单副本损坏也要能查出来。成本是副本数的倍数，可以作为档位参数。这时分区已经处于 RESTORE 状态、禁止写入，发布前再复查一次版本。
- **固定版本与并发**：
  - **读 (0,V] 是安全的**：在头锁内 `capture_consistent_rowsets_unlocked` 拿到 `RowsetSharedPtr` 之后，compaction 只会让这些 rowset 变成 stale；GC 要求 `use_count()==1` 才删文件，所以持有引用期间文件不会被删。新写入只会产生更高版本。
  - **时间窗 1**：备份侧如果在快照之后很久才发摘要任务，而这期间有新写入、并且 compaction 跨过了 V、并且 stale 版本已被清理（默认 600 秒），那么 capture (0,V] 会返回 `VERSION_ALREADY_MERGED`。处理方式是摘要缺失、以后不能复用（安全方向）。缓解办法是快照完成后立即下发，或者把计算合进快照回调。
  - **时间窗 2**：checksum 任务在出锁后才 `reader.init`，bitmap 是在 init 时 `get_agg` 的，而传进去的是 live 的 `delete_bitmap_ptr()`。如果中间有 stale 清理、删掉了 bitmap 条目，会读到本应被删的行（结果是不一致、降级，属安全方向）。**建议在头锁内取 `delete_bitmap().snapshot(V)` 得到一份不可变副本**，做法与 make_snapshot 相同。
  - 冷却到远端的数据：make_snapshot 本身就不支持，摘要也排除。

### 4.2 证据

- 任务类型枚举：`gensrc/thrift/Types.thrift:217-259`。`TCheckConsistencyReq`：`gensrc/thrift/AgentService.thrift:413-418`。`TSnapshotRequest`：`AgentService.thrift:450-467`。`TFinishTaskRequest.tablet_checksum`（i64）：`gensrc/thrift/MasterService.thrift:59-81`（:66）。
- BE 的 worker 注册：`be/agent/agent_server.cpp:124-125`（CHECK_CONSISTENCY）、`:133-134`（MAKE_SNAPSHOT）。回调：`be/agent/task_worker_pool.cpp:1053-1083`（checksum）、`:1418-1468`（make_snapshot）。线程数：`be/common/config.cpp:251`（check_consistency=1）、`:253`（upload=1）、`:258`（make_snapshot=5）。
- FE 把完成回报路由给一致性检查：`fe/master/MasterImpl.java:212-213`、`:603`。一致性检查的时间窗与超时：`fe-common/.../Config.java:750-759`。
- BackupJob：快照完成回报在 `fe/backup/BackupJob.java:280-335`；全部快照完成后转 UPLOAD_SNAPSHOT 在 `:756-770`；本地仓库（CCR）直接进 UPLOADING 在 `:775-779`；版本固定与下发快照在 `:536`、`:690-705`。
- 恢复侧本地 tablet 与备份 tablet 按下标绑定：`RestoreJob.java`（基线）`:1232-1297`。A1 分支：基线 `:790-829`、`:1391-1414`。
- 快照 capture (0,V]：`be/storage/snapshot/snapshot_manager.cpp:635`；bitmap 快照：`:657-663`；硬链接：`:670`；冷却数据分支：`:609-632`。
- GC 的 use_count 保护：`be/storage/storage_engine.cpp:1270-1285`。stale 清理时间：`be/common/config.cpp:396`（`tablet_rowset_stale_sweep_time_sec=600`）；清理实现：`be/storage/tablet/tablet.cpp:723-745`。
- capture 默认包含 stale rowset：`base_tablet.h:476-480`；capture 失败即返回错误：`rowset_version_mgr.cpp:149-150`。
- 快照返回 `VERSION_ALREADY_MERGED` 会导致 BackupJob 取消：`BackupJob.java:284-290`。

---

## 5. 成本

### 5.1 结论

- **必须读全部列**：摘要覆盖整行，只读 key 列无法发现 value 列的差异。唯一能省的是第 2.3 节排除的隐藏列。宽表可以借鉴 compaction 的垂直合并（先按 key 列组归并得到行来源序列，再逐个 value 列组读取），但那需要把"逐列哈希按行来源重新组装"做出来，原型阶段不做。
- **主要开销（按量级从大到小）**：
  1. 读盘、解压、页解码：字符串、JSONB、VARIANT 最重；非查询读取不用 page cache，不会污染缓存；
  2. **MoR 的堆归并**：逐行比较 key 的虚调用，加上 `_unique_key_next_block` 里逐行逐列的 `insert_from` 物化；
  3. 规范编码与每行哈希：向量化之后排在最后。
  
  现有 checksum 对 MoW 和 Duplicate 也做了 1、2 两项，**改成直读能省掉整段归并**。
- **可并行**：摘要是可加的，所以 Duplicate 和 MoW 可以按 rowset 或 segment 切分给多个线程，最后把各桶相加。MoR 只有在 rowset 之间不重叠（例如 full compaction 之后只剩一个 rowset）时才能免归并。
- **限流与线程池**：
  - 没有现成的"后台扫描限速"：本地文件读路径忽略 `io_ctx`，没有接限速；workload group 的 `IOThrottle` 只接在查询扫描路径上；
  - 可以复用的积木：`IOThrottle` 类本身是独立的（有 `acquire`、`update_next_io_time`、`set_io_bytes_per_second`），能在摘要循环里按读取的字节数手动计量；
  - 另外可以用独立 worker pool 的线程数控制并发，用 MemTrackerLimiter 挂内存（checksum 的做法），在 FE 侧沿用一致性检查的时间窗与"最久未查优先"。
- **可以直接复用的实测脚手架**：`enable_compaction_checksum`（默认关）会在 compaction 前后各跑一次 `EngineChecksumTask` 并比较结果。把新的摘要函数挂到同一个位置，可以在集群环境里低成本验证"compaction 前后摘要一致"，还能顺带测扫描耗时。

### 5.2 证据

- 非查询读取默认不用 page cache：`tablet_reader.h:137-139`。非查询读取标记为 disposable：`be/storage/rowset/beta_rowset_reader.cpp:214-223`。
- 归并与逐行物化：`block_reader.cpp:802-848`、`:890-900`（`_insert_data_normal`）。Level1Iterator 归并：`vcollect_iterator.cpp:689-715`。
- 本地文件读路径不使用 io_ctx：`be/io/fs/local_file_reader.cpp:150-151`。`IOThrottle` 接口：`be/runtime/workload_management/io_throttle.h:29-55`。workload group 的扫描限速接口：`be/runtime/workload_group/workload_group.h:182-186`。
- checksum 的内存跟踪：`engine_checksum_task.cpp:86-88`；挂载方式：`task_worker_pool.cpp:1058`。
- compaction 前后 checksum：`be/storage/compaction/compaction.cpp:703-756`；开关：`be/common/config.cpp:458`。
- compaction 使用的垂直合并先例：`be/storage/merger.cpp:300-350`。

---

## 6. 风险清单与原型范围

### 6.1 风险（按可能性 × 影响排序）

1. **MoW 可见行语义**（最高）：现成的 READER_CHECKSUM 没有带 bitmap，靠归并近似。这在常见场景下"碰巧对"，测试容易通过，但在部分列更新冲突重写、sequence 列与部分列更新组合、cluster key 下会出错。**原型必须改成"bitmap 快照 + 直读 + 自己过滤 delete sign"**，并且专门构造这几种场景的 UT，对照"同一数据经过 full compaction 后"的摘要。
2. **编码单射性与隐藏列**：
   - 如果图省事复用 `update_hash(es)_with_value`，会遇到 NULL 与 0 冲突、数组没有长度、复杂列不参与哈希，这些都是误判"相同"的**不安全**方向；
   - 如果漏排 `__DORIS_COMMIT_TSO_COL__`、`__DORIS_ROW_STORE_COL__` 这类跨集群一定不同或依赖写入器的列，会导致摘要永远不一致，复用率变成 0。
   - 应对：编码规范写进 `algo_version=1`，每个类型配"单射"与"布局无关"两组 UT。
3. **备份侧版本固定的时间窗与覆盖面**：
   - 快照之后 (0,V] 可能被合并掉（stale 清理 600 秒），摘要就缺失；
   - 物化索引（rollup/同步物化视图）也要算，聚合型同步物化视图会让整个分区失去资格；
   - 每个副本都要算，恢复侧的成本随副本数成倍增加。
4. **VARIANT**：物理形态随布局变化，canonical 路径虽然存在，但有数值规范化（整数与浮点合并），而且代码还在演进，版本兼容风险大。
5. **性能不达标**：MoR 的归并与物化、单线程池、没有 IO 限速，可能导致扫描带宽低于有效恢复带宽。要尽早在集群环境上分别测 Duplicate/MoW 直读与 MoR 归并的 MB/s。
6. **跨版本 BE**：备份端和恢复端 BE 版本不同时，读路径行为（CHAR 剥离、VERSION_COL 替换、variant 规范化）可能不同。`algo_version` 加上摘要头里的 BE 版本号，不一致时判为不命中。
7. **隐含前提**：Unique 表必须有 delete sign 列（`DORIS_CHECK_GT(delete_sign_idx, 0)`）。关闭批量删除的老表会让读路径报错，要先检查、判为不支持，不能让 BE 崩掉。

### 6.2 建议的原型范围

| 批次 | 模型 | 类型 | 说明 |
| --- | --- | --- | --- |
| P1 | Duplicate；Unique MoW（非 cluster key） | 全部整数、BOOLEAN、FLOAT/DOUBLE、DECIMALV2/V3 全宽度、DATE/DATETIME v1/v2、TIMESTAMPTZ、CHAR/VARCHAR/STRING、IPv4/IPv6，全部可空 | 直读 + bitmap 快照 + delete sign 过滤；分桶求和；UT 覆盖：compaction 前后、不同 rowset 切分、改一行/删一行/多一行重复行、部分列更新冲突重写 |
| P2 | Unique MoR；带 `sequence_col` 的 MoR/MoW | 同 P1，加 JSONB（原始字节）、VARBINARY、ARRAY/MAP/STRUCT | 沿用 READER_CHECKSUM 的归并分支；sequence 列参与摘要；测扫描带宽 |
| 延后 | cluster key MoW；sequence mapping（`has_seq_map`）；row binlog tablet | VARIANT | cluster key 在直读方案下理论上可行，需要单独验证；VARIANT 基于 `canonical_hash` 做 |
| 不支持（判为不可证，降级下载） | Aggregate，含聚合型同步物化视图；冷却/远端数据；云模式 | BITMAP、HLL、QUANTILE_STATE、AGG_STATE | BITMAP 以后可以按"有序元素流"补上 |

原型交付物对应 D3：一个与 FE 无关的 BE 函数 `compute_logical_digest` 及其 UT；一个通过 `enable_compaction_checksum` 同款钩子挂在 compaction 前后的调试开关，用于集群验证；Duplicate/MoW/MoR 三类表的扫描带宽实测表。
