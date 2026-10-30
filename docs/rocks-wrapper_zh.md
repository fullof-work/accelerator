[English](rocks-wrapper.md) | [简体中文](rocks-wrapper_zh.md)

# Cache RocksDB Wrapper

私有 binding 位于 `pkg/cache/rocks/internal/native`，仅实现缓存存储需要的固定四个
Column Family、打开、pinned 读取、写入、flush、compaction、属性和频率过滤。
RocksDB 版本和构建仍由原有 `deps/build-rocksdb.sh` 管理，wrapper 不新增 native
依赖构建流程。

## 公开引擎接口与所有权

RocksDB 9.7.4 的 C API 提供 pinned 读取，但不提供 `FilterBlobByKey`，也没有公开
转换接口让 opaque Options 接受自定义 C++ filter。因此，最小 C++ bridge 通过公开
头文件直接持有 DB，不转换 C handle 布局、不引用内部头文件、不全局注册 factory，
也不提供通用 Go 对象模型。

| 对象 | 生命周期 |
| --- | --- |
| DB 和四个 CF handle | 由一个 native owner 持有。打开部分失败时释放所有已返回的 handle 和 DB；先释放 handle，再关闭 DB。 |
| Options 和表配置 | Open 的局部对象，DB 复制 Options；共享表工厂、LRU 和 Bloom policy 的引用随 DB 生命周期保留和自动释放。 |
| ReadOptions / WriteOptions | 由 native owner 初始化一次，供并发同步操作共享；数据 CF 不写 WAL，default CF 保留 WAL。不再使用存放 native 对象的 `sync.Pool`。 |
| PinnableSlice | 仅限一次 Get；Go bridge 复制到现有 BlobPool 后释放 pin，panic 路径也会清理。native 视图不进入网络发送阶段。 |
| Compaction filter | 每个 DB 一个，由三个数据 CF 借用，存活至 DB 关闭、析构和后台 callback 结束之后。 |
| Go callback | 一个 `runtime/cgo.Handle`，在打开失败或 DB 析构完成后删除；C++ 只保留整数。callback panic 会保留 key 并记录日志。 |
| 错误和属性字符串 | 同一次 bridge 调用中复制到 Go 后释放。 |

存储的生命周期锁先排空完整请求，再开始释放。Close 随后等待 Sketch 后台任务退出、
持久化最终 Sketch。关闭可写 DB 前，native owner 显式等待三个数据 CF 分别 Flush
完成；保留首个 Flush 错误，同时仍关闭 DB 并释放全部 handle。最终 Sketch 持久化
错误也会返回调用方。只读 Close 跳过 Flush。并发 Close 等待同一个结果；关闭后
请求返回存储已关闭错误，缺失 key 仍是普通缓存未命中。

## 配置与持久化

- CF 名称仍为 `default`、`chunk`、`manifest`、`blob`；未知 partition 仍回退
  到 chunk。Object key 为 32 字节，Shard key 追加 `0x00`。Value、Shard 前缀和
  空 value 按未命中处理的行为不变。
- `__freq_sketch__` 仍位于 default CF，保持版本 2 编码。只恢复 active CMS；
  previous generation、访问次数进度和成功写入字节进度不持久化。default CF
  不参与频率过滤。
- `disk_bytes` 默认 1 TiB，通过 `mem_ratio`（默认 0.01，cache 最小 64 MiB）
  决定 block cache 大小，同时作为每个 Sketch generation 的成功 value 字节
  预算来推进软老化，不代表物理磁盘用量或存活数据大小。三个数据 CF 都参与，
  包括 Shard 前缀和重复写入；失败写入、读取及 default CF 持久化不增加字节。
  次数、字节或显式配置的定时器触发的每次轮换都会清零两种进度，单次写入最多
  轮换一代。淘汰由自然 compaction 执行，不新增空闲维护或强制 compaction，
  也不是磁盘配额。
- Block 默认 64 KiB，Bloom bits 默认 15；L0 index/filter pinning 和 index/filter
  cache 保持启用。序列化的 Bloom 描述现在包含参数，而不是原 C 包装层的通用名称。
- Direct reads 仍默认开启，flush/compaction 的 Direct I/O 行为不变。
- `write_buffer_bytes` 默认 256 MiB，四个写缓冲仍只作用于 default CF；数据 CF
  保留引擎默认值。后台 compaction 默认 8，后台 flush 为 2。
- 数据 CF 保留 BlobDB、4 KiB inline 阈值、256 MiB Blob 文件和 GC；压缩仍关闭，
  chunk、manifest 和 blob 写入固定禁用 WAL，default/Sketch 保留原 WAL 和
  sync 设置。这些是内部策略，不增加配置选项。
- 只读打开仍为容忍 WAL 文件的 `OpenForReadOnly`，可能向内存回放 WAL，不是
  secondary instance，也不创建 Sketch。
- Inline 过滤与 `FilterBlobByKey` 查询同一个 Sketch，只决定保留或删除。
  忽略 snapshot 的语义不变；GC 搬迁存活 Blob 时仍可能需要读取 value。

正常 Close 会 Flush 仍存活的数据 CF 写入并报告失败。非正常进程退出可能丢失
尚未 Flush 的缓存值，重开后变为可重新填充的普通 miss。default CF checkpoint
仍有 WAL，但没有启用逐次写入 sync：Fill 成功或未 Flush 的 checkpoint 不承诺
已同步持久化到能够抵御掉电的程度。`freq.disable_eviction` 只改变过滤策略，
不改变上述持久性边界。

## 验证命令

使用 `make test`、`make test-no-rocksdb`、`make test-release` 和原有架构/发布
目标。执行 `make deps-rocksdb` 后可运行专项测试：

```sh
export CGO_CPPFLAGS="-I$PWD/build/$(uname -m)/rocksdb/include"
export CGO_LDFLAGS="-L$PWD/build/$(uname -m)/rocksdb/lib -lrocksdb -lstdc++ -lm -lpthread -ldl"
go test -race -count=3 ./pkg/cache/rocks/...
GOEXPERIMENT=cgocheck2 go test ./pkg/cache/rocks/...
bash test/scripts/test_rocks_native.sh
```

最后一条命令需要 Valgrind，在不加载 Go runtime 的情况下测试真实 bridge 和
RocksDB 库，包括锁冲突、部分打开失败和反复打开/关闭。Go 测试覆盖 callback
生命周期、panic 恢复、并发关闭、损坏读取、只读/重开、BlobGC 和 inline/Blob
淘汰。恢复测试在正常 Close 与 SIGKILL 后分别核对已 Flush 前缀和未 Flush 尾部，
验证最新 Sketch checkpoint 的 WAL 恢复，以及 Flush 失败会返回且不遗留 DB 锁。
私有可选引擎计数器用于证明冷 Blob 过滤不读取 value；生产仍关闭 Statistics。
