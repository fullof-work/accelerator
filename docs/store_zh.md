[English](store.md) | [简体中文](store_zh.md)

# store — 持久层读写代理

`store-ctl` 向 `manifest-ctl` 和 cache origin 提供统一的持久层数据面。对象数据可
位于本地/共享文件系统或 S3-compatible object storage；generation 列表则可独立
来自主配置、普通文件或单独的 S3 object。

## 1. 核心模型

generation 只有一种表示：

```go
[]store.Generation // oldest -> newest
```

最后一项接收新写入，读取从最后一项向前查找。列表不按名称排序，也不自动去重。
名称必须匹配 `[A-Za-z0-9][A-Za-z0-9._-]{0,127}`；空列表、重复项、`.`、
`..`、路径分隔符、控制字符和过长列表都会被拒绝。

`AdmitWrite` / `AdmitWriteFor` 返回：

```go
type WriteAdmission struct {
    Generation store.Generation
    Salt       [32]byte
}
```

它只是路由和加密派生信息，不是凭据。空请求的 `AdmitWrite` 选择当前列表最后一项；
`AdmitWriteFor(generation)` 请求列表中仍存在的指定项。一次 manifest ingest 只调用
一次 admission RPC，全部 chunk Put 和最后的 manifest Put 都复用同一个 admission。
generation 仍在当前列表中时 admission 可继续写入；移出列表后，该 generation
不再接受 Put，也不再参与 Get。

generation salt 只有一个公共实现：

```go
salt, err := store.SaltForGeneration(generation)
```

Store server、离线 Manifest Bundle writer 和测试都调用它，不复制 derivation
domain 常量。函数先执行 `ValidateGeneration`；`NONE` 等任何合法字符串都按普通
generation 处理，没有保留值或特殊分支。

## 2. 配置

### 2.1 FS 数据后端

```yaml
listen: 127.0.0.1:7100
backend: fs
stats_interval: 30s
cache_listen: ""
fs:
  root: /var/store
  verify_content_key: true
  direct_io: false
```

`direct_io` 缺省为 `false`，只影响对象 Get；generation 文件和主配置始终使用
普通 I/O。

### 2.2 S3 数据后端

```yaml
listen: 127.0.0.1:7100
backend: s3
s3:
  endpoint: https://example-s3-endpoint
  region: us-east-1
  bucket: kuasar-store
  path_style: true
  prefix: production
  access_key: ${S3_ACCESS_KEY}
  secret_key: ${S3_SECRET_KEY}
  verify_content_key: true
  max_inflight: 64
  op_timeout: 10s
  max_object_size_bytes: 16777216
  tls:
    ca_cert: ""
    insecure_skip_verify: false
```

静态 AK/SK 必须成对出现；两者都为空时使用 AWS SDK 默认凭据链。字符串字段支持
`${VAR}` 展开。`region` 缺省为 `us-east-1`，`path_style` 缺省为 `true`。

`tls.ca_cert` 指向 PEM CA bundle 路径（可含多张证书），追加进系统信任库——用于
信任 CA 不在系统库中的 endpoint（或拦截代理）。`tls.insecure_skip_verify: true`
完全跳过 TLS 证书校验；不安全——流量（含凭据）可能被截获——仅供测试，勿用于
生产。两者互斥。二者都只作用于所配置 endpoint 的对象流量；SDK 凭据链的身份请求
（instance role、web identity、SSO）始终使用系统信任库，全拦截网络下需要把 CA
装入系统库或改用静态 AK/SK。

### 2.3 Generation source

`generations` 下必须且只能选择一种来源。

主配置内列表：

```yaml
generations:
  config:
    - G1
    - G2
    - G3
```

文件：

```yaml
generations:
  refresh_interval: 5s
  file:
    path: /var/store-meta/generations
```

独立 S3 object：

```yaml
generations:
  refresh_interval: 5s
  s3:
    endpoint: https://example-s3-endpoint
    region: us-east-1
    bucket: kuasar-store-meta
    key: production/__meta/generations
    path_style: true
    access_key: ${GENERATION_S3_ACCESS_KEY}
    secret_key: ${GENERATION_S3_SECRET_KEY}
    tls:
      ca_cert: ""
      insecure_skip_verify: false
```

`generations.s3.tls` 独立于数据后端的同名设置生效；省略 `generations` 段时，
后端本地的 legacy S3 位置继承数据后端的设置。

文件和 S3 object 都使用逐行文本格式，顺序为 oldest → newest。它们按
`refresh_interval` 周期刷新；`SIGHUP` 也会立即刷新。config source 在
`SIGHUP` 时重读主 YAML，但不会动态改变 backend、listen、凭据或其他配置。
只有完整加载、解析和校验成功的新列表才会替换当前不可变 slice；失败时记录错误
并继续使用上一份有效列表。启动时没有有效列表则拒绝启动。

未配置 `generations` 时保持旧部署兼容：

- FS：`<fs.root>/__meta/generations`
- S3：`<s3.prefix>/__meta/generations`

对象数据权限与 generation source 权限可以分离。例如数据节点可使用只读
generation 凭据和对象读写凭据，而不需要修改 generation object 的权限。

### 2.4 `verify_content_key`

`true`（默认）时，服务端对本次上传流计算 SHA-256，digest 必须等于请求 key。
校验失败只清理当前 writer 仍拥有的文件。已有目标的 dedup 仍只按 optional size
或存在性判断，不重新读取、哈希已有对象。

`false` 时不校验上传内容：

- 提供 size：实际上传大小必须等于 size；已有目标大小相等即 dedup。
- 未提供 size：已有普通文件/对象存在即 dedup；新上传不做预期大小校验。

因此，同大小错误内容无法被识别；未提供大小时，残留或不完整文件也可能被视为
存在。这是关闭内容校验后的明确取舍。

## 3. RPC 与 Backend

Store gRPC 提供：

```protobuf
rpc AdmitWrite(AdmitWriteRequest) returns (AdmitWriteResponse);
rpc Get(GetRequest) returns (stream GetResponse);
rpc Put(stream PutRequest) returns (PutResponse);
```

`AdmitWriteRequest.generation` 为空时保持选择最新可写 generation 的原语义；非空时
名称非法时返回 `InvalidArgument`;名称有效但不在本请求读取的当前列表中时返回
`FailedPrecondition`。服务端 generation 列表为空时返回 `Unavailable`。成功响应始终返回该 generation 的 canonical
`WriteAdmission`。官方 Go client 保留 `AdmitWrite(ctx)`，并新增
`AdmitWriteFor(ctx, generation)`；Put wire 与 Backend 接口不变。

`PutHeader` 包含 partition、32-byte key、generation 和真正有 presence 的
`optional uint64 size`。零表示已知的空对象；字段缺席才表示大小未知。官方 Go
client 接收完整 `[]byte`，所以总是发送 `uint64(len(data))`。服务端转换为
`int64` 前检查溢出。

数据 backend 只操作显式传入的单个 generation：

```go
type Backend interface {
    Get(ctx context.Context, generation store.Generation,
        partition store.Partition, key store.ContentKey) (bool, []byte, error)

    Exists(ctx context.Context, generation store.Generation,
        partition store.Partition, key store.ContentKey,
        expectedSize *int64) (bool, error)

    OpenPut(generation store.Generation, partition store.Partition,
        key store.ContentKey, expectedSize *int64) (store.PutHandle, error)
}
```

`OpenPut` 会复制 optional size，并固定 generation、partition 和 key。handle 在
Commit 时不重读列表、不重新选择 generation 或目标路径；Commit 传入的 key 与
绑定 key 不同会被拒绝。

Server Put 顺序如下：

1. 确认 admission generation 仍在本请求读取的当前列表中。
2. 解析 optional size 并调用 `Exists`。
3. 命中时立即返回 `is_new=false`，不接收 payload。
4. miss 时调用 `OpenPut`，累计实际字节数。
5. 提供 size 时，超过预期立即 Abort，EOF 时要求精确相等。
6. 按配置完成流式 digest 校验并 Commit。

FS `Exists` 只接受普通文件。已知 size 时必须完全相等；未知 size 时普通文件存在
即可。NotExist 是 miss；symlink、目录、特殊文件、permission、EIO、ESTALE 等
均返回错误。S3 使用调用方 context 执行 HEAD；NotFound 是 miss，其他远端错误
原样传播，已知 size 时比较 Content-Length。

gRPC Get 和内嵌 `cache_listen` 共用同一个反向查找入口。每个请求只读取一次
generation slice：

```go
for i := len(generations) - 1; i >= 0; i-- {
    found, data, err := backend.Get(ctx, generations[i], partition, key)
    if err != nil || found {
        return found, data, err
    }
}
```

### 3.1 单次读取与写入边界

Store `Get` 只执行一次业务读取。失败 stream 会被取消/释放，后续调用从头开始完整对象读取；已确认缺失与 transport/access failure 保持不同语义。现有 endpoint 与 operation timeout 约束该次尝试，后续重试/退避由调用者 context 负责。

读取合同从不授权重放 `Put`、Fill、ingestion、发布或快照捕获；响应丢失不能证明写入未生效。直接 CLI 读取把单次尝试错误交给命令所有者。只有 Store 掌握已确认缺失、完整性/格式失败等语义边界时才设置 `pkg/readerr` marker；不透明访问错误、网络 EOF/半流和后端内部取消保留原始原因。

## 4. FS direct-final-write

对象路径为：

```text
<root>/<partition>/<generation>/<aa>/<bb>/<content-key>
```

`OpenPut` 直接创建该最终路径：

1. 创建父目录并用 Lstat 检查目标。
2. 目标按 optional size 规则有效时返回 no-op handle；其 Write 接收并丢弃数据，
   Commit 返回 `isNew=false`。
3. 已知 size 且现有普通文件大小不符时记录文件 identity；删除前再次 Lstat，只有
   路径仍指向同一文件才删除，否则重新检查。
4. 使用 `O_WRONLY | O_CREAT | O_EXCL` 和 `O_NOFOLLOW` 防护直接创建最终文件。
   `EEXIST` 表示竞争，回到目标检查。

直接写 handle 记录最终路径、创建后的 identity、generation、partition、key、
optional size 和实际字节数。Write 直接写最终 fd，已知 size 时禁止越界。

Abort、digest mismatch、长度错误以及 Write/Sync/Close 失败，都先检查路径是否仍
指向 handle 创建的同一文件；只有 owner 才能删除。成功 Commit 执行文件 Sync、
Close 和父目录同步，然后再次确认 identity。如果路径已被另一 writer 替换：

- 替换目标满足 optional size/存在规则：loser 返回 `isNew=false`。
- 替换目标仍无效、消失或无法确认：loser 返回错误。
- loser 不删除另一 writer 的文件。

该并发判定不依赖进程内全局锁，适用于多个 writer 竞争同一个 content key。

### 4.1 可见性与故障边界
direct-final-write 不提供 crash-atomic publication，也不作等价承诺：

- 最终路径在写入期间可见，大小可能暂时不完整。
- 官方 client 始终发送 size，因此未完成大小通常不会成为后续 Put 的 dedup hit。
- manifest ingest 先完成全部 chunk Put，最后才发布 manifest；正常 reader 在引用
  发布前不会请求新 chunk。
- 崩溃留下的大小不一致文件可由后续同 size Put 识别并覆盖。
- 恰好达到完整大小但尚未持久化的残留，仅凭大小无法区分。
- 未提供 size 时，不完整文件也可能被当作存在。

这些边界来自“最终路径直接写入且不增加额外对象完成标志”的设计选择。

## 5. FS Direct I/O

Linux `direct_io: true` 时，对象 Get 使用 `golang.org/x/sys/unix` 打开
`O_DIRECT` fd，优先通过 `statx(STATX_DIOALIGN)` 获取 memory/offset alignment。
buffer 地址、offset 和读取长度都按要求对齐，返回前裁剪为文件真实长度；实现处理
空文件、小文件、非对齐长度、短读和 EOF。

filesystem 不报告有效 alignment，或 open/read 返回 `EINVAL`、
`EOPNOTSUPP`/unsupported 时，Get 返回明确错误，不静默切回 buffered I/O。
非 Linux 构建也返回明确 unsupported。是否启用应按目标 NFS/SFS Turbo 环境的
benchmark 决定：

```bash
go test -run '^$' -bench 'BenchmarkGet(Buffer|Direct)' -benchmem ./pkg/store/fs
```

## 6. Admin 命令

```bash
store-ctl init    --config FILE --generation G1
store-ctl rollout --config FILE --generation G2
store-ctl info    --config FILE
store-ctl purge   --config FILE --generation G1
store-ctl purge   --config FILE --all --confirm
store-ctl serve   --config FILE
```

- `info` 从 generation source 读取 oldest → newest 列表，最后一项显示为当前写入代，
  对象统计由显式 generation admin helper 完成。
- `init`/`rollout` 修改 file 或 S3 source；config source 明确只读，需由配置系统更新。
- `purge --generation` 拒绝最后一项，先从可写 source 移出 generation，再删除 FS/S3
  数据。S3 source 更新使用 ETag `If-Match` CAS，冲突后重读并有界重试。
- `purge --all` 是离线维护操作，仍要求 `--confirm`：执行前必须停止所有
  `store-ctl serve` 进程并静默所有 writer。file/S3 source 会被移除；config source
  下只清理对象数据，不修改主 YAML。source 缺失在运行中的 server 看来是刷新失败，
  按设计会继续保留上一份有效列表，因此该命令不能用作在线写入撤销机制。

写入代 rollout 不等于淘汰既有数据：

```text
[G1] -> [G1, G2]              新的默认写入使用 G2，G1 继续可读
[G1, G2] -> [G2]              只有保留的数据不再依赖 G1 时才可执行
```

第一步刷新后，新的默认 admission 使用 G2，列表中的 G1 admission 仍可完成，Get
按 G2、G1 读取。只要 G1 仍在列表中，显式 admission 也仍可选择 G1。因此，旧写入
完成既不是读保留策略，也不能证明 G1 可以删除。已有镜像、模板、快照及其父层链
可能长期引用 G1 中的对象。

移除 G1 前，部署方必须确认所有需保留的工件及父引用都不依赖只能经该代访问的
对象。使用替代来源时，必须验证原引用及所需内容仍能从该持久来源解析和读取；
缓存命中或存在新的写入代不能作为证据。无法证明时，必须在读取列表中保留 G1。
Server 不会在 rollout 时发现应用侧的有效引用或迁移对象。

物理删除前应协调全部 reader/writer，并允许在飞操作完成。Server 刷新后不再查找
已移出列表的代，即使其文件仍然存在；尚未刷新的 server 或在飞请求仍可能持有旧
列表。每个请求只捕获一份列表，刷新并非全体服务的同步屏障。
`purge --generation` 同时执行列表移除和破坏性数据删除，不是基于引用可达性的 GC。
备份与数据退役决策由部署方负责。`purge --all` 还要求退役或替换所有受影响引用，
并明确接受数据丢失或准备恢复方案，而不只是停止进程。有效的列表更新不要求重启
`store-ctl serve`，但这并不意味着删除操作天然安全。

## 7. S3 数据面

对象布局与 FS 一致：

```text
<prefix>/<partition>/<generation>/<aa>/<bb>/<content-key>
```

PutHandle 在内存中有界缓冲并以单次 PutObject 写入固定 key。`max_inflight` 限制
并发 S3 调用；`op_timeout` 为空时只受调用方 context 控制；
`max_object_size_bytes` 限制单对象大小。generation source 的 S3 endpoint、bucket、
key 和凭据可与数据 backend 完全不同。

## 8. `cache_listen` 与运维

`cache_listen` 非空时，store-ctl 启动只读 cache wire server。ObjectGet 通过
Server 的同一跨 generation 读取 helper 访问 chunk/manifest/blob；ObjectPut 和
shard 操作被拒绝。它是无 L1 的透传入口；需要本地缓存时仍部署 cache-ctl tiered。

`stats_interval` 缺省 30s，输出 get/put/admit 速率、带宽、dedup 比例、延迟、
inflight 和错误数；`0`/`off` 可关闭。`STORE_CTL_DEBUG=1` 可启用慢 S3 操作追踪，
`STORE_CTL_SLOW` 设置阈值。

`store-ctl serve --config FILE [--drain-timeout DURATION]` 的第一次 SIGTERM/SIGINT 关闭 admission 并排空已接纳请求;第二次退出信号取消在途工作,后续信号幂等。SIGHUP 保留 generation 刷新语义,进入退出阶段后不再触发刷新。Store 不另建管理端口或 readiness 框架。

排空覆盖 Get 的全部 stream Send,以及 Put 的接收、校验、Commit 和 SendAndClose。gRPC 与可选 cache_listen 共用退出屏障,任一侧先结束都不能提前关闭 backend。wire 的执行中、已接纳队列和 pending 请求均保留完整响应;停止本地 reader 不当作 peer 断线取消 handler。前台结束后 join generation refresh、stats 等实际使用者,最后关闭 App 拥有的 backend/client。借入 provider/logger 不由 App 关闭。强制取消保留既有 Put Abort 语义;已提交而响应丢失仍是不确定结果。

`--drain-timeout` 默认为 `0`,不增加内部总上限;正数是从首次软停开始、所有入口共享的一份优雅预算;负数在资源创建前拒绝。不增加 YAML/env 配置。预算耗尽可以取消工作和关闭运输层,但仍须等待不可取消的 FS/provider 调用真正结束,才能 Close 资源并返回。进程终止由 manager 或 SDK 宿主负责,不能把此预算当作 Run 的硬性返回时限。

普通 bind 退出会释放入口,下一实例在旧实例退出后重新初始化并 bind。排空不迁移业务连接、不改变 FS 最终一致或可撤销对象参与去重的约束;回滚不涉及格式变化。低层客户端仍为 single attempt,运输层错误不会变成成功或 miss。

验证：

```bash
CGO_ENABLED=0 go test ./pkg/store/... ./pkg/manifest/... ./cmd/store-ctl
CGO_ENABLED=1 go test -race ./pkg/store/... ./pkg/manifest/... ./cmd/store-ctl
make store-ctl
make manifest-ctl
make vet
make test-e2e-scripts
```

以上命令属于 source/unit/helper 验证。Store/cache 的产品 E2E 通过平台 prepared workspace 中的统一 runner 执行 `storage.*.sh` 用例，见 [`../test/e2e/README_zh.md`](../test/e2e/README_zh.md)。产品 E2E 只消费预构建产品，不在执行阶段编译产品或 source helper。

race detector 需要启用 CGO 并具备可用 C 工具链。上面的普通测试可以关闭 CGO，
`go test -race` 则不能使用 `CGO_ENABLED=0`。

## 9. 可复用的 Store App 入口

`app/store.Run(ctx, cfg, opts)` 拥有完整 Store 服务生命周期。`store-ctl serve` 只是一个调用方；定制二进制可直接静态链接同一个 App，不 exec、也不要求安装官方服务二进制。`Run` 不读取 `STORE_CONFIG`、不注册进程信号、不调用 `os.Exit`/`log.Fatal`、不修改进程全局 logger；配置加载、信号和退出策略由调用方拥有。

下面的最小定制程序有意复用仓库 YAML adapter：

```go
package main

import (
    "context"
    "log"
    "os"
    "os/signal"
    "syscall"

    storeapp "github.com/kuasar-sandbox/accelerator/app/store"
    storeconfig "github.com/kuasar-sandbox/accelerator/pkg/store/config"
)

func main() {
    if len(os.Args) != 2 { log.Fatal("usage: custom-store CONFIG") }
    cfg, err := storeconfig.LoadConfig(os.Args[1], true)
    if err != nil { log.Fatal(err) }
    ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
    defer stop()
    if err := storeapp.Run(ctx, *cfg, storeapp.Options{}); err != nil { log.Fatal(err) }
}
```

systemd 可直接启动该二进制：

```ini
[Service]
ExecStart=/opt/kuasar/bin/custom-store /etc/kuasar/store.yaml
Restart=on-failure
KillSignal=SIGTERM
```

也可直接用 Go 构造 `storeapp.Config`。动态 generation/credential provider 通过 Go API 的 `Options` 显式绑定，不写回 YAML。需要 reload 的定制调用方自行拥有 reload 触发和 provider callback；官方 `store-ctl` 只是把 SIGHUP 适配为这些输入。

`Options` 新增零值兼容的 `Drain <-chan struct{}` 和 `DrainTimeout time.Duration`。Drain 为 nil 保留上例的旧 SDK 取消用法;关闭 Drain 发起不可逆软停。ctx 取消始终取消在途工作,drain 中取消立即升级,同时发生时取消优先。初始化期间收到 drain 后不再发布 started 或开始 serving。宿主可传 `Options{Drain: drain, DrainTimeout: 30*time.Second}`,将第一次退出请求适配为 `close(drain)`,后续请求调用自己的 cancel;SDK 不注册信号。预算与资源顺序见 §8。

可复用入口的验收应独立编译定制 module，并在服务 `PATH` 中没有官方二进制时执行真实 Store 协议请求。

## 9. See also

- [manifest_zh.md](manifest_zh.md) — manifest ingest/fetch 与加密流程
- [cache_zh.md](cache_zh.md) — tiered cache、origin 和 wire protocol
- 仓库根目录 `README.md` / `Makefile` — 构建和完整测试入口

Store 单次读取与非重放边界见[§3.1](#31-单次读取与写入边界).
