[English](accelerator-ci.md) | [简体中文](accelerator-ci_zh.md)

# Accelerator CI 要求与检查

共享 runner 路由、profile、workflow 入口和上线状态由
[平台 CI 契约](https://github.com/kuasar-sandbox/kuasar-sandbox/blob/main/docs/ci_zh.md)
统一说明。本文记录 accelerator 的构建要求及其检查能够提供的证据。

## 构建与 ABI 要求

发布构建按原有 recipe 构建真实 RocksDB，不能以 `NO_ROCKSDB=1` 替代发布载荷。
Go 与 native 编译器由构建环境提供，保留既有工具链选择语义。
CI 在一次运行开始时选择已通过验证的 Workbench, 为原生 x86_64 与 aarch64 Runner
分别固定镜像身份. 普通构建、测试和打包使用 ordinary UID、只读根文件系统、移除
capabilities, 以及任务私有源码和缓存目录. `KUASAR_BUILD_JOBS` 将同一任务 CPU
预算传给 Go 和 RocksDB CMake 构建; 未配置预算的本地 native recipe 仍使用 `nproc`.
普通 Go RocksDB 测试保留上游绑定的压缩库链接参数，需要 Snappy、LZ4、Zstandard
与 zlib 开发库；这并不启用原有 native RocksDB recipe 的压缩功能。

Accelerator 的 [ABI 检查](../scripts/ci-check-abi.py) 要求 `manifest-ctl` 与
`store-ctl` 保持静态。`cache-ctl` 保留正常 CGO/glibc 和静态 RocksDB、libstdc++、
libgcc，GLIBC 需求不得超过声明的 2.38 基线。ABI 失败阻止打包。
Native recipe、摘要、许可、来源清单与 link-map 校验仍为必需检查。
Publisher 在独立 Workbench 调用中从受信源码编译归档校验器, 该调用禁用缓存恢复和
保存. 发布步骤直接执行校验器, 不再次编译.

## Prepared 产品 E2E 与本地证据

[Accelerator E2E 指南](../test/e2e/README_zh.md) 列出预构建产品、EROFS、Redis、
临时存储和权限要求，以及 accelerator 的 `storage.*.sh` / `image.*.sh` 产品 case。
产品 E2E 只通过平台统一的 prepared-workspace runner 执行；accelerator 不再拥有
`run_all.sh` 或 owner-suite 入口。本地 filesystem/cache 和 S3-compatible fixture
仅证明本地行为，不代表真实云覆盖。

普通无凭据验证先从精确预构建平台 release 准备 workspace，并显式选择五个非 OBS case：

```bash
RUNNER=/path/to/platform/test/e2e/e2e
RELEASE_DIR=/path/to/prebuilt/platform-release
WORK=/tmp/kuasar-e2e

"$RUNNER" prepare --release-dir "$RELEASE_DIR" --workdir "$WORK"
"$RUNNER" run --workdir "$WORK" \
  --include storage.cache.sh \
  --include storage.tiered-cache.sh \
  --include storage.cache-membership.sh \
  --include storage.store-cache.sh \
  --include image.manifest.sh
```

凭据化 OBS 保持显式 opt-in，并通过 case ID 选择，而不是依赖 `OBS_E2E` marker。
明确配置 `OBS_BUCKET`、`OBS_ENDPOINT`、`OBS_AK` 和 `OBS_SK` 后，在同一个 prepared
workspace 上运行：

```bash
"$RUNNER" run --workdir "$WORK" --include storage.obs.sh
```

已选云端 case 如果无法满足 endpoint 或凭据前置条件则失败；未选择的云端 case
不能算通过。CI 使用同一个平台 runner，并选择 exact admitted case ID。

从 accelerator checkout 运行离线检查：

```bash
python3 scripts/ci-test-workflows.py ../kuasar-sandbox  # 使用实际平台路径
python3 scripts/test-native-build-budget.py
(umask 022; bash scripts/test-release.sh)
make test-e2e-scripts
```

Workflow 检查解析组件的实际 YAML，验证其分配、workspace 和 ABI 契约。
发布测试使用合成 binary/link-map fixture 与模拟 API 响应。脚本测试验证 fixture
和生命周期边界，不会完成真实 E2E 的编号断言。这些检查通过不能证明真实 RocksDB
构建、prepared 产品 E2E 或云端资格验证已通过。

## 流式展平回归覆盖

运行 flatten 和 remote 测试前，先构建所选 guest-runtime 的 native 配方，并将
`MKFS_EROFS_PATH` 指向该精确 `mkfs.erofs`。Plan 测试要求工具报告 v1 能力，
不支持此能力的系统 mkfs 不能代替。[展平指南](https://github.com/kuasar-sandbox/guest-runtime/blob/main/docs/flatten_zh.md)
定义输入、源生命周期、原生计划和工件完成合同。

| 要求 | 实现 | 维护中的检查及证据范围 |
|---|---|---|
| 固定输入身份及完整校验 | `pkg/flatten/input.go`、`layer_stream.go`；`pkg/remote/stream.go` | 层 media/size/digest/DiffID/trailer/Close 与取消用例；实际本地 Registry → umoci → Plan → pipe → 当前工件 reader。 |
| 层文件系统语义 | `pkg/flatten/layer_apply_linux.go` | 实际 umoci 的 late whiteout/opaque、类型替换、implicit parent、链接、属性及空层；权限不足仍报错。 |
| 原生布局与顺序输出 | Guest native patch series；`pkg/flatten/plan_linux.go` | Raw/Plan 身份及提取内容；损坏、越界、源变化、纯元数据可重复 RunAt 和成功尾前失败。Native exact-read 注入沿用 guest-runtime 的 `test-erofs`。 |
| 归档与入口等价性 | `pkg/flatten/archive_linux.go`、`raw_linux.go` | 可 seek 归档、pipe spool 与目录对比；截断/拼接归档及索引预算拒绝。Guest CLI 测试覆盖 file、stdout 和上传/失败分支。 |
| 持久缓存发布与所有权 | `pkg/remote/prepare_linux.go`、`prefetch_linux.go`、cache/GC | 未验证 EOF 不能命中；显式字节预算、bypass、当前层优先、重复摘要、并发 GC lease 和等待完成的取消。转换政策测试拒绝旧转换命中，同时保留显式旧工件读取。 |
| 稳定选集与自有资源清理 | `pkg/flatten/prepared_linux.go`、`selection_linux.go` | 容忍 atime/排除子树变化，检测纳入元数据变化；apply/output 真实 ENOSPC、bind/output 别名、保留原文件及打开/已删除 FD 检查。 |

最后一行包含显式启用的真实挂载测试。普通用户编译测试程序，再在新的私有 mount
namespace 中以挂载权限仅运行这些用例。将 `TMPDIR` 设为私有测试目录，
`MKFS_EROFS_PATH` 指向所选 native 工具；全部挂载位于测试自有路径，在删除目录前
卸载。小容量 tmpfs 产生真实 ENOSPC，不会填满宿主文件系统。

```bash
CGO_ENABLED=0 go test -c -o /path/to/private/flatten-mount.test ./pkg/flatten
sudo unshare --mount --propagation private env \
  FLATTEN_TEST_MOUNTS=1 TMPDIR=/path/to/private/scratch \
  MKFS_EROFS_PATH=/path/to/selected/mkfs.erofs \
  /path/to/private/flatten-mount.test -test.v -test.run '^TestMounted'
```

这些测试建立本地合同。实际 runtime bundle/Guest 消费、Builder Phase A/B、静态包
校验、Guest ext4/Host COW 峰值及相同条件的全链性能仍须通过既有 prepared 产品与
Builder 测试入口，以精确 source/artifact set 验证。接收工件字节与 scratch 分开记录，
计入索引/decoder 内存及自有资源退出清理。包级测试或上述挂载夹具不能替代真实 VM
证据。

### 同条件展平测量

`test/scripts/bench_flatten.py` 使用预先构建的原生生产工具、真实 stdout pipe 和当前
artifact reader，默认交替顺序执行三轮。记录工具摘要、进程树采样 RSS、scratch
实际分配块/inode、已删除但仍打开的文件，并等待自有生产会话中的全部进程组退出后清理 scratch。
接收 artifact P 与可选 fsync 分开计量。可选 `--progress-timing` 为既有 CLI 进度行与
stdout 首次可读数据标注时间，辅助阶段归因；准备完成后 writer 才能输出 artifact 头。部分宿主不提供 child rusage I/O 计数；
采样 RSS 与块峰值受记录的采样间隔限制。

进程枚举使用 procfs 的父 PID，不依赖可选的逐任务 `children` 接口。

宽目录的块数/inode 遍历可能干扰管道接收和源文件 I/O。资源采样轮次之外，再在相同
资源、存储和缓存条件下使用 `--timing-only` 测量该影响。此模式仍核验身份和实际内容、
记录子进程 CPU/I/O 并等待自有资源清理，但将采样峰值和采样间隔记为 null。两类结果
都应保留；计时轮次不能作为内存、块数、inode 或打开文件的上界证据。


在相同 CPU、内存、存储和缓存条件下，对比原实现、正确 applier 加旧 mkfs，以及 Plan。
中间对照仅在独立的所选 accelerator 源码副本上应用
`test/fixtures/flatten-old-layout.patch`，使用相同的所选 guest CLI，以及包含 exact-read
修复的旧维护版 mkfs 构建。构建记录须包含全部 source commit 和补丁摘要。该补丁只作
测量夹具，正常产品构建使用 Plan 路径。

在既有 Workbench 中使用 Python 3.11 或更新版本。输入、scratch parent 与 output root
须互不包含，output root 须不存在。Registry 输入必须固定 SHA-256 digest。提供按相对
路径索引的预期内容 JSON：每项包含 `type`、`mode`、`uid`、`gid`，文件另含
`size`/`sha256`，符号链接含 `target`，特殊文件含 `device`。对选集内每组普通文件
硬链接，以字典序最小的相对路径为规范路径，其他成员用 `hardlink_to` 指向该路径。
每个首次出现的 artifact
身份都经所选 native fsck 提取，核验实际内容和元数据；额外提取在测量区间外执行。

```bash
python3 test/scripts/bench_flatten.py \
  --variant baseline,baseline/flatten-ctl,baseline/mkfs.erofs \
  --variant correct-applier,control/flatten-ctl,control/mkfs.erofs \
  --variant plan,plan/flatten-ctl,plan/mkfs.erofs \
  --input fixtures/image.tar --expected fixtures/expected.json \
  --consumer plan/flatten-ctl --fsck tools/fsck.erofs \
  --scratch-parent scratch --output-root results --trials 3 --fsync
```

正常语义夹具在不同实现间必须具有相同 artifact 身份。低/高重复、多层覆盖、小文件、
受限网络与显式缓存分别使用固定夹具。通过 `--config` 使用持久缓存时，其占用与
生命周期另计，不视作临时 scratch。全链耗时包含并行重叠工作，不简单相加重叠阶段。
这些宿主测量不能证明 Guest ext4 或 Host COW 峰值；后两项须在实际 Guest/Builder
链路中计量，并覆盖接收端和自有资源清理。
