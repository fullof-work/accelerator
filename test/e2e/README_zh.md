[English](README.md) | [简体中文](README_zh.md)

# Accelerator 产品 E2E 用例

Accelerator 产品 E2E 统一由平台提供的 prepared-workspace runner 执行。组件仓库只负责用例文件和 accelerator 专属 helper，不再提供独立的 `run_all.sh` 或另一套产品 E2E runner。

执行模型保持简单：

```text
预构建产品 -> prepare -> <suite>.<case>.sh -> run
```

用例 ID 就是文件名，suite 是文件名第一段。Accelerator 当前提供以下 `storage` 和 `image` 用例：

| 用例 | 验证契约 |
| --- | --- |
| `storage.cache.sh` | 文件系统/本地缓存与 Store 往返、cache 协议、tiered 只读写拒绝等缓存正确性。 |
| `storage.tiered-cache.sh` | Redis/tiered/sharded/EC、上游填充与回写、故障恢复、重启回填及无 origin 热读取。 |
| `storage.cache-membership.sh` | EC cache 成员滚动变化及利用存活节点完成重建。 |
| `storage.cache-probe.sh` | RocksDB/Redis 的真实 TCP/UDS Ping 与 single-attempt shard 往返; SIGHUP 排除仅监听的 peer. |
| `storage.store-cache.sh` | Store 内嵌只读 cache 协议、generation 切换、旧 manifest 读取、blob 命名空间及拒绝写入。 |
| `image.manifest.sh` | 真实 flatten/EROFS，以及 Manifest store/load、去重、校验、稀疏/零块与镜像差异契约。 |
| `storage.obs.sh` | 显式选择、需要凭据的 OBS/S3 兼容对象存储往返；不属于普通离线执行。 |

用例文件有意保持普通 `100644` 模式。公共 runner 使用 `bash` 执行选中的用例，不把 executable bit 变成第二套用例契约。

## 运行产品 E2E

使用 Kuasar 平台测试包发布的 runner 和 cases，或精确 integration CI 生成的 prepared workspace。执行产品 E2E 时不得重新构建 accelerator、编译测试 helper、查找相邻源码仓库或静默回退到源码。

普通、无需云凭据的 accelerator 用例可按以下 aggregate-release 方式运行：

```sh
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

上述普通命令有意不选择 `storage.obs.sh`。只有在明确具备所需 OBS/S3 兼容 endpoint 与凭据时才单独选择它，例如使用 `--include storage.obs.sh`。

平台 CI 使用同一个 runner，并从已准入的精确测试版本中解析用例文件名。Suite 选择遵循[平台 CI 契约](https://github.com/kuasar-sandbox/kuasar-sandbox/blob/main/docs/ci_zh.md)；同一个 suite 可以包含多个组件维护的用例。仅选择上面列出的 accelerator 用例时，使用示例中的显式 `--include` 文件名。

已选用例缺少所需产品或执行条件时必须失败。架构、root 权限及主机服务属于执行条件，不是 suite，也不能以成功 skip 代替验证。

## Source/helper 回归

只验证 fixture 或测试 helper、本身不证明产品行为的检查留在产品 E2E 之外：

```sh
make test-e2e-scripts
```

该目标运行端口租约回归和离线 Manifest/cache 边界测试。它属于 source/helper gate，因此可以使用源码级 fixture 和明确的 stub；这不等价于 artifact E2E。`make test` 也会执行这些回归。

## 运行条件

真实用例按各自契约检查条件，而不是依赖一个全局 owner runner 环境。常见要求包括 Linux、Bash 4+、Python 3、GNU 常用工具、可写临时空间以及通过 `BIN` 提供的精确 prepared 二进制。

Manifest/flatten 覆盖使用已准备的 `flatten-ctl` 内含的生成器，不要求或选择外部 `mkfs.erofs`。需要保留镜像所有权的 flatten export 要求 root 或非交互式 `sudo -n`。用例不使用 `sudo -E`，只把自有临时目录/配置传给提权操作。

涉及 Redis 的 tiered/cache 用例要求 `PATH` 中存在 `redis-server`，或显式提供对应可执行文件。测试自行启动并清理临时实例，不依赖外部 Redis 服务。

`storage.obs.sh` 必须显式选择，并在缺少以下任意云端输入时 fail closed：

- `OBS_BUCKET`
- `OBS_ENDPOINT`
- `OBS_AK`
- `OBS_SK`

`OBS_REGION` 和 `OBS_PREFIX` 可选。该用例不会把 `~/.obsconfig` 当作凭据来源，因为 `store-ctl` 本身不读取该文件。用例使用每轮独立 prefix，并在结束时尝试清理该 prefix。普通 storage/image 测试不需要 OBS 凭据或云 API。

## Manifest fixture 与离线行为

`lib/manifest_fixture.py` 使用 Python 标准库生成可重复的 Docker 格式数据归档，用于 source/helper 回归及 prepared fixture 生成。镜像包含架构、Linux runtime 元数据、UID/GID、环境变量和工作目录，并提供足够的共享/差异内容来验证去重和分块行为。

Artifact E2E 消费已经准备好的 manifest fixture 目录，不允许在执行阶段把动态生成 fixture 当成隐藏 fallback。开发场景显式提供 `IMAGE_A`/`IMAGE_B` 时，只使用本地 Docker daemon 已缓存的镜像，不会 pull、重新打 tag 或删除调用者镜像。

共享 runner 路由、provenance 校验、架构 lane 和上线规则统一见[平台 CI 契约](https://github.com/kuasar-sandbox/kuasar-sandbox/blob/main/docs/ci_zh.md)。本文只负责 accelerator 用例意图和运行前置条件。
