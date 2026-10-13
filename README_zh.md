[English](README.md) | [简体中文](README_zh.md)

# accelerator

首次运行跨组件沙箱或 Demo，推荐从项目[快速开始](https://github.com/kuasar-sandbox/kuasar-sandbox/blob/main/docs/quickstart_zh.md)
使用匹配聚合版本的 [workbench](https://github.com/kuasar-sandbox/kuasar-sandbox/blob/main/workbench/README_zh.md)。
本组件仍可独立构建和部署，workbench 不是生产运行的强制依赖。预构建架构以所选
发布版的实际资产为准；源码支持某架构不意味着所有历史版本都提供该架构制品。

`accelerator` 是 [Kuasar Sandbox](https://github.com/kuasar-sandbox/kuasar-sandbox) 中面向镜像、快照和稀疏工件的数据访问、存储、加密与缓存基础组件。

它提供可复用的稀疏数据和工件抽象、本地与共享文件访问、基于 Manifest 的内容组织、文件系统与 S3-compatible store、分层缓存、完整性校验和加密、OCI 拉取、确定性 EROFS 镜像展平、按需读取与预取。

内容寻址和去重是其中的能力,不是组件的唯一用途。共享范围由部署安全域和密钥材料控制。部署可以为快照使用直接文件,为大规模分发使用 Manifest/对象存储,也可以组合两种路径。

## 数据路径

| 路径 | 典型用途 | 特征 |
| --- | --- | --- |
| 本地文件 | 独立节点、本地 NVMe、临时或节点关联工件 | 直接文件语义,访问路径短 |
| 共享文件系统 | NAS、NFS 或其他一致挂载的文件系统 | 跨节点共享原生文件访问,不强制转换为分块对象 |
| Manifest 与对象存储 | 大规模分发、远端持久化、迁移和分层缓存 | 内容寻址对象、元数据描述的稀疏区段和按需读取 |

这些路径向下游暴露共同的逻辑数据与引用语义。因此 `sandboxer` 可以消费镜像或快照,不必让生命周期 API 绑定某种物理后端。

## 公共 Go 导出面

下游 import 面保持精简,主要使用纯 Go,使 `sandboxer` 等消费者不必把存储服务端、对象存储 SDK、纠删码或数据库实现链接到自己的二进制中。

| 包 | 用途 |
| --- | --- |
| `pkg/sparse` | 权威 Hole/Zero/Data 表示与稀疏随机/顺序读取 |
| `pkg/manifest` 及子包 | 内容组织、分块、加密、ingest、fetch 和 prefetch |
| `pkg/store` 与 `pkg/store/client` | 内容存储协议和客户端 |
| `pkg/cache` 与 `pkg/cache/client` | 缓存协议、本地/分片/分层组合与客户端 |
| `app/store` 与 `app/cache` | 供官方及定制二进制复用的完整 Store/Cache 服务生命周期 |
| `pkg/tarstream` | 携带身份及完整性元数据的本地稀疏传输工件 |
| `pkg/flatten`、`pkg/image`、`pkg/remote`、`pkg/tar` | OCI/目录拉取、镜像配置与 EROFS 展平基础能力 |

重服务端后端保留在组件二进制和 server package 后面。稀疏 Run/Stream、chunk-window 与本地 tarstream 的详细契约归属于 [Manifest §4.8](docs/manifest_zh.md#48-读路径细节)及[文件工件](docs/file-artifacts_zh.md),README 不另建一套协议。

## 二进制

| 二进制 | 用途 | 构建方式 |
| --- | --- | --- |
| `manifest-ctl` | ingest、检查、fetch 和转换 Manifest 工件 | 纯 Go |
| `store-ctl` | 文件系统或 S3-compatible 内容存储服务及管理 | 纯 Go |
| `cache-ctl` | 本地、分片、分层和纠删码缓存服务 | 使用当前源码选择的后端依赖 |

`flatten-ctl` 随 `guest-runtime` 的 Runtime 发行单元发布,共享镜像展平实现仍在本仓维护。

## 存储安全

组件对进入受保护工件和 Manifest 路径的数据提供完整性校验与加密。平台身份凭据和内容保护密钥是不同的概念。密钥范围、salt 和部署安全域共同决定加密内容可以在哪里共享或去重。

内容加密不意味着任何威胁模型下都不泄露元数据,也不意味着所有租户私有数据都应全局去重。部署应根据数据分类与信任边界选择共享域。直接本地或共享文件路径也可应用项目定义的本地工件保护策略。

## 构建与测试

构建使用环境提供的 Go，并继承 `GOROOT`、`GOTOOLCHAIN` 等工具链选择；发布自动化需要环境在 `PATH` 中提供支持 `api --slurp` 的 `gh`。项目不下载、替换或按固定二进制摘要认证这些环境工具。

```bash
make manifest-ctl store-ctl     # 纯 Go 命令行服务
make cache-ctl                  # 缓存服务及当前后端依赖
make cache-ctl NO_ROCKSDB=1     # 不带 RocksDB (-tags no_rocksdb, CGO_ENABLED=0)
make build                      # 全部组件二进制
make build TARGET_ARCH=aarch64  # 交叉编译;接受 amd64/arm64 别名
make vet                        # 检查面向下游的薄 Go 导出面
make test                       # 单元、后端及 source/helper 回归
make test-e2e-scripts           # 离线 E2E fixture/helper 回归，不是产品 E2E
```

产品 E2E 由平台统一的 prepared-workspace runner 对预构建制品执行；组件不再提供独立 owner `run_all.sh` 或 `make test-e2e` 入口。`prepare -> <suite>.<case>.sh -> run` 契约及 accelerator 用例前置条件见 [`test/e2e/README_zh.md`](test/e2e/README_zh.md)。

源码构建需要 Go 1.26.1 或更新版本，原生前置依赖随构建目标而异。当前源码通过仓库脚本说明并构建所需原生缓存依赖。真实对象存储测试必须使用显式测试凭据或本地 S3-compatible 服务;普通单元测试与本地文件系统测试不应要求生产云凭据。

`cache-ctl` 通常使用 CGO 以及 `deps/build-rocksdb.sh` 在本地构建的静态 `librocksdb.a`;`manifest-ctl`、`store-ctl` 与下游 import 面不需要 RocksDB。`make release VERSION=vX.Y.Z` 生成并检查本地发行 bundle;官方缓存载荷必须保留 RocksDB 支持。

### 私网与离线构建

本 module 没有跨仓 Go 依赖。可将 `GOPROXY` 指向部署镜像并使用对应 checksum database 策略,或在离线时使用已填充、经过验证的 module cache。兄弟仓协调开发可使用[项目工作区](https://github.com/kuasar-sandbox/kuasar-sandbox),但它不是本组件独立构建的前提。

## 独立运行

三个服务可独立于完整平台部署:

- 用 `manifest-ctl` ingest 或 fetch 稀疏工件;
- 为 `store-ctl` 配置文件系统根目录或 S3-compatible endpoint;
- 以本地、分片或分层拓扑运行 `cache-ctl`;
- 将 `app/store.Run` 或 `app/cache.Run` 静态链接进定制服务二进制。官方 CLI 与定制程序平级调用同一生命周期函数；定制程序不 exec、也不依赖安装 `store-ctl`/`cache-ctl`。

配置示例使用本地路径、文档保留 endpoint 和占位凭据。生产部署应使用受保护的 endpoint、显式资源预算、持久的权威存储,以及适合所选缓存拓扑的可观测性。

## 发行模型

所选 owner 源码在 `release/helper-inputs.json` 显式声明编译型 E2E helper 清单。当前清单为空。后续 helper 及其构建配方由此 owner 声明维护，聚合打包自动发现，无需平台文件名清单。

发行测试需要匹配的原生 EROFS Plan 能力。预检仅一次准入同一发行线上的
`guest-runtime` 源码分支，并记录精确提交。现有固定输入传输将该源码树交给
两个架构的构建任务；它们使用原生 EROFS 配方和缓存，在测试前检查 Plan 能力。
这是测试工具的源码依赖；组件构建无需已发布的 Runtime 二进制，Runtime
发布仍消费所选 accelerator 发行版。

使用组件 Makefile 从选定源码构建。`release.sh package` 使用匹配的
`bin/<arch>` 二进制,或显式指定的 `RELEASE_BIN_DIR`;只收集材料并生成 bundle,
不重新构建二进制,不重置源码或构建缓存。所选源码 checkout、依赖版本、原生
构建记录与产物应一并保留。

打包记录实际 Go 版本和生效的 module 替换。Go/module LICENSE、NOTICE 取自所选
编译器安装和匹配的 module 源码,保留嵌套路径。模块解析沿用正常 Go 缓存与路由,
下载模块的校验和须匹配二进制记录。组织命名空间本身不豁免模块的声明收集。
本组件没有内部兄弟组件依赖。官方包中不受支持的本地替换需要改用带版本的
module 输入。

材料放在 `share/licenses/<component>` 和 `share/sources/<component>`。
后者包含 `SOURCES.tsv`、`GO-BUILD-INFO.tsv`、`GO-MODULES.tsv` 与
`MATERIALS.sha256`。声明缺失、子目录不可读或遍历不完整时收集失败。
独立验证检查交付清单、校验和、必需文件、来源记录相符性、载荷身份及归档路径/
类型/权限。它不获取源码 checkout 或 Go 模块,不与远端源码树比较许可正文,
也不下载或认证编译器分发。校验和及 VCS 记录是相符性检查,不能证明任意生产者的身份。

归档名称标识请求的发行目标。项目及内部依赖记录在本地 Tag 匹配所选 commit 时
使用发行版本,否则记录 `git:<commit>`;打包不要求创建未来目标 Tag。
发布者在 Tag/Release 写入前把选定项目 SHA 传入验证器,采用 bundle 中
`release-notes.md` 正文,追加既有来源/Preview 标记。可信源码选择、构建/发布
权限分离及拒绝替换已发布资产的要求保持不变。
生产者提供的说明不得夹带发布者专属的来源/Preview 标记。

官方包要求三个 Linux 命令二进制匹配所选原生架构（x86_64 对应 amd64，aarch64 对应
arm64），并分别具有自身的 Accelerator main-package
身份;`cache-ctl` 必须包含 RocksDB。五个交付运维脚本取自所选源码树。
解包前拒绝额外/重复载荷、链接、错误属主或权限,以及其他组件的材料命名空间。

正常 cache 构建保留 `build/<arch>/cache-ctl.map`。打包读取该链接映射及匹配的
RocksDB 静态库/源码树;显式提供时使用 `RELEASE_ROCKSDB_SOURCE_DIR`。
记录实际 RocksDB、libstdc++、libgcc 等链接输入的摘要及 Debian/RPM 源包版本,
从对应包复制版权、许可、NOTICE 和引用的公共许可正文,不按包数据库摘要认证
已安装文件字节。RocksDB 材料必须包含 `AUTHORS`、`COPYING`、
`LICENSE.Apache`、`LICENSE.leveldb`。不同原生输入不能以相同材料名称相互覆盖;
RPM 收集要求完整枚举同源兄弟包。这些材料支持发行检视,不构成法律认证,
也不改变 RocksDB 后端或持久化策略。

本仓独立发布 `vX.Y.Z` 组件版本。已发布原生架构的组件归档包含三个服务二进制及发行合同选定的运维/性能辅助脚本。聚合 platform 包按显式文档/测试选择携带用户指南与规范 E2E 运行输入，内部设计文档和源码专用测试保留在源码仓。

项目主仓独立发布 `release-vX.Y.Z` 聚合版本,选择 `accelerator` 和其他发行单元的精确版本,验证资产并执行跨组件测试。聚合与组件版本号相互独立。

发布分支、Preview、Latest 协调及并发/取消处理以[项目发行文档](https://github.com/kuasar-sandbox/kuasar-sandbox/blob/main/docs/release_zh.md)为准;普通使用可从[最新 Stable 聚合渠道](https://github.com/kuasar-sandbox/kuasar-sandbox/releases/latest)进入。

## 文档

Manifest、Store 和 Cache 提供完整英中版本。既有英文后端指南仍可使用:

- [Manifest - 英文](docs/manifest.md) / [中文](docs/manifest_zh.md) - 稀疏 Manifest、ingest/fetch、分块、加密、密钥表和公共 SDK;
- [文件工件与载体](docs/file-artifacts_zh.md) - 不可变 tarstream 加密、Bundle 布局、访问及发布;
- [Store - 英文](docs/store.md) / [中文](docs/store_zh.md) - 文件系统与 S3-compatible store、代保留、完整性和显式 purge(不是可达性 GC);
- [Cache - 英文](docs/cache.md) / [中文](docs/cache_zh.md) - 本地、分片、分层与纠删码缓存;
- [`docs/cache-redis.md`(英文)](docs/cache-redis.md) - Redis-compatible UDS/TCP 后端与外部服务部署示例。

OCI/目录到 EROFS 的 CLI 用法归属于 [`guest-runtime`](https://github.com/kuasar-sandbox/guest-runtime),因为 `flatten-ctl` 属于 Runtime 发行单元。

README 提供组件公开入口;完整设计指南定义格式、运行限制、失败行为和测量要求。

## 项目边界

- MicroVM 生命周期与快照执行归属 [`sandboxer`](https://github.com/kuasar-sandbox/sandboxer);
- 网络分配与转发归属 [`connector`](https://github.com/kuasar-sandbox/connector);
- guest 内核、Runtime image 与 `flatten-ctl` 发行物归属 [`guest-runtime`](https://github.com/kuasar-sandbox/guest-runtime);
- 节点与集群编排归属 [`orchestrator`](https://github.com/kuasar-sandbox/orchestrator);
- 系统设计、共享集成测试、Demo 和聚合发行归属 [`kuasar-sandbox/kuasar-sandbox`](https://github.com/kuasar-sandbox/kuasar-sandbox)。

## 贡献与安全

请阅读本仓[贡献指南](CONTRIBUTING.md)与[组织贡献指南](https://github.com/kuasar-sandbox/.github/blob/main/CONTRIBUTING.md)。公共数据格式或下游 package 契约变更需要关联 Companion PR 并执行精确源码的项目级验证。

不要在公开 Issue 中报告漏洞或真实存储凭据。请使用 [Kuasar Sandbox 安全政策](https://github.com/kuasar-sandbox/kuasar-sandbox/security/policy)和 GitHub 私密漏洞报告。

## License

项目原创内容采用 [Apache License 2.0](LICENSE)。保留源码与发行资产中原生库、vendor code、生成代码、格式及第三方工具的许可证、署名、NOTICE 和源码义务。
