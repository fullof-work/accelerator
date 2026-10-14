[English](README.md) | [简体中文](README_zh.md)

# accelerator

For the first cross-component sandbox or Demo, follow the project
[Quick Start](https://github.com/kuasar-sandbox/kuasar-sandbox/blob/main/docs/quickstart.md)
with the matching aggregate release and
[workbench](https://github.com/kuasar-sandbox/kuasar-sandbox/blob/main/workbench/README.md).
This component can still be built/deployed independently; workbench is not a
production runtime dependency. Published architectures are specific to the
selected release; source support does not imply all historical assets exist.

`accelerator` is the **data access, storage, encryption, and cache infrastructure** for images, snapshots, and sparse artifacts in [Kuasar Sandbox](https://github.com/kuasar-sandbox/kuasar-sandbox).

It provides reusable sparse-data and artifact abstractions, local and shared-file access, Manifest-backed content organization, filesystem and S3-compatible stores, layered caches, integrity and encryption, OCI retrieval, deterministic EROFS image flattening, on-demand fetch, and prefetch.

Content addressing and deduplication are available capabilities, not the component's only purpose. Their sharing scope is controlled by deployment security domains and key material. A deployment can use direct files for snapshots, Manifest/object storage for large-scale distribution, or a combination of both.

## Data paths

| Data path | Typical use | Characteristics |
| --- | --- | --- |
| Local files | Standalone nodes, local NVMe, temporary or node-affine artifacts | Direct file semantics and a short access path |
| Shared filesystems | NAS, NFS, or another consistently mounted filesystem | Native file access shared across nodes; no mandatory chunk conversion |
| Manifest and object storage | Large-scale distribution, remote persistence, migration, and tiered caching | Content-addressed objects, metadata-described sparse ranges, and on-demand reads |

These paths expose common logical data and reference semantics to downstream components. `sandboxer` can therefore consume an image or snapshot without making the lifecycle API specific to one physical backend.

## Public Go surface

The downstream import surface is kept small and mostly pure Go, so consumers such as `sandboxer` do not pull storage-server, object-storage SDK, erasure-coding, or database implementations into their binaries.

| Package | Purpose |
| --- | --- |
| `pkg/sparse` | Authoritative Hole/Zero/Data representation and sparse random/sequential reads |
| `pkg/manifest` and subpackages | Content organization, chunking, encryption, ingest, fetch, and prefetch |
| `pkg/store` and `pkg/store/client` | Content-store protocol and client |
| `pkg/cache` and `pkg/cache/client` | Cache protocol, local/sharded/tiered composition, and clients |
| `app/store` and `app/cache` | Complete reusable Store/Cache service lifecycles for official and custom executables |
| `pkg/tarstream` | Sparse local transport artifact with identity and integrity metadata |
| `pkg/flatten`, `pkg/image`, `pkg/remote`, `pkg/tar` | OCI/directory retrieval, image configuration, and EROFS flattening primitives |

Heavy server backends remain behind component binaries and server packages.
The detailed sparse Run/Stream, chunk-window and local tarstream contracts belong
to [Manifest §4.8](docs/manifest.md#48-read-path-in-detail) and
[file artifacts](docs/file-artifacts.md), not a separate README protocol.

## Binaries

| Binary | Purpose | Build model |
| --- | --- | --- |
| `manifest-ctl` | Ingest, inspect, fetch, and transform Manifest-backed artifacts | Pure Go |
| `store-ctl` | Filesystem or S3-compatible content-store service and administration | Pure Go |
| `cache-ctl` | Local, sharded, tiered, and erasure-coded cache service | Uses the backend dependencies selected by the current source tree |

`flatten-ctl` is published with the `guest-runtime` Runtime release unit, while its shared image-flattening implementation is maintained here.

## Storage security

The component supports integrity verification and encryption for data that enters protected artifact and Manifest paths. Platform identity credentials and content-protection keys are separate concerns. Key scope, salt, and deployment security domains determine where encrypted content may be shared or deduplicated.

Do not interpret content encryption as a claim that every threat model leaks no metadata, or that all tenant-private data should be globally deduplicated. Deployments must choose a sharing domain appropriate for the data classification and trust boundary. Direct local or shared-file paths can also apply the project's documented local artifact protection policy.

## Build and test

Builds use environment-provided Go and inherit its `GOROOT` and `GOTOOLCHAIN` selection. Release automation requires a working `gh` with `api --slurp` support on `PATH`; the project does not install, replace, or authenticate these environment tools against fixed binary digests.

```bash
make manifest-ctl store-ctl     # pure-Go command-line services
make cache-ctl                  # build the cache service and its current backend dependencies
make cache-ctl NO_ROCKSDB=1     # build cache-ctl without RocksDB (-tags no_rocksdb, CGO_ENABLED=0)
make build                      # all component binaries
make build TARGET_ARCH=aarch64  # cross-compile; amd64/arm64 aliases are accepted
make vet                        # validate the thin downstream-facing Go surface
make test                       # unit tests and backend/source-helper regressions
make test-e2e-scripts           # offline E2E fixture/helper regressions; not product E2E
```

Product E2E is executed by the platform-owned prepared-workspace runner against prebuilt artifacts; the component no longer has a separate owner `run_all.sh` or `make test-e2e` entrypoint. See [`test/e2e/README.md`](test/e2e/README.md) for the `prepare -> <suite>.<case>.sh -> run` contract and accelerator case prerequisites.

Source builds require Go 1.26.1 or newer. Native prerequisites vary by target. The current source tree documents and builds any native cache dependencies through repository scripts. Real object-storage tests must use explicit test credentials or a local S3-compatible service; ordinary unit and local-filesystem tests must not require production cloud credentials.

`cache-ctl` normally uses CGO and a locally built static `librocksdb.a` from
`deps/build-rocksdb.sh`; `manifest-ctl`, `store-ctl` and the downstream import
surface do not require RocksDB. `make release VERSION=vX.Y.Z` creates and checks
a local release bundle; the official cache payload must retain RocksDB support.

### Private-network and offline builds

The EROFS planner depends on the external `github.com/kuasar-sandbox/go-erofs`
module. Mirror or pre-cache the exact version selected by `go.mod`, including
its pseudo-version when applicable, and verify it against `go.sum` before
building offline. Point `GOPROXY` at the deployment's mirror, with its
checksum-database policy, or use a populated verified module cache. A checkout
of go-erofs or a local replacement is not required. Coordinated sibling
development can use the [project workspace](https://github.com/kuasar-sandbox/kuasar-sandbox);
it is not a prerequisite for this component's standalone build.

## Running independently

The three services can be deployed independently of the full platform:

- use `manifest-ctl` to ingest or fetch sparse artifacts;
- run `store-ctl` with a filesystem root or an S3-compatible endpoint;
- run `cache-ctl` in a local, sharded, or tiered topology;
- link `app/store.Run` or `app/cache.Run` into a custom service binary. The official CLIs are peer callers of these same lifecycle functions; custom binaries do not exec or depend on `store-ctl`/`cache-ctl`.

Configuration examples must use local paths, documentation-reserved endpoints, and placeholder credentials. Production deployments should use protected endpoints, explicit resource budgets, durable authoritative storage, and observability appropriate to the selected cache topology.

## Release model

The selected owner source explicitly declares its compiled E2E helper inventory in `release/helper-inputs.json`. The current inventory is empty. Future helpers and their build recipe belong in this owner declaration; aggregate packaging discovers them without a platform filename list.

Production flatten planning uses the fixed external go-erofs module and has no
mkfs.erofs runtime dependency. Release comparison tests retain the matching
native EROFS Plan capability. Preflight admits
the `guest-runtime` source branch on the same release line once and records its
exact commit. The existing fixed-input transport carries that tree to both
architecture jobs, which use the native EROFS recipe/cache and check its Plan
capability before tests. This is a test-tool source dependency; the component
does not need a published Runtime binary to build, and Runtime publication
continues to consume the selected accelerator release.

Build from the selected source with the component Makefile. `release.sh package`
uses the matching binaries in `bin/<arch>`, or an explicit `RELEASE_BIN_DIR`;
it collects materials and creates the bundle without rebuilding those binaries
or resetting source/build caches. Keep the selected source checkouts, dependency
versions and native build records together with the outputs.

Packaging records the actual Go versions and effective module replacements.
Go/module LICENSE and NOTICE files come from the selected compiler installation
and matching module sources, preserving nested paths. Module resolution uses the
normal Go cache and routing; downloaded module checksums must match the binaries.
An organization namespace alone does not exempt modules from notice collection.
This component has no internal sibling dependencies. Unsupported local replacements
need versioned module inputs for the official package.

Materials live under `share/licenses/<component>` and
`share/sources/<component>`. The latter contains `SOURCES.tsv`,
`GO-BUILD-INFO.tsv`, `GO-MODULES.tsv` and `MATERIALS.sha256`.
Collection fails on missing notices, unreadable subtrees or partial traversals.
Independent validation checks the shipped inventory, checksums, required files,
source-record consistency, payload identities and archive paths/types/modes.
It does not fetch source checkouts or Go modules, compare notices with remote
source trees, or download/authenticate compiler distributions. Checksums and
VCS records are consistency checks, not proof of an arbitrary producer's identity.

The archive name identifies the requested release target. Project and internal
dependency records use a release version when its local tag matches the selected
commit, otherwise `git:<commit>`; packaging does not require creating future
target tags. The publisher passes the selected project SHA to validation before
Tag/Release writes, uses the bundle's `release-notes.md` body, and appends the
existing source/Preview markers. Trusted source selection, build/publish permission
separation and the refusal to replace published assets remain required.
Producer-supplied notes may not contain the publisher's reserved source/Preview markers.

The official package requires the three Linux command binaries for its selected
native architecture (amd64 for x86_64, arm64 for aarch64), with
their own Accelerator main-package identities; `cache-ctl` must include RocksDB.
The five shipped operational helpers are copied from the selected source tree.
Before extraction the validator rejects extra/duplicate payloads, links,
incorrect ownership or modes, and foreign material namespaces.

The normal cache build retains `build/<arch>/cache-ctl.map`. Packaging reads
that link map and the matching RocksDB library/source tree, including
`RELEASE_ROCKSDB_SOURCE_DIR` when supplied. It records the actual RocksDB,
libstdc++, libgcc and other linked system inputs with their digests and Debian/RPM
source-package versions. Copyright, license, NOTICE and referenced common-license
texts are copied from those packages; installed file bytes are not authenticated
against package-database digests. The RocksDB material set must include
`AUTHORS`, `COPYING`, `LICENSE.Apache` and `LICENSE.leveldb`.
Distinct native inputs cannot overwrite a shared material name. RPM collection
requires complete same-source sibling listings. These materials support release
review, not legal certification, and do not change the RocksDB backend or durability.

This repository publishes independent component versions named `vX.Y.Z`. Each published native-architecture component archive contains the three service binaries and the documented operational/performance helper scripts selected by the release contract. The aggregate platform archive carries selected user guides and canonical E2E runtime inputs from its explicit documentation/test selections. Internal design documents and source-only tests remain in the source repositories.

The project repository publishes aggregate versions named `release-vX.Y.Z`, selecting exact versions of `accelerator` and the other release units, validating their assets, and running cross-component tests. Aggregate and component version numbers are independent.

Release branches, Preview, Latest reconciliation and concurrency/cancellation
handling are owned by the [project release documentation](https://github.com/kuasar-sandbox/kuasar-sandbox/blob/main/docs/release.md).
For ordinary use, start from the [latest Stable aggregate channel](https://github.com/kuasar-sandbox/kuasar-sandbox/releases/latest).

## Documentation

Manifest, Store and Cache have complete English and Chinese editions. Existing English backend guides remain available:

- [Manifest — English](docs/manifest.md) / [Chinese](docs/manifest_zh.md) — sparse manifests, ingest/fetch, chunking, encryption, key tables, and public SDKs;
- [File artifacts and carriers](docs/file-artifacts.md) — immutable tarstream encryption, Bundle layout, access and publication.

- [Store — English](docs/store.md) / [Chinese](docs/store_zh.md) — filesystem and S3-compatible stores, generation retention, integrity, and explicit purge (not reachability GC);
- [Cache — English](docs/cache.md) / [Chinese](docs/cache_zh.md) — local, sharded, tiered, and erasure-coded caches;
- [`docs/cache-redis.md`](docs/cache-redis.md) — Redis-compatible UDS/TCP backend and external-service deployment examples.

OCI/directory-to-EROFS CLI usage is documented with [`guest-runtime`](https://github.com/kuasar-sandbox/guest-runtime) because `flatten-ctl` is part of the Runtime release unit.

The README provides the public component entry path; the complete design guides contain formats, operating limits, failure behavior and measurement requirements.

## Project boundaries

- MicroVM lifecycle and snapshot execution belong to [`sandboxer`](https://github.com/kuasar-sandbox/sandboxer);
- network allocation and forwarding belong to [`connector`](https://github.com/kuasar-sandbox/connector);
- the guest kernel, runtime image, and `flatten-ctl` release artifact belong to [`guest-runtime`](https://github.com/kuasar-sandbox/guest-runtime);
- node and cluster orchestration belong to [`orchestrator`](https://github.com/kuasar-sandbox/orchestrator);
- system-level design, shared integration tests, demos, and aggregate releases belong to [`kuasar-sandbox/kuasar-sandbox`](https://github.com/kuasar-sandbox/kuasar-sandbox).

## Contributing and security

Read the repository-specific [contribution guide](CONTRIBUTING.md) and the [organization contribution guide](https://github.com/kuasar-sandbox/.github/blob/main/CONTRIBUTING.md). Changes to public data formats or downstream package contracts require linked companion pull requests and exact-source project validation.

Do not report vulnerabilities or real storage credentials in public issues. Use the [Kuasar Sandbox Security Policy](https://github.com/kuasar-sandbox/kuasar-sandbox/security/policy) and GitHub private vulnerability reporting.

## License

Original project content is licensed under the [Apache License 2.0](LICENSE). Preserve the license, attribution, NOTICE, and source obligations of native libraries, vendored code, generated code, formats, and third-party tools included in source or release assets.
