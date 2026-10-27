[English](manifest.md) | [简体中文](manifest_zh.md)

# manifest — the interface to content-addressed storage

`manifest-ctl` is the entry and exit point for content-addressed data. Image, memory-snapshot, disk and user-file bytes can be ingested through the sparse-source API; the CLI `store` command specifically accepts a **tarstream artifact**, not an arbitrary raw file. Ingest produces a **Manifest**: a compact binary object containing chunk metadata and a sealed table of chunk keys. Its content key is the handle for later `load` operations and `manifest://<key>` references. Local Manifest Bundles provide another carrier for the same objects ([§2](file-artifacts.md#2-multi-manifest-bundle-carrier)).

## 1. Overview

### 1.1 Role

```mermaid
flowchart TD
  S["Sparse source: file, stdin or tarstream"] --> C["Chunk data; record holes"]
  C --> E["Choose RAW or Snappy; encrypt"]
  E --> P["Store chunk objects"]
  E --> M["Build index and seal key table"]
  P --> M
  M --> O["Store Manifest; return content key"]
```

The read direction reverses this flow: `load` gets the Manifest, resolves its chunk list, fetches through the configured cache or directly from store, decrypts and emits a tarstream artifact encoded according to `crypto.local`. A configured cache is the selected remote read path; this does not imply automatic direct-store retry on cache errors.

### 1.2 Principles

- **Write path:** `sparse.Source → chunker → Snappy/RAW selection → encrypt → store.Put → Manifest`. Hole runs are recorded without reading them. Zero runs synthesize zero bytes for chunking without fetching a payload.
- **Read path:** `Manifest → fetch → decrypt → io.Writer`.
- **Deduplication unit:** a variable-length FastCDC chunk or a fixed-size chunk.
- **Deduplication domains:** salt separates convergent-key domains. Chunk payloads are stored as ciphertext; the Manifest's index/geometry and sealed key-table container are not wholly encrypted. Salt separation and encryption do not implement tenant authorization (§4.4–4.5).

### 1.3 Non-goals

- Managing backend bytes directly: this belongs to [store](store.md).
- Maintaining a cache: this belongs to [cache](cache.md).
- Understanding Docker, OCI, EROFS or other high-level payload formats: the data layer handles bytes and sparse geometry.
- Implementing ACLs: the caller/deployment protects customer keys, endpoints and references.

## 2. Command-line interface

### 2.1 Shared flag

```text
Shared flag on subcommands that accept configuration:
  --manifest-config string    Manifest YAML path; overrides MANIFEST_CONFIG
```

Commands that use configuration require either `--manifest-config` or `MANIFEST_CONFIG` to name a valid YAML file. **Exceptions:** `config generate`, local-file/stdin `info`, and local-file `diff` do not need a configuration file. See §3.1 for the schema. The flag belongs after the subcommand, not before it.

### 2.2 Subcommands

| Command | Purpose |
|---|---|
| `store` | Ingest data, upload the resulting Manifest, and return its hex content key. |
| `load` | Read the plaintext logical data selected by a Manifest key, wrapped in a tarstream artifact. |
| `get-manifest` | Fetch raw Manifest bytes by hex key, chiefly for inspection. |
| `info` | Print Manifest metadata without unsealing its key table. |
| `verify` | Verify the stored Manifest and all nonzero chunks through the fetch path. |
| `diff` | Compare chunk overlap between two local Manifest files. |
| `config show` | Print parsed configuration as YAML. |
| `config generate` | Print a commented configuration template. |

Data sources and Manifest keys are **positional arguments**. Omitting the `store` input or the `info` source, or passing `-`, selects stdin. `load`, `get-manifest` and `verify` accept either a bare 64-character lowercase hex key or `manifest://<key>`. **Remote `info` requires the `manifest://` prefix**; a bare hex argument there is treated as a local filename. Put flags before positional arguments: Go's standard `flag` parser stops at the first non-flag argument.

`pkg/manifest` also exposes a canonical reference parser. A `manifest://` reference contains exactly one 64-character lowercase hexadecimal content key. Compose layers using an explicit reference array and `fetch.NewLayered`; `manifest://k1:k2` is not supported.

File references have this form:

```text
file://<path>[@digest:<digest>|@hmac:<digest>|@manifest:<key>][@location:<name>]
```

The three identity qualifiers are mutually exclusive. `@manifest` selects a root Manifest in a Manifest Bundle and must be rejected by a tarstream opener; a Bundle opener must reject `@digest` and `@hmac`. With a location, the path must be a basename. The location must match `[A-Za-z0-9][A-Za-z0-9._-]*` and carries a logical name, not a host directory. A leading digit is allowed so UUIDv7 sandbox/build IDs can be used directly.

### 2.3 `manifest-ctl store` — ingest

```text
manifest-ctl store [flags] <REF|->        # omitted input or "-" means stdin

Flags:
  --extra-salt string         Extra salt bytes mixed into the store's opaque salt
  --no-progress              Suppress live progress
```

The input is one selected logical source: a local tarstream path, an identity-bearing or located file reference, a Bundle selector, or a Manifest key/ref. A Bundle with multiple Manifests requires `@manifest:<key>`. Files are opened as bounded random-access views; stdin uses tarstream's one-pass `SourceFrom` protocol. Both paths use the shared `pkg/manifest/transfer` reader and writer. Holes come from authoritative carrier metadata. `crypto.local=off|auto|required` selects the existing local tarstream encryption policy.

Store also accepts `--ref-location`, `--strip-tail`, `--extract-tail PATH` and `--append-tail PATH` with the same meanings as load below. Source integrity, original sequential marker/trailer/EOF and requested output-file completion are checked before publishing the Store root Manifest.

Stdout contains one 64-character hex line: the uploaded Manifest content key. Stderr contains live progress and a human-readable final summary. **`--no-progress` suppresses live progress, not the final summary.** An illustrative summary is:

```text
image size:   10.0 GiB
stored bytes: 1.0 GiB
chunks:       stored=2048 dedup=18432 zero=0
compression:  raw=512 snappy=19968 logical=10.0 GiB encoded=2.1 GiB saved=7.9 GiB
manifest:     encoding=raw logical=1.8 MiB stored=1.8 MiB
manifest key: a1b2c3d4...
```

Examples assume a valid configuration and customer key. `disk.img` and `snap.bin` below are already tarstream artifacts, despite their filenames; abbreviated keys elsewhere in this guide are placeholders for complete keys.

```bash
# Artifact file → Manifest key
MKEY=$(manifest-ctl store disk.img)

# Artifact stdin → Manifest key
cat disk.img | manifest-ctl store > disk.key

# Docker archive → flatten → ingest; upload-only stdout is the Manifest key
docker save myapp:v1 | flatten-ctl export --upload > app.key

# Extra salt narrows the deduplication domain; flags precede the input
manifest-ctl store --extra-salt "tenant-xyz" snap.bin

# Change chunking or crypto policy through YAML
```

### 2.4 `manifest-ctl load` — read data

```text
manifest-ctl load [flags] REF...

Args:
  REF...                      Top-to-bottom layers: a bare key, manifest://,
                              local paths/file:// tarstream, or Bundle @manifest ref
Flags:
  --output string            Output path (default "-", stdout; refuses a terminal)
  --output-mode string       none, tarstream, or bundle (default tarstream, or
                             none when --store is present)
  --store                    ingest the final transformed logical source
  --extra-salt string        domain separation for Store and/or Bundle ingest
  --ref-location N=file:///P map a named file location (repeatable; P absolute)
  --strip-tail               remove a suffix ZIP from every input before layering
  --extract-tail PATH        save the original first input suffix ZIP
  --append-tail PATH         append a validated suffix last; existing tails require strip
  --name string              Payload tar entry name (default "image")
  --offset uint              Start of the logical window
  --length uint              Window length (0 = the remaining bytes)
  --no-progress              Suppress live progress
```

References are layered in argument order with `fetch.NewLayered` (first is topmost). Data and explicit zero bytes cover lower layers; holes remain transparent. An ordinary local path is accepted directly; a 64-character hexadecimal key retains its Manifest meaning. Use `./<name>` for a local filename that is itself a complete key. File identity qualifiers are verified and named locations use the shared `manifest.RefLocations` parser.

The operation order is: inspect/extract the original top tail; strip each source tail when requested; layer; apply offset/length; append the new tail. A bare source can accept `--append-tail` directly. Existing tails require `--strip-tail`; recognizable malformed tails fail. `--output-mode=none --extract-tail PATH` extracts only the bounded original metadata. Tail operations address the logical content, not the outer Bundle archive. Retained old/new tail bytes share a 64 MiB budget; decoded ZIP metadata is bounded separately.

`--store` writes the final transformed source and defaults file output to `none`. It combines with either file output mode. Store+Bundle obtains one admission and uses one Chunk/Manifest encoding stream with bounded backpressure, so both outputs have identical physical objects and root keys for the same options. `--extra-salt` applies to both Bundle-only and Store writes. Store+tarstream uses a bounded pipe to feed the existing ingest protocol, including when input is stdin.

With a named output file, the Store key is printed on stdout. With binary stdout, identifiers and diagnostics go to stderr. Bundle output reports its root key on stderr. Requested files are created exclusively and written directly; failures clean up only still-owned incomplete files. Existing files, input/output aliases and conflicting output paths are rejected before writing. The returned success covers source validation, file closure and Store-root publication. A later target failure may leave an independently completed file or already-written chunks; it never authorizes changing an old source.

Tarstream output preserves sparse metadata. `crypto.local=off` emits plaintext; `auto|required` emit encrypted v1. Non-seekable stdin uses tarstream; random-access Bundle input is supplied by a file/ref. No complete image buffer or staging file is created by these transfer paths.

New tarstream output always uses [presence v1](file-artifacts.md#11-page-presence-and-zero-transport): the final payload before an optional declared metadata tail is limited to 1 TiB; empty and byte-sized dense windows are supported without padding. Each Hole must start on a page boundary and end on a page boundary or at the exact payload end. An `--offset`/`--length` window or old input carrier with unrepresentable Hole boundaries is rejected before tarstream bytes are written, without changing the boundaries or re-encoding in the old format. Existing old tarstreams remain readable. These tarstream output constraints do not restrict byte-granularity Store-only or Bundle output.

```bash
# Read and merge explicit top-to-bottom layers
manifest-ctl load --output merged.tar A B C

# Write matching Bundle and Store objects in one operation
manifest-ctl load --store --output-mode=bundle --extra-salt tenant-a \
  --output merged.bundle A B C

# Replace the metadata tail after payload merging
manifest-ctl load --strip-tail --append-tail new-tail.zip \
  --output-mode=bundle --output merged.bundle A B C

# Extract metadata without writing payload bytes
manifest-ctl load --output-mode=none --extract-tail metadata.zip SOURCE

# stdin can feed Store and a tarstream output together
cat source.tar | manifest-ctl load --store --output-mode=tarstream \
  --output copied.tar -
```

### 2.5 `manifest-ctl get-manifest` — retrieve Manifest bytes

Fetch the raw Manifest object, mainly for debugging or offline `info`/`diff`. Retrieval follows the configured cache/store path.

```text
manifest-ctl get-manifest [--output -|FILE] <hex|manifest://hex>
```

```bash
# Inspect fetched bytes; alternatively use info manifest://<hex>
manifest-ctl get-manifest a1b2c3d4... | manifest-ctl info -

# Save the physical Manifest verbatim
manifest-ctl get-manifest --output disk.manifest a1b2c3d4...
```

### 2.6 `manifest-ctl info`

Read and summarize a Manifest from one of three sources: a local path, stdin (`-` or omitted), or `manifest://<hex>` through the configured remote reader. Local/stdin inspection needs neither a configuration, customer key nor store connection. Remote metadata retrieval needs configuration and connectivity but does not unseal the customer-key-protected key table.

```text
manifest-ctl info [--manifest-config <path>] <file|-|manifest://hex>
```

Illustrative output:

```text
version:       1
image size:    10.0 GiB (10737418240 bytes)
chunk mode:    cdc
chunk count:   20480
zero chunks:   0 (0 B)
holes:         0 (0 B)
min chunk:     128.0 KiB    (configured)
max chunk:     1.0 MiB    (configured)
avg chunk:     512.0 KiB

size distribution (data chunks):
       min(N)          P1          P5         P25         P50         P75         P95         P99      max(N)
  63.5K(1)       128.0K      256.0K      384.0K      512.0K      640.0K      896.0K        1.0M     1.0M(412)
  (max == configured max — 412/20480 (2.0%) chunks are forced CDC cuts)
key table:     655389 bytes (sealed)
manifest size: 1802334 bytes
```

The minimum/maximum marked `(configured)` are the bounds stored in the Manifest header, not measured extremes. The percentile distribution below covers nonzero data chunks. Parentheses in `min(N)`/`max(N)` count chunks at each observed extreme. The final diagnostic appears only when the measured maximum equals the configured cap, identifying forced-size CDC cuts.

`manifest-ctl info manifest://<hex>` is the direct equivalent of `get-manifest … | info -` for remote inspection.

### 2.7 `manifest-ctl verify`

Fetch and decrypt every nonzero chunk to verify the entire Manifest's availability and content. This command forces physical SHA-256 checking even if ordinary reads disable it.

```text
manifest-ctl verify [--no-progress] a1b2c3d4...        # or manifest://a1b2c3d4...
```

```text
verified: 20480  skipped(zero): 0  holes: 0  failed: 0  total chunks: 20480
```

### 2.8 `manifest-ctl diff` — deduplication analysis

```text
manifest-ctl diff <manifest-a> <manifest-b>
```

Both arguments are local Manifest files, which can be obtained with `get-manifest`. `diff` neither opens a store nor loads a configuration or customer key.

`shared` and `only in A/B` count distinct nonzero ContentKeys, not repeated entry positions. Their byte totals sum each unique chunk's logical size once, not compressed/encrypted Store bytes. Zero entries and holes are excluded from these sets; image size, raw entry count and holes remain separately reported. Conflicting logical sizes for the same ContentKey, within or across inputs, are rejected.

The CLI's `dedup ratio` is `1 - |A ∪ B| / (|A| + |B|)`, with zero for an empty denominator. Pairwise overlap (Jaccard), used by the warm-pool workload, is instead `shared / (shared + onlyA + onlyB)`. Neither is a universal VM-memory saving guarantee. Illustrative output:

```text
manifest A:  10.0 GiB, 20480 chunks (20480 unique)
manifest B:  10.1 GiB, 20512 chunks (20512 unique)
shared:      18432 chunks (9.0 GiB)
only in A:   2048 chunks (1.0 GiB)
only in B:   2080 chunks (1.1 GiB)
dedup ratio: 45.0%
```

### 2.9 `manifest-ctl config`

```text
manifest-ctl config show     [--manifest-config <path>]
manifest-ctl config generate
```

`generate` prints a commented template. `show` prints the loaded configuration as YAML; it does not prove that all backend connections and deferred constructors will succeed.

## 3. Configuration

### 3.1 manifest-config.yaml

```yaml
manifest:
  key: "0a1b2c3d..."              # Placeholder: a 32-byte hex customer key seals the key table
  verify_content: true           # Recheck physical Manifest/Chunk SHA-256; default true
  # write_generation: G3         # Optional write generation
store:
  endpoint: 127.0.0.1:7100        # Required for remote writes/direct-store reads; optional for offline Bundles
                                 # TCP host:port or UDS (/run/sandbox/store.sock or unix:///...)
  pool: 4                        # Independent grpc.ClientConn instances, round-robin
  timeout: 5s                    # Per-store-RPC timeout
cache:
  endpoint: 127.0.0.1:7070        # Empty selects direct store; otherwise use cache wire; UDS also accepted
  pool: 4
  timeout: 2s
chunker:
  mode: cdc                      # cdc | fixed
  cdc:
    min: 128KiB
    avg: 512KiB
    max: 1MiB
  fixed:
    size: 512KiB
crypto:
  chunk: aes                     # Only supported value
  manifest: aes                  # Only supported value
  local: off                     # off | auto | required; default off
```

Fields:

- `manifest.key` is the customer key that **seals the Manifest's key table**; it does not derive the chunk encryption key. Losing it prevents unsealing that Manifest's table. It may be omitted from YAML and supplied through **nonempty `MANIFEST_KEY`**, the primary delivery mechanism. A nonempty environment value overrides YAML; an empty one falls back to YAML. The key is resolved lazily and not written back into Config, so `config show` does not echo the environment-provided key. **A key explicitly stored in YAML remains in Config and can be printed by `show`.** If both sources are empty, operations needing the key fail.
- `manifest.verify_content` defaults to `true`. Ordinary Bundle/cache/store Fetchers, `GetManifestBlob` and `CheckManifest` share the policy. When enabled, they check `SHA256(physical Manifest) == requested key` before parsing and `SHA256(physical Chunk) == CiphertextHash` when first reading a chunk. When disabled, ContentKey is a locator and those complete-object hash scans are skipped. The process emits one clear warning. Envelope/geometry checks, key-table GCM authentication, chunk format/length validation, decryption and decompression remain. Chunk AES-CTR itself is not authenticated encryption; disabling the physical hash check removes that chunk-content integrity check. `manifest-ctl verify`, full Bundle verify/upload/export and object repair always force SHA verification.
- `manifest.write_generation` optionally selects a write generation. With `store.endpoint`, empty means `AdmitWrite()` selects the latest admitted generation; a value uses `AdmitWriteFor(generation)` and fails if it was removed. A Store RPC failure does not fall back to local derivation. An offline Bundle writer without Store uses the ordinary generation name `NONE` when unset, or the explicit value otherwise; both derive salt with public `store.SaltForGeneration`. `NONE` is neither reserved nor a special protocol value.
- `store.endpoint` points at the store-ctl gRPC service for remote object I/O. It accepts TCP `host:port` or a Unix socket. Bare `/run/sandbox/store.sock` is normalized to `unix:///run/sandbox/store.sock`; an explicit `unix:` target is passed through. Match store-ctl's `listen` address. Same-host UDS avoids TCP transport; local Bundle object I/O follows the separate Bundle reader/writer contract.
- `cache.endpoint`: empty selects direct-store reads; nonempty selects cache-ctl's wire protocol. It accepts `host:port`, explicit Unix targets or a bare socket path, matching cache-ctl's `listen`. This is source selection at construction, not error-triggered failover around a configured cache.
- `chunker.mode` is `cdc` (FastCDC) or `fixed`; see §4.1.
- `crypto.chunk` and `crypto.manifest` each accept only `aes`; other values fail when the crypto objects are constructed (§4.3–4.4).
- `crypto.local` is a local-format compatibility/enforcement policy, not an algorithm selector. `off` does not enable the local codec; `auto` reads legacy plaintext or encrypted carriers; `required` accepts only encrypted carriers. The default is `off`.

### 3.2 Loading order

The **configuration filename** comes only from the CLI flag or its environment variable; there is **no automatic discovery**:

| Source | Precedence |
|---|---|
| `--manifest-config FILE` | First, when nonempty. |
| `MANIFEST_CONFIG` | Used when the flag is empty. |
| Neither | Error for commands that need configuration; §2.1 lists the exceptions. |

The **customer key** has a separate precedence rule: nonempty `MANIFEST_KEY` overrides `manifest.key` from YAML. Thus a shared config file can omit the key while the command receives it separately.

`pkg/manifest.ParseConfig` also accepts **in-memory YAML bytes**, for example configuration delivered to sandbox-ctl over a config socket. Endpoint and crypto settings need not be written to disk. A caller may set the key separately in `Config.Manifest.Key`; the environment override still applies when `CustomerKey()` is used.

There are no per-field command-line overrides for chunker or crypto settings. Change the YAML to change those settings.

## 4. Design

### 4.1 Chunking

Chunking divides the logical byte stream into deduplicable units. Two modes are available.

#### FastCDC (cdc)

Variable-length, content-defined chunking selects boundaries using a rolling hash and Gear table.

| Parameter | Default | Meaning |
|---|---|---|
| `min` | 128 KiB | Minimum cut length; the final fragment of a data segment may be shorter. |
| `avg` | 512 KiB | Target average length used to derive the Gear masks. |
| `max` | 1 MiB | Maximum length; reaching it forces a cut. |

These sizes and fixed `size` must be multiples of 4 KiB. The chunker constructor validates them; merely parsing YAML is not equivalent to constructing a valid chunker. The Manifest ingester also enforces the canonical 64 MiB maximum decoded chunk size.

Content-defined boundaries can regain alignment after insertions/deletions and retain deduplication beyond the changed area. This implementation aligns cuts to 4 KiB boundaries; the amount of boundary disturbance depends on the edit, alignment, sparse segmentation and content. It does not guarantee that any arbitrary byte insertion changes only a fixed small number of chunks.

#### Fixed-size (fixed)

Fixed-size chunks default to 512 KiB. The layout is simple and predictable, but insertions/deletions that shift later fixed boundaries can substantially reduce deduplication. This is useful as a performance baseline and for predictable layouts; CDC is the default for content reuse. Relative speed and hit rate depend on the workload.

Change YAML `chunker.mode` to select the mode; the command-line interface is unchanged.

### 4.2 Convergent encryption

The invariant is **same plaintext + same salt → same ciphertext**, allowing objects within a shared deduplication domain to reuse stored chunk bytes. Chunk payloads are encrypted; this is not a claim that every Manifest field is secret or that a store cannot infer repeated content.

#### Key derivation

```text
salt = server_salt (+ extra_salt mixing)   # §4.5
key  = SHA256(salt || plaintext)          # convergent key
```

AES-CTR uses an all-zero IV. The key is derived from the **original plaintext**, while the fixed canonical encoder selects one RAW or Snappy payload for that plaintext. Thus the supported canonical writer does not encrypt two different encoded payloads with the same derived key/IV pair. Under the hash assumptions, identical plaintext in one salt domain produces the same physical object, the same `ContentKey = SHA256(object)`, and one stored byte sequence.

This requires the Go Snappy version, `snappy.Encode`, benefit threshold and format rules to stay fixed. The current reader rejects the older development layout. Do not mix incompatible writers within a salt/key domain. A future change to encoder output or thresholds requires domain separation and an explicitly reviewed migration; changing the format byte alone is insufficient. Verify the selected release's reader/writer compatibility and preserve existing assets and rollback data rather than treating an old “not yet released” note as permission to delete a live salt domain.

The canonical encoder is the block API in **`github.com/golang/snappy` v1.0.0**. Golden tests constrain identical encoded bytes across amd64, amd64 `noasm` and arm64. Byte-for-byte consistency takes precedence over a higher compression ratio. The standard and Snappy-compatible `klauspost/compress/s2` encoders can produce different block bytes across architecture implementations, so they are not used for canonical writes.

#### Content key

Chunks use `SHA256(physical_object)` as the store key; Manifests use `SHA256(physical_envelope)`. Salt is not separately concatenated into the address calculation. It changes encrypted object bytes, which changes their physical hash. Store can therefore expose one content-key KV view while preserving distinct encrypted objects for different salt domains. See [store](store.md).

### 4.3 Chunk encryption format

YAML `crypto.chunk` accepts only `aes`; all other values fail at construction, with no plaintext fallback.

Every nonzero chunk is encoded as a Go Snappy block candidate. Snappy is selected only if both canonical conditions hold; these are not configuration options:

```text
saved = rawSize - encodedSize
Snappy iff saved >= 4 KiB AND encodedSize * 4 <= rawSize * 3
```

The implementation evaluates this rule without overflowing adversarial integer lengths.

| Format byte | Payload | Physical object |
|---|---|---|
| `0x01` (`AESRaw`) | Original plaintext. | `[0x01] || AES-256-CTR(key, IV=0, plaintext)` |
| `0x02` (`AESSnappy`) | `snappy.Encode(plaintext)`. | `[0x02] || AES-256-CTR(key, IV=0, snappy_block)` |

The encryption layer derives `SHA256(salt || plaintext)` internally; its caller passes salt, not a chunk key. Distinct plaintexts receive distinct keys under the hash assumptions, avoiding intentional CTR keystream reuse within the canonical contract. The format byte is cleartext but included in the physical ContentKey.

The Manifest entry records the original plaintext size and the complete physical hash; it does not add a codec field. Unknown formats, inconsistent lengths, truncation, invalid Snappy blocks and Snappy objects that violate the fixed benefit threshold fail. There is no format guessing or fallback. With physical SHA verification enabled, changed object bytes fail hash validation; AES-CTR and structural checks alone do not authenticate all possible plaintext corruption when that verification is disabled.

### 4.4 Manifest encryption format

The Manifest's **key table** encodes all nonzero chunks' encryption keys.

| Mode | Behavior |
|---|---|
| `aes` | AES-GCM seals the key table with `manifest.key` and Manifest AAD; the customer key is required to unseal it. |

This protects the key table, not the entire Manifest index. A store holding only the objects does not obtain the customer key from them. However, the chunk key is derived from salt and plaintext independently of that customer key. An actor with the relevant salt and a plausible plaintext guess can derive a candidate chunk key and test the corresponding object. Same-domain deduplication also reveals equality. Therefore “the customer key never leaks” alone is not a universal guarantee that stored chunk contents cannot be inferred. Protect key/salt access, references and service authorization according to the deployment's trust boundary.

### 4.5 Salt and deduplication domains

Different salts derive different chunk keys from the same plaintext, yielding different ciphertext, physical ContentKeys and stored objects.

The effective salt combines:

- **`server_salt`:** each ingest obtains one write admission from store, carrying a generation and opaque salt. Default writes call `AdmitWrite()`; an explicit write generation uses `AdmitWriteFor`. All chunks and the final Manifest reuse the admission. A generation change affects newly admitted ingests, not already admitted work.
- **`extra_salt`:** optional bytes from `--extra-salt` (§2.3), mixed into the admitted salt.

```text
final_salt = server_salt                                                   # empty extra_salt
final_salt = SHA256("accelerator-extra-salt-v1" || server_salt || extra_salt) # nonempty extra_salt
```

`extra_salt` creates a narrower deduplication domain within one store generation, for example per tenant. This is domain separation, not an ACL.

### 4.6 Manifest binary format

The current Version1 **physical envelope** is:

```text
[4 bytes "MANI"][1 byte encoding][payload]

encoding 0x00 (RAW): payload = logical manifest bytes after magic
encoding 0x01 (Snappy): payload = snappy.Encode(logical manifest bytes after magic)
```

RAW/Snappy uses the same fixed minimum-4-KiB-saving and maximum-75%-encoded-size threshold as chunks. The reader does not accept the old `[MANI][Version1]...` physical representation.

Before Snappy allocation/decode, the reader calls `snappy.DecodedLen`. The complete logical Manifest, including magic, is limited to **64 MiB**. Encoded length, table offsets/lengths and integer additions are bounds checked. A Snappy envelope violating the benefit threshold is rejected. RAW parses directly from the envelope payload without copying a complete Manifest to rebuild the old layout; Snappy retains one bounded decoded buffer.

The logical layout uses little-endian integers; magic itself is the byte string `MANI`.

| Region | Layout |
|---|---|
| Header, 64 B | `magic[4]`, `version u8`, `chunk_mode u8`, reserved padding; `image_size u64`, `chunk_count u32`, configured `chunk_min/max u32 × 2`; key-table offset/length, hole count and hole-table offset; reserved bytes to 64 B. Chunk mode 1 = CDC, 2 = fixed. |
| Chunk index, 56 B per entry | `image_offset u64`, `plain_len u32`, `flags u32` (bit 0 = zero chunk), `cipher_hash[32]`, **8 reserved zero bytes**. Entries are sorted by image offset. |
| Holes, 16 B per extent | `offset u64`, `size u64`. Data entries and holes exactly tile `[0, image_size)`. |
| Key table | Customer-key-sealed AES-GCM table of nonzero chunk keys: `key_0[32], key_1[32], ...`. |

All-zero chunks (flag bit 0) are neither encrypted nor stored and have no key-table entry; reads synthesize zero bytes locally. A Hole is an externally declared absence of data, distinct from an all-zero data chunk. Data chunks and holes cover the image without overlaps or gaps.

For ordinary reads with `manifest.verify_content=true`, the physical envelope hash is checked against the requested Manifest key before parsing. The reader parses the header/index, finds the requested chunk by binary search, unseals the key table, fetches the chunk by `cipher_hash` through cache/store, decrypts and writes the logical bytes. Disabling ordinary verification changes the hash-check step as described in §3.1; explicit verify paths always force it.

### 4.7 Write path in detail

The stages below describe one logical chunk; encryption and upload run concurrently across chunks as explained afterward.

| Stage | Operation |
|---|---|
| Sparse input | Record and skip Hole runs; synthesize Zero runs without fetching data. |
| Chunking | Split contiguous non-hole segments using CDC or fixed mode. |
| Key derivation | `key = SHA256(salt || original_plaintext)`. |
| Encoding | Compute a Go Snappy candidate and select RAW/Snappy by the fixed rule. |
| Encryption | AES-CTR plus the format byte produces a physical object and its SHA-256. |
| Address | Reuse the crypto layer's returned physical hash as ContentKey; ingest does not hash it again. |
| Chunk upload | `store-ctl Put(admission, partition=chunk, key=ContentKey, size, ciphertext)`. The server checks existence first; a dedup hit can finish without uploading the body. Otherwise the fs backend writes under `chunk/{generation}/aa/bb/<hash>`; other backends follow their own storage layout. |
| Metadata | Append chunk metadata and its key in logical order. |
| Seal | Seal the key table with the customer key and Manifest AAD. |
| Envelope | Encode logical Manifest bytes as the canonical RAW/Snappy physical envelope. |
| Publication | Put the Manifest under the **same admission**, in the Manifest partition. |

The derive/encode/encrypt/Put work uses a **bounded worker pool**, with worker count obtained from the store writer's `PoolSize()` capability. The standard store client's pool is configured by `store.pool`; a writer that does not expose this capability uses one worker. This is an ingest implementation limit, not a claim that one gRPC connection can have only one RPC in flight.

Chunking remains sequential, the index is assembled in source order and progress callbacks are serialized. Out-of-order upload completion does not reorder the Manifest. Reproducible Manifest bytes additionally require the same sparse input, chunker/codec settings, salt/admission and customer-key/AAD inputs; concurrency alone does not establish cross-configuration determinism. Bounded parallel upload can overlap backend round trips for multi-GiB snapshots. The [project performance guide](https://github.com/kuasar-sandbox/kuasar-sandbox/blob/main/docs/perf.md) describes measurement; no throughput improvement is guaranteed without a recorded workload and run.

### 4.8 Read path in detail

| Interface/stage | Responsibility |
|---|---|
| Manifest key | An on-demand Getter fetches the Manifest partition object. |
| Metadata | Parse the index and unseal the key table. |
| Stream | One Manifest creates one Stream; callers use `NewLayered` for explicit top-to-bottom layer arrays. |
| `RunAt` | Resolve an executable sparse Run (Hole, Zero or Data). Manifest Data additionally implements `fetch.ChunkRun`. |
| `ResolveChunkWindow` | Optionally expand one physical chunk within the final visible view. |
| `Run.ReadAt` | Read one already-resolved visible run. |
| `Stream.ReadAt` | Collect runs, then fetch visible Data concurrently through the on-demand Getter, verify/decrypt and use the bounded plaintext chunk cache. |
| Optional `Prefetcher.Prefetch` | Use the prefetch Getter to warm cache/store objects, then Release immediately. |

`Stream.RunAt` returns an immutable `sparse.Run` spanning `[Offset(), End())`. `Run.ReadAt` takes an offset relative to `Offset()` and rejects reads across `End()`. `RunAt` consults only sparse maps, the Manifest index or tar extent metadata. It does not read payloads, invoke cache/store Get, verify/decrypt bytes, or advance a one-shot source. Manifest Data Runs retain the initially resolved chunk index and implement `fetch.ChunkRun`; Hole, Zero and tar/file Data Runs implement only the ordinary sparse Run interface.

`fetch.ResolveChunkWindow(stream, anchor, maxBytes)` offers optional bidirectional metadata resolution for callers that want to reuse one physical-chunk read. The anchor must come from `RunAt` on that same final composed stream. If the physical chunk fits `maxBytes`, the helper returns the largest contiguous final-visible window containing the anchor and served by that chunk. An upper Hole is transparent; upper Data and Zero are hard boundaries, as is the root Stream's `Size()`. Portions of a lower chunk separated by opaque upper content are never merged across that content.

Resolution does not call a payload Getter, verify, decrypt or decompress. An over-limit chunk or a package ChunkRun whose physical identity cannot be resolved retains the original anchor, so the caller can continue using forward-only semantics.

`Stream.ReadAt` first collects all runs covering the requested window, before modifying the destination buffer. It then concurrently calls `Run.ReadAt` only for Data; Hole and Zero fill with zeros. Layered Streams apply top-to-bottom visibility: Data and Zero hide lower layers; only Hole or being beyond a layer's size permits fallthrough. Each upper Hole tightens the bound before returning the final child Run. Nested overlays retain ChunkRun capability, and a lower chunk cannot cross a position where any upper layer becomes visible again. A partial read (`--offset`/`--length`) fetches only physical chunks needed for that window, without scanning/materializing the whole image.

A Stream supporting prefetch also implements `fetch.Prefetcher`. `Prefetch(ctx)` traverses the complete logical Stream and invokes full-physical-chunk cache Get only for finally visible ChunkRuns implementing the package's prefetch capability. Ordinary Data, Hole and Zero are no-ops. A successful Get is immediately Released without reading the Blob, verifying, decrypting or pinning it. Prefetch operates on the current composed Stream's full visible view; there is no longer a manifest-key API for selecting a leaf to prefetch.

Within one Fetcher, Manifest metadata reads, ordinary ReadAt calls and all Streams share a Getter and request scheduler. On-demand requests never wait for an already-started prefetch. While any on-demand operation is present, no new prefetch is admitted; at most one prefetch Get is in flight per Fetcher. A started prefetch is not preempted and can briefly overlap a later on-demand operation. Different Fetchers are independent. The scheduler creates no background goroutine and does not own the underlying Getter's lifetime.

Each Manifest Stream has an independent decrypted-chunk cache for on-demand partial reads. Its key includes `CiphertextHash`, the chunk decryption key and plaintext size: repeated physical content within that Stream can be reused without aliasing different keys or logical sizes. Both **32 entries** and **32 MiB** are hard cache-admission limits, with LRU eviction. An entry expires after **5 seconds** idle since its last hit. Each Stream keeps one timer for the earliest expiry, actively reclaiming stale data even when reads stop, without one timer per entry or a resident scanning goroutine.

Concurrent misses for one cache key coalesce into one Get, SHA verification, decryption and optional Snappy decode; different keys can load concurrently. Failed loads are not cached. Waiters with a still-valid context may retry rather than permanently inheriting the first loader's cancellation. A partial miss allocates one plaintext-sized cache buffer and writes directly into it while the immutable Blob remains alive; ciphertext is neither copied nor modified. Eviction, expiry and `Close()` clear plaintext. An entry currently being copied is removed from the LRU first and cleared after its last reader releases it.

A cold, full-physical-chunk read uses a direct path so a one-off sequential scan does not pollute the cache; an existing cache hit may still serve a full read. Plaintext chunks larger than 32 MiB also bypass cache admission. RAW full reads decrypt AES directly into the caller's buffer, without an extra plaintext allocation or full-chunk copy. Oversized RAW partial reads seek the CTR block/offset and decrypt only the requested range.

For Snappy full reads, encrypted payload is decrypted into bounded encoded scratch, `DecodedLen == entry.Size` is checked, then decode writes into the caller's buffer. Oversized Snappy partial reads must decode the whole block into bounded temporary plaintext and copy the requested range because the block format does not support arbitrary-range decode.

Snappy scratch uses explicit size-class free lists, not a `sync.Pool` with no retained-byte ceiling. Encode and decode have separate `max(1, GOMAXPROCS)` CPU-slot limits, **192 MiB active-byte budgets** and **64 MiB retained budgets**, totaling at most 256 MiB of codec-owned scratch per direction. The largest normally retained buffer is 2 MiB; larger buffers are discarded after use. Slot and weighted-byte admission honor caller cancellation. RAW reads do not take a decode slot. There is no background goroutine for scratch management.

Prefetch still performs only full physical-object cache Get and immediate Release. It neither verifies/decrypts nor populates the plaintext cache. Thus prefetch and on-demand ReadAt may each issue a Get, while repeated on-demand partial reads are coalesced and reused inside the Stream.


### 4.9 Source selection and lazy recovery

Manifest/Bundle reads keep the source selected by the existing metadata and ordered-reference rules. A failed local read never falls through to another Bundle or Store merely because it failed. When unavailable ordered refs leave lookup uncertain, a later remote miss cannot be promoted to proof of global absence; original causes remain inspectable.

Chunk-index preparation serializes loading, builds and validates a complete map in attempt-local state, and publishes it only after success. Failure or cancellation is not cached, so a later Open can retry initialization at the same selected source. Reader Close continues to wait for existing source leases and prevents late publication. Successful preparation is reused; failed preparation retains no poisoned index state.

## 5. Performance characteristics

The [project performance guide](https://github.com/kuasar-sandbox/kuasar-sandbox/blob/main/docs/perf.md) provides the measurement context for end-to-end `manifest://` loads with cold/warm L1. Record actual revisions, workload and cache state; document structure or benchmark names alone are not fresh measurements.

The principal variables are:

- Chunk mode: CDC can improve reuse after content changes but costs chunking work; its advantage over fixed mode is workload-dependent.
- Fixed RAW/Snappy selection and AES: incompressible data stays RAW; compressible data reduces physical hash/AES/wire bytes.
- Store latency and backend behavior, such as local fs versus remote S3-compatible object storage; see [store](store.md).

Repeatable microbenchmarks include `BenchmarkCompressionCandidates` (RAW, Go Snappy, S2, S2 Better and Zstd SpeedFastest controls), `BenchmarkAESChunkCanonicalCodec` and `BenchmarkManifestAESPhysicalRead`. Only Go Snappy enters the production codec; benchmark candidates are not configuration or negotiation options. Codec benchmarks also report `scratch-misses/op` to determine whether a warm size class reuses payload-sized buffers.

Bundle tail-index opening cost at 4K/20K/100K chunks is reported by `BenchmarkBundleOpen4K`, `BenchmarkBundleOpen20K` and `BenchmarkBundleOpen100K`. Open loads only the Manifest index, not the Chunk index, Central Directory, object Local Headers or payloads. `BenchmarkBundlePrepareChunks4K/20K` measures one-time selected-source Chunk-section preparation. `BenchmarkBundleGetAfterPrepare` measures O(1) lookup plus payload reading.

`BenchmarkBundleOpenHighRTT20K` injects a fixed ReaderAt RTT and reports `read-calls/op` and `read-bytes/op`. `BenchmarkVerifyContent` compares Manifest open and cold 1 MiB chunk reads with verification enabled/disabled.

`BenchmarkManifestSourceSearch` measures clean misses and final remote fallback with 0/1/8/32 refs. `BenchmarkMetadataOpen4K`/`BenchmarkMetadataOpen20K` measure prefix-only metadata opening and its independence from chunk count. Benchmark fixture names do not override container entry limits.

## 6. See Also

- [store](store.md): manifest-ctl writes remote bytes through store-ctl gRPC.
- [cache](cache.md): optional acceleration for reads; cache-ctl can use the same store client as its origin.
- [guest-runtime flatten](https://github.com/kuasar-sandbox/guest-runtime/blob/main/docs/flatten.md): image flattening followed by `flatten-ctl export --upload`.
- [sandboxer](https://github.com/kuasar-sandbox/sandboxer/blob/main/docs/sandbox.md): disk bases and snapshots referenced with Manifest keys.
- [system architecture](https://github.com/kuasar-sandbox/kuasar-sandbox/blob/main/docs/kuasar-sandbox.md): the Manifest abstraction in the overall platform.

Manifest source selection and lazy recovery are specified in [§4.9](#49-source-selection-and-lazy-recovery).

### Transfer identities and reconstructed boundaries

Explicit tarstream `@digest`/`@hmac` identities and Bundle `@manifest` selectors remain binding when ordinary `manifest.verify_content` is disabled. Content conversion completes carrier verification before reporting success. Carrier selection uses its bytes or an explicit selector rather than the filename extension. When a carrier declares a tail, extraction and stripping require that boundary to match the validated ZIP suffix.

For an unchanged complete Manifest/Bundle root, tarstream output recovers the boundary of a valid relative ZIP suffix. This preserves canonical image envelope identities across Store roundtrips; existing tarstream declarations and explicit windows retain their boundaries. Store statistics count new Manifest bytes only when the final Put creates the object. Tarstream-only loads report logical progress unless `--no-progress` is selected.

## 7. Independent Store roundtrip

Use this before adding a VM or cache. Prerequisites: Linux, Bash, Python 3,
`store-ctl` and `manifest-ctl` from one selected aggregate, plus that aggregate's
`flatten-ctl` from guest-runtime. Its [pure-Go tar stream/extract interface](https://github.com/kuasar-sandbox/guest-runtime/blob/main/docs/flatten.md#25-flatten-ctl-tar--tar-extraction-and-single-file-sparse-streams)
needs neither mkfs.erofs nor KVM/root. Check these subcommands in the selected
binary's help; older versions lacking this interface are outside this recipe.
Use matching selected versions, not unrelated latest component downloads.

The following disposable FS Store uses a private UDS and a generated test-only
customer key. No L2, MicroVM, registry, public listener or external data is involved.
[Store generation initialization](store.md#6-administrative-commands) and
§3 above remain authoritative. Run in a new shell. Exit cleanup always stops this
child; only success removes its temporary directory. Failure retains the private
directory and prints its path for diagnosis.

```bash
set -euo pipefail
umask 077
LAB=$(mktemp -d)
STORE_PID=
SUCCESS=0
cleanup() {
  result=$?
  trap - EXIT
  if [ -n "$STORE_PID" ]; then kill "$STORE_PID" 2>/dev/null || true; wait "$STORE_PID" 2>/dev/null || true; fi
  if [ "$result" -eq 0 ] && [ "$SUCCESS" -eq 1 ]; then
    rm -rf -- "$LAB"
  else
    printf 'Retained private diagnostics: %s (store.log)\n' "$LAB" >&2
  fi
  exit "$result"
}
trap cleanup EXIT
cat > "$LAB/store.yaml" <<EOF
listen: unix://$LAB/store.sock
backend: fs
fs:
  root: $LAB/objects
  verify_content_key: true
generations:
  file:
    path: $LAB/generations
EOF
store-ctl init --config "$LAB/store.yaml" --generation G1
store-ctl serve --config "$LAB/store.yaml" >"$LAB/store.log" 2>&1 &
STORE_PID=$!
for i in $(seq 1 50); do
  test -S "$LAB/store.sock" && break
  kill -0 "$STORE_PID"
  sleep 0.1
done
test -S "$LAB/store.sock"
export MANIFEST_KEY=$(python3 -c 'import secrets; print(secrets.token_hex(32))')
cat > "$LAB/manifest.yaml" <<EOF
store:
  endpoint: unix://$LAB/store.sock
  timeout: 5s
manifest:
  verify_content: true
chunker:
  mode: cdc
crypto:
  chunk: aes
  manifest: aes
  local: off
EOF
printf 'Kuasar independent storage roundtrip\n' > "$LAB/payload"
flatten-ctl tar stream -f "$LAB/input.tar" "image:$LAB/payload"
KEY=$(manifest-ctl store --manifest-config "$LAB/manifest.yaml" --no-progress "$LAB/input.tar")
manifest-ctl verify --manifest-config "$LAB/manifest.yaml" --no-progress "$KEY"
manifest-ctl load --manifest-config "$LAB/manifest.yaml" --output "$LAB/output.tar" "$KEY"
flatten-ctl tar extract -f "$LAB/output.tar" "image:$LAB/restored"
cmp "$LAB/payload" "$LAB/restored"
printf 'PASS: payload bytes match\n'
unset MANIFEST_KEY
SUCCESS=1
```

Expect a complete Manifest key, zero failed chunks from verify, and the final
PASS line. Compare extracted logical bytes: tar wrappers/names can differ after
load. A normal file or a generic tar is not a substitute for the platform
sparse/digest envelope. On failure inspect `store.log` in the retained directory
reported on stderr; check generation initialization, socket readiness and key availability;
do not retry an uncertain write as though it were a failed read. This validates
only a local byte roundtrip, not sparse VM state, cache tiers, S3 or recovery.
After diagnosis, explicitly remove only the reported private test directory.
For intentionally retained data, preserve the key/config/ref instead of using
this test-only cleanup; the generated key exists only in this shell environment.
