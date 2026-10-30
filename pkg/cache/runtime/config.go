// Package runtime loads YAML configuration and assembles the cache-ctl server.
package runtime

import (
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/kuasar-sandbox/accelerator/internal/util"
	"gopkg.in/yaml.v3"
)

// Config is the top-level YAML configuration for cache-ctl serve.
type Config struct {
	Mode         string `yaml:"mode"`          // "local" | "shard" | "tiered"
	Type         string `yaml:"type"`          // local/shard: "embedded" | "redis"
	Listen       string `yaml:"listen"`        // TCP, UDS, or fd=<name> (wire data)
	HealthListen string `yaml:"health_listen"` // TCP, UDS, or fd=<name> (gRPC health)
	RPCTimeout   string `yaml:"rpc_timeout"`   // e.g. "2s"
	// StatsInterval is the base period for the adaptive stats line printed to
	// stderr (silent in windows with no traffic). Empty/absent → 30s (on by
	// default); "0"/"off" disables it.
	StatsInterval string `yaml:"stats_interval"`
	// PprofListen, if non-empty, binds TCP or imports fd=<name> for HTTP serving
	// /debug/pprof/{profile,heap,goroutine,...}. Intended for
	// perf investigation — leave empty in production.
	PprofListen string        `yaml:"pprof_listen"` // TCP or fd=<name>
	Freq        FreqConfig    `yaml:"freq"`
	Rocks       RocksConfig   `yaml:"rocks"`  // used by type=embedded
	Redis       *RedisConfig  `yaml:"redis"`  // used by type=redis
	Tiers       []TierConfig  `yaml:"tiers"`  // tiered mode only
	Origin      *OriginConfig `yaml:"origin"` // tiered mode only
}

// FreqConfig holds CMS frequency sketch parameters. Embedded RocksDB also
// ages generations when successful value bytes reach RocksConfig.DiskBytes.
type FreqConfig struct {
	Counters        string `yaml:"counters"`       // counters per CMS row; default "8M"
	ResetAfter      string `yaml:"reset_after"`    // access count per generation; default "1M"
	ResetInterval   string `yaml:"reset_interval"` // optional timer, e.g. "1h"; unset disables it
	EvictThreshold  int    `yaml:"evict_threshold"`
	PersistInterval string `yaml:"persist_interval"` // optional timer, e.g. "5m"; Close always persists
	// DisableEviction turns off the frequency-based compaction filter
	// entirely. The sketch is still maintained for stats, but the
	// filter is never armed — no keys are evicted regardless of their
	// access count. Used when a cache-ctl local daemon plays the
	// "origin" role for a downstream tiered instance (bench scenarios),
	// where cold writes should survive frequency compaction. It does not
	// change the data-CF WAL policy or preserve unflushed values on a crash.
	DisableEviction bool `yaml:"disable_eviction"`
}

// RocksConfig holds RocksDB parameters.
//
// BlobDB is always enabled on chunk, manifest and blob CFs with fixed
// parameters (min_blob_size=4 KiB, blob_file_size=256 MiB) — there is
// no YAML knob to disable or tune it. Large values bypass the LSM main
// path automatically; small values (< 4 KiB) remain inline in the SST.
// These three rebuildable data CFs write without WAL; default/sketch keeps
// WAL. Writable Close waits for data flushes and reports errors. A crash
// can lose unflushed values, which become ordinary cache misses.
type RocksConfig struct {
	Path              string  `yaml:"path"`
	DiskBytes         string  `yaml:"disk_bytes"`          // default "1TiB": BlockCache sizing and soft write-turnover aging; not a quota
	MemRatio          float64 `yaml:"mem_ratio"`           // BlockCache = disk_bytes * mem_ratio
	DirectReads       *bool   `yaml:"direct_reads"`        // default true
	BloomBits         int     `yaml:"bloom_bits"`          // default 15
	BlockSize         string  `yaml:"block_size"`          // e.g. "64KiB"
	WriteBufferBytes  string  `yaml:"write_buffer_bytes"`  // default "256MiB"; default CF only, with four buffers
	MaxBackgroundJobs int     `yaml:"max_background_jobs"` // default 8
}

// RedisConfig configures a Redis-compatible backend reached over UDS or TCP.
// GET and SET use separate pools so asynchronous fill/repair traffic cannot
// consume the read-side connection budget.
type RedisConfig struct {
	Endpoint string `yaml:"endpoint"`
	GetPool  int    `yaml:"get_pool"`
	SetPool  int    `yaml:"set_pool"`
	Timeout  string `yaml:"timeout"`
}

// TierConfig describes one tier in a tiered-mode tier chain.
type TierConfig struct {
	Type        string `yaml:"type"`         // "embedded" | "redis" | "upstream" | "ec"
	MaxInflight int    `yaml:"max_inflight"` // synchronous lookup limit; 0 = unlimited

	// embedded
	Rocks *RocksConfig `yaml:"rocks"`

	// redis
	Redis *RedisConfig `yaml:"redis"`

	// upstream
	Endpoint string `yaml:"endpoint"`
	Pool     int    `yaml:"pool"`
	Timeout  string `yaml:"timeout"`

	// ec
	Cluster *ECClusterConfig `yaml:"cluster"`
}

// ECClusterConfig holds EC cluster parameters (embedded in TierConfig).
type ECClusterConfig struct {
	DataShards   int          `yaml:"data_shards"`
	ParityShards int          `yaml:"parity_shards"`
	Peers        []PeerConfig `yaml:"peers"`
	Pool         int          `yaml:"pool"`
	Timeout      string       `yaml:"timeout"`
}

// PeerConfig identifies a single shard peer.
type PeerConfig struct {
	ID       string `yaml:"id"`
	Endpoint string `yaml:"endpoint"`
}

// OriginConfig describes the L3 origin that cache-ctl falls through
// to on L1/L2 miss. The `type` discriminator selects between a
// store-ctl gRPC origin ("store") and a cache-ctl wire-client origin
// ("upstream") — the latter lets one cache-ctl instance point its
// origin at another cache-ctl endpoint for composite topologies.
type OriginConfig struct {
	// Type selects the origin flavour: "store" or "upstream".
	Type string `yaml:"type"`
	// Store holds the gRPC client parameters when Type == "store".
	Store *StoreClientConfig `yaml:"store"`
	// Upstream holds the cache-ctl wire client parameters when
	// Type == "upstream".
	Upstream *UpstreamClientConfig `yaml:"upstream"`
	// MaxInflight caps concurrent origin RPCs via the origin
	// adapter's semaphore (0 = unlimited).
	MaxInflight int `yaml:"max_inflight"`
}

// StoreClientConfig holds the gRPC client knobs for reaching a
// store-ctl daemon. Same shape as the manifest-ctl side — pool is
// the number of independent ClientConns for head-of-line relief.
type StoreClientConfig struct {
	Endpoint string `yaml:"endpoint"`
	Pool     int    `yaml:"pool"`
	Timeout  string `yaml:"timeout"`
}

// UpstreamClientConfig holds the cache-ctl wire-client knobs for
// reaching another cache-ctl endpoint as an origin. Same field shape
// as StoreClientConfig but they're distinct types so the wire-protocol
// client and gRPC store client can't be confused in Validate.
type UpstreamClientConfig struct {
	Endpoint string `yaml:"endpoint"`
	Pool     int    `yaml:"pool"`
	Timeout  string `yaml:"timeout"`
}

// LoadConfig reads and parses a YAML config file.
func LoadConfig(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("runtime: read config %s: %w", path, err)
	}
	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("runtime: parse config %s: %w", path, err)
	}
	return &cfg, nil
}

// Validate checks required fields based on mode.
func (c *Config) Validate() error {
	switch c.Mode {
	case "local", "shard":
		switch c.Type {
		case "embedded":
			if c.Rocks.Path == "" {
				return fmt.Errorf("runtime: rocks.path is required for mode %q type=embedded", c.Mode)
			}
			if c.Redis != nil {
				return fmt.Errorf("runtime: redis config is not valid for mode %q type=embedded", c.Mode)
			}
		case "redis":
			if err := validateRedisConfig(c.Redis, fmt.Sprintf("mode %q type=redis", c.Mode)); err != nil {
				return err
			}
			if c.Rocks != (RocksConfig{}) {
				return fmt.Errorf("runtime: rocks config is not valid for mode %q type=redis", c.Mode)
			}
		default:
			return fmt.Errorf("runtime: type must be \"embedded\" or \"redis\" for mode %q (got %q)", c.Mode, c.Type)
		}
	case "tiered":
		if len(c.Tiers) == 0 {
			return fmt.Errorf("runtime: at least one tier is required for tiered mode")
		}
		embeddedCount := 0
		for i, t := range c.Tiers {
			if t.MaxInflight < 0 {
				return fmt.Errorf("runtime: tiers[%d]: max_inflight must be >= 0", i)
			}
			switch t.Type {
			case "embedded":
				if t.Rocks == nil || t.Rocks.Path == "" {
					return fmt.Errorf("runtime: tiers[%d] (embedded): rocks.path is required", i)
				}
				if t.Redis != nil {
					return fmt.Errorf("runtime: tiers[%d] (embedded): redis config is not valid", i)
				}
				embeddedCount++
			case "redis":
				if err := validateRedisConfig(t.Redis, fmt.Sprintf("tiers[%d] (redis)", i)); err != nil {
					return err
				}
				if t.MaxInflight != 0 {
					return fmt.Errorf("runtime: tiers[%d] (redis): max_inflight is not valid; use redis.get_pool and redis.set_pool", i)
				}
				if t.Rocks != nil {
					return fmt.Errorf("runtime: tiers[%d] (redis): rocks config is not valid", i)
				}
			case "upstream":
				if t.Endpoint == "" {
					return fmt.Errorf("runtime: tiers[%d] (upstream): endpoint is required", i)
				}
			case "ec":
				if t.Cluster == nil || len(t.Cluster.Peers) == 0 {
					return fmt.Errorf("runtime: tiers[%d] (ec): cluster.peers is required", i)
				}
			default:
				return fmt.Errorf("runtime: tiers[%d]: unknown type %q", i, t.Type)
			}
		}
		// A cache-ctl process exposes exactly one local RocksDB instance,
		// so multiple embedded tiers have no physical meaning — they would
		// compete for the same mem/disk budget at the same latency class.
		if embeddedCount > 1 {
			return fmt.Errorf("runtime: at most one embedded tier is allowed, got %d", embeddedCount)
		}
		// Origin is required for tiered mode. Two flavours:
		//   - type: store — gRPC to a store-ctl daemon
		//   - type: upstream — wire-client to another cache-ctl
		if c.Origin == nil {
			return fmt.Errorf("runtime: origin is required for tiered mode")
		}
		switch c.Origin.Type {
		case "store":
			if c.Origin.Store == nil || c.Origin.Store.Endpoint == "" {
				return fmt.Errorf("runtime: origin.store.endpoint is required for type=store")
			}
		case "upstream":
			if c.Origin.Upstream == nil || c.Origin.Upstream.Endpoint == "" {
				return fmt.Errorf("runtime: origin.upstream.endpoint is required for type=upstream")
			}
		default:
			return fmt.Errorf("runtime: origin.type must be \"store\" or \"upstream\" (got %q)", c.Origin.Type)
		}
	default:
		return fmt.Errorf("runtime: unknown mode %q (expected local, shard, or tiered)", c.Mode)
	}

	if c.Listen == "" {
		return fmt.Errorf("runtime: listen address is required")
	}
	return nil
}

func validateRedisConfig(cfg *RedisConfig, scope string) error {
	if cfg == nil {
		return fmt.Errorf("runtime: %s: redis config is required", scope)
	}
	if _, _, err := ParseRedisEndpoint(cfg.Endpoint); err != nil {
		return fmt.Errorf("runtime: %s: %w", scope, err)
	}
	if cfg.GetPool < 0 {
		return fmt.Errorf("runtime: %s: redis.get_pool must be >= 0", scope)
	}
	if cfg.SetPool < 0 {
		return fmt.Errorf("runtime: %s: redis.set_pool must be >= 0", scope)
	}
	if cfg.Timeout != "" {
		d, err := time.ParseDuration(cfg.Timeout)
		if err != nil || d <= 0 {
			return fmt.Errorf("runtime: %s: redis.timeout must be a positive duration", scope)
		}
	}
	return nil
}

// ParseRedisEndpoint resolves the configured Redis endpoint into arguments for
// net.Dialer. Absolute paths and unix: forms select UDS; host:port selects TCP.
func ParseRedisEndpoint(endpoint string) (network, address string, err error) {
	if endpoint == "" {
		return "", "", errors.New("redis.endpoint is required")
	}
	if endpoint != strings.TrimSpace(endpoint) {
		return "", "", errors.New("redis.endpoint must not contain surrounding whitespace")
	}
	if path, ok := util.UnixAddr(endpoint); ok {
		if !filepath.IsAbs(path) {
			return "", "", errors.New("redis.endpoint Unix socket path must be absolute")
		}
		return "unix", path, nil
	}
	host, port, splitErr := net.SplitHostPort(endpoint)
	if splitErr != nil || host == "" || port == "" {
		return "", "", errors.New("redis.endpoint must be an absolute Unix socket path, unix:/path, or TCP host:port")
	}
	return "tcp", endpoint, nil
}

// ParseRPCTimeout parses the rpc_timeout field. Empty/absent/invalid =
// 0 = no per-request deadline: a request is bounded only by the client
// connection / caller cancellation, never an arbitrary number.
// Operators opt into a finite budget by setting rpc_timeout explicitly.
func (c *Config) ParseRPCTimeout() time.Duration {
	d, err := time.ParseDuration(c.RPCTimeout)
	if err != nil || d <= 0 {
		return 0
	}
	return d
}

// defaultStatsInterval is the base period for the adaptive stats line when
// stats_interval is unset.
const defaultStatsInterval = 30 * time.Second

// StatsIntervalDur parses stats_interval. Empty/absent → 30s (on by default);
// "0"/"off"/"none" → 0 (disabled); a valid Go duration overrides; an invalid
// value falls back to the default rather than silently disabling output.
func (c *Config) StatsIntervalDur() time.Duration {
	switch strings.ToLower(strings.TrimSpace(c.StatsInterval)) {
	case "":
		return defaultStatsInterval
	case "0", "off", "none", "disabled":
		return 0
	}
	d, err := time.ParseDuration(c.StatsInterval)
	if err != nil || d <= 0 {
		return defaultStatsInterval
	}
	return d
}

// BoolDefault returns the value of a *bool field, defaulting to def if nil.
func BoolDefault(b *bool, def bool) bool {
	if b == nil {
		return def
	}
	return *b
}
