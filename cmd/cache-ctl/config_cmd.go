package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/kuasar-sandbox/accelerator/pkg/cache/runtime"
	"gopkg.in/yaml.v3"
)

// cmdConfig dispatches `cache-ctl config <subcommand>`.
//
//	show      Print the resolved YAML at the configured path.
//	generate  Print a commented daemon-config template to stdout.
func cmdConfig(args []string) {
	if len(args) == 0 {
		fmt.Fprintln(os.Stderr, "Usage: cache-ctl config <show|generate> [flags]")
		os.Exit(1)
	}
	switch args[0] {
	case "show":
		cmdConfigShow(args[1:])
	case "generate":
		cmdConfigGenerate(args[1:])
	default:
		fmt.Fprintf(os.Stderr, "unknown config subcommand %q\n", args[0])
		os.Exit(1)
	}
}

func cmdConfigShow(args []string) {
	fs := flag.NewFlagSet("config show", flag.ExitOnError)
	configPath := fs.String("config", "", "YAML config file (overrides CACHE_CONFIG env)")
	fs.Parse(args)

	resolved := *configPath
	if resolved == "" {
		resolved = os.Getenv(cacheConfigEnv)
	}
	if resolved == "" {
		fatal("--config or %s required", cacheConfigEnv)
	}
	cfg, err := runtime.LoadConfig(resolved)
	if err != nil {
		fatal("%v", err)
	}
	out, err := yaml.Marshal(cfg)
	if err != nil {
		fatal("marshal config: %v", err)
	}
	os.Stdout.Write(out)
}

func cmdConfigGenerate(args []string) {
	fs := flag.NewFlagSet("config generate", flag.ExitOnError)
	fs.Parse(args)
	fmt.Print(cacheConfigTemplate)
}

// cacheConfigTemplate is a commented YAML for `cache-ctl config generate`.
// Picks the tiered mode (rocksdb L1 + store origin) as the most common
// production shape; users in other shapes can prune or adapt.
const cacheConfigTemplate = `# cache-ctl daemon configuration.
# Reference this file via --config or the CACHE_CONFIG environment
# variable. There is no auto-discovery; unset = error.

# Mode: local | shard | tiered.
mode: tiered

# Wire data plane. "host:port" for TCP, or a Unix socket path
# ("/run/sandbox/cache.sock" or "unix:///run/sandbox/cache.sock"); clients set
# cache.endpoint to the same.
listen: 127.0.0.1:7070

# Health + Info gRPC. "host:port" or a Unix socket path (same forms as listen);
# ping / info --endpoint then dial the unix:/// form.
health_listen: 127.0.0.1:7071

# Server request-context timeout; context-aware backends can cancel on expiry.
# This does not interrupt synchronous RocksDB CGO in the middle of a call.
rpc_timeout: 5s

# Periodic adaptive stats line to stderr: each period with traffic prints one
# summary (rates, bandwidth, latency p50/p99/max, inflight/conns, hit cascade,
# rocksdb/redis gauges); idle periods are silent. Default 30s; "0"/"off" disables.
# stats_interval: 30s

# Embedded aging follows disk_bytes with the default frequency settings.

tiers:
  - type: embedded   # rocksdb-backed local L1
    rocks:
      path: /var/lib/cache-ctl/rocks
      disk_bytes: 16GiB
      mem_ratio: 0.1
      direct_reads: true
      bloom_bits: 10

  # Redis-compatible UDS or TCP alternative (replace the embedded tier above):
  # - type: redis
  #   redis:
  #     endpoint: unix:///run/kuasar-cache/redis.sock
  #     get_pool: 32
  #     set_pool: 8
  #     timeout: 2s

origin:
  type: store        # treat store-ctl as the cold tier
  store:
    endpoint: 127.0.0.1:7100
    pool: 4
    timeout: 5s
  max_inflight: 32
`
