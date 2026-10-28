// cache-ctl is the CLI for the cache daemon and client operations.
//
// Subcommands:
//
//	serve       Start cache daemon (local/shard/tiered mode via YAML config)
//	object get  Read a complete object from cache
//	object put  Write a complete object to cache
//	shard get   Read an EC shard from cache
//	shard put   Write an EC shard to cache
//	ping        Health probe via gRPC health protocol
//	info        Inspect daemon and backend state
package main

import (
	"context"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	appcache "github.com/kuasar-sandbox/accelerator/app/cache"
	"github.com/kuasar-sandbox/accelerator/pkg/activation"
	"github.com/kuasar-sandbox/accelerator/pkg/cache"
	"github.com/kuasar-sandbox/accelerator/pkg/cache/client"
	"github.com/kuasar-sandbox/accelerator/pkg/cache/runtime"
	"github.com/kuasar-sandbox/accelerator/pkg/store"

	healthgrpc "google.golang.org/grpc/health/grpc_health_v1"
)

func main() {
	if len(os.Args) < 2 {
		printUsage()
		os.Exit(1)
	}
	switch os.Args[1] {
	case "serve":
		cmdServe(os.Args[2:])
	case "object":
		if len(os.Args) < 3 {
			fmt.Fprintln(os.Stderr, "usage: cache-ctl object {get|put}")
			os.Exit(1)
		}
		switch os.Args[2] {
		case "get":
			cmdObjectGet(os.Args[3:])
		case "put":
			cmdObjectPut(os.Args[3:])
		default:
			fmt.Fprintf(os.Stderr, "unknown object subcommand: %s\n", os.Args[2])
			os.Exit(1)
		}
	case "shard":
		if len(os.Args) < 3 {
			fmt.Fprintln(os.Stderr, "usage: cache-ctl shard {get|put}")
			os.Exit(1)
		}
		switch os.Args[2] {
		case "get":
			cmdShardGet(os.Args[3:])
		case "put":
			cmdShardPut(os.Args[3:])
		default:
			fmt.Fprintf(os.Stderr, "unknown shard subcommand: %s\n", os.Args[2])
			os.Exit(1)
		}
	case "ping":
		cmdPing(os.Args[2:])
	case "info":
		cmdInfo(os.Args[2:])
	case "bench":
		cmdBench(os.Args[2:])
	case "config":
		cmdConfig(os.Args[2:])
	default:
		fmt.Fprintf(os.Stderr, "unknown command: %s\n", os.Args[1])
		printUsage()
		os.Exit(1)
	}
}

func printUsage() {
	fmt.Fprintln(os.Stderr, "usage: cache-ctl <command> [args]")
	fmt.Fprintln(os.Stderr, "commands:")
	fmt.Fprintln(os.Stderr, "  serve       Start cache daemon")
	fmt.Fprintln(os.Stderr, "  object get  Read a complete object")
	fmt.Fprintln(os.Stderr, "  object put  Write a complete object")
	fmt.Fprintln(os.Stderr, "  shard get   Read an EC shard")
	fmt.Fprintln(os.Stderr, "  shard put   Write an EC shard")
	fmt.Fprintln(os.Stderr, "  ping        Health probe")
	fmt.Fprintln(os.Stderr, "  info        Inspect daemon and backend state")
	fmt.Fprintln(os.Stderr, "  bench       Run performance benchmark")
	fmt.Fprintln(os.Stderr, "  config      Inspect or generate the cache-ctl config")
	fmt.Fprintln(os.Stderr, "")
	fmt.Fprintln(os.Stderr, "Environment:")
	fmt.Fprintln(os.Stderr, "  CACHE_CONFIG    fallback for serve --config")
	fmt.Fprintln(os.Stderr, "  CACHE_ENDPOINT  fallback for object/shard/bench --endpoint")
}

// ── serve ──

// cacheConfigEnv overrides --config for cache-ctl serve when both are absent.
const cacheConfigEnv = "CACHE_CONFIG"

// cacheEndpointEnv supplies --endpoint for client commands (data port:
// object/shard/bench) when the flag is absent. ping/info/info --rocks-path
// are deliberately not affected: they speak the health endpoint or no
// network at all.
const cacheEndpointEnv = "CACHE_ENDPOINT"

// resolveDataEndpoint returns the cache data endpoint chosen from, in
// priority order, the --endpoint flag value then $CACHE_ENDPOINT.
// Empty result -> caller should fatal.
func resolveDataEndpoint(flagValue string) string {
	if flagValue != "" {
		return flagValue
	}
	return os.Getenv(cacheEndpointEnv)
}

func cmdServe(args []string) {
	fs := flag.NewFlagSet("serve", flag.ExitOnError)
	configPath := fs.String("config", "", "YAML config file (overrides CACHE_CONFIG env)")
	drainTimeout := fs.Duration("drain-timeout", 0, "graceful shutdown budget (0 = unlimited)")
	fs.Parse(args)
	if *drainTimeout < 0 {
		fatal("drain timeout must not be negative")
	}

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
	if err := cfg.Validate(); err != nil {
		fatal("%v", err)
	}

	files, err := activation.FilesFromEnv()
	if err != nil {
		fatal("activation: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	termCh := make(chan os.Signal, 2)
	hupCh := make(chan os.Signal, 1)
	signal.Notify(termCh, syscall.SIGINT, syscall.SIGTERM)
	signal.Notify(hupCh, syscall.SIGHUP)

	drain := make(chan struct{})
	opts := appcache.Options{Drain: drain, DrainTimeout: *drainTimeout, ListenerFDs: files}
	hasEC := false
	for _, tier := range cfg.Tiers {
		if tier.Type == "ec" {
			hasEC = true
			break
		}
	}
	var reload chan struct{}
	if hasEC {
		reload = make(chan struct{}, 1)
		opts.Reload = reload
		opts.LoadMembership = func(ctx context.Context) ([]appcache.MembershipUpdate, error) {
			return loadMembershipFile(ctx, resolved, cfg)
		}
	}

	done := make(chan struct{})
	var forward sync.WaitGroup
	forward.Add(1)
	go func() {
		defer forward.Done()
		draining, forced := false, false
		for {
			select {
			case sig := <-termCh:
				if !draining {
					draining = true
					fmt.Fprintf(os.Stderr, "cache-ctl: received %v, draining...\n", sig)
					close(drain)
				} else if !forced {
					forced = true
					fmt.Fprintf(os.Stderr, "cache-ctl: received %v, cancelling...\n", sig)
					cancel()
				}
			case <-hupCh:
				if draining {
					continue
				}
				if reload == nil {
					log.Printf("SIGHUP: no EC tiers; nothing to reload")
					continue
				}
				select {
				case reload <- struct{}{}:
				default:
				}
			case <-done:
				return
			}
		}
	}()

	err = appcache.Run(ctx, *cfg, opts)
	for _, file := range files {
		err = errors.Join(err, file.Close())
	}
	close(done)
	signal.Stop(termCh)
	signal.Stop(hupCh)
	cancel()
	forward.Wait()
	if err != nil {
		fatal("%v", err)
	}
}

func loadMembershipFile(ctx context.Context, path string, startup *runtime.Config) ([]appcache.MembershipUpdate, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	cfg, err := runtime.LoadConfig(path)
	if err != nil {
		return nil, err
	}
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	if len(cfg.Tiers) != len(startup.Tiers) {
		return nil, fmt.Errorf("tier count changed (was %d, now %d)", len(startup.Tiers), len(cfg.Tiers))
	}
	var updates []appcache.MembershipUpdate
	for i, oldTier := range startup.Tiers {
		if oldTier.Type != "ec" {
			continue
		}
		newTier := cfg.Tiers[i]
		if newTier.Type != "ec" || newTier.Cluster == nil {
			return nil, fmt.Errorf("tier[%d] was ec, now %q", i, newTier.Type)
		}
		updates = append(updates, appcache.MembershipUpdate{
			TierIndex: i,
			Peers:     append([]runtime.PeerConfig(nil), newTier.Cluster.Peers...),
		})
	}
	return updates, nil
}

// ── object get ──

func cmdObjectGet(args []string) {
	fs := flag.NewFlagSet("object get", flag.ExitOnError)
	endpoint := fs.String("endpoint", "", "cache-ctl data endpoint (overrides CACHE_ENDPOINT env)")
	namespace := fs.String("namespace", "chunk", "namespace (chunk|manifest)")
	hashHex := fs.String("hash", "", "32-byte hash (hex, required)")
	fs.Parse(args)

	if *hashHex == "" {
		fatal("--hash is required")
	}
	hash, err := hex.DecodeString(*hashHex)
	if err != nil || len(hash) != 32 {
		fatal("--hash must be 64 hex chars (32 bytes)")
	}

	resolved := resolveDataEndpoint(*endpoint)
	if resolved == "" {
		fatal("--endpoint or CACHE_ENDPOINT required")
	}

	c, err := client.NewGetter(resolved, client.Options{Pool: 1, Timeout: 5 * time.Second})
	if err != nil {
		fatal("dial: %v", err)
	}
	defer c.Close()

	var key store.ContentKey
	copy(key[:], hash)
	result, blob, err := c.Get(context.Background(), store.Partition(*namespace), key)
	if err != nil {
		fatal("get: %v", err)
	}
	if result != cache.CacheHit {
		fmt.Fprintln(os.Stderr, "MISS")
		os.Exit(1)
	}
	defer blob.Release()
	os.Stdout.Write(blob.Bytes())
}

// ── object put ──

func cmdObjectPut(args []string) {
	fs := flag.NewFlagSet("object put", flag.ExitOnError)
	endpoint := fs.String("endpoint", "", "cache-ctl data endpoint (overrides CACHE_ENDPOINT env)")
	namespace := fs.String("namespace", "chunk", "namespace")
	hashHex := fs.String("hash", "", "32-byte hash (hex, required)")
	valuePath := fs.String("value", "-", "value file (- for stdin)")
	fs.Parse(args)

	if *hashHex == "" {
		fatal("--hash is required")
	}
	hash, err := hex.DecodeString(*hashHex)
	if err != nil || len(hash) != 32 {
		fatal("--hash must be 64 hex chars")
	}

	var r io.Reader = os.Stdin
	if *valuePath != "-" {
		f, err := os.Open(*valuePath)
		if err != nil {
			fatal("open value: %v", err)
		}
		defer f.Close()
		r = f
	}
	value, err := io.ReadAll(r)
	if err != nil {
		fatal("read value: %v", err)
	}

	resolved := resolveDataEndpoint(*endpoint)
	if resolved == "" {
		fatal("--endpoint or CACHE_ENDPOINT required")
	}

	c, err := client.New(resolved, client.Options{Pool: 1, Timeout: 5 * time.Second})
	if err != nil {
		fatal("dial: %v", err)
	}
	defer c.Close()

	var key store.ContentKey
	copy(key[:], hash)
	if err := c.Fill(context.Background(), store.Partition(*namespace), key, value); err != nil {
		fatal("put: %v", err)
	}
	fmt.Fprintln(os.Stderr, "OK")
}

// ── shard get ──

func cmdShardGet(args []string) {
	fs := flag.NewFlagSet("shard get", flag.ExitOnError)
	endpoint := fs.String("endpoint", "", "cache-ctl data endpoint (overrides CACHE_ENDPOINT env)")
	namespace := fs.String("namespace", "chunk", "namespace")
	hashHex := fs.String("hash", "", "32-byte hash (hex, required)")
	fs.Parse(args)

	if *hashHex == "" {
		fatal("--hash is required")
	}
	hash, err := hex.DecodeString(*hashHex)
	if err != nil || len(hash) != 32 {
		fatal("--hash must be 64 hex chars")
	}

	resolved := resolveDataEndpoint(*endpoint)
	if resolved == "" {
		fatal("--endpoint or CACHE_ENDPOINT required")
	}

	c, err := client.NewShard(resolved, client.Options{Pool: 1, Timeout: 5 * time.Second})
	if err != nil {
		fatal("dial: %v", err)
	}
	defer c.Close()

	var key store.ContentKey
	copy(key[:], hash)
	result, blob, err := c.GetShard(context.Background(), store.Partition(*namespace), key)
	if err != nil {
		fatal("get shard: %v", err)
	}
	if result != cache.CacheHit {
		fmt.Fprintln(os.Stderr, "MISS")
		os.Exit(1)
	}
	defer blob.Release()
	// The peer returns [idx][total][data]. Report idx/total on stderr
	// for debugging; raw shard bytes go to stdout.
	idx, total, data, ok := cache.ParseShardPrefix(blob.Bytes())
	if !ok {
		fatal("shard value too short (%d bytes, need >= %d)", len(blob.Bytes()), cache.ShardPrefixSize)
	}
	fmt.Fprintf(os.Stderr, "HIT idx=%d total=%d size=%d\n", idx, total, len(data))
	os.Stdout.Write(data)
}

// ── shard put ──

func cmdShardPut(args []string) {
	fs := flag.NewFlagSet("shard put", flag.ExitOnError)
	endpoint := fs.String("endpoint", "", "cache-ctl data endpoint (overrides CACHE_ENDPOINT env)")
	namespace := fs.String("namespace", "chunk", "namespace")
	hashHex := fs.String("hash", "", "32-byte hash (hex, required)")
	idx := fs.Uint("idx", 0, "shard index (0..total-1)")
	total := fs.Uint("total", 5, "total shard count (K+P)")
	valuePath := fs.String("value", "-", "shard data file (- for stdin)")
	fs.Parse(args)

	if *hashHex == "" {
		fatal("--hash is required")
	}
	hash, err := hex.DecodeString(*hashHex)
	if err != nil || len(hash) != 32 {
		fatal("--hash must be 64 hex chars")
	}
	if *idx >= *total {
		fatal("--idx must be < --total")
	}

	var r io.Reader = os.Stdin
	if *valuePath != "-" {
		f, err := os.Open(*valuePath)
		if err != nil {
			fatal("open value: %v", err)
		}
		defer f.Close()
		r = f
	}
	data, err := io.ReadAll(r)
	if err != nil {
		fatal("read value: %v", err)
	}
	// Wrap bare data with [idx][total] prefix to match what EC peers
	// expect. This mirrors what the real tier.Fill path does via
	// EncodePrefixed; the CLI is just the single-peer debug surface.
	value := make([]byte, cache.ShardPrefixSize+len(data))
	cache.EncodeShardPrefix(value, byte(*idx), byte(*total))
	copy(value[cache.ShardPrefixSize:], data)

	resolved := resolveDataEndpoint(*endpoint)
	if resolved == "" {
		fatal("--endpoint or CACHE_ENDPOINT required")
	}

	c, err := client.NewShard(resolved, client.Options{Pool: 1, Timeout: 5 * time.Second})
	if err != nil {
		fatal("dial: %v", err)
	}
	defer c.Close()

	var key store.ContentKey
	copy(key[:], hash)
	if err := c.FillShard(context.Background(), store.Partition(*namespace), key, value); err != nil {
		fatal("put shard: %v", err)
	}
	fmt.Fprintln(os.Stderr, "OK")
}

// ── ping ──

func cmdPing(args []string) {
	fs := flag.NewFlagSet("ping", flag.ExitOnError)
	endpoint := fs.String("endpoint", "127.0.0.1:7071", "cache-ctl health endpoint")
	fs.Parse(args)

	p, err := client.DialPool(*endpoint, 1)
	if err != nil {
		fatal("dial: %v", err)
	}
	defer p.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	hc := healthgrpc.NewHealthClient(p.Next())
	resp, err := hc.Check(ctx, &healthgrpc.HealthCheckRequest{})
	if err != nil {
		fatal("health check: %v", err)
	}
	fmt.Println(resp.Status)
}

// ── info ── (see info.go for cmdInfo; supports remote gRPC + offline rocks)

// ── helpers ──

func fatal(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "cache-ctl: "+format+"\n", args...)
	os.Exit(1)
}
