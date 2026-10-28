package cache

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/http/pprof"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/kuasar-sandbox/accelerator/internal/appshutdown"
	"github.com/kuasar-sandbox/accelerator/internal/util"
	"github.com/kuasar-sandbox/accelerator/internal/util/obstat"
	"github.com/kuasar-sandbox/accelerator/pkg/activation"
	pkgcache "github.com/kuasar-sandbox/accelerator/pkg/cache"
	"github.com/kuasar-sandbox/accelerator/pkg/cache/client"
	"github.com/kuasar-sandbox/accelerator/pkg/cache/ec"
	cachepb "github.com/kuasar-sandbox/accelerator/pkg/cache/pb"
	"github.com/kuasar-sandbox/accelerator/pkg/cache/redisstore"
	"github.com/kuasar-sandbox/accelerator/pkg/cache/rocks"
	"github.com/kuasar-sandbox/accelerator/pkg/cache/runtime"
	"github.com/kuasar-sandbox/accelerator/pkg/cache/server"
	"github.com/kuasar-sandbox/accelerator/pkg/store"
	storeclient "github.com/kuasar-sandbox/accelerator/pkg/store/client"
	"google.golang.org/grpc"
	healthgrpc "google.golang.org/grpc/health/grpc_health_v1"
)

type Config = runtime.Config
type PeerConfig = runtime.PeerConfig

type MembershipUpdate struct {
	TierIndex int
	Peers     []PeerConfig
}

type Options struct {
	// Closing Drain starts irreversible soft shutdown; nil preserves ctx-only use.
	Drain <-chan struct{}
	// DrainTimeout bounds graceful waiting, never the lifetime of resource users.
	// Zero is unlimited; negative values are rejected before acquiring resources.
	DrainTimeout time.Duration
	// ListenerFDs binds fd=<name> entrances to borrowed original Files. Run owns
	// listener duplicates; the caller closes original Files after Run returns.
	ListenerFDs    map[string]*os.File
	Logger         *slog.Logger
	Reload         <-chan struct{}
	LoadMembership func(context.Context) ([]MembershipUpdate, error)
}

type directStore interface {
	pkgcache.Tier
	pkgcache.ShardTier
	Close() error
}

func Run(ctx context.Context, cfg Config, opts Options) (retErr error) {
	if opts.DrainTimeout < 0 {
		return errors.New("cache: drain timeout must not be negative")
	}
	if ctx == nil {
		return errors.New("cache: nil context")
	}
	if err := ctx.Err(); err != nil {
		if err == context.Canceled {
			return nil
		}
		return err
	}
	cfg, err := prepareConfig(cfg, opts)
	if err != nil {
		return err
	}
	life := appshutdown.New(ctx, opts.Drain, opts.DrainTimeout)
	defer func() { retErr = errors.Join(retErr, life.Err()); life.Close() }()
	ctx = life.Context
	inherited, err := activation.Import(opts.ListenerFDs, cfg.Listen, cfg.HealthListen, cfg.PprofListen)
	if err != nil {
		return err
	}
	defer func() {
		for _, listener := range inherited {
			_ = listener.Close()
		}
	}()
	if !life.Active(nil) {
		return nil
	}
	log := opts.Logger
	if log == nil {
		log = slog.New(slog.NewTextHandler(os.Stderr, nil))
	}

	blobPool := pkgcache.NewPool(512 << 10)
	var (
		direct       directStore
		rocksStore   rocks.Interface
		redisStore   *redisstore.Store
		tierBackend  pkgcache.Tier
		shardBackend pkgcache.ShardTier
		primaryRocks rocks.Interface
		primaryRedis *redisstore.Store
		tieredComps  *tieredComponents
		tieredCache  *pkgcache.TieredCache
		originSpec   *OriginSpec
		owned        []func() error
	)
	closeOwned := func() error {
		life.Close()
		var errs []error
		for i := len(owned) - 1; i >= 0; i-- {
			if e := owned[i](); e != nil {
				errs = append(errs, e)
			}
		}
		return errors.Join(errs...)
	}

	if cfg.Mode == "local" || cfg.Mode == "shard" {
		switch cfg.Type {
		case "embedded":
			rocksStore, err = rocks.Open(cfg.Rocks, cfg.Freq)
			if err != nil {
				return startupError(ctx, fmt.Errorf("cache: open rocks: %w", err))
			}
			direct = rocksStore
		case "redis":
			redisStore, err = redisstore.Open(*cfg.Redis, blobPool)
			if err != nil {
				return startupError(ctx, fmt.Errorf("cache: open redis: %w", err))
			}
			direct = redisStore
		}
		owned = append(owned, direct.Close)
		tierBackend, shardBackend = direct, direct
		primaryRocks, primaryRedis = rocksStore, redisStore
	} else {
		tieredComps, err = buildTieredChain(&cfg, blobPool)
		if err != nil {
			return startupError(ctx, err)
		}
		owned = append(owned, tieredComps.Close)
		var origin pkgcache.Getter
		maxInflight := cfg.Origin.MaxInflight
		if maxInflight < 0 {
			maxInflight = 0
		}
		switch cfg.Origin.Type {
		case "store":
			sc := cfg.Origin.Store
			pool := sc.Pool
			if pool <= 0 {
				pool = 4
			}
			var timeout time.Duration
			if sc.Timeout != "" {
				if d, e := time.ParseDuration(sc.Timeout); e == nil && d > 0 {
					timeout = d
				}
			}
			originStore, e := storeclient.New(sc.Endpoint, pool, timeout)
			if e != nil {
				return startupError(ctx, errors.Join(fmt.Errorf("cache: create origin client: %w", e), closeOwned()))
			}
			owned = append(owned, originStore.Close)
			origin = pkgcache.NewOriginAdapter(pkgcache.NewStoreOrigin(originStore), maxInflight)
			originSpec = &OriginSpec{Type: "store", Endpoint: sc.Endpoint}
		case "upstream":
			uc := cfg.Origin.Upstream
			pool := uc.Pool
			if pool <= 0 {
				pool = 4
			}
			var timeout time.Duration
			if uc.Timeout != "" {
				if d, e := time.ParseDuration(uc.Timeout); e == nil && d > 0 {
					timeout = d
				}
			}
			originClient, e := client.NewGetter(uc.Endpoint, client.Options{Pool: pool, Timeout: timeout, BlobPool: blobPool})
			if e != nil {
				return startupError(ctx, errors.Join(fmt.Errorf("cache: create upstream origin client: %w", e), closeOwned()))
			}
			owned = append(owned, originClient.Close)
			origin = pkgcache.NewOriginAdapter(originClient, maxInflight)
			originSpec = &OriginSpec{Type: "upstream", Endpoint: uc.Endpoint}
		}
		tieredCache = pkgcache.NewTieredCache(origin, tieredComps.Tiers...)
		tierBackend = &tieredBackend{tc: tieredCache}
	}
	if !life.Active(nil) {
		err = ctx.Err()
		return startupError(ctx, errors.Join(err, closeOwned()))
	}

	handler := server.NewCacheHandler(tierBackend, shardBackend)
	ws := server.NewWireServer(handler, 120*time.Second, cfg.ParseRPCTimeout())
	dataLis, err := util.ListenActivated(cfg.Listen, inherited)
	if err != nil {
		return startupError(ctx, errors.Join(fmt.Errorf("cache: listen %s: %w", cfg.Listen, err), closeOwned()))
	}
	var healthLis net.Listener
	if cfg.HealthListen != "" {
		healthLis, err = util.ListenActivated(cfg.HealthListen, inherited)
		if err != nil {
			return startupError(ctx, errors.Join(fmt.Errorf("cache: listen health %s: %w", cfg.HealthListen, err), dataLis.Close(), closeOwned()))
		}
	}
	var pprofLis net.Listener
	if cfg.PprofListen != "" {
		if strings.HasPrefix(cfg.PprofListen, "fd=") {
			// Import above already rejected any explicit FD error before resources.
			pprofLis = inherited[strings.TrimPrefix(cfg.PprofListen, "fd=")]
		} else {
			pprofLis, err = net.Listen("tcp", cfg.PprofListen)
		}
		if err != nil {
			log.Warn("cache pprof disabled", "listen", cfg.PprofListen, "error", err)
		}
	}
	closeListeners := func() error {
		var errs []error
		if dataLis != nil {
			errs = append(errs, dataLis.Close())
		}
		if healthLis != nil {
			errs = append(errs, healthLis.Close())
		}
		if pprofLis != nil {
			errs = append(errs, pprofLis.Close())
		}
		return errors.Join(errs...)
	}
	if !life.Active(nil) {
		err = ctx.Err()
		return startupError(ctx, errors.Join(err, closeListeners(), closeOwned()))
	}

	var grpcServer *grpc.Server
	var healthSrv interface {
		Shutdown()
		SetServingStatus(string, healthgrpc.HealthCheckResponse_ServingStatus)
	}
	if healthLis != nil {
		grpcServer = grpc.NewServer(grpc.WaitForHandlers(true))
		hs := server.RegisterHealth(grpcServer)
		healthSrv = hs
		infoSrv := NewInfoServer(cfg.Mode, handler)
		if tieredCache != nil {
			infoSrv.SetTiered(tieredCache, tieredComps.TierSpecs, originSpec)
		}
		if primaryRocks != nil {
			infoSrv.SetTopLevelRocks(primaryRocks)
		}
		if primaryRedis != nil {
			infoSrv.SetTopLevelRedis(primaryRedis)
		}
		cachepb.RegisterInfoServer(grpcServer, infoSrv)
		if len(collectRedisStores(primaryRedis, tieredComps)) > 0 {
			healthSrv.SetServingStatus("", healthgrpc.HealthCheckResponse_NOT_SERVING)
		}
	}
	var httpHandlers sync.WaitGroup
	var pprofServer *http.Server
	if pprofLis != nil {
		mux := http.NewServeMux()
		mux.HandleFunc("/debug/pprof/", pprof.Index)
		mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
		mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
		mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
		mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
		pprofServer = &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if !life.Active(func() { httpHandlers.Add(1) }) {
				http.Error(w, "server is stopping", http.StatusServiceUnavailable)
				return
			}
			defer httpHandlers.Done()
			mux.ServeHTTP(w, r)
		})}
	}

	runCtx, cancel := context.WithCancel(ctx)
	var observers sync.WaitGroup
	type serveResult struct {
		name string
		err  error
	}
	results := make(chan serveResult, 2)
	required := 1
	if grpcServer != nil {
		required++
	}
	var pprofDone chan error
	if pprofServer != nil {
		pprofDone = make(chan error, 1)
	}
	if !life.Active(func() {
		redisStores := collectRedisStores(primaryRedis, tieredComps)
		if healthSrv != nil && len(redisStores) > 0 {
			observers.Add(1)
			go func() {
				defer observers.Done()
				monitorRedisHealth(runCtx, healthSrv, redisStores)
			}()
		}
		var statsRocks rocks.Interface = primaryRocks
		if tieredComps != nil {
			statsRocks = tieredComps.EmbeddedStore
		}
		observers.Add(1)
		go func() {
			defer observers.Done()
			obstat.RunAdaptive(runCtx, cfg.StatsIntervalDur(), cacheSampler(cfg.Mode, ws, tieredCache, statsRocks, collectRedisGaugeSources(primaryRedis, tieredComps)), func(f string, a ...any) {
				log.Info(fmt.Sprintf(f, a...))
			})
		}()
		if opts.Reload != nil {
			observers.Add(1)
			go func() {
				defer observers.Done()
				runMembership(runCtx, opts.Reload, opts.LoadMembership, tieredComps, log)
			}()
		}
		go func() { results <- serveResult{"wire", ws.Serve(dataLis)} }()
		if grpcServer != nil {
			go func() { results <- serveResult{"grpc", grpcServer.Serve(healthLis)} }()
		}
		if pprofServer != nil {
			go func() { pprofDone <- pprofServer.Serve(pprofLis) }()
		}
		log.Info("cache started", "mode", cfg.Mode, "listen", cfg.Listen, "health", cfg.HealthListen, "pprof", cfg.PprofListen)

	}) {
		cancel()
		return startupError(ctx, errors.Join(ctx.Err(), closeListeners(), closeOwned()))
	}

	var out error
	done := 0
	force := false
	select {
	case <-life.Stopping():
		force = ctx.Err() != nil
	case r := <-results:
		done++
		out = serveError(r.name, r.err)
		life.Cancel()
		force = true
	}
	cancel()
	if healthSrv != nil {
		// The existing gRPC health server fences later SetServingStatus calls
		// after Shutdown, including an already-running Redis probe's success.
		healthSrv.Shutdown()
	}
	ws.Drain()
	pprofCtx, cancelPprof := context.WithCancel(ctx)
	defer cancelPprof()
	frontDone := make(chan struct{})
	var stops sync.WaitGroup
	stops.Add(1)
	go func() { defer stops.Done(); ws.Wait() }()
	if grpcServer != nil {
		stops.Add(1)
		// Management Watch streams may be indefinite. Stop their transports and
		// join actual Info handlers before any backend is closed.
		go func() { defer stops.Done(); grpcServer.Stop() }()
	}
	if pprofServer != nil {
		stops.Add(1)
		go func() { defer stops.Done(); _ = pprofServer.Shutdown(pprofCtx) }()
	}
	go func() {
		stops.Wait()
		// Shutdown can return on budget expiry before HTTP handlers finish.
		// Keep cancellation escalation live until those actual users join too.
		httpHandlers.Wait()
		close(frontDone)
	}()
	var forced sync.WaitGroup
	forceStop := func() {
		if tieredCache != nil {
			tieredCache.Stop()
		}
		cancelPprof()
		if pprofServer != nil {
			_ = pprofServer.Close()
		}
		forced.Add(1)
		go func() { defer forced.Done(); ws.Stop() }()
	}
	if force {
		forceStop()
	} else {
		select {
		case <-frontDone:
		case <-ctx.Done():
			forceStop()
		}
	}
	<-frontDone
	forced.Wait()
	// Foreground handlers can no longer enqueue fills or repairs. Cancel and
	// join their background users before closing the shared dependencies.
	if tieredCache != nil {
		tieredCache.Stop()
	}
	for done < required {
		r := <-results
		done++
		if !expectedStop(r.err) {
			out = errors.Join(out, serveError(r.name, r.err))
		}
	}
	if pprofDone != nil {
		if e := <-pprofDone; e != nil && !errors.Is(e, http.ErrServerClosed) && !errors.Is(e, net.ErrClosed) {
			log.Warn("cache pprof stopped", "error", e)
		}
	}
	observers.Wait()
	if tieredCache != nil {
		tieredCache.Wait()
	}
	out = errors.Join(out, closeOwned())
	log.Info("cache stopped", "error", out)
	return out
}

func prepareConfig(cfg Config, opts Options) (Config, error) {
	if opts.DrainTimeout < 0 {
		return Config{}, errors.New("drain timeout must not be negative")
	}
	cfg = cloneConfig(cfg)
	if (opts.Reload == nil) != (opts.LoadMembership == nil) {
		return Config{}, errors.New("cache: Reload and LoadMembership must be configured together")
	}
	if err := cfg.Validate(); err != nil {
		return Config{}, err
	}
	if opts.Reload != nil {
		hasEC := false
		for _, t := range cfg.Tiers {
			if t.Type == "ec" {
				hasEC = true
				break
			}
		}
		if !hasEC {
			return Config{}, errors.New("cache: membership reload requires an existing EC tier")
		}
	}
	return cfg, nil
}

func cloneConfig(in Config) Config {
	out := in
	if in.Redis != nil {
		v := *in.Redis
		out.Redis = &v
	}
	out.Tiers = make([]runtime.TierConfig, len(in.Tiers))
	for i, t := range in.Tiers {
		out.Tiers[i] = t
		if t.Rocks != nil {
			v := *t.Rocks
			out.Tiers[i].Rocks = &v
		}
		if t.Redis != nil {
			v := *t.Redis
			out.Tiers[i].Redis = &v
		}
		if t.Cluster != nil {
			v := *t.Cluster
			v.Peers = append([]runtime.PeerConfig(nil), t.Cluster.Peers...)
			out.Tiers[i].Cluster = &v
		}
	}
	if in.Origin != nil {
		v := *in.Origin
		if in.Origin.Store != nil {
			x := *in.Origin.Store
			v.Store = &x
		}
		if in.Origin.Upstream != nil {
			x := *in.Origin.Upstream
			v.Upstream = &x
		}
		out.Origin = &v
	}
	return out
}

func serveError(name string, err error) error {
	if err == nil {
		return fmt.Errorf("cache: %s serve stopped unexpectedly", name)
	}
	return fmt.Errorf("cache: %s serve: %w", name, err)
}

func expectedStop(err error) bool {
	return err == nil || errors.Is(err, grpc.ErrServerStopped) || errors.Is(err, net.ErrClosed) || errors.Is(err, http.ErrServerClosed)
}

func startupError(ctx context.Context, err error) error {
	if ctx.Err() == context.Canceled && canceledLeaves(err) {
		return nil
	}
	return err
}

func canceledLeaves(err error) bool {
	if err == nil {
		return true
	}
	if e, ok := err.(interface{ Unwrap() []error }); ok {
		xs := e.Unwrap()
		if len(xs) == 0 {
			return false
		}
		for _, x := range xs {
			if !canceledLeaves(x) {
				return false
			}
		}
		return true
	}
	if e, ok := err.(interface{ Unwrap() error }); ok {
		x := e.Unwrap()
		return x != nil && canceledLeaves(x)
	}
	return err == context.Canceled
}

type tieredBackend struct{ tc *pkgcache.TieredCache }

var errTieredFill = errors.New("writes are not supported in tiered mode")

func (b *tieredBackend) Get(ctx context.Context, p store.Partition, key store.ContentKey) (pkgcache.CacheResult, pkgcache.Blob, error) {
	found, blob, err := b.tc.Get(ctx, p, key)
	if err != nil {
		return pkgcache.CacheMiss, nil, err
	}
	if !found {
		return pkgcache.CacheMiss, nil, nil
	}
	return pkgcache.CacheHit, blob, nil
}
func (*tieredBackend) Fill(context.Context, store.Partition, store.ContentKey, []byte) error {
	return errTieredFill
}
func (*tieredBackend) RejectsWrites() bool { return true }

func collectRedisStores(primary *redisstore.Store, comps *tieredComponents) []*redisstore.Store {
	var stores []*redisstore.Store
	if primary != nil {
		stores = append(stores, primary)
	}
	if comps != nil {
		for _, spec := range comps.TierSpecs {
			if spec.RedisStore != nil {
				stores = append(stores, spec.RedisStore)
			}
		}
	}
	return stores
}

func collectRedisGaugeSources(primary *redisstore.Store, comps *tieredComponents) []redisGaugeSource {
	var sources []redisGaugeSource
	if primary != nil {
		sources = append(sources, redisGaugeSource{label: "redis", store: primary})
	}
	if comps != nil {
		for i, spec := range comps.TierSpecs {
			if spec.RedisStore != nil {
				sources = append(sources, redisGaugeSource{label: fmt.Sprintf("redis[L%d]", i), store: spec.RedisStore})
			}
		}
	}
	return sources
}

func monitorRedisHealth(ctx context.Context, healthSrv interface {
	SetServingStatus(string, healthgrpc.HealthCheckResponse_ServingStatus)
}, stores []*redisstore.Store) {
	probe := func() {
		status := healthgrpc.HealthCheckResponse_SERVING
		for _, store := range stores {
			probeCtx, cancel := context.WithTimeout(ctx, time.Second)
			err := store.Probe(probeCtx)
			cancel()
			if err != nil {
				status = healthgrpc.HealthCheckResponse_NOT_SERVING
				break
			}
		}
		if ctx.Err() != nil {
			return
		}
		healthSrv.SetServingStatus("", status)
	}
	probe()
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			probe()
		case <-ctx.Done():
			return
		}
	}
}

func runMembership(ctx context.Context, reload <-chan struct{}, load func(context.Context) ([]MembershipUpdate, error), comps *tieredComponents, log *slog.Logger) {
	for reload != nil {
		select {
		case <-ctx.Done():
			return
		case _, ok := <-reload:
			if !ok {
				reload = nil
				continue
			}
			if ctx.Err() != nil {
				return
			}
			updates, err := load(ctx)
			if ctx.Err() != nil {
				return
			}
			if err != nil {
				if ctx.Err() == nil {
					log.Error("cache membership reload failed", "error", err)
				}
				continue
			}
			updates, err = validateMembershipUpdates(updates, comps)
			if err != nil {
				log.Error("cache membership reload rejected", "error", err)
				continue
			}
			for _, update := range updates {
				spec := comps.TierSpecs[update.TierIndex]
				candidates := make([]ec.Peer, len(update.Peers))
				for i, p := range update.Peers {
					candidates[i] = ec.Peer{ID: p.ID, Endpoint: p.Endpoint}
				}
				live := probeAndFilter(ctx, candidates, log)
				if ctx.Err() != nil {
					return
				}
				if len(live) == 0 {
					log.Warn("cache membership reload retained old membership", "tier", update.TierIndex, "reason", "no live peers")
					continue
				}
				m := ec.Membership{Epoch: spec.EC.Epoch() + 1, Peers: live}
				if err := spec.EC.ApplyMembership(m); err != nil {
					log.Error("cache membership apply failed", "tier", update.TierIndex, "error", err)
				}
			}
		}
	}
}

func validateMembershipUpdates(in []MembershipUpdate, comps *tieredComponents) ([]MembershipUpdate, error) {
	if comps == nil {
		return nil, errors.New("cache: membership updates require tiered mode")
	}
	out := make([]MembershipUpdate, len(in))
	seen := make(map[int]struct{}, len(in))
	for i, u := range in {
		if u.TierIndex < 0 || u.TierIndex >= len(comps.TierSpecs) {
			return nil, fmt.Errorf("cache: membership update tier index %d out of range", u.TierIndex)
		}
		if _, ok := seen[u.TierIndex]; ok {
			return nil, fmt.Errorf("cache: duplicate membership update for tier %d", u.TierIndex)
		}
		seen[u.TierIndex] = struct{}{}
		spec := comps.TierSpecs[u.TierIndex]
		if spec.Type != "ec" || spec.EC == nil {
			return nil, fmt.Errorf("cache: membership update tier %d is not EC", u.TierIndex)
		}
		out[i] = MembershipUpdate{TierIndex: u.TierIndex, Peers: append([]PeerConfig(nil), u.Peers...)}
	}
	return out, nil
}

func probeAndFilter(parent context.Context, candidates []ec.Peer, log *slog.Logger) []ec.Peer {
	ctx, cancel := context.WithTimeout(parent, 3*time.Second)
	defer cancel()
	type result struct {
		p  ec.Peer
		ok bool
	}
	resCh := make(chan result, len(candidates))
	var wg sync.WaitGroup
	for _, p := range candidates {
		wg.Add(1)
		go func(p ec.Peer) {
			defer wg.Done()
			err := ec.ProbePeer(ctx, p.Endpoint)
			if err != nil && ctx.Err() == nil {
				log.Warn("cache membership peer probe failed", "peer", p.ID, "endpoint", p.Endpoint, "error", err)
			}
			resCh <- result{p: p, ok: err == nil}
		}(p)
	}
	wg.Wait()
	close(resCh)
	out := make([]ec.Peer, 0, len(candidates))
	for r := range resCh {
		if r.ok {
			out = append(out, r.p)
		}
	}
	return out
}

func startRedisHealthMonitor(ctx context.Context, healthSrv interface {
	SetServingStatus(string, healthgrpc.HealthCheckResponse_ServingStatus)
}, stores []*redisstore.Store) <-chan struct{} {
	done := make(chan struct{})
	go func() {
		defer close(done)
		monitorRedisHealth(ctx, healthSrv, stores)
	}()
	return done
}
