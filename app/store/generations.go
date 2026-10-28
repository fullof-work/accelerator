package store

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	pkgstore "github.com/kuasar-sandbox/accelerator/pkg/store"
	storeconfig "github.com/kuasar-sandbox/accelerator/pkg/store/config"
	"github.com/kuasar-sandbox/accelerator/pkg/store/s3/sdkclient"
)

// Config is the declarative Store configuration shared with store-ctl.
type Config = storeconfig.Config

// Credentials is the provider-neutral credential value accepted by Store.
type Credentials = sdkclient.Credentials

// CredentialsProvider supplies credentials for one explicitly bound purpose.
type CredentialsProvider = sdkclient.CredentialsProvider

// GenerationSource is an authoritative source of complete, oldest-to-newest
// generation lists. A zero RefreshInterval disables periodic refresh.
type GenerationSource struct {
	Load            func(context.Context) ([]pkgstore.Generation, error)
	RefreshInterval time.Duration
}

// Options contains the process-local inputs to the Store application.
type Options struct {
	// Closing Drain starts irreversible soft shutdown; nil preserves ctx-only use.
	Drain <-chan struct{}
	// DrainTimeout bounds graceful waiting, never the lifetime of resource users.
	// Zero is unlimited; negative values are rejected before acquiring resources.
	DrainTimeout          time.Duration
	Logger                *slog.Logger
	Reload                <-chan struct{}
	Generations           *GenerationSource
	ObjectCredentials     CredentialsProvider
	GenerationCredentials CredentialsProvider
}

// prepareConfig makes and normalizes the configuration that Run will own. It
// validates only selected inputs and performs no external I/O or callbacks.
func prepareConfig(cfg Config, opts Options) (Config, error) {
	if opts.DrainTimeout < 0 {
		return Config{}, errors.New("drain timeout must not be negative")
	}
	effective := cfg.Clone()
	overridden := opts.Generations != nil
	if overridden {
		if opts.Generations.Load == nil {
			return Config{}, errors.New("store: generation source Load is required")
		}
		if opts.Generations.RefreshInterval < 0 {
			return Config{}, errors.New("store: generation refresh interval must not be negative")
		}
	}
	if err := effective.NormalizeEffective(true, !overridden, opts.ObjectCredentials == nil, opts.GenerationCredentials == nil); err != nil {
		return Config{}, err
	}
	if opts.ObjectCredentials != nil {
		if effective.Backend != "s3" {
			return Config{}, errors.New("store: object credentials require s3 backend")
		}
		effective.S3.AccessKey = ""
		effective.S3.SecretKey = ""
	}
	if overridden {
		if opts.GenerationCredentials != nil {
			return Config{}, errors.New("store: generation credentials are unused with explicit generations")
		}
	} else if opts.GenerationCredentials != nil {
		if effective.Generations == nil || effective.Generations.S3 == nil {
			return Config{}, errors.New("store: generation credentials require built-in s3 generations")
		}
		effective.Generations.S3.AccessKey = ""
		effective.Generations.S3.SecretKey = ""
	}
	if effective.Backend == "s3" {
		if _, err := effective.S3OpTimeout(); err != nil {
			return Config{}, fmt.Errorf("store: invalid s3 op_timeout: %w", err)
		}
	}
	return effective, nil
}

// generationInput owns refresh scheduling for one authoritative source. Calls
// to load happen only on the run goroutine, so timer and explicit refreshes can
// never overlap. The source and its callback are borrowed.
type generationInput struct {
	load     func(context.Context) ([]pkgstore.Generation, error)
	interval time.Duration

	loadMu   sync.Mutex
	snapshot atomic.Value // immutable []pkgstore.Generation
}

func newGenerationInput(ctx context.Context, source *GenerationSource) (*generationInput, error) {
	if ctx == nil {
		return nil, errors.New("store: nil generation context")
	}
	if source == nil {
		return nil, errors.New("store: generation source is required")
	}
	if source.Load == nil {
		return nil, errors.New("store: generation source Load is required")
	}
	if source.RefreshInterval < 0 {
		return nil, errors.New("store: generation refresh interval must not be negative")
	}
	g := &generationInput{load: source.Load, interval: source.RefreshInterval}
	if err := g.refresh(ctx); err != nil {
		return nil, err
	}
	return g, nil
}

func (g *generationInput) refresh(ctx context.Context) error {
	g.loadMu.Lock()
	defer g.loadMu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	next, err := g.load(ctx)
	if err != nil {
		return err
	}
	// Copy before validation and publication so validation and the active
	// snapshot cannot be changed through caller-owned backing storage.
	next = append([]pkgstore.Generation(nil), next...)
	if err := pkgstore.ValidateGenerations(next); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	g.snapshot.Store(next)
	return nil
}

func (g *generationInput) current() []pkgstore.Generation {
	return append([]pkgstore.Generation(nil), g.view()...)
}

// view returns the immutable snapshot used by the Store server request path.
// Callers must not mutate the returned slice.
func (g *generationInput) view() []pkgstore.Generation {
	return g.snapshot.Load().([]pkgstore.Generation)
}

// run refreshes until ctx is cancelled. It returns only after any synchronous
// Load already in progress has returned; consequently its return is the
// controller's quiescence boundary.
func (g *generationInput) run(ctx context.Context, reload <-chan struct{}, logger *slog.Logger) {
	var ticker *time.Ticker
	var ticks <-chan time.Time
	if g.interval > 0 {
		ticker = time.NewTicker(g.interval)
		ticks = ticker.C
		defer ticker.Stop()
	}

	for {
		if ctx.Err() != nil {
			return
		}
		select {
		case <-ctx.Done():
			return
		case _, ok := <-reload:
			if !ok {
				reload = nil
				continue
			}
			if ctx.Err() == nil {
				g.refreshAndLog(ctx, logger)
			}
		case <-ticks:
			if ctx.Err() == nil {
				g.refreshAndLog(ctx, logger)
			}
		}
	}
}

func (g *generationInput) refreshAndLog(ctx context.Context, logger *slog.Logger) {
	if err := g.refresh(ctx); err != nil && logger != nil {
		logger.Error("generation refresh failed; keeping previous list", "error", err)
	}
}
