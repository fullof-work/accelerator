package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	appstore "github.com/kuasar-sandbox/accelerator/app/store"
	"github.com/kuasar-sandbox/accelerator/pkg/activation"
)

// cmdServe owns CLI configuration, process signals, and exit behavior. The
// application package owns the complete Store serving lifecycle.
func cmdServe(args []string) {
	fset := flag.NewFlagSet("serve", flag.ExitOnError)
	configPath := fset.String("config", "", "YAML config file (overrides STORE_CONFIG env)")
	drainTimeout := fset.Duration("drain-timeout", 0, "graceful shutdown budget (0 = unlimited)")
	fset.Parse(args)
	if *drainTimeout < 0 {
		fatal("drain timeout must not be negative")
	}

	resolved := resolveConfigPath(*configPath)
	if resolved == "" {
		fatal("--config or %s required", storeConfigEnv)
	}
	cfg, err := LoadConfig(resolved, true)
	if err != nil {
		fatal("%v", err)
	}

	term := make(chan os.Signal, 2)
	hup := make(chan os.Signal, 1)
	signal.Notify(term, syscall.SIGINT, syscall.SIGTERM)
	signal.Notify(hup, syscall.SIGHUP)
	files, err := activation.FilesFromEnv()
	if err != nil {
		fatal("activation: %v", err)
	}
	err = serveStore(*cfg, resolved, term, hup, os.Stderr, *drainTimeout, func(ctx context.Context, cfg appstore.Config, opts appstore.Options) error {
		opts.ListenerFDs = files
		return appstore.Run(ctx, cfg, opts)
	})
	for _, file := range files {
		err = errors.Join(err, file.Close())
	}
	signal.Stop(hup)
	signal.Stop(term)
	if err != nil {
		fatal("serve: %v", err)
	}
}

type storeRunner func(context.Context, appstore.Config, appstore.Options) error

func serveStore(cfg Config, resolved string, term, hup <-chan os.Signal, output io.Writer, drainTimeout time.Duration, run storeRunner) error {
	reload := make(chan struct{}, 1)
	drain := make(chan struct{})
	opts := appstore.Options{Reload: reload, Drain: drain, DrainTimeout: drainTimeout}
	if cfg.Generations.IsConfigSource() {
		source := configGenerationSource{path: resolved}
		opts.Generations = &appstore.GenerationSource{Load: source.Load}
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	var signalWG sync.WaitGroup
	signalWG.Add(1)
	go func() {
		defer signalWG.Done()
		draining, forced := false, false
		for {
			select {
			case <-done:
				return
			case _, ok := <-hup:
				if !ok {
					hup = nil
					continue
				}
				if draining {
					continue
				}
				select {
				case reload <- struct{}{}:
				default:
				}
			case sig, ok := <-term:
				if !ok {
					term = nil
					continue
				}
				if !draining {
					draining = true
					fmt.Fprintf(output, "store-ctl: received %s, draining...\n", sig)
					close(drain)
				} else if !forced {
					forced = true
					fmt.Fprintf(output, "store-ctl: received %s, cancelling...\n", sig)
					cancel()
				}
			}
		}
	}()

	err := run(ctx, cfg, opts)
	close(done)
	cancel()
	signalWG.Wait()
	return err
}
