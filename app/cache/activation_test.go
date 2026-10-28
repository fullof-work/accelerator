package cache

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/kuasar-sandbox/accelerator/pkg/cache/runtime"
)

func TestRunRejectsExplicitFDConfigurationBeforeBackendCreation(t *testing.T) {
	for _, entrance := range []string{"listen", "health", "pprof"} {
		t.Run(entrance, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "must-not-open")
			cfg := Config{Mode: "local", Type: "embedded", Listen: "127.0.0.1:0", Rocks: runtime.RocksConfig{Path: path, DiskBytes: "64MiB", MemRatio: 0.05}}
			switch entrance {
			case "listen":
				cfg.Listen = "fd=missing"
			case "health":
				cfg.HealthListen = "fd=missing"
			case "pprof":
				cfg.PprofListen = "fd=missing"
			}
			err := Run(context.Background(), cfg, Options{})
			if err == nil || !strings.Contains(err.Error(), "missing fd=missing") {
				t.Fatalf("explicit FD config = %v", err)
			}
			if _, err := os.Stat(path); !os.IsNotExist(err) {
				t.Fatalf("backend touched before FD validation: %v", err)
			}
		})
	}
}
