// Package remote pulls OCI images straight from a registry and adapts them
// into a flatten.Source, so flatten-ctl can flatten `repo:tag` without an
// intermediate `docker save`. It is the single home of the
// go-containerregistry dependency. Runtime code can read flattened-image
// metadata through pkg/image without pulling in registry or layer-application
// dependencies.
//
// Responsibilities: reference resolution + platform selection (pull.go),
// env-based credentials (auth.go), a shared OCI-layout blob cache with
// atomic writes and size-cap LRU eviction (cache.go, gc.go), and the OCI
// Referrers write-back / idempotent-skip (referrer.go).
package remote

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net/http"
	"os"
	"runtime"
	"time"

	v1 "github.com/google/go-containerregistry/pkg/v1"
	ggcrremote "github.com/google/go-containerregistry/pkg/v1/remote"
	"gopkg.in/yaml.v3"

	"github.com/kuasar-sandbox/accelerator/internal/util"
)

const (
	defaultMaxSize  = 10 << 30 // 10 GiB
	defaultPullJobs = 4
)

// Config is the YAML schema for flatten-ctl's remote-pull, blob-cache, and
// Referrers behaviour. It mirrors the manifest-config convention: load via
// LoadConfig from --config or $FLATTEN_CONFIG. Secrets never live here
// — registry credentials come from FLATTEN_REGISTRY_* env, the HMAC key from
// the manifest customer key. A missing file is fine: defaults apply.
// RefererArtifactType is the fixed OCI artifact type of the flatten-manifest
// referrer. It is a constant (not configurable) so FindReferrer/PutReferrer
// always agree across tools and versions.
const RefererArtifactType = "application/vnd.kuasar.flatten-manifest.v1"

type Config struct {
	// TmpDir is the parent of per-run scratch directories (flatten output, the
	// ephemeral blob cache). Empty → $TMPDIR or /tmp.
	TmpDir string `yaml:"tmpdir"`
	// Platform selects which manifest to pull from a multi-arch index, as
	// "os/arch[/variant]". Empty follows the host: linux/<runtime.GOARCH>.
	Platform string `yaml:"platform"`
	// Insecure allows plain-HTTP / skip-TLS registries (dev / private).
	Insecure bool `yaml:"insecure"`
	// PullJobs bounds legacy Pull's complete downloads. Streaming Prepare uses
	// at most one future layer and Cache.InFlightBytes, with the current layer
	// admitted first. <1 → defaultPullJobs.
	PullJobs int           `yaml:"pull_jobs"`
	TLS      TLSConfig     `yaml:"tls"`
	Cache    CacheConfig   `yaml:"cache"`
	Referer  RefererConfig `yaml:"referer"`

	// OnPullProgress, if non-nil, is invoked as each missing layer blob
	// finishes downloading in Pull, with the running (done, total) count of
	// layers that needed fetching (cache hits are not counted). Set by the
	// CLI to render pull progress; not serialised.
	OnPullProgress func(done, total int) `yaml:"-"`
	// OnPullRetry reports each outer retry, including resolve/config retries.
	OnPullRetry func(PullRetryEvent) `yaml:"-"`

	platform      v1.Platform       // parsed from Platform in normalize
	maxSize       int64             // parsed from Cache.MaxSize (bytes; 0 = unlimited)
	cacheInFlight int64             // maximum bytes in pending streaming cache writes
	transport     http.RoundTripper // built from TLS in normalize; nil = ggcr default
}

// TLSConfig tunes how the HTTPS transport verifies certificates when pulling.
// It applies to every registry request and, crucially, to the CDN blob
// redirects (e.g. *.cloudfront.docker.com) — an intercepting corporate proxy
// re-signs those with a private CA, which the default system trust store
// rejects. (This is distinct from the registry-scheme switch Insecure, which
// only allows plain-HTTP registries and does not affect TLS verification.)
type TLSConfig struct {
	// CACert is a path to an extra CA bundle (PEM, may hold several certs)
	// added to the system trust store — the secure way to trust a MITM
	// proxy's private CA. Empty → system store only.
	CACert string `yaml:"ca_cert"`
	// InsecureSkipVerify disables certificate verification entirely. The
	// blunt escape hatch when the proxy CA isn't available; prefer CACert.
	InsecureSkipVerify bool `yaml:"insecure_skip_verify"`
}

// CacheConfig configures the shared OCI-layout blob cache.
type CacheConfig struct {
	// Dir is the cache root (a standard OCI image layout). A shared path
	// dedups across tenants; a per-task path isolates them. Empty → default.
	Dir string `yaml:"dir"`
	// MaxSize caps the cache (human size, e.g. "10GiB"). "" → default,
	// "0" → unlimited (prune only via `cache gc`).
	MaxSize string `yaml:"max_size"`
	// InFlightBytes bounds pending compressed cache data in streaming export.
	// Empty selects 256 MiB. Larger layers bypass the cache, unless Required.
	InFlightBytes string `yaml:"in_flight_bytes,omitempty"`
	// Required rejects a layer that cannot fit the streaming cache budget.
	// It requires an explicit persistent Dir; it does not affect legacy Pull.
	Required bool `yaml:"required,omitempty"`
}

// RefererConfig configures OCI Referrers helpers. The artifact type is fixed
// (RefererArtifactType), not configurable.
type RefererConfig struct {
	// Desc is the public owner descriptor appended to the owner annotation.
	Desc string `yaml:"desc"`
	// Key is the HMAC message paired with the customer key; defaults to Desc.
	Key string `yaml:"key"`
	// Validity, when set (a Go duration), writes an expiry into valid_at.
	Validity string `yaml:"validity"`
}

// LoadConfig reads the remote config from flagPath, or $envName when flagPath
// is empty. A missing path yields an all-defaults Config (the common case for
// a plain `export <ref>`); a present-but-unreadable/invalid file errors.
func LoadConfig(flagPath, envName string) (*Config, error) {
	path := flagPath
	if path == "" {
		path = os.Getenv(envName)
	}
	c := &Config{}
	if path != "" {
		data, err := os.ReadFile(path)
		if err != nil {
			return nil, fmt.Errorf("remote: read config %q: %w", path, err)
		}
		if err := yaml.Unmarshal(data, c); err != nil {
			return nil, fmt.Errorf("remote: parse config %q: %w", path, err)
		}
	}
	if err := c.normalize(); err != nil {
		return nil, err
	}
	return c, nil
}

func (c *Config) normalize() error {
	if c.PullJobs < 1 {
		c.PullJobs = defaultPullJobs
	}
	// Cache is opt-in: streaming Prepare stores no compressed layers without
	// cache.dir. Legacy Pull/OpenCache retain the complete-download contract.
	switch c.Cache.MaxSize {
	case "":
		c.maxSize = defaultMaxSize
	case "0":
		c.maxSize = 0
	default:
		v, err := util.ParseSize(c.Cache.MaxSize)
		if err != nil || v > 1<<63-1 {
			if err == nil {
				err = fmt.Errorf("size exceeds int64")
			}
			return fmt.Errorf("remote: cache.max_size: %w", err)
		}
		c.maxSize = int64(v)
	}
	if c.Platform == "" {
		c.platform = v1.Platform{OS: "linux", Architecture: runtime.GOARCH}
	} else {
		p, err := v1.ParsePlatform(c.Platform)
		if err != nil {
			return fmt.Errorf("remote: platform %q: %w", c.Platform, err)
		}
		c.platform = *p
	}
	c.cacheInFlight = 256 << 20
	if c.Cache.InFlightBytes != "" {
		v, err := util.ParseSize(c.Cache.InFlightBytes)
		if err != nil || v == 0 || v > 1<<63-1 {
			return fmt.Errorf("remote: cache.in_flight_bytes must be a positive byte limit")
		}
		c.cacheInFlight = int64(v)
	}
	if c.Cache.Required && c.Cache.Dir == "" {
		return fmt.Errorf("remote: cache.required needs an explicit cache.dir")
	}
	if c.Referer.Key == "" {
		c.Referer.Key = c.Referer.Desc
	}
	if c.Referer.Validity != "" {
		d, err := time.ParseDuration(c.Referer.Validity)
		if err != nil {
			return fmt.Errorf("remote: referer.validity: %w", err)
		}
		if d <= 0 {
			return errors.New("remote: referer.validity must be positive")
		}
	}
	tr, err := c.buildTransport()
	if err != nil {
		return err
	}
	c.transport = tr
	return nil
}

// buildTransport returns a custom HTTP transport when TLS tuning is requested
// (a CA bundle and/or skip-verify), or nil to let ggcr use its default. It
// clones ggcr's DefaultTransport so proxy-from-env, dialer, and timeout tuning
// are preserved, and only overrides TLSClientConfig.
func (c *Config) buildTransport() (http.RoundTripper, error) {
	if c.TLS.CACert == "" && !c.TLS.InsecureSkipVerify {
		return nil, nil
	}
	base, ok := ggcrremote.DefaultTransport.(*http.Transport)
	if !ok {
		base, _ = http.DefaultTransport.(*http.Transport)
	}
	tr := base.Clone()

	tlsCfg := &tls.Config{InsecureSkipVerify: c.TLS.InsecureSkipVerify} //nolint:gosec // opt-in for MITM proxies via config
	if c.TLS.CACert != "" {
		pem, err := os.ReadFile(c.TLS.CACert)
		if err != nil {
			return nil, fmt.Errorf("remote: tls.ca_cert %q: %w", c.TLS.CACert, err)
		}
		pool, err := x509.SystemCertPool()
		if err != nil || pool == nil {
			pool = x509.NewCertPool()
		}
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("remote: tls.ca_cert %q: no PEM certificates found", c.TLS.CACert)
		}
		tlsCfg.RootCAs = pool
	}
	tr.TLSClientConfig = tlsCfg
	return tr, nil
}

// CacheDir returns the configured cache root ("" = ephemeral).
func (c *Config) CacheDir() string { return c.Cache.Dir }

// MaxCacheBytes returns the resolved cache size cap (0 = unlimited).
func (c *Config) MaxCacheBytes() int64 { return c.maxSize }

// SetPlatform overrides the configured platform (the --platform flag) and
// re-parses it. Empty is a no-op (keeps the configured/host default).
func (c *Config) SetPlatform(p string) error {
	if p == "" {
		return nil
	}
	pl, err := v1.ParsePlatform(p)
	if err != nil {
		return fmt.Errorf("remote: platform %q: %w", p, err)
	}
	c.Platform, c.platform = p, *pl
	return nil
}

// OpenCache opens the blob cache. With cache.dir set it is persistent and the
// returned cleanup is a no-op; empty (the default) yields an ephemeral cache
// under tmpdir that cleanup removes. Callers must defer cleanup().
func (c *Config) OpenCache() (*Cache, func(), error) {
	dir := c.Cache.Dir
	cleanup := func() {}
	if dir == "" {
		d, err := os.MkdirTemp(c.TmpDir, "flatten-cache-")
		if err != nil {
			return nil, nil, fmt.Errorf("remote: ephemeral cache: %w", err)
		}
		dir, cleanup = d, func() { os.RemoveAll(d) }
	}
	cache, err := OpenCache(dir, c.maxSize)
	if err != nil {
		cleanup()
		return nil, nil, err
	}
	return cache, cleanup, nil
}
