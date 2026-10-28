package config

import (
	"fmt"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// defaultStatsInterval is the base period for the adaptive stats line when
// stats_interval is unset.
const defaultStatsInterval = 30 * time.Second

const defaultS3Region = "us-east-1"

// StatsIntervalDur parses stats_interval. Empty/absent → 30s (on by default);
// "0"/"off"/"none" → 0 (disabled); any valid Go duration overrides; an invalid
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

// Config is the YAML configuration shared by every store-ctl
// subcommand. Two backend flavours, selected by `backend:`:
//
//	backend: fs   — local filesystem
//	backend: s3   — S3-compatible object storage
//
// Generations selects a config, file, or S3 list source. Admin subcommands can
// mutate only writable file/S3 sources; serve loads and refreshes the selected
// source independently of the object data backend.
type Config struct {
	// Listen is the gRPC server address (TCP, UDS, or fd=<name>). Required by
	// `serve`; ignored by other subcommands.
	Listen string `yaml:"listen"`

	// Backend selects the backing storage. "fs" or "s3". Required.
	Backend string `yaml:"backend"`

	// StatsInterval is the base period for the adaptive stats line the
	// daemon prints to stderr (silent when there was no traffic in the
	// window). Empty/absent → 30s (on by default); "0"/"off" disables it.
	StatsInterval string `yaml:"stats_interval"`

	// CacheListen, when non-empty, starts an embedded read-only cache wire
	// server on this address (TCP, UDS, or fd=<name>) alongside the store
	// gRPC. It serves cache-protocol object reads (chunk/manifest/blob)
	// straight from the backend, so cache clients can read store content
	// without a separate cache-ctl. Writes are rejected (use the store
	// gRPC). Empty = disabled.
	CacheListen string `yaml:"cache_listen"`

	// FS / S3 hold backend-specific config. Exactly one must match
	// Backend after legacy input has been normalised.
	FS *FSConfig `yaml:"fs,omitempty"`
	S3 *S3Config `yaml:"s3,omitempty"`

	// OBS exists only to detect and silently normalise the complete
	// legacy backend=obs + obs: input pair. It is nil after LoadConfig.
	OBS *S3Config `yaml:"obs,omitempty"`

	// Generations selects exactly one oldest-to-newest list source. When
	// omitted, LoadConfig normalises it to the legacy backend-local path.
	Generations *GenerationsConfig `yaml:"generations,omitempty"`

	fsSectionSet  bool
	s3SectionSet  bool
	obsSectionSet bool
}

// UnmarshalYAML records mapping-key presence independently of decoded pointer
// values. This distinguishes an omitted section from an explicitly present
// empty/null section, which is required to reject every mixed config form.
func (c *Config) UnmarshalYAML(node *yaml.Node) error {
	type plainConfig Config
	var decoded plainConfig
	if err := node.Decode(&decoded); err != nil {
		return err
	}
	*c = Config(decoded)
	if node.Kind != yaml.MappingNode {
		return nil
	}
	for i := 0; i+1 < len(node.Content); i += 2 {
		switch node.Content[i].Value {
		case "fs":
			c.fsSectionSet = true
		case "s3":
			c.s3SectionSet = true
		case "obs":
			c.obsSectionSet = true
		}
	}
	return nil
}

// FSConfig is the filesystem backend's parameter set.
type FSConfig struct {
	// Root is the on-disk directory that holds the store. It must
	// exist before the filesystem object backend can be opened.
	Root string `yaml:"root"`

	// VerifyContentKey: re-hash on Put. Default true.
	VerifyContentKey *bool `yaml:"verify_content_key"`

	// DirectIO enables strict O_DIRECT reads for object Get only.
	DirectIO bool `yaml:"direct_io"`
}

// GenerationsConfig selects one mutually-exclusive generation source.
type GenerationsConfig struct {
	RefreshInterval string                `yaml:"refresh_interval,omitempty"`
	Config          []string              `yaml:"config,omitempty"`
	File            *GenerationFileConfig `yaml:"file,omitempty"`
	S3              *GenerationS3Config   `yaml:"s3,omitempty"`

	configSet bool
	fileSet   bool
	s3Set     bool
}

// IsConfigSource reports whether the inline config source was selected.
func (c *GenerationsConfig) IsConfigSource() bool {
	return c != nil && (c.configSet || c.Config != nil)
}

func (c *GenerationsConfig) UnmarshalYAML(node *yaml.Node) error {
	type plain GenerationsConfig
	var decoded plain
	if err := node.Decode(&decoded); err != nil {
		return err
	}
	*c = GenerationsConfig(decoded)
	if node.Kind != yaml.MappingNode {
		return nil
	}
	for i := 0; i+1 < len(node.Content); i += 2 {
		switch node.Content[i].Value {
		case "config":
			c.configSet = true
		case "file":
			c.fileSet = true
		case "s3":
			c.s3Set = true
		}
	}
	return nil
}

type GenerationFileConfig struct {
	Path string `yaml:"path"`
}

// GenerationS3Config is intentionally independent from the data backend's
// S3 location and credentials.
type GenerationS3Config struct {
	Endpoint  string `yaml:"endpoint"`
	Region    string `yaml:"region"`
	Bucket    string `yaml:"bucket"`
	Key       string `yaml:"key"`
	PathStyle *bool  `yaml:"path_style"`
	AccessKey string `yaml:"access_key"`
	SecretKey string `yaml:"secret_key"`

	// TLS tunes certificate verification for the generation endpoint,
	// independently of the data backend's s3.tls.
	TLS *S3TLSConfig `yaml:"tls,omitempty"`
}

// S3TLSConfig tunes how the endpoint's HTTPS certificates are verified,
// mirroring pkg/remote's and the orchestrator build registry's tls
// block. The zero value is strict verification against the system
// trust store.
type S3TLSConfig struct {
	// CACert is a path to a PEM CA bundle (may hold several
	// certificates) appended to the system trust store — the way to
	// trust an endpoint (or intercepting proxy) whose CA is not in
	// the system store. Supports ${VAR} expansion.
	CACert string `yaml:"ca_cert"`

	// InsecureSkipVerify disables certificate verification entirely.
	// Insecure — traffic including credentials can be intercepted;
	// testing only. Mutually exclusive with ca_cert.
	InsecureSkipVerify bool `yaml:"insecure_skip_verify"`
}

// validate rejects the one nonsensical combination. prefix locates the
// block in error messages (e.g. "s3.tls", "generations.s3.tls").
func (c *S3TLSConfig) validate(prefix string) error {
	if c == nil {
		return nil
	}
	if c.CACert != "" && c.InsecureSkipVerify {
		return fmt.Errorf("store-ctl: %s.ca_cert and %s.insecure_skip_verify are mutually exclusive", prefix, prefix)
	}
	return nil
}

func (c *GenerationS3Config) expandEnv() {
	c.Endpoint = expandEnv(c.Endpoint)
	c.Region = expandEnv(c.Region)
	c.Bucket = expandEnv(c.Bucket)
	c.Key = expandEnv(c.Key)
	c.AccessKey = expandEnv(c.AccessKey)
	c.SecretKey = expandEnv(c.SecretKey)
	if c.TLS != nil {
		c.TLS.CACert = expandEnv(c.TLS.CACert)
	}
}

func (c *GenerationS3Config) PathStyleEnabled() bool {
	if c.PathStyle == nil {
		return true
	}
	return *c.PathStyle
}

// S3Config is the S3-compatible backend's parameter set. String fields
// support `${VAR}` shell-style env-var expansion so the yaml can be
// committed without secrets.
type S3Config struct {
	// Endpoint is the S3-compatible endpoint URL. Required.
	Endpoint string `yaml:"endpoint"`

	// Region is the signing region. Empty defaults to us-east-1.
	Region string `yaml:"region"`

	// Bucket is the target bucket name. Required.
	Bucket string `yaml:"bucket"`

	// PathStyle selects endpoint/bucket/key addressing. Empty defaults
	// to true; set false for virtual-host-only services.
	PathStyle *bool `yaml:"path_style"`

	// Prefix is an optional in-bucket key prefix (multi-tenant
	// namespacing). Trailing slash optional; normalised internally.
	Prefix string `yaml:"prefix"`

	// AccessKey / SecretKey are optional static credentials. When both
	// are empty, the AWS SDK default credential provider chain is used.
	AccessKey string `yaml:"access_key"`
	SecretKey string `yaml:"secret_key"`

	// VerifyContentKey: re-hash on Put. Default true.
	VerifyContentKey *bool `yaml:"verify_content_key"`

	// MaxInflight bounds concurrent S3 calls. Default 64.
	MaxInflight int `yaml:"max_inflight"`

	// OpTimeout is the per-call wall-clock budget (Go duration
	// string, e.g. "10s"). Empty/absent = 0 = no per-op deadline.
	OpTimeout string `yaml:"op_timeout"`

	// MaxObjectSize bounds Get response bytes. Default 16 MiB.
	MaxObjectSize int64 `yaml:"max_object_size_bytes"`

	// TLS tunes certificate verification for the endpoint.
	TLS *S3TLSConfig `yaml:"tls,omitempty"`
}

// envVarPattern matches `${VAR_NAME}` with alphanumeric / underscore
// names. Used by expandEnv on selected string fields.
var envVarPattern = regexp.MustCompile(`\$\{([A-Za-z_][A-Za-z0-9_]*)\}`)

// expandEnv replaces every `${VAR}` in s with os.Getenv(VAR). An
// undefined env var maps to empty string (callers must validate
// required fields after expansion).
func expandEnv(s string) string {
	return envVarPattern.ReplaceAllStringFunc(s, func(match string) string {
		name := match[2 : len(match)-1]
		return os.Getenv(name)
	})
}

func (c *S3Config) expandEnv() {
	c.Endpoint = expandEnv(c.Endpoint)
	c.Region = expandEnv(c.Region)
	c.Bucket = expandEnv(c.Bucket)
	c.Prefix = expandEnv(c.Prefix)
	c.AccessKey = expandEnv(c.AccessKey)
	c.SecretKey = expandEnv(c.SecretKey)
	c.OpTimeout = expandEnv(c.OpTimeout)
	if c.TLS != nil {
		c.TLS.CACert = expandEnv(c.TLS.CACert)
	}
}

// normalizeBackend validates that the selected backend and config section
// match. The only accepted legacy form is the complete backend=obs + obs:
// pair, which is immediately converted to backend=s3 + S3.
func (c *Config) normalizeBackend() error {
	hasFS := c.fsSectionSet || c.FS != nil
	hasS3 := c.s3SectionSet || c.S3 != nil
	hasOBS := c.obsSectionSet || c.OBS != nil
	if hasS3 && hasOBS {
		return fmt.Errorf(`store-ctl: "s3" and "obs" config sections cannot both be set`)
	}

	switch c.Backend {
	case "fs":
		if hasS3 || hasOBS {
			return fmt.Errorf("store-ctl: object storage config is not valid for backend=fs")
		}
		if !hasFS || c.FS == nil {
			return fmt.Errorf("store-ctl: fs config is required for backend=fs")
		}
	case "s3":
		if hasOBS {
			return fmt.Errorf(`store-ctl: "obs" config cannot be used with backend=s3`)
		}
		if hasFS {
			return fmt.Errorf(`store-ctl: "fs" config cannot be used with backend=s3`)
		}
		if !hasS3 || c.S3 == nil {
			return fmt.Errorf("store-ctl: s3 config is required for backend=s3")
		}
	case "obs":
		if hasS3 {
			return fmt.Errorf(`store-ctl: "s3" config cannot be used with backend=obs`)
		}
		if hasFS {
			return fmt.Errorf(`store-ctl: "fs" config cannot be used with backend=obs`)
		}
		if !hasOBS || c.OBS == nil {
			return fmt.Errorf("store-ctl: obs config is required for backend=obs")
		}
		c.Backend = "s3"
		c.S3 = c.OBS
		c.OBS = nil
		c.s3SectionSet = true
		c.obsSectionSet = false
	default:
		return fmt.Errorf("store-ctl: unknown backend %q (want fs|s3)", c.Backend)
	}
	return nil
}

// LoadConfig reads, validates, and normalises a Config from path.
// `requireListen` is set by `serve` (which needs to bind a port);
// admin subcommands clear it because they never bind anything.
func LoadConfig(path string, requireListen bool) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("store-ctl: read config %s: %w", path, err)
	}
	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("store-ctl: parse config %s: %w", path, err)
	}
	if err := cfg.normalize(requireListen, true); err != nil {
		return nil, err
	}
	return &cfg, nil
}

// Normalize validates and applies defaults to a Config constructed directly in
// Go. Unlike LoadConfig, it never reads a file or expands environment variables.
func (c *Config) Normalize(requireListen bool) error {
	return c.normalize(requireListen, false)
}

// Clone returns an independent copy, including configuration-presence state.
func (c Config) Clone() Config {
	cloneBool := func(value *bool) *bool {
		if value == nil {
			return nil
		}
		copy := *value
		return &copy
	}
	cloneTLS := func(value *S3TLSConfig) *S3TLSConfig {
		if value == nil {
			return nil
		}
		copy := *value
		return &copy
	}
	cloneS3 := func(value *S3Config) *S3Config {
		if value == nil {
			return nil
		}
		copy := *value
		copy.PathStyle = cloneBool(value.PathStyle)
		copy.VerifyContentKey = cloneBool(value.VerifyContentKey)
		copy.TLS = cloneTLS(value.TLS)
		return &copy
	}

	copy := c
	if c.FS != nil {
		fs := *c.FS
		fs.VerifyContentKey = cloneBool(c.FS.VerifyContentKey)
		copy.FS = &fs
	}
	copy.S3 = cloneS3(c.S3)
	copy.OBS = cloneS3(c.OBS)
	if c.Generations != nil {
		generations := *c.Generations
		generations.Config = append([]string(nil), c.Generations.Config...)
		if c.Generations.File != nil {
			file := *c.Generations.File
			generations.File = &file
		}
		if c.Generations.S3 != nil {
			s3 := *c.Generations.S3
			s3.PathStyle = cloneBool(c.Generations.S3.PathStyle)
			s3.TLS = cloneTLS(c.Generations.S3.TLS)
			generations.S3 = &s3
		}
		copy.Generations = &generations
	}
	return copy
}

// NormalizeEffective applies direct-Go defaults while validating only the
// credential and generation inputs selected by the caller. It never performs
// file I/O or environment expansion. The booleans suppress only static-pair
// validation; all active location, TLS, and duration validation is retained.
func (c *Config) NormalizeEffective(requireListen, generations, objectStatic, generationStatic bool) error {
	return c.normalizeSelected(requireListen, false, generations, objectStatic, generationStatic)
}

func (c *Config) normalize(requireListen, expandEnvironment bool) error {
	return c.normalizeSelected(requireListen, expandEnvironment, true, true, true)
}

func (c *Config) normalizeSelected(requireListen, expandEnvironment, generations, objectStatic, generationStatic bool) error {
	if requireListen && c.Listen == "" {
		return fmt.Errorf("store-ctl: listen is required")
	}
	if c.Backend == "" {
		return fmt.Errorf("store-ctl: backend is required (want fs|s3)")
	}
	if err := c.normalizeBackend(); err != nil {
		return err
	}

	switch c.Backend {
	case "fs":
		if c.FS.Root == "" {
			return fmt.Errorf("store-ctl: fs.root is required for backend=fs")
		}
	case "s3":
		if expandEnvironment {
			c.S3.expandEnv()
		}
		if c.S3.Region == "" {
			c.S3.Region = defaultS3Region
		}
		if c.S3.Endpoint == "" {
			return fmt.Errorf("store-ctl: s3.endpoint is required for backend=s3")
		}
		if c.S3.Bucket == "" {
			return fmt.Errorf("store-ctl: s3.bucket is required for backend=s3")
		}
		if objectStatic && (c.S3.AccessKey == "") != (c.S3.SecretKey == "") {
			return fmt.Errorf("store-ctl: s3.access_key and s3.secret_key must be set together")
		}
		if err := c.S3.TLS.validate("s3.tls"); err != nil {
			return err
		}
	}
	if generations {
		return c.normaliseGenerations(expandEnvironment, generationStatic)
	}
	return nil
}

func (c *Config) normaliseGenerations(expandEnvironment, validateStatic bool) error {
	if c.Generations == nil {
		switch c.Backend {
		case "fs":
			c.Generations = &GenerationsConfig{
				File:    &GenerationFileConfig{Path: filepath.Join(c.FS.Root, "__meta", "generations")},
				fileSet: true,
			}
		case "s3":
			var pathStyle *bool
			if c.S3.PathStyle != nil {
				value := *c.S3.PathStyle
				pathStyle = &value
			}
			var tls *S3TLSConfig
			if c.S3.TLS != nil {
				value := *c.S3.TLS
				tls = &value
			}
			c.Generations = &GenerationsConfig{
				S3: &GenerationS3Config{
					Endpoint:  c.S3.Endpoint,
					Region:    c.S3.Region,
					Bucket:    c.S3.Bucket,
					Key:       path.Join(strings.Trim(c.S3.Prefix, "/"), "__meta/generations"),
					PathStyle: pathStyle,
					AccessKey: c.S3.AccessKey,
					SecretKey: c.S3.SecretKey,
					TLS:       tls,
				},
				s3Set: true,
			}
		}
	}
	g := c.Generations
	count := 0
	if g.configSet || g.Config != nil {
		g.configSet = true
		count++
	}
	if g.fileSet || g.File != nil {
		g.fileSet = true
		count++
	}
	if g.s3Set || g.S3 != nil {
		g.s3Set = true
		count++
	}
	if count != 1 {
		return fmt.Errorf("store-ctl: generations must configure exactly one of config, file, or s3")
	}
	if g.fileSet && g.File == nil {
		return fmt.Errorf("store-ctl: generations.file must be a mapping with path")
	}
	if g.s3Set && g.S3 == nil {
		return fmt.Errorf("store-ctl: generations.s3 must be a mapping")
	}
	if g.configSet {
		if g.RefreshInterval != "" {
			return fmt.Errorf("store-ctl: generations.refresh_interval is not valid for config source")
		}
		return nil
	}
	if g.File != nil {
		if expandEnvironment {
			g.File.Path = expandEnv(g.File.Path)
		}
		if g.File.Path == "" {
			return fmt.Errorf("store-ctl: generations.file.path is required")
		}
	}
	if g.S3 != nil {
		if expandEnvironment {
			g.S3.expandEnv()
		}
		if g.S3.Region == "" {
			g.S3.Region = defaultS3Region
		}
		if g.S3.Endpoint == "" || g.S3.Bucket == "" || g.S3.Key == "" {
			return fmt.Errorf("store-ctl: generations.s3 endpoint, bucket, and key are required")
		}
		if validateStatic && (g.S3.AccessKey == "") != (g.S3.SecretKey == "") {
			return fmt.Errorf("store-ctl: generations.s3 access_key and secret_key must be set together")
		}
		if err := g.S3.TLS.validate("generations.s3.tls"); err != nil {
			return err
		}
	}
	if g.RefreshInterval == "" {
		g.RefreshInterval = "5s"
	}
	duration, err := time.ParseDuration(g.RefreshInterval)
	if err != nil || duration <= 0 {
		return fmt.Errorf("store-ctl: invalid generations.refresh_interval %q", g.RefreshInterval)
	}
	return nil
}

func (c *Config) GenerationRefreshInterval() time.Duration {
	if c.Generations == nil || c.Generations.configSet {
		return 0
	}
	duration, _ := time.ParseDuration(c.Generations.RefreshInterval)
	return duration
}

// VerifyKey returns the resolved verify-content-key value for the
// selected data backend. Both flavours default to true.
func (c *Config) VerifyKey() bool {
	switch c.Backend {
	case "s3":
		if c.S3.VerifyContentKey == nil {
			return true
		}
		return *c.S3.VerifyContentKey
	default: // "fs"
		if c.FS.VerifyContentKey == nil {
			return true
		}
		return *c.FS.VerifyContentKey
	}
}

// S3PathStyle returns the resolved S3 bucket-addressing mode. Custom
// endpoints default to path-style because they cannot be assumed to
// provide wildcard bucket DNS records and matching certificates.
func (c *Config) S3PathStyle() bool {
	if c.S3.PathStyle == nil {
		return true
	}
	return *c.S3.PathStyle
}

// S3OpTimeout returns the parsed S3 op-timeout. Empty/absent = 0 =
// no per-op deadline: an S3 op is bounded only by the caller's
// context, never an arbitrary number. Operators opt into a finite
// budget by setting s3.op_timeout explicitly.
func (c *Config) S3OpTimeout() (time.Duration, error) {
	if c.S3.OpTimeout == "" {
		return 0, nil
	}
	return time.ParseDuration(c.S3.OpTimeout)
}
