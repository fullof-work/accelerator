// Package flatten verifies and applies OCI layers into a private rootfs, then
// prepares a deterministic single-device EROFS layout. PreparedImage streams
// the existing artifact directly from that stable source. The explicit raw
// file APIs use the same layout and append the runtime-config ZIP.
package flatten

import (
	"crypto/sha256"
	"fmt"
	"io"
	"os"

	"github.com/kuasar-sandbox/accelerator/internal/util"
)

// dockerManifestEntry describes one image in a docker-archive tar.
type dockerManifestEntry struct {
	Config   string   `json:"Config"`
	RepoTags []string `json:"RepoTags"`
	Layers   []string `json:"Layers"`
}

// Options tweaks Flatten's environment. Zero value reproduces the
// historic behaviour (temp dir under $TMPDIR or /tmp).
type Options struct {
	// LayerLimits and PlanLimits bound the streaming preparation path.
	LayerLimits   LayerLimits
	PlanLimits    PlanLimits
	ArchiveLimits ArchiveLimits
	// TmpDir overrides the parent of the per-run scratch directory.
	// Useful when /tmp is small and images are large. Empty → default
	// (os.MkdirTemp("", ...) which respects $TMPDIR).
	TmpDir string

	// Progress, if non-nil, receives coarse stage notifications during a
	// flatten so a caller (the CLI) can render progress; the library stays
	// silent when nil. stage is a short token; done/total carry per-stage
	// detail:
	//   - "apply-layers": 1-based layer index in done, layer count in total.
	//   - "build-erofs" / "append-config": done=total=0.
	Progress func(stage string, done, total int)

	// CleanupWarning, if non-nil, reports failure to remove an empty staging
	// directory after an explicit raw file API has committed its destination.
	// It runs synchronously before return. The committed output remains valid;
	// this diagnostic does not turn success into an error. Nil stays silent.
	// Failures before rename, including source cleanup, remain return errors.
	CleanupWarning func(error)
}

// LayerOpener opens one layer's *uncompressed* tar stream. The caller
// closes the returned ReadCloser. Each call should yield a fresh stream
// positioned at the start of the layer tar.
type LayerOpener func() (io.ReadCloser, error)

// Source supplies the ordered layers (base→top) and the raw OCI image
// config JSON for a single image. The existing remote.Pull
// result implements this contract. New registry callers should use the
// descriptor-verified streaming input instead.
type Source interface {
	// Layers returns layer openers in application order (base first).
	// Each opener yields an *uncompressed* tar stream.
	Layers() ([]LayerOpener, error)
	// ConfigJSON returns the raw OCI image-config JSON document, which
	// Build projects via image.ExtractRuntimeConfigFromJSON.
	ConfigJSON() ([]byte, error)
}

// Verify flattens the same input twice and checks byte-identical output.
// Returns (match, hash1, hash2, error). Both inputs are rewound to the start
// before flattening.
func Verify(input1, input2 io.ReadSeeker) (bool, string, string, error) {
	tmp1, err := os.CreateTemp("", "verify1-*.img")
	if err != nil {
		return false, "", "", fmt.Errorf("verify: create temp1: %w", err)
	}
	defer os.Remove(tmp1.Name())
	defer tmp1.Close()

	tmp2, err := os.CreateTemp("", "verify2-*.img")
	if err != nil {
		return false, "", "", fmt.Errorf("verify: create temp2: %w", err)
	}
	defer os.Remove(tmp2.Name())
	defer tmp2.Close()

	if _, err := input1.Seek(0, io.SeekStart); err != nil {
		return false, "", "", fmt.Errorf("verify: seek input1: %w", err)
	}
	if err := Flatten(input1, tmp1.Name()); err != nil {
		return false, "", "", fmt.Errorf("verify: flatten input1: %w", err)
	}

	if _, err := input2.Seek(0, io.SeekStart); err != nil {
		return false, "", "", fmt.Errorf("verify: seek input2: %w", err)
	}
	if err := Flatten(input2, tmp2.Name()); err != nil {
		return false, "", "", fmt.Errorf("verify: flatten input2: %w", err)
	}

	hash1, err := hashFile(tmp1.Name())
	if err != nil {
		return false, "", "", fmt.Errorf("verify: hash output1: %w", err)
	}

	hash2, err := hashFile(tmp2.Name())
	if err != nil {
		return false, "", "", fmt.Errorf("verify: hash output2: %w", err)
	}

	return hash1 == hash2, hash1, hash2, nil
}

// locateMkfsErofs resolves the mkfs.erofs binary with a fixed
// precedence: $MKFS_EROFS_PATH > directory of running flatten-ctl
// executable > $PATH. The hint message in the returned error keeps
// users pointed at `make deps-erofs`.
func locateMkfsErofs() (string, error) {
	if p := os.Getenv("MKFS_EROFS_PATH"); p != "" {
		return p, nil
	}
	p, err := util.LocateBinary("mkfs.erofs")
	if err != nil {
		return "", fmt.Errorf("flatten: mkfs.erofs not found " +
			"(set MKFS_EROFS_PATH, place it alongside flatten-ctl, " +
			"or add it to PATH; run `make deps-erofs` to build it)")
	}
	return p, nil
}

// hashFile computes the SHA-256 hex digest of a file.
func hashFile(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()

	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return fmt.Sprintf("%x", h.Sum(nil)), nil
}
