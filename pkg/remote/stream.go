package remote

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"

	v1 "github.com/google/go-containerregistry/pkg/v1"
	ggcrremote "github.com/google/go-containerregistry/pkg/v1/remote"

	"github.com/kuasar-sandbox/accelerator/pkg/flatten"
)

// Stream fixes the selected manifest, config and all layer identities before
// returning an input. It opens each blob only when flatten applies that layer,
// and never stages the full compressed image. Each opener requests the same
// digest again if a caller rebuilds in a fresh scratch tree. Pull retains its
// complete-download and reopenable-cache semantics for existing callers.
func (c *Config) Stream(ctx context.Context, res *Resolved) (*flatten.ImageInput, error) {
	if res == nil {
		return nil, fmt.Errorf("remote: nil resolved image")
	}
	var config []byte
	var manifest v1.Manifest
	err := c.retryPull(ctx, "stream metadata "+res.Digest.String(), metadataRetryDelay, func() error {
		// A new image on each attempt avoids ggcr memoizing partial config
		// bytes after a failed read. The immutable digest is never re-resolved.
		img, err := ggcrremote.Image(res.Digest, c.metadataOpts(ctx)...)
		if err != nil {
			return err
		}
		raw, err := img.RawManifest()
		if err != nil {
			return err
		}
		if len(raw) > 16<<20 {
			return fmt.Errorf("remote: image manifest exceeds 16 MiB")
		}
		if got := fmt.Sprintf("sha256:%x", sha256.Sum256(raw)); got != res.Digest.DigestStr() {
			return fmt.Errorf("remote: manifest digest mismatch: got %s", got)
		}
		if err = json.Unmarshal(raw, &manifest); err != nil {
			return err
		}
		if manifest.Config.Size < 0 || manifest.Config.Size > 16<<20 {
			return fmt.Errorf("remote: invalid image config size %d", manifest.Config.Size)
		}
		config, err = img.RawConfigFile()
		if err != nil {
			return err
		}
		if int64(len(config)) != manifest.Config.Size {
			return fmt.Errorf("remote: image config size mismatch")
		}
		if got := fmt.Sprintf("sha256:%x", sha256.Sum256(config)); got != manifest.Config.Digest.String() {
			return fmt.Errorf("remote: image config digest mismatch: got %s", got)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	var cfg v1.ConfigFile
	if err := json.NewDecoder(bytes.NewReader(config)).Decode(&cfg); err != nil {
		return nil, err
	}
	// Variant is optional in an OCI config; Resolve already selected the
	// index descriptor. Reject a stated conflict, not an omitted field.
	if cfg.OS != c.platform.OS || cfg.Architecture != c.platform.Architecture || c.platform.Variant != "" && cfg.Variant != "" && cfg.Variant != c.platform.Variant {
		return nil, fmt.Errorf("remote: image config platform %s/%s/%s does not match selected %s", cfg.OS, cfg.Architecture, cfg.Variant, c.platform)
	}
	if len(manifest.Layers) != len(cfg.RootFS.DiffIDs) {
		return nil, fmt.Errorf("remote: layer descriptors and DiffIDs differ in count")
	}
	layers := make([]flatten.LayerInput, len(manifest.Layers))
	for i, desc := range manifest.Layers {
		ref := res.Repo.Digest(desc.Digest.String())
		layers[i] = flatten.LayerInput{MediaType: string(desc.MediaType), Size: desc.Size, Digest: desc.Digest.String(), DiffID: cfg.RootFS.DiffIDs[i].String(),
			Open: func(ctx context.Context) (io.ReadCloser, error) {
				blob, err := ggcrremote.Layer(ref, c.remoteOpts(ctx)...)
				if err != nil {
					return nil, err
				}
				return blob.Compressed()
			}}
	}
	return flatten.NewImageInput(config, layers)
}
