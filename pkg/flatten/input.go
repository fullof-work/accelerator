package flatten

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/kuasar-sandbox/accelerator/pkg/image"
)

const maxImageConfigBytes = 16 << 20
const maxImageLayers = 65536

// ImageInput is a fixed image config and ordered, reopenable layer identities.
// NewImageInput checks the complete descriptor/DiffID correspondence before any
// expensive work. Callers may reuse it to rebuild from a new private rootfs
// after failure. It contains no open bodies and owns no persistent cache.
type ImageInput struct {
	config []byte
	layers []LayerInput
}

func NewImageInput(config []byte, layers []LayerInput) (*ImageInput, error) {
	diffIDs, err := imageDiffIDs(config, len(layers))
	if err != nil {
		return nil, err
	}
	for i, l := range layers {
		if err := l.validate(); err != nil {
			return nil, fmt.Errorf("flatten: layer %d: %w", i, err)
		}
		if diffIDs[i] != l.DiffID {
			return nil, fmt.Errorf("flatten: layer %d DiffID does not match image config", i)
		}
	}
	return &ImageInput{config: bytes.Clone(config), layers: append([]LayerInput(nil), layers...)}, nil
}

func imageDiffIDs(config []byte, count int) ([]string, error) {
	if len(config) == 0 || len(config) > maxImageConfigBytes || count > maxImageLayers {
		return nil, errors.New("flatten: image config or layer count exceeds limits")
	}
	var cfg struct {
		RootFS struct {
			Type    string   `json:"type"`
			DiffIDs []string `json:"diff_ids"`
		} `json:"rootfs"`
	}
	if err := json.Unmarshal(config, &cfg); err != nil {
		return nil, fmt.Errorf("flatten: image config: %w", err)
	}
	if cfg.RootFS.Type != "layers" || len(cfg.RootFS.DiffIDs) != count {
		return nil, errors.New("flatten: image config rootfs does not match layer descriptors")
	}
	if _, err := image.ExtractRuntimeConfigFromJSON(config); err != nil {
		return nil, fmt.Errorf("flatten: runtime config: %w", err)
	}
	for i, id := range cfg.RootFS.DiffIDs {
		if !sha256Identity(id) {
			return nil, fmt.Errorf("flatten: layer %d has an invalid DiffID", i)
		}
	}
	return cfg.RootFS.DiffIDs, nil
}

// ConfigJSON returns a copy of the fixed OCI config.
func (in *ImageInput) ConfigJSON() []byte { return bytes.Clone(in.config) }

// Layers returns a copy of the ordered descriptors and their reopenable input
// functions, for adapters that add an explicit persistent cache.
func (in *ImageInput) Layers() []LayerInput { return append([]LayerInput(nil), in.layers...) }
