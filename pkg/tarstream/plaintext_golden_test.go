package tarstream

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"testing"

	"github.com/kuasar-sandbox/accelerator/pkg/sparse"
)

func TestPlaintextWriteGolden(t *testing.T) {
	var sparseBody [16384]byte
	copy(sparseBody[:], "HEAD")
	copy(sparseBody[12288:], "TAIL")
	tests := []struct {
		name         string
		body         []byte
		holes        []sparse.Extent
		wantLen      int
		wantArtifact string
		wantDigest   string
	}{
		{name: "dense", body: []byte("hello"), wantLen: 4096, wantArtifact: "f1d5e23dc117b29dc95c3d44cd68d67b12a90c24799edd48e41f64c3246bf927", wantDigest: "8a9ec06ea9f3218f043b8dae59191c883b3a06d31d163d0cd25c91b2bc930939"},
		{name: "empty", wantLen: 3584, wantArtifact: "5991713edcaa3c5f133db162ceffa14bec1352f5fcd838846ccfea0b25260d87", wantDigest: "af1b0d75a63812f2bc96e4d14a45844b1770ba3db5e69009fade484410ea8bbc"},
		{name: "hole", body: make([]byte, 8192), holes: []sparse.Extent{{Offset: 0, Size: 8192}}, wantLen: 5120, wantArtifact: "e5ec4ce4b7c8e60998cf58ad35e006c3b0bac4323dd2b52605b3c79559dee228", wantDigest: "036c9ca07fd3de9a1131b8787140038cf7d6f6eb2d2b4bbc9cfb765c45d8fe4c"},
		{name: "sparse/image", body: sparseBody[:], holes: []sparse.Extent{{Offset: 4096, Size: 8192}}, wantLen: 13312, wantArtifact: "d3125e8fe871236fd9ed948f6f61b1f1bf66828656dc149f2b2c0677486cfb6c", wantDigest: "e342a173270c8d2119b0ef290bc52d2fc092bdeb07eda5691a01d1f54ea94125"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			source, err := sparse.NewSource(bytes.NewReader(tc.body), uint64(len(tc.body)), tc.holes)
			if err != nil {
				t.Fatal(err)
			}
			var artifact bytes.Buffer
			scheme, digest, err := WriteTo(context.Background(), &artifact, tc.name, source)
			if err != nil {
				t.Fatal(err)
			}
			if scheme != DigestScheme || digest != tc.wantDigest {
				t.Fatalf("digest = %s:%s, want digest:%s", scheme, digest, tc.wantDigest)
			}
			if artifact.Len() != tc.wantLen {
				t.Fatalf("artifact length = %d, want %d", artifact.Len(), tc.wantLen)
			}
			if got := fmt.Sprintf("%x", sha256.Sum256(artifact.Bytes())); got != tc.wantArtifact {
				t.Fatalf("artifact SHA-256 = %s, want %s", got, tc.wantArtifact)
			}
		})
	}
}
