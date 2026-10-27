package tarstream

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"io"
	"strings"
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
		{name: "dense", body: []byte("hello"), wantLen: 5632, wantArtifact: "d91e36eb67f87fc0a059c67b57e2139603c64485f7413560dcd05f180e26d627", wantDigest: "8a9ec06ea9f3218f043b8dae59191c883b3a06d31d163d0cd25c91b2bc930939"},
		{name: "empty", wantLen: 5120, wantArtifact: "df79d50ffb781c4eb108b704bc88bf703d0552fa6c5661d6721b584818f2e2ed", wantDigest: "af1b0d75a63812f2bc96e4d14a45844b1770ba3db5e69009fade484410ea8bbc"},
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

// These fixed compressed fixtures retain the pre-presence empty and byte-sized
// dense writer goldens. They are only decoded for reader-compatibility tests;
// tests must not recreate a production path for writing the legacy format.
func legacyPlaintextArtifact(t *testing.T, name string) []byte {
	t.Helper()
	var encoded string
	switch name {
	case "empty":
		encoded = "H4sIAAAAAAACAwtIrPBITUxJLSrWM9BPzS0oqWSgOjAAAjMTEzANBOg0UBKJDRI3NDQ2NGFQqGCgAygtLkksAlrPMDKBkamCd6hjsGOQXkFiZU5+YopecWZVqq0Bl6GJQkFiSYYtOE1wWSpAhRlGwbACNMrypOV/DLa5ASj/G4zmf5oDvezSxOLEIr2UzPTU4hK9xDTDJIMUc9NEM2MLQ6M0o6RkS7NUkxRDk0QTUwsTkyRDc3ODpETjlCTTVDNLAwPLNGDVYQKUMDRITbRISkrGGf8m+OLfFC3+jQxNDQxH45+eIDo+mVFGmpNruWbnrOyrP1dPExZ/uLjOL3nJwh7vCYGZcaMl5SgYBaNgFAwvAAABfo/tAA4AAA=="
	case "dense":
		encoded = "H4sIAAAAAAACAwtIrPBITUxJLSrWM9BPSc0rTmWgOjAAAjMTEzANBOg0UBKJDRI3NDQyNWFQqGCgAygtLkksAlrPMDKBkamCd6hjsGOQXkFiZU5+YopecWZVqq0pl6GJQkFiSYYtOE1wWSpAhRlGwbACNMrypOV/A1PU/G9gZm5myKBgMJr/aQ4yUnNy8kezwYgFetmlicWJRXopmempxSV6FomWqckGZqmJlmnGRoYWaQYmxkkWKYmpppaGlobJFhbGScaJBmYpxoYphmbGKQbJKUamyZaGSUZJyZbGBpbGljjzvwm+/G+KVv8bGRqZm4/mfzoCVn2DRee2fnLp2lTVw5v0MWbqki/L9/0UmLB8fUnyi7LLm06P5pRRMApGwSgYXgAA+/5J9QAQAAA="
	default:
		t.Fatalf("unknown legacy golden %q", name)
	}
	r, err := gzip.NewReader(base64.NewDecoder(base64.StdEncoding, strings.NewReader(encoded)))
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	artifact, err := io.ReadAll(r)
	if err != nil {
		t.Fatal(err)
	}
	return artifact
}

func TestLegacyPlaintextGoldenReadCompatibility(t *testing.T) {
	for _, tc := range []struct {
		name, body, artifactHash, digest string
		size                             int
	}{
		{"empty", "", "5991713edcaa3c5f133db162ceffa14bec1352f5fcd838846ccfea0b25260d87", "af1b0d75a63812f2bc96e4d14a45844b1770ba3db5e69009fade484410ea8bbc", 3584},
		{"dense", "hello", "f1d5e23dc117b29dc95c3d44cd68d67b12a90c24799edd48e41f64c3246bf927", "8a9ec06ea9f3218f043b8dae59191c883b3a06d31d163d0cd25c91b2bc930939", 4096},
	} {
		t.Run(tc.name, func(t *testing.T) {
			artifact := legacyPlaintextArtifact(t, tc.name)
			if len(artifact) != tc.size || fmt.Sprintf("%x", sha256.Sum256(artifact)) != tc.artifactHash {
				t.Fatal("legacy golden bytes changed")
			}
			for _, sequential := range []bool{false, true} {
				var source sparse.Source
				var name string
				var err error
				if sequential {
					source, name, err = SourceFrom(readerOnly{bytes.NewReader(artifact)}, "", WithExpectedDigest(DigestScheme, tc.digest))
				} else {
					source, name, err = SourceAt(bytes.NewReader(artifact), int64(len(artifact)), "", WithExpectedDigest(DigestScheme, tc.digest))
				}
				if err != nil || name != tc.name || source.Size() != uint64(len(tc.body)) {
					t.Fatalf("legacy source %t: %q, %v", sequential, name, err)
				}
				got := make([]byte, len(tc.body))
				if len(got) != 0 {
					if n, err := source.ReadAt(context.Background(), got, 0); n != len(got) || err != nil {
						t.Fatalf("legacy read %t: %d, %v", sequential, n, err)
					}
				}
				if string(got) != tc.body {
					t.Fatalf("legacy body %t: %q", sequential, got)
				}
			}
		})
	}
}
