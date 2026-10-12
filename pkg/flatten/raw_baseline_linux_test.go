//go:build linux

package flatten

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

// This is the previous mkfs invocation, retained only as a byte-identity
// regression oracle. Production export never creates its private data blob.
// buildEROFS invokes mkfs.erofs to produce a deterministic, dedup-friendly EROFS image.
// Uses chunk-based layout (-Ededupe --chunksize=4096) to minimize metadata size and
// stabilize data block offsets across images, maximizing CDC cross-image dedup.
// No compression: raw bytes enable CDC dedup (consistent with kuasar-sandbox.md §5 design matrix).
func buildEROFS(mkfsPath, rootfsDir, outputPath string, extra ...string) error {
	args := []string{
		"-Ededupe",         // intra-image file dedup
		"--chunksize=4096", // chunk-based layout: metadata 7.6MiB→0.6MiB
		// NOTE: no --all-root. The image's real uid/gid/mode is preserved by
		// the layer applier (from the layer tar headers) so a non-root
		// guest user can write to its home; mkfs.erofs records that ownership.
		// Determinism holds: ownership comes from the (fixed) image layers, and
		// flatten runs as root in the sandbox-builder unit. (The runtime erofs —
		// sandbox-init, all root — is built by separate shell scripts that keep
		// --all-root; this Go path is image-flatten only.)
		"-T0",                                        // fixed timestamp (epoch)
		"-b4096",                                     // 4K block size
		"-x-1",                                       // disable xattrs
		"-U", "00000000-0000-0000-0000-000000000000", // fixed UUID
		"--quiet",
	}
	args = append(args, extra...)
	args = append(args, outputPath, rootfsDir)
	cmd := exec.Command(mkfsPath, args...)
	// Chunk-mode mkfs.erofs stages dedup data through tmpfile()/$TMPDIR
	// (default /tmp). Pin it to the output's directory — the flatten scratch
	// — so the build works on rootfs without a /tmp (the in-guest import
	// sandbox boots an EMPTY ext4) and the staging lands on the disk sized
	// for it.
	cmd.Env = append(os.Environ(), "TMPDIR="+filepath.Dir(outputPath))
	cmd.Stdout = io.Discard
	// Capture stderr so a failure carries mkfs.erofs's own diagnostic rather
	// than a bare "exit status 1". (On success mkfs may still print benign
	// notes to stderr — e.g. the "Compression is not enabled" hint — which we
	// ignore.)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		if msg := strings.TrimSpace(stderr.String()); msg != "" {
			return fmt.Errorf("flatten: mkfs.erofs: %w: %s", err, msg)
		}
		return fmt.Errorf("flatten: mkfs.erofs: %w", err)
	}
	return nil
}
