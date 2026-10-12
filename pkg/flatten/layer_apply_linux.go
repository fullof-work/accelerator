//go:build linux

package flatten

import (
	"archive/tar"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"path"
	"path/filepath"
	"strings"

	securejoin "github.com/cyphar/filepath-securejoin"
	"github.com/opencontainers/umoci/oci/layer"
	"golang.org/x/sys/unix"
)

// applyVerifiedLayer owns the reader until the synchronous applier, decoder and
// Close have all returned. On any error the entire private rootfs is invalid;
// callers must discard it, never replay a layer into this half-applied tree.
func applyVerifiedLayer(ctx context.Context, in LayerInput, root string, limits LayerLimits) (err error) {
	limits, err = limits.normalized()
	if err != nil {
		return err
	}
	s, err := openLayer(ctx, in, limits)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, s.Close()) }()
	if err = unpackLayer(ctx, s, root, limits.MetadataBytes); err != nil {
		return err
	}
	return s.finish()
}

// unpackLayer is the UnpackLayer loop using umoci's public TarExtractor API.
// Keeping the loop here permits accounting for its path table before each
// UnpackEntry and checking xattrs that umoci can silently omit in a userns or on
// filesystems without xattr support. All filesystem application semantics stay
// in umoci; no CLI, CAS, bundle or rootless ownership mapping is used.
func unpackLayer(ctx context.Context, r io.Reader, root string, budget int64) error {
	// archive/tar bounds each extended-header body to 1 MiB. Reserve space
	// for parsing that body, its strings/maps and the extractor itself before
	// asking for a header, rather than accounting only after allocation.
	if budget < 8<<20 {
		return errors.New("flatten: layer metadata budget must be at least 8 MiB")
	}
	budget -= 8 << 20
	te := layer.NewTarExtractor(&layer.UnpackOptions{OnDiskFormat: layer.DirRootfs{}})
	tr := tar.NewReader(r)
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		hdr, err := tr.Next()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return fmt.Errorf("flatten: tar header: %w", err)
		}
		if err := layerPath(hdr.Name); err != nil {
			return err
		}
		if hdr.Typeflag == tar.TypeLink {
			if err := layerPath(hdr.Linkname); err != nil {
				return err
			}
		}
		// Charge every entry and ancestor, including duplicates. This is a
		// conservative bound on umoci's upperPaths and temporary path storage,
		// without building another full path set in the adapter.
		charge := int64(2048 + len(hdr.Name) + len(hdr.Linkname))
		for p := path.Clean(hdr.Name); p != "."; p = path.Dir(p) {
			charge += int64(256 + 2*len(p))
		}
		for k, v := range hdr.PAXRecords {
			charge += int64(256 + 2*len(k) + 2*len(v))
		}
		if charge > budget {
			return errors.New("flatten: layer metadata budget exceeded")
		}
		budget -= charge
		if err := te.UnpackEntry(root, hdr, tr); err != nil {
			return fmt.Errorf("flatten: umoci unpack %q: %w", hdr.Name, err)
		}
		if err := checkLayerXattrs(root, hdr); err != nil {
			return err
		}
	}
}

func layerPath(name string) error {
	if name == "" || len(name) > 4096 || strings.HasPrefix(name, "/") || strings.IndexByte(name, 0) >= 0 {
		return fmt.Errorf("flatten: invalid layer path %q", name)
	}
	for _, part := range strings.Split(name, "/") {
		if part == ".." {
			return fmt.Errorf("flatten: parent traversal in layer path %q", name)
		}
	}
	return nil
}

func checkLayerXattrs(root string, hdr *tar.Header) error {
	if len(hdr.Xattrs) == 0 || strings.HasPrefix(path.Base(hdr.Name), ".wh.") {
		return nil
	}
	name := path.Clean(hdr.Name)
	target := root
	if name != "." {
		parent, err := securejoin.SecureJoin(root, path.Dir(name))
		if err != nil {
			return err
		}
		target = filepath.Join(parent, path.Base(name))
	}
	for name, want := range hdr.Xattrs {
		// Linux xattr values are at most 64 KiB. Read exactly the expected
		// size, so a different/missing value or a library omission fails.
		if len(want) > 65536 {
			return fmt.Errorf("flatten: oversized xattr %q on %q", name, hdr.Name)
		}
		got := make([]byte, len(want)+1)
		n, err := unix.Lgetxattr(target, name, got)
		if err != nil {
			return fmt.Errorf("flatten: xattr %q on %q was not preserved: %w", name, hdr.Name, err)
		}
		if !bytes.Equal(got[:n], []byte(want)) {
			return fmt.Errorf("flatten: xattr %q on %q was not preserved", name, hdr.Name)
		}
	}
	return nil
}
