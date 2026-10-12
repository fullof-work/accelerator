//go:build linux

package flatten

import (
	"archive/tar"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"strings"
	"sync"
)

type archiveMember struct {
	offset, size int64
	alias        string // validated legacy Skopeo alias, never followed
}
type archiveInput struct {
	reader  io.ReaderAt
	size    int64
	file    *os.File
	id      fileIdentity
	members map[string]archiveMember
}

type contextReader struct {
	ctx context.Context
	r   io.Reader
}

func (r contextReader) Read(b []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	return r.r.Read(b)
}

// PrepareArchive consumes and closes input exactly once, including on failure.
// Close must unblock a concurrent Read. A seekable ReaderAt is indexed in place;
// otherwise one bounded outer archive is spooled. No inner layer is extracted
// into a second staging directory. Input/spool resources are released after all
// layers validate, before native planning; the resulting rootfs belongs to the
// returned PreparedImage.
func PrepareArchive(ctx context.Context, input io.ReadCloser, opts Options) (_ *PreparedImage, retErr error) {
	if input == nil {
		return nil, fmt.Errorf("flatten: nil archive input")
	}
	return prepareArchive(ctx, input, opts, input.Close)
}

func prepareArchive(ctx context.Context, input io.Reader, opts Options, closeBody func() error) (_ *PreparedImage, retErr error) {
	if input == nil {
		return nil, fmt.Errorf("flatten: nil archive input")
	}
	var closeOnce sync.Once
	var closeErr error
	closeInput := func() error {
		closeOnce.Do(func() {
			if closeBody != nil {
				closeErr = closeBody()
			}
		})
		return closeErr
	}
	cancelDone := make(chan struct{})
	stop := context.AfterFunc(ctx, func() { closeInput(); close(cancelDone) })
	var finishOnce sync.Once
	var finishErr error
	finishInput := func() error {
		finishOnce.Do(func() {
			if !stop() {
				<-cancelDone
			}
			finishErr = errors.Join(closeInput(), ctx.Err())
		})
		return finishErr
	}
	var spool *os.File
	var spoolName string
	var cleanupOnce sync.Once
	var cleanupErr error
	cleanup := func() error {
		cleanupOnce.Do(func() {
			cleanupErr = finishInput()
			if spool != nil {
				cleanupErr = errors.Join(cleanupErr, spool.Close())
			}
			if spoolName != "" {
				cleanupErr = errors.Join(cleanupErr, os.Remove(spoolName))
			}
		})
		return cleanupErr
	}
	defer func() { retErr = errors.Join(retErr, cleanup()) }()
	limits, err := opts.ArchiveLimits.defaults()
	if err != nil {
		return nil, err
	}
	if err = ctx.Err(); err != nil {
		return nil, err
	}
	a := &archiveInput{}
	if rs, ok := input.(interface {
		io.ReaderAt
		io.Seeker
	}); ok {
		start, e := rs.Seek(0, io.SeekCurrent)
		if e == nil {
			end, e := rs.Seek(0, io.SeekEnd)
			if e != nil {
				return nil, e
			}
			if _, e = rs.Seek(start, io.SeekStart); e != nil {
				return nil, e
			}
			if start < 0 || end < start || end-start > limits.Bytes {
				return nil, fmt.Errorf("flatten: archive length exceeds limit")
			}
			a.reader, a.size = io.NewSectionReader(rs, start, end-start), end-start
			if f, ok := input.(*os.File); ok {
				a.file = f
				a.id, e = fileID(f)
				if e != nil {
					return nil, e
				}
			}
		}
	}
	if a.reader == nil {
		spool, err = os.CreateTemp(opts.TmpDir, "flatten-archive-*.spool")
		if err != nil {
			return nil, err
		}
		spoolName = spool.Name()
		n, e := io.CopyBuffer(spool, io.LimitReader(contextReader{ctx, input}, limits.Bytes+1), make([]byte, 1<<20))
		if e != nil {
			return nil, e
		}
		if n > limits.Bytes {
			return nil, fmt.Errorf("flatten: archive spool exceeds byte limit")
		}
		if e = finishInput(); e != nil {
			return nil, e
		}
		a.reader, a.size, a.file = spool, n, spool
		a.id, err = fileID(spool)
		if err != nil {
			return nil, err
		}
	}
	if err = a.index(ctx, limits); err != nil {
		return nil, err
	}
	in, err := a.image(ctx)
	if err != nil {
		return nil, err
	}
	return prepareImage(ctx, in, opts, func() error { return errors.Join(a.stable(), cleanup()) })
}

func (a *archiveInput) stable() error {
	if a.file == nil {
		return nil
	}
	id, err := fileID(a.file)
	if err != nil {
		return err
	}
	if id != a.id {
		return fmt.Errorf("flatten: outer archive changed")
	}
	return nil
}

func archivePath(s string) (string, error) {
	for strings.HasPrefix(s, "./") {
		s = strings.TrimPrefix(s, "./")
	}
	s = strings.TrimSuffix(s, "/")
	if len(s) > 4096 || !validPlanPath(s) {
		return "", fmt.Errorf("flatten: invalid archive member %q", s)
	}
	return s, nil
}

func (a *archiveInput) index(ctx context.Context, limits ArchiveLimits) error {
	a.members = make(map[string]archiveMember)
	r := io.NewSectionReader(a.reader, 0, a.size)
	tr := tar.NewReader(r)
	remaining := limits.IndexBytes - 8<<20 // bounded tar extended-header parser
	var lastEnd int64
	for count := 0; ; count++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		h, err := tr.Next()
		if err == io.EOF {
			end, e := r.Seek(0, io.SeekCurrent)
			if e != nil {
				return e
			}
			if end < lastEnd+1024 {
				return fmt.Errorf("flatten: outer archive is missing its complete end marker")
			}
			// Docker-save ends with two zero blocks and optional zero padding.
			// Do not silently accept a second archive or nonzero trailing data.
			padding := io.NewSectionReader(a.reader, end-1024, a.size-end+1024)
			buf := make([]byte, 64<<10)
			for {
				if err := ctx.Err(); err != nil {
					return err
				}
				n, e := padding.Read(buf)
				for _, b := range buf[:n] {
					if b != 0 {
						return fmt.Errorf("flatten: nonzero outer archive trailer")
					}
				}
				if e == io.EOF {
					break
				}
				if e != nil {
					return e
				}
			}
			break
		}
		if err != nil {
			return fmt.Errorf("flatten: outer archive: %w", err)
		}
		if count >= limits.Members {
			return fmt.Errorf("flatten: archive member limit exceeded")
		}
		if (h.Name == "." || h.Name == "./") && h.Typeflag == tar.TypeDir && h.Size == 0 {
			lastEnd, err = r.Seek(0, io.SeekCurrent)
			if err != nil {
				return err
			}
			continue
		}
		name, err := archivePath(h.Name)
		if err != nil {
			return err
		}
		if _, ok := a.members[name]; ok {
			return fmt.Errorf("flatten: duplicate archive member %q", name)
		}
		charge := int64(256 + 2*len(name) + 2*len(h.Linkname))
		if charge > remaining {
			return fmt.Errorf("flatten: archive index budget exceeded")
		}
		remaining -= charge
		if h.Typeflag != tar.TypeReg && h.Typeflag != tar.TypeDir && h.Typeflag != tar.TypeSymlink {
			return fmt.Errorf("flatten: unsupported outer archive member type for %q", name)
		}
		for k := range h.PAXRecords {
			if strings.HasPrefix(k, "GNU.sparse.") {
				return fmt.Errorf("flatten: sparse outer archive members are unsupported")
			}
		}
		off, err := r.Seek(0, io.SeekCurrent)
		if err != nil {
			return err
		}
		if h.Size < 0 || off < 0 || off > a.size || h.Size > a.size-off || (512-h.Size%512)%512 > a.size-off-h.Size {
			return fmt.Errorf("flatten: truncated archive member %q", name)
		}
		lastEnd = off + h.Size + (512-h.Size%512)%512
		if h.Typeflag == tar.TypeSymlink {
			// Match the platform's existing Docker-save compatibility contract:
			// Skopeo may include <hex>/layer.tar -> ../<diff-id>.tar alongside
			// the manifest-selected regular layer. Do not follow arbitrary links.
			parts := strings.Split(name, "/")
			target := strings.TrimPrefix(h.Linkname, "../")
			if h.Size != 0 || len(parts) != 2 || parts[1] != "layer.tar" ||
				!sha256Identity("sha256:"+parts[0]) || h.Linkname != "../"+target ||
				!strings.HasSuffix(target, ".tar") || !sha256Identity("sha256:"+strings.TrimSuffix(target, ".tar")) {
				return fmt.Errorf("flatten: unsupported outer archive link %q", name)
			}
			a.members[name] = archiveMember{offset: -1, alias: target}
		} else if h.Typeflag == tar.TypeDir {
			if h.Size != 0 {
				return fmt.Errorf("flatten: outer archive directory has data")
			}
			a.members[name] = archiveMember{offset: -1}
		} else {
			a.members[name] = archiveMember{offset: off, size: h.Size}
		}
	}
	return a.stable()
}

func (a *archiveInput) readMember(name string, maxBytes int64) ([]byte, error) {
	name, err := archivePath(name)
	if err != nil {
		return nil, err
	}
	m, ok := a.members[name]
	if !ok || m.offset < 0 {
		return nil, fmt.Errorf("flatten: missing archive member %q", name)
	}
	if m.size > maxBytes {
		return nil, fmt.Errorf("flatten: archive metadata member exceeds limit")
	}
	return io.ReadAll(io.NewSectionReader(a.reader, m.offset, m.size))
}

func (a *archiveInput) image(ctx context.Context) (*ImageInput, error) {
	b, err := a.readMember("manifest.json", 16<<20)
	if err != nil {
		return nil, err
	}
	var entries []dockerManifestEntry
	if err = json.Unmarshal(b, &entries); err != nil {
		return nil, err
	}
	if len(entries) != 1 {
		return nil, fmt.Errorf("flatten: Docker-save requires exactly one image, got %d", len(entries))
	}
	entry := entries[0]
	config, err := a.readMember(entry.Config, maxImageConfigBytes)
	if err != nil {
		return nil, err
	}
	var cfg struct {
		RootFS struct {
			DiffIDs []string `json:"diff_ids"`
		} `json:"rootfs"`
	}
	if err = json.Unmarshal(config, &cfg); err != nil {
		return nil, err
	}
	if len(entry.Layers) != len(cfg.RootFS.DiffIDs) || len(entry.Layers) > maxImageLayers {
		return nil, fmt.Errorf("flatten: archive layer/DiffID count mismatch")
	}
	layers := make([]LayerInput, len(entry.Layers))
	selected := make(map[string]bool, len(entry.Layers))
	for i, name := range entry.Layers {
		name, err = archivePath(name)
		if err != nil {
			return nil, err
		}
		m, ok := a.members[name]
		if !ok || m.offset < 0 {
			return nil, fmt.Errorf("flatten: missing archive layer %q", name)
		}
		selected[name] = true
		media := "application/vnd.oci.image.layer.v1.tar"
		// Docker-save has no layer media-type field. Retain its established
		// gzip archive compatibility (including ggcr's .tar.gz dialect), and
		// assign an explicit decoder type once, before opening any layer.
		var magic [4]byte
		if _, err = io.ReadFull(io.NewSectionReader(a.reader, m.offset, m.size), magic[:]); err != nil {
			return nil, err
		}
		if bytes.Equal(magic[:2], []byte{0x1f, 0x8b}) {
			media += "+gzip"
		} else if bytes.Equal(magic[:], []byte{0x28, 0xb5, 0x2f, 0xfd}) {
			media += "+zstd"
		}
		digest := cfg.RootFS.DiffIDs[i]
		if media != "application/vnd.oci.image.layer.v1.tar" {
			// Unlike a Registry descriptor, legacy save manifests do not name
			// the compressed identity. Compute it from the stable indexed member
			// without extracting/staging it; application checks it again.
			h := sha256.New()
			if _, err = io.CopyBuffer(h, contextReader{ctx, io.NewSectionReader(a.reader, m.offset, m.size)}, make([]byte, 64<<10)); err != nil {
				return nil, err
			}
			digest = fmt.Sprintf("sha256:%x", h.Sum(nil))
		}
		layers[i] = LayerInput{MediaType: media, Digest: digest, DiffID: cfg.RootFS.DiffIDs[i], Size: m.size, Open: func(ctx context.Context) (io.ReadCloser, error) {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			if err := a.stable(); err != nil {
				return nil, err
			}
			return io.NopCloser(io.NewSectionReader(a.reader, m.offset, m.size)), nil
		}}
	}
	for name, m := range a.members {
		if m.alias != "" && !selected[m.alias] {
			return nil, fmt.Errorf("flatten: archive alias %q does not name a selected regular layer", name)
		}
	}
	if err = a.stable(); err != nil {
		return nil, err
	}
	// Standard Docker-save config names are content identities. Verify them
	// when present; non-content-addressed legacy names remain supported.
	base := strings.TrimSuffix(path.Base(entry.Config), ".json")
	if sha256Identity("sha256:"+base) && fmt.Sprintf("%x", sha256.Sum256(config)) != base {
		return nil, fmt.Errorf("flatten: archive config digest mismatch")
	}
	return NewImageInput(config, layers)
}
