//go:build linux

package flatten

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/kuasar-sandbox/accelerator/pkg/image"
	"github.com/kuasar-sandbox/accelerator/pkg/sparse"
	"github.com/kuasar-sandbox/accelerator/pkg/tailzip"
	"github.com/kuasar-sandbox/accelerator/pkg/tarstream"
)

// PreparedImage retains the stable source, layout and open plan files until
// Close. Writes are serialized and may be repeated while the source remains
// stable. Close cancels an active write, waits for it to return, then closes
// descriptors and removes only the scratch directory owned by this object.
// The caller owns the output writer; its Write must return for Close to finish.
type PreparedImage struct {
	mu           sync.Mutex
	writeMu      sync.Mutex
	closed       bool
	cancel       context.CancelFunc
	closeOnce    sync.Once
	closeErr     error
	work         string
	plan         *erofsPlan
	source       sparse.Source
	selection    *sourceSelection
	writerMemory uint64
}

// PrepareDir plans an existing directory without modifying or copying it.
// The caller must stop all writers before this call and keep the selection
// stable until Close. Validation detects changes; it cannot create an atomic
// snapshot of a concurrently changing directory.
func PrepareDir(ctx context.Context, root string, opts Options, dopts DirOptions) (_ *PreparedImage, retErr error) {
	limits, err := opts.PlanLimits.defaults()
	if err != nil {
		return nil, err
	}
	if limits.IndexMemoryBytes < 16<<20 {
		return nil, fmt.Errorf("flatten: preparation requires at least 16 MiB index budget")
	}
	if err = ctx.Err(); err != nil {
		return nil, err
	}
	abs, err := filepath.Abs(root)
	if err != nil {
		return nil, err
	}
	abs, err = filepath.EvalSymlinks(abs)
	if err != nil {
		return nil, err
	}
	if abs == "/" && !dopts.SkipMounts {
		return nil, fmt.Errorf("flatten: exporting / requires --skip-mounts")
	}
	var excludes []string
	for _, p := range dopts.Skip {
		rel, e := normalizeSkip(p)
		if e != nil {
			return nil, e
		}
		excludes = append(excludes, rel)
	}
	if dopts.SkipMounts {
		mounts, e := mountsUnder(abs)
		if e != nil {
			return nil, e
		}
		excludes = append(excludes, mounts...)
	}
	config := dopts.ConfigJSON
	if config == nil {
		config = []byte("{}")
	}
	if len(config) > maxImageConfigBytes {
		return nil, fmt.Errorf("flatten: runtime config exceeds limit")
	}
	wrapped, err := image.WrapRuntimeConfigJSON(config)
	if err != nil {
		return nil, err
	}
	cfg, err := image.ExtractRuntimeConfigFromJSON(wrapped)
	if err != nil {
		return nil, err
	}
	work, err := os.MkdirTemp(opts.TmpDir, "flatten-prepared-*")
	if err != nil {
		return nil, err
	}
	defer func() {
		if retErr != nil {
			retErr = errors.Join(retErr, removeOwnedWork(work))
		}
	}()
	owned := append(append([]string(nil), dopts.OutputPaths...), work)
	selection, err := selectSourceWithOwned(ctx, abs, excludes, owned, dopts.OutputFiles, limits.IndexMemoryBytes/4)
	if err != nil {
		return nil, err
	}
	if dopts.Warnf != nil {
		for _, p := range selection.excludes {
			dopts.Warnf("flatten: excluding source path %s", p)
		}
	}
	return prepareSelected(ctx, work, selection, cfg, opts, limits)
}

// PrepareImage verifies and applies each complete layer into one private rootfs.
// On failure the whole rootfs is discarded after the applier/body/owned planning work
// has stopped. Reusing the immutable ImageInput starts from a fresh rootfs.
func PrepareImage(ctx context.Context, in *ImageInput, opts Options) (_ *PreparedImage, retErr error) {
	return prepareImage(ctx, in, opts, nil)
}

func prepareImage(ctx context.Context, in *ImageInput, opts Options, afterApply func() error) (_ *PreparedImage, retErr error) {
	if in == nil {
		return nil, fmt.Errorf("flatten: nil image input")
	}
	return prepareLayers(ctx, in.config, len(in.layers), opts, func(i int, root string) error {
		return applyVerifiedLayer(ctx, in.layers[i], root, opts.LayerLimits)
	}, afterApply)
}

func prepareLayers(ctx context.Context, config []byte, count int, opts Options, apply func(int, string) error, afterApply func() error) (_ *PreparedImage, retErr error) {
	cfg, err := image.ExtractRuntimeConfigFromJSON(config)
	if err != nil {
		return nil, err
	}
	limits, err := opts.PlanLimits.defaults()
	if err != nil {
		return nil, err
	}
	if limits.IndexMemoryBytes < 16<<20 {
		return nil, fmt.Errorf("flatten: preparation requires at least 16 MiB index budget")
	}
	if _, err = opts.LayerLimits.normalized(); err != nil {
		return nil, err
	}
	if err = ctx.Err(); err != nil {
		return nil, err
	}
	work, err := os.MkdirTemp(opts.TmpDir, "flatten-prepared-*")
	if err != nil {
		return nil, err
	}
	defer func() {
		if retErr != nil {
			retErr = errors.Join(retErr, removeOwnedWork(work))
		}
	}()
	root := filepath.Join(work, "rootfs")
	if err = os.Mkdir(root, 0755); err != nil {
		return nil, err
	}
	for i := 0; i < count; i++ {
		if opts.Progress != nil {
			opts.Progress("apply-layers", i+1, count)
		}
		if err = apply(i, root); err != nil {
			return nil, fmt.Errorf("flatten: layer %d: %w", i, err)
		}
	}
	if afterApply != nil {
		if err = afterApply(); err != nil {
			return nil, err
		}
	}
	selection, err := selectSource(ctx, root, nil, limits.IndexMemoryBytes/4)
	if err != nil {
		return nil, err
	}
	return prepareSelected(ctx, work, selection, cfg, opts, limits)
}

func prepareSelected(ctx context.Context, work string, selection *sourceSelection, cfg *image.RuntimeConfig, opts Options, limits PlanLimits) (_ *PreparedImage, retErr error) {
	layout, name := filepath.Join(work, "layout"), filepath.Join(work, "plan")
	if opts.Progress != nil {
		opts.Progress("build-erofs", 0, 0)
	}
	if err := buildGoPlan(ctx, selection, layout, name, limits); err != nil {
		return nil, err
	}
	// Planning has returned and released its indexes. No data emission overlaps
	// planning or layer application; GC remains the Go runtime's responsibility.
	readerLimits := limits
	readerLimits.IndexMemoryBytes /= 2
	p, err := openPlan(ctx, selection.root, layout, name, readerLimits)
	if err != nil {
		return nil, err
	}
	defer func() {
		if retErr != nil {
			retErr = errors.Join(retErr, p.Close())
		}
	}()
	if err = selection.validate(ctx); err != nil {
		return nil, err
	}
	body, err := cfg.MarshalDeterministic()
	if err != nil {
		return nil, err
	}
	tail, err := tailzip.Encode([]tailzip.Entry{{Name: image.ConfigFileName, Body: body}})
	if err != nil {
		return nil, err
	}
	// Reserve independent shares for source validation and writer metadata.
	// Account for the encoded suffix and the copy retained by tailzip.Append.
	writerMemory := limits.IndexMemoryBytes / 4
	if uint64(len(tail))*2+2<<20 > writerMemory {
		return nil, fmt.Errorf("flatten: runtime config exceeds index budget")
	}
	writerMemory -= uint64(len(tail)) * 2
	src, err := tailzip.Append(p, tail, tailzip.Options{})
	if err != nil {
		return nil, err
	}
	return &PreparedImage{work: work, plan: p, source: src, selection: selection, writerMemory: writerMemory}, nil
}

func (p *PreparedImage) begin(ctx context.Context) (context.Context, func(), error) {
	p.writeMu.Lock()
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		p.writeMu.Unlock()
		return nil, nil, os.ErrClosed
	}
	ctx, cancel := context.WithCancel(ctx)
	p.cancel = cancel
	p.mu.Unlock()
	return ctx, func() { p.mu.Lock(); p.cancel = nil; p.mu.Unlock(); cancel(); p.writeMu.Unlock() }, nil
}

func (p *PreparedImage) validate(ctx context.Context) error {
	if p.plan.current != nil {
		err := p.plan.current.Close()
		p.plan.current, p.plan.currentID, p.plan.dataValid = nil, 0, 0
		if err != nil {
			return err
		}
	}
	if err := p.plan.verifyPlanFiles(ctx); err != nil {
		return err
	}
	return p.selection.validate(ctx)
}

// WriteArtifact writes directly to any io.Writer, including a non-seekable
// pipe. No raw image is materialized. Failure must be treated as an incomplete
// artifact by the receiver; the carrier marker and success tail are withheld.
func (p *PreparedImage) WriteArtifact(ctx context.Context, w io.Writer, name string, options ...tarstream.WriteOption) (string, string, error) {
	ctx, done, err := p.begin(ctx)
	if err != nil {
		return "", "", err
	}
	defer done()
	if err = p.validate(ctx); err != nil {
		return "", "", err
	}
	options = append(append([]tarstream.WriteOption(nil), options...), tarstream.WithBeforeCommit(p.validate), tarstream.WithIndexMemoryLimit(p.writerMemory))
	return tarstream.WriteTo(ctx, w, name, p.source, options...)
}

// WriteRaw is the explicit raw EROFS+runtime-config API. It uses exactly the
// same plan/layout as WriteArtifact. A successful return includes final source
// validation; callers must not publish partial output on any error.
func (p *PreparedImage) WriteRaw(ctx context.Context, w io.Writer) error {
	ctx, done, err := p.begin(ctx)
	if err != nil {
		return err
	}
	defer done()
	if err = p.validate(ctx); err != nil {
		return err
	}
	buf := make([]byte, planReadBatch)
	for off := uint64(0); off < p.source.Size(); {
		if err = ctx.Err(); err != nil {
			return err
		}
		want := min(uint64(len(buf)), p.source.Size()-off)
		n, e := p.source.ReadAt(ctx, buf[:want], off)
		if e != nil {
			return e
		}
		if uint64(n) != want {
			return io.ErrUnexpectedEOF
		}
		n, e = w.Write(buf[:n])
		if e != nil {
			return e
		}
		if uint64(n) != want {
			return io.ErrShortWrite
		}
		off += want
	}
	return p.validate(ctx)
}

func (p *PreparedImage) Close() error {
	p.closeOnce.Do(func() {
		p.mu.Lock()
		p.closed = true
		if p.cancel != nil {
			p.cancel()
		}
		p.mu.Unlock()
		p.writeMu.Lock()
		defer p.writeMu.Unlock()
		p.closeErr = errors.Join(p.plan.Close(), removeOwnedWork(p.work))
	})
	return p.closeErr
}
