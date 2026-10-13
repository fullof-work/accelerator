//go:build linux

package flatten

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"syscall"
)

// Build writes an explicit raw EROFS image with the runtime-config ZIP. It
// retains Source's reopenable, uncompressed LayerOpener contract; compressed
// descriptor verification remains the responsibility of that Source. Every
// complete uncompressed stream is verified against the fixed config's DiffID.
// The destination is replaced only after preparation, output and cleanup succeed.
func Build(src Source, outputPath string, opts Options) error {
	return writeRawFile(context.Background(), outputPath, func(_ string) (*PreparedImage, error) {
		return prepareSource(context.Background(), src, opts)
	})
}

func prepareSource(ctx context.Context, src Source, opts Options) (*PreparedImage, error) {
	if src == nil {
		return nil, fmt.Errorf("flatten: nil source")
	}
	config, err := src.ConfigJSON()
	if err != nil {
		return nil, err
	}
	config = bytes.Clone(config)
	layers, err := src.Layers()
	if err != nil {
		return nil, err
	}
	layers = append([]LayerOpener(nil), layers...)
	diffIDs, err := imageDiffIDs(config, len(layers))
	if err != nil {
		return nil, err
	}
	for i, open := range layers {
		if open == nil {
			return nil, fmt.Errorf("flatten: nil layer opener %d", i)
		}
	}
	return prepareLayers(ctx, config, len(layers), opts, func(i int, root string) error {
		return applySourceLayer(ctx, layers[i], diffIDs[i], root, opts.LayerLimits)
	}, nil)
}

func applySourceLayer(ctx context.Context, open LayerOpener, diffID, root string, limits LayerLimits) (retErr error) {
	limits, err := limits.normalized()
	if err != nil {
		return err
	}
	if err = ctx.Err(); err != nil {
		return err
	}
	body, err := open()
	if err != nil {
		if body != nil {
			err = errors.Join(err, body.Close())
		}
		return err
	}
	if body == nil {
		return fmt.Errorf("flatten: layer opener returned a nil body")
	}
	// Source does not expose a compressed size or digest. Do not invent one
	// or turn a consumed reader into a supposedly reopenable LayerInput.
	r := &checkedReader{ctx: ctx, r: body, h: sha256.New(), limit: limits.DecodedBytes, label: "decoded layer"}
	s := &layerStream{body: body, compressed: r, decoded: r, cancelDone: make(chan struct{})}
	s.stopCancel = context.AfterFunc(ctx, func() { s.closeBody(); close(s.cancelDone) })
	defer func() { retErr = errors.Join(retErr, s.Close()) }()
	if err = unpackLayer(ctx, r, root, limits.MetadataBytes); err != nil {
		return err
	}
	if _, err = io.Copy(io.Discard, r); err != nil {
		return err
	}
	return r.verify(diffID)
}

// Flatten converts docker-save input into an explicit raw image. The caller
// retains ownership of input. Use PrepareArchive for cancellation with an owned,
// closeable body and direct artifact output.
func Flatten(input io.Reader, outputPath string) error {
	return FlattenWith(input, outputPath, Options{})
}

func FlattenWith(input io.Reader, outputPath string, opts Options) error {
	return writeRawFile(context.Background(), outputPath, func(_ string) (*PreparedImage, error) {
		return prepareArchive(context.Background(), input, opts, nil)
	})
}

func FlattenFile(inputPath, outputPath string) error {
	return FlattenFileWith(inputPath, outputPath, Options{})
}

func FlattenFileWith(inputPath, outputPath string, opts Options) error {
	return writeRawFile(context.Background(), outputPath, func(_ string) (*PreparedImage, error) {
		f, err := os.Open(inputPath)
		if err != nil {
			return nil, err
		}
		return PrepareArchive(context.Background(), f, opts)
	})
}

// BuildFromDir exports a caller-stabilized directory into an explicit raw file.
// It excludes its scratch, pending output and destination before planning. The
// source remains owned by the caller and must stay stable until this returns.
func BuildFromDir(root, outputPath string, opts Options, dopts DirOptions) error {
	return writeRawFile(context.Background(), outputPath, func(pending string) (*PreparedImage, error) {
		dopts.OutputPaths = append(append([]string(nil), dopts.OutputPaths...), outputPath, pending)
		return PrepareDir(context.Background(), root, opts, dopts)
	})
}

func writeRawFile(ctx context.Context, output string, prepare func(string) (*PreparedImage, error)) (retErr error) {
	pending, err := reserveRawOutput(output)
	if err != nil {
		return err
	}
	defer func() { retErr = errors.Join(retErr, pending.Close()) }()
	// Exclude the complete private staging directory before planning.
	p, err := prepare(pending.path)
	if err != nil {
		return err
	}
	err = p.WriteRaw(ctx, pending.file)
	// Close input resources and the output before commit. Every late failure
	// preserves the previous destination; no additional fsync is introduced.
	err = errors.Join(err, pending.closeFile(), p.Close())
	if err != nil {
		return err
	}
	return pending.commit()
}

// outputPermissions accepts only ordinary destination files. Atomic replacement
// must not unlink a device, directory or symlink. Check again at commit time.
func outputPermissions(output string) (os.FileMode, *syscall.Stat_t, error) {
	st, err := os.Lstat(output)
	if errors.Is(err, os.ErrNotExist) {
		return 0666, nil, nil
	}
	if err != nil {
		return 0, nil, err
	}
	if !st.Mode().IsRegular() {
		return 0, nil, fmt.Errorf("destination is not a regular file: %s", output)
	}
	return st.Mode().Perm(), st.Sys().(*syscall.Stat_t), nil
}
