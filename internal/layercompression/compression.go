// Package layercompression detects and decodes stored OCI layer bytes.
// Detection follows the content rather than a manifest's media type.
package layercompression

import (
	"bufio"
	"compress/gzip"
	"errors"
	"fmt"
	"io"

	"github.com/klauspost/compress/zstd"
)

// Kind identifies the encoding of the stored bytes.
type Kind uint8

const (
	Plain Kind = iota
	Gzip
	Zstd
)

// Options configures the zstd decoder. Zero fields retain the library defaults.
// Callers retain responsibility for limits on stored and decoded byte counts.
type Options struct {
	DecoderMemory      uint64
	DecoderConcurrency int
}

// Detect identifies plain, gzip or zstd bytes, including a leading zstd
// skippable frame. The returned reader preserves every byte consumed while
// detecting the format and must replace r for subsequent reads. Detect never
// closes r. Short input without a complete compression magic is plain; source
// errors other than EOF are preserved.
func Detect(r io.Reader) (Kind, io.Reader, error) {
	buffered := bufio.NewReader(r)
	prefix, err := buffered.Peek(4)
	if err != nil && !errors.Is(err, io.EOF) {
		return Plain, buffered, err
	}
	if len(prefix) >= 2 && prefix[0] == 0x1f && prefix[1] == 0x8b {
		return Gzip, buffered, nil
	}
	if len(prefix) == 4 {
		// Header.Decode checks both standard and skippable magic before it
		// parses the frame header. Four bytes are enough to reject a different
		// format; a matching but incomplete header remains zstd so its decoder
		// reports truncation instead of silently accepting it as plain bytes.
		var header zstd.Header
		if err := header.Decode(prefix); !errors.Is(err, zstd.ErrMagicMismatch) {
			return Zstd, buffered, nil
		}
	}
	return Plain, buffered, nil
}

// Open detects and decodes r. Close only releases decoder resources: the caller
// owns r and must close it, including after an error. Reading to EOF is required
// to check compressed stream trailers. Once compressed magic is recognized,
// decoding errors are returned and never cause a fallback to plain bytes.
func Open(r io.Reader, options Options) (io.ReadCloser, Kind, error) {
	kind, buffered, err := Detect(r)
	if err != nil {
		return nil, kind, err
	}
	switch kind {
	case Gzip:
		decoder, err := gzip.NewReader(buffered)
		if err != nil {
			return nil, kind, fmt.Errorf("gzip header: %w", err)
		}
		return decoder, kind, nil
	case Zstd:
		var opts []zstd.DOption
		if options.DecoderMemory != 0 {
			opts = append(opts, zstd.WithDecoderMaxMemory(options.DecoderMemory))
		}
		if options.DecoderConcurrency != 0 {
			opts = append(opts, zstd.WithDecoderConcurrency(options.DecoderConcurrency))
		}
		decoder, err := zstd.NewReader(buffered, opts...)
		if err != nil {
			if decoder != nil {
				decoder.Close()
			}
			return nil, kind, fmt.Errorf("zstd header: %w", err)
		}
		return decoder.IOReadCloser(), kind, nil
	default:
		return io.NopCloser(buffered), kind, nil
	}
}
