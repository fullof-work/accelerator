package tarstream

import (
	"context"
	"crypto/subtle"
	"encoding/hex"
	"fmt"
	"reflect"
	"strings"
)

const (
	DigestScheme     = "digest"
	DigestSchemeHMAC = "hmac"
)

type WriteOption interface {
	applyWrite(*writeOptions) error
}

type ReadOption interface {
	applyRead(*readOptions) error
}

type CodecOption interface {
	WriteOption
	ReadOption
}

type writeOptions struct {
	codec        Codec
	codecSet     bool
	beforeCommit func(context.Context) error
	indexMemory  uint64
}

type indexMemoryOption uint64

// WithIndexMemoryLimit bounds the writer's extent indexes, sparse-map encoding,
// metadata tail and working buffers. The budget is checked before emitting any
// bytes. Omission preserves the existing writer behavior.
func WithIndexMemoryLimit(bytes uint64) WriteOption { return indexMemoryOption(bytes) }

func (o indexMemoryOption) applyWrite(dst *writeOptions) error {
	if o == 0 || dst.indexMemory != 0 {
		return fmt.Errorf("%w: invalid or duplicate index memory limit", ErrInvalidOption)
	}
	dst.indexMemory = uint64(o)
	return nil
}

type beforeCommitOption struct{ check func(context.Context) error }

// WithBeforeCommit runs check once after all source payload bytes have been
// consumed, before the carrier digest marker, archive trailer, and final
// encrypted record are written. It also runs for empty and all-Hole/Zero sources;
// no extra EOF read is required. An earlier read/write failure skips check.
//
// The caller retains ownership of the source and must keep it stable and open
// until WriteTo returns. check must not modify it. A failed check leaves an
// incomplete output; the caller must discard it and must not commit a file,
// upload, or conversion-cache result. This hook does not make a mutable source
// an atomic snapshot, or confer durability on the destination. CarrierDigest
// rejects this option because deriving an identity does not consume the source.
func WithBeforeCommit(check func(context.Context) error) WriteOption {
	return beforeCommitOption{check: check}
}

func (o beforeCommitOption) applyWrite(dst *writeOptions) error {
	if o.check == nil {
		return fmt.Errorf("%w: WithBeforeCommit requires a non-nil check", ErrInvalidOption)
	}
	if dst.beforeCommit != nil {
		return fmt.Errorf("%w: duplicate WithBeforeCommit", ErrInvalidOption)
	}
	dst.beforeCommit = o.check
	return nil
}

type readOptions struct {
	codec       Codec
	codecSet    bool
	required    bool
	expected    [32]byte
	expectedSet bool
	scheme      string
}

type codecOption struct {
	codec    Codec
	required bool
}

// WithCodec supplies the encrypted tarstream v1 artifact codec. Required only
// controls whether readers reject plaintext; every writer with a codec emits
// encrypted v1.
func WithCodec(codec Codec, required bool) CodecOption {
	return codecOption{codec: codec, required: required}
}

func (o codecOption) applyWrite(dst *writeOptions) error {
	if dst.codecSet {
		return fmt.Errorf("%w: duplicate WithCodec", ErrInvalidOption)
	}
	if nilInterface(o.codec) {
		return fmt.Errorf("%w: WithCodec requires a non-nil codec", ErrInvalidOption)
	}
	dst.codec, dst.codecSet = o.codec, true
	return nil
}

func (o codecOption) applyRead(dst *readOptions) error {
	if dst.codecSet {
		return fmt.Errorf("%w: duplicate WithCodec", ErrInvalidOption)
	}
	if nilInterface(o.codec) {
		return fmt.Errorf("%w: WithCodec requires a non-nil codec", ErrInvalidOption)
	}
	dst.codec, dst.codecSet, dst.required = o.codec, true, o.required
	return nil
}

type expectedDigestOption struct {
	scheme string
	digest string
}

// WithExpectedDigest requires the carrier identity to match the supplied
// generic digest qualifier and 64-character lowercase hexadecimal value.
func WithExpectedDigest(scheme, digest string) ReadOption {
	return expectedDigestOption{scheme: scheme, digest: digest}
}

func (o expectedDigestOption) applyRead(dst *readOptions) error {
	if dst.expectedSet {
		return fmt.Errorf("%w: duplicate WithExpectedDigest", ErrInvalidOption)
	}
	digest, err := decodeDigest(o.scheme, o.digest)
	if err != nil {
		return err
	}
	dst.expected, dst.expectedSet, dst.scheme = digest, true, o.scheme
	return nil
}

func parseWriteOptions(options []WriteOption) (writeOptions, error) {
	var result writeOptions
	for i, option := range options {
		if option == nil {
			return result, fmt.Errorf("%w: nil write option %d", ErrInvalidOption, i)
		}
		if err := option.applyWrite(&result); err != nil {
			return result, err
		}
	}
	return result, nil
}

func parseReadOptions(options []ReadOption) (readOptions, error) {
	var result readOptions
	for i, option := range options {
		if option == nil {
			return result, fmt.Errorf("%w: nil read option %d", ErrInvalidOption, i)
		}
		if err := option.applyRead(&result); err != nil {
			return result, err
		}
	}
	if result.expectedSet {
		switch result.scheme {
		case DigestScheme:
			if result.required {
				return result, fmt.Errorf("%w: plaintext digest is forbidden by required policy", ErrInvalidOption)
			}
		case DigestSchemeHMAC:
			if result.codec == nil {
				return result, fmt.Errorf("%w: hmac digest requires a codec", ErrInvalidOption)
			}
		default:
			return result, fmt.Errorf("%w: unsupported digest scheme", ErrInvalidOption)
		}
	}
	return result, nil
}

func decodeDigest(scheme, digest string) ([32]byte, error) {
	var result [32]byte
	if scheme != DigestScheme && scheme != DigestSchemeHMAC {
		return result, fmt.Errorf("%w: unsupported digest scheme", ErrInvalidDigest)
	}
	if len(digest) != hex.EncodedLen(len(result)) || strings.ToLower(digest) != digest {
		return result, fmt.Errorf("%w: digest must be 64 lowercase hex characters", ErrInvalidDigest)
	}
	if _, err := hex.Decode(result[:], []byte(digest)); err != nil {
		return result, fmt.Errorf("%w: malformed hexadecimal digest", ErrInvalidDigest)
	}
	return result, nil
}

func externalDigest(codec Codec, plain [32]byte) (string, [32]byte) {
	if codec == nil {
		return DigestScheme, plain
	}
	return DigestSchemeHMAC, codec.KeyedDigest(plain)
}

func checkExpected(options readOptions, scheme string, actual [32]byte) error {
	if options.expectedSet && (options.scheme != scheme || subtle.ConstantTimeCompare(options.expected[:], actual[:]) != 1) {
		return ErrDigestMismatch
	}
	return nil
}

func nilInterface(value any) bool {
	if value == nil {
		return true
	}
	v := reflect.ValueOf(value)
	switch v.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return v.IsNil()
	default:
		return false
	}
}
