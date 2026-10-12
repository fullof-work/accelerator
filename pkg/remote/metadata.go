package remote

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"

	ggcrremote "github.com/google/go-containerregistry/pkg/v1/remote"
)

const maxMetadataBytes = 16 << 20

// metadataTransport bounds metadata before ggcr's ReadAll and checks the
// original body Close, which ggcr otherwise defers without reporting its error.
// Authentication, proxy, TLS, redirects and retry policy still use remoteOpts.
// Layer bodies never pass through this transport.
type metadataTransport struct{ base http.RoundTripper }

func (t metadataTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	resp, err := t.base.RoundTrip(req)
	if err != nil {
		return resp, err
	}
	if resp.Body == nil {
		return resp, nil
	}
	if req.Method != http.MethodHead && resp.ContentLength > maxMetadataBytes {
		return nil, errors.Join(fmt.Errorf("remote: registry metadata exceeds %d bytes", maxMetadataBytes), resp.Body.Close())
	}
	b, readErr := io.ReadAll(io.LimitReader(resp.Body, maxMetadataBytes+1))
	closeErr := resp.Body.Close()
	if len(b) > maxMetadataBytes {
		readErr = fmt.Errorf("remote: registry metadata exceeds %d bytes", maxMetadataBytes)
	}
	if err := errors.Join(readErr, closeErr); err != nil {
		return nil, err
	}
	resp.Body = io.NopCloser(bytes.NewReader(b))
	return resp, nil
}

func (c *Config) metadataOpts(ctx context.Context) []ggcrremote.Option {
	base := c.transport
	if base == nil {
		base = ggcrremote.DefaultTransport
	}
	return append(c.remoteOpts(ctx), ggcrremote.WithTransport(metadataTransport{base}))
}
