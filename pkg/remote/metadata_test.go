package remote

import (
	"bytes"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
)

type metadataBody struct {
	io.Reader
	closed   int
	closeErr error
	read     int
}

func (b *metadataBody) Close() error { b.closed++; return b.closeErr }
func (b *metadataBody) Read(p []byte) (int, error) {
	n, err := b.Reader.Read(p)
	b.read += n
	return n, err
}

func TestMetadataTransportBoundsAndClose(t *testing.T) {
	for _, tc := range []struct {
		name     string
		size     int64
		body     []byte
		closeErr error
		want     string
	}{
		{"success", 2, []byte("{}"), nil, ""},
		{"close", 2, []byte("{}"), errors.New("body close failed"), "body close failed"},
		{"declared", maxMetadataBytes + 1, nil, nil, "exceeds"},
		{"chunked", -1, bytes.Repeat([]byte("x"), maxMetadataBytes+2), nil, "exceeds"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b := &metadataBody{Reader: bytes.NewReader(tc.body), closeErr: tc.closeErr}
			tr := metadataTransport{pullTestTransport(func(r *http.Request) (*http.Response, error) {
				return &http.Response{StatusCode: 200, Body: b, ContentLength: tc.size, Header: make(http.Header)}, nil
			})}
			req, _ := http.NewRequest(http.MethodGet, "http://registry.invalid/v2/manifest", nil)
			resp, err := tr.RoundTrip(req)
			if tc.want != "" && (err == nil || !strings.Contains(err.Error(), tc.want)) || tc.want == "" && err != nil {
				t.Fatalf("RoundTrip: %v", err)
			}
			if resp != nil {
				resp.Body.Close()
			}
			if b.closed != 1 || b.read > maxMetadataBytes+1 {
				t.Fatalf("closed=%d read=%d", b.closed, b.read)
			}
			if tc.name == "declared" && b.read != 0 {
				t.Fatal("read an oversized declared response")
			}
		})
	}
}
