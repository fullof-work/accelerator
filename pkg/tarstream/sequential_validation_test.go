package tarstream

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"

	"github.com/kuasar-sandbox/accelerator/pkg/sparse"
)

type finalDataEOFReader struct{ *bytes.Reader }

func (r finalDataEOFReader) Read(buffer []byte) (int, error) {
	n, err := r.Reader.Read(buffer)
	if n > 0 && r.Reader.Len() == 0 {
		return n, io.EOF
	}
	return n, err
}

func TestSequentialPlaintextFullValidation(t *testing.T) {
	body := bytes.Repeat([]byte("0123456789abcdef"), 512)
	artifact := mustWrite(t, "image", body, nil)

	tampered := append([]byte(nil), artifact...)
	cursor := bytes.NewReader(artifact)
	if _, err := locate(cursor, "image", seekSkip(cursor)); err != nil {
		t.Fatal(err)
	}
	start, _ := cursor.Seek(0, io.SeekCurrent)
	tampered[start] ^= 1
	for _, test := range []struct {
		name string
		open func() (sparse.Source, string, error)
	}{
		{name: "reader-only", open: func() (sparse.Source, string, error) {
			return SourceFrom(readerOnly{bytes.NewReader(tampered)}, "")
		}},
		{name: "seekable", open: func() (sparse.Source, string, error) {
			return SourceFrom(bytes.NewReader(tampered), "")
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			source, _, err := test.open()
			if err != nil {
				t.Fatal(err)
			}
			buffer := make([]byte, len(body))
			if _, err := source.ReadAt(context.Background(), buffer, 0); !errors.Is(err, ErrDigestMismatch) {
				t.Fatalf("payload tamper error = %v", err)
			}
		})
	}
	buffer := make([]byte, len(body))

	appended := append(append([]byte(nil), artifact...), 0)
	source, _, err := SourceFrom(readerOnly{bytes.NewReader(appended)}, "")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := source.ReadAt(context.Background(), buffer, 0); !errors.Is(err, ErrInvalidCanonicalTarstream) {
		t.Fatalf("trailing byte error = %v", err)
	}

	dataEOF := finalDataEOFReader{bytes.NewReader(appended)}
	source, _, err = SourceFrom(readerOnly{dataEOF}, "")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := source.ReadAt(context.Background(), buffer, 0); !errors.Is(err, ErrInvalidCanonicalTarstream) {
		t.Fatalf("trailing byte with data+EOF error = %v", err)
	}

	truncated := artifact[:len(artifact)-1]
	source, _, err = SourceFrom(readerOnly{bytes.NewReader(truncated)}, "")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := source.ReadAt(context.Background(), buffer, 0); !errors.Is(err, ErrInvalidCanonicalTarstream) {
		t.Fatalf("truncated trailer error = %v", err)
	}
}

func TestSequentialEmptyArtifactFinalizesAtOpen(t *testing.T) {
	for _, artifact := range [][]byte{mustWrite(t, "empty", nil, nil), legacyPlaintextArtifact(t, "empty")} {
		artifact[len(artifact)-1] = 1
		if _, _, err := SourceFrom(readerOnly{bytes.NewReader(artifact)}, ""); !errors.Is(err, ErrInvalidCanonicalTarstream) {
			t.Fatalf("empty artifact trailer error = %v", err)
		}
	}
}
