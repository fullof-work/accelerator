//go:build !linux

package remote

import (
	"os"
	"time"
)

func pinPendingBlob(f *os.File) error { return nil }
func pinBlob(f *os.File) error        { return nil }
func removeIdleBlob(path string, cutoff time.Time) (bool, error) {
	st, err := os.Stat(path)
	if os.IsNotExist(err) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if st.ModTime().After(cutoff) {
		return false, nil
	}
	err = os.Remove(path)
	return err == nil, err
}

// lockFile is a no-op on non-linux platforms. flatten-ctl targets linux
// (the Makefile builds GOOS=linux); this keeps `go build` / `go test` green
// on developer machines without pulling in an OS-specific locking dependency.
func lockFile(path string) (func(), error) {
	return func() {}, nil
}

func lockPublication(path string) (func(), error) { return lockFile(path) }
