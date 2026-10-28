//go:build !linux

package activation

import (
	"errors"
	"net"
	"os"
)

func filesFromEnv(string, string, string) (map[string]*os.File, error) {
	return nil, errors.New("activation: named listening descriptors require Linux")
}

func importFiles(map[string]*os.File) (map[string]net.Listener, error) {
	return nil, errors.New("activation: named listening descriptors require Linux")
}
