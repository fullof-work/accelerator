//go:build !linux

package flatten

import (
	"errors"
	"io"
)

// EROFS preparation uses Linux inode, ownership, special-file and sparse-file
// semantics. Other platforms can still consume the existing artifact format.
var errPlatform = errors.New("flatten: image preparation requires Linux")

func Build(Source, string, Options) error                    { return errPlatform }
func Flatten(io.Reader, string) error                        { return errPlatform }
func FlattenWith(io.Reader, string, Options) error           { return errPlatform }
func FlattenFile(string, string) error                       { return errPlatform }
func FlattenFileWith(string, string, Options) error          { return errPlatform }
func BuildFromDir(string, string, Options, DirOptions) error { return errPlatform }
