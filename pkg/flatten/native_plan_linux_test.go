//go:build linux

package flatten

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"github.com/kuasar-sandbox/accelerator/internal/util"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
)

// locateMkfsErofs resolves the mkfs.erofs binary with a fixed
// precedence: $MKFS_EROFS_PATH > directory of running flatten-ctl
// executable > $PATH. The hint message in the returned error keeps
// users pointed at `make deps-erofs`.
func locateMkfsErofs() (string, error) {
	if p := os.Getenv("MKFS_EROFS_PATH"); p != "" {
		return p, nil
	}
	p, err := util.LocateBinary("mkfs.erofs")
	if err != nil {
		return "", fmt.Errorf("flatten: mkfs.erofs not found " +
			"(set MKFS_EROFS_PATH, place it alongside flatten-ctl, " +
			"or add it to PATH; run `make deps-erofs` to build it)")
	}
	return p, nil
}

type boundedDiagnostic struct{ bytes.Buffer }

func (b *boundedDiagnostic) Write(p []byte) (int, error) {
	n := len(p)
	if remaining := 64<<10 - b.Len(); remaining > 0 {
		b.Buffer.Write(p[:min(remaining, n)])
	}
	return n, nil
}

func buildPlan(ctx context.Context, mkfs string, selection *sourceSelection, layout, name string, limits PlanLimits) error {
	args := []string{"-Ededupe", "--chunksize=4096", "-T0", "-b4096", "-x-1", "-U", "00000000-0000-0000-0000-000000000000", "--quiet", "--ignore-mtime",
		"--plan=" + name, "--plan-memory=" + strconv.FormatUint(limits.NativeMemoryBytes, 10), "--plan-bytes=" + strconv.FormatUint(limits.PlanBytes, 10)}
	for _, p := range selection.excludes {
		args = append(args, "--exclude-path="+p)
	}
	args = append(args, layout, selection.root)
	cmd := exec.CommandContext(ctx, mkfs, args...)
	for _, env := range os.Environ() {
		if !strings.HasPrefix(env, "TMPDIR=") && !strings.HasPrefix(env, "SOURCE_DATE_EPOCH=") {
			cmd.Env = append(cmd.Env, env)
		}
	}
	cmd.Env = append(cmd.Env, "TMPDIR="+filepath.Dir(layout))
	var diagnostic boundedDiagnostic
	cmd.Stdout, cmd.Stderr = io.Discard, &diagnostic
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("flatten: native plan: %w: %s", errors.Join(err, ctx.Err()), strings.TrimSpace(diagnostic.String()))
	}
	return ctx.Err()
}
