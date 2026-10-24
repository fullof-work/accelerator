//go:build linux

package flatten

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	erofs "github.com/kuasar-sandbox/go-erofs"
	"golang.org/x/sys/unix"
)

func TestGoPlannerContentAndSelection(t *testing.T) {
	// The production path must not resolve or execute a native planner.
	t.Setenv("MKFS_EROFS_PATH", "/nonexistent/native-planner")
	root, work := t.TempDir(), t.TempDir()
	payload := append(bytes.Repeat([]byte("repeated content"), 4096), []byte("tail")...)
	for _, name := range []string{"data", "duplicate"} {
		if err := os.WriteFile(filepath.Join(root, name), payload, 0640); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.Link(filepath.Join(root, "data"), filepath.Join(root, "hardlink")); err != nil {
		t.Fatal(err)
	}
	if err := os.Link(filepath.Join(root, "data"), filepath.Join(root, "skip-link")); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("data", filepath.Join(root, "symlink")); err != nil {
		t.Fatal(err)
	}
	if err := os.Link(filepath.Join(root, "symlink"), filepath.Join(root, "symlink-alias")); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(root, "empty"), 0751); err != nil {
		t.Fatal(err)
	}
	if err := unix.Mkfifo(filepath.Join(root, "fifo"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Link(filepath.Join(root, "fifo"), filepath.Join(root, "fifo-alias")); err != nil {
		t.Fatal(err)
	}
	sparse, err := os.Create(filepath.Join(root, "sparse"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err = sparse.WriteAt([]byte("end"), 2<<20); err != nil {
		t.Fatal(err)
	}
	if err = sparse.Close(); err != nil {
		t.Fatal(err)
	}
	if err = unix.Setxattr(filepath.Join(root, "data"), "user.not-exported", []byte("value"), 0); err != nil {
		t.Fatal(err)
	}
	if os.Geteuid() == 0 {
		if err = os.Chown(filepath.Join(root, "data"), 12345, 23456); err != nil {
			t.Fatal(err)
		}
	}
	if err = os.Chmod(filepath.Join(root, "data"), fs.ModeSetuid|0640); err != nil {
		t.Fatal(err)
	}
	opts := Options{TmpDir: work}
	dopts := DirOptions{Skip: []string{"skip-link"}}
	p, err := PrepareDir(context.Background(), root, opts, dopts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := p.Close(); err != nil {
			t.Error(err)
		}
	})
	var raw bytes.Buffer
	if err = p.WriteRaw(context.Background(), &raw); err != nil {
		t.Fatal(err)
	}
	img, err := erofs.Open(bytes.NewReader(raw.Bytes()))
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"data", "duplicate", "hardlink", "symlink", "symlink-alias", "sparse"} {
		want, err := os.ReadFile(filepath.Join(root, name))
		if err != nil {
			t.Fatal(err)
		}
		got, err := fs.ReadFile(img, name)
		if err != nil || !bytes.Equal(got, want) {
			t.Fatalf("%s content: %v", name, err)
		}
	}
	if _, err = fs.Stat(img, "skip-link"); !os.IsNotExist(err) {
		t.Fatalf("excluded link: %v", err)
	}
	lstat := img.(interface {
		Lstat(string) (fs.FileInfo, error)
	})
	for _, pair := range [][2]string{{"data", "hardlink"}, {"symlink", "symlink-alias"}, {"fifo", "fifo-alias"}} {
		a, err := lstat.Lstat(pair[0])
		if err != nil {
			t.Fatal(err)
		}
		b, err := lstat.Lstat(pair[1])
		if err != nil {
			t.Fatal(err)
		}
		x, y := a.Sys().(*erofs.Stat), b.Sys().(*erofs.Stat)
		if x.Ino != y.Ino || x.Nlink != 2 {
			t.Fatalf("hard-link selection %v: %+v %+v", pair, x, y)
		}
	}
	info, err := fs.Stat(img, "data")
	if err != nil {
		t.Fatal(err)
	}
	stat := info.Sys().(*erofs.Stat)
	if info.Mode() != fs.ModeSetuid|0640 || !info.ModTime().Equal(time.Unix(0, 0)) {
		t.Fatalf("metadata: %v %v", info.Mode(), info.ModTime())
	}
	if os.Geteuid() == 0 && (stat.UID != 12345 || stat.GID != 23456) {
		t.Fatalf("ownership: %+v", stat)
	}
	if len(stat.Xattrs) != 0 {
		t.Fatal("xattrs exported")
	}
	duplicate, err := fs.Stat(img, "duplicate")
	if err != nil {
		t.Fatal(err)
	}
	if duplicate.Sys().(*erofs.Stat).Ino == stat.Ino {
		t.Fatal("content dedup merged independent inodes")
	}
	if p.plan.dataLength >= uint64(len(payload))*2 {
		t.Fatal("duplicate blocks were not shared")
	}
	again, err := PrepareDir(context.Background(), root, opts, dopts)
	if err != nil {
		t.Fatal(err)
	}
	var second bytes.Buffer
	if err = again.WriteRaw(context.Background(), &second); err != nil {
		t.Fatal(err)
	}
	if err = again.Close(); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(raw.Bytes(), second.Bytes()) {
		t.Fatal("repeated planning is not deterministic")
	}
	if fsck, err := exec.LookPath("fsck.erofs"); err == nil {
		file := filepath.Join(t.TempDir(), "image.erofs")
		if err = os.WriteFile(file, raw.Bytes()[:p.plan.Size()], 0600); err != nil {
			t.Fatal(err)
		}
		extract := filepath.Join(t.TempDir(), "extract")
		if out, err := exec.Command(fsck, "--extract="+extract, file).CombinedOutput(); err != nil {
			t.Fatalf("independent fsck: %v %s", err, out)
		}
		for _, name := range []string{"data", "duplicate", "hardlink", "sparse"} {
			want, err := os.ReadFile(filepath.Join(root, name))
			if err != nil {
				t.Fatal(err)
			}
			got, err := os.ReadFile(filepath.Join(extract, name))
			if err != nil || !bytes.Equal(got, want) {
				t.Fatalf("native extraction %s: %v", name, err)
			}
		}
	} else {
		t.Log("native fsck unavailable; Go semantic checks completed")
	}
}

func TestGoPlannerFailureCleansOwnedScratch(t *testing.T) {
	for _, kind := range []string{"plan-budget", "memory-budget", "canceled"} {
		t.Run(kind, func(t *testing.T) {
			root, work := t.TempDir(), t.TempDir()
			opts := Options{TmpDir: work}
			ctx := context.Background()
			switch kind {
			case "plan-budget":
				opts.PlanLimits.PlanBytes = 192
			case "memory-budget":
				opts.PlanLimits.NativeMemoryBytes = 16 << 20
				file, err := os.Create(filepath.Join(root, "unique"))
				if err != nil {
					t.Fatal(err)
				}
				for i := 0; i < 50000; i++ {
					sum := sha256.Sum256([]byte(fmt.Sprint(i)))
					if _, err = file.Write(bytes.Repeat(sum[:], 128)); err != nil {
						t.Fatal(err)
					}
				}
				if err = file.Close(); err != nil {
					t.Fatal(err)
				}
			case "canceled":
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			}
			p, err := PrepareDir(ctx, root, opts, DirOptions{})
			if err == nil {
				p.Close()
				t.Fatal("expected failure")
			}
			if kind == "canceled" && !errors.Is(err, context.Canceled) {
				t.Fatal(err)
			}
			if kind != "canceled" && !strings.Contains(err.Error(), "budget") {
				t.Fatal(err)
			}
			entries, err := os.ReadDir(work)
			if err != nil || len(entries) != 0 {
				t.Fatalf("owned scratch remains: %v %v", entries, err)
			}
		})
	}
}

func TestPlanningTableGrowthAndCollisions(t *testing.T) {
	memory := planningMemory{limit: 64 << 10}
	table := planningTable[uint64, uint64]{memory: &memory, hash: func(k uint64) uint64 { return k % 4 }}
	var inserted uint64
	for ; inserted < 10000; inserted++ {
		err := table.insert(context.Background(), inserted, inserted+7)
		if memory.used > memory.limit || memory.peak > memory.limit {
			t.Fatal("table exceeded admitted budget")
		}
		if err != nil {
			break
		}
	}
	if inserted == 10000 {
		t.Fatal("table budget was not enforced")
	}
	for k := uint64(0); k < inserted; k++ {
		if v, ok := table.find(k); !ok || v != k+7 {
			t.Fatalf("lost collision entry %d", k)
		}
	}
	if _, ok := table.find(inserted + 1); ok {
		t.Fatal("unexpected table entry")
	}
}

func TestGoPlannerPaddedBlockSharing(t *testing.T) {
	root := t.TempDir()
	short := []byte("a partial block")
	block := make([]byte, 4096)
	copy(block, short)
	inputs := map[string][]byte{"a-short": short, "b-padded": block, "c-two": append(append([]byte(nil), block...), short...)}
	for name, data := range inputs {
		if err := os.WriteFile(filepath.Join(root, name), data, 0600); err != nil {
			t.Fatal(err)
		}
	}
	p, err := PrepareDir(context.Background(), root, Options{}, DirOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	if p.plan.count != 1 {
		t.Fatalf("padded content was not shared: %d blocks", p.plan.count)
	}
	rec, err := p.plan.record(0)
	if err != nil {
		t.Fatal(err)
	}
	if rec.length != uint32(len(short)) || rec.digest != sha256.Sum256(short) {
		t.Fatal("record does not verify actual source length")
	}
	raw := readPlanSource(t, p.plan)
	img, err := erofs.Open(bytes.NewReader(raw))
	if err != nil {
		t.Fatal(err)
	}
	for name, want := range inputs {
		got, err := fs.ReadFile(img, name)
		if err != nil || !bytes.Equal(got, want) {
			t.Fatalf("%s: %v", name, err)
		}
	}
}

func TestGoPlannerBatchHashContentAndDeterminism(t *testing.T) {
	root, work := t.TempDir(), t.TempDir()
	payload := bytes.Repeat(bytes.Repeat([]byte{0xa7}, planBlockSize), 2*planReadBatch/planBlockSize+1)
	// Distinguish full blocks at both worker and batch boundaries, including a
	// differing final byte. A prefix-only equality shortcut would corrupt these.
	for _, block := range []int{0, 63, 64, 127, 128, 255, 256, 511, 512} {
		payload[(block+1)*planBlockSize-1] = byte(block)
	}
	payload = append(payload, bytes.Repeat([]byte{0xa7}, 513)...)
	if err := os.WriteFile(filepath.Join(root, "data"), payload, 0600); err != nil {
		t.Fatal(err)
	}
	previous := runtime.GOMAXPROCS(1)
	defer runtime.GOMAXPROCS(previous)
	var first []byte
	for _, parallelism := range []int{1, 2, 4} {
		runtime.GOMAXPROCS(parallelism)
		p, err := PrepareDir(context.Background(), root, Options{TmpDir: work}, DirOptions{})
		if err != nil {
			t.Fatal(err)
		}
		var raw bytes.Buffer
		err = p.WriteRaw(context.Background(), &raw)
		closeErr := p.Close()
		if err != nil || closeErr != nil {
			t.Fatal(err, closeErr)
		}
		img, err := erofs.Open(bytes.NewReader(raw.Bytes()))
		if err != nil {
			t.Fatal(err)
		}
		actual, err := fs.ReadFile(img, "data")
		if err != nil || !bytes.Equal(actual, payload) {
			t.Fatalf("parallelism %d: content differs: %v", parallelism, err)
		}
		if first != nil && !bytes.Equal(first, raw.Bytes()) {
			t.Fatal("worker count changed image identity")
		}
		first = append(first[:0], raw.Bytes()...)
	}
}

type gatedHashContext struct {
	context.Context
	started chan struct{}
	release <-chan struct{}
}

func (c *gatedHashContext) Err() error {
	if c.Context.Err() == nil {
		c.started <- struct{}{}
		<-c.release
	}
	return c.Context.Err()
}

func TestGoPlannerCancelledBatchJoinsWorkers(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	release := make(chan struct{})
	gated := &gatedHashContext{Context: ctx, started: make(chan struct{}, 4), release: release}
	g := &goPlanner{ctx: gated, buffer: make([]byte, planReadBatch), hashWorkers: 4}
	done := make(chan error, 1)
	go func() { done <- g.hashBatch(planReadBatch / planBlockSize) }()
	for i := 0; i < 4; i++ {
		<-gated.started
	}
	cancel()
	close(release)
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("cancel: %v", err)
	}
	// Cleanup can immediately reuse admitted storage after cancellation returns.
	// The race suite also detects a worker escaping this lifetime boundary.
	clear(g.buffer)
	clear(g.sums[:])
}

// image.flatten E2E checks the same fixed digest on both native architectures.
// Numeric owners and modes are fixed so host user/umask cannot alter the input.
func TestGoPlannerCrossArchitectureGolden(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("fixed numeric ownership requires the root integration suite")
	}
	root := t.TempDir()
	payload := make([]byte, planReadBatch+73)
	for i := range payload {
		payload[i] = byte((i*17 + i/4096) % 251)
	}
	for name, body := range map[string][]byte{"data": payload, "duplicate": payload, "short": []byte("fixed tail\n"), "empty-file": nil} {
		if err := os.WriteFile(filepath.Join(root, name), body, 0600); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.Link(filepath.Join(root, "data"), filepath.Join(root, "hardlink")); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("data", filepath.Join(root, "symlink")); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(root, "empty-directory"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := unix.Mkfifo(filepath.Join(root, "fifo"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := filepath.Walk(root, func(path string, info fs.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if err := os.Lchown(path, 12345, 23456); err != nil {
			return err
		}
		if info.Mode()&fs.ModeSymlink != 0 {
			return nil
		}
		mode := fs.FileMode(0640)
		if info.IsDir() {
			mode = 0751
		}
		return os.Chmod(path, mode)
	}); err != nil {
		t.Fatal(err)
	}
	p, err := PrepareDir(context.Background(), root, Options{TmpDir: t.TempDir()}, DirOptions{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := p.Close(); err != nil {
			t.Error(err)
		}
	})
	var artifact bytes.Buffer
	if _, _, err := p.WriteArtifact(context.Background(), &artifact, "image"); err != nil {
		t.Fatal(err)
	}
	// The presence carrier introduced in #186 changes the envelope, not the
	// EROFS payload. This exact digest also matches guest-runtime's native test.
	const want = "6978a0c147d368c148c453b16d90b1dcff5026b7cd682b66379d6a6eff74b1ca"
	if got := fmt.Sprintf("%x", sha256.Sum256(artifact.Bytes())); got != want {
		t.Fatalf("fixed Go EROFS artifact changed on %s: got %s, want %s", runtime.GOARCH, got, want)
	}
}
