//go:build linux

package flatten

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"golang.org/x/sys/unix"

	"github.com/kuasar-sandbox/accelerator/pkg/image"
	"github.com/kuasar-sandbox/accelerator/pkg/sparse"
	"github.com/kuasar-sandbox/accelerator/pkg/tarstream"
)

func makePlanFixture(t *testing.T, empty bool) (*erofsPlan, string, string, string) {
	t.Helper()
	root, work := t.TempDir(), t.TempDir()
	if !empty {
		var b bytes.Buffer
		for i := 0; i < 300; i++ {
			sum := sha256.Sum256([]byte(fmt.Sprint(i)))
			b.Write(bytes.Repeat(sum[:], 128))
		}
		b.WriteString("partial tail")
		if err := os.WriteFile(filepath.Join(root, "payload"), b.Bytes(), 0640); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(root, "duplicate"), b.Bytes(), 0640); err != nil {
			t.Fatal(err)
		}
		if err := os.Link(filepath.Join(root, "payload"), filepath.Join(root, "hardlink")); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink("payload", filepath.Join(root, "symlink")); err != nil {
			t.Fatal(err)
		}
		if err := os.Mkdir(filepath.Join(root, "empty-dir"), 0750); err != nil {
			t.Fatal(err)
		}
		f, err := os.Create(filepath.Join(root, "sparse"))
		if err != nil {
			t.Fatal(err)
		}
		if _, err = f.WriteAt([]byte("sparse tail"), 3<<20); err != nil {
			t.Fatal(err)
		}
		if err = f.Close(); err != nil {
			t.Fatal(err)
		}
	}
	l := PlanLimits{}
	l, _ = l.defaults()
	selection, err := selectSource(context.Background(), root, nil, l.IndexMemoryBytes)
	if err != nil {
		t.Fatal(err)
	}
	layout, name := filepath.Join(work, "layout"), filepath.Join(work, "plan")
	if err = buildGoPlan(context.Background(), selection, layout, name, l); err != nil {
		t.Fatal(err)
	}
	p, err := openPlan(context.Background(), root, layout, name, l)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { p.Close() })
	return p, root, layout, name
}

func readPlanSource(t *testing.T, s sparse.Source) []byte {
	t.Helper()
	b := make([]byte, s.Size())
	for off := uint64(0); off < s.Size(); {
		want := min(uint64(10331), s.Size()-off) // cross chunks and batch boundaries
		n, err := s.ReadAt(context.Background(), b[off:off+want], off)
		if err != nil || uint64(n) != want {
			t.Fatalf("read @%d: %d %v", off, n, err)
		}
		off += want
	}
	return b
}

func TestGoPlanMatchesLayoutAndContent(t *testing.T) {
	for _, empty := range []bool{true, false} {
		t.Run(fmt.Sprint(empty), func(t *testing.T) {
			p, root, layout, _ := makePlanFixture(t, empty)
			want := readPlanSource(t, p)
			full := filepath.Join(t.TempDir(), "raw")
			if err := os.WriteFile(full, want, 0600); err != nil {
				t.Fatal(err)
			}
			for i := 0; i < 2; i++ {
				// Out-of-order metadata queries must not consume the emitter.
				for _, off := range []uint64{p.Size() - 1, 0, p.Size() / 2, 0} {
					if _, err := p.RunAt(off, p.Size()); err != nil {
						t.Fatal(err)
					}
				}
				got := readPlanSource(t, p)
				if !bytes.Equal(got, want) {
					t.Fatal("Plan metadata query changed emitted bytes")
				}
			}
			if !empty {
				for off := p.dataStart; off < p.dataStart+p.dataLength; {
					run, err := p.RunAt(off, p.Size())
					if err != nil || run.Kind() != sparse.Data {
						t.Fatalf("virtual payload reported as hole: %v", err)
					}
					off = run.End()
				}
				id, err := fileID(p.layout)
				if err != nil {
					t.Fatal(err)
				}
				_ = id
				st, err := os.Stat(layout)
				if err != nil {
					t.Fatal(err)
				}
				if st.Size() != int64(p.Size()) {
					t.Fatal("layout length")
				}
			}
			// Inspect extracted bytes and metadata, including hardlink identity.
			fsck, err := exec.LookPath("fsck.erofs")
			if err != nil {
				t.Skip("fsck.erofs not available for independent content verification")
			}
			extracted := filepath.Join(t.TempDir(), "extract")
			if out, err := exec.Command(fsck, "--extract="+extracted, full).CombinedOutput(); err != nil {
				t.Fatalf("fsck extraction: %s %v", out, err)
			}
			if !empty {
				for _, name := range []string{"payload", "duplicate", "sparse"} {
					a, e := os.ReadFile(filepath.Join(root, name))
					if e != nil {
						t.Fatal(e)
					}
					b, e := os.ReadFile(filepath.Join(extracted, name))
					if e != nil || !bytes.Equal(a, b) {
						t.Fatalf("extracted %s: %v", name, e)
					}
				}
				a, _ := os.Stat(filepath.Join(extracted, "payload"))
				b, _ := os.Stat(filepath.Join(extracted, "hardlink"))
				if !os.SameFile(a, b) || a.Mode().Perm() != 0640 {
					t.Fatal("hardlink/mode lost")
				}
				target, e := os.Readlink(filepath.Join(extracted, "symlink"))
				if e != nil || target != "payload" {
					t.Fatal("symlink lost")
				}
			}
		})
	}
}

func TestPlanRejectsCorruptionAndBounds(t *testing.T) {
	p, root, layout, name := makePlanFixture(t, false)
	_ = p
	for _, kind := range []string{"version", "features", "reserved", "overflow", "overlap", "source", "padding", "truncated-record", "truncated-layout", "source-path", "source-count-overflow", "digest", "budget"} {
		t.Run(kind, func(t *testing.T) {
			work := t.TempDir()
			dest := filepath.Join(work, "plan")
			lay := filepath.Join(work, "layout")
			for src, dst := range map[string]string{name: dest, name + ".chunks": dest + ".chunks", name + ".sources": dest + ".sources", layout: lay} {
				b, e := os.ReadFile(src)
				if e != nil {
					t.Fatal(e)
				}
				if e = os.WriteFile(dst, b, 0600); e != nil {
					t.Fatal(e)
				}
			}
			h, _ := os.ReadFile(dest)
			rec, _ := os.ReadFile(dest + ".chunks")
			sources, _ := os.ReadFile(dest + ".sources")
			limits := PlanLimits{}
			switch kind {
			case "version":
				h[8]++
			case "features":
				h[16] |= 2
			case "reserved":
				h[176] = 1
			case "overflow":
				binary.LittleEndian.PutUint64(h[48:], ^uint64(0))
			case "overlap":
				binary.LittleEndian.PutUint64(rec[64:], 0)
			case "source":
				binary.LittleEndian.PutUint64(rec[16:], ^uint64(0))
			case "padding":
				rec[28]++
			case "truncated-record":
				rec = rec[:len(rec)-1]
			case "truncated-layout":
				if err := os.Truncate(lay, 1); err != nil {
					t.Fatal(err)
				}
			case "source-count-overflow":
				binary.LittleEndian.PutUint64(h[56:], maxPlanSources+1)
			case "source-path":
				sources[80] = '/'
			case "digest":
				h[80] ^= 1
			case "budget":
				limits.PlanBytes = planHeaderSize
			}
			// Authenticate the malformed records, exercising structure checks rather
			// than merely making every case fail its file hash.
			rh, sh := sha256.Sum256(rec), sha256.Sum256(sources)
			copy(h[112:], rh[:])
			copy(h[144:], sh[:])
			for dst, b := range map[string][]byte{dest: h, dest + ".chunks": rec, dest + ".sources": sources} {
				if e := os.WriteFile(dst, b, 0600); e != nil {
					t.Fatal(e)
				}
			}
			got, err := openPlan(context.Background(), root, lay, dest, limits)
			if err == nil {
				got.Close()
				t.Fatal("corrupt plan accepted")
			}
		})
	}
}

func preparedFixture(t *testing.T, empty bool) *PreparedImage {
	t.Helper()
	var entries []layerEntry
	if !empty {
		entries = []layerEntry{{name: "payload", body: strings.Repeat("verified data", 120000)}}
	}
	b := testLayerTar(t, entries...)
	h := sha256.Sum256(b)
	id := fmt.Sprintf("sha256:%x", h)
	l := LayerInput{MediaType: "application/vnd.oci.image.layer.v1.tar", Digest: id, DiffID: id, Size: int64(len(b)), Open: func(context.Context) (io.ReadCloser, error) { return io.NopCloser(bytes.NewReader(b)), nil }}
	input, err := NewImageInput([]byte(fmt.Sprintf(`{"architecture":"amd64","os":"linux","rootfs":{"type":"layers","diff_ids":[%q]},"config":{"Cmd":["/bin/sh"]}}`, id)), []LayerInput{l})
	if err != nil {
		t.Fatal(err)
	}
	p, err := PrepareImage(context.Background(), input, Options{TmpDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { p.Close() })
	return p
}

func TestPreparedUmociPlanPipeToCurrentConsumer(t *testing.T) {
	for _, empty := range []bool{true, false} {
		t.Run(fmt.Sprint(empty), func(t *testing.T) {
			p := preparedFixture(t, empty)
			var raw bytes.Buffer
			if err := p.WriteRaw(context.Background(), &raw); err != nil {
				t.Fatal(err)
			}
			r, w := io.Pipe()
			done := make(chan error, 1)
			go func() {
				_, _, err := p.WriteArtifact(context.Background(), w, "image")
				w.CloseWithError(err)
				done <- err
			}()
			received, _, err := tarstream.SourceFrom(r, "image")
			if err != nil {
				r.Close()
				<-done
				t.Fatal(err)
			}
			got := readPlanSource(t, received)
			if err = <-done; err != nil {
				t.Fatal(err)
			}
			r.Close()
			if !bytes.Equal(got, raw.Bytes()) {
				t.Fatal("consumer bytes differ from raw API")
			}
			cfg, err := image.ReadConfig(bytes.NewReader(got), int64(len(got)))
			if err != nil || len(cfg.Cmd) != 1 || cfg.Cmd[0] != "/bin/sh" {
				t.Fatalf("runtime config: %v %v", cfg, err)
			}
			work := p.work
			if err = p.Close(); err != nil {
				t.Fatal(err)
			}
			if _, err = os.Stat(work); !os.IsNotExist(err) {
				t.Fatal("owned scratch not removed")
			}
		})
	}
}

type mutateWriter struct {
	bytes.Buffer
	mutate func()
	once   bool
}

func (w *mutateWriter) Write(b []byte) (int, error) {
	n, err := w.Buffer.Write(b)
	if !w.once && bytes.Contains(b, []byte("config.json")) {
		w.once = true
		w.mutate()
	}
	return n, err
}

func TestPreparedFinalMutationWithholdsSuccessTail(t *testing.T) {
	for _, empty := range []bool{false, true} {
		t.Run(fmt.Sprint(empty), func(t *testing.T) {
			p := preparedFixture(t, empty)
			w := &mutateWriter{mutate: func() {
				if err := os.WriteFile(filepath.Join(p.selection.root, "added"), []byte("late"), 0600); err != nil {
					t.Fatal(err)
				}
			}}
			_, digest, err := p.WriteArtifact(context.Background(), w, "image")
			if !w.once || err == nil || digest != "" {
				t.Fatalf("late mutation result: %v %s", err, digest)
			}
			if bytes.Contains(w.Bytes(), []byte(tarstream.DigestMarkerPrefix)) {
				t.Fatal("success marker escaped")
			}
		})
	}
}

func TestSelectionIgnoresAtimeAndExcludedChildren(t *testing.T) {
	root := t.TempDir()
	if err := os.Mkdir(filepath.Join(root, "scratch"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "file"), []byte("data"), 0600); err != nil {
		t.Fatal(err)
	}
	s, err := selectSource(context.Background(), root, []string{"scratch"}, 4<<20)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = os.ReadFile(filepath.Join(root, "file")); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(root, "scratch", "new"), []byte("excluded"), 0600); err != nil {
		t.Fatal(err)
	}
	if err = s.validate(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err = os.Chmod(filepath.Join(root, "file"), 0644); err != nil {
		t.Fatal(err)
	}
	if err = s.validate(context.Background()); err == nil {
		t.Fatal("included mode mutation ignored")
	}
}

func TestPlannedSourceReplacedByFIFOIsRejectedWithoutBlocking(t *testing.T) {
	p, root, _, _ := makePlanFixture(t, false)
	rec, err := p.record(0)
	if err != nil {
		t.Fatal(err)
	}
	name := filepath.Join(root, p.files[rec.source-1].name)
	if err = os.Remove(name); err != nil {
		t.Fatal(err)
	}
	if err = unix.Mkfifo(name, 0600); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { _, err := p.chunk(context.Background(), rec); done <- err }()
	select {
	case err = <-done:
		if err == nil {
			t.Fatal("replaced FIFO accepted")
		}
	case <-time.After(time.Second):
		// Release a regressed blocking FIFO opener before test-owned cleanup.
		fd, openErr := unix.Open(name, unix.O_WRONLY|unix.O_NONBLOCK, 0)
		if openErr != nil {
			t.Fatal(openErr)
		}
		unix.Close(fd)
		<-done
		t.Fatal("planned source open blocked on a replaced FIFO")
	}
}
