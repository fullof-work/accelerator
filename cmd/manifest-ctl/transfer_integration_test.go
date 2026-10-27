package main

import (
	"bytes"
	"context"
	"errors"
	"golang.org/x/sys/unix"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kuasar-sandbox/accelerator/pkg/manifest"
	"github.com/kuasar-sandbox/accelerator/pkg/manifest/bundle"
	"github.com/kuasar-sandbox/accelerator/pkg/manifest/chunker"
	manifestcrypto "github.com/kuasar-sandbox/accelerator/pkg/manifest/crypto"
	"github.com/kuasar-sandbox/accelerator/pkg/sparse"
	"github.com/kuasar-sandbox/accelerator/pkg/store"
	storeclient "github.com/kuasar-sandbox/accelerator/pkg/store/client"
	storefs "github.com/kuasar-sandbox/accelerator/pkg/store/fs"
	"github.com/kuasar-sandbox/accelerator/pkg/store/pb"
	storeserver "github.com/kuasar-sandbox/accelerator/pkg/store/server"
	"github.com/kuasar-sandbox/accelerator/pkg/tailzip"
	"github.com/kuasar-sandbox/accelerator/pkg/tarstream"
	"google.golang.org/grpc"
	"gopkg.in/yaml.v3"
)

type countedStore struct {
	*storeserver.Server
	admissions atomic.Int32
}

func (s *countedStore) AdmitWrite(ctx context.Context, r *pb.AdmitWriteRequest) (*pb.AdmitWriteResponse, error) {
	s.admissions.Add(1)
	return s.Server.AdmitWrite(ctx, r)
}
func transferConfig(t *testing.T, online bool) (*manifest.Config, string, *countedStore) {
	t.Helper()
	t.Setenv(manifest.CustomerKeyEnv, "")
	cfg := &manifest.Config{Manifest: manifest.ManifestSubConfig{Key: strings.Repeat("ab", 32), WriteGeneration: "G1"}, Chunker: chunker.Config{Mode: "fixed", Fixed: chunker.FixedConfig{Size: "4096"}}, Crypto: manifestcrypto.Config{Chunk: "aes", Manifest: "aes", Local: manifestcrypto.LocalOff}}
	var server *countedStore
	if online {
		socket := filepath.Join(t.TempDir(), "s.sock")
		listener, err := net.Listen("unix", socket)
		if err != nil {
			t.Fatal(err)
		}
		backend, err := storefs.New(storefs.Config{Root: t.TempDir()})
		if err != nil {
			t.Fatal(err)
		}
		service, err := storeserver.New(storeserver.Options{Backend: backend, Generations: func() []store.Generation { return []store.Generation{"G1"} }, VerifyKey: true})
		if err != nil {
			t.Fatal(err)
		}
		server = &countedStore{Server: service}
		grpcServer := grpc.NewServer()
		pb.RegisterStoreServer(grpcServer, server)
		go func() { _ = grpcServer.Serve(listener) }()
		t.Cleanup(func() { grpcServer.Stop(); _ = listener.Close() })
		cfg.Store = manifest.StoreConfig{Endpoint: socket, Pool: 2, Timeout: "5s"}
	}
	body, err := yaml.Marshal(cfg)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "manifest.yaml")
	if err = os.WriteFile(path, body, 0600); err != nil {
		t.Fatal(err)
	}
	return cfg, path, server
}
func artifactFixture(t *testing.T, body []byte, holes []sparse.Extent, tail []byte) (string, []byte, string) {
	t.Helper()
	src, err := sparse.NewSource(bytes.NewReader(body), uint64(len(body)), holes)
	if err != nil {
		t.Fatal(err)
	}
	if tail != nil {
		src, err = tailzip.Append(src, tail, tailzip.Options{})
		if err != nil {
			t.Fatal(err)
		}
	}
	var encoded bytes.Buffer
	_, digest, err := tarstream.WriteTo(context.Background(), &encoded, "image", src)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "source.tar")
	if err = os.WriteFile(path, encoded.Bytes(), 0600); err != nil {
		t.Fatal(err)
	}
	return path, encoded.Bytes(), digest
}
func invokeTransfer(t *testing.T, config string, storeCommand bool, input io.Reader, args ...string) ([]byte, []byte, error) {
	t.Helper()
	var stdout, stderr bytes.Buffer
	full := append([]string{"--manifest-config", config, "--no-progress"}, args...)
	err := runTransfer(context.Background(), full, storeCommand, transferIO{input, &stdout, &stderr})
	return stdout.Bytes(), stderr.Bytes(), err
}
func readArtifact(t *testing.T, data []byte) ([]byte, sparse.Source) {
	t.Helper()
	src, _, err := tarstream.SourceAt(bytes.NewReader(data), int64(len(data)), "")
	if err != nil {
		t.Fatal(err)
	}
	body := make([]byte, src.Size())
	if len(body) > 0 {
		n, err := src.ReadAt(context.Background(), body, 0)
		if err != nil || n != len(body) {
			t.Fatalf("artifact read %d %v", n, err)
		}
	}
	return body, src
}
func TestTransferLocalPathsTailModesAndExpectedIdentity(t *testing.T) {
	_, config, _ := transferConfig(t, false)
	oldTail, _ := tailzip.Encode([]tailzip.Entry{{Name: "config.json", Body: []byte("old")}})
	newTail, _ := tailzip.Encode([]tailzip.Entry{{Name: "config.json", Body: []byte("new")}})
	source, _, digest := artifactFixture(t, []byte("data"), nil, oldTail)
	plain, _, _ := artifactFixture(t, []byte("data"), nil, nil)
	appendPath := filepath.Join(t.TempDir(), "tail.zip")
	if err := os.WriteFile(appendPath, newTail, 0600); err != nil {
		t.Fatal(err)
	}
	t.Run("bare local path", func(t *testing.T) {
		out, _, err := invokeTransfer(t, config, false, nil, source)
		if err != nil {
			t.Fatal(err)
		}
		got, _ := readArtifact(t, out)
		if !bytes.Equal(got, append([]byte("data"), oldTail...)) {
			t.Fatal("content changed")
		}
	})
	t.Run("extract only", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "extracted.zip")
		out, _, err := invokeTransfer(t, config, false, nil, "--output-mode=none", "--extract-tail", path, source)
		if err != nil {
			t.Fatal(err)
		}
		got, err := os.ReadFile(path)
		if err != nil || !bytes.Equal(got, oldTail) || len(out) != 0 {
			t.Fatalf("extracted %x output %x error %v", got, out, err)
		}
	})
	t.Run("append bare payload", func(t *testing.T) {
		out, _, err := invokeTransfer(t, config, false, nil, "--append-tail", appendPath, plain)
		if err != nil {
			t.Fatal(err)
		}
		got, _ := readArtifact(t, out)
		if !bytes.Equal(got, append([]byte("data"), newTail...)) {
			t.Fatal("append differs")
		}
	})
	t.Run("replace after window", func(t *testing.T) {
		out, _, err := invokeTransfer(t, config, false, nil, "--strip-tail", "--append-tail", appendPath, "--offset=1", "--length=2", source)
		if err != nil {
			t.Fatal(err)
		}
		got, _ := readArtifact(t, out)
		if !bytes.Equal(got, append([]byte("at"), newTail...)) {
			t.Fatalf("window/tail order %x", got)
		}
	})
	t.Run("existing tail requires strip", func(t *testing.T) {
		_, _, err := invokeTransfer(t, config, false, nil, "--append-tail", appendPath, source)
		if err == nil {
			t.Fatal("missing strip accepted")
		}
	})
	t.Run("located digest", func(t *testing.T) {
		ref := "file://source.tar@digest:" + digest + "@location:input"
		out, _, err := invokeTransfer(t, config, false, nil, "--ref-location", "input=file://"+filepath.Dir(source), ref)
		if err != nil {
			t.Fatal(err)
		}
		got, _ := readArtifact(t, out)
		if !bytes.Equal(got, append([]byte("data"), oldTail...)) {
			t.Fatal("located bytes changed")
		}
	})
	t.Run("wrong digest", func(t *testing.T) {
		_, _, err := invokeTransfer(t, config, false, nil, "file://"+source+"@digest:"+strings.Repeat("0", 64))
		if !errors.Is(err, tarstream.ErrDigestMismatch) {
			t.Fatalf("expected identity ignored: %v", err)
		}
	})
}
func TestTransferMultiLayerPreservesHoleAndWrittenZero(t *testing.T) {
	_, config, _ := transferConfig(t, false)
	top := append(bytes.Repeat([]byte{'A'}, 4096), make([]byte, 4*4096)...)
	a, _, _ := artifactFixture(t, top, []sparse.Extent{{Offset: 4096, Size: 4096}, {Offset: 3 * 4096, Size: 2 * 4096}}, nil)
	lower := append(bytes.Repeat([]byte{'B'}, 4*4096), make([]byte, 4096)...)
	b, _, _ := artifactFixture(t, lower, []sparse.Extent{{Offset: 4 * 4096, Size: 4096}}, nil)
	out, _, err := invokeTransfer(t, config, false, nil, a, b)
	if err != nil {
		t.Fatal(err)
	}
	got, src := readArtifact(t, out)
	want := append(bytes.Repeat([]byte{'A'}, 4096), bytes.Repeat([]byte{'B'}, 4096)...)
	want = append(want, make([]byte, 4096)...)
	want = append(want, bytes.Repeat([]byte{'B'}, 4096)...)
	want = append(want, make([]byte, 4096)...)
	if !bytes.Equal(got, want) {
		t.Fatalf("layer result %x want %x", got, want)
	}
	hole, err := src.RunAt(4*4096, 4096)
	if err != nil || hole.Kind() != sparse.Hole {
		t.Fatalf("lost hole: %v %v", hole, err)
	}
	present, err := src.RunAt(2*4096, 4096)
	if err != nil || present.Kind() == sparse.Hole {
		t.Fatalf("written zero became hole: %v %v", present, err)
	}
}
func TestTransferStoreBundleShareAdmissionAndEveryEncodedObject(t *testing.T) {
	cfg, config, server := transferConfig(t, true)
	data := append(bytes.Repeat([]byte{0x55}, 4096), make([]byte, 8192)...)
	path, _, _ := artifactFixture(t, data, []sparse.Extent{{Offset: 8192, Size: 4096}}, nil)
	output := filepath.Join(t.TempDir(), "result.bundle")
	stdout, _, err := invokeTransfer(t, config, false, nil, "--store", "--output-mode=bundle", "--output", output, "--extra-salt", "tenant-a", path)
	if err != nil {
		t.Fatal(err)
	}
	if server.admissions.Load() != 1 {
		t.Fatalf("admission acquired %d times", server.admissions.Load())
	}
	key, err := manifest.ParseHexKey(strings.TrimSpace(string(stdout)))
	if err != nil {
		t.Fatalf("stdout is not consumer key: %s %v", stdout, err)
	}
	reader, err := bundle.Open(output)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	keys := reader.ManifestKeys()
	if len(keys) != 1 || keys[0] != key {
		t.Fatal("Store and Bundle roots differ")
	}
	client, err := storeclient.New(cfg.Store.Endpoint, 2, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	for partition, keys := range map[store.Partition][]store.ContentKey{store.PartitionManifest: reader.ManifestKeys(), store.PartitionChunk: reader.ChunkKeys()} {
		for _, key := range keys {
			found, remote, err := client.Get(context.Background(), partition, key)
			if err != nil || !found {
				t.Fatalf("Store object missing %v", err)
			}
			_, local, err := reader.Getter().Get(context.Background(), partition, key)
			if err != nil {
				t.Fatal(err)
			}
			same := bytes.Equal(remote, local.Bytes())
			local.Release()
			if !same {
				t.Fatal("encoded object mismatch")
			}
		}
	}
	output2, _, err := invokeTransfer(t, config, true, nil, "--extra-salt", "tenant-a", output)
	if err != nil {
		t.Fatal(err)
	}
	if strings.TrimSpace(string(output2)) != manifest.HexKey(key) {
		t.Fatal("store did not accept Bundle as the same logical source")
	}
	bundleBytes, _, err := invokeTransfer(t, config, false, nil, "--output-mode=bundle", "--extra-salt", "tenant-a", path)
	if err != nil {
		t.Fatal(err)
	}
	r2, err := bundle.NewReader(bytes.NewReader(bundleBytes), int64(len(bundleBytes)))
	if err != nil {
		t.Fatalf("Bundle stdout corrupted by text: %v", err)
	}
	defer r2.Close()
	if r2.ManifestKeys()[0] != key {
		t.Fatal("Bundle-only extra-salt differs")
	}
}
func TestTransferPipeTailReplacementAndTrailerVerification(t *testing.T) {
	cfg, config, _ := transferConfig(t, true)
	oldTail, _ := tailzip.Encode([]tailzip.Entry{{Name: "config.json", Body: []byte("old")}})
	newTail, _ := tailzip.Encode([]tailzip.Entry{{Name: "config.json", Body: []byte("new")}})
	_, encoded, _ := artifactFixture(t, []byte("payload"), nil, oldTail)
	appendPath := filepath.Join(t.TempDir(), "new.zip")
	os.WriteFile(appendPath, newTail, 0600)
	extracted := filepath.Join(t.TempDir(), "old.zip")
	stdout, _, err := invokeTransfer(t, config, true, bytes.NewBuffer(encoded), "--strip-tail", "--append-tail", appendPath, "--extract-tail", extracted, "-")
	if err != nil {
		t.Fatal(err)
	}
	key, err := manifest.ParseHexKey(strings.TrimSpace(string(stdout)))
	if err != nil {
		t.Fatal(err)
	}
	fetcher, err := cfg.NewFetcher()
	if err != nil {
		t.Fatal(err)
	}
	defer fetcher.Close()
	stream, err := fetcher.OpenManifest(context.Background(), key)
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()
	got := make([]byte, stream.Size())
	n, err := stream.ReadAt(context.Background(), got, 0)
	if err != nil || n != len(got) {
		t.Fatalf("read stored %d %v", n, err)
	}
	if !bytes.Equal(got, append([]byte("payload"), newTail...)) {
		t.Fatal("pipe tail replacement failed")
	}
	saved, err := os.ReadFile(extracted)
	if err != nil || !bytes.Equal(saved, oldTail) {
		t.Fatal("original pipe tail was not extracted")
	}
	bad := bytes.Clone(encoded)
	bad[len(bad)-1] = 1
	out := filepath.Join(t.TempDir(), "invalid.bundle")
	_, _, err = invokeTransfer(t, config, false, bytes.NewBuffer(bad), "--strip-tail", "--output-mode=bundle", "--output", out, "-")
	if err == nil {
		t.Fatal("bad pipe trailer accepted after stripping")
	}
	if _, err := os.Stat(out); !os.IsNotExist(err) {
		t.Fatalf("invalid final remains: %v", err)
	}
}
func TestTransferPreflightAndOwnedOutput(t *testing.T) {
	_, config, _ := transferConfig(t, false)
	src, _, _ := artifactFixture(t, []byte("safe"), nil, nil)
	before, _ := os.ReadFile(src)
	if _, _, err := invokeTransfer(t, config, false, nil, "--output", src, src); err == nil {
		t.Fatal("source alias accepted")
	}
	after, _ := os.ReadFile(src)
	if !bytes.Equal(before, after) {
		t.Fatal("source overwritten")
	}
	huge := filepath.Join(t.TempDir(), "huge-tail.zip")
	f, err := os.Create(huge)
	if err != nil {
		t.Fatal(err)
	}
	f.Truncate(maxTransferTail + 1)
	f.Close()
	if _, _, err := invokeTransfer(t, config, false, nil, "--append-tail", huge, src); err == nil {
		t.Fatal("oversized tail accepted")
	}
	path := filepath.Join(t.TempDir(), "result")
	file, finish, err := openDirectOutput(path)
	if err != nil {
		t.Fatal(err)
	}
	file.Write([]byte("owned"))
	os.Remove(path)
	os.WriteFile(path, []byte("replacement"), 0600)
	if err := finish(errors.New("injected failure")); err == nil {
		t.Fatal("path replacement not rejected")
	}
	got, err := os.ReadFile(path)
	if err != nil || string(got) != "replacement" {
		t.Fatal("another writer's inode removed")
	}
}

func TestTransferStoreAndTarstreamConsumePipeOnce(t *testing.T) {
	cfg, config, server := transferConfig(t, true)
	tail, _ := tailzip.Encode([]tailzip.Entry{{Name: "config.json", Body: []byte("original")}})
	_, encoded, _ := artifactFixture(t, []byte("payload"), nil, tail)
	output := filepath.Join(t.TempDir(), "dual.tar")
	stdout, _, err := invokeTransfer(t, config, false, bytes.NewBuffer(encoded), "--store", "--output-mode=tarstream", "--output", output, "--strip-tail", "-")
	if err != nil {
		t.Fatal(err)
	}
	if server.admissions.Load() != 1 {
		t.Fatalf("admission count=%d", server.admissions.Load())
	}
	key, err := manifest.ParseHexKey(strings.TrimSpace(string(stdout)))
	if err != nil {
		t.Fatal(err)
	}
	body, err := os.ReadFile(output)
	if err != nil {
		t.Fatal(err)
	}
	got, _ := readArtifact(t, body)
	if string(got) != "payload" {
		t.Fatalf("tarstream=%q", got)
	}
	fetcher, err := cfg.NewFetcher()
	if err != nil {
		t.Fatal(err)
	}
	defer fetcher.Close()
	stream, err := fetcher.OpenManifest(context.Background(), key)
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()
	stored := make([]byte, stream.Size())
	if _, err := stream.ReadAt(context.Background(), stored, 0); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, stored) {
		t.Fatal("pipe dual outputs differ")
	}
}

type injectedTransferWriter struct{ err error }

func (w injectedTransferWriter) Write([]byte) (int, error) { return 0, w.err }
func TestTransferDualOutputFailureDoesNotStall(t *testing.T) {
	cfg, _, _ := transferConfig(t, true)
	key, err := cfg.CustomerKey()
	if err != nil {
		t.Fatal(err)
	}
	src, err := sparse.NewSource(bytes.NewReader([]byte("payload")), 7, nil)
	if err != nil {
		t.Fatal(err)
	}
	failure := errors.New("injected output failure")
	done := make(chan error, 1)
	go func() {
		_, err := writeTransfer(context.Background(), cfg, key, src, "tarstream", injectedTransferWriter{failure}, "image", true, nil, nil, false, func() error { return nil })
		done <- err
	}()
	select {
	case err := <-done:
		if !errors.Is(err, failure) {
			t.Fatalf("error=%v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("dual output stalled after sink failure")
	}
}

func TestTransferRejectsNonSeekablePathWithoutWaitingForWriter(t *testing.T) {
	_, config, _ := transferConfig(t, false)
	fifo := filepath.Join(t.TempDir(), "input.pipe")
	if err := unix.Mkfifo(fifo, 0600); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { _, _, err := invokeTransfer(t, config, false, nil, fifo); done <- err }()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("FIFO accepted as a random-access ref")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("input capability check blocked waiting for FIFO writer")
	}
}
