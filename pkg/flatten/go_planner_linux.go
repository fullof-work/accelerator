//go:build linux

package flatten

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"hash"
	"io"
	"io/fs"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"unsafe"

	erofs "github.com/kuasar-sandbox/go-erofs"
	"golang.org/x/sys/unix"
)

// The legacy public NativeMemoryBytes field now admits controlled Go planning
// allocations. Half belongs to the library; half to source/block indexing.
// Selection and the eventual reader/writer retain their IndexMemoryBytes shares.
// Reservations include capacity-growth overlap, but are not a process RSS cap.
type planningMemory struct{ used, peak, limit uint64 }

func (m *planningMemory) reserve(n uint64) error {
	if n > m.limit-m.used {
		return fmt.Errorf("flatten: planning memory budget exceeded")
	}
	m.used += n
	m.peak = max(m.peak, m.used)
	return nil
}
func (m *planningMemory) release(n uint64) { m.used -= n }

type planningSlot[K comparable, V any] struct {
	key      K
	value    V
	occupied bool
}
type planningTable[K comparable, V any] struct {
	slots  []planningSlot[K, V]
	count  uint64
	hash   func(K) uint64
	memory *planningMemory
}

func (t *planningTable[K, V]) find(key K) (V, bool) {
	var zero V
	if len(t.slots) == 0 {
		return zero, false
	}
	mask := uint64(len(t.slots) - 1)
	for i := t.hash(key) & mask; ; i = (i + 1) & mask {
		s := &t.slots[i]
		if !s.occupied {
			return zero, false
		}
		if s.key == key {
			return s.value, true
		}
	}
}
func (t *planningTable[K, V]) insert(ctx context.Context, key K, value V) error {
	if t.count+1 > uint64(len(t.slots))*3/4 {
		capacity := max(256, uint64(len(t.slots))*2)
		size := uint64(unsafe.Sizeof(planningSlot[K, V]{}))
		if capacity > uint64(math.MaxInt)/size {
			return fmt.Errorf("flatten: planning table capacity overflow")
		}
		charge := capacity*size + 64
		if err := t.memory.reserve(charge); err != nil {
			return err
		}
		slots := make([]planningSlot[K, V], int(capacity))
		mask := capacity - 1
		for j, s := range t.slots {
			if j%1024 == 0 {
				if err := ctx.Err(); err != nil {
					return err
				}
			}
			if !s.occupied {
				continue
			}
			i := t.hash(s.key) & mask
			for slots[i].occupied {
				i = (i + 1) & mask
			}
			slots[i] = s
		}
		if len(t.slots) > 0 {
			t.memory.release(uint64(len(t.slots))*size + 64)
		}
		t.slots = slots
	}
	mask := uint64(len(t.slots) - 1)
	i := t.hash(key) & mask
	for t.slots[i].occupied {
		i = (i + 1) & mask
	}
	t.slots[i] = planningSlot[K, V]{key: key, value: value, occupied: true}
	t.count++
	return nil
}

type inodeKey struct{ dev, ino uint64 }

func inodeHash(k inodeKey) uint64 {
	x := k.ino ^ (k.dev * 0x9e3779b97f4a7c15)
	x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9
	x = (x ^ (x >> 27)) * 0x94d049bb133111eb
	return x ^ (x >> 31)
}

type goPlanner struct {
	ctx                                          context.Context
	selection                                    *sourceSelection
	limits                                       PlanLimits
	memory                                       planningMemory
	builder                                      *erofs.LayoutBuilder
	parents                                      [2049]erofs.NodeID // paths are limited to 4096 bytes by selection
	depth                                        int
	root                                         *os.Root
	rootDescriptor                               *os.File
	blocks                                       planningTable[[32]byte, uint64]
	links                                        planningTable[inodeKey, erofs.NodeID]
	buffer                                       []byte
	sums                                         [planReadBatch / planBlockSize][sha256.Size]byte
	hashWorkers                                  int
	chunks, sources                              *bufio.Writer
	chunkHash, sourceHash                        hash.Hash
	records, sourceCount, sourceBytes, planBytes uint64
}

func buildGoPlan(ctx context.Context, selection *sourceSelection, layoutPath, name string, limits PlanLimits) (retErr error) {
	if err := ctx.Err(); err != nil {
		return err
	}
	memory := planningMemory{limit: limits.NativeMemoryBytes / 2}
	// Batch reads and digests, two buffered plan streams, fixed parent stack and
	// bounded metadata hashing workspace. Admit before allocating/opening.
	if err := memory.reserve(2 * planReadBatch); err != nil {
		return err
	}
	if limits.PlanBytes < planHeaderSize {
		return fmt.Errorf("flatten: plan byte budget exceeded")
	}
	g := &goPlanner{ctx: ctx, selection: selection, limits: limits, planBytes: planHeaderSize, memory: memory}
	g.buffer = make([]byte, planReadBatch)
	// Bound hashing CPU pressure as well as buffer memory.
	g.hashWorkers = min(2, runtime.GOMAXPROCS(0))
	g.blocks = planningTable[[32]byte, uint64]{hash: func(k [32]byte) uint64 { return binary.LittleEndian.Uint64(k[:8]) }, memory: &g.memory}
	g.links = planningTable[inodeKey, erofs.NodeID]{hash: inodeHash, memory: &g.memory}
	var err error
	if g.root, err = os.OpenRoot(selection.root); err != nil {
		return err
	}
	defer func() { retErr = errors.Join(retErr, g.root.Close()) }()
	if g.rootDescriptor, err = g.root.OpenFile(".", unix.O_PATH|unix.O_DIRECTORY, 0); err != nil {
		return err
	}
	defer func() { retErr = errors.Join(retErr, g.rootDescriptor.Close()) }()
	var files []*os.File
	defer func() {
		for _, f := range files {
			retErr = errors.Join(retErr, f.Close())
		}
	}()
	for _, path := range []string{layoutPath, name + ".chunks", name + ".sources", name} {
		f, e := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0600)
		if e != nil {
			return e
		}
		files = append(files, f)
	}
	g.chunkHash, g.sourceHash = sha256.New(), sha256.New()
	g.chunks = bufio.NewWriterSize(io.MultiWriter(files[1], g.chunkHash), 64<<10)
	g.sources = bufio.NewWriterSize(io.MultiWriter(files[2], g.sourceHash), 64<<10)
	defer func() {
		if g.builder != nil {
			retErr = errors.Join(retErr, g.builder.Close())
		}
	}()
	h := sha256.New()
	remaining := selection.budget
	if err = selection.walk(ctx, ".", h, &remaining, g.visit); err != nil {
		return err
	}
	if got := [32]byte(h.Sum(nil)); got != selection.stamp {
		return fmt.Errorf("flatten: selected source tree changed during planning")
	}
	l, err := g.builder.Prepare(ctx, g.records)
	if err != nil {
		return fmt.Errorf("flatten: EROFS layout: %w", err)
	}
	defer func() { retErr = errors.Join(retErr, l.Close()) }()
	if err = g.chargePlan(l.Size() - l.DataSize()); err != nil {
		return err
	}
	if err = files[0].Truncate(int64(l.Size())); err != nil {
		return err
	}
	if err = l.WriteMetadata(ctx, files[0]); err != nil {
		return err
	}
	if err = g.chunks.Flush(); err != nil {
		return err
	}
	if err = g.sources.Flush(); err != nil {
		return err
	}
	var header [planHeaderSize]byte
	copy(header[:], "KERPLAN\x00")
	binary.LittleEndian.PutUint32(header[8:], 1)
	binary.LittleEndian.PutUint32(header[12:], planHeaderSize)
	for _, v := range []struct {
		offset int
		value  uint64
	}{{16, 1}, {24, l.Size()}, {32, l.DataOffset()}, {40, l.DataSize()}, {48, g.records}, {56, g.sourceCount}, {64, g.records * planRecordSize}, {72, g.sourceBytes}} {
		binary.LittleEndian.PutUint64(header[v.offset:], v.value)
	}
	digest, err := hashPlannedMetadata(ctx, files[0], l.Size(), l.DataOffset(), l.DataSize(), g.buffer)
	if err != nil {
		return err
	}
	copy(header[80:112], digest[:])
	copy(header[112:144], g.chunkHash.Sum(nil))
	copy(header[144:176], g.sourceHash.Sum(nil))
	n, err := files[3].Write(header[:])
	if err == nil && n != len(header) {
		err = io.ErrShortWrite
	}
	if err != nil {
		return err
	}
	return ctx.Err()
}

func (g *goPlanner) chargePlan(n uint64) error {
	if n > g.limits.PlanBytes-g.planBytes {
		return fmt.Errorf("flatten: plan byte budget exceeded")
	}
	g.planBytes += n
	return nil
}

func nodeMetadata(st *unix.Stat_t) erofs.Node {
	mode := fs.FileMode(st.Mode & 0777)
	if st.Mode&unix.S_ISUID != 0 {
		mode |= fs.ModeSetuid
	}
	if st.Mode&unix.S_ISGID != 0 {
		mode |= fs.ModeSetgid
	}
	if st.Mode&unix.S_ISVTX != 0 {
		mode |= fs.ModeSticky
	}
	switch st.Mode & unix.S_IFMT {
	case unix.S_IFDIR:
		mode |= fs.ModeDir
	case unix.S_IFLNK:
		mode |= fs.ModeSymlink
	case unix.S_IFIFO:
		mode |= fs.ModeNamedPipe
	case unix.S_IFSOCK:
		mode |= fs.ModeSocket
	case unix.S_IFCHR:
		mode |= fs.ModeDevice | fs.ModeCharDevice
	case unix.S_IFBLK:
		mode |= fs.ModeDevice
	case unix.S_IFREG:
	default:
		mode |= fs.ModeIrregular
	}
	n := erofs.Node{Mode: mode, UID: st.Uid, GID: st.Gid}
	if mode.IsRegular() {
		n.Size = uint64(st.Size)
	}
	// Linux new_encode_dev, used by EROFS's 32-bit i_rdev field.
	if mode&fs.ModeDevice != 0 {
		major, minor := unix.Major(uint64(st.Rdev)), unix.Minor(uint64(st.Rdev))
		n.Rdev = (minor & 0xff) | (major << 8) | ((minor & 0xfffff00) << 12)
	}
	return n
}

func (g *goPlanner) visit(rel string, st *unix.Stat_t, leave bool) error {
	if leave {
		g.depth--
		return nil
	}
	if st.Mode&unix.S_IFMT == unix.S_IFCHR || st.Mode&unix.S_IFMT == unix.S_IFBLK {
		if unix.Major(uint64(st.Rdev)) > 0xfff || unix.Minor(uint64(st.Rdev)) > 0xfffff {
			return fmt.Errorf("flatten: device number exceeds EROFS representation")
		}
	}
	n := nodeMetadata(st)
	if rel == "." {
		var err error
		g.builder, err = erofs.NewLayoutBuilder(n, erofs.LayoutOptions{BlockSize: planBlockSize, MemoryLimit: g.limits.NativeMemoryBytes - g.memory.limit, MetadataLimit: g.limits.PlanBytes})
		g.depth = 1
		return err
	}
	if g.depth < 1 || g.depth >= len(g.parents) {
		return fmt.Errorf("flatten: selected directory depth exceeds path bound")
	}
	parent := g.parents[g.depth-1]
	key := inodeKey{uint64(st.Dev), st.Ino}
	if !n.Mode.IsDir() && st.Nlink > 1 {
		if target, ok := g.links.find(key); ok {
			_, err := g.builder.Link(parent, filepath.Base(rel), target)
			return err
		}
	}
	if n.Mode&fs.ModeSymlink != 0 {
		target, err := g.root.Readlink(rel)
		if err != nil {
			return err
		}
		n.LinkTarget = target
	}
	id, err := g.builder.Add(parent, filepath.Base(rel), n)
	if err != nil {
		return err
	}
	if n.Mode.IsDir() {
		g.parents[g.depth] = id
		g.depth++
		return nil
	}
	if st.Nlink > 1 {
		if err = g.links.insert(g.ctx, key, id); err != nil {
			return err
		}
	}
	if n.Mode.IsRegular() {
		return g.file(rel, statID(st), id)
	}
	return nil
}

func (g *goPlanner) file(rel string, expected fileIdentity, id erofs.NodeID) (retErr error) {
	f, err := openPlanSource(g.root, g.rootDescriptor, rel)
	if err != nil {
		return err
	}
	defer func() { retErr = errors.Join(retErr, f.Close()) }()
	actual, err := descriptorID(f.fd)
	if err != nil {
		return err
	}
	if actual != expected || actual.mode&unix.S_IFMT != unix.S_IFREG {
		return fmt.Errorf("flatten: selected file changed: %s", rel)
	}
	var source uint64
	for offset := uint64(0); offset < actual.size; {
		want := min(uint64(len(g.buffer)), actual.size-offset)
		n, err := f.readAt(g.ctx, g.buffer[:want], int64(offset))
		if err != nil || uint64(n) != want {
			return fmt.Errorf("flatten: read selected file %s: %w", rel, errors.Join(err, io.ErrUnexpectedEOF))
		}
		padded := (want + planBlockSize - 1) &^ (uint64(planBlockSize) - 1)
		clear(g.buffer[want:padded])
		if err = g.hashBatch(int(padded / planBlockSize)); err != nil {
			return err
		}
		for pos := uint64(0); pos < want; pos += planBlockSize {
			if err = g.ctx.Err(); err != nil {
				return err
			}
			block := g.buffer[pos : pos+planBlockSize]
			sum := g.sums[pos/planBlockSize]
			physical, ok := g.blocks.find(sum)
			if !ok {
				if g.records >= math.MaxUint32-1 {
					return fmt.Errorf("flatten: EROFS block address range exceeded")
				}
				if source == 0 {
					if err = g.addSource(rel, actual); err != nil {
						return err
					}
					source = g.sourceCount
				}
				if err = g.chargePlan(planRecordSize); err != nil {
					return err
				}
				physical = g.records
				if err = g.blocks.insert(g.ctx, sum, physical); err != nil {
					return err
				}
				length := uint32(min(uint64(planBlockSize), want-pos))
				digest := sum
				if length != planBlockSize {
					digest = sha256.Sum256(block[:length])
				}
				var record [planRecordSize]byte
				binary.LittleEndian.PutUint64(record[0:], physical*planBlockSize)
				binary.LittleEndian.PutUint64(record[8:], offset+pos)
				binary.LittleEndian.PutUint64(record[16:], source)
				binary.LittleEndian.PutUint32(record[24:], length)
				binary.LittleEndian.PutUint32(record[28:], planBlockSize-length)
				copy(record[32:], digest[:])
				if _, err = g.chunks.Write(record[:]); err != nil {
					return err
				}
				g.records++
			}
			if err = g.builder.AddBlocks(id, physical, 1); err != nil {
				return err
			}
		}
		offset += want
	}
	actual, err = descriptorID(f.fd)
	if err != nil {
		return err
	}
	if actual != expected {
		return fmt.Errorf("flatten: source changed while planning: %s", rel)
	}
	return nil
}

// hashBatch uses the already-admitted read buffer and fixed digest array.
// Hashing may run in parallel; allocation and deduplication still follow source
// order. Every worker is joined before the buffer is reused or cleanup begins.
func (g *goPlanner) hashBatch(blocks int) error {
	workers := g.hashWorkers
	if blocks < 64 {
		workers = 1
	}
	if workers == 1 {
		g.hashRange(0, blocks)
		return g.ctx.Err()
	}
	var group sync.WaitGroup
	for worker := 0; worker < workers; worker++ {
		start, end := blocks*worker/workers, blocks*(worker+1)/workers
		group.Add(1)
		go func() {
			defer group.Done()
			g.hashRange(start, end)
		}()
	}
	group.Wait()
	return g.ctx.Err()
}

func (g *goPlanner) hashRange(start, end int) {
	for index := start; index < end; index++ {
		if g.ctx.Err() != nil {
			return
		}
		pos := index * planBlockSize
		block := g.buffer[pos : pos+planBlockSize]
		if index > start && bytes.Equal(block, g.buffer[pos-planBlockSize:pos]) {
			// Compare all 4096 bytes before reusing the exact full SHA-256.
			g.sums[index] = g.sums[index-1]
		} else {
			g.sums[index] = sha256.Sum256(block)
		}
	}
}

func (g *goPlanner) addSource(rel string, id fileIdentity) error {
	if !validPlanPath(rel) || len(rel) > 4096 || g.sourceCount >= maxPlanSources {
		return fmt.Errorf("flatten: invalid planned source")
	}
	n := uint64(80 + len(rel))
	if err := g.chargePlan(n); err != nil {
		return err
	}
	// Match the eventual reader's admission before growing the on-disk index.
	count, bytes := g.sourceCount+1, g.sourceBytes+n
	indexLimit := g.limits.IndexMemoryBytes / 2
	// Bound each term before multiplication now that source count has no
	// arbitrary million-entry cap. These match openPlan's pre-allocation checks.
	if bytes > indexLimit/2 || count > indexLimit/256 ||
		2*bytes+count*256+2*planReadBatch > indexLimit {
		return fmt.Errorf("flatten: plan index memory budget exceeded")
	}
	var b [80]byte
	for i, v := range []uint64{count, id.dev, id.ino, id.size, uint64(id.mtime), uint64(id.ctime)} {
		binary.LittleEndian.PutUint64(b[i*8:], v)
	}
	for i, v := range []uint32{id.mtimeNS, id.ctimeNS, id.mode, id.uid, id.gid, uint32(len(rel))} {
		binary.LittleEndian.PutUint32(b[48+i*4:], v)
	}
	binary.LittleEndian.PutUint64(b[72:], id.nlink)
	if _, err := g.sources.Write(b[:]); err != nil {
		return err
	}
	if _, err := g.sources.WriteString(rel); err != nil {
		return err
	}
	g.sourceCount, g.sourceBytes = count, bytes
	return nil
}

func hashPlannedMetadata(ctx context.Context, f *os.File, size, start, length uint64, buffer []byte) ([32]byte, error) {
	h := sha256.New()
	for _, span := range [][2]uint64{{0, start}, {start + length, size}} {
		for offset := span[0]; offset < span[1]; {
			if err := ctx.Err(); err != nil {
				return [32]byte{}, err
			}
			want := min(uint64(len(buffer)), span[1]-offset)
			n, err := f.ReadAt(buffer[:want], int64(offset))
			if err != nil || uint64(n) != want {
				return [32]byte{}, errors.Join(err, io.ErrUnexpectedEOF)
			}
			h.Write(buffer[:n])
			offset += want
		}
	}
	return [32]byte(h.Sum(nil)), nil
}
