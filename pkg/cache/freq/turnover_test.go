package freq

import (
	"sync"
	"testing"
)

func TestCountAndByteLimitsRollOnlyOnce(t *testing.T) {
	s := New(Config{Counters: 1024, ResetAfter: 2, ResetBytes: 8})
	defer s.Close()
	key := tkey(1)
	s.TouchBytes(key, 4)
	s.TouchBytes(key, 4)
	if got := s.Estimate(key); got != 1 {
		t.Fatalf("simultaneous count/byte rollover: got %d, want 1", got)
	}
	s.Touch(key)
	if got := s.Estimate(key); got != 2 {
		t.Fatalf("new generation plus previous history: got %d, want 2", got)
	}
}

func TestRolloverRestartsBothProgressCounters(t *testing.T) {
	t.Run("count clears bytes", func(t *testing.T) {
		s := New(Config{Counters: 1024, ResetAfter: 3, ResetBytes: 10})
		defer s.Close()
		key := tkey(2)
		s.TouchBytes(key, 4)
		s.TouchBytes(key, 4)
		s.Touch(key) // Count rolls with only eight bytes written.
		s.TouchBytes(key, 2)
		if got := s.Estimate(key); got != 2 {
			t.Fatalf("old byte progress caused a second rollover: got %d, want 2", got)
		}
	})
	t.Run("bytes clear count", func(t *testing.T) {
		s := New(Config{Counters: 1024, ResetAfter: 4, ResetBytes: 8})
		defer s.Close()
		key := tkey(3)
		s.Touch(key)
		s.Touch(key)
		s.TouchBytes(key, 8) // Bytes roll before the fourth access.
		s.Touch(key)
		if got := s.Estimate(key); got != 2 {
			t.Fatalf("old count progress caused a second rollover: got %d, want 2", got)
		}
	})
	t.Run("explicit reset clears bytes", func(t *testing.T) {
		s := New(Config{Counters: 1024, ResetAfter: 1000, ResetBytes: 10})
		defer s.Close()
		key := tkey(4)
		for i := 0; i < 4; i++ {
			s.Touch(key)
		}
		s.TouchBytes(key, 8)
		s.Reset() // The existing timer also uses this path.
		s.TouchBytes(key, 2)
		if got := s.Estimate(key); got != 3 {
			t.Fatalf("explicit reset retained old byte progress: got %d, want 3", got)
		}
	})
}

func TestTouchBytesZeroAndDisabled(t *testing.T) {
	s := New(Config{Counters: 1024, ResetAfter: 1000, ResetBytes: 8})
	defer s.Close()
	key := tkey(5)
	s.TouchBytes(key, 7)
	for i := 0; i < 6; i++ {
		s.Touch(key)
	}
	s.TouchBytes(key, 0)
	if got := s.Estimate(key); got != 8 {
		t.Fatalf("reads/empty payload advanced bytes: got %d, want 8", got)
	}
	s.TouchBytes(key, 1)
	if got := s.Estimate(key); got != 4 {
		t.Fatalf("byte boundary did not roll: got %d, want 4", got)
	}

	disabled := New(Config{Counters: 1024, ResetAfter: 1000})
	defer disabled.Close()
	disabled.TouchBytes(key, ^uint64(0))
	disabled.TouchBytes(key, ^uint64(0))
	if got := disabled.Estimate(key); got != 2 {
		t.Fatalf("zero byte limit must disable byte rollover: got %d, want 2", got)
	}
}

func TestByteProgressSaturatesWithoutRepeatedRollover(t *testing.T) {
	max := ^uint64(0)
	s := New(Config{Counters: 1024, ResetAfter: 1000, ResetBytes: max - 3})
	defer s.Close()
	key := tkey(6)
	s.TouchBytes(key, max-4)
	s.TouchBytes(key, 10) // An unchecked addition would wrap below the limit.
	if got := s.Estimate(key); got != 1 {
		t.Fatalf("overflowing sum failed to roll: got %d, want 1", got)
	}

	large := New(Config{Counters: 1024, ResetAfter: 1000, ResetBytes: 1})
	defer large.Close()
	for i := 0; i < 10; i++ {
		large.Touch(key)
	}
	large.TouchBytes(key, max)
	if got := large.Estimate(key); got != 5 {
		t.Fatalf("one large write must roll only once: got %d, want 5", got)
	}
}

func TestConcurrentTriggersCannotRollTheSameGenerationTwice(t *testing.T) {
	s := New(Config{Counters: 1024, ResetAfter: 1000})
	defer s.Close()
	key := tkey(7)
	for i := 0; i < 10; i++ {
		s.Touch(key)
	}
	// Model count, byte and timer triggers that have all observed the same
	// generation, including triggers delayed until another one completes.
	g := s.current.Load()
	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			s.rotate(g)
		}()
	}
	wg.Wait()
	if got := s.Estimate(key); got != 5 {
		t.Fatalf("overlapping triggers lost previous history: got %d, want 5", got)
	}
	s.Reset()
	s.rotate(g) // A stale trigger must not resurrect discarded history.
	if got := s.Estimate(key); got != 0 {
		t.Fatalf("stale trigger replaced a newer generation: got %d, want 0", got)
	}
}

func TestSeparateResetAfterByteRolloverStillAdvances(t *testing.T) {
	s := New(Config{Counters: 1024, ResetAfter: 1000, ResetBytes: 8})
	defer s.Close()
	key := tkey(8)
	for i := 0; i < 10; i++ {
		s.Touch(key)
	}
	s.TouchBytes(key, 8)
	if got := s.Estimate(key); got != 5 {
		t.Fatalf("byte rollover: got %d, want 5", got)
	}
	// A subsequent timer tick is a separate reset, not an overlapping
	// trigger. Preserve that existing behavior rather than delaying it.
	s.Reset()
	if got := s.Estimate(key); got != 0 {
		t.Fatalf("separate reset was postponed: got %d, want 0", got)
	}
}

func TestCheckpointRestoresCountersWithoutTurnoverProgress(t *testing.T) {
	cfg := Config{Counters: 1024, ResetAfter: 4, ResetBytes: 10}
	s := New(cfg)
	defer s.Close()
	key := tkey(9)
	s.TouchBytes(key, 6)
	s.Touch(key)
	checkpoint := s.Marshal()
	if checkpoint[0] != 0x02 {
		t.Fatalf("checkpoint format changed: 0x%02x", checkpoint[0])
	}
	restored := New(cfg)
	defer restored.Close()
	if err := restored.Unmarshal(checkpoint); err != nil {
		t.Fatal(err)
	}
	restored.TouchBytes(key, 4)
	restored.Touch(key)
	if got := restored.Estimate(key); got != 4 {
		t.Fatalf("checkpoint retained old progress or lost counters: got %d, want 4", got)
	}
}

func TestConcurrentTouchBytesResetAndCheckpoint(t *testing.T) {
	s := New(Config{Counters: 4096, ResetAfter: 100, ResetBytes: 1024})
	defer s.Close()
	var wg sync.WaitGroup
	for worker := 0; worker < 8; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			key := tkey(worker)
			for i := 0; i < 1000; i++ {
				s.TouchBytes(key, 32)
				s.Estimate(key)
				if i%100 == 0 {
					s.Reset()
					s.Marshal()
				}
			}
		}(worker)
	}
	wg.Wait()
	s.Reset()
	key := tkey(100)
	for i := 0; i < 10; i++ {
		s.Touch(key)
	}
	if got := s.Estimate(key); got < 10 {
		t.Fatalf("sketch unusable after concurrent rollovers: got %d", got)
	}
}
