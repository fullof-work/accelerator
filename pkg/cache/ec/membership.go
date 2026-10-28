package ec

import (
	"context"
	"fmt"
	"log"
	"sort"
	"strings"
	"time"

	"github.com/kuasar-sandbox/accelerator/pkg/cache/client"
)

// Membership is the (epoch, peers) pair a cache-ctl client advertises
// as the cluster's current view. Epoch is monotonically increasing;
// peers are NOT required to be sorted (the router sorts internally
// when building the Maglev table).
type Membership struct {
	Epoch int64
	Peers []Peer
}

// MembershipDiff reports the peer-level delta between two memberships,
// useful for logging SIGHUP reload outcomes.
type MembershipDiff struct {
	Added   []Peer
	Removed []Peer
}

func diffMembership(oldPeers, newPeers []Peer) MembershipDiff {
	oldByID := make(map[string]Peer, len(oldPeers))
	for _, p := range oldPeers {
		oldByID[p.ID] = p
	}
	newByID := make(map[string]Peer, len(newPeers))
	for _, p := range newPeers {
		newByID[p.ID] = p
	}
	var added, removed []Peer
	for id, p := range newByID {
		if _, ok := oldByID[id]; !ok {
			added = append(added, p)
		}
	}
	for id, p := range oldByID {
		if _, ok := newByID[id]; !ok {
			removed = append(removed, p)
		}
	}
	sort.Slice(added, func(i, j int) bool { return added[i].ID < added[j].ID })
	sort.Slice(removed, func(i, j int) bool { return removed[i].ID < removed[j].ID })
	return MembershipDiff{Added: added, Removed: removed}
}

func formatPeerIDs(peers []Peer) string {
	ids := make([]string, len(peers))
	for i, p := range peers {
		ids[i] = p.ID
	}
	return "[" + strings.Join(ids, " ") + "]"
}

// ProbePeer completes one wire Ping on an independent short-lived connection.
// A listening socket alone does not establish health. This proves only that
// the peer's wire service responds, not that every object or shard is available.
//
// Timeout: hard-coded small value (not tuneable) because this runs
// inline with SIGHUP handling; a slow peer should be excluded, not
// delay the whole reload.
func ProbePeer(ctx context.Context, endpoint string) error {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	if err := client.Ping(ctx, endpoint); err != nil {
		return fmt.Errorf("probe %s: %w", endpoint, err)
	}
	return nil
}

// ApplyMembership atomically swaps the tier's routing table to the
// given membership and reconciles the connection pool (closing dead
// endpoints; new peers lazily dial on first use). Rejects non-
// monotonic epoch updates. The diff is logged at membership-change
// granularity — callers don't need to log again.
//
// Thread-safety: the router's atomic.Pointer swap is lock-free; pool
// Reconcile holds its own lock briefly. In-flight Get/Fill calls that
// captured the old router state continue on the old snapshot until
// they return.
func (t *impl) ApplyMembership(m Membership) error {
	oldState, newState, err := t.router.ApplyMembership(m.Epoch, m.Peers)
	if err != nil {
		return err
	}
	closed := t.pool.Reconcile(newState.peers)
	diff := diffMembership(oldState.peers, newState.peers)
	log.Printf("ec: membership epoch %d → %d  added=%s  removed=%s  closed_conns=%d",
		oldState.epoch, newState.epoch, formatPeerIDs(diff.Added), formatPeerIDs(diff.Removed), len(closed))
	return nil
}
