//go:build linux

package flatten

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestSelectionReusesCompletedSiblingBudget(t *testing.T) {
	root := t.TempDir()
	for i := 0; i < 12; i++ {
		d := filepath.Join(root, fmt.Sprintf("dir-%02d", i))
		if err := os.Mkdir(d, 0755); err != nil {
			t.Fatal(err)
		}
		for j := 0; j < 12; j++ {
			if err := os.WriteFile(filepath.Join(d, fmt.Sprintf("file-%02d", j)), nil, 0600); err != nil {
				t.Fatal(err)
			}
		}
	}
	// Each live subtree fits, but the cumulative traversal costs do not.
	s, err := selectSource(context.Background(), root, nil, 8<<10)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.validate(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "dir-00", "file-00"), []byte("changed"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := s.validate(context.Background()); err == nil || !strings.Contains(err.Error(), "source tree changed") {
		t.Fatalf("source mutation was not detected: %v", err)
	}
}

func TestSelectionStillBoundsLiveIndexes(t *testing.T) {
	t.Run("wide", func(t *testing.T) {
		root := t.TempDir()
		for i := 0; i < 64; i++ {
			if err := os.WriteFile(filepath.Join(root, fmt.Sprintf("file-%02d", i)), nil, 0600); err != nil {
				t.Fatal(err)
			}
		}
		if _, err := selectSource(context.Background(), root, nil, 4<<10); err == nil || !strings.Contains(err.Error(), "directory index budget exceeded") {
			t.Fatalf("wide directory accepted: %v", err)
		}
	})
	t.Run("deep", func(t *testing.T) {
		root := t.TempDir()
		if err := os.MkdirAll(filepath.Join(root, strings.Repeat("d/", 16)), 0755); err != nil {
			t.Fatal(err)
		}
		if _, err := selectSource(context.Background(), root, nil, 4<<10); err == nil || !strings.Contains(err.Error(), "source selection budget exceeded") {
			t.Fatalf("deep traversal accepted: %v", err)
		}
	})
}

func TestSelectionRetainsDiscoveredAliasCharges(t *testing.T) {
	root := t.TempDir()
	output := filepath.Join(t.TempDir(), "output")
	if err := os.WriteFile(output, nil, 0600); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 8; i++ {
		d := filepath.Join(root, fmt.Sprintf("dir-%02d", i))
		if err := os.Mkdir(d, 0755); err != nil {
			t.Fatal(err)
		}
		for j := 0; j < 16; j++ {
			if err := os.Link(output, filepath.Join(d, fmt.Sprintf("alias-%02d", j))); err != nil {
				t.Fatal(err)
			}
		}
	}
	// Output aliases remain in excludes after their walk frames return.
	if _, err := selectSourceWithOwned(context.Background(), root, nil, []string{output}, nil, 8<<10); err == nil || !strings.Contains(err.Error(), "source selection budget exceeded") {
		t.Fatalf("persistent alias charges were released: %v", err)
	}
}
