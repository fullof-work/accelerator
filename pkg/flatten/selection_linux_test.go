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

func TestSelectionDefaultAndExplicitBudget(t *testing.T) {
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
	// Explicit quotas retain their conservative cumulative admission behavior.
	if _, err := selectSource(context.Background(), root, nil, 8<<10); err == nil || !strings.Contains(err.Error(), "budget exceeded") {
		t.Fatalf("explicit quota no longer enforced: %v", err)
	}
	limits, err := (PlanLimits{}).defaults()
	if err != nil {
		t.Fatal(err)
	}
	s, err := selectSource(context.Background(), root, nil, limits.IndexMemoryBytes/4)
	if err != nil {
		t.Fatal(err)
	}
	if err = s.validate(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(root, "dir-00", "file-00"), []byte("changed"), 0600); err != nil {
		t.Fatal(err)
	}
	if err = s.validate(context.Background()); err == nil || !strings.Contains(err.Error(), "source tree changed") {
		t.Fatalf("source mutation was not detected: %v", err)
	}
}
