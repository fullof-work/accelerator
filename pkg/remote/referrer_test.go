package remote

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

	"github.com/google/go-containerregistry/pkg/v1/random"
	ggcrremote "github.com/google/go-containerregistry/pkg/v1/remote"
	"github.com/kuasar-sandbox/accelerator/pkg/image"
)

func TestOwnerValueDeterministicAndKeyed(t *testing.T) {
	o := Owner{Desc: "acme-prod", Key: "acme-prod"}
	k1 := bytes.Repeat([]byte{1}, 32)
	k2 := bytes.Repeat([]byte{2}, 32)

	a := o.OwnerValue(k1)
	if a != o.OwnerValue(k1) {
		t.Fatal("ownerValue not deterministic for the same key")
	}
	if a == o.OwnerValue(k2) {
		t.Fatal("ownerValue must differ for different customer keys")
	}
	if !strings.HasSuffix(a, " acme-prod") {
		t.Fatalf("owner %q must end with the public desc", a)
	}
	// The HMAC token is the first space-separated field, hex-encoded SHA-256.
	if first := strings.Fields(a)[0]; len(first) != 64 {
		t.Fatalf("hmac token %q is not 64 hex chars", first)
	}
}

func TestFindReferrerByOwnerReportsUnsupported(t *testing.T) {
	host := startRegistry(t)
	img, err := random.Image(512, 1)
	if err != nil {
		t.Fatal(err)
	}
	ref := host + "/test/unsupported:v1"
	pushImage(t, ref, img)

	cfg := &Config{Insecure: true, Cache: CacheConfig{Dir: t.TempDir()}}
	if err := cfg.normalize(); err != nil {
		t.Fatal(err)
	}
	res, err := cfg.Resolve(context.Background(), ref)
	if err != nil {
		t.Fatal(err)
	}
	_, supported, ok, err := cfg.FindReferrerByOwner(context.Background(), res, "owner")
	if err != nil {
		t.Fatal(err)
	}
	if supported || ok {
		t.Fatalf("expected unsupported miss, got supported=%v ok=%v", supported, ok)
	}
}

func TestParseValidAt(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	imp := now.Add(-2 * time.Hour).Format(time.RFC3339)
	tests := []struct {
		name string
		raw  string
		ok   bool
	}{
		{name: "missing", raw: ""},
		{name: "malformed import", raw: "not-a-time"},
		{name: "future import", raw: now.Add(time.Hour).Format(time.RFC3339)},
		{name: "import only", raw: imp, ok: true},
		{name: "past expiry", raw: imp + " " + now.Add(-time.Hour).Format(time.RFC3339)},
		{name: "equal expiry", raw: imp + " " + now.Format(time.RFC3339)},
		{name: "future expiry", raw: imp + " " + now.Add(time.Hour).Format(time.RFC3339), ok: true},
		{name: "expiry before import", raw: imp + " " + now.Add(-3*time.Hour).Format(time.RFC3339)},
		{name: "extra field", raw: imp + " " + now.Add(time.Hour).Format(time.RFC3339) + " extra"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, ok := parseValidAt(tt.raw, now)
			if ok != tt.ok {
				t.Fatalf("parseValidAt(%q) ok=%v, want %v", tt.raw, ok, tt.ok)
			}
		})
	}
}

func TestPutReferrerByOwnerRejectsEmptyOwner(t *testing.T) {
	cfg := &Config{}
	err := cfg.PutReferrerByOwner(context.Background(), nil,
		"1111111111111111111111111111111111111111111111111111111111111111", " ")
	if err == nil {
		t.Fatal("empty owner was accepted")
	}
}

func TestNormalizeRejectsNonPositiveRefererValidity(t *testing.T) {
	for _, validity := range []string{"0s", "-1s"} {
		cfg := &Config{Referer: RefererConfig{Validity: validity}}
		if err := cfg.normalize(); err == nil {
			t.Fatalf("normalize accepted validity %q", validity)
		}
	}
}

func TestValidAtPreservesSubsecondValidity(t *testing.T) {
	cfg := &Config{Referer: RefererConfig{Validity: "1ns"}}
	raw, err := cfg.validAtValue()
	if err != nil {
		t.Fatal(err)
	}
	fields := strings.Fields(raw)
	if len(fields) != 2 {
		t.Fatalf("valid_at=%q, want import and expiry", raw)
	}
	imported, err := time.Parse(time.RFC3339Nano, fields[0])
	if err != nil {
		t.Fatal(err)
	}
	expires, err := time.Parse(time.RFC3339Nano, fields[1])
	if err != nil {
		t.Fatal(err)
	}
	if !expires.After(imported) {
		t.Fatalf("valid_at=%q lost positive subsecond validity", raw)
	}
}

func TestPreferReferrerUsesNewestImportThenDigest(t *testing.T) {
	now := time.Now()
	if !preferReferrer(false, time.Time{}, "", now, "a") {
		t.Fatal("first candidate was not selected")
	}
	if preferReferrer(true, now, "a", now.Add(-time.Second), "z") {
		t.Fatal("older candidate replaced the current selection")
	}
	if !preferReferrer(true, now, "a", now.Add(time.Second), "a") {
		t.Fatal("newer candidate was not selected")
	}
	if !preferReferrer(true, now, "a", now, "b") {
		t.Fatal("digest tie-break did not select the deterministic winner")
	}
}

// TestReferrerRoundTrip — write a flatten-manifest referrer to the subject's
// repo, then find it back by owner; a different customer key must not match.
func TestReferrerRoundTrip(t *testing.T) {
	host := startRegistry(t)
	img, err := random.Image(512, 2)
	if err != nil {
		t.Fatal(err)
	}
	ref := host + "/test/base:v1"
	pushImage(t, ref, img)

	cfg := &Config{
		Insecure: true,
		Cache:    CacheConfig{Dir: t.TempDir()},
		Referer: RefererConfig{
			Desc: "acme-prod",
			Key:  "acme-prod",
		},
	}
	if err := cfg.normalize(); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	res, err := cfg.Resolve(ctx, ref)
	if err != nil {
		t.Fatal(err)
	}
	ck := bytes.Repeat([]byte{7}, 32)
	const manifestID = "1111111111111111111111111111111111111111111111111111111111111111"

	if _, ok, err := cfg.FindReferrer(ctx, res, ck); err != nil || ok {
		t.Fatalf("expected initial miss, got ok=%v err=%v", ok, err)
	}
	if err := cfg.PutReferrer(ctx, res, manifestID, ck); err != nil {
		t.Fatalf("PutReferrer: %v", err)
	}
	id, ok, err := cfg.FindReferrer(ctx, res, ck)
	if err != nil {
		t.Fatalf("FindReferrer: %v", err)
	}
	if !ok || id != manifestID {
		t.Fatalf("expected hit id=%s, got ok=%v id=%q", manifestID, ok, id)
	}
	// A different customer key yields a different owner → no match.
	if _, ok, _ := cfg.FindReferrer(ctx, res, bytes.Repeat([]byte{9}, 32)); ok {
		t.Fatal("a different customer key must not match the owner")
	}
}

func TestReferrerConversionPolicyMissAndWriteback(t *testing.T) {
	host := startRegistry(t)
	img, err := random.Image(512, 1)
	if err != nil {
		t.Fatal(err)
	}
	ref := host + "/test/conversion:v1"
	pushImage(t, ref, img)
	cfg := &Config{Insecure: true, Referer: RefererConfig{Desc: "owner"}}
	if err := cfg.normalize(); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	res, err := cfg.Resolve(ctx, ref)
	if err != nil {
		t.Fatal(err)
	}
	key := bytes.Repeat([]byte{7}, 32)
	owner := cfg.refererOwner().OwnerValue(key)
	subject, err := ggcrremote.Get(res.Digest, cfg.remoteOpts(ctx)...)
	if err != nil {
		t.Fatal(err)
	}
	oldID := strings.Repeat("1", 64)
	for _, policy := range []string{"", "legacy-erofs-v0"} {
		anns := map[string]string{AnnOwner: owner, AnnID: oldID, AnnValidAt: time.Now().UTC().Add(-time.Second).Format(time.RFC3339Nano)}
		if policy != "" {
			anns[AnnConversion] = policy
		}
		artifact, err := referrerArtifact(anns, subject.Descriptor)
		if err != nil {
			t.Fatal(err)
		}
		digest, err := artifact.Digest()
		if err != nil {
			t.Fatal(err)
		}
		if err := ggcrremote.Write(res.Repo.Digest(digest.String()), artifact, cfg.remoteOpts(ctx)...); err != nil {
			t.Fatal(err)
		}
		if id, hit, err := cfg.FindReferrer(ctx, res, key); err != nil || hit || id != "" {
			t.Fatalf("accepted policy %q: %q %v %v", policy, id, hit, err)
		}
		// The old artifact is still present and readable by an explicit digest.
		if _, err := ggcrremote.Get(res.Repo.Digest(digest.String()), cfg.remoteOpts(ctx)...); err != nil {
			t.Fatal(err)
		}
	}
	newID := strings.Repeat("2", 64)
	if err := cfg.PutReferrer(ctx, res, newID, key); err != nil {
		t.Fatal(err)
	}
	if id, hit, err := cfg.FindReferrer(ctx, res, key); err != nil || !hit || id != newID {
		t.Fatalf("writeback mismatch: %q %v %v", id, hit, err)
	}
	idx, err := ggcrremote.Referrers(res.Digest, cfg.remoteOpts(ctx)...)
	if err != nil {
		t.Fatal(err)
	}
	im, err := idx.IndexManifest()
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, d := range im.Manifests {
		anns, err := cfg.manifestAnnotations(ctx, res.Repo, d.Digest)
		if err != nil {
			t.Fatal(err)
		}
		if anns[AnnID] == newID {
			found = true
			if anns[AnnConversion] != image.ConversionSemantics {
				t.Fatal("writeback omitted actual conversion policy")
			}
		}
	}
	if !found {
		t.Fatal("missing referrer")
	}
}

func setReferrerTestNow(t *testing.T, now *time.Time) {
	t.Helper()
	old := referrerNow
	referrerNow = func() time.Time { return *now }
	t.Cleanup(func() { referrerNow = old })
}

func TestExpiredReferrerIsMiss(t *testing.T) {
	now := time.Date(2026, 9, 20, 0, 0, 0, 0, time.UTC)
	setReferrerTestNow(t, &now)
	host := startRegistry(t)
	img, err := random.Image(512, 1)
	if err != nil {
		t.Fatal(err)
	}
	ref := host + "/test/expired:v1"
	pushImage(t, ref, img)

	cfg := &Config{
		Insecure: true,
		Cache:    CacheConfig{Dir: t.TempDir()},
		Referer: RefererConfig{
			Desc:     "expired-owner",
			Validity: "1s",
		},
	}
	if err := cfg.normalize(); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	res, err := cfg.Resolve(ctx, ref)
	if err != nil {
		t.Fatal(err)
	}
	key := bytes.Repeat([]byte{3}, 32)
	const manifestID = "2222222222222222222222222222222222222222222222222222222222222222"
	if err := cfg.PutReferrer(ctx, res, manifestID, key); err != nil {
		t.Fatal(err)
	}
	now = now.Add(1100 * time.Millisecond)
	if id, ok, err := cfg.FindReferrer(ctx, res, key); err != nil || ok || id != "" {
		t.Fatalf("expired lookup id=%q ok=%v err=%v", id, ok, err)
	}
}

func TestExpiredNewerReferrerFallsBackToNewestValidCandidate(t *testing.T) {
	now := time.Date(2026, 9, 20, 0, 0, 0, 0, time.UTC)
	setReferrerTestNow(t, &now)
	host := startRegistry(t)
	img, err := random.Image(512, 1)
	if err != nil {
		t.Fatal(err)
	}
	ref := host + "/test/expiry-fallback:v1"
	pushImage(t, ref, img)

	cfg := &Config{
		Insecure: true,
		Cache:    CacheConfig{Dir: t.TempDir()},
		Referer:  RefererConfig{Desc: "expiry-fallback"},
	}
	if err := cfg.normalize(); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	res, err := cfg.Resolve(ctx, ref)
	if err != nil {
		t.Fatal(err)
	}
	key := bytes.Repeat([]byte{4}, 32)
	const stableID = "3333333333333333333333333333333333333333333333333333333333333333"
	const expiringID = "4444444444444444444444444444444444444444444444444444444444444444"
	if err := cfg.PutReferrer(ctx, res, stableID, key); err != nil {
		t.Fatal(err)
	}
	now = now.Add(1100 * time.Millisecond)
	cfg.Referer.Validity = "3s"
	if err := cfg.PutReferrer(ctx, res, expiringID, key); err != nil {
		t.Fatal(err)
	}
	if id, ok, err := cfg.FindReferrer(ctx, res, key); err != nil || !ok || id != expiringID {
		t.Fatalf("newest live lookup id=%q ok=%v err=%v", id, ok, err)
	}
	now = now.Add(3100 * time.Millisecond)
	if id, ok, err := cfg.FindReferrer(ctx, res, key); err != nil || !ok || id != stableID {
		t.Fatalf("post-expiry lookup id=%q ok=%v err=%v", id, ok, err)
	}
}
