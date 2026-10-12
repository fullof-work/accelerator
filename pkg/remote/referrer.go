package remote

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/kuasar-sandbox/accelerator/pkg/image"

	"github.com/google/go-containerregistry/pkg/name"
	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/empty"
	"github.com/google/go-containerregistry/pkg/v1/mutate"
	ggcrremote "github.com/google/go-containerregistry/pkg/v1/remote"
	"github.com/google/go-containerregistry/pkg/v1/remote/transport"
	"github.com/google/go-containerregistry/pkg/v1/static"
	"github.com/google/go-containerregistry/pkg/v1/types"
)

// Annotation keys on the flatten-manifest referrer (the vnd.kuasar.* namespace,
// matching RefererArtifactType). owner ties the stored id to its owner via an
// HMAC; id is the accelerator manifest content key; valid_at records the import
// time and an optional expiry.
const (
	AnnOwner      = "vnd.kuasar.flatten-manifest.owner"
	AnnID         = "vnd.kuasar.flatten-manifest.id"
	AnnValidAt    = "vnd.kuasar.flatten-manifest.valid_at"
	AnnConversion = "vnd.kuasar.flatten-manifest.conversion"
)

// The OCI 1.1 "empty descriptor" layer carried by the referrer artifact: the
// fixed 2-byte "{}" blob under the empty media type. Spec guidance for
// artifact manifests; some registries (e.g. SWR) reject zero-layer manifests
// with MANIFEST_INVALID. The well-known digest makes it free to store.
var (
	emptyLayerBlob      = []byte("{}")
	emptyLayerMediaType = types.MediaType("application/vnd.oci.empty.v1+json")
	referrerNow         = time.Now
)

var ErrReferrersUnsupported = errors.New("remote: registry does not support OCI referrers")

// Owner identifies a flatten-manifest referrer's owner.
type Owner struct {
	ArtifactType string
	Desc         string
	Key          string // HMAC message; defaults to Desc
}

func (c *Config) refererOwner() Owner {
	return Owner{
		ArtifactType: RefererArtifactType,
		Desc:         c.Referer.Desc,
		Key:          c.Referer.Key,
	}
}

// ownerValue computes the owner annotation: "<hmac-hex> <desc>", where
// hmac = HMAC-SHA256(key=customerKey, msg=Owner.Key). The token is constant
// per (customer key, referer key) pair — computable before export, scoped to
// the tenant, and unforgeable without the secret customer key.
func (o Owner) OwnerValue(customerKey []byte) string {
	if o.Key == "" {
		o.Key = o.Desc
	}
	mac := hmac.New(sha256.New, customerKey)
	mac.Write([]byte(o.Key))
	return hex.EncodeToString(mac.Sum(nil)) + " " + o.Desc
}

func (o Owner) ownerValue(customerKey []byte) string { return o.OwnerValue(customerKey) }

// FindReferrer looks for a flatten-manifest referrer on the subject image
// whose owner matches this owner (HMAC over customerKey) and whose valid_at
// has not expired and whose conversion semantics match this generator. On a
// match it returns the stored manifest id and ok=true,
// letting the caller skip pull + flatten + upload entirely. No matching
// referrer yields ok=false, nil. Lookup/transport failures are returned so the
// caller can decide (typically warn and fall back to a full export).
func (c *Config) FindReferrer(ctx context.Context, subj *Resolved, customerKey []byte) (id string, ok bool, err error) {
	o := c.refererOwner()
	if o.ArtifactType == "" {
		return "", false, errors.New("remote: referrer artifact type is empty")
	}
	return c.findReferrerByOwner(ctx, subj, o.ownerValue(customerKey))
}

// FindReferrerByOwner is the atomic lookup used by build orchestrators that
// compute the owner token outside the guest. supported=false means the registry
// did not expose the OCI 1.1 Referrers API; callers decide whether to fail or
// fall back to a full flatten without writeback.
func (c *Config) FindReferrerByOwner(ctx context.Context, subj *Resolved, ownerValue string) (id string, supported bool, ok bool, err error) {
	if RefererArtifactType == "" {
		return "", false, false, errors.New("remote: referrer artifact type is empty")
	}
	if strings.TrimSpace(ownerValue) == "" {
		return "", false, false, errors.New("remote: referrer owner is empty")
	}
	supported, err = c.ReferrersSupported(ctx, subj)
	if err != nil {
		return "", false, false, err
	}
	if !supported {
		return "", false, false, nil
	}
	id, ok, err = c.findReferrerByOwner(ctx, subj, ownerValue)
	return id, true, ok, err
}

func (c *Config) findReferrerByOwner(ctx context.Context, subj *Resolved, ownerValue string) (id string, ok bool, err error) {
	opts := append(c.remoteOpts(ctx), ggcrremote.WithFilter("artifactType", RefererArtifactType))
	idx, err := ggcrremote.Referrers(subj.Digest, opts...)
	if err != nil {
		return "", false, fmt.Errorf("remote: list referrers for %s: %w", subj.Digest, err)
	}
	im, err := idx.IndexManifest()
	if err != nil {
		return "", false, fmt.Errorf("remote: read referrers index: %w", err)
	}
	now := referrerNow()
	var bestID, bestDigest string
	var bestImported time.Time
	for _, d := range im.Manifests {
		if d.ArtifactType != "" && d.ArtifactType != RefererArtifactType {
			continue
		}
		anns := d.Annotations
		if anns[AnnOwner] == "" {
			// The descriptor carries no annotations — e.g. ggcr's tag-schema
			// fallback (for registries without the Referrers API) records only
			// artifactType. Fetch the manifest to read its annotations, but
			// only for our artifact type so we don't pull unrelated referrers
			// (SBOMs, signatures, ...). A compliant Referrers API includes the
			// annotations on the descriptor, taking the fast path here.
			if d.ArtifactType != "" && d.ArtifactType != RefererArtifactType {
				continue
			}
			anns, err = c.manifestAnnotations(ctx, subj.Repo, d.Digest)
			if err != nil {
				continue // unreadable / not an image manifest — skip
			}
		}
		if anns[AnnOwner] != ownerValue || anns[AnnConversion] != image.ConversionSemantics {
			continue
		}
		imported, valid := parseValidAt(anns[AnnValidAt], now)
		mid := anns[AnnID]
		if !valid || !validManifestID(mid) {
			continue
		}
		digest := d.Digest.String()
		if preferReferrer(bestID != "", bestImported, bestDigest, imported, digest) {
			bestID, bestImported, bestDigest = mid, imported, digest
		}
	}
	return bestID, bestID != "", nil
}

func preferReferrer(hasBest bool, bestImported time.Time, bestDigest string, imported time.Time, digest string) bool {
	return !hasBest || imported.After(bestImported) ||
		(imported.Equal(bestImported) && digest > bestDigest)
}

// manifestAnnotations fetches a referrer manifest and returns its annotations.
// Used when the referrers index descriptor omits them (the tag-schema
// fallback path).
func (c *Config) manifestAnnotations(ctx context.Context, repo name.Repository, d v1.Hash) (map[string]string, error) {
	img, err := ggcrremote.Image(repo.Digest(d.String()), c.remoteOpts(ctx)...)
	if err != nil {
		return nil, err
	}
	m, err := img.Manifest()
	if err != nil {
		return nil, err
	}
	return m.Annotations, nil
}

// ReferrersSupported probes the real OCI 1.1 Referrers API endpoint. The ggcr
// Referrers helper silently falls back to the tag schema, which is useful for
// standalone clients but hides the unsupported case from build policy.
func (c *Config) ReferrersSupported(ctx context.Context, subj *Resolved) (bool, error) {
	base := ggcrremote.DefaultTransport
	if c.transport != nil {
		base = c.transport
	}
	tr, err := transport.NewWithContext(ctx, subj.Repo.Registry, authenticator(), base,
		[]string{subj.Repo.Scope(transport.PullScope)})
	if err != nil {
		return false, fmt.Errorf("remote: referrers auth transport: %w", err)
	}
	u := url.URL{
		Scheme: subj.Repo.Scheme(),
		Host:   subj.Repo.RegistryStr(),
		Path:   fmt.Sprintf("/v2/%s/referrers/%s", subj.Repo.RepositoryStr(), subj.Hash.String()),
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return false, err
	}
	req.Header.Set("Accept", string(types.OCIImageIndex))
	resp, err := (&http.Client{Transport: tr}).Do(req)
	if err != nil {
		return false, fmt.Errorf("remote: probe referrers for %s: %w", subj.Digest, err)
	}
	defer resp.Body.Close()
	switch resp.StatusCode {
	case http.StatusOK:
		ct := strings.TrimSpace(strings.Split(resp.Header.Get("Content-Type"), ";")[0])
		return ct == string(types.OCIImageIndex), nil
	case http.StatusNotFound, http.StatusBadRequest, http.StatusNotAcceptable:
		return false, nil
	default:
		if err := transport.CheckError(resp, http.StatusOK); err != nil {
			return false, fmt.Errorf("remote: probe referrers for %s: %w", subj.Digest, err)
		}
		return false, nil
	}
}

// PutReferrer writes a flatten-manifest referrer on the subject image: an OCI
// artifact whose config media type carries the artifactType (so the registry
// reports that as the referrer's artifactType, per the OCI fallback rule), a
// subject pointing at the subject image, and the owner/id/valid_at
// annotations. Requires push access to the subject repository — OCI referrers
// live in the subject's repo.
func (c *Config) PutReferrer(ctx context.Context, subj *Resolved, id string, customerKey []byte) error {
	o := c.refererOwner()
	if o.ArtifactType == "" {
		return errors.New("remote: referrer artifact type is empty")
	}
	return c.PutReferrerByOwner(ctx, subj, id, o.ownerValue(customerKey))
}

// PutReferrerByOwner writes a flatten-manifest referrer using a precomputed
// owner annotation value. This keeps manifest customer keys out of build guests.
// id must have been produced by the generator with image.ConversionSemantics;
// callers must check the actual generator, rather than label old artifacts.
func (c *Config) PutReferrerByOwner(ctx context.Context, subj *Resolved, id, ownerValue string) error {
	if strings.TrimSpace(ownerValue) == "" {
		return errors.New("remote: referrer owner is empty")
	}
	if !validManifestID(id) {
		return fmt.Errorf("remote: manifest id %q is not a 64-hex key", id)
	}
	validAt, err := c.validAtValue()
	if err != nil {
		return err
	}
	anns := map[string]string{
		AnnOwner:      ownerValue,
		AnnID:         id,
		AnnValidAt:    validAt,
		AnnConversion: image.ConversionSemantics,
	}

	// Subject descriptor = the platform-selected image manifest.
	sd, err := ggcrremote.Get(subj.Digest, c.remoteOpts(ctx)...)
	if err != nil {
		return fmt.Errorf("remote: subject descriptor %s: %w", subj.Digest, err)
	}

	img, err := referrerArtifact(anns, sd.Descriptor)
	if err != nil {
		return err
	}

	d, err := img.Digest()
	if err != nil {
		return fmt.Errorf("remote: referrer digest: %w", err)
	}
	ref := subj.Repo.Digest(d.String())
	if err := ggcrremote.Write(ref, img, c.remoteOpts(ctx)...); err != nil {
		return fmt.Errorf("remote: write referrer (needs push access to %s): %w", subj.Repo, err)
	}
	return nil
}

// referrerArtifact builds the OCI image manifest that registers a
// flatten-manifest referrer on a subject image.
//
// No mutate.ArtifactType in this ggcr version: the artifactType is carried as
// the config media type. Registries surface a referrer's config media type as
// its artifactType when the artifactType field is absent, so the
// WithFilter("artifactType", ...) lookup in FindReferrer still matches.
//
// The manifest is forced to OCI (empty.Image serialises as Docker schema2 by
// default, which has no subject semantics — a compliant OCI 1.1 registry then
// ignores the subject and never indexes the referrer), and carries the OCI 1.1
// "empty descriptor" layer: some registries (e.g. SWR) reject zero-layer
// manifests outright with MANIFEST_INVALID. The layer is the fixed well-known
// 2-byte blob, so stores dedup it to nothing.
//
// Order matters: mutate wrappers overwrite Subject (mutate.image.compute
// assigns it unconditionally), so the layer must be appended BEFORE Subject
// wraps the image. Annotations merges and is order-safe.
func referrerArtifact(anns map[string]string, subject v1.Descriptor) (v1.Image, error) {
	art := mutate.ConfigMediaType(empty.Image, types.MediaType(RefererArtifactType))
	art = mutate.MediaType(art, types.OCIManifestSchema1)
	withLayer, err := mutate.AppendLayers(art, static.NewLayer(emptyLayerBlob, emptyLayerMediaType))
	if err != nil {
		return nil, fmt.Errorf("remote: referrer empty layer: %w", err)
	}
	annotated := mutate.Annotations(withLayer, anns)
	withSubject := mutate.Subject(annotated, subject)
	img, ok := withSubject.(v1.Image)
	if !ok {
		return nil, errors.New("remote: referrer artifact is not an image")
	}
	return img, nil
}

func validManifestID(id string) bool {
	if len(id) != 64 {
		return false
	}
	_, err := hex.DecodeString(id)
	return err == nil
}

// validAtValue builds the valid_at annotation: "<import RFC3339>" plus, when
// referer.validity is set, " <expiry RFC3339>".
func (c *Config) validAtValue() (string, error) {
	now := referrerNow().UTC()
	v := now.Format(time.RFC3339Nano)
	if c.Referer.Validity == "" {
		return v, nil
	}
	dur, err := time.ParseDuration(c.Referer.Validity)
	if err != nil {
		return "", fmt.Errorf("remote: referer.validity: %w", err)
	}
	if dur <= 0 {
		return "", errors.New("remote: referer.validity must be positive")
	}
	return v + " " + now.Add(dur).Format(time.RFC3339Nano), nil
}

// parseValidAt validates "<import>[ <expiry>]" and reports whether the record
// is eligible at now. Import-only records do not expire.
func parseValidAt(validAt string, now time.Time) (time.Time, bool) {
	fields := strings.Fields(validAt)
	if len(fields) != 1 && len(fields) != 2 {
		return time.Time{}, false
	}
	imported, err := time.Parse(time.RFC3339Nano, fields[0])
	if err != nil || imported.After(now) {
		return time.Time{}, false
	}
	if len(fields) == 1 {
		return imported, true
	}
	expires, err := time.Parse(time.RFC3339Nano, fields[1])
	if err != nil || !expires.After(imported) || !now.Before(expires) {
		return time.Time{}, false
	}
	return imported, true
}
