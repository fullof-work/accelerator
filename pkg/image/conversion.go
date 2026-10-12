package image

// ConversionSemantics identifies the generator policy used for conversion
// result caches, not the EROFS Plan schema or artifact content identity.
// This policy applies verified OCI layers using umoci, preserves numeric
// ownership and OCI replacement/link semantics, and emits single-device,
// uncompressed EROFS with 4 KiB full-SHA256 dedup, fixed UUID/time and no
// xattrs (including capabilities). Runtime config and artifact/encryption
// identity rules remain unchanged. Bump this value for an output-policy or
// layer-application semantic change; implementation-only changes need no bump.
// Missing and different values are conversion misses; explicit old artifacts
// and the original compressed-layer cache remain usable.
const ConversionSemantics = "umoci-erofs-no-xattr-v1"
