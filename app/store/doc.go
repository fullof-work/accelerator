// Package store defines the application boundary for a standalone content-store
// service. The official store-ctl command and statically customized executables
// are peer callers of the same in-process service implementation, not a launcher
// and a child component.
//
// The caller owns argument parsing, configuration selection, process signals,
// and exit status. The application owns the resources it creates, including the
// object backend, generation refresh, gRPC serving, optional read-only cache
// serving, and statistics. Borrowed loggers and input providers remain owned by
// the caller. Application shutdown must finish using them before returning.
//
// ListenerFDs is an explicit borrowed descriptor input. Named fd=<name>
// entrances use App-owned listener copies; the caller retains originals.
// Environment parsing belongs to the CLI or public pkg/activation helper.
//
// Declarative configuration is separate from process-local input bindings.
// A programmatically supplied inline generation list has no implicit YAML path.
// A caller that needs dynamic generations supplies their source explicitly.
// An explicit source is authoritative: its failures never select another source.
// A failed refresh retains the last valid snapshot from that same source.
// Object-data credentials and generation-source credentials are independent
// bindings, even when both destinations use the same S3-compatible endpoint.
//
// This boundary does not introduce bootstrap, re-execution, a plugin registry,
// arbitrary backend factories, or a dependency on node-ctl. It preserves the
// existing store protocol, generation rules, content verification, and built-in
// backends. Operational commands remain separate from the service entry point.
package store
