// Package cache defines the application boundary for a standalone cache service.
// The official cache-ctl command and statically customized executables are peer
// callers of the same in-process service implementation. A service manager can
// launch either executable directly, without node-ctl or another launcher.
//
// The caller owns argument parsing, configuration selection, process signals,
// and exit status. The application owns its built-in direct or tiered assembly,
// origin clients, shared payload pool, wire serving, health and Info serving,
// statistics, and optional profiling listener. Shutdown must stop admission and
// finish handlers, asynchronous fills and repairs, refresh, and observation work
// before releasing resources still used by those activities.
//
// ListenerFDs is an explicit borrowed descriptor input. Named fd=<name>
// entrances use App-owned listener copies; the caller retains originals.
// Environment parsing belongs to the CLI or public pkg/activation helper.
//
// Configuration defines the initial topology and resource budgets. An optional
// membership input supplies candidate peers only for existing EC tiers. Refresh
// retains the existing per-tier probe and membership application semantics; it
// is not full configuration reload or a transaction across all tiers.
// Input bindings are process-local and are not reconstructed from a bootstrap.
//
// This boundary preserves the existing wire protocol, Blob ownership, built-in
// backends, lookup and repair algorithms, and resource budgets. It does not add
// arbitrary backend, tier, or origin factories, a global plugin registry, or a
// dependency on the official command binary in customized deployments.
package cache
