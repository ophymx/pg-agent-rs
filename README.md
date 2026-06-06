# pg-agent-rs

Rust port of `pg_agent` — the daemon that replaces pgpool-II's shell-script
hooks (`failover.sh`, `follow_primary.sh`, `recovery_1st_stage`,
`pgpool_remote_start`, `escalation.sh`) and the SSH-based remote execution
they depend on.

- **What this is and why:** [SPEC.md](SPEC.md)
- **Where this is going:** [ROADMAP.md](ROADMAP.md)

## Layout

```
crates/
├── pg-agent-proto/      tonic + prost generated types
├── pg-agent-hookspec/   positional-arg schemas (no proto dep)
├── pg-agent-core/       Agent, config, peers, db, systemd, pgstandby,
│                        walstore, maintenance, healthz, certreload, preflight
├── pg-agentd/           daemon binary (runs on every PostgreSQL backend)
├── pg-agentc/           pgpool hook client (thin Unix-socket forwarder)
└── pg-agentctl/         operator CLI (preflight, maintenance, gen-pgpool, …)
proto/                   .proto source files (compiled by pg-agent-proto's build.rs)
```

## Build

```
cargo check --workspace      # quick verify
cargo build --release        # ships three binaries: pg_agentd, pg_agentc, pg_agentctl
cargo test --workspace
```

Requires `protoc` (3.x) on `$PATH` for the proto crate's build.rs.

## Status

Scaffolding only — types and traits are declared per the SPEC; method bodies
are `todo!()` / stubs. Implementation work tracked against SPEC §-numbers in
the source comments.
