# pg-agent-rs

Rust port of `pg_agent` — the daemon that replaces pgpool-II's shell-script
hooks (`failover.sh`, `follow_primary.sh`, `recovery_1st_stage`,
`pgpool_remote_start`, `escalation.sh`) and the SSH-based remote execution
they depend on.

It also owns the promotion decision, on a quorum-backed lease in a Raft
log the agents replicate among themselves — no external DCS.

- **What this is and why:** [SPEC.md](SPEC.md)
- **How an operator deploys it:** [BOOTSTRAP.md](BOOTSTRAP.md)
- **Where this is going:** [ROADMAP.md](ROADMAP.md)
- **Why promotion works this way:** [docs/promotion-authority.md](docs/promotion-authority.md)
  and [docs/quorum-commit.md](docs/quorum-commit.md)

## Binaries

| Binary         | Role |
|----------------|------|
| `pg_agentd`    | Coordinator daemon. `serve` (default) runs the gRPC + healthz listeners; `validate-env` is the `nginx -t` equivalent — wired as `ExecStartPre=`. |
| `pg_agentc`    | One-shot pgpool hook client. Marshals positional argv into a single Unix-socket RPC, then exits. |
| `pg_agentctl`  | Operator CLI: hook config (`print-hooks`, `check-hooks`, `gen-pgpool`), cluster operations (`cluster init/status/recover/handoff/pause/resume/allow-async`), and the two journals (`maintenance`, `ops`). Every subcommand routes through the local daemon over the Unix socket — no TLS material needed on the operator's host. Run `--help` for the current surface. |

## Layout

```
crates/
├── pg-agent-proto/      tonic + prost generated types
├── pg-agent-hookspec/   positional-arg schemas (no proto dep)
├── pgman/               single-PostgreSQL-instance management: SQL surface,
│                        basebackup/rewind/recovery config, WAL archive,
│                        process-control seam (knows nothing of the agent)
├── pg-agent-core/       Agent, config, peers, systemd, consensus, HA loop,
│                        maintenance, healthz, certreload, preflight
├── pg-agentd/           daemon binary (runs on every PostgreSQL backend)
├── pg-agentc/           pgpool hook client (thin Unix-socket forwarder)
└── pg-agentctl/         operator CLI
proto/                   .proto source files (compiled by pg-agent-proto's build.rs)
packaging/               systemd unit + scriptlets (.deb + .rpm metadata lives in crates/pg-agentd/Cargo.toml)
testing/                 dockerized 3-node acceptance suite (Rust harness)
```

## Build

```
cargo check --workspace      # quick verify
cargo build --release        # ships three binaries: pg_agentd, pg_agentc, pg_agentctl
cargo test --workspace
./scripts/build-pkgs.sh      # static-musl .deb + .rpm (cargo-deb / cargo-generate-rpm)
```

Needs nothing but a Rust toolchain: the proto crate's build.rs uses the
`protoc` vendored by `protoc-bin-vendored`, so there is no system package
to install first.

## Status

v1 feature-complete: every SPEC §5 workflow shipped, every `pg_agentctl`
subcommand wired, packaging produces `.deb` + `.rpm`. See
[ROADMAP.md](ROADMAP.md) for what comes next.

## License

Dual-licensed under [MIT](LICENSE-MIT) or [Apache 2.0](LICENSE-APACHE),
at your option.
