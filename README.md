# pg-agent-rs

Rust port of `pg_agent` — the daemon that replaces pgpool-II's shell-script
hooks (`failover.sh`, `follow_primary.sh`, `recovery_1st_stage`,
`pgpool_remote_start`, `escalation.sh`) and the SSH-based remote execution
they depend on.

- **What this is and why:** [SPEC.md](SPEC.md)
- **How an operator deploys it:** [BOOTSTRAP.md](BOOTSTRAP.md)
- **Where this is going:** [ROADMAP.md](ROADMAP.md)

## Binaries

| Binary         | Role |
|----------------|------|
| `pg_agentd`    | Coordinator daemon. `serve` (default) runs the gRPC + healthz listeners; `validate-env` is the `nginx -t` equivalent — wired as `ExecStartPre=`. |
| `pg_agentc`    | One-shot pgpool hook client. Marshals positional argv into a single Unix-socket RPC, then exits. |
| `pg_agentctl`  | Operator CLI: `print-hooks`, `check-hooks`, `gen-pgpool`, `maintenance {list,show,retry}`, `cluster {init,status}`. Every subcommand routes through the local daemon over the Unix socket — no TLS material needed on the operator's host. |

## Layout

```
crates/
├── pg-agent-proto/      tonic + prost generated types
├── pg-agent-hookspec/   positional-arg schemas (no proto dep)
├── pg-agent-core/       Agent, config, peers, db, systemd, pgstandby,
│                        walstore, maintenance, healthz, certreload, preflight
├── pg-agentd/           daemon binary (runs on every PostgreSQL backend)
├── pg-agentc/           pgpool hook client (thin Unix-socket forwarder)
└── pg-agentctl/         operator CLI
proto/                   .proto source files (compiled by pg-agent-proto's build.rs)
packaging/               nfpm.yaml + systemd unit + scriptlets (.deb + .rpm)
```

## Build

```
cargo check --workspace      # quick verify
cargo build --release        # ships three binaries: pg_agentd, pg_agentc, pg_agentctl
cargo test --workspace
./scripts/build-pkgs.sh      # nfpm-produced .deb + .rpm
```

Requires `protoc` (3.x) on `$PATH` for the proto crate's build.rs.

## Status

v1 feature-complete: every SPEC §5 workflow shipped, every `pg_agentctl`
subcommand wired, packaging produces `.deb` + `.rpm`. See
[ROADMAP.md](ROADMAP.md) for what comes next.

## License

Dual-licensed under [MIT](LICENSE-MIT) or [Apache 2.0](LICENSE-APACHE),
at your option.
