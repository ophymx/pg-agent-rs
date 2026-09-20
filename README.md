# pg-agent-rs

A high-availability agent for PostgreSQL clusters fronted by pgpool-II.

It replaces pgpool's shell-script hooks (`failover.sh`,
`follow_primary.sh`, `recovery_1st_stage`, `pgpool_remote_start`,
`escalation.sh`) and the SSH-based remote execution they depend on with a
typed RPC surface over an mTLS mesh.

More importantly, it owns the promotion decision. pgpool's watchdog is a
failure detector, not a consensus protocol — it can answer "can I reach
X?" but not "is X primary?", and treating the first answer as the second
is how split-brain happens. Here the decision is a compare-and-swap on a
lease in a Raft log the agents replicate among themselves, so there is no
external DCS to operate.

- **What this is and why:** [SPEC.md](SPEC.md)
- **How an operator deploys it:** [BOOTSTRAP.md](BOOTSTRAP.md)
- **Where this is going:** [ROADMAP.md](ROADMAP.md)
- **Why promotion works this way:** [docs/promotion-authority.md](docs/promotion-authority.md)
  and [docs/quorum-commit.md](docs/quorum-commit.md)

## What it assumes

Worth checking before reading further — these are structural, not
configurable:

- **pgpool-II co-located with PostgreSQL** on every backend node. A
  separate-middleware topology is out of scope.
- **At least three nodes.** Consensus is not optional and a two-node Raft
  cluster tolerates zero failures, so `validate-env` refuses a smaller
  pool rather than letting that be discovered during an outage.
- **systemd**, reached over D-Bus with a polkit rule. PostgreSQL's
  lifecycle belongs to the agent, not to `Restart=`.
- **HAProxy** (or equivalent) as the L4 entry point. VIP management is
  deliberately absent.
- Debian- or RHEL-family layouts. Both are covered by the acceptance
  matrix — run before each release, not in CI, which cannot host it (see
  [Testing](#testing)); other distros need five path fields set
  explicitly.

## Binaries

| Binary         | Role |
|----------------|------|
| `pg_agentd`    | Coordinator daemon. `serve` (default) runs the gRPC + healthz listeners; `validate-env` is the `nginx -t` equivalent — wired as `ExecStartPre=`. |
| `pg_agentc`    | One-shot pgpool hook client. Marshals positional argv into a single Unix-socket RPC, then exits. |
| `pg_agentctl`  | Operator CLI: hook config (`print-hooks`, `check-hooks`, `gen-pgpool`), cluster operations (`cluster init/status/recover/handoff/start-primary/pause/resume/allow-async`), and the two journals (`maintenance`, `ops`). Every subcommand routes through the local daemon over the Unix socket — no TLS material needed on the operator's host. Run `--help` for the current surface. |

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

## Testing

Unit tests run under `cargo test`. The claims that matter — that a
partitioned primary fences itself, that exactly one node is ever
promoted, that acknowledged writes survive every induced failure — are
asserted by a dockerized three-node acceptance suite that boots the real
packages, the real systemd units and real streaming replication, and
manufactures the failures:

```
testing/acceptance.sh        # baseline cell
testing/matrix.sh            # every OS / PostgreSQL cell
```

See [testing/README.md](testing/README.md). Discoveries it has made are
logged in [testing/FINDINGS.md](testing/FINDINGS.md), and much of the
design is easier to understand from those than from the specification.

CI runs the unit tests, `clippy -D warnings`, rustfmt and the licence
gate on every push. It does **not** run the acceptance suite: that needs
privileged containers running systemd as PID 1 with `cgroup: host`, and
one scenario SIGKILLs PID 1 to force a container restart — none of which
a hosted runner will do. The matrix is run before a release, and its
result is what the [Status](#status) section below is reporting.

## Status

Feature-complete and exercised end to end: every workflow shipped, every
`pg_agentctl` subcommand wired, packaging produces `.deb` + `.rpm`, and
the acceptance suite passes across Debian 12/13, Ubuntu 24.04 and Rocky 9
on PostgreSQL 15–17. It has not been through wide production use beyond
the cluster it was built for — read [ROADMAP.md](ROADMAP.md) for what is
missing and [TODO.md](TODO.md) for known open defects before you rely on
it.

## License

Dual-licensed under [MIT](LICENSE-MIT) or [Apache 2.0](LICENSE-APACHE),
at your option.
