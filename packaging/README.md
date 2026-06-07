# Packaging

`nfpm` (https://github.com/goreleaser/nfpm) builds the `.deb` and
`.rpm` from one YAML. Same binaries, same systemd unit, same
scriptlets — no separate `debian/` and `rpm/spec` to keep in sync.

## Layout

```
packaging/
├── nfpm.yaml            # single source of truth for both formats
├── pg_agentd.service    # systemd unit (matches SPEC §10.4)
├── config.toml.sample   # /usr/share/pg_agent/config.toml.sample
└── scripts/
    ├── postinstall.sh   # daemon-reload + restart-if-running
    ├── preremove.sh     # stop on real removal (skip on upgrade)
    └── postremove.sh    # disable + daemon-reload on real removal
```

The scripts are deliberately **bare `systemctl`** — no
`deb-systemd-helper`, no `dh_installsystemd`. nfpm doesn't ship
those helpers anyway, and `systemctl` works identically on Debian
and RHEL, so the same script file handles both formats. See SPEC
§10.4 and BOOTSTRAP.md Phase 1.1 for the
"deliberately-do-not-auto-enable" rationale.

## Build

Use the wrapper script — it does the `cargo build --release`,
extracts `VERSION` from `Cargo.toml`, exports it (nfpm requires the
env var actually be in the environment, not just a shell var), and
invokes `nfpm pkg` for both formats:

```sh
scripts/build-pkgs.sh             # both .deb and .rpm
scripts/build-pkgs.sh deb         # just .deb
scripts/build-pkgs.sh rpm         # just .rpm
VERSION=1.2.3-rc1 scripts/build-pkgs.sh   # override the version
```

Outputs land in `dist/` (git-ignored).

If you really want to invoke nfpm by hand:

```sh
cargo build --release
export VERSION=$(awk -F'"' '/^version =/ { print $2; exit }' Cargo.toml)
mkdir -p dist
nfpm pkg --config packaging/nfpm.yaml --packager deb \
         --target "dist/pg-agent-rs_${VERSION}_amd64.deb"
nfpm pkg --config packaging/nfpm.yaml --packager rpm \
         --target "dist/pg-agent-rs-${VERSION}-1.x86_64.rpm"
```

`VERSION` MUST be exported — `VAR=val && cmd` is a shell-var
assignment, not an env-var export, so nfpm's `${VERSION}`
substitution won't see it. Forgetting the export → nfpm falls back
to its `0.0.0~rc0` default and the package's `Version:` field is
wrong.

## Behaviour matrix

| Event                               | Result                                              |
|-------------------------------------|-----------------------------------------------------|
| `apt install pg-agent-rs` (fresh)   | files placed; service NOT enabled, NOT started      |
| `apt upgrade pg-agent-rs`, running  | files replaced; service restarted in postinstall    |
| `apt upgrade pg-agent-rs`, stopped  | files replaced; service stays stopped               |
| `apt remove pg-agent-rs`            | service stopped, files removed (`/etc/pg_agent` kept) |
| `apt purge pg-agent-rs`             | service stopped + disabled, files + config removed  |
| `dnf install` / `dnf upgrade` / `dnf remove` | same shape via rpm scriptlet args            |

The "fresh install does NOT enable the service" rule is deliberate —
the agent needs a real `config.toml` + mTLS material in place before
it can do anything useful. Ansible (or the operator) runs
`systemctl enable --now pg_agentd.service` after staging config.
See BOOTSTRAP.md Phase 1.7.

## Why not `--no-enable` via `dh_installsystemd`?

We deliberately bypass `dh_installsystemd` entirely. The flag would
work for the `.deb` half but doesn't exist for `.rpm`, and `nfpm`'s
unified-script model is the simpler home for the "don't enable"
posture. One script, two outputs.

## Binary linkage

`cargo build --release` produces a dynamically-linked binary
against glibc (+ libgcc_s, libm). No system OpenSSL or D-Bus
library — rustls + zbus are pure-Rust. The resulting binary runs on
any modern Debian/RHEL host without explicit package dependencies
beyond what's in libc6 / glibc, which is universal.

For maximal portability (older distros, minimal containers) build
against `x86_64-unknown-linux-musl` for a fully static binary — a
few MB larger but zero runtime deps. Not the default; the glibc
build is what `cargo build --release` produces today.

## What's NOT in the package

- `/etc/pg_agent/config.toml` — Ansible writes the real one. We
  ship the dir empty + a sample under `/usr/share/pg_agent/`.
- `/etc/pg_agent/tls/` — operator's responsibility (cert
  distribution lives with the wider Ansible cert role).
- `~postgres/.postgresql/` — libpq's default for replication
  cert material; Ansible places certs there. Not the package's
  business.
- `~postgres/.pcppass` — pgpool's PCP password file. Ansible
  writes it.
- `/etc/pgpool2/pgpool_node_id` — pgpool's own per-host file; both
  pg-agent and pgpool read it. Ansible writes it.

The package ships **only the bits we own**: binaries, the systemd
unit, the sample config, the license texts.
