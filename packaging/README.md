# Packaging

`cargo-deb` and `cargo-generate-rpm` build the `.deb` and `.rpm`.
Package metadata lives in `crates/pg-agentd/Cargo.toml` under
`[package.metadata.deb]` and `[package.metadata.generate-rpm]` — the
same file that declares the binary, rather than a separate YAML that
can drift from it. Both formats ship the same binaries, the same
systemd unit, and the same scriptlets.

Packages are built from **statically linked musl binaries**. That is
the point, not a detail: a dynamically linked build inherits the build
host's glibc floor, and neither packager can invent a floor the binary
does not declare — which is how a package came to install on Debian 12
and then die at exec (testing/README.md finding 26). Both packagers run
with `--no-build` so they package what `scripts/build-pkgs.sh` produced
rather than triggering a second, host-native build.

## Layout

```
packaging/
├── pg_agentd.service    # systemd unit (matches SPEC §10.4)
├── config.toml.sample   # /usr/share/pg_agent/config.toml.sample
└── scripts/
    ├── postinstall.sh   # mkdir /etc/pg_agent, daemon-reload, restart-if-running
    ├── preremove.sh     # stop on real removal (skip on upgrade)
    ├── postremove.sh    # disable + daemon-reload on real removal
    └── deb/             # Debian-named symlinks to the three above
        ├── postinst -> ../postinstall.sh
        ├── prerm    -> ../preremove.sh
        └── postrm   -> ../postremove.sh
```

One set of scripts serves both formats: they were written to handle
both argument conventions (deb's `configure`/`upgrade` strings and
rpm's instance counts). cargo-deb wants a directory of Debian-named
files, hence the symlinks; cargo-generate-rpm takes explicit paths.

The scripts are deliberately **bare `systemctl`** — no
`deb-systemd-helper`, no `dh_installsystemd`. `systemctl` works
identically on Debian and RHEL, so one implementation covers both. See
SPEC §10.4 and BOOTSTRAP.md Phase 1.1 for the
"deliberately-do-not-auto-enable" rationale.

`/etc/pg_agent` is created by the post-install scriptlet rather than
shipped as a packaged directory: neither tool has a file-less directory
asset, and an empty directory is not worth a placeholder file.

## Gotchas worth knowing before editing the metadata

- **cargo-generate-rpm scripts take "a string OR a file path"** and
  resolve the path themselves. A path it cannot resolve is not an
  error — it becomes an inline script whose body is the path text, so
  the package installs cleanly and does nothing. Verify with
  `rpm -qp --scripts <pkg>` after any change.
- **`recommends` is a sub-table** for the RPM (`name = "version-req"`,
  Cargo-style) and a plain string for the deb. Sub-tables must stay
  last in the section: TOML puts every key after a sub-table header
  inside that sub-table.
- **Asset paths are relative to `crates/pg-agentd/Cargo.toml`**, hence
  the `../../` prefixes.
- **The deb synopsis is the crate's `description` field** verbatim, so
  that field is kept to one short line and the detail lives in
  `extended-description`.
- cargo-deb has no arbitrary control fields, so there is no `Bugs:`
  header; the issues URL lives in the extended description instead.

## Build

Use the wrapper script. It builds the release binaries **for the musl
target**, stages them into `dist/staging/`, and invokes both packagers
against that staging directory:

```sh
scripts/build-pkgs.sh             # both .deb and .rpm
scripts/build-pkgs.sh deb         # just .deb
scripts/build-pkgs.sh rpm         # just .rpm
VERSION=1.2.3-rc1 scripts/build-pkgs.sh   # override the version
TARGET=x86_64-unknown-linux-gnu scripts/build-pkgs.sh  # escape hatch
```

Outputs land in `dist/` (git-ignored).

Prerequisites: `cargo install cargo-deb cargo-generate-rpm`,
`rustup target add x86_64-unknown-linux-musl`, and a musl C toolchain
(`musl-tools` on Debian/Ubuntu) for ring's assembly.

Staging exists because neither tool expands environment variables in
asset paths, and cargo-deb resolves them relative to the manifest — so
the target triple cannot be templated into the metadata. Staging to a
fixed location keeps one build feeding both packagers, and keeps the
packaged artifact traceable to the build that produced it.

The `TARGET` escape hatch exists for debugging only. Shipping a
dynamically linked package reintroduces finding 26 — and the
`bookworm-pg15` matrix cell is the thing that will catch it, since that
distro cannot run such a build at all.

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
work for the `.deb` half but has no `.rpm` equivalent, so a shared
scriptlet is the simpler home for the "don't enable" posture. One
script, two outputs. cargo-deb has a `systemd-units` feature that
would generate enable/start scriptlets for us; it is deliberately NOT
used, because starting the daemon before Ansible has staged a
`config.toml` is precisely what this posture avoids.

## Binary linkage

**Statically linked against musl. Zero runtime dependencies — no
libc, no libgcc_s, no libm.** That is why both packages declare no
`Depends:`/`Requires:` beyond soft recommends: there is nothing to
depend on.

This is feasible because the dependency set is pure Rust apart from
ring's C/asm — no system OpenSSL (rustls) and no system D-Bus (zbus).
Keeping it that way is a constraint, not an accident: see the
`tokio-rustls` entry in the workspace `Cargo.toml`, which pins
`default-features = false` precisely so a default-feature change
cannot quietly reintroduce a C dependency and with it a glibc floor.

A plain `cargo build --release` still produces a dynamically linked
glibc binary — fine for development, **wrong to ship**. It carries the
build host's glibc floor while the package declares none, so it
installs on an older distro and dies at exec (finding 26). Always
package via `scripts/build-pkgs.sh`.

One accepted consequence: **NSS plugins do not work.** A static musl
binary resolves names through DNS and `/etc/hosts` only, ignoring
`nsswitch.conf`, so LDAP/SSSD/mDNS-based host resolution is
unsupported. Stated rather than worked around — the deployments this
targets discover peers through DNS.

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
- `pgpool_node_id` — pgpool's own per-host file; both pg-agent and
  pgpool read it. Ansible writes it, into pgpool's config directory:
  `/etc/pgpool2` on Debian, `/etc/pgpool-II` on RHEL. The agent probes
  both (finding 28).

The package ships **only the bits we own**: binaries, the systemd
unit, the sample config, the license texts.
