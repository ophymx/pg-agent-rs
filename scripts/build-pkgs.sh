#!/bin/sh
# Build the pg-agent-rs `.deb` and `.rpm` via cargo-deb and
# cargo-generate-rpm. Package metadata lives in
# crates/pg-agentd/Cargo.toml; see packaging/README.md.
#
# Usage:
#   scripts/build-pkgs.sh              # both formats
#   scripts/build-pkgs.sh deb          # just the .deb
#   scripts/build-pkgs.sh rpm          # just the .rpm
#   VERSION=1.2.3 scripts/build-pkgs.sh   # stamp exactly this version
#
# Without VERSION, the package version is the Cargo workspace version
# when HEAD sits on that version's tag with a clean tree, and
# `<workspace>+git<date>.<sha>[.dirty]` otherwise — so a dev build is
# never mistaken for the release it was cut from (see "Versions").
#
# Run from the repository root (it relies on relative paths).

set -eu

cd "$(dirname "$0")/.."

# Check the tool for the format actually requested, so building just
# the .deb does not demand the RPM packager. Checked at all because the
# failure mode otherwise is a bare "command not found" for a cargo
# subcommand, which reads like a broken toolchain rather than a missing
# one-line install.
need() {
    command -v "$1" >/dev/null 2>&1 && return 0
    echo "error: $1 not found in PATH" >&2
    echo "       install with: cargo install $1" >&2
    exit 1
}

# Versions.
#
# The workspace version alone is not enough to name a package, because
# the tree is at a released version for the whole stretch between one
# tag and the next bump. A dev build cut there would be stamped as the
# release itself — same version, different binary — and nothing
# downstream could tell them apart: apt's `state: latest` sees nothing
# to upgrade, and `--reinstall` fetches the release while reporting no
# change. The db cluster takes dev builds by `dpkg -i` (home-ansible
# tools/pushdb), so this is the case that happens, not a hypothetical.
#
# So a build is stamped as the release only when HEAD is exactly the
# release's tag and the tree is clean. Anything else gets
# `<workspace>+git<yyyymmdd>.<sha>`, with `.dirty` appended when the
# tree has uncommitted changes. That string sorts ABOVE the release
# under both dpkg and rpm comparison, which is the ordering a dev loop
# wants: the pushed build stays until pkg publishes the next version,
# and a node goes back to the release by name
# (`apt-get install --allow-downgrades pg-agent-rs=<workspace>`),
# never by accident. `dirty` is flagged because a build from an
# uncommitted tree has no sha anyone can check out.
#
# VERSION in the environment stamps exactly that string, no suffix,
# for the hand-cut case. It is the package version, not a Cargo one,
# so "1.2.3-rc1" is fine for the .deb; rpm forbids `-` in a version,
# so the rpm half rejects it.
#
# The rpm carries the dev stamp in its Release field (`1.git…`) rather
# than Version, which is where rpm convention puts it and the only
# place cargo-generate-rpm lets the script set it. The ordering
# argument is the same: `1.git…` > `1`.
WORKSPACE_VERSION=$(awk -F'"' '/^version =/ { print $2; exit }' Cargo.toml)
if [ -z "$WORKSPACE_VERSION" ]; then
    echo "error: could not derive the workspace version from Cargo.toml" >&2
    exit 1
fi

RPM_RELEASE=1
if [ -n "${VERSION:-}" ]; then
    DEB_VERSION="$VERSION"
    RPM_VERSION="$VERSION"
else
    VERSION="$WORKSPACE_VERSION"
    DEB_VERSION="$VERSION"
    RPM_VERSION="$VERSION"
    dirty=""
    [ -z "$(git status --porcelain 2>/dev/null)" ] || dirty=".dirty"
    if [ -n "$dirty" ] \
        || ! git describe --tags --exact-match --match "v${VERSION}" HEAD >/dev/null 2>&1; then
        stamp="git$(date -u +%Y%m%d).$(git rev-parse --short=7 HEAD)${dirty}"
        DEB_VERSION="${VERSION}+${stamp}"
        RPM_RELEASE="1.${stamp}"
        echo "==> dev build: HEAD is not a clean v${VERSION}; stamping ${DEB_VERSION}"
    fi
fi
export VERSION

# Build the release binaries once, STATICALLY against musl.
#
# Not a preference — a correctness fix. A dynamically-linked build
# carries the build host's glibc floor and the package declares none,
# so it installs on an older distro and dies at exec with
# `GLIBC_2.39 not found` (testing/FINDINGS.md finding 26). Debian 12 and
# RHEL 9 are both below a Debian 13 builder's floor, which rules out
# the .rpm target entirely. Static musl removes the floor rather than
# documenting it: one artifact verified running on Debian 12, Ubuntu
# 24.04, Rocky 9 and Alpine.
#
# Feasible here because the dependency set is pure Rust apart from
# ring's C/asm — see the tokio-rustls note in the workspace Cargo.toml,
# which keeps it that way. Requires `musl-tools` and
# `rustup target add x86_64-unknown-linux-musl`.
#
# NSS plugins are unsupported by construction: a static musl binary
# resolves via DNS and /etc/hosts, not nsswitch.conf. That is a stated
# boundary (packaging/README.md), and the deployments this targets
# discover peers through DNS.
TARGET="${TARGET:-x86_64-unknown-linux-musl}"
export TARGET

# Package architecture, derived from the target rather than assumed.
#
# The two formats spell the same machine differently — Debian says
# amd64/arm64 where RPM says x86_64/aarch64 — and BOTH names have to be
# right for reasons beyond the filename: the architecture recorded
# INSIDE the package is what apt and dnf use to refuse a package built
# for another machine. A mislabelled arm64 build would install happily
# on x86_64 and die at exec, which is finding 26's failure mode wearing
# a different hat.
#
# Unknown targets stop here. Guessing an architecture name is exactly
# the kind of helpfulness that produces a package claiming to be
# something it is not.
case "$TARGET" in
    x86_64-unknown-linux-musl)  DEB_ARCH=amd64; RPM_ARCH=x86_64 ;;
    aarch64-unknown-linux-musl) DEB_ARCH=arm64; RPM_ARCH=aarch64 ;;
    *)
        echo "error: no package-architecture mapping for TARGET=$TARGET" >&2
        echo "       add one to scripts/build-pkgs.sh rather than letting the" >&2
        echo "       packagers guess" >&2
        exit 2
        ;;
esac

echo "==> cargo build --release --target ${TARGET}"
cargo build --release --target "$TARGET"

mkdir -p dist

# Stage the built binaries at a FIXED path, because neither packager
# expands environment variables in asset paths and cargo-deb resolves
# them relative to the manifest — so the target triple cannot be
# templated into the metadata. Staging keeps one build feeding both
# packagers, and keeps the packaged artifact traceable to it.
mkdir -p dist/staging
for b in pg_agentd pg_agentc pg_agentctl; do
    cp -f "target/${TARGET}/release/${b}" "dist/staging/${b}"
done
echo "==> staged $(file -b dist/staging/pg_agentd | cut -d, -f1-2)"

build_deb() {
    need cargo-deb
    out="dist/pg-agent-rs_${DEB_VERSION}_${DEB_ARCH}.deb"
    # --no-build: the static musl build above IS the artifact. Letting
    # cargo-deb rebuild would produce a host-native dynamic binary —
    # finding 26 walking straight back in.
    #
    # --target is NOT a build instruction here (--no-build already
    # settled that); it is how cargo-deb learns which architecture to
    # stamp on the package. The assets are fixed paths under
    # dist/staging, so it changes nothing else.
    #
    # --deb-version: the control file's Version, which is what dpkg and
    # apt compare. Without it cargo-deb copies Cargo.toml's, and the
    # filename would be the only thing that knew this was a dev build.
    echo "==> cargo deb → ${out}"
    cargo deb --no-build --no-strip --target "$TARGET" -p pg-agentd \
        --deb-version "$DEB_VERSION" --output "$out"
}

build_rpm() {
    need cargo-generate-rpm
    case "$RPM_VERSION" in
        *-*)
            echo "error: rpm forbids '-' in a version (VERSION=${RPM_VERSION})" >&2
            exit 2
            ;;
    esac
    out="dist/pg-agent-rs-${RPM_VERSION}-${RPM_RELEASE}.${RPM_ARCH}.rpm"
    # -a rather than --target: --target would send it looking for
    # artifacts under a target subdirectory, and the binaries it
    # packages are staged at a fixed path. Only the recorded
    # architecture needs to change.
    #
    # -s overlays [package.metadata.generate-rpm]; `release` is the
    # one field the dev stamp lands in (see "Versions"). The version
    # itself is always Cargo.toml's — cargo-generate-rpm has no
    # override — so VERSION=… is refused here rather than silently
    # producing a file whose name disagrees with its header.
    if [ "$RPM_VERSION" != "$WORKSPACE_VERSION" ]; then
        echo "error: cargo-generate-rpm stamps the Cargo.toml version (${WORKSPACE_VERSION});" >&2
        echo "       VERSION=${RPM_VERSION} cannot be applied to the .rpm — bump Cargo.toml instead" >&2
        exit 2
    fi
    echo "==> cargo generate-rpm → ${out}"
    cargo generate-rpm -a "$RPM_ARCH" -p crates/pg-agentd \
        -s "release = \"${RPM_RELEASE}\"" --output "$out"
}

case "${1:-all}" in
    all)
        build_deb
        build_rpm
        ;;
    deb)
        build_deb
        ;;
    rpm)
        build_rpm
        ;;
    *)
        echo "usage: $0 [all|deb|rpm]" >&2
        exit 2
        ;;
esac

echo ""
echo "==> dist/"
ls -lh dist/
