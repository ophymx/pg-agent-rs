#!/bin/sh
# Build the pg-agent-rs `.deb` and `.rpm` via nfpm.
#
# Usage:
#   scripts/build-pkgs.sh              # both formats
#   scripts/build-pkgs.sh deb          # just the .deb
#   scripts/build-pkgs.sh rpm          # just the .rpm
#   VERSION=1.2.3 scripts/build-pkgs.sh   # override version (default:
#                                       # Cargo workspace version)
#
# Run from the repository root (it relies on relative paths). nfpm
# resolves `src:` entries relative to its current working directory.

set -eu

cd "$(dirname "$0")/.."

if ! command -v nfpm >/dev/null 2>&1; then
    echo "error: nfpm not found in PATH" >&2
    echo "       install with: go install github.com/goreleaser/nfpm/v2/cmd/nfpm@latest" >&2
    exit 1
fi

# Pick a version. CLI arg of "1.2.3-rc1" isn't supported here — set
# VERSION as an env var instead.
if [ -z "${VERSION:-}" ]; then
    VERSION=$(awk -F'"' '/^version =/ { print $2; exit }' Cargo.toml)
fi
if [ -z "$VERSION" ]; then
    echo "error: could not derive VERSION from Cargo.toml" >&2
    exit 1
fi
export VERSION

# Build the release binaries once.
echo "==> cargo build --release"
cargo build --release

mkdir -p dist

build_deb() {
    out="dist/pg-agent-rs_${VERSION}_amd64.deb"
    echo "==> nfpm pkg deb → ${out}"
    nfpm pkg --config packaging/nfpm.yaml --packager deb --target "$out"
}

build_rpm() {
    out="dist/pg-agent-rs-${VERSION}-1.x86_64.rpm"
    echo "==> nfpm pkg rpm → ${out}"
    nfpm pkg --config packaging/nfpm.yaml --packager rpm --target "$out"
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
