#!/usr/bin/env bash
# Install the pinned restic release (needed by scripts/backup-offsite.sh) on an x86_64 Linux
# host, checked against its SHA-256. Distribution packages can be too old: the backup needs
# `--stdin-from-command` (restic >= 0.17).
#
# A version bump changes both pins: take the new SHA-256 from the release's SHA256SUMS after
# verifying SHA256SUMS.asc against the restic maintainers' signing key.
#
# Usage: ./deploy/install-restic.sh   (as root)
set -euo pipefail

RESTIC_VERSION="0.19.1"
RESTIC_SHA256="f415415624dcc452f2a02b8c33641791a8c6d6d3b65bbb3543fcf9a25151585c"
URL="https://github.com/restic/restic/releases/download/v${RESTIC_VERSION}/restic_${RESTIC_VERSION}_linux_amd64.bz2"

if [ "$(uname -m)" != "x86_64" ]; then
    echo "==> FATAL: the pinned restic build is linux_amd64; this host is $(uname -m)" >&2
    exit 1
fi

tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT

echo "==> Downloading restic ${RESTIC_VERSION}"
curl -fsSL -o "$tmp/restic.bz2" "$URL"
echo "${RESTIC_SHA256}  $tmp/restic.bz2" | sha256sum -c -
bunzip2 "$tmp/restic.bz2"
install -m 0755 "$tmp/restic" /usr/local/bin/restic
/usr/local/bin/restic version
