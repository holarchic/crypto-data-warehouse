#!/usr/bin/env bash
# Install the pinned rclone release (restic's transport to a B2 bucket, see scripts/offsite-lib.sh)
# on an x86_64 Debian/Ubuntu host, checked against its SHA-256. Distribution packages can be too
# old: Ubuntu 24.04's rclone 1.60 cannot authorize a B2 key restricted to one bucket.
#
# A version bump changes both pins: take the new SHA-256 from the release's SHA256SUMS after
# verifying its signature against the rclone maintainer's signing key.
#
# Usage: ./deploy/install-rclone.sh   (as root)
set -euo pipefail

RCLONE_VERSION="1.75.2"
RCLONE_SHA256="efbfe852181f7191eb3c9043ed1ab49c9b2d0ba045c61c926a5da111c939ec5c"
URL="https://downloads.rclone.org/v${RCLONE_VERSION}/rclone-v${RCLONE_VERSION}-linux-amd64.deb"

if [ "$(uname -m)" != "x86_64" ]; then
    echo "==> FATAL: the pinned rclone package is linux-amd64; this host is $(uname -m)" >&2
    exit 1
fi

tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT

echo "==> Downloading rclone ${RCLONE_VERSION}"
curl -fsSL -o "$tmp/rclone.deb" "$URL"
echo "${RCLONE_SHA256}  $tmp/rclone.deb" | sha256sum -c -
dpkg -i "$tmp/rclone.deb"
rclone version
