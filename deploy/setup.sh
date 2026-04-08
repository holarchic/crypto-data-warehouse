#!/usr/bin/env bash
# One-time server provisioning for candle-collector.
# Run as root on a fresh Ubuntu 24.04 VM.
#
# Usage: ./deploy/setup.sh [GCP_IP_1] [GCP_IP_2] ...
#   GCP IPs are added to UFW allow rules for port 8000.
#   If none provided, only SSH is allowed (use SSH tunnel for API access).
#
# Idempotent — safe to re-run.
set -euo pipefail

# -- Helpers ------------------------------------------------------------------

info()  { echo "==> $*"; }
warn()  { echo "==> WARNING: $*" >&2; }

# -- Prereqs ------------------------------------------------------------------

info "Updating packages"
apt-get update -qq
apt-get upgrade -y -qq

info "Installing prerequisites"
apt-get install -y -qq git curl ufw

# -- Docker -------------------------------------------------------------------

if command -v docker &>/dev/null; then
    info "Docker already installed: $(docker --version)"
else
    info "Installing Docker Engine"
    curl -fsSL https://get.docker.com | sh
fi

# Ensure docker compose plugin is available
if ! docker compose version &>/dev/null; then
    info "Installing Docker Compose plugin"
    apt-get install -y -qq docker-compose-plugin
fi

info "Docker Compose: $(docker compose version --short)"

# Add non-root user to docker group if running as root via sudo
if [ -n "${SUDO_USER:-}" ]; then
    usermod -aG docker "$SUDO_USER"
    info "Added $SUDO_USER to docker group"
fi

# -- Docker log rotation ------------------------------------------------------

DAEMON_JSON="/etc/docker/daemon.json"
if [ ! -f "$DAEMON_JSON" ] || ! grep -q "max-size" "$DAEMON_JSON" 2>/dev/null; then
    info "Configuring Docker log rotation"
    cat > "$DAEMON_JSON" <<'JSONEOF'
{
  "log-driver": "json-file",
  "log-opts": {
    "max-size": "10m",
    "max-file": "3"
  }
}
JSONEOF
    systemctl restart docker
    info "Docker daemon restarted with log rotation"
else
    info "Docker log rotation already configured"
fi

# -- UFW firewall (defense-in-depth) ------------------------------------------
# Primary firewall is Hetzner Cloud Firewall (hypervisor level).
# UFW is a second layer. Docker manipulates iptables directly and can
# bypass UFW — the Hetzner firewall is what you actually trust.

info "Configuring UFW"
ufw default deny incoming
ufw default allow outgoing
ufw allow 22/tcp comment "SSH"

# Add GCP IPs for API access
GCP_IPS=("$@")
if [ ${#GCP_IPS[@]} -gt 0 ]; then
    for ip in "${GCP_IPS[@]}"; do
        ufw allow from "$ip" to any port 8000 proto tcp comment "candle-collector API"
        info "UFW: allowed $ip -> port 8000"
    done
else
    warn "No GCP IPs provided. Port 8000 not opened in UFW."
    warn "Use SSH tunnel for API access: ssh -L 8000:localhost:8000 root@<server-ip>"
fi

# Enable UFW (non-interactive)
echo "y" | ufw enable
ufw reload

# -- Project directory --------------------------------------------------------

PROJECT_DIR="/opt/candle-collector"
if [ ! -d "$PROJECT_DIR" ]; then
    mkdir -p "$PROJECT_DIR"
    info "Created $PROJECT_DIR"
else
    info "$PROJECT_DIR already exists"
fi

# -- Summary ------------------------------------------------------------------

info "Setup complete"
echo ""
echo "  Docker:    $(docker --version)"
echo "  Compose:   $(docker compose version --short)"
echo "  Log rotation: 3 x 10MB per container"
echo "  UFW status:"
ufw status numbered
echo ""
echo "Next steps:"
echo "  1. Generate deploy key:  ssh-keygen -t ed25519 -C 'deploy-key'"
echo "  2. Add public key to GitHub repo -> Settings -> Deploy Keys"
echo "  3. Clone repo:  git clone git@github.com:holarchic/candle-collector.git $PROJECT_DIR"
echo "  4. Create .env: cp $PROJECT_DIR/.env.example $PROJECT_DIR/.env && nano $PROJECT_DIR/.env"
echo "  5. Deploy:      $PROJECT_DIR/deploy/deploy.sh"
