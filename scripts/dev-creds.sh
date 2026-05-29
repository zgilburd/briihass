#!/usr/bin/env bash
# dev-creds.sh — manage briihass dev credentials.
#
# Storage model: plaintext at ~/.config/briihass/credentials.env, mode 600.
# The file lives OUTSIDE the repo and is never committed to anything. For
# production, inject the same env vars from your platform's secret manager
# (e.g. sealed secrets in your infra repo). See
# docs/adr/0003-credential-handling.md.
#
# Subcommands:
#   --check   (default) Verify the file exists, has 600 perms, and that
#             required keys are present. Prints "OK" or what's missing.
#   --edit    Open the file in $EDITOR. Creates it from a template if missing,
#             and re-applies 600 perms on exit.
#   --path    Echo the absolute path to the credentials file.
#   --print   Echo a `set -a; source <path>; set +a` line you can eval to
#             load creds into the current shell.
#
# This script intentionally does NOT support encryption, intended for dev/test
# only. If you want your credentials to be safe, store them safely in k8s.

set -euo pipefail

CRED_DIR="${HOME}/.config/briihass"
CRED_FILE="${CRED_DIR}/credentials.env"
REQUIRED_KEYS=(
  VRIOT_API_USER VRIOT_API_PASS
  VSZ_API_USER VSZ_API_PASS VSZ_HOST
  INGEST_SHARED_SECRET
  MQTT_USER MQTT_PASS
)

usage() {
  sed -n '2,18p' "$0"
  exit "${1:-0}"
}

ensure_dir() {
  mkdir -p "$CRED_DIR"
  chmod 700 "$CRED_DIR"
}

cmd_path() { echo "$CRED_FILE"; }

cmd_print() {
  [[ -f "$CRED_FILE" ]] || { echo "no credentials file at $CRED_FILE — run --edit first" >&2; exit 1; }
  echo "set -a; source '$CRED_FILE'; set +a"
}

cmd_check() {
  if [[ ! -f "$CRED_FILE" ]]; then
    echo "missing: $CRED_FILE (run: $0 --edit)" >&2
    exit 1
  fi
  local perms
  perms="$(stat -c '%a' "$CRED_FILE")"
  if [[ "$perms" != "600" ]]; then
    echo "warning: $CRED_FILE perms are $perms, should be 600. Fixing." >&2
    chmod 600 "$CRED_FILE"
  fi
  local missing=()
  for key in "${REQUIRED_KEYS[@]}"; do
    grep -qE "^${key}=" "$CRED_FILE" || missing+=("$key")
  done
  if (( ${#missing[@]} > 0 )); then
    echo "missing keys in $CRED_FILE:" >&2
    printf '  - %s\n' "${missing[@]}" >&2
    exit 1
  fi
  echo "OK: $CRED_FILE has all required keys and 600 perms."
}

cmd_edit() {
  ensure_dir
  if [[ ! -f "$CRED_FILE" ]]; then
    cat > "$CRED_FILE" <<'EOF'
# briihass dev credentials. KEY=value, shell-sourceable. Mode 600.
# Lives OUTSIDE the repo at ~/.config/briihass/credentials.env.
# NEVER paste this content into chat, commits, or PRs.
#
# vRIoT controller (mgmt API)
VRIOT_API_USER=admin
VRIOT_API_PASS=changeme
#
# vSmartZone (AP/zone/venue enrichment)
VSZ_API_USER=admin
VSZ_API_PASS=changeme
VSZ_HOST=vsz.your.domain
#
# Inbound shared secret — value the vRIoT iBeacon plugin presents on POSTs.
# Exact mechanism (header, basic auth, HMAC, etc.) is TBD until captured —
# see ADR-0001. Generate with: openssl rand -base64 48
INGEST_SHARED_SECRET=changeme-generate-with-openssl
#
# Mosquitto credentials (the same broker HA already uses)
MQTT_USER=briihass
MQTT_PASS=changeme
EOF
    chmod 600 "$CRED_FILE"
    echo "seeded $CRED_FILE with template — edit the changeme values" >&2
  fi
  chmod 600 "$CRED_FILE"
  "${EDITOR:-vi}" "$CRED_FILE"
  chmod 600 "$CRED_FILE"
}

case "${1:---check}" in
  --check) cmd_check ;;
  --edit)  cmd_edit ;;
  --path)  cmd_path ;;
  --print) cmd_print ;;
  -h|--help) usage 0 ;;
  *) echo "unknown subcommand: $1" >&2; usage 1 ;;
esac
