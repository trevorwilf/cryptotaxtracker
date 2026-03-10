#!/bin/bash
set -euo pipefail

# =============================================================================
# Build_tax_collector.sh
#
# Builds the Tax Collector Docker image from:
#   https://github.com/trevorwilf/cryptotaxtracker.git  (branch: main)
#
# The script clones (or updates) the repo, verifies the project structure,
# then builds the Docker image.  Follows the same pattern as the other
# Trading Pod build scripts (Build_hummingbot_nonkyc.sh, etc.).
#
# Usage:
#   chmod +x Build_tax_collector.sh
#   ./Build_tax_collector.sh                  # build with defaults
#   ./Build_tax_collector.sh --no-cache       # force full Docker rebuild
#   ./Build_tax_collector.sh --tag v2         # custom tag
#   ./Build_tax_collector.sh --dir /tmp/x     # custom working directory
#   ./Build_tax_collector.sh --branch dev     # use a different branch
# =============================================================================

# ── Configuration ──────────────────────────────────────────────────────────

REPO="https://github.com/trevorwilf/cryptotaxtracker.git"
BRANCH="main"

IMAGE_NAME="tax-collector"
IMAGE_TAG="latest"

# Working directory for the build
BUILD_DIR="${BUILD_DIR:-/tmp/cryptotaxtracker-build}"

# Docker build flags
DOCKER_BUILD_FLAGS=""

# ── Parse CLI args ─────────────────────────────────────────────────────────

while [[ $# -gt 0 ]]; do
  case "$1" in
    --tag)      IMAGE_TAG="$2"; shift 2 ;;
    --tag=*)    IMAGE_TAG="${1#*=}"; shift ;;
    --branch)   BRANCH="$2"; shift 2 ;;
    --branch=*) BRANCH="${1#*=}"; shift ;;
    --no-cache) DOCKER_BUILD_FLAGS="--no-cache"; shift ;;
    --dir)      BUILD_DIR="$2"; shift 2 ;;
    --dir=*)    BUILD_DIR="${1#*=}"; shift ;;
    --help|-h)
      echo "Usage: $0 [--tag TAG] [--branch BRANCH] [--no-cache] [--dir BUILD_DIR]"
      echo ""
      echo "  --tag TAG        Docker image tag (default: latest)"
      echo "  --branch BRANCH  Git branch to clone (default: main)"
      echo "  --no-cache       Force full Docker rebuild"
      echo "  --dir DIR        Working directory (default: /tmp/cryptotaxtracker-build)"
      exit 0
      ;;
    *) echo "Unknown option: $1"; exit 1 ;;
  esac
done

# ── Functions ──────────────────────────────────────────────────────────────

log()  { echo "[build] $*"; }
warn() { echo "[build] ⚠  $*"; }
die()  { echo "[build] ✗  $*" >&2; exit 1; }
ok()   { echo "[build] ✓  $*"; }

check_prereqs() {
  for cmd in git docker; do
    command -v "$cmd" >/dev/null 2>&1 || die "'$cmd' is required but not found."
  done
  ok "Prerequisites: git, docker"
}

# ── Main ───────────────────────────────────────────────────────────────────

echo "╔══════════════════════════════════════════════════════════════╗"
echo "║  Tax Collector — Docker Image Build                          ║"
echo "╚══════════════════════════════════════════════════════════════╝"
echo ""

check_prereqs

SRC_DIR="$BUILD_DIR/cryptotaxtracker"
mkdir -p "$BUILD_DIR"

# ── Step 1: Clone / update repo ───────────────────────────────────────────

log "Step 1/4: Clone/update source repo"
if [ -d "$SRC_DIR/.git" ]; then
  log "  Updating existing clone..."
  cd "$SRC_DIR"
  git fetch origin "$BRANCH" --depth=1
  git reset --hard "origin/$BRANCH"
  git clean -fdx
else
  log "  Cloning $REPO ($BRANCH)..."
  rm -rf "$SRC_DIR"
  git clone --depth=1 --branch "$BRANCH" "$REPO" "$SRC_DIR"
fi

cd "$SRC_DIR"
REPO_SHA="$(git rev-parse --short HEAD)"
ok "Source repo at $REPO_SHA ($BRANCH)"

# ── Step 2: Verify project structure ──────────────────────────────────────

log "Step 2/4: Verifying project structure"

[ -f "$SRC_DIR/Dockerfile" ]              || die "Dockerfile not found in repo root"
[ -f "$SRC_DIR/app/main.py" ]             || die "app/main.py not found"
[ -f "$SRC_DIR/app/requirements.txt" ]    || die "app/requirements.txt not found"
[ -f "$SRC_DIR/app/database.py" ]         || die "app/database.py not found"
[ -f "$SRC_DIR/app/price_oracle.py" ]     || die "app/price_oracle.py not found"
[ -d "$SRC_DIR/app/exchanges" ]           || die "app/exchanges/ directory not found"
[ -f "$SRC_DIR/app/exchanges/nonkyc.py" ] || die "NonKYC exchange plugin not found"
[ -f "$SRC_DIR/app/exchanges/mexc.py" ]   || die "MEXC exchange plugin not found"

PY_COUNT="$(find "$SRC_DIR/app" -name '*.py' | wc -l | tr -d ' ')"
if [ "${PY_COUNT:-0}" -lt 5 ]; then
  die "Only $PY_COUNT Python files found — project seems incomplete."
fi
ok "Project structure verified ($PY_COUNT Python files)"

# ── Step 3: Build Docker image ────────────────────────────────────────────

log "Step 3/4: Building Docker image"

FULL_TAG="${IMAGE_NAME}:${IMAGE_TAG}"
log "  Image: $FULL_TAG"

docker build $DOCKER_BUILD_FLAGS \
  -t "$FULL_TAG" \
  --label "org.opencontainers.image.description=Tax Collector — trade data aggregator" \
  --label "tax-collector.source.repo=$REPO" \
  --label "tax-collector.source.branch=$BRANCH" \
  --label "tax-collector.source.sha=$REPO_SHA" \
  --label "tax-collector.build.date=$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
  "$SRC_DIR"

ok "Image built: $FULL_TAG"

# ── Step 4: Sanity checks ────────────────────────────────────────────────

log "Step 4/4: Verifying image"

log "  Checking Python imports..."
docker run --rm "$FULL_TAG" python -c "
from exchanges.nonkyc import NonKYCExchange
from exchanges.mexc import MEXCExchange
from exports.xlsx_export import generate_tax_xlsx
from price_oracle import PriceOracle
print('NonKYC plugin:  OK')
print('MEXC plugin:    OK')
print('XLSX export:    OK')
print('Price oracle:   OK')
" 2>&1 && ok "All imports verified" \
  || die "Import verification failed!"

log "  Checking FastAPI app..."
docker run --rm "$FULL_TAG" python -c "
from main import app
print(f'App title: {app.title}')
print(f'Version:   {app.version}')
print(f'Routes:    {len(app.routes)}')
" 2>&1 && ok "FastAPI app verified" \
  || die "FastAPI app failed to load!"

# ── Summary ───────────────────────────────────────────────────────────────

echo ""
echo "╔══════════════════════════════════════════════════════════════╗"
echo "║  Build Complete!                                            ║"
echo "╠══════════════════════════════════════════════════════════════╣"
echo "║                                                            ║"
echo "║  Image:     $FULL_TAG"
echo "║  Repo:      $REPO"
echo "║  Branch:    $BRANCH ($REPO_SHA)"
echo "║  Exchanges: NonKYC, MEXC                                    ║"
echo "║  API Port:  8100                                            ║"
echo "║  Dashboard: http://<TRUENAS_IP>:8100                        ║"
echo "║                                                            ║"
echo "║  Compose:                                                   ║"
echo "║    image: $FULL_TAG"
echo "║                                                            ║"
echo "╚══════════════════════════════════════════════════════════════╝"
echo ""

docker image inspect "$FULL_TAG" --format='Image size: {{.Size}}' 2>/dev/null | \
  awk '{printf "Image size: %.0f MB\n", $3/1024/1024}' || true