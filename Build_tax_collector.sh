#!/bin/bash
set -euo pipefail

# =============================================================================
# Build_tax_collector.sh
#
# Builds the Tax Collector Docker image for the Hummingbot Trading Pod stack.
#
# Usage:
#   chmod +x Build_tax_collector.sh
#   ./Build_tax_collector.sh                  # build with defaults
#   ./Build_tax_collector.sh --no-cache       # force full Docker rebuild
#   ./Build_tax_collector.sh --dir /tmp/x     # custom build context
# =============================================================================

IMAGE_NAME="tax-collector"
IMAGE_TAG="latest"
DOCKER_BUILD_FLAGS=""

# ── Parse CLI args ─────────────────────────────────────────────────────────

while [[ $# -gt 0 ]]; do
  case "$1" in
    --tag)      IMAGE_TAG="$2"; shift 2 ;;
    --tag=*)    IMAGE_TAG="${1#*=}"; shift ;;
    --no-cache) DOCKER_BUILD_FLAGS="--no-cache"; shift ;;
    --dir)      BUILD_DIR="$2"; shift 2 ;;
    --dir=*)    BUILD_DIR="${1#*=}"; shift ;;
    --help|-h)
      echo "Usage: $0 [--tag TAG] [--no-cache] [--dir BUILD_DIR]"
      exit 0
      ;;
    *) echo "Unknown option: $1"; exit 1 ;;
  esac
done

# Default build dir = directory containing this script
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
BUILD_DIR="${BUILD_DIR:-$SCRIPT_DIR}"

log()  { echo "[build] $*"; }
ok()   { echo "[build] ✓  $*"; }
die()  { echo "[build] ✗  $*" >&2; exit 1; }

echo "╔══════════════════════════════════════════════════════════════╗"
echo "║  Tax Collector — Docker Image Build                          ║"
echo "╚══════════════════════════════════════════════════════════════╝"
echo ""

# ── Prerequisites ──────────────────────────────────────────────────────────

command -v docker >/dev/null 2>&1 || die "'docker' is required but not found."
ok "Docker found"

# ── Verify build context ──────────────────────────────────────────────────

[ -f "$BUILD_DIR/Dockerfile" ]              || die "Dockerfile not found in $BUILD_DIR"
[ -f "$BUILD_DIR/app/main.py" ]             || die "app/main.py not found"
[ -f "$BUILD_DIR/app/requirements.txt" ]    || die "app/requirements.txt not found"
[ -f "$BUILD_DIR/app/exchanges/nonkyc.py" ] || die "NonKYC exchange plugin not found"
[ -f "$BUILD_DIR/app/exchanges/mexc.py" ]   || die "MEXC exchange plugin not found"

PY_COUNT="$(find "$BUILD_DIR/app" -name '*.py' | wc -l | tr -d ' ')"
ok "Build context verified ($PY_COUNT Python files)"

# ── Build ─────────────────────────────────────────────────────────────────

FULL_TAG="${IMAGE_NAME}:${IMAGE_TAG}"
log "Building image: $FULL_TAG"

docker build $DOCKER_BUILD_FLAGS \
  -t "$FULL_TAG" \
  --label "org.opencontainers.image.description=Tax Collector — trade data aggregator" \
  --label "tax-collector.build.date=$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
  "$BUILD_DIR"

ok "Image built: $FULL_TAG"

# ── Sanity checks ─────────────────────────────────────────────────────────

log "Verifying Python imports..."
docker run --rm "$FULL_TAG" python -c "
from exchanges.nonkyc import NonKYCExchange
from exchanges.mexc import MEXCExchange
from exports.xlsx_export import generate_tax_xlsx
print('NonKYC plugin: OK')
print('MEXC plugin:   OK')
print('XLSX export:   OK')
" 2>&1 && ok "All imports verified" \
  || die "Import verification failed!"

log "Verifying FastAPI app loads..."
docker run --rm "$FULL_TAG" python -c "
from main import app
print(f'App title: {app.title}')
print(f'Routes: {len(app.routes)}')
" 2>&1 && ok "FastAPI app verified" \
  || die "FastAPI app failed to load!"

# ── Summary ───────────────────────────────────────────────────────────────

echo ""
echo "╔══════════════════════════════════════════════════════════════╗"
echo "║  Build Complete!                                            ║"
echo "╠══════════════════════════════════════════════════════════════╣"
echo "║                                                            ║"
echo "║  Image:  $FULL_TAG"
echo "║  Exchanges: NonKYC, MEXC                                    ║"
echo "║  API Port:  8100                                            ║"
echo "║                                                            ║"
echo "║  Add to your compose file with:                             ║"
echo "║    image: $FULL_TAG"
echo "║                                                            ║"
echo "╚══════════════════════════════════════════════════════════════╝"
echo ""

docker image inspect "$FULL_TAG" --format='Image size: {{.Size}}' 2>/dev/null | \
  awk '{printf "Image size: %.0f MB\n", $3/1024/1024}' || true
