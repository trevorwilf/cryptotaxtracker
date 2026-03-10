#!/bin/bash
set -euo pipefail

# =============================================================================
# run_tests.sh — Test runner for the Tax Collector / CryptoTaxTracker
#
# Usage:
#   ./run_tests.sh                    # run all unit tests
#   ./run_tests.sh --unit             # unit tests only (no DB needed)
#   ./run_tests.sh --integration      # integration tests (needs test Postgres)
#   ./run_tests.sh --all              # unit + integration
#   ./run_tests.sh --coverage         # unit tests with coverage report
#   ./run_tests.sh --docker           # run tests inside Docker container
#   ./run_tests.sh --file test_tax_engine  # run specific test file
#
# Prerequisites:
#   pip install -r requirements-test.txt
#   pip install -r app/requirements.txt
# =============================================================================

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
APP_DIR="$SCRIPT_DIR/app"
TEST_DIR="$SCRIPT_DIR/tests"

# Colors
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m'

log()  { echo -e "${GREEN}[test]${NC} $*"; }
warn() { echo -e "${YELLOW}[test]${NC} $*"; }
fail() { echo -e "${RED}[test]${NC} $*" >&2; exit 1; }

# Parse args
MODE="unit"
COVERAGE=false
DOCKER=false
SPECIFIC_FILE=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --unit)        MODE="unit"; shift ;;
    --integration) MODE="integration"; shift ;;
    --all)         MODE="all"; shift ;;
    --coverage)    COVERAGE=true; shift ;;
    --docker)      DOCKER=true; shift ;;
    --file)        SPECIFIC_FILE="$2"; shift 2 ;;
    --help|-h)
      echo "Usage: $0 [--unit|--integration|--all] [--coverage] [--docker] [--file test_name]"
      exit 0
      ;;
    *) SPECIFIC_FILE="$1"; shift ;;
  esac
done

# ── Docker mode ───────────────────────────────────────────────────────────

if $DOCKER; then
  log "Building test image..."
  docker build -t tax-collector-test -f - "$SCRIPT_DIR" <<'DOCKERFILE'
FROM python:3.12-slim
WORKDIR /app
COPY app/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt
COPY requirements-test.txt /tmp/requirements-test.txt
RUN pip install --no-cache-dir -r /tmp/requirements-test.txt
COPY app/ /app/
COPY tests/ /app/tests/
COPY pytest.ini /app/pytest.ini
ENV PYTHONPATH=/app
DOCKERFILE

  log "Running tests in Docker..."
  docker run --rm \
    --network host \
    -e TEST_DATABASE_URL="${TEST_DATABASE_URL:-postgresql+asyncpg://test:test@localhost:5433/test_tax}" \
    tax-collector-test \
    python -m pytest tests/ -v --tb=short
  exit $?
fi

# ── Local mode ────────────────────────────────────────────────────────────

# Ensure we can import the app modules
export PYTHONPATH="${APP_DIR}:${PYTHONPATH:-}"

# Check deps
python -c "import pytest" 2>/dev/null || fail "pytest not installed. Run: pip install -r requirements-test.txt"

echo ""
echo "╔══════════════════════════════════════════════════════════════╗"
echo "║  Tax Collector — Test Suite                                  ║"
echo "╚══════════════════════════════════════════════════════════════╝"
echo ""

# Build pytest args
PYTEST_ARGS="-v --tb=short"

if [ -n "$SPECIFIC_FILE" ]; then
  # Run specific test file
  if [[ "$SPECIFIC_FILE" != test_* ]]; then
    SPECIFIC_FILE="test_${SPECIFIC_FILE}"
  fi
  if [[ "$SPECIFIC_FILE" != *.py ]]; then
    SPECIFIC_FILE="${SPECIFIC_FILE}.py"
  fi
  PYTEST_ARGS="$PYTEST_ARGS $TEST_DIR/$SPECIFIC_FILE"
  log "Running: $SPECIFIC_FILE"
elif [ "$MODE" = "unit" ]; then
  PYTEST_ARGS="$PYTEST_ARGS -m 'not integration and not slow' $TEST_DIR/"
  log "Running unit tests (no DB required)"
elif [ "$MODE" = "integration" ]; then
  PYTEST_ARGS="$PYTEST_ARGS -m integration $TEST_DIR/"
  log "Running integration tests (requires test Postgres on port 5433)"
  # Check if test DB is running
  if ! pg_isready -h localhost -p 5433 -U test -q 2>/dev/null; then
    warn "Test Postgres not running. Starting it..."
    docker compose -f "$SCRIPT_DIR/docker-compose.test.yaml" up -d
    log "Waiting for test DB..."
    sleep 3
  fi
elif [ "$MODE" = "all" ]; then
  PYTEST_ARGS="$PYTEST_ARGS $TEST_DIR/"
  log "Running ALL tests"
fi

if $COVERAGE; then
  PYTEST_ARGS="$PYTEST_ARGS --cov=. --cov-report=term-missing --cov-report=html:coverage_html"
  log "Coverage reporting enabled"
fi

# Run
cd "$SCRIPT_DIR"
log "PYTHONPATH=$PYTHONPATH"
log "pytest $PYTEST_ARGS"
echo ""

python -m pytest $PYTEST_ARGS

EXIT_CODE=$?

echo ""
if [ $EXIT_CODE -eq 0 ]; then
  log "✓  All tests passed!"
  if $COVERAGE; then
    log "Coverage report: coverage_html/index.html"
  fi
else
  fail "✗  $EXIT_CODE test(s) failed"
fi

exit $EXIT_CODE
