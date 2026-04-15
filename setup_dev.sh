#!/usr/local/bin/bash
# -----------------------------------------------------------------------------
# setup_dev.sh
#
# Prepares the local development environment:
#   1. Loads .env
#   2. Starts LocalStack if not already running
#   3. Creates the SQS test queue if it doesn't exist
#   4. Creates the S3 cursor bucket if CURSOR_STORAGE=s3
# -----------------------------------------------------------------------------

set -euo pipefail

# ── Colours ──────────────────────────────────────────────────────────────────
RED='\033[0;31m'; YELLOW='\033[1;33m'; GREEN='\033[0;32m'
CYAN='\033[0;36m'; BOLD='\033[1m'; RESET='\033[0m'

info()    { echo -e "${CYAN}[INFO]${RESET}  $*"; }
success() { echo -e "${GREEN}[OK]${RESET}    $*"; }
warn()    { echo -e "${YELLOW}[WARN]${RESET}  $*"; }
error()   { echo -e "${RED}[ERROR]${RESET} $*" >&2; }
die()     { error "$*"; exit 1; }

# ── Config ────────────────────────────────────────────────────────────────────
SQS_QUEUE_NAME="${SQS_QUEUE_NAME:-my-queue}"

# ── Load .env ────────────────────────────────────────────────────────────────
ENV_FILE=".env"
if [[ -f "$ENV_FILE" ]]; then
    set -o allexport
    # shellcheck source=.env
    source "$ENV_FILE"
    set +o allexport
    info "Loaded ${ENV_FILE}"
else
    warn ".env not found — copy .env.example to .env and fill in your credentials."
    exit 1
fi

echo ""
echo -e "${BOLD}━━━  Azure Change Feed Poller — Dev Setup  ━━━${RESET}"
echo ""

# ─────────────────────────────────────────────────────────────────────────────
# 1. LocalStack — start if not running
# ─────────────────────────────────────────────────────────────────────────────
echo ""
info "Checking LocalStack status..."

LOCALSTACK_RUNNING=false
if localstack status 2>&1 | grep -q "running"; then
    LOCALSTACK_RUNNING=true
    success "LocalStack is already running."
else
    info "LocalStack is not running — starting it now..."
    localstack start -d

    info "Waiting for LocalStack to become ready (timeout: ${LOCALSTACK_START_TIMEOUT}s)..."
    ELAPSED=0
    until localstack status 2>&1 | grep -q "running"; do
        sleep 2
        ELAPSED=$((ELAPSED + 2))
        if [[ $ELAPSED -ge $LOCALSTACK_START_TIMEOUT ]]; then
            die "LocalStack did not start within ${LOCALSTACK_START_TIMEOUT}s. Run 'localstack logs' for details."
        fi
    done
    success "LocalStack started."
fi

# ─────────────────────────────────────────────────────────────────────────────
# 2. SQS queue — create if it doesn't exist
# ─────────────────────────────────────────────────────────────────────────────
echo ""
info "Checking SQS queue '${SQS_QUEUE_NAME}' on LocalStack..."

EXISTING_QUEUE_URL=$(awslocal sqs get-queue-url \
    --queue-name "$SQS_QUEUE_NAME" \
    --query "QueueUrl" \
    -o text 2>/dev/null || true)

if [[ -n "$EXISTING_QUEUE_URL" && "$EXISTING_QUEUE_URL" != "None" ]]; then
    SQS_QUEUE_URL="$EXISTING_QUEUE_URL"
    success "Queue already exists: ${SQS_QUEUE_URL}"
else
    info "Queue not found — creating '${SQS_QUEUE_NAME}'..."
    SQS_QUEUE_URL=$(awslocal sqs create-queue \
        --queue-name "$SQS_QUEUE_NAME" \
        --output text)
    success "Queue created: ${SQS_QUEUE_URL}"
fi

# ─────────────────────────────────────────────────────────────────────────────
# 3. S3 cursor bucket — create if CURSOR_STORAGE=s3 and bucket doesn't exist
# ─────────────────────────────────────────────────────────────────────────────
if [[ "${CURSOR_STORAGE:-local}" == "s3" ]]; then
    echo ""
    info "CURSOR_STORAGE=s3 — checking S3 bucket '${CURSOR_S3_BUCKET}'..."

    [[ -n "${CURSOR_S3_BUCKET:-}" ]] || die "CURSOR_S3_BUCKET must be set in .env when CURSOR_STORAGE=s3"

    if awslocal s3api head-bucket --bucket "$CURSOR_S3_BUCKET" &>/dev/null; then
        success "Bucket already exists: ${CURSOR_S3_BUCKET}"
    else
        info "Bucket not found — creating '${CURSOR_S3_BUCKET}'..."
        awslocal s3api create-bucket --bucket "$CURSOR_S3_BUCKET"
        success "Bucket created: ${CURSOR_S3_BUCKET}"
    fi
else
    info "CURSOR_STORAGE=local — skipping S3 bucket creation."
fi

# ── Summary ───────────────────────────────────────────────────────────────────
echo ""
echo -e "${BOLD}━━━  Setup complete  ━━━${RESET}"
echo ""
echo -e "  ${CYAN}SQS_QUEUE_URL${RESET}   ${SQS_QUEUE_URL}"
if [[ "${CURSOR_STORAGE:-local}" == "s3" ]]; then
    echo -e "  ${CYAN}CURSOR_S3_BUCKET${RESET}  ${CURSOR_S3_BUCKET}"
fi
echo ""
echo -e "  Run the poller with:"
echo -e "    ${BOLD}uv run main.py${RESET}"
echo ""
