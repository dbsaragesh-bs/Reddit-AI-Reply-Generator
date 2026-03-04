#!/bin/bash
# ============================================================
# Start RedditAI Platform with Ngrok Tunneling
# ============================================================

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
INFRA_DIR="$PROJECT_ROOT/infrastructure"
COMPOSE_FILE="$INFRA_DIR/docker-compose.yml"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m'

echo -e "${CYAN}"
echo "============================================"
echo "  RedditAI - Ngrok Public Deployment"
echo "============================================"
echo -e "${NC}"

# Check NGROK_AUTHTOKEN
if [ -z "$NGROK_AUTHTOKEN" ]; then
    source "$INFRA_DIR/.env" 2>/dev/null || true
fi

if [ -z "$NGROK_AUTHTOKEN" ]; then
    echo -e "${RED}ERROR: NGROK_AUTHTOKEN is not set.${NC}"
    echo ""
    echo "Get your token from: https://dashboard.ngrok.com/get-started/your-authtoken"
    echo "Then set it in infrastructure/.env:"
    echo "  NGROK_AUTHTOKEN=your_token_here"
    echo ""
    exit 1
fi

echo -e "${YELLOW}Step 1: Starting all services with ngrok profile...${NC}"
docker compose -f "$COMPOSE_FILE" --profile ngrok up -d --build

echo ""
echo -e "${YELLOW}Step 2: Waiting for ngrok tunnel to establish...${NC}"

MAX_RETRIES=30
RETRY_DELAY=3
FRONTEND_URL=""

for i in $(seq 1 $MAX_RETRIES); do
    TUNNELS=$(curl -s http://localhost:4042/api/tunnels 2>/dev/null || echo "")

    if [ -n "$TUNNELS" ] && echo "$TUNNELS" | grep -q "public_url"; then
        FRONTEND_URL=$(echo "$TUNNELS" | python3 -c "
import sys, json
data = json.load(sys.stdin)
for t in data.get('tunnels', []):
    if t.get('name') == 'frontend':
        print(t['public_url'])
        break
" 2>/dev/null || echo "")

        if [ -n "$FRONTEND_URL" ]; then
            break
        fi
    fi

    echo "  Waiting for tunnel... ($i/$MAX_RETRIES)"
    sleep $RETRY_DELAY
done

if [ -z "$FRONTEND_URL" ]; then
    echo -e "${RED}ERROR: Could not retrieve ngrok tunnel URL.${NC}"
    echo "Check ngrok logs: docker compose -f $COMPOSE_FILE logs ngrok"
    exit 1
fi

echo ""
echo -e "${GREEN}============================================${NC}"
echo -e "${GREEN}  PLATFORM IS LIVE!${NC}"
echo -e "${GREEN}============================================${NC}"
echo ""
echo -e "  ${CYAN}Public URL (share this):${NC}  $FRONTEND_URL"
echo -e "  ${CYAN}Ngrok Dashboard:${NC}          http://localhost:4042"
echo ""
echo -e "  ${YELLOW}Local access still works:${NC}"
echo -e "  Frontend:    http://localhost:3000"
echo -e "  API Gateway: http://localhost:8001"
echo ""
echo -e "${GREEN}Share the Public URL above with users so they can${NC}"
echo -e "${GREEN}log in and converse on the platform!${NC}"
echo ""
echo -e "${YELLOW}To stop: docker compose -f $COMPOSE_FILE --profile ngrok down${NC}"
