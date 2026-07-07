#!/bin/bash
set -e

PROJECT="/Users/admin/Documents/OverVK"
LOGDIR="$PROJECT/test-logs"
BIN="$LOGDIR/overvk"
SERVER_LOG="$LOGDIR/server.log"
CLIENT_LOG="$LOGDIR/client.log"
RESULTS="$LOGDIR/results.log"
SOCKS_PORT=8888
BOOTSTRAP_WAIT=60
REQUEST_TIMEOUT=120

mkdir -p "$LOGDIR"
cd "$PROJECT"

echo "=== Building overvk ===" | tee "$RESULTS"
go build -o "$BIN" ./cmd/overvk
echo "Build OK" | tee -a "$RESULTS"

cleanup() {
    echo "Cleaning up..."
    [ -n "$CLIENT_PID" ] && kill "$CLIENT_PID" 2>/dev/null; wait "$CLIENT_PID" 2>/dev/null
    [ -n "$SERVER_PID" ] && kill "$SERVER_PID" 2>/dev/null; wait "$SERVER_PID" 2>/dev/null
    true
}
trap cleanup EXIT

echo "=== Starting server ===" | tee -a "$RESULTS"
"$BIN" config/server.yaml >"$SERVER_LOG" 2>&1 &
SERVER_PID=$!
sleep 2

echo "=== Starting client ===" | tee -a "$RESULTS"
"$BIN" config/client.yaml >"$CLIENT_LOG" 2>&1 &
CLIENT_PID=$!

echo "=== Waiting for SOCKS5 on port $SOCKS_PORT ===" | tee -a "$RESULTS"
DEADLINE=$((SECONDS + BOOTSTRAP_WAIT))
while [ $SECONDS -lt $DEADLINE ]; do
    if nc -z 127.0.0.1 $SOCKS_PORT 2>/dev/null; then
        echo "SOCKS5 ready after ${SECONDS}s" | tee -a "$RESULTS"
        break
    fi
    sleep 1
done

if ! nc -z 127.0.0.1 $SOCKS_PORT 2>/dev/null; then
    echo "SOCKS5 not ready after ${BOOTSTRAP_WAIT}s — aborting" | tee -a "$RESULTS"
    exit 1
fi

sleep 3

# URLs from smallest to largest
URLS=(
    "http://example.com"
    "https://ifconfig.me"
    "https://www.google.com/robots.txt"
    "https://cdn.jsdelivr.net/npm/jquery@3.7.1/dist/jquery.min.js"
    "https://norvig.com/big.txt"
)

echo "" | tee -a "$RESULTS"
echo "=== Running fetch tests ===" | tee -a "$RESULTS"

for url in "${URLS[@]}"; do
    echo -n "Fetching: $url ... " | tee -a "$RESULTS"
    HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}|%{size_download}|%{time_total}|%{time_starttransfer}" \
        --socks5-hostname "127.0.0.1:$SOCKS_PORT" \
        --max-time "$REQUEST_TIMEOUT" \
        "$url" 2>&1) || true

    IFS='|' read -r CODE SIZE TOTAL TTFB <<< "$HTTP_CODE"
    echo "HTTP $CODE | ${SIZE}B | TTFB=${TTFB}s | Total=${TOTAL}s" | tee -a "$RESULTS"

    sleep 2
done

echo "" | tee -a "$RESULTS"
echo "=== Tests complete ===" | tee -a "$RESULTS"
echo "Server log: $SERVER_LOG" | tee -a "$RESULTS"
echo "Client log: $CLIENT_LOG" | tee -a "$RESULTS"

sleep 3
echo "=== Log sizes ===" | tee -a "$RESULTS"
wc -l "$SERVER_LOG" "$CLIENT_LOG" | tee -a "$RESULTS"
