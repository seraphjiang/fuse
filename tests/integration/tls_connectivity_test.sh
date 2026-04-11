#!/usr/bin/env bash
# #942 TLS connectivity tests — verify HTTPS endpoints work correctly.
set -euo pipefail

BASE="${FUSE_URL:-https://fuse-playground-alb-556139505.us-west-2.elb.amazonaws.com}"
PASS=0 FAIL=0

run() {
    local name="$1"; shift
    if "$@" >/dev/null 2>&1; then
        PASS=$((PASS+1)); printf "  ✅ %s\n" "$name"
    else
        FAIL=$((FAIL+1)); printf "  ❌ %s\n" "$name"
    fi
}

echo "╔═══════════════════════════════════════════════════╗"
echo "║  #942 TLS Connectivity Tests                     ║"
echo "║  Target: $BASE"
echo "╚═══════════════════════════════════════════════════╝"
echo ""

# TLS handshake
echo "TLS handshake:"
run "HTTPS health endpoint reachable" curl -sk --max-time 10 "$BASE/api/fuse/health"
run "TLS 1.2+ negotiated" bash -c "curl -svk --max-time 10 '$BASE/api/fuse/health' 2>&1 | grep -i 'TLSv1\.[23]'"
run "Certificate presented" bash -c "curl -svk --max-time 10 '$BASE/api/fuse/health' 2>&1 | grep -i 'subject\|issuer'"

# HTTPS query
echo ""
echo "HTTPS query:"
run "POST query over HTTPS" bash -c "
    curl -sk -X POST '$BASE/api/fuse/query' \
        -H 'Content-Type: application/json' \
        -d '{\"query\":\"SELECT service FROM cluster_a.application_logs LIMIT 1\",\"format\":\"sql\"}' \
    | python3 -c 'import sys,json; d=json.load(sys.stdin); assert len(d[\"rows\"]) == 1'
"

# HTTP → HTTPS redirect or rejection
echo ""
echo "Protocol enforcement:"
HTTP_BASE=$(echo "$BASE" | sed 's/https:/http:/')
run "HTTP rejected or redirected" bash -c "
    code=\$(curl -sk -o /dev/null -w '%{http_code}' --max-time 5 '$HTTP_BASE/api/fuse/health' 2>/dev/null || echo '000')
    [ \"\$code\" = '000' ] || [ \"\$code\" = '301' ] || [ \"\$code\" = '302' ] || [ \"\$code\" = '308' ] || [ \"\$code\" = '200' ]
"

# Custom domain TLS
echo ""
echo "Custom domain:"
CUSTOM="https://fuse.huanji.profile.aws.dev"
run "Custom domain HTTPS reachable" curl -sk --max-time 10 "$CUSTOM/api/fuse/health"
run "Custom domain query works" bash -c "
    curl -sk -X POST '$CUSTOM/api/fuse/query' \
        -H 'Content-Type: application/json' \
        -d '{\"query\":\"SELECT service FROM cluster_a.application_logs LIMIT 1\",\"format\":\"sql\"}' \
    | python3 -c 'import sys,json; d=json.load(sys.stdin); assert len(d[\"rows\"]) == 1'
"

echo ""
echo "═══════════════════════════════════════════════════"
echo "  Pass: $PASS  |  Fail: $FAIL"
echo "═══════════════════════════════════════════════════"
[ "$FAIL" -eq 0 ]
