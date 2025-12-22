#!/bin/bash
#
# Huawei FusionSolar API Health Check
# Tests connectivity and authentication to Huawei FusionSolar API
#
# Usage: ./huawei_healthcheck.sh -u <username> -p <password>
#

# Configuration
BASE_URL="https://sg5.fusionsolar.huawei.com"
LOGIN_ENDPOINT="/thirdData/login"
HEALTH_ENDPOINT="/thirdData/getStationList"
TIMEOUT=10

# Variables
USERNAME=""
PASSWORD=""

# Functions
usage() {
    echo "Usage: $0 -u <username> -p <password>"
    echo ""
    echo "Options:"
    echo "  -u    Username (required)"
    echo "  -p    Password/SystemCode (required)"
    echo "  -h    Show this help message"
    exit 1
}

# Escape string for JSON
json_escape() {
    local string="$1"
    echo -n "$string" | python3 -c 'import json,sys; print(json.dumps(sys.stdin.read()))'
}

output_json() {
    local health_status="$1"
    local message="$2"
    local http_code="$3"
    local raw_response="$4"
    local timestamp=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
    
    # Determine health_code: 1 = healthy, 0 = unhealthy
    local health_code=0
    if [ "$health_status" = "healthy" ]; then
        health_code=1
    fi
    
    # Escape raw response for JSON
    local escaped_raw
    escaped_raw=$(json_escape "$raw_response")
    
    echo "{\"vendor\":\"huawei\",\"status\":\"$health_status\",\"health_code\":$health_code,\"message\":\"$message\",\"http_code\":$http_code,\"raw\":$escaped_raw,\"timestamp\":\"$timestamp\"}"
}

# Parse arguments
while getopts "u:p:h" opt; do
    case $opt in
        u) USERNAME="$OPTARG" ;;
        p) PASSWORD="$OPTARG" ;;
        h) usage ;;
        *) usage ;;
    esac
done

# Validate required arguments
if [ -z "$USERNAME" ] || [ -z "$PASSWORD" ]; then
    output_json "error" "Missing required arguments: username and password" 0 ""
    exit 1
fi

# Create temp files
RESPONSE_FILE=$(mktemp)
HEADER_FILE=$(mktemp)
trap "rm -f $RESPONSE_FILE $HEADER_FILE" EXIT

# Step 1: Login and get XSRF-TOKEN
LOGIN_URL="${BASE_URL}${LOGIN_ENDPOINT}"
LOGIN_BODY=$(cat <<EOF
{
    "userName": "$USERNAME",
    "systemCode": "$PASSWORD"
}
EOF
)

HTTP_CODE=$(curl -s \
    --max-time "$TIMEOUT" \
    -X POST \
    -H "Content-Type: application/json" \
    -D "$HEADER_FILE" \
    -d "$LOGIN_BODY" \
    -o "$RESPONSE_FILE" \
    -w "%{http_code}" \
    "$LOGIN_URL" 2>/dev/null) || {
    output_json "error" "Failed to connect to Huawei API" 0 ""
    exit 1
}

RESPONSE_BODY=$(cat "$RESPONSE_FILE")

# Check if login was successful
if [ "$HTTP_CODE" != "200" ]; then
    output_json "error" "Login failed" "$HTTP_CODE" "$RESPONSE_BODY"
    exit 1
fi

# Extract XSRF-TOKEN from response headers
XSRF_TOKEN=$(grep -i "set-cookie.*XSRF-TOKEN" "$HEADER_FILE" 2>/dev/null | sed 's/.*XSRF-TOKEN=\([^;]*\).*/\1/' | head -1 | tr -d '\r' || true)

if [ -z "$XSRF_TOKEN" ]; then
    # Check if response contains success=false
    if echo "$RESPONSE_BODY" | grep -q '"success":false'; then
        output_json "error" "Authentication failed - invalid credentials" "$HTTP_CODE" "$RESPONSE_BODY"
        exit 1
    fi
    output_json "error" "Failed to extract XSRF-TOKEN from response" "$HTTP_CODE" "$RESPONSE_BODY"
    exit 1
fi

# Step 2: Test health endpoint with token
HEALTH_URL="${BASE_URL}${HEALTH_ENDPOINT}"

HEALTH_HTTP_CODE=$(curl -s \
    --max-time "$TIMEOUT" \
    -X POST \
    -H "Content-Type: application/json" \
    -H "XSRF-TOKEN: $XSRF_TOKEN" \
    -o "$RESPONSE_FILE" \
    -w "%{http_code}" \
    "$HEALTH_URL" 2>/dev/null) || {
    output_json "error" "Health check request failed" 0 ""
    exit 1
}

HEALTH_BODY=$(cat "$RESPONSE_FILE")

# Check health response
if [ "$HEALTH_HTTP_CODE" = "200" ]; then
    if echo "$HEALTH_BODY" | grep -q '"success":true'; then
        output_json "healthy" "API is responding correctly" "$HEALTH_HTTP_CODE" "$HEALTH_BODY"
        exit 0
    else
        output_json "unhealthy" "API returned unsuccessful response" "$HEALTH_HTTP_CODE" "$HEALTH_BODY"
        exit 1
    fi
else
    output_json "unhealthy" "API health check failed" "$HEALTH_HTTP_CODE" "$HEALTH_BODY"
    exit 1
fi
