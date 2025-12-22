#!/bin/bash
#
# Solarman API Health Check
# Tests connectivity and authentication to Solarman API
#
# Usage: ./solarman_healthcheck.sh -u <username> -p <password> -i <app_id> -s <app_secret>
#

# Configuration
BASE_URL="https://globalapi.solarmanpv.com"
TOKEN_ENDPOINT="/account/v1.0/token"
HEALTH_ENDPOINT="/account/v1.0/info"
TIMEOUT=10

# Variables
USERNAME=""
PASSWORD=""
APP_ID=""
APP_SECRET=""

# Functions
usage() {
    echo "Usage: $0 -u <username> -p <password> -i <app_id> -s <app_secret>"
    echo ""
    echo "Options:"
    echo "  -u    Username/Email (required)"
    echo "  -p    Password - will be SHA256 encoded (required)"
    echo "  -i    App ID (required)"
    echo "  -s    App Secret (required)"
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
    
    echo "{\"vendor\":\"solarman\",\"status\":\"$health_status\",\"health_code\":$health_code,\"message\":\"$message\",\"http_code\":$http_code,\"raw\":$escaped_raw,\"timestamp\":\"$timestamp\"}"
}

# SHA256 encode password (lowercase hex output, matching Go implementation)
sha256_encode() {
    local input="$1"
    echo -n "$input" | openssl dgst -sha256 | awk '{print $2}'
}

# Parse arguments
while getopts "u:p:i:s:h" opt; do
    case $opt in
        u) USERNAME="$OPTARG" ;;
        p) PASSWORD="$OPTARG" ;;
        i) APP_ID="$OPTARG" ;;
        s) APP_SECRET="$OPTARG" ;;
        h) usage ;;
        *) usage ;;
    esac
done

# Validate required arguments
if [ -z "$USERNAME" ] || [ -z "$PASSWORD" ] || [ -z "$APP_ID" ] || [ -z "$APP_SECRET" ]; then
    output_json "error" "Missing required arguments: username, password, app_id, and app_secret are required" 0 ""
    exit 1
fi

# Encode password with SHA256 (same as Go: fmt.Sprintf("%x", hashPassword[:]))
ENCODED_PASSWORD=$(sha256_encode "$PASSWORD")

# Step 1: Get access token
TOKEN_URL="${BASE_URL}${TOKEN_ENDPOINT}?appId=${APP_ID}"

# Build request body matching Go implementation:
# Uses "username" field (not "email") as per client.go GetBasicToken()
TOKEN_BODY=$(cat <<EOF
{
    "appSecret": "$APP_SECRET",
    "username": "$USERNAME",
    "password": "$ENCODED_PASSWORD"
}
EOF
)

# Create temp file for response
RESPONSE_FILE=$(mktemp)
trap "rm -f $RESPONSE_FILE" EXIT

HTTP_CODE=$(curl -s \
    --max-time "$TIMEOUT" \
    -X POST \
    -H "Content-Type: application/json" \
    -d "$TOKEN_BODY" \
    -o "$RESPONSE_FILE" \
    -w "%{http_code}" \
    "$TOKEN_URL" 2>/dev/null) || {
    output_json "error" "Failed to connect to Solarman API" 0 ""
    exit 1
}

RESPONSE_BODY=$(cat "$RESPONSE_FILE")

# Check if token request was successful
if [ "$HTTP_CODE" != "200" ]; then
    output_json "error" "Token request failed" "$HTTP_CODE" "$RESPONSE_BODY"
    exit 1
fi

# Extract access token from response
ACCESS_TOKEN=$(echo "$RESPONSE_BODY" | grep -o '"access_token":"[^"]*"' | sed 's/"access_token":"\([^"]*\)"/\1/' || true)

if [ -z "$ACCESS_TOKEN" ]; then
    if echo "$RESPONSE_BODY" | grep -q '"success":false'; then
        ERROR_MSG=$(echo "$RESPONSE_BODY" | grep -o '"msg":"[^"]*"' | sed 's/"msg":"\([^"]*\)"/\1/' || echo "Authentication failed")
        output_json "error" "$ERROR_MSG" "$HTTP_CODE" "$RESPONSE_BODY"
        exit 1
    fi
    output_json "error" "Failed to extract access token from response" "$HTTP_CODE" "$RESPONSE_BODY"
    exit 1
fi

# Step 2: Test health endpoint with token
HEALTH_URL="${BASE_URL}${HEALTH_ENDPOINT}?language=en"

HEALTH_HTTP_CODE=$(curl -s \
    --max-time "$TIMEOUT" \
    -X POST \
    -H "Content-Type: application/json" \
    -H "Authorization: Bearer $ACCESS_TOKEN" \
    -o "$RESPONSE_FILE" \
    -w "%{http_code}" \
    "$HEALTH_URL" 2>/dev/null) || {
    output_json "error" "Health check request failed" 0 ""
    exit 1
}

HEALTH_BODY=$(cat "$RESPONSE_FILE")

# Check health response
if [ "$HEALTH_HTTP_CODE" = "200" ]; then
    if echo "$HEALTH_BODY" | grep -q '"orgInfoList"'; then
        output_json "healthy" "API is responding correctly" "$HEALTH_HTTP_CODE" "$HEALTH_BODY"
        exit 0
    elif echo "$HEALTH_BODY" | grep -q '"success":true'; then
        output_json "healthy" "API is responding correctly" "$HEALTH_HTTP_CODE" "$HEALTH_BODY"
        exit 0
    else
        output_json "unhealthy" "API returned unexpected response" "$HEALTH_HTTP_CODE" "$HEALTH_BODY"
        exit 1
    fi
else
    output_json "unhealthy" "API health check failed" "$HEALTH_HTTP_CODE" "$HEALTH_BODY"
    exit 1
fi
