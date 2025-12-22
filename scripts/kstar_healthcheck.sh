#!/bin/bash
#
# Kstar API Health Check
# Tests connectivity and authentication to Kstar Solar API
#
# Usage: ./kstar_healthcheck.sh -u <usercode> -p <password>
#

# Configuration
BASE_URL="http://solar.kstar.com:9000/public"
HEALTH_ENDPOINT="/power/info"
TIMEOUT=10

# Variables
USERCODE=""
PASSWORD=""

# Functions
usage() {
    echo "Usage: $0 -u <usercode> -p <password>"
    echo ""
    echo "Options:"
    echo "  -u    User Code (required)"
    echo "  -p    Password - will be MD5 encoded (required)"
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
    
    echo "{\"vendor\":\"kstar\",\"status\":\"$health_status\",\"health_code\":$health_code,\"message\":\"$message\",\"http_code\":$http_code,\"raw\":$escaped_raw,\"timestamp\":\"$timestamp\"}"
}

# MD5 encode password (uppercase)
md5_encode() {
    local input="$1"
    echo -n "$input" | md5 | tr '[:lower:]' '[:upper:]'
}

# Calculate SHA1 signature
calculate_sign() {
    local usercode="$1"
    local password="$2"
    local query_string="password=${password}&userCode=${usercode}"
    echo -n "$query_string" | openssl dgst -sha1 | awk '{print $2}'
}

# Parse arguments
while getopts "u:p:h" opt; do
    case $opt in
        u) USERCODE="$OPTARG" ;;
        p) PASSWORD="$OPTARG" ;;
        h) usage ;;
        *) usage ;;
    esac
done

# Validate required arguments
if [ -z "$USERCODE" ] || [ -z "$PASSWORD" ]; then
    output_json "error" "Missing required arguments: usercode and password" 0 ""
    exit 1
fi

# Create temp file for response
RESPONSE_FILE=$(mktemp)
trap "rm -f $RESPONSE_FILE" EXIT

# Encode password with MD5 (uppercase)
ENCODED_PASSWORD=$(md5_encode "$PASSWORD")

# Calculate signature
SIGN=$(calculate_sign "$USERCODE" "$ENCODED_PASSWORD")

# Build health check URL
HEALTH_URL="${BASE_URL}${HEALTH_ENDPOINT}?userCode=${USERCODE}&password=${ENCODED_PASSWORD}&sign=${SIGN}"

# Make health check request
HTTP_CODE=$(curl -s \
    --max-time "$TIMEOUT" \
    -X GET \
    -o "$RESPONSE_FILE" \
    -w "%{http_code}" \
    "$HEALTH_URL" 2>/dev/null) || {
    output_json "error" "Failed to connect to Kstar API" 0 ""
    exit 1
}

RESPONSE_BODY=$(cat "$RESPONSE_FILE")

# Check response
if [ "$HTTP_CODE" = "200" ]; then
    if echo "$RESPONSE_BODY" | grep -q '"success":true'; then
        output_json "healthy" "API is responding correctly" "$HTTP_CODE" "$RESPONSE_BODY"
        exit 0
    elif echo "$RESPONSE_BODY" | grep -q '"success":false'; then
        ERROR_MSG=$(echo "$RESPONSE_BODY" | grep -o '"code":"[^"]*"' | sed 's/"code":"\([^"]*\)"/\1/' || echo "Authentication failed")
        output_json "error" "API returned error: $ERROR_MSG" "$HTTP_CODE" "$RESPONSE_BODY"
        exit 1
    else
        if echo "$RESPONSE_BODY" | grep -q '"data"'; then
            output_json "healthy" "API is responding correctly" "$HTTP_CODE" "$RESPONSE_BODY"
            exit 0
        fi
        output_json "unhealthy" "API returned unexpected response" "$HTTP_CODE" "$RESPONSE_BODY"
        exit 1
    fi
else
    output_json "unhealthy" "API health check failed" "$HTTP_CODE" "$RESPONSE_BODY"
    exit 1
fi
