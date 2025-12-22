#!/bin/bash
#
# Growatt API Health Check
# Tests connectivity and authentication to Growatt OpenAPI
#
# Usage: ./growatt_healthcheck.sh -u <username> -t <token>
#

# Configuration
BASE_URL="https://openapi.growatt.com/v1"
HEALTH_ENDPOINT="/plant/user_plant_list"
TIMEOUT=10

# Variables
USERNAME=""
TOKEN=""

# Functions
usage() {
    echo "Usage: $0 -u <username> -t <token>"
    echo ""
    echo "Options:"
    echo "  -u    Username (required)"
    echo "  -t    API Token (required)"
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
    
    echo "{\"vendor\":\"growatt\",\"status\":\"$health_status\",\"health_code\":$health_code,\"message\":\"$message\",\"http_code\":$http_code,\"raw\":$escaped_raw,\"timestamp\":\"$timestamp\"}"
}

# Parse arguments
while getopts "u:t:h" opt; do
    case $opt in
        u) USERNAME="$OPTARG" ;;
        t) TOKEN="$OPTARG" ;;
        h) usage ;;
        *) usage ;;
    esac
done

# Validate required arguments
if [ -z "$USERNAME" ] || [ -z "$TOKEN" ]; then
    output_json "error" "Missing required arguments: username and token" 0 ""
    exit 1
fi

# Create temp file for response
RESPONSE_FILE=$(mktemp)
trap "rm -f $RESPONSE_FILE" EXIT

# Build health check URL
HEALTH_URL="${BASE_URL}${HEALTH_ENDPOINT}?user_name=${USERNAME}&page=1&perpage=1"

# Make health check request
HTTP_CODE=$(curl -s \
    --max-time "$TIMEOUT" \
    -X POST \
    -H "Content-Type: application/json" \
    -H "Token: $TOKEN" \
    -o "$RESPONSE_FILE" \
    -w "%{http_code}" \
    "$HEALTH_URL" 2>/dev/null) || {
    output_json "error" "Failed to connect to Growatt API" 0 ""
    exit 1
}

RESPONSE_BODY=$(cat "$RESPONSE_FILE")

# Check response
if [ "$HTTP_CODE" = "200" ]; then
    if echo "$RESPONSE_BODY" | grep -q '"error_code":0'; then
        output_json "healthy" "API is responding correctly" "$HTTP_CODE" "$RESPONSE_BODY"
        exit 0
    elif echo "$RESPONSE_BODY" | grep -q '"error_code"'; then
        ERROR_CODE=$(echo "$RESPONSE_BODY" | grep -o '"error_code":[0-9]*' | sed 's/"error_code"://')
        ERROR_MSG=$(echo "$RESPONSE_BODY" | grep -o '"error_msg":"[^"]*"' | sed 's/"error_msg":"\([^"]*\)"/\1/' || echo "API error")
        output_json "error" "API returned error code $ERROR_CODE: $ERROR_MSG" "$HTTP_CODE" "$RESPONSE_BODY"
        exit 1
    elif echo "$RESPONSE_BODY" | grep -q '"data"'; then
        output_json "healthy" "API is responding correctly" "$HTTP_CODE" "$RESPONSE_BODY"
        exit 0
    else
        output_json "unhealthy" "API returned unexpected response" "$HTTP_CODE" "$RESPONSE_BODY"
        exit 1
    fi
else
    output_json "unhealthy" "API health check failed" "$HTTP_CODE" "$RESPONSE_BODY"
    exit 1
fi
