#!/bin/bash
TUNNEL_URL_FILE="/root/polymarket/.tunnel_url"
PORT=3847
pkill -f "cloudflared.*$PORT" 2>/dev/null
sleep 1
cloudflared tunnel --url http://localhost:$PORT --no-autoupdate 2>&1 | while read line; do
    if echo "$line" | grep -qE "https://.*trycloudflare\.com"; then
        url=$(echo "$line" | grep -oE "https://[a-z0-9-]+\.trycloudflare\.com")
        echo "$url" > "$TUNNEL_URL_FILE"
        echo "[tunnel] URL captured: $url"
    fi
    echo "[cf] $line"
done
