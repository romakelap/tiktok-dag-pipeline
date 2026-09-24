#!/bin/bash

# ============================================
# Init Airflow Variables
# ============================================
# Pre-set Discord webhook URL.
# Token Echotik tetap perlu di-paste manual di Airflow UI atau via script

set -e

DISCORD_WEBHOOK="https://discord.com/api/webhooks/1504847346339549315/pPd5AnmTjQLc5xA-0fMFCOoIIC7I-IgdP7c352Ud-4W7MoX5w0OK8Rsgw7eiR0qLhbHI"

echo "🔧 Setting Airflow Variables..."

# Set Discord webhook
docker compose exec airflow-webserver airflow variables set DISCORD_WEBHOOK_URL "$DISCORD_WEBHOOK"
echo "✅ DISCORD_WEBHOOK_URL set"

# Set placeholder untuk session token (user harus replace via UI)
docker compose exec airflow-webserver airflow variables set ECHOTIK_SESSION_TOKEN "PASTE_YOUR_TOKEN_HERE"
echo "⚠️  ECHOTIK_SESSION_TOKEN: placeholder set. PERLU update via UI dengan token asli!"

echo ""
echo "=================================="
echo "✅ Variables set!"
echo "=================================="
echo ""
echo "NEXT: Go to http://localhost:8080 → Admin → Variables"
echo "      Update ECHOTIK_SESSION_TOKEN dengan token Nico dari browser"
echo ""
