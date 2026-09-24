#!/bin/bash

# ============================================
# Quick Setup Script — Echotik Airflow Pipeline
# ============================================

set -e

echo "🚀 Echotik Airflow Pipeline Setup"
echo "=================================="
echo ""

# Check Docker
if ! command -v docker &> /dev/null; then
    echo "❌ Docker not found. Please install Docker Desktop first."
    exit 1
fi

echo "✅ Docker found"

# Check if .env exists
if [ ! -f .env ]; then
    echo "Creating .env file..."
    cat > .env << 'EOF'
AIRFLOW_UID=50000
_AIRFLOW_WWW_USER_USERNAME=airflow
_AIRFLOW_WWW_USER_PASSWORD=airflow
EOF
    echo "✅ .env created"
fi

# Create required directories
mkdir -p logs raw-data/json raw-data/Excel
echo "✅ Directories created"

# Initialize Airflow
echo ""
echo "📦 Initializing Airflow (this may take 1-2 minutes)..."
docker compose up airflow-init

echo ""
echo "🚀 Starting Airflow services..."
docker compose up -d

echo ""
echo "⏳ Waiting for services to be healthy (30s)..."
sleep 30

# Check status
echo ""
echo "📊 Service Status:"
docker compose ps

echo ""
echo "=================================="
echo "✅ Setup complete!"
echo "=================================="
echo ""
echo "📋 NEXT STEPS:"
echo ""
echo "1. Open Airflow UI: http://localhost:8080"
echo "   Username: airflow"
echo "   Password: airflow"
echo ""
echo "2. Set Airflow Variables (Admin → Variables):"
echo "   - ECHOTIK_SESSION_TOKEN: <paste from browser>"
echo "   - DISCORD_WEBHOOK_URL: <your discord webhook>"
echo ""
echo "3. Enable DAG 'echotik_data_collection'"
echo ""
echo "4. Click ▶️ to trigger first run"
echo ""
echo "=================================="
