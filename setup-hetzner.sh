#!/bin/bash
# ============================================================
# setup-hetzner.sh — Deploy Echotik Pipeline ke Hetzner VM
# Jalankan sekali saat pertama kali setup di VM
# ============================================================

set -e

PROJECT_DIR="/home/ubuntu/echotik/dag-collection-tt"
RAW_DATA_DIR="/home/ubuntu/echotik/raw-data"

echo ""
echo "╔══════════════════════════════════════════╗"
echo "║   Echotik Pipeline — Hetzner Setup       ║"
echo "╚══════════════════════════════════════════╝"
echo ""

# ─── 1. Cek .env ────────────────────────────────────────────
if [ ! -f "$PROJECT_DIR/.env" ]; then
  echo "❌ File .env tidak ditemukan!"
  echo "   Buat dulu: cp .env.hetzner.example .env && nano .env"
  exit 1
fi

# Cek FERNET_KEY tidak kosong
source "$PROJECT_DIR/.env"
if [ -z "$FERNET_KEY" ] || [ "$FERNET_KEY" = "<hasil_generate_fernet_key>" ]; then
  echo "❌ FERNET_KEY belum diisi di .env"
  echo "   Generate: python3 -c \"from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())\""
  exit 1
fi
if [ -z "$AIRFLOW_ADMIN_PASSWORD" ] || [ "$AIRFLOW_ADMIN_PASSWORD" = "<password_airflow_ui_kamu>" ]; then
  echo "❌ AIRFLOW_ADMIN_PASSWORD belum diisi di .env"
  exit 1
fi

echo "✅ .env OK"

# ─── 2. Buat direktori raw-data ──────────────────────────────
mkdir -p "$RAW_DATA_DIR"/{json,Excel,Excel/processed,Excel/failed}
echo "✅ Direktori raw-data dibuat: $RAW_DATA_DIR"

# ─── 3. Set permission ──────────────────────────────────────
sudo chown -R 50000:0 "$RAW_DATA_DIR" 2>/dev/null || \
  chown -R ubuntu:ubuntu "$RAW_DATA_DIR"
echo "✅ Permission raw-data OK"

# ─── 4. Init Airflow ─────────────────────────────────────────
echo ""
echo "📦 Menjalankan airflow-init (bisa 2-3 menit)..."
cd "$PROJECT_DIR"
docker compose -f docker-compose.hetzner.yaml up airflow-init
echo "✅ Airflow init selesai"

# ─── 5. Start semua service ──────────────────────────────────
echo ""
echo "🚀 Menjalankan semua service..."
docker compose -f docker-compose.hetzner.yaml up -d
echo ""
echo "⏳ Menunggu services healthy (60 detik)..."
sleep 60

# ─── 6. Cek status ──────────────────────────────────────────
echo ""
echo "📊 Status services:"
docker compose -f docker-compose.hetzner.yaml ps

# ─── 7. Set Airflow Variables ────────────────────────────────
echo ""
echo "🔧 Setting Airflow Variables..."

# DB Connection String (pakai MySQL container)
MYSQL_CONN="mysql+pymysql://echotik:${MYSQL_PASSWORD}@mysql:3306/tiktok_oltp"
docker compose -f docker-compose.hetzner.yaml exec -T airflow-apiserver \
  airflow variables set DB_CONNECTION_STRING "$MYSQL_CONN"
echo "✅ DB_CONNECTION_STRING set"

# Placeholder token (harus diupdate manual via UI)
docker compose -f docker-compose.hetzner.yaml exec -T airflow-apiserver \
  airflow variables set ECHOTIK_BEARER_TOKEN "PASTE_YOUR_TOKEN_HERE"
echo "⚠️  ECHOTIK_BEARER_TOKEN: PERLU diupdate manual via Airflow UI"

# Discord (opsional — skip jika tidak ada)
if [ ! -z "$DISCORD_WEBHOOK_URL" ]; then
  docker compose -f docker-compose.hetzner.yaml exec -T airflow-apiserver \
    airflow variables set DISCORD_WEBHOOK_URL "$DISCORD_WEBHOOK_URL"
  echo "✅ DISCORD_WEBHOOK_URL set"
fi

# ─── Selesai ─────────────────────────────────────────────────
echo ""
echo "╔══════════════════════════════════════════╗"
echo "║   ✅ Setup Selesai!                       ║"
echo "╚══════════════════════════════════════════╝"
echo ""
echo "📋 NEXT STEPS:"
echo ""
echo "1. Buka Airflow UI:"
echo "   http://$(curl -s ifconfig.me):8085"
echo "   Username: $AIRFLOW_ADMIN_USER"
echo "   Password: (sesuai .env)"
echo ""
echo "2. Update Airflow Variables:"
echo "   Admin → Variables → ECHOTIK_BEARER_TOKEN"
echo "   → paste token dari browser kamu"
echo ""
echo "3. Enable DAG 'echotik_data_collection'"
echo "   → klik toggle ON di Airflow UI"
echo ""
echo "4. (Opsional) Setup Cloudflare Tunnel:"
echo "   bash setup-tunnel.sh"
echo ""
