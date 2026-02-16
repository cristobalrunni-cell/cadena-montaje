#!/bin/bash
# ═══════════════════════════════════════════════════════════════
# MoneyBall — Deploy to Google Cloud Platform
# ═══════════════════════════════════════════════════════════════
#
# Pre-requisitos:
#   1. gcloud CLI instalado y autenticado: gcloud auth login
#   2. Proyecto seleccionado: gcloud config set project glass-timing-393523
#   3. APIs habilitadas (se habilitan abajo si no lo están)
#
# Uso: bash deploy_gcp.sh
#
# ═══════════════════════════════════════════════════════════════

set -e  # Exit on error

PROJECT_ID="glass-timing-393523"
REGION="europe-west1"
DB_INSTANCE="moneyball-db"
DB_NAME="moneyball_rrss"
DB_USER="moneyball"
DB_PASS="$(openssl rand -base64 20 | tr -dc 'a-zA-Z0-9' | head -c 20)"
BUCKET="moneyball-media-${PROJECT_ID}"
SERVICE_NAME="moneyball"
JWT_SECRET="$(openssl rand -hex 32)"
AUTH_USER="admin"
AUTH_PASS="moneyball2024"

echo "═══════════════════════════════════════════════════════════"
echo "  MoneyBall GCP Deployment"
echo "═══════════════════════════════════════════════════════════"
echo "  Project:  $PROJECT_ID"
echo "  Region:   $REGION"
echo "  DB:       $DB_INSTANCE / $DB_NAME"
echo "  Bucket:   $BUCKET"
echo "  Service:  $SERVICE_NAME"
echo "═══════════════════════════════════════════════════════════"
echo ""

# ─── Step 0: Set project ───
echo ">>> Step 0: Setting project..."
gcloud config set project $PROJECT_ID

# ─── Step 1: Enable APIs ───
echo ">>> Step 1: Enabling APIs..."
gcloud services enable \
  sqladmin.googleapis.com \
  run.googleapis.com \
  cloudbuild.googleapis.com \
  storage.googleapis.com \
  containerregistry.googleapis.com \
  artifactregistry.googleapis.com

# ─── Step 2: Create Cloud SQL PostgreSQL instance ───
echo ""
echo ">>> Step 2: Creating Cloud SQL instance (this takes ~5 min)..."
echo "    Instance: $DB_INSTANCE (PostgreSQL 15, db-f1-micro)"

# Check if instance already exists
if gcloud sql instances describe $DB_INSTANCE --project=$PROJECT_ID 2>/dev/null; then
  echo "    Instance already exists, skipping creation."
else
  gcloud sql instances create $DB_INSTANCE \
    --database-version=POSTGRES_15 \
    --tier=db-f1-micro \
    --region=$REGION \
    --storage-type=SSD \
    --storage-size=10GB \
    --project=$PROJECT_ID
fi

# Create database
echo "    Creating database: $DB_NAME"
gcloud sql databases create $DB_NAME --instance=$DB_INSTANCE 2>/dev/null || echo "    Database already exists."

# Create user
echo "    Creating user: $DB_USER (password saved to .env.gcp)"
gcloud sql users create $DB_USER --instance=$DB_INSTANCE --password="$DB_PASS" 2>/dev/null || echo "    User already exists."

# Get connection name
CONNECTION_NAME=$(gcloud sql instances describe $DB_INSTANCE --format='value(connectionName)')
echo "    Connection name: $CONNECTION_NAME"

# Build DATABASE_URL for Cloud Run (uses Unix socket via Cloud SQL Proxy)
DATABASE_URL="postgresql://${DB_USER}:${DB_PASS}@localhost/${DB_NAME}?host=/cloudsql/${CONNECTION_NAME}"

# ─── Step 3: Create Cloud Storage bucket ───
echo ""
echo ">>> Step 3: Creating Cloud Storage bucket..."
gsutil mb -l $REGION gs://$BUCKET 2>/dev/null || echo "    Bucket already exists."

# Make bucket publicly readable (for frames/screenshots)
echo "    Setting public access..."
gsutil iam ch allUsers:objectViewer gs://$BUCKET

# ─── Step 4: Upload frames and screenshots to Cloud Storage ───
echo ""
echo ">>> Step 4: Uploading media to Cloud Storage..."
FRAMES_DIR="$HOME/.openclaw/workspace/escenas_frames"
SCREENSHOTS_DIR="$HOME/.openclaw/workspace/moneyball/meta_ads_screenshots"

if [ -d "$FRAMES_DIR" ]; then
  echo "    Uploading frames (~11GB, this may take a while)..."
  gsutil -m cp -r "$FRAMES_DIR"/* gs://$BUCKET/frames/ 2>/dev/null || true
  echo "    Frames uploaded."
else
  echo "    WARNING: Frames directory not found at $FRAMES_DIR"
fi

if [ -d "$SCREENSHOTS_DIR" ]; then
  echo "    Uploading meta screenshots..."
  gsutil -m cp -r "$SCREENSHOTS_DIR"/* gs://$BUCKET/meta-screenshots/ 2>/dev/null || true
  echo "    Screenshots uploaded."
else
  echo "    WARNING: Screenshots directory not found at $SCREENSHOTS_DIR"
fi

# ─── Step 5: Save config ───
echo ""
echo ">>> Step 5: Saving configuration to .env.gcp..."
cat > .env.gcp << EOF
# MoneyBall GCP Configuration
# Generated: $(date -u +"%Y-%m-%dT%H:%M:%SZ")

PROJECT_ID=$PROJECT_ID
REGION=$REGION
CONNECTION_NAME=$CONNECTION_NAME

# Database
DATABASE_URL=$DATABASE_URL
DB_INSTANCE=$DB_INSTANCE
DB_NAME=$DB_NAME
DB_USER=$DB_USER
DB_PASS=$DB_PASS

# Cloud Storage
GCS_BUCKET=$BUCKET

# Auth
JWT_SECRET=$JWT_SECRET
AUTH_USER=$AUTH_USER
AUTH_PASS=$AUTH_PASS

# Gemini (add your key here)
GEMINI_API_KEY=<YOUR_GEMINI_API_KEY>
EOF

echo "    Saved to .env.gcp"
echo ""
echo "═══════════════════════════════════════════════════════════"
echo "  NEXT STEPS (run manually):"
echo "═══════════════════════════════════════════════════════════"
echo ""
echo "  1. MIGRATE DATA — Run Cloud SQL Proxy + migration script:"
echo "     # Terminal 1: Start Cloud SQL Proxy"
echo "     cloud-sql-proxy $CONNECTION_NAME --port 5432"
echo ""
echo "     # Terminal 2: Run migration"
echo "     DATABASE_URL=postgresql://$DB_USER:$DB_PASS@localhost:5432/$DB_NAME node migrate_to_postgres.js"
echo ""
echo "  2. ADD GEMINI KEY — Edit .env.gcp and set GEMINI_API_KEY"
echo ""
echo "  3. BUILD & DEPLOY:"
echo "     gcloud builds submit --tag gcr.io/$PROJECT_ID/$SERVICE_NAME"
echo ""
echo "     gcloud run deploy $SERVICE_NAME \\"
echo "       --image gcr.io/$PROJECT_ID/$SERVICE_NAME \\"
echo "       --region $REGION \\"
echo "       --memory 2Gi --cpu 2 --timeout 600 \\"
echo "       --set-env-vars \"DATABASE_URL=$DATABASE_URL\" \\"
echo "       --set-env-vars \"GEMINI_API_KEY=<YOUR_KEY>\" \\"
echo "       --set-env-vars \"GCS_BUCKET=$BUCKET\" \\"
echo "       --set-env-vars \"JWT_SECRET=$JWT_SECRET\" \\"
echo "       --set-env-vars \"AUTH_USER=$AUTH_USER\" \\"
echo "       --set-env-vars \"AUTH_PASS=$AUTH_PASS\" \\"
echo "       --add-cloudsql-instances $CONNECTION_NAME \\"
echo "       --allow-unauthenticated"
echo ""
echo "  4. ACCESS: The URL will be shown after deploy completes."
echo "     Login with: user=$AUTH_USER pass=$AUTH_PASS"
echo ""
echo "═══════════════════════════════════════════════════════════"
echo "  DB Password: $DB_PASS"
echo "  JWT Secret:  $JWT_SECRET"
echo "  (Also saved in .env.gcp)"
echo "═══════════════════════════════════════════════════════════"
