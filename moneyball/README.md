# RUNNERPRO + MoneyBall Dashboard

Plataforma unificada de contenido y analytics. Desplegada en **Google Cloud Run**.

**URL produccion:** `https://moneyball-dashboard-324233202358.europe-west1.run.app/`

## Estructura

```
/                          -> runnerpro.html (app principal)
/dashboard                 -> moneyball-dashboard.html (Base de datos: videos, escenas, analytics)
/operational-dashboards    -> operational-dashboards.html (Dashboards operacionales)
/vertical-sankey/*         -> vertical-sankey/ (Funnel marketing)
/api/*                     -> API REST (creators, videos, scenes, pipeline, cadena...)
```

### Sidebar RUNNERPRO
- **Vistas:** Pipeline (Kanban), Funnel, Bases de datos, Calendario
- **Operacional:** Dashboards (Adquisicion & Trial, Retencion & Churn, Flujo Mensual, Base Clientes, Revenue, Trial)

### Scripts Cadena de Montaje
- `crea_idea.js` — Genera ideas de contenido (lee de Notion + Gemini)
- `crea_guion_ceo_media.js` — Genera guiones (Gemini)
- `crea_visual.js` — Genera storyboards visuales (Claude Opus + Gemini + Google Docs)

## GCP

| Recurso | Valor |
|---------|-------|
| Proyecto | `glass-timing-393523` |
| Region | `europe-west1` |
| Cloud Run service | `moneyball-dashboard` |
| Cloud SQL instance | `moneyball-db` (PostgreSQL 15) |
| Cloud SQL IP publica | `34.140.147.114` |
| Cloud SQL connection | `glass-timing-393523:europe-west1:moneyball-db` |
| Cloud Storage bucket | `moneyball-media-glass-timing-393523` |
| Container Registry | `gcr.io/glass-timing-393523/moneyball-dashboard` |

### Base de datos
- **DB:** `moneyball_rrss`
- **User:** `moneyball`
- **Pass:** `<ver .env.gcp>`
- **Tablas:** `contenido`, `escenas`, `meta_ads`

## Deploy desde cero (otro PC)

### 1. Prerequisitos
```bash
# Instalar gcloud CLI + autenticarse
gcloud auth login
gcloud config set project glass-timing-393523

# Instalar GitHub CLI
gh auth login
```

### 2. Clonar y configurar
```bash
gh repo clone cristobalrunni-cell/runnerpro-moneyball
cd runnerpro-moneyball

# Crear .env para desarrollo local
cp .env.gcp .env
# Editar .env con tus credenciales reales

npm install
```

### 3. Autorizar IP publica (si cambia de red)
```bash
# Obtener tu IP publica
curl ifconfig.me

# Autorizar en Cloud SQL
gcloud sql instances patch moneyball-db \
  --authorized-networks=TU_IP/32 \
  --project=glass-timing-393523 --quiet
```

### 4. Probar local
```bash
node server.js
# -> http://localhost:8080/
```

### 5. Build y deploy a Cloud Run
```bash
# Build imagen Docker
gcloud builds submit --tag gcr.io/glass-timing-393523/moneyball-dashboard \
  --project=glass-timing-393523

# Deploy
gcloud run deploy moneyball-dashboard \
  --image gcr.io/glass-timing-393523/moneyball-dashboard \
  --platform managed \
  --region europe-west1 \
  --project glass-timing-393523 \
  --allow-unauthenticated \
  --add-cloudsql-instances glass-timing-393523:europe-west1:moneyball-db \
  --set-env-vars "\
DATABASE_URL=<TU_DATABASE_URL>,\
GCS_BUCKET=<TU_GCS_BUCKET>,\
GEMINI_API_KEY=<TU_GEMINI_API_KEY>,\
NOTION_API_KEY=<TU_NOTION_API_KEY>,\
GOOGLE_SERVICE_ACCOUNT_PATH=/app/service_account.json,\
ANTHROPIC_AUTH_TOKEN=<TU_ANTHROPIC_AUTH_TOKEN>,\
ESTILOS_DIR=/app/estilos" \
  --memory 1Gi \
  --port 8080 \
  --quiet
```

## Notas tecnicas

### Express 5
Usa Express 5 — las wildcards requieren nombre: `/frames/{*path}` (no `/frames/*`), y se acceden con `req.params.path`.

### Cloud SQL desde local
Conexion directa via IP publica `34.140.147.114`. No necesita Cloud SQL Proxy. Solo hay que autorizar la IP del PC en las authorized networks (paso 3).

### Cloud Run DATABASE_URL
En Cloud Run se usa Unix socket: `?host=/cloudsql/glass-timing-393523:europe-west1:moneyball-db`. El `@localhost` es ignorado porque el socket tiene prioridad.

### crea_visual.js en produccion
Lee credenciales de env vars (no de archivos locales):
- `NOTION_API_KEY` — API key de Notion
- `GEMINI_API_KEY` — API key de Gemini
- `ANTHROPIC_AUTH_TOKEN` — Token OAuth de Claude (OpenClaw)
- `GOOGLE_SERVICE_ACCOUNT_PATH` — Ruta al service account JSON (copiado en Docker)
- `ESTILOS_DIR` — Directorio con ESTILO_*.md (copiado en Docker)
- `DATABASE_URL` — Si existe, usa PostgreSQL en vez de SQLite
