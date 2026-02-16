#!/usr/bin/env node
/**
 * migrate_to_postgres.js — Migra moneyball_rrss.db (SQLite) → PostgreSQL (Cloud SQL)
 *
 * Uso:
 *   DATABASE_URL=postgres://user:pass@host/db node migrate_to_postgres.js
 *
 * O con Cloud SQL Proxy:
 *   DATABASE_URL=postgres://moneyball:PASSWORD@localhost:5432/moneyball_rrss node migrate_to_postgres.js
 *
 * Lo que hace:
 *   1. Crea las 3 tablas (contenido, escenas, meta_ads) en PostgreSQL
 *   2. Lee los datos de SQLite
 *   3. Inserta en batch (500 filas)
 *   4. Crea los índices
 *   5. Verifica conteos
 */

const Database = require('better-sqlite3');
const { Pool } = require('pg');
const path = require('path');

const SQLITE_PATH = path.join(process.env.HOME, '.openclaw/workspace/moneyball_rrss.db');
const BATCH_SIZE = 500;

// ═══════════════════════════════════════════════════════════════
// PostgreSQL Schema
// ═══════════════════════════════════════════════════════════════

const CREATE_TABLES = `
-- Drop existing tables (order matters for FK)
DROP TABLE IF EXISTS escenas CASCADE;
DROP TABLE IF EXISTS meta_ads CASCADE;
DROP TABLE IF EXISTS contenido CASCADE;

-- Table: contenido
CREATE TABLE contenido (
    id SERIAL PRIMARY KEY,
    creador TEXT NOT NULL,
    url TEXT UNIQUE,
    visitas INTEGER,
    likes INTEGER,
    comentarios INTEGER,
    duracion TEXT,
    fecha_publicacion TEXT,
    descripcion TEXT,
    transcripcion TEXT,
    visual TEXT,
    hook TEXT,
    tematica TEXT,
    formula_hook TEXT,
    semantica_cluster TEXT,
    semantica_inicio TEXT,
    semantica_ruta TEXT,
    visual_inicio TEXT,
    visual_ruta TEXT,
    visual_cluster TEXT,
    ejecutado TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    escenas_cortes TEXT,
    embedding TEXT,
    referente_para TEXT
);

CREATE INDEX idx_creador ON contenido(creador);
CREATE INDEX idx_url ON contenido(url);
CREATE INDEX idx_fecha ON contenido(fecha_publicacion);

-- Table: escenas
CREATE TABLE escenas (
    id SERIAL PRIMARY KEY,
    video_id INTEGER NOT NULL REFERENCES contenido(id),
    video_url TEXT NOT NULL,
    escena_numero INTEGER NOT NULL,
    tiempo_inicio REAL NOT NULL,
    tiempo_fin REAL NOT NULL,
    duracion_seg REAL NOT NULL,
    fotogramas TEXT,
    escenario TEXT,
    personajes TEXT,
    objetivo_visual TEXT,
    edicion_visual TEXT,
    camara_edicion TEXT,
    descripcion_completa TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    embedding TEXT
);

CREATE INDEX idx_escenas_video ON escenas(video_id);
CREATE INDEX idx_escenas_url ON escenas(video_url);

-- Table: meta_ads
CREATE TABLE meta_ads (
    id SERIAL PRIMARY KEY,
    anunciante TEXT NOT NULL,
    page_id TEXT,
    ad_id TEXT UNIQUE,
    ad_url TEXT,
    estado TEXT,
    fecha_inicio TEXT,
    fecha_fin TEXT,
    plataformas TEXT,
    tipo_media TEXT,
    media_url TEXT,
    texto_principal TEXT,
    titulo TEXT,
    descripcion TEXT,
    cta TEXT,
    transcripcion TEXT,
    visual TEXT,
    hook TEXT,
    tematica TEXT,
    formula_hook TEXT,
    semantica_cluster TEXT,
    semantica_inicio TEXT,
    semantica_ruta TEXT,
    visual_inicio TEXT,
    visual_ruta TEXT,
    visual_cluster TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    edad_min INTEGER,
    edad_max INTEGER,
    genero_audiencia TEXT,
    pais_audiencia TEXT,
    alcance_eu INTEGER,
    url_destino TEXT,
    instagram_handle TEXT,
    nomenclatura_ad TEXT,
    fecha_analisis TEXT,
    embedding TEXT
);

CREATE INDEX idx_meta_ads_anunciante ON meta_ads(anunciante);
CREATE INDEX idx_meta_ads_ad_id ON meta_ads(ad_id);
`;

// ═══════════════════════════════════════════════════════════════
// Helpers
// ═══════════════════════════════════════════════════════════════

function log(msg) {
  console.log(`[${new Date().toISOString().slice(11, 19)}] ${msg}`);
}

/**
 * Insert rows in batches using parameterized queries
 */
async function batchInsert(pool, tableName, columns, rows) {
  if (rows.length === 0) return 0;

  let inserted = 0;
  for (let i = 0; i < rows.length; i += BATCH_SIZE) {
    const batch = rows.slice(i, i + BATCH_SIZE);

    // Build VALUES clause: ($1,$2,$3), ($4,$5,$6), ...
    const placeholders = [];
    const values = [];
    let paramIdx = 1;

    for (const row of batch) {
      const rowPlaceholders = [];
      for (const col of columns) {
        rowPlaceholders.push(`$${paramIdx++}`);
        values.push(row[col] !== undefined ? row[col] : null);
      }
      placeholders.push(`(${rowPlaceholders.join(',')})`);
    }

    const sql = `INSERT INTO ${tableName} (${columns.join(',')}) VALUES ${placeholders.join(',')}`;
    await pool.query(sql, values);

    inserted += batch.length;
    if (rows.length > BATCH_SIZE) {
      log(`  ${tableName}: ${inserted}/${rows.length} (${Math.round(inserted / rows.length * 100)}%)`);
    }
  }

  return inserted;
}

// ═══════════════════════════════════════════════════════════════
// Main
// ═══════════════════════════════════════════════════════════════

async function main() {
  const dbUrl = process.env.DATABASE_URL;
  if (!dbUrl) {
    console.error('ERROR: Set DATABASE_URL env var. Example:');
    console.error('  DATABASE_URL=postgres://moneyball:PASS@localhost:5432/moneyball_rrss node migrate_to_postgres.js');
    process.exit(1);
  }

  log('=== MoneyBall SQLite → PostgreSQL Migration ===');
  log(`SQLite: ${SQLITE_PATH}`);
  log(`PostgreSQL: ${dbUrl.replace(/:[^:@]+@/, ':****@')}`);

  // Open SQLite
  const sqlite = new Database(SQLITE_PATH, { readonly: true, fileMustExist: true });
  log('SQLite opened OK');

  // Connect to PostgreSQL
  const pool = new Pool({ connectionString: dbUrl });
  try {
    await pool.query('SELECT 1');
    log('PostgreSQL connected OK');
  } catch (err) {
    console.error('ERROR connecting to PostgreSQL:', err.message);
    process.exit(1);
  }

  // 1. Create schema
  log('\n--- Step 1: Creating tables ---');
  await pool.query(CREATE_TABLES);
  log('Tables created OK');

  // 2. Read SQLite data
  log('\n--- Step 2: Reading SQLite data ---');

  const contenidoRows = sqlite.prepare('SELECT * FROM contenido ORDER BY id').all();
  log(`contenido: ${contenidoRows.length} rows`);

  const escenasRowsRaw = sqlite.prepare('SELECT * FROM escenas ORDER BY id').all();
  // Filter out orphan scenes (video_id not in contenido)
  const validVideoIds = new Set(contenidoRows.map(r => r.id));
  const escenasRows = escenasRowsRaw.filter(r => validVideoIds.has(r.video_id));
  const orphaned = escenasRowsRaw.length - escenasRows.length;
  log(`escenas: ${escenasRows.length} rows${orphaned > 0 ? ` (${orphaned} orphaned rows skipped)` : ''}`);

  const metaAdsRows = sqlite.prepare('SELECT * FROM meta_ads ORDER BY id').all();
  log(`meta_ads: ${metaAdsRows.length} rows`);

  // 3. Insert into PostgreSQL
  log('\n--- Step 3: Inserting into PostgreSQL ---');

  // contenido — preserve original IDs
  const contenidoCols = [
    'id', 'creador', 'url', 'visitas', 'likes', 'comentarios', 'duracion',
    'fecha_publicacion', 'descripcion', 'transcripcion', 'visual', 'hook',
    'tematica', 'formula_hook', 'semantica_cluster', 'semantica_inicio',
    'semantica_ruta', 'visual_inicio', 'visual_ruta', 'visual_cluster',
    'ejecutado', 'created_at', 'updated_at', 'escenas_cortes', 'embedding',
    'referente_para'
  ];
  const cInserted = await batchInsert(pool, 'contenido', contenidoCols, contenidoRows);
  log(`contenido: ${cInserted} rows inserted`);

  // Reset sequence to max id
  await pool.query(`SELECT setval('contenido_id_seq', (SELECT COALESCE(MAX(id), 0) FROM contenido))`);

  // escenas — preserve original IDs
  const escenasCols = [
    'id', 'video_id', 'video_url', 'escena_numero', 'tiempo_inicio',
    'tiempo_fin', 'duracion_seg', 'fotogramas', 'escenario', 'personajes',
    'objetivo_visual', 'edicion_visual', 'camara_edicion', 'descripcion_completa',
    'created_at', 'embedding'
  ];
  const eInserted = await batchInsert(pool, 'escenas', escenasCols, escenasRows);
  log(`escenas: ${eInserted} rows inserted`);

  await pool.query(`SELECT setval('escenas_id_seq', (SELECT COALESCE(MAX(id), 0) FROM escenas))`);

  // meta_ads — preserve original IDs
  const metaAdsCols = [
    'id', 'anunciante', 'page_id', 'ad_id', 'ad_url', 'estado',
    'fecha_inicio', 'fecha_fin', 'plataformas', 'tipo_media', 'media_url',
    'texto_principal', 'titulo', 'descripcion', 'cta', 'transcripcion',
    'visual', 'hook', 'tematica', 'formula_hook', 'semantica_cluster',
    'semantica_inicio', 'semantica_ruta', 'visual_inicio', 'visual_ruta',
    'visual_cluster', 'created_at', 'updated_at', 'edad_min', 'edad_max',
    'genero_audiencia', 'pais_audiencia', 'alcance_eu', 'url_destino',
    'instagram_handle', 'nomenclatura_ad', 'fecha_analisis', 'embedding'
  ];
  const mInserted = await batchInsert(pool, 'meta_ads', metaAdsCols, metaAdsRows);
  log(`meta_ads: ${mInserted} rows inserted`);

  await pool.query(`SELECT setval('meta_ads_id_seq', (SELECT COALESCE(MAX(id), 0) FROM meta_ads))`);

  // 4. Verify counts
  log('\n--- Step 4: Verification ---');
  const pgContenido = (await pool.query('SELECT COUNT(*) as cnt FROM contenido')).rows[0].cnt;
  const pgEscenas = (await pool.query('SELECT COUNT(*) as cnt FROM escenas')).rows[0].cnt;
  const pgMetaAds = (await pool.query('SELECT COUNT(*) as cnt FROM meta_ads')).rows[0].cnt;

  const pass = (a, b) => parseInt(a) === parseInt(b) ? '  OK' : '  MISMATCH!';

  log(`contenido: SQLite=${contenidoRows.length} | PG=${pgContenido} ${pass(contenidoRows.length, pgContenido)}`);
  log(`escenas:   SQLite=${escenasRows.length} | PG=${pgEscenas} ${pass(escenasRows.length, pgEscenas)}`);
  log(`meta_ads:  SQLite=${metaAdsRows.length} | PG=${pgMetaAds} ${pass(metaAdsRows.length, pgMetaAds)}`);

  const allMatch = parseInt(pgContenido) === contenidoRows.length
    && parseInt(pgEscenas) === escenasRows.length
    && parseInt(pgMetaAds) === metaAdsRows.length;

  if (allMatch) {
    log('\n=== Migration completed successfully! All counts match. ===');
  } else {
    log('\n=== WARNING: Some counts do not match! Check the output above. ===');
    process.exit(1);
  }

  // Cleanup
  sqlite.close();
  await pool.end();
}

main().catch(err => {
  console.error('FATAL:', err);
  process.exit(1);
});
