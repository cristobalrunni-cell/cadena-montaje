#!/usr/bin/env node
/**
 * RUNNERPRO + MoneyBall — API Server (Cloud Run / PostgreSQL)
 *
 * Uso: DATABASE_URL=... node server.js
 * Puerto: 8080 (estándar Cloud Run)
 */

require('dotenv').config();
const express = require('express');
const { Pool } = require('pg');
const path = require('path');
const fs = require('fs');
const { spawn } = require('child_process');

// ═══════════════════════════════════════════════════════════════
// CONFIG (env vars for Cloud Run)
// ═══════════════════════════════════════════════════════════════
const PORT = parseInt(process.env.PORT) || 8080;
const GCS_BUCKET = process.env.GCS_BUCKET || '';  // e.g. 'moneyball-media-glass-timing'
const GCS_BASE = GCS_BUCKET ? `https://storage.googleapis.com/${GCS_BUCKET}` : '';

const RUNNERPRO_PATH = path.join(__dirname, 'runnerpro.html');
const DASHBOARD_PATH = path.join(__dirname, 'moneyball-dashboard.html');
const OP_DASHBOARDS_PATH = path.join(__dirname, 'operational-dashboards.html');
const PIPELINE_PATH = path.join(__dirname, 'pipeline.js');
const META_PIPELINE_PATH = path.join(__dirname, 'pipeline_meta.js');
const PIPELINE_CWD = __dirname;
const CREATORS_MD_DIR = path.join(__dirname, 'creators_md');
const NODE_BIN = process.env.NODE_BIN || 'node';

// ═══════════════════════════════════════════════════════════════
// DATABASE (PostgreSQL Pool)
// ═══════════════════════════════════════════════════════════════
const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  max: 20,
  idleTimeoutMillis: 30000,
  connectionTimeoutMillis: 5000,
});

// Helper: query shorthand
async function dbQuery(text, params) {
  const result = await pool.query(text, params);
  return result.rows;
}
async function dbQueryOne(text, params) {
  const result = await pool.query(text, params);
  return result.rows[0] || null;
}

// ═══════════════════════════════════════════════════════════════
// EXPRESS APP
// ═══════════════════════════════════════════════════════════════
const app = express();
app.use(express.json());

// ═══════════════════════════════════════════════════════════════
// STATIC FILES
// ═══════════════════════════════════════════════════════════════
app.get('/', (req, res) => res.sendFile(RUNNERPRO_PATH));
app.get('/dashboard', (req, res) => res.sendFile(DASHBOARD_PATH));
app.get('/operational-dashboards', (req, res) => res.sendFile(OP_DASHBOARDS_PATH));
app.use('/vertical-sankey', express.static(path.join(__dirname, 'vertical-sankey')));

// Frames and screenshots — redirect to Cloud Storage if configured
if (GCS_BASE) {
  app.get('/frames/{*path}', (req, res) => {
    const filePath = req.params.path;
    res.redirect(`${GCS_BASE}/frames/${filePath}`);
  });
  app.get('/meta-screenshots/{*path}', (req, res) => {
    const filePath = req.params.path;
    res.redirect(`${GCS_BASE}/meta-screenshots/${filePath}`);
  });
} else {
  // Fallback: serve from local dirs (for local dev)
  const framesDir = process.env.FRAMES_DIR || path.join(__dirname, 'escenas_frames');
  const screenshotsDir = process.env.SCREENSHOTS_DIR || path.join(__dirname, 'meta_ads_screenshots');
  app.use('/frames', express.static(framesDir, { maxAge: '1h' }));
  app.use('/meta-screenshots', express.static(screenshotsDir, { maxAge: '1h' }));
}

// --- Pipeline state (in-memory) ---
const pipelineRuns = new Map();
const pipelineBatches = new Map();

// ═══════════════════════════════════════════════════════════════
// HELPER: P75
// ═══════════════════════════════════════════════════════════════
async function getP75(creador) {
  const rows = await dbQuery(
    `SELECT visitas FROM contenido WHERE creador = $1 AND visitas IS NOT NULL ORDER BY visitas DESC`,
    [creador]
  );
  if (rows.length === 0) return 0;
  const idx = Math.floor(rows.length * 0.25);
  return rows[idx]?.visitas || 0;
}

// ═══════════════════════════════════════════════════════════════
// API: Creators overview
// ═══════════════════════════════════════════════════════════════
app.get('/api/creators', async (req, res) => {
  try {
    const creators = await dbQuery(`
      SELECT
        creador,
        COUNT(*) as total_videos,
        ROUND(AVG(visitas)) as avg_visitas,
        ROUND(AVG(likes)) as avg_likes,
        ROUND(AVG(comentarios)) as avg_comentarios,
        MAX(visitas) as max_visitas,
        MIN(fecha_publicacion) as primera_fecha,
        MAX(fecha_publicacion) as ultima_fecha
      FROM contenido
      GROUP BY creador
      ORDER BY total_videos DESC
    `);

    const videoCols = ['creador','url','visitas','likes','comentarios','duracion','fecha_publicacion','descripcion','transcripcion','visual','hook','tematica','formula_hook','semantica_cluster','semantica_inicio','semantica_ruta','visual_inicio','visual_ruta','visual_cluster'];
    const videoCompleteExpr = videoCols.map(c => `CASE WHEN ${c} IS NOT NULL AND ${c}::text != '' THEN 1 ELSE 0 END`).join(' + ');
    const videoColCount = videoCols.length;

    const sceneCols = ['video_id','video_url','escena_numero','tiempo_inicio','tiempo_fin','duracion_seg','fotogramas','escenario','personajes','objetivo_visual','edicion_visual','camara_edicion','descripcion_completa'];
    const sceneCompleteExpr = sceneCols.map(c => `CASE WHEN ${c} IS NOT NULL AND ${c}::text != '' THEN 1 ELSE 0 END`).join(' + ');
    const sceneColCount = sceneCols.length;

    for (const c of creators) {
      const scenes = await dbQueryOne(`
        SELECT COUNT(*) as cnt FROM escenas e
        JOIN contenido c ON e.video_id = c.id
        WHERE c.creador = $1
      `, [c.creador]);
      c.total_escenas = parseInt(scenes?.cnt) || 0;

      const vPct = await dbQueryOne(`
        SELECT ROUND(AVG((${videoCompleteExpr}) * 100.0 / ${videoColCount}), 1) as pct
        FROM contenido WHERE creador = $1
      `, [c.creador]);
      c.video_complete_pct = parseFloat(vPct?.pct) || 0;

      if (c.total_escenas > 0) {
        const sPct = await dbQueryOne(`
          SELECT ROUND(AVG((${sceneCompleteExpr}) * 100.0 / ${sceneColCount}), 1) as pct
          FROM escenas e JOIN contenido c ON e.video_id = c.id WHERE c.creador = $1
        `, [c.creador]);
        c.scene_complete_pct = parseFloat(sPct?.pct) || 0;
      } else {
        c.scene_complete_pct = 0;
      }
    }

    res.json({ creators });
  } catch (err) {
    console.error('GET /api/creators error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// ═══════════════════════════════════════════════════════════════
// API: Videos list (paginated, filterable, sortable)
// ═══════════════════════════════════════════════════════════════
app.get('/api/videos', async (req, res) => {
  try {
    const page = Math.max(1, parseInt(req.query.page) || 1);
    const limit = Math.min(100, Math.max(1, parseInt(req.query.limit) || 50));
    const offset = (page - 1) * limit;
    const creador = req.query.creador || '';
    const search = req.query.search || '';
    const sortBy = ['visitas', 'likes', 'comentarios', 'fecha_publicacion', 'created_at', 'duracion', 'tematica', 'formula_hook', 'semantica_inicio', 'visual_inicio'].includes(req.query.sort)
      ? req.query.sort : 'visitas';
    const order = req.query.order === 'ASC' ? 'ASC' : 'DESC';

    let where = '1=1';
    const params = [];
    let pIdx = 1;

    if (creador) {
      where += ` AND creador = $${pIdx++}`;
      params.push(creador);
    }
    if (search) {
      where += ` AND (LOWER(hook) LIKE $${pIdx} OR LOWER(transcripcion) LIKE $${pIdx} OR LOWER(descripcion) LIKE $${pIdx})`;
      pIdx++;
      params.push(`%${search.toLowerCase()}%`);
    }

    const countRow = await dbQueryOne(`SELECT COUNT(*) as cnt FROM contenido WHERE ${where}`, params);
    const total = parseInt(countRow.cnt);

    const videos = await dbQuery(`
      SELECT id, creador, url, visitas, likes, comentarios, duracion,
             fecha_publicacion, descripcion, transcripcion, visual, hook,
             tematica, formula_hook,
             semantica_cluster, semantica_inicio, semantica_ruta,
             visual_inicio, visual_ruta, visual_cluster,
             ejecutado, escenas_cortes, referente_para
      FROM contenido
      WHERE ${where}
      ORDER BY ${sortBy} ${order} NULLS LAST
      LIMIT $${pIdx++} OFFSET $${pIdx++}
    `, [...params, limit, offset]);

    res.json({ videos, total, page, pages: Math.ceil(total / limit), limit });
  } catch (err) {
    console.error('GET /api/videos error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// ═══════════════════════════════════════════════════════════════
// API: Video detail + scenes
// ═══════════════════════════════════════════════════════════════
app.get('/api/videos/:id', async (req, res) => {
  try {
    const id = parseInt(req.params.id);
    if (isNaN(id)) return res.status(400).json({ error: 'Invalid ID' });

    const video = await dbQueryOne(`
      SELECT id, creador, url, visitas, likes, comentarios, duracion,
             fecha_publicacion, descripcion, transcripcion, hook,
             formula_hook, semantica_inicio, semantica_ruta,
             visual_inicio, visual_ruta, tematica, created_at
      FROM contenido WHERE id = $1
    `, [id]);

    if (!video) return res.status(404).json({ error: 'Video not found' });

    const scenes = await dbQuery(`
      SELECT id, escena_numero, tiempo_inicio, tiempo_fin, duracion_seg,
             fotogramas, escenario, personajes, objetivo_visual,
             edicion_visual, camara_edicion, descripcion_completa
      FROM escenas
      WHERE video_id = $1
      ORDER BY escena_numero
    `, [id]);

    for (const s of scenes) {
      try { s.fotogramas = JSON.parse(s.fotogramas || '[]'); }
      catch { s.fotogramas = []; }
    }

    res.json({ video, scenes });
  } catch (err) {
    console.error('GET /api/videos/:id error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// ═══════════════════════════════════════════════════════════════
// API: Scenes explorer (paginated)
// ═══════════════════════════════════════════════════════════════
app.get('/api/scenes', async (req, res) => {
  try {
    const page = Math.max(1, parseInt(req.query.page) || 1);
    const limit = Math.min(100, Math.max(1, parseInt(req.query.limit) || 24));
    const offset = (page - 1) * limit;
    const creador = req.query.creador || '';
    const videoId = parseInt(req.query.video_id) || 0;

    let where = '1=1';
    const params = [];
    let pIdx = 1;

    if (creador) {
      where += ` AND c.creador = $${pIdx++}`;
      params.push(creador);
    }
    if (videoId) {
      where += ` AND e.video_id = $${pIdx++}`;
      params.push(videoId);
    }

    const countRow = await dbQueryOne(`
      SELECT COUNT(*) as cnt FROM escenas e
      JOIN contenido c ON e.video_id = c.id
      WHERE ${where}
    `, params);
    const total = parseInt(countRow.cnt);

    const scenes = await dbQuery(`
      SELECT e.id, e.video_id, e.escena_numero, e.tiempo_inicio, e.tiempo_fin,
             e.duracion_seg, e.fotogramas, e.escenario, e.personajes,
             e.objetivo_visual, e.descripcion_completa,
             c.creador, c.url as video_url, c.visitas
      FROM escenas e
      JOIN contenido c ON e.video_id = c.id
      WHERE ${where}
      ORDER BY e.id DESC
      LIMIT $${pIdx++} OFFSET $${pIdx++}
    `, [...params, limit, offset]);

    for (const s of scenes) {
      try { s.fotogramas = JSON.parse(s.fotogramas || '[]'); }
      catch { s.fotogramas = []; }
    }

    res.json({ scenes, total, page, pages: Math.ceil(total / limit), limit });
  } catch (err) {
    console.error('GET /api/scenes error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// ═══════════════════════════════════════════════════════════════
// API: MoneyBall analytics — formula_hook
// ═══════════════════════════════════════════════════════════════
app.get('/api/analytics/hooks', async (req, res) => {
  try {
    const creador = req.query.creador;
    if (!creador) return res.status(400).json({ error: 'creador required' });

    const p75 = await getP75(creador);

    const patterns = await dbQuery(`
      SELECT
        formula_hook,
        COUNT(*) as total,
        ROUND(AVG(visitas)) as avg_visitas,
        ROUND(SUM(CASE WHEN visitas >= $1 THEN 1.0 ELSE 0.0 END) / COUNT(*) * 100, 1) as tasa_exito
      FROM contenido
      WHERE creador = $2 AND formula_hook IS NOT NULL AND formula_hook != ''
      GROUP BY formula_hook
      HAVING COUNT(*) >= 2
      ORDER BY tasa_exito DESC
    `, [p75, creador]);

    res.json({ patterns, p75, creador });
  } catch (err) {
    console.error('GET /api/analytics/hooks error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// ═══════════════════════════════════════════════════════════════
// API: MoneyBall analytics — semantica_inicio
// ═══════════════════════════════════════════════════════════════
app.get('/api/analytics/semantica', async (req, res) => {
  try {
    const creador = req.query.creador;
    if (!creador) return res.status(400).json({ error: 'creador required' });
    const p75 = await getP75(creador);

    const patterns = await dbQuery(`
      SELECT
        semantica_inicio,
        COUNT(*) as total,
        ROUND(AVG(visitas)) as avg_visitas,
        ROUND(SUM(CASE WHEN visitas >= $1 THEN 1.0 ELSE 0.0 END) / COUNT(*) * 100, 1) as tasa_exito
      FROM contenido
      WHERE creador = $2 AND semantica_inicio IS NOT NULL AND semantica_inicio != ''
      GROUP BY semantica_inicio
      HAVING COUNT(*) >= 2
      ORDER BY tasa_exito DESC
    `, [p75, creador]);

    res.json({ patterns, p75, creador });
  } catch (err) {
    console.error('GET /api/analytics/semantica error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// ═══════════════════════════════════════════════════════════════
// API: MoneyBall analytics — visual_inicio
// ═══════════════════════════════════════════════════════════════
app.get('/api/analytics/visual', async (req, res) => {
  try {
    const creador = req.query.creador;
    if (!creador) return res.status(400).json({ error: 'creador required' });
    const p75 = await getP75(creador);

    const patterns = await dbQuery(`
      SELECT
        visual_inicio,
        COUNT(*) as total,
        ROUND(AVG(visitas)) as avg_visitas,
        ROUND(SUM(CASE WHEN visitas >= $1 THEN 1.0 ELSE 0.0 END) / COUNT(*) * 100, 1) as tasa_exito
      FROM contenido
      WHERE creador = $2 AND visual_inicio IS NOT NULL AND visual_inicio != ''
      GROUP BY visual_inicio
      HAVING COUNT(*) >= 2
      ORDER BY tasa_exito DESC
    `, [p75, creador]);

    res.json({ patterns, p75, creador });
  } catch (err) {
    console.error('GET /api/analytics/visual error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// ═══════════════════════════════════════════════════════════════
// API: Creator comparison
// ═══════════════════════════════════════════════════════════════
app.get('/api/analytics/compare', async (req, res) => {
  try {
    const creatorsRaw = await dbQuery(`SELECT DISTINCT creador FROM contenido ORDER BY creador`);
    const creators = creatorsRaw.map(r => r.creador);

    const comparison = [];
    for (const creador of creators) {
      const stats = await dbQueryOne(`
        SELECT
          COUNT(*) as total_videos,
          ROUND(AVG(visitas)) as avg_visitas,
          ROUND(AVG(likes)) as avg_likes,
          ROUND(AVG(comentarios)) as avg_comentarios,
          MAX(visitas) as max_visitas
        FROM contenido WHERE creador = $1
      `, [creador]);

      const p75 = await getP75(creador);

      const hookSuccess = await dbQueryOne(`
        SELECT ROUND(SUM(CASE WHEN visitas >= $1 THEN 1.0 ELSE 0.0 END) / COUNT(*) * 100, 1) as rate
        FROM contenido WHERE creador = $2 AND formula_hook IS NOT NULL AND formula_hook != ''
      `, [p75, creador]);

      const sceneCount = await dbQueryOne(`
        SELECT COUNT(*) as cnt FROM escenas e
        JOIN contenido c ON e.video_id = c.id WHERE c.creador = $1
      `, [creador]);

      comparison.push({
        creador,
        ...stats,
        p75,
        hook_success_rate: parseFloat(hookSuccess?.rate) || 0,
        total_escenas: parseInt(sceneCount?.cnt) || 0
      });
    }

    res.json({ comparison });
  } catch (err) {
    console.error('GET /api/analytics/compare error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// ═══════════════════════════════════════════════════════════════
// API: Activity log (DB updates grouped by day)
// ═══════════════════════════════════════════════════════════════
app.get('/api/activity', async (req, res) => {
  try {
    const group = req.query.group || 'day';
    let dateFn;
    if (group === 'week') dateFn = "to_char(created_at, 'IYYY-\"W\"IW')";
    else if (group === 'month') dateFn = "to_char(created_at, 'YYYY-MM')";
    else dateFn = "DATE(created_at)::text";

    const videosAdded = await dbQuery(`
      SELECT ${dateFn} as period, creador, COUNT(*) as videos_added
      FROM contenido
      WHERE created_at IS NOT NULL
      GROUP BY period, creador
      ORDER BY period DESC, videos_added DESC
    `);

    const dateFnUpd = dateFn.replace(/created_at/g, 'updated_at');
    const videosUpdated = await dbQuery(`
      SELECT ${dateFnUpd} as period, creador, COUNT(*) as videos_updated
      FROM contenido
      WHERE updated_at IS NOT NULL AND updated_at != created_at
      GROUP BY period, creador
      ORDER BY period DESC, videos_updated DESC
    `);

    const dateFnEsc = dateFn.replace(/created_at/g, 'e.created_at');
    const scenesAdded = await dbQuery(`
      SELECT ${dateFnEsc} as period, c.creador, COUNT(*) as scenes_added
      FROM escenas e
      JOIN contenido c ON e.video_id = c.id
      WHERE e.created_at IS NOT NULL
      GROUP BY period, c.creador
      ORDER BY period DESC, scenes_added DESC
    `);

    const summary = await dbQuery(`
      SELECT ${dateFn} as period,
        COUNT(*) as total_videos,
        COUNT(DISTINCT creador) as creators_active
      FROM contenido
      WHERE created_at IS NOT NULL
      GROUP BY period
      ORDER BY period DESC
    `);

    const sceneSummary = await dbQuery(`
      SELECT ${dateFnEsc} as period, COUNT(*) as total_scenes
      FROM escenas e
      WHERE e.created_at IS NOT NULL
      GROUP BY period
    `);
    const sceneMap = Object.fromEntries(sceneSummary.map(r => [r.period, parseInt(r.total_scenes)]));

    const updateSummary = await dbQuery(`
      SELECT ${dateFnUpd} as period, COUNT(*) as total_updated
      FROM contenido
      WHERE updated_at IS NOT NULL AND updated_at != created_at
      GROUP BY period
    `);
    const updateMap = Object.fromEntries(updateSummary.map(r => [r.period, parseInt(r.total_updated)]));

    for (const s of summary) {
      s.total_scenes = sceneMap[s.period] || 0;
      s.total_updated = updateMap[s.period] || 0;
    }

    const totals = {
      videos: parseInt((await dbQueryOne('SELECT COUNT(*) as cnt FROM contenido')).cnt),
      scenes: parseInt((await dbQueryOne('SELECT COUNT(*) as cnt FROM escenas')).cnt),
      creators: parseInt((await dbQueryOne('SELECT COUNT(DISTINCT creador) as cnt FROM contenido')).cnt),
      meta_ads: parseInt((await dbQueryOne('SELECT COUNT(*) as cnt FROM meta_ads')).cnt),
      embeddings_videos: parseInt((await dbQueryOne("SELECT COUNT(*) as cnt FROM contenido WHERE embedding IS NOT NULL AND embedding != ''")).cnt),
      embeddings_scenes: parseInt((await dbQueryOne("SELECT COUNT(*) as cnt FROM escenas WHERE embedding IS NOT NULL AND embedding != ''")).cnt),
    };

    res.json({ summary, videosAdded, videosUpdated, scenesAdded, totals, group });
  } catch (err) {
    console.error('GET /api/activity error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// ═══════════════════════════════════════════════════════════════
// META ADS ENDPOINTS
// ═══════════════════════════════════════════════════════════════

app.get('/api/meta/advertisers', async (req, res) => {
  try {
    const advertisers = await dbQuery(`
      SELECT
        anunciante,
        COUNT(*) as total_ads,
        SUM(CASE WHEN estado = 'activo' THEN 1 ELSE 0 END) as activos,
        SUM(CASE WHEN estado != 'activo' THEN 1 ELSE 0 END) as inactivos,
        SUM(CASE WHEN tipo_media = 'video' THEN 1 ELSE 0 END) as videos,
        SUM(CASE WHEN tipo_media = 'imagen' OR tipo_media = 'image' THEN 1 ELSE 0 END) as imagenes,
        SUM(CASE WHEN tipo_media = 'carrusel' OR tipo_media = 'carousel' THEN 1 ELSE 0 END) as carruseles,
        MIN(fecha_inicio) as primera_fecha,
        MAX(fecha_inicio) as ultima_fecha
      FROM meta_ads
      GROUP BY anunciante
      ORDER BY total_ads DESC
    `);

    const adCols = ['anunciante','ad_id','ad_url','estado','fecha_inicio','tipo_media','media_url',
                    'texto_principal','titulo','descripcion','cta',
                    'transcripcion','visual','hook','tematica','formula_hook',
                    'semantica_cluster','semantica_inicio','semantica_ruta',
                    'visual_inicio','visual_ruta','visual_cluster'];
    const completeExpr = adCols.map(c => `CASE WHEN ${c} IS NOT NULL AND ${c}::text != '' THEN 1 ELSE 0 END`).join(' + ');

    for (const a of advertisers) {
      const pct = await dbQueryOne(`
        SELECT ROUND(AVG((${completeExpr}) * 100.0 / ${adCols.length}), 1) as pct
        FROM meta_ads WHERE anunciante = $1
      `, [a.anunciante]);
      a.complete_pct = parseFloat(pct?.pct) || 0;
    }

    res.json({ advertisers });
  } catch (err) {
    console.error('GET /api/meta/advertisers error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

app.get('/api/meta/ads', async (req, res) => {
  try {
    const page = Math.max(1, parseInt(req.query.page) || 1);
    const limit = Math.min(100, Math.max(1, parseInt(req.query.limit) || 50));
    const offset = (page - 1) * limit;
    const anunciante = req.query.anunciante || '';
    const search = req.query.search || '';
    const sortBy = ['fecha_inicio','tipo_media','estado','anunciante','created_at']
      .includes(req.query.sort) ? req.query.sort : 'created_at';
    const order = req.query.order === 'ASC' ? 'ASC' : 'DESC';

    let where = '1=1';
    const params = [];
    let pIdx = 1;

    if (anunciante) { where += ` AND anunciante = $${pIdx++}`; params.push(anunciante); }
    if (search) {
      where += ` AND (LOWER(hook) LIKE $${pIdx} OR LOWER(texto_principal) LIKE $${pIdx} OR LOWER(transcripcion) LIKE $${pIdx} OR LOWER(titulo) LIKE $${pIdx})`;
      pIdx++;
      params.push(`%${search.toLowerCase()}%`);
    }

    const countRow = await dbQueryOne(`SELECT COUNT(*) as cnt FROM meta_ads WHERE ${where}`, params);
    const total = parseInt(countRow.cnt);

    const ads = await dbQuery(`
      SELECT *
      FROM meta_ads
      WHERE ${where}
      ORDER BY ${sortBy} ${order} NULLS LAST
      LIMIT $${pIdx++} OFFSET $${pIdx++}
    `, [...params, limit, offset]);

    res.json({ ads, total, page, pages: Math.ceil(total / limit), limit });
  } catch (err) {
    console.error('GET /api/meta/ads error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

app.get('/api/meta/ads/:id', async (req, res) => {
  try {
    const id = parseInt(req.params.id);
    if (isNaN(id)) return res.status(400).json({ error: 'Invalid ID' });
    const ad = await dbQueryOne('SELECT * FROM meta_ads WHERE id = $1', [id]);
    if (!ad) return res.status(404).json({ error: 'Ad not found' });
    res.json({ ad });
  } catch (err) {
    console.error('GET /api/meta/ads/:id error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

app.get('/api/meta/analytics/hooks', async (req, res) => {
  try {
    const anunciante = req.query.anunciante;
    let where = "formula_hook IS NOT NULL AND formula_hook != ''";
    const params = [];
    let pIdx = 1;
    if (anunciante) { where += ` AND anunciante = $${pIdx++}`; params.push(anunciante); }

    const patterns = await dbQuery(`
      SELECT formula_hook, COUNT(*) as total
      FROM meta_ads WHERE ${where}
      GROUP BY formula_hook
      ORDER BY total DESC
    `, params);

    res.json({ patterns, anunciante });
  } catch (err) {
    console.error('GET /api/meta/analytics/hooks error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

app.get('/api/meta/analytics/semantica', async (req, res) => {
  try {
    const anunciante = req.query.anunciante;
    let where = "semantica_inicio IS NOT NULL AND semantica_inicio != ''";
    const params = [];
    let pIdx = 1;
    if (anunciante) { where += ` AND anunciante = $${pIdx++}`; params.push(anunciante); }

    const patterns = await dbQuery(`
      SELECT semantica_inicio, COUNT(*) as total
      FROM meta_ads WHERE ${where}
      GROUP BY semantica_inicio
      ORDER BY total DESC
    `, params);

    res.json({ patterns, anunciante });
  } catch (err) {
    console.error('GET /api/meta/analytics/semantica error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

app.get('/api/meta/analytics/visual', async (req, res) => {
  try {
    const anunciante = req.query.anunciante;
    let where = "visual_inicio IS NOT NULL AND visual_inicio != ''";
    const params = [];
    let pIdx = 1;
    if (anunciante) { where += ` AND anunciante = $${pIdx++}`; params.push(anunciante); }

    const patterns = await dbQuery(`
      SELECT visual_inicio, COUNT(*) as total
      FROM meta_ads WHERE ${where}
      GROUP BY visual_inicio
      ORDER BY total DESC
    `, params);

    res.json({ patterns, anunciante });
  } catch (err) {
    console.error('GET /api/meta/analytics/visual error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

app.get('/api/meta/analytics/compare', async (req, res) => {
  try {
    const advRaw = await dbQuery('SELECT DISTINCT anunciante FROM meta_ads ORDER BY anunciante');
    const advertisers = advRaw.map(r => r.anunciante);

    const comparison = [];
    for (const anunciante of advertisers) {
      const stats = await dbQueryOne(`
        SELECT
          COUNT(*) as total_ads,
          SUM(CASE WHEN estado = 'activo' THEN 1 ELSE 0 END) as activos,
          SUM(CASE WHEN tipo_media = 'video' THEN 1 ELSE 0 END) as videos,
          SUM(CASE WHEN tipo_media = 'imagen' OR tipo_media = 'image' THEN 1 ELSE 0 END) as imagenes,
          SUM(CASE WHEN tipo_media = 'carrusel' OR tipo_media = 'carousel' THEN 1 ELSE 0 END) as carruseles
        FROM meta_ads WHERE anunciante = $1
      `, [anunciante]);
      comparison.push({ anunciante, ...stats });
    }

    res.json({ comparison });
  } catch (err) {
    console.error('GET /api/meta/analytics/compare error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

app.get('/api/meta/pipeline/status', async (req, res) => {
  try {
    const advertisers = await dbQuery(`
      SELECT anunciante, COUNT(*) as total,
        SUM(CASE WHEN ad_url IS NULL OR ad_url = '' THEN 1 ELSE 0 END) as sin_url,
        SUM(CASE WHEN estado IS NULL OR estado = '' THEN 1 ELSE 0 END) as sin_estado,
        SUM(CASE WHEN fecha_inicio IS NULL OR fecha_inicio = '' THEN 1 ELSE 0 END) as sin_fecha,
        SUM(CASE WHEN tipo_media IS NULL OR tipo_media = '' THEN 1 ELSE 0 END) as sin_tipo,
        SUM(CASE WHEN media_url IS NULL OR media_url = '' THEN 1 ELSE 0 END) as sin_media,
        SUM(CASE WHEN texto_principal IS NULL OR texto_principal = '' THEN 1 ELSE 0 END) as sin_texto,
        SUM(CASE WHEN titulo IS NULL OR titulo = '' THEN 1 ELSE 0 END) as sin_titulo,
        SUM(CASE WHEN descripcion IS NULL OR descripcion = '' THEN 1 ELSE 0 END) as sin_descripcion,
        SUM(CASE WHEN cta IS NULL OR cta = '' THEN 1 ELSE 0 END) as sin_cta,
        SUM(CASE WHEN transcripcion IS NULL OR transcripcion = '' THEN 1 ELSE 0 END) as sin_transcripcion,
        SUM(CASE WHEN visual IS NULL OR visual = '' THEN 1 ELSE 0 END) as sin_visual,
        SUM(CASE WHEN hook IS NULL OR hook = '' THEN 1 ELSE 0 END) as sin_hook,
        SUM(CASE WHEN tematica IS NULL OR tematica = '' THEN 1 ELSE 0 END) as sin_tematica,
        SUM(CASE WHEN formula_hook IS NULL OR formula_hook = '' THEN 1 ELSE 0 END) as sin_formula,
        SUM(CASE WHEN semantica_inicio IS NULL OR semantica_inicio = '' THEN 1 ELSE 0 END) as sin_sem_i,
        SUM(CASE WHEN semantica_ruta IS NULL OR semantica_ruta = '' THEN 1 ELSE 0 END) as sin_sem_r,
        SUM(CASE WHEN semantica_cluster IS NULL OR semantica_cluster = '' THEN 1 ELSE 0 END) as sin_sem_c,
        SUM(CASE WHEN visual_inicio IS NULL OR visual_inicio = '' THEN 1 ELSE 0 END) as sin_vis_i,
        SUM(CASE WHEN visual_ruta IS NULL OR visual_ruta = '' THEN 1 ELSE 0 END) as sin_vis_r,
        SUM(CASE WHEN visual_cluster IS NULL OR visual_cluster = '' THEN 1 ELSE 0 END) as sin_vis_c,
        SUM(CASE WHEN nomenclatura_ad IS NULL OR nomenclatura_ad = '' THEN 1 ELSE 0 END) as sin_nom
      FROM meta_ads
      GROUP BY anunciante
      ORDER BY total DESC
    `);

    res.json({ advertisers });
  } catch (err) {
    console.error('GET /api/meta/pipeline/status error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// ═══════════════════════════════════════════════════════════════
// PIPELINE ENDPOINTS
// ═══════════════════════════════════════════════════════════════

app.get('/api/pipeline/status', async (req, res) => {
  try {
    const creators = await dbQuery(`
      SELECT creador, COUNT(*) as total,
        SUM(CASE WHEN visitas IS NULL OR visitas = 0 THEN 1 ELSE 0 END) as sin_visitas,
        SUM(CASE WHEN likes IS NULL OR likes = 0 THEN 1 ELSE 0 END) as sin_likes,
        SUM(CASE WHEN comentarios IS NULL OR comentarios = 0 THEN 1 ELSE 0 END) as sin_comentarios,
        SUM(CASE WHEN duracion IS NULL OR duracion = '' THEN 1 ELSE 0 END) as sin_duracion,
        SUM(CASE WHEN fecha_publicacion IS NULL OR fecha_publicacion = '' THEN 1 ELSE 0 END) as sin_fecha,
        SUM(CASE WHEN descripcion IS NULL OR descripcion = '' THEN 1 ELSE 0 END) as sin_descripcion,
        SUM(CASE WHEN transcripcion IS NULL OR transcripcion = '' THEN 1 ELSE 0 END) as sin_transcripcion,
        SUM(CASE WHEN visual IS NULL OR visual = '' THEN 1 ELSE 0 END) as sin_visual,
        SUM(CASE WHEN hook IS NULL OR hook = '' THEN 1 ELSE 0 END) as sin_hook,
        SUM(CASE WHEN tematica IS NULL OR tematica = '' THEN 1 ELSE 0 END) as sin_tematica,
        SUM(CASE WHEN formula_hook IS NULL OR formula_hook = '' THEN 1 ELSE 0 END) as sin_formula_hook,
        SUM(CASE WHEN semantica_inicio IS NULL OR semantica_inicio = '' THEN 1 ELSE 0 END) as sin_sem_inicio,
        SUM(CASE WHEN semantica_ruta IS NULL OR semantica_ruta = '' THEN 1 ELSE 0 END) as sin_sem_ruta,
        SUM(CASE WHEN semantica_cluster IS NULL OR semantica_cluster = '' THEN 1 ELSE 0 END) as sin_sem_cluster,
        SUM(CASE WHEN visual_inicio IS NULL OR visual_inicio = '' THEN 1 ELSE 0 END) as sin_vis_inicio,
        SUM(CASE WHEN visual_ruta IS NULL OR visual_ruta = '' THEN 1 ELSE 0 END) as sin_vis_ruta,
        SUM(CASE WHEN visual_cluster IS NULL OR visual_cluster = '' THEN 1 ELSE 0 END) as sin_vis_cluster,
        SUM(CASE WHEN escenas_cortes IS NULL OR escenas_cortes = '' THEN 1 ELSE 0 END) as sin_escenas,
        SUM(CASE WHEN embedding IS NULL OR embedding = '' THEN 1 ELSE 0 END) as sin_embedding
      FROM contenido
      GROUP BY creador
      ORDER BY total DESC
    `);

    const escenasStatus = await dbQuery(`
      SELECT c.creador,
        COUNT(*) as total_escenas,
        SUM(CASE WHEN e.escenario IS NULL OR e.escenario = '' THEN 1 ELSE 0 END) as sin_analisis,
        SUM(CASE WHEN e.embedding IS NULL OR e.embedding = '' THEN 1 ELSE 0 END) as sin_emb_escena
      FROM escenas e
      JOIN contenido c ON e.video_id = c.id
      GROUP BY c.creador
    `);
    const escenasMap = Object.fromEntries(escenasStatus.map(e => [e.creador, e]));

    for (const c of creators) {
      const esc = escenasMap[c.creador] || {};
      c.total_escenas = parseInt(esc.total_escenas) || 0;
      c.sin_analisis_escenas = parseInt(esc.sin_analisis) || 0;
      c.sin_emb_escena = parseInt(esc.sin_emb_escena) || 0;
    }

    let contextFiles = [];
    try {
      contextFiles = fs.readdirSync(CREATORS_MD_DIR).filter(f => f.endsWith('.md')).sort();
    } catch (e) { /* dir might not exist */ }

    res.json({ creators, contextFiles });
  } catch (err) {
    console.error('GET /api/pipeline/status error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// --- Pipeline: List runs ---
app.get('/api/pipeline/runs', (req, res) => {
  const runs = [];
  for (const [runId, run] of pipelineRuns) {
    if (run.batchId) continue;
    runs.push({
      runId,
      status: run.status,
      startedAt: run.startedAt,
      endedAt: run.endedAt,
      args: run.args,
      outputLines: run.output.length,
      exitCode: run.exitCode
    });
  }

  for (const [batchId, batch] of pipelineBatches) {
    runs.push({
      runId: batchId,
      status: batch.status,
      startedAt: batch.startedAt,
      endedAt: batch.endedAt,
      args: [`${batch.workers} workers`],
      outputLines: batch.output.length,
      exitCode: batch.status === 'completed' ? 0 : batch.status === 'running' ? null : 1,
      isBatch: true,
      workers: batch.workers,
      runIds: batch.runIds
    });
  }

  runs.sort((a, b) => new Date(b.startedAt) - new Date(a.startedAt));
  res.json({ runs });
});

// --- Pipeline: Helper to build base args ---
function buildPipelineArgs(body) {
  const isMeta = body.table === 'meta_ads';
  const args = [isMeta ? META_PIPELINE_PATH : PIPELINE_PATH];

  if (isMeta) {
    if (body.anunciante) args.push(`--anunciante=${body.anunciante}`);
  } else {
    if (body.creador) args.push(`--creador=${body.creador}`);
  }

  if (body.ids && body.ids.length) args.push(`--ids=${body.ids.join(',')}`);
  if (body.where) args.push(`--where=${body.where}`);
  if (body.only && body.only.length) args.push(`--only=${body.only.join(',')}`);
  if (body.skip && body.skip.length) args.push(`--skip=${body.skip.join(',')}`);

  if (!isMeta) {
    if (body.context) args.push(`--context=${body.context}`);
    if (body.contextFile) {
      const cfPath = path.join(CREATORS_MD_DIR, body.contextFile);
      args.push(`--context-file=${cfPath}`);
    }
    if (body.forceStats) args.push('--force-stats');
  } else {
    if (body.force) args.push('--force');
  }

  if (body.limit) args.push(`--limit=${body.limit}`);
  if (body.dryRun) args.push('--dry-run');
  return args;
}

// --- Pipeline: Spawn a single child process and track it ---
function spawnPipelineRun(args, runId, workerLabel) {
  const child = spawn(NODE_BIN, args, {
    cwd: PIPELINE_CWD,
    env: { ...process.env },
    stdio: ['ignore', 'pipe', 'pipe']
  });

  const prefix = workerLabel != null ? `[W${workerLabel}] ` : '';

  const run = {
    process: child,
    status: 'running',
    startedAt: new Date().toISOString(),
    endedAt: null,
    args: args.slice(1),
    output: [],
    exitCode: null,
    batchId: null
  };

  let stdoutBuf = '';
  child.stdout.on('data', (chunk) => {
    stdoutBuf += chunk.toString();
    const lines = stdoutBuf.split('\n');
    stdoutBuf = lines.pop();
    lines.forEach(line => { if (line.trim()) run.output.push(prefix + line); });
  });

  let stderrBuf = '';
  child.stderr.on('data', (chunk) => {
    stderrBuf += chunk.toString();
    const lines = stderrBuf.split('\n');
    stderrBuf = lines.pop();
    lines.forEach(line => { if (line.trim()) run.output.push(prefix + '[stderr] ' + line); });
  });

  child.on('close', (code) => {
    if (stdoutBuf.trim()) run.output.push(prefix + stdoutBuf.trim());
    if (stderrBuf.trim()) run.output.push(prefix + '[stderr] ' + stderrBuf.trim());
    run.exitCode = code;
    run.status = code === 0 ? 'completed' : 'failed';
    run.endedAt = new Date().toISOString();

    const completed = [...pipelineRuns.entries()]
      .filter(([, r]) => r.status !== 'running')
      .sort((a, b) => new Date(b[1].startedAt) - new Date(a[1].startedAt));
    while (completed.length > 20) {
      const [oldId] = completed.pop();
      pipelineRuns.delete(oldId);
    }
  });

  pipelineRuns.set(runId, run);
  return run;
}

// --- Pipeline: Start a run (single or parallel) ---
app.post('/api/pipeline/run', (req, res) => {
  const body = req.body || {};
  const isMeta = body.table === 'meta_ads';
  if (!body.creador && !body.anunciante && !body.ids && !body.where) {
    return res.status(400).json({ error: isMeta ? 'Necesitas al menos anunciante, ids o where' : 'Necesitas al menos creador, ids o where' });
  }

  const workers = Math.min(10, Math.max(1, parseInt(body.workers) || 1));

  for (const [id, run] of pipelineRuns) {
    if (run.status === 'running') {
      return res.status(409).json({ error: 'Ya hay un pipeline en ejecución', runId: id });
    }
  }

  if (workers === 1) {
    const args = buildPipelineArgs(body);
    if (body.mod != null && body.part != null) {
      args.push(`--mod=${body.mod}`);
      args.push(`--part=${body.part}`);
    }
    const runId = Date.now().toString(36) + Math.random().toString(36).slice(2, 6);
    spawnPipelineRun(args, runId, null);
    return res.json({ runId, status: 'running', args: pipelineRuns.get(runId).args });
  }

  // Parallel: spawn N workers
  const batchId = 'batch_' + Date.now().toString(36) + Math.random().toString(36).slice(2, 6);
  const runIds = [];
  const batch = {
    batchId, workers, runIds: [], status: 'running',
    startedAt: new Date().toISOString(), endedAt: null,
    output: [], _cursors: {}
  };

  for (let i = 0; i < workers; i++) {
    const args = buildPipelineArgs(body);
    args.push(`--mod=${workers}`);
    args.push(`--part=${i}`);
    const runId = batchId + '_w' + i;
    const run = spawnPipelineRun(args, runId, i);
    run.batchId = batchId;
    runIds.push(runId);
    batch._cursors[runId] = 0;
  }

  batch.runIds = runIds;
  pipelineBatches.set(batchId, batch);

  const mergeInterval = setInterval(() => {
    for (const rid of batch.runIds) {
      const run = pipelineRuns.get(rid);
      if (!run) continue;
      while (batch._cursors[rid] < run.output.length) {
        batch.output.push(run.output[batch._cursors[rid]]);
        batch._cursors[rid]++;
      }
    }
    const allDone = batch.runIds.every(rid => {
      const r = pipelineRuns.get(rid);
      return r && r.status !== 'running';
    });
    if (allDone) {
      clearInterval(mergeInterval);
      const anyFailed = batch.runIds.some(rid => pipelineRuns.get(rid)?.status === 'failed');
      batch.status = anyFailed ? 'failed' : 'completed';
      batch.endedAt = new Date().toISOString();
      const completed = batch.runIds.filter(rid => pipelineRuns.get(rid)?.status === 'completed').length;
      const failed = batch.runIds.filter(rid => pipelineRuns.get(rid)?.status === 'failed').length;
      batch.output.push(`\n═══════════════════════════════════════════`);
      batch.output.push(`Batch completo: ${completed}/${workers} workers OK${failed > 0 ? `, ${failed} con errores` : ''}`);
      batch.output.push(`═══════════════════════════════════════════`);
    }
  }, 200);

  res.json({ batchId, runIds, workers, status: 'running' });
});

// --- Pipeline: SSE stream for a run ---
app.get('/api/pipeline/stream/:runId', (req, res) => {
  const run = pipelineRuns.get(req.params.runId);
  if (!run) return res.status(404).json({ error: 'Run not found' });

  res.setHeader('Content-Type', 'text/event-stream');
  res.setHeader('Cache-Control', 'no-cache');
  res.setHeader('Connection', 'keep-alive');
  res.flushHeaders();

  let cursor = 0;
  for (const line of run.output) {
    res.write(`data: ${JSON.stringify({ type: 'log', text: line })}\n\n`);
    cursor++;
  }

  if (run.status !== 'running') {
    res.write(`data: ${JSON.stringify({ type: 'done', status: run.status, exitCode: run.exitCode })}\n\n`);
    return res.end();
  }

  const interval = setInterval(() => {
    while (cursor < run.output.length) {
      res.write(`data: ${JSON.stringify({ type: 'log', text: run.output[cursor] })}\n\n`);
      cursor++;
    }
    if (run.status !== 'running') {
      res.write(`data: ${JSON.stringify({ type: 'done', status: run.status, exitCode: run.exitCode })}\n\n`);
      clearInterval(interval);
      res.end();
    }
  }, 300);

  req.on('close', () => clearInterval(interval));
});

// --- Pipeline: SSE stream for a batch ---
app.get('/api/pipeline/stream/batch/:batchId', (req, res) => {
  const batch = pipelineBatches.get(req.params.batchId);
  if (!batch) return res.status(404).json({ error: 'Batch not found' });

  res.setHeader('Content-Type', 'text/event-stream');
  res.setHeader('Cache-Control', 'no-cache');
  res.setHeader('Connection', 'keep-alive');
  res.flushHeaders();

  let cursor = 0;
  for (const line of batch.output) {
    res.write(`data: ${JSON.stringify({ type: 'log', text: line })}\n\n`);
    cursor++;
  }

  if (batch.status !== 'running') {
    res.write(`data: ${JSON.stringify({ type: 'done', status: batch.status, exitCode: batch.status === 'completed' ? 0 : 1 })}\n\n`);
    return res.end();
  }

  const interval = setInterval(() => {
    while (cursor < batch.output.length) {
      res.write(`data: ${JSON.stringify({ type: 'log', text: batch.output[cursor] })}\n\n`);
      cursor++;
    }
    if (batch.status !== 'running') {
      res.write(`data: ${JSON.stringify({ type: 'done', status: batch.status, exitCode: batch.status === 'completed' ? 0 : 1 })}\n\n`);
      clearInterval(interval);
      res.end();
    }
  }, 300);

  req.on('close', () => clearInterval(interval));
});

// --- Pipeline: Cancel a run or batch ---
app.post('/api/pipeline/cancel/:runId', (req, res) => {
  const runId = req.params.runId;

  const batch = pipelineBatches.get(runId);
  if (batch) {
    for (const rid of batch.runIds) {
      const run = pipelineRuns.get(rid);
      if (run && run.status === 'running') {
        try { run.process.kill('SIGTERM'); } catch (e) {}
        run.status = 'cancelled';
        run.endedAt = new Date().toISOString();
        run.output.push('Pipeline cancelled by user');
      }
    }
    batch.status = 'cancelled';
    batch.endedAt = new Date().toISOString();
    batch.output.push('Batch cancelado por el usuario');
    return res.json({ status: 'cancelled', batchId: runId });
  }

  const run = pipelineRuns.get(runId);
  if (!run) return res.status(404).json({ error: 'Run not found' });
  if (run.status !== 'running') return res.json({ status: run.status });

  try { run.process.kill('SIGTERM'); } catch (e) {}
  run.status = 'cancelled';
  run.endedAt = new Date().toISOString();
  run.output.push('Pipeline cancelled by user');

  res.json({ status: 'cancelled' });
});

// ═══════════════════════════════════════════════════════════════
// CADENA DE MONTAJE
// ═══════════════════════════════════════════════════════════════
const CADENA_SCRIPTS = {
  'crea-idea': 'crea_idea.js',
  'guion-ia': 'crea_guion_ceo_media.js',
  'visual-ia': 'crea_visual.js'
};

app.post('/api/cadena/run', (req, res) => {
  const { process: proc, semana, dryRun, context } = req.body || {};
  if (!proc || !CADENA_SCRIPTS[proc]) {
    return res.status(400).json({ error: `Proceso no válido. Usa: ${Object.keys(CADENA_SCRIPTS).join(', ')}` });
  }

  for (const [id, run] of pipelineRuns) {
    if (run.status === 'running' && id.startsWith('cadena-')) {
      return res.status(409).json({ error: 'Ya hay un proceso de cadena en ejecución', runId: id });
    }
  }

  const scriptPath = path.join(PIPELINE_CWD, CADENA_SCRIPTS[proc]);
  const args = [scriptPath];
  if (semana) args.push(`--semana=${semana}`);
  if (dryRun) args.push('--dry-run');
  if (context && typeof context === 'object') {
    for (const [pilar, texto] of Object.entries(context)) {
      if (texto && typeof texto === 'string' && texto.trim()) {
        args.push(`--context-${pilar}=${texto.trim()}`);
      }
    }
  }

  const runId = `cadena-${proc}-${Date.now()}`;
  spawnPipelineRun(args, runId, null);
  res.json({ runId, status: 'running', process: proc, semana: semana || 'auto' });
});

app.get('/api/cadena/active', (req, res) => {
  for (const [id, run] of pipelineRuns) {
    if (id.startsWith('cadena-') && run.status === 'running') {
      const proc = id.replace(/^cadena-/, '').replace(/-\d+$/, '');
      return res.json({ runId: id, process: proc, status: 'running', output: run.output });
    }
  }
  let latest = null;
  let latestTime = 0;
  for (const [id, run] of pipelineRuns) {
    if (id.startsWith('cadena-') && run.status !== 'running' && run.endedAt) {
      const ended = new Date(run.endedAt).getTime();
      if (ended > latestTime) { latestTime = ended; latest = { id, run }; }
    }
  }
  if (latest && (Date.now() - latestTime < 60000)) {
    const proc = latest.id.replace(/^cadena-/, '').replace(/-\d+$/, '');
    return res.json({
      runId: latest.id, process: proc, status: latest.run.status,
      exitCode: latest.run.exitCode, output: latest.run.output
    });
  }
  res.json({ runId: null });
});

// ── Flowchart: auto-parse script source to extract flow steps ──
app.get('/api/cadena/flowchart/:process', (req, res) => {
  const proc = req.params.process;
  if (!CADENA_SCRIPTS[proc]) {
    return res.status(400).json({ error: 'Proceso no válido' });
  }
  const scriptPath = path.join(PIPELINE_CWD, CADENA_SCRIPTS[proc]);
  if (!fs.existsSync(scriptPath)) {
    return res.status(404).json({ error: 'Script no encontrado' });
  }
  const src = fs.readFileSync(scriptPath, 'utf-8');

  // Parse steps from // Paso N: or // FASE N: or // === FASE N: === comments inside main()
  const mainMatch = src.match(/async function main\(\)\s*\{([\s\S]+)$/);
  const steps = [];
  if (mainMatch) {
    const mainBody = mainMatch[1];
    // Match: // Paso N: label, // === FASE N: label ===, // FASE N: label
    const stepRe = /\/\/\s*(?:={3,}\s*)?(?:Paso|FASE)\s*(\d+[A-Z]?)\s*(?:[:—–-]+|:\s*)\s*(.+)/gi;
    let s;
    while ((s = stepRe.exec(mainBody)) !== null) {
      let label = s[2].replace(/\s*={3,}\s*$/, '').replace(/\(.*?\)\s*$/, '').trim();
      // Remove trailing "===" artifacts
      label = label.replace(/\s*=+\s*$/, '').trim();
      // Skip if label contains only "Procesar cada" type meta-steps when Pasos exist
      steps.push({ id: s[1].trim(), label });
    }
  }

  // Deduplicate: if we have both FASE-level and Paso-level, keep only Pasos (more granular)
  const hasPasos = steps.some(s => parseInt(s.id) >= 1);
  const hasFases = steps.some(s => s.id === '0');
  let flow = steps;
  if (hasPasos && hasFases && steps.length > 5) {
    // Keep FASE 0 as first step, then all numbered Pasos
    const fase0 = steps.find(s => s.id === '0');
    const pasos = steps.filter(s => parseInt(s.id) >= 1);
    // Re-number sequentially
    flow = [fase0, ...pasos].filter(Boolean).map((s, i) => ({ id: String(i), label: s.label }));
  }

  // Fallback: parse // === SECTION === headers from whole file
  if (flow.length === 0) {
    const sectionRe = /^\/\/\s*={3,}\s*(.+?)\s*={3,}\s*$/gm;
    let m;
    const skip = ['CONFIGURACIÓN', 'UTILIDADES', 'MAIN'];
    let idx = 1;
    while ((m = sectionRe.exec(src)) !== null) {
      const name = m[1].trim();
      if (skip.includes(name)) continue;
      flow.push({ id: String(idx++), label: name });
    }
  }

  res.json({ process: proc, script: CADENA_SCRIPTS[proc], steps: flow });
});

// ═══════════════════════════════════════════════════════════════
// START SERVER
// ═══════════════════════════════════════════════════════════════
app.listen(PORT, () => {
  console.log(`\n  RUNNERPRO running on port ${PORT}`);
  console.log(`  Database: ${process.env.DATABASE_URL ? 'PostgreSQL (connected)' : 'NOT CONFIGURED'}`);
  console.log(`  Cloud Storage: ${GCS_BUCKET || 'local'}\n`);
});
