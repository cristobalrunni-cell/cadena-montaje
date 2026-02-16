#!/usr/bin/env node
/**
 * MoneyBall Pipeline Orchestrator
 *
 * Detecta columnas vacías por fila y ejecuta solo las fases necesarias.
 *
 * Uso:
 *   node pipeline.js --creador=veneno
 *   node pipeline.js --ids=100,101,102
 *   node pipeline.js --where="creador = 'veneno' AND visitas > 10000"
 *   node pipeline.js --creador=veneno --only=semantica,visual_analysis
 *   node pipeline.js --creador=veneno --skip=embeddings,frames
 *   node pipeline.js --creador=veneno --context="Creador de moda urbana"
 *   node pipeline.js --creador=veneno --context-file=./contextos/veneno.md
 *   node pipeline.js --creador=veneno --limit=10
 *   node pipeline.js --creador=veneno --dry-run
 *   node pipeline.js --creador=veneno --mod=4 --part=0
 */

const fs = require('fs');
const path = require('path');
const { execSync } = require('child_process');
const { Pool } = require('pg');
const { GoogleGenerativeAI } = require('@google/generative-ai');

// ═══════════════════════════════════════════════════════════════
// CONFIG
// ═══════════════════════════════════════════════════════════════
const CONFIG = {
  promptsDir: path.join(__dirname, 'prompts'),
  framesDir: process.env.FRAMES_DIR || path.join(__dirname, 'escenas_frames'),
  tempDir: '/tmp/moneyball_pipeline',
  geminiModel: 'gemini-2.5-flash',
  embeddingModel: 'gemini-embedding-001',
  concurrency: 3,
  delayMs: 500,
};

// Gemini key: from env or file
const GEMINI_KEY = process.env.GEMINI_API_KEY
  || (() => { try { return fs.readFileSync(path.join(process.env.HOME, '.config/gemini/api_key'), 'utf8').trim(); } catch(e) { return ''; } })();

// Load prompts
const PROMPT_VIDEO_TRANS = fs.readFileSync(path.join(CONFIG.promptsDir, 'video_transcripcion.txt'), 'utf8');
const PROMPT_VIDEO_VISUAL = fs.readFileSync(path.join(CONFIG.promptsDir, 'video_visual.txt'), 'utf8');
const PROMPT_SEMANTICA = fs.readFileSync(path.join(CONFIG.promptsDir, 'transcripcion.txt'), 'utf8');
const PROMPT_VISUAL = fs.readFileSync(path.join(CONFIG.promptsDir, 'visual.txt'), 'utf8');

// PATH for execSync (yt-dlp, ffmpeg)
const EXEC_ENV = { ...process.env };

// Temp dir
if (!fs.existsSync(CONFIG.tempDir)) fs.mkdirSync(CONFIG.tempDir, { recursive: true });

// Phase names for CLI
const ALL_PHASES = ['refresh_stats', 'transcripcion', 'semantica', 'visual_analysis', 'escenas', 'analyze_escenas', 'frames', 'embeddings', 'emb_escenas'];

// ═══════════════════════════════════════════════════════════════
// CLI ARGUMENT PARSING
// ═══════════════════════════════════════════════════════════════
function parseArgs() {
  const args = process.argv.slice(2);
  const get = (name) => {
    const arg = args.find(a => a.startsWith(`--${name}=`));
    return arg ? arg.split('=').slice(1).join('=') : null;
  };
  const has = (name) => args.includes(`--${name}`);

  return {
    creador: get('creador'),
    ids: get('ids') ? get('ids').split(',').map(Number) : null,
    where: get('where'),
    only: get('only') ? get('only').split(',') : null,
    skip: get('skip') ? get('skip').split(',') : [],
    context: get('context'),
    contextFile: get('context-file'),
    limit: get('limit') ? parseInt(get('limit')) : null,
    dryRun: has('dry-run'),
    forceStats: has('force-stats'),
    mod: get('mod') ? parseInt(get('mod')) : null,
    part: get('part') !== null && get('part') !== undefined ? parseInt(get('part')) : null,
    help: has('help') || has('h'),
  };
}

function showHelp() {
  console.log(`
  MoneyBall Pipeline Orchestrator
  ═══════════════════════════════════════════════════════════════

  FILTROS:
    --creador=veneno          Filtrar por creador
    --ids=100,101,102         Filtrar por IDs específicos
    --where="SQL condition"   Filtro SQL custom
    --limit=10                Limitar número de filas
    --mod=4 --part=0          Paralelización (id % mod = part)

  FASES:
    --only=semantica,visual_analysis   Ejecutar solo estas fases
    --skip=embeddings,frames           Saltar estas fases
    Fases: ${ALL_PHASES.join(', ')}

  CONTEXTO:
    --context="texto"          Contexto inline para prompts IA
    --context-file=./file.md   Archivo de contexto

  OPCIONES:
    --dry-run                  Mostrar qué haría sin ejecutar
    --force-stats              Forzar refresh_stats incluso si ya hay datos
    --help                     Mostrar esta ayuda
  `);
}

// ═══════════════════════════════════════════════════════════════
// GEMINI API HELPERS
// ═══════════════════════════════════════════════════════════════
async function callGeminiText(prompt, maxRetries = 5) {
  for (let attempt = 0; attempt < maxRetries; attempt++) {
    try {
      const response = await fetch(
        `https://generativelanguage.googleapis.com/v1beta/models/${CONFIG.geminiModel}:generateContent?key=${GEMINI_KEY}`,
        {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            contents: [{ parts: [{ text: prompt }] }],
            generationConfig: {
              temperature: 0.3,
              maxOutputTokens: 2000,
              responseMimeType: 'application/json',
            },
          }),
        }
      );

      if (response.ok) {
        const data = await response.json();
        const text = data.candidates?.[0]?.content?.parts?.[0]?.text;
        if (!text) throw new Error('No response text from Gemini');

        try {
          return JSON.parse(text);
        } catch (e) {
          // Try extracting JSON from markdown
          const match = text.match(/\{[\s\S]*\}/);
          if (match) return JSON.parse(match[0]);
          throw new Error(`Invalid JSON: ${text.substring(0, 200)}`);
        }
      }

      if ([429, 500, 503].includes(response.status)) {
        const wait = Math.min(30000, 2000 * Math.pow(2, attempt));
        console.log(`    ⚠️ Gemini ${response.status}, retry ${attempt + 1}/${maxRetries} en ${wait / 1000}s...`);
        await sleep(wait);
        continue;
      }

      const errText = await response.text();
      throw new Error(`Gemini API error: ${response.status} - ${errText.substring(0, 200)}`);
    } catch (err) {
      if (err.message?.includes('Gemini API error')) throw err;
      if (attempt === maxRetries - 1) throw err;
      const wait = Math.min(30000, 2000 * Math.pow(2, attempt));
      console.log(`    ⚠️ Network error, retry ${attempt + 1}/${maxRetries} en ${wait / 1000}s...`);
      await sleep(wait);
    }
  }
  return {};
}

async function callGeminiWithVideo(fileUri, mimeType, prompt, maxRetries = 5) {
  for (let attempt = 0; attempt < maxRetries; attempt++) {
    try {
      const response = await fetch(
        `https://generativelanguage.googleapis.com/v1beta/models/${CONFIG.geminiModel}:generateContent?key=${GEMINI_KEY}`,
        {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            contents: [{
              parts: [
                { file_data: { mime_type: mimeType, file_uri: fileUri } },
                { text: prompt },
              ],
            }],
            generationConfig: { temperature: 0.3, maxOutputTokens: 8000 },
          }),
        }
      );

      if (response.ok) {
        const data = await response.json();
        return data.candidates?.[0]?.content?.parts?.[0]?.text || '';
      }

      if ([429, 500, 503].includes(response.status)) {
        const wait = Math.min(30000, 2000 * Math.pow(2, attempt));
        console.log(`    ⚠️ Gemini ${response.status}, retry ${attempt + 1}/${maxRetries}...`);
        await sleep(wait);
        continue;
      }

      const errText = await response.text();
      throw new Error(`Gemini API error: ${response.status} - ${errText.substring(0, 200)}`);
    } catch (err) {
      if (err.message?.includes('Gemini API error')) throw err;
      if (attempt === maxRetries - 1) throw err;
      await sleep(Math.min(30000, 2000 * Math.pow(2, attempt)));
    }
  }
  return '';
}

async function generateEmbedding(texto) {
  const genAI = new GoogleGenerativeAI(GEMINI_KEY);
  const model = genAI.getGenerativeModel({ model: CONFIG.embeddingModel });
  try {
    const result = await model.embedContent(texto);
    return result.embedding.values;
  } catch (err) {
    console.log(`    ❌ Embedding error: ${err.message}`);
    return null;
  }
}

// ═══════════════════════════════════════════════════════════════
// VIDEO HELPERS (from process_videos.js)
// ═══════════════════════════════════════════════════════════════
function downloadVideo(url, outputPath) {
  try {
    execSync(`yt-dlp -f "best[ext=mp4]/best" -o "${outputPath}" "${url}" --quiet --no-warnings`, {
      timeout: 180000, env: EXEC_ENV,
    });
    return fs.existsSync(outputPath);
  } catch (err) {
    return false;
  }
}

async function uploadToGemini(filePath) {
  const fileSize = fs.statSync(filePath).size;
  const fileName = path.basename(filePath);

  const initRes = await fetch(
    `https://generativelanguage.googleapis.com/upload/v1beta/files?key=${GEMINI_KEY}`,
    {
      method: 'POST',
      headers: {
        'X-Goog-Upload-Protocol': 'resumable',
        'X-Goog-Upload-Command': 'start',
        'X-Goog-Upload-Header-Content-Length': fileSize.toString(),
        'X-Goog-Upload-Header-Content-Type': 'video/mp4',
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({ file: { display_name: fileName } }),
    }
  );

  const uploadUrl = initRes.headers.get('X-Goog-Upload-URL');
  if (!uploadUrl) throw new Error('No upload URL from Gemini Files API');

  const fileData = fs.readFileSync(filePath);
  const uploadRes = await fetch(uploadUrl, {
    method: 'PUT',
    headers: {
      'Content-Length': fileSize.toString(),
      'X-Goog-Upload-Offset': '0',
      'X-Goog-Upload-Command': 'upload, finalize',
    },
    body: fileData,
  });

  if (!uploadRes.ok) throw new Error(`Upload failed: ${await uploadRes.text()}`);
  const result = await uploadRes.json();
  return result.file;
}

async function waitForGeminiFile(fileName, maxWait = 60) {
  for (let i = 0; i < maxWait; i++) {
    const res = await fetch(`https://generativelanguage.googleapis.com/v1beta/${fileName}?key=${GEMINI_KEY}`);
    const file = await res.json();
    if (file.state === 'ACTIVE') return file;
    if (file.state === 'FAILED') throw new Error('Gemini file processing failed');
    await sleep(2000);
  }
  throw new Error('Timeout waiting for Gemini file processing');
}

async function deleteGeminiFile(fileName) {
  try {
    await fetch(`https://generativelanguage.googleapis.com/v1beta/${fileName}?key=${GEMINI_KEY}`, { method: 'DELETE' });
  } catch (e) { /* ignore */ }
}

// ═══════════════════════════════════════════════════════════════
// F0: REFRESH STATS (yt-dlp metadata fetch)
// ═══════════════════════════════════════════════════════════════
function fetchStatsYtDlp(url) {
  try {
    const raw = execSync(
      `yt-dlp --skip-download --no-warnings --print "%(view_count)s|%(like_count)s|%(comment_count)s|%(duration)s|%(upload_date)s|%(description)s|%(title)s" "${url}"`,
      { encoding: 'utf8', timeout: 60000, env: EXEC_ENV, maxBuffer: 5 * 1024 * 1024 }
    ).trim();

    // Take only the first line (some URLs might output multiple lines)
    const line = raw.split('\n')[0];
    const parts = line.split('|');

    // Parse values, treating 'NA' as null
    const parseNum = (s) => { const n = parseInt(s); return (isNaN(n) || s === 'NA') ? null : n; };
    const parseStr = (s) => (!s || s === 'NA') ? null : s.trim();

    return {
      visitas: parseNum(parts[0]),
      likes: parseNum(parts[1]),
      comentarios: parseNum(parts[2]),
      duracion: parseStr(parts[3]),
      fecha_publicacion: parseStr(parts[4]),
      descripcion: parseStr(parts[5]),
      title: parseStr(parts[6]),
    };
  } catch (err) {
    return null;
  }
}

async function phaseRefreshStats(row, db) {
  try {
    const url = row.url;
    if (!url) return { success: false, detail: 'No URL' };

    // Detect platform
    const isInstagram = url.includes('instagram.com');
    const isTikTok = url.includes('tiktok.com');

    if (!isInstagram && !isTikTok) {
      return { success: false, detail: `URL no reconocida: ${url.substring(0, 40)}` };
    }

    // Fetch stats via yt-dlp (works for both TikTok and Instagram)
    const stats = fetchStatsYtDlp(url);
    if (!stats) {
      return { success: false, detail: 'yt-dlp no pudo obtener metadatos' };
    }

    // Build update query — only update non-null values
    const updates = [];
    const values = [];

    if (stats.visitas != null) { updates.push('visitas = ?'); values.push(stats.visitas); }
    if (stats.likes != null) { updates.push('likes = ?'); values.push(stats.likes); }
    if (stats.comentarios != null) { updates.push('comentarios = ?'); values.push(stats.comentarios); }
    if (stats.duracion != null) { updates.push('duracion = ?'); values.push(stats.duracion); }
    if (stats.fecha_publicacion != null) {
      // Format date: 20240315 → 2024-03-15
      let d = stats.fecha_publicacion;
      if (/^\d{8}$/.test(d)) d = `${d.slice(0,4)}-${d.slice(4,6)}-${d.slice(6,8)}`;
      updates.push('fecha_publicacion = ?');
      values.push(d);
    }
    if (stats.descripcion != null && (isEmpty(row.descripcion) || stats.descripcion.length > 0)) {
      updates.push('descripcion = ?');
      values.push(stats.descripcion);
    }

    if (updates.length === 0) {
      return { success: true, detail: 'Sin datos nuevos de yt-dlp' };
    }

    // Build PostgreSQL parameterized update
    const pgSets = [];
    const pgVals = [];
    let idx = 1;
    for (let v = 0; v < values.length; v++) {
      const col = updates[v].split(' = ')[0];
      pgSets.push(`${col} = $${idx++}`);
      pgVals.push(values[v]);
    }
    pgSets.push('updated_at = NOW()');
    pgVals.push(row.id);
    await db.query(`UPDATE contenido SET ${pgSets.join(', ')} WHERE id = $${idx}`, pgVals);

    // Update in-memory row
    if (stats.visitas != null) row.visitas = stats.visitas;
    if (stats.likes != null) row.likes = stats.likes;
    if (stats.comentarios != null) row.comentarios = stats.comentarios;

    const detail = [];
    if (stats.visitas != null) detail.push(`views=${fmtK2(stats.visitas)}`);
    if (stats.likes != null) detail.push(`likes=${fmtK2(stats.likes)}`);
    if (stats.comentarios != null) detail.push(`cmts=${fmtK2(stats.comentarios)}`);
    return { success: true, detail: detail.join(' ') || 'Updated' };

  } catch (err) {
    return { success: false, detail: err.message };
  }
}

function fmtK2(n) { if (n >= 1e6) return (n/1e6).toFixed(1)+'M'; if (n >= 1e3) return (n/1e3).toFixed(1)+'K'; return n.toString(); }

// ═══════════════════════════════════════════════════════════════
// STORYBOARD HELPERS (from process_escenas_v2.js)
// ═══════════════════════════════════════════════════════════════
const STORYBOARDS_DIR = process.env.STORYBOARDS_DIR || path.join(__dirname, 'escenas_storyboards');
if (!fs.existsSync(STORYBOARDS_DIR)) fs.mkdirSync(STORYBOARDS_DIR, { recursive: true });

const PROMPT_ESCENA = `Analiza estos fotogramas de una escena de video. Son frames consecutivos de la misma escena.

Devuelve JSON con estos campos exactos:
{
  "escenario": "descripción del lugar, iluminación, colores, atmósfera",
  "personajes": "quién aparece, vestimenta, postura, expresión",
  "objetivo_visual": "qué busca provocar esta escena (captar atención, generar confianza, etc.)",
  "edicion_visual": "textos en pantalla, iconos, emojis, efectos, gráficos, animaciones visibles",
  "camara_edicion": "tipo de plano, ángulo, movimiento de cámara"
}

Solo el JSON, nada más.`;

function extractSceneFrames(videoPath, videoId, escenaNum, inicio, fin) {
  const escenaDir = path.join(CONFIG.framesDir, `video_${videoId}`, `escena_${escenaNum}`);
  if (!fs.existsSync(escenaDir)) fs.mkdirSync(escenaDir, { recursive: true });

  const duracion = fin - inicio;
  const numFrames = Math.min(6, Math.ceil(duracion));
  const interval = duracion / numFrames;

  const frames = [];
  for (let i = 0; i < numFrames; i++) {
    const timestamp = inicio + (i * interval);
    const framePath = path.join(escenaDir, `frame_${String(i + 1).padStart(3, '0')}.jpg`);

    try {
      execSync(`ffmpeg -ss ${timestamp} -i "${videoPath}" -vframes 1 -q:v 2 "${framePath}" -y 2>/dev/null`, {
        timeout: 30000, env: EXEC_ENV,
      });
      if (fs.existsSync(framePath)) frames.push(framePath);
    } catch (err) { /* ignore */ }
  }

  return frames;
}

function createStoryboard(frames, videoId, escenaNum) {
  if (frames.length === 0) return null;

  const storyboardDir = path.join(STORYBOARDS_DIR, `video_${videoId}`);
  if (!fs.existsSync(storyboardDir)) fs.mkdirSync(storyboardDir, { recursive: true });

  const storyboardPath = path.join(storyboardDir, `escena_${escenaNum}.jpg`);

  try {
    if (frames.length === 1) {
      fs.copyFileSync(frames[0], storyboardPath);
      return storyboardPath;
    }

    const cols = Math.min(3, frames.length);
    const rows = Math.ceil(frames.length / cols);

    const rowFiles = [];
    for (let r = 0; r < rows; r++) {
      const rowFrames = frames.slice(r * cols, (r + 1) * cols);
      while (rowFrames.length < cols && rowFrames.length > 0) {
        rowFrames.push(rowFrames[rowFrames.length - 1]);
      }

      const rowPath = `/tmp/row_${videoId}_${escenaNum}_${r}.jpg`;
      const inputs = rowFrames.map(f => `-i "${f}"`).join(' ');
      execSync(`ffmpeg ${inputs} -filter_complex "hstack=inputs=${rowFrames.length}" -q:v 2 "${rowPath}" -y 2>/dev/null`, {
        timeout: 30000, env: EXEC_ENV,
      });
      rowFiles.push(rowPath);
    }

    if (rowFiles.length === 1) {
      fs.copyFileSync(rowFiles[0], storyboardPath);
    } else {
      const inputs = rowFiles.map(f => `-i "${f}"`).join(' ');
      execSync(`ffmpeg ${inputs} -filter_complex "vstack=inputs=${rowFiles.length}" -q:v 2 "${storyboardPath}" -y 2>/dev/null`, {
        timeout: 30000, env: EXEC_ENV,
      });
    }

    rowFiles.forEach(f => { try { fs.unlinkSync(f); } catch(e) {} });

    if (fs.existsSync(storyboardPath)) return storyboardPath;
  } catch (err) {
    if (frames.length > 0) {
      fs.copyFileSync(frames[0], storyboardPath);
      return storyboardPath;
    }
  }

  return null;
}

async function callGeminiWithImage(imagePath, prompt, maxRetries = 5) {
  const imageData = fs.readFileSync(imagePath);
  const base64 = imageData.toString('base64');

  for (let attempt = 0; attempt < maxRetries; attempt++) {
    try {
      const response = await fetch(
        `https://generativelanguage.googleapis.com/v1beta/models/${CONFIG.geminiModel}:generateContent?key=${GEMINI_KEY}`,
        {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            contents: [{
              parts: [
                { inline_data: { mime_type: 'image/jpeg', data: base64 } },
                { text: prompt },
              ],
            }],
            generationConfig: {
              temperature: 0.2,
              maxOutputTokens: 2000,
              responseMimeType: 'application/json',
            },
          }),
        }
      );

      if (response.ok) {
        const data = await response.json();
        const text = data.candidates?.[0]?.content?.parts?.[0]?.text || '{}';
        try {
          return JSON.parse(text);
        } catch (e) {
          const match = text.match(/\{[\s\S]*\}/);
          if (match) return JSON.parse(match[0]);
          return {};
        }
      }

      if ([429, 500, 503].includes(response.status)) {
        const wait = Math.min(30000, 2000 * Math.pow(2, attempt));
        console.log(`      ⚠️ Gemini ${response.status}, retry ${attempt + 1}/${maxRetries}...`);
        await sleep(wait);
        continue;
      }

      const errText = await response.text();
      throw new Error(`Gemini API error: ${response.status} - ${errText.substring(0, 200)}`);
    } catch (err) {
      if (err.message?.includes('Gemini API error')) throw err;
      if (attempt === maxRetries - 1) throw err;
      await sleep(Math.min(30000, 2000 * Math.pow(2, attempt)));
    }
  }
  return {};
}

// ═══════════════════════════════════════════════════════════════
// PARSING HELPERS (from extract_escenas_from_visual.js)
// ═══════════════════════════════════════════════════════════════
function parseTimestamp(ts) {
  const match = ts.match(/(\d+):(\d+(?:\.\d+)?)/);
  if (!match) return 0;
  return parseInt(match[1]) * 60 + parseFloat(match[2]);
}

function parseVisualToEscenas(visual) {
  if (!visual) return [];
  const escenas = [];
  const regex = /\[(\d+:\d+(?:\.\d+)?)\s*[–-]\s*(\d+:\d+(?:\.\d+)?)\]\s*([^\n]+)/g;
  let match;
  const matches = [];

  while ((match = regex.exec(visual)) !== null) {
    matches.push({
      inicio: parseTimestamp(match[1]),
      fin: parseTimestamp(match[2]),
      titulo: match[3].trim(),
      startIndex: match.index,
      endIndex: regex.lastIndex,
    });
  }

  for (let i = 0; i < matches.length; i++) {
    const m = matches[i];
    const nextStart = matches[i + 1]?.startIndex || visual.length;
    const contenido = visual.slice(m.endIndex, nextStart).trim();

    const escenario = contenido.match(/-\s*Escenario:\s*([\s\S]*?)(?=-\s*\w|$)/i)?.[1]?.trim() || '';
    const personajes = contenido.match(/-\s*Personajes:\s*([\s\S]*?)(?=-\s*\w|$)/i)?.[1]?.trim() || '';
    const objetivo = contenido.match(/-\s*Objetivo\s*visual:\s*([\s\S]*?)(?=-\s*\w|$)/i)?.[1]?.trim() || '';
    const texto = contenido.match(/-\s*Texto\s*en\s*pantalla:\s*([\s\S]*?)(?=-\s*\w|$)/i)?.[1]?.trim() || '';
    const camara = contenido.match(/-\s*Cámara\s*y\s*edición:\s*([\s\S]*?)(?=-\s*\w|$)/i)?.[1]?.trim() || '';

    escenas.push({
      inicio: m.inicio,
      fin: m.fin,
      titulo: m.titulo,
      escenario,
      personajes,
      objetivo_visual: objetivo,
      edicion_visual: texto,
      camara_edicion: camara,
    });
  }
  return escenas;
}

// ═══════════════════════════════════════════════════════════════
// EMBEDDING TEXT BUILDER (from generate_contenido_embeddings.js)
// ═══════════════════════════════════════════════════════════════
function buildEmbeddingText(row) {
  const parts = [];
  if (row.transcripcion) parts.push(`TRANSCRIPCIÓN: ${row.transcripcion.substring(0, 3000)}`);
  if (row.descripcion) parts.push(`DESCRIPCIÓN: ${row.descripcion}`);
  if (row.visual) parts.push(`ANÁLISIS VISUAL: ${row.visual.substring(0, 1500)}`);
  if (row.hook) parts.push(`HOOK: ${row.hook}`);
  if (row.tematica) parts.push(`TEMÁTICA: ${row.tematica}`);
  if (row.semantica_ruta) parts.push(`RUTA: ${row.semantica_ruta}`);
  if (row.semantica_cluster) parts.push(`CLUSTER: ${row.semantica_cluster}`);
  if (row.visual_ruta) parts.push(`VISUAL RUTA: ${row.visual_ruta}`);
  if (row.visual_cluster) parts.push(`VISUAL CLUSTER: ${row.visual_cluster}`);
  return parts.join('\n\n').substring(0, 8000);
}

// ═══════════════════════════════════════════════════════════════
// CONTEXT INJECTION
// ═══════════════════════════════════════════════════════════════
function injectContext(prompt, context) {
  if (!context) return prompt;
  return prompt + `\n\n═══════════════════════════════════════════\nCONTEXTO ADICIONAL DEL CREADOR:\n═══════════════════════════════════════════\n${context}\n`;
}

// ═══════════════════════════════════════════════════════════════
// PHASE DETECTION
// ═══════════════════════════════════════════════════════════════
function isEmpty(val) {
  return val === null || val === undefined || val === '';
}

function detectPhases(row, escenaCount) {
  const needed = [];

  // F0: Refresh stats (si faltan visitas, likes, comentarios, duracion, fecha, descripcion)
  if (isEmpty(row.visitas) || row.visitas === 0 ||
      isEmpty(row.likes) || row.likes === 0 ||
      isEmpty(row.comentarios) || row.comentarios === 0 ||
      isEmpty(row.duracion) || isEmpty(row.fecha_publicacion) || isEmpty(row.descripcion)) {
    needed.push('refresh_stats');
  }

  // F1: Transcripción + Visual (necesita descargar video)
  if (isEmpty(row.transcripcion) || isEmpty(row.visual)) {
    needed.push('transcripcion');
  }

  // F2: Semántica (necesita transcripcion)
  if (!isEmpty(row.transcripcion) && (
    isEmpty(row.hook) || isEmpty(row.tematica) || isEmpty(row.formula_hook) ||
    isEmpty(row.semantica_inicio) || isEmpty(row.semantica_ruta) || isEmpty(row.semantica_cluster)
  )) {
    needed.push('semantica');
  }

  // F3: Visual estructura (necesita visual)
  if (!isEmpty(row.visual) && (
    isEmpty(row.visual_inicio) || isEmpty(row.visual_ruta) || isEmpty(row.visual_cluster)
  )) {
    needed.push('visual_analysis');
  }

  // F4: Escenas (necesita visual + no tener escenas)
  if (!isEmpty(row.visual) && isEmpty(row.escenas_cortes) && escenaCount === 0) {
    needed.push('escenas');
  }

  // F4.5: Analyze escenas (storyboard → Gemini image analysis)
  // Needs escenas to exist with empty analysis fields
  // (escenaCount > 0 is checked, or escenas phase will create them)

  // F5: Frames (se evalúa por separado tras F4)
  // Solo si ya hay escenas sin frames

  // F6: Embedding contenido
  if (isEmpty(row.embedding) && (!isEmpty(row.transcripcion) || !isEmpty(row.visual))) {
    needed.push('embeddings');
  }

  return needed;
}

function filterPhases(detected, only, skip) {
  let phases = detected;
  if (only && only.length > 0) {
    phases = detected.filter(p => only.includes(p));
  }
  if (skip && skip.length > 0) {
    phases = phases.filter(p => !skip.includes(p));
  }
  return phases;
}

// ═══════════════════════════════════════════════════════════════
// PHASE EXECUTION
// ═══════════════════════════════════════════════════════════════

// F1: Transcripción + Visual via video upload
async function phaseTranscripcion(row, db, context) {
  const videoPath = path.join(CONFIG.tempDir, `video_${row.id}.mp4`);
  let geminiFile = null;

  try {
    console.log(`    📥 Descargando video...`);
    if (!downloadVideo(row.url, videoPath)) {
      throw new Error('No se pudo descargar el video');
    }

    console.log(`    📤 Subiendo a Gemini (${(fs.statSync(videoPath).size / 1024 / 1024).toFixed(1)} MB)...`);
    geminiFile = await uploadToGemini(videoPath);

    console.log(`    ⏳ Esperando procesamiento...`);
    const processed = await waitForGeminiFile(geminiFile.name);

    const results = {};

    if (isEmpty(row.transcripcion)) {
      console.log(`    🎤 Generando transcripción...`);
      const promptTrans = injectContext(PROMPT_VIDEO_TRANS, context);
      results.transcripcion = await callGeminiWithVideo(processed.uri, processed.mimeType, promptTrans);
    }

    if (isEmpty(row.visual)) {
      console.log(`    🎬 Generando análisis visual...`);
      const promptVisual = injectContext(PROMPT_VIDEO_VISUAL, context);
      results.visual = await callGeminiWithVideo(processed.uri, processed.mimeType, promptVisual);
    }

    // Update DB
    if (results.transcripcion || results.visual) {
      const pgSets = [];
      const pgVals = [];
      let idx = 1;
      if (results.transcripcion) { pgSets.push(`transcripcion = $${idx++}`); pgVals.push(results.transcripcion); }
      if (results.visual) { pgSets.push(`visual = $${idx++}`); pgVals.push(results.visual); }
      pgSets.push('updated_at = NOW()');
      pgVals.push(row.id);
      await db.query(`UPDATE contenido SET ${pgSets.join(', ')} WHERE id = $${idx}`, pgVals);

      // Update in-memory row
      if (results.transcripcion) row.transcripcion = results.transcripcion;
      if (results.visual) row.visual = results.visual;
    }

    const tLen = results.transcripcion ? results.transcripcion.length : 0;
    const vLen = results.visual ? results.visual.length : 0;
    return { success: true, detail: `trans=${tLen}c vis=${vLen}c` };

  } catch (err) {
    return { success: false, detail: err.message };
  } finally {
    if (fs.existsSync(videoPath)) try { fs.unlinkSync(videoPath); } catch (e) {}
    if (geminiFile) await deleteGeminiFile(geminiFile.name);
  }
}

// F2: Análisis Semántico
async function phaseSemantica(row, db, context) {
  try {
    const prompt = injectContext(PROMPT_SEMANTICA, context)
      .replace('{{TRANSCRIPCION}}', row.transcripcion || '');

    const result = await callGeminiText(prompt);

    await db.query(`
      UPDATE contenido SET
        hook = $1, tematica = $2, formula_hook = $3,
        semantica_inicio = $4, semantica_ruta = $5, semantica_cluster = $6,
        updated_at = NOW()
      WHERE id = $7
    `, [
      result.Hook || '', result.Tematica || '', result.Formula_Hook || '',
      result.Semantica_Inicio || '', result.Semantica_Ruta || '', result.Semantica_Cluster || '',
      row.id
    ]);

    // Update in-memory
    row.hook = result.Hook || '';
    row.tematica = result.Tematica || '';
    row.formula_hook = result.Formula_Hook || '';
    row.semantica_inicio = result.Semantica_Inicio || '';
    row.semantica_ruta = result.Semantica_Ruta || '';
    row.semantica_cluster = result.Semantica_Cluster || '';

    return { success: true, detail: `hook="${(result.Hook || '').substring(0, 40)}" tema="${result.Tematica || ''}"` };
  } catch (err) {
    return { success: false, detail: err.message };
  }
}

// F3: Visual Structure Analysis
async function phaseVisualAnalysis(row, db, context) {
  try {
    const prompt = injectContext(PROMPT_VISUAL, context)
      .replace('{{VISUAL}}', (row.visual || '').substring(0, 15000));

    const result = await callGeminiText(prompt);

    await db.query(`
      UPDATE contenido SET
        visual_inicio = $1, visual_ruta = $2, visual_cluster = $3,
        updated_at = NOW()
      WHERE id = $4
    `, [
      result.Visual_Inicio || result.visual_inicio || '',
      result.Visual_Ruta || result.visual_ruta || '',
      result.Visual_Cluster || result.visual_cluster || '',
      row.id
    ]);

    const vi = result.Visual_Inicio || result.visual_inicio || '';
    row.visual_inicio = vi;
    row.visual_ruta = result.Visual_Ruta || result.visual_ruta || '';
    row.visual_cluster = result.Visual_Cluster || result.visual_cluster || '';

    return { success: true, detail: `visual_inicio="${vi.substring(0, 40)}"` };
  } catch (err) {
    return { success: false, detail: err.message };
  }
}

// F4: Escenas extraction (parsing only, no AI)
async function phaseEscenas(row, db) {
  try {
    const escenas = parseVisualToEscenas(row.visual);
    if (escenas.length === 0) {
      return { success: false, detail: 'No escenas found in visual field' };
    }

    // Save escenas_cortes
    const cortesJson = JSON.stringify(escenas.map(e => ({ inicio: e.inicio, fin: e.fin })));
    await db.query('UPDATE contenido SET escenas_cortes = $1 WHERE id = $2', [cortesJson, row.id]);
    row.escenas_cortes = cortesJson;

    // Insert escenas
    for (let i = 0; i < escenas.length; i++) {
      const e = escenas[i];
      const dur = e.fin - e.inicio;
      const desc = `[${e.inicio.toFixed(1)}s - ${e.fin.toFixed(1)}s] ${e.titulo}\nEscenario: ${e.escenario}\nPersonajes: ${e.personajes}\nObjetivo: ${e.objetivo_visual}\nEdición: ${e.edicion_visual}\nCámara: ${e.camara_edicion}`;

      try {
        await db.query(`
          INSERT INTO escenas (video_id, video_url, escena_numero, tiempo_inicio, tiempo_fin, duracion_seg,
            fotogramas, escenario, personajes, objetivo_visual, edicion_visual, camara_edicion, descripcion_completa)
          VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
        `, [row.id, row.url, i + 1, e.inicio, e.fin, dur, '[]',
          e.escenario, e.personajes, e.objetivo_visual, e.edicion_visual, e.camara_edicion, desc]);
      } catch (insertErr) {
        // Skip duplicates
      }
    }

    return { success: true, detail: `${escenas.length} escenas` };
  } catch (err) {
    return { success: false, detail: err.message };
  }
}

// F4.5: Analyze escenas with storyboard + Gemini image
async function phaseAnalyzeEscenas(row, db) {
  const videoPath = path.join(CONFIG.tempDir, `video_${row.id}.mp4`);

  try {
    // Get escenas that need analysis (escenario is empty or null)
    const escenasRes = await db.query(`
      SELECT id, escena_numero, tiempo_inicio, tiempo_fin
      FROM escenas
      WHERE video_id = $1
        AND (escenario IS NULL OR escenario = '')
      ORDER BY escena_numero
    `, [row.id]);
    const escenas = escenasRes.rows;

    if (escenas.length === 0) return { success: true, detail: 'All escenas already analyzed' };

    // Download video
    console.log(`\n    📥 Descargando video para storyboards...`);
    if (!downloadVideo(row.url, videoPath)) {
      throw new Error('No se pudo descargar el video');
    }

    let analyzed = 0;

    for (const esc of escenas) {
      process.stdout.write(`    🎬 Escena ${esc.escena_numero}... `);

      // Extract frames
      const frames = extractSceneFrames(videoPath, row.id, esc.escena_numero, esc.tiempo_inicio, esc.tiempo_fin);
      if (frames.length === 0) {
        console.log('⚠️ sin frames');
        continue;
      }

      // Create storyboard
      const storyboard = createStoryboard(frames, row.id, esc.escena_numero);
      if (!storyboard) {
        console.log('⚠️ no storyboard');
        continue;
      }

      // Analyze with Gemini
      const analysis = await callGeminiWithImage(storyboard, PROMPT_ESCENA);

      // Build descripcion_completa
      const desc = `[${esc.tiempo_inicio.toFixed(1)}s - ${esc.tiempo_fin.toFixed(1)}s]\n` +
        `Escenario: ${analysis.escenario || ''}\n` +
        `Personajes: ${analysis.personajes || ''}\n` +
        `Objetivo: ${analysis.objetivo_visual || ''}\n` +
        `Edición: ${analysis.edicion_visual || ''}\n` +
        `Cámara: ${analysis.camara_edicion || ''}`;

      // Update DB
      await db.query(`
        UPDATE escenas SET
          escenario = $1,
          personajes = $2,
          objetivo_visual = $3,
          edicion_visual = $4,
          camara_edicion = $5,
          fotogramas = $6,
          descripcion_completa = $7
        WHERE id = $8
      `, [
        analysis.escenario || '',
        analysis.personajes || '',
        analysis.objetivo_visual || '',
        analysis.edicion_visual || '',
        analysis.camara_edicion || '',
        JSON.stringify(frames),
        desc,
        esc.id
      ]);

      analyzed++;
      console.log('✅');
      await sleep(300);
    }

    return { success: true, detail: `${analyzed}/${escenas.length} escenas analizadas` };

  } catch (err) {
    return { success: false, detail: err.message };
  } finally {
    if (fs.existsSync(videoPath)) try { fs.unlinkSync(videoPath); } catch (e) {}
  }
}

// F5: Frame extraction
async function phaseFrames(row, db) {
  const videoPath = path.join(CONFIG.tempDir, `video_${row.id}.mp4`);

  try {
    // Get escenas for this video that need frames
    const escenasRes = await db.query(`
      SELECT id, escena_numero, tiempo_inicio, tiempo_fin, duracion_seg, fotogramas
      FROM escenas WHERE video_id = $1 ORDER BY escena_numero
    `, [row.id]);
    const escenas = escenasRes.rows;

    const needFrames = escenas.filter(e => {
      try { return !e.fotogramas || e.fotogramas === '[]' || JSON.parse(e.fotogramas).length === 0; }
      catch { return true; }
    });

    if (needFrames.length === 0) return { success: true, detail: 'All frames exist' };

    // Download video
    console.log(`    📥 Descargando video para frames...`);
    if (!downloadVideo(row.url, videoPath)) {
      throw new Error('No se pudo descargar el video');
    }

    let totalFrames = 0;

    for (const esc of needFrames) {
      const escenaDir = path.join(CONFIG.framesDir, `video_${row.id}`, `escena_${esc.escena_numero}`);
      if (!fs.existsSync(escenaDir)) fs.mkdirSync(escenaDir, { recursive: true });

      const frames = [];
      const dur = Math.max(esc.duracion_seg || (esc.tiempo_fin - esc.tiempo_inicio), 0.5);
      const numFrames = Math.min(6, Math.ceil(dur));

      for (let i = 0; i < numFrames; i++) {
        const ts = esc.tiempo_inicio + (i * dur / numFrames);
        const fp = path.join(escenaDir, `frame_${i}.jpg`);
        try {
          execSync(`ffmpeg -ss ${ts} -i "${videoPath}" -vframes 1 -q:v 2 "${fp}" -y 2>/dev/null`, { timeout: 30000, env: EXEC_ENV });
          if (fs.existsSync(fp)) frames.push(fp);
        } catch (e) {}
      }

      if (frames.length > 0) {
        await db.query('UPDATE escenas SET fotogramas = $1 WHERE id = $2', [JSON.stringify(frames), esc.id]);
        totalFrames += frames.length;
      }
    }

    return { success: true, detail: `${totalFrames} frames en ${needFrames.length} escenas` };

  } catch (err) {
    return { success: false, detail: err.message };
  } finally {
    if (fs.existsSync(videoPath)) try { fs.unlinkSync(videoPath); } catch (e) {}
  }
}

// F6: Embeddings contenido
async function phaseEmbeddings(row, db) {
  try {
    const texto = buildEmbeddingText(row);
    if (texto.length < 50) return { success: false, detail: 'Text too short for embedding' };

    const embedding = await generateEmbedding(texto);
    if (!embedding) return { success: false, detail: 'Embedding generation failed' };

    await db.query('UPDATE contenido SET embedding = $1 WHERE id = $2', [JSON.stringify(embedding), row.id]);
    row.embedding = 'set';

    return { success: true, detail: `${embedding.length} dims` };
  } catch (err) {
    return { success: false, detail: err.message };
  }
}

// F7: Embeddings escenas
async function phaseEmbEscenas(row, db) {
  try {
    const escenasRes = await db.query(`
      SELECT id, escenario, personajes, objetivo_visual, edicion_visual, camara_edicion
      FROM escenas WHERE video_id = $1 AND (embedding IS NULL OR embedding = '')
    `, [row.id]);
    const escenas = escenasRes.rows;

    if (escenas.length === 0) return { success: true, detail: 'No escenas need embeddings' };

    let done = 0;
    for (const esc of escenas) {
      const texto = [esc.escenario, esc.personajes, esc.objetivo_visual, esc.edicion_visual, esc.camara_edicion]
        .filter(Boolean).join('\n');
      if (texto.length < 20) continue;

      const emb = await generateEmbedding(texto);
      if (emb) {
        await db.query('UPDATE escenas SET embedding = $1 WHERE id = $2', [JSON.stringify(emb), esc.id]);
        done++;
      }
      await sleep(100);
    }

    return { success: true, detail: `${done}/${escenas.length} escenas` };
  } catch (err) {
    return { success: false, detail: err.message };
  }
}

// ═══════════════════════════════════════════════════════════════
// UTILS
// ═══════════════════════════════════════════════════════════════
function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

function formatDuration(ms) {
  const s = Math.floor(ms / 1000);
  if (s < 60) return `${s}s`;
  const m = Math.floor(s / 60);
  return `${m}m ${s % 60}s`;
}

// ═══════════════════════════════════════════════════════════════
// MAIN
// ═══════════════════════════════════════════════════════════════
async function main() {
  const opts = parseArgs();

  if (opts.help) { showHelp(); return; }
  if (!opts.creador && !opts.ids && !opts.where) {
    console.log('Error: Necesitas --creador, --ids o --where');
    showHelp();
    process.exit(1);
  }

  // Load context
  let context = opts.context || null;
  if (opts.contextFile) {
    const cfPath = path.resolve(opts.contextFile);
    if (fs.existsSync(cfPath)) {
      context = fs.readFileSync(cfPath, 'utf8');
    } else {
      console.error(`Error: Archivo de contexto no encontrado: ${cfPath}`);
      process.exit(1);
    }
  }

  // Header
  console.log('\n═══════════════════════════════════════════════════════════════');
  console.log('  MoneyBall Pipeline Orchestrator');
  console.log('═══════════════════════════════════════════════════════════════');
  if (opts.creador) console.log(`  Creador: ${opts.creador}`);
  if (opts.ids) console.log(`  IDs: ${opts.ids.join(', ')}`);
  if (opts.where) console.log(`  Where: ${opts.where}`);
  if (opts.only) console.log(`  Solo fases: ${opts.only.join(', ')}`);
  if (opts.skip.length) console.log(`  Skip fases: ${opts.skip.join(', ')}`);
  if (opts.limit) console.log(`  Límite: ${opts.limit}`);
  if (context) console.log(`  Contexto: ${context.length} chars`);
  if (opts.forceStats) console.log(`  ⚡ Force stats refresh`);
  if (opts.dryRun) console.log(`  *** DRY RUN ***`);
  console.log('═══════════════════════════════════════════════════════════════\n');

  // Connect to PostgreSQL
  const dbUrl = process.env.DATABASE_URL;
  if (!dbUrl) {
    console.error('ERROR: Set DATABASE_URL env var');
    process.exit(1);
  }
  const db = new Pool({ connectionString: dbUrl, max: 5 });

  try {
    // Build WHERE clause with $1, $2... placeholders
    let where = '1=1';
    const params = [];
    let pIdx = 1;

    if (opts.creador) {
      where += ` AND creador = $${pIdx++}`;
      params.push(opts.creador);
    }
    if (opts.ids) {
      const idPlaceholders = opts.ids.map(() => `$${pIdx++}`).join(',');
      where += ` AND id IN (${idPlaceholders})`;
      params.push(...opts.ids);
    }
    if (opts.where) {
      where += ` AND (${opts.where})`;
    }
    if (opts.mod !== null && opts.part !== null) {
      where += ` AND id % ${opts.mod} = ${opts.part}`;
    }

    let query = `
      SELECT id, creador, url, visitas, likes, comentarios, duracion,
             fecha_publicacion, descripcion, transcripcion, visual,
             hook, tematica, formula_hook,
             semantica_inicio, semantica_ruta, semantica_cluster,
             visual_inicio, visual_ruta, visual_cluster,
             escenas_cortes, embedding
      FROM contenido
      WHERE ${where}
      ORDER BY id
    `;
    if (opts.limit) query += ` LIMIT ${opts.limit}`;

    const result = await db.query(query, params);
    const rows = result.rows;
    console.log(`📊 ${rows.length} filas seleccionadas\n`);

    if (rows.length === 0) {
      console.log('No hay filas que procesar con ese filtro.');
      return;
    }

    // Stats tracking
    const stats = {};
    ALL_PHASES.forEach(p => { stats[p] = { executed: 0, errors: 0 }; });
    const startTime = Date.now();

    // Process each row
    for (let i = 0; i < rows.length; i++) {
      const row = rows[i];

      // Count existing escenas for this video
      const escenaCountRes = await db.query('SELECT COUNT(*) as cnt FROM escenas WHERE video_id = $1', [row.id]);
      const escenaCount = parseInt(escenaCountRes.rows[0]?.cnt) || 0;

      // Detect needed phases
      const detected = detectPhases(row, escenaCount);
      const phases = filterPhases(detected, opts.only, opts.skip);

      // Force refresh_stats even if fields already have data
      if (opts.forceStats && !phases.includes('refresh_stats') && !opts.skip.includes('refresh_stats')) {
        if (!opts.only || opts.only.includes('refresh_stats')) {
          phases.unshift('refresh_stats');
        }
      }

      // Check if analyze_escenas phase should be added (storyboard + Gemini image)
      if (!opts.skip.includes('analyze_escenas') && (!opts.only || opts.only.includes('analyze_escenas'))) {
        if (escenaCount > 0 || phases.includes('escenas')) {
          const naRes = await db.query(`
            SELECT COUNT(*) as cnt FROM escenas
            WHERE video_id = $1 AND (escenario IS NULL OR escenario = '')
          `, [row.id]);
          const needAnalysis = parseInt(naRes.rows[0]?.cnt) || 0;
          if (needAnalysis > 0 && !phases.includes('analyze_escenas')) {
            phases.push('analyze_escenas');
          }
        }
      }

      // Check if frames phase should be added
      if (!opts.skip.includes('frames') && (!opts.only || opts.only.includes('frames'))) {
        if (escenaCount > 0 || phases.includes('escenas')) {
          const nfRes = await db.query(`
            SELECT COUNT(*) as cnt FROM escenas
            WHERE video_id = $1 AND (fotogramas IS NULL OR fotogramas = '' OR fotogramas = '[]')
          `, [row.id]);
          const needFrames = parseInt(nfRes.rows[0]?.cnt) || 0;
          if (needFrames > 0 && !phases.includes('frames')) {
            phases.push('frames');
          }
        }
      }

      // Check for emb_escenas
      if (!opts.skip.includes('emb_escenas') && (!opts.only || opts.only.includes('emb_escenas'))) {
        if (escenaCount > 0 || phases.includes('escenas')) {
          const neRes = await db.query(`
            SELECT COUNT(*) as cnt FROM escenas
            WHERE video_id = $1 AND (embedding IS NULL OR embedding = '')
          `, [row.id]);
          const needEmb = parseInt(neRes.rows[0]?.cnt) || 0;
          if (needEmb > 0 && !phases.includes('emb_escenas')) {
            phases.push('emb_escenas');
          }
        }
      }

      // Log
      const emptyFields = [];
      if (isEmpty(row.visitas) || row.visitas === 0) emptyFields.push('visitas');
      if (isEmpty(row.likes) || row.likes === 0) emptyFields.push('likes');
      if (isEmpty(row.comentarios) || row.comentarios === 0) emptyFields.push('comentarios');
      if (isEmpty(row.duracion)) emptyFields.push('duracion');
      if (isEmpty(row.fecha_publicacion)) emptyFields.push('fecha');
      if (isEmpty(row.descripcion)) emptyFields.push('descripcion');
      if (isEmpty(row.transcripcion)) emptyFields.push('transcripcion');
      if (isEmpty(row.visual)) emptyFields.push('visual');
      if (isEmpty(row.hook)) emptyFields.push('hook');
      if (isEmpty(row.tematica)) emptyFields.push('tematica');
      if (isEmpty(row.formula_hook)) emptyFields.push('formula_hook');
      if (isEmpty(row.semantica_inicio)) emptyFields.push('sem_inicio');
      if (isEmpty(row.semantica_ruta)) emptyFields.push('sem_ruta');
      if (isEmpty(row.semantica_cluster)) emptyFields.push('sem_cluster');
      if (isEmpty(row.visual_inicio)) emptyFields.push('vis_inicio');
      if (isEmpty(row.visual_ruta)) emptyFields.push('vis_ruta');
      if (isEmpty(row.visual_cluster)) emptyFields.push('vis_cluster');
      if (isEmpty(row.escenas_cortes)) emptyFields.push('escenas_cortes');
      if (isEmpty(row.embedding)) emptyFields.push('embedding');

      console.log(`[${i + 1}/${rows.length}] ID ${row.id} (${row.creador})`);
      if (emptyFields.length > 0) {
        console.log(`  Vacías: ${emptyFields.join(', ')}`);
      } else {
        console.log(`  ✅ Todas las columnas rellenas`);
      }

      if (phases.length === 0) {
        console.log(`  → Nada que hacer\n`);
        continue;
      }

      // Sort phases by dependency order (ALL_PHASES defines the order)
      phases.sort((a, b) => ALL_PHASES.indexOf(a) - ALL_PHASES.indexOf(b));

      console.log(`  Fases: ${phases.join(', ')}`);

      if (opts.dryRun) {
        console.log(`  → [DRY RUN] Se ejecutarían: ${phases.join(', ')}\n`);
        phases.forEach(p => stats[p].executed++);
        continue;
      }

      // Execute phases in dependency order
      for (const phase of phases) {
        let result;
        process.stdout.write(`  ▶ ${phase}... `);

        switch (phase) {
          case 'refresh_stats':
            result = await phaseRefreshStats(row, db);
            break;
          case 'transcripcion':
            result = await phaseTranscripcion(row, db, context);
            break;
          case 'semantica':
            // Need transcripcion first
            if (isEmpty(row.transcripcion)) {
              result = { success: false, detail: 'Needs transcripcion first' };
              break;
            }
            result = await phaseSemantica(row, db, context);
            break;
          case 'visual_analysis':
            if (isEmpty(row.visual)) {
              result = { success: false, detail: 'Needs visual first' };
              break;
            }
            result = await phaseVisualAnalysis(row, db, context);
            break;
          case 'escenas':
            if (isEmpty(row.visual)) {
              result = { success: false, detail: 'Needs visual first' };
              break;
            }
            result = await phaseEscenas(row, db);
            break;
          case 'analyze_escenas':
            result = await phaseAnalyzeEscenas(row, db);
            break;
          case 'frames':
            result = await phaseFrames(row, db);
            break;
          case 'embeddings':
            result = await phaseEmbeddings(row, db);
            break;
          case 'emb_escenas':
            result = await phaseEmbEscenas(row, db);
            break;
          default:
            result = { success: false, detail: `Unknown phase: ${phase}` };
        }

        if (result.success) {
          console.log(`✅ ${result.detail}`);
          stats[phase].executed++;
        } else {
          console.log(`❌ ${result.detail}`);
          stats[phase].errors++;
        }

        // Small delay between phases
        await sleep(200);
      }

      console.log('');
    }

    // Summary
    const elapsed = Date.now() - startTime;
    console.log('═══════════════════════════════════════════════════════════════');
    console.log('  RESUMEN');
    console.log('═══════════════════════════════════════════════════════════════');
    console.log(`  Filas: ${rows.length}`);

    for (const phase of ALL_PHASES) {
      const s = stats[phase];
      if (s.executed > 0 || s.errors > 0) {
        console.log(`  ${phase}: ${s.executed} OK, ${s.errors} errores`);
      }
    }

    console.log(`  Tiempo: ${formatDuration(elapsed)}`);
    console.log('═══════════════════════════════════════════════════════════════\n');

  } finally {
    await db.end();
  }
}

main().catch(err => {
  console.error('Error fatal:', err);
  process.exit(1);
});
