#!/usr/bin/env node
/**
 * MoneyBall Meta Ads Pipeline Orchestrator
 *
 * Analiza anuncios de Meta Ads (video e imagen) usando Gemini AI.
 * Para VIDEO: Paso1 (transcripcion+visual) → Paso2 (10 campos semanticos)
 * Para IMAGE: Paso1 (analisis visual) → Paso2 (4 campos)
 *
 * Uso:
 *   node pipeline_meta.js --anunciante=runna
 *   node pipeline_meta.js --ids=349,350
 *   node pipeline_meta.js --where="tipo_media = 'VIDEO'"
 *   node pipeline_meta.js --anunciante=runna --only=semantica
 *   node pipeline_meta.js --anunciante=runna --limit=10 --dry-run
 *   node pipeline_meta.js --anunciante=runna --mod=4 --part=0
 */

const fs = require('fs');
const path = require('path');
const https = require('https');
const { execSync } = require('child_process');
const { Pool } = require('pg');
const { GoogleGenerativeAI } = require('@google/generative-ai');
const puppeteer = require(path.join(__dirname, 'node_modules/puppeteer'));

// ═══════════════════════════════════════════════════════════════
// CONFIG
// ═══════════════════════════════════════════════════════════════
const CONFIG = {
  promptsDir: path.join(__dirname, 'prompts'),
  mediaDir: path.join(__dirname, 'meta_ads_media'),
  screenshotsDir: path.join(__dirname, 'meta_ads_screenshots'),
  tempDir: '/tmp/moneyball_meta_pipeline',
  geminiModel: 'gemini-2.5-flash',
  embeddingModel: 'gemini-embedding-001',
  delayMs: 500,
};

// Gemini key: from env or file
const GEMINI_KEY = process.env.GEMINI_API_KEY
  || (() => { try { return fs.readFileSync(path.join(process.env.HOME, '.config/gemini/api_key'), 'utf8').trim(); } catch(e) { return ''; } })();
const EXEC_ENV = { ...process.env };

// Ensure dirs
if (!fs.existsSync(CONFIG.tempDir)) fs.mkdirSync(CONFIG.tempDir, { recursive: true });
if (!fs.existsSync(CONFIG.mediaDir)) fs.mkdirSync(CONFIG.mediaDir, { recursive: true });

// Load prompts
const PROMPT_VIDEO_PASO1 = fs.readFileSync(path.join(CONFIG.promptsDir, 'meta_video_paso1.txt'), 'utf8');
const PROMPT_VIDEO_PASO2 = fs.readFileSync(path.join(CONFIG.promptsDir, 'meta_video_paso2.txt'), 'utf8');
const PROMPT_IMAGEN_PASO1 = fs.readFileSync(path.join(CONFIG.promptsDir, 'meta_imagen_paso1.txt'), 'utf8');
const PROMPT_IMAGEN_PASO2 = fs.readFileSync(path.join(CONFIG.promptsDir, 'meta_imagen_paso2.txt'), 'utf8');

// Phase names
const ALL_PHASES = ['download_media', 'transcripcion_visual', 'analisis_imagen', 'semantica', 'analisis_imagen_2', 'embeddings'];
const PHASE_LABELS = {
  download_media: 'Download Media',
  transcripcion_visual: 'Transcripcion+Visual (Video)',
  analisis_imagen: 'Analisis Visual (Imagen)',
  semantica: 'Semantica (Video Paso2)',
  analisis_imagen_2: 'Analisis Imagen Paso2',
  embeddings: 'Embeddings',
};

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
    anunciante: get('anunciante'),
    ids: get('ids') ? get('ids').split(',').map(Number) : null,
    where: get('where'),
    only: get('only') ? get('only').split(',') : null,
    skip: get('skip') ? get('skip').split(',') : [],
    limit: get('limit') ? parseInt(get('limit')) : null,
    dryRun: has('dry-run'),
    force: has('force'),
    mod: get('mod') ? parseInt(get('mod')) : null,
    part: get('part') !== null && get('part') !== undefined ? parseInt(get('part')) : null,
    help: has('help') || has('h'),
  };
}

function showHelp() {
  console.log(`
  MoneyBall Meta Ads Pipeline
  ═══════════════════════════════════════════════════════════════

  FILTROS:
    --anunciante=runna        Filtrar por anunciante
    --ids=349,350             Filtrar por IDs
    --where="SQL condition"   Filtro SQL custom
    --limit=10                Limitar filas
    --mod=4 --part=0          Paralelizacion (id % mod = part)

  FASES:
    --only=semantica,analisis_imagen_2   Solo estas fases
    --skip=download_media                Saltar fases
    Fases: ${ALL_PHASES.join(', ')}

  OPCIONES:
    --dry-run                 Mostrar sin ejecutar
    --force                   Forzar re-analisis
    --help                    Ayuda
  `);
}

// ═══════════════════════════════════════════════════════════════
// UTILS
// ═══════════════════════════════════════════════════════════════
function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }
function isEmpty(val) { return val === null || val === undefined || val === ''; }
function formatDuration(ms) {
  const s = Math.floor(ms / 1000);
  if (s < 60) return `${s}s`;
  const m = Math.floor(s / 60);
  return `${m}m ${s % 60}s`;
}

// ═══════════════════════════════════════════════════════════════
// GEMINI API HELPERS (from pipeline.js)
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
              maxOutputTokens: 4000,
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
        const text = data.candidates?.[0]?.content?.parts?.[0]?.text || '';
        if (!text) {
          const finishReason = data.candidates?.[0]?.finishReason;
          const blockReason = data.promptFeedback?.blockReason;
          if (blockReason) console.log(`    ⚠️ Gemini blocked: ${blockReason}`);
          else if (finishReason && finishReason !== 'STOP') console.log(`    ⚠️ Gemini finish: ${finishReason}`);
          else console.log(`    ⚠️ Gemini returned empty response`);
        }
        return text;
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

async function callGeminiWithImage(imagePath, prompt, maxRetries = 5) {
  const imageData = fs.readFileSync(imagePath);
  const base64 = imageData.toString('base64');
  const mimeType = imagePath.endsWith('.png') ? 'image/png' : 'image/jpeg';

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
                { inline_data: { mime_type: mimeType, data: base64 } },
                { text: prompt },
              ],
            }],
            generationConfig: {
              temperature: 0.2,
              maxOutputTokens: 4000,
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
// GEMINI FILE UPLOAD (for video)
// ═══════════════════════════════════════════════════════════════
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
// MEDIA RESOLUTION — find or download media file
// ═══════════════════════════════════════════════════════════════
// ── Puppeteer: extract media URL from Facebook Ads Library ──
// Input: ad_url (https://www.facebook.com/ads/library/?id=XXXXX)
// Abre la URL, busca el snapshot del ad en el HTML vía deeplinkAdID
// Extrae video/image URL directa para pasar a Gemini sin descarga

let _browser = null;

async function getBrowser() {
  if (_browser && _browser.isConnected()) return _browser;
  _browser = await puppeteer.launch({ headless: 'new', args: ['--no-sandbox', '--disable-setuid-sandbox'] });
  return _browser;
}

async function closeBrowser() {
  if (_browser) { try { await _browser.close(); } catch (e) {} _browser = null; }
}

async function extractMediaUrlFromFacebook(row) {
  const adUrl = row.ad_url;
  if (!adUrl) return null;

  const adId = row.ad_id || (adUrl.match(/[?&]id=(\d+)/)?.[1]);
  if (!adId) return null;

  console.log(`    🌐 Puppeteer: cargando ad ${adId}...`);
  const browser = await getBrowser();
  const page = await browser.newPage();

  try {
    await page.setUserAgent('Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36');
    await page.goto(adUrl, { waitUntil: 'networkidle2', timeout: 45000 });
    await sleep(3000);

    const html = await page.content();

    // ── Estrategia 1: buscar por deeplinkAdID (número sin comillas) ──
    // Facebook embebe el ad target como "deeplinkAdID":XXXXXXX
    const deeplinkPattern = `"deeplinkAdID":${adId}`;
    const deeplinkPos = html.indexOf(deeplinkPattern);

    let snapshot = null;

    if (deeplinkPos !== -1) {
      // Encontrado vía deeplink — buscar el ad_archive_id y snapshot más cercano
      // Buscar TODOS los ad_archive_id con sus posiciones
      const archiveRegex = /"ad_archive_id":"(\d+)"/g;
      let match;
      let bestArchiveId = null;
      let bestDist = Infinity;
      let bestPos = -1;

      while ((match = archiveRegex.exec(html)) !== null) {
        const dist = Math.abs(match.index - deeplinkPos);
        // También verificar match directo
        if (match[1] === adId) {
          bestArchiveId = match[1];
          bestPos = match.index;
          break;
        }
        if (dist < bestDist) {
          bestDist = dist;
          bestArchiveId = match[1];
          bestPos = match.index;
        }
      }

      // Si encontramos el ID directo o el más cercano al deeplink
      if (bestPos !== -1) {
        const snapStart = html.indexOf('"snapshot":', bestPos);
        if (snapStart !== -1 && snapStart - bestPos < 3000) {
          snapshot = parseSnapshotFromHtml(html, snapStart);
        }
      }
    }

    // ── Estrategia 2: buscar ad_archive_id directo ──
    if (!snapshot) {
      const directPattern = `"ad_archive_id":"${adId}"`;
      const directPos = html.indexOf(directPattern);
      if (directPos !== -1) {
        const snapStart = html.indexOf('"snapshot":', directPos);
        if (snapStart !== -1 && snapStart - directPos < 3000) {
          snapshot = parseSnapshotFromHtml(html, snapStart);
        }
      }
    }

    // ── Estrategia 3: buscar con collation pattern ──
    if (!snapshot) {
      const patterns = [`"${adId}","collation_`, `"${adId}","collation`];
      for (const pat of patterns) {
        const pos = html.indexOf(pat);
        if (pos !== -1) {
          const snapStart = html.indexOf('"snapshot":', pos);
          if (snapStart !== -1 && snapStart - pos < 5000) {
            snapshot = parseSnapshotFromHtml(html, snapStart);
            if (snapshot) break;
          }
        }
      }
    }

    if (!snapshot) {
      console.log(`    ❌ Ad no encontrado en Ads Library HTML`);
      return null;
    }

    // Extraer media URL del snapshot
    return extractMediaFromSnapshot(snapshot);
  } catch (err) {
    console.log(`    ❌ Puppeteer error: ${err.message}`);
    return null;
  } finally {
    await page.close();
  }
}

function parseSnapshotFromHtml(html, snapshotKeyPos) {
  const braceStart = html.indexOf('{', snapshotKeyPos + 10);
  if (braceStart === -1) return null;

  let depth = 0;
  let braceEnd = braceStart;
  for (let i = braceStart; i < html.length && i < braceStart + 80000; i++) {
    if (html[i] === '{') depth++;
    if (html[i] === '}') depth--;
    if (depth === 0) { braceEnd = i + 1; break; }
  }

  try {
    const raw = html.substring(braceStart, braceEnd).replace(/\\\//g, '/');
    return JSON.parse(raw);
  } catch (e) {
    return null;
  }
}

function extractMediaFromSnapshot(snapshot) {
  if (!snapshot) return null;

  const format = snapshot.display_format || '';
  console.log(`    📋 Formato: ${format}`);

  let mediaUrl = null;
  let mediaType = null;

  // 1. Top-level videos
  if (snapshot.videos?.length > 0) {
    const v = snapshot.videos[0];
    mediaUrl = v.video_hd_url || v.video_sd_url;
    if (mediaUrl) mediaType = 'video';
  }
  // 2. Top-level images
  if (!mediaUrl && snapshot.images?.length > 0) {
    const img = snapshot.images[0];
    mediaUrl = img.original_image_url || img.resized_image_url;
    if (mediaUrl) mediaType = 'image';
  }
  // 3. Cards (DCO ads)
  if (!mediaUrl && snapshot.cards?.length > 0) {
    const card = snapshot.cards[0];
    if (card.video_hd_url || card.video_sd_url) {
      mediaUrl = card.video_hd_url || card.video_sd_url;
      mediaType = 'video';
    } else if (card.original_image_url || card.resized_image_url) {
      mediaUrl = card.original_image_url || card.resized_image_url;
      mediaType = 'image';
    }
  }

  if (!mediaUrl) {
    console.log(`    ❌ No media URL en snapshot`);
    return null;
  }

  console.log(`    ✅ ${mediaType} URL extraída (${format})`);
  return { url: mediaUrl, type: mediaType, mimeType: mediaType === 'video' ? 'video/mp4' : 'image/jpeg' };
}

// ── Gemini con URL directa (sin descarga ni upload) ──

async function callGeminiWithVideoUrl(videoUrl, prompt, maxRetries = 5) {
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
                { text: prompt },
                { fileData: { fileUri: videoUrl, mimeType: 'video/mp4' } },
              ],
            }],
            generationConfig: { temperature: 0.3, maxOutputTokens: 8000 },
          }),
        }
      );

      if (response.ok) {
        const data = await response.json();
        const text = data.candidates?.[0]?.content?.parts?.[0]?.text || '';
        if (!text) {
          const blockReason = data.promptFeedback?.blockReason;
          const finishReason = data.candidates?.[0]?.finishReason;
          if (blockReason) console.log(`    ⚠️ Gemini blocked: ${blockReason}`);
          else if (finishReason && finishReason !== 'STOP') console.log(`    ⚠️ Gemini finish: ${finishReason}`);
          else console.log(`    ⚠️ Gemini respuesta vacía`);
        }
        return text;
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

async function callGeminiWithImageUrl(imageUrl, prompt, maxRetries = 5) {
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
                { fileData: { fileUri: imageUrl, mimeType: 'image/jpeg' } },
                { text: prompt },
              ],
            }],
            generationConfig: {
              temperature: 0.2,
              maxOutputTokens: 4000,
              responseMimeType: 'application/json',
            },
          }),
        }
      );

      if (response.ok) {
        const data = await response.json();
        const text = data.candidates?.[0]?.content?.parts?.[0]?.text || '{}';
        try { return JSON.parse(text); } catch (e) {
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
// PHASE DETECTION
// ═══════════════════════════════════════════════════════════════
function detectPhases(row) {
  const needed = [];
  const tipo = (row.tipo_media || '').toLowerCase();
  const isVideo = tipo === 'video' || tipo === 'vid';
  const isImage = tipo === 'image' || tipo === 'imagen' || tipo === 'img';

  if (isVideo) {
    // VIDEO flow
    if (isEmpty(row.transcripcion) || isEmpty(row.visual)) {
      needed.push('download_media');
      needed.push('transcripcion_visual');
    }
    if (!isEmpty(row.transcripcion) && !isEmpty(row.visual) && (
      isEmpty(row.hook) || isEmpty(row.tematica) || isEmpty(row.formula_hook) ||
      isEmpty(row.semantica_inicio) || isEmpty(row.semantica_ruta) || isEmpty(row.semantica_cluster) ||
      isEmpty(row.visual_inicio) || isEmpty(row.visual_ruta) || isEmpty(row.visual_cluster) ||
      isEmpty(row.nomenclatura_ad)
    )) {
      needed.push('semantica');
    }
  } else if (isImage) {
    // IMAGE flow
    if (isEmpty(row.visual)) {
      needed.push('download_media');
      needed.push('analisis_imagen');
    }
    if (!isEmpty(row.visual) && (
      isEmpty(row.hook) || isEmpty(row.tematica) || isEmpty(row.visual_cluster) ||
      isEmpty(row.nomenclatura_ad)
    )) {
      needed.push('analisis_imagen_2');
    }
  }
  // Skip CAROUSEL, PAGE_LIKE, pending, empty tipo_media

  // Embeddings: needs at least hook or visual to generate embedding
  if ((isVideo || isImage) && isEmpty(row.embedding) && (!isEmpty(row.hook) || !isEmpty(row.visual))) {
    needed.push('embeddings');
  }

  return needed;
}

function filterPhases(detected, only, skip) {
  let phases = detected;
  if (only && only.length > 0) {
    phases = detected.filter(p => only.includes(p));
    // If user asks for semantica but row needs download first, include download
    if (phases.includes('transcripcion_visual') && !phases.includes('download_media') && detected.includes('download_media')) {
      phases.unshift('download_media');
    }
    if (phases.includes('analisis_imagen') && !phases.includes('download_media') && detected.includes('download_media')) {
      phases.unshift('download_media');
    }
  }
  if (skip && skip.length > 0) {
    phases = phases.filter(p => !skip.includes(p));
  }
  return phases;
}

// ═══════════════════════════════════════════════════════════════
// PHASE EXECUTION
// ═══════════════════════════════════════════════════════════════

// Resolve media: Puppeteer abre ad_url → extrae URL directa del video/imagen
async function phaseDownloadMedia(row) {
  try {
    const extracted = await extractMediaUrlFromFacebook(row);
    if (extracted) {
      row._mediaPath = null;  // No file, URL-based
      row._mediaType = extracted.type;
      row._mediaUrl = extracted.url;
      row._mediaTemp = false;
      return { success: true, detail: `URL ${extracted.type}: ok` };
    }

    return { success: false, detail: 'No media found' };
  } catch (err) {
    return { success: false, detail: err.message };
  }
}

// VIDEO Paso 1: Analyze video → transcription + visual analysis
// Supports URL-based (Puppeteer) and file-based (local) video
async function phaseTranscripcionVisual(row, db) {
  let geminiFile = null;
  try {
    let rawText;

    if (row._mediaUrl && row._mediaType === 'video') {
      // URL directa → Gemini sin descarga
      console.log(`    🎬 Analizando video por URL (Paso 1)...`);
      rawText = await callGeminiWithVideoUrl(row._mediaUrl, PROMPT_VIDEO_PASO1);
    } else if (row._mediaPath && fs.existsSync(row._mediaPath)) {
      // Archivo local → upload a Gemini
      console.log(`    📤 Subiendo a Gemini (${(fs.statSync(row._mediaPath).size / 1024 / 1024).toFixed(1)} MB)...`);
      geminiFile = await uploadToGemini(row._mediaPath);
      console.log(`    ⏳ Procesando...`);
      const processed = await waitForGeminiFile(geminiFile.name);
      console.log(`    🎬 Analizando video (Paso 1)...`);
      rawText = await callGeminiWithVideo(processed.uri, processed.mimeType, PROMPT_VIDEO_PASO1);
    } else {
      return { success: false, detail: 'No video disponible' };
    }

    if (!rawText) {
      return { success: false, detail: 'Gemini no devolvió respuesta' };
    }

    // Parse JSON response — strip markdown fences and handle truncated JSON
    let result;
    let cleaned = rawText.trim();
    // Strip markdown code fences
    cleaned = cleaned.replace(/^```(?:json)?\s*/i, '').replace(/\s*```\s*$/, '');
    try {
      result = JSON.parse(cleaned);
    } catch (e) {
      // Try extracting the largest JSON block
      const match = cleaned.match(/\{[\s\S]*\}/);
      if (match) {
        try { result = JSON.parse(match[0]); } catch (e2) {
          // If JSON is truncated, try to repair: add missing closing brackets
          let repaired = match[0];
          const opens = (repaired.match(/\{/g) || []).length;
          const closes = (repaired.match(/\}/g) || []).length;
          if (opens > closes) {
            // Remove trailing incomplete value and close brackets
            repaired = repaired.replace(/,?\s*"[^"]*"?\s*:?\s*[^}\]]*$/, '');
            repaired += ']}'.repeat(opens - closes);
            try { result = JSON.parse(repaired); } catch (e3) {
              console.log(`\n    [DEBUG] Raw (500): ${rawText.substring(0, 500)}`);
              throw new Error('No se pudo parsear respuesta Gemini video');
            }
          } else {
            console.log(`\n    [DEBUG] Raw (500): ${rawText.substring(0, 500)}`);
            throw new Error('No se pudo parsear respuesta Gemini video');
          }
        }
      } else {
        console.log(`\n    [DEBUG] Raw (500): ${rawText.substring(0, 500)}`);
        throw new Error('No se pudo parsear respuesta Gemini video');
      }
    }

    const transcripcion = result.TranscripcionAudio || '';
    const visualJson = JSON.stringify(result.AnalisisVisual || []);

    // Update DB
    const updates = [];
    const values = [];
    if (transcripcion) { updates.push('transcripcion = ?'); values.push(transcripcion); }
    if (visualJson && visualJson !== '[]') { updates.push('visual = ?'); values.push(visualJson); }

    if (updates.length > 0) {
      updates.push("updated_at = NOW()");
      values.push(row.id);
      // Convert ? placeholders to $1, $2...
      let paramIdx = 1;
      const pgUpdates = updates.map(u => u.includes('=') && u.includes('NOW()') ? u : u.replace('?', () => `$${paramIdx++}`));
      // Actually: build proper parameterized update
      const setClauses = [];
      const pgValues = [];
      let pi = 1;
      if (transcripcion) { setClauses.push(`transcripcion = $${pi++}`); pgValues.push(transcripcion); }
      if (visualJson && visualJson !== '[]') { setClauses.push(`visual = $${pi++}`); pgValues.push(visualJson); }
      setClauses.push('updated_at = NOW()');
      pgValues.push(row.id);
      await db.query(`UPDATE meta_ads SET ${setClauses.join(', ')} WHERE id = $${pi}`, pgValues);
      row.transcripcion = transcripcion;
      row.visual = visualJson;
    }

    return { success: true, detail: `trans=${transcripcion.length}c vis=${visualJson.length}c` };
  } catch (err) {
    return { success: false, detail: err.message };
  } finally {
    if (geminiFile) await deleteGeminiFile(geminiFile.name);
    if (row._mediaTemp && row._mediaPath) {
      try { fs.unlinkSync(row._mediaPath); } catch (e) {}
    }
  }
}

// IMAGE Paso 1: Analyze image → visual description
// Supports URL-based (Puppeteer) and file-based (local) image
async function phaseAnalisisImagen(row, db) {
  try {
    let result;

    if (row._mediaUrl && row._mediaType === 'image') {
      // URL directa → Gemini sin descarga
      console.log(`    🖼️ Analizando imagen por URL (Paso 1)...`);
      result = await callGeminiWithImageUrl(row._mediaUrl, PROMPT_IMAGEN_PASO1);
    } else if (row._mediaPath && fs.existsSync(row._mediaPath)) {
      // Archivo local
      console.log(`    🖼️ Analizando imagen (Paso 1)...`);
      result = await callGeminiWithImage(row._mediaPath, PROMPT_IMAGEN_PASO1);
    } else {
      return { success: false, detail: 'No image disponible' };
    }

    const visualJson = JSON.stringify(result.TranscripcionVisual || result);

    if (visualJson && visualJson !== '{}') {
      await db.query(`UPDATE meta_ads SET visual = $1, updated_at = NOW() WHERE id = $2`, [visualJson, row.id]);
      row.visual = visualJson;
    }

    return { success: true, detail: `visual=${visualJson.length}c` };
  } catch (err) {
    return { success: false, detail: err.message };
  }
}

// VIDEO Paso 2: Semantic analysis (10 fields)
async function phaseSemantica(row, db) {
  try {
    const prompt = PROMPT_VIDEO_PASO2
      .replace('{{TRANSCRIPCION}}', row.transcripcion || '')
      .replace('{{VISUAL}}', (row.visual || '').substring(0, 15000));

    const result = await callGeminiText(prompt);

    await db.query(`
      UPDATE meta_ads SET
        hook = $1, tematica = $2, formula_hook = $3,
        semantica_inicio = $4, semantica_ruta = $5, semantica_cluster = $6,
        visual_inicio = $7, visual_ruta = $8, visual_cluster = $9,
        nomenclatura_ad = $10,
        updated_at = NOW()
      WHERE id = $11
    `, [
      result.Hook || '', result.Tematica || '', result.Formula_Hook || '',
      result.Semantica_Inicio || '', result.Semantica_Ruta || '', result.Semantica_Cluster || '',
      result.Visual_Inicio || '', result.Visual_Ruta || '', result.Visual_Cluster || '',
      result.Nomenclatura_AD || '',
      row.id
    ]);

    row.hook = result.Hook || '';
    row.tematica = result.Tematica || '';
    row.nomenclatura_ad = result.Nomenclatura_AD || '';

    return { success: true, detail: `hook="${(result.Hook || '').substring(0, 40)}" nom="${(result.Nomenclatura_AD || '').substring(0, 30)}"` };
  } catch (err) {
    return { success: false, detail: err.message };
  }
}

// IMAGE Paso 2: Analyze visual → 4 fields
async function phaseAnalisisImagen2(row, db) {
  try {
    const prompt = PROMPT_IMAGEN_PASO2
      .replace('{{VISUAL}}', (row.visual || '').substring(0, 15000));

    const result = await callGeminiText(prompt);

    await db.query(`
      UPDATE meta_ads SET
        hook = $1, tematica = $2, visual_cluster = $3,
        nomenclatura_ad = $4,
        updated_at = NOW()
      WHERE id = $5
    `, [
      result.Hook || '', result.Tematica || '', result.Visual_Cluster || '',
      result.Nomenclatura_AD || '',
      row.id
    ]);

    row.hook = result.Hook || '';
    row.tematica = result.Tematica || '';
    row.nomenclatura_ad = result.Nomenclatura_AD || '';

    return { success: true, detail: `hook="${(result.Hook || '').substring(0, 40)}" vis="${(result.Visual_Cluster || '').substring(0, 30)}"` };
  } catch (err) {
    return { success: false, detail: err.message };
  }
}

// ═══════════════════════════════════════════════════════════════
// EMBEDDINGS — generate semantic embedding for search
// ═══════════════════════════════════════════════════════════════
async function generateEmbedding(text, maxRetries = 5) {
  for (let attempt = 0; attempt < maxRetries; attempt++) {
    try {
      const response = await fetch(
        `https://generativelanguage.googleapis.com/v1beta/models/${CONFIG.embeddingModel}:embedContent?key=${GEMINI_KEY}`,
        {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            model: `models/${CONFIG.embeddingModel}`,
            content: { parts: [{ text }] },
          }),
        }
      );

      if (response.ok) {
        const data = await response.json();
        return data.embedding?.values || null;
      }

      if ([429, 500, 503].includes(response.status)) {
        const wait = Math.min(30000, 2000 * Math.pow(2, attempt));
        console.log(`    ⚠️ Embedding ${response.status}, retry ${attempt + 1}/${maxRetries}...`);
        await sleep(wait);
        continue;
      }

      const errText = await response.text();
      throw new Error(`Embedding API error: ${response.status} - ${errText.substring(0, 200)}`);
    } catch (err) {
      if (err.message?.includes('Embedding API error')) throw err;
      if (attempt === maxRetries - 1) throw err;
      await sleep(Math.min(30000, 2000 * Math.pow(2, attempt)));
    }
  }
  return null;
}

function buildEmbeddingText(row) {
  // Concatenar campos relevantes para generar un embedding representativo del ad
  const parts = [];
  if (row.anunciante) parts.push(`Anunciante: ${row.anunciante}`);
  if (row.hook) parts.push(`Hook: ${row.hook}`);
  if (row.tematica) parts.push(`Temática: ${row.tematica}`);
  if (row.formula_hook) parts.push(`Fórmula Hook: ${row.formula_hook}`);
  if (row.nomenclatura_ad) parts.push(`Nomenclatura: ${row.nomenclatura_ad}`);
  if (row.texto_principal) parts.push(`Texto: ${row.texto_principal}`);
  if (row.titulo) parts.push(`Título: ${row.titulo}`);
  if (row.descripcion) parts.push(`Descripción: ${row.descripcion}`);
  if (row.cta) parts.push(`CTA: ${row.cta}`);
  if (row.transcripcion) parts.push(`Transcripción: ${row.transcripcion.substring(0, 2000)}`);
  if (row.semantica_cluster) parts.push(`Cluster Semántico: ${row.semantica_cluster}`);
  if (row.visual_cluster) parts.push(`Cluster Visual: ${row.visual_cluster}`);
  // Visual JSON — extract key descriptions, truncate
  if (row.visual) {
    try {
      const vis = JSON.parse(row.visual);
      if (Array.isArray(vis)) {
        const desc = vis.map(v => v.Descripcion || v.descripcion || '').filter(Boolean).join('. ');
        if (desc) parts.push(`Visual: ${desc.substring(0, 1500)}`);
      } else if (typeof vis === 'object') {
        const desc = vis.Descripcion || vis.descripcion || JSON.stringify(vis).substring(0, 1500);
        parts.push(`Visual: ${desc}`);
      }
    } catch (e) {
      parts.push(`Visual: ${row.visual.substring(0, 1500)}`);
    }
  }
  return parts.join('\n');
}

async function phaseEmbeddings(row, db) {
  try {
    const text = buildEmbeddingText(row);
    if (!text || text.length < 20) {
      return { success: false, detail: 'Texto insuficiente para embedding' };
    }

    console.log(`    🔢 Generando embedding (${text.length}c)...`);
    const embedding = await generateEmbedding(text);

    if (!embedding || embedding.length === 0) {
      return { success: false, detail: 'Embedding vacío' };
    }

    const embeddingJson = JSON.stringify(embedding);
    await db.query(`UPDATE meta_ads SET embedding = $1, updated_at = NOW() WHERE id = $2`, [embeddingJson, row.id]);
    row.embedding = embeddingJson;

    return { success: true, detail: `embedding=${embedding.length}d` };
  } catch (err) {
    return { success: false, detail: err.message };
  }
}

// ═══════════════════════════════════════════════════════════════
// MAIN
// ═══════════════════════════════════════════════════════════════
async function main() {
  const opts = parseArgs();

  if (opts.help) { showHelp(); return; }
  if (!opts.anunciante && !opts.ids && !opts.where) {
    console.log('Error: Necesitas --anunciante, --ids o --where');
    showHelp();
    process.exit(1);
  }

  // Header
  console.log('\n═══════════════════════════════════════════════════════════════');
  console.log('  MoneyBall Meta Ads Pipeline');
  console.log('═══════════════════════════════════════════════════════════════');
  if (opts.anunciante) console.log(`  Anunciante: ${opts.anunciante}`);
  if (opts.ids) console.log(`  IDs: ${opts.ids.join(', ')}`);
  if (opts.where) console.log(`  Where: ${opts.where}`);
  if (opts.only) console.log(`  Solo fases: ${opts.only.join(', ')}`);
  if (opts.skip.length) console.log(`  Skip fases: ${opts.skip.join(', ')}`);
  if (opts.limit) console.log(`  Limite: ${opts.limit}`);
  if (opts.force) console.log(`  ⚡ Force re-analysis`);
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
    // Build WHERE with $1, $2... placeholders
    let where = '1=1';
    const params = [];
    let pIdx = 1;

    if (opts.anunciante) {
      where += ` AND anunciante = $${pIdx++}`;
      params.push(opts.anunciante);
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
      SELECT id, anunciante, ad_id, ad_url, tipo_media, media_url,
             texto_principal, titulo, descripcion, cta,
             transcripcion, visual, hook, tematica, formula_hook,
             semantica_inicio, semantica_ruta, semantica_cluster,
             visual_inicio, visual_ruta, visual_cluster,
             nomenclatura_ad, embedding
      FROM meta_ads
      WHERE ${where}
      ORDER BY id
    `;
    if (opts.limit) query += ` LIMIT ${opts.limit}`;

    const result = await db.query(query, params);
    const rows = result.rows;
    console.log(`📊 ${rows.length} filas seleccionadas\n`);

    if (rows.length === 0) {
      console.log('No hay filas que procesar.');
      return;
    }

    // Stats
    const stats = {};
    ALL_PHASES.forEach(p => { stats[p] = { executed: 0, errors: 0 }; });
    const startTime = Date.now();

    // Process each row
    for (let i = 0; i < rows.length; i++) {
      const row = rows[i];
      const tipo = (row.tipo_media || '').toLowerCase();

      // Detect needed phases
      let detected = detectPhases(row);

      // Force mode: re-detect everything
      if (opts.force) {
        const isVideo = tipo === 'video' || tipo === 'vid';
        const isImage = tipo === 'image' || tipo === 'imagen' || tipo === 'img';
        if (isVideo) detected = ['download_media', 'transcripcion_visual', 'semantica', 'embeddings'];
        else if (isImage) detected = ['download_media', 'analisis_imagen', 'analisis_imagen_2', 'embeddings'];
      }

      const phases = filterPhases(detected, opts.only, opts.skip);

      // Log
      const emptyFields = [];
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
      if (isEmpty(row.nomenclatura_ad)) emptyFields.push('nomenclatura');
      if (isEmpty(row.embedding)) emptyFields.push('embedding');

      console.log(`[${i + 1}/${rows.length}] ID ${row.id} (${row.anunciante}) [${row.tipo_media || '?'}] ad:${row.ad_id || '?'}`);
      if (emptyFields.length > 0) {
        console.log(`  Vacias: ${emptyFields.join(', ')}`);
      } else {
        console.log(`  ✅ Todas las columnas rellenas`);
      }

      if (phases.length === 0) {
        console.log(`  → Nada que hacer\n`);
        continue;
      }

      // Sort by dependency order
      phases.sort((a, b) => ALL_PHASES.indexOf(a) - ALL_PHASES.indexOf(b));
      console.log(`  Fases: ${phases.join(', ')}`);

      if (opts.dryRun) {
        console.log(`  → [DRY RUN] Se ejecutarian: ${phases.join(', ')}\n`);
        phases.forEach(p => stats[p].executed++);
        continue;
      }

      // Execute phases
      for (const phase of phases) {
        let result;
        process.stdout.write(`  ▶ ${phase}... `);

        switch (phase) {
          case 'download_media':
            result = await phaseDownloadMedia(row);
            break;
          case 'transcripcion_visual':
            result = await phaseTranscripcionVisual(row, db);
            break;
          case 'analisis_imagen':
            result = await phaseAnalisisImagen(row, db);
            break;
          case 'semantica':
            if (isEmpty(row.transcripcion) && isEmpty(row.visual)) {
              result = { success: false, detail: 'Needs transcripcion+visual first' };
              break;
            }
            result = await phaseSemantica(row, db);
            break;
          case 'analisis_imagen_2':
            if (isEmpty(row.visual)) {
              result = { success: false, detail: 'Needs visual first' };
              break;
            }
            result = await phaseAnalisisImagen2(row, db);
            break;
          case 'embeddings':
            if (isEmpty(row.hook) && isEmpty(row.visual)) {
              result = { success: false, detail: 'Needs hook or visual first' };
              break;
            }
            result = await phaseEmbeddings(row, db);
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

        await sleep(200);
      }

      console.log('');
      // Delay between rows to avoid rate limiting
      await sleep(CONFIG.delayMs);
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
    await closeBrowser();
  }
}

main().catch(err => {
  console.error('Error fatal:', err);
  closeBrowser().finally(() => process.exit(1));
});
