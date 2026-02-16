#!/usr/bin/env node
/**
 * CREA VISUAL — Sistema autónomo de generación de storyboards visuales CEO Media
 *
 * Documentación: https://www.notion.so/Visual-IA-304a67fd29b280e9bc08dc1ceecae362
 *
 * Uso: node crea_visual.js [--semana=YYYY-MM-DD] [--dry-run] [--pilar=victor_heras] [--context="texto libre"]
 *
 * Fases:
 * 0. Buscar páginas con estado "Guión" en Notion (semana objetivo)
 * 1. Por cada página:
 *    a. Leer guión completo desde Notion
 *    b. Determinar pilar/estilo (Victor Heras, Veneno, Nude Project)
 *    c. Parsear guión en beats (escenas)
 *    d. FASE 1 — Biblia Visual: contexto global del video (protagonista, localizaciones, estilo)
 *    e. FASE 2 — Escenas: por cada beat:
 *       - Búsqueda semántica en BBDD de escenas del referente
 *       - Generar adaptación de escena con Gemini (usando Biblia Visual + Manual de Estilo)
 *       - Generar imagen storyboard con Gemini (4 paneles sketch)
 *    f. Generar Google Doc nativo via API (Biblia Visual + escenas + imágenes)
 *    g. Guardar Doc en Google Drive (CEO Media/Storyboards/YYYY-MM)
 *    h. Escribir link del storyboard en Notion (append a la página)
 *    i. Cambiar estado "Creación" → "Grabar"
 */

const Database = require('better-sqlite3');
const fs = require('fs');
const path = require('path');
const { GoogleGenerativeAI } = require('@google/generative-ai');
const { google } = require('googleapis');
const Anthropic = require('@anthropic-ai/sdk');
// Google Docs nativo (sin Puppeteer)

// === Helper: lee archivo si existe, si no usa env var ===
function readKeyFile(filePath, envVar) {
  if (process.env[envVar]) return process.env[envVar];
  try { return fs.readFileSync(filePath, 'utf8').trim(); }
  catch(e) { return ''; }
}

// === CONFIGURACIÓN ===
const HOME = process.env.HOME || '/root';
const CONFIG = {
  DB_PATH: path.join(HOME, '.openclaw/workspace/moneyball_rrss.db'),
  NOTION_DB_ID: '13da67fd-29b2-801f-bac3-c72d4fb45bf2',  // Calendario Social Media
  NOTION_API_KEY: readKeyFile(path.join(HOME, '.config/notion/api_key'), 'NOTION_API_KEY'),
  GEMINI_API_KEY: readKeyFile(path.join(HOME, '.config/gemini/api_key'), 'GEMINI_API_KEY'),
  SERVICE_ACCOUNT_PATH: process.env.GOOGLE_SERVICE_ACCOUNT_PATH || path.join(HOME, '.config/google/service_account.json'),
  PERFIL: 'cristobal running',

  // Directorios
  FRAMES_DIR: process.env.FRAMES_DIR || path.join(HOME, '.openclaw/workspace/escenas_frames'),
  OUTPUT_DIR: process.env.OUTPUT_DIR || (process.env.DATABASE_URL ? '/tmp/storyboards_visual_ia' : path.join(HOME, '.openclaw/workspace/storyboards_visual_ia')),

  // Estilos de edición por pilar
  ESTILOS: {
    'victor_heras': { creador: 'victor_heras', archivo: 'ESTILO_VICTOR_HERAS.md', nombre: 'Victor Heras' },
    'veneno': { creador: 'veneno', archivo: 'ESTILO_VENENO.md', nombre: 'Veneno' },
    'nudeproject': { creador: 'nudeproject', archivo: 'ESTILO_NUDEPROJECT.md', nombre: 'Nude Project' }
  },

  // Mapeo pilar Notion → estilo visual
  PILAR_ESTILO_MAP: {
    '🎯 Víctor Heras': 'victor_heras',
    '🐍 Veneno': 'veneno',
    '📸 Post Personal': 'victor_heras',
    '📖 Stories': 'victor_heras',
    '🎬 Andrea': 'victor_heras',
    '⚡ Beltrán': 'victor_heras'
  },

  // Reverse map: emoji label → pilar key
  PILAR_MAP: {
    '🎬 Andrea': 'andrea',
    '⚡ Beltrán': 'beltran',
    '🎯 Víctor Heras': 'victor_heras',
    '🐍 Veneno': 'veneno',
    '📸 Post Personal': 'post_personal',
    '📖 Stories': 'stories'
  },

  // Mapeo pilar → tipo
  PILARES: {
    'andrea': { tipo: 'batch' },
    'beltran': { tipo: 'batch', flujoPorDefinir: true },
    'victor_heras': { tipo: 'normal' },
    'veneno': { tipo: 'normal' },
    'post_personal': { tipo: 'normal' },
    'stories': { tipo: 'normal' }
  },

  // Google Drive folder structure
  DRIVE_ROOT_FOLDER: 'CEO Media',
  DRIVE_SUB_FOLDER: 'Storyboards'
};

// Ensure output dir exists
if (!fs.existsSync(CONFIG.OUTPUT_DIR)) fs.mkdirSync(CONFIG.OUTPUT_DIR, { recursive: true });

// === CLAUDE OPUS 4.5 (via OpenClaw OAuth or env var) ===
let CLAUDE_TOKEN = process.env.ANTHROPIC_AUTH_TOKEN || '';
if (!CLAUDE_TOKEN) {
  try {
    const OPENCLAW_AUTH = JSON.parse(fs.readFileSync(path.join(HOME, '.openclaw/agents/main/agent/auth-profiles.json'), 'utf8'));
    CLAUDE_TOKEN = OPENCLAW_AUTH.profiles['anthropic:default']?.token || '';
  } catch(e) { /* no local auth file */ }
}
const claudeClient = new Anthropic({
  apiKey: null,
  authToken: CLAUDE_TOKEN,
  defaultHeaders: {
    'accept': 'application/json',
    'anthropic-dangerous-direct-browser-access': 'true',
    'anthropic-beta': 'claude-code-20250219,oauth-2025-04-20',
    'user-agent': 'claude-cli/2.1.2 (external, cli)',
    'x-app': 'cli'
  },
  dangerouslyAllowBrowser: true
});
const CLAUDE_MODEL = 'claude-opus-4-5';
const CLAUDE_SYSTEM = [{ type: 'text', text: 'You are Claude Code, Anthropics official CLI for Claude.' }];

async function claudeGenerate(prompt, maxTokens = 4096) {
  const msg = await claudeClient.messages.create({
    model: CLAUDE_MODEL,
    max_tokens: maxTokens,
    system: CLAUDE_SYSTEM,
    messages: [{ role: 'user', content: prompt }]
  });
  return msg.content[0].text;
}

// === UTILIDADES ===
function getNextMonday() {
  const today = new Date();
  const day = today.getDay();
  const diff = day === 0 ? 1 : 8 - day;
  const nextMonday = new Date(today);
  nextMonday.setDate(today.getDate() + diff);
  return nextMonday.toISOString().split('T')[0];
}

function addDays(dateStr, days) {
  const date = new Date(dateStr);
  date.setDate(date.getDate() + days);
  return date.toISOString().split('T')[0];
}

function normalizarAño(dateStr) {
  // Si la fecha tiene un año pasado, asume el año actual
  const currentYear = new Date().getFullYear();
  const parts = dateStr.split('-');
  if (parts.length === 3 && parseInt(parts[0]) < currentYear) {
    parts[0] = String(currentYear);
    return parts.join('-');
  }
  return dateStr;
}

function parseArgs() {
  const args = { semana: getNextMonday(), dryRun: false, pilar: null, context: null };
  process.argv.slice(2).forEach(arg => {
    if (arg.startsWith('--semana=')) args.semana = normalizarAño(arg.split('=')[1]);
    if (arg === '--dry-run') args.dryRun = true;
    if (arg.startsWith('--pilar=')) args.pilar = arg.split('=')[1];
    if (arg.startsWith('--context=')) args.context = arg.slice('--context='.length);
  });
  return args;
}

async function fetchWithRetry(url, options, maxRetries = 3) {
  for (let attempt = 0; attempt <= maxRetries; attempt++) {
    const resp = await fetch(url, options);
    if (resp.status === 429 && attempt < maxRetries) {
      const wait = Math.pow(2, attempt) * 1000;
      console.log(`   ⏳ Rate limit (429), esperando ${wait / 1000}s...`);
      await new Promise(r => setTimeout(r, wait));
      continue;
    }
    return resp;
  }
}

// === LEER MANUAL DE ESTILO ===
function leerManualEstilo(estiloKey) {
  const estilo = CONFIG.ESTILOS[estiloKey];
  if (!estilo) return '';
  const estiloPath = process.env.ESTILOS_DIR
    ? path.join(process.env.ESTILOS_DIR, estilo.archivo)
    : path.join(HOME, '.openclaw/workspace/moneyball', estilo.archivo);
  if (!fs.existsSync(estiloPath)) {
    console.log(`   ⚠️ Manual de estilo no encontrado: ${estiloPath}`);
    return '';
  }
  const contenido = fs.readFileSync(estiloPath, 'utf8');
  console.log(`   🎨 Manual de estilo leído: ${estilo.archivo} (${(contenido.length / 1024).toFixed(1)} KB)`);
  return contenido;
}

// === NOTION: Query páginas con estado "Guión" ===
async function queryNotionGuiones(semana) {
  const weekStart = semana;
  const weekEnd = addDays(semana, 6);

  console.log(`🔍 Buscando páginas en Estado="Creación": ${weekStart} → ${weekEnd}`);

  let results = [];
  let cursor = undefined;

  do {
    const body = {
      filter: {
        and: [
          { property: '⚫Estado', status: { equals: 'Creación' } },
          { property: '⚫Perfil (vacío = RunnerPro)', multi_select: { contains: CONFIG.PERFIL } },
          { property: '⚫Publicación', date: { on_or_after: weekStart } },
          { property: '⚫Publicación', date: { on_or_before: weekEnd } }
        ]
      },
      page_size: 100
    };
    if (cursor) body.start_cursor = cursor;

    const resp = await fetchWithRetry(`https://api.notion.com/v1/databases/${CONFIG.NOTION_DB_ID}/query`, {
      method: 'POST',
      headers: {
        'Authorization': `Bearer ${CONFIG.NOTION_API_KEY}`,
        'Notion-Version': '2022-06-28',
        'Content-Type': 'application/json'
      },
      body: JSON.stringify(body)
    });

    const data = await resp.json();
    if (data.results) results = results.concat(data.results);
    cursor = data.has_more ? data.next_cursor : undefined;
  } while (cursor);

  console.log(`   ✅ ${results.length} guiones encontrados`);
  return results;
}

// === NOTION: Leer guión de una página ===
async function leerGuionNotion(pageId) {
  let guionTexto = '';
  let inGuion = false;
  let hasVisual = false;
  let cursor = undefined;
  const beats = [];

  do {
    const url = `https://api.notion.com/v1/blocks/${pageId}/children?page_size=100${cursor ? `&start_cursor=${cursor}` : ''}`;
    const resp = await fetchWithRetry(url, {
      headers: {
        'Authorization': `Bearer ${CONFIG.NOTION_API_KEY}`,
        'Notion-Version': '2022-06-28'
      }
    });
    const data = await resp.json();
    if (!data.results) break;

    for (const block of data.results) {
      const tipo = block.type;
      const richText = block[tipo]?.rich_text;
      const texto = richText?.map(t => t.plain_text).join('') || '';

      // Detectar si ya tiene storyboard
      if (tipo === 'heading_1' && texto.includes('STORYBOARD')) {
        hasVisual = true;
      }
      if (tipo === 'heading_3' && texto.includes('STORYBOARD VISUAL')) {
        hasVisual = true;
      }

      // Extraer sección GUIÓN NARRATIVO (del formato de crea_guion)
      if (tipo === 'heading_2' && texto.includes('GUIÓN NARRATIVO')) {
        inGuion = true;
        continue;
      }
      // Fin de sección de guión
      if (inGuion && tipo === 'heading_2') {
        inGuion = false;
        continue;
      }

      // Dentro de la sección GUIÓN NARRATIVO
      if (inGuion) {
        // Las escenas tienen heading_3 con nombre y tiempo
        if (tipo === 'heading_3') {
          beats.push({ nombre: texto, frases: [], intencion: '' });
        }
        // Intención = paragraph con 🎯 (ej: "🎯 Establecer éxito desde el inicio")
        if (tipo === 'paragraph' && beats.length > 0 && texto.includes('🎯')) {
          beats[beats.length - 1].intencion = texto.replace('🎯', '').trim();
        }
        // Guión real = quote blocks (la barrita lateral)
        if (tipo === 'quote' && beats.length > 0 && texto.trim()) {
          beats[beats.length - 1].frases.push(texto);
        }
        // Fallback: numbered/bulleted list items también pueden ser frases
        if ((tipo === 'numbered_list_item' || tipo === 'bulleted_list_item') && beats.length > 0 && texto.trim()) {
          beats[beats.length - 1].frases.push(texto);
        }
      }

      // También capturar texto completo del guión para fallback
      if (richText && richText.length > 0) {
        guionTexto += texto + '\n';
      }
    }

    cursor = data.has_more ? data.next_cursor : undefined;
  } while (cursor);

  return { guionTexto, beats, hasVisual };
}

// === DETERMINAR PILAR Y ESTILO ===
function determinarPilarYEstilo(notionPage) {
  const pilarSelect = notionPage.properties?.['🟣Pilar de contenido']?.select?.name || '';
  const pilarKey = CONFIG.PILAR_MAP[pilarSelect] || null;
  const estiloKey = CONFIG.PILAR_ESTILO_MAP[pilarSelect] || 'victor_heras';
  return { pilarKey, pilarSelect, estiloKey };
}

// === ACTUALIZAR ESTADO EN NOTION ===
async function actualizarEstadoNotion(pageId, nuevoEstado) {
  const resp = await fetchWithRetry(`https://api.notion.com/v1/pages/${pageId}`, {
    method: 'PATCH',
    headers: {
      'Authorization': `Bearer ${CONFIG.NOTION_API_KEY}`,
      'Notion-Version': '2022-06-28',
      'Content-Type': 'application/json'
    },
    body: JSON.stringify({
      properties: {
        '⚫Estado': { status: { name: nuevoEstado } }
      }
    })
  });

  const data = await resp.json();
  if (data.id) {
    console.log(`   🔄 Estado → ${nuevoEstado}`);
  } else {
    console.log(`   ⚠️ Error cambiando estado: ${JSON.stringify(data).slice(0, 200)}`);
  }
}

// === ESCRIBIR LINK DE STORYBOARD EN NOTION ===
async function escribirStoryboardNotion(pageId, docUrl) {
  const resp = await fetchWithRetry(`https://api.notion.com/v1/blocks/${pageId}/children`, {
    method: 'PATCH',
    headers: {
      'Authorization': `Bearer ${CONFIG.NOTION_API_KEY}`,
      'Notion-Version': '2022-06-28',
      'Content-Type': 'application/json'
    },
    body: JSON.stringify({
      children: [
        { object: 'block', type: 'divider', divider: {} },
        {
          object: 'block', type: 'heading_2',
          heading_2: { rich_text: [{ type: 'text', text: { content: '🎬 STORYBOARD VISUAL IA' } }] }
        },
        {
          object: 'block', type: 'paragraph',
          paragraph: { rich_text: [{
            type: 'text',
            text: { content: '📄 Ver Storyboard en Drive', link: { url: docUrl } },
            annotations: { bold: true }
          }] }
        },
        {
          object: 'block', type: 'paragraph',
          paragraph: { rich_text: [{ type: 'text', text: { content: `Generado automáticamente: ${new Date().toISOString().split('T')[0]}` } }] }
        }
      ]
    })
  });

  const data = await resp.json();
  if (data.results) {
    console.log(`   ✅ Storyboard link escrito en Notion`);
  } else {
    console.log(`   ⚠️ Error escribiendo en Notion: ${JSON.stringify(data).slice(0, 200)}`);
  }
}

// === GEMINI: Embeddings y similitud ===
const POSICION = { HOOK: 'hook', MEDIO: 'medio', FINAL: 'final' };

function calcularPosicion(escenaNumero, totalEscenas) {
  if (totalEscenas <= 2) return POSICION.MEDIO;
  const porcentaje = escenaNumero / totalEscenas;
  if (porcentaje <= 0.25 || escenaNumero <= 2) return POSICION.HOOK;
  if (porcentaje >= 0.75 || escenaNumero >= totalEscenas - 1) return POSICION.FINAL;
  return POSICION.MEDIO;
}

async function generarEmbedding(texto) {
  if (!texto || !texto.trim()) {
    throw new Error('No se puede generar embedding de texto vacío');
  }
  const genAI = new GoogleGenerativeAI(CONFIG.GEMINI_API_KEY);
  const model = genAI.getGenerativeModel({ model: 'gemini-embedding-001' });
  const result = await model.embedContent(texto);
  return result.embedding.values;
}

function cosineSimilarity(a, b) {
  let dot = 0, normA = 0, normB = 0;
  for (let i = 0; i < a.length; i++) {
    dot += a[i] * b[i];
    normA += a[i] * a[i];
    normB += b[i] * b[i];
  }
  return dot / (Math.sqrt(normA) * Math.sqrt(normB));
}

// === BÚSQUEDA SEMÁNTICA DE ESCENAS ===
async function buscarEscenaReferencia(dbOrPool, videoStats, textoGuion, posicion, creador, usadasMap, ultimaEscenaId) {
  let escenas;
  if (dbOrPool.prepare) {
    // SQLite
    escenas = dbOrPool.prepare(`
      SELECT e.*, c.creador, c.url
      FROM escenas e
      JOIN contenido c ON e.video_id = c.id
      WHERE e.embedding IS NOT NULL AND c.creador = ?
    `).all(creador);
  } else {
    // PostgreSQL Pool
    const res = await dbOrPool.query(`
      SELECT e.*, c.creador, c.url
      FROM escenas e
      JOIN contenido c ON e.video_id = c.id
      WHERE e.embedding IS NOT NULL AND c.creador = $1
    `, [creador]);
    escenas = res.rows;
  }

  const filtradas = escenas.filter(e => {
    const total = videoStats[e.video_id] || 1;
    return calcularPosicion(e.escena_numero, total) === posicion;
  });

  const queryEmb = await generarEmbedding(textoGuion);
  const resultados = filtradas.map(e => {
    try {
      const emb = JSON.parse(e.embedding);
      return { ...e, similitud: cosineSimilarity(queryEmb, emb) };
    } catch {
      return null;
    }
  }).filter(Boolean);

  resultados.sort((a, b) => b.similitud - a.similitud);

  // Buscar la mejor escena: no más de 2 usos, no consecutiva
  for (const escena of resultados) {
    const usadas = usadasMap[escena.id] || 0;
    const esConsecutiva = escena.id === ultimaEscenaId;
    if (usadas >= 2) continue;
    if (esConsecutiva && usadas >= 1) continue;
    return escena;
  }

  return resultados[0] || null;
}

// === FASE 1: BIBLIA VISUAL ===
async function generarBibliaVisual(beats) {
  const guionCompleto = beats.map((b, i) => `${i + 1}. [${b.nombre}] "${b.frases.join(' ')}"`).join('\n');

  const prompt = `Eres un director creativo de contenido para redes sociales (TikTok/Reels).

GUIÓN COMPLETO DEL VIDEO:
${guionCompleto}

Crea la BIBLIA VISUAL del video: un documento que define todos los elementos visuales para que haya COHERENCIA entre todas las escenas.

Responde en JSON exacto (sin markdown, sin backticks):
{
  "protagonista": {
    "edad": "Rango de edad específico (ej: 28-32 años)",
    "genero": "Hombre/Mujer",
    "look": "Descripción física y vestimenta (ej: pelo corto, camiseta técnica azul, shorts negros)",
    "personalidad_visual": "Cómo se le ve en cámara (ej: cercano, expresivo, gesticula mucho)"
  },
  "localizaciones": {
    "principal": "Localización principal donde transcurre la mayoría del video",
    "secundarias": ["Lista de otras localizaciones si las hay"]
  },
  "estilo_visual": {
    "paleta_colores": "Colores dominantes del video",
    "iluminacion": "Tipo de luz (natural exterior, golden hour, etc.)",
    "encuadres_predominantes": "Planos más usados (plano medio, primer plano, etc.)"
  },
  "estilo_edicion": {
    "ritmo": "Velocidad general (rápido, pausado, variable)",
    "transiciones_principales": "Tipos de transición que se repiten",
    "tipografia": "Estilo de textos (fuente, colores, posición)",
    "elementos_recurrentes": "Gráficos o elementos que aparecen varias veces"
  },
  "tono": "Descripción del tono general del video en una frase"
}`;

  try {
    let text = await claudeGenerate(prompt);
    text = text.replace(/```json\n?/g, '').replace(/```\n?/g, '').trim();
    text = text.replace(/,\s*}/g, '}').replace(/,\s*]/g, ']');
    return JSON.parse(text);
  } catch (err) {
    console.log(`   ⚠️ Error generando Biblia Visual: ${err.message}`);
    return null;
  }
}

// === FASE 2: ADAPTACIÓN DE ESCENA (Claude Opus 4.5) ===
async function generarAdaptacionEscena(escenaRef, beat, visualGlobal, numEscena, totalEscenas, manualEstilo, estiloNombre) {
  const guionBeat = beat.frases.join(' ').trim() || beat.nombre;

  const prompt = `Eres un director de contenido para redes sociales. Vas a describir UNA ESCENA CONCRETA para grabar.

=== BIBLIA VISUAL DEL VIDEO (OBLIGATORIO SEGUIR) ===
${JSON.stringify(visualGlobal, null, 2)}

=== CONTEXTO ===
Esta es la ESCENA ${numEscena} de ${totalEscenas} del video.

REFERENCIA VISUAL (${estiloNombre}):
- Escenario: ${escenaRef.escenario || 'No especificado'}
- Cámara: ${escenaRef.camara_edicion || 'No especificado'}

BEAT: ${beat.nombre}
${beat.intencion ? `INTENCIÓN: ${beat.intencion}` : ''}
GUIÓN: "${guionBeat}"

Describe LA ESCENA respetando la biblia visual (mismo protagonista, mismas localizaciones, mismo estilo). La acción del protagonista debe basarse en el GUIÓN, no asumir que siempre está corriendo.

${manualEstilo ? `
=== MANUAL DE ESTILO (OBLIGATORIO SEGUIR) ===
${manualEstilo.substring(0, 3000)}
...
=== FIN MANUAL ===

REGLA CRÍTICA: El estilo de ${estiloNombre} casi NUNCA usa "persona hablando a cámara". En su lugar usa:
- B-roll de acciones (correr, estirar, mirar el reloj, atarse zapatillas)
- Planos de detalle (manos, pies, objetos)
- Planos de producto/lifestyle
- Imágenes con texto overlay grande
- POV del protagonista
NO describes "protagonista hablando a cámara mirando al espectador" a menos que sea absolutamente necesario.
` : ''}

⚠️ REGLA CRÍTICA DE BREVEDAD: Cada campo del JSON debe ser MUY BREVE, máximo 10-15 palabras. Una sola frase corta y directa. NO explicaciones largas, NO listas, NO descripciones detalladas. Como una nota rápida para el equipo de grabación.

Responde en JSON exacto (sin markdown, sin backticks):
{
  "escena_visual": {
    "localizacion": "Lugar concreto en máx 5 palabras (ej: 'Parque al amanecer')",
    "accion_protagonista": "Acción física breve (ej: 'Ata zapatillas en banco')",
    "encuadre": "Plano y ángulo breve (ej: 'Plano medio frontal')",
    "elementos_fisicos": "Objetos clave o 'Ninguno' (ej: 'Zapatillas, reloj')"
  },
  "edicion": {
    "cortes": "Tipo de corte breve (ej: 'Jump cut rápido')",
    "efectos_camara": "Efecto breve (ej: 'Zoom lento de acercamiento')"
  },
  "elementos_visuales": {
    "tipo": "texto_clave|grafismo|iconos|texto_y_grafismo|ninguno",
    "contenido": "Palabras clave breve (ej: '30% MÁS EFICIENTE')",
    "posicion": "Dónde en pantalla breve (ej: 'Centro, tamaño grande')",
    "animacion": "Cómo entra breve (ej: 'Aparece palabra a palabra')",
    "composicion_grabacion": "Posición protagonista breve (ej: 'Descentrado a la izquierda')"
  }
}`;

  try {
    let text = await claudeGenerate(prompt);
    text = text.replace(/```json\n?/g, '').replace(/```\n?/g, '').trim();
    return JSON.parse(text);
  } catch (err) {
    console.log(`   ⚠️ Error generando adaptación escena: ${err.message}`);
    return null;
  }
}

// === GENERAR IMAGEN STORYBOARD ===
async function generarImagenStoryboard(escenaDesc, guion, filename, estiloKey) {
  const genAI = new GoogleGenerativeAI(CONFIG.GEMINI_API_KEY);
  const model = genAI.getGenerativeModel({ model: 'gemini-2.5-flash-image' });

  // Instrucción anti-talking-head según el estilo
  let estiloVisualInstruccion = '';
  if (estiloKey === 'nudeproject') {
    estiloVisualInstruccion = `
🚫 ESTILO NUDE PROJECT - EVITAR TALKING HEAD
Este estilo NUNCA muestra personas hablando directamente a cámara. En su lugar:
- Muestra ACCIONES: correr, atar zapatillas, mirar reloj, estirar
- Planos de DETALLE: manos, pies, dispositivos, objetos
- B-ROLL lifestyle: café, ciudad, amanecer, parque
- POV (punto de vista del protagonista)
- Planos GENERALES de entorno
Si la escena describe "hablar", muestra al protagonista HACIENDO algo mientras se escucha la voz en off.
`;
  } else if (estiloKey === 'veneno') {
    estiloVisualInstruccion = `
🎬 ESTILO VENENO - CONVERSACIONAL
Este estilo SÍ puede mostrar personas hablando, pero de forma natural y dinámica, no estática.
`;
  }

  const fullPrompt = `Eres un ilustrador profesional de storyboard especializado en videos verticales para redes sociales (Instagram Reels, TikTok, Shorts). Genera una única imagen storyboard con estas reglas obligatorias:
${estiloVisualInstruccion}

📐 FORMATO VISUAL (MUY IMPORTANTE)
- Output: una sola imagen HORIZONTAL en formato 16:9 (widescreen)
- Aspect ratio EXACTO: 1920x1080 o similar proporción 16:9
- NO cuadrada, NO vertical
- Dentro deben aparecer 4 frames verticales tipo pantalla móvil (9:16)
- Los frames deben estar alineados en una fila: [FRAME 1] [FRAME 2] [FRAME 3] [FRAME 4]
- Cada frame parece una captura consecutiva del mismo Reel

⏱️ CONTEXTO NARRATIVO CLAVE
Esto representa una sola micro-escena de 4 a 5 segundos. Por tanto:
- Misma acción, mismo personaje, mismo lugar
- Cambios mínimos y progresivos entre frames
- No saltos bruscos ni escenas independientes

🎨 ESTILO VISUAL
- Boceto a lápiz / sketch storyboard
- Blanco y negro
- Grafito suave, minimalista, limpio

⚫ COLOR
- Todo monocromo
- Solo color permitido en: títulos, palabras clave, flechas o grafismos mínimos

🚫 NO INVENTAR
Respeta estrictamente lo que se describe:
- No añadas elementos nuevos
- No cambies entorno ni ropa
- No metas personajes extra

📝 SUBTÍTULOS Y GUIÓN (MUY IMPORTANTE)
✅ El subtítulo NO debe repetirse entero en todos los frames.
En su lugar:
- Divide el guion en 4 partes
- Cada frame muestra solo la parte correspondiente
- Debe sentirse como subtítulos progresivos de un Reel real

⭐ PALABRAS CLAVE DESTACADAS
En cada frame, si hay una palabra importante, destácala visualmente:
- con color suave
- subrayado
- tipografía diferente
Pero SOLO la keyword, no todo el texto.

📌 ESCENA A ILUSTRAR
${escenaDesc}

🗣️ GUIÓN COMPLETO (dividir en 4 partes progresivas)
"${guion}"

✅ OUTPUT FINAL
Una sola imagen storyboard con:
- 4 pantallas verticales alineadas
- continuidad real de micro-secuencia
- subtítulos repartidos progresivamente
- keywords destacadas
No incluyas explicación, solo genera la imagen.`;

  try {
    const result = await model.generateContent(fullPrompt);
    const response = result.response;

    for (const part of response.candidates[0].content.parts) {
      if (part.inlineData) {
        const imgPath = path.join(CONFIG.OUTPUT_DIR, filename);
        fs.writeFileSync(imgPath, Buffer.from(part.inlineData.data, 'base64'));
        console.log(`      📸 Storyboard guardado: ${filename}`);
        return imgPath;
      }
    }
  } catch (err) {
    console.log(`      ⚠️ Error generando imagen storyboard: ${err.message}`);
  }
  return null;
}

// === GENERAR PDF ===
// ============================================================
// GOOGLE DOCS: DocBuilder + generación nativa
// ============================================================

function hexToRgb(hex) {
  const r = /^#?([a-f\d]{2})([a-f\d]{2})([a-f\d]{2})$/i.exec(hex);
  return r ? { red: parseInt(r[1], 16) / 255, green: parseInt(r[2], 16) / 255, blue: parseInt(r[3], 16) / 255 } : { red: 0, green: 0, blue: 0 };
}

function rgb(hex) { return { color: { rgbColor: hexToRgb(hex) } }; }
function rgbWhite() { return { color: { rgbColor: { red: 1, green: 1, blue: 1 } } }; }

class DocBuilder {
  constructor() { this.requests = []; this.idx = 1; }

  text(t) {
    const start = this.idx;
    this.requests.push({ insertText: { location: { index: start }, text: t } });
    this.idx += t.length;
    return { s: start, e: this.idx };
  }

  style(s, e, ts) {
    const fields = Object.keys(ts).join(',');
    this.requests.push({ updateTextStyle: { range: { startIndex: s, endIndex: e }, textStyle: ts, fields } });
  }

  paraStyle(s, e, ps) {
    const fields = Object.keys(ps).join(',');
    this.requests.push({ updateParagraphStyle: { range: { startIndex: s, endIndex: e }, paragraphStyle: ps, fields } });
  }

  image(uri, wPt, hPt) {
    const idx = this.idx;
    this.requests.push({
      insertInlineImage: {
        location: { index: idx }, uri,
        objectSize: { width: { magnitude: wPt, unit: 'PT' }, height: { magnitude: hPt, unit: 'PT' } }
      }
    });
    this.idx += 1;
    return idx;
  }

  pageBreak() {
    this.requests.push({ insertPageBreak: { location: { index: this.idx } } });
    this.idx += 1;
  }

  heading(txt, level, bgHex, fgHex) {
    const r = this.text(txt + '\n');
    this.style(r.s, r.e - 1, { bold: true, fontSize: { magnitude: level === 1 ? 16 : level === 2 ? 13 : 11, unit: 'PT' }, foregroundColor: fgHex ? rgb(fgHex) : rgbWhite() });
    if (bgHex) this.paraStyle(r.s, r.e, { shading: { backgroundColor: rgb(bgHex) }, spaceBelow: { magnitude: 4, unit: 'PT' } });
    return r;
  }

  colorBlock(label, lines, bgHex, borderHex) {
    const lr = this.text(label + '\n');
    this.style(lr.s, lr.e - 1, { bold: true, fontSize: { magnitude: 10, unit: 'PT' }, foregroundColor: borderHex ? rgb(borderHex) : undefined });
    this.paraStyle(lr.s, lr.e, {
      shading: { backgroundColor: rgb(bgHex) },
      borderLeft: { color: rgb(borderHex), width: { magnitude: 3, unit: 'PT' }, dashStyle: 'SOLID', padding: { magnitude: 6, unit: 'PT' } }
    });
    for (const line of lines) {
      const r = this.text(line + '\n');
      const colonIdx = line.indexOf(':');
      if (colonIdx > 0) {
        this.style(r.s, r.s + colonIdx + 1, { bold: true, fontSize: { magnitude: 9, unit: 'PT' } });
        this.style(r.s + colonIdx + 1, r.e - 1, { fontSize: { magnitude: 9, unit: 'PT' } });
      } else {
        this.style(r.s, r.e - 1, { fontSize: { magnitude: 9, unit: 'PT' } });
      }
      this.paraStyle(r.s, r.e, {
        shading: { backgroundColor: rgb(bgHex) },
        borderLeft: { color: rgb(borderHex), width: { magnitude: 3, unit: 'PT' }, dashStyle: 'SOLID', padding: { magnitude: 6, unit: 'PT' } },
        indentStart: { magnitude: 14, unit: 'PT' }
      });
    }
  }

  getRequests() { return this.requests; }
}

// === Subir imagen a Drive (para incrustar en Google Doc) ===
async function uploadImageToDrive(drive, filePath, fileName, folderId) {
  if (!fs.existsSync(filePath)) return null;
  const file = await drive.files.create({
    requestBody: { name: fileName, parents: [folderId] },
    media: { mimeType: filePath.endsWith('.png') ? 'image/png' : 'image/jpeg', body: fs.createReadStream(filePath) },
    fields: 'id'
  });
  await drive.permissions.create({
    fileId: file.data.id,
    requestBody: { role: 'reader', type: 'anyone' }
  });
  return `https://drive.google.com/uc?id=${file.data.id}`;
}

// === Subir todas las imágenes de un storyboard ===
async function uploadAllImages(drive, escenas, folderId) {
  const urls = {};
  for (const r of escenas) {
    if (r.frames?.length) {
      urls[`frames_${r.num}`] = [];
      for (let j = 0; j < r.frames.length; j++) {
        const url = await uploadImageToDrive(drive, r.frames[j], `beat${r.num}_frame${j + 1}${path.extname(r.frames[j])}`, folderId);
        if (url) urls[`frames_${r.num}`].push(url);
        await new Promise(r => setTimeout(r, 200));
      }
    }
    if (r.storyboardImg) {
      urls[`storyboard_${r.num}`] = await uploadImageToDrive(drive, r.storyboardImg, `beat${r.num}_storyboard.png`, folderId);
      await new Promise(r => setTimeout(r, 200));
    }
  }
  return urls;
}

// === Construir Biblia Visual en el Doc ===
function buildBibliaVisual(b, vg) {
  b.heading('📋 BIBLIA VISUAL DEL VIDEO', 1, '#667eea');
  const tonoR = b.text((vg?.tono || '') + '\n\n');
  b.style(tonoR.s, tonoR.e - 2, { italic: true, fontSize: { magnitude: 10, unit: 'PT' }, foregroundColor: rgbWhite() });
  b.paraStyle(tonoR.s, tonoR.s + 1, { shading: { backgroundColor: rgb('#667eea') } });

  const sections = [
    { emoji: '👤', title: 'PROTAGONISTA', lines: [`Edad: ${vg?.protagonista?.edad || '?'}`, `Look: ${vg?.protagonista?.look || '?'}`, `Actitud: ${vg?.protagonista?.personalidad_visual || '?'}`] },
    { emoji: '📍', title: 'LOCALIZACIONES', lines: [`Principal: ${vg?.localizaciones?.principal || '?'}`, `Secundarias: ${vg?.localizaciones?.secundarias?.join(', ') || 'Ninguna'}`] },
    { emoji: '🎨', title: 'ESTILO VISUAL', lines: [`Paleta: ${vg?.estilo_visual?.paleta_colores || '?'}`, `Luz: ${vg?.estilo_visual?.iluminacion || '?'}`, `Planos: ${vg?.estilo_visual?.encuadres_predominantes || '?'}`] },
    { emoji: '✂️', title: 'ESTILO EDICIÓN', lines: [`Ritmo: ${vg?.estilo_edicion?.ritmo || '?'}`, `Tipografía: ${vg?.estilo_edicion?.tipografia || '?'}`, `Elementos: ${vg?.estilo_edicion?.elementos_recurrentes || '?'}`] }
  ];

  for (const s of sections) {
    const tr = b.text(`${s.emoji} ${s.title}\n`);
    b.style(tr.s, tr.e - 1, { bold: true, fontSize: { magnitude: 11, unit: 'PT' } });
    b.paraStyle(tr.s, tr.e, { shading: { backgroundColor: rgb('#eef0ff') }, spaceAbove: { magnitude: 8, unit: 'PT' } });

    for (const line of s.lines) {
      const colonIdx = line.indexOf(':');
      const lr = b.text(line + '\n');
      if (colonIdx > 0) {
        b.style(lr.s, lr.s + colonIdx + 1, { bold: true, fontSize: { magnitude: 9, unit: 'PT' } });
        b.style(lr.s + colonIdx + 1, lr.e - 1, { fontSize: { magnitude: 9, unit: 'PT' } });
      }
      b.paraStyle(lr.s, lr.e, { shading: { backgroundColor: rgb('#eef0ff') }, indentStart: { magnitude: 18, unit: 'PT' } });
    }
  }

  b.text('\n');
  b.pageBreak();
  b.text('\n');
}

// === Construir sección de cada Beat en el Doc ===
function buildBeatSection(b, r, imageUrls, estiloNombre) {
  const posColor = { hook: '#e63946', medio: '#457b9d', final: '#2a9d8f' };
  const posEmoji = { hook: '🎬', medio: '📖', final: '📢' };
  const color = posColor[r.posicion] || '#457b9d';
  const emoji = posEmoji[r.posicion] || '📖';

  // Header
  const simText = r.similitud ? ` [${(r.similitud * 100).toFixed(0)}%]` : '';
  b.heading(`${r.num}. ${r.nombre} ${emoji}${simText}`, 2, color);

  // GUIÓN
  b.colorBlock('🎤 GUIÓN', [`"${r.guion}"`], '#fff3cd', '#ffc107');

  // Frames de referencia
  const frameUrls = imageUrls[`frames_${r.num}`] || [];
  if (frameUrls.length > 0) {
    const fl = b.text(`📸 Referencia ${estiloNombre} (Escena #${r.escenaId || '?'})\n`);
    b.style(fl.s, fl.e - 1, { bold: true, fontSize: { magnitude: 8, unit: 'PT' } });
    b.paraStyle(fl.s, fl.e, { shading: { backgroundColor: rgb('#f8f9fa') } });

    for (const url of frameUrls) {
      b.image(url, 55, 98);
    }
    b.text('\n');
  }

  // ESCENA
  b.colorBlock('🎬 ESCENA (qué se graba)', [
    `📍 Localización: ${r.escena_visual?.localizacion || '?'}`,
    `🏃 Acción: ${r.escena_visual?.accion_protagonista || '?'}`,
    `📷 Encuadre: ${r.escena_visual?.encuadre || '?'}`,
    `🎒 Elementos: ${r.escena_visual?.elementos_fisicos || 'Ninguno'}`
  ], '#f0fff4', '#28a745');

  // EDICIÓN
  b.colorBlock('✂️ EDICIÓN', [
    `🔪 Cortes: ${r.edicion?.cortes || '?'}`,
    `📹 Efectos: ${r.edicion?.efectos_camara || '?'}`
  ], '#fff5f5', '#e63946');

  // ELEMENTOS VISUALES
  const evType = (r.elementos_visuales?.tipo || '?').toUpperCase();
  if (r.elementos_visuales?.tipo !== 'ninguno') {
    b.colorBlock(`🎨 ELEMENTOS VISUALES [${evType}]`, [
      `▸ ${r.elementos_visuales?.contenido || ''}`,
      `Posición: ${r.elementos_visuales?.posicion || '-'}`,
      `Animación: ${r.elementos_visuales?.animacion || '-'}`,
      `Composición: ${r.elementos_visuales?.composicion_grabacion || '-'}`
    ], '#f5f0ff', '#7c3aed');
  } else {
    b.colorBlock(`🎨 ELEMENTOS VISUALES [NINGUNO]`, ['Sin overlay'], '#f5f0ff', '#7c3aed');
  }

  // Storyboard AI
  const storyUrl = imageUrls[`storyboard_${r.num}`];
  if (storyUrl) {
    const sl = b.text('🎨 STORYBOARD VISUAL (AI Generated)\n');
    b.style(sl.s, sl.e - 1, { bold: true, fontSize: { magnitude: 10, unit: 'PT' }, foregroundColor: rgb('#667eea') });
    b.paraStyle(sl.s, sl.e, { alignment: 'CENTER' });
    b.image(storyUrl, 400, 225);
    b.text('\n');
  }

  b.text('\n');
  b.pageBreak();
  b.text('\n');
}

// === Generar Google Doc nativo (reemplaza generarPDF + subirADrive) ===
async function generarGoogleDoc(titulo, visualGlobal, escenas, estiloNombre) {
  const auth = new google.auth.GoogleAuth({
    keyFile: CONFIG.SERVICE_ACCOUNT_PATH,
    scopes: ['https://www.googleapis.com/auth/documents', 'https://www.googleapis.com/auth/drive']
  });
  const docs = google.docs({ version: 'v1', auth });
  const drive = google.drive({ version: 'v3', auth });

  // 1. Crear doc vacío
  console.log('   📝 Creando Google Doc...');
  const createRes = await docs.documents.create({
    requestBody: { title: `🎬 Storyboard: ${titulo}` }
  });
  const docId = createRes.data.documentId;
  console.log(`   ✅ Doc ID: ${docId}`);

  // 2. Mover doc a carpeta CEO Media / Storyboards / YYYY-MM
  const yearMonth = new Date().toISOString().slice(0, 7);
  const folders = [CONFIG.DRIVE_ROOT_FOLDER, CONFIG.DRIVE_SUB_FOLDER, yearMonth];
  let parentId = null;

  for (const folderName of folders) {
    const query = parentId
      ? `name='${folderName}' and mimeType='application/vnd.google-apps.folder' and '${parentId}' in parents and trashed=false`
      : `name='${folderName}' and mimeType='application/vnd.google-apps.folder' and trashed=false`;
    const res = await drive.files.list({ q: query, fields: 'files(id, name)' });
    if (res.data.files.length > 0) {
      parentId = res.data.files[0].id;
    } else {
      const folder = await drive.files.create({
        requestBody: { name: folderName, mimeType: 'application/vnd.google-apps.folder', parents: parentId ? [parentId] : [] },
        fields: 'id'
      });
      parentId = folder.data.id;
    }
  }

  // Mover doc a la carpeta destino
  if (parentId) {
    const file = await drive.files.get({ fileId: docId, fields: 'parents' });
    const prevParents = file.data.parents ? file.data.parents.join(',') : '';
    await drive.files.update({ fileId: docId, addParents: parentId, removeParents: prevParents, fields: 'id, parents' });
    console.log(`   📁 Doc movido a ${folders.join(' / ')}`);
  }

  // 3. Crear carpeta para imágenes dentro de la misma carpeta
  const imgFolder = await drive.files.create({
    requestBody: { name: `storyboard_images_${titulo.replace(/[^a-zA-Z0-9]/g, '_').substring(0, 30)}`, mimeType: 'application/vnd.google-apps.folder', parents: parentId ? [parentId] : [] },
    fields: 'id'
  });
  await drive.permissions.create({ fileId: imgFolder.data.id, requestBody: { role: 'reader', type: 'anyone' } });

  // 4. Subir imágenes a Drive
  console.log('   ☁️ Subiendo imágenes a Drive...');
  const imageUrls = await uploadAllImages(drive, escenas, imgFolder.data.id);
  const totalImages = Object.values(imageUrls).flat().filter(Boolean).length;
  console.log(`   ✅ ${totalImages} imágenes subidas`);

  // 5. Construir contenido del doc
  console.log('   🔨 Construyendo contenido...');
  const b = new DocBuilder();

  // Header
  b.heading(`🎬 Storyboard: ${titulo}`, 1, '#e63946');
  const subR = b.text(`Estilo: ${estiloNombre} | ${new Date().toLocaleDateString('es-ES')}\n\n`);
  b.style(subR.s, subR.e - 2, { fontSize: { magnitude: 10, unit: 'PT' }, foregroundColor: rgbWhite() });
  b.paraStyle(subR.s, subR.s + 1, { shading: { backgroundColor: rgb('#ff6b6b') } });

  // Biblia Visual
  buildBibliaVisual(b, visualGlobal);

  // Beats
  for (const r of escenas) {
    buildBeatSection(b, r, imageUrls, estiloNombre);
  }

  // Footer
  const fr = b.text(`\nCEO Media | Storyboard generado con Gemini AI | ${new Date().toLocaleDateString('es-ES')}\n`);
  b.style(fr.s + 1, fr.e - 1, { italic: true, fontSize: { magnitude: 9, unit: 'PT' }, foregroundColor: rgb('#999999') });
  b.paraStyle(fr.s + 1, fr.e, { alignment: 'CENTER' });

  // 6. Ejecutar batchUpdate
  console.log(`   📤 Enviando ${b.getRequests().length} requests a Docs API...`);
  await docs.documents.batchUpdate({
    documentId: docId,
    requestBody: { requests: b.getRequests() }
  });

  // 7. Hacer doc editable por cualquiera con el link
  await drive.permissions.create({
    fileId: docId,
    requestBody: { role: 'writer', type: 'anyone' }
  });

  const docUrl = `https://docs.google.com/document/d/${docId}/edit`;
  console.log(`   ✅ Google Doc: ${docUrl}`);
  return docUrl;
}

// === MAIN ===
async function main() {
  const args = parseArgs();
  console.log(`\n🎬 CREA VISUAL — Semana ${args.semana}\n`);
  console.log(`📄 Documentación: https://www.notion.so/Visual-IA-304a67fd29b280e9bc08dc1ceecae362`);
  if (args.context) console.log(`💬 Contexto: ${args.context}`);
  console.log('');

  // === FASE 0: Buscar guiones en Notion ===
  const guionesNotion = await queryNotionGuiones(args.semana);

  if (guionesNotion.length === 0) {
    console.log('⚠️ No se encontraron páginas con Estado="Creación" para esta semana.');
    console.log('   Asegúrate de que crea_guion.js se ha ejecutado primero.');
    return;
  }

  let videoStats;
  let db = null;
  let pgPool = null;
  if (process.env.DATABASE_URL) {
    const { Pool } = require('pg');
    pgPool = new Pool({ connectionString: process.env.DATABASE_URL });
    const res = await pgPool.query('SELECT video_id, COUNT(*) as total FROM escenas GROUP BY video_id');
    videoStats = res.rows.reduce((acc, r) => { acc[r.video_id] = parseInt(r.total); return acc; }, {});
  } else {
    db = new Database(CONFIG.DB_PATH, { readonly: true });
    videoStats = db.prepare('SELECT video_id, COUNT(*) as total FROM escenas GROUP BY video_id')
      .all().reduce((acc, r) => { acc[r.video_id] = r.total; return acc; }, {});
  }

  let procesados = 0;
  let saltados = 0;
  let errores = 0;

  // === FASE PREVIA: Filtrar guiones válidos ===
  const guionesValidos = [];
  for (const page of guionesNotion) {
    const pageId = page.id;
    const titulo = page.properties?.['Nombre']?.title?.[0]?.plain_text || 'Sin título';
    const { pilarKey, pilarSelect, estiloKey } = determinarPilarYEstilo(page);

    if (!pilarKey || !CONFIG.PILARES[pilarKey]) {
      console.log(`⏭️ "${titulo}" — pilar no reconocido, saltando`);
      saltados++;
      continue;
    }
    if (CONFIG.PILARES[pilarKey].tipo === 'batch') {
      console.log(`⏭️ "${titulo}" — ${pilarKey} es batch, se salta`);
      saltados++;
      continue;
    }
    if (args.pilar && args.pilar !== pilarKey) {
      console.log(`⏭️ "${titulo}" — filtro --pilar=${args.pilar}`);
      saltados++;
      continue;
    }
    if (args.dryRun) {
      console.log(`[DRY-RUN] Generaría storyboard para "${titulo}" (estilo: ${estiloKey})`);
      procesados++;
      continue;
    }
    guionesValidos.push({ page, pageId, titulo, pilarKey, estiloKey });
  }

  if (args.dryRun || guionesValidos.length === 0) {
    if (db) db.close();
    if (pgPool) await pgPool.end();
    console.log(`\n✅ CREA VISUAL completado!`);
    console.log(`   Procesados: ${procesados} | Saltados: ${saltados} | Errores: ${errores}\n`);
    return;
  }

  // === PROCESAMIENTO PARALELO (máx 4 workers) ===
  const MAX_PARALLEL = Math.min(4, guionesValidos.length);
  console.log(`\n🚀 Procesando ${guionesValidos.length} guiones en paralelo (máx ${MAX_PARALLEL} simultáneos)\n`);

  async function procesarGuion(item, workerNum) {
    const { page, pageId, titulo, pilarKey, estiloKey } = item;
    const tag = `[W${workerNum}]`;

    try {
      console.log(`${tag} 📌 "${titulo}" (pilar: ${pilarKey}, estilo: ${estiloKey})`);

      // Paso 1: Leer guión de Notion
      console.log(`${tag}    📖 Leyendo guión desde Notion...`);
      const { beats, guionTexto, hasVisual } = await leerGuionNotion(pageId);

      if (hasVisual) {
        console.log(`${tag}    ⏭️ Ya tiene storyboard, saltando`);
        return 'saltado';
      }

      if (beats.length === 0) {
        console.log(`${tag}    ⚠️ No se encontraron beats en el guión, saltando`);
        return 'saltado';
      }

      console.log(`${tag}    📄 Guión: ${beats.length} escenas/beats`);
      for (const b of beats) {
        console.log(`${tag}       📝 "${b.nombre}" → ${b.frases.length} frases${b.frases.length === 0 ? ' (fallback)' : ''}`);
      }

      // Paso 2: Cargar manual de estilo
      const estiloConfig = CONFIG.ESTILOS[estiloKey];
      const manualEstilo = leerManualEstilo(estiloKey);

      // Paso 3: FASE 1 — Generar Biblia Visual
      console.log(`${tag}    🎨 FASE 1: Generando Biblia Visual...`);
      const visualGlobal = await generarBibliaVisual(beats);

      if (!visualGlobal) {
        console.log(`${tag}    ❌ Error generando Biblia Visual`);
        return 'error';
      }

      console.log(`${tag}    ✅ Biblia Visual: ${visualGlobal.protagonista?.edad || '?'}, ${visualGlobal.localizaciones?.principal || '?'}`);

      // Paso 4: FASE 2 — Procesar cada escena
      console.log(`${tag}    🎬 FASE 2: Procesando escenas...`);
      const escenasResultado = [];
      const escenasUsadas = {};
      let ultimaEscenaId = null;

      for (let i = 0; i < beats.length; i++) {
        const beat = beats[i];
        let guionBeat = beat.frases.join(' ').trim();
        if (!guionBeat) {
          console.log(`${tag}       ⚠️ Beat "${beat.nombre}" sin frases, usando nombre`);
          guionBeat = beat.nombre;
        }
        const posicion = calcularPosicion(i + 1, beats.length);

        console.log(`${tag}       ▶️ ${i + 1}/${beats.length} ${beat.nombre}`);

        // Búsqueda semántica de escena de referencia
        const escenaRef = await buscarEscenaReferencia(
          pgPool || db, videoStats, guionBeat, posicion, estiloConfig.creador, escenasUsadas, ultimaEscenaId
        );

        if (escenaRef) {
          escenasUsadas[escenaRef.id] = (escenasUsadas[escenaRef.id] || 0) + 1;
          ultimaEscenaId = escenaRef.id;
          console.log(`${tag}       🔍 Ref: escena #${escenaRef.id} (${(escenaRef.similitud * 100).toFixed(0)}%) [uso ${escenasUsadas[escenaRef.id]}/2]`);
        }

        // Frames de referencia
        let frames = [];
        if (escenaRef) {
          const videoDir = path.join(CONFIG.FRAMES_DIR, `video_${escenaRef.video_id}`);
          if (fs.existsSync(videoDir)) {
            frames = fs.readdirSync(videoDir)
              .filter(f => f.endsWith('.jpg') || f.endsWith('.png'))
              .sort()
              .slice(0, 4)
              .map(f => path.join(videoDir, f));
          }
        }

        // Generar adaptación de escena
        let adaptacion = null;
        if (escenaRef) {
          adaptacion = await generarAdaptacionEscena(
            escenaRef, beat, visualGlobal, i + 1, beats.length, manualEstilo, estiloConfig.nombre
          );
        }

        // Generar imagen storyboard
        let storyboardImg = null;
        if (adaptacion?.escena_visual) {
          const escenaDesc = `
PROTAGONISTA: ${visualGlobal?.protagonista?.edad || 'adulto joven'}, ${visualGlobal?.protagonista?.look || 'ropa deportiva'}
LOCALIZACIÓN: ${adaptacion.escena_visual?.localizacion || visualGlobal?.localizaciones?.principal || 'exterior'}
ACCIÓN: ${adaptacion.escena_visual?.accion_protagonista || 'según guión'}
ENCUADRE: ${adaptacion.escena_visual?.encuadre || 'plano medio'}
ELEMENTOS VISUALES: ${adaptacion.elementos_visuales?.contenido || 'texto con palabras clave'}
          `.trim();

          const imgFilename = `${pageId.substring(0, 8)}_escena_${i + 1}.png`;
          storyboardImg = await generarImagenStoryboard(escenaDesc, guionBeat, imgFilename, estiloKey);
        }

        escenasResultado.push({
          num: i + 1,
          nombre: beat.nombre,
          posicion,
          guion: guionBeat,
          escenaId: escenaRef?.id,
          similitud: escenaRef?.similitud,
          frames,
          escena_visual: adaptacion?.escena_visual || {},
          edicion: adaptacion?.edicion || {},
          elementos_visuales: adaptacion?.elementos_visuales || {},
          storyboardImg
        });

        // Rate limit entre escenas
        await new Promise(r => setTimeout(r, 1000));
      }

      // Paso 5: Generar Google Doc nativo
      console.log(`${tag}    📄 Generando Google Doc...`);
      let driveUrl = '';
      try {
        driveUrl = await generarGoogleDoc(titulo, visualGlobal, escenasResultado, estiloConfig.nombre);
        console.log(`${tag}    ✅ Google Doc: ${driveUrl}`);
      } catch (err) {
        console.log(`${tag}    ❌ Error generando Google Doc: ${err.message}`);
        driveUrl = '';
      }

      // Paso 7: Escribir link en Notion
      console.log(`${tag}    📝 Escribiendo storyboard en Notion...`);
      await escribirStoryboardNotion(pageId, driveUrl);

      // Paso 8: Cambiar estado Creación → Grabar
      await actualizarEstadoNotion(pageId, 'Grabar');

      console.log(`${tag}    ✅ "${titulo}" completado!`);
      return 'ok';

    } catch (err) {
      console.log(`${tag}    ❌ Error procesando "${titulo}": ${err.message}`);
      return 'error';
    }
  }

  // Pool de workers con concurrencia limitada
  const cola = [...guionesValidos];
  const resultados = [];
  let workerCounter = 0;

  async function worker() {
    const num = ++workerCounter;
    while (cola.length > 0) {
      const item = cola.shift();
      const resultado = await procesarGuion(item, num);
      resultados.push(resultado);
    }
  }

  const workers = [];
  for (let i = 0; i < MAX_PARALLEL; i++) {
    workers.push(worker());
  }
  await Promise.all(workers);

  procesados += resultados.filter(r => r === 'ok').length;
  saltados += resultados.filter(r => r === 'saltado').length;
  errores += resultados.filter(r => r === 'error').length;

  if (db) db.close();
  if (pgPool) await pgPool.end();

  console.log(`\n✅ CREA VISUAL completado!`);
  console.log(`   Procesados: ${procesados} | Saltados: ${saltados} | Errores: ${errores}\n`);
}

main().catch(console.error);
