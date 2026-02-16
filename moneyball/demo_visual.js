#!/usr/bin/env node
/**
 * DEMO VISUAL — Ejecuta el flujo completo de crea_visual.js con un guión inventado.
 * No depende de Notion ni Google Drive. Genera PDF local.
 *
 * Uso: node demo_visual.js [--estilo=victor_heras|veneno|nudeproject]
 */

const Database = require('better-sqlite3');
const fs = require('fs');
const path = require('path');
const { GoogleGenerativeAI } = require('@google/generative-ai');
const puppeteer = require('puppeteer');

// === CONFIG ===
const DB_PATH = path.join(process.env.HOME, '.openclaw/workspace/moneyball_rrss.db');
const FRAMES_DIR = path.join(process.env.HOME, '.openclaw/workspace/escenas_frames');
const GEMINI_KEY = fs.readFileSync(path.join(process.env.HOME, '.config/gemini/api_key'), 'utf8').trim();
const OUTPUT_DIR = path.join(process.env.HOME, '.openclaw/workspace/storyboards_visual_ia/demo');

if (!fs.existsSync(OUTPUT_DIR)) fs.mkdirSync(OUTPUT_DIR, { recursive: true });

const args = process.argv.slice(2);
const estiloArg = args.find(a => a.startsWith('--estilo='));
const ESTILO_KEY = estiloArg ? estiloArg.split('=')[1] : 'victor_heras';

const ESTILOS = {
  victor_heras: { creador: 'victor_heras', archivo: 'ESTILO_VICTOR_HERAS.md', nombre: 'Victor Heras' },
  veneno: { creador: 'veneno', archivo: 'ESTILO_VENENO.md', nombre: 'Veneno' },
  nudeproject: { creador: 'nudeproject', archivo: 'ESTILO_NUDEPROJECT.md', nombre: 'Nude Project' }
};

const CONFIG_ESTILO = ESTILOS[ESTILO_KEY] || ESTILOS.victor_heras;

// Leer manual de estilo
const ESTILO_PATH = path.join(process.env.HOME, '.openclaw/workspace/moneyball', CONFIG_ESTILO.archivo);
const MANUAL_ESTILO = fs.existsSync(ESTILO_PATH) ? fs.readFileSync(ESTILO_PATH, 'utf8') : '';

// === GUIÓN INVENTADO: "Por qué deberías correr en ayunas" ===
const TITULO = 'Por qué deberías correr en ayunas';
const BEATS = [
  {
    nombre: 'HOOK — Pregunta provocadora (0:00-0:03)',
    frases: ['¿Sabías que correr en ayunas puede hacerte un 30% más eficiente quemando grasa?']
  },
  {
    nombre: 'PROBLEMA — Lo que hace la mayoría (0:03-0:08)',
    frases: ['La mayoría de runners se levantan, desayunan tostadas con mantequilla, esperan una hora, y salen a correr.', 'Y luego se preguntan por qué no pierden peso.']
  },
  {
    nombre: 'DATO CLAVE — La ciencia (0:08-0:15)',
    frases: ['Cuando corres en ayunas, tu cuerpo ya ha agotado las reservas de glucógeno de la noche.', 'Así que tu único combustible disponible es la grasa.', 'Es como obligar a tu cuerpo a usar la reserva en vez del depósito principal.']
  },
  {
    nombre: 'PERO — La trampa (0:15-0:22)',
    frases: ['Pero ojo. Si corres demasiado rápido en ayunas, tu cuerpo entra en modo supervivencia.', 'Empieza a quemar músculo en vez de grasa.', 'La clave es mantener la intensidad baja. Zona 2.']
  },
  {
    nombre: 'CÓMO HACERLO — El método (0:22-0:32)',
    frases: ['Mi rutina: me levanto a las 6:30, bebo un vaso de agua con sal, y salgo a rodar 40 minutos a ritmo conversacional.', 'Sin música. Sin reloj. Solo yo y la calle.', 'Al volver, desayuno fuerte: avena, plátano y proteína.']
  },
  {
    nombre: 'CTA — Cierre (0:32-0:37)',
    frases: ['Si quieres el plan completo de 4 semanas para empezar a correr en ayunas, lo tienes en el link de mi bio.', 'Nos vemos mañana a las 7.']
  }
];

// === UTILIDADES ===
const POSICION = { HOOK: 'hook', MEDIO: 'medio', FINAL: 'final' };

function calcularPosicion(escenaNumero, totalEscenas) {
  if (totalEscenas <= 2) return POSICION.MEDIO;
  const porcentaje = escenaNumero / totalEscenas;
  if (porcentaje <= 0.25 || escenaNumero <= 2) return POSICION.HOOK;
  if (porcentaje >= 0.75 || escenaNumero >= totalEscenas - 1) return POSICION.FINAL;
  return POSICION.MEDIO;
}

async function generarEmbedding(texto) {
  const genAI = new GoogleGenerativeAI(GEMINI_KEY);
  const model = genAI.getGenerativeModel({ model: 'gemini-embedding-001' });
  const result = await model.embedContent(texto);
  return result.embedding.values;
}

function cosineSimilarity(a, b) {
  let dot = 0, normA = 0, normB = 0;
  for (let i = 0; i < a.length; i++) {
    dot += a[i] * b[i]; normA += a[i] * a[i]; normB += b[i] * b[i];
  }
  return dot / (Math.sqrt(normA) * Math.sqrt(normB));
}

// === BÚSQUEDA SEMÁNTICA ===
async function buscarEscenaReferencia(db, videoStats, textoGuion, posicion, usadasMap, ultimaEscenaId) {
  const escenas = db.prepare(`
    SELECT e.*, c.creador, c.url
    FROM escenas e JOIN contenido c ON e.video_id = c.id
    WHERE e.embedding IS NOT NULL AND c.creador = ?
  `).all(CONFIG_ESTILO.creador);

  const filtradas = escenas.filter(e => {
    const total = videoStats[e.video_id] || 1;
    return calcularPosicion(e.escena_numero, total) === posicion;
  });

  const queryEmb = await generarEmbedding(textoGuion);
  const resultados = filtradas.map(e => {
    try { return { ...e, similitud: cosineSimilarity(queryEmb, JSON.parse(e.embedding)) }; }
    catch { return null; }
  }).filter(Boolean);
  resultados.sort((a, b) => b.similitud - a.similitud);

  for (const escena of resultados) {
    const usadas = usadasMap[escena.id] || 0;
    if (usadas >= 2) continue;
    if (escena.id === ultimaEscenaId && usadas >= 1) continue;
    return escena;
  }
  return resultados[0] || null;
}

// === FASE 1: BIBLIA VISUAL ===
async function generarBibliaVisual(beats) {
  const genAI = new GoogleGenerativeAI(GEMINI_KEY);
  const model = genAI.getGenerativeModel({ model: 'gemini-3-flash-preview' });

  const guionCompleto = beats.map((b, i) => `${i + 1}. [${b.nombre}] "${b.frases.join(' ')}"`).join('\n');

  const prompt = `Eres un director creativo de contenido para redes sociales (TikTok/Reels).

GUIÓN COMPLETO DEL VIDEO:
${guionCompleto}

Crea la BIBLIA VISUAL del video: un documento que define todos los elementos visuales para que haya COHERENCIA entre todas las escenas.

Responde en JSON exacto (sin markdown):
{
  "protagonista": {
    "edad": "Rango de edad específico",
    "genero": "Hombre/Mujer",
    "look": "Descripción física y vestimenta",
    "personalidad_visual": "Cómo se le ve en cámara"
  },
  "localizaciones": {
    "principal": "Localización principal",
    "secundarias": ["Otras localizaciones"]
  },
  "estilo_visual": {
    "paleta_colores": "Colores dominantes",
    "iluminacion": "Tipo de luz",
    "encuadres_predominantes": "Planos más usados"
  },
  "estilo_edicion": {
    "ritmo": "Velocidad general",
    "transiciones_principales": "Transiciones recurrentes",
    "tipografia": "Estilo de textos",
    "elementos_recurrentes": "Elementos que se repiten"
  },
  "tono": "Descripción del tono general en una frase"
}`;

  const result = await model.generateContent(prompt);
  let text = result.response.text();
  text = text.replace(/```json\n?/g, '').replace(/```\n?/g, '').trim();
  text = text.replace(/,\s*}/g, '}').replace(/,\s*]/g, ']');
  return JSON.parse(text);
}

// === FASE 2: ADAPTACIÓN DE ESCENA ===
async function generarAdaptacionEscena(escenaRef, beat, visualGlobal, numEscena, totalEscenas) {
  const genAI = new GoogleGenerativeAI(GEMINI_KEY);
  const model = genAI.getGenerativeModel({ model: 'gemini-3-flash-preview' });
  const guionBeat = beat.frases.join(' ');

  const prompt = `Eres un director de contenido para redes sociales. Describe UNA ESCENA CONCRETA para grabar.

=== BIBLIA VISUAL (OBLIGATORIO SEGUIR) ===
${JSON.stringify(visualGlobal, null, 2)}

=== CONTEXTO ===
ESCENA ${numEscena} de ${totalEscenas}.

REFERENCIA VISUAL (${CONFIG_ESTILO.nombre}):
- Escenario: ${escenaRef.escenario || 'No especificado'}
- Cámara: ${escenaRef.camara_edicion || 'No especificado'}

BEAT: ${beat.nombre}
GUIÓN: "${guionBeat}"

La acción del protagonista debe basarse en el GUIÓN, no asumir que siempre está corriendo.

${MANUAL_ESTILO ? `=== MANUAL DE ESTILO ===\n${MANUAL_ESTILO.substring(0, 3000)}\n=== FIN MANUAL ===

REGLA CRÍTICA: El estilo de ${CONFIG_ESTILO.nombre} casi NUNCA usa "persona hablando a cámara". En su lugar usa B-roll, planos de detalle, POV, producto/lifestyle.` : ''}

Responde en JSON exacto (sin markdown):
{
  "escena_visual": {
    "localizacion": "Dónde se graba",
    "accion_protagonista": "Qué hace el protagonista (según el guión)",
    "encuadre": "Tipo de plano y ángulo",
    "elementos_fisicos": "Objetos en escena o 'Ninguno'"
  },
  "edicion": {
    "cortes": "Tipo de corte",
    "efectos_camara": "Efectos de cámara"
  },
  "elementos_visuales": {
    "tipo": "texto_clave | grafismo | iconos | texto_y_grafismo | ninguno",
    "contenido": "Palabras clave o descripción del grafismo",
    "posicion": "Dónde aparece en pantalla",
    "animacion": "Cómo entra/sale",
    "composicion_grabacion": "Cómo posicionar al protagonista"
  }
}`;

  const result = await model.generateContent(prompt);
  let text = result.response.text();
  text = text.replace(/```json\n?/g, '').replace(/```\n?/g, '').trim();
  return JSON.parse(text);
}

// === GENERAR IMAGEN STORYBOARD ===
async function generarImagenStoryboard(escenaDesc, guion, filename) {
  const genAI = new GoogleGenerativeAI(GEMINI_KEY);
  const model = genAI.getGenerativeModel({ model: 'gemini-2.5-flash-image' });

  let estiloInstr = '';
  if (ESTILO_KEY === 'nudeproject') {
    estiloInstr = '\n🚫 ESTILO NUDE PROJECT: NUNCA talking head. Muestra ACCIONES, DETALLES, B-ROLL, POV.\n';
  } else if (ESTILO_KEY === 'veneno') {
    estiloInstr = '\n🎬 ESTILO VENENO: Puede mostrar personas hablando de forma natural y dinámica.\n';
  }

  const fullPrompt = `Eres un ilustrador profesional de storyboard para videos verticales (Instagram Reels, TikTok). Genera UNA imagen storyboard:
${estiloInstr}
📐 FORMATO: Imagen HORIZONTAL 16:9. Dentro: 4 frames verticales 9:16 alineados [F1][F2][F3][F4]. Cada frame = captura consecutiva del mismo Reel.

⏱️ Micro-escena de 4-5 segundos. Misma acción, personaje, lugar. Cambios mínimos y progresivos.

🎨 ESTILO: Boceto a lápiz, blanco y negro, grafito suave. Solo color en títulos/keywords.

📝 SUBTÍTULOS: Divide el guion en 4 partes progresivas (NO repetir entero). Destaca keywords con color.

📌 ESCENA:
${escenaDesc}

🗣️ GUIÓN (dividir en 4):
"${guion}"

Genera solo la imagen, sin explicación.`;

  try {
    const result = await model.generateContent(fullPrompt);
    for (const part of result.response.candidates[0].content.parts) {
      if (part.inlineData) {
        const imgPath = path.join(OUTPUT_DIR, filename);
        fs.writeFileSync(imgPath, Buffer.from(part.inlineData.data, 'base64'));
        return imgPath;
      }
    }
  } catch (err) {
    console.log(`      ⚠️ Error imagen: ${err.message}`);
  }
  return null;
}

// === GENERAR PDF ===
function frameToBase64(framePath) {
  if (!fs.existsSync(framePath)) return null;
  return `data:image/${path.extname(framePath).slice(1)};base64,${fs.readFileSync(framePath).toString('base64')}`;
}

async function generarPDF(titulo, visualGlobal, escenas, outputPath) {
  const posColor = { hook: '#e63946', medio: '#457b9d', final: '#2a9d8f' };
  const posEmoji = { hook: '🎬', medio: '📖', final: '📢' };

  const html = `<!DOCTYPE html>
<html><head><meta charset="UTF-8"><title>Storyboard: ${titulo}</title>
<style>
  @page { size: A4; margin: 5mm; }
  body { font-family: -apple-system, sans-serif; margin: 0; padding: 5px; color: #333; font-size: 10pt; line-height: 1.4; }
  .header { background: linear-gradient(135deg, #e63946, #ff6b6b); color: white; padding: 12px; border-radius: 8px; margin-bottom: 8px; }
  .header h1 { margin: 0; font-size: 1.4em; }
  .visual-global { background: linear-gradient(135deg, #667eea, #764ba2); color: white; padding: 15px; border-radius: 10px; margin-bottom: 15px; page-break-after: always; }
  .visual-global h2 { margin: 0 0 12px 0; font-size: 1.2em; }
  .vg-grid { display: grid; grid-template-columns: 1fr 1fr; gap: 12px; }
  .vg-section { background: rgba(255,255,255,0.15); padding: 10px; border-radius: 6px; }
  .vg-section h4 { margin: 0 0 6px 0; font-size: 0.9em; }
  .vg-section p { margin: 3px 0; font-size: 0.85em; opacity: 0.95; }
  .beat { border: 1px solid #ddd; border-radius: 6px; margin: 0 0 5px 0; overflow: hidden; page-break-inside: avoid; page-break-after: always; }
  .beat:last-child { page-break-after: auto; }
  .beat-header { padding: 6px 10px; color: white; }
  .beat-header h2 { margin: 0; font-size: 0.9em; }
  .beat-content { padding: 8px; }
  .guion-box { background: #fff3cd; padding: 5px 8px; border-radius: 4px; margin-bottom: 5px; border-left: 3px solid #ffc107; }
  .guion-label { font-weight: bold; color: #856404; font-size: 0.7em; }
  .guion-text { font-size: 0.8em; margin-top: 2px; line-height: 1.3; }
  .frames-section { background: #f8f9fa; padding: 4px; border-radius: 4px; margin-bottom: 6px; border: 1px solid #dee2e6; }
  .frames-label { font-weight: bold; color: #495057; font-size: 0.7em; margin-bottom: 3px; }
  .frames-row { display: flex; gap: 3px; justify-content: center; flex-wrap: wrap; }
  .frames-row img { height: 70px; width: auto; border-radius: 3px; border: 1px solid #ccc; }
  .two-columns { display: flex; gap: 6px; margin: 5px 0; }
  .column { flex: 1; }
  .section-box { padding: 5px 6px; border-radius: 4px; height: 100%; }
  .section-box h4 { margin: 0 0 3px 0; font-size: 0.7em; }
  .section-box ul { margin: 0; padding-left: 12px; font-size: 0.7em; }
  .section-box li { margin: 1px 0; }
  .escena-box { background: #f0fff4; border-left: 2px solid #28a745; }
  .edicion-box { background: #fff5f5; border-left: 2px solid #e63946; }
  .storyboard-ai { margin-top: 6px; text-align: center; background: #f8f9fa; padding: 6px; border-radius: 4px; border: 1px solid #667eea; }
  .storyboard-ai-label { font-weight: bold; color: #667eea; font-size: 0.75em; margin-bottom: 4px; }
  .storyboard-ai img { max-width: 70%; max-height: 280px; border-radius: 4px; border: 1px solid #ddd; display: block; margin: 0 auto; }
  .ev-box { background: #f5f0ff; padding: 6px 8px; border-radius: 4px; margin: 6px 0; border-left: 3px solid #7c3aed; }
  .footer { margin-top: 15px; text-align: center; color: #666; font-size: 0.8em; border-top: 1px solid #ddd; padding-top: 10px; }
</style></head><body>

<div class="header">
  <h1>🎬 Storyboard DEMO: ${titulo}</h1>
  <p style="margin:5px 0 0 0;font-size:0.9em;">Estilo: ${CONFIG_ESTILO.nombre} | ${new Date().toLocaleDateString('es-ES')} | DEMO</p>
</div>

<div class="visual-global">
  <h2>📋 BIBLIA VISUAL DEL VIDEO</h2>
  <p style="margin:0 0 12px 0;opacity:0.9;font-size:0.9em;">${visualGlobal?.tono || ''}</p>
  <div class="vg-grid">
    <div class="vg-section">
      <h4>👤 PROTAGONISTA</h4>
      <p><strong>Edad:</strong> ${visualGlobal?.protagonista?.edad || '?'}</p>
      <p><strong>Look:</strong> ${visualGlobal?.protagonista?.look || '?'}</p>
      <p><strong>Actitud:</strong> ${visualGlobal?.protagonista?.personalidad_visual || '?'}</p>
    </div>
    <div class="vg-section">
      <h4>📍 LOCALIZACIONES</h4>
      <p><strong>Principal:</strong> ${visualGlobal?.localizaciones?.principal || '?'}</p>
      <p><strong>Secundarias:</strong> ${visualGlobal?.localizaciones?.secundarias?.join(', ') || 'Ninguna'}</p>
    </div>
    <div class="vg-section">
      <h4>🎨 ESTILO VISUAL</h4>
      <p><strong>Paleta:</strong> ${visualGlobal?.estilo_visual?.paleta_colores || '?'}</p>
      <p><strong>Luz:</strong> ${visualGlobal?.estilo_visual?.iluminacion || '?'}</p>
      <p><strong>Planos:</strong> ${visualGlobal?.estilo_visual?.encuadres_predominantes || '?'}</p>
    </div>
    <div class="vg-section">
      <h4>✂️ ESTILO EDICIÓN</h4>
      <p><strong>Ritmo:</strong> ${visualGlobal?.estilo_edicion?.ritmo || '?'}</p>
      <p><strong>Tipografía:</strong> ${visualGlobal?.estilo_edicion?.tipografia || '?'}</p>
      <p><strong>Elementos:</strong> ${visualGlobal?.estilo_edicion?.elementos_recurrentes || '?'}</p>
    </div>
  </div>
</div>

${escenas.map(r => `
<div class="beat">
  <div class="beat-header" style="background: ${posColor[r.posicion] || '#457b9d'}">
    <h2>${r.num}. ${r.nombre} ${posEmoji[r.posicion] || '📖'} ${r.similitud ? `<span style="background:#28a745;color:white;padding:2px 6px;border-radius:8px;font-size:0.75em;">${(r.similitud * 100).toFixed(0)}%</span>` : ''}</h2>
  </div>
  <div class="beat-content">
    <div class="guion-box">
      <div class="guion-label">🎤 GUIÓN:</div>
      <div class="guion-text">"${r.guion}"</div>
    </div>
    ${r.frames?.length ? `
    <div class="frames-section">
      <div class="frames-label">📸 Referencia ${CONFIG_ESTILO.nombre} (Escena #${r.escenaId || '?'})</div>
      <div class="frames-row">
        ${r.frames.map(f => { const b = frameToBase64(f); return b ? `<img src="${b}">` : ''; }).join('') || '<p style="color:#999;">Sin frames</p>'}
      </div>
    </div>` : ''}
    <div class="two-columns">
      <div class="column">
        <div class="section-box escena-box">
          <h4>🎬 ESCENA (qué se graba)</h4>
          <ul>
            <li><strong>📍 Localización:</strong> ${r.escena_visual?.localizacion || '?'}</li>
            <li><strong>🏃 Acción:</strong> ${r.escena_visual?.accion_protagonista || '?'}</li>
            <li><strong>📷 Encuadre:</strong> ${r.escena_visual?.encuadre || '?'}</li>
            <li><strong>🎒 Elementos:</strong> ${r.escena_visual?.elementos_fisicos || 'Ninguno'}</li>
          </ul>
        </div>
      </div>
      <div class="column">
        <div class="section-box edicion-box">
          <h4>✂️ EDICIÓN</h4>
          <ul>
            <li><strong>🔪 Cortes:</strong> ${r.edicion?.cortes || '?'}</li>
            <li><strong>📹 Efectos:</strong> ${r.edicion?.efectos_camara || '?'}</li>
          </ul>
        </div>
      </div>
    </div>
    <div class="ev-box">
      <h4 style="margin:0 0 4px 0;color:#7c3aed;font-size:0.75em;">🎨 ELEMENTOS VISUALES <span style="background:${r.elementos_visuales?.tipo === 'ninguno' ? '#6b7280' : '#7c3aed'};color:white;padding:1px 6px;border-radius:3px;font-size:0.7em;margin-left:6px;">${(r.elementos_visuales?.tipo || '?').toUpperCase()}</span></h4>
      ${r.elementos_visuales?.tipo !== 'ninguno' ? `
      <div style="background:#1a1a2e;color:white;padding:6px 10px;border-radius:4px;margin-bottom:4px;font-size:0.8em;text-align:center;font-weight:bold;">
        ${r.elementos_visuales?.contenido || ''}
      </div>
      <div style="display:grid;grid-template-columns:1fr 1fr;gap:4px;font-size:0.7em;">
        <div><strong>Estilo:</strong> ${r.elementos_visuales?.estilo || '-'}</div>
        <div><strong>Posición:</strong> ${r.elementos_visuales?.posicion || '-'}</div>
        <div><strong>Animación:</strong> ${r.elementos_visuales?.animacion || '-'}</div>
        <div><strong>Composición:</strong> ${r.elementos_visuales?.composicion_grabacion || '-'}</div>
      </div>` : '<p style="color:#6b7280;font-style:italic;margin:0;font-size:0.7em;">Sin overlay</p>'}
    </div>
    ${r.storyboardImg ? `
    <div class="storyboard-ai">
      <div class="storyboard-ai-label">🎨 STORYBOARD VISUAL (AI Generated)</div>
      <img src="${frameToBase64(r.storyboardImg)}" alt="Storyboard">
    </div>` : ''}
  </div>
</div>`).join('')}

<div class="footer">
  <strong>CEO Media</strong> | DEMO Storyboard generado con Gemini AI | ${new Date().toLocaleDateString('es-ES')}
</div>
</body></html>`;

  const htmlPath = outputPath.replace('.pdf', '.html');
  fs.writeFileSync(htmlPath, html);

  const browser = await puppeteer.launch({ headless: true });
  const page = await browser.newPage();
  await page.setContent(html, { waitUntil: 'networkidle0' });
  await page.pdf({
    path: outputPath, format: 'A4', printBackground: true,
    margin: { top: '20mm', bottom: '20mm', left: '15mm', right: '15mm' }
  });
  await browser.close();
  return outputPath;
}

// === MAIN ===
async function main() {
  const startTime = Date.now();
  console.log(`\n🎬 DEMO VISUAL — "${TITULO}"`);
  console.log(`   Estilo: ${CONFIG_ESTILO.nombre} (${CONFIG_ESTILO.creador})`);
  console.log(`   Beats: ${BEATS.length}`);
  console.log(`   Manual estilo: ${MANUAL_ESTILO ? `${(MANUAL_ESTILO.length / 1024).toFixed(1)} KB` : 'No encontrado'}`);
  console.log('');

  const db = new Database(DB_PATH, { readonly: true });
  const videoStats = db.prepare(`SELECT video_id, COUNT(*) as total FROM escenas GROUP BY video_id`)
    .all().reduce((acc, r) => { acc[r.video_id] = r.total; return acc; }, {});

  // ========== FASE 1: BIBLIA VISUAL ==========
  console.log('📋 FASE 1: Generando Biblia Visual...');
  const t1 = Date.now();
  const visualGlobal = await generarBibliaVisual(BEATS);
  console.log(`   ✅ Biblia Visual generada (${((Date.now() - t1) / 1000).toFixed(1)}s)`);
  console.log(`   👤 ${visualGlobal.protagonista?.edad}, ${visualGlobal.protagonista?.look}`);
  console.log(`   📍 ${visualGlobal.localizaciones?.principal}`);
  console.log(`   🎭 ${visualGlobal.tono}`);
  console.log('');

  // Guardar JSON
  fs.writeFileSync(path.join(OUTPUT_DIR, 'biblia_visual.json'), JSON.stringify(visualGlobal, null, 2));

  // ========== FASE 2: ESCENAS ==========
  console.log('🎬 FASE 2: Procesando escenas...\n');
  const escenasUsadas = {};
  let ultimaEscenaId = null;
  const resultados = [];

  for (let i = 0; i < BEATS.length; i++) {
    const beat = BEATS[i];
    const guionBeat = beat.frases.join(' ');
    const posicion = calcularPosicion(i + 1, BEATS.length);

    console.log(`   ▶️ ${i + 1}/${BEATS.length} ${beat.nombre}`);

    // 2a. Búsqueda semántica
    const t2a = Date.now();
    const escenaRef = await buscarEscenaReferencia(db, videoStats, guionBeat, posicion, escenasUsadas, ultimaEscenaId);

    if (escenaRef) {
      escenasUsadas[escenaRef.id] = (escenasUsadas[escenaRef.id] || 0) + 1;
      ultimaEscenaId = escenaRef.id;
      console.log(`      🔍 Ref: escena #${escenaRef.id} — ${(escenaRef.similitud * 100).toFixed(0)}% (${((Date.now() - t2a) / 1000).toFixed(1)}s)`);
    }

    // Frames
    let frames = [];
    if (escenaRef) {
      const videoDir = path.join(FRAMES_DIR, `video_${escenaRef.video_id}`);
      if (fs.existsSync(videoDir)) {
        frames = fs.readdirSync(videoDir).filter(f => f.endsWith('.jpg') || f.endsWith('.png'))
          .sort().slice(0, 4).map(f => path.join(videoDir, f));
      }
    }

    // 2b. Adaptación de escena
    let adaptacion = null;
    if (escenaRef) {
      const t2b = Date.now();
      adaptacion = await generarAdaptacionEscena(escenaRef, beat, visualGlobal, i + 1, BEATS.length);
      console.log(`      🧠 Adaptación: ${adaptacion?.elementos_visuales?.tipo || '?'} (${((Date.now() - t2b) / 1000).toFixed(1)}s)`);
    }

    // 2c. Imagen storyboard
    let storyboardImg = null;
    if (adaptacion?.escena_visual) {
      const escenaDesc = `PROTAGONISTA: ${visualGlobal?.protagonista?.edad || '?'}, ${visualGlobal?.protagonista?.look || '?'}
LOCALIZACIÓN: ${adaptacion.escena_visual?.localizacion || '?'}
ACCIÓN: ${adaptacion.escena_visual?.accion_protagonista || '?'}
ENCUADRE: ${adaptacion.escena_visual?.encuadre || '?'}
ELEMENTOS VISUALES: ${adaptacion.elementos_visuales?.contenido || ''}`.trim();

      const t2c = Date.now();
      storyboardImg = await generarImagenStoryboard(escenaDesc, guionBeat, `demo_escena_${i + 1}.png`);
      if (storyboardImg) console.log(`      📸 Storyboard generado (${((Date.now() - t2c) / 1000).toFixed(1)}s)`);
    }

    resultados.push({
      num: i + 1, nombre: beat.nombre, posicion, guion: guionBeat,
      escenaId: escenaRef?.id, similitud: escenaRef?.similitud, frames,
      escena_visual: adaptacion?.escena_visual || {},
      edicion: adaptacion?.edicion || {},
      elementos_visuales: adaptacion?.elementos_visuales || {},
      storyboardImg
    });

    console.log('');
    await new Promise(r => setTimeout(r, 1500)); // Rate limit
  }

  db.close();

  // ========== FASE 3: PDF ==========
  console.log('📄 FASE 3: Generando PDF...');
  const pdfPath = path.join(OUTPUT_DIR, `DEMO_${TITULO.replace(/\s+/g, '_')}_storyboard.pdf`);
  await generarPDF(TITULO, visualGlobal, resultados, pdfPath);

  const totalTime = ((Date.now() - startTime) / 1000).toFixed(0);
  console.log(`\n${'='.repeat(60)}`);
  console.log(`✅ DEMO completada en ${totalTime}s`);
  console.log(`   📄 PDF: ${pdfPath}`);
  console.log(`   📂 HTML: ${pdfPath.replace('.pdf', '.html')}`);
  console.log(`   📊 Biblia: ${path.join(OUTPUT_DIR, 'biblia_visual.json')}`);
  console.log(`${'='.repeat(60)}\n`);

  // Abrir PDF automáticamente
  try {
    require('child_process').execSync(`open "${pdfPath}"`);
  } catch {}
}

main().catch(console.error);
