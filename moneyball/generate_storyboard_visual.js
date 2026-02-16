#!/usr/bin/env node
/**
 * Genera Storyboard con imágenes AI para cada escena
 */

const Database = require('better-sqlite3');
const path = require('path');
const fs = require('fs');
const { GoogleGenerativeAI } = require('@google/generative-ai');
const { execSync } = require('child_process');

// ====== CONFIGURACIÓN DE ESTILO (VARIABLE) ======
// USO: node generate_storyboard_visual.js [--estilo=victor_heras|veneno|nudeproject]
const args = process.argv.slice(2);
const estiloArg = args.find(a => a.startsWith('--estilo='));
const ESTILO = estiloArg ? estiloArg.split('=')[1] : 'victor_heras';

const ESTILOS = {
  victor_heras: { creador: 'victor_heras', archivo: 'ESTILO_VICTOR_HERAS.md', nombre: 'Victor Heras' },
  veneno: { creador: 'veneno', archivo: 'ESTILO_VENENO.md', nombre: 'Veneno' },
  nudeproject: { creador: 'nudeproject', archivo: 'ESTILO_NUDEPROJECT.md', nombre: 'Nude Project' }
};

const CONFIG_ESTILO = ESTILOS[ESTILO] || ESTILOS.victor_heras;
const ESTILO_PATH = path.join(process.env.HOME, '.openclaw/workspace/moneyball', CONFIG_ESTILO.archivo);
const MANUAL_ESTILO = fs.existsSync(ESTILO_PATH) ? fs.readFileSync(ESTILO_PATH, 'utf-8') : '';
// ====== FIN CONFIGURACIÓN DE ESTILO ======

const DB_PATH = path.join(process.env.HOME, '.openclaw/workspace/moneyball_rrss.db');
const FRAMES_DIR = path.join(process.env.HOME, '.openclaw/workspace/escenas_frames');
const GEMINI_KEY = fs.readFileSync(path.join(process.env.HOME, '.config/gemini/api_key'), 'utf-8').trim();
const OUTPUT_DIR = path.join(process.env.HOME, '.openclaw/workspace/storyboard_images');
const OUTPUT_HTML = path.join(process.env.HOME, '.openclaw/workspace/storyboard_runclub_v5.html');
const OUTPUT_PDF = path.join(process.env.HOME, '.openclaw/workspace/storyboard_runclub_v5.pdf');

// Ensure output dir exists
if (!fs.existsSync(OUTPUT_DIR)) fs.mkdirSync(OUTPUT_DIR, { recursive: true });

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
    dot += a[i] * b[i];
    normA += a[i] * a[i];
    normB += b[i] * b[i];
  }
  return dot / (Math.sqrt(normA) * Math.sqrt(normB));
}

async function buscarEscenaReferencia(db, videoStats, textoGuion, posicion, usadasMap, ultimaEscenaId) {
  // usadasMap = { escenaId: count } - cuántas veces se ha usado cada escena
  // ultimaEscenaId = ID de la escena anterior (para evitar consecutivas)
  
  const escenas = db.prepare(`
    SELECT e.*, c.creador, c.url
    FROM escenas e
    JOIN contenido c ON e.video_id = c.id
    WHERE e.embedding IS NOT NULL AND c.creador = ?
  `).all(CONFIG_ESTILO.creador);
  
  const filtradas = escenas.filter(e => {
    const total = videoStats[e.video_id] || 1;
    return calcularPosicion(e.escena_numero, total) === posicion;
  });
  
  const queryEmb = await generarEmbedding(textoGuion);
  const resultados = filtradas.map(e => ({
    ...e,
    similitud: cosineSimilarity(queryEmb, JSON.parse(e.embedding))
  }));
  
  resultados.sort((a, b) => b.similitud - a.similitud);
  
  // Buscar la mejor escena que cumpla las restricciones:
  // 1. No usada más de 2 veces
  // 2. No consecutiva (distinta a la anterior)
  for (const escena of resultados) {
    const usadas = usadasMap[escena.id] || 0;
    const esConsecutiva = escena.id === ultimaEscenaId;
    
    if (usadas >= 2) continue;  // Max 2 usos
    if (esConsecutiva && usadas >= 1) continue;  // No consecutiva si ya usada
    
    return escena;
  }
  
  // Fallback: devolver la mejor aunque se repita
  return resultados[0];
}

// FASE 1: Generar el Visual Global del video completo
async function generarVisualGlobal(beats) {
  const genAI = new GoogleGenerativeAI(GEMINI_KEY);
  const model = genAI.getGenerativeModel({ model: 'gemini-2.0-flash' });
  
  const guionCompleto = beats.map((b, i) => `${i + 1}. [${b.titulo}] "${b.guion}"`).join('\n');
  
  const prompt = `Eres un director creativo de contenido de Running para redes sociales (TikTok/Reels).

GUIÓN COMPLETO DEL VIDEO:
${guionCompleto}

Crea la BIBLIA VISUAL del video: un documento que define todos los elementos visuales para que haya COHERENCIA entre todas las escenas.

Responde en JSON exacto (sin markdown):
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
    const result = await model.generateContent(prompt);
    let text = result.response.text();
    text = text.replace(/```json\n?/g, '').replace(/```\n?/g, '').trim();
    // Fix common JSON issues
    text = text.replace(/,\s*}/g, '}').replace(/,\s*]/g, ']');
    try {
      return JSON.parse(text);
    } catch (parseErr) {
      console.error('JSON parse error, raw response:', text.substring(0, 500));
      throw parseErr;
    }
  } catch (err) {
    console.error('Error generando visual global:', err.message);
    return null;
  }
}

// FASE 2: Generar cada escena CON el contexto del visual global
async function generarAdaptacionYPrompt(escena, beat, guion, visualGlobal, numEscena, totalEscenas) {
  const genAI = new GoogleGenerativeAI(GEMINI_KEY);
  const model = genAI.getGenerativeModel({ model: 'gemini-2.0-flash' });
  
  const prompt = `Eres un director de contenido para redes sociales. Vas a describir UNA ESCENA CONCRETA para grabar.

=== BIBLIA VISUAL DEL VIDEO (OBLIGATORIO SEGUIR) ===
${JSON.stringify(visualGlobal, null, 2)}

=== CONTEXTO ===
Esta es la ESCENA ${numEscena} de ${totalEscenas} del video.

REFERENCIA VISUAL (${CONFIG_ESTILO.nombre}):
- Escenario: ${escena.escenario || 'No especificado'}
- Cámara: ${escena.camara_edicion || 'No especificado'}

BEAT: ${beat}
GUIÓN: "${guion}"

Describe LA ESCENA respetando la biblia visual (mismo protagonista, mismas localizaciones, mismo estilo). La acción del protagonista debe basarse en el GUIÓN, no asumir que siempre está corriendo.

IMPORTANTE - Aplica el estilo de ${CONFIG_ESTILO.nombre}:
${MANUAL_ESTILO ? `
=== MANUAL DE ESTILO (OBLIGATORIO SEGUIR) ===
${MANUAL_ESTILO.substring(0, 3000)}
...
=== FIN MANUAL ===

REGLA CRÍTICA: El estilo de ${CONFIG_ESTILO.nombre} casi NUNCA usa "persona hablando a cámara". En su lugar usa:
- B-roll de acciones (correr, estirar, mirar el reloj, atarse zapatillas)
- Planos de detalle (manos, pies, objetos)
- Planos de producto/lifestyle
- Imágenes con texto overlay grande
- POV del protagonista
NO describes "protagonista hablando a cámara mirando al espectador" a menos que sea absolutamente necesario.
` : `- Adapta los elementos visuales según el estilo del creador de referencia`}

Responde en JSON exacto (sin markdown):
{
  "escena_visual": {
    "localizacion": "Dónde se graba exactamente (según el guión: oficina, calle, cafetería, casa...)",
    "accion_protagonista": "Qué hace el protagonista físicamente en esta escena (según el guión)",
    "encuadre": "Tipo de plano (medio, primer plano, general) y ángulo",
    "elementos_fisicos": "Objetos físicos en escena (zapatillas, reloj, botella...) o 'Ninguno'"
  },
  "edicion": {
    "cortes": "Tipo de corte específico (seco, jump cut, transición...)",
    "efectos_camara": "Efectos de cámara (zoom lento, shake, estático...)"
  },
  "elementos_visuales": {
    "tipo": "texto_clave | grafismo | iconos | texto_y_grafismo | ninguno",
    "contenido": "Si hay texto: qué palabras/frase clave (NO el guión literal). Si hay grafismo: descripción (barra FC, gráfico 80/20, iconos...). Si ninguno: 'Sin overlay'",
    "estilo": "Cómo se ve: tipografía, color, tamaño, efectos (glow, sombra, outline...)",
    "posicion": "Dónde aparece en pantalla y tamaño relativo",
    "animacion": "Cómo entra/fluye/sale (aparece al decirlo, zoom in, palabras que se suman...)",
    "composicion_grabacion": "Cómo debe grabarse/posicionarse el protagonista para dejar espacio"
  },
  "storyboard_prompt": "Descripción para generar storyboard de 4 paneles. Protagonista: ${visualGlobal?.protagonista?.edad || 'adulto joven'}, ${visualGlobal?.protagonista?.look || 'ropa deportiva'}. Localización: ${visualGlobal?.localizaciones?.principal || 'parque'}."
}`;

  try {
    const result = await model.generateContent(prompt);
    let text = result.response.text();
    // Clean JSON
    text = text.replace(/```json\n?/g, '').replace(/```\n?/g, '').trim();
    return JSON.parse(text);
  } catch (err) {
    console.error('Error generando adaptación:', err.message);
    return null;
  }
}

async function generarImagenStoryboard(escenaDesc, guion, filename) {
  const genAI = new GoogleGenerativeAI(GEMINI_KEY);
  const model = genAI.getGenerativeModel({ model: 'gemini-2.5-flash-image' });
  
  // Instrucción anti-talking-head según el estilo
  const estiloVisualInstruccion = ESTILO === 'nudeproject' 
    ? `
🚫 ESTILO NUDE PROJECT - EVITAR TALKING HEAD
Este estilo NUNCA muestra personas hablando directamente a cámara. En su lugar:
- Muestra ACCIONES: correr, atar zapatillas, mirar reloj, estirar
- Planos de DETALLE: manos, pies, dispositivos, objetos
- B-ROLL lifestyle: café, ciudad, amanecer, parque
- POV (punto de vista del protagonista)
- Planos GENERALES de entorno
Si la escena describe "hablar", muestra al protagonista HACIENDO algo mientras se escucha la voz en off.
`
    : ESTILO === 'veneno'
    ? `
🎬 ESTILO VENENO - CONVERSACIONAL
Este estilo SÍ puede mostrar personas hablando, pero de forma natural y dinámica, no estática.
`
    : '';
  
  const fullPrompt = `Eres un ilustrador profesional de storyboard especializado en videos verticales para redes sociales (Instagram Reels, TikTok, Shorts). Genera una única imagen storyboard con estas reglas obligatorias:
${estiloVisualInstruccion}

📐 FORMATO VISUAL (MUY IMPORTANTE)
- Output: una sola imagen HORIZONTAL en formato 16:9 (widescreen, como pantalla de cine)
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
- Solo color permitido en:
  - títulos
  - palabras clave
  - flechas o grafismos mínimos

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
        const imgPath = path.join(OUTPUT_DIR, filename);
        fs.writeFileSync(imgPath, Buffer.from(part.inlineData.data, 'base64'));
        console.log(`   📸 Imagen guardada: ${filename}`);
        return imgPath;
      }
    }
  } catch (err) {
    console.error('Error generando imagen:', err.message);
  }
  return null;
}

async function main() {
  console.log(`🎬 Generando Storyboard con estilo ${CONFIG_ESTILO.nombre}...\n`);
  
  const db = new Database(DB_PATH, { readonly: true });
  const videoStats = db.prepare(`SELECT video_id, COUNT(*) as total FROM escenas GROUP BY video_id`)
    .all().reduce((acc, r) => { acc[r.video_id] = r.total; return acc; }, {});
  
  // GUIÓN: Run Club RunnerPro - Capítulo 1 V5 (cambios Carmen)
  // 6 escenas con mezcla de primer PDF + nuevos textos
  const beats = [
    { titulo: 'HOOK', posicion: POSICION.HOOK, guion: 'Este miércoles ya es real: primera salida del RunClub de RunnerPro.' },
    { titulo: 'Situación 1', posicion: POSICION.MEDIO, guion: 'Han sido días intensos preparando el primer Run Club de RunnerPro.' },
    { titulo: 'Situación 2', posicion: POSICION.MEDIO, guion: 'Y por fin ha llegado el momento.' },
    { titulo: 'Desarrollo 1', posicion: POSICION.MEDIO, guion: 'Después de pensar que no venía nadie... es oficial.', efectoEdicion: 'EFECTO: Muchas capturas rápidas de las inscripciones apareciendo en pantalla antes de "es oficial"' },
    { titulo: 'Desarrollo 2', posicion: POSICION.MEDIO, guion: 'Cada miércoles a las 7AM. Quedamos. Corremos. Desayunamos. Mismo sitio, misma hora. Al acabar, te tomas un café con gente que acaba de hacer lo mismo que tú. Y luego te vas a trabajar con la sensación de que el día ya ha merecido la pena antes de las nueve. Así que si quieres venir, no te pierdas el capítulo 1. El link está en mi bio.' },
    { titulo: 'Desenlace', posicion: POSICION.FINAL, guion: 'Nos vemos el próximo miércoles, capítulo 2.' }
  ];
  
  // ESTILO EDICIÓN RUNNI: Classic 2, blanco/negro, subtítulos centrados abajo
  const ESTILO_EDICION_RUNNI = `
REGLAS DE EDICIÓN OBLIGATORIAS:
- Tipografía: SIEMPRE "Classic 2" (letra redondita). Nunca otro tipo.
- Color de letra: SIEMPRE blanco o negro. NUNCA azul ni otros colores.
- Subtítulos: Parte inferior centrada.
- Formato: Igual que los tips/referencias de Sergio.
`;
  
  // FASE 1: Generar Visual Global
  console.log('📋 FASE 1: Generando Visual Global del video...');
  const visualGlobal = await generarVisualGlobal(beats);
  if (!visualGlobal) {
    console.error('❌ Error generando visual global');
    process.exit(1);
  }
  console.log('✅ Visual Global generado:');
  console.log(`   👤 Protagonista: ${visualGlobal.protagonista?.edad}, ${visualGlobal.protagonista?.look}`);
  console.log(`   📍 Localización: ${visualGlobal.localizaciones?.principal}`);
  console.log(`   🎨 Estilo: ${visualGlobal.tono}`);
  
  // Guardar visual global
  fs.writeFileSync(path.join(OUTPUT_DIR, 'visual_global.json'), JSON.stringify(visualGlobal, null, 2));
  
  const resultados = [];
  
  // FASE 2: Generar cada escena con contexto
  console.log('\n📋 FASE 2: Generando escenas con contexto...');
  
  // Tracking para evitar repeticiones
  const escenasUsadas = {};  // { escenaId: count }
  let ultimaEscenaId = null;
  
  for (let i = 0; i < beats.length; i++) {
    const beat = beats[i];
    console.log(`\n▶️ ${i + 1}. ${beat.titulo}`);
    
    // Buscar escena con restricciones de repetición
    const escena = await buscarEscenaReferencia(db, videoStats, beat.guion, beat.posicion, escenasUsadas, ultimaEscenaId);
    if (!escena) { console.log('   ❌ No escena'); continue; }
    
    // Actualizar tracking
    escenasUsadas[escena.id] = (escenasUsadas[escena.id] || 0) + 1;
    ultimaEscenaId = escena.id;
    
    console.log(`   ✅ Escena #${escena.id} (${(escena.similitud * 100).toFixed(0)}%) [uso ${escenasUsadas[escena.id]}/2]`);
    
    // Frames
    const videoDir = path.join(FRAMES_DIR, `video_${escena.video_id}`);
    let frames = [];
    if (fs.existsSync(videoDir)) {
      frames = fs.readdirSync(videoDir).filter(f => f.endsWith('.jpg') || f.endsWith('.png'))
        .sort().slice(0, 4).map(f => path.join(videoDir, f));
    }
    
    // Generar adaptación + prompt para imagen CON contexto visual global
    console.log('   🤖 Generando escena con contexto global...');
    const adaptacion = await generarAdaptacionYPrompt(escena, beat.titulo, beat.guion, visualGlobal, i + 1, beats.length);
    
    // Generar imagen storyboard
    let storyboardImg = null;
    if (adaptacion?.escena_visual) {
      console.log('   🎨 Generando storyboard visual...');
      // Construir descripción de escena
      const efectoExtra = beat.efectoEdicion ? `\nEFECTO EDICIÓN ESPECIAL: ${beat.efectoEdicion}` : '';
      const escenaDesc = `
PROTAGONISTA: Corredor ${visualGlobal?.protagonista?.edad || '28-35 años'}, ${visualGlobal?.protagonista?.look || 'camiseta técnica, shorts'}
LOCALIZACIÓN: ${adaptacion.escena_visual?.localizacion || visualGlobal?.localizaciones?.principal || 'Parque urbano'}
ACCIÓN: ${adaptacion.escena_visual?.accion_protagonista || 'Según guión'}
ENCUADRE: ${adaptacion.escena_visual?.encuadre || 'Plano medio'}
ELEMENTOS VISUALES: ${adaptacion.elementos_visuales?.contenido || 'Texto con palabras clave'}${efectoExtra}
      `.trim();
      storyboardImg = await generarImagenStoryboard(escenaDesc, beat.guion, `storyboard_${i + 1}_${beat.titulo.toLowerCase().replace(/\s+/g, '_')}.png`);
    }
    
    resultados.push({
      num: i + 1,
      beat: beat.titulo,
      posicion: beat.posicion,
      guion: beat.guion,
      efectoEdicion: beat.efectoEdicion || null,
      escena: { id: escena.id, video_id: escena.video_id, similitud: escena.similitud, escenario: escena.escenario, frames },
      escena_visual: adaptacion?.escena_visual || {},
      edicion: adaptacion?.edicion || {},
      elementos_visuales: adaptacion?.elementos_visuales || {},
      storyboardImg
    });
    
    await new Promise(r => setTimeout(r, 2000)); // Rate limit
  }
  
  db.close();
  
  // Generate HTML
  console.log('\n📄 Generando HTML...');
  const html = generateHTML(resultados, visualGlobal);
  fs.writeFileSync(OUTPUT_HTML, html);
  
  // Convert to PDF
  console.log('🔄 Convirtiendo a PDF...');
  try {
    execSync(`/Applications/Google\\ Chrome.app/Contents/MacOS/Google\\ Chrome --headless --disable-gpu --print-to-pdf="${OUTPUT_PDF}" "file://${OUTPUT_HTML}" 2>/dev/null`);
    console.log(`\n✅ PDF: ${OUTPUT_PDF}`);
  } catch (err) {
    console.log('Error PDF, abre HTML manualmente');
  }
}

function frameToBase64(framePath) {
  if (!fs.existsSync(framePath)) return null;
  return `data:image/${path.extname(framePath).slice(1)};base64,${fs.readFileSync(framePath).toString('base64')}`;
}

function generateHTML(resultados, visualGlobal) {
  const posColor = { hook: '#e63946', medio: '#457b9d', final: '#2a9d8f' };
  const posEmoji = { hook: '🎬', medio: '📖', final: '📢' };
  
  return `<!DOCTYPE html>
<html>
<head>
  <meta charset="UTF-8">
  <title>Storyboard Running v7</title>
  <style>
    @page { size: A4; margin: 5mm; }
    body { font-family: -apple-system, sans-serif; margin: 0; padding: 5px; color: #333; font-size: 10pt; line-height: 1.4; }
    .header { background: linear-gradient(135deg, #e63946, #ff6b6b); color: white; padding: 12px; border-radius: 8px; margin-bottom: 8px; }
    .header h1 { margin: 0; font-size: 1.4em; }
    
    .visual-global { background: linear-gradient(135deg, #667eea, #764ba2); color: white; padding: 15px; border-radius: 10px; margin-bottom: 15px; page-break-after: always; }
    .visual-global h2 { margin: 0 0 12px 0; font-size: 1.2em; }
    .visual-global-grid { display: grid; grid-template-columns: 1fr 1fr; gap: 12px; }
    .visual-global-section { background: rgba(255,255,255,0.15); padding: 10px; border-radius: 6px; }
    .visual-global-section h4 { margin: 0 0 6px 0; font-size: 0.9em; }
    .visual-global-section p { margin: 3px 0; font-size: 0.85em; opacity: 0.95; }
    
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
    
    .similitud { background: #28a745; color: white; padding: 2px 6px; border-radius: 8px; font-size: 0.75em; }
    .footer { margin-top: 15px; text-align: center; color: #666; font-size: 0.8em; border-top: 1px solid #ddd; padding-top: 10px; }
  </style>
</head>
<body>

<div class="header">
  <h1>🏃 Storyboard Running: Video Zona 2</h1>
  <p style="margin:5px 0 0 0;font-size:0.9em;">Guía completa con storyboards visuales AI | ${new Date().toLocaleDateString('es-ES')}</p>
</div>

<div class="visual-global">
  <h2>📋 BIBLIA VISUAL DEL VIDEO</h2>
  <p style="margin:0 0 12px 0;opacity:0.9;font-size:0.9em;">${visualGlobal?.tono || 'Tono definido para el video'}</p>
  <div class="visual-global-grid">
    <div class="visual-global-section">
      <h4>👤 PROTAGONISTA</h4>
      <p><strong>Edad:</strong> ${visualGlobal?.protagonista?.edad || 'Por definir'}</p>
      <p><strong>Look:</strong> ${visualGlobal?.protagonista?.look || 'Por definir'}</p>
      <p><strong>Actitud:</strong> ${visualGlobal?.protagonista?.personalidad_visual || 'Por definir'}</p>
    </div>
    <div class="visual-global-section">
      <h4>📍 LOCALIZACIONES</h4>
      <p><strong>Principal:</strong> ${visualGlobal?.localizaciones?.principal || 'Por definir'}</p>
      <p><strong>Secundarias:</strong> ${visualGlobal?.localizaciones?.secundarias?.join(', ') || 'Ninguna'}</p>
    </div>
    <div class="visual-global-section">
      <h4>🎨 ESTILO VISUAL</h4>
      <p><strong>Paleta:</strong> ${visualGlobal?.estilo_visual?.paleta_colores || 'Por definir'}</p>
      <p><strong>Luz:</strong> ${visualGlobal?.estilo_visual?.iluminacion || 'Por definir'}</p>
      <p><strong>Planos:</strong> ${visualGlobal?.estilo_visual?.encuadres_predominantes || 'Por definir'}</p>
    </div>
    <div class="visual-global-section">
      <h4>✂️ ESTILO EDICIÓN</h4>
      <p><strong>Ritmo:</strong> ${visualGlobal?.estilo_edicion?.ritmo || 'Por definir'}</p>
      <p><strong>Tipografía:</strong> ${visualGlobal?.estilo_edicion?.tipografia || 'Por definir'}</p>
      <p><strong>Elementos:</strong> ${visualGlobal?.estilo_edicion?.elementos_recurrentes || 'Por definir'}</p>
    </div>
  </div>
</div>

${resultados.map(r => `
<div class="beat">
  <div class="beat-header" style="background: ${posColor[r.posicion]}">
    <h2>${r.num}. ${r.beat} ${posEmoji[r.posicion]} <span class="similitud">${(r.escena.similitud * 100).toFixed(0)}%</span></h2>
  </div>
  <div class="beat-content">
    <div class="guion-box">
      <div class="guion-label">🎤 GUIÓN:</div>
      <div class="guion-text">"${r.guion}"</div>
      ${r.efectoEdicion ? `<div style="margin-top: 5px; padding: 4px 8px; background: #ff6b6b; color: white; border-radius: 4px; font-size: 0.75em;"><strong>⚡ EFECTO ESPECIAL:</strong> ${r.efectoEdicion}</div>` : ''}
    </div>
    
    <div class="frames-section">
      <div class="frames-label">📸 Referencia ${CONFIG_ESTILO.nombre} (Escena #${r.escena.id})</div>
      <div class="frames-row">
        ${r.escena.frames.map(f => { const b = frameToBase64(f); return b ? `<img src="${b}">` : ''; }).join('') || '<p style="color:#999;">Sin frames</p>'}
      </div>
    </div>
    
    <div class="two-columns">
      <div class="column">
        <div class="section-box escena-box">
          <h4>🎬 ESCENA (qué se graba)</h4>
          <ul>
            <li><strong>📍 Localización:</strong> ${r.escena_visual?.localizacion || 'Por definir'}</li>
            <li><strong>🏃 Acción:</strong> ${r.escena_visual?.accion_protagonista || 'Por definir'}</li>
            <li><strong>📷 Encuadre:</strong> ${r.escena_visual?.encuadre || 'Plano medio'}</li>
            <li><strong>🎒 Elementos físicos:</strong> ${r.escena_visual?.elementos_fisicos || 'Ninguno especial'}</li>
          </ul>
        </div>
      </div>
      <div class="column">
        <div class="section-box edicion-box">
          <h4>✂️ EDICIÓN</h4>
          <ul>
            <li><strong>🔪 Cortes:</strong> ${r.edicion?.cortes || 'Por definir'}</li>
            <li><strong>📹 Efectos cámara:</strong> ${r.edicion?.efectos_camara || 'Por definir'}</li>
          </ul>
        </div>
      </div>
    </div>
    
    <div class="elementos-visuales-box" style="background: #f5f0ff; padding: 6px 8px; border-radius: 4px; margin: 6px 0; border-left: 3px solid #7c3aed;">
      <h4 style="margin: 0 0 4px 0; color: #7c3aed; font-size: 0.75em;">🎨 ELEMENTOS VISUALES <span style="background: ${r.elementos_visuales?.tipo === 'ninguno' ? '#6b7280' : '#7c3aed'}; color: white; padding: 1px 6px; border-radius: 3px; font-size: 0.7em; margin-left: 6px;">${r.elementos_visuales?.tipo?.toUpperCase() || 'POR DEFINIR'}</span></h4>
      ${r.elementos_visuales?.tipo !== 'ninguno' ? `
      <div style="background: #1a1a2e; color: white; padding: 6px 10px; border-radius: 4px; margin-bottom: 4px; font-size: 0.8em; text-align: center; font-weight: bold;">
        ${r.elementos_visuales?.contenido || 'Sin definir'}
      </div>
      <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 4px; font-size: 0.7em;">
        <div><strong>Estilo:</strong> ${r.elementos_visuales?.estilo || '-'}</div>
        <div><strong>Posición:</strong> ${r.elementos_visuales?.posicion || '-'}</div>
        <div><strong>Animación:</strong> ${r.elementos_visuales?.animacion || '-'}</div>
        <div><strong>Composición:</strong> ${r.elementos_visuales?.composicion_grabacion || '-'}</div>
      </div>
      ` : '<p style="color: #6b7280; font-style: italic; margin: 0; font-size: 0.7em;">Sin overlay</p>'}</div>
    
    ${r.storyboardImg ? `
    <div class="storyboard-ai">
      <div class="storyboard-ai-label">🎨 STORYBOARD VISUAL (AI Generated)</div>
      <img src="${frameToBase64(r.storyboardImg)}" alt="Storyboard">
    </div>
    ` : ''}
  </div>
</div>
`).join('')}

<div class="footer">
  <strong>RunnerPro</strong> | Storyboards generados con Gemini AI
</div>

</body>
</html>`;
}

main().catch(console.error);
