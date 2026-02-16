#!/usr/bin/env node
/**
 * CREA GUION — Sistema autónomo de generación de guiones CEO Media
 *
 * Documentación: https://www.notion.so/Guion-IA-CEO-Media-303a67fd29b280188115cd8bd2fb4c87
 *
 * Uso: node crea_guion.js [--semana=YYYY-MM-DD] [--dry-run] [--pilar=victor_heras] [--context="texto libre"]
 *
 * Fases:
 * 0. Leer estrategia CEO Media desde Notion
 * 1. Buscar páginas con estado "Idea" en Notion (semana objetivo)
 * 2. Por cada página (ramificado por pilar):
 *    a. Leer idea + comentarios de Lucía desde Notion
 *    b. Histórico propio (no repetir)
 *    c. Búsqueda semántica en BBDD del referente (contenido + escenas)
 *    d. Guía de estilo (si aplica)
 *    e. MoneyBall MD (patrones, anti-patrones, fórmulas — del archivo local)
 *    f. Calcular duración target desde videos similares
 *    g. Generar guión completo con Gemini 3 Flash
 *    h. Escribir guión en Notion (append a la página)
 *    i. Cambiar estado "Idea" → "Creación"
 */

const Database = require('better-sqlite3');
const fs = require('fs');
const path = require('path');

// === CONFIGURACIÓN ===
const CONFIG = {
  DB_PATH: path.join(process.env.HOME, '.openclaw/workspace/moneyball_rrss.db'),
  NOTION_DB_ID: '13da67fd-29b2-801f-bac3-c72d4fb45bf2',  // Calendario Social Media
  NOTION_API_KEY: fs.readFileSync(path.join(process.env.HOME, '.config/notion/api_key'), 'utf8').trim(),
  GEMINI_API_KEY: fs.readFileSync(path.join(process.env.HOME, '.config/gemini/api_key'), 'utf8').trim(),
  PERFIL: 'cristobal running',

  // Estrategia CEO Media
  ESTRATEGIA_PAGE_ID: '303a67fd29b2806192cfefc5f1d95dc2',

  // MoneyBall MD por creador
  MONEYBALL_MD: {
    'victor_heras': 'victor_heras.md',
    'andreadomin_': 'andreadomin_.md',
    'veneno': 'runnerpro_veneno_content.md',
    'carokrafts': 'carokrafts.md',
    'nudeproject': 'nudeproject.md'
  },

  // Guías de estilo por pilar
  ESTILO_GUIAS: {
    'victor_heras': 'ESTILO_VICTOR_HERAS.md',
    'veneno': 'ESTILO_VENENO.md'
  },

  // Descripciones de cada pilar
  PILAR_DESCRIPTIONS: {
    'victor_heras': 'Contenido educativo de alto valor sobre running y entrenamiento. Tips, técnicas y conocimiento que convierte seguidores en leads cualificados para RunnerPro. Referente: Victor Heras.',
    'veneno': 'Build in Public de RunnerPro. Transparencia total: métricas, decisiones, retos, fracasos y victorias de construir una startup. Genera conexión emocional y confianza.',
    'post_personal': 'Mi vida como corredor. Carreras, retos personales, experiencias auténticas. Contenido para la audiencia más cercana que fortalece la comunidad.',
    'stories': 'Narrativa diaria que engancha. Behind the scenes, Q&A, día a día. La gente debe querer seguir viéndolas como una serie. Integra todo el ecosistema de contenido.'
  },

  // Mapeo pilar → referente y tipo
  PILARES: {
    'andrea': { referente: 'andreadomin_', tipo: 'batch' },
    'beltran': { referente: 'beltran', tipo: 'batch', flujoPorDefinir: true },
    'victor_heras': { referente: 'victor_heras', tipo: 'normal' },
    'veneno': { referente: 'veneno', tipo: 'normal' },
    'post_personal': { referente: 'cristobal_running', tipo: 'normal' },
    'stories': { referente: 'cristobal_running', tipo: 'normal' }
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

  DURACION_DEFAULT: 37,  // (30+45)/2
  DURACION_MAX: 60,
  DURACION_MAX_VENENO: 120
};

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

function parseArgs() {
  const args = { semana: getNextMonday(), dryRun: false, pilar: null, context: null };
  process.argv.slice(2).forEach(arg => {
    if (arg.startsWith('--semana=')) args.semana = arg.split('=')[1];
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

// === LEER MONEYBALL MD LOCAL ===
function leerMoneyballMD(creador) {
  const archivo = CONFIG.MONEYBALL_MD[creador];
  if (!archivo) {
    console.log(`   ℹ️ Sin archivo MoneyBall MD para ${creador}`);
    return '';
  }
  const mdPath = path.join(process.env.HOME, '.openclaw/workspace/moneyball/creators_md', archivo);
  if (!fs.existsSync(mdPath)) {
    console.log(`   ⚠️ Archivo no encontrado: ${mdPath}`);
    return '';
  }
  const contenido = fs.readFileSync(mdPath, 'utf8');
  console.log(`   📄 MoneyBall MD leído: ${archivo} (${(contenido.length / 1024).toFixed(1)} KB)`);
  return contenido;
}

// === LEER GUÍA DE ESTILO ===
function leerGuiaEstilo(pilar) {
  const archivo = CONFIG.ESTILO_GUIAS[pilar];
  if (!archivo) {
    console.log(`   ℹ️ Sin guía de estilo para ${pilar}`);
    return '';
  }
  const estiloPath = path.join(process.env.HOME, '.openclaw/workspace/moneyball', archivo);
  if (!fs.existsSync(estiloPath)) {
    console.log(`   ⚠️ Guía de estilo no encontrada: ${estiloPath}`);
    return '';
  }
  const contenido = fs.readFileSync(estiloPath, 'utf8');
  console.log(`   🎨 Guía de estilo leída: ${archivo} (${(contenido.length / 1024).toFixed(1)} KB)`);
  return contenido;
}

// === LEER ESTRATEGIA CEO MEDIA DESDE NOTION ===
async function leerEstrategia() {
  console.log('📋 Leyendo estrategia CEO Media desde Notion...');
  let textoCompleto = '';
  let cursor = undefined;
  let paginaNum = 0;

  do {
    const url = `https://api.notion.com/v1/blocks/${CONFIG.ESTRATEGIA_PAGE_ID}/children?page_size=100${cursor ? `&start_cursor=${cursor}` : ''}`;
    const resp = await fetchWithRetry(url, {
      headers: {
        'Authorization': `Bearer ${CONFIG.NOTION_API_KEY}`,
        'Notion-Version': '2022-06-28'
      }
    });

    const data = await resp.json();
    if (!data.results) {
      console.log('   ⚠️ Error leyendo estrategia:', JSON.stringify(data).slice(0, 200));
      break;
    }

    paginaNum++;
    for (const block of data.results) {
      const tipo = block.type;
      const richText = block[tipo]?.rich_text;
      if (richText && richText.length > 0) {
        const texto = richText.map(t => t.plain_text).join('');
        if (tipo.startsWith('heading')) {
          textoCompleto += `\n## ${texto}\n`;
        } else if (tipo === 'bulleted_list_item') {
          textoCompleto += `- ${texto}\n`;
        } else if (tipo === 'numbered_list_item') {
          textoCompleto += `1. ${texto}\n`;
        } else if (tipo === 'to_do') {
          const checked = block.to_do?.checked ? '✅' : '⬜';
          textoCompleto += `${checked} ${texto}\n`;
        } else {
          textoCompleto += `${texto}\n`;
        }
      }
    }

    cursor = data.has_more ? data.next_cursor : undefined;
  } while (cursor);

  console.log(`   ✅ Estrategia leída: ${(textoCompleto.length / 1024).toFixed(1)} KB (${paginaNum} página(s) API)`);
  return textoCompleto;
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

// === QUERY NOTION: Buscar páginas con estado "Idea" ===
async function queryNotionIdeas(semana) {
  const weekStart = semana;
  const weekEnd = addDays(semana, 6);

  console.log(`🔍 Buscando ideas en Notion: ${weekStart} → ${weekEnd}`);

  let results = [];
  let cursor = undefined;

  do {
    const body = {
      filter: {
        and: [
          { property: '⚫Estado', status: { equals: 'Idea' } },
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

  console.log(`   ✅ ${results.length} ideas encontradas`);
  return results;
}

// === LEER PÁGINA NOTION (bloques + comentarios) ===
async function leerPaginaNotion(pageId) {
  let textoCompleto = '';
  let hasGuion = false;
  let cursor = undefined;

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
      if (richText && richText.length > 0) {
        const texto = richText.map(t => t.plain_text).join('');

        // Detectar si ya tiene guión escrito
        if (tipo === 'heading_1' && texto.includes('GUIÓN:')) {
          hasGuion = true;
        }

        if (tipo.startsWith('heading')) {
          textoCompleto += `\n## ${texto}\n`;
        } else if (tipo === 'bulleted_list_item') {
          textoCompleto += `- ${texto}\n`;
        } else if (tipo === 'numbered_list_item') {
          textoCompleto += `1. ${texto}\n`;
        } else if (tipo === 'callout') {
          textoCompleto += `> ${texto}\n`;
        } else {
          textoCompleto += `${texto}\n`;
        }
      }
    }

    cursor = data.has_more ? data.next_cursor : undefined;
  } while (cursor);

  // Leer comentarios
  let comentarios = [];
  let commentCursor = undefined;

  do {
    const url = `https://api.notion.com/v1/comments?block_id=${pageId}&page_size=100${commentCursor ? `&start_cursor=${commentCursor}` : ''}`;
    const resp = await fetchWithRetry(url, {
      headers: {
        'Authorization': `Bearer ${CONFIG.NOTION_API_KEY}`,
        'Notion-Version': '2022-06-28'
      }
    });
    const data = await resp.json();
    if (data.results) {
      for (const comment of data.results) {
        const texto = comment.rich_text?.map(t => t.plain_text).join('') || '';
        if (texto) comentarios.push({ texto, fecha: comment.created_time });
      }
    }
    commentCursor = data.has_more ? data.next_cursor : undefined;
  } while (commentCursor);

  return { textoCompleto, comentarios, hasGuion };
}

// === DETERMINAR PILAR DESDE PÁGINA NOTION ===
function determinarPilar(notionPage) {
  const pilarSelect = notionPage.properties?.['🟣Pilar de contenido']?.select?.name || '';
  return CONFIG.PILAR_MAP[pilarSelect] || null;
}

// === HISTÓRICO: Últimas publicaciones propias ===
function obtenerHistorico(db, limit = 10) {
  return db.prepare(`
    SELECT url, transcripcion, semantica_inicio, formula_hook, created_at
    FROM contenido
    WHERE creador = 'cristobal_running'
    ORDER BY created_at DESC
    LIMIT ?
  `).all(limit);
}

// === DESCRIPCIONES DEL REFERENTE (para estilo de caption) ===
function leerDescripcionesReferente(db, creador, limit = 5) {
  const rows = db.prepare(`
    SELECT url, descripcion, visitas
    FROM contenido
    WHERE creador = ? AND descripcion IS NOT NULL AND descripcion != ''
    ORDER BY visitas DESC
    LIMIT ?
  `).all(creador, limit);

  console.log(`   📝 ${rows.length} descripciones del referente cargadas`);
  return rows;
}

// === ANALIZAR ESTRUCTURA DE ESCENAS DEL REFERENTE ===
function analizarEstructuraReferente(db, creador, videosSimilares) {
  // Obtener IDs de los videos similares (usamos hasta 5 mejores)
  const videoIds = videosSimilares
    .slice(0, 5)
    .map(v => v.id)
    .filter(Boolean);

  if (videoIds.length === 0) {
    console.log('   📐 Sin videos similares para analizar estructura');
    return null;
  }

  // Obtener todas las escenas de esos videos (con duración real > 1s para filtrar errores)
  const placeholders = videoIds.map(() => '?').join(',');
  const escenas = db.prepare(`
    SELECT e.video_id, e.escena_numero, e.duracion_seg, e.objetivo_visual,
           c.transcripcion, c.duracion as video_duracion, c.visitas
    FROM escenas e
    JOIN contenido c ON e.video_id = c.id
    WHERE e.video_id IN (${placeholders}) AND e.duracion_seg > 1
    ORDER BY e.video_id, e.escena_numero
  `).all(...videoIds);

  if (escenas.length === 0) {
    console.log('   📐 Sin escenas analizadas para estos videos');
    return null;
  }

  // Agrupar por video para calcular nº escenas por video
  const videoMap = {};
  for (const e of escenas) {
    if (!videoMap[e.video_id]) videoMap[e.video_id] = { escenas: [], duracion: parseFloat(e.video_duracion) || 0, visitas: e.visitas };
    videoMap[e.video_id].escenas.push(e);
  }

  // Calcular nº medio de escenas por video
  const numEscenasPorVideo = Object.values(videoMap).map(v => v.escenas.length);
  const avgNumEscenas = Math.round(numEscenasPorVideo.reduce((a, b) => a + b, 0) / numEscenasPorVideo.length);

  // Calcular patrón por posición de escena
  const posicionMap = {};
  for (const video of Object.values(videoMap)) {
    for (const e of video.escenas) {
      const pos = e.escena_numero;
      if (!posicionMap[pos]) posicionMap[pos] = { duraciones: [], intenciones: [] };
      posicionMap[pos].duraciones.push(e.duracion_seg);
      if (e.objetivo_visual) posicionMap[pos].intenciones.push(e.objetivo_visual);
    }
  }

  // Construir plantilla ordenada
  const plantilla = [];
  const posiciones = Object.keys(posicionMap).map(Number).sort((a, b) => a - b);

  for (const pos of posiciones) {
    if (pos > avgNumEscenas + 2) break; // No ir más allá del patrón medio

    const data = posicionMap[pos];
    const durs = data.duraciones;
    const avgDur = Math.round((durs.reduce((a, b) => a + b, 0) / durs.length) * 10) / 10;
    const avgPalabras = Math.round(avgDur * 2.5); // ~2.5 palabras/segundo

    // Agrupar intenciones similares (tomar la más frecuente simplificada)
    const intencionResumen = data.intenciones.length > 0
      ? _resumirIntenciones(data.intenciones)
      : 'Desarrollo narrativo';

    plantilla.push({
      posicion: pos,
      duracion_media: avgDur,
      palabras_aprox: avgPalabras,
      n_videos: durs.length,
      intencion: intencionResumen
    });
  }

  const resultado = {
    num_escenas_medio: avgNumEscenas,
    videos_analizados: Object.keys(videoMap).length,
    plantilla
  };

  console.log(`   📐 Estructura referente: ${avgNumEscenas} escenas/video (de ${resultado.videos_analizados} videos)`);
  plantilla.forEach(p => {
    console.log(`      Escena ${p.posicion}: ~${p.duracion_media}s, ~${p.palabras_aprox} palabras → ${p.intencion.slice(0, 60)}`);
  });

  return resultado;
}

// Helper: Resumir intenciones de escenas en una frase corta
function _resumirIntenciones(intenciones) {
  // Tomar keywords comunes de las intenciones
  const keywords = {};
  const stopwords = ['de','del','la','el','en','y','a','al','con','un','una','los','las','para','que','por','se','es','su','o','lo','como','más','no','pero','está','ese','esta','esa','también'];

  for (const intencion of intenciones) {
    const words = intencion.toLowerCase()
      .replace(/[^a-záéíóúñü\s]/g, '')
      .split(/\s+/)
      .filter(w => w.length > 3 && !stopwords.includes(w));

    for (const w of words) {
      keywords[w] = (keywords[w] || 0) + 1;
    }
  }

  // Top 3 keywords
  const top = Object.entries(keywords)
    .sort((a, b) => b[1] - a[1])
    .slice(0, 4)
    .map(([w]) => w);

  if (top.length === 0) return 'Desarrollo narrativo';

  // Capitalizar primera letra
  const resumen = top.join(', ');
  return resumen.charAt(0).toUpperCase() + resumen.slice(1);
}

// === BÚSQUEDA SEMÁNTICA (contenido + escenas) ===
async function busquedaSemantica(db, query, creador, limit = 5) {
  const embeddingResp = await fetch(
    `https://generativelanguage.googleapis.com/v1beta/models/gemini-embedding-001:embedContent?key=${CONFIG.GEMINI_API_KEY}`,
    {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        model: 'models/gemini-embedding-001',
        content: { parts: [{ text: query }] }
      })
    }
  );

  const embData = await embeddingResp.json();
  const queryEmb = embData.embedding?.values;

  if (!queryEmb) {
    console.log('   ⚠️ No se pudo generar embedding');
    return { contenido: [], escenas: [] };
  }

  const cosineSim = (a, b) => {
    let dot = 0, normA = 0, normB = 0;
    for (let i = 0; i < a.length; i++) {
      dot += a[i] * b[i];
      normA += a[i] * a[i];
      normB += b[i] * b[i];
    }
    return dot / (Math.sqrt(normA) * Math.sqrt(normB));
  };

  // Buscar en CONTENIDO del creador
  const videos = db.prepare(`
    SELECT id, url, transcripcion, hook, semantica_inicio, duracion, visitas, embedding
    FROM contenido
    WHERE creador = ? AND embedding IS NOT NULL
    LIMIT 100
  `).all(creador);

  const resultadosContenido = videos
    .map(v => {
      try {
        const emb = JSON.parse(v.embedding);
        return { ...v, embedding: undefined, similarity: cosineSim(queryEmb, emb) };
      } catch {
        return null;
      }
    })
    .filter(Boolean)
    .sort((a, b) => b.similarity - a.similarity)
    .slice(0, limit);

  console.log(`   🔍 Semántica: ${resultadosContenido.length} contenidos similares`);

  return { contenido: resultadosContenido };
}

// === CALCULAR DURACIÓN TARGET ===
function calcularDuracionTarget(ejemplosSimilares, pilar) {
  const duraciones = ejemplosSimilares
    .map(e => parseFloat(e.duracion))
    .filter(d => !isNaN(d) && d > 0);

  if (duraciones.length === 0) {
    console.log(`   ⏱️ Sin datos de duración → default: ${CONFIG.DURACION_DEFAULT}s`);
    return CONFIG.DURACION_DEFAULT;
  }

  const avg = duraciones.reduce((a, b) => a + b, 0) / duraciones.length;
  const maxAllowed = pilar === 'veneno' ? CONFIG.DURACION_MAX_VENENO : CONFIG.DURACION_MAX;
  const target = Math.min(Math.round(avg), maxAllowed);

  console.log(`   ⏱️ Duración target: ${target}s (media de ${duraciones.length} similares: ${avg.toFixed(1)}s)`);
  return target;
}

// === CONSTRUIR PROMPT DE GUIÓN ===
function buildGuionPrompt({
  pilar, pilarConfig, estrategia, ideaTexto, comentarios,
  historico, ejemplosSimilares, moneyballMD, estiloGuia,
  duracionTarget, descripcionesReferente, contextoExtra,
  estructuraReferente
}) {
  const comentariosTexto = comentarios.length > 0
    ? comentarios.map(c => `- "${c.texto}"`).join('\n')
    : 'Sin comentarios';

  // Construir la plantilla de escenas calibrada
  let plantillaEscenas = '';
  if (estructuraReferente && estructuraReferente.plantilla.length > 0) {
    plantillaEscenas = `
## 📐 ESTRUCTURA DE ESCENAS DEL REFERENTE (${pilarConfig.referente}) — CALIBRACIÓN OBLIGATORIA
Basado en el análisis de ${estructuraReferente.videos_analizados} videos similares del referente.
El referente usa una media de **${estructuraReferente.num_escenas_medio} escenas** por video.

**DEBES generar exactamente ${estructuraReferente.num_escenas_medio} escenas siguiendo esta plantilla:**

| Escena | Duración | Palabras | Intención narrativa |
|--------|----------|----------|---------------------|
${estructuraReferente.plantilla.map(p =>
  `| Escena ${p.posicion} | ~${p.duracion_media}s | ~${p.palabras_aprox} palabras | ${p.intencion} |`
).join('\n')}

**REGLAS DE CALIBRACIÓN:**
- Cada escena = UN BLOQUE DE TEXTO CONTINUO (exactamente lo que dice Cristóbal a cámara)
- Respeta la duración de cada escena: si la plantilla dice 8s, escribe ~20 palabras (2.5 palabras/segundo)
- Respeta la INTENCIÓN: si la escena 3 del referente es de "tensión" (3s), tu escena 3 también debe ser corta y de tensión
- La suma de duraciones de todas las escenas debe ≈ ${duracionTarget}s
- NO uses bullets ni listas dentro de la escena. Es texto corrido, como hablaría alguien a cámara.
`;
  }

  return `
Eres un guionista experto en contenido viral para redes sociales (Instagram Reels, TikTok). Genera un GUIÓN COMPLETO para grabar, basado en la idea validada.

## ESTRATEGIA CEO MEDIA
${estrategia || 'Sin estrategia disponible'}

## PILAR: ${pilar.toUpperCase()}
${CONFIG.PILAR_DESCRIPTIONS[pilar] || 'Sin descripción del pilar'}

## IDEA VALIDADA (generar guión para esta idea)
${ideaTexto}

## COMENTARIOS DEL EQUIPO (incorporar al guión)
${comentariosTexto}

## HISTÓRICO RECIENTE (NO repetir estructuras ni frases similares)
${historico.slice(0, 10).map(h => `- [${h.semantica_inicio || 'tema'}] "${(h.transcripcion || '').slice(0, 120)}..."`).join('\n')}

## TRANSCRIPCIONES SIMILARES DEL REFERENTE (${pilarConfig.referente}) — USAR COMO PLANTILLA DE RITMO Y CADENCIA
${ejemplosSimilares.slice(0, 3).map((e, i) => `
### Ejemplo ${i + 1} (${e.duracion || '?'}s, ${e.visitas || '?'} visitas)
URL: ${e.url || 'N/A'}
Hook: "${(e.hook || '').slice(0, 100)}"
Transcripción: "${(e.transcripcion || '').slice(0, 500)}..."
`).join('\n')}
${plantillaEscenas}
## MONEYBALL — ANÁLISIS COMPLETO DEL REFERENTE (${pilarConfig.referente})
Estudia estos patrones ganadores, anti-patrones, fórmulas de hook y semánticas con sus % de éxito. Usa esta información para escribir el guión.
${moneyballMD || 'Sin datos MoneyBall MD'}

${estiloGuia ? `## GUÍA DE ESTILO DEL REFERENTE\n${estiloGuia}\n` : ''}

## DESCRIPCIONES/CAPTIONS DEL REFERENTE (para estilo de caption)
${descripcionesReferente.slice(0, 3).map(d => `- "${(d.descripcion || '').slice(0, 200)}"`).join('\n') || 'Sin descripciones'}

## DURACIÓN TARGET: ${duracionTarget} segundos

## TONO DE MARCA (Cristóbal Redondo)
- Accesible, entusiasta, coloquial, humilde
- Muletillas: "es decir", "al final", "yo diría que", "depende"
- Suavizadores: "un pelín", "un poquito", "más o menos"
- NO copies el tono del referente. Adapta la ESTRUCTURA del referente al tono de Cristóbal.

${contextoExtra ? `## CONTEXTO ADICIONAL DEL USUARIO\n${contextoExtra}\n` : ''}

## REGLAS CRÍTICAS DEL GUIÓN
1. Cada escena tiene un campo "texto" que es EXACTAMENTE lo que dice Cristóbal a cámara (texto corrido, NO lista de frases).
2. Cada escena debe respetar la duración y nº de palabras de la plantilla del referente.
3. Nunca exceder ${pilar === 'veneno' ? '120' : '60'} segundos de duración total.
4. No intros largas — al grano desde el segundo 1.
5. El hook debe captar atención en menos de 1 segundo.
6. Incluye "texto_pantalla" en el hook (la keyword que aparece en pantalla).
7. La descripcion_publicacion debe seguir la estructura del referente (CTA + hashtags) pero con el tono de Cristóbal.
8. Cada escena tiene su duración calibrada por la plantilla. La suma ≈ ${duracionTarget}s.
9. Usa al menos 2 patrones ganadores (>30% éxito) del MoneyBall MD.
10. NO repitas estructuras del histórico reciente.
11. El guión debe sonar EXACTAMENTE como si lo hubiera escrito el referente en FORMA, pero con contenido de RunnerPro/CEO Media y tono de Cristóbal.
12. Máximo 15 palabras por frase dentro del texto de cada escena.

Responde SOLO con este formato JSON:
{
  "titulo": "Título del video",
  "metadata": {
    "pilar": "${pilar}",
    "duracion_target": ${duracionTarget},
    "num_escenas": ${estructuraReferente ? estructuraReferente.num_escenas_medio : 6},
    "plataforma": "Instagram",
    "objetivo": "Branding|Engagement|Leads"
  },
  "hook": {
    "texto": "Frase exacta de apertura (max 15 palabras)",
    "tipo": "Pregunta|Declaración|Número|Contraste|Advertencia",
    "texto_pantalla": "1-3 palabras clave que aparecen en pantalla"
  },
  "escenas": [
    {
      "nombre": "Nombre descriptivo de la escena",
      "intencion": "Qué busca esta escena narrativamente",
      "duracion": 8,
      "palabras_target": 20,
      "texto": "Texto corrido exacto que dice Cristóbal a cámara en esta escena. Tal cual, sin bullets."
    }
  ],
  "cta": {
    "cierre": "Frase de cierre del video",
    "accion": "Acción que pido al espectador"
  },
  "descripcion_publicacion": "Caption completo con CTA + hashtags",
  "justificacion": {
    "patrones_usados": ["Patrón 1 (X% éxito)", "Patrón 2 (Y% éxito)"],
    "referencias": ["URL o descripción de los videos de referencia usados"]
  }
}
`;
}

// === GENERAR GUIÓN CON GEMINI ===
async function generarGuion(params) {
  const prompt = buildGuionPrompt(params);

  const resp = await fetch(
    `https://generativelanguage.googleapis.com/v1beta/models/gemini-3-flash-preview:generateContent?key=${CONFIG.GEMINI_API_KEY}`,
    {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        contents: [{ parts: [{ text: prompt }] }],
        generationConfig: { temperature: 0.7, maxOutputTokens: 4000 }
      })
    }
  );

  const data = await resp.json();
  let text = data.candidates?.[0]?.content?.parts?.[0]?.text || '';

  // Limpiar markdown code blocks
  text = text.replace(/```json\n?/g, '').replace(/```\n?/g, '').trim();

  const jsonMatch = text.match(/\{[\s\S]*\}/);
  if (jsonMatch) {
    try {
      return JSON.parse(jsonMatch[0]);
    } catch (e) {
      console.log('   ⚠️ Error parseando JSON de Gemini:', e.message);
      console.log('   Raw (primeros 500 chars):', text.slice(0, 500));
      return null;
    }
  }
  console.log('   ⚠️ No se encontró JSON en respuesta de Gemini');
  return null;
}

// === CONSTRUIR BLOQUES NOTION PARA EL GUIÓN ===
function buildNotionBlocks(guion) {
  const blocks = [];

  // 1. Separador
  blocks.push({ object: 'block', type: 'divider', divider: {} });

  // 2. Título del guión
  blocks.push({
    object: 'block', type: 'heading_1',
    heading_1: { rich_text: [{ type: 'text', text: { content: `🎬 GUIÓN: ${guion.titulo}` } }] }
  });

  // 3. Metadata
  const meta = guion.metadata || {};
  blocks.push({
    object: 'block', type: 'callout',
    callout: {
      icon: { emoji: '📋' },
      rich_text: [{
        type: 'text',
        text: { content: `Pilar: ${meta.pilar || '?'} | Duración: ${meta.duracion_target || '?'}s | Plataforma: ${meta.plataforma || 'Instagram'} | Objetivo: ${meta.objetivo || '?'}` }
      }]
    }
  });

  // 4. Hook
  blocks.push({
    object: 'block', type: 'heading_2',
    heading_2: { rich_text: [{ type: 'text', text: { content: '🪝 HOOK (0-3 seg)' } }] }
  });

  const hook = guion.hook || {};
  blocks.push({
    object: 'block', type: 'quote',
    quote: { rich_text: [{ type: 'text', text: { content: hook.texto || '' } }] }
  });
  blocks.push({
    object: 'block', type: 'paragraph',
    paragraph: { rich_text: [
      { type: 'text', text: { content: 'Tipo: ' }, annotations: { bold: true } },
      { type: 'text', text: { content: `${hook.tipo || '?'} | ` } },
      { type: 'text', text: { content: 'Texto pantalla: ' }, annotations: { bold: true } },
      { type: 'text', text: { content: hook.texto_pantalla || 'ninguno' } }
    ] }
  });

  // 5. Escenas
  blocks.push({
    object: 'block', type: 'heading_2',
    heading_2: { rich_text: [{ type: 'text', text: { content: '📖 GUIÓN NARRATIVO' } }] }
  });

  for (const [idx, escena] of (guion.escenas || []).entries()) {
    // Cabecera de escena con duración y palabras
    const durLabel = escena.duracion ? `${escena.duracion}s` : (escena.tiempo || '?');
    const palabrasLabel = escena.palabras_target ? ` · ~${escena.palabras_target}p` : '';
    blocks.push({
      object: 'block', type: 'heading_3',
      heading_3: { rich_text: [{ type: 'text', text: { content: `Escena ${idx + 1} — ${escena.nombre} (${durLabel}${palabrasLabel})` } }] }
    });

    // Intención narrativa (si existe)
    if (escena.intencion) {
      blocks.push({
        object: 'block', type: 'paragraph',
        paragraph: { rich_text: [
          { type: 'text', text: { content: '🎯 ' }, annotations: { bold: true } },
          { type: 'text', text: { content: escena.intencion }, annotations: { italic: true } }
        ] }
      });
    }

    // Texto de la escena (nuevo formato: texto corrido)
    if (escena.texto) {
      blocks.push({
        object: 'block', type: 'quote',
        quote: { rich_text: [{ type: 'text', text: { content: escena.texto } }] }
      });
    }

    // Fallback: soporte legacy para lo_que_digo (por si acaso)
    if (!escena.texto && escena.lo_que_digo && escena.lo_que_digo.length > 0) {
      blocks.push({
        object: 'block', type: 'quote',
        quote: { rich_text: [{ type: 'text', text: { content: escena.lo_que_digo.join(' ') } }] }
      });
    }
  }

  // 6. CTA
  const cta = guion.cta || {};
  if (cta.cierre || cta.accion) {
    blocks.push({
      object: 'block', type: 'heading_2',
      heading_2: { rich_text: [{ type: 'text', text: { content: '📢 CTA' } }] }
    });
    if (cta.cierre) {
      blocks.push({
        object: 'block', type: 'paragraph',
        paragraph: { rich_text: [
          { type: 'text', text: { content: 'Cierre: ' }, annotations: { bold: true } },
          { type: 'text', text: { content: cta.cierre } }
        ] }
      });
    }
    if (cta.accion) {
      blocks.push({
        object: 'block', type: 'paragraph',
        paragraph: { rich_text: [
          { type: 'text', text: { content: 'Acción: ' }, annotations: { bold: true } },
          { type: 'text', text: { content: cta.accion } }
        ] }
      });
    }
  }

  // 7. Descripción publicación
  if (guion.descripcion_publicacion) {
    blocks.push({
      object: 'block', type: 'heading_2',
      heading_2: { rich_text: [{ type: 'text', text: { content: '📋 DESCRIPCIÓN PUBLICACIÓN' } }] }
    });
    blocks.push({
      object: 'block', type: 'callout',
      callout: {
        icon: { emoji: '📱' },
        rich_text: [{ type: 'text', text: { content: guion.descripcion_publicacion } }]
      }
    });
  }

  // 8. Justificación
  const just = guion.justificacion || {};
  if ((just.patrones_usados && just.patrones_usados.length) || (just.referencias && just.referencias.length)) {
    blocks.push({
      object: 'block', type: 'heading_2',
      heading_2: { rich_text: [{ type: 'text', text: { content: '📊 JUSTIFICACIÓN' } }] }
    });

    for (const patron of (just.patrones_usados || [])) {
      blocks.push({
        object: 'block', type: 'bulleted_list_item',
        bulleted_list_item: { rich_text: [{ type: 'text', text: { content: `Patrón: ${patron}` } }] }
      });
    }
    for (const ref of (just.referencias || [])) {
      const isUrl = /^https?:\/\//.test(ref);
      blocks.push({
        object: 'block', type: 'bulleted_list_item',
        bulleted_list_item: { rich_text: [{
          type: 'text',
          text: { content: isUrl ? ref : `Ref: ${ref}`, ...(isUrl ? { link: { url: ref } } : {}) }
        }] }
      });
    }
  }

  return blocks;
}

// === ESCRIBIR GUIÓN EN NOTION (append a página existente) ===
async function escribirGuionNotion(pageId, guion) {
  const blocks = buildNotionBlocks(guion);

  // Notion API limit: max 100 blocks per request
  const BATCH_SIZE = 100;
  for (let i = 0; i < blocks.length; i += BATCH_SIZE) {
    const batch = blocks.slice(i, i + BATCH_SIZE);
    const resp = await fetchWithRetry(`https://api.notion.com/v1/blocks/${pageId}/children`, {
      method: 'PATCH',
      headers: {
        'Authorization': `Bearer ${CONFIG.NOTION_API_KEY}`,
        'Notion-Version': '2022-06-28',
        'Content-Type': 'application/json'
      },
      body: JSON.stringify({ children: batch })
    });

    const data = await resp.json();
    if (!data.results) {
      console.log(`   ⚠️ Error escribiendo bloques: ${JSON.stringify(data).slice(0, 300)}`);
      return false;
    }
  }

  console.log(`   ✅ Guión escrito en Notion (${blocks.length} bloques)`);
  return true;
}

// === MAIN ===
async function main() {
  const args = parseArgs();
  console.log(`\n🎬 CREA GUIÓN — Semana ${args.semana}\n`);
  console.log(`📄 Documentación: https://www.notion.so/Guion-IA-CEO-Media-303a67fd29b280188115cd8bd2fb4c87`);
  if (args.context) console.log(`💬 Contexto: ${args.context}`);
  console.log('');

  // === FASE 0: Leer estrategia CEO Media ===
  let estrategia = '';
  if (!args.dryRun) {
    estrategia = await leerEstrategia();
  }

  // === FASE 1: Buscar ideas en Notion ===
  const ideasNotion = await queryNotionIdeas(args.semana);

  if (ideasNotion.length === 0) {
    console.log('⚠️ No se encontraron ideas con Estado="Idea" para esta semana.');
    console.log('   Asegúrate de que crea_idea.js se ha ejecutado primero.');
    return;
  }

  const db = new Database(CONFIG.DB_PATH);

  // === FASE 2: Procesar cada idea ===
  let procesados = 0;
  let saltados = 0;
  let errores = 0;

  for (const page of ideasNotion) {
    const pageId = page.id;
    const titulo = page.properties?.['Nombre']?.title?.[0]?.plain_text || 'Sin título';
    const pilar = determinarPilar(page);

    console.log(`\n📌 Procesando: "${titulo}" (pilar: ${pilar || 'desconocido'})`);

    // Validar pilar
    if (!pilar || !CONFIG.PILARES[pilar]) {
      console.log(`   ⚠️ Pilar no reconocido, saltando`);
      saltados++;
      continue;
    }

    const pilarConfig = CONFIG.PILARES[pilar];

    // Saltar batches
    if (pilarConfig.tipo === 'batch') {
      console.log(`   🚧 FLUJO POR DEFINIR — ${pilar} es batch, se salta`);
      saltados++;
      continue;
    }

    // Filtro por --pilar
    if (args.pilar && args.pilar !== pilar) {
      console.log(`   ⏭️ Saltando (filtro --pilar=${args.pilar})`);
      saltados++;
      continue;
    }

    if (args.dryRun) {
      console.log(`   [DRY-RUN] Generaría guión para "${titulo}" (pilar: ${pilar})`);
      procesados++;
      continue;
    }

    try {
      // Paso 1: Leer página Notion (idea + comentarios)
      console.log('   📖 Leyendo idea desde Notion...');
      const { textoCompleto: ideaTexto, comentarios, hasGuion } = await leerPaginaNotion(pageId);

      if (hasGuion) {
        console.log('   ⏭️ Ya tiene guión escrito, saltando');
        saltados++;
        continue;
      }

      if (!ideaTexto.trim()) {
        console.log('   ⚠️ Página vacía, saltando');
        saltados++;
        continue;
      }

      console.log(`   📄 Idea: ${(ideaTexto.length / 1024).toFixed(1)} KB, ${comentarios.length} comentarios`);

      // Paso 2: Histórico propio
      const historico = obtenerHistorico(db, 10);

      // Paso 3: Búsqueda semántica en BBDD del referente
      const queryText = titulo + ' ' + ideaTexto.slice(0, 200);
      const ejemplos = await busquedaSemantica(db, queryText, pilarConfig.referente, 5);

      // Paso 4: Guía de estilo
      const estiloGuia = leerGuiaEstilo(pilar);

      // Paso 5: MoneyBall MD (patrones, anti-patrones, fórmulas — todo viene del MD)
      const moneyballMD = leerMoneyballMD(pilarConfig.referente);

      // Paso 6: Calcular duración target
      const duracionTarget = calcularDuracionTarget(ejemplos.contenido, pilar);

      // Paso 7: Analizar estructura de escenas del referente
      console.log('   📐 Analizando estructura de escenas del referente...');
      const estructuraReferente = analizarEstructuraReferente(db, pilarConfig.referente, ejemplos.contenido);

      // Paso 8: Descripciones del referente (para caption)
      const descripcionesReferente = leerDescripcionesReferente(db, pilarConfig.referente);

      // Paso 9: Generar guión con Gemini 3 Flash
      console.log('   🤖 Generando guión con Gemini 3 Flash...');
      const guion = await generarGuion({
        pilar, pilarConfig, estrategia, ideaTexto, comentarios,
        historico,
        ejemplosSimilares: ejemplos.contenido,
        moneyballMD, estiloGuia, duracionTarget,
        descripcionesReferente,
        contextoExtra: args.context,
        estructuraReferente
      });

      if (!guion) {
        console.log('   ❌ Error generando guión');
        errores++;
        continue;
      }

      console.log(`   ✅ Guión generado: "${guion.titulo}" (${guion.escenas?.length || 0} escenas, ~${guion.metadata?.duracion_target || '?'}s)`);

      // Paso 10: Escribir guión en Notion
      console.log('   📝 Escribiendo guión en Notion...');
      const written = await escribirGuionNotion(pageId, guion);

      if (!written) {
        console.log('   ❌ Error escribiendo en Notion');
        errores++;
        continue;
      }

      // Paso 11: Actualizar estado Idea → Creación
      await actualizarEstadoNotion(pageId, 'Creación');

      procesados++;

      // Rate limit entre páginas
      await new Promise(r => setTimeout(r, 2000));

    } catch (err) {
      console.log(`   ❌ Error procesando: ${err.message}`);
      errores++;
    }
  }

  db.close();

  console.log(`\n✅ CREA GUIÓN completado!`);
  console.log(`   Procesados: ${procesados} | Saltados: ${saltados} | Errores: ${errores}\n`);
}

main().catch(console.error);
