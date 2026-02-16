#!/usr/bin/env node
/**
 * CREA IDEA — Sistema autónomo de generación de ideas CEO Media
 *
 * Documentación: https://www.notion.so/Idea-IA-CEO-Media-2f0a67fd29b28038b00ad84e6a309a8f
 *
 * Uso: node crea_idea.js [--semana=YYYY-MM-DD] [--dry-run] [--pilar=victor_heras] [--context="texto libre"]
 *
 * Fases:
 * 1. Leer estrategia CEO Media desde Notion
 * 2. Crear 14 páginas en Notion (semana siguiente)
 * 3. Generar ideas:
 *    - Pilares normales (VH, Veneno, Post, Stories): MoneyBall MD + BBDD + búsqueda semántica (contenido + escenas)
 *    - Andrea (batch): 14 ideas desde documento fuente
 *    - Beltrán (batch): 21 ideas (flujo por definir)
 * 4. Cambiar estado de cada página de "Plantilla" a "Idea"
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

  // Estrategia CEO Media (leer antes de generar)
  ESTRATEGIA_PAGE_ID: '303a67fd29b2806192cfefc5f1d95dc2',

  // Documento fuente Andrea
  ANDREA_DOC_ID: '19HDKpV-Ca9OP1WJCv1rASXGZeGUu1sP7',

  // MoneyBall MD por creador (reemplaza Google Sheets)
  MONEYBALL_MD: {
    'victor_heras': 'victor_heras.md',
    'andreadomin_': 'andreadomin_.md',
    'veneno': 'runnerpro_veneno_content.md',
    'carokrafts': 'carokrafts.md',
    'nudeproject': 'nudeproject.md'
  },

  // Descripciones de cada pilar (contexto para Gemini)
  PILAR_DESCRIPTIONS: {
    'victor_heras': 'Contenido educativo de alto valor sobre running y entrenamiento. Tips, técnicas y conocimiento que convierte seguidores en leads cualificados para RunnerPro. Referente: Victor Heras.',
    'veneno': 'Build in Public de RunnerPro. Transparencia total: métricas, decisiones, retos, fracasos y victorias de construir una startup. Genera conexión emocional y confianza.',
    'post_personal': 'Mi vida como corredor. Carreras, retos personales, experiencias auténticas. Contenido para la audiencia más cercana que fortalece la comunidad.',
    'stories': 'Narrativa diaria que engancha. Behind the scenes, Q&A, día a día. La gente debe querer seguir viéndolas como una serie. Integra todo el ecosistema de contenido.'
  },

  // Mapeo pilar → configuración
  PILARES: {
    'andrea': {
      referente: 'andreadomin_',
      dias: 'rango',
      redSocial: ['TikTok', 'Instagram'],
      tipo: 'batch',
      ideas: 14,  // 7 × 2 por si alguna se descarta
      docFuente: '19HDKpV-Ca9OP1WJCv1rASXGZeGUu1sP7'
    },
    'beltran': {
      referente: 'beltran',
      dias: 'rango',
      redSocial: ['TikTok', 'Instagram'],
      tipo: 'batch',
      ideas: 21,
      flujoPorDefinir: true
    },
    'victor_heras': {
      referente: 'victor_heras',
      dias: [0, 2, 4, 6],  // Lun, Mié, Vie, Dom
      redSocial: ['Instagram'],
      tipo: 'normal'
    },
    'veneno': {
      referente: 'veneno',
      dias: [1, 3, 5],  // Mar, Jue, Sáb
      redSocial: ['Instagram'],
      tipo: 'normal'
    },
    'post_personal': {
      referente: 'cristobal_running',
      dias: [6],  // Domingo
      redSocial: ['Instagram'],
      tipo: 'normal'
    },
    'stories': {
      referente: 'cristobal_running',
      dias: [0, 2, 4, 6],  // Lun, Mié, Vie, Dom
      redSocial: ['Stories'],
      tipo: 'normal'
    }
  }
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

// === LEER ESTRATEGIA CEO MEDIA DESDE NOTION ===
async function leerEstrategia() {
  console.log('📋 Leyendo estrategia CEO Media desde Notion...');
  let textoCompleto = '';
  let cursor = undefined;
  let paginaNum = 0;

  do {
    const url = `https://api.notion.com/v1/blocks/${CONFIG.ESTRATEGIA_PAGE_ID}/children?page_size=100${cursor ? `&start_cursor=${cursor}` : ''}`;
    const resp = await fetch(url, {
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
  const resp = await fetch(`https://api.notion.com/v1/pages/${pageId}`, {
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

// === MONEYBALL: Calcular patrones ganadores desde BBDD ===
function calcularMoneyball(db, creador) {
  console.log(`📊 Calculando MoneyBall para ${creador}...`);

  const allVisitas = db.prepare(`
    SELECT visitas FROM contenido
    WHERE creador = ? AND visitas IS NOT NULL
    ORDER BY visitas DESC
  `).all(creador).map(r => r.visitas);

  if (allVisitas.length === 0) {
    console.log(`   ⚠️ Sin datos de visitas para ${creador}`);
    return { umbralExito: 0, patronesHook: [], patronesSem: [], antiPatrones: [] };
  }

  const p75Index = Math.floor(allVisitas.length * 0.25);
  const umbralExito = allVisitas[p75Index] || 0;
  console.log(`   Umbral éxito (P75): ${umbralExito} visitas`);

  // Patrones por formula_hook
  const patronesHook = db.prepare(`
    SELECT
      formula_hook,
      COUNT(*) as total,
      SUM(CASE WHEN visitas >= ? THEN 1 ELSE 0 END) as exitosos,
      ROUND(SUM(CASE WHEN visitas >= ? THEN 1.0 ELSE 0.0 END) / COUNT(*) * 100, 1) as tasa_exito
    FROM contenido
    WHERE creador = ? AND formula_hook IS NOT NULL
    GROUP BY formula_hook
    HAVING total >= 3
    ORDER BY tasa_exito DESC
  `).all(umbralExito, umbralExito, creador);

  // Patrones por semántica
  const patronesSem = db.prepare(`
    SELECT
      semantica_inicio,
      COUNT(*) as total,
      ROUND(SUM(CASE WHEN visitas >= ? THEN 1.0 ELSE 0.0 END) / COUNT(*) * 100, 1) as tasa_exito
    FROM contenido
    WHERE creador = ? AND semantica_inicio IS NOT NULL
    GROUP BY semantica_inicio
    HAVING total >= 3
    ORDER BY tasa_exito DESC
  `).all(umbralExito, creador);

  return {
    umbralExito,
    patronesHook: patronesHook.filter(p => p.tasa_exito >= 25),
    patronesSem: patronesSem.filter(p => p.tasa_exito >= 25),
    antiPatrones: patronesHook.filter(p => p.tasa_exito < 15 && p.total >= 5)
  };
}

// === HISTÓRICO: Últimas publicaciones propias ===
function obtenerHistorico(db, limit = 10) {
  const rows = db.prepare(`
    SELECT url, transcripcion, semantica_inicio, formula_hook, created_at
    FROM contenido
    WHERE creador = 'cristobal_running'
    ORDER BY created_at DESC
    LIMIT ?
  `).all(limit);

  return rows;
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

  // === Buscar en CONTENIDO del creador (RAMIFICACIÓN POR PILAR) ===
  const videos = db.prepare(`
    SELECT id, url, transcripcion, hook, semantica_inicio, embedding
    FROM contenido
    WHERE creador = ? AND embedding IS NOT NULL
    LIMIT 100
  `).all(creador);

  const resultadosContenido = videos
    .map(v => {
      try {
        const emb = JSON.parse(v.embedding);
        return { ...v, similarity: cosineSim(queryEmb, emb) };
      } catch {
        return null;
      }
    })
    .filter(Boolean)
    .sort((a, b) => b.similarity - a.similarity)
    .slice(0, limit);

  // === Buscar en ESCENAS del creador (RAMIFICACIÓN POR PILAR) ===
  const escenas = db.prepare(`
    SELECT e.id, e.video_url, e.escena_numero, e.descripcion_completa,
           e.escenario, e.objetivo_visual, e.edicion_visual, e.camara_edicion, e.embedding
    FROM escenas e
    JOIN contenido c ON e.video_id = c.id
    WHERE c.creador = ? AND e.embedding IS NOT NULL
    LIMIT 200
  `).all(creador);

  const resultadosEscenas = escenas
    .map(e => {
      try {
        const emb = JSON.parse(e.embedding);
        return { ...e, similarity: cosineSim(queryEmb, emb) };
      } catch {
        return null;
      }
    })
    .filter(Boolean)
    .sort((a, b) => b.similarity - a.similarity)
    .slice(0, limit);

  console.log(`   🔍 Semántica: ${resultadosContenido.length} contenidos + ${resultadosEscenas.length} escenas`);

  return { contenido: resultadosContenido, escenas: resultadosEscenas };
}

// === GENERAR IDEA CON GEMINI ===
async function generarIdea(pilar, pilarConfig, moneyball, historico, ejemplos, moneyballMD, estrategia, contextoExtra) {
  const prompt = `
Eres un experto en contenido viral para redes sociales. Genera UNA idea de contenido para el pilar "${pilar}" de CEO Media (marca personal de Cristóbal Redondo, CEO de RunnerPro).

## ESTRATEGIA CEO MEDIA
${estrategia || 'Sin estrategia disponible'}

## PILAR: ${pilar.toUpperCase()}
${CONFIG.PILAR_DESCRIPTIONS[pilar] || 'Sin descripción del pilar'}

## MONEYBALL COMPLETO (Análisis del referente: ${pilarConfig.referente})
${moneyballMD || 'Sin datos MoneyBall MD disponibles'}

## PATRONES GANADORES (MoneyBall BBDD)
${moneyball.patronesHook.slice(0, 5).map(p => `- ${p.formula_hook}: ${p.tasa_exito}% éxito (${p.total} videos)`).join('\n') || 'Sin datos suficientes'}

## SEMÁNTICA QUE FUNCIONA
${moneyball.patronesSem.slice(0, 5).map(p => `- ${p.semantica_inicio}: ${p.tasa_exito}% éxito`).join('\n') || 'Sin datos suficientes'}

## ANTI-PATRONES (EVITAR)
${moneyball.antiPatrones.map(p => `- ${p.formula_hook}: solo ${p.tasa_exito}% éxito`).join('\n') || 'Ninguno identificado'}

## HISTÓRICO RECIENTE (no repetir)
${historico.slice(0, 5).map(h => `- ${h.semantica_inicio || 'tema'}: "${(h.transcripcion || '').slice(0, 100)}..."`).join('\n')}

## EJEMPLOS REALES DEL REFERENTE (contenido)
${ejemplos.contenido.slice(0, 3).map(e => `- [${e.url}] "${(e.transcripcion || '').slice(0, 150)}..."`).join('\n') || 'Sin ejemplos de contenido'}

## ESCENAS REALES DEL REFERENTE (visual)
${ejemplos.escenas.slice(0, 3).map(e => `- [Escena ${e.escena_numero}] ${e.escenario || ''} | Visual: ${e.objetivo_visual || ''} | Edición: ${e.edicion_visual || ''}`).join('\n') || 'Sin escenas disponibles'}

## TONO DE MARCA
- Accesible, entusiasta, coloquial, humilde
- Muletillas: "es decir", "al final", "yo diría que", "depende"
- Suavizadores: "un pelín", "un poquito", "más o menos"
${contextoExtra ? `\n## CONTEXTO ADICIONAL DEL USUARIO\n${contextoExtra}\n` : ''}
## REGLAS
1. Usa al menos 2 patrones ganadores (>30% éxito)
2. NO repitas temáticas del histórico reciente
3. El hook debe ser una frase exacta replicando la fórmula ganadora
4. Adapta el contenido a running/RunnerPro
5. Justifica con datos reales del MoneyBall
6. Mantén el tono de Cristóbal (no copies el tono del referente)

Responde SOLO con este formato JSON:
{
  "titulo": "Título descriptivo del episodio",
  "tension": "El conflicto o pregunta que genera enganche",
  "hook": "Frase exacta de apertura",
  "semantica_inicio": "Cómo arranca el mensaje",
  "semantica_ruta": "Cómo se desarrolla",
  "visual_ruta": "Qué se ve en pantalla",
  "probabilidad_exito": 45,
  "patrones_utilizados": ["Patrón 1 (X%)", "Patrón 2 (Y%)"],
  "antipatrones_evitados": ["Antipatrón 1"],
  "justificacion": "Por qué esta idea tiene alta probabilidad de éxito"
}
`;

  const resp = await fetch(
    `https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent?key=${CONFIG.GEMINI_API_KEY}`,
    {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        contents: [{ parts: [{ text: prompt }] }],
        generationConfig: { temperature: 0.8, maxOutputTokens: 2000 }
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
      console.log('   ⚠️ Error parseando JSON de Gemini');
      return null;
    }
  }
  return null;
}

// === NOTION: Crear página ===
async function crearPaginaNotion(pilar, fecha, fechaFin = null, idea = null, isBatch = false, batchContent = null) {
  const pilarEmojis = {
    'andrea': '🎬 Andrea',
    'beltran': '⚡ Beltrán',
    'script': '📝 Script',
    'victor_heras': '🎯 Víctor Heras',
    'veneno': '🐍 Veneno',
    'post_personal': '📸 Post Personal',
    'stories': '📖 Stories'
  };

  const properties = {
    'Nombre': { title: [{ text: { content: 'Crear Idea' } }] },
    '🟣Pilar de contenido': { select: { name: pilarEmojis[pilar] || pilar } },
    '⚫Perfil (vacío = RunnerPro)': { multi_select: [{ name: CONFIG.PERFIL }] },
    '⚫Red Social': { multi_select: CONFIG.PILARES[pilar].redSocial.map(r => ({ name: r })) },
    '⚫Publicación': fechaFin
      ? { date: { start: fecha, end: fechaFin } }
      : { date: { start: fecha } }
  };

  let children = [];

  if (isBatch && batchContent) {
    // Contenido batch (Andrea/Beltrán)
    children = batchContent;
  } else if (idea) {
    // Idea individual
    children = [
      { object: 'block', type: 'heading_2', heading_2: { rich_text: [{ type: 'text', text: { content: '💡 IDEA' } }] } },
      { object: 'block', type: 'paragraph', paragraph: { rich_text: [
        { type: 'text', text: { content: `Título: ` }, annotations: { bold: true } },
        { type: 'text', text: { content: idea.titulo } }
      ] } },
      { object: 'block', type: 'paragraph', paragraph: { rich_text: [
        { type: 'text', text: { content: `Tensión: ` }, annotations: { bold: true } },
        { type: 'text', text: { content: idea.tension } }
      ] } },
      { object: 'block', type: 'paragraph', paragraph: { rich_text: [
        { type: 'text', text: { content: `Hook: ` }, annotations: { bold: true } },
        { type: 'text', text: { content: idea.hook } }
      ] } },
      { object: 'block', type: 'heading_2', heading_2: { rich_text: [{ type: 'text', text: { content: '🎯 ESTRUCTURA' } }] } },
      { object: 'block', type: 'paragraph', paragraph: { rich_text: [
        { type: 'text', text: { content: `Semántica Inicio: ` }, annotations: { bold: true } },
        { type: 'text', text: { content: idea.semantica_inicio } }
      ] } },
      { object: 'block', type: 'paragraph', paragraph: { rich_text: [
        { type: 'text', text: { content: `Semántica Ruta: ` }, annotations: { bold: true } },
        { type: 'text', text: { content: idea.semantica_ruta } }
      ] } },
      { object: 'block', type: 'paragraph', paragraph: { rich_text: [
        { type: 'text', text: { content: `Visual Ruta: ` }, annotations: { bold: true } },
        { type: 'text', text: { content: idea.visual_ruta } }
      ] } },
      { object: 'block', type: 'heading_2', heading_2: { rich_text: [{ type: 'text', text: { content: '📊 JUSTIFICACIÓN' } }] } },
      { object: 'block', type: 'paragraph', paragraph: { rich_text: [
        { type: 'text', text: { content: `Probabilidad de éxito: ` }, annotations: { bold: true } },
        { type: 'text', text: { content: `${idea.probabilidad_exito}%` } }
      ] } },
      { object: 'block', type: 'paragraph', paragraph: { rich_text: [
        { type: 'text', text: { content: `Patrones utilizados: ` }, annotations: { bold: true } },
        { type: 'text', text: { content: (idea.patrones_utilizados || []).join(', ') } }
      ] } },
      { object: 'block', type: 'paragraph', paragraph: { rich_text: [
        { type: 'text', text: { content: `Anti-patrones evitados: ` }, annotations: { bold: true } },
        { type: 'text', text: { content: (idea.antipatrones_evitados || []).join(', ') || 'Ninguno' } }
      ] } },
      { object: 'block', type: 'paragraph', paragraph: { rich_text: [
        { type: 'text', text: { content: idea.justificacion } }
      ] } }
    ];
  }

  const body = {
    parent: { database_id: CONFIG.NOTION_DB_ID },
    properties,
    children
  };

  const resp = await fetch('https://api.notion.com/v1/pages', {
    method: 'POST',
    headers: {
      'Authorization': `Bearer ${CONFIG.NOTION_API_KEY}`,
      'Notion-Version': '2022-06-28',
      'Content-Type': 'application/json'
    },
    body: JSON.stringify(body)
  });

  const data = await resp.json();
  if (data.id) {
    console.log(`   ✅ Página creada: ${pilar} - ${fecha}`);

    // Cambiar estado de Plantilla → Idea (si tiene idea o es batch)
    if (idea || (isBatch && batchContent)) {
      await actualizarEstadoNotion(data.id, 'Idea');
    }

    return data.id;
  } else {
    console.log(`   ❌ Error: ${JSON.stringify(data).slice(0, 300)}`);
    return null;
  }
}

// === MAIN ===
async function main() {
  const args = parseArgs();
  console.log(`\n🎯 CREA IDEA — Semana ${args.semana}\n`);
  console.log(`📄 Documentación: https://www.notion.so/Idea-IA-CEO-Media-2f0a67fd29b28038b00ad84e6a309a8f`);
  if (args.context) console.log(`💬 Contexto: ${args.context}`);
  console.log('');

  // Paso 0: Leer estrategia CEO Media (una sola vez)
  let estrategia = '';
  if (!args.dryRun) {
    estrategia = await leerEstrategia();
  }

  const db = new Database(CONFIG.DB_PATH);

  for (const [pilar, config] of Object.entries(CONFIG.PILARES)) {
    if (args.pilar && args.pilar !== pilar) continue;

    console.log(`\n📌 ${pilar.toUpperCase()}`);

    // === BATCHES (Andrea, Beltrán) ===
    if (config.tipo === 'batch') {
      const fechaFin = addDays(args.semana, 6);

      if (config.flujoPorDefinir) {
        console.log(`   🚧 FLUJO POR DEFINIR`);
        if (!args.dryRun) {
          const batchContent = [
            { object: 'block', type: 'callout', callout: {
              icon: { emoji: '🚧' },
              rich_text: [{ type: 'text', text: { content: `Flujo por definir\n\n${config.ideas} ideas pendientes de generar` } }]
            }}
          ];
          await crearPaginaNotion(pilar, args.semana, fechaFin, null, true, batchContent);
        } else {
          console.log(`   [DRY-RUN] Crearía página batch: ${pilar} ${args.semana} - ${fechaFin} (flujo por definir)`);
        }
      } else {
        // Andrea: leer documento fuente
        console.log(`   📄 Batch con ${config.ideas} ideas desde documento fuente`);
        if (!args.dryRun) {
          const batchContent = [
            { object: 'block', type: 'callout', callout: {
              icon: { emoji: '📄' },
              rich_text: [{ type: 'text', text: { content: `Documento fuente: https://docs.google.com/document/d/${config.docFuente}/edit\n\n${config.ideas} ideas a seleccionar` } }]
            }},
            { object: 'block', type: 'to_do', to_do: { rich_text: [{ type: 'text', text: { content: 'Leer documento fuente' } }], checked: false }},
            { object: 'block', type: 'to_do', to_do: { rich_text: [{ type: 'text', text: { content: `Seleccionar ${config.ideas} ideas` } }], checked: false }},
            { object: 'block', type: 'to_do', to_do: { rich_text: [{ type: 'text', text: { content: 'Escribir ideas (título + hook + estructura)' } }], checked: false }},
            { object: 'block', type: 'divider', divider: {} },
            { object: 'block', type: 'heading_2', heading_2: { rich_text: [{ type: 'text', text: { content: 'Ideas' } }] }}
          ];
          await crearPaginaNotion(pilar, args.semana, fechaFin, null, true, batchContent);
        } else {
          console.log(`   [DRY-RUN] Crearía página batch: ${pilar} ${args.semana} - ${fechaFin}`);
        }
      }
      continue;
    }

    // === PILARES NORMALES ===
    for (const diaOffset of config.dias) {
      const fecha = addDays(args.semana, diaOffset);

      let idea = null;
      if (!args.dryRun) {
        // Paso 1: Histórico propio
        const historico = obtenerHistorico(db);

        // Paso 2: MoneyBall del referente (BBDD SQL)
        const moneyball = calcularMoneyball(db, config.referente);

        // Paso 3: MoneyBall MD del referente (archivo local)
        const moneyballMD = leerMoneyballMD(config.referente);

        // Paso 4: Búsqueda semántica en BBDD del referente (contenido + escenas)
        const ejemplos = await busquedaSemantica(db, 'contenido viral running motivación', config.referente);

        // Paso 5: Generar idea
        idea = await generarIdea(pilar, config, moneyball, historico, ejemplos, moneyballMD, estrategia, args.context);

        if (idea) {
          console.log(`   💡 Idea: "${idea.titulo}" (${idea.probabilidad_exito}% éxito)`);
        }

        // Paso 6: Escribir en Notion (incluye cambio de estado Plantilla → Idea)
        await crearPaginaNotion(pilar, fecha, null, idea);
      } else {
        const moneyball = calcularMoneyball(db, config.referente);
        const moneyballMD = leerMoneyballMD(config.referente);
        console.log(`   📊 Patrones: ${moneyball.patronesHook.length} hooks, ${moneyball.patronesSem.length} semántica`);
        console.log(`   [DRY-RUN] Crearía página: ${pilar} ${fecha}`);
      }
    }
  }

  db.close();
  console.log('\n✅ CREA IDEA completado!\n');
}

main().catch(console.error);
