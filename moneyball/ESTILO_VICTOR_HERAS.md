# Estilo de Edición Victor Heras - Base de Conocimiento

_Creado: 2026-02-08_
_Fuente: Análisis de contenido de @victor_heras en BBDD MoneyBall RRSS_

---

## 🎯 Filosofía General

Victor Heras NO usa subtítulos literales del guión. Su enfoque es:
- **Comunicar visualmente el CONCEPTO**, no transcribir lo que dice
- **Menos es más** - minimalista pero impactante
- **Cada escena decide** qué elemento visual necesita (o ninguno)

---

## 📝 Tipos de Elementos Visuales

### 1. TEXTO CLAVE (más común)
- **Qué es:** 1-5 palabras que RESUMEN la idea central
- **NO es:** El guión literal como subtítulo
- **Ejemplos encontrados:**
  - "El algoritmo" (no "Te voy a explicar cómo funciona el algoritmo")
  - "servicio" (palabra suelta que representa el concepto)
  - "personalmente" (énfasis en una palabra clave)
  - "esto" (palabra que acompaña un gesto)
  - "Este mes he ganado" (frase corta de impacto)

### 2. GRAFISMO
- **Qué es:** Elemento visual que refuerza o reemplaza al texto
- **Ejemplos:**
  - Fuegos artificiales (momentos de celebración/logro)
  - Barra de frecuencia cardíaca (cuando habla de esfuerzo)
  - Gráfico 80/20 (cuando explica proporciones)
  - Iconos de proceso (ojo → persona → servicio)
  - Contador de km/seguidores subiendo

### 3. TEXTO + GRAFISMO (combinado)
- **Cuándo:** El número/dato ES el grafismo
- **Ejemplo:** "80%" aparece grande y animado - es texto Y grafismo a la vez

### 4. ICONOS
- **Cuándo:** Para representar procesos o conceptos abstractos
- **Estilo:** Circulares, limpios, en fila horizontal
- **Ejemplo:** 👁️ → ⭐ → 🍽️ (para explicar "servicio")

### 5. NINGUNO
- **Cuándo:** La escena visual es suficiente por sí sola
- **Ejemplo:** Transiciones, momentos de acción pura, "respirar" visual

---

## 🎨 Estilo Visual de Textos

### Tipografía
- **Fuente:** Sans-serif siempre (moderna, limpia)
- **Peso:** Varía según importancia:
  - Regular para texto secundario
  - Bold/Semi-bold para texto principal
  - Extra bold para impacto máximo

### Colores
- **Principal:** Blanco puro (#FFFFFF)
- **Contraste:** Depende del fondo:
  - Fondo oscuro → blanco sin efectos
  - Fondo claro → blanco con outline negro
  - Fondo variable → blanco con sombra sutil

### Efectos
- **Outline negro:** Cuando el fondo es claro o variable
- **Sombra sutil:** Drop shadow ligero para profundidad
- **Glow:** Ocasional, para momentos especiales
- **Sin efectos:** Cuando el contraste ya es bueno

### Posición
- **Más común:** Tercio superior, centrado horizontal
- **Alternativa:** Media-izquierda/derecha (a la altura del pecho)
- **Regla:** NUNCA tapar la cara del protagonista

---

## ✨ Animaciones

### Entrada de texto
- **Más común:** Aparece cuando se dice la palabra (sincronizado)
- **Alternativas:**
  - Fade in suave
  - Desde abajo hacia arriba
  - Zoom in desde pequeño
  - Palabras que se van sumando

### Durante
- **Estático:** Lo más común, el texto simplemente está
- **Sutil:** Ligero pulso o brillo en momentos clave

### Salida
- **Corte seco:** Desaparece con el corte de escena
- **Fade out:** Antes del siguiente texto

---

## 🎬 Relación con la Grabación

### Composición
- **Dejar espacio:** El protagonista se posiciona para dejar aire donde irá el texto
- **Zona segura:** Tercio superior generalmente libre para textos
- **Encuadre:** Plano medio deja espacio arriba; primer plano puede no tener texto

### Cuándo NO poner texto
- Cuando la acción visual es suficiente
- En transiciones rápidas
- Cuando el protagonista ocupa toda la pantalla
- Para dar "respiro" visual entre textos

---

## 📊 Patrones por Tipo de Escena

### HOOK (inicio)
- **Texto:** Frase corta de impacto o pregunta
- **Estilo:** Bold, grande, centrado arriba
- **Grafismo:** Raro, mejor ir directo al mensaje

### DESARROLLO (medio)
- **Texto:** Palabras clave que refuerzan puntos
- **Estilo:** Varía, puede haber escenas sin texto
- **Grafismo:** Aquí van los gráficos explicativos

### PROBLEMA/DOLOR
- **Texto:** Palabra que representa el dolor
- **Grafismo:** Iconos negativos (❌, señal de stop, etc.)
- **Efecto:** Puede usar shake o zoom dramático

### REVELACIÓN/SOLUCIÓN
- **Texto:** El dato clave grande ("80%", "ZONA 2")
- **Grafismo:** El texto ES el grafismo
- **Efecto:** Aparición con impacto

### CTA (final)
- **Texto:** Acción clara ("PLAN", "Comenta", "Link")
- **Estilo:** Puede incluir elementos interactivos (barra de comentario simulada)
- **Grafismo:** Flechas, iconos de acción

---

## ❌ Lo que Victor Heras NO hace

1. **NO subtítulos literales** del guión completo
2. **NO texto en TODAS las escenas** - algunas van limpias
3. **NO tipografías decorativas** - siempre sans-serif
4. **NO colores de texto** variados - casi siempre blanco
5. **NO animaciones excesivas** - simple y limpio
6. **NO tapar la cara** del protagonista con texto

---

## ✅ Checklist para Aplicar el Estilo

Antes de definir elementos visuales para una escena:

- [ ] ¿Qué CONCEPTO quiero comunicar visualmente?
- [ ] ¿Necesita texto, grafismo, ambos, o ninguno?
- [ ] Si texto: ¿Cuál es la palabra/frase CLAVE (no el guión literal)?
- [ ] ¿Dónde hay espacio en el encuadre para el texto?
- [ ] ¿Cómo debe animarse para fluir con el audio?
- [ ] ¿Hay suficiente contraste o necesita outline/sombra?

---

## 🔗 Referencias

- **BBDD Escenas:** `~/.openclaw/workspace/moneyball_rrss.db`
- **Creador:** victor_heras
- **Videos analizados:** 150 videos, ~1000+ escenas
- **Script de análisis:** `generate_storyboard_visual.js`

---

## 🎬 PROMPT MAESTRO PARA STORYBOARDS

```
Eres un ilustrador profesional de storyboard especializado en videos verticales para redes sociales (Instagram Reels, TikTok, Shorts). Genera una única imagen storyboard con estas reglas obligatorias:

📐 FORMATO VISUAL
- Output: una sola imagen en formato horizontal 16:9
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
Si proporciono un texto completo de guion o subtítulo, debes cumplir esto:
✅ El subtítulo NO debe repetirse entero en todos los frames.
En su lugar:
- Divide el guion en 4 partes
- Cada frame muestra solo la parte correspondiente
- Debe sentirse como subtítulos progresivos de un Reel real

Ejemplo:
Guion completo: "Entrenar lento te hace más rápido si controlas el esfuerzo"
Debe repartirse así:
- Frame 1: "Entrenar lento…"
- Frame 2: "…te hace más rápido"
- Frame 3: "…si controlas…"
- Frame 4: "…el esfuerzo"

⭐ PALABRAS CLAVE DESTACADAS
En cada frame, si hay una palabra importante, destácala visualmente:
- con color suave
- subrayado
- tipografía diferente
Pero SOLO la keyword, no todo el texto.
Ejemplo: "más rápido" o "control" o "esfuerzo"

📌 ESCENAS
- FRAME 1: {ESCENA_1}
- FRAME 2: Continuación inmediata
- FRAME 3: Evolución natural
- FRAME 4: Cierre breve

🗣️ GUIÓN GLOBAL (INPUT)
Guion completo del Reel: {GUION_TOTAL}

✅ OUTPUT FINAL
Una sola imagen storyboard con:
- 4 pantallas verticales alineadas
- continuidad real de micro-secuencia
- subtítulos repartidos progresivamente
- keywords destacadas
No incluyas explicación, solo genera la imagen.
```

---

_Este documento se actualizará con nuevos aprendizajes sobre el estilo de edición._
