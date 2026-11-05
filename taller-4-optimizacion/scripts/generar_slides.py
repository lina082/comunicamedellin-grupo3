#!/usr/bin/env python3
"""
Genera Taller4_ComunicaMedellin.pptx con el mismo sistema visual del
Taller 3 (Mini-Lakehouse ComunicaMedellín.pptx): cuaderno de laboratorio,
Baloo 2 + IBM Plex Mono, tarjetas rounded-rectangle con borde de tinta
gruesa y relleno solido, paleta azul/amarillo/teal/rojo sobre blanco.

8 diapositivas, una persona por paso (Lina siempre en la parte practica:
demo en vivo + reflexion arquitectural), siguiendo el mismo patron del
PPTX del Taller 3.
"""
from pathlib import Path

from pptx import Presentation
from pptx.util import Emu, Pt
from pptx.dml.color import RGBColor
from pptx.enum.text import PP_ALIGN, MSO_ANCHOR
from pptx.enum.shapes import MSO_SHAPE
from pptx.oxml.ns import qn

BASE = Path(__file__).resolve().parent.parent
OUT = BASE / "Taller4_ComunicaMedellin.pptx"

# ── Paleta (identica al Taller 3) ─────────────────────────────────────
INK = RGBColor(0x15, 0x18, 0x1F)
INK_SOFT = RGBColor(0x4B, 0x51, 0x66)
INK_FAINT = RGBColor(0x87, 0x8A, 0x94)
BG = RGBColor(0xFF, 0xFF, 0xFF)
PAPER = RGBColor(0xF5, 0xF7, 0xFC)
LINE_SOFT = RGBColor(0xCC, 0xD3, 0xE6)
BLUE = RGBColor(0x2F, 0x5F, 0xD6)
BLUE_SOFT = RGBColor(0xE4, 0xEB, 0xFC)
BLUE_DARK = RGBColor(0x1D, 0x3C, 0x91)
YELLOW = RGBColor(0xFF, 0xD2, 0x3F)
YELLOW_SOFT = RGBColor(0xFF, 0xF1, 0xC2)
ON_YELLOW = RGBColor(0x24, 0x1F, 0x12)
TEAL = RGBColor(0x1F, 0x9E, 0x8F)
TEAL_SOFT = RGBColor(0xD9, 0xF2, 0xEE)
RED = RGBColor(0xD6, 0x45, 0x3C)
RED_SOFT = RGBColor(0xFB, 0xE3, 0xE0)
DARK_PANEL = RGBColor(0x1A, 0x1D, 0x26)
DARK_LINE = RGBColor(0x33, 0x3A, 0x4D)

DISPLAY = "Baloo 2"
MONO = "IBM Plex Mono"

EMU_IN = 914400
SLIDE_W = Emu(int(13.333 * EMU_IN))
SLIDE_H = Emu(int(7.5 * EMU_IN))

prs = Presentation()
prs.slide_width = SLIDE_W
prs.slide_height = SLIDE_H
BLANK = prs.slide_layouts[6]


def slide():
    s = prs.slides.add_slide(BLANK)
    bg = s.background
    bg.fill.solid()
    bg.fill.fore_color.rgb = BG
    return s


def in_(v):
    return Emu(int(v * EMU_IN))


def box(s, x, y, w, h, fill=None, line=None, line_w=2.5, shape=MSO_SHAPE.ROUNDED_RECTANGLE, radius=None):
    sp = s.shapes.add_shape(shape, in_(x), in_(y), in_(w), in_(h))
    sp.shadow.inherit = False
    if fill is None:
        sp.fill.background()
    else:
        sp.fill.solid()
        sp.fill.fore_color.rgb = fill
    if line is None:
        sp.line.fill.background()
    else:
        sp.line.color.rgb = line
        sp.line.width = Pt(line_w)
    if shape == MSO_SHAPE.ROUNDED_RECTANGLE and radius is not None:
        try:
            sp.adjustments[0] = radius
        except Exception:
            pass
    return sp


def text(s, x, y, w, h, runs, align=PP_ALIGN.LEFT, anchor=MSO_ANCHOR.TOP, line_spacing=1.1, wrap=True):
    """runs: list of (text, font, size_pt, color, bold, italic) OR list of
    lines, each a list of such tuples (multi-paragraph)."""
    tb = s.shapes.add_textbox(in_(x), in_(y), in_(w), in_(h))
    tf = tb.text_frame
    tf.word_wrap = wrap
    tf.vertical_anchor = anchor
    tf.margin_left = 0
    tf.margin_right = 0
    tf.margin_top = 0
    tf.margin_bottom = 0

    if runs and not isinstance(runs[0], list):
        runs = [runs]

    for i, line_runs in enumerate(runs):
        p = tf.paragraphs[0] if i == 0 else tf.add_paragraph()
        p.alignment = align
        p.line_spacing = line_spacing
        for (txt, font, size_pt, color, bold, italic) in line_runs:
            r = p.add_run()
            r.text = txt
            r.font.name = font
            r.font.size = Pt(size_pt)
            r.font.color.rgb = color
            r.font.bold = bold
            r.font.italic = italic
    return tb


def R(txt, font=DISPLAY, size=14, color=INK, bold=False, italic=False):
    return (txt, font, size, color, bold, italic)


def eyebrow(s, x, y, w, label):
    text(s, x, y, w, 0.3, [R(label, MONO, 11, BLUE, True)])


def heading(s, x, y, w, title, size=26):
    text(s, x, y, w, 0.9, [R(title, DISPLAY, size, INK, True)], line_spacing=1.08)


def footer_who(s, who, step):
    text(s, 0.89, 6.9, 6, 0.25, [R(f"{step} · {who}", MONO, 10.5, INK_FAINT, False)])


def chip(s, x, y, w, h, label, fill, line=INK, text_color=INK, size=11, font=DISPLAY, bold=True):
    box(s, x, y, w, h, fill=fill, line=line, line_w=2, radius=0.5)
    text(s, x, y, w, h, [R(label, font, size, text_color, bold)],
         align=PP_ALIGN.CENTER, anchor=MSO_ANCHOR.MIDDLE)


# ════════════════════════════════════════════════════════════════════
# SLIDE 1 — Portada
# ════════════════════════════════════════════════════════════════════
s = slide()
box(s, 0.9, 2.4, 0.56, 0.56, fill=YELLOW, line=INK, line_w=2.5, shape=MSO_SHAPE.OVAL)
text(s, 0.9, 2.4, 0.56, 0.56, [R("D", DISPLAY, 22, INK, True)], align=PP_ALIGN.CENTER, anchor=MSO_ANCHOR.MIDDLE)
text(s, 1.64, 2.5, 6, 0.3, [R("COMUNICAMEDELLÍN · TALLER 4", MONO, 14, BLUE, True)])
text(s, 0.89, 3.0, 10.5, 1.1, [R("Laboratorio de Optimización", DISPLAY, 46, INK, True)], line_spacing=1.05)
text(s, 0.89, 4.1, 9.3, 0.8,
     [R("Diagnóstico, intervención y medición sobre el pipeline del Taller 3 — leer planes de ejecución, aplicar una técnica, medir el antes y el después.", DISPLAY, 17, INK_SOFT, False)],
     line_spacing=1.3)
text(s, 0.89, 6.75, 7, 0.3, [R("Equipo 3 · Simón · Elizabeth · Nilton · Julián · Lina", DISPLAY, 12, INK_FAINT, False)])
text(s, 0.89, 7.05, 7, 0.3, [R("Tópicos Avanzados en Bases de Datos · ITM 2026", DISPLAY, 12, INK_FAINT, False)])

# ════════════════════════════════════════════════════════════════════
# SLIDE 2 — Apertura: panorama (Simon)
# ════════════════════════════════════════════════════════════════════
s = slide()
eyebrow(s, 0.89, 0.55, 8, "APERTURA · SIMÓN")
heading(s, 0.89, 0.85, 10.8, "De dónde partimos: el pipeline del Taller 3")

steps = [
    ("Bronze/Silver/Gold", "106.370 filas de\nentregas en Parquet", BLUE_SOFT),
    ("1 row group", "fact_entregas y\nfact_presencia_hora", YELLOW_SOFT),
    ("Sin filtro = scan", "Q1/Q3/Q5 originales:\nagregaciones globales", BLUE_SOFT),
    ("+ WHERE realista", "última semana, estado\nfallido, zona Centro", TEAL_SOFT),
    ("Indicador 4", "data skipping\nausente", INK),
]
card_w, gap = 1.95, 0.28
total_w = len(steps) * card_w + (len(steps) - 1) * gap
x0 = (13.333 - total_w) / 2
y0 = 2.0
for i, (label, sub, fill) in enumerate(steps):
    x = x0 + i * (card_w + gap)
    txt_color = BG if fill == INK else INK
    sub_color = RGBColor(0xCC,0xD3,0xE6) if fill == INK else INK_SOFT
    box(s, x, y0, card_w, 3.25, fill=fill, line=INK, line_w=2.5, radius=0.09)
    box(s, x + (card_w-0.62)/2, y0+0.35, 0.62, 0.62, fill=BG if fill!=INK else DARK_PANEL, line=INK, line_w=2, shape=MSO_SHAPE.OVAL)
    text(s, x, y0+0.4, card_w, 0.5, [R(str(i+1), DISPLAY, 20, txt_color, True)], align=PP_ALIGN.CENTER, anchor=MSO_ANCHOR.MIDDLE)
    text(s, x+0.12, y0+1.2, card_w-0.24, 0.6, [R(label, DISPLAY, 14.5, txt_color, True)], align=PP_ALIGN.CENTER, line_spacing=1.1)
    text(s, x+0.12, y0+1.95, card_w-0.24, 1.0, [R(sub, DISPLAY, 11.5, sub_color, False)], align=PP_ALIGN.CENTER, line_spacing=1.25)
    if i < len(steps)-1:
        text(s, x+card_w, y0+1.4, gap, 0.5, [R("→", DISPLAY, 20, INK_FAINT, True)], align=PP_ALIGN.CENTER, anchor=MSO_ANCHOR.MIDDLE)

text(s, 0.89, 5.75, 10.8, 1.0,
     [R("Hoy recorremos estas cinco paradas: primero el diagnóstico sobre el pipeline real del Taller 3, luego tres intervenciones medidas, y cerramos con una demo en vivo y la reflexión arquitectural.", DISPLAY, 14.5, INK_SOFT, False)],
     line_spacing=1.35)
footer_who(s, "Simón Ramírez", "PASO 0")

# ════════════════════════════════════════════════════════════════════
# SLIDE 3 — Diagnostico: Indicador 4 (Simon)
# ════════════════════════════════════════════════════════════════════
s = slide()
eyebrow(s, 0.89, 0.55, 8, "PASO 1 · SIMÓN")
heading(s, 0.89, 0.85, 10.8, "La causa raíz: un solo row group")

chip(s, 0.89, 1.65, 2.3, 0.65, "fact_entregas", BLUE_SOFT, size=13)
chip(s, 3.39, 1.65, 2.3, 0.65, "fact_presencia", BLUE_SOFT, size=13)
chip(s, 0.89, 2.55, 2.3, 0.65, "1 row group", YELLOW_SOFT, size=13)

box(s, 0.89, 3.6, 4.6, 0.95, fill=TEAL_SOFT, line=TEAL, line_w=2, radius=0.15)
text(s, 1.15, 3.75, 4.1, 0.3, [R("¿CÓMO SE REVELA?", MONO, 10.5, TEAL, True)])
text(s, 1.15, 4.0, 4.1, 0.5, [R("parquet_metadata() → 1 bloque,\nsin min/max que podar", DISPLAY, 11.5, INK, False)], line_spacing=1.15)

box(s, 5.75, 1.65, 6.7, 4.9, fill=None, line=None)
text(s, 5.75, 1.75, 6.7, 0.5, [R("Por qué esto importa", DISPLAY, 15, INK, True)])
text(s, 5.75, 2.3, 6.7, 1.0,
     [R("Las Q1/Q3/Q5 originales del Taller 3 eran agregaciones sobre todo el histórico, sin WHERE — ya eran un full scan \"correcto\". Les añadimos un filtro realista (última semana, estado fallido, zona Centro) para que hubiera algo que optimizar de verdad.", DISPLAY, 13, INK_SOFT, False)],
     line_spacing=1.35)
text(s, 5.75, 3.55, 6.7, 1.0,
     [R("Con un solo row group, DuckDB no tiene ningún bloque con min/max que pueda descartar: así el filtro sea muy selectivo, decodifica el archivo completo igual. Ese es el Indicador 4 del vocabulario del taller — y afecta por igual a las tres consultas.", DISPLAY, 13, INK_SOFT, False)],
     line_spacing=1.35)
box(s, 5.75, 4.75, 6.7, 1.6, fill=PAPER, line=INK, line_w=2, radius=0.08)
text(s, 6.0, 4.95, 6.2, 0.3, [R("DIAGNÓSTICO CONFIRMADO", MONO, 10.5, INK_SOFT, True)])
text(s, 6.0, 5.3, 6.2, 0.9,
     [R("106.370 filas · 1 row group · ningún predicado puede podar bloques", DISPLAY, 13, INK, True)],
     line_spacing=1.2)
footer_who(s, "Simón Ramírez", "PASO 1")

# ════════════════════════════════════════════════════════════════════
# SLIDE 4 — Q1 y Q3: intervencion comparada (Elizabeth / Nilton)
# ════════════════════════════════════════════════════════════════════
s = slide()
eyebrow(s, 0.89, 0.55, 8, "PASOS 2–3 · ELIZABETH · NILTON")
heading(s, 0.89, 0.85, 10.8, "Un mismo orden físico no sirve a dos filtros")

# left: Q1
box(s, 0.89, 1.65, 5.6, 4.25, fill=BLUE_SOFT, line=BLUE, line_w=2.5, radius=0.07)
text(s, 1.1, 1.85, 5.2, 0.3, [R("Q1 · ELIZABETH", MONO, 11, BLUE_DARK, True)])
text(s, 1.1, 2.2, 5.2, 0.55, [R("Volumen diario, última semana", DISPLAY, 17, INK, True)], line_spacing=1.1)
text(s, 1.1, 2.85, 5.2, 1.1,
     [R("Ordenar por tiempo_sk + ROW_GROUP_SIZE 5000 → 18 row groups. El filtro de fecha cubre ~27% de las filas.", DISPLAY, 12.5, INK_SOFT, False)],
     line_spacing=1.3)
chip(s, 1.1, 4.1, 1.85, 0.6, "3.78 ms", BG, size=13)
text(s, 1.1, 4.75, 1.85, 0.25, [R("antes", MONO, 9.5, INK_FAINT, False)], align=PP_ALIGN.CENTER)
chip(s, 3.1, 4.1, 1.85, 0.6, "2.60 ms", BG, size=13)
text(s, 3.1, 4.75, 1.85, 0.25, [R("después", MONO, 9.5, INK_FAINT, False)], align=PP_ALIGN.CENTER)
chip(s, 1.1, 5.2, 3.85, 0.65, "1.45x más rápido", TEAL, size=14, text_color=BG)

# right: Q3
box(s, 6.75, 1.65, 5.7, 4.25, fill=RED_SOFT, line=RED, line_w=2.5, radius=0.07)
text(s, 6.95, 1.85, 5.3, 0.3, [R("Q3 · NILTON", MONO, 11, RED, True)])
text(s, 6.95, 2.2, 5.3, 0.55, [R("Entregas fallidas por severidad/zona", DISPLAY, 17, INK, True)], line_spacing=1.1)
text(s, 6.95, 2.85, 5.3, 1.1,
     [R("Segunda copia ordenada por estado → 18 row groups. estado='fallido' es solo ~5% de las filas: el filtro más selectivo de los tres.", DISPLAY, 12.5, INK_SOFT, False)],
     line_spacing=1.3)
chip(s, 6.95, 4.1, 1.85, 0.6, "3.14 ms", BG, size=13)
text(s, 6.95, 4.75, 1.85, 0.25, [R("antes", MONO, 9.5, INK_FAINT, False)], align=PP_ALIGN.CENTER)
chip(s, 8.95, 4.1, 1.85, 0.6, "1.99 ms", BG, size=13)
text(s, 8.95, 4.75, 1.85, 0.25, [R("después", MONO, 9.5, INK_FAINT, False)], align=PP_ALIGN.CENTER)
chip(s, 6.95, 5.2, 3.9, 0.65, "1.58x más rápido — el mayor", TEAL, size=13, text_color=BG)

text(s, 0.89, 6.15, 10.8, 0.6,
     [R("Un único orden físico favorece una columna a la vez: por eso se generaron dos copias del mismo archivo, una por tiempo_sk y otra por estado.", DISPLAY, 12.5, INK_SOFT, False)],
     line_spacing=1.3)
footer_who(s, "Elizabeth Méndez · Nilton Orejuela", "PASOS 2–3")

# ════════════════════════════════════════════════════════════════════
# SLIDE 5 — Q5 + tabla de benchmarks (Julian)
# ════════════════════════════════════════════════════════════════════
s = slide()
eyebrow(s, 0.89, 0.55, 8, "PASO 4 · JULIÁN · BENCHMARKS CONSOLIDADOS")
heading(s, 0.89, 0.85, 10.8, "Tres factores, un mismo patrón")

rows = [
    ("Q1 — Volumen diario", "3.78 → 2.60 ms", "1.45x", BLUE),
    ("Q3 — Entregas fallidas", "3.14 → 1.99 ms", "1.58x", TEAL),
    ("Q5 — Presencia zona Centro", "0.91 → 0.87 ms", "1.05x", INK_FAINT),
]
y = 1.9
for label, times, factor, accent in rows:
    box(s, 0.89, y, 7.1, 0.85, fill=PAPER, line=LINE_SOFT, line_w=1.25, radius=0.18)
    text(s, 1.15, y, 3.0, 0.85, [R(label, DISPLAY, 13.5, INK, True)], anchor=MSO_ANCHOR.MIDDLE)
    text(s, 4.2, y, 2.2, 0.85, [R(times, MONO, 12, INK_SOFT, False)], anchor=MSO_ANCHOR.MIDDLE)
    box(s, 6.55, y+0.13, 1.3, 0.59, fill=accent, line=None, radius=0.35)
    text(s, 6.55, y+0.13, 1.3, 0.59, [R(factor, DISPLAY, 14, BG, True)], align=PP_ALIGN.CENTER, anchor=MSO_ANCHOR.MIDDLE)
    y += 1.0

box(s, 8.3, 1.9, 4.15, 3.15, fill=DARK_PANEL, line=DARK_LINE, line_w=1.5, radius=0.07)
text(s, 8.6, 2.1, 3.6, 0.3, [R("POR QUÉ ES MODESTO", MONO, 10, RGBColor(0x9a,0xa2,0xb2), True)])
text(s, 8.6, 2.5, 3.6, 2.3,
     [R("Archivo de 1.3 MB cabe en caché del SO: la mejora viene de menos CPU decodificando, no de I/O evitado.\n\nQ5 filtra zona, solo 3 valores — techo de 1 de 2 row groups podables.\n\n\"Un 5% bien justificado vale más que un 90% sin entender por qué.\"", DISPLAY, 12, RGBColor(0xe9,0xe6,0xdf), False)],
     line_spacing=1.3)

text(s, 0.89, 5.25, 10.8, 1.1,
     [R("Confirmamos con el profiler que total_bytes_read fue prácticamente igual antes y después (~1.7 KB) — la ganancia real es de decodificación, no de disco.", DISPLAY, 13, INK_SOFT, False)],
     line_spacing=1.35)
footer_who(s, "Julián Vélez", "PASO 4")

# ════════════════════════════════════════════════════════════════════
# SLIDE 6 — Lina: demo en vivo (PARTE PRACTICA)
# ════════════════════════════════════════════════════════════════════
s = slide()
eyebrow(s, 0.89, 0.55, 8, "PASO 5 · LINA · PARTE PRÁCTICA")
heading(s, 0.89, 0.85, 10.8, "Ahora lo ejecutamos de verdad")

box(s, 0.89, 1.9, 8.4, 3.9, fill=DARK_PANEL, line=DARK_LINE, line_w=1.5, radius=0.06)
demo_steps = [
    ("1", "python scripts/diagnostico.py", "EXPLAIN ANALYZE + baseline sobre la Gold original"),
    ("2", "python scripts/intervencion.py", "reescribe Parquet ordenado, row groups chicos"),
    ("3", "python scripts/medicion_despues.py", "EXPLAIN ANALYZE + medición sobre lo optimizado"),
]
yy = 2.25
for num, cmd, desc in demo_steps:
    box(s, 1.2, yy, 0.45, 0.45, fill=BLUE, line=None, shape=MSO_SHAPE.OVAL)
    text(s, 1.2, yy, 0.45, 0.45, [R(num, DISPLAY, 14, BG, True)], align=PP_ALIGN.CENTER, anchor=MSO_ANCHOR.MIDDLE)
    text(s, 1.85, yy-0.05, 7.1, 0.35, [R(cmd, MONO, 13, RGBColor(0xd7,0xdd,0xe8), True)])
    text(s, 1.85, yy+0.33, 7.1, 0.35, [R(desc, DISPLAY, 11.5, RGBColor(0x9a,0xa2,0xb2), False)])
    yy += 1.15

box(s, 9.6, 1.9, 2.85, 3.9, fill=YELLOW_SOFT, line=INK, line_w=2.25, radius=0.08)
text(s, 9.85, 2.1, 2.4, 0.3, [R("EN VIVO", MONO, 10, ON_YELLOW, True)])
text(s, 9.85, 2.5, 2.4, 3.1,
     [R("No es una animación: cada comando corre contra la Gold real del Taller 3 y reproduce los mismos números que ya vieron en esta sustentación.", DISPLAY, 12.5, ON_YELLOW, False)],
     line_spacing=1.35)

text(s, 0.89, 6.1, 10.8, 0.7,
     [R("comunicamedellin-lakehouse.vercel.app · github.com/lina082/comunicamedellin-grupo3", MONO, 12, INK_SOFT, False)])
footer_who(s, "Lina Sevillano", "PASO 5")

# ════════════════════════════════════════════════════════════════════
# SLIDE 7 — Lina: reflexion arquitectural (PARTE PRACTICA)
# ════════════════════════════════════════════════════════════════════
s = slide()
eyebrow(s, 0.89, 0.55, 8, "PASO 6 · LINA · REFLEXIÓN ARQUITECTURAL")
heading(s, 0.89, 0.85, 10.8, "Si rediseñáramos el pipeline hoy")

box(s, 0.89, 1.9, 5.5, 4.4, fill=TEAL_SOFT, line=TEAL, line_w=2, radius=0.1)
text(s, 1.15, 2.1, 5.0, 0.4, [R("YA NO SERÍA UN PASO POSTERIOR", DISPLAY, 13, INK, True)])
text(s, 1.15, 2.6, 5.0, 1.5,
     [R("transform_gold.py debería declarar desde el principio qué columna se filtra más, y fijar ROW_GROUP_SIZE a propósito — no heredar el default de pandas/pyarrow pensado para datasets mucho más grandes.", DISPLAY, 12.5, INK, False)],
     line_spacing=1.35)
text(s, 1.15, 4.3, 5.0, 0.4, [R("EN PRODUCCIÓN", DISPLAY, 13, INK, True)])
text(s, 1.15, 4.75, 5.0, 1.4,
     [R("Particionar por año/mes (I-6) para consultas de rango temporal — las más frecuentes en un dashboard operacional.", DISPLAY, 12.5, INK, False)],
     line_spacing=1.35)

box(s, 6.75, 1.9, 5.7, 4.4, fill=PAPER, line=INK, line_w=2, radius=0.1)
text(s, 7.0, 2.1, 5.2, 0.4, [R("UN LÍMITE REAL", DISPLAY, 13, INK, True)])
text(s, 7.0, 2.6, 5.2, 1.6,
     [R("Ninguna técnica de acceso arregla una consulta sobre una columna de baja cardinalidad, como zona (solo 3 valores). El techo lo pone la cardinalidad del predicado, no el orden físico.", DISPLAY, 12.5, INK, False)],
     line_spacing=1.35)
text(s, 7.0, 4.4, 5.2, 0.4, [R("LA RESPUESTA CORRECTA AHÍ", DISPLAY, 13, INK, True)])
text(s, 7.0, 4.85, 5.2, 1.3,
     [R("No es más índices — es una vista pre-agregada por zona y hora, con ~72 filas en vez de 10.000.", DISPLAY, 12.5, INK, False)],
     line_spacing=1.35)

footer_who(s, "Lina Sevillano", "PASO 6")

# ════════════════════════════════════════════════════════════════════
# SLIDE 8 — Cierre / preguntas
# ════════════════════════════════════════════════════════════════════
s = slide()
text(s, 0.89, 2.65, 10.8, 1.0, [R("¿Preguntas?", DISPLAY, 42, INK, True)])

names = [
    ("Simón", BLUE_SOFT),
    ("Elizabeth", YELLOW_SOFT),
    ("Nilton", TEAL_SOFT),
    ("Julián", RED_SOFT),
    ("Lina", RGBColor(0xEC, 0xE3, 0xFB)),
]
nx = 0.89
for name, fill in names:
    w = 0.6 + len(name) * 0.1
    chip(s, nx, 3.75, w, 0.55, name, fill, line=None, size=13)
    nx += w + 0.18

text(s, 0.89, 4.6, 10, 0.4, [R("Cualquiera de nosotros puede responder sobre cualquier parte del Taller 4.", DISPLAY, 14, INK_SOFT, False)])
text(s, 0.89, 6.9, 9, 0.3, [R("comunicamedellin-lakehouse.vercel.app · github.com/lina082/comunicamedellin-grupo3", MONO, 11.5, INK_FAINT, False)])

prs.save(str(OUT))
print(f"PPTX generado: {OUT}")
