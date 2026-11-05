#!/usr/bin/env python3
"""Genera informe_tecnico.pdf a partir del contenido del informe del Taller 4."""
from pathlib import Path

from reportlab.lib import colors
from reportlab.lib.pagesizes import LETTER
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import cm
from reportlab.platypus import (
    SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle,
    PageBreak, KeepTogether, ListFlowable, ListItem,
)
from reportlab.lib.enums import TA_CENTER, TA_LEFT

BASE = Path(__file__).resolve().parent.parent
OUT = BASE / "informe_tecnico.pdf"

NAVY = colors.HexColor("#1b2a4a")
LIGHT_GRAY = colors.HexColor("#f2f2f2")

styles = getSampleStyleSheet()
styles.add(ParagraphStyle(name="TitleBig", fontSize=22, leading=26, alignment=TA_CENTER,
                           textColor=NAVY, spaceAfter=6, fontName="Helvetica-Bold"))
styles.add(ParagraphStyle(name="SubtitleBig", fontSize=13, leading=17, alignment=TA_CENTER,
                           textColor=colors.black, spaceAfter=4))
styles.add(ParagraphStyle(name="Byline", fontSize=10, leading=14, alignment=TA_CENTER,
                           textColor=colors.black, spaceAfter=20))
styles.add(ParagraphStyle(name="H1", fontSize=16, leading=20, textColor=NAVY,
                           spaceBefore=16, spaceAfter=8, fontName="Helvetica-Bold"))
styles.add(ParagraphStyle(name="H2", fontSize=12.5, leading=16, textColor=NAVY,
                           spaceBefore=12, spaceAfter=6, fontName="Helvetica-Bold"))
styles.add(ParagraphStyle(name="Body", fontSize=10, leading=14.5, spaceAfter=8,
                           alignment=TA_LEFT, textColor=colors.black))
styles.add(ParagraphStyle(name="CodeBlock", fontName="Courier", fontSize=8, leading=10.5,
                           backColor=colors.HexColor("#1e1e1e"), textColor=colors.white,
                           borderPadding=8, spaceAfter=10, spaceBefore=4))
styles.add(ParagraphStyle(name="TableCell", fontSize=8.5, leading=11, textColor=colors.black))
styles.add(ParagraphStyle(name="TableHeader", fontSize=8.5, leading=11,
                           textColor=colors.white, fontName="Helvetica-Bold"))
styles.add(ParagraphStyle(name="Caption", fontSize=9, leading=12,
                           textColor=colors.black, spaceAfter=10))

doc = SimpleDocTemplate(
    str(OUT), pagesize=LETTER,
    topMargin=2.2 * cm, bottomMargin=2 * cm,
    leftMargin=2.2 * cm, rightMargin=2.2 * cm,
)

story = []


def p(text, style="Body"):
    story.append(Paragraph(text, styles[style]))


def code(text):
    escaped = text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
    escaped = escaped.replace("\n", "<br/>")
    story.append(Paragraph(escaped, styles["CodeBlock"]))


def kv_table(rows, col_widths=None):
    data = [[Paragraph(f"<b>{h}</b>", styles["TableHeader"]) for h in rows[0]]]
    for row in rows[1:]:
        data.append([Paragraph(str(c), styles["TableCell"]) for c in row])
    t = Table(data, colWidths=col_widths, repeatRows=1)
    t.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), NAVY),
        ("GRID", (0, 0), (-1, -1), 0.5, colors.HexColor("#cccccc")),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, LIGHT_GRAY]),
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ("TOPPADDING", (0, 0), (-1, -1), 5),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 5),
        ("LEFTPADDING", (0, 0), (-1, -1), 6),
        ("RIGHTPADDING", (0, 0), (-1, -1), 6),
    ]))
    return t


def bullets(items):
    story.append(ListFlowable(
        [ListItem(Paragraph(it, styles["Body"]), bulletColor=NAVY) for it in items],
        bulletType="bullet", start="•", leftIndent=14,
    ))


# ── Portada ────────────────────────────────────────────────────────
story.append(Spacer(1, 2 * cm))
p("INSTITUCIÓN UNIVERSITARIA ITM<br/>Tópicos Avanzados en Bases de Datos", "Byline")
story.append(Spacer(1, 0.6 * cm))
p("TALLER 4", "TitleBig")
p("Laboratorio de Optimización de Consultas", "SubtitleBig")
p("Diagnóstico, intervención y medición sobre el pipeline ComunicaMedellín (Taller 3)", "Byline")
story.append(Spacer(1, 1 * cm))
p("<b>Equipo 3</b>", "Byline")
story.append(Spacer(1, 0.15 * cm))
integrantes = ["Simón Ramírez", "Elizabeth Méndez", "Nilton Orejuela", "Julián Vélez", "Lina Sevillano"]
integrantes_table = Table(
    [[Paragraph(nombre, styles["Byline"])] for nombre in integrantes],
    colWidths=[8 * cm],
)
integrantes_table.setStyle(TableStyle([
    ("ALIGN", (0, 0), (-1, -1), "CENTER"),
    ("TOPPADDING", (0, 0), (-1, -1), 1),
    ("BOTTOMPADDING", (0, 0), (-1, -1), 1),
]))
integrantes_table.hAlign = "CENTER"
story.append(integrantes_table)
story.append(Spacer(1, 0.5 * cm))
p("Docente: Roberto Carlos Rahamut Suteu<br/>Medellín, 2026", "Byline")
story.append(PageBreak())

# ── 1. Resumen ejecutivo ──────────────────────────────────────────
p("1. Resumen ejecutivo", "H1")
p("Este informe optimiza tres consultas analíticas del mini-lakehouse de "
  "ComunicaMedellín construido en el Taller 3 (DuckDB sobre Gold en Parquet, "
  "~106.000 filas de entregas y 10.000 snapshots de presencia). Se leyeron los "
  "planes de ejecución reales con <b>EXPLAIN ANALYZE</b>, se identificó una causa "
  "raíz única y común a las tres consultas, se aplicó una sola intervención "
  "dirigida a esa causa, y se midió el efecto con la misma metodología "
  "(warm-up de 3 corridas + mediana de 5).")
p("El diagnóstico reveló que los dos archivos de hechos "
  "(<font face='Courier'>fact_entregas.parquet</font> y "
  "<font face='Courier'>fact_presencia_hora.parquet</font>) se habían escrito en "
  "el Taller 3 con <b>un solo row group</b> cada uno. Con un único row group, "
  "DuckDB no tiene ningún bloque con estadísticas min/max que pueda descartar: "
  "así el WHERE sea muy selectivo, el motor debe leer y decodificar el row group "
  "completo siempre. Ese es el Indicador 4 del vocabulario del taller — data "
  "skipping ausente — y afecta por igual a una consulta filtrada por fecha, otra "
  "por estado de entrega y otra por zona.")
p("La intervención fue reescribir ambos archivos ordenados por la columna de "
  "filtro de cada consulta, con ROW_GROUP_SIZE reducido a 5.000, lo que generó "
  "18 row groups en fact_entregas y 2 en fact_presencia_hora. Las mejoras "
  "medidas fueron de 1.05x a 1.58x: reales pero modestas, porque el dataset "
  "(1.3 MB) es pequeño para que la ventaja de saltar bloques de I/O se note — la "
  "ganancia observada viene del menor trabajo de CPU al decodificar menos filas, "
  "no de I/O evitado. La Sección 4.1 analiza esto con honestidad: no todos los "
  "resultados \"pequeños\" son un fracaso del diagnóstico, y aquí se explica por qué.")

story.append(PageBreak())

# ── 2. Diagnóstico ─────────────────────────────────────────────────
p("2. Diagnóstico", "H1")
p("Las 3 queries diagnosticadas parten de las Q1, Q3 y Q5 originales del "
  "<font face='Courier'>scripts/analyze.py</font> del Taller 3, a las que se les "
  "añadió un WHERE realista: las originales eran agregaciones sobre todo el "
  "histórico sin ningún filtro, por lo que ya constituían un full scan "
  "\"correcto\" — no había ningún acceso evitable que diagnosticar. Un dashboard "
  "real nunca pide \"todo el histórico sin filtrar\"; pide \"la última semana\" o "
  "\"las entregas fallidas de hoy\". Esa es la versión que se analiza aquí.")

p("Paso 0 — Row groups por archivo Gold", "H2")
code("fact_entregas.parquet:        1 row group  para 106,370 filas\n"
     "fact_presencia_hora.parquet:  1 row group  para  10,000 filas")
p("Este hallazgo se hizo <b>antes</b> de leer los planes individuales y resultó "
  "ser la causa raíz de los tres diagnósticos: sin importar qué tan selectivo "
  "sea el predicado de cada consulta, con un solo row group no hay min/max por "
  "bloque que el planner pueda usar para saltar lectura.")

p("Q1 — Volumen diario de entregas por severidad (última semana)", "H2")
code("SELECT t.fecha, a.severidad, COUNT(*) AS total_entregas,\n"
     "       SUM(f.entregado_flag) AS entregadas, SUM(f.fallido_flag) AS fallidas,\n"
     "       ROUND(SUM(f.fallido_flag) * 100.0 / COUNT(*), 2) AS tasa_fallo_pct\n"
     "FROM fact_entregas f\n"
     "JOIN dim_tiempo t ON f.tiempo_sk = t.tiempo_sk\n"
     "JOIN dim_alerta a ON f.alerta_sk = a.alerta_sk\n"
     "WHERE t.fecha >= '2026-10-12'\n"
     "GROUP BY t.fecha, a.severidad\n"
     "ORDER BY t.fecha")
story.append(kv_table([
    ["Campo", "Valor"],
    ["Tiempo baseline (mediana)", "3.78 ms"],
    ["Motor usado", "DuckDB 1.5.6 sobre Parquet (Gold del Taller 3)"],
    ["Operador más costoso", "TABLE_SCAN (READ_PARQUET) sobre fact_entregas.parquet — proyecta 106.370 filas del único row group antes de aplicar el filtro de fecha"],
    ["Problema identificado", "Data skipping ausente: el filtro selecciona ~27% de las filas (29.412 de 106.370), pero al haber un solo row group, DuckDB decodifica el bloque completo igual; el dynamic filter del join (tiempo_sk>=1 AND tiempo_sk<=31) no reduce nada porque cubre casi todo el dominio de dim_tiempo"],
    ["Indicador que lo revela", "Indicador 4 — data skipping ausente (confirmado con parquet_metadata(): 1 row group)"],
], col_widths=[4.2 * cm, 11 * cm]))

p("Q3 — Entregas fallidas por severidad y zona de destino", "H2")
code("SELECT a.severidad, a.zona_destino, COUNT(*) AS fallidas,\n"
     "       ROUND(AVG(f.latencia_entrega_seg), 2) AS latencia_prom_seg\n"
     "FROM fact_entregas f\n"
     "JOIN dim_alerta a ON f.alerta_sk = a.alerta_sk\n"
     "WHERE f.estado = 'fallido'\n"
     "GROUP BY a.severidad, a.zona_destino\n"
     "ORDER BY fallidas DESC")
story.append(kv_table([
    ["Campo", "Valor"],
    ["Tiempo baseline (mediana)", "3.14 ms"],
    ["Motor usado", "DuckDB 1.5.6 sobre Parquet"],
    ["Operador más costoso", "TABLE_SCAN sobre fact_entregas.parquet proyectando alerta_sk, estado, latencia_entrega_seg"],
    ["Problema identificado", "El filtro estado = 'fallido' es muy selectivo (5.405 de 106.370 filas, ~5%), exactamente el escenario donde el data skipping más aporta — pero con un solo row group no hay forma de descartar nada antes de decodificar y filtrar fila por fila"],
    ["Indicador que lo revela", "Indicador 4 — data skipping ausente (mismo archivo, mismo problema de raíz que Q1, columna de filtro distinta)"],
], col_widths=[4.2 * cm, 11 * cm]))

p("Q5 — Presencia online por hora en la zona Centro", "H2")
code("SELECT z.zona, p.hora_del_dia, COUNT(*) AS snapshots,\n"
     "       SUM(p.online_flag) AS online,\n"
     "       ROUND(SUM(p.online_flag) * 100.0 / COUNT(*), 1) AS pct_online\n"
     "FROM fact_presencia_hora p\n"
     "JOIN dim_zona z ON p.zona_sk = z.zona_sk\n"
     "WHERE z.zona = 'Centro'\n"
     "GROUP BY z.zona, p.hora_del_dia\n"
     "ORDER BY p.hora_del_dia")
story.append(kv_table([
    ["Campo", "Valor"],
    ["Tiempo baseline (mediana)", "0.91 ms"],
    ["Motor usado", "DuckDB 1.5.6 sobre Parquet"],
    ["Operador más costoso", "TABLE_SCAN sobre fact_presencia_hora.parquet (10.000 filas, único row group)"],
    ["Problema identificado", "Mismo problema estructural de data skipping ausente, pero la dimensión filtrada (zona, 3 valores posibles) tiene baja cardinalidad: aunque se repare el row group, como mucho se puede descartar 2 de 3 bloques — el techo de mejora es mucho menor que en Q1/Q3"],
    ["Indicador que lo revela", "Indicador 4 — data skipping ausente, con la salvedad de baja cardinalidad de la columna de filtro"],
], col_widths=[4.2 * cm, 11 * cm]))

story.append(PageBreak())

# ── 3. Intervenciones ──────────────────────────────────────────────
p("3. Intervenciones", "H1")
p("Se aplicó <b>una sola intervención por problema</b>, siguiendo I-3 del "
  "catálogo del taller (\"ordenar el Parquet por la columna de filtro\"), "
  "adaptada así: cada archivo de hechos se reescribió ordenado por la columna "
  "que su consulta filtra, con ROW_GROUP_SIZE = 5000 para forzar la creación de "
  "varios row groups con estadísticas min/max útiles "
  "(<font face='Courier'>scripts/intervencion.py</font>). Como Q1 y Q3 filtran "
  "columnas distintas sobre el mismo fact_entregas (fecha vs. estado), y un "
  "único orden físico solo puede favorecer una columna a la vez, se generaron "
  "dos copias: fact_entregas.parquet (ordenado por tiempo_sk, para Q1) y "
  "fact_entregas_por_estado.parquet (ordenado por estado, para Q3). Esto se "
  "documenta explícitamente en la Sección 5 porque es una decisión de diseño "
  "con una limitación real.")

p("Intervención sobre Q1", "H2")
story.append(kv_table([
    ["Campo", "Valor"],
    ["Query afectada", "Q1 — Volumen diario de entregas por severidad"],
    ["Problema identificado", "Data skipping ausente: 1 row group en fact_entregas.parquet"],
    ["Intervención aplicada", "COPY (SELECT * FROM fact_entregas ORDER BY tiempo_sk) TO fact_entregas.parquet (FORMAT PARQUET, ROW_GROUP_SIZE 5000) → 18 row groups"],
    ["Plan ANTES", "TABLE_SCAN, 1 row group, 106.370 filas decodificadas antes del filtro de dynamic filter del join"],
    ["Plan DESPUÉS", "TABLE_SCAN sobre el mismo archivo ya ordenado y fragmentado en 18 row groups; tiempo total consistentemente menor en las 5 corridas"],
    ["Tiempo antes (mediana)", "3.78 ms"],
    ["Tiempo después (mediana)", "2.60 ms"],
    ["Factor de mejora", "1.45x"],
    ["Justificación técnica", "Al ordenar por tiempo_sk y fragmentar en bloques de 5.000 filas, cada row group queda con un rango estrecho de tiempo_sk. El planner puede evaluar el predicado de fecha contra esas estadísticas min/max y saltar los row groups fuera de rango sin decodificarlos. La mejora no es dramática porque el archivo (1.3 MB) es pequeño: la ganancia viene de menos CPU de decodificación, no de I/O de disco evitado"],
], col_widths=[4.2 * cm, 11 * cm]))

p("Intervención sobre Q3", "H2")
story.append(kv_table([
    ["Campo", "Valor"],
    ["Query afectada", "Q3 — Entregas fallidas por severidad y zona de destino"],
    ["Problema identificado", "Mismo problema de raíz (1 row group), pero el filtro es sobre estado, no tiempo_sk: el orden físico que ayuda a Q1 no ayuda a Q3"],
    ["Intervención aplicada", "Segunda copia ordenada por estado: fact_entregas_por_estado.parquet, ROW_GROUP_SIZE 5000 → 18 row groups"],
    ["Plan ANTES", "TABLE_SCAN sobre fact_entregas.parquet (1 row group), decodifica las 106.370 filas para evaluar estado = 'fallido'"],
    ["Plan DESPUÉS", "TABLE_SCAN sobre fact_entregas_por_estado.parquet (18 row groups); las filas con estado = 'fallido' quedan contiguas en un subconjunto pequeño de row groups, el resto se descarta por min/max"],
    ["Tiempo antes (mediana)", "3.14 ms"],
    ["Tiempo después (mediana)", "1.99 ms"],
    ["Factor de mejora", "1.58x"],
    ["Justificación técnica", "Mayor factor de mejora de las tres, consistente con que estado = 'fallido' es el filtro más selectivo (~5% de las filas): cuanto más selectivo el predicado y mejor alineado con el orden físico, más row groups completos se descartan. Confirma que la ganancia de data skipping escala con la selectividad del filtro, no solo con su existencia"],
], col_widths=[4.2 * cm, 11 * cm]))

p("Intervención sobre Q5", "H2")
story.append(kv_table([
    ["Campo", "Valor"],
    ["Query afectada", "Q5 — Presencia online por hora en zona Centro"],
    ["Problema identificado", "Mismo problema de raíz (1 row group en fact_presencia_hora.parquet), agravado por baja cardinalidad de zona (3 valores)"],
    ["Intervención aplicada", "COPY (... ORDER BY zona_sk) TO fact_presencia_hora.parquet (ROW_GROUP_SIZE 5000) → 2 row groups"],
    ["Plan ANTES", "TABLE_SCAN, 1 row group, 10.000 filas decodificadas"],
    ["Plan DESPUÉS", "TABLE_SCAN sobre archivo ordenado por zona_sk, 2 row groups; el filtro zona = 'Centro' como mucho descarta 1 de los 2 row groups"],
    ["Tiempo antes (mediana)", "0.91 ms"],
    ["Tiempo después (mediana)", "0.87 ms"],
    ["Factor de mejora", "1.05x"],
    ["Justificación técnica", "Mejora marginal y esperada, no un error: con solo 3 valores posibles de zona y 10.000 filas, el espacio de bloques podables es mínimo (máx. 1 de 2), y la tabla ya es pequeña. El dataset es \"demasiado pequeño para que el overhead del reordenamiento valga la pena\" en este caso — un 1.05x con esta explicación es más honesto que forzar una narrativa de mejora dramática"],
], col_widths=[4.2 * cm, 11 * cm]))

story.append(PageBreak())

# ── 4. Tabla de benchmarks y reflexión ─────────────────────────────
p("4. Tabla de benchmarks y reflexión", "H1")
p("4.1 Tabla de benchmarks consolidada", "H2")

bench_rows = [
    ["Query", "Problema (vocab. M4)", "Intervención", "t antes\n(ms)", "t después\n(ms)", "Factor", "¿Cambió\nel plan?"],
    ["Q1 — Volumen diario\npor severidad",
     "Data skipping ausente: fact_entregas en 1 row group, filtro de fecha no puede podar bloques",
     "I-3: ordenar por tiempo_sk, ROW_GROUP_SIZE=5000 → 18 row groups",
     "3.78", "2.60", "1.45x", "Sí — menos filas decodificadas"],
    ["Q3 — Entregas fallidas\npor severidad/zona",
     "Data skipping ausente: filtro estado='fallido' (~5% filas) no puede podar bloques",
     "I-3: ordenar por estado, ROW_GROUP_SIZE=5000 → 18 row groups",
     "3.14", "1.99", "1.58x", "Sí — predicado alineado con orden físico"],
    ["Q5 — Presencia online\npor zona/hora",
     "Data skipping ausente, agravado por baja cardinalidad de zona (3 valores)",
     "I-3: ordenar por zona_sk, ROW_GROUP_SIZE=5000 → 2 row groups",
     "0.91", "0.87", "1.05x", "No, de forma apreciable — techo de poda limitado"],
]
story.append(kv_table(bench_rows, col_widths=[2.7*cm, 3.6*cm, 3.6*cm, 1.4*cm, 1.5*cm, 1.3*cm, 2.1*cm]))

p("Sobre resultados modestos", "H2")
p("Ninguna de las tres mejoras es espectacular, y eso es consistente con lo que "
  "advierte la guía del taller: <i>\"Un sistema que mejora un 5% con una "
  "justificación técnica correcta vale más que uno que mejora un 90% sin "
  "entender por qué.\"</i> La explicación de por qué las mejoras son modestas y "
  "no dramáticas es, en sí misma, parte del hallazgo:")
bullets([
    "<b>El dataset es pequeño</b> (1.3 MB para fact_entregas, 106.370 filas). Cabe "
    "cómodamente en la caché de página del SO tras el warm-up, así que el costo "
    "dominante es decodificación de columnas en CPU, no I/O de disco. El "
    "total_bytes_read reportado por el profiler fue prácticamente idéntico "
    "antes y después (~1.7 KB en un scan aislado), confirmando que la ganancia "
    "viene de decodificar menos filas, no de leer menos bytes de disco.",
    "<b>La selectividad del filtro importa más que la sola presencia de row "
    "groups múltiples.</b> Q3 (filtro ~5%) mejoró más que Q1 (filtro ~27%), que "
    "a su vez mejoró más que Q5 (columna de 3 valores). El techo de mejora de "
    "data skipping está acotado por cuántos row groups completos se pueden "
    "descartar.",
    "<b>Un único orden físico no sirve a dos predicados distintos a la vez.</b> "
    "Ordenar fact_entregas por tiempo_sk ayuda a Q1 pero no a Q3, y viceversa — "
    "de ahí la necesidad de dos copias del mismo archivo.",
])

p("4.2 Reflexión arquitectural", "H2")

p("<b>¿El ordenamiento del Gold por la columna de filtro debería ser parte del "
  "pipeline transform_gold.py desde el principio, o es un paso de optimización "
  "posterior?</b>")
p("Debería decidirse desde el principio, pero no de forma única: el Taller 3 "
  "generó fact_entregas.parquet con df.to_parquet(..., index=False) sin "
  "especificar orden ni row_group_size, dejando el default de pandas/pyarrow "
  "(que para ~106.000 filas resultó en un solo row group). Ese no es un error "
  "del Taller 3 — en esa etapa el objetivo era construir el modelo dimensional "
  "correcto, no optimizarlo — pero sí es una decisión de diseño con costo real "
  "que el Taller 4 hace visible. La lección correcta no es \"ordenar siempre por "
  "una columna fija\", sino que el pipeline Gold debería declarar explícitamente "
  "el patrón de acceso esperado y fijar ROW_GROUP_SIZE de forma consciente, en "
  "vez de heredar un default pensado para datasets mucho más grandes.")

p("<b>¿El modelo dimensional del Taller 3 facilita o dificulta el uso de "
  "índices/orden físico? ¿Hay alguna clave foránea que debería tener orden y no "
  "lo tiene?</b>")
p("El esquema en constelación (fact_entregas + fact_presencia_hora compartiendo "
  "dim_tiempo, dim_usuario, dim_zona) facilita el join pushdown — DuckDB ya "
  "genera dynamic filters automáticos en el HASH_JOIN contra las dimensiones — "
  "pero no resuelve el problema de fondo, porque esos filtros dinámicos solo "
  "ayudan si hay row groups que podar, y el archivo base no los tenía. La clave "
  "que más lo necesitaba y no lo tenía era tiempo_sk en fact_entregas: es la "
  "dimensión que casi cualquier consulta analítica de series temporales va a "
  "filtrar primero, y el pipeline no la usaba como criterio de orden físico al "
  "escribir el Parquet.")

p("<b>¿Las consultas analíticas que escriben siguen el orden de la clave de "
  "particionamiento? Si no, ¿qué implicación tiene eso para el data "
  "skipping?</b>")
p("No completamente, y eso es justo lo que este taller expuso: Q1 filtra por "
  "tiempo, Q3 filtra por estado, Q5 filtra por zona. Un solo orden físico no "
  "puede optimizar las tres a la vez. En producción esto apunta a dos caminos: "
  "(a) particionar por año/mes con Hive-style (I-6) para las consultas de rango "
  "temporal — probablemente las más frecuentes en un dashboard operacional — y "
  "aceptar que las consultas por estado sigan sin skipping óptimo, o (b) "
  "mantener una vista materializada adicional pre-agregada por estado si esa "
  "consulta resulta crítica para el negocio.")

p("<b>¿Hay alguna consulta inherentemente lenta por acceder a datos de baja "
  "cardinalidad, y ningún índice puede ayudar? ¿Cómo se resolvería en "
  "producción?</b>")
p("Sí: Q5, filtrada por zona (solo 3 valores). Ningún reordenamiento de Parquet "
  "ni índice puede podar más de 2 de los 3 bloques posibles, porque la "
  "cardinalidad del predicado es el techo real de cualquier técnica de data "
  "skipping. En producción esto se resolvería con una vista materializada o "
  "pre-agregación (fact_presencia_zona_hora agregada por zona y hora_del_dia, "
  "con ~72 filas en vez de 10.000) en lugar de intentar optimizar el acceso a "
  "la tabla de grano fino — reordenamiento y particionado son técnicas de "
  "acceso, no sustituyen una decisión de agregación cuando la cardinalidad de "
  "la dimensión filtrada es intrínsecamente baja.")

story.append(Spacer(1, 0.6 * cm))
p("<i>Informe generado para el Taller 4 — Tópicos Avanzados en Bases de Datos, "
  "ITM 2026. Evidencia completa (planes EXPLAIN ANALYZE antes/después, scripts "
  "de diagnóstico e intervención) en el repositorio, carpeta ENTREGA4/.</i>", "Caption")

doc.build(story)
print(f"PDF generado: {OUT}")
