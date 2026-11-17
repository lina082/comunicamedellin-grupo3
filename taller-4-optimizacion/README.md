# Taller 4 — Laboratorio de Optimización de Consultas

**Equipo 3** · Tópicos Avanzados en Bases de Datos · ITM 2026

Diagnóstico, intervención y medición sobre el pipeline de ComunicaMedellín
construido en el Taller 3 (`../taller-3-lakehouse`). Usa EXPLAIN ANALYZE de
DuckDB para diagnosticar 3 consultas analíticas, identifica una causa raíz
común (data skipping ausente por falta de fragmentación en row groups del
Parquet Gold), aplica la intervención I-3 del catálogo del taller
(ordenar + reducir `ROW_GROUP_SIZE`), y mide el efecto real.

La carpeta [`gold_taller3/`](gold_taller3/) es una copia congelada del
Parquet Gold exacto sobre el que se hizo este diagnóstico (el estado de
`taller-3-lakehouse/lakehouse/gold/` puede seguir cambiando con el
tiempo). Todos los scripts de este taller leen de esa copia, para que
los resultados del informe sigan siendo reproducibles.

## Entregable principal

[`informe_tecnico.pdf`](informe_tecnico.pdf) — informe técnico completo
(resumen ejecutivo, diagnóstico, intervenciones, tabla de benchmarks,
reflexión arquitectural).

## Cómo reproducir

```bash
pip install duckdb pandas reportlab python-pptx
python3 scripts/diagnostico.py       # EXPLAIN ANALYZE + baseline sobre gold_taller3/
python3 scripts/intervencion.py      # genera lakehouse_optimizado/ (Parquet ordenado, row groups chicos)
python3 scripts/medicion_despues.py  # EXPLAIN ANALYZE + medición sobre lakehouse_optimizado/
python3 scripts/generar_informe_pdf.py
python3 scripts/generar_slides.py    # genera Taller4_ComunicaMedellin.pptx
```

## Demo en vivo (front conectado por API)

Igual que el resto del proyecto, la sustentación no se hace desde la
terminal: se corre desde [`lakehouse.html`](lakehouse.html), el mismo
panel del Taller 3 extendido con 3 pasos nuevos (L6 diagnóstico, L7
intervención, L8 medición), que ejecutan los scripts reales de esta
carpeta vía WebSocket.

```bash
cd ../api && uvicorn main:app --port 8765   # levanta la API (puerto 8765)
```

Luego abre `lakehouse.html` en el navegador y entra a los pasos **06 ·
DIAG**, **07 · FIX** y **08 · BENCH** al final de la franja de pasos.
Cada botón "ejecutar" corre el script real (`diagnostico.py`,
`intervencion.py`, `medicion_despues.py`) contra `gold_taller3/` y
muestra en vivo el plan EXPLAIN ANALYZE, los row groups generados y las
medianas, exactamente como en `informe_tecnico.pdf`. El router de estos
endpoints está en [`../api/optimizacion.py`](../api/optimizacion.py).

## Estructura

```
taller-4-optimizacion/
├── scripts/
│   ├── diagnostico.py          ← Paso 1: EXPLAIN ANALYZE + baseline de Q1/Q3/Q5
│   ├── intervencion.py         ← Paso 2: reescribe Gold ordenada, row groups chicos
│   ├── medicion_despues.py     ← Paso 2: EXPLAIN ANALYZE + medición post-intervención
│   ├── generar_informe_pdf.py  ← genera informe_tecnico.pdf
│   └── generar_slides.py       ← genera Taller4_ComunicaMedellin.pptx
├── evidencia/
│   ├── Q1/Q3/Q5_plan_baseline.txt   ← EXPLAIN ANALYZE antes (texto completo)
│   ├── Q1/Q3/Q5_plan_despues.txt    ← EXPLAIN ANALYZE después
│   ├── resumen_baseline.md
│   └── resumen_despues.md
├── gold_taller3/                ← copia congelada de la Gold diagnosticada
├── lakehouse_optimizado/        ← copia de la Gold con la intervención aplicada
├── lakehouse.html                ← demo en vivo (front conectado a ../api/optimizacion.py)
├── informe_tecnico.md           ← fuente del informe
├── informe_tecnico.pdf          ← entregable final
├── Taller4_ComunicaMedellin.pptx ← diapositivas de sustentación
└── Taller#4_Lab de Optimización y Rendimiento.pdf   ← enunciado
```

## Resumen de resultados

| Query | Problema | Intervención | Antes | Después | Factor |
|---|---|---|---|---|---|
| Q1 — Volumen diario por severidad (última semana) | Data skipping ausente (1 row group) | Ordenar por `tiempo_sk`, row_group_size=5000 | 3.78 ms | 2.60 ms | 1.45x |
| Q3 — Entregas fallidas por severidad/zona | Data skipping ausente (1 row group) | Ordenar por `estado`, row_group_size=5000 | 3.14 ms | 1.99 ms | 1.58x |
| Q5 — Presencia online por zona/hora | Data skipping ausente + baja cardinalidad | Ordenar por `zona_sk`, row_group_size=5000 | 0.91 ms | 0.87 ms | 1.05x |

Ver `informe_tecnico.pdf` sección 4.1 para el análisis de por qué las
mejoras son modestas (dataset pequeño, costo dominante es CPU de
decodificación, no I/O de disco) y 4.2 para la reflexión arquitectural.

## Integrantes

- Simón Ramírez
- Elizabeth Méndez
- Nilton Orejuela
- Julián Vélez
- Lina Sevillano

## Docente

Roberto Carlos Rahamut Suteu — ITM 2026
