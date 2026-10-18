# ComunicaMedellín — Mini-Lakehouse (Taller 3)

**Equipo 3** · Tópicos Avanzados en Bases de Datos · ITM 2026

Extiende el proyecto operacional de ComunicaMedellín (carpeta raíz de este
repositorio, Módulos 1 y 2) con una capa analítica mínima pero completa: un pipeline
ETL que mueve datos del OLTP (Cassandra) a un lakehouse en capas
Bronze/Silver/Gold sobre Parquet, modelado con un esquema dimensional en
constelación y consultado con DuckDB.

## Descripción

El dominio de negocio es el mismo del sistema operacional: alertas
ciudadanas con fan-out masivo y presencia de usuarios en tiempo real. Este
taller responde una pregunta distinta a la del Módulo 1 — ya no "¿qué
motor OLTP usar?" sino "¿qué tan efectivo fue el fan-out y qué patrones
de presencia hay por zona?", algo que Cassandra no puede responder sin
`ALLOW FILTERING` (ver demo `demo-analitica` del proyecto base).

Se modelan dos procesos de negocio en un **esquema en constelación**:

- **FACT_ENTREGAS** — granularidad: una fila = un intento de entrega de
  una alerta a un usuario (fuente: `estado_entrega` + `historial_usuario`).
- **FACT_PRESENCIA_HORA** — granularidad: una fila = un snapshot de
  presencia de un usuario en una hora (fuente: `presencia`).

Ver el diseño completo (las 5 preguntas del Paso 1) en
[`docs/modelo_dimensional.md`](docs/modelo_dimensional.md).

## Arquitectura

```
Cassandra (OLTP) → Bronze (raw) → Silver (limpio) → Gold (dimensional) → DuckDB
```

Ver diagrama ASCII completo en
[`docs/arquitectura_lakehouse.txt`](docs/arquitectura_lakehouse.txt) y el
diagrama del modelo estrella/constelación en
[`docs/modelo_dimensional.dbml`](docs/modelo_dimensional.dbml) (pegar en
[dbdiagram.io](https://dbdiagram.io) para exportar el PNG).

Herramientas: **Python + pandas** (ETL), **DuckDB** (motor analítico
columnar), **Parquet** (formato del lake), **cassandra-driver** (lectura
del OLTP real).

## Requisitos

- Python 3.10+
- El clúster de Cassandra del proyecto base corriendo (raíz del repo):
  ```bash
  cd ..
  make up       # levanta cass1/cass2/cass3 (~2 min la primera vez)
  make schema   # crea el keyspace 'comunica'
  make seed     # 10.000 usuarios simulados
  ```
- `pip install -r requirements.txt`

## Poblar datos de alertas (una sola vez)

El Módulo 1 solo siembra usuarios (`presencia`). Este taller necesita
además volumen y variedad en `alertas`, `historial_usuario` y
`estado_entrega`, así que agrega su propio seed:

```bash
python scripts/seed_alertas.py
```

Genera 60 alertas reales (severidades `info`/`advertencia`/`critica`,
repartidas en los últimos 30 días y en las 3 zonas) y hace fan-out real a
una muestra de usuarios, escribiendo en Cassandra. Solo hace falta
correrlo una vez; el pipeline del lakehouse lee lo que ya está en el
clúster.

## Ejecutar el pipeline completo

```bash
pip install -r requirements.txt
python scripts/seed_alertas.py   # solo la primera vez
python main.py
```

`main.py` corre `extract.py → transform_silver.py → transform_gold.py →
analyze.py` en secuencia y se detiene si algún paso falla.

## Resultados del Benchmark

Consulta: contar entregas fallidas sobre toda la tabla de hechos (que en
Cassandra exige recorrer `estado_entrega` completa con `ALLOW FILTERING`,
porque `estado` no es parte de la partition key).

| Motor | Query | Tiempo | Filas |
|---|---|---|---|
| Cassandra (OLTP, `ALLOW FILTERING`) | Entregas fallidas, todo el dataset | ~60 ms | 24.002 |
| DuckDB (columnar, sobre Gold) | misma consulta | ~1.5 ms | 24.002 |

**Factor de mejora: ~40-80x** (varía por corrida; ver salida real de
`python scripts/analyze.py`).

Con ~24.000 filas la diferencia ya es significativa porque la consulta en
Cassandra fuerza un escaneo completo de todas las particiones del
clúster (lo que el modelo NoSQL del Módulo 2 justamente evita para las
consultas de servicio); DuckDB en cambio solo lee las dos columnas
(`fallido_flag`) que necesita del Parquet columnar. La ventaja crecería
aún más con datasets de cientos de miles de filas, donde Cassandra además
empieza a sufrir timeouts de coordinador en el `ALLOW FILTERING`.

## Reporte de calidad Silver

Generado automáticamente en cada corrida: [`docs/reporte_calidad.txt`](docs/reporte_calidad.txt).

## Demo interactiva (como la del Módulo 2)

Igual que `frontend/taller.html` (Módulo 2), hay una página con un botón
por cada paso del taller que ejecuta el script real (no una simulación) y
muestra el comando, la salida y las métricas en vivo:

```bash
cd ..
make api          # terminal 1 → API FastAPI en :8765 (monta api/lakehouse.py)
```

Luego abre [`lakehouse.html`](lakehouse.html) en el navegador (doble clic,
sin build ni servidor de frontend). Los 6 puntos:

| Punto | Qué ejecuta de verdad |
|---|---|
| 00 · Poblar alertas | `scripts/seed_alertas.py` — una vez, antes del resto |
| 01 · Bronze | `scripts/extract.py` contra Cassandra real |
| 02 · Silver | `scripts/transform_silver.py` + reporte de calidad |
| 03 · Gold | `scripts/transform_gold.py` — 4 dimensiones + 2 hechos |
| 04 · Las 5 consultas | `scripts/analyze.py`, tiempos medidos reales |
| 05 · Benchmark | Cassandra (`ALLOW FILTERING`) vs DuckDB, factor de mejora real |

El backend de esta demo (`../api/lakehouse.py`) corre cada script del
taller con `subprocess` y parsea su salida real para emitirla paso a paso
por WebSocket — el mismo patrón que `../api/taller.py` del Módulo 2.

## Estructura

```
taller-3-lakehouse/
├── lakehouse/
│   ├── bronze/              ← Parquet raw de Cassandra (con timestamp)
│   ├── silver/               ← Parquet limpio
│   └── gold/                 ← modelo dimensional (4 dims + 2 facts)
├── scripts/
│   ├── seed_alertas.py       ← puebla alertas/historial/entregas reales
│   ├── extract.py            ← Paso 2: extracción Bronze
│   ├── transform_silver.py   ← Paso 3: limpieza + reporte de calidad
│   ├── transform_gold.py     ← Paso 4: modelo dimensional
│   └── analyze.py            ← Paso 5: 5 consultas + benchmark
├── docs/
│   ├── modelo_dimensional.md         ← Paso 1: las 5 preguntas de diseño
│   ├── modelo_dimensional.dbml       ← diagrama fuente para dbdiagram.io
│   ├── modelo_dimensional_visual.html← versión ilustrada del Paso 1
│   ├── arquitectura_lakehouse.txt
│   └── reporte_calidad.txt           ← generado por transform_silver.py
├── lakehouse.html             ← demo interactiva (ver sección anterior)
├── Taller.pdf                 ← enunciado original del docente
├── requirements.txt
└── main.py                    ← ejecuta todo el pipeline en orden
```

El backend de la demo interactiva (`api/lakehouse.py`) vive en `../api/`
junto al resto de la API del proyecto, no dentro de esta carpeta.

## Integrantes

- Lina S.

## Docente

Roberto Carlos Rahamut Suteu — ITM 2026
