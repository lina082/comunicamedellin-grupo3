# ComunicaMedellín — Fan-out Masivo y Presencia en Tiempo Real

**Equipo 3** · Tópicos Avanzados de Bases de Datos · Módulo 1 · ITM 2026

Canal de alertas ciudadanas con más de 100.000 suscriptores: entrega casi
simultánea de alertas urgentes y seguimiento del estado de presencia de cada
usuario.

---

## Arranque rápido

```bash
# 1. Levantar el clúster (~2 min la primera vez)
make up

# 2. Crear el esquema en Cassandra
make schema

# 3. Cargar 10.000 usuarios simulados
make seed

# 4. Demo principal del reto: ONE vs QUORUM
make demo-consistencia
```

Requisitos: Docker, Docker Compose, Python 3.10+, ~3 GB de RAM libre.

```bash
pip install -r demo/requirements.txt
```

Ver todos los comandos disponibles: `make help`

---

## Stack

| Componente | Rol | Sección del módulo | Licencia |
|---|---|---|---|
| **Apache Cassandra** ×3 | Presencia + historial · AP · `ONE`/`QUORUM` | 1.2, 1.3, 3.3 | Apache 2.0 |
| **Apache Kafka** (KRaft) | Fan-out particionado por zona | 4.2 | Apache 2.0 |
| **Apache Zookeeper** | Perfil de aprendizaje (coordinación) | 1.3 | Apache 2.0 |
| **ClickHouse** | Ruta analítica columnar | 2.2, 2.4 | Apache 2.0 |
| **FastAPI** | API + WebSocket | — | MIT |
| **HTML/JS** | Frontend sin build | — | — |

Todo el stack es software libre. Consumo aproximado: **2.8 GB**.

---

## Las cuatro cargas de trabajo

| | Carga | Tipo | Patrón | Sección |
|---|---|---|---|---|
| C1 | Publicar alerta | OLTP ligero | 1 escritura, baja frecuencia | 2.1 |
| C2 | Fan-out a 100.000 | Streaming | 1 escritura → 100K entregas | 4.2 |
| C3 | Historial por usuario | OLTP escritura | 100K inserciones por alerta | 1.3, 2.1 |
| C4 | Presencia | Clave-valor | ~10K escrituras/s, alta rotación | 1.2, 1.3 |

**Hallazgo central:** C2 y C4 parecen la misma carga pero exigen garantías
opuestas. La presencia tolera desactualización; la entrega de la alerta no
tolera pérdida. Eso obliga a **dos decisiones CAP distintas dentro del mismo
sistema**.

---

## Decisión CAP

**Presencia → AP, nivel `ONE`.** Se sacrifica Consistencia. Que el estado
«en línea» llegue 2 s tarde no altera ninguna decisión de negocio. Elegir CP
sería peor: durante una partición el sistema rechazaría *heartbeats* y la app
mostraría a todos desconectados.

**Historial → AP, escritura `QUORUM`.** La alerta no puede perderse (seguridad
pública). `QUORUM` asegura durabilidad ante la caída de un nodo.

### Contraste con TuBoleta

| | TuBoleta (Equipo 1) | ComunicaMedellín (Equipo 3) |
|---|---|---|
| Recurso | Silla A-12: finito y exclusivo | Mensaje: replicable |
| Conflicto | Dos compradores compiten | Nadie compite |
| Si falla C | Doble venta → contingencia legal | Presencia desfasada 2 s |
| Decisión | **CP** (Raft, serializable) | **AP** (eventual, ajustable) |

---

## Demos

### Requeridas por el reto

```bash
make status              # 3 nodos y distribución de tokens
make seed                # 10.000 usuarios concurrentes
make demo-consistencia   # ONE vs QUORUM leyendo de 2 nodos
make demo-fanout         # Fan-out particionado por zona
```

### Frontend en vivo

```bash
make api        # terminal 1 → http://localhost:8765
make frontend   # terminal 2 → http://localhost:4173
```

Muestra la lectura del mismo usuario desde los nodos 1 y 3 en paralelo, con
conmutador `ONE`/`QUORUM` en caliente. Con `ONE` y el botón «Martillo»
aparecen divergencias; con `QUORUM` desaparecen y sube la latencia.

### Módulo 2 — pipelines del taller (recomendado para la sustentación)

Una página local con **un botón por cada punto del enunciado**. Al pulsarlo
ejecuta la operación real contra el clúster y muestra el pipeline por dentro:
el comando que corre, lo que responde y por qué.

```bash
make api       # terminal 1 → API en :8765
make taller    # terminal 2 → http://localhost:4173/taller.html
```

| Punto | Qué ejecuta de verdad |
|---|---|
| 01 · Las cinco consultas | Q1–Q5 medidas, con el token y las réplicas donde vive el dato |
| 02 · Fan-out a 10.000 | escribe 10.000 filas con QUORUM; throughput y p50/p95/p99 |
| 03 · Caída real de un nodo | `docker stop` de verdad mientras se escribe |
| 04 · Reparación | conteo de particiones, `nodetool repair`, conteo otra vez |
| 05 · Idempotencia | reenvía el mismo evento 3 veces, con y sin el bug |
| 06 · Analítica | la agregación en ClickHouse y forzada en Cassandra |

Los puntos 03 y 04 están encadenados a propósito: el 03 deja el nodo caído y el
04 lo devuelve y lo repara. La cabecera de la página avisa si un nodo quedó abajo.

### Módulo 2 — demos por terminal

```bash
make demo-fanout         # fan-out a 10.000: throughput + p50/p95/p99
make demo-idempotencia   # reenvía el mismo evento: no duplica
make demo-reparacion     # nodo caído prolongado → hints, repair
make demo-analitica      # misma consulta OLAP: ClickHouse vs Cassandra
make hotspot             # tamaño de partición por tabla
make modelo              # compila docs/modelo-datos.pdf
```

Diapositivas del Módulo 2: [`docs/presentacion-modulo2.pdf`](docs/presentacion-modulo2.pdf)
(`make slides-m2`). Guion con tiempos, reparto y preguntas probables:
[`demo/GUION-EXPOSICION.md`](demo/GUION-EXPOSICION.md).

`demo-fanout` entrega 9.999 alertas en 1,33 s (**7.509/s**) con latencia p50
93 ms / p95 143 ms / p99 156 ms. El *throughput* mide el caudal del sistema; la
latencia, lo que espera una escritura concreta.

`demo-analitica` ejecuta la misma agregación en los dos motores: Cassandra
**rechaza** el `GROUP BY` fuera de la clave primaria, y forzada con
`ALLOW FILTERING` cuesta 7,79 µs/fila frente a 0,04 en ClickHouse (200× más).

`demo-idempotencia` contrasta el timestamp del evento (1 fila tras 3 reenvíos)
con el del *worker* (3 filas: el bug clásico del fan-out).

`demo-reparacion` desactiva *hinted handoff*, tumba un nodo, escribe 2.000
particiones sin él y mide el hueco antes y después de `nodetool repair`:

| Particiones en disco | cass1 | cass2 *(caído)* | cass3 |
|---|---|---|---|
| línea base | 0 | 0 | 0 |
| tras escribir sin cass2 | 1.999 | **0** | 1.999 |
| tras `nodetool repair` | 1.999 | **1.999** | 1.999 |

> El hueco **no** se ve con un `SELECT`: con RF=3 sobre 3 nodos los tres son
> réplica de todo, así que una lectura con `ONE` dirigida a cass2 la sirve
> cualquier réplica viva. La divergencia es interna — solo visible con
> `nodetool tablestats`. Eso *es* la consistencia eventual.

### Extras (más allá de lo pedido)

```bash
make demo-nodo-caido   # tumba un nodo: AP sigue respondiendo
make ch-seed           # 1M de eventos en ClickHouse
make ch-demo           # agregaciones columnares + ratio de compresión
make kafka-replay      # relectura desde offset 0 (Kappa, Sec. 4.2)
make up-zk && make zk-shell   # coordinación con Zookeeper
```

---

## Demo dentro de Cassandra (CQL en vivo)

Los `make` de arriba orquestan desde fuera. Esta sección es para **entrar al
motor y mostrar los datos y el comportamiento distribuido en vivo**, que es lo
que se defiende en la sustentación.

### Entrar al shell

```bash
docker exec -it cass1 cqlsh     # nodo de medellin
docker exec -it cass3 cqlsh     # nodo de miami (para contrastar)
```

Ya dentro, todo lo que sigue se escribe en el prompt `cqlsh>`.

### 1. Reconocer el terreno

```sql
DESCRIBE KEYSPACES;
DESCRIBE KEYSPACE comunica;     -- muestra el RF por datacenter
USE comunica;
DESCRIBE TABLES;                -- alertas, historial_usuario, presencia
DESCRIBE TABLE presencia;
```

El `DESCRIBE KEYSPACE` imprime la línea que sustenta toda la decisión de diseño:

```
replication = {'class': 'NetworkTopologyStrategy', 'medellin': '2', 'miami': '1'}
```

### 2. Ver los datos

```sql
-- usuarios (10.000 sembrados por seed_usuarios.py)
SELECT usuario_id, nombre, comuna, zona FROM presencia LIMIT 10;

-- consulta eficiente: por partition key, va directo a la réplica
SELECT * FROM presencia WHERE usuario_id = 'CC65027980';

-- alertas publicadas
SELECT titulo, severidad, zona_destino, total_envios FROM alertas LIMIT 10;

-- historial de un usuario, ya ordenado por CLUSTERING ORDER (enviado_en DESC)
SELECT enviado_en, titulo, severidad
  FROM historial_usuario
  WHERE usuario_id = 'CC65027980';
```

### 3. Dónde vive físicamente cada dato

Esto hace visible el particionado por token — el corazón del modelo:

```sql
SELECT token(usuario_id), usuario_id, nombre
  FROM presencia WHERE usuario_id = 'CC65027980';
```

```
 system.token(usuario_id) | usuario_id | nombre
--------------------------+------------+-------------------------------
     -9222786510659648211 | CC65027980 | Jorge Libardo Hernández Gómez
```

Y desde fuera, qué nodos guardan esa partición:

```bash
docker exec cass1 nodetool getendpoints comunica presencia CC65027980
# 172.24.0.4  → cass1  (medellin)
# 172.24.0.6  → cass3  (miami)
# 172.24.0.5  → cass2  (medellin)
```

Tres réplicas: dos en medellin (RF=2) y una en miami (RF=1). Coincide con el
keyspace.

### 4. LOCAL_QUORUM vs QUORUM, demostrado con TRACING

**Esta es la demo central.** `TRACING ON` imprime cada mensaje entre nodos, así
que la diferencia deja de ser teórica y se ve en pantalla.

```sql
CONSISTENCY LOCAL_QUORUM;
TRACING ON;
SELECT nombre FROM presencia WHERE usuario_id = 'CC65027980';
```

En el trace solo aparecen nodos de **medellin** (`172.24.0.4` y `172.24.0.5`):

```
reading digest from /172.24.0.5:7000
Sending READ_REQ message to /172.24.0.5:7000
```

Ahora el contraste:

```sql
CONSISTENCY QUORUM;
SELECT nombre FROM presencia WHERE usuario_id = 'CC65027980';
```

Aparece **miami** (`172.24.0.6`) — la consulta cruza entre datacenters:

```
reading digest from /172.24.0.6:7000
Sending READ_REQ message to /172.24.0.6:7000
```

Ese salto es exactamente la latencia que `LOCAL_QUORUM` evita. Para cerrar:

```sql
TRACING OFF;
CONSISTENCY LOCAL_ONE;    -- vuelta al modo rápido
```

### 5. Escribir y ver la replicación

```sql
CONSISTENCY LOCAL_QUORUM;

INSERT INTO alertas (alerta_id, titulo, cuerpo, severidad, zona_destino, publicada_en, total_envios)
VALUES (uuid(), 'Simulacro de sismo', 'Evacuación preventiva 10:00 a.m.',
        'alta', 'norte', toTimestamp(now()), 0);

SELECT titulo, severidad, publicada_en FROM alertas LIMIT 3;
```

Salir (`exit`) y leer **desde el otro datacenter** para comprobar que replicó:

```bash
docker exec cass3 cqlsh -e "SELECT titulo, zona_destino FROM comunica.alertas LIMIT 5;"
```

### 6. Tolerancia a fallos, en vivo

```bash
docker stop cass2                       # cae un nodo de medellin
docker exec cass1 nodetool status       # cass2 aparece como 'DN'
```

Ya con el nodo caído, desde `cqlsh`:

```sql
CONSISTENCY ONE;
SELECT nombre FROM presencia WHERE usuario_id = 'CC65027980';   -- ✅ responde

CONSISTENCY LOCAL_QUORUM;
SELECT nombre FROM presencia WHERE usuario_id = 'CC65027980';   -- ❌ falla
```

El error es el resultado esperado, y conviene explicarlo así:

> `medellin` tiene RF=2, luego `LOCAL_QUORUM = floor(2/2)+1 = 2`. Con un nodo
> caído solo queda 1 réplica viva, así que no hay quórum. Es la elección **AP**
> del teorema CAP hecha explícita: con `ONE` se sacrifica consistencia y se
> conserva disponibilidad; con `LOCAL_QUORUM` ocurre lo contrario.

Restaurar:

```bash
docker start cass2
docker exec cass1 nodetool status       # esperar a que vuelva a 'UN'
```

### 7. Qué NO se puede consultar (y por qué)

Vale la pena mostrarlo: justifica el modelado de datos ante el jurado.

```sql
SELECT * FROM presencia WHERE comuna = 'Manrique';
```

```
InvalidRequest: ... cannot be restricted as the query would require filtering
```

`comuna` no es partition key, así que Cassandra tendría que barrer todos los
nodos. Se puede forzar con `ALLOW FILTERING`, pero eso es precisamente la señal
de que **la tabla no se diseñó para esa consulta**. La regla del modelo: una
tabla por patrón de acceso, y la partition key se elige desde la consulta, no
desde la entidad.

Lo mismo con agregaciones:

```sql
SELECT zona, COUNT(*) FROM presencia GROUP BY zona;
-- InvalidRequest: Group by is currently only supported on the columns of the PRIMARY KEY
```

Ese trabajo analítico es el que se delega a **ClickHouse** (`make ch-demo`).

### Chuleta de operación (fuera de cqlsh)

```bash
docker exec cass1 nodetool status                 # UN/DN por nodo y datacenter
docker exec cass1 nodetool describecluster        # snitch, partitioner, esquema
docker exec cass1 nodetool ring                   # distribución de tokens
docker exec cass1 nodetool tablestats comunica.presencia
docker exec cass1 nodetool netstats               # replicación en curso
```


---

## Sobre Zookeeper

Kafka 3.5 marcó Zookeeper como *deprecated* y Kafka 4.0 lo eliminó. El perfil
por defecto usa **KRaft** (consenso Raft dentro del broker). El perfil
Zookeeper existe para **ver** cómo funciona un servicio de coordinación
distribuida (Sec. 1.3), no porque sea lo recomendado hoy.

```bash
make up-zk
make zk-shell
# dentro del shell:
#   ls /brokers/ids      → brokers registrados
#   get /controller      → quién ganó la elección de líder
#   ls /brokers/topics   → metadata de tópicos
```

---

## Por qué no CockroachDB

Es la elección correcta para TuBoleta, no para este caso:

- Es **CP** con consenso Raft: los nodos **nunca divergen**, así que el
  trade-off que el reto pide demostrar sería inobservable.
- No tiene niveles `ONE`/`QUORUM` por consulta — su propuesta de valor es que
  no tengas que pensar en eso.
- El enunciado pide explícitamente un modelo **clave-valor**, no SQL relacional.
- Usa licencia BSL, que no es open source según la OSI.

Lo mencionamos en la exposición como contraste argumentativo: conocer ambas
familias y saber cuándo aplica cada una.

---

## Documentación

| Formato | Archivo | Generar |
|---|---|---|
| PDF | `docs/arquitectura.pdf` — documento de entrega, 5 páginas A4 | `make pdf` |
| PDF | `docs/presentacion.pdf` — 10 diapositivas 16:9 | `make slides` |
| HTML | `docs/web/` — cuatro documentos, ya versionados | — |
| MD | `docs/costos.md` — análisis de TCO | — |

```bash
make docs     # compila los dos PDFs
```

Los PDF requieren una instalación completa de TeX Live (ver `docs/README-latex.md`).
Los documentos HTML se abren con doble clic; ver
[docs/web/README.md](docs/web/README.md) para el detalle de cada uno.

---

## Taller 3 — Mini-Lakehouse

Extiende este mismo proyecto con una capa analítica: Cassandra (OLTP) →
Bronze → Silver → Gold (Parquet) → DuckDB, con un esquema dimensional en
constelación y benchmark contra la base operacional. Ver
[`taller-3-lakehouse/README.md`](taller-3-lakehouse/README.md) para el
detalle completo.

```bash
cd taller-3-lakehouse
pip install -r requirements.txt
python scripts/seed_alertas.py   # solo la primera vez
python main.py                   # corre todo el pipeline
```

Demo interactiva igual a `frontend/taller.html` del Módulo 2:
`make api` (arriba) y abrir `taller-3-lakehouse/lakehouse.html`.

---

## Taller 4 — Optimización de Rendimiento

Diagnostica con `EXPLAIN ANALYZE` las tres consultas más representativas del
Taller 3, aplica una sola intervención por problema encontrado (reordenar el
Parquet de Gold y reducir `ROW_GROUP_SIZE` para habilitar *data skipping*), y
mide el resultado real antes y después. Ver
[`taller-4-optimizacion/README.md`](taller-4-optimizacion/README.md) para el
detalle completo, incluyendo los planes de ejecución íntegros en
`taller-4-optimizacion/evidencia/`.

Para reproducir la demo sin depender de que el entorno siga corriendo en la
máquina de quien sustenta, cualquier persona con acceso al repositorio puede
levantarla por su cuenta:

```bash
git clone https://github.com/lina082/comunicamedellin-grupo3.git
cd comunicamedellin-grupo3
make up                                  # ~4 min la primera vez
make api                                 # terminal 1 → API en :8765
```

Luego abrir `taller-4-optimizacion/lakehouse.html` en el navegador. Ya trae
incorporados, al final de la franja de pasos del Taller 3, tres pasos nuevos
que ejecutan los scripts reales de esta carpeta contra `gold_taller3/`:

| Paso | Qué ejecuta de verdad |
|---|---|
| 06 · DIAG | `scripts/diagnostico.py`: `EXPLAIN ANALYZE` de Q1, Q3 y Q5 sobre la Gold original, con calentamiento y mediana de 5 corridas |
| 07 · FIX | `scripts/intervencion.py`: reescribe el Parquet ordenado por la columna de filtro de cada consulta, con `ROW_GROUP_SIZE` reducido |
| 08 · BENCH | `scripts/medicion_despues.py`: repite el mismo protocolo de medición contra la Gold ya optimizada |

Cada paso muestra el comando real y su salida en vivo por WebSocket; no son
capturas de pantalla ni una grabación. El router que expone estos tres
endpoints está en [`api/optimizacion.py`](api/optimizacion.py).

---

## Notas de operación

**El arranque tarda ~4 minutos.** Los nodos de Cassandra arrancan en
secuencia: cada uno espera a que el anterior esté completamente unido al
clúster. Cassandra no permite que dos nodos hagan *bootstrap* a la vez
(`cannot bootstrap while cassandra.consistent.rangemovement is true`), así que
el `healthcheck` comprueba `Mode: NORMAL` — es decir, que el nodo terminó de
unirse — y no solo que el proceso esté vivo.

**Verificar que el clúster está completo** antes de correr las demos:

```bash
docker exec cass1 nodetool status   # deben aparecer 3 líneas 'UN'
```

`UN` significa Up/Normal. Con menos de 3, esperar un poco más.

**Si algo queda inconsistente**, reiniciar de cero:

```bash
make clean && make up && make schema
```

---

## Estructura

```
.
├── docker-compose.yml              # Cassandra ×3 + Kafka KRaft + ClickHouse
├── docker-compose.zookeeper.yml    # perfil de aprendizaje
├── Makefile                        # interfaz de todos los comandos
├── infra/
│   ├── cql/schema.cql              # esquema Cassandra comentado
│   └── clickhouse/                 # esquema OLAP + consultas de demo
├── demo/scripts/                   # scripts del reto práctico
├── api/
│   ├── main.py                     # FastAPI + WebSocket
│   ├── taller.py                   # pipelines del Módulo 2 (6 puntos)
│   └── lakehouse.py                # pipeline del Taller 3 (6 puntos)
├── frontend/
│   ├── index.html                  # panel en vivo (sin build)
│   ├── taller.html                 # demo interactiva Módulo 2
│   └── demo-data.json              # corrida real grabada (modo demo)
├── taller-3-lakehouse/             # Taller 3: Mini-Lakehouse (Bronze/Silver/Gold)
│   └── README.md                   # ver ahí para el detalle completo
└── docs/
    ├── arquitectura.tex            # documento de entrega
    ├── presentacion.tex            # diapositivas Beamer
    └── web/                        # los mismos documentos en HTML
```
