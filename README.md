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
make frontend   # terminal 2 → http://localhost:3000
```

Muestra la lectura del mismo usuario desde los nodos 1 y 3 en paralelo, con
conmutador `ONE`/`QUORUM` en caliente. Con `ONE` y el botón «Martillo»
aparecen divergencias; con `QUORUM` desaparecen y sube la latencia.

### Extras (más allá de lo pedido)

```bash
make demo-nodo-caido   # tumba un nodo: AP sigue respondiendo
make ch-seed           # 1M de eventos en ClickHouse
make ch-demo           # agregaciones columnares + ratio de compresión
make kafka-replay      # relectura desde offset 0 (Kappa, Sec. 4.2)
make up-zk && make zk-shell   # coordinación con Zookeeper
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

```bash
make docs     # compila los dos PDFs
```

Los PDF requieren una instalación completa de TeX Live (ver `docs/README-latex.md`).
Los documentos HTML se abren con doble clic; ver
[docs/web/README.md](docs/web/README.md) para el detalle de cada uno.

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
├── api/main.py                     # FastAPI + WebSocket
├── frontend/
│   ├── index.html                  # panel en vivo (sin build)
│   └── demo-data.json              # corrida real grabada (modo demo)
└── docs/
    ├── arquitectura.tex            # documento de entrega
    ├── presentacion.tex            # diapositivas Beamer
    └── web/                        # los mismos documentos en HTML
```
