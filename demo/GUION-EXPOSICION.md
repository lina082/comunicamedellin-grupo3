# Guion de la demo — Módulo 2

**22 de septiembre · 15 min de exposición + 5 de preguntas**

Diapositivas: `docs/presentacion-modulo2.pdf` (`make slides-m2`).
Cada slide lleva arriba a la derecha el nombre de quien la expone.

Requisitos del reto práctico y dónde se cumple cada uno:

| Requisito del taller | Comando | Evidencia que se ve en pantalla |
|---|---|---|
| Caída real de un nodo **mientras se escribe** | `make demo-reparacion` | `nodetool status` pasa a `DN`; el clúster sigue aceptando escrituras |
| Recuperación/reparación con conteo antes/después | `make demo-reparacion` | 1.999 / **0** / 1.999 → 1.999 / **1.999** / 1.999 |
| Idempotencia: reenviar el mismo evento | `make demo-idempotencia` | 1 fila vs 3 filas según de dónde venga el timestamp |
| Fan-out real a 10.000 con throughput **y latencia** | `make demo-fanout` | entregas/s por zona + p50/p95/p99 por escritura |
| Consulta analítica en la base correcta vs forzada en la operacional | `make demo-analitica` | Cassandra rechaza el `GROUP BY`; ClickHouse responde |

---

## Antes de empezar (10 min antes)

```bash
make up          # ~4 min: 3 nodos Cassandra + Kafka + ClickHouse
make schema      # 7 tablas
make seed        # 10.000 usuarios
make ch-seed     # eventos analíticos en ClickHouse
make status      # confirmar 3 nodos UN antes de exponer
```

> Si un nodo no arranca tras reiniciar Docker, suele ser colisión de IP
> (`A node with address ... already exists`). Se resuelve con
> `make clean && make up`, que reinicia los volúmenes.

---

## Orden de la exposición (15 min)

Las diapositivas siguen **el orden de las preguntas guía del enunciado**, no un orden
temático. Cada bloque abre con una portadilla que muestra la pregunta literal, y cada
diapositiva lleva la guía bajo el título.

| Min | Pregunta guía | Quién | Slides |
|---|---|---|---|
| 0–1 | *(ruta y contexto)* | Julián | 2–3 |
| 1–4 | **1 ·** Tablas: Partition Key + Clustering Key + columnas | Julián | 4–6 |
| 4–7 | **3 ·** Fan-out-on-Write: 1 alerta → 100.000 registros | Simón | 7–9 |
| 7–10 | **4 ·** Demasiados usuarios en la misma partición | Elizabeth | 10–12 |
| 10–13 | **5 ·** Replicación, hinted handoff, reparación | Nilton | 13–15 |
| 13–15 | **2 ·** Las cinco consultas reales | **demo en vivo** | 16–20 |

**La pregunta 2 no se expone en diapositivas: se responde ejecutando las consultas**
contra el clúster, desde `localhost:4173/taller.html`.

Durante la demo conviene correr, en este orden: P1 (las cinco consultas), y después
los que refuercen lo ya expuesto — P2 fan-out, P5 idempotencia, P6 analítica.
`demo-reparacion` (P4) tarda ~3 min: lánzalo en otra terminal al empezar el bloque de Nilton.

## Preguntas probables y dónde está la respuesta

**¿Por qué no CockroachDB / MongoDB?**
CockroachDB es CP con consenso Raft: correcto para TuBoleta, donde dos personas
compiten por la misma silla. Aquí nadie compite por un mensaje — pagaríamos
consenso en cada *heartbeat* sin ganar nada. MongoDB también es CP. *(Informe M1,
alternativas descartadas.)*

**¿Por qué RF=3 y no 2?**
Con RF=2 el quórum sigue siendo 2, es decir todas las réplicas: perder un nodo
bloquearía las escrituras. RF=3 es el mínimo que tolera una caída. *(§2.)*

**¿El fan-out no satura Cassandra?**
Medimos 6.322 escrituras/s en este clúster de demo; una alerta tarda ~15,8 s. Por
eso el fan-out es asíncrono: la Alcaldía recibe confirmación cuando la alerta
entra en Kafka, no cuando terminan las 100.000 escrituras. *(§4.1.)*

**¿Y si hay que cancelar una alerta ya enviada?**
No se borran 100.000 filas: un `DELETE` escribe un *tombstone* que persiste
`gc_grace_seconds` y que toda lectura debe descartar. Se marca `cancelada=true`
en `alertas` (una escritura) y el cliente filtra al leer. *(§4.3.)*

**¿Por qué el hueco de datos no se ve con un SELECT?**
Con RF=3 sobre 3 nodos los tres son réplica de todo (`nodetool getendpoints`), así
que una lectura con `ONE` la sirve cualquier réplica viva. La divergencia es
interna, visible solo con `nodetool tablestats`. Eso *es* la consistencia
eventual: el cliente no lee datos incorrectos, las réplicas divergen mientras el
servicio responde bien. *(§6.)*

**¿Cuánto ocupa esto en un año?**
45,93 GB con RF=3 a 20 alertas/día, unos 15 GB por nodo. El fan-out no es un
problema de almacenamiento sino de distribución de escrituras. *(§4.1.)*
