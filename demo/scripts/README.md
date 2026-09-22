# Scripts del reto práctico

| Script | Qué demuestra | Requisito |
|---|---|---|
| `seed_usuarios.py` | 10.000 usuarios actualizando presencia concurrentemente | Exigido |
| `demo_consistencia.py` | `ONE` vs `QUORUM` leyendo desde dos nodos | **Exigido — núcleo** |
| `demo_fanout.py` | Fan-out particionado por zona vía Kafka | Exigido |
| `demo_nodo_caido.py` | Disponibilidad con un nodo caído | Extra |
| `seed_clickhouse.py` | 1M de eventos en el motor columnar | Extra |
| `_common.py` | Utilidades compartidas (conexión, colores) | — |

## Antes de ejecutar

```bash
pip install -r demo/requirements.txt
make up && make schema
```

## Nota sobre la conexión

`_common.conectar()` usa `WhiteListRoundRobinPolicy` para forzar la conexión a
**un nodo concreto**, sin balanceo de carga. Sin eso, el driver elegiría el nodo
por su cuenta y la comparación entre nodo 1 y nodo 3 no sería válida.

## Demo manual en CQL

Los scripts de esta carpeta automatizan las demos. Para **mostrarlas a mano
dentro de `cqlsh`** (particionado por token, `LOCAL_QUORUM` vs `QUORUM` con
`TRACING`, caída de nodo), ver la sección *«Demo dentro de Cassandra (CQL en
vivo)»* del [README principal](../../README.md).

## Módulo 2 — modelado de datos

| Script | Qué demuestra |
|---|---|
| `demo_idempotencia.py` | Reenviar el mismo evento de Kafka no duplica filas si `enviado_en` viene del evento y no de `now()` en el worker. Incluye el guardia LWT para efectos no idempotentes (el push). |
| `demo_reparacion.py` | Nodo caído prolongado: hinted handoff, read repair y `nodetool repair`. Mide el hueco de datos por nodo antes y después de reparar. |
| `demo_analitica.py` | La misma agregación en ClickHouse y forzada en Cassandra. Muestra el rechazo del `GROUP BY`, el costo de `ALLOW FILTERING` y normaliza por fila procesada. |

### Nota metodológica sobre `demo_reparacion.py`

El hueco **no se puede medir con un `SELECT`**. Con RF=3 sobre 3 nodos los tres
son réplica de todo (`nodetool getendpoints` lo confirma), así que una lectura
con `CONSISTENCY ONE` dirigida al nodo con el hueco se sirve desde cualquier
réplica viva y devuelve el valor correcto.

Tampoco sirve `Local write count`: es un contador acumulado desde el arranque
del proceso, se reinicia con el nodo, y `repair` no lo incrementa porque escribe
SSTables directamente sin pasar por el camino normal de escritura.

La métrica correcta es **`Number of partitions` de `nodetool tablestats`**, que
refleja lo que cada nodo tiene físicamente en disco.

El script desactiva `hinted handoff` (y purga los hints residuales) antes de
tumbar el nodo: sin eso el coordinador guarda las escrituras y se las entrega al
volver, cerrando el hueco antes de poder medirlo. El `finally` lo restaura.
