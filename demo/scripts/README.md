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
