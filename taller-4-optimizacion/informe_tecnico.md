---
title: "Taller 4 — Laboratorio de Optimización de Consultas"
subtitle: "Diagnóstico, intervención y medición sobre el pipeline ComunicaMedellín (Taller 3)"
author: "Equipo 3 — Simón Ramírez, Elizabeth Méndez, Nilton Orejuela, Julián Vélez, Lina Sevillano — Tópicos Avanzados en Bases de Datos, ITM 2026"
---

# 1. Resumen ejecutivo

Este informe optimiza tres consultas analíticas del mini-lakehouse de
ComunicaMedellín construido en el Taller 3 (DuckDB sobre Gold en Parquet,
~106.000 filas de entregas y 10.000 snapshots de presencia). Se leyeron
los planes de ejecución reales con `EXPLAIN ANALYZE`, se identificó una
causa raíz única y común a las tres consultas, se aplicó una sola
intervención dirigida a esa causa, y se midió el efecto con la misma
metodología (warm-up de 3 corridas + mediana de 5).

El diagnóstico reveló que los dos archivos de hechos (`fact_entregas.parquet`
y `fact_presencia_hora.parquet`) se habían escrito en el Taller 3 con
**un solo row group** cada uno. Con un único row group, DuckDB no tiene
ningún bloque con estadísticas min/max que pueda descartar: así el
`WHERE` sea muy selectivo, el motor debe leer y decodificar el row group
completo siempre. Ese es el Indicador 4 del vocabulario del taller —
data skipping ausente — y afecta por igual a una consulta filtrada por
fecha, otra por estado de entrega y otra por zona.

La intervención fue reescribir ambos archivos ordenados por la columna
de filtro de cada consulta, con `ROW_GROUP_SIZE` reducido a 5.000, lo
que generó 18 row groups en `fact_entregas` y 2 en `fact_presencia_hora`.
Las mejoras medidas fueron de 1.05x a 1.58x: reales pero modestas, porque
el dataset (1.3 MB) es pequeño para que la ventaja de saltar bloques de
I/O se note — la ganancia observada viene del menor trabajo de CPU al
decodificar menos filas, no de I/O evitado. La Sección 4.1 analiza esto
con honestidad: no todos los resultados "pequeños" son un fracaso del
diagnóstico, y aquí se explica por qué.

---

# 2. Diagnóstico

Las 3 queries diagnosticadas parten de las Q1, Q3 y Q5 originales del
`scripts/analyze.py` del Taller 3, a las que se les añadió un `WHERE`
realista: las originales eran agregaciones sobre **todo** el histórico
sin ningún filtro, por lo que ya constituían un full scan "correcto" —
no había ningún acceso evitable que diagnosticar. Un dashboard real
nunca pide "todo el histórico sin filtrar"; pide "la última semana" o
"las entregas fallidas de hoy". Esa es la versión que se analiza aquí.

## Paso 0 — Row groups por archivo Gold

```
fact_entregas.parquet:        1 row group  para 106,370 filas
fact_presencia_hora.parquet:  1 row group  para  10,000 filas
```

Este hallazgo se hizo **antes** de leer los planes individuales y resultó
ser la causa raíz de los tres diagnósticos: sin importar qué tan
selectivo sea el predicado de cada consulta, con un solo row group no
hay min/max por bloque que el planner pueda usar para saltar lectura.

## Q1 — Volumen diario de entregas por severidad (última semana)

```sql
SELECT t.fecha, a.severidad, COUNT(*) AS total_entregas,
       SUM(f.entregado_flag) AS entregadas, SUM(f.fallido_flag) AS fallidas,
       ROUND(SUM(f.fallido_flag) * 100.0 / COUNT(*), 2) AS tasa_fallo_pct
FROM fact_entregas f
JOIN dim_tiempo t ON f.tiempo_sk = t.tiempo_sk
JOIN dim_alerta a ON f.alerta_sk = a.alerta_sk
WHERE t.fecha >= '2026-10-12'
GROUP BY t.fecha, a.severidad
ORDER BY t.fecha
```

| Campo | Valor |
|---|---|
| Tiempo baseline (mediana) | 3.78 ms |
| Motor usado | DuckDB 1.5.6 sobre Parquet (Gold del Taller 3) |
| Operador más costoso | `TABLE_SCAN` (`READ_PARQUET`) sobre `fact_entregas.parquet` — proyecta 106.370 filas del único row group antes de aplicar el filtro de fecha |
| Problema identificado | Data skipping ausente: el filtro `t.fecha >= '2026-10-12'` selecciona ~27% de las filas (29.412 de 106.370), pero al haber un solo row group, DuckDB decodifica el bloque completo igual; el `HASH_JOIN` con `dim_tiempo` aplica un *dynamic filter* (`tiempo_sk>=1 AND tiempo_sk<=31`) que no reduce nada porque el rango cubre casi todo el dominio de `dim_tiempo` |
| Indicador que lo revela | Indicador 4 — data skipping ausente en Parquet (confirmado con `parquet_metadata()`: 1 row group) |

## Q3 — Entregas fallidas por severidad y zona de destino

```sql
SELECT a.severidad, a.zona_destino, COUNT(*) AS fallidas,
       ROUND(AVG(f.latencia_entrega_seg), 2) AS latencia_prom_seg
FROM fact_entregas f
JOIN dim_alerta a ON f.alerta_sk = a.alerta_sk
WHERE f.estado = 'fallido'
GROUP BY a.severidad, a.zona_destino
ORDER BY fallidas DESC
```

| Campo | Valor |
|---|---|
| Tiempo baseline (mediana) | 3.14 ms |
| Motor usado | DuckDB 1.5.6 sobre Parquet |
| Operador más costoso | `TABLE_SCAN` sobre `fact_entregas.parquet` proyectando `alerta_sk`, `estado`, `latencia_entrega_seg` |
| Problema identificado | El filtro `estado = 'fallido'` es muy selectivo (5.405 de 106.370 filas, ~5%), exactamente el escenario donde el data skipping más aporta — pero con un solo row group no hay forma de descartar nada antes de decodificar y filtrar fila por fila |
| Indicador que lo revela | Indicador 4 — data skipping ausente (mismo archivo, mismo problema de raíz que Q1, columna de filtro distinta) |

## Q5 — Presencia online por hora en la zona Centro

```sql
SELECT z.zona, p.hora_del_dia, COUNT(*) AS snapshots,
       SUM(p.online_flag) AS online,
       ROUND(SUM(p.online_flag) * 100.0 / COUNT(*), 1) AS pct_online
FROM fact_presencia_hora p
JOIN dim_zona z ON p.zona_sk = z.zona_sk
WHERE z.zona = 'Centro'
GROUP BY z.zona, p.hora_del_dia
ORDER BY p.hora_del_dia
```

| Campo | Valor |
|---|---|
| Tiempo baseline (mediana) | 0.91 ms |
| Motor usado | DuckDB 1.5.6 sobre Parquet |
| Operador más costoso | `TABLE_SCAN` sobre `fact_presencia_hora.parquet` (10.000 filas, único row group) |
| Problema identificado | Mismo problema estructural de data skipping ausente, pero aquí la dimensión filtrada (`zona`, 3 valores posibles) tiene baja cardinalidad: aunque se repare el row group, como mucho se puede descartar 2 de 3 bloques — el techo de mejora es mucho menor que en Q1/Q3 |
| Indicador que lo revela | Indicador 4 — data skipping ausente, con la salvedad de baja cardinalidad de la columna de filtro |

---

# 3. Intervenciones

Se aplicó **una sola intervención por problema**, siguiendo I-3 del
catálogo del taller ("ordenar el Parquet por la columna de filtro"),
adaptada así: cada archivo de hechos se reescribió ordenado por la
columna que su consulta filtra, con `ROW_GROUP_SIZE = 5000` para forzar
la creación de varios row groups con estadísticas min/max útiles
(`scripts/intervencion.py`). Como Q1 y Q3 filtran columnas distintas
sobre el mismo `fact_entregas` (fecha vs. estado), y un único orden
físico solo puede favorecer una columna a la vez, se generaron dos
copias: `fact_entregas.parquet` (ordenado por `tiempo_sk`, para Q1) y
`fact_entregas_por_estado.parquet` (ordenado por `estado`, para Q3).
Esto se documenta explícitamente en la Sección 5 (Reflexión
arquitectural) porque es una decisión de diseño con una limitación real.

## Intervención sobre Q1

| Campo | Valor |
|---|---|
| Query afectada | Q1 — Volumen diario de entregas por severidad |
| Problema identificado en el diagnóstico | Data skipping ausente: 1 row group en `fact_entregas.parquet` |
| Intervención aplicada | `COPY (SELECT * FROM fact_entregas ORDER BY tiempo_sk) TO fact_entregas.parquet (FORMAT PARQUET, ROW_GROUP_SIZE 5000)` → 18 row groups |
| Plan ANTES | `TABLE_SCAN` sobre `fact_entregas.parquet`, 1 row group, 106.370 filas decodificadas antes del filtro de `dynamic filter` del join |
| Plan DESPUÉS | `TABLE_SCAN` sobre el mismo archivo ya ordenado y fragmentado en 18 row groups; el operador sigue mostrando 29.412 filas de salida (cardinalidad post-filtro, DuckDB no imprime en el plan de texto cuántos row groups se podaron), pero el tiempo total bajó de forma consistente en las 5 corridas |
| Tiempo antes (mediana) | 3.78 ms |
| Tiempo después (mediana) | 2.60 ms |
| Factor de mejora | 1.45x |
| Justificación técnica | Al ordenar por `tiempo_sk` y fragmentar en bloques de 5.000 filas, cada row group queda con un rango estrecho de `tiempo_sk` (min/max). El planner de DuckDB puede evaluar el predicado `t.fecha >= '2026-10-12'` (traducido vía join a un rango de `tiempo_sk`) contra esas estadísticas y saltar los row groups fuera de rango sin decodificarlos. Con un solo row group previo, esa poda era imposible. La mejora es real pero no dramática porque el archivo (1.3 MB) es pequeño: la ganancia viene de menos CPU de decodificación, no de I/O de disco evitado (ver Sección 4.1) |

## Intervención sobre Q3

| Campo | Valor |
|---|---|
| Query afectada | Q3 — Entregas fallidas por severidad y zona de destino |
| Problema identificado en el diagnóstico | Mismo problema de raíz (1 row group), pero el filtro es sobre `estado`, no sobre `tiempo_sk`: el orden físico que ayuda a Q1 no ayuda a Q3 |
| Intervención aplicada | Segunda copia ordenada por `estado`: `fact_entregas_por_estado.parquet`, también con `ROW_GROUP_SIZE 5000` → 18 row groups |
| Plan ANTES | `TABLE_SCAN` sobre `fact_entregas.parquet` (1 row group), decodifica las 106.370 filas para evaluar `estado = 'fallido'` |
| Plan DESPUÉS | `TABLE_SCAN` sobre `fact_entregas_por_estado.parquet` (18 row groups); al estar ordenado por `estado`, las filas con `estado = 'fallido'` quedan contiguas en un subconjunto pequeño de row groups, y el resto se descarta por estadística min/max |
| Tiempo antes (mediana) | 3.14 ms |
| Tiempo después (mediana) | 1.99 ms |
| Factor de mejora | 1.58x |
| Justificación técnica | Esta es la intervención con mayor factor de mejora de las tres, consistente con que `estado = 'fallido'` es el filtro más selectivo (~5% de las filas): cuanto más selectivo el predicado y mejor alineado con el orden físico, más row groups completos se pueden descartar. Confirma el Indicador 4 del taller: la ganancia de data skipping escala con la selectividad del filtro, no solo con su existencia |

## Intervención sobre Q5

| Campo | Valor |
|---|---|
| Query afectada | Q5 — Presencia online por hora en zona Centro |
| Problema identificado en el diagnóstico | Mismo problema de raíz (1 row group en `fact_presencia_hora.parquet`), agravado por baja cardinalidad de la columna de filtro (`zona`, solo 3 valores) |
| Intervención aplicada | `COPY (... ORDER BY zona_sk) TO fact_presencia_hora.parquet (ROW_GROUP_SIZE 5000)` → 2 row groups (10.000 filas / 5.000 = 2 bloques) |
| Plan ANTES | `TABLE_SCAN` sobre `fact_presencia_hora.parquet`, 1 row group, 10.000 filas decodificadas |
| Plan DESPUÉS | `TABLE_SCAN` sobre el archivo ordenado por `zona_sk`, 2 row groups; como las 3 zonas no se reparten de forma pareja entre exactamente 2 bloques, el filtro por `zona = 'Centro'` como mucho descarta 1 de los 2 row groups |
| Tiempo antes (mediana) | 0.91 ms |
| Tiempo después (mediana) | 0.87 ms |
| Factor de mejora | 1.05x |
| Justificación técnica | La mejora es marginal y es el resultado esperado, no un error: con solo 3 valores posibles de zona y 10.000 filas, el espacio de bloques que se puede podar es mínimo (como mucho 1 de 2 row groups), y la tabla ya es pequeña (10.000 filas caben en memoria sin esfuerzo). El dataset es, en palabras de la guía del taller, "demasiado pequeño para que el overhead del reordenamiento valga la pena" en este caso particular — un resultado de 1.05x con esta explicación es más honesto que forzar una narrativa de mejora dramática que los datos no sostienen |

---

# 4. Tabla de benchmarks y reflexión

## 4.1 Tabla de benchmarks consolidada

| Query | Problema (vocabulario M4) | Intervención | t antes (ms) | t después (ms) | Factor | ¿Cambió el plan? |
|---|---|---|---|---|---|---|
| Q1 — Volumen diario por severidad | Data skipping ausente: `fact_entregas` en 1 row group, filtro `fecha >= '2026-10-12'` no puede podar bloques | I-3: ordenar Parquet por `tiempo_sk`, `ROW_GROUP_SIZE=5000` → 18 row groups | 3.78 | 2.60 | 1.45x | Sí — mismo operador (`TABLE_SCAN` + `HASH_JOIN`), pero ahora puede descartar row groups fuera de rango antes de decodificar |
| Q3 — Entregas fallidas por severidad/zona | Data skipping ausente: filtro `estado = 'fallido'` (~5% de filas) no puede podar bloques con 1 row group | I-3: ordenar Parquet por `estado`, `ROW_GROUP_SIZE=5000` → 18 row groups | 3.14 | 1.99 | 1.58x | Sí — misma estructura de plan, menos filas decodificadas por estar el predicado alineado con el orden físico |
| Q5 — Presencia online por zona/hora | Data skipping ausente, agravado por baja cardinalidad de `zona` (3 valores) | I-3: ordenar Parquet por `zona_sk`, `ROW_GROUP_SIZE=5000` → 2 row groups | 0.91 | 0.87 | 1.05x | No cambia de forma apreciable — techo de poda limitado a 1 de 2 row groups |

### Sobre resultados modestos

Ninguna de las tres mejoras es espectacular, y eso es consistente con lo
que advierte la guía del taller: *"Un sistema que mejora un 5% con una
justificación técnica correcta vale más que uno que mejora un 90% sin
entender por qué."* La explicación de por qué las mejoras son modestas
y no dramáticas es, en sí misma, parte del hallazgo:

1. **El dataset es pequeño** (1.3 MB para `fact_entregas`, con 106.370
   filas). Un archivo de ese tamaño cabe cómodamente en la caché de
   página del sistema operativo después del warm-up, así que el costo
   dominante no es I/O de disco sino **decodificación de columnas en
   CPU**. El `total_bytes_read` reportado por el profiler de DuckDB fue
   prácticamente idéntico antes y después (~1.7 KB en ambos casos para
   un scan aislado), confirmando que la ganancia viene de decodificar
   menos filas, no de leer menos bytes de disco.
2. **La selectividad del filtro importa más que la sola presencia de
   row groups múltiples.** Q3 (filtro ~5% de las filas) mejoró más que
   Q1 (filtro ~27%), que a su vez mejoró más que Q5 (filtro sobre una
   columna de 3 valores posibles). El techo de mejora de data skipping
   está acotado por cuántos row groups completos se pueden descartar,
   y eso depende de qué tan bien se alinee el orden físico con la
   selectividad real del predicado.
3. **Un único orden físico no sirve a dos predicados distintos a la
   vez.** Ordenar `fact_entregas` por `tiempo_sk` ayuda a Q1 pero no a
   Q3, y viceversa — de ahí que se hayan necesitado dos copias del
   mismo archivo. Esta tensión es exactamente el tipo de decisión de
   diseño que se explora en la Sección 4.2.

## 4.2 Reflexión arquitectural

**¿El ordenamiento del Gold por la columna de filtro debería ser parte
del pipeline `transform_gold.py` desde el principio, o es un paso de
optimización posterior?**

Debería decidirse desde el principio, pero no de forma única: el
Taller 3 generó `fact_entregas.parquet` con `df.to_parquet(..., index=False)`
sin especificar orden ni `row_group_size`, dejando el default de pandas/
pyarrow (que para ~106.000 filas resultó en un solo row group). Ese no
es un error del Taller 3 — en esa etapa el objetivo era construir el
modelo dimensional correcto, no optimizarlo — pero sí es una decisión
de diseño que tiene costo real y que el Taller 4 hace visible. La
lección correcta no es "ordenar siempre por una columna fija", sino que
**el pipeline Gold debería declarar explícitamente el patrón de acceso
esperado** (qué columnas se filtran con más frecuencia) y fijar
`ROW_GROUP_SIZE` de forma consciente, en vez de heredar un default
pensado para datasets mucho más grandes.

**¿El modelo dimensional del Taller 3 facilita o dificulta el uso de
índices/orden físico? ¿Hay alguna clave foránea que debería tener orden
y no lo tiene?**

El esquema en constelación (`fact_entregas` + `fact_presencia_hora`
compartiendo `dim_tiempo`, `dim_usuario`, `dim_zona`) facilita el join
pushdown — DuckDB ya genera *dynamic filters* automáticos en el
`HASH_JOIN` contra las dimensiones (visibles en los planes como
`optional: tiempo_sk>=1 AND tiempo_sk<=31`) — pero **no resuelve el
problema de fondo**, porque esos filtros dinámicos solo ayudan si hay
row groups que podar, y el archivo base no los tenía. La clave que más
lo necesitaba y no lo tenía era `tiempo_sk` en `fact_entregas`: es la
dimensión que casi cualquier consulta analítica de series temporales va
a filtrar primero, y sin embargo el pipeline no la usaba como criterio
de orden físico al escribir el Parquet.

**¿Las consultas analíticas que escriben siguen el orden de la clave de
particionamiento? Si no, ¿qué implicación tiene eso para el data
skipping?**

No completamente, y eso es justo lo que este taller expuso: Q1 filtra
por tiempo, Q3 filtra por estado, Q5 filtra por zona. Un solo orden
físico no puede optimizar las tres a la vez (de ahí la necesidad de una
segunda copia ordenada por `estado` para Q3). En producción, esto
apunta a dos caminos razonables: (a) particionar por `año/mes` con
Hive-style (I-6 del catálogo) para las consultas de rango temporal —
que son probablemente las más frecuentes en un dashboard operacional —
y aceptar que las consultas por `estado` sigan sin skipping óptimo, o
(b) mantener una vista materializada adicional pre-agregada por estado
si esa consulta resulta ser crítica para el negocio (ej. un panel de
alertas fallidas en tiempo real).

**¿Hay alguna consulta inherentemente lenta por acceder a datos de baja
cardinalidad, y ningún índice puede ayudar? ¿Cómo se resolvería en
producción?**

Sí: Q5, filtrada por `zona` (solo 3 valores). Ningún reordenamiento de
Parquet ni índice puede podar más de 2 de los 3 bloques posibles,
porque la cardinalidad del predicado es el techo real de cualquier
técnica de data skipping. En producción, esto se resolvería con una
**vista materializada o pre-agregación** (`fact_presencia_zona_hora`
agregada de antemano por `zona` y `hora_del_dia`, con ~72 filas en vez
de 10.000) en lugar de intentar optimizar el acceso a la tabla de grano
fino para esta consulta específica — I-6 y reordenamiento son técnicas
de *acceso*, no sustituyen una decisión de *agregación* cuando la
cardinalidad de la dimensión filtrada es intrínsecamente baja.

---

*Informe generado para el Taller 4 — Tópicos Avanzados en Bases de Datos,
ITM 2026. Evidencia completa (planes `EXPLAIN ANALYZE` antes/después,
scripts de diagnóstico e intervención) en el repositorio, carpeta
`ENTREGA4/`.*
