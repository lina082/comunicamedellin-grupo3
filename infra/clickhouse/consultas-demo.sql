-- ============================================================
-- CONSULTAS PARA LA EXPOSICION (bloque analitico, ~2 min)
-- Ejecutar con:  make ch-demo
-- ============================================================

-- ------------------------------------------------------------
-- Q1. Usuarios en linea por zona, ultima hora
--     Cassandra necesitaria ALLOW FILTERING + scan del cluster.
-- ------------------------------------------------------------
SELECT
    zona,
    uniqExact(usuario_id) AS usuarios_unicos,
    countIf(estado = 'online') AS eventos_online
FROM comunica.presencia_eventos
WHERE ts > now() - INTERVAL 1 HOUR
GROUP BY zona
ORDER BY usuarios_unicos DESC
FORMAT PrettyCompact;

-- ------------------------------------------------------------
-- Q2. Efectividad del fan-out: % entregado en los primeros 30 s
--     Responde: "el sistema entrego a 100K casi al mismo tiempo?"
-- ------------------------------------------------------------
SELECT
    alerta_id,
    count()                                    AS total_entregas,
    round(quantile(0.50)(latencia_ms))         AS p50_ms,
    round(quantile(0.95)(latencia_ms))         AS p95_ms,
    round(quantile(0.99)(latencia_ms))         AS p99_ms,
    round(100 * countIf(latencia_ms <= 30000) / count(), 2) AS pct_bajo_30s
FROM comunica.entregas
GROUP BY alerta_id
ORDER BY total_entregas DESC
LIMIT 5
FORMAT PrettyCompact;

-- ------------------------------------------------------------
-- Q3. Curva horaria de presencia (serie de tiempo)
-- ------------------------------------------------------------
SELECT
    toStartOfHour(ts) AS hora,
    zona,
    uniqExact(usuario_id) AS activos
FROM comunica.presencia_eventos
WHERE ts > now() - INTERVAL 24 HOUR
GROUP BY hora, zona
ORDER BY hora DESC, zona
LIMIT 20
FORMAT PrettyCompact;

-- ------------------------------------------------------------
-- Q3b. Curva de actividad por hora (la mas visual de todas)
--      Muestra el pico de la tarde y el valle de la madrugada.
-- ------------------------------------------------------------
SELECT
    toHour(ts) AS hora,
    uniqExact(usuario_id) AS usuarios_activos,
    bar(usuarios_activos, 0, 25000, 50) AS curva
FROM comunica.presencia_eventos
GROUP BY hora
ORDER BY hora
FORMAT PrettyCompact;

-- ------------------------------------------------------------
-- Q4. Compresion columnar real (dato duro para la exposicion)
-- ------------------------------------------------------------
SELECT
    table,
    formatReadableSize(sum(data_uncompressed_bytes)) AS sin_comprimir,
    formatReadableSize(sum(data_compressed_bytes))   AS comprimido,
    round(sum(data_uncompressed_bytes) / sum(data_compressed_bytes), 1) AS ratio
FROM system.parts
WHERE database = 'comunica' AND active
GROUP BY table
ORDER BY table
FORMAT PrettyCompact;

-- ============================================================
-- CONSULTAS ADICIONALES
-- Ejecutar en http://localhost:8123/play (usuario demo / demo)
-- o con:  make ch-extra
-- ============================================================

-- ------------------------------------------------------------
-- Q5. Percentiles de latencia por zona
--     quantile() es una funcion de agregacion nativa: en
--     PostgreSQL habria que usar percentile_cont y ordenar.
-- ------------------------------------------------------------
SELECT
    zona,
    round(quantile(0.50)(latencia_ms)) AS p50,
    round(quantile(0.95)(latencia_ms)) AS p95,
    round(quantile(0.99)(latencia_ms)) AS p99,
    max(latencia_ms) AS peor_caso
FROM comunica.presencia_eventos
GROUP BY zona
ORDER BY p95 DESC
FORMAT PrettyCompact;

-- ------------------------------------------------------------
-- Q6. Reparto por dispositivo, con barra y porcentaje
--     La window function sum() OVER () calcula el total sin
--     necesidad de una subconsulta.
-- ------------------------------------------------------------
SELECT
    dispositivo,
    count() AS eventos,
    round(100 * count() / sum(count()) OVER (), 1) AS pct,
    bar(count(), 0, 750000, 30) AS distribucion
FROM comunica.presencia_eventos
GROUP BY dispositivo
ORDER BY eventos DESC
FORMAT PrettyCompact;

-- ------------------------------------------------------------
-- Q7. Conteo exacto frente a HyperLogLog
--     uniqExact mantiene un hash set en memoria; uniq usa
--     HyperLogLog, que consume memoria constante a cambio de
--     un error tipico del 0,5 %. Con miles de millones de
--     filas, esa diferencia es lo que hace viable la consulta.
-- ------------------------------------------------------------
SELECT
    uniqExact(usuario_id) AS exacto,
    uniq(usuario_id)      AS aproximado,
    round(100 * abs(uniq(usuario_id) - uniqExact(usuario_id))
          / uniqExact(usuario_id), 3) AS error_pct
FROM comunica.presencia_eventos
FORMAT PrettyCompact;

-- ------------------------------------------------------------
-- Q8. Tasa de conexion por hora
--     countIf agrega un filtro dentro de la agregacion, sin
--     necesidad de CASE WHEN.
-- ------------------------------------------------------------
SELECT
    toHour(ts) AS hora,
    count() AS eventos,
    countIf(estado = 'online') AS conectados,
    round(100 * countIf(estado = 'online') / count(), 1) AS pct_online
FROM comunica.presencia_eventos
GROUP BY hora
ORDER BY hora
FORMAT PrettyCompact;

-- ------------------------------------------------------------
-- Q9. Compresion columna por columna  ← la mas reveladora
--     Muestra POR QUE un motor columnar comprime tanto: al
--     guardar juntos los valores de una misma columna, la
--     entropia baja. 'zona' solo tiene 3 valores distintos en
--     un millon de filas y comprime unas 200 veces.
-- ------------------------------------------------------------
SELECT
    column AS columna,
    formatReadableSize(sum(column_data_uncompressed_bytes)) AS sin_comprimir,
    formatReadableSize(sum(column_data_compressed_bytes))   AS comprimido,
    round(sum(column_data_uncompressed_bytes)
          / sum(column_data_compressed_bytes), 1) AS ratio
FROM system.parts_columns
WHERE database = 'comunica' AND table = 'presencia_eventos' AND active
GROUP BY column
ORDER BY ratio DESC
FORMAT PrettyCompact;

-- ------------------------------------------------------------
-- Q10. Cuanto trabajo se ahorro la consulta
--      ClickHouse registra cada consulta en system.query_log.
--      Util para demostrar que el motor columnar leyo solo las
--      columnas necesarias, no la tabla entera.
-- ------------------------------------------------------------
SELECT
    substring(query, 1, 46) AS consulta,
    formatReadableQuantity(read_rows) AS filas_leidas,
    formatReadableSize(read_bytes)    AS bytes_leidos,
    round(query_duration_ms) AS ms
FROM system.query_log
WHERE type = 'QueryFinish'
  AND query LIKE '%presencia_eventos%'
  AND event_time > now() - INTERVAL 1 HOUR
ORDER BY event_time DESC
LIMIT 8
FORMAT PrettyCompact;
