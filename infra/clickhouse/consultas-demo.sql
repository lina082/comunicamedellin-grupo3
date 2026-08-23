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
