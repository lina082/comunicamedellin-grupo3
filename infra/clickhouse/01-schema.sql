-- ============================================================
-- ComunicaMedellin - Esquema ClickHouse (ruta analitica)
-- Sec. 2.2 (OLAP columnar) | Sec. 2.4 (caso Mercado Libre)
-- ============================================================
--
-- Por que ClickHouse y no Cassandra para esto:
--   Cassandra (AP, wide-column) esta optimizada para lectura por
--   clave de particion. Un GROUP BY sobre 100K usuarios la obliga
--   a un scan completo del cluster (ALLOW FILTERING).
--   ClickHouse es columnar: solo lee las columnas involucradas.
--   Es el conflicto OLTP vs OLAP de la Sec. 2.2, hecho demo.
-- ============================================================

CREATE DATABASE IF NOT EXISTS comunica;
USE comunica;

-- ------------------------------------------------------------
-- 1) Tabla destino: eventos de presencia (columnar, comprimida)
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS presencia_eventos
(
    ts          DateTime64(3),
    usuario_id  String,
    estado      LowCardinality(String),   -- solo 2 valores -> dict encoding
    zona        LowCardinality(String),   -- solo 3 valores
    dispositivo LowCardinality(String),
    latencia_ms UInt32
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(ts)
ORDER BY (zona, ts, usuario_id)
TTL toDateTime(ts) + INTERVAL 30 DAY;

-- ------------------------------------------------------------
-- 2) Kafka Engine: consume el topico SIN escribir un consumer
--    (Sec. 4.2 - el log distribuido alimenta la capa analitica)
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS presencia_kafka
(
    ts          DateTime64(3),
    usuario_id  String,
    estado      String,
    zona        String,
    dispositivo String,
    latencia_ms UInt32
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:9092',
    kafka_topic_list = 'presencia-eventos',
    kafka_group_name = 'clickhouse-analitica',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 1,
    kafka_skip_broken_messages = 100;

-- ------------------------------------------------------------
-- 3) Materialized View: puente Kafka -> MergeTree
-- ------------------------------------------------------------
CREATE MATERIALIZED VIEW IF NOT EXISTS presencia_mv
TO presencia_eventos AS
SELECT ts, usuario_id, estado, zona, dispositivo, latencia_ms
FROM presencia_kafka;

-- ------------------------------------------------------------
-- 4) Entregas de alertas (para medir efectividad del fan-out)
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS entregas
(
    alerta_id    UUID,
    usuario_id   String,
    zona         LowCardinality(String),
    publicada_en DateTime64(3),
    entregada_en DateTime64(3),
    latencia_ms  UInt32
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(publicada_en)
ORDER BY (alerta_id, zona, usuario_id);
