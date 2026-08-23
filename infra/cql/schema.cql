-- ============================================================
-- ComunicaMedellin - Esquema Cassandra
-- Grupo 3 | Modulo 1 - ITM 2026
-- ============================================================
--
-- Justificacion teorica:
--   Sec. 1.3 - Cassandra (AP, wide-column). Replicacion leaderless,
--              sin punto unico de falla (caso Netflix).
--   Sec. 3.3 - Arquitectura distribuida: sharding por clave de
--              particion, replicacion configurable.
--   Sec. 1.2 - CAP: elegimos AP. La consistencia se ajusta POR
--              CONSULTA (ONE / QUORUM), no globalmente.
-- ============================================================

-- RF=3 sobre 3 nodos: cada dato vive en los 3 nodos.
-- Esto es lo que permite comparar ONE vs QUORUM:
--   ONE    -> responde 1 replica  (rapido, puede estar desactualizado)
--   QUORUM -> responden 2 de 3    (consistente, mas lento)
CREATE KEYSPACE IF NOT EXISTS comunica
WITH replication = {
  'class': 'SimpleStrategy',
  'replication_factor': 3
};

USE comunica;

-- ------------------------------------------------------------
-- TABLA 1: PRESENCIA  (carga C4 - clave-valor de alta rotacion)
-- ------------------------------------------------------------
-- Responde a la pregunta (b) del enunciado:
--   "Por que un modelo clave-valor en vez de uno relacional?"
--
-- Patron de acceso unico: dame el estado del usuario X.
-- No hay JOINs, no hay queries analiticas, no hay integridad
-- referencial que mantener. Un modelo normalizado (3NF, Sec 2.1)
-- solo agregaria costo sin aportar nada.
--
-- usuario_id es la clave de particion -> distribuye la carga
-- uniformemente entre los 3 nodos por hash del token.
CREATE TABLE IF NOT EXISTS presencia (
    usuario_id   text PRIMARY KEY,   -- cedula: CC12345678
    nombre       text,
    telefono     text,
    estado       text,        -- 'online' | 'offline'
    last_seen    timestamp,
    zona         text,        -- norte | centro | sur
    comuna       text,        -- comuna de Medellin
    lat          double,
    lon          double,
    dispositivo  text,
    escrito_por  text         -- nodo que atendio la escritura (util en la demo)
) WITH comment = 'Presencia en tiempo real. Consistencia eventual (AP).'
  AND default_time_to_live = 0;

-- ------------------------------------------------------------
-- TABLA 2: HISTORIAL POR USUARIO  (carga C3 - fan-out on write)
-- ------------------------------------------------------------
-- Modelo "fan-out on write": al publicar UNA alerta, se escribe
-- una fila por cada suscriptor. Se privilegia lectura barata
-- (el usuario abre su historial y lee una sola particion).
--
-- Clave compuesta:
--   particion   = usuario_id  -> todo el historial de un usuario junto
--   clustering  = enviado_en DESC -> los mas recientes primero
CREATE TABLE IF NOT EXISTS historial_usuario (
    usuario_id  text,
    enviado_en  timestamp,
    alerta_id   uuid,
    titulo      text,
    cuerpo      text,
    severidad   text,        -- info | advertencia | critica
    zona        text,
    PRIMARY KEY ((usuario_id), enviado_en)
) WITH CLUSTERING ORDER BY (enviado_en DESC)
  AND comment = 'Historial por usuario. Fan-out on write.';

-- ------------------------------------------------------------
-- TABLA 3: ALERTAS PUBLICADAS  (catalogo, baja frecuencia)
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS alertas (
    alerta_id    uuid PRIMARY KEY,
    titulo       text,
    cuerpo       text,
    severidad    text,
    zona_destino text,
    publicada_en timestamp,
    total_envios int
) WITH comment = 'Catalogo de alertas emitidas.';

-- ------------------------------------------------------------
-- Verificacion rapida
-- ------------------------------------------------------------
-- DESCRIBE KEYSPACE comunica;
-- SELECT * FROM presencia LIMIT 5;
