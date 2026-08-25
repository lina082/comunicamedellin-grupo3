-- ============================================================
-- QUE SE PUEDE Y QUE NO SE PUEDE CONSULTAR EN CASSANDRA
-- ComunicaMedellin - Grupo 3
-- ============================================================
--
-- LA REGLA:
--   En SQL modelas los datos y luego consultas lo que quieras.
--   En Cassandra es al reves: primero defines que consultas
--   necesitas, y de ahi sale el modelo. Se llama "query-first
--   design" y es consecuencia de ser distribuida (Sec. 3.3):
--   los datos viven repartidos por hash de la clave de
--   particion, asi que solo se puede buscar eficientemente
--   por esa clave.
--
-- Nuestras claves:
--   presencia          PRIMARY KEY (usuario_id)
--   historial_usuario  PRIMARY KEY ((usuario_id), enviado_en)
--   alertas            PRIMARY KEY (alerta_id)
--
-- Ejecutar con:  make cql
-- ============================================================

USE comunica;


-- ════════════════════════════════════════════════════════════
--  LO QUE SI SE PUEDE
-- ════════════════════════════════════════════════════════════

-- 1. Por clave de particion. El caso ideal: un solo salto al
--    nodo que tiene el dato. Es lo que hace la app 99 % del tiempo.
SELECT usuario_id, nombre, comuna, estado, last_seen
FROM presencia
WHERE usuario_id = 'CC65027980';

-- 2. Varias claves a la vez con IN. Cuidado: el coordinador
--    consulta una particion por cada valor, asi que con listas
--    largas se vuelve caro.
SELECT usuario_id, nombre, zona
FROM presencia
WHERE usuario_id IN ('CC65027980', 'CC61474411');

-- 3. Particion + rango sobre la clave de clustering.
--    historial_usuario ordena por enviado_en DESC, asi que
--    "los ultimos N mensajes" es una lectura secuencial barata.
SELECT usuario_id, enviado_en, titulo, severidad
FROM historial_usuario
WHERE usuario_id = 'CC65027980'
LIMIT 5;

-- 4. Rango temporal dentro de una particion.
SELECT enviado_en, titulo
FROM historial_usuario
WHERE usuario_id = 'CC65027980'
  AND enviado_en > '2026-08-01';

-- 5. Invertir el orden de clustering.
SELECT enviado_en, titulo
FROM historial_usuario
WHERE usuario_id = 'CC65027980'
ORDER BY enviado_en ASC
LIMIT 3;

-- 6. Elegir el nivel de consistencia por consulta.
--    Esto es lo que ningun otro motor del modulo permite.
CONSISTENCY ONE;
SELECT usuario_id, estado, last_seen FROM presencia
WHERE usuario_id = 'CC65027980';

CONSISTENCY QUORUM;
SELECT usuario_id, estado, last_seen FROM presencia
WHERE usuario_id = 'CC65027980';

CONSISTENCY ONE;   -- volver al valor por defecto

-- 7. Escritura condicional (lightweight transaction).
--    Usa Paxos, no el camino normal de escritura: es entre 10 y
--    20 veces mas lento. Solo cuando la exclusion importa de
--    verdad; en TuBoleta (Equipo 1) seria el mecanismo correcto.
INSERT INTO alertas (alerta_id, titulo, severidad, publicada_en)
VALUES (uuid(), 'Prueba LWT', 'info', toTimestamp(now()))
IF NOT EXISTS;

-- 8. TTL: el dato se borra solo. Util para presencia, que no
--    necesita conservarse indefinidamente.
UPDATE presencia USING TTL 3600
SET estado = 'online'
WHERE usuario_id = 'CC65027980';

-- 9. Metadatos del esquema (esto si es SQL normal).
SELECT table_name FROM system_schema.tables
WHERE keyspace_name = 'comunica';

SELECT column_name, type, kind FROM system_schema.columns
WHERE keyspace_name = 'comunica' AND table_name = 'presencia';


-- ════════════════════════════════════════════════════════════
--  LO QUE NO SE PUEDE  (descomentar para ver el error)
-- ════════════════════════════════════════════════════════════

-- ❌ Filtrar por una columna que no es clave
-- SELECT * FROM presencia WHERE zona = 'norte';
--    InvalidRequest: ... use ALLOW FILTERING
--    Por que: zona no participa en el hash, asi que habria que
--    preguntar a los 3 nodos y revisar todas las filas.

-- ❌ Agregar por una columna cualquiera
-- SELECT zona, count(*) FROM presencia GROUP BY zona;
--    InvalidRequest: Group by is currently only supported on
--    the columns of the PRIMARY KEY
--    Por que: exactamente para esto existe ClickHouse en la
--    arquitectura (Sec. 2.2).

-- ❌ Ordenar por una columna arbitraria
-- SELECT usuario_id FROM presencia ORDER BY nombre;
--    InvalidRequest: ORDER BY is only supported when the
--    partition key is restricted by an EQ or an IN
--    Por que: el orden solo existe DENTRO de una particion,
--    definido por la clave de clustering.

-- ❌ JOIN entre tablas
-- SELECT p.nombre, h.titulo FROM presencia p JOIN historial_usuario h ...
--    CQL no tiene JOIN. Un join cruzaria datos de nodos
--    distintos por red. Por eso el modelo se desnormaliza:
--    nombre y comuna viven junto a presencia.

-- ❌ Subconsultas, UNION, HAVING
--    Tampoco existen. CQL se parece a SQL en la sintaxis, pero
--    no es un lenguaje relacional.


-- ════════════════════════════════════════════════════════════
--  LA VALVULA DE ESCAPE: ALLOW FILTERING
-- ════════════════════════════════════════════════════════════
-- Funciona, pero es un anti-patron: obliga a un scan completo
-- del cluster. Con 10.000 filas responde; con 100 millones
-- tumba el nodo coordinador.
--
-- Lo usamos SOLO en los scripts de demo, para traer usuarios de
-- ejemplo. Nunca en el camino critico.

SELECT usuario_id, nombre, comuna
FROM presencia
WHERE zona = 'norte'
LIMIT 5
ALLOW FILTERING;

-- La forma CORRECTA de responder esa misma pregunta seria crear
-- una segunda tabla con la zona como clave de particion:
--
--   CREATE TABLE presencia_por_zona (
--       zona       text,
--       usuario_id text,
--       nombre     text,
--       PRIMARY KEY ((zona), usuario_id)
--   );
--
-- Es duplicar el dato para servir otra consulta. Eso es
-- "una tabla por consulta", el modelado propio de Cassandra.
