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
-- Topologia: cass1 y cass2 en el DC 'medellin', cass3 en 'miami'.
--
-- NetworkTopologyStrategy coloca las replicas POR DATACENTER, no
-- solo por posicion en el anillo. Eso es lo que habilita los
-- niveles LOCAL_* (Sec. 3.3: la replicacion es consciente de la
-- topologia fisica).
--
-- {medellin:2, miami:1} = 3 replicas en total:
--   2 en la ciudad principal, 1 en el respaldo.
--
-- Con SimpleStrategy (lo que teniamos antes) las 3 replicas
-- podrian caer todas en la misma ciudad, y LOCAL_QUORUM no
-- tendria sentido.
CREATE KEYSPACE IF NOT EXISTS comunica
WITH replication = {
  'class': 'NetworkTopologyStrategy',
  'medellin': 2,
  'miami': 1
};

USE comunica;

-- ============================================================
-- NOTA SOBRE NORMALIZACION Y ACID  (leer antes de la exposicion)
-- ============================================================
-- Este esquema NO esta normalizado y NO ofrece ACID entre filas.
-- Es deliberado, no un descuido:
--
-- 1) DESNORMALIZACION (Sec. 2.2)
--    En un modelo relacional 3NF tendriamos usuarios, comunas y
--    zonas en tablas separadas, unidas por JOIN. Cassandra no
--    tiene JOINs, y aunque los tuviera cruzarian datos que viven
--    en nodos distintos. La regla de modelado aqui es "una tabla
--    por consulta": se duplica el dato para que cada lectura
--    toque una sola particion.
--    Consecuencia: nombre, comuna y zona viven junto a presencia.
--
-- 2) BASE en vez de ACID (Sec. 1.2, 2.2)
--    Cassandra es AP: renuncia a la C de CAP, y ACID empieza por
--    consistencia. Lo que SI garantiza:
--      Atomicidad  -> si, dentro de una misma particion
--      Durabilidad -> si, con QUORUM sobrevive a un nodo caido
--      Aislamiento -> NO entre particiones
--      Consistencia inmediata -> NO, es eventual
--
-- 3) QUIEN CUIDA LA INTEGRIDAD ENTONCES
--    El codigo. Sin FOREIGN KEY ni CHECK, el motor acepta
--    "El Poblado / zona centro" sin protestar. Por eso la zona
--    SIEMPRE se deriva de la comuna en _datos.py (zona_de) y el
--    endpoint POST /presencia no toca zona ni comuna.
--    Ese es el precio real de renunciar al modelo relacional, y
--    conviene decirlo en voz alta en la exposicion.
-- ============================================================


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

-- ============================================================
-- MODULO 2 - Tablas anadidas por el taller de modelado NoSQL
-- ============================================================
-- Las tres tablas anteriores nacieron del Modulo 1, donde la
-- pregunta era "que motor". El Modulo 2 pregunta "que modelo", y
-- eso obliga a partir de las CONSULTAS, no de las entidades.
--
-- Al listar las cinco consultas reales del sistema aparecieron dos
-- que ninguna tabla existente puede responder sin ALLOW FILTERING:
--
--   Q4  usuarios en linea de una zona  -> presencia esta particionada
--                                         por usuario_id, la zona no
--                                         participa en el hash
--   Q5  estado de entrega de una alerta -> no se guardaba en ningun lado
--
-- La respuesta de Cassandra a eso no es un indice: es otra tabla.
-- Se duplica el dato para servir otra consulta (Modulo 1, Sec. 2.2).
-- ============================================================


-- ------------------------------------------------------------
-- TABLA 4: PRESENCIA POR ZONA  (Q4 - vista invertida de presencia)
-- ------------------------------------------------------------
-- Misma informacion que 'presencia', indexada al reves.
--
-- Clave compuesta:
--   particion  = (zona, estado)  clave COMPUESTA, no solo zona
--   clustering = usuario_id      unicidad dentro de la particion
--
-- Por que (zona, estado) y no solo zona:
--   Con 3 zonas y 100.000 usuarios, particionar solo por zona da
--   ~33.000 filas por particion concentradas en 3 tokens del anillo.
--   Eso es un hotspot de libro: 3 particiones calientes recibiendo
--   ~10.000 escrituras/s mientras el resto del anillo esta ocioso.
--   Anadir 'estado' al hash sube a 6 particiones (3 zonas x 2
--   estados). Sigue siendo poco: por eso ademas se aplica bucketing
--   por comuna en la variante de abajo.
--
-- Nota de consistencia: esta tabla y 'presencia' se escriben en la
-- misma operacion logica pero NO son atomicas entre si (particiones
-- distintas -> sin aislamiento, ver nota BASE arriba). Pueden
-- divergir unos milisegundos. Es aceptable: ambas son consistencia
-- eventual por diseno.
--
-- El TTL de 900 s es el mecanismo de limpieza: si un usuario deja de
-- reportar heartbeat, su fila 'online' expira sola en 15 minutos.
-- Sin esto la tabla acumularia usuarios fantasma para siempre, porque
-- al cambiar de estado el usuario cambia de PARTICION y la fila vieja
-- no se sobrescribe: hay que borrarla o dejarla expirar.
CREATE TABLE IF NOT EXISTS presencia_por_zona (
    zona        text,
    estado      text,
    usuario_id  text,
    nombre      text,
    comuna      text,
    last_seen   timestamp,
    PRIMARY KEY ((zona, estado), usuario_id)
) WITH comment = 'Q4: usuarios en linea por zona. Vista invertida de presencia.'
  AND default_time_to_live = 900;


-- ------------------------------------------------------------
-- TABLA 4b: PRESENCIA POR COMUNA  (Q4 sin hotspot)
-- ------------------------------------------------------------
-- Version con bucketing real. La comuna es la unidad geografica
-- natural de la Alcaldia (16 comunas) y multiplica x16 el numero de
-- particiones frente a la zona.
--
--   16 comunas x 2 estados = 32 particiones
--   100.000 / 32 ~ 3.100 filas por particion
--
-- Sigue sin ser una distribucion perfecta, pero el objetivo aqui no
-- es repartir por igual: es que ninguna particion crezca sin limite.
-- Con 3.100 filas de ~120 bytes cada particion pesa ~370 KB, muy
-- por debajo del limite practico de 100 MB por particion.
--
-- La consulta por zona se resuelve con el driver: N consultas
-- paralelas (una por comuna de la zona) en vez de una consulta a una
-- particion gigante. Es mas trabajo para el cliente y menos para el
-- coordinador, que es el intercambio correcto en Cassandra.
CREATE TABLE IF NOT EXISTS presencia_por_comuna (
    comuna      text,
    estado      text,
    usuario_id  text,
    nombre      text,
    zona        text,
    last_seen   timestamp,
    PRIMARY KEY ((comuna, estado), usuario_id)
) WITH comment = 'Q4 sin hotspot: bucketing por comuna (16 buckets vs 3 zonas).'
  AND default_time_to_live = 900;


-- ------------------------------------------------------------
-- TABLA 5: ESTADO DE ENTREGA  (Q5 - trazabilidad del fan-out)
-- ------------------------------------------------------------
-- Responde: "de los 100.000 destinatarios de la alerta X, a cuantos
-- les llego y cuantos siguen pendientes?"
--
-- Clave compuesta:
--   particion  = (alerta_id, bucket)  bucket = hash(usuario) % 32
--   clustering = usuario_id
--
-- POR QUE EL BUCKET ES OBLIGATORIO AQUI:
--   Si la particion fuera solo alerta_id, una alerta a 100.000
--   suscriptores generaria UNA particion de 100.000 filas
--   (~9,5 MB). Al publicar 10 alertas simultaneas, 10 particiones
--   absorben el 100 % de la escritura del cluster: los 3 nodos
--   quedan desbalanceados y el resto del anillo ocioso.
--   Con 32 buckets la misma alerta se reparte en 32 particiones de
--   ~3.100 filas (~300 KB), distribuidas por todo el anillo.
--
--   Este es el hotspot mas grave del sistema y el ejemplo mas claro
--   de que la partition key no se elige por la entidad, sino por
--   como se distribuye la carga.
--
-- El TTL de 30 dias es deliberado: la trazabilidad de entrega es
-- dato operativo, no historico. A los 30 dias se borra sola y el
-- espacio se recupera en la siguiente compactacion.
CREATE TABLE IF NOT EXISTS estado_entrega (
    alerta_id    uuid,
    bucket       int,          -- hash(usuario_id) % 32
    usuario_id   text,
    estado       text,         -- 'enviado' | 'entregado' | 'leido' | 'fallido'
    intentos     int,
    actualizado  timestamp,
    PRIMARY KEY ((alerta_id, bucket), usuario_id)
) WITH comment = 'Q5: trazabilidad de entrega. Bucketing x32 contra hotspot.'
  AND default_time_to_live = 2592000;


-- ------------------------------------------------------------
-- TABLA 6: EVENTOS PROCESADOS  (idempotencia frente a Kafka)
-- ------------------------------------------------------------
-- Kafka garantiza entrega "al menos una vez" (at-least-once): si un
-- worker procesa un mensaje y muere antes de confirmar el offset,
-- al reiniciar vuelve a recibir el mismo mensaje. En un fan-out eso
-- significa reenviar la alerta a 100.000 personas por segunda vez.
--
-- La defensa NO es esta tabla. La defensa principal es que las
-- escrituras del fan-out sean idempotentes POR DISENO:
--
--   historial_usuario  PK ((usuario_id), enviado_en)
--   Reprocesar el mismo evento reescribe la MISMA fila con el mismo
--   timestamp. Cassandra resuelve por last-write-wins y el resultado
--   es identico. Cero duplicados, sin coordinacion.
--
--   Condicion: enviado_en debe venir del EVENTO (lo fija el
--   productor), nunca de now() en el worker. Con now() cada
--   reintento crearia una fila nueva y el usuario veria la alerta
--   repetida. Es el error clasico del fan-out.
--
-- Esta tabla cubre lo que el last-write-wins no cubre: los efectos
-- colaterales no idempotentes (enviar el push, contar la metrica).
-- Antes de disparar el push el worker registra el evento con
-- IF NOT EXISTS; si la fila ya existe, el evento es un reintento y
-- se descarta el push.
--
-- Coste: IF NOT EXISTS es una lightweight transaction (Paxos), de
-- 10 a 20 veces mas lenta que una escritura normal. Por eso se usa
-- SOLO en el guardia del push, no en el camino del fan-out.
CREATE TABLE IF NOT EXISTS eventos_procesados (
    evento_id    text,         -- clave idempotente: alerta_id:usuario_id
    procesado_en timestamp,
    worker       text,
    PRIMARY KEY (evento_id)
) WITH comment = 'Idempotencia: guardia contra reproceso de Kafka (at-least-once).'
  AND default_time_to_live = 604800;


-- ------------------------------------------------------------
-- TABLA DE CONTRASTE: entrega SIN bucketing
-- ------------------------------------------------------------
-- No forma parte del modelo propuesto. Existe solo para medir el
-- hotspot y sostener con datos la decision de la tabla 5.
--
-- Es identica a estado_entrega salvo por la clave de particion:
-- aqui la particion es solo alerta_id, que es lo que saldria de
-- modelar por entidades en vez de por distribucion de carga.
--
-- Medido con 50.000 filas (make hotspot):
--
--   (alerta_id)            1 particion   de 1.909,8 KB
--   (alerta_id, bucket)   32 particiones de    71,7 KB
--
-- Proyectado a 100.000 destinatarios, la version sin bucket
-- concentra 3,73 MB y, sobre todo, las 100.000 escrituras en UN
-- token del anillo. Cabe de sobra en el limite de 100 MB por
-- particion: el problema no es el tamano, es la distribucion.
-- Sobre 3 nodos el efecto se disimula porque todos son replica de
-- todo; sobre 12 nodos, 3 quedarian saturados y 9 ociosos.
CREATE TABLE IF NOT EXISTS entrega_sin_bucket (
    alerta_id   uuid,
    usuario_id  text,
    estado      text,
    actualizado timestamp,
    PRIMARY KEY ((alerta_id), usuario_id)
) WITH comment = 'CONTRASTE, no usar: mide el hotspot sin bucketing.';
