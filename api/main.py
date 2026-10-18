"""
ComunicaMedellin - API de demostracion
=======================================
Expone por WebSocket la lectura simultanea del mismo usuario desde
dos nodos distintos, para que el frontend muestre la divergencia
de consistencia en vivo.

Sec. 1.2 (CAP) | Sec. 1.3 (Cassandra AP) | Sec. 3.3 (distribuida)
"""
import asyncio, json, time, uuid, datetime as dt
from contextlib import asynccontextmanager

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from cassandra import ConsistencyLevel
from cassandra.cluster import Cluster, ExecutionProfile, EXEC_PROFILE_DEFAULT
from cassandra.policies import WhiteListRoundRobinPolicy
from cassandra.query import SimpleStatement

NODOS = {"cass1": 9042, "cass2": 9043, "cass3": 9044}
NIVELES = {"ONE": ConsistencyLevel.ONE,
           "QUORUM": ConsistencyLevel.QUORUM,
           "ALL": ConsistencyLevel.ALL}

conexiones: dict[str, tuple] = {}


def _conectar(nodo: str):
    perfil = ExecutionProfile(
        load_balancing_policy=WhiteListRoundRobinPolicy(["127.0.0.1"]),
        request_timeout=15)
    cl = Cluster(["127.0.0.1"], port=NODOS[nodo],
                 execution_profiles={EXEC_PROFILE_DEFAULT: perfil},
                 protocol_version=4)
    return cl, cl.connect("comunica")


@asynccontextmanager
async def lifespan(app: FastAPI):
    for nodo in NODOS:
        try:
            conexiones[nodo] = _conectar(nodo)
            print(f"  conectado a {nodo}")
        except Exception as e:
            print(f"  {nodo} no disponible: {type(e).__name__}")
    yield
    for cl, _ in conexiones.values():
        cl.shutdown()


app = FastAPI(title="ComunicaMedellin - Grupo 3", lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"],
                   allow_methods=["*"], allow_headers=["*"])

# Pipelines del taller del Modulo 2 (un endpoint por punto del enunciado).
# Comparten el pool de conexiones de arriba en vez de abrir uno propio.
from taller import router as taller_router, ctx as taller_ctx  # noqa: E402
taller_ctx["conexiones"] = conexiones
app.include_router(taller_router)

# Pipeline del Mini-Lakehouse (Taller 3): extract/silver/gold/analyze
# reales en ../taller-3-lakehouse/scripts/, expuestos punto por punto.
from lakehouse import router as lakehouse_router  # noqa: E402
app.include_router(lakehouse_router)


def leer(nodo: str, usuario: str, nivel: str):
    """Lee la presencia de un usuario desde un nodo concreto."""
    if nodo not in conexiones:
        return {"nodo": nodo, "error": "no disponible"}
    _, s = conexiones[nodo]
    q = SimpleStatement(
        "SELECT usuario_id, nombre, estado, last_seen, zona, comuna, escrito_por "
        "FROM presencia WHERE usuario_id=%s")
    q.consistency_level = NIVELES.get(nivel, ConsistencyLevel.ONE)
    try:
        t0 = time.perf_counter()
        f = s.execute(q, (usuario,)).one()
        ms = (time.perf_counter() - t0) * 1000
        return {
            "nodo": nodo, "latencia_ms": round(ms, 2),
            "estado": f.estado if f else None,
            "nombre": f.nombre if f else None,
            "last_seen": f.last_seen.isoformat() if f and f.last_seen else None,
            "zona": f.zona if f else None,
            "comuna": f.comuna if f else None,
            "escrito_por": f.escrito_por if f else None,
        }
    except Exception as e:
        return {"nodo": nodo, "error": type(e).__name__}


class Escritura(BaseModel):
    usuario_id: str = "usuario:demo:A-12"
    estado: str = "online"
    zona: str = "centro"
    nivel: str = "ONE"


@app.post("/presencia")
def escribir(e: Escritura):
    """Actualiza la presencia de un usuario.

    NO toca zona ni comuna: son atributos del suscriptor, no del
    heartbeat. Al desnormalizar (Sec. 2.2) el motor ya no valida
    la coherencia entre columnas -- si esta escritura cambiara la
    zona sin cambiar la comuna, dejaria filas invalidas como
    "El Poblado / zona centro". La integridad la sostiene el
    codigo, que es el precio de renunciar al modelo relacional.
    """
    _, s = conexiones["cass1"]
    q = SimpleStatement(
        "UPDATE presencia SET estado=%s, last_seen=%s, "
        "dispositivo=%s, escrito_por=%s WHERE usuario_id=%s")
    q.consistency_level = NIVELES.get(e.nivel, ConsistencyLevel.ONE)
    t0 = time.perf_counter()
    s.execute(q, (e.estado, dt.datetime.now(dt.timezone.utc),
                  "web", "cass1", e.usuario_id))
    return {"ok": True, "latencia_ms": round((time.perf_counter() - t0) * 1000, 2)}


@app.get("/suscriptores")
def suscriptores(limite: int = 12):
    """Lista algunos suscriptores reales para poblar el selector
    del frontend. Sin esto habria que copiar cedulas a mano."""
    if "cass1" not in conexiones:
        return []
    _, s = conexiones["cass1"]
    filas = s.execute(
        f"SELECT usuario_id, nombre, comuna, zona FROM presencia LIMIT {int(limite)}")
    return [{"usuario_id": f.usuario_id, "nombre": f.nombre,
             "comuna": f.comuna, "zona": f.zona}
            for f in filas if f.nombre]



# ─────────────────────────────────────────────────────────────
# Endpoints didacticos: mostrar la base de datos en pantalla
# ─────────────────────────────────────────────────────────────

@app.get("/tablas")
def tablas():
    """Esquema del keyspace: que tablas hay y con que columnas.

    Sirve para explicar en clase el modelo clave-valor (Sec. 1.3)
    sin tener que abrir cqlsh en una terminal aparte.
    """
    if "cass1" not in conexiones:
        return []
    _, s = conexiones["cass1"]
    out = []
    filas = s.execute("""
        SELECT table_name FROM system_schema.tables
        WHERE keyspace_name='comunica'""")
    for t in filas:
        cols = s.execute("""
            SELECT column_name, type, kind FROM system_schema.columns
            WHERE keyspace_name='comunica' AND table_name=%s""", (t.table_name,))
        n = s.execute(f"SELECT COUNT(*) AS c FROM {t.table_name}").one()
        out.append({
            "tabla": t.table_name,
            "filas": n.c if n else 0,
            "columnas": [
                {"nombre": c.column_name, "tipo": c.type,
                 "clave": c.kind}          # partition_key | clustering | regular
                for c in sorted(cols, key=lambda c: (c.kind != "partition_key",
                                                     c.kind != "clustering",
                                                     c.column_name))
            ],
        })
    return sorted(out, key=lambda x: x["tabla"])


@app.get("/datos/{tabla}")
def datos(tabla: str, limite: int = 12):
    """Filas reales de una tabla, tal como estan guardadas."""
    if tabla not in {"presencia", "historial_usuario", "alertas"}:
        return {"error": "tabla no permitida"}
    if "cass1" not in conexiones:
        return {"error": "sin conexion"}
    _, s = conexiones["cass1"]
    filas = s.execute(f"SELECT * FROM {tabla} LIMIT {int(limite)}")
    cols = list(filas.column_names)
    return {
        "tabla": tabla,
        "columnas": cols,
        "filas": [
            [(v.isoformat(sep=" ", timespec="milliseconds")
              if isinstance(v, dt.datetime) else
              (str(v) if v is not None else None))
             for v in row]
            for row in filas
        ],
    }


@app.get("/anillo")
def anillo():
    """Como se reparten los datos entre los 3 nodos.

    Muestra que Cassandra distribuye por hash de la clave de
    particion (Sec. 3.3), no por rangos que uno elija.
    """
    if "cass1" not in conexiones:
        return {"nodos": []}
    _, s = conexiones["cass1"]
    vivos = []
    for nodo in NODOS:
        vivos.append({"nodo": nodo, "activo": nodo in conexiones})
    # Reparto real por zona (dato de negocio, no de infraestructura)
    zonas = {}
    for z in ("norte", "centro", "sur"):
        r = s.execute(
            "SELECT COUNT(*) AS c FROM presencia WHERE zona=%s ALLOW FILTERING",
            (z,)).one()
        zonas[z] = r.c if r else 0
    total = s.execute("SELECT COUNT(*) AS c FROM presencia").one()
    return {"nodos": vivos, "zonas": zonas,
            "total": total.c if total else 0}


@app.get("/replicas/{usuario}")
def replicas(usuario: str):
    """Que devuelve CADA nodo para el mismo usuario.

    Es el corazon de la demo: con RF=3 el dato vive en los tres,
    pero pueden no estar sincronizados (Sec. 1.2, consistencia
    eventual).
    """
    out = []
    for nodo in NODOS:
        out.append(leer(nodo, usuario, "ONE"))
    return out



# ─────────────────────────────────────────────────────────────
# Simulador de fan-out: la vista del funcionario de la Alcaldia
# ─────────────────────────────────────────────────────────────

@app.websocket("/ws-fanout")
async def ws_fanout(sock: WebSocket):
    """Transmite el recorrido REAL de una alerta, paso a paso.

    El frontend dibuja el flujo (API -> Kafka -> workers -> Cassandra
    -> dispositivos) y necesita saber cuando ocurre cada etapa para
    animarla. Aqui no simulamos: publicamos en Kafka de verdad y
    escribimos en Cassandra de verdad, emitiendo un evento por cada
    hito para que la animacion siga al sistema y no al reves.
    """
    await sock.accept()
    try:
        while True:
            cfg = json.loads(await sock.receive_text())
            titulo = (cfg.get("titulo") or "Alerta sin titulo")[:200]
            cuerpo = (cfg.get("cuerpo") or "")[:500]
            severidad = cfg.get("severidad", "critica")
            destinos = max(3, min(120, int(cfg.get("destinos", 12))))
            # Ritmo de la animacion: con pocos dispositivos conviene
            # verlos llegar de a uno; con 100 hay que acelerar o la
            # demo se vuelve eterna.
            pausa = 0.09 if destinos <= 24 else (0.045 if destinos <= 60 else 0.022)
            alerta_id = uuid.uuid4()
            t_ini = time.perf_counter()

            def ms():
                return round((time.perf_counter() - t_ini) * 1000, 1)

            await sock.send_json({"evento": "inicio", "ms": ms(),
                                  "alerta_id": str(alerta_id),
                                  "titulo": titulo, "severidad": severidad})

            # ── 1. Publicar en Kafka: una particion por zona ──
            zonas = ["norte", "centro", "sur"]
            try:
                from kafka import KafkaProducer
                prod = KafkaProducer(
                    bootstrap_servers="localhost:29092",
                    value_serializer=lambda v: json.dumps(v).encode(),
                    key_serializer=lambda k: str(k).encode(), acks=1)
                for i, z in enumerate(zonas):
                    md = prod.send(
                        "alertas-ciudadanas", key=z, partition=i,
                        value={"alerta_id": str(alerta_id), "titulo": titulo,
                               "cuerpo": cuerpo, "severidad": severidad,
                               "zona_destino": z}).get(timeout=8)
                    await sock.send_json({
                        "evento": "kafka", "ms": ms(), "zona": z,
                        "particion": md.partition, "offset": md.offset})
                    await asyncio.sleep(0.18)
                prod.flush(); prod.close()
            except Exception as e:
                await sock.send_json({"evento": "kafka_error", "ms": ms(),
                                      "detalle": type(e).__name__})

            # ── 2. Repartir los destinatarios entre las 3 zonas ──
            if "cass1" not in conexiones:
                await sock.send_json({"evento": "fin", "ms": ms(),
                                      "error": "sin conexion a Cassandra"})
                continue
            _, ses = conexiones["cass1"]
            suscriptores = []
            for z in zonas:
                # Reparto equitativo: si destinos=100, van 34/33/33
                cupo = destinos // 3 + (1 if zonas.index(z) < destinos % 3 else 0)
                filas = ses.execute(
                    "SELECT usuario_id, nombre, comuna, zona FROM presencia "
                    "WHERE zona=%s LIMIT %s ALLOW FILTERING",
                    (z, max(1, cupo)))
                suscriptores += [
                    {"usuario_id": f.usuario_id, "nombre": f.nombre,
                     "comuna": f.comuna, "zona": f.zona}
                    for f in filas if f.nombre]

            await sock.send_json({"evento": "workers", "ms": ms(),
                                  "total": len(suscriptores)})

            # ── 3. Escribir el historial de cada uno (QUORUM) ──
            # QUORUM porque una alerta de emergencia no se puede perder.
            prep = ses.prepare(
                "INSERT INTO historial_usuario (usuario_id, enviado_en, "
                "alerta_id, titulo, cuerpo, severidad, zona) "
                "VALUES (?,?,?,?,?,?,?)")
            prep.consistency_level = ConsistencyLevel.QUORUM

            for u in suscriptores:
                t0 = time.perf_counter()
                try:
                    ses.execute(prep, (
                        u["usuario_id"], dt.datetime.now(dt.timezone.utc),
                        alerta_id, titulo, cuerpo, severidad, u["zona"]))
                    lat = round((time.perf_counter() - t0) * 1000, 1)
                    await sock.send_json({
                        "evento": "entrega", "ms": ms(), "latencia_ms": lat,
                        "usuario_id": u["usuario_id"], "nombre": u["nombre"],
                        "comuna": u["comuna"], "zona": u["zona"]})
                except Exception as e:
                    await sock.send_json({
                        "evento": "entrega_error", "ms": ms(),
                        "usuario_id": u["usuario_id"],
                        "detalle": type(e).__name__})
                await asyncio.sleep(pausa)

            # ── 4. Registrar la alerta en el catalogo ──
            try:
                ses.execute(
                    "INSERT INTO alertas (alerta_id, titulo, cuerpo, severidad, "
                    "zona_destino, publicada_en, total_envios) "
                    "VALUES (%s,%s,%s,%s,%s,%s,%s)",
                    (alerta_id, titulo, cuerpo, severidad, "todas",
                     dt.datetime.now(dt.timezone.utc), len(suscriptores)))
            except Exception:
                pass

            await sock.send_json({"evento": "fin", "ms": ms(),
                                  "total": len(suscriptores)})
    except WebSocketDisconnect:
        pass



# ─────────────────────────────────────────────────────────────
# Laboratorio de red: simular fallos reales con tc netem
# ─────────────────────────────────────────────────────────────
#
# Los tres nodos comparten una maquina, asi que la red entre
# ellos es perfecta: microsegundos, cero perdida. Eso NO existe
# en produccion. Aqui se degrada a proposito para poder observar
# lo que el laboratorio esconde.
#
# tc = traffic control del kernel de Linux
# netem = network emulator, su modulo de emulacion de red

import subprocess

# Puerto 7000 = comunicacion ENTRE nodos de Cassandra.
# Se degrada solo ese: el 9042 (consultas del cliente) queda
# intacto, para que el cliente lea rapido de una replica que
# aun no recibio la escritura.
PUERTO_REPLICACION = 7000


def _tc(nodo: str, *args: str):
    """Ejecuta tc dentro de un contenedor."""
    r = subprocess.run(
        ["docker", "exec", "--privileged", nodo, "tc", *args],
        capture_output=True, text=True, timeout=15)
    return r.returncode, (r.stderr or r.stdout).strip()


def _limpiar(nodo: str):
    _tc(nodo, "qdisc", "del", "dev", "eth0", "root")


def _aplicar(nodo: str, netem_args: list[str]):
    """Aplica una regla netem SOLO al trafico de replicacion."""
    _limpiar(nodo)
    # Tres bandas de prioridad; la 3 sera la degradada
    c, e = _tc(nodo, "qdisc", "add", "dev", "eth0", "root",
               "handle", "1:", "prio")
    if c != 0:
        return False, e
    c, e = _tc(nodo, "qdisc", "add", "dev", "eth0", "parent", "1:3",
               "handle", "30:", "netem", *netem_args)
    if c != 0:
        return False, e
    # Filtrar por puerto de replicacion, en ambos sentidos
    for direccion in ("dport", "sport"):
        _tc(nodo, "filter", "add", "dev", "eth0", "protocol", "ip",
            "parent", "1:0", "prio", "3", "u32",
            "match", "ip", direccion, str(PUERTO_REPLICACION), "0xffff",
            "flowid", "1:3")
    return True, "ok"


class ReglaRed(BaseModel):
    nodo: str = "cass3"
    escenario: str = "normal"   # normal | lento | inestable | saturado | perdida
    valor: int = 250            # ms de retardo, o % de perdida


ESCENARIOS = {
    "normal": {
        "nombre": "Red sana",
        "desc": "Sin degradacion. Los tres nodos replican en microsegundos.",
        "args": None,
    },
    "lento": {
        "nombre": "Enlace intercontinental",
        "desc": "Retardo constante, como un enlace Medellin-Miami.",
        "args": lambda v: ["delay", f"{v}ms"],
    },
    "inestable": {
        "nombre": "Red inestable",
        "desc": "Retardo con jitter: la latencia varia entre paquetes.",
        "args": lambda v: ["delay", f"{v}ms", f"{max(10, v//3)}ms",
                           "distribution", "normal"],
    },
    "perdida": {
        "nombre": "Perdida de paquetes",
        "desc": "Un porcentaje de paquetes desaparece. Fuerza retransmisiones.",
        "args": lambda v: ["loss", f"{v}%"],
    },
    "saturado": {
        "nombre": "Ancho de banda limitado",
        "desc": "Enlace saturado: pocos kbit/s entre nodos.",
        "args": lambda v: ["rate", f"{max(64, v)}kbit"],
    },
}


@app.get("/red/estado")
def red_estado():
    """Que reglas de red tiene cada nodo ahora mismo."""
    out = []
    for nodo in NODOS:
        c, salida = _tc(nodo, "qdisc", "show", "dev", "eth0")
        activo = "netem" in salida
        detalle = ""
        if activo:
            for linea in salida.splitlines():
                if "netem" in linea:
                    detalle = linea.split("netem", 1)[1].strip()
                    break
        out.append({"nodo": nodo, "degradado": activo, "detalle": detalle})
    return {"nodos": out, "escenarios": [
        {"id": k, "nombre": v["nombre"], "desc": v["desc"]}
        for k, v in ESCENARIOS.items()]}


@app.post("/red/aplicar")
def red_aplicar(r: ReglaRed):
    """Degrada (o restaura) la red de un nodo."""
    if r.nodo not in NODOS:
        return {"ok": False, "error": "nodo desconocido"}
    esc = ESCENARIOS.get(r.escenario)
    if not esc:
        return {"ok": False, "error": "escenario desconocido"}

    if esc["args"] is None:
        _limpiar(r.nodo)
        return {"ok": True, "nodo": r.nodo, "escenario": "normal",
                "mensaje": f"Red de {r.nodo} restaurada"}

    ok, detalle = _aplicar(r.nodo, esc["args"](r.valor))
    return {"ok": ok, "nodo": r.nodo, "escenario": r.escenario,
            "valor": r.valor, "mensaje": detalle}


@app.get("/red/medir/{usuario}")
def red_medir(usuario: str, nivel: str = "ONE"):
    """Escribe y lee desde los 3 nodos, devolviendo latencias.

    Permite ver en vivo como una degradacion de red afecta a cada
    nivel de consistencia.
    """
    if "cass1" not in conexiones:
        return {"error": "sin conexion"}
    from cassandra.query import SimpleStatement
    cl = NIVELES.get(nivel, ConsistencyLevel.ONE)

    _, s1 = conexiones["cass1"]
    q_esc = SimpleStatement(
        "UPDATE presencia SET estado=%s, last_seen=%s, "
        "escrito_por=%s WHERE usuario_id=%s")
    q_esc.consistency_level = cl
    ahora = dt.datetime.now(dt.timezone.utc)
    t0 = time.perf_counter()
    escritura = None
    try:
        s1.execute(q_esc, ("online", ahora, "cass1", usuario))
        escritura = round((time.perf_counter() - t0) * 1000, 2)
    except Exception as e:
        escritura = None
        err_esc = type(e).__name__

    lecturas = []
    for nodo in NODOS:
        lecturas.append(leer(nodo, usuario, nivel))
    return {"nivel": nivel, "escritura_ms": escritura, "lecturas": lecturas}


@app.get("/salud")
def salud():
    return {"nodos_conectados": list(conexiones.keys())}


@app.websocket("/ws")
async def ws(sock: WebSocket):
    """Empuja lecturas simultaneas de LOS TRES nodos al frontend.

    Con RF=3 el dato vive en los tres, asi que mostrarlos todos
    hace visible cual va atrasado (Sec. 1.2, consistencia eventual).
    """
    await sock.accept()
    usuario, nivel = "usuario:demo:A-12", "ONE"
    try:
        while True:
            try:
                msg = await asyncio.wait_for(sock.receive_text(), timeout=0.4)
                cfg = json.loads(msg)
                usuario = cfg.get("usuario", usuario)
                nivel = cfg.get("nivel", nivel)
            except (asyncio.TimeoutError, json.JSONDecodeError):
                pass

            loop = asyncio.get_event_loop()
            n1, n3 = await asyncio.gather(
                loop.run_in_executor(None, leer, "cass1", usuario, nivel),
                loop.run_in_executor(None, leer, "cass3", usuario, nivel))

            divergente = n1.get("last_seen") != n3.get("last_seen")
            delta = 0.0
            if divergente and n1.get("last_seen") and n3.get("last_seen"):
                d1 = dt.datetime.fromisoformat(n1["last_seen"])
                d3 = dt.datetime.fromisoformat(n3["last_seen"])
                delta = abs((d1 - d3).total_seconds() * 1000)

            await sock.send_json({
                "ts": dt.datetime.now().isoformat(),
                "nivel": nivel, "usuario": usuario,
                "nodo1": n1, "nodo3": n3,
                "nodos": [leer(n, usuario, nivel) for n in NODOS],
                "divergente": divergente, "delta_ms": round(delta, 1),
            })
    except WebSocketDisconnect:
        pass
