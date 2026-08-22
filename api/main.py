"""
ComunicaMedellin - API de demostracion
=======================================
Expone por WebSocket la lectura simultanea del mismo usuario desde
dos nodos distintos, para que el frontend muestre la divergencia
de consistencia en vivo.

Sec. 1.2 (CAP) | Sec. 1.3 (Cassandra AP) | Sec. 3.3 (distribuida)
"""
import asyncio, json, time, datetime as dt
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


def leer(nodo: str, usuario: str, nivel: str):
    """Lee la presencia de un usuario desde un nodo concreto."""
    if nodo not in conexiones:
        return {"nodo": nodo, "error": "no disponible"}
    _, s = conexiones[nodo]
    q = SimpleStatement(
        "SELECT usuario_id, estado, last_seen, zona, escrito_por "
        "FROM presencia WHERE usuario_id=%s")
    q.consistency_level = NIVELES.get(nivel, ConsistencyLevel.ONE)
    try:
        t0 = time.perf_counter()
        f = s.execute(q, (usuario,)).one()
        ms = (time.perf_counter() - t0) * 1000
        return {
            "nodo": nodo, "latencia_ms": round(ms, 2),
            "estado": f.estado if f else None,
            "last_seen": f.last_seen.isoformat() if f and f.last_seen else None,
            "zona": f.zona if f else None,
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
    _, s = conexiones["cass1"]
    q = SimpleStatement(
        "UPDATE presencia SET estado=%s, last_seen=%s, zona=%s, "
        "dispositivo=%s, escrito_por=%s WHERE usuario_id=%s")
    q.consistency_level = NIVELES.get(e.nivel, ConsistencyLevel.ONE)
    t0 = time.perf_counter()
    s.execute(q, (e.estado, dt.datetime.now(dt.timezone.utc),
                  e.zona, "web", "cass1", e.usuario_id))
    return {"ok": True, "latencia_ms": round((time.perf_counter() - t0) * 1000, 2)}


@app.get("/salud")
def salud():
    return {"nodos_conectados": list(conexiones.keys())}


@app.websocket("/ws")
async def ws(sock: WebSocket):
    """Empuja lecturas simultaneas de nodo1 y nodo3 al frontend."""
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
                "divergente": divergente, "delta_ms": round(delta, 1),
            })
    except WebSocketDisconnect:
        pass
