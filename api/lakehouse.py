"""
ComunicaMedellin - Pipeline del Mini-Lakehouse (Taller 3)
===========================================================
Un endpoint por cada paso de la guia. Cada uno ejecuta el script REAL
del taller (en ../taller-3-lakehouse/scripts/) contra el cluster
Cassandra vivo y va emitiendo por WebSocket lo que ese paso hace de
verdad: el comando, lo que escribe, y por que.

Los puntos, tal como los pide la guia del Taller 3:
  L0  Poblar alertas reales (seed_alertas.py)       -- una vez
  L1  Extraccion -> Bronze (extract.py)
  L2  Limpieza -> Silver (transform_silver.py)
  L3  Modelo dimensional -> Gold (transform_gold.py)
  L4  Las 5 consultas analiticas en DuckDB (analyze.py)
  L5  Benchmark: Cassandra (ALLOW FILTERING) vs DuckDB columnar
"""
import asyncio
import re
import subprocess
import sys
import time
from pathlib import Path

from fastapi import APIRouter, WebSocket, WebSocketDisconnect

router = APIRouter(prefix="/lakehouse")
ctx: dict = {}

TALLER3 = Path(__file__).resolve().parents[1] / "taller-3-lakehouse"
PYTHON = sys.executable


# ─────────────────────────────────────────────────────────────
# Utilidades de emision (mismo contrato que api/taller.py)
# ─────────────────────────────────────────────────────────────
async def paso(sock, n, titulo, detalle=None, cmd=None, salida=None,
               estado="ok", datos=None):
    await sock.send_json({
        "tipo": "paso", "n": n, "titulo": titulo, "detalle": detalle,
        "cmd": cmd, "salida": salida, "estado": estado, "datos": datos or {},
    })
    await asyncio.sleep(0.15)


async def fin(sock, resumen, metricas=None):
    await sock.send_json({"tipo": "fin", "resumen": resumen,
                          "metricas": metricas or {}})


def correr(script: str) -> tuple[str, float, int]:
    """Ejecuta un script del taller con el interprete del venv y
    devuelve (stdout+stderr combinados, segundos, returncode)."""
    t0 = time.perf_counter()
    r = subprocess.run(
        [PYTHON, str(TALLER3 / "scripts" / script)],
        cwd=str(TALLER3), capture_output=True, text=True,
    )
    dur = time.perf_counter() - t0
    salida = (r.stdout or "") + (r.stderr or "")
    return salida, dur, r.returncode


def extraer_numero(patron: str, texto: str, default="0"):
    m = re.search(patron, texto)
    return m.group(1) if m else default


# ═════════════════════════════════════════════════════════════
# L0 — Poblar datos reales de alertas (seed_alertas.py)
# ═════════════════════════════════════════════════════════════
async def l0_seed(sock, params):
    await paso(sock, 1, "Por que hace falta este paso",
               "El Modulo 1 solo siembra usuarios (tabla 'presencia'). Sin "
               "alertas, historial y estado de entrega reales, Bronze estaria "
               "vacio. Este seed publica 60 alertas variadas y hace fan-out "
               "real a una muestra de usuarios, escribiendo en Cassandra.",
               cmd="python scripts/seed_alertas.py")

    salida, dur, code = correr("seed_alertas.py")
    n_alertas = extraer_numero(r"(\d+)\s+alertas,", salida)
    n_entregas = extraer_numero(r"([\d,]+)\s+filas en", salida).replace(",", "")

    if code != 0:
        await paso(sock, 2, "El seed fallo", salida[-600:], estado="error")
        await fin(sock, "No se pudo poblar Cassandra. Revisa que el cluster "
                         "este arriba (`make up` en comunicamedellin-grupo3).")
        return

    await paso(sock, 2, "Alertas publicadas con fan-out real",
               f"Completado en {dur:.1f} s contra el cluster Cassandra real "
               f"(cass1:9042). Severidades info/advertencia/critica, 3 zonas, "
               f"30 dias de historia.",
               salida=salida.strip()[-500:],
               datos={"alertas": int(n_alertas or 0), "entregas": int(n_entregas or 0)})

    await fin(sock,
              f"{n_alertas} alertas con {n_entregas} entregas ya estan en "
              f"'alertas', 'historial_usuario' y 'estado_entrega'. El pipeline "
              f"del lakehouse (L1-L5) ya tiene algo real que extraer.",
              {"alertas": int(n_alertas or 0), "entregas": int(n_entregas or 0),
               "seg": round(dur, 1)})


# ═════════════════════════════════════════════════════════════
# L1 — Extraccion a Bronze
# ═════════════════════════════════════════════════════════════
async def l1_bronze(sock, params):
    await paso(sock, 1, "Que hace Bronze",
               "Copia 'presencia', 'alertas', 'historial_usuario' y "
               "'estado_entrega' de Cassandra a Parquet, tal cual estan. "
               "Bronze es inmutable: cada corrida agrega un archivo nuevo "
               "con timestamp, nunca sobreescribe.",
               cmd="python scripts/extract.py")

    salida, dur, code = correr("extract.py")
    if code != 0:
        await paso(sock, 2, "La extraccion fallo", salida[-600:], estado="error")
        await fin(sock, "Revisa la conexion a Cassandra (cass1:9042).")
        return

    tablas = re.findall(r"Extrayendo (\w+)\.\.\.\s*\n\s*-> ([\d,]+) filas", salida)
    for i, (tabla, filas) in enumerate(tablas, start=2):
        await paso(sock, i, f"Bronze: {tabla}",
                   f"SELECT * FROM {tabla} -> Parquet sin transformar.",
                   cmd=f"extraer_tabla(session, 'SELECT * FROM {tabla}', '{tabla}')",
                   salida=f"{filas} filas guardadas",
                   datos={"tabla": tabla, "filas": int(filas.replace(",", ""))})

    total = sum(int(f.replace(",", "")) for _, f in tablas)
    await fin(sock,
              f"{len(tablas)} tablas, {total:,} filas copiadas de Cassandra a "
              f"lakehouse/bronze/ en {dur:.1f} s. Nada se transformo todavia.",
              {"tablas": len(tablas), "filas": total, "seg": round(dur, 1)})


# ═════════════════════════════════════════════════════════════
# L2 — Limpieza a Silver
# ═════════════════════════════════════════════════════════════
async def l2_silver(sock, params):
    await paso(sock, 1, "Que hace Silver",
               "Lee el Parquet Bronze MAS RECIENTE de cada tabla y resuelve "
               "calidad: tipos, nulos, duplicados. No aplica reglas de "
               "negocio todavia -- eso es trabajo de Gold.",
               cmd="python scripts/transform_silver.py")

    salida, dur, code = correr("transform_silver.py")
    if code != 0:
        await paso(sock, 2, "La limpieza fallo", salida[-600:], estado="error")
        await fin(sock, "Revisa que Bronze tenga datos (corre L1 primero).")
        return

    bloques = re.findall(
        r"=== REPORTE DE CALIDAD — (\w+) ===\s*"
        r"Filas Bronze originales :\s*([\d,]+)\s*"
        r"Filas Silver resultantes:\s*([\d,]+)\s*"
        r"Filas eliminadas\s*:\s*([\d,]+)\s*"
        r"Porcentaje retenido\s*:\s*([\d.]+)%",
        salida,
    )
    for i, (tabla, antes, despues, elim, pct) in enumerate(bloques, start=2):
        await paso(sock, i, f"Silver: {tabla.lower()}",
                   f"{antes} filas Bronze -> {despues} filas Silver "
                   f"({elim} eliminadas por nulos/duplicados).",
                   salida=f"Retenido: {pct}%",
                   datos={"tabla": tabla.lower(), "retenido_pct": float(pct)})

    await fin(sock,
              f"{len(bloques)} tablas limpiadas en {dur:.1f} s. El reporte "
              f"completo queda en docs/reporte_calidad.txt (entregable del "
              f"Paso 3).",
              {"tablas": len(bloques), "seg": round(dur, 1)})


# ═════════════════════════════════════════════════════════════
# L3 — Modelo dimensional Gold
# ═════════════════════════════════════════════════════════════
async def l3_gold(sock, params):
    await paso(sock, 1, "Que hace Gold",
               "Construye el esquema en constelacion: 4 dimensiones "
               "conformadas (DIM_TIEMPO, DIM_USUARIO, DIM_ZONA, DIM_ALERTA) "
               "y 2 tablas de hechos (FACT_ENTREGAS, FACT_PRESENCIA_HORA). "
               "Ver docs/modelo_dimensional.md para las 5 preguntas de diseno.",
               cmd="python scripts/transform_gold.py")

    salida, dur, code = correr("transform_gold.py")
    if code != 0:
        await paso(sock, 2, "El modelado fallo", salida[-600:], estado="error")
        await fin(sock, "Revisa que Silver tenga datos (corre L2 primero).")
        return

    tablas = re.findall(r"(DIM_\w+|FACT_\w+): ([\d,]+) (?:fechas|usuarios|zonas|alertas|filas)", salida)
    for i, (tabla, n) in enumerate(tablas, start=2):
        es_hecho = tabla.startswith("FACT")
        await paso(sock, i, f"{'Hecho' if es_hecho else 'Dimension'}: {tabla}",
                   "Grano: 1 fila = 1 intento de entrega." if tabla == "FACT_ENTREGAS"
                   else "Grano: 1 fila = 1 usuario x 1 hora." if tabla == "FACT_PRESENCIA_HORA"
                   else None,
                   salida=f"{n} filas",
                   datos={"tabla": tabla, "filas": int(n.replace(",", ""))},
                   estado="ok" if not es_hecho else "aviso")

    await fin(sock,
              f"Esquema en constelacion completo en {dur:.1f} s: "
              f"{sum(1 for t,_ in tablas if t.startswith('DIM'))} dimensiones + "
              f"{sum(1 for t,_ in tablas if t.startswith('FACT'))} hechos en "
              f"lakehouse/gold/*.parquet. DuckDB ya puede leerlos directo.",
              {"tablas": len(tablas), "seg": round(dur, 1)})


# ═════════════════════════════════════════════════════════════
# L4 — Las 5 consultas analiticas (DuckDB sobre Gold)
# ═════════════════════════════════════════════════════════════
async def l4_consultas(sock, params):
    await paso(sock, 1, "Por que estas 5 y no otras",
               "Cubren las dos tablas de hechos: volumen y fallos de entrega "
               "por tiempo/severidad/zona/usuario, y proporcion de tiempo "
               "online por zona y hora. DuckDB lee Gold directamente, sin "
               "cargar nada en memoria aparte.",
               cmd="python scripts/analyze.py")

    salida, dur, code = correr("analyze.py")
    if code != 0:
        await paso(sock, 2, "El analisis fallo", salida[-600:], estado="error")
        await fin(sock, "Revisa que Gold tenga datos (corre L3 primero).")
        return

    consultas = re.findall(
        r"\[(Q\d) — ([^\]]+)\] — ([\d.]+) ms — (\d+) filas", salida)
    for i, (cod, titulo, ms, filas) in enumerate(consultas, start=2):
        await paso(sock, i, f"{cod} — {titulo}",
                   None,
                   salida=f"{filas} filas en {ms} ms",
                   datos={"cod": cod, "ms": float(ms), "filas": int(filas)})

    await fin(sock,
              f"{len(consultas)} consultas analiticas resueltas en {dur:.1f} s "
              f"sobre Parquet columnar, todas en el orden de milisegundos. Ver "
              f"salida completa con `python scripts/analyze.py`.",
              {"consultas": len(consultas), "seg": round(dur, 1)})


# ═════════════════════════════════════════════════════════════
# L5 — Benchmark: Cassandra (OLTP) vs DuckDB (columnar)
# ═════════════════════════════════════════════════════════════
async def l5_benchmark(sock, params):
    await paso(sock, 1, "La pregunta de negocio",
               "Contar entregas fallidas sobre todo el dataset. 'estado' no "
               "es parte de la partition key de estado_entrega, asi que "
               "Cassandra necesita ALLOW FILTERING: recorrer TODAS las "
               "particiones del cluster y descartar en memoria.")

    salida, dur, code = correr("analyze.py")
    if code != 0:
        await paso(sock, 2, "El benchmark fallo", salida[-600:], estado="error")
        await fin(sock, "Revisa que Gold tenga datos (corre L3 primero).")
        return

    m = re.search(
        r"Cassandra\s+\(ALLOW FILTERING\):\s*([\d.]+) ms\s+->\s*([\d,]+) filas, "
        r"([\d,]+) fallidas\s*\n"
        r"DuckDB\s+\(Parquet Gold\)\s*:\s*([\d.]+) ms\s+->\s*([\d,]+) filas, "
        r"([\d,]+) fallidas\s*\n"
        r"Factor de mejora\s*:\s*([\d.]+)x",
        salida,
    )
    if not m:
        await paso(sock, 2, "No se pudo leer el resultado del benchmark",
                   salida[-600:], estado="aviso")
        await fin(sock, "Revisa la salida de analyze.py manualmente.")
        return

    ms_cass, filas_cass, fallidas_cass, ms_duck, filas_duck, fallidas_duck, factor = m.groups()

    await paso(sock, 2, "Cassandra: recorrido completo con ALLOW FILTERING",
               "El coordinador pregunta a todos los nodos y filtra en memoria. "
               "Es exactamente el conflicto OLTP/OLAP de la Sec. 2.2.",
               cmd="SELECT estado FROM estado_entrega ALLOW FILTERING;",
               salida=f"{ms_cass} ms -> {filas_cass} filas, {fallidas_cass} fallidas",
               estado="aviso",
               datos={"ms": float(ms_cass), "filas": int(filas_cass.replace(",", ""))})

    await paso(sock, 3, "DuckDB: mismo conteo sobre el Gold columnar",
               "Solo lee la columna fallido_flag del Parquet. No hay "
               "coordinador, no hay red entre nodos.",
               cmd="SELECT COUNT(*), SUM(fallido_flag) FROM fact_entregas;",
               salida=f"{ms_duck} ms -> {filas_duck} filas, {fallidas_duck} fallidas",
               datos={"ms": float(ms_duck), "filas": int(filas_duck.replace(",", ""))})

    await paso(sock, 4, "El factor de mejora",
               "Con pocos miles de filas la ventaja ya es clara porque "
               "Cassandra fuerza un scan completo del cluster; con datasets "
               "de cientos de miles la brecha crece aun mas (Sec. 5.2 de la "
               "guia).",
               salida=f"{factor}x mas rapido en DuckDB",
               datos={"factor": float(factor)})

    await fin(sock,
              f"Cassandra: {ms_cass} ms (ALLOW FILTERING, scan completo). "
              f"DuckDB: {ms_duck} ms (columnar, Parquet). Factor: {factor}x. "
              f"No es que un motor sea mejor en absoluto: Cassandra esta "
              f"optimizada para servir por clave de particion, no para "
              f"agregar sobre columnas arbitrarias.",
              {"ms_cassandra": float(ms_cass), "ms_duckdb": float(ms_duck),
               "factor": float(factor), "filas": int(filas_cass.replace(",", "")),
               "fallidas": int(fallidas_cass.replace(",", ""))})


# ═════════════════════════════════════════════════════════════
PIPELINES = {
    "l0": l0_seed, "l1": l1_bronze, "l2": l2_silver,
    "l3": l3_gold, "l4": l4_consultas, "l5": l5_benchmark,
}


@router.websocket("/ws/{punto}")
async def ws_pipeline(sock: WebSocket, punto: str):
    await sock.accept()
    fn = PIPELINES.get(punto)
    if not fn:
        await sock.send_json({"tipo": "error", "mensaje": f"punto desconocido: {punto}"})
        await sock.close()
        return
    try:
        params = {}
        try:
            params = await asyncio.wait_for(sock.receive_json(), timeout=1.5)
        except (asyncio.TimeoutError, Exception):
            pass
        await sock.send_json({"tipo": "inicio", "punto": punto})
        await fn(sock, params or {})
    except WebSocketDisconnect:
        return
    except Exception as e:
        try:
            await sock.send_json({"tipo": "error",
                                  "mensaje": f"{type(e).__name__}: {e}"})
        except Exception:
            pass
    finally:
        try:
            await sock.close()
        except Exception:
            pass


@router.get("/estado")
def estado():
    """Que hay en el lakehouse ahora mismo, para pintar el panel inicial."""
    info = {"bronze": 0, "silver": 0, "gold": 0, "cassandra_vivo": False}
    for zona in ("bronze", "silver", "gold"):
        carpeta = TALLER3 / "lakehouse" / zona
        info[zona] = len(list(carpeta.glob("*.parquet"))) if carpeta.exists() else 0
    r = subprocess.run(
        ["docker", "ps", "--filter", "name=cass1", "--format", "{{.Names}}"],
        capture_output=True, text=True)
    info["cassandra_vivo"] = "cass1" in r.stdout
    return info
