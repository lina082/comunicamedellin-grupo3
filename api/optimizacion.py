"""
ComunicaMedellin - Pipeline de Optimizacion (Taller 4)
========================================================
Mismo contrato que api/lakehouse.py: un endpoint por paso, cada uno
ejecuta el script REAL del taller (en ../taller-4-optimizacion/scripts/)
y va emitiendo por WebSocket lo que ese paso hace de verdad.

Los puntos, continuando la numeracion del pipeline del Taller 3:
  L6  Diagnostico: EXPLAIN ANALYZE + row groups del Gold (diagnostico.py)
  L7  Intervencion I-3: reordenar Parquet + ROW_GROUP_SIZE (intervencion.py)
  L8  Medicion DESPUES sobre la Gold optimizada (medicion_despues.py)
"""
import asyncio
import re
import subprocess
import sys
import time
from pathlib import Path

from fastapi import APIRouter, WebSocket, WebSocketDisconnect

router = APIRouter(prefix="/optimizacion")

TALLER4 = Path(__file__).resolve().parents[1] / "taller-4-optimizacion"
PYTHON = sys.executable


# ─────────────────────────────────────────────────────────────
# Utilidades de emision (mismo contrato que api/lakehouse.py)
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
    """Ejecuta un script del taller con el interprete actual y
    devuelve (stdout+stderr combinados, segundos, returncode)."""
    t0 = time.perf_counter()
    r = subprocess.run(
        [PYTHON, str(TALLER4 / "scripts" / script)],
        cwd=str(TALLER4), capture_output=True, text=True,
    )
    dur = time.perf_counter() - t0
    salida = (r.stdout or "") + (r.stderr or "")
    return salida, dur, r.returncode


# ═════════════════════════════════════════════════════════════
# L6 — Diagnostico (EXPLAIN ANALYZE + row groups, antes de intervenir)
# ═════════════════════════════════════════════════════════════
async def l6_diagnostico(sock, params):
    await paso(sock, 1, "Por que hace falta este paso",
               "El Modulo 4 exige leer el plan de ejecucion real antes de "
               "intervenir, nunca optimizar a ciegas. Se adaptan las 3 "
               "consultas mas representativas del Taller 3 (Q1, Q3, Q5) "
               "agregandoles un WHERE realista, y se corre EXPLAIN ANALYZE "
               "contra el Gold actual, sin ninguna intervencion todavia.",
               cmd="python scripts/diagnostico.py")

    salida, dur, code = correr("diagnostico.py")
    if code != 0:
        await paso(sock, 2, "El diagnostico fallo", salida[-600:], estado="error")
        await fin(sock, "Revisa que gold_taller3/ tenga los Parquet del Taller 3.")
        return

    rg = re.findall(r"(\w+): (\d+) row group\(s\) para ([\d,]+) filas", salida)
    for i, (tabla, n, filas) in enumerate(rg, start=2):
        await paso(sock, i, f"Row groups: {tabla}.parquet",
                   "Un solo row group significa que no hay estadisticas de "
                   "minimo y maximo por bloque: el planificador no puede "
                   "descartar lectura sin importar que tan selectivo sea "
                   "el filtro." if n == "1" else None,
                   salida=f"{n} row group(s) para {filas} filas",
                   datos={"tabla": tabla, "row_groups": int(n), "filas": int(filas.replace(",", ""))},
                   estado="aviso" if n == "1" else "ok")

    medianas = re.findall(r"\[(Q\d)\] mediana=([\d.]+) ms  min=([\d.]+) ms  max=([\d.]+) ms", salida)
    for i, (cod, mediana, mn, mx) in enumerate(medianas, start=len(rg) + 2):
        await paso(sock, i, f"{cod}: EXPLAIN ANALYZE baseline",
                   "Operador mas costoso: TABLE_SCAN, decodifica todas las "
                   "filas del row group unico antes de aplicar cualquier "
                   "filtro.",
                   cmd=f"EXPLAIN ANALYZE {cod}...",
                   salida=f"mediana {mediana} ms (min {mn}, max {mx})",
                   datos={"cod": cod, "ms": float(mediana)})

    await fin(sock,
              f"Diagnostico completo en {dur:.1f} s. Causa raiz identificada: "
              f"fact_entregas.parquet y fact_presencia_hora.parquet tienen un "
              f"solo row group cada uno, asi que ningun filtro, por selectivo "
              f"que sea, puede evitar el escaneo completo. Planes guardados en "
              f"evidencia/Q1_plan_baseline.txt, Q3_plan_baseline.txt y "
              f"Q5_plan_baseline.txt.",
              {"seg": round(dur, 1), "queries": len(medianas)})


# ═════════════════════════════════════════════════════════════
# L7 — Intervencion I-3 (reordenar Parquet + ROW_GROUP_SIZE)
# ═════════════════════════════════════════════════════════════
async def l7_intervencion(sock, params):
    await paso(sock, 1, "La tecnica elegida: I-3",
               "Una sola intervencion por problema diagnosticado: ordenar el "
               "Parquet por la columna que cada query filtra y fijar "
               "ROW_GROUP_SIZE en 5.000, para que cada bloque tenga un rango "
               "acotado de la columna de filtro y el planificador pueda "
               "descartarlo sin decodificarlo.",
               cmd="python scripts/intervencion.py")

    salida, dur, code = correr("intervencion.py")
    if code != 0:
        await paso(sock, 2, "La intervencion fallo", salida[-600:], estado="error")
        await fin(sock, "Revisa que el diagnostico (L6) se haya corrido primero.")
        return

    archivos = re.findall(r"(\S+\.parquet) optimizado: (\d+) row groups \(ordenado por (\w+)\)", salida)
    for i, (archivo, n, col) in enumerate(archivos, start=2):
        await paso(sock, i, f"Reescrito: {archivo}",
                   f"COPY (SELECT * FROM read_parquet(...) ORDER BY {col}) "
                   f"TO '{archivo}' (FORMAT PARQUET, ROW_GROUP_SIZE 5000)",
                   salida=f"{n} row groups generados, ordenado por {col}",
                   datos={"archivo": archivo, "row_groups": int(n), "columna": col})

    await paso(sock, len(archivos) + 2, "Por que dos copias de fact_entregas",
               "Q1 filtra por fecha (tiempo_sk) y Q3 filtra por estado. Un "
               "unico orden fisico no sirve para los dos predicados a la "
               "vez, asi que se genero fact_entregas_por_estado.parquet "
               "como segunda copia, ordenada por estado, en vez de forzar "
               "un compromiso que no optimizaria ninguna de las dos.")

    await fin(sock,
              f"Intervencion aplicada en {dur:.1f} s sobre "
              f"lakehouse_optimizado/: {len(archivos)} archivos de hechos "
              f"reescritos con row groups fragmentados, dimensiones "
              f"copiadas sin cambios.",
              {"seg": round(dur, 1), "archivos": len(archivos)})


# ═════════════════════════════════════════════════════════════
# L8 — Medicion DESPUES (sobre la Gold optimizada)
# ═════════════════════════════════════════════════════════════
async def l8_medicion(sock, params):
    await paso(sock, 1, "Que se vuelve a medir",
               "El mismo protocolo del diagnostico (L6): 3 calentamientos "
               "sin medir, 5 corridas medidas, se reporta la mediana. Ahora "
               "contra lakehouse_optimizado/, para que la comparacion sea "
               "limpia entre antes y despues.",
               cmd="python scripts/medicion_despues.py")

    salida, dur, code = correr("medicion_despues.py")
    if code != 0:
        await paso(sock, 2, "La medicion fallo", salida[-600:], estado="error")
        await fin(sock, "Revisa que la intervencion (L7) se haya corrido primero.")
        return

    medianas = re.findall(r"\[(Q\d)\] mediana=([\d.]+) ms  min=([\d.]+) ms  max=([\d.]+) ms", salida)

    baseline = {"Q1": 3.78, "Q3": 3.14, "Q5": 0.91}
    for i, (cod, mediana, mn, mx) in enumerate(medianas, start=2):
        antes = baseline.get(cod)
        factor = round(antes / float(mediana), 2) if antes and float(mediana) > 0 else None
        await paso(sock, i, f"{cod}: despues de la intervencion",
                   f"Antes (informe Taller 4): {antes} ms -> Despues (esta corrida): {mediana} ms"
                   if antes else None,
                   cmd=f"EXPLAIN ANALYZE {cod}... (sobre lakehouse_optimizado/)",
                   salida=f"mediana {mediana} ms (min {mn}, max {mx})"
                          + (f" — factor {factor}x vs. informe" if factor else ""),
                   datos={"cod": cod, "ms_despues": float(mediana),
                          "ms_antes": antes, "factor": factor})

    await fin(sock,
              f"Medicion completa en {dur:.1f} s. Los tiempos de esta corrida "
              f"pueden variar levemente frente al informe por el estado de "
              f"cache de esta maquina en este momento; lo que debe "
              f"mantenerse es el orden relativo entre Q1, Q3 y Q5, porque "
              f"depende de la selectividad del filtro de cada consulta, no "
              f"de la maquina. Planes guardados en evidencia/*_plan_despues.txt.",
              {"seg": round(dur, 1), "queries": len(medianas)})


# ═════════════════════════════════════════════════════════════
PIPELINES = {
    "l6": l6_diagnostico, "l7": l7_intervencion, "l8": l8_medicion,
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


TABLA_POR_PASO = {
    "l6": {"archivo": "Q1_plan_baseline.txt", "tabla": "fact_entregas.parquet (Gold, sin intervencion)"},
    "l7": {"archivo": None, "tabla": "lakehouse_optimizado/*.parquet"},
    "l8": {"archivo": "Q1_plan_despues.txt", "tabla": "fact_entregas.parquet (Gold, optimizado)"},
}


def _row_groups(ruta: Path):
    import duckdb
    con = duckdb.connect()
    try:
        n = con.execute(
            f"SELECT COUNT(DISTINCT row_group_id) FROM parquet_metadata('{ruta}')"
        ).fetchone()[0]
        filas = con.execute(f"SELECT COUNT(*) FROM read_parquet('{ruta}')").fetchone()[0]
        return n, filas
    finally:
        con.close()


@router.get("/muestra/{paso}")
def muestra(paso: str):
    """Evidencia real del paso: el plan EXPLAIN ANALYZE guardado (L6/L8)
    o los row groups de cada Parquet reescrito (L7), para el panel del
    frontend junto al codigo anotado."""
    evidencia = TALLER4 / "evidencia"
    if paso in ("l6", "l8"):
        sufijo = "baseline" if paso == "l6" else "despues"
        columnas = ["query", "mediana_ms", "min_ms", "max_ms"]
        filas = []
        resumen = evidencia / f"resumen_{sufijo}.md"
        if resumen.exists():
            texto = resumen.read_text()
            for m in re.finditer(
                r"## (Q\d).*?\*\*([\d.]+) ms\*\*\n- Min: ([\d.]+) ms \| Max: ([\d.]+) ms",
                texto, re.S,
            ):
                filas.append([m.group(1), m.group(2), m.group(3), m.group(4)])
        return {
            "fuente": f"evidencia/resumen_{sufijo}.md",
            "tabla": "medianas EXPLAIN ANALYZE" if paso == "l6" else "medianas despues de I-3",
            "cmd": "EXPLAIN ANALYZE <query>  (3 warm-up + 5 corridas medidas)",
            "columnas": columnas, "filas": filas,
            "aviso": None if filas else "corre este paso primero",
        }
    if paso == "l7":
        opt = TALLER4 / "lakehouse_optimizado"
        columnas = ["archivo", "row_groups", "filas"]
        filas = []
        if opt.exists():
            for nombre in ["fact_entregas.parquet", "fact_entregas_por_estado.parquet", "fact_presencia_hora.parquet"]:
                ruta = opt / nombre
                if ruta.exists():
                    n, f = _row_groups(ruta)
                    filas.append([nombre, str(n), str(f)])
        return {
            "fuente": "lakehouse_optimizado/ (parquet_metadata)",
            "tabla": "row groups por archivo reescrito",
            "cmd": "SELECT COUNT(DISTINCT row_group_id) FROM parquet_metadata('...')",
            "columnas": columnas, "filas": filas,
            "aviso": None if filas else "corre este paso primero",
        }
    return {"error": f"paso desconocido: {paso}"}


@router.get("/estado")
def estado():
    """Que hay en el lakehouse optimizado ahora mismo, para el panel."""
    info = {"gold": 0, "optimizado": 0}
    gold = TALLER4 / "gold_taller3"
    opt = TALLER4 / "lakehouse_optimizado"
    info["gold"] = len(list(gold.glob("*.parquet"))) if gold.exists() else 0
    info["optimizado"] = len(list(opt.glob("*.parquet"))) if opt.exists() else 0
    return info
