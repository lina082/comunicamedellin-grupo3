#!/usr/bin/env python3
"""
Taller 3 - Paso 5: consultas analiticas sobre Gold + benchmark
operacional (Cassandra) vs columnar (DuckDB sobre Parquet).
"""
import sys
import time
from pathlib import Path

import duckdb

BASE = Path(__file__).resolve().parent.parent
GOLD = BASE / "lakehouse" / "gold"

sys.path.insert(0, str(BASE.parent / "demo" / "scripts"))
from _common import conectar  # noqa: E402


def registrar_vistas(con):
    con.execute(f"CREATE VIEW fact_entregas AS SELECT * FROM read_parquet('{GOLD}/fact_entregas.parquet')")
    con.execute(f"CREATE VIEW fact_presencia_hora AS SELECT * FROM read_parquet('{GOLD}/fact_presencia_hora.parquet')")
    con.execute(f"CREATE VIEW dim_tiempo AS SELECT * FROM read_parquet('{GOLD}/dim_tiempo.parquet')")
    con.execute(f"CREATE VIEW dim_usuario AS SELECT * FROM read_parquet('{GOLD}/dim_usuario.parquet')")
    con.execute(f"CREATE VIEW dim_zona AS SELECT * FROM read_parquet('{GOLD}/dim_zona.parquet')")
    con.execute(f"CREATE VIEW dim_alerta AS SELECT * FROM read_parquet('{GOLD}/dim_alerta.parquet')")


# ── Query 1: volumen de entregas por mes y severidad ─────────────────
Q1 = """
SELECT t.anio, t.mes, t.mes_nombre, a.severidad,
       COUNT(*) AS total_entregas,
       SUM(f.entregado_flag) AS entregadas,
       SUM(f.fallido_flag) AS fallidas,
       ROUND(SUM(f.fallido_flag) * 100.0 / COUNT(*), 2) AS tasa_fallo_pct
FROM fact_entregas f
JOIN dim_tiempo t ON f.tiempo_sk = t.tiempo_sk
JOIN dim_alerta a ON f.alerta_sk = a.alerta_sk
GROUP BY t.anio, t.mes, t.mes_nombre, a.severidad
ORDER BY t.anio, t.mes
"""

# ── Query 2: top 10 zonas por volumen de entregas fallidas ────────────
Q2 = """
SELECT z.zona,
       COUNT(*) AS total_entregas,
       SUM(f.fallido_flag) AS fallidas,
       ROUND(AVG(f.latencia_entrega_seg), 2) AS latencia_prom_seg
FROM fact_entregas f
JOIN dim_zona z ON f.zona_sk = z.zona_sk
GROUP BY z.zona
ORDER BY fallidas DESC
LIMIT 10
"""

# ── Query 3: tasa de fallo por severidad y zona de destino ───────────
Q3 = """
SELECT a.severidad, a.zona_destino,
       COUNT(*) AS total,
       SUM(f.fallido_flag) AS fallidas,
       ROUND(SUM(f.fallido_flag) * 100.0 / COUNT(*), 2) AS tasa_fallo_pct
FROM fact_entregas f
JOIN dim_alerta a ON f.alerta_sk = a.alerta_sk
GROUP BY a.severidad, a.zona_destino
ORDER BY tasa_fallo_pct DESC
"""

# ── Query 4: usuarios mas activos (mayor numero de entregas recibidas) ─
Q4 = """
SELECT u.usuario_id, u.nombre, u.zona,
       COUNT(*) AS entregas_recibidas,
       SUM(f.entregado_flag) AS entregadas
FROM fact_entregas f
JOIN dim_usuario u ON f.usuario_sk = u.usuario_sk
GROUP BY u.usuario_id, u.nombre, u.zona
ORDER BY entregas_recibidas DESC
LIMIT 10
"""

# ── Query 5: proporcion de tiempo online por zona y hora del dia ─────
Q5 = """
SELECT z.zona, p.hora_del_dia,
       COUNT(*) AS snapshots,
       SUM(p.online_flag) AS online,
       ROUND(SUM(p.online_flag) * 100.0 / COUNT(*), 1) AS pct_online
FROM fact_presencia_hora p
JOIN dim_zona z ON p.zona_sk = z.zona_sk
GROUP BY z.zona, p.hora_del_dia
ORDER BY z.zona, p.hora_del_dia
"""


def ejecutar_consultas(con):
    consultas = [
        ("Q1 — Volumen de entregas por mes y severidad", Q1),
        ("Q2 — Top zonas por entregas fallidas", Q2),
        ("Q3 — Tasa de fallo por severidad y zona", Q3),
        ("Q4 — Usuarios con mas entregas recibidas", Q4),
        ("Q5 — Proporcion de tiempo online por zona/hora", Q5),
    ]
    for nombre, query in consultas:
        inicio = time.time()
        resultado = con.execute(query).fetchdf()
        elapsed = (time.time() - inicio) * 1000
        print(f"\n[{nombre}] — {elapsed:.1f} ms — {len(resultado)} filas")
        print(resultado.to_string(index=False))


# ── Benchmark: misma agregacion en Cassandra (forzada) vs DuckDB ─────
def benchmark(con_duck):
    print("\n" + "=" * 70)
    print("BENCHMARK — Cassandra (OLTP, ALLOW FILTERING) vs DuckDB (columnar)")
    print("=" * 70)

    _, session = conectar("cass1")

    t0 = time.time()
    filas_cass = list(session.execute(
        "SELECT estado FROM estado_entrega ALLOW FILTERING"))
    t_cass = (time.time() - t0) * 1000
    total_cass = len(filas_cass)
    fallidas_cass = sum(1 for r in filas_cass if r.estado == "fallido")

    t0 = time.time()
    r = con_duck.execute(
        "SELECT COUNT(*) AS total, SUM(fallido_flag) AS fallidas FROM fact_entregas"
    ).fetchone()
    t_duck = (time.time() - t0) * 1000

    print(f"\nCassandra  (ALLOW FILTERING): {t_cass:>8.1f} ms  "
          f"-> {total_cass:,} filas, {fallidas_cass:,} fallidas")
    print(f"DuckDB     (Parquet Gold)   : {t_duck:>8.1f} ms  "
          f"-> {r[0]:,} filas, {r[1]:,} fallidas")
    if t_duck > 0:
        print(f"Factor de mejora            : {t_cass / t_duck:.1f}x")
    print(
        "\nNota: con pocos miles de filas la ventaja columnar puede ser "
        "pequeña o incluso desfavorable a DuckDB — el beneficio real se "
        "nota cuando se escanean muchas filas y pocas columnas sobre "
        "datasets grandes (ver seccion 5.2 de la guia del taller)."
    )


def main():
    con = duckdb.connect()
    registrar_vistas(con)
    ejecutar_consultas(con)
    benchmark(con)


if __name__ == "__main__":
    main()
