#!/usr/bin/env python3
"""
Taller 4 - Paso 1: Diagnostico.
Ejecuta EXPLAIN ANALYZE y mide el tiempo baseline (warm-up + mediana de 5)
para las 3 consultas elegidas del Taller 3: Q1, Q3, Q5.
Guarda todo en evidencia/ para el informe.
"""
import time
from pathlib import Path

import duckdb

BASE = Path(__file__).resolve().parent.parent
GOLD = BASE / "gold_taller3"
EVIDENCIA = BASE / "evidencia"
EVIDENCIA.mkdir(parents=True, exist_ok=True)


def registrar_vistas(con):
    con.execute(f"CREATE VIEW fact_entregas AS SELECT * FROM read_parquet('{GOLD}/fact_entregas.parquet')")
    con.execute(f"CREATE VIEW fact_presencia_hora AS SELECT * FROM read_parquet('{GOLD}/fact_presencia_hora.parquet')")
    con.execute(f"CREATE VIEW dim_tiempo AS SELECT * FROM read_parquet('{GOLD}/dim_tiempo.parquet')")
    con.execute(f"CREATE VIEW dim_usuario AS SELECT * FROM read_parquet('{GOLD}/dim_usuario.parquet')")
    con.execute(f"CREATE VIEW dim_zona AS SELECT * FROM read_parquet('{GOLD}/dim_zona.parquet')")
    con.execute(f"CREATE VIEW dim_alerta AS SELECT * FROM read_parquet('{GOLD}/dim_alerta.parquet')")


#  Las 3 queries se adaptan del Taller 3 agregandoles un WHERE realista.
#  Las originales (Q1/Q3/Q5 sin filtro) son agregaciones globales: ya
#  escanean toda la tabla porque no hay ningun predicado que reducir, asi
#  que no hay "acceso evitable" que diagnosticar. Un dashboard real nunca
#  pide "todo el historico" sin filtro -- pide "la ultima semana" o
#  "las alertas criticas". Esa es la version que se diagnostica aqui.

# ── Q1': volumen de entregas por dia, ultima semana ───────────────────
Q1 = """
SELECT t.fecha, a.severidad,
       COUNT(*) AS total_entregas,
       SUM(f.entregado_flag) AS entregadas,
       SUM(f.fallido_flag) AS fallidas,
       ROUND(SUM(f.fallido_flag) * 100.0 / COUNT(*), 2) AS tasa_fallo_pct
FROM fact_entregas f
JOIN dim_tiempo t ON f.tiempo_sk = t.tiempo_sk
JOIN dim_alerta a ON f.alerta_sk = a.alerta_sk
WHERE t.fecha >= '2026-10-12'
GROUP BY t.fecha, a.severidad
ORDER BY t.fecha
"""

# ── Q3': entregas fallidas (estado = 'fallido') por severidad/zona ────
Q3 = """
SELECT a.severidad, a.zona_destino,
       COUNT(*) AS fallidas,
       ROUND(AVG(f.latencia_entrega_seg), 2) AS latencia_prom_seg
FROM fact_entregas f
JOIN dim_alerta a ON f.alerta_sk = a.alerta_sk
WHERE f.estado = 'fallido'
GROUP BY a.severidad, a.zona_destino
ORDER BY fallidas DESC
"""

# ── Q5': presencia online por hora, zona Centro ───────────────────────
Q5 = """
SELECT z.zona, p.hora_del_dia,
       COUNT(*) AS snapshots,
       SUM(p.online_flag) AS online,
       ROUND(SUM(p.online_flag) * 100.0 / COUNT(*), 1) AS pct_online
FROM fact_presencia_hora p
JOIN dim_zona z ON p.zona_sk = z.zona_sk
WHERE z.zona = 'Centro'
GROUP BY z.zona, p.hora_del_dia
ORDER BY p.hora_del_dia
"""

CONSULTAS = [
    ("Q1", "Volumen diario de entregas por severidad - ultima semana (fecha >= 2026-10-12)", Q1),
    ("Q3", "Entregas fallidas por severidad y zona de destino (estado = 'fallido')", Q3),
    ("Q5", "Presencia online por hora en la zona Centro", Q5),
]


def medir_mediana(con, query, nombre):
    for _ in range(3):
        con.execute(query).fetchall()
    tiempos = []
    for _ in range(5):
        t0 = time.perf_counter()
        con.execute(query).fetchall()
        tiempos.append((time.perf_counter() - t0) * 1000)
    tiempos.sort()
    mediana = tiempos[2]
    print(f"[{nombre}] mediana={mediana:.2f} ms  min={tiempos[0]:.2f} ms  max={tiempos[-1]:.2f} ms  (5 corridas: {[round(t,2) for t in tiempos]})")
    return mediana, tiempos


def row_groups_info(con):
    GOLD_STR = str(GOLD)
    info = {}
    for t in ["fact_entregas", "fact_presencia_hora"]:
        n = con.execute(
            f"SELECT COUNT(DISTINCT row_group_id) FROM parquet_metadata('{GOLD_STR}/{t}.parquet')"
        ).fetchone()[0]
        filas = con.execute(f"SELECT COUNT(*) FROM read_parquet('{GOLD_STR}/{t}.parquet')").fetchone()[0]
        info[t] = (n, filas)
    return info


def main():
    con = duckdb.connect()
    registrar_vistas(con)

    rg_info = row_groups_info(con)
    print("\n=== Row groups por archivo Gold (clave para data skipping) ===")
    for t, (n, filas) in rg_info.items():
        print(f"  {t}: {n} row group(s) para {filas:,} filas")

    resumen_lines = ["# Diagnostico baseline - Taller 4\n"]
    resumen_lines.append("## Row groups por archivo Gold\n")
    for t, (n, filas) in rg_info.items():
        resumen_lines.append(f"- `{t}.parquet`: **{n} row group(s)** para {filas:,} filas")
    resumen_lines.append("")

    for codigo, nombre, query in CONSULTAS:
        print(f"\n{'='*70}\n{codigo} — {nombre}\n{'='*70}")

        plan = con.execute(f"EXPLAIN ANALYZE {query}").fetchall()
        plan_texto = "\n".join(row[1] for row in plan) if plan and len(plan[0]) > 1 else "\n".join(str(row) for row in plan)
        plan_path = EVIDENCIA / f"{codigo}_plan_baseline.txt"
        plan_path.write_text(plan_texto)
        print(plan_texto)

        mediana, tiempos = medir_mediana(con, query, codigo)

        resumen_lines.append(f"## {codigo} — {nombre}\n")
        resumen_lines.append(f"- Mediana (5 corridas, con warm-up de 3): **{mediana:.2f} ms**")
        resumen_lines.append(f"- Min: {tiempos[0]:.2f} ms | Max: {tiempos[-1]:.2f} ms")
        resumen_lines.append(f"- Plan completo: `{plan_path.name}`\n")

    (EVIDENCIA / "resumen_baseline.md").write_text("\n".join(resumen_lines))
    print(f"\nEvidencia guardada en {EVIDENCIA}")


if __name__ == "__main__":
    main()
