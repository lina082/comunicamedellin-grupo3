#!/usr/bin/env python3
"""
Taller 4 - Paso 2: medir el DESPUES sobre la Gold optimizada
(lakehouse_optimizado/) y guardar planes + tiempos para comparar
contra evidencia/resumen_baseline.md.
"""
import time
from pathlib import Path

import duckdb

BASE = Path(__file__).resolve().parent.parent
GOLD_OPT = BASE / "lakehouse_optimizado"
EVIDENCIA = BASE / "evidencia"
EVIDENCIA.mkdir(parents=True, exist_ok=True)


def registrar_vistas(con):
    con.execute(f"CREATE VIEW fact_entregas AS SELECT * FROM read_parquet('{GOLD_OPT}/fact_entregas.parquet')")
    con.execute(f"CREATE VIEW fact_entregas_por_estado AS SELECT * FROM read_parquet('{GOLD_OPT}/fact_entregas_por_estado.parquet')")
    con.execute(f"CREATE VIEW fact_presencia_hora AS SELECT * FROM read_parquet('{GOLD_OPT}/fact_presencia_hora.parquet')")
    con.execute(f"CREATE VIEW dim_tiempo AS SELECT * FROM read_parquet('{GOLD_OPT}/dim_tiempo.parquet')")
    con.execute(f"CREATE VIEW dim_usuario AS SELECT * FROM read_parquet('{GOLD_OPT}/dim_usuario.parquet')")
    con.execute(f"CREATE VIEW dim_zona AS SELECT * FROM read_parquet('{GOLD_OPT}/dim_zona.parquet')")
    con.execute(f"CREATE VIEW dim_alerta AS SELECT * FROM read_parquet('{GOLD_OPT}/dim_alerta.parquet')")


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

# Usa fact_entregas_por_estado: version ordenada por 'estado' (I-3 dirigida
# al predicado real de esta query, distinta de la de Q1 porque el orden
# fisico util depende de la columna que cada query filtra).
Q3 = """
SELECT a.severidad, a.zona_destino,
       COUNT(*) AS fallidas,
       ROUND(AVG(f.latencia_entrega_seg), 2) AS latencia_prom_seg
FROM fact_entregas_por_estado f
JOIN dim_alerta a ON f.alerta_sk = a.alerta_sk
WHERE f.estado = 'fallido'
GROUP BY a.severidad, a.zona_destino
ORDER BY fallidas DESC
"""

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
    ("Q1", "Volumen diario de entregas por severidad - ultima semana", Q1),
    ("Q3", "Entregas fallidas por severidad y zona de destino", Q3),
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


def main():
    con = duckdb.connect()
    registrar_vistas(con)

    resumen_lines = ["# Medicion DESPUES de la intervencion - Taller 4\n"]

    for codigo, nombre, query in CONSULTAS:
        print(f"\n{'='*70}\n{codigo} — {nombre}\n{'='*70}")

        plan = con.execute(f"EXPLAIN ANALYZE {query}").fetchall()
        plan_texto = "\n".join(row[1] for row in plan) if plan and len(plan[0]) > 1 else "\n".join(str(row) for row in plan)
        plan_path = EVIDENCIA / f"{codigo}_plan_despues.txt"
        plan_path.write_text(plan_texto)

        mediana, tiempos = medir_mediana(con, query, codigo)

        resumen_lines.append(f"## {codigo} — {nombre}\n")
        resumen_lines.append(f"- Mediana (5 corridas, con warm-up de 3): **{mediana:.2f} ms**")
        resumen_lines.append(f"- Min: {tiempos[0]:.2f} ms | Max: {tiempos[-1]:.2f} ms")
        resumen_lines.append(f"- Plan completo: `{plan_path.name}`\n")

    (EVIDENCIA / "resumen_despues.md").write_text("\n".join(resumen_lines))
    print(f"\nEvidencia guardada en {EVIDENCIA}")


if __name__ == "__main__":
    main()
