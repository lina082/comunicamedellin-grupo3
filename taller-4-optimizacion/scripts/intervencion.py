#!/usr/bin/env python3
"""
Taller 4 - Paso 2: Intervencion I-3 (ordenar el Parquet por la columna de
filtro + reducir row_group_size) sobre una copia optimizada de la Gold.

Diagnostico (ver evidencia/resumen_baseline.md): fact_entregas.parquet y
fact_presencia_hora.parquet tienen 1 solo row group cada uno (106,370 y
10,000 filas respectivamente). Con un solo row group, DuckDB no puede
aplicar data skipping sin importar que tan selectivo sea el WHERE: no
hay min/max por bloque que descartar, hay que leer y decodificar todo
el row group siempre. Esta es la causa raiz comun a las 3 queries.

Intervencion: reescribir ambos Parquet ordenados por su columna de
filtro mas frecuente, con row_group_size pequeno para generar varios
row groups con estadisticas de min/max utiles.
  - fact_entregas       -> ordenado por tiempo_sk (filtro de Q1, y
                            estado que correlaciona con tiempo de forma
                            debil, asi que tiempo_sk es la mejor apuesta
                            de orden fisico unico)
  - fact_presencia_hora -> ordenado por zona_sk (filtro de Q5)
"""
from pathlib import Path

import duckdb

BASE = Path(__file__).resolve().parent.parent
GOLD = BASE / "gold_taller3"
GOLD_OPT = BASE / "lakehouse_optimizado"
GOLD_OPT.mkdir(parents=True, exist_ok=True)

ROW_GROUP_SIZE = 5_000


def main():
    con = duckdb.connect()

    # fact_entregas: ordenar por tiempo_sk (columna de filtro de Q1)
    con.execute(f"""
        COPY (
            SELECT * FROM read_parquet('{GOLD}/fact_entregas.parquet')
            ORDER BY tiempo_sk
        ) TO '{GOLD_OPT}/fact_entregas.parquet'
        (FORMAT PARQUET, ROW_GROUP_SIZE {ROW_GROUP_SIZE})
    """)
    n = con.execute(f"SELECT COUNT(DISTINCT row_group_id) FROM parquet_metadata('{GOLD_OPT}/fact_entregas.parquet')").fetchone()[0]
    print(f"fact_entregas.parquet optimizado: {n} row groups (ordenado por tiempo_sk)")

    # fact_presencia_hora: ordenar por zona_sk (columna de filtro de Q5)
    con.execute(f"""
        COPY (
            SELECT * FROM read_parquet('{GOLD}/fact_presencia_hora.parquet')
            ORDER BY zona_sk
        ) TO '{GOLD_OPT}/fact_presencia_hora.parquet'
        (FORMAT PARQUET, ROW_GROUP_SIZE {ROW_GROUP_SIZE})
    """)
    n = con.execute(f"SELECT COUNT(DISTINCT row_group_id) FROM parquet_metadata('{GOLD_OPT}/fact_presencia_hora.parquet')").fetchone()[0]
    print(f"fact_presencia_hora.parquet optimizado: {n} row groups (ordenado por zona_sk)")

    # Las dimensiones no cambian: se copian tal cual para tener la Gold
    # optimizada completa y autocontenida.
    for dim in ["dim_tiempo", "dim_usuario", "dim_zona", "dim_alerta"]:
        con.execute(f"""
            COPY (SELECT * FROM read_parquet('{GOLD}/{dim}.parquet'))
            TO '{GOLD_OPT}/{dim}.parquet' (FORMAT PARQUET)
        """)
    print("Dimensiones copiadas sin cambios.")

    # Q3' filtra por f.estado = 'fallido' directamente sobre fact_entregas,
    # no por tiempo. Como el fallo esta disperso en el tiempo (no es un
    # atributo correlacionado con tiempo_sk), ordenar por tiempo_sk no
    # ayuda a Q3'. Se genera una segunda copia ordenada por estado para
    # evaluar esa intervencion de forma honesta y separada.
    con.execute(f"""
        COPY (
            SELECT * FROM read_parquet('{GOLD}/fact_entregas.parquet')
            ORDER BY estado
        ) TO '{GOLD_OPT}/fact_entregas_por_estado.parquet'
        (FORMAT PARQUET, ROW_GROUP_SIZE {ROW_GROUP_SIZE})
    """)
    n = con.execute(f"SELECT COUNT(DISTINCT row_group_id) FROM parquet_metadata('{GOLD_OPT}/fact_entregas_por_estado.parquet')").fetchone()[0]
    print(f"fact_entregas_por_estado.parquet optimizado: {n} row groups (ordenado por estado)")


if __name__ == "__main__":
    main()
