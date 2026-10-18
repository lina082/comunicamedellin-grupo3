#!/usr/bin/env python3
"""
Taller 3 - Reporte de negocio sobre Gold.

Gold no es solo "las tablas correctas": es el punto donde los datos se
convierten en valor para la organizacion. Este script responde eso con
el framework North Star / metricas de apoyo / vanity metrics.

  NORTH STAR METRIC - la UNICA metrica que resume si el producto
  cumple su promesa central. Debe ser accionable (el equipo puede
  moverla) y predictiva (si sube, el negocio mejora de verdad). Para
  ComunicaMedellin la promesa es "la alerta de emergencia llega a
  tiempo" -- no "se publica", sino que LLEGA y llega RAPIDO. Por eso:

      % de entregas con latencia_entrega_seg <= SLA_SEGUNDOS

  Entregar tarde una alerta de riesgo publico equivale a no entregarla.
  Esta metrica no se puede "inflar" sin mejorar de verdad el sistema
  (al contrario de un vanity metric), y cualquier equipo (infra,
  modelado de datos, Kafka, Cassandra) tiene una palanca real sobre
  ella.

  METRICAS DE APOYO - no resumen el negocio por si solas, pero
  explican POR QUE sube o baja la North Star. Son el "diagnostico".

  VANITY METRICS - numeros grandes que se ven bien en una diapositiva
  pero no dicen si el servicio funciona. Utiles para contexto de
  escala, nunca para decidir si el producto esta sano.
"""
import sys
from pathlib import Path

import duckdb

BASE = Path(__file__).resolve().parent.parent
GOLD = BASE / "lakehouse" / "gold"
SILVER = BASE / "lakehouse" / "silver"

SLA_SEGUNDOS = 90  # umbral de "a tiempo": p85-p90 real del dataset


def registrar_vistas(con):
    con.execute(f"CREATE VIEW fact_entregas AS SELECT * FROM read_parquet('{GOLD}/fact_entregas.parquet')")
    con.execute(f"CREATE VIEW fact_presencia_hora AS SELECT * FROM read_parquet('{GOLD}/fact_presencia_hora.parquet')")
    con.execute(f"CREATE VIEW dim_tiempo AS SELECT * FROM read_parquet('{GOLD}/dim_tiempo.parquet')")
    con.execute(f"CREATE VIEW dim_usuario AS SELECT * FROM read_parquet('{GOLD}/dim_usuario.parquet')")
    con.execute(f"CREATE VIEW dim_zona AS SELECT * FROM read_parquet('{GOLD}/dim_zona.parquet')")
    con.execute(f"CREATE VIEW dim_alerta AS SELECT * FROM read_parquet('{GOLD}/dim_alerta.parquet')")
    con.execute(f"CREATE VIEW presencia AS SELECT * FROM read_parquet('{SILVER}/presencia_silver.parquet')")


def linea(ancho=78, char="="):
    return char * ancho


def north_star(con):
    print(linea())
    print("NORTH STAR METRIC")
    print(linea())
    print(f"Pregunta de negocio: ¿qué proporción de alertas llega a tiempo\n"
          f"(≤ {SLA_SEGUNDOS}s desde el envío) para que el ciudadano pueda actuar?\n")

    r = con.execute(f"""
        SELECT
            COUNT(*) AS total,
            SUM(CASE WHEN latencia_entrega_seg <= {SLA_SEGUNDOS} THEN 1 ELSE 0 END) AS a_tiempo
        FROM fact_entregas
        WHERE latencia_entrega_seg IS NOT NULL
    """).fetchone()
    total, a_tiempo = r
    pct = (a_tiempo / total * 100) if total else 0.0

    print(f"  % de entregas a tiempo (≤ {SLA_SEGUNDOS}s)  {pct:>6.1f}%")
    print(f"  entregas medidas                    {total:>8,}")
    print(f"  entregas a tiempo                   {a_tiempo:>8,}")
    print(
        "\n  Por qué ESTA y no otra: es la única métrica que colapsa todo el "
        "sistema (fan-out, Cassandra, red) en un solo número que un no-técnico "
        "entiende, es accionable (bajar p99 de Cassandra, mejorar el fan-out, "
        "reducir timeouts) y es imposible de inflar sin mejorar el servicio de "
        "verdad — a diferencia de 'alertas publicadas', que sube aunque nadie "
        "las reciba."
    )
    return pct


def metricas_apoyo(con):
    print("\n" + linea())
    print("MÉTRICAS DE APOYO — explican el porqué de la North Star")
    print(linea())

    print("\n[1] Tasa de fallo por severidad")
    print("    Pregunta: ¿el sistema falla más justo en las alertas que más importan?")
    df1 = con.execute("""
        SELECT a.severidad,
               COUNT(*) AS total,
               SUM(f.fallido_flag) AS fallidas,
               ROUND(SUM(f.fallido_flag) * 100.0 / COUNT(*), 2) AS tasa_fallo_pct
        FROM fact_entregas f
        JOIN dim_alerta a ON f.alerta_sk = a.alerta_sk
        GROUP BY a.severidad
        ORDER BY tasa_fallo_pct DESC
    """).fetchdf()
    print(df1.to_string(index=False))

    print("\n[2] Latencia p50 / p90 por zona")
    print("    Pregunta: ¿hay una zona de la ciudad sistemáticamente más lenta?")
    df2 = con.execute("""
        SELECT z.zona,
               ROUND(MEDIAN(f.latencia_entrega_seg), 1) AS p50_seg,
               ROUND(APPROX_QUANTILE(f.latencia_entrega_seg, 0.9), 1) AS p90_seg
        FROM fact_entregas f
        JOIN dim_zona z ON f.zona_sk = z.zona_sk
        WHERE f.latencia_entrega_seg IS NOT NULL
        GROUP BY z.zona
        ORDER BY p90_seg DESC
    """).fetchdf()
    print(df2.to_string(index=False))

    print("\n[3] % de usuarios online al momento de recibir la alerta")
    print("    Pregunta: ¿entregamos rápido a gente que de todas formas no iba a verla?")
    print("    Limitación: 'presencia' es un snapshot actual, no el estado exacto en el")
    print("    instante de cada envío histórico (ver docs/modelo_dimensional.md,")
    print("    Pregunta 2). Se usa como proxy, no como dato exacto por entrega.")
    df3 = con.execute("""
        SELECT z.zona,
               COUNT(*) AS entregas,
               SUM(CASE WHEN p.estado = 'online' THEN 1 ELSE 0 END) AS destinatario_online_hoy,
               ROUND(SUM(CASE WHEN p.estado = 'online' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS pct_online
        FROM fact_entregas f
        JOIN dim_usuario u ON f.usuario_sk = u.usuario_sk
        JOIN dim_zona z ON f.zona_sk = z.zona_sk
        JOIN presencia p ON p.usuario_id = u.usuario_id
        GROUP BY z.zona
        ORDER BY pct_online DESC
    """).fetchdf()
    print(df3.to_string(index=False))


def vanity_metrics(con):
    print("\n" + linea())
    print("VANITY METRICS — se ven bien, no dicen si el servicio funciona")
    print(linea())

    r = con.execute("""
        SELECT
            (SELECT COUNT(*) FROM dim_alerta) AS alertas_publicadas,
            (SELECT COUNT(*) FROM dim_usuario) AS usuarios_registrados,
            (SELECT COUNT(*) FROM fact_entregas) AS entregas_historicas
    """).fetchone()
    alertas, usuarios, entregas = r
    print(f"\n  Alertas publicadas (histórico)      {alertas:>8,}")
    print(f"  Usuarios registrados                {usuarios:>8,}")
    print(f"  Entregas históricas totales          {entregas:>8,}")
    print(
        "\n  Por qué son vanity: los tres SIEMPRE suben con el tiempo, nunca "
        "bajan, y subir no implica que el servicio mejoró — solo que pasó más "
        "tiempo o se publicaron más alertas. Sirven para dar contexto de "
        "escala en una diapositiva ('operamos sobre 10.000 usuarios'), nunca "
        "para decidir si el sistema está sano. Un equipo que optimizara para "
        "'más alertas publicadas' empeoraría la experiencia real sin que la "
        "métrica lo reflejara — el riesgo clásico de medir lo fácil en vez de "
        "lo importante."
    )


def main():
    con = duckdb.connect()
    registrar_vistas(con)
    north_star(con)
    metricas_apoyo(con)
    vanity_metrics(con)
    print("\n" + linea())
    print(f"SLA de la North Star: ≤ {SLA_SEGUNDOS}s (editable en SLA_SEGUNDOS, "
          f"scripts/reporte_negocio.py)")
    print(linea())


if __name__ == "__main__":
    main()
