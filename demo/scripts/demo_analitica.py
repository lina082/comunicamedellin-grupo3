#!/usr/bin/env python3
"""
MODULO 2 - La misma consulta analitica en los dos motores
==========================================================
Cumple el requisito del reto practico:

  "Consulta analitica real ejecutada primero en la base 'correcta'
   y luego forzada en la operacional, mostrando en vivo por que
   falla o es costosa."

LA PREGUNTA DE NEGOCIO
----------------------
  "Cuantos usuarios unicos hay en linea por zona?"

Es una agregacion: GROUP BY sobre una columna que no es clave de
particion. Se ejecuta en los dos motores y se mide la diferencia.

  ClickHouse  columnar (Modulo 1, Sec. 2.2, 2.4) -> lee solo 2
              columnas, agrega con dict encoding. Es su caso de uso.

  Cassandra   wide-column orientada a lectura por clave. La misma
              consulta necesita ALLOW FILTERING: el coordinador
              pregunta a TODOS los nodos y descarta filas en memoria.

LO QUE SE DEMUESTRA
-------------------
  1. Cassandra RECHAZA la consulta sin ALLOW FILTERING (error real).
  2. Con ALLOW FILTERING responde, pero el costo crece con el
     tamano del cluster, no con el tamano del resultado.
  3. El GROUP BY directamente NO EXISTE fuera de la clave primaria.
  4. ClickHouse resuelve lo mismo leyendo solo las columnas usadas.

Teoria aplicada:
  Modulo 1, Sec. 2.2 - conflicto OLTP vs OLAP: un motor optimizado
                       para escribir por clave no puede agregar.
  Modulo 1, Sec. 2.4 - caso Mercado Libre (columnar analitico).
"""
import sys, time, subprocess, statistics as st
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from _common import conectar, titulo, seccion, color, requiere_driver  # noqa

REPETICIONES = 5


def clickhouse(sql):
    """Ejecuta una consulta en ClickHouse y devuelve (salida, ms)."""
    t0 = time.perf_counter()
    r = subprocess.run(
        ["docker", "exec", "clickhouse", "clickhouse-client", "--query", sql],
        capture_output=True, text=True)
    ms = (time.perf_counter() - t0) * 1000
    return r.stdout.strip(), ms, r.returncode == 0


def hay_clickhouse():
    r = subprocess.run(["docker", "ps", "--filter", "name=clickhouse",
                        "--format", "{{.Names}}"], capture_output=True, text=True)
    return "clickhouse" in r.stdout


def contar_filas_cassandra(s):
    try:
        return s.execute("SELECT count(*) AS n FROM presencia").one().n
    except Exception:
        return 0


def main():
    requiere_driver()
    titulo("MODULO 2 — La misma consulta en el motor correcto y en el equivocado")
    print("  Pregunta de negocio:")
    print(color("    ¿Cuantos usuarios unicos hay en linea, por zona?", "bold"))
    print()
    print("  Es una agregacion sobre una columna que NO es clave de")
    print("  particion. Se ejecuta en los dos motores y se compara.")

    cluster, s = conectar("cass1")
    try:
        n_filas = contar_filas_cassandra(s)
        print(f"\n  Filas en Cassandra (tabla presencia): {color(f'{n_filas:,}', 'cyan')}")
        if n_filas < 1000:
            print(color("\n  Hay pocos datos. Ejecuta 'make seed' antes para que", "amarillo"))
            print(color("  la comparacion sea significativa.", "amarillo"))

        # ---------------------------------------------------------
        seccion("1. CASSANDRA — la consulta natural, tal cual se escribiria en SQL")
        print(color("    SELECT zona, count(*) FROM presencia GROUP BY zona;", "cyan"))
        print()
        try:
            s.execute("SELECT zona, count(*) FROM presencia GROUP BY zona")
            print(color("    (inesperado: no fallo)", "amarillo"))
        except Exception as e:
            print(f"    {color('RECHAZADA', 'rojo')} — {type(e).__name__}")
            msg = str(e).split("message=")[-1].strip('"\' ')[:200]
            print(color(f"    {msg}", "dim"))
            print()
            print("    Cassandra solo agrupa por columnas de la CLAVE PRIMARIA.")
            print("    'zona' no participa en el hash, asi que las filas de una")
            print("    misma zona estan repartidas por todo el anillo.")

        # ---------------------------------------------------------
        seccion("2. CASSANDRA — forzada con ALLOW FILTERING")
        print("    La valvula de escape: funciona, pero obliga al coordinador")
        print("    a preguntar a TODOS los nodos y descartar filas en memoria.")
        print()
        zonas = ["norte", "centro", "sur"]
        lat_cass, total_cass = [], 0
        for intento in range(REPETICIONES):
            t0 = time.perf_counter()
            conteo = {}
            for z in zonas:
                filas = s.execute(
                    "SELECT usuario_id FROM presencia WHERE zona=%s AND "
                    "estado='online' ALLOW FILTERING", (z,))
                conteo[z] = sum(1 for _ in filas)
            ms = (time.perf_counter() - t0) * 1000
            lat_cass.append(ms)
            total_cass = sum(conteo.values())
            if intento == 0:
                for z, n in conteo.items():
                    print(f"      zona {z:<8} {n:>7,} usuarios en linea")
        p50_cass = st.median(lat_cass)
        print()
        print(f"    Latencia (mediana de {REPETICIONES}): "
              f"{color(f'{p50_cass:,.1f} ms', 'rojo')}")
        print(color("    Nota: son 3 consultas, una por zona. El GROUP BY lo hace", "dim"))
        print(color("    el cliente en memoria, no el motor.", "dim"))

        # ---------------------------------------------------------
        seccion("3. CLICKHOUSE — el motor correcto para esta pregunta")
        if not hay_clickhouse():
            print(color("    ClickHouse no esta corriendo. Levantalo con:", "amarillo"))
            print("      docker compose up -d clickhouse && make ch-seed")
            print()
            print("    Sin el, la comparacion queda incompleta.")
            p50_ch = None
        else:
            sql = ("SELECT zona, uniqExact(usuario_id) AS usuarios "
                   "FROM comunica.presencia_eventos "
                   "WHERE estado='online' GROUP BY zona ORDER BY usuarios DESC")
            print(color("    " + sql.replace("FROM", "\n      FROM")
                        .replace("WHERE", "\n      WHERE"), "cyan"))
            print()
            salida, _, ok = clickhouse(sql)   # calentamiento
            if not ok or not salida:
                print(color("    Sin datos en ClickHouse. Ejecuta 'make ch-seed'.", "amarillo"))
                p50_ch = None
            else:
                lat_ch = []
                for _ in range(REPETICIONES):
                    salida, ms, _ = clickhouse(sql)
                    lat_ch.append(ms)
                for linea in salida.splitlines():
                    partes = linea.split("\t")
                    if len(partes) == 2:
                        print(f"      zona {partes[0]:<8} {int(partes[1]):>7,} usuarios unicos")
                p50_ch = st.median(lat_ch)
                print()
                print(f"    Latencia (mediana de {REPETICIONES}): "
                      f"{color(f'{p50_ch:,.1f} ms', 'verde')}")

                # Cuanto dato tuvo que leer cada motor
                bytes_leidos, _, _ = clickhouse(
                    "SELECT formatReadableSize(sum(bytes_on_disk)) FROM system.parts "
                    "WHERE database='comunica' AND table='presencia_eventos' AND active")
                if bytes_leidos:
                    print(color(f"    Tamano de la tabla en disco: {bytes_leidos}", "dim"))
                    print(color("    Al ser columnar solo leyo 'zona' y 'usuario_id',", "dim"))
                    print(color("    no las 6 columnas de cada fila.", "dim"))

        # ---------------------------------------------------------
        seccion("RESULTADO")
        n_ch = 0
        if p50_ch:
            salida_n, _, _ = clickhouse(
                "SELECT count() FROM comunica.presencia_eventos")
            try:
                n_ch = int(salida_n)
            except ValueError:
                n_ch = 0

        print(f"    {'Motor':<30}{'filas':>12}{'latencia':>12}")
        print(f"    {'Cassandra (ALLOW FILTERING)':<30}{n_filas:>12,}"
              f"{p50_cass:>9,.1f} ms")
        if p50_ch:
            print(f"    {'ClickHouse (columnar)':<30}{n_ch:>12,}"
                  f"{p50_ch:>9,.1f} ms")
        print()
        print(color("    OJO AL COMPARAR: no son la misma cantidad de datos.", "amarillo"))
        print("    ClickHouse esta agregando mucho mas volumen en menos tiempo,")
        print("    asi que el cociente directo SUBESTIMA la diferencia real.")
        if p50_ch and n_ch and n_filas:
            us_cass = p50_cass * 1000 / max(n_filas, 1)
            us_ch = p50_ch * 1000 / max(n_ch, 1)
            print()
            print(f"    Normalizado por fila procesada:")
            print(f"      Cassandra   {us_cass:>8.2f} us/fila")
            print(f"      ClickHouse  {color(f'{us_ch:>8.2f} us/fila', 'verde')}")
            if us_ch:
                print(f"      {color(f'{us_cass/us_ch:.0f}x menos costo por fila', 'verde')}")
        print()
        print("  Por que la diferencia crece con el tamano del sistema:")
        print()
        print("    Cassandra  el costo depende del numero de NODOS y de filas")
        print("               almacenadas, no del tamano del resultado. Con")
        print("               100 millones de filas el coordinador debe")
        print("               recorrerlas todas para devolver 3 numeros.")
        print()
        print("    ClickHouse el costo depende de las COLUMNAS consultadas.")
        print("               Lee 2 de 6 columnas, comprimidas con dict")
        print("               encoding porque 'zona' tiene 3 valores distintos.")
        print()
        print(color("  Conclusion (Modulo 1, Sec. 2.2): no es que un motor sea", "bold"))
        print(color("  mejor que el otro. Es que resuelven preguntas distintas,", "bold"))
        print(color("  y por eso la arquitectura tiene los dos.", "bold"))
    finally:
        cluster.shutdown()


if __name__ == "__main__":
    main()
