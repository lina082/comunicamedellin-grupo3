#!/usr/bin/env python3
"""
Carga 10.000 usuarios simulados actualizando presencia de forma
constante y simultanea (requisito literal del reto).

Sec. 3.3 - la clave de particion (usuario_id) distribuye la carga
           uniformemente entre los 3 nodos por hash del token.
"""
import sys, time, random, datetime as dt
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from _common import conectar, titulo, color, ZONAS, DISPOSITIVOS, requiere_driver  # noqa

TOTAL = 10_000
HILOS = 16


def main():
    requiere_driver()
    from cassandra import ConsistencyLevel
    from cassandra.query import BatchStatement

    titulo(f"Cargando {TOTAL:,} usuarios simulados")
    print(color("  Escrituras concurrentes con consistencia ONE (AP)", "dim"))

    cluster, s = conectar("cass1")
    prep = s.prepare(
        "UPDATE presencia SET estado=?, last_seen=?, zona=?, "
        "dispositivo=?, escrito_por=? WHERE usuario_id=?")
    prep.consistency_level = ConsistencyLevel.ONE

    t0 = time.perf_counter()
    futuros, hechos = [], 0

    for i in range(TOTAL):
        ahora = dt.datetime.now(dt.timezone.utc)
        futuros.append(s.execute_async(prep, (
            random.choices(["online", "offline"], weights=[7, 3])[0],
            ahora,
            ZONAS[i % len(ZONAS)],
            random.choice(DISPOSITIVOS),
            "cass1",
            f"usuario:{i:06d}",
        )))
        if len(futuros) >= 500:
            for f in futuros:
                f.result()
            hechos += len(futuros)
            futuros.clear()
            pct = 100 * hechos / TOTAL
            print(f"\r  {hechos:>6,}/{TOTAL:,}  [{'█' * int(pct/4):<25}] {pct:5.1f}%",
                  end="", flush=True)

    for f in futuros:
        f.result()
    hechos += len(futuros)

    dur = time.perf_counter() - t0
    print(f"\r  {hechos:>6,}/{TOTAL:,}  [{'█'*25}] 100.0%")
    print()
    print(f"  Tiempo total : {dur:.1f} s")
    print(f"  Throughput   : {color(f'{TOTAL/dur:,.0f} escrituras/s', 'verde')}")
    print()
    print(color("  Verifica la distribucion con:  make status", "dim"))
    cluster.shutdown()


if __name__ == "__main__":
    main()
