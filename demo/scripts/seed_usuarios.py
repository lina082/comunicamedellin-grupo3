#!/usr/bin/env python3
"""
Carga 10.000 suscriptores simulados con datos colombianos realistas.

Requisito literal del reto:
  "Generar un script que simule al menos 10.000 usuarios actualizando
   su estado ('en linea' / timestamp de ultima conexion) de forma
   constante y simultanea."

Los datos vienen de Faker con locale es_CO (ver _datos.py): cedulas,
nombres, telefonos y comunas reales de Medellin. En la exposicion se
nota la diferencia contra 'usuario:000123'.

Sec. 3.3 - la clave de particion (usuario_id) distribuye la carga
           uniformemente entre los 3 nodos por hash del token.
"""
import sys, time, datetime as dt
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from _common import conectar, titulo, color, requiere_driver  # noqa: E402
from _datos import sembrar, usuario  # noqa: E402

TOTAL = 10_000
LOTE = 500


def main():
    requiere_driver()
    from cassandra import ConsistencyLevel

    titulo(f"Cargando {TOTAL:,} suscriptores de ComunicaMedellin")
    print(color("  Datos generados con Faker (es_CO): cedulas, nombres,", "dim"))
    print(color("  telefonos y comunas reales de Medellin.", "dim"))
    print(color("  Escrituras concurrentes con consistencia ONE (AP).", "dim"))

    sembrar()  # semilla fija -> la demo es reproducible
    cluster, s = conectar("cass1")
    prep = s.prepare("""
        UPDATE presencia
        SET nombre=?, telefono=?, estado=?, last_seen=?, zona=?,
            comuna=?, lat=?, lon=?, dispositivo=?, escrito_por=?
        WHERE usuario_id=?""")
    prep.consistency_level = ConsistencyLevel.ONE

    t0 = time.perf_counter()
    futuros, hechos = [], 0

    for i in range(TOTAL):
        u = usuario(i)
        ahora = dt.datetime.now(dt.timezone.utc)
        futuros.append(s.execute_async(prep, (
            u["nombre"], u["telefono"], u["estado"], ahora, u["zona"],
            u["comuna"], u["lat"], u["lon"], u["dispositivo"], "cass1",
            u["usuario_id"],
        )))
        if len(futuros) >= LOTE:
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

    # Muestra para que se vea que los datos son creibles
    print()
    print(color("  Muestra de lo cargado:", "dim"))
    filas = s.execute(
        "SELECT usuario_id, nombre, zona, comuna, estado FROM presencia LIMIT 4")
    for f in filas:
        est = color(f.estado, "verde" if f.estado == "online" else "dim")
        print(f"    {f.usuario_id}  {(f.nombre or '')[:24]:<24} "
              f"{f.zona:<7} {(f.comuna or ''):<16} {est}")

    print()
    print(color("  Verifica la distribucion con:  make status", "dim"))
    cluster.shutdown()


if __name__ == "__main__":
    main()
