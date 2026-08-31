#!/usr/bin/env python3
"""
EXTRA - Replicacion multi-datacenter y niveles LOCAL_*
=======================================================
Ningun equipo del taller esta obligado a mostrar esto.

Simula la topologia real que describe el caso AeroMedellin
(Equipo 6): dos centros de datos, Medellin como principal y
Miami como respaldo.

Sec. 3.3 - la replicacion es consciente de la topologia fisica:
           NetworkTopologyStrategy coloca replicas por datacenter,
           no solo por posicion en el anillo.
Sec. 1.2 - LOCAL_QUORUM da consistencia dentro de una ciudad sin
           pagar el viaje a la otra. Es el nivel mas usado en
           produccion multi-DC.
"""
import statistics as st
import sys, time, datetime as dt
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from _common import conectar, titulo, seccion, color, requiere_driver  # noqa

RONDAS = 40
USUARIO = "usuario:multidc:001"


def medir(sesion, consulta, nivel, params):
    from cassandra import ConsistencyLevel  # noqa
    consulta.consistency_level = nivel
    t0 = time.perf_counter()
    fila = sesion.execute(consulta, params).one()
    return (time.perf_counter() - t0) * 1000, fila


def probar(nombre, nivel, s_esc, s_med, s_mia, q_esc, q_lec, explica):
    """Escribe desde Medellin y lee desde ambas ciudades."""
    lat_esc, lat_med, lat_mia = [], [], []
    fallos = 0

    for _ in range(RONDAS):
        ahora = dt.datetime.now(dt.timezone.utc)
        try:
            l, _ = medir(s_esc, q_esc, nivel,
                         ("online", ahora, "centro", "android", "cass1", USUARIO))
            lat_esc.append(l)
        except Exception:
            fallos += 1
            continue
        try:
            l1, _ = medir(s_med, q_lec, nivel, (USUARIO,))
            lat_med.append(l1)
        except Exception:
            pass
        try:
            l3, _ = medir(s_mia, q_lec, nivel, (USUARIO,))
            lat_mia.append(l3)
        except Exception:
            pass

    def p50(xs):
        return st.median(xs) if xs else float("nan")

    print(f"  {color(nombre, 'bold')}")
    print(f"    {explica}")
    if fallos:
        print(f"    {color(f'{fallos}/{RONDAS} escrituras fallaron', 'rojo')}")
    print(f"    {'escritura desde Medellin:':<32} {p50(lat_esc):>7.2f} ms")
    print(f"    {'lectura desde Medellin:':<32} {p50(lat_med):>7.2f} ms")
    print(f"    {'lectura desde Miami:':<32} {p50(lat_mia):>7.2f} ms")
    print()
    return {"nivel": nombre, "esc": p50(lat_esc),
            "med": p50(lat_med), "mia": p50(lat_mia), "fallos": fallos}


def main():
    requiere_driver()
    from cassandra import ConsistencyLevel
    from cassandra.query import SimpleStatement

    titulo("EXTRA — Multi-datacenter: Medellin y Miami")
    print("  Topologia:  cass1 + cass2 -> medellin")
    print("              cass3         -> miami")
    print("  Replicacion: NetworkTopologyStrategy {medellin:2, miami:1}")
    print()
    print(color("  Con 250 ms de retardo simulado entre ciudades,", "dim"))
    print(color("  cada nivel paga un precio distinto.", "dim"))

    cl_e, s_esc = conectar("cass1")   # escritura desde Medellin
    cl_m, s_med = conectar("cass2")   # lectura desde Medellin
    cl_i, s_mia = conectar("cass3")   # lectura desde Miami

    q_esc = SimpleStatement(
        "UPDATE presencia SET estado=%s, last_seen=%s, zona=%s, "
        "dispositivo=%s, escrito_por=%s WHERE usuario_id=%s")
    q_lec = SimpleStatement(
        "SELECT usuario_id, estado, last_seen FROM presencia WHERE usuario_id=%s")

    seccion("COMPARATIVA DE NIVELES")
    r = []
    r.append(probar("ONE", ConsistencyLevel.ONE, s_esc, s_med, s_mia,
                    q_esc, q_lec,
                    "responde la primera replica, este donde este"))
    r.append(probar("LOCAL_QUORUM", ConsistencyLevel.LOCAL_QUORUM,
                    s_esc, s_med, s_mia, q_esc, q_lec,
                    "mayoria DENTRO de la ciudad del coordinador"))
    r.append(probar("QUORUM", ConsistencyLevel.QUORUM, s_esc, s_med, s_mia,
                    q_esc, q_lec,
                    "mayoria global: cruza el oceano"))
    r.append(probar("EACH_QUORUM", ConsistencyLevel.EACH_QUORUM,
                    s_esc, s_med, s_mia, q_esc, q_lec,
                    "mayoria en CADA ciudad (solo escritura)"))

    seccion("LECTURA DE RESULTADOS")
    print("""  LOCAL_QUORUM es el nivel que usa la mayoria de sistemas
  multi-DC en produccion, y la razon se ve en los numeros:

    - Da consistencia real dentro de la ciudad que atiende
      la peticion.
    - No espera al datacenter remoto, asi que evita el
      sobrecosto del enlace transatlantico.
    - Si Miami queda incomunicado, Medellin sigue operando
      con normalidad: es AP entre ciudades, CP dentro de una.

  QUORUM global, en cambio, exige mayoria sobre el total de
  replicas. Con {medellin:2, miami:1} son 2 de 3, asi que a
  veces necesita la replica de Miami y paga el viaje.

  Aplicado al caso: un ciudadano en Medellin consulta replicas
  de Medellin. La presencia se mantiene en ONE porque su
  inconsistencia no cuesta nada; el historial de alertas usaria
  LOCAL_QUORUM, que garantiza durabilidad sin latencia
  intercontinental.""")

    for c in (cl_e, cl_m, cl_i):
        c.shutdown()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nInterrumpido.")
