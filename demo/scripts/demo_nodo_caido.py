#!/usr/bin/env python3
"""
EXTRA (va mas alla del reto) - Tolerancia a particion
======================================================
El reto del Equipo 3 no pide esto; lo pide el Equipo 6.
Lo incluimos porque demuestra AP en accion: tumbamos un nodo
y el sistema sigue respondiendo.

Sec. 1.2 - CAP: con AP, el sistema permanece disponible aunque
           un nodo quede incomunicado.
Sec. 3.3 - replicacion leaderless: no hay un lider que perder.
"""
import sys, time, subprocess, datetime as dt
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from _common import conectar, titulo, seccion, color, requiere_driver  # noqa

NODO_VICTIMA = "cass2"
USUARIO = "usuario:tolerancia:001"


def docker(*args):
    return subprocess.run(["docker", *args], capture_output=True, text=True)


def nodetool_status():
    r = docker("exec", "cass1", "nodetool", "status")
    for linea in r.stdout.splitlines():
        if linea.startswith(("UN", "DN", "UL", "DL")):
            estado = linea[:2]
            c = "verde" if estado == "UN" else "rojo"
            print("   " + color(linea, c))


def escribir_y_leer(s, prep_w, prep_r, etiqueta):
    """Intenta una escritura + lectura con ONE. Devuelve True si funciono."""
    try:
        t0 = time.perf_counter()
        s.execute(prep_w, ("online", dt.datetime.now(dt.timezone.utc),
                           "centro", "android", "cass1", USUARIO))
        fila = s.execute(prep_r, (USUARIO,)).one()
        ms = (time.perf_counter() - t0) * 1000
        print(f"   {etiqueta:<28} {color('OK', 'verde')}  "
              f"{ms:>6.1f} ms   estado={fila.estado}")
        return True
    except Exception as e:
        print(f"   {etiqueta:<28} {color('FALLO', 'rojo')}  {type(e).__name__}")
        return False


def main():
    requiere_driver()
    from cassandra import ConsistencyLevel

    titulo("EXTRA — Tolerancia a fallos: que pasa si cae un nodo")
    print("  Con AP y RF=3, perder un nodo no interrumpe el servicio.")

    cluster, s = conectar("cass1")
    prep_w = s.prepare(
        "UPDATE presencia SET estado=?, last_seen=?, zona=?, "
        "dispositivo=?, escrito_por=? WHERE usuario_id=?")
    prep_r = s.prepare("SELECT usuario_id, estado FROM presencia WHERE usuario_id=?")
    prep_w.consistency_level = ConsistencyLevel.ONE
    prep_r.consistency_level = ConsistencyLevel.ONE

    seccion("1. Estado inicial del cluster")
    nodetool_status()
    print()
    escribir_y_leer(s, prep_w, prep_r, "Escritura+lectura ONE")

    seccion(f"2. Tumbando el nodo {NODO_VICTIMA}")
    print(color(f"   $ docker stop {NODO_VICTIMA}", "dim"))
    docker("stop", NODO_VICTIMA)
    print("   Esperando a que el cluster detecte la caida (~25s)...")
    time.sleep(25)
    nodetool_status()

    seccion("3. El sistema con un nodo caido")
    ok_one = escribir_y_leer(s, prep_w, prep_r, "ONE  (1 de 3 replicas)")

    prep_w.consistency_level = ConsistencyLevel.QUORUM
    prep_r.consistency_level = ConsistencyLevel.QUORUM
    ok_quo = escribir_y_leer(s, prep_w, prep_r, "QUORUM (2 de 3 replicas)")

    prep_w.consistency_level = ConsistencyLevel.ALL
    prep_r.consistency_level = ConsistencyLevel.ALL
    ok_all = escribir_y_leer(s, prep_w, prep_r, "ALL  (3 de 3 replicas)")

    seccion("4. Restaurando el nodo")
    print(color(f"   $ docker start {NODO_VICTIMA}", "dim"))
    docker("start", NODO_VICTIMA)
    print("   Esperando reincorporacion (~40s)...")
    time.sleep(40)
    nodetool_status()

    seccion("LECTURA DE RESULTADOS")
    print(f"""  Con 1 de 3 nodos caidos:
    ONE     -> {color('sigue funcionando', 'verde') if ok_one else color('fallo', 'rojo')}   (le basta 1 replica)
    QUORUM  -> {color('sigue funcionando', 'verde') if ok_quo else color('fallo', 'rojo')}   (2 de 3 aun disponibles)
    ALL     -> {color('sigue funcionando', 'verde') if ok_all else color('fallo como se esperaba', 'amarillo')}   (exige las 3)

  Esto es el teorema CAP hecho experimento (Sec. 1.2):
  a mayor exigencia de consistencia, menor disponibilidad
  ante particion. Elegir ONE para presencia no es descuido:
  es la decision que mantiene el canal de alertas en pie
  justamente cuando algo esta fallando.""")

    cluster.shutdown()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print(color(f"\nInterrumpido. Verifica: docker start {NODO_VICTIMA}", "amarillo"))
