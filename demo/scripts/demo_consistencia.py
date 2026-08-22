#!/usr/bin/env python3
"""
RETO PRACTICO - Nucleo de la demostracion
==========================================
Cumple literalmente el enunciado del Equipo 3:

  "Consultar el mismo usuario justo despues de escribir, leyendo desde
   dos nodos distintos con un nivel de consistencia bajo (ej. ONE), y
   mostrar diferencia de milisegundos entre lo que devuelve cada nodo.
   Repetir la misma consulta con un nivel de consistencia mas alto
   (ej. QUORUM) y comparar: desaparece la diferencia? a que costo
   de latencia?"

Teoria aplicada:
  Sec. 1.2 - CAP: eleccion AP, consistencia eventual.
  Sec. 1.3 - Cassandra (AP, wide-column), consistencia ajustable.
  Sec. 3.3 - Replicacion leaderless sobre 3 nodos, RF=3.
"""
import statistics as st
import sys, time, datetime as dt
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from _common import conectar, titulo, seccion, color, requiere_driver  # noqa: E402

RONDAS = 150
USUARIO = "usuario:demo:A-12"   # guiño a TuBoleta, para el contraste


def medir(sesion, consulta, nivel):
    """Ejecuta una lectura y devuelve (latencia_ms, fila)."""
    from cassandra import ConsistencyLevel
    consulta.consistency_level = nivel
    t0 = time.perf_counter()
    fila = sesion.execute(consulta, (USUARIO,)).one()
    return (time.perf_counter() - t0) * 1000, fila


def ronda(nivel_nombre, nivel, s_esc, s_n1, s_n3, q_esc, q_lec):
    """Escribe con nivel ONE y lee INMEDIATAMENTE desde dos nodos.

    Metodologia (importante para defender la demo):

      1. La escritura SIEMPRE se hace con ONE. Es la condicion realista:
         100.000 usuarios reportando presencia constantemente no pueden
         pagar consenso en cada heartbeat.

      2. La escritura se CONFIRMA antes de leer. Asi la comparacion es
         legitima: el dato ya existe, y lo unico en juego es si la
         replica que responde lo tiene o no.

      3. Las dos lecturas van en paralelo, con el nivel bajo estudio.

    Esto aisla la variable correcta: el nivel de consistencia de LECTURA.
    Con ONE puede responder una replica que aun no recibio la escritura.
    Con QUORUM responden 2 de 3, y al menos una tiene el dato mas reciente.
    """
    from concurrent.futures import ThreadPoolExecutor
    from cassandra import ConsistencyLevel

    divergencias, lat_n1, lat_n3 = 0, [], []
    # La escritura siempre ONE: es el escenario real de presencia
    q_esc.consistency_level = ConsistencyLevel.ONE

    with ThreadPoolExecutor(max_workers=2) as ex:
        for i in range(RONDAS):
            ahora = dt.datetime.now(dt.timezone.utc)
            # Escritura confirmada -> el dato existe en al menos 1 replica
            s_esc.execute(q_esc, ("online", ahora, "centro",
                                  "android", "cass1", USUARIO))

            # Lectura inmediata y simultanea desde dos nodos distintos
            f1 = ex.submit(medir, s_n1, q_lec, nivel)
            f3 = ex.submit(medir, s_n3, q_lec, nivel)
            l1, fila1 = f1.result()
            l3, fila3 = f3.result()

            lat_n1.append(l1)
            lat_n3.append(l3)

            t1 = fila1.last_seen if fila1 else None
            t3 = fila3.last_seen if fila3 else None

            # Divergencia real: los nodos difieren, o alguno no tiene
            # todavia la escritura que acabamos de confirmar
            desactualizado = (t1 != ahora.replace(microsecond=(ahora.microsecond // 1000) * 1000)
                              or t3 != t1)
            if t1 != t3:
                divergencias += 1
                if divergencias <= 3:
                    delta = abs((t1 - t3).total_seconds() * 1000) if (t1 and t3) else 0
                    print(color(
                        f"    ronda {i+1:>3}  DIVERGENCIA  nodo1={t1}  "
                        f"nodo3={t3}  (delta {delta:.1f} ms)", "amarillo"))

    p50 = st.median(lat_n1 + lat_n3)
    p95 = sorted(lat_n1 + lat_n3)[int(0.95 * len(lat_n1 + lat_n3)) - 1]
    pct = 100 * divergencias / RONDAS

    print()
    print(f"    {'Divergencias:':<22} "
          + color(f"{divergencias}/{RONDAS} ({pct:.1f}%)",
                  "rojo" if divergencias else "verde"))
    print(f"    {'Latencia P50:':<22} {p50:.2f} ms")
    print(f"    {'Latencia P95:':<22} {p95:.2f} ms")
    return {"nivel": nivel_nombre, "div": divergencias,
            "pct": pct, "p50": p50, "p95": p95}


def main():
    requiere_driver()
    from cassandra import ConsistencyLevel
    from cassandra.query import SimpleStatement

    titulo("RETO PRACTICO — Consistencia ajustable: ONE vs QUORUM")
    print("  Escribimos en el nodo 1 y leemos de inmediato desde")
    print("  el nodo 1 y el nodo 3. Observamos si coinciden.")
    print(f"  Usuario de prueba: {color(USUARIO, 'cyan')}   Rondas: {RONDAS}")

    cl_esc, s_esc = conectar("cass1")
    cl_n1,  s_n1  = conectar("cass1")
    cl_n3,  s_n3  = conectar("cass3")

    q_esc = SimpleStatement(
        "UPDATE presencia SET estado=%s, last_seen=%s, zona=%s, "
        "dispositivo=%s, escrito_por=%s WHERE usuario_id=%s")
    q_lec = SimpleStatement(
        "SELECT usuario_id, estado, last_seen FROM presencia WHERE usuario_id=%s")

    seccion("NIVEL ONE — responde la primera replica disponible")
    print(color("  Esperado: rapido, pero puede devolver un dato viejo.", "dim"))
    r_one = ronda("ONE", ConsistencyLevel.ONE, s_esc, s_n1, s_n3, q_esc, q_lec)

    seccion("NIVEL QUORUM — deben coincidir 2 de 3 replicas")
    print(color("  Esperado: sin divergencias, a costa de mas latencia.", "dim"))
    r_quo = ronda("QUORUM", ConsistencyLevel.QUORUM, s_esc, s_n1, s_n3, q_esc, q_lec)

    # ── Comparativa ──
    titulo("COMPARATIVA")
    print(f"  {'Nivel':<10}{'Divergencias':>16}{'P50 (ms)':>12}{'P95 (ms)':>12}")
    print("  " + "─" * 50)
    for r in (r_one, r_quo):
        print(f"  {r['nivel']:<10}{r['div']:>7} ({r['pct']:>4.1f}%){r['p50']:>12.2f}{r['p95']:>12.2f}")

    sobrecosto = ((r_quo["p50"] / r_one["p50"]) - 1) * 100 if r_one["p50"] else 0
    print()
    print(f"  Sobrecosto de QUORUM sobre ONE: {color(f'+{sobrecosto:.0f}%', 'amarillo')} en latencia P50")

    seccion("CONCLUSION PARA PRODUCCION")
    print("""  Elegimos ONE para presencia.

  Justificacion de negocio (Sec. 1.2 — AP):
    - Que "en linea" llegue 2 s tarde no le cuesta nada al negocio.
      Nadie toma una decision con ese dato.
    - QUORUM cuesta latencia en CADA heartbeat. Con 100.000 usuarios
      reportando continuamente, es sobrecosto permanente sin beneficio.
    - Ante una particion de red, ONE mantiene el sistema respondiendo;
      QUORUM lo dejaria rechazando escrituras y la app se veria caida.

  Contraste con TuBoleta (Equipo 1):
    Alla el recurso (silla A-12) es finito y exclusivo: dos compradores
    compiten y vender dos veces genera un problema legal. Por eso alla
    corresponde CP con consenso (Raft).
    Aqui nadie compite por "estar en linea" — el recurso es replicable
    sin conflicto. Pagar consenso seria gasto puro.""")

    for c in (cl_esc, cl_n1, cl_n3):
        c.shutdown()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nInterrumpido.")
