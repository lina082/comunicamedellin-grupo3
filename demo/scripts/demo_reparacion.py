#!/usr/bin/env python3
"""
MODULO 2 - Nodo caido prolongado, hinted handoff y reparacion
==============================================================
Responde el ejercicio final del taller:

  "Que ocurre con los datos cuando un nodo Cassandra permanece
   desconectado durante un periodo prolongado y como se recupera
   posteriormente?"

LOS TRES MECANISMOS, EN ORDEN
-----------------------------
  1. HINTED HANDOFF   ventana corta (por defecto 3 h)
     El coordinador guarda en disco local las escrituras destinadas
     al nodo caido y se las entrega cuando vuelve. Es automatico.

  2. READ REPAIR      oportunista, solo lo que se lee
     Al leer con QUORUM el coordinador detecta replicas divergentes
     y las corrige en segundo plano. Solo repara datos consultados.

  3. NODETOOL REPAIR  manual/programado, unica garantia completa
     Compara arboles de Merkle entre replicas y sincroniza TODO.
     Es lo unico que cubre un nodo caido MAS de la ventana de hints.

EL PUNTO CRITICO
----------------
  max_hint_window_in_ms = 10800000  (3 horas)

  Si el nodo vuelve ANTES de 3 h  -> los hints lo ponen al dia solo.
  Si vuelve DESPUES de 3 h        -> el coordinador YA DEJO de guardar
                                     hints. Ese nodo tiene un hueco de
                                     datos permanente y solo 'nodetool
                                     repair' lo cierra.

  Por eso la practica estandar es correr repair periodicamente, dentro
  de gc_grace_seconds (10 dias por defecto). Si no se repara en ese
  plazo, los borrados pueden "resucitar" (zombie data): el tombstone
  se purga en las replicas vivas antes de que el nodo caido se entere
  de que el dato fue borrado.

Teoria aplicada:
  Modulo 1, Sec. 1.2 - CAP: AP significa aceptar escrituras aunque una
                       replica este incomunicada; la consistencia se
                       reconstruye despues (consistencia eventual).
  Modulo 1, Sec. 3.3 - replicacion leaderless: no hay lider que perder,
                       cualquier nodo coordina.
"""
import sys, time, subprocess, datetime as dt
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from _common import conectar, titulo, seccion, color, requiere_driver  # noqa

VICTIMA = "cass2"
PREFIJO = "CC-REPARACION-"
ESCRITURAS = 2000


def docker(*args, check=False):
    return subprocess.run(["docker", *args], capture_output=True, text=True)


def nodetool(nodo, *args):
    return docker("exec", nodo, "nodetool", *args)


def estado_anillo(nodo="cass1"):
    r = nodetool(nodo, "status")
    for linea in r.stdout.splitlines():
        if linea.startswith(("UN", "DN", "UL", "DL")):
            c = "verde" if linea.startswith("UN") else "rojo"
            print("     " + color(linea.rstrip(), c))


def hints_pendientes(nodo="cass1"):
    """Cuenta los ficheros de hints acumulados en el coordinador."""
    r = docker("exec", nodo, "sh", "-c",
               "ls -1 /var/lib/cassandra/hints/ 2>/dev/null | wc -l")
    try:
        return int(r.stdout.strip())
    except ValueError:
        return 0


def datos_locales(nodo):
    """Datos que ESTE nodo tiene fisicamente en su propio disco.

    No se puede medir con un SELECT. Con RF=3 sobre 3 nodos los tres
    son replica de todo (verificable con 'nodetool getendpoints'), asi
    que un SELECT ... CONSISTENCY ONE dirigido a cass2 puede servirse
    desde cass1 o cass3 y devolveria el valor correcto aunque cass2
    tuviera un hueco: el coordinador siempre elige una replica viva.

    Tampoco sirve 'Local write count': es un contador acumulado desde
    el arranque del proceso, se reinicia con el nodo, y la reparacion
    no lo incrementa porque escribe SSTables directamente en vez de
    pasar por el camino normal de escritura.

    La metrica que si refleja el estado real del disco es el numero de
    particiones almacenadas y el espacio que ocupan. Si el nodo estuvo
    caido le faltan particiones; tras el repair converge con el resto.
    """
    nodetool(nodo, "flush", "comunica")
    r = nodetool(nodo, "tablestats", "comunica.historial_usuario")
    parts, space = -1, -1
    for linea in r.stdout.splitlines():
        if "Number of partitions" in linea:
            try: parts = int(linea.split(":")[1].strip())
            except (IndexError, ValueError): pass
        elif "Space used (live)" in linea:
            try: space = int(linea.split(":")[1].strip())
            except (IndexError, ValueError): pass
    return parts, space


def tabla_locales(etiqueta):
    """Imprime particiones y espacio en disco de los 3 nodos."""
    print(f"     {etiqueta}")
    vals = {}
    for n in ("cass1", "cass2", "cass3"):
        parts, space = datos_locales(n)
        vals[n] = parts
        marca = "  <-- victima" if n == VICTIMA else ""
        print(f"       {n}  particiones en disco: {parts:>6}   "
              f"{space/1024:>8.1f} KB{marca}")
    ref = max(vals.values())
    if vals[VICTIMA] < ref:
        print(f"       {color('divergencia: a ' + VICTIMA + ' le faltan ' + str(ref - vals[VICTIMA]) + ' particiones', 'rojo')}")
    else:
        print(f"       {color('las tres replicas coinciden', 'verde')}")
    return vals


def escribir(s, desde, hasta, etiqueta):
    """Escribe con ONE: la escritura se acepta aunque falte una replica."""
    from cassandra import ConsistencyLevel
    q = s.prepare(
        """INSERT INTO historial_usuario
           (usuario_id, enviado_en, alerta_id, titulo, severidad, zona)
           VALUES (?, ?, uuid(), ?, 'critica', 'centro')""")
    q.consistency_level = ConsistencyLevel.ONE

    base = dt.datetime.now(dt.timezone.utc)
    for i in range(desde, hasta):
        s.execute(q, (f"{PREFIJO}{i:03d}",
                      base + dt.timedelta(milliseconds=i),
                      f"{etiqueta} #{i}"))
    print(f"     {hasta - desde} escrituras con "
          f"{color('CONSISTENCY ONE', 'cyan')} — {etiqueta}")


def main():
    requiere_driver()
    from cassandra import ConsistencyLevel

    titulo("MODULO 2 — Nodo caido prolongado: hints, read repair y repair")
    print("  Se tumba un nodo MIENTRAS se escribe, se sigue escribiendo sin")
    print("  el, y se mide el hueco de datos antes y despues de repararlo.")

    cluster, s = conectar("cass1")
    try:
        # ---------------------------------------------------------
        seccion("1. Estado inicial del anillo")
        estado_anillo()
        # TRUNCATE, no DELETE: los DELETE dejan tombstones y las
        # estadisticas de SSTable siguen contando esas particiones,
        # lo que falsearia la medicion del hueco.
        s.execute("TRUNCATE historial_usuario")
        time.sleep(3)

        seccion("2. Linea base: las tres replicas vacias")
        for n in ("cass1", "cass2", "cass3"):
            nodetool(n, "flush", "comunica")
        tabla_locales("Datos EN DISCO por nodo antes de escribir:")

        # ---------------------------------------------------------
        seccion(f"3. Se cae {VICTIMA} DURANTE la escritura")
        print("     Primero se desactiva hinted handoff en los otros nodos:")
        print("     sin esto cass1 guardaria las escrituras y se las entregaria")
        print("     a cass2 al volver, cerrando el hueco antes de poder medirlo.")
        for n in ("cass1", "cass3"):
            nodetool(n, "disablehandoff")
            # Purgar hints de corridas anteriores: si quedan en disco, se
            # entregan al reincorporarse la victima y cierran el hueco
            # antes de que podamos medirlo.
            nodetool(n, "truncatehints")
        print()
        print(f"     docker stop {VICTIMA}  (caida real, no latencia de red)")
        docker("stop", VICTIMA)
        time.sleep(12)          # el gossip tarda en marcarlo DN
        estado_anillo()

        print()
        print("     El cluster SIGUE aceptando escrituras: eso es la A de AP")
        print("     (Modulo 1, Sec. 1.2). Con CP el sistema las rechazaria.")
        escribir(s, 0, ESCRITURAS, "con un nodo caido")

        print()
        print(color("     Con hinted handoff ACTIVO (produccion), cass1 guardaria", "dim"))
        print(color("     estas escrituras en nombre de cass2 durante 3 h", "dim"))
        print(color("     (max_hint_window_in_ms) y se las entregaria al volver.", "dim"))
        print(color("     Pasado ese plazo deja de guardarlas: hueco permanente.", "dim"))

        # ---------------------------------------------------------
        seccion("4. Vuelve el nodo — evidencia ANTES de reparar")
        docker("start", VICTIMA)
        print("     Esperando a que el nodo entre en estado NORMAL...")
        for _ in range(40):
            time.sleep(5)
            r = nodetool(VICTIMA, "info")
            if "Native Transport active: true" in r.stdout:
                break
        time.sleep(10)
        estado_anillo()
        print()
        antes = tabla_locales("Datos EN DISCO por nodo (sin coordinacion):")
        hueco = max(antes.values()) - antes[VICTIMA]
        print()
        print(f"     Hueco de datos en {VICTIMA}: "
              f"{color(str(hueco) + ' particiones', 'rojo')} que nunca recibio.")
        print()
        print(color("     Por que no se ve con un SELECT:", "amarillo"))
        print("       Con RF=3 sobre 3 nodos los tres son replica de todo")
        print("       (nodetool getendpoints lo confirma). Un SELECT con ONE")
        print("       dirigido a cass2 se sirve desde cualquier replica viva y")
        print("       devuelve el valor correcto aunque cass2 tenga el hueco.")
        print("       El cliente NO percibe la inconsistencia: eso es")
        print("       exactamente lo que significa consistencia eventual.")

        seccion("5. Reparacion explicita — nodetool repair")
        print("     Compara arboles de Merkle entre replicas y sincroniza")
        print("     TODO el rango, no solo lo que alguien haya leido.")
        t0 = time.perf_counter()
        r = nodetool(VICTIMA, "repair", "-full", "comunica")
        seg = time.perf_counter() - t0
        ok = r.returncode == 0
        print(f"     {'completado' if ok else 'FALLO'} en {seg:.1f} s")
        for linea in r.stdout.splitlines()[-4:]:
            if linea.strip():
                print(color("       " + linea.strip()[:90], "dim"))

        seccion("6. Evidencia DESPUES de reparar")
        despues = tabla_locales("Datos EN DISCO por nodo (sin coordinacion):")
        recuperadas = despues[VICTIMA] - antes[VICTIMA]
        print()
        print(f"     {VICTIMA} recupero {color(str(recuperadas), 'verde')} particiones")
        print("     durante la reparacion: son las que le faltaban.")

        seccion("RESUMEN PARA LA EXPOSICION")
        print(f"     particiones faltantes antes del repair : {color(str(hueco), 'rojo')}")
        print(f"     particiones recuperadas por el repair   : {color(str(recuperadas), 'verde')}")
        print(f"     estado final: {'CONVERGIDO' if despues[VICTIMA] == max(despues.values()) else 'AUN DIVERGENTE'}")
        print()
        print("     Los tres mecanismos, en orden de cobertura:")
        print("       hinted handoff  automatico, ventana de 3 h. En esta demo")
        print("                       se desactiva (nodetool disablehandoff) para")
        print("                       que el hueco sea observable; con handoff")
        print("                       activo cass2 se pondria al dia solo.")
        print("       read repair     solo lo que se lee, oportunista")
        print("       nodetool repair unica garantia completa; debe correrse")
        print("                       dentro de gc_grace_seconds (10 dias) o")
        print("                       los borrados pueden resucitar")

        s.execute("TRUNCATE historial_usuario")
    finally:
        cluster.shutdown()
        docker("start", VICTIMA)
        # Restaurar el comportamiento por defecto del cluster.
        for n in ("cass1", "cass3"):
            nodetool(n, "enablehandoff")


if __name__ == "__main__":
    main()
