#!/usr/bin/env python3
"""
MODULO 2 - Idempotencia y eventos duplicados de Kafka
======================================================
Responde la pregunta guia del taller:

  "Como evitar duplicados?"  (fan-out on write, 1 alerta -> 100.000 filas)

EL PROBLEMA REAL
----------------
Kafka garantiza entrega "al menos una vez" (at-least-once). Si un
worker procesa un mensaje y muere ANTES de confirmar el offset, al
reiniciar vuelve a recibir el mismo mensaje. En un fan-out eso
significa reenviar la alerta a 100.000 personas por segunda vez.

Cassandra NO tiene UNIQUE ni PRIMARY KEY que rechace un INSERT
repetido: un INSERT sobre una clave existente es un UPSERT silencioso.
Eso, que suena a defecto, es exactamente la defensa.

LO QUE DEMUESTRA ESTE SCRIPT
----------------------------
  Caso A  timestamp del EVENTO   -> reprocesar NO duplica  (correcto)
  Caso B  timestamp del WORKER   -> reprocesar SI duplica  (el bug)
  Caso C  guardia con LWT        -> bloquea efectos colaterales

Teoria aplicada:
  Modulo 1, Sec. 4.2 - Kafka como log inmutable; el reprocesamiento
                       consiste en releer desde el offset.
  Modulo 2           - last-write-wins como mecanismo de idempotencia.
"""
import sys, time, uuid, datetime as dt
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from _common import conectar, titulo, seccion, color, requiere_driver  # noqa

USUARIO = "CC-IDEMPOTENCIA-001"
REENVIOS = 3


def limpiar(s):
    s.execute("DELETE FROM historial_usuario WHERE usuario_id = %s", (USUARIO,))
    s.execute("DELETE FROM eventos_procesados WHERE evento_id = %s",
              (f"{USUARIO}:caso-c",))


def contar(s):
    return s.execute(
        "SELECT count(*) AS n FROM historial_usuario WHERE usuario_id = %s",
        (USUARIO,)).one().n


def caso_a(s, evento):
    """Timestamp tomado del EVENTO: la clave primaria no cambia."""
    seccion("CASO A — enviado_en viene del EVENTO  (diseno correcto)")
    print("  El productor fija el timestamp una sola vez y viaja dentro")
    print("  del mensaje. Reprocesar reescribe la MISMA clave primaria:")
    print(color("      PRIMARY KEY ((usuario_id), enviado_en)", "cyan"))
    print("  Cassandra resuelve por last-write-wins -> misma fila.\n")

    for intento in range(1, REENVIOS + 1):
        s.execute(
            """INSERT INTO historial_usuario
               (usuario_id, enviado_en, alerta_id, titulo, severidad, zona)
               VALUES (%s, %s, %s, %s, %s, %s)""",
            (USUARIO, evento["enviado_en"], evento["alerta_id"],
             evento["titulo"], "critica", "centro"))
        n = contar(s)
        etiqueta = "entrega original" if intento == 1 else f"reintento {intento - 1}"
        c = "verde" if n == 1 else "rojo"
        print(f"    {etiqueta:<20} filas en historial: {color(str(n), c)}")

    return contar(s)


def caso_b(s, evento):
    """Timestamp generado en el WORKER: cada reintento crea una fila."""
    seccion("CASO B — enviado_en generado con now() en el WORKER  (el bug)")
    print("  Cada reintento produce un timestamp distinto, luego una")
    print("  CLAVE PRIMARIA distinta, luego una fila nueva. El usuario")
    print("  ve la misma alerta repetida N veces.\n")

    limpiar(s)
    for intento in range(1, REENVIOS + 1):
        s.execute(
            """INSERT INTO historial_usuario
               (usuario_id, enviado_en, alerta_id, titulo, severidad, zona)
               VALUES (%s, %s, %s, %s, %s, %s)""",
            (USUARIO, dt.datetime.now(dt.timezone.utc), evento["alerta_id"],
             evento["titulo"], "critica", "centro"))
        time.sleep(0.01)   # basta 1 ms de diferencia para romperlo
        n = contar(s)
        etiqueta = "entrega original" if intento == 1 else f"reintento {intento - 1}"
        c = "verde" if n == 1 else "rojo"
        print(f"    {etiqueta:<20} filas en historial: {color(str(n), c)}")

    return contar(s)


def caso_c(s):
    """Guardia con lightweight transaction para efectos colaterales."""
    seccion("CASO C — guardia LWT para efectos NO idempotentes")
    print("  El last-write-wins arregla la FILA, pero no arregla el push")
    print("  al celular ni el contador de metricas: eso ya salio.")
    print("  Antes de disparar el push, el worker reclama el evento con")
    print(color("      INSERT ... IF NOT EXISTS", "cyan"), "(Paxos).\n")

    evento_id = f"{USUARIO}:caso-c"
    for intento in range(1, REENVIOS + 1):
        r = s.execute(
            """INSERT INTO eventos_procesados (evento_id, procesado_en, worker)
               VALUES (%s, %s, %s) IF NOT EXISTS""",
            (evento_id, dt.datetime.now(dt.timezone.utc), "worker-1")).one()
        gano = r.applied
        if gano:
            print(f"    intento {intento}: {color('APLICADO', 'verde')}"
                  "   -> se envia el push")
        else:
            print(f"    intento {intento}: {color('RECHAZADO', 'amarillo')}"
                  "  -> ya procesado, push descartado")
    print()
    print(color("  Coste: Paxos es 10-20x mas lento que una escritura normal.", "dim"))
    print(color("  Por eso el guardia va SOLO en el push, no en las 100.000", "dim"))
    print(color("  escrituras del fan-out.", "dim"))


def main():
    requiere_driver()
    titulo("MODULO 2 — Idempotencia frente a eventos duplicados de Kafka")
    print("  Kafka entrega at-least-once. Un worker que muere antes de")
    print("  confirmar el offset reprocesa el mismo mensaje al reiniciar.")

    cluster, s = conectar("cass1")
    try:
        limpiar(s)
        evento = {
            "alerta_id": uuid.uuid4(),
            "enviado_en": dt.datetime.now(dt.timezone.utc),
            "titulo": "Creciente subita - Rio Medellin",
        }
        print(f"\n  Evento de prueba: alerta {color(str(evento['alerta_id'])[:8], 'cyan')}"
              f" -> usuario {color(USUARIO, 'cyan')}")
        print(f"  Se reenvia {REENVIOS} veces simulando el reintento de Kafka.")

        n_a = caso_a(s, evento)
        n_b = caso_b(s, evento)
        caso_c(s)

        seccion("RESULTADO")
        ok_a = color("1 fila  — sin duplicados", "verde") if n_a == 1 else color(f"{n_a} filas", "rojo")
        ok_b = color(f"{n_b} filas — DUPLICADO", "rojo") if n_b > 1 else color(f"{n_b} fila", "verde")
        print(f"    Caso A (timestamp del evento):  {ok_a}")
        print(f"    Caso B (timestamp del worker):  {ok_b}")
        print()
        print("  Conclusion para la exposicion:")
        print("  La idempotencia del fan-out no cuesta nada — no hace falta")
        print("  deduplicar ni coordinar. Basta con que la clave primaria se")
        print("  derive del EVENTO y no del momento en que se procesa.")

        limpiar(s)
    finally:
        cluster.shutdown()


if __name__ == "__main__":
    main()
