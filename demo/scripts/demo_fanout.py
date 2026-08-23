#!/usr/bin/env python3
"""
RETO - Fan-out masivo por particiones de zona
==============================================
Responde la pregunta (d) del enunciado:

  "Que estrategia usarian para que el envio de la alerta se reparta
   entre varios nodos segun la zona o el grupo de usuarios, en vez de
   que un solo nodo intente notificar a los 100.000 suscriptores?"

Teoria aplicada:
  Sec. 4.2 - Kafka como log inmutable y particionado.
  Sec. 3.3 - sharding por zona: la zona es la clave de particion,
             asi cada worker consume una particion distinta.
"""
import sys, time, json, uuid, datetime as dt
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from _common import conectar, titulo, seccion, color, requiere_driver  # noqa
from _datos import sembrar, alerta as generar_alerta, ZONAS  # noqa

TOPICO = "alertas-ciudadanas"
SUSCRIPTORES = 10_000
BOOTSTRAP = "localhost:29092"


def publicar_en_kafka(alerta):
    """Publica la alerta particionada por zona (Sec. 4.2)."""
    try:
        from kafka import KafkaProducer
    except ImportError:
        print(color("  kafka-python no instalado; se omite el paso Kafka.", "amarillo"))
        return False

    prod = KafkaProducer(
        bootstrap_servers=BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode(),
        key_serializer=lambda k: k.encode(),
        acks=1,
    )
    for idx, zona in enumerate(ZONAS):
        msg = dict(alerta, zona_destino=zona)
        # Particion explicita por zona.
        #
        # Con key= sola, Kafka aplica murmur2(key) % num_particiones y
        # dos zonas pueden colisionar en la misma particion (a nosotros
        # nos paso: 'norte' y 'sur' cayeron ambas en la 0, dejando la 2
        # vacia). Fijar la particion garantiza el reparto 1:1 que
        # queremos mostrar: una zona = una particion = un worker.
        md = prod.send(TOPICO, key=zona, value=msg,
                       partition=idx).get(timeout=10)
        etiqueta = color(f"{zona:<7}", "cyan")
        print(f"    zona {etiqueta} -> particion {md.partition}  offset {md.offset}")
    prod.flush()
    prod.close()
    return True


def entregar_zona(zona, alerta, n):
    """Un worker por zona escribe el historial de sus suscriptores."""
    from cassandra import ConsistencyLevel
    cluster, s = conectar("cass1")
    prep = s.prepare(
        "INSERT INTO historial_usuario (usuario_id, enviado_en, alerta_id, "
        "titulo, cuerpo, severidad, zona) VALUES (?,?,?,?,?,?,?)")
    # QUORUM: la alerta NO se puede perder (seguridad publica)
    prep.consistency_level = ConsistencyLevel.QUORUM

    t0 = time.perf_counter()
    futuros = []
    for i in range(n):
        futuros.append(s.execute_async(prep, (
            f"usuario:{zona}:{i:06d}",
            dt.datetime.now(dt.timezone.utc),
            alerta["alerta_id"],
            alerta["titulo"],
            alerta["cuerpo"],
            alerta["severidad"],
            zona,
        )))
        if len(futuros) >= 400:
            for f in futuros:
                f.result()
            futuros.clear()
    for f in futuros:
        f.result()

    dur = time.perf_counter() - t0
    cluster.shutdown()
    return zona, n, dur


def main():
    requiere_driver()
    titulo("RETO — Fan-out masivo particionado por zona")

    sembrar()
    a = generar_alerta(severidad="critica")
    alerta = {
        "alerta_id": uuid.uuid4(),
        "titulo": a["titulo"],
        "cuerpo": a["cuerpo"],
        "severidad": a["severidad"],
    }
    print(f"  Alerta   : {color(alerta['titulo'], 'bold')}")
    print(f"  Severidad: {color(alerta['severidad'], 'rojo')}")
    print(f"  Alcance  : {SUSCRIPTORES:,} suscriptores en {len(ZONAS)} zonas")

    seccion("PASO 1 — Publicar en el log distribuido (Kafka, Sec. 4.2)")
    serializable = dict(alerta, alerta_id=str(alerta["alerta_id"]))
    publicar_en_kafka(serializable)

    seccion("PASO 2 — Fan-out paralelo: un worker por zona (Sec. 3.3)")
    print(color("  Sin esto, un solo nodo intentaria notificar a todos.", "dim"))
    por_zona = SUSCRIPTORES // len(ZONAS)

    t0 = time.perf_counter()
    with ThreadPoolExecutor(max_workers=len(ZONAS)) as ex:
        resultados = list(ex.map(
            lambda z: entregar_zona(z, alerta, por_zona), ZONAS))
    total_dur = time.perf_counter() - t0

    print()
    for zona, n, dur in resultados:
        print(f"    zona {zona:<8} {n:>6,} entregas en {dur:>5.2f}s "
              f"({n/dur:>7,.0f}/s)")

    entregado = sum(r[1] for r in resultados)
    print()
    print(f"  {'Total entregado:':<22} {color(f'{entregado:,}', 'verde')}")
    print(f"  {'Tiempo (paralelo):':<22} {total_dur:.2f} s")
    print(f"  {'Throughput global:':<22} {color(f'{entregado/total_dur:,.0f} entregas/s', 'verde')}")

    secuencial = sum(r[2] for r in resultados)
    print(f"  {'Si fuera secuencial:':<22} {secuencial:.2f} s "
          f"({color(f'{secuencial/total_dur:.1f}x mas lento', 'amarillo')})")

    seccion("CONCLUSION")
    print(f"""  El particionado por zona convierte 1 escritura en N flujos
  paralelos e independientes. Escalar es agregar particiones y
  workers, no un servidor mas grande (Sec. 3.3 vs 3.2).

  Extrapolando a los 100.000 suscriptores reales del caso:
  aproximadamente {100_000/(entregado/total_dur):.1f} s con esta misma tasa.""")


if __name__ == "__main__":
    main()
