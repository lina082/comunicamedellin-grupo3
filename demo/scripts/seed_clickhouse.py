#!/usr/bin/env python3
"""
Genera eventos analiticos en ClickHouse (ruta OLAP).
Sec. 2.2 / 2.4 - motor columnar para agregaciones.
"""
import sys, random, datetime as dt, urllib.request
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from _common import titulo, color, ZONAS, DISPOSITIVOS  # noqa

CH = "http://localhost:8123/"
AUTH = ("demo", "demo")
FILAS = 1_000_000
LOTE = 50_000


def ejecutar(sql, datos=None):
    url = CH + "?" + urllib.parse.urlencode({"query": sql})
    req = urllib.request.Request(url, data=datos)
    cred = f"{AUTH[0]}:{AUTH[1]}"
    import base64
    req.add_header("Authorization", "Basic " + base64.b64encode(cred.encode()).decode())
    with urllib.request.urlopen(req, timeout=120) as r:
        return r.read().decode()


def main():
    titulo(f"Generando {FILAS:,} eventos de presencia en ClickHouse")
    print(color("  Motor columnar: solo lee las columnas de la consulta.", "dim"))

    ahora = dt.datetime.now()
    insertados = 0

    while insertados < FILAS:
        n = min(LOTE, FILAS - insertados)
        filas = []
        for _ in range(n):
            ts = ahora - dt.timedelta(seconds=random.randint(0, 86400))
            filas.append("\t".join([
                ts.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3],
                f"usuario:{random.randint(0, 99999):06d}",
                random.choices(["online", "offline"], weights=[7, 3])[0],
                random.choice(ZONAS),
                random.choice(DISPOSITIVOS),
                str(random.randint(5, 900)),
            ]))
        ejecutar("INSERT INTO comunica.presencia_eventos FORMAT TabSeparated",
                 ("\n".join(filas) + "\n").encode())
        insertados += n
        pct = 100 * insertados / FILAS
        print(f"\r  {insertados:>9,}/{FILAS:,}  [{'█' * int(pct/4):<25}] {pct:5.1f}%",
              end="", flush=True)

    print()
    print()
    tam = ejecutar("""SELECT formatReadableSize(sum(data_uncompressed_bytes)),
                             formatReadableSize(sum(data_compressed_bytes)),
                             round(sum(data_uncompressed_bytes)/sum(data_compressed_bytes),1)
                      FROM system.parts
                      WHERE database='comunica' AND table='presencia_eventos' AND active
                      FORMAT TabSeparated""").strip().split("\t")
    if len(tam) == 3:
        print(f"  {'Sin comprimir:':<18} {tam[0]}")
        print(f"  {'Comprimido:':<18} {color(tam[1], 'verde')}")
        print(f"  {'Ratio:':<18} {color(tam[2] + ':1', 'verde')}")
    print()
    print(color("  Ahora ejecuta:  make ch-demo", "dim"))


if __name__ == "__main__":
    import urllib.parse
    main()
