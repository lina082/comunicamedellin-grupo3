#!/usr/bin/env python3
"""
Graba una corrida REAL del fan-out para el modo demostracion.

El frontend desplegado en Vercel no puede alcanzar el cluster local,
asi que reproduce esta captura. Los datos NO son inventados: salen
de una ejecucion verdadera contra Cassandra y Kafka.

Uso:  make grabar-demo   (con el cluster y la API corriendo)
"""
import asyncio, json, sys, urllib.request
from pathlib import Path

RAIZ = Path(__file__).resolve().parents[2]
SALIDA = RAIZ / "frontend" / "demo-data.json"
API = "http://localhost:8765"
DESTINOS = 100


def get(ruta):
    with urllib.request.urlopen(f"{API}/{ruta}", timeout=20) as r:
        return json.loads(r.read())


async def capturar_fanout():
    import websockets
    eventos = []
    async with websockets.connect("ws://localhost:8765/ws-fanout") as ws:
        await ws.send(json.dumps({
            "titulo": "Cierre vial - Avenida Regional",
            "cuerpo": "Cierre total por emergencia entre la Calle 30 y la "
                      "Calle 44. Use rutas alternas.",
            "severidad": "critica", "destinos": DESTINOS}))
        while True:
            d = json.loads(await asyncio.wait_for(ws.recv(), timeout=40))
            eventos.append(d)
            if d["evento"] == "fin":
                return eventos


def main():
    try:
        salud = get("salud")
    except Exception:
        print("La API no responde. Levanta el cluster y la API primero:")
        print("    make up && make api")
        sys.exit(1)

    if not salud.get("nodos_conectados"):
        print("La API responde pero no hay nodos de Cassandra conectados.")
        sys.exit(1)

    print(f"Nodos conectados: {', '.join(salud['nodos_conectados'])}")
    print(f"Grabando fan-out de {DESTINOS} entregas...")
    eventos = asyncio.run(capturar_fanout())

    captura = {
        "_nota": "Corrida REAL capturada del cluster local. Se usa solo "
                 "cuando la API no esta disponible (modo demo en Vercel).",
        "fanout": eventos,
        "tablas": get("tablas"),
        "anillo": get("anillo"),
        "suscriptores": get("suscriptores?limite=25"),
        "datos": {t: get(f"datos/{t}?limite=15")
                  for t in ("presencia", "historial_usuario", "alertas")},
    }
    SALIDA.write_text(json.dumps(captura, ensure_ascii=False,
                                 separators=(",", ":")))

    entregas = [e for e in eventos if e["evento"] == "entrega"]
    print()
    print(f"  entregas   : {len(entregas)}")
    print(f"  duracion   : {eventos[-1]['ms']/1000:.2f} s")
    print(f"  tamano     : {SALIDA.stat().st_size/1024:.0f} KB")
    print(f"  guardado en: {SALIDA.relative_to(RAIZ)}")


if __name__ == "__main__":
    main()
