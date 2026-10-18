#!/usr/bin/env python3
"""
Taller 3 - Paso 0: siembra de alertas reales en Cassandra.

El proyecto ya tiene seed_usuarios.py (Modulo 1) para la tabla
'presencia'. Faltaba volumen y variedad en 'alertas', 'historial_usuario'
y 'estado_entrega' para que el Mini-Lakehouse tenga algo interesante que
analizar: varias severidades, varias zonas, varios dias.

Genera N_ALERTAS alertas con fecha distribuida en los ultimos 30 dias,
hace fan-out a una muestra de usuarios reales de 'presencia' (no a los
100.000 simulados del reto de Modulo 1, para que el pipeline corra en
segundos) y registra el resultado de la entrega en 'estado_entrega' con
una tasa de fallo variable segun severidad y dispositivo.

Uso:
    python scripts/seed_alertas.py
"""
import random
import sys
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "demo" / "scripts"))
from _common import conectar  # noqa: E402
from _datos import PLANTILLAS_ALERTA, VIAS, RIOS, INSTITUCIONES, COMUNAS, ZONAS  # noqa: E402

N_ALERTAS = 60
USUARIOS_POR_ALERTA = 400          # muestra de suscriptores por alerta
DIAS_HISTORIA = 30
SEMILLA = 7

# Tasa de fallo base de entrega por severidad: una alerta critica se
# reintenta mas y por eso falla menos al final.
TASA_FALLO_POR_SEVERIDAD = {"critica": 0.02, "advertencia": 0.05, "info": 0.08}


def generar_alerta_variada(rng):
    sev, titulo, cuerpo = rng.choice(PLANTILLAS_ALERTA)
    zona = rng.choice(ZONAS)
    ctx = {
        "via": rng.choice(VIAS),
        "rio": rng.choice(RIOS),
        "institucion": rng.choice(INSTITUCIONES),
        "zona": zona,
        "comuna": rng.choice(COMUNAS[zona])[0],
    }
    return {
        "alerta_id": uuid.uuid4(),
        "titulo": titulo.format(**ctx),
        "cuerpo": cuerpo.format(**ctx),
        "severidad": sev,
        "zona_destino": zona,
    }


def main():
    rng = random.Random(SEMILLA)
    cluster, session = conectar("cass1")

    usuarios = [r.usuario_id for r in session.execute(
        "SELECT usuario_id FROM presencia")]
    if not usuarios:
        print("No hay usuarios en 'presencia'. Corre primero:")
        print("  make seed   (dentro de comunicamedellin-grupo3)")
        sys.exit(1)
    print(f"Usuarios disponibles en presencia: {len(usuarios):,}")

    ins_alerta = session.prepare(
        "INSERT INTO alertas (alerta_id, titulo, cuerpo, severidad, "
        "zona_destino, publicada_en, total_envios) VALUES (?,?,?,?,?,?,?)")
    ins_historial = session.prepare(
        "INSERT INTO historial_usuario (usuario_id, enviado_en, alerta_id, "
        "titulo, cuerpo, severidad, zona) VALUES (?,?,?,?,?,?,?)")
    ins_entrega = session.prepare(
        "INSERT INTO estado_entrega (alerta_id, bucket, usuario_id, estado, "
        "intentos, actualizado) VALUES (?,?,?,?,?,?)")

    ahora = datetime.now(timezone.utc)
    total_entregas = 0

    for i in range(N_ALERTAS):
        alerta = generar_alerta_variada(rng)
        publicada_en = ahora - timedelta(
            days=rng.uniform(0, DIAS_HISTORIA),
            hours=rng.uniform(0, 24),
        )
        destinatarios = rng.sample(usuarios, min(USUARIOS_POR_ALERTA, len(usuarios)))

        session.execute(ins_alerta, (
            alerta["alerta_id"], alerta["titulo"], alerta["cuerpo"],
            alerta["severidad"], alerta["zona_destino"], publicada_en,
            len(destinatarios),
        ))

        tasa_fallo = TASA_FALLO_POR_SEVERIDAD[alerta["severidad"]]
        for usuario_id in destinatarios:
            enviado_en = publicada_en + timedelta(seconds=rng.uniform(0, 5))
            session.execute(ins_historial, (
                usuario_id, enviado_en, alerta["alerta_id"],
                alerta["titulo"], alerta["cuerpo"], alerta["severidad"],
                alerta["zona_destino"],
            ))

            fallo = rng.random() < tasa_fallo
            if fallo:
                estado_envio = "fallido"
                intentos = rng.randint(2, 4)
            else:
                estado_envio = rng.choices(
                    ["entregado", "leido"], weights=[35, 65])[0]
                intentos = 1

            bucket = hash(usuario_id) % 32
            actualizado = enviado_en + timedelta(seconds=rng.uniform(1, 120))
            session.execute(ins_entrega, (
                alerta["alerta_id"], bucket, usuario_id, estado_envio,
                intentos, actualizado,
            ))
            total_entregas += 1

        if (i + 1) % 10 == 0:
            print(f"  {i+1}/{N_ALERTAS} alertas publicadas "
                  f"({total_entregas:,} entregas registradas)")

    cluster.shutdown()
    print(f"\nListo: {N_ALERTAS} alertas, {total_entregas:,} filas en "
          f"historial_usuario y estado_entrega.")


if __name__ == "__main__":
    main()
