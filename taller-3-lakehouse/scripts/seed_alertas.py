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

SUCIEDAD DELIBERADA (Paso 3 de la guia: "un dato Silver debe poder ser
analizado sin sorpresas tecnicas" solo tiene sentido si Bronze llega
sucio de verdad). Se inyecta, con probabilidad controlada, exactamente
lo que Cassandra deja escribir sin violar su propio esquema -- nada
que el motor real rechazaria, nada fabricado fuera de Cassandra:

  - NULOS en campos de texto no-clave de particion: severidad,
    zona_destino, estado. Simula un productor que omitio un campo.
  - FORMATO INCONSISTENTE: severidad con mayusculas mezcladas y
    espacios sobrantes ('CRITICA ', ' Critica', 'critica'), como si
    distintos clientes (app/web/cajero) normalizaran distinto.
  - VALORES IMPOSIBLES pero del tipo correcto: intentos negativo,
    actualizado anterior a enviado_en. En un sistema AP con relojes de
    nodo independientes (Sec. 1.2 del Modulo 1) esto ocurre de verdad:
    dos escrituras concurrentes pueden registrarse fuera de orden.

  NO se inyectan duplicados: la PRIMARY KEY de cada tabla (ver
  infra/cql/schema.cql) hace que Cassandra sobreescriba cualquier
  reintento con la misma clave, asi que un duplicado real nunca
  llegaria a Bronze. Simularlo fuera de Cassandra (p.ej. duplicando
  filas en pandas dentro de extract.py) seria inventar un escenario
  que el motor real no produce.

Cada fila sucia se cuenta en SUCIEDAD_INYECTADA para poder verificar
despues, en el reporte de Silver, que el pipeline la detecto toda.

Uso:
    python scripts/seed_alertas.py
"""
import random
import sys
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "comunicamedellin-grupo3" / "demo" / "scripts"))
from _common import conectar  # noqa: E402
from _datos import PLANTILLAS_ALERTA, VIAS, RIOS, INSTITUCIONES, COMUNAS, ZONAS  # noqa: E402

N_ALERTAS = 60
USUARIOS_POR_ALERTA = 400          # muestra de suscriptores por alerta
DIAS_HISTORIA = 30
SEMILLA = 7

# Tasa de fallo base de entrega por severidad: una alerta critica se
# reintenta mas y por eso falla menos al final.
TASA_FALLO_POR_SEVERIDAD = {"critica": 0.02, "advertencia": 0.05, "info": 0.08}

# Probabilidades de cada tipo de suciedad, por fila de historial/entrega.
P_NULO_SEVERIDAD = 0.04
P_NULO_ZONA_DESTINO_ALERTA = 0.05   # por alerta, no por fila
P_NULO_ESTADO_ENTREGA = 0.03
P_FORMATO_SEVERIDAD_SUCIO = 0.10
P_INTENTOS_NEGATIVO = 0.02
P_RELOJ_DESINCRONIZADO = 0.025      # actualizado antes que enviado_en

VARIANTES_SUCIAS_SEVERIDAD = {
    "critica": ["CRITICA", " critica", "Critica "],
    "advertencia": ["ADVERTENCIA", " advertencia", "Advertencia"],
    "info": ["INFO", " info", "Info "],
}

SUCIEDAD_INYECTADA = {
    "nulo_severidad": 0, "nulo_zona_destino": 0, "nulo_estado": 0,
    "formato_sucio_severidad": 0,
    "intentos_negativo": 0, "reloj_desincronizado": 0,
}


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
        severidad_logica = alerta["severidad"]  # nunca se ensucia: fija la tasa de fallo real
        publicada_en = ahora - timedelta(
            days=rng.uniform(0, DIAS_HISTORIA),
            hours=rng.uniform(0, 24),
        )
        destinatarios = rng.sample(usuarios, min(USUARIOS_POR_ALERTA, len(usuarios)))

        # Suciedad de alerta: a veces el productor olvida la zona_destino.
        zona_destino_a_guardar = alerta["zona_destino"]
        if rng.random() < P_NULO_ZONA_DESTINO_ALERTA:
            zona_destino_a_guardar = None
            SUCIEDAD_INYECTADA["nulo_zona_destino"] += 1

        session.execute(ins_alerta, (
            alerta["alerta_id"], alerta["titulo"], alerta["cuerpo"],
            severidad_logica, zona_destino_a_guardar, publicada_en,
            len(destinatarios),
        ))

        tasa_fallo = TASA_FALLO_POR_SEVERIDAD[severidad_logica]
        for usuario_id in destinatarios:
            enviado_en = publicada_en + timedelta(seconds=rng.uniform(0, 5))

            # Suciedad de historial: severidad nula, o con formato sucio
            # (mayusculas/espacios), nunca ambas en la misma fila.
            severidad_a_guardar = severidad_logica
            if rng.random() < P_NULO_SEVERIDAD:
                severidad_a_guardar = None
                SUCIEDAD_INYECTADA["nulo_severidad"] += 1
            elif rng.random() < P_FORMATO_SEVERIDAD_SUCIO:
                severidad_a_guardar = rng.choice(VARIANTES_SUCIAS_SEVERIDAD[severidad_logica])
                SUCIEDAD_INYECTADA["formato_sucio_severidad"] += 1

            session.execute(ins_historial, (
                usuario_id, enviado_en, alerta["alerta_id"],
                alerta["titulo"], alerta["cuerpo"], severidad_a_guardar,
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

            # Suciedad de entrega: estado nulo, intentos negativo (bug de
            # contador), o reloj desincronizado entre nodos (Sec. 1.2:
            # consistencia eventual AP -> dos escrituras concurrentes
            # pueden quedar fuera de orden).
            if rng.random() < P_NULO_ESTADO_ENTREGA:
                estado_envio = None
                SUCIEDAD_INYECTADA["nulo_estado"] += 1
            if rng.random() < P_INTENTOS_NEGATIVO:
                intentos = -intentos
                SUCIEDAD_INYECTADA["intentos_negativo"] += 1

            actualizado = enviado_en + timedelta(seconds=rng.uniform(1, 120))
            if rng.random() < P_RELOJ_DESINCRONIZADO:
                actualizado = enviado_en - timedelta(seconds=rng.uniform(1, 30))
                SUCIEDAD_INYECTADA["reloj_desincronizado"] += 1

            bucket = hash(usuario_id) % 32
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
    print("\nSuciedad deliberada inyectada (para que Silver tenga que trabajar):")
    for tipo, n in SUCIEDAD_INYECTADA.items():
        print(f"  {tipo:<26} {n:>6,}")


if __name__ == "__main__":
    main()
