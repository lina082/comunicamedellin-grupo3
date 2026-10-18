#!/usr/bin/env python3
"""
Taller 3 - Paso 4: modelo dimensional Gold.

Esquema en constelacion (dos hechos comparten dimensiones conformadas):

  FACT_ENTREGAS       — 1 fila = 1 intento de entrega de 1 alerta a 1 usuario
  FACT_PRESENCIA_HORA — 1 fila = 1 usuario x 1 hora de reloj (snapshot)

  DIM_TIEMPO   — conformada, comparten ambos hechos
  DIM_USUARIO  — conformada
  DIM_ZONA     — conformada
  DIM_ALERTA   — solo FACT_ENTREGAS

Ver docs/modelo_dimensional.md para la justificacion de grano,
dimensiones y clasificacion de metricas (Paso 1 del taller).
"""
from pathlib import Path

import duckdb
import pandas as pd

BASE = Path(__file__).resolve().parent.parent
SILVER = BASE / "lakehouse" / "silver"
GOLD = BASE / "lakehouse" / "gold"
GOLD.mkdir(parents=True, exist_ok=True)

con = duckdb.connect()


def leer_silver(nombre: str) -> pd.DataFrame:
    return con.execute(
        f"SELECT * FROM read_parquet('{SILVER}/{nombre}_silver.parquet')"
    ).fetchdf()


def a_naive(serie: pd.Series) -> pd.Series:
    """Quita el timezone (si lo tiene) para poder comparar/mergear
    columnas datetime que llegan con tz distinto segun la fuente
    (Cassandra devuelve America/Bogota, DuckDB a veces UTC naive)."""
    serie = pd.to_datetime(serie)
    if getattr(serie.dt, "tz", None) is not None:
        serie = serie.dt.tz_localize(None)
    return serie


# ══════════════════════════════════════════════════════════════════
# DIM_TIEMPO — generada del rango de fechas combinado de ambos hechos
# ══════════════════════════════════════════════════════════════════
def construir_dim_tiempo(fecha_min, fecha_max):
    fecha_min, fecha_max = a_naive(pd.Series([fecha_min, fecha_max]))
    fechas = pd.date_range(start=fecha_min.normalize(), end=fecha_max.normalize(), freq="D")
    dim_tiempo = pd.DataFrame({
        "tiempo_sk": range(1, len(fechas) + 1),
        "fecha": fechas,
        "anio": fechas.year,
        "trimestre": fechas.quarter,
        "mes": fechas.month,
        "mes_nombre": fechas.strftime("%B"),
        "dia": fechas.day,
        "dia_semana": fechas.dayofweek,  # 0 = lunes
        "dia_nombre": fechas.strftime("%A"),
        "es_fin_semana": fechas.dayofweek >= 5,
    })
    dim_tiempo.to_parquet(GOLD / "dim_tiempo.parquet", index=False)
    print(f"DIM_TIEMPO: {len(dim_tiempo):,} fechas")
    return dim_tiempo


# ══════════════════════════════════════════════════════════════════
# DIM_USUARIO — SCD Tipo 1 (sobreescribe; el dato de perfil es volatil
# porque 'presencia' se reescribe constantemente, no hay historial que
# conservar para zona/comuna/dispositivo en el modelo operacional actual)
# ══════════════════════════════════════════════════════════════════
def construir_dim_usuario(presencia):
    dim_usuario = presencia[["usuario_id", "nombre", "zona", "comuna", "dispositivo"]].copy()
    dim_usuario = dim_usuario.drop_duplicates(subset=["usuario_id"])
    dim_usuario.insert(0, "usuario_sk", range(1, len(dim_usuario) + 1))
    dim_usuario.to_parquet(GOLD / "dim_usuario.parquet", index=False)
    print(f"DIM_USUARIO: {len(dim_usuario):,} usuarios")
    return dim_usuario


# ══════════════════════════════════════════════════════════════════
# DIM_ZONA — conformada entre los dos hechos. Tipo 1, 3 zonas fijas.
# ══════════════════════════════════════════════════════════════════
def construir_dim_zona(presencia):
    zonas = sorted(presencia["zona"].dropna().unique())
    dim_zona = pd.DataFrame({
        "zona_sk": range(1, len(zonas) + 1),
        "zona": zonas,
    })
    dim_zona.to_parquet(GOLD / "dim_zona.parquet", index=False)
    print(f"DIM_ZONA: {len(dim_zona):,} zonas")
    return dim_zona


# ══════════════════════════════════════════════════════════════════
# DIM_ALERTA — Tipo 0, una alerta publicada no cambia de contenido.
# ══════════════════════════════════════════════════════════════════
def construir_dim_alerta(alertas):
    dim_alerta = alertas[["alerta_id", "titulo", "severidad", "zona_destino"]].copy()
    dim_alerta = dim_alerta.drop_duplicates(subset=["alerta_id"])
    dim_alerta.insert(0, "alerta_sk", range(1, len(dim_alerta) + 1))
    dim_alerta.to_parquet(GOLD / "dim_alerta.parquet", index=False)
    print(f"DIM_ALERTA: {len(dim_alerta):,} alertas")
    return dim_alerta


# ══════════════════════════════════════════════════════════════════
# FACT_ENTREGAS — grano: 1 fila = 1 intento de entrega (alerta, usuario)
#   monto_aditivo    : intentos (ADITIVA)
#   flag aditivo      : entregado_flag, fallido_flag (ADITIVA, 0/1)
#   no aditiva        : latencia_entrega_seg (solo tiene sentido AVG)
# ══════════════════════════════════════════════════════════════════
def construir_fact_entregas(estado_entrega, historial, dim_tiempo, dim_usuario, dim_zona, dim_alerta):
    df = estado_entrega.merge(
        historial[["usuario_id", "alerta_id", "enviado_en", "zona"]],
        on=["usuario_id", "alerta_id"], how="left",
    )
    df["actualizado"] = a_naive(df["actualizado"])
    df["enviado_en"] = a_naive(df["enviado_en"])
    df["fecha_solo"] = df["actualizado"].dt.normalize()

    tiempo_lookup = dim_tiempo[["tiempo_sk", "fecha"]].copy()
    tiempo_lookup["fecha"] = a_naive(tiempo_lookup["fecha"])
    df = df.merge(tiempo_lookup, left_on="fecha_solo", right_on="fecha", how="left")

    df = df.merge(dim_usuario[["usuario_sk", "usuario_id"]], on="usuario_id", how="left")
    df = df.merge(dim_zona[["zona_sk", "zona"]], on="zona", how="left")
    df = df.merge(dim_alerta[["alerta_sk", "alerta_id"]], on="alerta_id", how="left")

    df["entregado_flag"] = df["estado"].isin(["entregado", "leido"]).astype(int)
    df["fallido_flag"] = (df["estado"] == "fallido").astype(int)
    df["latencia_entrega_seg"] = (df["actualizado"] - df["enviado_en"]).dt.total_seconds()
    df.loc[df["latencia_entrega_seg"] < 0, "latencia_entrega_seg"] = pd.NA

    fact = df[[
        "tiempo_sk", "usuario_sk", "alerta_sk", "zona_sk",
        "estado", "intentos", "entregado_flag", "fallido_flag",
        "latencia_entrega_seg",
    ]].copy()
    fact.insert(0, "entrega_sk", range(1, len(fact) + 1))

    fact.to_parquet(GOLD / "fact_entregas.parquet", index=False)
    print(f"FACT_ENTREGAS: {len(fact):,} filas")
    return fact


# ══════════════════════════════════════════════════════════════════
# FACT_PRESENCIA_HORA — grano: 1 fila = 1 usuario x 1 hora de reloj.
# 'presencia' en Cassandra es un snapshot (sin historial de cambios de
# estado), asi que el hecho se construye proyectando el estado actual
# de cada usuario sobre la hora de su ultimo heartbeat (last_seen).
# Es la limitacion real de modelar presencia sin un stream de eventos
# (ver docs/modelo_dimensional.md, Pregunta 5).
#   aditiva     : eventos (siempre 1 por fila, agregable por conteo)
#   aditiva 0/1 : online_flag
#   no aditiva  : lo que se promedia es la PROPORCION online, no se suma
# ══════════════════════════════════════════════════════════════════
def construir_fact_presencia_hora(presencia, dim_tiempo, dim_usuario, dim_zona):
    df = presencia.copy()
    df["last_seen"] = a_naive(df["last_seen"])
    df["hora"] = df["last_seen"].dt.floor("h")
    df["fecha_solo"] = df["hora"].dt.normalize()

    tiempo_lookup = dim_tiempo[["tiempo_sk", "fecha"]].copy()
    tiempo_lookup["fecha"] = a_naive(tiempo_lookup["fecha"])
    df = df.merge(tiempo_lookup, left_on="fecha_solo", right_on="fecha", how="left")

    df = df.merge(dim_usuario[["usuario_sk", "usuario_id"]], on="usuario_id", how="left")
    df = df.merge(dim_zona[["zona_sk", "zona"]], on="zona", how="left")

    df["hora_del_dia"] = df["hora"].dt.hour
    df["online_flag"] = (df["estado"] == "online").astype(int)
    df["eventos"] = 1

    fact = df[[
        "tiempo_sk", "usuario_sk", "zona_sk", "hora_del_dia",
        "online_flag", "eventos",
    ]].copy()
    fact.insert(0, "presencia_sk", range(1, len(fact) + 1))

    fact.to_parquet(GOLD / "fact_presencia_hora.parquet", index=False)
    print(f"FACT_PRESENCIA_HORA: {len(fact):,} filas")
    return fact


def main():
    presencia = leer_silver("presencia")
    alertas = leer_silver("alertas")
    historial = leer_silver("historial_usuario")
    estado_entrega = leer_silver("estado_entrega")

    fecha_min = min(alertas["publicada_en"].min(), presencia["last_seen"].min())
    fecha_max = max(estado_entrega["actualizado"].max(), presencia["last_seen"].max())

    dim_tiempo = construir_dim_tiempo(fecha_min, fecha_max)
    dim_usuario = construir_dim_usuario(presencia)
    dim_zona = construir_dim_zona(presencia)
    dim_alerta = construir_dim_alerta(alertas)

    construir_fact_entregas(estado_entrega, historial, dim_tiempo, dim_usuario, dim_zona, dim_alerta)
    construir_fact_presencia_hora(presencia, dim_tiempo, dim_usuario, dim_zona)

    print("\nGold completo.")


if __name__ == "__main__":
    main()
