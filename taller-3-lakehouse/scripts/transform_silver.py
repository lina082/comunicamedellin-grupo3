#!/usr/bin/env python3
"""
Taller 3 - Paso 3: limpieza Silver.

Silver no aplica reglas de NEGOCIO (eso es Gold), pero si aplica reglas
de CALIDAD con criterio explicito, no un fillna() ciego:

  - Campo CRITICO sin el cual la fila no se puede interpretar de forma
    honesta (severidad, estado de entrega) -> se DESCARTA. No se
    inventa el valor porque inventarlo cambiaria el analisis (ej. una
    entrega sin estado no se puede contar como exitosa ni como
    fallida).
  - Campo NO CRITICO que SI se puede inferir de otra fuente del propio
    dataset (zona_destino de una alerta, a partir de la zona real de
    sus destinatarios en historial_usuario) -> se RELLENA cruzando
    tablas, nunca con un valor inventado desde cero.
  - FORMATO inconsistente (severidad con mayusculas/espacios) -> se
    NORMALIZA, no se descarta: el dato esta completo, solo mal escrito.
  - VALOR IMPOSIBLE del tipo correcto (intentos negativo, actualizado
    anterior a enviado_en) -> se DESCARTA y se reporta como error de
    reloj distribuido: en un sistema AP (Sec. 1.2, consistencia
    eventual) dos escrituras concurrentes pueden registrarse fuera de
    orden: no es basura aleatoria, es la naturaleza del sistema.

Cada regla queda contada por separado en el reporte, no solo como un
numero agregado de "filas eliminadas".
"""
import sys
from io import StringIO
from pathlib import Path

import duckdb
import pandas as pd

BASE = Path(__file__).resolve().parent.parent
BRONZE = BASE / "lakehouse" / "bronze"
SILVER = BASE / "lakehouse" / "silver"
SILVER.mkdir(parents=True, exist_ok=True)

SEVERIDADES_VALIDAS = ["critica", "advertencia", "info"]

con = duckdb.connect()
_buffer = StringIO()


class _Tee:
    """Escribe a stdout y a un buffer en paralelo, para poder
    imprimir el reporte en consola Y persistirlo en un archivo."""

    def __init__(self, *streams):
        self.streams = streams

    def write(self, data):
        for s in self.streams:
            s.write(data)

    def flush(self):
        for s in self.streams:
            s.flush()


sys.stdout = _Tee(sys.stdout, _buffer)


def leer_bronze(nombre: str) -> pd.DataFrame:
    """Lee solo el archivo Bronze MAS RECIENTE de la tabla.

    Cada corrida de extract.py agrega un nuevo Parquet con timestamp
    sin borrar los anteriores (Bronze es inmutable, Paso 2 de la guia).
    Si Silver leyera con un glob '*' sobre todos los snapshots,
    reprocesaria los mismos registros tantas veces como corridas de
    extract.py existan. Silver trabaja siempre sobre el snapshot mas
    reciente; los snapshots viejos quedan en Bronze como archivo
    historico/auditoria, no se usan en el pipeline.
    """
    archivos = sorted(BRONZE.glob(f"{nombre}_*.parquet"))
    if not archivos:
        raise FileNotFoundError(f"No hay Parquet Bronze para '{nombre}'. Corre extract.py primero.")
    mas_reciente = archivos[-1]
    return con.execute(f"SELECT * FROM read_parquet('{mas_reciente}')").fetchdf()


def normalizar_severidad(serie: pd.Series) -> pd.Series:
    """Formato sucio -> valor normalizado. No cuenta como fila perdida:
    el dato estaba completo, solo mal escrito (' CRITICA' -> 'critica')."""
    return serie.str.strip().str.lower()


def reporte_calidad(nombre, antes, despues, motivos=None):
    print(f"\n=== REPORTE DE CALIDAD — {nombre.upper()} ===")
    print(f"Filas Bronze originales : {len(antes):>10,}")
    print(f"Filas Silver resultantes: {len(despues):>10,}")
    print(f"Filas eliminadas        : {len(antes) - len(despues):>10,}")
    pct = (len(despues) / len(antes) * 100) if len(antes) else 0.0
    print(f"Porcentaje retenido     : {pct:>9.1f}%")
    if motivos:
        print("Motivo de cada eliminacion:")
        for motivo, n in motivos.items():
            if n:
                print(f"  {motivo:<42} {n:>8,}")


def limpiar_presencia():
    df_raw = leer_bronze("presencia")
    df = df_raw.copy()
    df["usuario_id"] = df["usuario_id"].astype(str).str.strip()
    df["estado"] = df["estado"].fillna("offline")
    df["zona"] = df["zona"].fillna("desconocida")
    df["comuna"] = df["comuna"].fillna("desconocida")
    df["last_seen"] = pd.to_datetime(df["last_seen"], utc=True)
    df = df.dropna(subset=["usuario_id", "last_seen"])
    df = df.drop_duplicates(subset=["usuario_id"])
    reporte_calidad("presencia", df_raw, df)
    df.to_parquet(SILVER / "presencia_silver.parquet", index=False)
    return df


def limpiar_alertas(historial_crudo: pd.DataFrame):
    df_raw = leer_bronze("alertas")
    df = df_raw.copy()
    df["alerta_id"] = df["alerta_id"].astype(str)
    df["publicada_en"] = pd.to_datetime(df["publicada_en"], utc=True)

    # zona_destino NO es critica: se puede inferir, porque cada fila de
    # historial_usuario de esa alerta YA trae su propia zona real (la
    # del destinatario). Si la mayoria de destinatarios cae en una
    # zona, esa es la zona_destino mas honesta que se puede reconstruir
    # sin inventar un dato nuevo.
    zona_moda_por_alerta = (
        historial_crudo.dropna(subset=["zona"])
        .groupby("alerta_id")["zona"]
        .agg(lambda s: s.mode().iat[0] if not s.mode().empty else None)
    )
    zona_moda_por_alerta.index = zona_moda_por_alerta.index.astype(str)
    n_zona_nula = df["zona_destino"].isna().sum()
    mascara_nula = df["zona_destino"].isna()
    df.loc[mascara_nula, "zona_destino"] = df.loc[mascara_nula, "alerta_id"].map(zona_moda_por_alerta)
    n_zona_inferida = n_zona_nula - df["zona_destino"].isna().sum()
    n_zona_sin_inferir = df["zona_destino"].isna().sum()
    df = df.dropna(subset=["zona_destino"])  # las pocas que ni cruzando se pudieron inferir

    df["severidad"] = normalizar_severidad(df["severidad"])
    n_severidad_nula = df["severidad"].isna().sum()
    df = df.dropna(subset=["alerta_id", "publicada_en", "severidad"])
    df = df.drop_duplicates(subset=["alerta_id"])

    motivos = {
        "severidad nula (critica, no se infiere)": n_severidad_nula,
        "zona_destino nula, inferida de historial_usuario": -n_zona_inferida,  # informativo, no resta filas
        "zona_destino nula, sin destinatarios para inferir": n_zona_sin_inferir,
    }
    reporte_calidad("alertas", df_raw, df, motivos)
    df.to_parquet(SILVER / "alertas_silver.parquet", index=False)
    return df


def limpiar_historial():
    df_raw = leer_bronze("historial_usuario")
    df = df_raw.copy()
    df["usuario_id"] = df["usuario_id"].astype(str).str.strip()
    df["alerta_id"] = df["alerta_id"].astype(str)
    df["enviado_en"] = pd.to_datetime(df["enviado_en"], utc=True)
    df["zona"] = df["zona"].fillna("desconocida")

    # severidad SI es critica en historial: es lo que clasifica cada
    # entrega para el reporte de negocio (Gold). Formato sucio se
    # normaliza; nulo real se descarta, no se adivina.
    df["severidad"] = normalizar_severidad(df["severidad"])
    n_severidad_nula = df["severidad"].isna().sum()

    df = df.dropna(subset=["usuario_id", "alerta_id", "enviado_en", "severidad"])
    df = df.drop_duplicates(subset=["usuario_id", "alerta_id", "enviado_en"])

    motivos = {"severidad nula (critica, no se infiere)": n_severidad_nula}
    reporte_calidad("historial_usuario", df_raw, df, motivos)
    df.to_parquet(SILVER / "historial_usuario_silver.parquet", index=False)
    return df


def limpiar_estado_entrega(historial_limpio: pd.DataFrame):
    df_raw = leer_bronze("estado_entrega")
    df = df_raw.copy()
    df["usuario_id"] = df["usuario_id"].astype(str).str.strip()
    df["alerta_id"] = df["alerta_id"].astype(str)
    df["actualizado"] = pd.to_datetime(df["actualizado"], utc=True)
    df["intentos"] = pd.to_numeric(df["intentos"], errors="coerce")

    # estado SI es critico: sin el no se puede clasificar la entrega
    # como exitosa o fallida, y adivinarlo sesgaria el benchmark/Gold.
    n_estado_nulo = df["estado"].isna().sum()

    # intentos negativo es imposible por definicion (no se puede
    # reintentar un numero negativo de veces) -> se descarta.
    n_intentos_negativo = (df["intentos"] < 0).sum()

    # Reloj distribuido: 'actualizado' no puede ser anterior al
    # 'enviado_en' real de esa entrega (cruce con historial_usuario,
    # que ya viene limpio). Si no hay match en historial (la fila de
    # historial tambien se descarto por sucia), no se puede validar y
    # se descarta por precaucion.
    hist = historial_limpio[["usuario_id", "alerta_id", "enviado_en"]].rename(
        columns={"enviado_en": "_enviado_en_ref"})
    df = df.merge(hist, on=["usuario_id", "alerta_id"], how="left")
    sin_referencia = df["_enviado_en_ref"].isna()
    reloj_invertido = (~sin_referencia) & (df["actualizado"] < df["_enviado_en_ref"])
    n_reloj_invertido = reloj_invertido.sum()
    n_sin_referencia = sin_referencia.sum()

    mascara_valida = (
        df["estado"].notna()
        & (df["intentos"].isna() | (df["intentos"] >= 0))
        & ~reloj_invertido
        & ~sin_referencia
    )
    df = df[mascara_valida].drop(columns=["_enviado_en_ref"])
    df["intentos"] = df["intentos"].fillna(1).astype(int)
    df = df.drop_duplicates(subset=["alerta_id", "bucket", "usuario_id"])

    motivos = {
        "estado nulo (critico, no se infiere)": n_estado_nulo,
        "intentos negativo (valor imposible)": n_intentos_negativo,
        "reloj distribuido: actualizado < enviado_en": n_reloj_invertido,
        "sin fila de historial para validar el reloj": n_sin_referencia,
    }
    reporte_calidad("estado_entrega", df_raw, df, motivos)
    df.to_parquet(SILVER / "estado_entrega_silver.parquet", index=False)
    return df


def main():
    limpiar_presencia()
    historial_crudo = leer_bronze("historial_usuario")
    limpiar_alertas(historial_crudo)
    historial_limpio = limpiar_historial()
    limpiar_estado_entrega(historial_limpio)
    print("\nSilver completo.")

    reporte_path = BASE / "docs" / "reporte_calidad.txt"
    reporte_path.write_text(_buffer.getvalue(), encoding="utf-8")


if __name__ == "__main__":
    main()
