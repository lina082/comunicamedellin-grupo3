#!/usr/bin/env python3
"""
Taller 3 - Paso 3: limpieza Silver.

Silver no aplica reglas de negocio, solo calidad: tipos correctos,
nulos resueltos, duplicados eliminados. Lee el Bronze mas reciente de
cada tabla (glob con *) y escribe un Parquet limpio por tabla.
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


def reporte_calidad(nombre, antes, despues):
    print(f"\n=== REPORTE DE CALIDAD — {nombre.upper()} ===")
    print(f"Filas Bronze originales : {len(antes):>10,}")
    print(f"Filas Silver resultantes: {len(despues):>10,}")
    print(f"Filas eliminadas        : {len(antes) - len(despues):>10,}")
    pct = (len(despues) / len(antes) * 100) if len(antes) else 0.0
    print(f"Porcentaje retenido     : {pct:>9.1f}%")


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


def limpiar_alertas():
    df_raw = leer_bronze("alertas")
    df = df_raw.copy()
    df["alerta_id"] = df["alerta_id"].astype(str)
    df["severidad"] = df["severidad"].fillna("info")
    df["zona_destino"] = df["zona_destino"].fillna("desconocida")
    df["publicada_en"] = pd.to_datetime(df["publicada_en"], utc=True)
    df = df.dropna(subset=["alerta_id", "publicada_en"])
    df = df.drop_duplicates(subset=["alerta_id"])
    reporte_calidad("alertas", df_raw, df)
    df.to_parquet(SILVER / "alertas_silver.parquet", index=False)
    return df


def limpiar_historial():
    df_raw = leer_bronze("historial_usuario")
    df = df_raw.copy()
    df["usuario_id"] = df["usuario_id"].astype(str).str.strip()
    df["alerta_id"] = df["alerta_id"].astype(str)
    df["enviado_en"] = pd.to_datetime(df["enviado_en"], utc=True)
    df["severidad"] = df["severidad"].fillna("info")
    df["zona"] = df["zona"].fillna("desconocida")
    df = df.dropna(subset=["usuario_id", "alerta_id", "enviado_en"])
    df = df.drop_duplicates(subset=["usuario_id", "alerta_id", "enviado_en"])
    reporte_calidad("historial_usuario", df_raw, df)
    df.to_parquet(SILVER / "historial_usuario_silver.parquet", index=False)
    return df


def limpiar_estado_entrega():
    df_raw = leer_bronze("estado_entrega")
    df = df_raw.copy()
    df["usuario_id"] = df["usuario_id"].astype(str).str.strip()
    df["alerta_id"] = df["alerta_id"].astype(str)
    df["estado"] = df["estado"].fillna("enviado")
    df["intentos"] = pd.to_numeric(df["intentos"], errors="coerce").fillna(1).astype(int)
    df["actualizado"] = pd.to_datetime(df["actualizado"], utc=True)
    df = df.dropna(subset=["usuario_id", "alerta_id", "actualizado"])
    df = df.drop_duplicates(subset=["alerta_id", "bucket", "usuario_id"])
    reporte_calidad("estado_entrega", df_raw, df)
    df.to_parquet(SILVER / "estado_entrega_silver.parquet", index=False)
    return df


def main():
    limpiar_presencia()
    limpiar_alertas()
    limpiar_historial()
    limpiar_estado_entrega()
    print("\nSilver completo.")

    reporte_path = BASE / "docs" / "reporte_calidad.txt"
    reporte_path.write_text(_buffer.getvalue(), encoding="utf-8")


if __name__ == "__main__":
    main()
