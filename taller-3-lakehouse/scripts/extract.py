#!/usr/bin/env python3
"""
Taller 3 - Paso 2: extraccion Bronze.

Copia las tablas de Cassandra (fuente OLTP de ComunicaMedellin) a
Parquet, tal cual estan, sin transformar. Bronze es inmutable: cada
corrida escribe un archivo nuevo con timestamp, nunca sobreescribe.
"""
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "demo" / "scripts"))
from _common import conectar  # noqa: E402

BRONZE = Path(__file__).resolve().parent.parent / "lakehouse" / "bronze"
BRONZE.mkdir(parents=True, exist_ok=True)

FECHA_EXTRACCION = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M")


def extraer_tabla(session, query: str, nombre: str) -> pd.DataFrame:
    print(f"Extrayendo {nombre}...")
    filas = list(session.execute(query))
    df = pd.DataFrame(filas)
    df["_extraido_en"] = datetime.now(timezone.utc)
    df["_fuente"] = "cassandra"
    ruta = BRONZE / f"{nombre}_{FECHA_EXTRACCION}.parquet"
    df.to_parquet(ruta, index=False)
    print(f"  -> {len(df):,} filas guardadas en {ruta.relative_to(BRONZE.parent.parent)}")
    return df


def main():
    cluster, session = conectar("cass1")

    extraer_tabla(session, "SELECT * FROM presencia", "presencia")
    extraer_tabla(session, "SELECT * FROM alertas", "alertas")
    extraer_tabla(session, "SELECT * FROM historial_usuario", "historial_usuario")
    extraer_tabla(session, "SELECT * FROM estado_entrega", "estado_entrega")

    cluster.shutdown()
    print("\nBronze completo.")


if __name__ == "__main__":
    main()
