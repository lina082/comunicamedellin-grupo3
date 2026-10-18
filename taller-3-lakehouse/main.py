#!/usr/bin/env python3
"""main.py — Pipeline completo del Mini-Lakehouse de ComunicaMedellin."""
import subprocess
import sys
import time

PASOS = [
    ("Extracción → Bronze", "scripts/extract.py"),
    ("Limpieza → Silver", "scripts/transform_silver.py"),
    ("Modelo → Gold", "scripts/transform_gold.py"),
    ("Análisis y Benchmark", "scripts/analyze.py"),
    ("Reporte de negocio", "scripts/reporte_negocio.py"),
]


def main():
    print("=" * 60)
    print(" MINI-LAKEHOUSE — ComunicaMedellín")
    print("=" * 60)

    t_total = time.time()
    for nombre, script in PASOS:
        print(f"\n>>> {nombre}")
        t0 = time.time()
        result = subprocess.run([sys.executable, script])
        if result.returncode != 0:
            print(f"ERROR en {script}. Pipeline detenido.")
            sys.exit(1)
        print(f"  Completado en {time.time() - t0:.1f}s")

    print(f"\nPipeline completo en {time.time() - t_total:.1f}s")
    print("Archivos Gold disponibles en: lakehouse/gold/")


if __name__ == "__main__":
    main()
