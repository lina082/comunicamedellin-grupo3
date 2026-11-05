# Diagnostico baseline - Taller 4

## Row groups por archivo Gold

- `fact_entregas.parquet`: **1 row group(s)** para 106,370 filas
- `fact_presencia_hora.parquet`: **1 row group(s)** para 10,000 filas

## Q1 — Volumen diario de entregas por severidad - ultima semana (fecha >= 2026-10-12)

- Mediana (5 corridas, con warm-up de 3): **3.78 ms**
- Min: 3.57 ms | Max: 5.88 ms
- Plan completo: `Q1_plan_baseline.txt`

## Q3 — Entregas fallidas por severidad y zona de destino (estado = 'fallido')

- Mediana (5 corridas, con warm-up de 3): **3.14 ms**
- Min: 2.75 ms | Max: 4.87 ms
- Plan completo: `Q3_plan_baseline.txt`

## Q5 — Presencia online por hora en la zona Centro

- Mediana (5 corridas, con warm-up de 3): **0.91 ms**
- Min: 0.84 ms | Max: 1.01 ms
- Plan completo: `Q5_plan_baseline.txt`
