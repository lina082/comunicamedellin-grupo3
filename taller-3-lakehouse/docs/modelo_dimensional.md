# Modelo Dimensional — Mini-Lakehouse ComunicaMedellín

Taller 3 · Paso 1 · Diseño del Data Warehouse (sin código)

---

## Pregunta 1 — ¿Cuál es el proceso de negocio que vamos a analizar?

Se modelan **dos** procesos de negocio porque el proyecto base (Módulo 1)
expone dos cargas con requisitos CAP opuestos (ver `README.md` del
proyecto):

1. **Entrega de alertas ciudadanas** — qué tan efectivo y rápido es el
   fan-out de una alerta a sus suscriptores.
2. **Presencia de usuarios** — comportamiento de conexión (online/offline)
   por zona y hora.

Son dos hechos distintos con dimensiones compartidas: un **esquema en
constelación**, no dos lakehouses separados.

## Pregunta 2 — ¿Cuál es la granularidad?

| Hecho | Granularidad (una fila =) |
|---|---|
| `FACT_ENTREGAS` | un intento de entrega de **una alerta** a **un usuario** |
| `FACT_PRESENCIA_HORA` | un snapshot de presencia de **un usuario** en **una hora** de reloj |

Ambas pasan la regla de oro: se describen en una frase corta sin usar
"depende".

**Limitación reconocida:** `presencia` en Cassandra es un snapshot (última
cédula escrita), no un stream de cambios de estado. `FACT_PRESENCIA_HORA`
se construye proyectando ese snapshot sobre la hora de `last_seen`, así que
hoy representa "última hora vista", no "cada hora que el usuario estuvo
conectado". Para un historial real de presencia haría falta materializar
los eventos de Kafka (`presencia-eventos`) en vez de leer solo la tabla
`presencia`. Documentado también en `transform_gold.py`.

## Pregunta 3 — ¿Cuáles son las dimensiones?

Esquema en constelación: `DIM_TIEMPO`, `DIM_USUARIO` y `DIM_ZONA` son
**conformadas** (compartidas por ambos hechos); `DIM_ALERTA` es exclusiva
de `FACT_ENTREGAS`.

| Dimensión | Atributos principales | ¿Cambia en el tiempo? (SCD) |
|---|---|---|
| `DIM_TIEMPO` | fecha, año, mes, trimestre, día, día_semana, es_fin_semana | No (Tipo 0) |
| `DIM_USUARIO` | usuario_id, nombre, zona, comuna, dispositivo | Sí — zona/comuna pueden cambiar si el usuario se muda (Tipo 1, simplificado) |
| `DIM_ZONA` | zona (norte / centro / sur) | No (Tipo 0) |
| `DIM_ALERTA` | alerta_id, título, severidad, zona_destino | No — una alerta publicada no se edita (Tipo 0) |

## Pregunta 4 — ¿Cuáles son las métricas (hechos)?

| Tabla de hechos | Métrica | Tipo | Por qué |
|---|---|---|---|
| `FACT_ENTREGAS` | `intentos` | Aditiva | se puede sumar por alerta, usuario o zona |
| `FACT_ENTREGAS` | `entregado_flag`, `fallido_flag` | Aditiva (0/1) | cuenta de éxitos/fallos al sumarse |
| `FACT_ENTREGAS` | `latencia_entrega_seg` | No aditiva | solo tiene sentido como AVG/percentil, nunca sumada entre usuarios |
| `FACT_PRESENCIA_HORA` | `eventos` | Aditiva | cuenta de snapshots |
| `FACT_PRESENCIA_HORA` | `online_flag` | Semi-aditiva | se puede sumar por zona/hora (da "usuarios online"), pero sumarlo en el tiempo no tiene sentido — por eso Q5 usa `AVG`/proporción, no `SUM` acumulado |

### De las métricas al valor de negocio

Tener las métricas correctas no es tener valor de negocio: hace falta
decidir cuál de ellas *resume* el producto y cuáles solo lo *explican*.
`scripts/reporte_negocio.py` aplica el framework North Star / apoyo /
vanity sobre estas mismas métricas de `FACT_ENTREGAS`:

- **North Star: % de entregas con `latencia_entrega_seg` ≤ 90s.**
  Construida sobre la métrica no aditiva de la tabla — justo la que
  "solo tiene sentido como percentil" es la que mejor resume si el
  sistema cumple su promesa (entregar rápido), no un conteo.
- **Apoyo:** `fallido_flag` agrupado por severidad (¿falla más donde
  más importa?), `latencia_entrega_seg` agrupado por zona (¿hay una
  zona lenta?), cruce con `online_flag` de `FACT_PRESENCIA_HORA`
  (¿a quién le llega rápido algo que igual no va a ver?).
- **Vanity:** conteos simples de `DIM_ALERTA`, `DIM_USUARIO` y filas
  de `FACT_ENTREGAS` — crecen solos con el tiempo, no dicen si el
  servicio funciona.

Detalle completo y resultados reales en el README del proyecto,
sección "Reporte de negocio".

## Pregunta 5 — Estrategia SCD

Dimensión elegida: **`DIM_USUARIO`**.

El modelo operacional (`presencia` en Cassandra) **no** versiona cambios
de zona/comuna: al actualizar la fila se pierde el valor anterior
(`UPDATE`-in-place, propio de un modelo clave-valor). Por eso en este
Mini-Lakehouse `DIM_USUARIO` se implementa como **SCD Tipo 1** (se
sobrescribe, sin historial): es la estrategia honesta dado lo que la
fuente realmente ofrece, no una elección cosmética.

```text
-- Si se quisiera Tipo 2 (historial real), habria que capturar el cambio
-- ANTES de que Cassandra sobreescriba la fila, por ejemplo consumiendo
-- el topico Kafka 'presencia-eventos' en vez de la tabla 'presencia':

usuario_sk | usuario_id | zona   | valido_desde | valido_hasta | activo
-----------|------------|--------|--------------|--------------|-------
1          | CC650279   | norte  | 2026-01-01   | 2026-06-30   | false
2          | CC650279   | centro | 2026-07-01   | 9999-12-31   | true
```

`DIM_ALERTA` se documenta como Tipo 0 porque, a diferencia de `DIM_USUARIO`,
el contenido de una alerta publicada nunca se modifica después de emitida
(es un evento inmutable, no un perfil).

---

## Diagrama del modelo en constelación

```
                         DIM_TIEMPO
                              │
              ┌───────────────┼───────────────┐
              │                                │
        FACT_ENTREGAS                  FACT_PRESENCIA_HORA
         entrega_sk                        presencia_sk
         tiempo_sk  ───────────────────────── tiempo_sk
         usuario_sk ───────┐    ┌──────────── usuario_sk
         alerta_sk         │    │              zona_sk
         zona_sk ──────────┼────┼───────────── (zona_sk)
         intentos          │    │              hora_del_dia
         entregado_flag    │    │              online_flag
         fallido_flag      │    │              eventos
         latencia_entrega_seg│  │
              │             │    │
              │        DIM_USUARIO
              │        usuario_sk
              │        usuario_id, nombre, zona, comuna, dispositivo
              │
        DIM_ALERTA                         DIM_ZONA
        alerta_sk                          zona_sk
        alerta_id, titulo, severidad,      zona
        zona_destino
```

Diagrama exportable a `dbdiagram.io` / draw.io: ver
`docs/modelo_dimensional.dbml` para el código fuente DBML listo para
pegar en https://dbdiagram.io.
