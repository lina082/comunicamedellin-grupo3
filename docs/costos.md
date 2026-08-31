# Análisis de costos

No lo pide el enunciado del Equipo 3, pero el módulo lista el **TCO** entre los
criterios de selección de arquitectura (Sec. 3.4), y es una pregunta probable del
jurado.

Los volúmenes salen de los parámetros del caso; las tasas de compresión y
throughput, de mediciones sobre nuestro clúster.

> Cifras en pesos con TRM del **31 de agosto de 2026: $3.202,79 COP/USD**.

## Supuestos

| Parámetro | Valor |
|---|---|
| Suscriptores | 100.000 |
| Alertas publicadas | 20/día |
| Heartbeat de presencia | 1/minuto por usuario |
| Retención del historial | 90 días (TTL) |
| Factor de replicación | 3 |
| Precios | AWS `sa-east-1`, reservadas 1 año |

## Volumen de datos

| Dato | Cálculo | Resultado |
|---|---|---|
| Presencia | 100K × 200 B × RF 3 | **60 MB** |
| Historial | 100K × 20 × 400 B × RF 3 × 90 d | **0,22 TB** |
| Eventos analíticos | 144 M/día × 25 B comprimidos | **1,31 TB/año** |

El historial no crece indefinidamente porque el TTL lo estabiliza. Los eventos
analíticos sí, y son el componente que domina el almacenamiento.

## Infraestructura mensual

| Componente | Cantidad | Tipo | USD | COP |
|---|---|---|---|---|
| Cassandra | 3 | `m6g.large` | 186 | $595.719 |
| Kafka | 3 | `m6g.large` | 186 | $595.719 |
| ClickHouse | 2 | `m6g.xlarge` | 248 | $794.292 |
| API y workers | 3 | `t4g.medium` | 75 | $240.209 |
| Disco `gp3` | — | 2,2 TB | 220 | $704.614 |
| Backups S3 | — | 2,2 TB | 51 | $163.342 |
| Red | — | entre-AZ y salida | 200 | $640.558 |
| **Total mensual** | | | **1.166** | **$3,7 M** |
| **Total anual** | | | **13.992** | **$44,8 M** |

## Por unidad

| | USD | COP |
|---|---|---|
| Por suscriptor al año | 0,14 | **$448** |
| Por alerta enviada | 1,92 | $6.139 |

Menos de **500 pesos al año por ciudadano** conectado al canal.

## Comparativa anual

| Opción | USD | COP | Observación |
|---|---|---|---|
| Servidor único grande | 8.592 | $27,5 M | SPOF, y **no resuelve el fan-out** |
| **Nuestra arquitectura** | **13.987** | **$44,8 M** | Sin SPOF, escala añadiendo nodos |
| Equivalente gestionado | 38.400 | $123,0 M | Astra DB + MSK + ClickHouse Cloud |

## TCO a 3 años

| Concepto | COP |
|---|---|
| Infraestructura | $134,4 M |
| Medio SRE (~$120 M/año) | $360,0 M |
| **Total** | **$494,4 M** |

El personal cuesta **2,7 veces más que la infraestructura**. Es el argumento real
a favor de la opción gestionada: no ahorra servidores, ahorra horas de operación.

## El argumento

La arquitectura distribuida cuesta **1,6 veces** el servidor único, no diez. Ese
sobrecosto —unos **$17 M COP al año**— compra tres cosas que el servidor grande no
puede dar a ningún precio:

1. **Sin punto único de falla** — si cae un nodo, el canal sigue operando
2. **Fan-out real** — el servidor único entrega en serie, y con 100.000
   destinatarios el último espera minutos
3. **Escalado horizontal** — crecer es añadir nodos, no migrar a una máquina mayor

Para una alcaldía, $44,8 M COP anuales es una fracción del presupuesto de
comunicaciones, y el canal opera justo cuando más se necesita.

## Cómo reducirlo

- **Bajar el heartbeat** de 1/min a 1/5 min: elimina el 80 % de los eventos
  analíticos, que son el componente dominante. Encaja con la decisión CAP —
  si la presencia ya tolera estar desactualizada, importa poco.
- **Tiering en ClickHouse**: mover a S3 lo que pase de 90 días
- **Instancias `spot` para los workers**: son sin estado, toleran interrupción
- **TTL agresivo en presencia**: el estado no necesita historial
