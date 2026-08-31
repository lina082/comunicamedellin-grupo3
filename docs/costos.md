# Análisis de costos

No lo pide el enunciado del Equipo 3, pero el módulo lista el **TCO** entre los
criterios de selección de arquitectura (Sec. 3.4), y es una pregunta probable del
jurado.

Los volúmenes salen de los parámetros del caso; las tasas de compresión y
throughput, de mediciones sobre nuestro clúster.

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

## Infraestructura y costo anual

| Componente | Instancias | Tipo | USD/mes |
|---|---|---|---|
| Cassandra | 3 | `m6g.large` | 186 |
| Kafka | 3 | `m6g.large` | 186 |
| ClickHouse | 2 | `m6g.xlarge` | 248 |
| API y workers | 3 | `t4g.medium` | 75 |
| Disco `gp3` | — | 2,2 TB | 220 |
| Backups S3 | — | 2,2 TB | 51 |
| Red | — | entre-AZ y salida | 200 |
| **Total** | | | **1.166** |

**≈ 14.000 USD/año** de infraestructura.

- Por suscriptor: **0,14 USD/año**
- Por alerta enviada: **1,92 USD**

## Comparativa

| Opción | USD/año | Observación |
|---|---|---|
| Servidor único grande | 8.592 | SPOF, y **no resuelve el fan-out**: entrega en serie |
| **Nuestra arquitectura** | **13.987** | Sin SPOF, escala añadiendo nodos |
| Equivalente gestionado | 38.400 | Astra DB + MSK + ClickHouse Cloud |

Añadiendo medio SRE dedicado (~30.000 USD/año en Colombia), el **TCO a 3 años
ronda los 132.000 USD**.

## El argumento

La arquitectura distribuida cuesta **1,6 veces** el servidor único, no diez. Ese
sobrecosto compra tres cosas que el servidor grande no puede dar a ningún precio:

1. **Sin punto único de falla** — si cae un nodo, el canal sigue operando
2. **Fan-out real** — el servidor único entrega en serie, y con 100.000
   destinatarios el último espera minutos
3. **Escalado horizontal** — crecer es añadir nodos, no migrar a una máquina mayor

La opción gestionada es 2,7× más cara, pero elimina la operación. Para una
alcaldía sin equipo de plataforma dedicado, puede salir a cuenta.

## Cómo reducirlo

- **TTL agresivo en presencia**: el estado no necesita historial
- **Tiering en ClickHouse**: mover a S3 lo que pase de 90 días
- **Instancias `spot` para los workers**: son sin estado, toleran interrupción
- **Comprimir el heartbeat**: pasar de 1/min a 1/5 min reduce el 80 % de los
  eventos analíticos, que son el componente dominante
