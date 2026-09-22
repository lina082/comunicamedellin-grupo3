# Repaso rápido — 20 minutos antes de exponer

> Todo lo de aquí está verificado contra `docs/modelo-datos.pdf` y las corridas
> reales del clúster. Si algo no te suena, está en el informe con más detalle.

---

## En una frase

El Módulo 1 respondió **qué motor** (Cassandra). Este módulo responde
**qué modelo**: cómo guardar los datos para enviar una alerta a 100.000 personas
sin saturar el sistema, y qué pasa cuando algo falla.

---

## Los 6 números que hay que recordar

| Número | Qué es | Dónde lo dices |
|---|---|---|
| **5 consultas → 7 tablas** | dos consultas no tenían tabla; hubo que crearlas | slide 3 |
| **1.909 KB vs 71,7 KB** | 1 partición gigante vs 32 repartidas (hotspot) | slide 7 |
| **7.509 entregas/s** | fan-out medido; p99 de 156 ms | slide 6 |
| **1 fila vs 3 filas** | idempotencia: timestamp del evento vs del worker | slide 8 |
| **1.999 / 0 / 1.999** | el hueco del nodo caído, antes de reparar | slide 9 |
| **45,9 GB al año** | almacenamiento con RF=3 → 15 GB por nodo | slide 6 |

Si te preguntan algo y no recuerdas el número exacto: **di el orden de magnitud y
que está medido en el clúster**. Es preferible a inventar una cifra.

---

## Las 5 ideas, en orden de exposición

### 1. El modelo sale de las consultas (slides 2–3)
En SQL modelas entidades y después consultas lo que quieras. En Cassandra al
revés: los datos se reparten por el *hash* de la clave de partición, así que solo
puedes buscar rápido **por esa clave**.

> Al escribir nuestras 5 consultas, **dos no tenían tabla que las respondiera**.
> La solución en Cassandra no es un índice: es otra tabla. Por eso pasamos de 3 a 7.

### 2. La clave se elige por distribución, no por entidad (slide 7)
Lo natural sería particionar `estado_entrega` por `alerta_id`. **Es el error.**

> 3,73 MB caben de sobra en el límite de 100 MB. **El problema no es el tamaño**:
> es que las 100.000 escrituras van a *un* punto del anillo. Con 3 nodos no se
> nota porque todos son réplica de todo. Con 12 nodos: 3 saturados, 9 ociosos.

### 3. La consistencia se decide en cada consulta (slide 5)
Lo que se configura una vez es la replicación (RF=3). El nivel se elige por operación.

- `presencia` → **ONE**: que llegue 2 segundos tarde no cambia ninguna decisión
- `historial` → **QUORUM** al escribir: es seguridad pública, no se puede perder

> El historial escribe con QUORUM pero lee con ONE. Es intencional: necesitamos
> **durabilidad**, no **lectura inmediata**.

### 4. La idempotencia se diseña, no se añade (slide 8)
Kafka entrega *al menos una vez*: si un worker muere antes de confirmar, reprocesa.

> Basta con que el timestamp venga **del evento** y no de `now()` en el worker.
> Misma clave primaria → Cassandra sobrescribe → 1 fila. Si lo genera el worker,
> cada reintento crea una fila nueva. **Basta 1 ms de diferencia para romperlo.**

### 5. El hueco de un nodo caído es invisible (slide 9)
Tumbamos un nodo, escribimos 2.000 particiones sin él, lo devolvimos.

> El hueco **no se ve con un SELECT**: con RF=3 sobre 3 nodos los tres son réplica
> de todo, así que la lectura la sirve cualquier réplica viva y devuelve el dato
> correcto. **Consistencia eventual no significa que el cliente lea mal: significa
> que las réplicas divergen por dentro mientras el servicio responde bien.**

---

## La demo: 4 comandos

Antes de exponer (tarda ~5 min en total):

```bash
make up && make schema && make seed && make ch-seed
make status      # confirmar que los 3 nodos dicen UN
```

| Comando | Qué se ve | Dura |
|---|---|---|
| `make demo-fanout` | 10.000 entregas, throughput y latencia | ~15 s |
| `make demo-idempotencia` | 1 fila vs 3 filas | ~10 s |
| `make demo-reparacion` | el hueco y la reparación | **~3 min** |
| `make demo-analitica` | Cassandra rechaza el GROUP BY | ~20 s |

**`demo-reparacion` tarda 3 minutos porque reinicia un nodo de verdad.**
Lánzalo en otra terminal cuando empiece el bloque anterior.

> Si un nodo no arranca y dice `A node with address ... already exists`, es
> colisión de IPs de Docker: `make clean && make up`.

---

## Si preguntan

**¿Por qué no CockroachDB o MongoDB?**
Los dos son CP. CockroachDB sirve para TuBoleta, donde dos personas compiten por
la misma silla. Aquí nadie compite por un mensaje: pagaríamos consenso en cada
*heartbeat* sin ganar nada.

**¿Por qué RF=3 y no 2?**
Con RF=2 el quórum sigue siendo 2, o sea todas las réplicas: perder un nodo
bloquearía las escrituras. RF=3 es el mínimo que tolera una caída.

**¿El fan-out no satura Cassandra?**
A 7.509/s una alerta tarda ~13 s. Por eso es **asíncrono**: la Alcaldía recibe
confirmación cuando la alerta entra en Kafka, no cuando terminan las escrituras.

**¿Y si hay que cancelar una alerta ya enviada?**
No se borran 100.000 filas. Un DELETE escribe un *tombstone* que dura 10 días y
que toda lectura debe descartar. Se marca `cancelada=true` en `alertas` (una sola
escritura) y el cliente filtra al leer.

**¿Por qué incluyeron `estado_entrega` si era opcional?**
Porque es donde se demuestra el hotspot del bucketing ×32. Sin esa tabla no
habría dónde mostrar ese hallazgo.

**¿Un usuario nuevo recibe las alertas viejas?**
No: su fila nunca se escribió. Es consecuencia del *fan-out-on-write* y es el
comportamiento deseado. Si hiciera falta, se relee Kafka desde el *offset*.
