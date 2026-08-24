# Panel ComunicaMedellín

Interfaz de la demo. Funciona en dos modos.

## Modo real (para la exposición)

Necesita el clúster corriendo:

```bash
make up && make schema && make seed   # una vez
make api                              # terminal 1
make frontend                         # terminal 2 → localhost:4173
```

Todo lo que ves ocurre de verdad: publica en Kafka, escribe en
Cassandra con QUORUM, y lee de los 3 nodos por separado.

## Modo demostración (Vercel)

Si la API no responde, el panel reproduce una **corrida real
grabada** (`demo-data.json`): 100 entregas en 3,05 s, capturadas
del clúster local. Se avisa en pantalla para no confundir.

Qué funciona sin clúster:
- Enviar alerta → reproduce el fan-out grabado
- Datos guardados, Esquema, Reparto → datos de la captura

Qué **no** funciona: la pestaña "Réplicas en vivo", porque lee
los 3 nodos en tiempo real y eso no se puede grabar.

### Volver a grabar la captura

Con el clúster arriba:

```bash
make grabar-demo
```

## Despliegue

```bash
cd frontend
vercel --prod
```

Es HTML estático: sin build, sin dependencias.
