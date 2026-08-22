#!/usr/bin/env bash
# ============================================================
# Simula latencia de red entre nodos (Medellin <-> Miami)
# ============================================================
#
# POR QUE ESTO ES NECESARIO
#
# Los 3 nodos corren en la misma maquina: la replicacion tarda
# microsegundos y converge antes de que podamos leer. Sin latencia
# real, la ventana de inconsistencia de ONE es invisible y la demo
# no muestra nada.
#
# En produccion esa latencia existe siempre (aqui: dos centros de
# datos). La introducimos con tc netem, que es la herramienta
# estandar de Linux para emular condiciones de red.
#
# El retardo es ASIMETRICO a proposito:
#   - puerto 7000 (replicacion entre nodos) -> 250 ms de retardo
#   - puerto 9042 (lecturas del cliente)    -> sin retardo
#
# Asi el cliente lee rapido de un nodo que aun no recibio la
# escritura, que es exactamente el escenario que ONE no protege.
#
# Uso:
#   ./demo/scripts/latencia_red.sh on    # activar
#   ./demo/scripts/latencia_red.sh off   # quitar
#   ./demo/scripts/latencia_red.sh show  # ver reglas
# ============================================================
set -euo pipefail

NODO="${NODO:-cass3}"
RETARDO="${RETARDO:-250ms}"

activar() {
  docker exec --privileged "$NODO" tc qdisc del dev eth0 root 2>/dev/null || true
  docker exec --privileged "$NODO" tc qdisc add dev eth0 root handle 1: prio
  docker exec --privileged "$NODO" tc qdisc add dev eth0 parent 1:3 handle 30: \
      netem delay "$RETARDO"
  for dir in dport sport; do
    docker exec --privileged "$NODO" tc filter add dev eth0 protocol ip \
        parent 1:0 prio 3 u32 match ip "$dir" 7000 0xffff flowid 1:3
  done
  echo "Retardo de $RETARDO aplicado a la replicacion de $NODO (puerto 7000)."
  echo "Las lecturas del cliente (9042) no se ven afectadas."
}

case "${1:-on}" in
  on)   activar ;;
  off)  docker exec --privileged "$NODO" tc qdisc del dev eth0 root 2>/dev/null \
          && echo "Retardo eliminado de $NODO." || echo "No habia reglas activas." ;;
  show) docker exec --privileged "$NODO" tc qdisc show dev eth0 ;;
  *)    echo "Uso: $0 {on|off|show}"; exit 1 ;;
esac
