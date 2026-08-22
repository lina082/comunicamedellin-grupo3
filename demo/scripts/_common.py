"""Utilidades compartidas por los scripts de demo."""
import os, sys, time

ZONAS = ["norte", "centro", "sur"]
DISPOSITIVOS = ["android", "ios", "web"]

NODOS = {
    "cass1": ("127.0.0.1", 9042),
    "cass2": ("127.0.0.1", 9043),
    "cass3": ("127.0.0.1", 9044),
}

C = {
    "reset": "\033[0m", "bold": "\033[1m", "dim": "\033[2m",
    "rojo": "\033[31m", "verde": "\033[32m", "amarillo": "\033[33m",
    "azul": "\033[34m", "cyan": "\033[36m",
}

def color(txt, c):
    return f"{C.get(c,'')}{txt}{C['reset']}"

def titulo(txt):
    print()
    print(color("═" * 68, "cyan"))
    print(color(f"  {txt}", "bold"))
    print(color("═" * 68, "cyan"))

def seccion(txt):
    print()
    print(color(f"── {txt} " + "─" * max(0, 64 - len(txt)), "dim"))

def requiere_driver():
    try:
        import cassandra  # noqa: F401
    except ImportError:
        print(color("\nFalta el driver de Cassandra.", "rojo"))
        print("Instalalo con:\n")
        print("    pip install -r demo/requirements.txt\n")
        sys.exit(1)

def conectar(nodo="cass1", keyspace="comunica"):
    """Conecta a UN nodo especifico, sin balanceo, para que la demo
    de consistencia realmente lea del nodo que decimos."""
    requiere_driver()
    from cassandra.cluster import Cluster, ExecutionProfile, EXEC_PROFILE_DEFAULT
    from cassandra.policies import WhiteListRoundRobinPolicy
    host, port = NODOS[nodo]
    perfil = ExecutionProfile(
        load_balancing_policy=WhiteListRoundRobinPolicy([host]),
        request_timeout=30,
    )
    cluster = Cluster(
        [host], port=port,
        execution_profiles={EXEC_PROFILE_DEFAULT: perfil},
        protocol_version=4,
    )
    return cluster, cluster.connect(keyspace)

def esperar(seg, msg=""):
    if msg:
        print(color(f"  {msg}", "dim"))
    time.sleep(seg)
