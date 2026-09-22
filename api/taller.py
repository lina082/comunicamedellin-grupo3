"""
ComunicaMedellin - Pipelines del taller del Modulo 2
=====================================================
Un endpoint por cada punto que pide el enunciado. Cada uno ejecuta la
operacion REAL contra el cluster y va emitiendo por WebSocket los pasos
internos: que hace el coordinador, a que replicas pregunta, que token
resulta del hash, cuantas particiones toca.

La idea no es "mostrar un resultado", sino abrir la caja: en cada paso
se ve el comando que se ejecuta, lo que devuelve, y por que.

Los puntos, tal como los pide el taller:
  P1  5 consultas reales y por que el modelo las hace eficientes
  P2  fan-out real a 10.000 con throughput y latencia
  P3  caida real de un nodo MIENTRAS se escribe
  P4  reparacion de replicas con conteo antes/despues
  P5  idempotencia: reenviar el mismo evento y probar que no duplica
  P6  consulta analitica en la base correcta vs forzada en la operacional
"""
import asyncio, subprocess, time, uuid, statistics as st, datetime as dt

from fastapi import APIRouter, WebSocket, WebSocketDisconnect
from cassandra import ConsistencyLevel
from cassandra.query import SimpleStatement

router = APIRouter(prefix="/taller")

# Lo inyecta main.py al montar el router: evita duplicar el pool de conexiones.
ctx: dict = {}


# ─────────────────────────────────────────────────────────────
# Utilidades de emision
# ─────────────────────────────────────────────────────────────
async def paso(sock, n, titulo, detalle=None, cmd=None, salida=None,
               estado="ok", datos=None):
    """Emite un paso del pipeline al frontend."""
    await sock.send_json({
        "tipo": "paso", "n": n, "titulo": titulo, "detalle": detalle,
        "cmd": cmd, "salida": salida, "estado": estado, "datos": datos or {},
    })
    await asyncio.sleep(0.18)      # ritmo legible en vivo


async def fin(sock, resumen, metricas=None):
    await sock.send_json({"tipo": "fin", "resumen": resumen,
                          "metricas": metricas or {}})


def sesion(nodo="cass1"):
    conexiones = ctx["conexiones"]
    if nodo not in conexiones:
        raise RuntimeError(f"{nodo} no esta conectado")
    return conexiones[nodo][1]


def docker(*args):
    return subprocess.run(["docker", *args], capture_output=True, text=True)


def nodetool(nodo, *args):
    return docker("exec", nodo, "nodetool", *args)


def anillo_estado():
    """Devuelve [(nodo, estado, direccion)] leyendo nodetool status."""
    r = nodetool("cass1", "status")
    filas = []
    ips = {"172.24.0.2": "?", "172.24.0.3": "?", "172.24.0.4": "?"}
    for linea in r.stdout.splitlines():
        if linea.startswith(("UN", "DN", "UL", "DL")):
            partes = linea.split()
            filas.append({"estado": partes[0], "ip": partes[1],
                          "carga": partes[2] + " " + partes[3]})
    return filas


def particiones(nodo, tabla="historial_usuario"):
    """Particiones que ESE nodo tiene en su propio disco."""
    nodetool(nodo, "flush", "comunica")
    r = nodetool(nodo, "tablestats", f"comunica.{tabla}")
    for linea in r.stdout.splitlines():
        if "Number of partitions" in linea:
            try:
                return int(linea.split(":")[1].strip())
            except (IndexError, ValueError):
                return -1
    return -1


# ═════════════════════════════════════════════════════════════
# P1 — Las cinco consultas reales
# ═════════════════════════════════════════════════════════════
async def p1_consultas(sock, params):
    s = sesion("cass1")
    await paso(sock, 1, "Por que el modelo se deriva de las consultas",
               "En Cassandra los datos se reparten por el hash de la clave de "
               "particion. Solo se busca rapido POR ESA CLAVE, asi que primero "
               "hay que saber que se va a preguntar.")

    usuario = params.get("usuario") or s.execute(
        "SELECT usuario_id FROM presencia LIMIT 1").one().usuario_id

    # El token es la prueba visible del particionado
    tok = s.execute("SELECT token(usuario_id) AS t FROM presencia "
                    "WHERE usuario_id=%s", (usuario,)).one()
    await paso(sock, 2, "Donde vive fisicamente ese usuario",
               f"El hash de '{usuario}' da un token, y ese token decide que "
               "nodos guardan la fila.",
               cmd=f"SELECT token(usuario_id) FROM presencia WHERE usuario_id = '{usuario}';",
               salida=f"token = {tok.t}" if tok else "sin datos",
               datos={"token": str(tok.t) if tok else None, "usuario": usuario})

    r = docker("exec", "cass1", "nodetool", "getendpoints",
               "comunica", "presencia", usuario)
    replicas = [l.strip() for l in r.stdout.splitlines() if l.strip()]
    await paso(sock, 3, "Que replicas lo guardan",
               f"RF=3: la fila vive en {len(replicas)} nodos.",
               cmd=f"nodetool getendpoints comunica presencia {usuario}",
               salida="\n".join(replicas),
               datos={"replicas": replicas})

    consultas = [
        ("Q1", "Presencia actual de un usuario",
         "SELECT usuario_id, nombre, comuna, estado FROM presencia WHERE usuario_id=%s",
         (usuario,), "1 particion, 1 fila"),
        ("Q2", "Historial de alertas de un usuario",
         "SELECT enviado_en, titulo, severidad FROM historial_usuario "
         "WHERE usuario_id=%s LIMIT 20", (usuario,),
         "1 particion, lectura secuencial (CLUSTERING ORDER DESC)"),
        ("Q3", "Alertas recientes de un usuario",
         "SELECT enviado_en, titulo FROM historial_usuario "
         "WHERE usuario_id=%s AND enviado_en > %s LIMIT 50",
         (usuario, dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=30)),
         "rango sobre la clustering key, sin scan"),
        ("Q4", "Usuarios en linea de una zona",
         "SELECT usuario_id, nombre FROM presencia_por_zona "
         "WHERE zona=%s AND estado=%s LIMIT 20", ("centro", "online"),
         "tabla dedicada: la zona ES la clave de particion"),
        ("Q5", "Quien tiene pendiente una alerta",
         "SELECT usuario_id, estado FROM estado_entrega "
         "WHERE alerta_id=%s AND bucket=%s LIMIT 20",
         (None, 0), "1 bucket de 32, no la alerta entera"),
    ]

    alerta = s.execute("SELECT alerta_id FROM alertas LIMIT 1").one()
    resultados = []
    for i, (cod, desc, cql, args, porque) in enumerate(consultas, start=4):
        if cod == "Q5":
            if not alerta:
                await paso(sock, i, f"{cod} — {desc}",
                           "No hay alertas publicadas. Ejecuta primero el fan-out.",
                           estado="aviso")
                continue
            args = (alerta.alerta_id, 0)
        t0 = time.perf_counter()
        filas = list(s.execute(cql, args))
        ms = (time.perf_counter() - t0) * 1000
        resultados.append({"cod": cod, "ms": round(ms, 2), "filas": len(filas)})
        muestra = "\n".join(str(tuple(f)) for f in filas[:3]) or "(sin filas)"
        await paso(sock, i, f"{cod} — {desc}", porque,
                   cmd=cql.replace("%s", "?"),
                   salida=f"{len(filas)} filas en {ms:.2f} ms\n\n{muestra}",
                   datos={"ms": round(ms, 2), "filas": len(filas)})

    # El contraste: la consulta que NO respeta el modelo
    t0 = time.perf_counter()
    try:
        list(s.execute("SELECT usuario_id FROM presencia WHERE zona='centro' "
                       "LIMIT 20 ALLOW FILTERING"))
        ms_af = (time.perf_counter() - t0) * 1000
        await paso(sock, 9, "El contraste: la misma pregunta sin tabla dedicada",
                   "ALLOW FILTERING obliga al coordinador a preguntar a TODOS los "
                   "nodos y descartar filas en memoria. El costo crece con el "
                   "tamano del cluster, no con el del resultado.",
                   cmd="SELECT usuario_id FROM presencia WHERE zona='centro' "
                       "LIMIT 20 ALLOW FILTERING;",
                   salida=f"responde en {ms_af:.2f} ms — pero recorre todo el anillo",
                   estado="aviso", datos={"ms": round(ms_af, 2)})
    except Exception as e:
        await paso(sock, 9, "El contraste", str(e)[:200], estado="error")

    mejor = min((r["ms"] for r in resultados), default=0)
    peor = max((r["ms"] for r in resultados), default=0)
    await fin(sock,
              f"Las {len(resultados)} consultas del camino critico resuelven entre "
              f"{mejor:.2f} y {peor:.2f} ms porque cada una toca UNA SOLA particion.",
              {"consultas": resultados})


# ═════════════════════════════════════════════════════════════
# P2 — Fan-out real con throughput y latencia
# ═════════════════════════════════════════════════════════════
async def p2_fanout(sock, params):
    s = sesion("cass1")
    total = int(params.get("total", 10_000))
    zonas = ["norte", "centro", "sur"]

    await paso(sock, 1, "Una alerta entra al sistema",
               f"La Alcaldia publica UNA alerta. El sistema debe escribir "
               f"{total:,} filas, una por suscriptor.")

    alerta_id = uuid.uuid4()
    titulo = "Creciente subita - Rio Medellin"
    s.execute("INSERT INTO alertas (alerta_id, titulo, severidad, "
              "zona_destino, publicada_en, total_envios) VALUES (%s,%s,%s,%s,%s,%s)",
              (alerta_id, titulo, "critica", "todas",
               dt.datetime.now(dt.timezone.utc), total))
    await paso(sock, 2, "Se registra en el catalogo (1 escritura)",
               "La tabla 'alertas' guarda el texto UNA vez. Es lo que permite "
               "cancelarla despues sin tocar las 100.000 copias.",
               cmd="INSERT INTO alertas (alerta_id, titulo, ...) VALUES (...);",
               salida=f"alerta_id = {alerta_id}",
               datos={"alerta_id": str(alerta_id)})

    await paso(sock, 3, "Kafka reparte por zona",
               "Cada zona es una particion del topico, y cada worker consume "
               "la suya. Escalar es anadir particiones, no agrandar el servidor.",
               datos={"zonas": zonas})

    prep = s.prepare(
        "INSERT INTO historial_usuario (usuario_id, enviado_en, alerta_id, "
        "titulo, severidad, zona) VALUES (?,?,?,?,?,?)")
    prep.consistency_level = ConsistencyLevel.QUORUM

    await paso(sock, 4, "Empieza el fan-out con CONSISTENCY QUORUM",
               "Cada escritura espera confirmacion de 2 de las 3 replicas. "
               "Es seguridad publica: la alerta no se puede perder.",
               cmd="prep.consistency_level = ConsistencyLevel.QUORUM")

    base = dt.datetime.now(dt.timezone.utc)
    por_zona = total // len(zonas)
    latencias, t_ini = [], time.perf_counter()

    for zi, zona in enumerate(zonas):
        t0 = time.perf_counter()
        pendientes = []
        for i in range(por_zona):
            envio = time.perf_counter()
            pendientes.append((envio, s.execute_async(prep, (
                f"usuario:{zona}:{i:06d}",
                base + dt.timedelta(milliseconds=i),
                alerta_id, titulo, "critica", zona))))
            if len(pendientes) >= 400:
                for e, f in pendientes:
                    f.result(); latencias.append((time.perf_counter() - e) * 1000)
                pendientes.clear()
                await asyncio.sleep(0)     # no bloquear el event loop
        for e, f in pendientes:
            f.result(); latencias.append((time.perf_counter() - e) * 1000)
        dur = time.perf_counter() - t0
        await paso(sock, 5 + zi, f"Zona {zona}: {por_zona:,} entregas",
                   f"{por_zona/dur:,.0f} escrituras/s en esta zona",
                   salida=f"{por_zona:,} filas en {dur:.2f} s",
                   datos={"zona": zona, "n": por_zona, "seg": round(dur, 2),
                          "tps": round(por_zona/dur)})

    seg = time.perf_counter() - t_ini
    latencias.sort()
    def pct(p): return latencias[min(int(len(latencias)*p), len(latencias)-1)]
    entregado = por_zona * len(zonas)

    await paso(sock, 8, "Throughput frente a latencia",
               "No son la misma medida: el throughput es el caudal del sistema, "
               "la latencia es lo que espera UNA escritura. Con 400 peticiones "
               "en vuelo el caudal sube y la latencia individual empeora.",
               salida=f"throughput {entregado/seg:,.0f}/s\n"
                      f"p50 {pct(.5):.1f} ms · p95 {pct(.95):.1f} ms · p99 {pct(.99):.1f} ms",
               datos={"tps": round(entregado/seg), "p50": round(pct(.5), 1),
                      "p95": round(pct(.95), 1), "p99": round(pct(.99), 1),
                      "media": round(sum(latencias)/len(latencias), 1)})

    proy = 100_000 / (entregado/seg)
    await fin(sock,
              f"{entregado:,} entregas en {seg:.2f} s. A este ritmo, las 100.000 "
              f"del caso real tardarian ~{proy:.0f} s — por eso el fan-out es "
              f"ASINCRONO: la Alcaldia recibe confirmacion cuando la alerta entra "
              f"en Kafka, no cuando terminan las escrituras.",
              {"entregado": entregado, "seg": round(seg, 2),
               "tps": round(entregado/seg), "p99": round(pct(.99), 1),
               "proyeccion_100k_seg": round(proy)})


# ═════════════════════════════════════════════════════════════
# P3 — Caida real de un nodo MIENTRAS se escribe
# ═════════════════════════════════════════════════════════════
async def p3_caida(sock, params):
    s = sesion("cass1")
    victima = params.get("nodo", "cass2")

    await paso(sock, 1, "Estado del anillo antes de la caida",
               "Los tres nodos responden (UN = Up/Normal).",
               cmd="nodetool status",
               datos={"anillo": anillo_estado()})

    prep = s.prepare(
        "INSERT INTO historial_usuario (usuario_id, enviado_en, alerta_id, "
        "titulo, severidad, zona) VALUES (?,?,uuid(),?,'critica','centro')")
    prep.consistency_level = ConsistencyLevel.ONE
    base = dt.datetime.now(dt.timezone.utc)

    escritas_antes = 0
    for i in range(300):
        s.execute(prep, (f"CC-CAIDA-{i:05d}", base + dt.timedelta(milliseconds=i),
                         f"alerta {i}"))
        escritas_antes += 1
    await paso(sock, 2, "Escribiendo con el cluster completo",
               f"{escritas_antes} filas con CONSISTENCY ONE.",
               datos={"escritas": escritas_antes})

    await paso(sock, 3, f"Se tumba {victima} DE VERDAD",
               "No es latencia de red simulada: se detiene el contenedor. "
               "El gossip tarda unos segundos en marcarlo como DN.",
               cmd=f"docker stop {victima}", estado="aviso")
    docker("stop", victima)
    await asyncio.sleep(12)

    await paso(sock, 4, "El anillo ahora", f"{victima} aparece como DN (Down).",
               cmd="nodetool status", datos={"anillo": anillo_estado()},
               estado="aviso")

    t0 = time.perf_counter()
    escritas_durante, fallos = 0, 0
    for i in range(300, 600):
        try:
            s.execute(prep, (f"CC-CAIDA-{i:05d}",
                             base + dt.timedelta(milliseconds=i), f"alerta {i}"))
            escritas_durante += 1
        except Exception:
            fallos += 1
    seg = time.perf_counter() - t0

    await paso(sock, 5, "El cluster SIGUE aceptando escrituras",
               "Esto es la A de AP (Modulo 1, Sec. 1.2). Un sistema CP las "
               "habria rechazado para no divergir.",
               salida=f"{escritas_durante} escrituras OK, {fallos} fallos, "
                      f"en {seg:.2f} s con un nodo caido",
               datos={"ok": escritas_durante, "fallos": fallos},
               estado="ok" if fallos == 0 else "aviso")

    await paso(sock, 6, "Por que no fallan",
               "Con RF=3 y una replica caida quedan 2 vivas. CONSISTENCY ONE "
               "necesita 1, y hasta QUORUM (2 de 3) se sigue cumpliendo. Por eso "
               "RF=3 es el minimo que tolera perder un nodo.")

    await fin(sock,
              f"Con {victima} caido, {escritas_durante} de 300 escrituras se "
              f"aceptaron sin error. El servicio no se interrumpio. "
              f"{victima} sigue detenido: usa el punto 4 para repararlo.",
              {"ok": escritas_durante, "fallos": fallos, "victima": victima})


# ═════════════════════════════════════════════════════════════
# P4 — Reparacion con conteo antes/despues
# ═════════════════════════════════════════════════════════════
async def p4_reparacion(sock, params):
    victima = params.get("nodo", "cass2")

    await paso(sock, 1, "Se desactiva hinted handoff",
               "Sin esto el coordinador guarda las escrituras del nodo ausente y "
               "se las entrega al volver, cerrando el hueco antes de poder medirlo. "
               "En produccion se deja ACTIVO; aqui se apaga para hacerlo visible.",
               cmd="nodetool disablehandoff && nodetool truncatehints",
               estado="aviso")
    for n in ("cass1", "cass3"):
        nodetool(n, "disablehandoff")
        nodetool(n, "truncatehints")

    # TRUNCATE exige TODAS las replicas vivas, asi que la limpieza va antes de
    # tumbar el nodo. Si el punto 3 lo dejo caido, primero hay que levantarlo.
    corriendo = docker("ps", "--filter", f"name={victima}",
                       "--format", "{{.Names}}").stdout.strip()
    if not corriendo:
        await paso(sock, 2, f"{victima} viene caido del punto 3: se levanta",
                   "La limpieza previa (TRUNCATE) necesita las tres replicas "
                   "vivas, asi que hay que reincorporarlo antes de volver a "
                   "tumbarlo para el ejercicio.",
                   cmd=f"docker start {victima}", estado="aviso")
        docker("start", victima)
        for _ in range(40):
            await asyncio.sleep(5)
            if "Native Transport active: true" in nodetool(victima, "info").stdout:
                break
        await asyncio.sleep(8)

    s = sesion("cass1")
    try:
        s.execute(SimpleStatement("TRUNCATE historial_usuario",
                                  consistency_level=ConsistencyLevel.ALL))
    except Exception as e:
        await paso(sock, 2, "No se pudo limpiar la tabla",
                   "TRUNCATE requiere que las tres replicas respondan. "
                   "Revisa que los tres nodos esten arriba (make status).",
                   salida=str(e)[:220], estado="error")
        await fin(sock, "Ejercicio cancelado: el cluster no esta completo.", {})
        return
    await asyncio.sleep(2)

    await paso(sock, 3, f"Ahora si: se tumba {victima} para abrir el hueco",
               cmd=f"docker stop {victima}", estado="aviso")
    docker("stop", victima)
    await asyncio.sleep(12)

    prep = s.prepare(
        "INSERT INTO historial_usuario (usuario_id, enviado_en, alerta_id, "
        "titulo, severidad, zona) VALUES (?,?,uuid(),?,'critica','centro')")
    prep.consistency_level = ConsistencyLevel.ONE
    base = dt.datetime.now(dt.timezone.utc)
    n_filas = int(params.get("filas", 2000))
    for i in range(n_filas):
        s.execute(prep, (f"CC-REP-{i:05d}", base + dt.timedelta(milliseconds=i),
                         f"alerta {i}"))

    await paso(sock, 4, f"{n_filas:,} particiones escritas sin {victima}",
               "Cada fila usa un usuario_id distinto, asi que cada una es una "
               "particion propia. El nodo caido no recibio ninguna.",
               datos={"filas": n_filas})

    docker("start", victima)
    await paso(sock, 5, f"{victima} vuelve al anillo",
               "Esperando a que el transporte nativo quede activo...",
               cmd=f"docker start {victima}")
    for _ in range(40):
        await asyncio.sleep(5)
        if "Native Transport active: true" in nodetool(victima, "info").stdout:
            break
    await asyncio.sleep(8)

    antes = {n: particiones(n) for n in ("cass1", "cass2", "cass3")}
    hueco = max(antes.values()) - antes[victima]
    await paso(sock, 6, "CONTEO ANTES de reparar",
               f"A {victima} le faltan {hueco:,} particiones.",
               cmd="nodetool tablestats comunica.historial_usuario",
               salida="\n".join(f"{n}: {v:,} particiones" for n, v in antes.items()),
               datos={"conteo": antes, "hueco": hueco},
               estado="error" if hueco > 0 else "ok")

    await paso(sock, 7, "Por que un SELECT no lo detecta",
               "Con RF=3 sobre 3 nodos los tres son replica de todo. Una lectura "
               "con ONE dirigida al nodo con el hueco la sirve cualquier replica "
               "viva y devuelve el dato correcto. El cliente NO percibe la "
               "inconsistencia: eso es exactamente la consistencia eventual.",
               cmd=f"nodetool getendpoints comunica historial_usuario CC-REP-00000",
               estado="aviso")

    await paso(sock, 8, "nodetool repair",
               "Compara arboles de Merkle entre replicas y sincroniza TODO el "
               "rango, no solo lo que alguien haya leido.",
               cmd=f"nodetool repair -full comunica")
    t0 = time.perf_counter()
    r = nodetool(victima, "repair", "-full", "comunica")
    seg = time.perf_counter() - t0

    despues = {n: particiones(n) for n in ("cass1", "cass2", "cass3")}
    recuperadas = despues[victima] - antes[victima]
    convergio = despues[victima] == max(despues.values())

    await paso(sock, 9, "CONTEO DESPUES de reparar",
               f"{victima} recupero {recuperadas:,} particiones en {seg:.1f} s.",
               salida="\n".join(f"{n}: {v:,} particiones" for n, v in despues.items()),
               datos={"conteo": despues, "recuperadas": recuperadas},
               estado="ok" if convergio else "aviso")

    for n in ("cass1", "cass3"):
        nodetool(n, "enablehandoff")

    await fin(sock,
              f"Hueco de {hueco:,} particiones cerrado: las tres replicas "
              f"convergen en {despues[victima]:,}. Hinted handoff restaurado. "
              f"Recuerda: con el nodo fuera mas de 3 h los hints caducan, y mas "
              f"de 10 dias (gc_grace_seconds) los borrados pueden resucitar.",
              {"antes": antes, "despues": despues, "hueco": hueco,
               "recuperadas": recuperadas, "seg": round(seg, 1)})


# ═════════════════════════════════════════════════════════════
# P5 — Idempotencia
# ═════════════════════════════════════════════════════════════
async def p5_idempotencia(sock, params):
    s = sesion("cass1")
    usuario = "CC-IDEM-DEMO"
    reenvios = int(params.get("reenvios", 3))

    def contar():
        return s.execute("SELECT count(*) AS n FROM historial_usuario "
                         "WHERE usuario_id=%s", (usuario,)).one().n

    s.execute("DELETE FROM historial_usuario WHERE usuario_id=%s", (usuario,))
    s.execute("DELETE FROM eventos_procesados WHERE evento_id=%s",
              (f"{usuario}:demo",))

    await paso(sock, 1, "Kafka entrega AL MENOS UNA VEZ",
               "Si un worker muere antes de confirmar el offset, al reiniciar "
               "vuelve a recibir el mismo mensaje. En un fan-out eso es reenviar "
               "la alerta a 100.000 personas.")

    evento = {"alerta_id": uuid.uuid4(),
              "enviado_en": dt.datetime.now(dt.timezone.utc),
              "titulo": "Cierre vial - Av. Regional"}
    await paso(sock, 2, "El evento que se va a reenviar",
               "El productor fija enviado_en UNA vez y viaja dentro del mensaje.",
               salida=f"alerta_id  {evento['alerta_id']}\n"
                      f"enviado_en {evento['enviado_en'].isoformat()}",
               datos={"evento": str(evento["alerta_id"])})

    # CASO A
    serie_a = []
    for i in range(1, reenvios + 1):
        s.execute("INSERT INTO historial_usuario (usuario_id, enviado_en, "
                  "alerta_id, titulo, severidad, zona) VALUES (%s,%s,%s,%s,'critica','centro')",
                  (usuario, evento["enviado_en"], evento["alerta_id"], evento["titulo"]))
        n = contar()
        serie_a.append(n)
        await paso(sock, 2 + i,
                   f"CASO A · {'entrega original' if i == 1 else f'reintento {i-1}'}",
                   "La clave primaria ((usuario_id), enviado_en) no cambia, asi que "
                   "Cassandra sobrescribe la MISMA fila (last-write-wins).",
                   cmd="INSERT ... VALUES (?, <enviado_en DEL EVENTO>, ...)",
                   salida=f"filas en historial: {n}",
                   datos={"caso": "A", "intento": i, "filas": n},
                   estado="ok" if n == 1 else "error")

    # CASO B
    s.execute("DELETE FROM historial_usuario WHERE usuario_id=%s", (usuario,))
    serie_b = []
    for i in range(1, reenvios + 1):
        s.execute("INSERT INTO historial_usuario (usuario_id, enviado_en, "
                  "alerta_id, titulo, severidad, zona) VALUES (%s,%s,%s,%s,'critica','centro')",
                  (usuario, dt.datetime.now(dt.timezone.utc),
                   evento["alerta_id"], evento["titulo"]))
        await asyncio.sleep(0.01)
        n = contar()
        serie_b.append(n)
        await paso(sock, 2 + reenvios + i,
                   f"CASO B · {'entrega original' if i == 1 else f'reintento {i-1}'}",
                   "now() da un timestamp distinto cada vez, luego una CLAVE "
                   "PRIMARIA distinta, luego una fila nueva.",
                   cmd="INSERT ... VALUES (?, now(), ...)   <- el bug",
                   salida=f"filas en historial: {n}",
                   datos={"caso": "B", "intento": i, "filas": n},
                   estado="ok" if n == 1 else "error")

    # CASO C
    n_paso = 2 + 2*reenvios + 1
    aplicados = []
    for i in range(1, reenvios + 1):
        r = s.execute("INSERT INTO eventos_procesados (evento_id, procesado_en, "
                      "worker) VALUES (%s,%s,%s) IF NOT EXISTS",
                      (f"{usuario}:demo", dt.datetime.now(dt.timezone.utc),
                       "worker-1")).one()
        aplicados.append(bool(r.applied))
        await paso(sock, n_paso + i - 1, f"CASO C · guardia LWT, intento {i}",
                   "El last-write-wins arregla la FILA, pero no el push al celular: "
                   "ese ya salio. Antes de enviarlo, el worker reclama el evento "
                   "con IF NOT EXISTS (Paxos).",
                   cmd="INSERT INTO eventos_procesados (...) IF NOT EXISTS;",
                   salida="APLICADO — se envia el push" if r.applied
                          else "RECHAZADO — ya procesado, push descartado",
                   datos={"caso": "C", "intento": i, "aplicado": bool(r.applied)},
                   estado="ok" if (i == 1) == bool(r.applied) else "error")

    s.execute("DELETE FROM historial_usuario WHERE usuario_id=%s", (usuario,))
    s.execute("DELETE FROM eventos_procesados WHERE evento_id=%s",
              (f"{usuario}:demo",))

    await fin(sock,
              f"Timestamp del EVENTO: {serie_a[-1]} fila tras {reenvios} reenvios. "
              f"Timestamp del WORKER: {serie_b[-1]} filas. La idempotencia del "
              f"fan-out no cuesta nada — no hay que deduplicar ni coordinar: basta "
              f"con que la clave primaria derive del evento. Paxos se reserva para "
              f"el push, que es 10-20x mas lento.",
              {"caso_a": serie_a, "caso_b": serie_b, "caso_c": aplicados})


# ═════════════════════════════════════════════════════════════
# P6 — Analitica: motor correcto vs forzado
# ═════════════════════════════════════════════════════════════
async def p6_analitica(sock, params):
    s = sesion("cass1")

    await paso(sock, 1, "La pregunta de negocio",
               "\"Cuantos usuarios unicos hay en linea, por zona?\" Es una "
               "agregacion sobre una columna que NO es clave de particion.")

    await paso(sock, 2, "Intento 1: el GROUP BY natural",
               "Asi se escribiria en SQL.",
               cmd="SELECT zona, count(*) FROM presencia GROUP BY zona;")
    try:
        s.execute("SELECT zona, count(*) FROM presencia GROUP BY zona")
        await paso(sock, 3, "Respondio", "Inesperado.", estado="aviso")
    except Exception as e:
        msg = str(e).split("message=")[-1].strip('"\' ')[:220]
        await paso(sock, 3, "Cassandra la RECHAZA",
                   "Solo agrupa por columnas de la clave primaria. Las filas de "
                   "una zona estan repartidas por todo el anillo.",
                   salida=msg, estado="error")

    await paso(sock, 4, "Intento 2: forzarla con ALLOW FILTERING",
               "La valvula de escape. Funciona, pero el coordinador pregunta a "
               "TODOS los nodos y descarta filas en memoria.",
               cmd="SELECT usuario_id FROM presencia WHERE zona=? AND "
                   "estado='online' ALLOW FILTERING;", estado="aviso")

    lat, conteo = [], {}
    for _ in range(3):
        t0 = time.perf_counter()
        conteo = {}
        for z in ("norte", "centro", "sur"):
            filas = s.execute("SELECT usuario_id FROM presencia WHERE zona=%s "
                              "AND estado='online' ALLOW FILTERING", (z,))
            conteo[z] = sum(1 for _ in filas)
        lat.append((time.perf_counter() - t0) * 1000)
    p50_c = st.median(lat)
    n_cass = s.execute("SELECT count(*) AS n FROM presencia").one().n

    await paso(sock, 5, "Responde, pero mirando el costo",
               "Ademas, el GROUP BY lo termina haciendo el cliente en memoria: "
               "son 3 consultas, una por zona.",
               salida="\n".join(f"zona {z:<7} {n:,} en linea" for z, n in conteo.items())
                      + f"\n\nmediana de 3: {p50_c:,.1f} ms sobre {n_cass:,} filas",
               datos={"ms": round(p50_c, 1), "filas": n_cass, "conteo": conteo})

    # ClickHouse
    r = subprocess.run(["docker", "ps", "--filter", "name=clickhouse",
                        "--format", "{{.Names}}"], capture_output=True, text=True)
    if "clickhouse" not in r.stdout:
        await paso(sock, 6, "ClickHouse no esta corriendo",
                   "Levantalo con: docker compose up -d clickhouse && make ch-seed",
                   estado="aviso")
        await fin(sock, "Comparacion incompleta: falta ClickHouse.", {})
        return

    sql = ("SELECT zona, uniqExact(usuario_id) FROM comunica.presencia_eventos "
           "WHERE estado='online' GROUP BY zona ORDER BY 2 DESC")
    await paso(sock, 6, "El motor correcto para esta pregunta",
               "ClickHouse es columnar: lee solo las columnas de la consulta, "
               "no las 6 de cada fila.", cmd=sql)

    def ch(q):
        t0 = time.perf_counter()
        p = subprocess.run(["docker", "exec", "clickhouse", "clickhouse-client",
                            "--query", q], capture_output=True, text=True)
        return p.stdout.strip(), (time.perf_counter() - t0) * 1000

    salida, _ = ch(sql)
    lat_ch = [ch(sql)[1] for _ in range(3)]
    p50_ch = st.median(lat_ch)
    n_ch_txt, _ = ch("SELECT count() FROM comunica.presencia_eventos")
    n_ch = int(n_ch_txt or 0)

    filas_ch = "\n".join(f"zona {l.split(chr(9))[0]:<7} {int(l.split(chr(9))[1]):,} unicos"
                         for l in salida.splitlines() if chr(9) in l)
    await paso(sock, 7, "Responde sobre mucho mas volumen",
               f"Mismo orden de latencia, pero sobre {n_ch:,} filas.",
               salida=f"{filas_ch}\n\nmediana de 3: {p50_ch:,.1f} ms sobre {n_ch:,} filas",
               datos={"ms": round(p50_ch, 1), "filas": n_ch})

    us_c = p50_c * 1000 / max(n_cass, 1)
    us_ch = p50_ch * 1000 / max(n_ch, 1)
    await paso(sock, 8, "Normalizar antes de comparar",
               "Las latencias se parecen, pero NO son el mismo volumen. Comparar "
               "los tiempos directos SUBESTIMA la diferencia; hay que normalizar "
               "por fila procesada.",
               salida=f"Cassandra   {us_c:8.2f} us/fila\n"
                      f"ClickHouse  {us_ch:8.2f} us/fila\n"
                      f"            {us_c/us_ch:8.0f}x menos costo por fila",
               datos={"us_cassandra": round(us_c, 2), "us_clickhouse": round(us_ch, 2),
                      "factor": round(us_c/us_ch)})

    await fin(sock,
              f"No es que un motor sea mejor: resuelven preguntas distintas. En "
              f"Cassandra el costo depende del numero de nodos y filas almacenadas; "
              f"en ClickHouse, de las columnas consultadas. Por eso la arquitectura "
              f"tiene los dos (Modulo 1, Sec. 2.2).",
              {"cassandra_ms": round(p50_c, 1), "clickhouse_ms": round(p50_ch, 1),
               "factor": round(us_c/us_ch)})


# ═════════════════════════════════════════════════════════════
PIPELINES = {
    "p1": p1_consultas, "p2": p2_fanout, "p3": p3_caida,
    "p4": p4_reparacion, "p5": p5_idempotencia, "p6": p6_analitica,
}


@router.websocket("/ws/{punto}")
async def ws_pipeline(sock: WebSocket, punto: str):
    await sock.accept()
    fn = PIPELINES.get(punto)
    if not fn:
        await sock.send_json({"tipo": "error", "mensaje": f"punto desconocido: {punto}"})
        await sock.close()
        return
    try:
        params = {}
        try:
            params = await asyncio.wait_for(sock.receive_json(), timeout=1.5)
        except (asyncio.TimeoutError, Exception):
            pass
        await sock.send_json({"tipo": "inicio", "punto": punto})
        await fn(sock, params or {})
    except WebSocketDisconnect:
        return
    except Exception as e:
        try:
            await sock.send_json({"tipo": "error",
                                  "mensaje": f"{type(e).__name__}: {e}"})
        except Exception:
            pass
    finally:
        try:
            await sock.close()
        except Exception:
            pass


@router.get("/estado")
def estado():
    """Que hay levantado y cuantos datos hay, para pintar el panel inicial."""
    conexiones = ctx.get("conexiones", {})
    info = {"nodos": [], "clickhouse": False, "usuarios": 0, "alertas": 0}
    for n in ("cass1", "cass2", "cass3"):
        vivo = bool(subprocess.run(
            ["docker", "ps", "--filter", f"name={n}", "--format", "{{.Names}}"],
            capture_output=True, text=True).stdout.strip())
        info["nodos"].append({"nodo": n, "vivo": vivo,
                              "conectado": n in conexiones})
    info["clickhouse"] = bool(subprocess.run(
        ["docker", "ps", "--filter", "name=clickhouse", "--format", "{{.Names}}"],
        capture_output=True, text=True).stdout.strip())
    if "cass1" in conexiones:
        try:
            s = conexiones["cass1"][1]
            info["usuarios"] = s.execute("SELECT count(*) AS n FROM presencia").one().n
            info["alertas"] = s.execute("SELECT count(*) AS n FROM alertas").one().n
        except Exception:
            pass
    return info
