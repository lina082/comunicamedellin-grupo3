"""
Generacion de datos sinteticos realistas para ComunicaMedellin.

Usamos Faker con locale es_CO para que la demo muestre nombres,
cedulas y telefonos colombianos en vez de 'usuario:000123'. El
jurado ve datos que parecen reales, no rellenos.

Las comunas y coordenadas son de Medellin y estan escritas a mano:
Faker con es_CO genera ciudades de todo el pais, no barrios de una
ciudad concreta.
"""
import random
from faker import Faker

fake = Faker("es_CO")

# Las 16 comunas de Medellin agrupadas en las 3 zonas del proyecto.
# La zona es la clave de particionamiento en Kafka (Sec. 3.3).
COMUNAS = {
    "norte": [
        ("Popular",        6.2967, -75.5450),
        ("Santa Cruz",     6.2925, -75.5556),
        ("Manrique",       6.2790, -75.5480),
        ("Aranjuez",       6.2820, -75.5620),
        ("Castilla",       6.2900, -75.5750),
        ("Doce de Octubre",6.3010, -75.5700),
        ("Robledo",        6.2830, -75.5910),
    ],
    "centro": [
        ("La Candelaria",  6.2476, -75.5658),
        ("Villa Hermosa",  6.2540, -75.5470),
        ("Buenos Aires",   6.2400, -75.5520),
        ("Laureles",       6.2450, -75.5950),
        ("La America",     6.2530, -75.6050),
        ("San Javier",     6.2560, -75.6180),
    ],
    "sur": [
        ("El Poblado",     6.2100, -75.5680),
        ("Guayabal",       6.2130, -75.5850),
        ("Belen",          6.2280, -75.6020),
    ],
}

ZONAS = list(COMUNAS.keys())

# Indice inverso: de comuna a su zona. La zona SIEMPRE se deriva de
# la comuna, nunca se escribe suelta. Al desnormalizar (Cassandra no
# tiene JOINs, Sec. 2.2) el motor ya no valida la coherencia entre
# columnas, asi que la garantiza el codigo que escribe.
ZONA_DE_COMUNA = {c[0]: z for z, cs in COMUNAS.items() for c in cs}


def zona_de(comuna):
    """Devuelve la zona a la que pertenece una comuna de Medellin."""
    return ZONA_DE_COMUNA.get(comuna)
DISPOSITIVOS = ["android", "ios", "web"]
# Distribucion realista del mercado colombiano
PESO_DISPOSITIVO = [72, 19, 9]

SEVERIDADES = ["info", "advertencia", "critica"]

# Alertas reales del tipo que emite la Alcaldia de Medellin
PLANTILLAS_ALERTA = [
    ("critica", "Cierre vial - {via}",
     "Cierre total por emergencia en {via}. Use rutas alternas."),
    ("critica", "Alerta de creciente subita - {rio}",
     "Nivel critico en {rio}. Evacue la zona de influencia."),
    ("advertencia", "Lluvias intensas - zona {zona}",
     "Se esperan lluvias fuertes en las proximas 3 horas. Precaucion."),
    ("advertencia", "Movilidad afectada - {via}",
     "Congestion severa por accidente. Considere transporte publico."),
    ("info", "Cancelacion de clases - {institucion}",
     "Se suspenden las actividades academicas por condiciones climaticas."),
    ("info", "Jornada de vacunacion - comuna {comuna}",
     "Puesto de vacunacion disponible de 8:00 a.m. a 4:00 p.m."),
]

VIAS = ["la Avenida Regional", "la Calle 30", "la Autopista Sur",
        "la Avenida Oriental", "la Carrera 70", "la Avenida 33",
        "el Tunel de Oriente", "la Via Las Palmas"]
RIOS = ["la Quebrada La Iguana", "el Rio Medellin",
        "la Quebrada Santa Elena", "la Quebrada La Presidenta"]
INSTITUCIONES = ["el ITM", "la Universidad de Antioquia",
                 "la I.E. Marco Fidel Suarez", "el Politecnico JIC"]


_cedulas_usadas = set()


def sembrar(semilla=42):
    """Fija la semilla para que la demo sea reproducible.

    Importante para la exposicion: si algo falla en vivo, al volver
    a correrlo salen exactamente los mismos datos.
    """
    Faker.seed(semilla)
    random.seed(semilla)
    _cedulas_usadas.clear()


def _cedula_unica():
    """Genera una cedula que no se haya usado antes.

    Necesario porque usuario_id es la clave primaria en Cassandra:
    una cedula repetida sobrescribiria al usuario anterior en vez
    de crear uno nuevo. Con 10.000 usuarios y 8 digitos las
    colisiones son raras pero ocurren (probamos: 1 en 3.000).
    """
    while True:
        cc = f"CC{fake.random_number(digits=8, fix_len=True)}"
        if cc not in _cedulas_usadas:
            _cedulas_usadas.add(cc)
            return cc


def usuario(indice):
    """Genera un suscriptor con datos colombianos realistas."""
    zona = ZONAS[indice % len(ZONAS)]
    comuna, lat, lon = random.choice(COMUNAS[zona])
    return {
        "usuario_id": _cedula_unica(),
        "nombre": fake.name(),
        # Formato unico +573XXXXXXXXX. Faker es_CO mezcla seis
        # formatos distintos e incluye lineas gratuitas 018000 como
        # si fueran celulares, y eso se ve descuidado en pantalla.
        "telefono": f"+573{random.choice('0125')}{random.randint(0, 9_999_999):07d}",
        "zona": zona,
        "comuna": comuna,
        # Dispersion de ~1 km alrededor del centroide de la comuna
        "lat": round(lat + random.uniform(-0.009, 0.009), 6),
        "lon": round(lon + random.uniform(-0.009, 0.009), 6),
        "dispositivo": random.choices(DISPOSITIVOS, weights=PESO_DISPOSITIVO)[0],
        "estado": random.choices(["online", "offline"], weights=[68, 32])[0],
    }


def alerta(severidad=None):
    """Genera una alerta ciudadana verosimil.

    severidad: 'critica' | 'advertencia' | 'info' | None (cualquiera).
    Para la demo de fan-out conviene forzar 'critica': el caso del
    enunciado es una emergencia, no un aviso de rutina.
    """
    candidatas = PLANTILLAS_ALERTA
    if severidad:
        candidatas = [p for p in PLANTILLAS_ALERTA if p[0] == severidad] or PLANTILLAS_ALERTA
    sev, titulo, cuerpo = random.choice(candidatas)
    zona = random.choice(ZONAS)
    ctx = {
        "via": random.choice(VIAS),
        "rio": random.choice(RIOS),
        "institucion": random.choice(INSTITUCIONES),
        "zona": zona,
        "comuna": random.choice(COMUNAS[zona])[0],
    }
    return {
        "titulo": titulo.format(**ctx),
        "cuerpo": cuerpo.format(**ctx),
        "severidad": sev,
        "zona_destino": zona,
    }
