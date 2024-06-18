"""
================
configuracion.py
================

Desarrollado por Sergio Jiménez Romero y Alberto Velasco Rodríguez

En este fichero estan los parametros para ejecutar los distintos programas del proyecto
"""

# Ruta a los datos
DIRECTORIO_DATOS = "datos"
NOMBRE_FICHEROS_DATOS = [
    "Digital_Music_5.json",
    "Musical_Instruments_5.json",
    "Toys_and_Games_5.json",
    "Video_Games_5.json",
] # se usan en load_data.py
NOMBRE_FICHEROS_EXTRA = "Amazon_Instant_Video_5.json" # este es el que se usa en inserta_dataset.py


# Credenciales necesarias
USUARIO_SQL = "alfaduck"
PASSWORD_SQL = "omegaduck"
USUARIO_NEO = "neo4j"
PASSWORD_NEO = "omegaduck"

# Nombre de las bases de datos y colecciones
# SQL
NOMBRE_BASE_SQL = "reviews_product_SQL"
GUIAS_TABLAS_SQL = {
    "reviewer": ["reviewerID", "reviewerName"],
    "product": ["asin", "type"],
    "review": [
        "id",
        "reviewerID",
        "asin",
        "type",
        "overall",
        "unixReviewTime",
        "reviewTime",
    ],
}

# MongoDB
NOMBRE_BASE_MONGODB = "reviews_product_Mongo"
NOMBRE_TABLA_MONGODB = "review"
GUIA_TABLA_MONGODB = ["id", "reviewText", "summary", "helpful"]

# URI Neo4J
URI = "neo4j://localhost:7687"

# Datos Neo4j
N_USUARIOS_NEO = 30
EJERCICIO = 3
CAT_EJERCICIO_2 = "Video_Games_5"
N_USUARIOS_NEO_EJ3 = 400
