"""
================
config.py
================

Developed by Sergio Jiménez Romero and Alberto Velasco Rodríguez

This file contains the parameters to run the different programs of the project
"""

# Path to the data
DIRECTORIO_DATOS = "data"
NOMBRE_FICHEROS_DATOS = [
    "Digital_Music_5.json",
    "Musical_Instruments_5.json",
    "Toys_and_Games_5.json",
    "Video_Games_5.json",
]  # used in load_data.py
NOMBRE_FICHEROS_EXTRA = "Amazon_Instant_Video_5.json"  # used in inserta_dataset.py


# Required credentials
USUARIO_SQL = "alfaduck"
PASSWORD_SQL = "omegaduck"
USUARIO_NEO = "neo4j"
PASSWORD_NEO = "omegaduck"

# Database and collection names
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

# Neo4J URI
URI = "neo4j://localhost:7687"

# Neo4j Data
N_USUARIOS_NEO = 30
EJERCICIO = 3
CAT_EJERCICIO_2 = "Video_Games_5"
N_USUARIOS_NEO_EJ3 = 400
