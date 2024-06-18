"""
================
load_data.py
================

Desarrollado por Sergio Jiménez Romero y Alberto Velasco Rodríguez

Este fichero crea las bases de datos de SQL y mongoDB.

En cuanto a los parametros de configuracion, se deben comprobar las credenciales de SQL y las rutas a los datos.
"""

import configuracion as c
import os
import json
from pymongo import MongoClient
import pymysql
from time import perf_counter


# *** SQL ***
def crear_base_datos_sql() -> None:
    """Prueba a borrar la base de datos, la vuelve a crear y hace commit.
    """
    conexion_mysl_crea =pymysql.connect(
    host='localhost',
    user=c.USUARIO_SQL,
    password=c.PASSWORD_SQL
    )

    with conexion_mysl_crea:
        cursor =conexion_mysl_crea.cursor()

        try:
            sql =f'DROP DATABASE {c.NOMBRE_BASE_SQL};'
            cursor.execute(sql)
            conexion_mysl_crea.commit()
        except pymysql.err.OperationalError:
            pass

        sql =f'CREATE DATABASE {c.NOMBRE_BASE_SQL};'
        cursor.execute(sql)
        conexion_mysl_crea.commit()

        cursor.close()

def crear_tabla_sql(sql_tabla:str) -> None:
    """Ejecuta la consulta para la creación de tablas dada y hace commit.

    Args:
        sql_tabla (str): consulta sql para crear la tabla
    """
    conexion_mysl =pymysql.connect(
    host='localhost',
    user=c.USUARIO_SQL,
    password=c.PASSWORD_SQL,
    database=c.NOMBRE_BASE_SQL
    )

    with conexion_mysl:
        cursor =conexion_mysl.cursor()
        cursor.execute(sql_tabla)
        conexion_mysl.commit()
        cursor.close()


def crear_sql_insercion(nombre_tabla:str, guia:list) -> str:
    """Devuelve una query sql para insertar datos a la tabla 'nombre_tabla',
    que sigue la estructura 'guia'.

    Args:
        nombre_tabla (str): nombre de la tabla a la que se quieren introducir los datos.
        guia (list): nombres de los campos de la tabla

    Returns:
        str: consulta sql parametrizada para intrudicir datos
    """
    # Ahora se crean strings para introducir los nombres de las columnas y después los datos en el INSERT INTO
    string_param = ''
    string_guia = ''
    for columna in guia:
        string_param = f'{string_param}%s, '
        string_guia = f'{string_guia}{columna}, '
    string_param = string_param.rstrip(', ')
    string_guia = string_guia.rstrip(', ')

    return f'''INSERT INTO {nombre_tabla} ({string_guia})
            VALUES ({string_param});
    '''

# *** MongoDB ***
def crear_base_datos_mongodb() -> None:
    """Borra la base de datos de mongoDB y vuelve a crearla junto a la colección.
    """
    # Creo la conexión a la nueva base de datos
    CONNECTION_STRING = "mongodb://localhost:27017"
    client = MongoClient(CONNECTION_STRING)
    client.drop_database(c.NOMBRE_BASE_MONGODB)

    db = client[c.NOMBRE_BASE_MONGODB]
    db.create_collection(c.NOMBRE_TABLA_MONGODB)


# *** General ***
def limpiar_datos() -> None:
    """Limpia e inserta los datos a las bases de datos vacías.
    """
    sqls_insercion = [crear_sql_insercion(nombre_tabla, guia_tabla) for nombre_tabla, guia_tabla in c.GUIAS_TABLAS_SQL.items()]

    conexion_mysl_tabla =pymysql.connect(
    host='localhost',
    user=c.USUARIO_SQL,
    password=c.PASSWORD_SQL,
    database=c.NOMBRE_BASE_SQL
    )

    # Creo la conexión a la nueva base de datos
    CONNECTION_STRING = "mongodb://localhost:27017"
    client = MongoClient(CONNECTION_STRING)
    db = client[c.NOMBRE_BASE_MONGODB]

    # Accedo a la colección
    coleccion = db[c.NOMBRE_TABLA_MONGODB]

    with conexion_mysl_tabla:
        cursor =conexion_mysl_tabla.cursor()

        reviewers = {} # debe ser un diccionario para guardar el nombre del reviewer para  conservar el primero que aparezca
        id_review = 1
        for nombre in c.NOMBRE_FICHEROS_DATOS:
            asins = []
            ruta = os.path.join(c.DIRECTORIO_DATOS, nombre)
            print(nombre[:-5])
            with open(ruta, 'r') as f:
                for linea in f:
                    linea = json.loads(linea)

                    # *** Tratamiento de los datos ***
                    linea["id"] = id_review

                    linea["type"] = nombre[:-5]

                    # Pasamos a None ciertos datos
                    for lista_guia in c.GUIAS_TABLAS_SQL.values():
                        for guia_dato in lista_guia:
                            if guia_dato not in linea:
                                linea[guia_dato] = None
                            elif linea[guia_dato] in ['', ' ']:
                                linea[guia_dato] = None

                    # Pasamos reviewTime a formato DATE
                    if linea["reviewTime"] is not None:
                        # Ejemplo de reviewTime: 04 22, 2014
                        # Podemos ver que los tres elementos los sepáran los espacios
                        reviewtime = linea["reviewTime"].split(' ')
                        if len(reviewtime) == 3:
                            mes, dia, anno = reviewtime
                            dia = dia.strip(',') # day es el que se quedaba con la coma de registered
                            linea["reviewTime"] = f'{anno}-{mes}-{dia}'
                        else: # en caso de que no sea correcto lo podremos a NULL
                            linea["reviewTime"] = None
                    
                    # Insertamos el reviewer si no se ha creado ya
                    if linea["reviewerID"] is not None:
                        if linea["reviewerID"] not in reviewers:  # en las claves de reviewers
                            reviewers[linea["reviewerID"]] = linea["reviewerName"]
                            cursor.execute(sqls_insercion[0], [linea[guia_dato] for guia_dato in c.GUIAS_TABLAS_SQL["reviewer"]])
                        else:
                            linea["reviewerName"] = reviewers[linea["reviewerID"]]
                    
                    # Insertamos el producto si no se ha hecho ya
                    if linea["asin"] is not None and linea["asin"] not in asins:
                        asins.append(linea["asin"])
                        cursor.execute(sqls_insercion[1], [linea[guia_dato] for guia_dato in c.GUIAS_TABLAS_SQL["product"]])

                    # Creamos las reviews
                    cursor.execute(sqls_insercion[2], [linea[guia_dato] for guia_dato in c.GUIAS_TABLAS_SQL["review"]])

                    coleccion.insert_one({guia_dato: linea[guia_dato] for guia_dato in c.GUIA_TABLA_MONGODB})

                    id_review += 1

        conexion_mysl_tabla.commit()
        cursor.close()


def load_data():
    """Crea las bases de datos y limpia y carga los datos a ellas.
    """
    sqls_tablas = ['''
        CREATE TABLE reviewer (
            reviewerID VARCHAR(40) NOT NULL,
            reviewerName TEXT,
            PRIMARY KEY (reviewerID)
        );''','''
        CREATE TABLE product (
            asin VARCHAR(40) NOT NULL,
            type VARCHAR(80) NOT NULL,
            PRIMARY KEY (asin, type)
        );''','''
        CREATE TABLE review (
            id INT NOT NULL,
            reviewerID VARCHAR(40),
            asin VARCHAR(40),
            type VARCHAR(80),
            overall INT,
            unixReviewTime INT,
            reviewTime VARCHAR(50),
            PRIMARY KEY (id),
            FOREIGN KEY (reviewerID) REFERENCES reviewer(reviewerID),
            FOREIGN KEY (asin, type) REFERENCES product(asin, type)
        );''']
    crear_base_datos_sql()
    for sql in sqls_tablas:
        crear_tabla_sql(sql)
    crear_base_datos_mongodb()

    limpiar_datos()

if __name__ == "__main__":
    t = perf_counter()
    load_data()
    print(f"Tiempo en cargar los datos: {perf_counter() - t}")