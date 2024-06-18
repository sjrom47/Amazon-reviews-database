"""
================
inserta_dataset.py
================

Desarrollado por Sergio Jiménez Romero y Alberto Velasco Rodríguez

Este fichero inserta limpia e inserta los datos de un nuevo fichero a las bases de datos ya creadas.

En cuanto a los parametros de configuracion, se deben comprobar las credenciales de SQL y las rutas a los datos.
"""

import configuracion as c
import os
import json
from pymongo import MongoClient
import pymysql
from time import perf_counter


def crear_sql_insercion(nombre_tabla, guia) -> str:
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


# *** General ***
def inserta_dataset(nombre_fichero):
    """Limpia e inserta los datos a las bases de datos ya creadas.
    """
    sqls_insercion = [crear_sql_insercion(nombre_tabla, guia_tabla) for nombre_tabla, guia_tabla in c.GUIAS_TABLAS_SQL.items()]

    sql_mayor_id='''SELECT id
                    FROM review
                    ORDER BY id DESC
                    LIMIT 1;'''

    sql_reviewername='''SELECT reviewerName
                        FROM reviewer
                        WHERE reviewerID = %s
                        LIMIT 1;'''

    conexion_mysl_tabla =pymysql.connect(
    host='localhost',
    user=c.USUARIO_SQL,
    password=c.PASSWORD_SQL,
    database=c.NOMBRE_BASE_SQL
    )

    # Creo la conexión a la base de datos
    CONNECTION_STRING = "mongodb://localhost:27017"
    client = MongoClient(CONNECTION_STRING)
    db = client[c.NOMBRE_BASE_MONGODB]

    # Accedo a la colección
    coleccion = db[c.NOMBRE_TABLA_MONGODB]

    with conexion_mysl_tabla:
        cursor =conexion_mysl_tabla.cursor()

        # Obtenemos el siguiente id que tocaría
        cursor.execute(sql_mayor_id)
        id_review = int(cursor.fetchone()[0]) + 1
        
        reviewers = {} # debe ser un diccionario para guardar el nombre del reviewer para  conservar el primero que aparezca
        asins = []
        ruta = os.path.join(c.DIRECTORIO_DATOS, nombre_fichero)
        print(nombre_fichero[:-5])
        with open(ruta, 'r') as f:
            for linea in f:
                linea = json.loads(linea)

                # *** Tratamiento de los datos ***
                linea["id"] = id_review
                linea["type"] = nombre_fichero[:-5]

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
                        dia = dia.strip(',') # dia es el que se quedaba con la coma de registered
                        linea["reviewTime"] = f'{anno}-{mes}-{dia}'
                    else: # en caso de que no sea correcto lo podremos a NULL
                        linea["reviewTime"] = None
                
                # Insertamos el reviewer si no se ha creado ya
                if linea["reviewerID"] is not None:
                    if linea["reviewerID"] not in reviewers:  # en las claves de reviewers
                        # Debemos asegurarnos de que no se intrujera antes
                        cursor.execute(sql_reviewername, linea["reviewerID"])
                        reviewername = cursor.fetchone()

                        if reviewername is not None:
                            reviewers[linea["reviewerID"]] = linea["reviewerName"]
                        else:
                            cursor.execute(sqls_insercion[0], [linea[guia_dato] for guia_dato in c.GUIAS_TABLAS_SQL["reviewer"]])
                    else:
                        linea["reviewerName"] = reviewers[linea["reviewerID"]]

                # Insertamos el producto si no se ha hecho ya
                if linea["asin"] is not None and linea["asin"] not in asins:
                    asins.append(linea["asin"])
                    cursor.execute(sqls_insercion[1], [linea[guia_dato] for guia_dato in c.GUIAS_TABLAS_SQL["product"]])

                cursor.execute(sqls_insercion[2], [linea[guia_dato] for guia_dato in c.GUIAS_TABLAS_SQL["review"]])

                coleccion.insert_one({guia_dato: linea[guia_dato] for guia_dato in c.GUIA_TABLA_MONGODB})

                id_review += 1
        
        conexion_mysl_tabla.commit()
        cursor.close()

if __name__ == "__main__":
    t = perf_counter()
    inserta_dataset(c.NOMBRE_FICHEROS_EXTRA)
    print(f"Tiempo en cargar los datos: {perf_counter() - t}")