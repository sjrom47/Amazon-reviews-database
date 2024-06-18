"""
================
neo4JProyecto.py
================

Desarrollado por Sergio Jiménez Romero y Alberto Velasco Rodríguez

Este fichero crea un grafo en neo4J siguiendo los requisitos del proyecto con 4 casos 
distintos, uno por ejercicio

En cuanto a los parametros de configuracion, se deben comprobar las credenciales de SQL,
de neo4J, la URI de neo4J, verificar que los parametros sean los deseados (estan explicados
en las funciones correspondientes) y verificar que el contenedor de docker este corriendo
"""

from neo4j import GraphDatabase
import pandas as pd
import os
import pymysql
import configuracion as c
import random

# driver de conexion a neo4J
driver = GraphDatabase.driver(c.URI, auth=(c.USUARIO_NEO, c.PASSWORD_NEO))

# Conexion a SQL
conexion_mysql = pymysql.connect(
    host="localhost",
    user=c.USUARIO_SQL,
    password=c.PASSWORD_SQL,
    database=c.NOMBRE_BASE_SQL,  # Tu usuario
)

# FUNCIONES AUXILIARES


def creacion_nodos(func):
    """
    Este decorador se ejecuta en cada ejecucion de las funciones de los ejercicios. Garantiza
    que la base de datos de neo4J este vacia antes de insertar los datos y muestra que los datos
    estan cargados o ejecuta una query adicional en funcion de lo que sea necesario

    Args:
        func (func): la funcion del ejercicio correspondiente
    """

    def wrapper(*args, **kwargs):
        with driver.session() as session:
            # Borramos todos los datos de la base de datos
            consulta = "MATCH (n) DETACH DELETE n"
            session.run(consulta)

            # Ahora no tenemos ninguno
            consulta = """
                        MATCH (n) RETURN n
                    """
            resultado = session.run(consulta)
            print("Comprobamos que este vacio")
            res = resultado.data()
            print(res)
            # Obtenemos las consultas para crear la base de datos
            with conexion_mysql:
                consulta, consulta_extra = func(*args, **kwargs)
            # las ejecutamos y mostramos todos los datos de los reviewers
            session.run(consulta)
            consulta = """
                              MATCH (n) RETURN n.reviewerID
                           """
            resultado = session.run(consulta)
            print("Comprobamos que estan los datos")
            res = resultado.data()
            print(res)

            # Si falta por ejecutar alguna consulta
            if consulta_extra:
                exec_consultas(consulta_extra)
            else:
                print("Datos insertados, se puede mirar en neo4J")

    return wrapper


def exec_consultas(consulta):
    """
    Ejecucion de las consultas de neo4J. Se muestran los resultados de las consultas.

    Args:
        consulta (str): la consulta de neo4J a ejecutar
    """
    with driver.session() as session:

        resultado = session.run(consulta)
        res = resultado.data()

        dataframe = pd.DataFrame([dict(record) for record in res])
        print(dataframe.to_string())


# Queries a SQL
"""
En todas estas queries los datos se pasan de agrupacion por filas (que devuelve SQL)
a agrupaciones por columnas para facilitar el proceso de trabajar con ellos a la hora de
insertarlos en sql. 
Por ello se ejecutara list(zip(*datos)), que consigue precisamente esto
"""


def obtener_usuarios(n_usuarios=c.N_USUARIOS_NEO):
    """
    Query correspondiente al ejercicio 1. Devuelve el reviewerId y el asin
    del producto valorado para los primeros N_USUARIOS_NEO ordenados por numero de reviews

    Returns:
        list, list: la primera es una lista con tuplas reviewer-producto en las que se
                    especifica que productos ha valorado cada reviewer. La segunda es una
                    lista de los usuarios sin repeticiones
    """

    cursor = conexion_mysql.cursor()
    sql = """SELECT r.reviewerID, r.asin
                FROM review r
                INNER JOIN (SELECT reviewerID
                                    FROM review
                                    GROUP BY reviewerID
                                    ORDER BY COUNT(*) DESC
                                    LIMIT %s) as t ON r.reviewerID = t.reviewerID;"""
    cursor.execute(sql, n_usuarios)
    datos = cursor.fetchall()
    usuarios, productos = list(zip(*datos))
    user_prod = {}
    for user, asin in zip(usuarios, productos):
        user_prod.setdefault(user, []).append(asin)
    usuarios = list(set(usuarios))
    return user_prod, usuarios


def obtener_all_asins():
    """
    Query correspondiente al ejercicio 2. Obtiene todos los asins
    en la base de datos y los devuelve

    Returns:
        list: la lista con todos los asins
    """

    cursor = conexion_mysql.cursor()
    sql = """SELECT DISTINCT asin
            FROM product
            """
    cursor.execute(sql)
    datos = cursor.fetchall()
    return list(datos)


def obtener_articulos(articulos):
    """
    Query asociada al ejercicio 2. Obtiene toda la informacion pedida de las
    reviews de los articulos dados

    Returns:
        list, list: la primera es una lista que contiene todos los datos separados
                    en 4 listas (una por columna), mientras que la segunda da todos
                    los usuarios distintos
    """

    cursor = conexion_mysql.cursor()
    sql = """SELECT asin, reviewerID, reviewTime, overall
                FROM review
                WHERE asin IN %s"""
    cursor.execute(sql, [articulos])
    datos = cursor.fetchall()
    datos = list(zip(*datos))
    usuarios = list(set(datos[1]))

    return datos, usuarios


def obtener_usuarios_y_tipos(n_usuarios=c.N_USUARIOS_NEO_EJ3):
    """
    Query asociada al ejercicio 3. Devuelve el id del reviewer el tipo
    y cuántas reviews ha hecho para ese tipo para aquellos rerviewers entre
    los primeros N_USUARIOS_NEO_EJ3 por orden alfabetico del reviewer name

    Returns:
        list, list, list: devuelve tres listas. La primera y la segunda son los
                          usuarios y tipos unicos respectivamente, mientras que
                          la tercera contiene los datos de cuantas reviews ha
                          hecho cada usuario a cada tipo
    """
    cursor = conexion_mysql.cursor()
    sql = """SELECT reviewerID, type, COUNT(*)
            FROM review r 
            WHERE reviewerID in (SELECT r2.reviewerID
                                FROM review r2 
                                INNER JOIN (SELECT reviewerID
                                            FROM reviewer
                                            ORDER BY reviewerName
                                            LIMIT %s) AS rev ON r2.reviewerID = rev.reviewerID 
                                GROUP BY reviewerID
                                HAVING COUNT(DISTINCT type) > 1)
            GROUP BY reviewerID, type;
                 """
    cursor.execute(sql, n_usuarios)
    datos = cursor.fetchall()
    usuarios, tipos, _ = list(zip(*datos))
    usuarios = list(set(usuarios))
    tipos = list(set(tipos))
    return usuarios, tipos, datos


def articulos_populares():
    """
    Query asociada al ejercicio 4. Devuelve los asin de los 5 productos
    mas populares con menos de 40 reviews además de los reviewers que los han
    valorado

    Returns:
        list, list, list: los usuarios y productos unicos respectivamente seguidos de los
                          datos de qué reviewer ha valorado qué productos
    """
    cursor = conexion_mysql.cursor()
    sql = """SELECT reviewerID, r.asin
             FROM review r
             INNER JOIN (SELECT asin
                            FROM review
                            GROUP BY asin
                            HAVING COUNT(*) < 40
                            ORDER BY COUNT(*) DESC
                            LIMIT 5) AS r2 on r.asin = r2.asin

                 """
    cursor.execute(sql)
    datos = cursor.fetchall()
    usuarios, productos = list(zip(*datos))
    usuarios = list(set(usuarios))
    productos = list(set(productos))
    return usuarios, productos, datos


# EJERCICIO 1


@creacion_nodos
def ejercicio1():
    """
    Funcion asociada al primer ejercicio. Calcula los primeros usuarios por numero de reviews,
    calcula sus similitudes de Jaccard y crea la consulta para añadir todos estos datos a neo4J.
    Además ejecuta una consulta que muestra el usuario que tiene más vecinos

    Returns:
        str, str: la consulta para crear la base de datos y la consulta para mostrar al que tiene
                  mas vecinos
    """
    # Si se quiere modificar el numero de usuarios, cambiar el valor de la variable N_USUARIOS_NEO
    user_prod, usuarios = obtener_usuarios()
    matriz_sim = calcular_similitudes(user_prod, usuarios)
    consulta = similitudes_neo4J(usuarios, matriz_sim)
    consulta_extra = """MATCH (r:REVIEWER) - [:SIM] -> (:REVIEWER)
    WITH r, COUNT{(r:REVIEWER) - [:SIM] -> (:REVIEWER)} AS c_sim
    WITH max(c_sim) as max
    MATCH (r:REVIEWER) - [:SIM] -> (:REVIEWER)
    WHERE COUNT{(r:REVIEWER) - [:SIM] -> (:REVIEWER)} = max
    RETURN DISTINCT r, COUNT{(:REVIEWER) - [:SIM] -> (e:REVIEWER)}"""
    return consulta, consulta_extra


def calcular_similitudes(user_prod, usuarios):
    """
    Calcula las similitudes de Jaccard de un conjunto de usuarios dados.
    Este calculo se apoya en un fichero. Si existe carga los datos y si no
    crea el fichero y guarda los datos ahí

    Args:
        user_prod (list): informacion sobre que usuario ha valorado que producto
        usuarios (list): la lista de los usuarios unicos

    Returns:
        list: una matriz con todas las similitudes
    """
    if not os.path.exists("similitudes.txt"):
        matriz_sim = []
        for i in range(len(usuarios)):
            sim_us = []
            for j in range(len(usuarios)):
                prods_us1 = user_prod[usuarios[i]]
                prods_us2 = user_prod[usuarios[j]]
                similitud = (len(set(prods_us1) & set(prods_us2))) / len(
                    set(prods_us1) | set(prods_us2)
                )

                sim_us.append(str(similitud))
            matriz_sim.append(sim_us)
        with open("similitudes.txt", "w") as f:
            f.write("\n".join([",".join(i) for i in matriz_sim]))
    else:
        with open("similitudes.txt", "r") as f:
            matriz_sim = [l.split(",") for l in f.readlines()]

    return matriz_sim


def similitudes_neo4J(usuarios, matriz_sim):
    """
    Crea una consulta de neo4J que dados los usuarios y su matriz de similitudes crea un
    grafo en neo4J con toda esta informacion

    Args:
        usuarios (list): lista de usuarios unicos
        matriz_sim (list): matriz con las similitudes de un usuario con otro

    Returns:
        str: la consulta para crear el grafo
    """
    q_usuarios = ",\n".join(
        [f"(reviewer_{n}:REVIEWER{{reviewerID:'{n}'}})" for n in usuarios]
    )
    q_similitudes = ""
    for i in range(len(usuarios)):
        for j in range(len(usuarios)):
            if i != j and matriz_sim[i][j] != "0.0":
                # No guardamos informacion en el grafo de la similitud de un usuario consigo mismo (siempre 1) o de usuarios cuya similitud sea 0
                q_similitudes += f"(reviewer_{usuarios[i]}) - [:SIM{{similitud:{float(matriz_sim[i][j])}}}] -> (reviewer_{usuarios[j]}),\n"

    q_unidas = ",\n\n".join([q_usuarios, q_similitudes])
    consulta = f"CREATE\n{q_unidas}".rstrip("\n,")
    return consulta


# EJERCICIO 2


@creacion_nodos
def ejercicio2():
    """
    La funcion crea un grafo con la informacion de los usuarios que han valorado n productos
    escogidos aleatoriamente de la categoria especificada por la variable CAT_EJERCICIO_2 del
    fichero de configuracion ademas de la informacion de los propios productos

    Returns:
        str, None: La consulta para crear el grafo junto con un None para significar que no
                   hay consulta adicional
    """
    n = None
    while not n:
        try:
            n = int(input("Intorduce el numero de productos: "))
        except ValueError:
            print("Valor no valido")
    # Obtenemos todos los asins y seleccionamos n de forma aleatoria
    asins = obtener_all_asins()
    asins_elegidos = random.sample(asins, n)
    datos, usuarios = obtener_articulos(asins_elegidos)
    consulta = productos_aleatorios_neo4J(asins_elegidos, usuarios, datos)
    return consulta, None


def productos_aleatorios_neo4J(productos, usuarios, datos):
    """
    Creamos la consulta para crear el grafo del ejercicio 2, un grafo que
    guarda información sobre los usuarios y los productos que valoran

    Args:
        productos (list): lista de los productos sin repetidos
        usuarios (list): lista de los usuarios sin repetidos
        datos (list): lista de listas con informacion de las reseñas

    Returns:
        str: la consulta para crear el grafo
    """
    q_usuarios = ",\n".join(
        [f"(reviewer_{n}:REVIEWER{{reviewerID:'{n}'}})" for n in usuarios]
    )
    q_productos = ",\n".join(
        [f"(product_{n[0]}:PRODUCT{{asin:'{n[0]}'}})" for n in productos]
    )
    q_relaciones = ""
    for prod, user, t, nota in zip(*datos):
        q_relaciones += f"(reviewer_{user}) - [:REVIEWS{{time:'{t}', overall:{nota}}}] -> (product_{prod}),\n"
    q_relaciones = q_relaciones.rstrip(",\n")
    q_unidas = ",\n\n".join([q_usuarios, q_productos, q_relaciones])
    consulta = f"CREATE\n{q_unidas}"
    return consulta


# EJERCICIO 3


@creacion_nodos
def ejercicio3():
    """
    Crea un grafo con información sobre los usuarios de los 400 primeros que
    han valorado más de un tipo de producto, representando que usuarios han
    valorado que tipos

    Returns:
        str, None: la consulta y un None para indicar que no hay consulta extra
    """
    usuarios, tipos, datos = obtener_usuarios_y_tipos()
    consulta = reviews_por_tipo(usuarios, tipos, datos)
    return consulta, None


def reviews_por_tipo(usuarios, tipos, datos):
    """
    Crea la consulta para hacer el grafo del ejercicio 3 en el que se
    guardan los usuarios que han valorado más de un tipo y qué tipos han
    valorado

    Args:
        usuarios (list): lista de los usuarios que han valorado mas de un tipo sin repetidos
        tipos (list): los tipos de articulos valorados
        datos (list): datos sobre que usuarios han valorado que tipos

    Returns:
        str: la consulta para crear el grafo
    """
    q_usuarios = ",\n".join(
        [f"(reviewer_{n}:REVIEWER{{reviewerID:'{n}'}})" for n in usuarios]
    )
    q_productos = ",\n".join([f"(type_{n}:TYPE{{asin:'{n}'}})" for n in tipos])
    q_relaciones = ""

    for user, tipo, n in datos:
        q_relaciones += (
            f"(reviewer_{user}) - [:REVIEWS{{n_products:{n}}}] -> (type_{tipo}),\n"
        )
    q_relaciones = q_relaciones.rstrip(",\n")
    q_unidas = ",\n\n".join([q_usuarios, q_productos, q_relaciones])
    consulta = f"CREATE\n{q_unidas}"
    return consulta


# EJERCICIO 4


@creacion_nodos
def ejercicio4():
    """
    Crea un grafo con información sobre los usuarios que han valorado los 5
    articulos mas populares con menos de 40 reviews y representa que usuarios han
    valorado que productos y además indica cuantos productos en comun de los 5 tienen
    2 usuarios

    Returns:
        str, str: la consulta y un None para indicar que no hay consulta extra
    """
    usuarios, productos, datos = articulos_populares()
    consulta = articulos_y_usuarios(usuarios, productos, datos)
    # Para la informacion sobre los productos en comun que tienen dos usuarios
    consulta_neo_enlaces = """MATCH (u1:REVIEWER) - [:REVIEWS] -> (p:PRODUCT) <- [:REVIEWS] - (u2:REVIEWER)
                              WITH u1, u2, COUNT(p) AS num_comun
                              WHERE u1 <> u2 
                              MERGE (u1) - [:ENLACE{ n_prods_comun:num_comun}] -> (u2)
        """
    return consulta, consulta_neo_enlaces


def articulos_y_usuarios(usuarios, productos, datos):
    """
    Crea la consulta para hacer el grafo del ejercicio 4 en el que se
    guardan los usuarios que han valorado alguno de los 5 productos mas
    populares con menos de 40 reviews

    Args:
        usuarios (list): lista de los usuarios sin repetidos
        productos (list): los productos valorados
        datos (list): datos sobre que usuarios han valorado que productos

    Returns:
        str: la consulta para crear el grafo
    """
    q_usuarios = ",\n".join(
        [f"(reviewer_{n}:REVIEWER{{reviewerID:'{n}'}})" for n in usuarios]
    )
    q_productos = ",\n".join(
        [f"(product_{n}:PRODUCT{{asin:'{n}'}})" for n in productos]
    )
    q_relaciones = ""

    for user, tipo in datos:
        q_relaciones += f"(reviewer_{user}) - [:REVIEWS] -> (product_{tipo}),\n"
    q_relaciones = q_relaciones.rstrip(",\n")
    q_unidas = ",\n\n".join([q_usuarios, q_productos, q_relaciones])
    consulta = f"CREATE\n{q_unidas}"
    return consulta


if __name__ == "__main__":
    if c.EJERCICIO == 1:
        ejercicio1()
    elif c.EJERCICIO == 2:
        ejercicio2()
    elif c.EJERCICIO == 3:
        ejercicio3()
    else:
        ejercicio4()
