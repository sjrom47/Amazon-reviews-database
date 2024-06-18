"""
=====================
menu_visualizacion.py
=====================

Desarrollado por Sergio Jiménez Romero y Alberto Velasco Rodríguez

Este fichero crea un dashboard en el que hay graficos sobre los datos de 
las reviews de amazon. Es necesario ejecutar load_data.py antes de poder ejecutar
este.

En cuanto a los parametros de configuracion, solo es necesario asegurarse de que las 
credenciales de SQL y mongoDB sean correctas
"""

import dash

from dash import html, dcc, dash_table


from dash.dependencies import Input, Output
import pandas as pd
import numpy as np
import plotly.express as px
from dash import Dash

import pymysql
from pymongo import MongoClient, InsertOne, UpdateOne, ReplaceOne
import json
import os
import configuracion as c
from wordcloud import WordCloud
import pandas as pd


def get_client() -> MongoClient:
    """
    Funcion para obtener una conexion del cliente
    """
    # Indicamos la cadena de conexion (en este caso, localhost)
    CONNECTION_STRING = "mongodb://localhost:27017"
    # creamos la conexion empleando mongoClient
    return MongoClient(CONNECTION_STRING)


def get_database(database: str):
    """
    Funcion para obtener la base de datos de MongoDB

    Args:
        database (str): el nombre de la base de datos

    Returns:
        database: conexion de la base de datos
    """
    client = get_client()
    databases = client.list_database_names()
    if database not in databases:
        raise Exception("No existe la base de datos")

    # devolvemos la conexion de la bbdd
    return client[database]


def sql_queries(sql, data=None):
    """
    Funcion para automatizar las queries de SQL al servidor

    Args:
        sql (str): la query de SQL
        data (list, optional): datos si le queremos pasar parametros a la query de SQL. Defaults to None.

    Returns:
        list: lista de tuplas que contienen los resultados para cada uno de los parametros pedidos
    """
    conexion_mysql = pymysql.connect(
        host="localhost",
        user=c.USUARIO_SQL,
        password=c.PASSWORD_SQL,
        database=c.NOMBRE_BASE_SQL,  # Tu usuario
    )
    with conexion_mysql:
        cursor = conexion_mysql.cursor()
        if data:
            # Si tiene parametros los sustituimos en la query
            cursor.execute(sql, data)
        else:
            cursor.execute(sql)
        datos = cursor.fetchall()
        # Pasamos los datos de filas a columnas para poder operar mejor sobre ellos
        datos = list(zip(*datos))
    return datos


# Conexion a SQL
conexion_mysql = pymysql.connect(
    host="localhost",
    user=c.USUARIO_SQL,
    password=c.PASSWORD_SQL,
    database=c.NOMBRE_BASE_SQL,  # Tu usuario
)
# Conexion a mongo
dbname = get_database(c.NOMBRE_BASE_MONGODB)
collection = dbname[c.NOMBRE_TABLA_MONGODB]
# Hacemos algunas queries antes para sacar las categorias y los numeros de los productos
with conexion_mysql:
    cursor = conexion_mysql.cursor()
    sql = """SELECT DISTINCT(type)
                FROM product"""
    cursor.execute(sql)
    tipos_producto = [i[0] for i in cursor.fetchall()]
    sql = """SELECT DISTINCT(asin)
                FROM product"""
    cursor.execute(sql)
    nums_articulos = [i[0] for i in cursor.fetchall()]
# Estilos del dashboard
tabs_styles = {"height": "44px"}
tab_style = {
    "borderBottom": "1px solid #d6d6d6",
    "padding": "6px",
    "fontWeight": "bold",
}

tab_selected_style = {
    "borderTop": "1px solid #d6d6d6",
    "borderBottom": "1px solid #d6d6d6",
    "backgroundColor": "#119DFF",
    "color": "white",
    "padding": "6px",
}
# Query para sacar el numero de usuarios en el grafico sin callback
# La hacemos antes porque el grafico no se actualiza
query_usuarios = """SELECT COUNT(*), num_rev
                    FROM (SELECT reviewerID, COUNT(*) AS num_rev
                        FROM review
                        GROUP BY reviewerID) AS conteo
                    GROUP BY num_rev
                    ORDER BY num_rev

"""
usuarios, n_reviews = sql_queries(query_usuarios)
# Transformacion para que el histograma no agrupe varias cantidades en una sola bin
usuarios = [
    usuarios[n_reviews.index(i)] if i in n_reviews else 0
    for i in range(max(n_reviews) + 1)
]
n_reviews = list(range(max(n_reviews) + 1))
# Estilo de los graficos
graph_style = {
    "width": "45%",
    "height": "60vh",
    "display": "inline-block",
    "padding": "2%",
}

# Step 1. Create the app
app = Dash(__name__)

# Step 2. Set the title
app.title = "Menu visualizacion reviews Amazon"

# Step 3. Set the layout

app.layout = html.Div(
    [
        # Titulo
        html.H1(
            "Menu visualizacion reviews Amazon",
            style={"text-align": "center", "font_family": "sans-serif"},
        ),
        # Contenido
        # Cada div corresponde a un grafico y su dropdown. Hay que hacerlo asi porque si no los
        # dropdowns ocupan todo el ancho de la pagina
        html.Div(
            [
                dcc.Dropdown(
                    id="dropdown-1",  ## dropdown menu
                    options=[
                        {"label": i, "value": i} for i in ["Todo"] + tipos_producto
                    ],
                    value="Todo",
                ),  ## selected state
                dcc.Graph(id="graph-1"),  # Reviews por año
            ],
            style=graph_style,
        ),
        html.Div(
            [
                dcc.Dropdown(
                    id="dropdown-2",  ## dropdown menu
                    options=[
                        {"label": i, "value": i} for i in ["Todo"] + tipos_producto
                    ],
                    value="Todo",
                ),  ## selected state
                dcc.Graph(id="graph-2"),  # Evolucion de popularidad
            ],
            style=graph_style,
        ),
        html.Div(
            [
                dcc.Dropdown(
                    id="dropdown-3",  ## dropdown menu
                    options=[
                        {"label": i, "value": i}
                        for i in ["Todo"] + tipos_producto + nums_articulos
                    ],
                    value="Todo",
                ),  ## selected state
                dcc.Graph(id="graph-3"),  # Reviews por nota
            ],
            style=graph_style,
        ),
        html.Div(
            [
                dcc.Dropdown(
                    id="dropdown-4",  ## dropdown menu
                    options=[
                        {"label": i, "value": i} for i in ["Todo"] + tipos_producto
                    ],
                    value="Todo",
                ),  ## selected state
                dcc.Graph(id="graph-4"),  # Evolucion de las reviews
            ],
            style=graph_style,
        ),
        # Este grafico no tiene un dropdown ni un callback, no se actualiza
        html.Div(
            dcc.Graph(
                figure=px.histogram(
                    x=n_reviews,
                    y=usuarios,
                    title="Reviews por usuario",
                    nbins=len(usuarios),
                ),
                id="graph-5",  # Reviews por usuario
            ),
            style=graph_style,
        ),
        html.Div(
            [
                dcc.Dropdown(
                    id="dropdown-6",  ## dropdown menu
                    options=[{"label": i, "value": i} for i in tipos_producto],
                    value=tipos_producto[0],
                ),  ## selected state
                dcc.Graph(id="graph-6"),  # Wordcloud
            ],
            style=graph_style,
        ),  ## selected state
        html.Div(
            [
                dcc.Dropdown(
                    id="dropdown-7",  ## dropdown menu
                    options=[
                        {"label": i, "value": i} for i in ["Todo"] + tipos_producto
                    ],
                    value="Todo",
                ),  ## selected state
                dcc.Graph(id="graph-7"),  # Evolucion de las medias
            ],
            style=graph_style,
        ),  ## selected state
    ]
)


# Step3. Set the callback functions
"""
En todas las funciones de callback en cuyo grafico se pueda seleccionar Todo, este inmediatamente
va a ser sustituido por una lista con todas las categorias, que son halladas por una query antes de
que se lance el dashboard 
"""


# En estos decoradores se especifica cual es el input y el output de
# cada funcion de callback
@app.callback(
    Output(component_id="graph-1", component_property="figure"),
    Input(component_id="dropdown-1", component_property="value"),
)
def update_graph(selected_category):
    """
    Actualiza el grafico 1 en funcion de la categoria elegida
    """
    if selected_category == "Todo":
        selected_category = tipos_producto
    else:
        selected_category = [selected_category]

    sql = """SELECT year(reviewTime), COUNT(*)
                FROM review
                WHERE type in %s
                GROUP BY year(reviewTime);
        """
    x, y = sql_queries(sql, [selected_category])
    bar_fig = px.bar(
        x=x,
        y=y,
        title="Reviews por año de todos los productos",
    )
    return bar_fig


@app.callback(
    Output(component_id="graph-2", component_property="figure"),
    Input(component_id="dropdown-2", component_property="value"),
)
def update_graph(selected_category):
    """
    Actauliza el grafico 2 en funcion de la categoria elegida
    """
    if selected_category == "Todo":
        selected_category = tipos_producto
    else:
        selected_category = [selected_category]

    sql = """SELECT asin, COUNT(*)
                FROM review
                WHERE type in %s
                GROUP BY asin
                ORDER BY COUNT(*) DESC;
        """

    x, y = sql_queries(sql, [selected_category])
    x = [x for x, _ in enumerate(x)]
    line_fig = px.line(
        x=x,
        y=y,
        title="Evolucion de popularidad de todos los productos",
    )
    return line_fig


@app.callback(
    Output(component_id="graph-3", component_property="figure"),
    Input(component_id="dropdown-3", component_property="value"),
)
def update_graph(selected_category):
    """
    Actualiza el grafico 3 en funcion de la categoria elegida
    """
    if selected_category == "Todo":
        selected_category = tipos_producto
    else:
        selected_category = [selected_category]

    # Como no sabemos si lo que nos llega es un asin o una categoria, probamos a buscar ambos
    sql = """SELECT overall, COUNT(*)
                FROM review
                WHERE type in %s OR asin in %s
                GROUP BY overall
                ORDER BY overall;
        """

    x, y = sql_queries(sql, [selected_category, selected_category])
    bar_fig = px.bar(
        x=x,
        y=y,
        title="Reviews por nota de todos los productos",
    )
    return bar_fig


@app.callback(
    Output(component_id="graph-4", component_property="figure"),
    Input(component_id="dropdown-4", component_property="value"),
)
def update_graph(selected_category):
    """
    Actualiza el grafico 4 en funcion de la categoria seleccionada
    """
    if selected_category == "Todo":
        selected_category = tipos_producto
    else:
        selected_category = [selected_category]

    sql = """SELECT unixReviewTime
                FROM review
                WHERE type in %s 
                ORDER BY unixReviewTime;
        """
    d = sql_queries(sql, [selected_category])
    y, x = list(zip(*enumerate(d[0])))
    y = y[::100]
    x = x[::100]
    line_fig = px.line(
        x=x,
        y=y,
        title="Evolucion de las review a lo largo del tiempo de todos los productos",
    )
    return line_fig


@app.callback(
    Output(component_id="graph-6", component_property="figure"),
    Input(component_id="dropdown-6", component_property="value"),
)
def update_graph(selected_category):
    """
    Actualiza el grafico 6 en funcion de la categoria seleccionada
    """

    sql = """SELECT id
                FROM review
                WHERE type in %s 
        """
    d = sql_queries(sql, [[selected_category]])
    x = [int(i) for i in d[0]]
    data = collection.find({"id": {"$in": x}}, {"summary": 1, "_id": 0})
    wordcloud_text = " ".join(
        [" ".join([i for i in j["summary"].split() if len(i) > 2]) for j in data]
    )
    word_cloud = WordCloud(background_color="white").generate(wordcloud_text)

    # Para poder mostrarla en plotly, es necesario convertir la wordcloud a imagen y exportarla de
    # esta manera
    word_cloud = px.imshow(word_cloud.to_array())
    word_cloud.update_layout(
        xaxis={"visible": False},
        yaxis={"visible": False},
        margin={"t": 0, "b": 0, "l": 0, "r": 0},
        hovermode=False,
        paper_bgcolor="#F9F9FA",
        plot_bgcolor="#F9F9FA",
    )
    return word_cloud


@app.callback(
    Output(component_id="graph-7", component_property="figure"),
    Input(component_id="dropdown-7", component_property="value"),
)
def update_graph(selected_category):
    """
    Actualiza el grafico 7 en funcion de la categoria elegida
    """
    if selected_category == "Todo":
        selected_category = tipos_producto
    else:
        selected_category = [selected_category]

    sql = """SELECT type, YEAR(reviewTime), AVG(overall)
                FROM review
                WHERE type in %s 
                GROUP BY type, YEAR(reviewTime)
                ORDER BY YEAR(reviewTime);
        """
    types, year, overall = sql_queries(sql, [selected_category])
    df = pd.DataFrame([types, year, overall]).transpose()
    df.columns = ["tipo", "año", "media_overall"]
    selected_df = df[df["tipo"].isin(selected_category)]

    line_fig = px.line(
        selected_df,
        x="año",
        y="media_overall",
        color="tipo",
        title="Evolucion de las medias de las reviews",
    )
    return line_fig


# Step 4. run the app in the external mode

if __name__ == "__main__":
    app.run_server()
