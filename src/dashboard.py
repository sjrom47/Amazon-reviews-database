"""
=====================
dashboard.py
=====================

Developed by Sergio Jiménez Romero and Alberto Velasco Rodríguez

This file creates a dashboard with graphs about Amazon review data. It is necessary to run load_data.py before running this.

Regarding the configuration parameters, it is only necessary to ensure that the SQL and MongoDB credentials are correct.
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
import config as c
from wordcloud import WordCloud
import pandas as pd


def get_client() -> MongoClient:
    """
    Function to get a client connection
    """
    # Indicate the connection string (in this case, localhost)
    CONNECTION_STRING = "mongodb://localhost:27017"
    # create the connection using mongoClient
    return MongoClient(CONNECTION_STRING)


def get_database(database: str):
    """
    Function to get the MongoDB database

    Args:
        database (str): the name of the database

    Returns:
        database: database connection
    """
    client = get_client()
    databases = client.list_database_names()
    if database not in databases:
        raise Exception("The database does not exist")

    # return the database connection
    return client[database]


def sql_queries(sql, data=None):
    """
    Function to automate SQL queries to the server

    Args:
        sql (str): the SQL query
        data (list, optional): data if we want to pass parameters to the SQL query. Defaults to None.

    Returns:
        list: list of tuples containing the results for each of the requested parameters
    """
    mysql_connection = pymysql.connect(
        host="localhost",
        user=c.USUARIO_SQL,
        password=c.PASSWORD_SQL,
        database=c.NOMBRE_BASE_SQL,  # Your user
    )
    with mysql_connection:
        cursor = mysql_connection.cursor()
        if data:
            # If it has parameters, we substitute them in the query
            cursor.execute(sql, data)
        else:
            cursor.execute(sql)
        data = cursor.fetchall()
        # Transform the data from rows to columns to better operate on them
        data = list(zip(*data))
    return data


# SQL connection
mysql_connection = pymysql.connect(
    host="localhost",
    user=c.USUARIO_SQL,
    password=c.PASSWORD_SQL,
    database=c.NOMBRE_BASE_SQL,  # Your user
)
# Mongo connection
dbname = get_database(c.NOMBRE_BASE_MONGODB)
collection = dbname[c.NOMBRE_TABLA_MONGODB]
# We do some queries beforehand to get the categories and product numbers
with mysql_connection:
    cursor = mysql_connection.cursor()
    sql = """SELECT DISTINCT(type)
                FROM product"""
    cursor.execute(sql)
    product_types = [i[0] for i in cursor.fetchall()]
    sql = """SELECT DISTINCT(asin)
                FROM product"""
    cursor.execute(sql)
    product_numbers = [i[0] for i in cursor.fetchall()]
# Dashboard styles
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
# Query to get the number of users in the graph without callback
# We do it beforehand because the graph does not update
query_users = """SELECT COUNT(*), num_rev
                    FROM (SELECT reviewerID, COUNT(*) AS num_rev
                        FROM review
                        GROUP BY reviewerID) AS conteo
                    GROUP BY num_rev
                    ORDER BY num_rev

"""
users, n_reviews = sql_queries(query_users)
# Transformation so that the histogram does not group several quantities into a single bin
users = [
    users[n_reviews.index(i)] if i in n_reviews else 0
    for i in range(max(n_reviews) + 1)
]
n_reviews = list(range(max(n_reviews) + 1))
# Graph styles
graph_style = {
    "width": "45%",
    "height": "60vh",
    "display": "inline-block",
    "padding": "2%",
}

# Step 1. Create the app
app = Dash(__name__)

# Step 2. Set the title
app.title = "Amazon Reviews Visualization Menu"

# Step 3. Set the layout

app.layout = html.Div(
    [
        # Title
        html.H1(
            "Amazon Reviews Visualization Menu",
            style={"text-align": "center", "font_family": "sans-serif"},
        ),
        # Content
        # Each div corresponds to a graph and its dropdown. It has to be done this way because otherwise the
        # dropdowns occupy the entire width of the page
        html.Div(
            [
                dcc.Dropdown(
                    id="dropdown-1",  ## dropdown menu
                    options=[{"label": i, "value": i} for i in ["All"] + product_types],
                    value="All",
                ),  ## selected state
                dcc.Graph(id="graph-1"),  # Reviews per year
            ],
            style=graph_style,
        ),
        html.Div(
            [
                dcc.Dropdown(
                    id="dropdown-2",  ## dropdown menu
                    options=[{"label": i, "value": i} for i in ["All"] + product_types],
                    value="All",
                ),  ## selected state
                dcc.Graph(id="graph-2"),  # Popularity evolution
            ],
            style=graph_style,
        ),
        html.Div(
            [
                dcc.Dropdown(
                    id="dropdown-3",  ## dropdown menu
                    options=[
                        {"label": i, "value": i}
                        for i in ["All"] + product_types + product_numbers
                    ],
                    value="All",
                ),  ## selected state
                dcc.Graph(id="graph-3"),  # Reviews by rating
            ],
            style=graph_style,
        ),
        html.Div(
            [
                dcc.Dropdown(
                    id="dropdown-4",  ## dropdown menu
                    options=[{"label": i, "value": i} for i in ["All"] + product_types],
                    value="All",
                ),  ## selected state
                dcc.Graph(id="graph-4"),  # Reviews evolution
            ],
            style=graph_style,
        ),
        # This graph does not have a dropdown or a callback, it does not update
        html.Div(
            dcc.Graph(
                figure=px.histogram(
                    x=n_reviews,
                    y=users,
                    title="Reviews per user",
                    nbins=len(users),
                ),
                id="graph-5",  # Reviews per user
            ),
            style=graph_style,
        ),
        html.Div(
            [
                dcc.Dropdown(
                    id="dropdown-6",  ## dropdown menu
                    options=[{"label": i, "value": i} for i in product_types],
                    value=product_types[0],
                ),  ## selected state
                dcc.Graph(id="graph-6"),  # Wordcloud
            ],
            style=graph_style,
        ),  ## selected state
        html.Div(
            [
                dcc.Dropdown(
                    id="dropdown-7",  ## dropdown menu
                    options=[{"label": i, "value": i} for i in ["All"] + product_types],
                    value="All",
                ),  ## selected state
                dcc.Graph(id="graph-7"),  # Average evolution
            ],
            style=graph_style,
        ),  ## selected state
    ]
)


# Step 3. Set the callback functions
"""
In all callback functions where the graph can select All, this will immediately
be replaced by a list with all categories, which are found by a query before
the dashboard is launched
"""


# In these decorators, the input and output of
# each callback function are specified
@app.callback(
    Output(component_id="graph-1", component_property="figure"),
    Input(component_id="dropdown-1", component_property="value"),
)
def update_graph(selected_category):
    """
    Updates graph 1 based on the selected category
    """
    if selected_category == "All":
        selected_category = product_types
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
        title="Reviews per year of all products",
    )
    return bar_fig


@app.callback(
    Output(component_id="graph-2", component_property="figure"),
    Input(component_id="dropdown-2", component_property="value"),
)
def update_graph(selected_category):
    """
    Updates graph 2 based on the selected category
    """
    if selected_category == "All":
        selected_category = product_types
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
        title="Popularity evolution of all products",
    )
    return line_fig


@app.callback(
    Output(component_id="graph-3", component_property="figure"),
    Input(component_id="dropdown-3", component_property="value"),
)
def update_graph(selected_category):
    """
    Updates graph 3 based on the selected category
    """
    if selected_category == "All":
        selected_category = product_types
    else:
        selected_category = [selected_category]

    # Since we don't know if what we receive is an asin or a category, we try to search for both
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
        title="Reviews by rating of all products",
    )
    return bar_fig


@app.callback(
    Output(component_id="graph-4", component_property="figure"),
    Input(component_id="dropdown-4", component_property="value"),
)
def update_graph(selected_category):
    """
    Updates graph 4 based on the selected category
    """
    if selected_category == "All":
        selected_category = product_types
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
        title="Review evolution over time of all products",
    )
    return line_fig


@app.callback(
    Output(component_id="graph-6", component_property="figure"),
    Input(component_id="dropdown-6", component_property="value"),
)
def update_graph(selected_category):
    """
    Updates graph 6 based on the selected category
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

    # To display it in plotly, it is necessary to convert the wordcloud to an image and export it this way
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
    Updates graph 7 based on the selected category
    """
    if selected_category == "All":
        selected_category = product_types
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
    df.columns = ["type", "year", "average_overall"]
    selected_df = df[df["type"].isin(selected_category)]

    line_fig = px.line(
        selected_df,
        x="year",
        y="average_overall",
        color="type",
        title="Average review evolution",
    )
    return line_fig


# Step 4. run the app in the external mode

if __name__ == "__main__":
    app.run_server()
