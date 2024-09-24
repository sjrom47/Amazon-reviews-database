"""
================
load_data.py
================

Developed by Sergio Jiménez Romero and Alberto Velasco Rodríguez

This file creates the SQL and MongoDB databases.

Regarding the configuration parameters, SQL credentials and data paths should be checked.
"""

import config as c
import os
import json
from pymongo import MongoClient
import pymysql
from time import perf_counter


# *** SQL ***
def create_sql_database() -> None:
    """Attempts to drop the database, recreates it, and commits."""
    mysql_connection_create = pymysql.connect(
        host="localhost", user=c.USUARIO_SQL, password=c.PASSWORD_SQL
    )

    with mysql_connection_create:
        cursor = mysql_connection_create.cursor()

        try:
            sql = f"DROP DATABASE {c.NOMBRE_BASE_SQL};"
            cursor.execute(sql)
            mysql_connection_create.commit()
        except pymysql.err.OperationalError:
            pass

        sql = f"CREATE DATABASE {c.NOMBRE_BASE_SQL};"
        cursor.execute(sql)
        mysql_connection_create.commit()

        cursor.close()


def create_sql_table(sql_table: str) -> None:
    """Executes the given table creation query and commits.

    Args:
        sql_table (str): SQL query to create the table
    """
    mysql_connection = pymysql.connect(
        host="localhost",
        user=c.USUARIO_SQL,
        password=c.PASSWORD_SQL,
        database=c.NOMBRE_BASE_SQL,
    )

    with mysql_connection:
        cursor = mysql_connection.cursor()
        cursor.execute(sql_table)
        mysql_connection.commit()
        cursor.close()


def create_sql_insertion(table_name: str, guide: list) -> str:
    """Returns an SQL query to insert data into the 'table_name' table,
    which follows the 'guide' structure.

    Args:
        table_name (str): name of the table to which the data is to be inserted.
        guide (list): names of the table fields

    Returns:
        str: parameterized SQL query to insert data
    """
    # Now strings are created to introduce the column names and then the data in the INSERT INTO
    string_param = ""
    string_guide = ""
    for column in guide:
        string_param = f"{string_param}%s, "
        string_guide = f"{string_guide}{column}, "
    string_param = string_param.rstrip(", ")
    string_guide = string_guide.rstrip(", ")

    return f"""INSERT INTO {table_name} ({string_guide})
            VALUES ({string_param});
    """


# *** MongoDB ***
def create_mongodb_database() -> None:
    """Drops the MongoDB database and recreates it along with the collection."""
    # Create the connection to the new database
    CONNECTION_STRING = "mongodb://localhost:27017"
    client = MongoClient(CONNECTION_STRING)
    client.drop_database(c.NOMBRE_BASE_MONGODB)

    db = client[c.NOMBRE_BASE_MONGODB]
    db.create_collection(c.NOMBRE_TABLA_MONGODB)


# *** General ***
def clean_data() -> None:
    """Cleans and inserts data into the empty databases."""
    sql_insertions = [
        create_sql_insertion(table_name, table_guide)
        for table_name, table_guide in c.GUIAS_TABLAS_SQL.items()
    ]

    mysql_connection_table = pymysql.connect(
        host="localhost",
        user=c.USUARIO_SQL,
        password=c.PASSWORD_SQL,
        database=c.NOMBRE_BASE_SQL,
    )

    # Create the connection to the new database
    CONNECTION_STRING = "mongodb://localhost:27017"
    client = MongoClient(CONNECTION_STRING)
    db = client[c.NOMBRE_BASE_MONGODB]

    # Access the collection
    collection = db[c.NOMBRE_TABLA_MONGODB]

    with mysql_connection_table:
        cursor = mysql_connection_table.cursor()

        reviewers = (
            {}
        )  # must be a dictionary to save the reviewer's name to keep the first one that appears
        id_review = 1
        for name in c.NOMBRE_FICHEROS_DATOS:
            asins = []
            path = os.path.join(c.DIRECTORIO_DATOS, name)
            print(name[:-5])
            with open(path, "r") as f:
                for line in f:
                    line = json.loads(line)

                    # *** Data processing ***
                    line["id"] = id_review

                    line["type"] = name[:-5]

                    # Set certain data to None
                    for guide_list in c.GUIAS_TABLAS_SQL.values():
                        for guide_data in guide_list:
                            if guide_data not in line:
                                line[guide_data] = None
                            elif line[guide_data] in ["", " "]:
                                line[guide_data] = None

                    # Convert reviewTime to DATE format
                    if line["reviewTime"] is not None:
                        # Example of reviewTime: 04 22, 2014
                        # We can see that the three elements are separated by spaces
                        reviewtime = line["reviewTime"].split(" ")
                        if len(reviewtime) == 3:
                            month, day, year = reviewtime
                            day = day.strip(
                                ","
                            )  # day is the one that kept the comma from registered
                            line["reviewTime"] = f"{year}-{month}-{day}"
                        else:  # if it is not correct, we set it to NULL
                            line["reviewTime"] = None

                    # Insert the reviewer if it has not already been created
                    if line["reviewerID"] is not None:
                        if line["reviewerID"] not in reviewers:  # in the reviewers keys
                            reviewers[line["reviewerID"]] = line["reviewerName"]
                            cursor.execute(
                                sql_insertions[0],
                                [
                                    line[guide_data]
                                    for guide_data in c.GUIAS_TABLAS_SQL["reviewer"]
                                ],
                            )
                        else:
                            line["reviewerName"] = reviewers[line["reviewerID"]]

                    # Insert the product if it has not already been done
                    if line["asin"] is not None and line["asin"] not in asins:
                        asins.append(line["asin"])
                        cursor.execute(
                            sql_insertions[1],
                            [
                                line[guide_data]
                                for guide_data in c.GUIAS_TABLAS_SQL["product"]
                            ],
                        )

                    # Create the reviews
                    cursor.execute(
                        sql_insertions[2],
                        [
                            line[guide_data]
                            for guide_data in c.GUIAS_TABLAS_SQL["review"]
                        ],
                    )

                    collection.insert_one(
                        {
                            guide_data: line[guide_data]
                            for guide_data in c.GUIA_TABLA_MONGODB
                        }
                    )

                    id_review += 1

        mysql_connection_table.commit()
        cursor.close()


def load_data():
    """Creates the databases and cleans and loads the data into them."""
    sql_tables = [
        """
        CREATE TABLE reviewer (
            reviewerID VARCHAR(40) NOT NULL,
            reviewerName TEXT,
            PRIMARY KEY (reviewerID)
        );""",
        """
        CREATE TABLE product (
            asin VARCHAR(40) NOT NULL,
            type VARCHAR(80) NOT NULL,
            PRIMARY KEY (asin, type)
        );""",
        """
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
        );""",
    ]
    create_sql_database()
    for sql in sql_tables:
        create_sql_table(sql)
    create_mongodb_database()

    clean_data()


if __name__ == "__main__":
    t = perf_counter()
    load_data()
    print(f"Time to load data: {perf_counter() - t}")
