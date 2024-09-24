"""
================
insert_dataset.py
================

Developed by Sergio Jiménez Romero and Alberto Velasco Rodríguez

This file cleans and inserts data from a new file into the already created databases.

Regarding the configuration parameters, SQL credentials and data paths should be checked.
"""

import config as c
import os
import json
from pymongo import MongoClient
import pymysql
from time import perf_counter


def create_sql_insertion(table_name, guide) -> str:
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


# *** General ***
def insert_dataset(file_name):
    """Cleans and inserts data into the already created databases."""
    sql_insertions = [
        create_sql_insertion(table_name, table_guide)
        for table_name, table_guide in c.GUIAS_TABLAS_SQL.items()
    ]

    sql_max_id = """SELECT id
                    FROM review
                    ORDER BY id DESC
                    LIMIT 1;"""

    sql_reviewername = """SELECT reviewerName
                        FROM reviewer
                        WHERE reviewerID = %s
                        LIMIT 1;"""

    mysql_connection = pymysql.connect(
        host="localhost",
        user=c.USUARIO_SQL,
        password=c.PASSWORD_SQL,
        database=c.NOMBRE_BASE_SQL,
    )

    # Create the database connection
    CONNECTION_STRING = "mongodb://localhost:27017"
    client = MongoClient(CONNECTION_STRING)
    db = client[c.NOMBRE_BASE_MONGODB]

    # Access the collection
    collection = db[c.NOMBRE_TABLA_MONGODB]

    with mysql_connection:
        cursor = mysql_connection.cursor()

        # Get the next id
        cursor.execute(sql_max_id)
        id_review = int(cursor.fetchone()[0]) + 1

        reviewers = (
            {}
        )  # must be a dictionary to save the reviewer's name to keep the first one that appears
        asins = []
        path = os.path.join(c.DIRECTORIO_DATOS, file_name)
        print(file_name[:-5])
        with open(path, "r") as f:
            for line in f:
                line = json.loads(line)

                # *** Data processing ***
                line["id"] = id_review
                line["type"] = file_name[:-5]

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
                        # We must ensure that it was not inserted before
                        cursor.execute(sql_reviewername, line["reviewerID"])
                        reviewername = cursor.fetchone()

                        if reviewername is not None:
                            reviewers[line["reviewerID"]] = line["reviewerName"]
                        else:
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

                cursor.execute(
                    sql_insertions[2],
                    [line[guide_data] for guide_data in c.GUIAS_TABLAS_SQL["review"]],
                )

                collection.insert_one(
                    {
                        guide_data: line[guide_data]
                        for guide_data in c.GUIA_TABLA_MONGODB
                    }
                )

                id_review += 1

        mysql_connection.commit()
        cursor.close()


if __name__ == "__main__":
    t = perf_counter()
    insert_dataset(c.NOMBRE_FICHEROS_EXTRA)
    print(f"Time to load data: {perf_counter() - t}")
