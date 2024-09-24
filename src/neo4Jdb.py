"""
===========
neo4Jdb.py
===========

Developed by Sergio Jiménez Romero and Alberto Velasco Rodríguez

This file creates a graph in neo4J following the project requirements with 4 different cases, one per exercise.

Regarding the configuration parameters, SQL credentials, neo4J credentials, the neo4J URI must be checked, 
verify that the parameters are as desired (they are explained in the corresponding functions) and verify that the Docker
container is running.
"""

from neo4j import GraphDatabase
import pandas as pd
import os
import pymysql
import config as c
import random

# neo4j driver connection
driver = GraphDatabase.driver(c.URI, auth=(c.USUARIO_NEO, c.PASSWORD_NEO))

# SQL connection
mysql_connection = pymysql.connect(
    host="localhost",
    user=c.USUARIO_SQL,
    password=c.PASSWORD_SQL,
    database=c.NOMBRE_BASE_SQL,  # User
)

# Auxiliary functions


def create_nodes(func):
    """
    This decorator is executed on each run of the exercise functions. It ensures
    that the neo4J database is empty before inserting the data and shows that the data
    is loaded or executes an additional query depending on what is necessary

    Args:
        func (func): the corresponding exercise function
    """

    def wrapper(*args, **kwargs):
        with driver.session() as session:
            # We delete everything in the database
            query = "MATCH (n) DETACH DELETE n"
            session.run(query)

            # Now it's empty
            query = """
                        MATCH (n) RETURN n
                    """
            result = session.run(query)
            print("Checking if it's empty")
            res = result.data()
            print(res)
            # We get the queries to create the database
            with mysql_connection:
                query, extra_query = func(*args, **kwargs)
            # We execute them and show all the data of the reviewers
            session.run(query)
            query = """
                              MATCH (n) RETURN n.reviewerID
                           """
            result = session.run(query)
            print("Checking if the data is present")
            res = result.data()
            print(res)

            # If there is any query left to execute
            if extra_query:
                exec_queries(extra_query)
            else:
                print("Data inserted, you can check in neo4J")

    return wrapper


def exec_queries(query):
    """
    Execution of neo4J queries. The results of the queries are displayed.

    Args:
        query (str): the neo4J query to execute
    """
    with driver.session() as session:

        result = session.run(query)
        res = result.data()

        dataframe = pd.DataFrame([dict(record) for record in res])
        print(dataframe.to_string())


# SQL queries
"""
In all these queries, the data is transformed from row grouping (returned by SQL)
to column grouping to facilitate the process of working with them when inserting into SQL.
Therefore, list(zip(*data)) will be executed, which achieves precisely this.
"""


def get_users(n_users=c.N_USUARIOS_NEO):
    """
    Query corresponding to exercise 1. Returns the reviewerId and the asin
    of the reviewed product for the first N_USERS_NEO ordered by number of reviews

    Returns:
        list, list: the first is a list with reviewer-product tuples specifying
                    which products each reviewer has reviewed. The second is a
                    list of users without repetitions
    """

    cursor = mysql_connection.cursor()
    sql = """SELECT r.reviewerID, r.asin
                FROM review r
                INNER JOIN (SELECT reviewerID
                                    FROM review
                                    GROUP BY reviewerID
                                    ORDER BY COUNT(*) DESC
                                    LIMIT %s) as t ON r.reviewerID = t.reviewerID;"""
    cursor.execute(sql, n_users)
    data = cursor.fetchall()
    users, products = list(zip(*data))
    user_prod = {}
    for user, asin in zip(users, products):
        user_prod.setdefault(user, []).append(asin)
    users = list(set(users))
    return user_prod, users


def get_all_asins():
    """
    Query corresponding to exercise 2. Gets all the asins
    in the database and returns them

    Returns:
        list: the list with all the asins
    """

    cursor = mysql_connection.cursor()
    sql = """SELECT DISTINCT asin
            FROM product
            """
    cursor.execute(sql)
    data = cursor.fetchall()
    return list(data)


def get_articles(articles):
    """
    Query associated with exercise 2. Gets all the requested information
    from the reviews of the given articles

    Returns:
        list, list: the first is a list containing all the data separated
                    into 4 lists (one per column), while the second gives all
                    the distinct users
    """

    cursor = mysql_connection.cursor()
    sql = """SELECT asin, reviewerID, reviewTime, overall
                FROM review
                WHERE asin IN %s"""
    cursor.execute(sql, [articles])
    data = cursor.fetchall()
    data = list(zip(*data))
    users = list(set(data[1]))

    return data, users


def get_users_and_types(n_users=c.N_USUARIOS_NEO_EJ3):
    """
    Query associated with exercise 3. Returns the reviewer id, the type,
    and how many reviews they have done for that type for those reviewers among
    the first N_USERS_NEO_EJ3 ordered alphabetically by reviewer name

    Returns:
        list, list, list: returns three lists. The first and second are the
                          unique users and types respectively, while the third
                          contains the data of how many reviews each user has
                          done for each type
    """
    cursor = mysql_connection.cursor()
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
    cursor.execute(sql, n_users)
    data = cursor.fetchall()
    users, types, _ = list(zip(*data))
    users = list(set(users))
    types = list(set(types))
    return users, types, data


def popular_articles():
    """
    Query associated with exercise 4. Returns the asin of the 5 most popular
    products with less than 40 reviews as well as the reviewers who have
    reviewed them

    Returns:
        list, list, list: the unique users and products respectively followed by the
                          data of which reviewer has reviewed which products
    """
    cursor = mysql_connection.cursor()
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
    data = cursor.fetchall()
    users, products = list(zip(*data))
    users = list(set(users))
    products = list(set(products))
    return users, products, data


# EXERCISE 1


@create_nodes
def exercise1():
    """
    Function associated with the first exercise. Calculates the top users by number of reviews,
    calculates their Jaccard similarities, and creates the query to add all this data to neo4J.
    Also executes a query that shows the user with the most neighbors

    Returns:
        str, str: the query to create the database and the query to show the one with
                  the most neighbors
    """
    # If you want to modify the number of users, change the value of the variable N_USERS_NEO
    user_prod, users = get_users()
    sim_matrix = calculate_similarities(user_prod, users)
    query = similarities_neo4J(users, sim_matrix)
    extra_query = """MATCH (r:REVIEWER) - [:SIM] -> (:REVIEWER)
    WITH r, COUNT{(r:REVIEWER) - [:SIM] -> (:REVIEWER)} AS c_sim
    WITH max(c_sim) as max
    MATCH (r:REVIEWER) - [:SIM] -> (:REVIEWER)
    WHERE COUNT{(r:REVIEWER) - [:SIM] -> (:REVIEWER)} = max
    RETURN DISTINCT r, COUNT{(:REVIEWER) - [:SIM] -> (e:REVIEWER)}"""
    return query, extra_query


def calculate_similarities(user_prod, users):
    """
    Calculates the Jaccard similarities of a given set of users.
    This calculation relies on a file. If it exists, it loads the data; if not,
    it creates the file and saves the data there

    Args:
        user_prod (list): information about which user has reviewed which product
        users (list): the list of unique users

    Returns:
        list: a matrix with all the similarities
    """
    if not os.path.exists("similarities.txt"):
        sim_matrix = []
        for i in range(len(users)):
            sim_us = []
            for j in range(len(users)):
                prods_us1 = user_prod[users[i]]
                prods_us2 = user_prod[users[j]]
                similarity = (len(set(prods_us1) & set(prods_us2))) / len(
                    set(prods_us1) | set(prods_us2)
                )

                sim_us.append(str(similarity))
            sim_matrix.append(sim_us)
        with open("similarities.txt", "w") as f:
            f.write("\n".join([",".join(i) for i in sim_matrix]))
    else:
        with open("similarities.txt", "r") as f:
            sim_matrix = [l.split(",") for l in f.readlines()]

    return sim_matrix


def similarities_neo4J(users, sim_matrix):
    """
    Creates a neo4J query that, given the users and their similarity matrix, creates a
    graph in neo4J with all this information

    Args:
        users (list): list of unique users
        sim_matrix (list): matrix with the similarities of one user with another

    Returns:
        str: the query to create the graph
    """
    q_users = ",\n".join(
        [f"(reviewer_{n}:REVIEWER{{reviewerID:'{n}'}})" for n in users]
    )
    q_similarities = ""
    for i in range(len(users)):
        for j in range(len(users)):
            if i != j and sim_matrix[i][j] != "0.0":
                # We do not store information in the graph of the similarity of a user with themselves (always 1) or of users whose similarity is 0
                q_similarities += f"(reviewer_{users[i]}) - [:SIM{{similarity:{float(sim_matrix[i][j])}}}] -> (reviewer_{users[j]}),\n"

    q_combined = ",\n\n".join([q_users, q_similarities])
    query = f"CREATE\n{q_combined}".rstrip("\n,")
    return query


# EXERCISE 2


@create_nodes
def exercise2():
    """
    The function creates a graph with the information of the users who have reviewed n products
    randomly chosen from the category specified by the variable CAT_EXERCISE_2 in the
    configuration file, as well as the information of the products themselves

    Returns:
        str, None: The query to create the graph along with a None to signify that there is no
                   additional query
    """
    n = None
    while not n:
        try:
            n = int(input("Enter the number of products: "))
        except ValueError:
            print("Invalid value")
    # We get all the asins and select n randomly
    asins = get_all_asins()
    chosen_asins = random.sample(asins, n)
    data, users = get_articles(chosen_asins)
    query = random_products_neo4J(chosen_asins, users, data)
    return query, None


def random_products_neo4J(products, users, data):
    """
    We create the query to create the graph for exercise 2, a graph that
    stores information about the users and the products they review

    Args:
        products (list): list of unique products
        users (list): list of unique users
        data (list): list of lists with review information

    Returns:
        str: the query to create the graph
    """
    q_users = ",\n".join(
        [f"(reviewer_{n}:REVIEWER{{reviewerID:'{n}'}})" for n in users]
    )
    q_products = ",\n".join(
        [f"(product_{n[0]}:PRODUCT{{asin:'{n[0]}'}})" for n in products]
    )
    q_relationships = ""
    for prod, user, t, rating in zip(*data):
        q_relationships += f"(reviewer_{user}) - [:REVIEWS{{time:'{t}', overall:{rating}}}] -> (product_{prod}),\n"
    q_relationships = q_relationships.rstrip(",\n")
    q_combined = ",\n\n".join([q_users, q_products, q_relationships])
    query = f"CREATE\n{q_combined}"
    return query


# EXERCISE 3


@create_nodes
def exercise3():
    """
    Creates a graph with information about the users among the first 400 who
    have reviewed more than one type of product, representing which users have
    reviewed which types

    Returns:
        str, None: the query and a None to indicate that there is no extra query
    """
    users, types, data = get_users_and_types()
    query = reviews_by_type(users, types, data)
    return query, None


def reviews_by_type(users, types, data):
    """
    Creates the query to make the graph for exercise 3 in which
    the users who have reviewed more than one type and which types they have
    reviewed are stored

    Args:
        users (list): list of users who have reviewed more than one type without repetitions
        types (list): the types of reviewed items
        data (list): data about which users have reviewed which types

    Returns:
        str: the query to create the graph
    """
    q_users = ",\n".join(
        [f"(reviewer_{n}:REVIEWER{{reviewerID:'{n}'}})" for n in users]
    )
    q_products = ",\n".join([f"(type_{n}:TYPE{{asin:'{n}'}})" for n in types])
    q_relationships = ""

    for user, type_, n in data:
        q_relationships += (
            f"(reviewer_{user}) - [:REVIEWS{{n_products:{n}}}] -> (type_{type_}),\n"
        )
    q_relationships = q_relationships.rstrip(",\n")
    q_combined = ",\n\n".join([q_users, q_products, q_relationships])
    query = f"CREATE\n{q_combined}"
    return query


# EXERCISE 4


@create_nodes
def exercise4():
    """
    Creates a graph with information about the users who have reviewed the 5
    most popular items with less than 40 reviews and represents which users have
    reviewed which products and also indicates how many common products of the 5
    two users have

    Returns:
        str, str: the query and a None to indicate that there is no extra query
    """
    users, products, data = popular_articles()
    query = articles_and_users(users, products, data)
    # For information about the common products that two users have
    query_neo_links = """MATCH (u1:REVIEWER) - [:REVIEWS] -> (p:PRODUCT) <- [:REVIEWS] - (u2:REVIEWER)
                              WITH u1, u2, COUNT(p) AS num_common
                              WHERE u1 <> u2 
                              MERGE (u1) - [:LINK{ n_common_prods:num_common}] -> (u2)
        """
    return query, query_neo_links


def articles_and_users(users, products, data):
    """
    Creates the query to make the graph for exercise 4 in which
    the users who have reviewed any of the 5 most popular products with less than 40 reviews
    are stored

    Args:
        users (list): list of unique users
        products (list): the reviewed products
        data (list): data about which users have reviewed which products

    Returns:
        str: the query to create the graph
    """
    q_users = ",\n".join(
        [f"(reviewer_{n}:REVIEWER{{reviewerID:'{n}'}})" for n in users]
    )
    q_products = ",\n".join([f"(product_{n}:PRODUCT{{asin:'{n}'}})" for n in products])
    q_relationships = ""

    for user, type_ in data:
        q_relationships += f"(reviewer_{user}) - [:REVIEWS] -> (product_{type_}),\n"
    q_relationships = q_relationships.rstrip(",\n")
    q_combined = ",\n\n".join([q_users, q_products, q_relationships])
    query = f"CREATE\n{q_combined}"
    return query


if __name__ == "__main__":
    if c.EJERCICIO == 1:
        exercise1()
    elif c.EJERCICIO == 2:
        exercise2()
    elif c.EJERCICIO == 3:
        exercise3()
    else:
        exercise4()
