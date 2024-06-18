# Amazon review database 📊

This repository contains the final project for the Database course, developed by Sergio Jiménez Romero and Alberto Velasco Rodríguez.

<!-- TABLE OF CONTENTS -->
- [Amazon review database 📊](#amazon-review-database-)
  - [About the project ℹ️](#about-the-project-ℹ️)
  - [Libraries and dependencies 📚](#libraries-and-dependencies-)
    - [Database Design and Data Loading](#database-design-and-data-loading)
    - [Data Loading](#data-loading)
    - [Python Application for Data Access and Visualization 🧰](#python-application-for-data-access-and-visualization-)
    - [Python Application with Neo4j Integration 🎨](#python-application-with-neo4j-integration-)
      - [How it works ⚙️](#how-it-works-️)
      - [How to use ⏩](#how-to-use-)
  - [Developers 🔧](#developers-)

## About the project ℹ️

This project involves the design, implementation, and management of a database, along with developing Python applications to interact with the database and visualize data. It includes integration with Neo4j for graph-based data analysis and visualization.

For this project we have created several databse and visualization systems around the amazon review dataset, found [here](https://cseweb.ucsd.edu/~jmcauley/datasets/amazon/links.html)

For the project to work, you will have to first load the files into SQL and MongoDB, as the dashboard and neo4J rely on queries to SQL and MongoDB. 

## Libraries and dependencies 📚

> We used python 3.11.5 for the development of this project, so check the compatibility of the required libraries with your python version.

The first step is to install the required dependencies. The `requirements.txt` file contains all the necessary libraries to run the code without errors.

```bash
pip install -r requirements.txt
```
### Database Design and Data Loading
The project includes the design of a relational schema to structure the data effectively. The schema is designed to facilitate efficient data retrieval and manipulation.
### Data Loading

Data is loaded into the database using SQL scripts. These scripts ensure that the data is properly formatted and inserted into the respective tables in the database.
### Python Application for Data Access and Visualization 🧰

A Python application is developed to access and visualize the data stored in the database. The application includes various features to query the database and display the results in a user-friendly manner.

Using plotly we are able to visualize the data while imposing different filters. This allows a greater control and simplifies the data analysis tasks

<div align="center">
    <img src="/images/dashbard.png" alt="dashboard" width="60%" height="50%">
</div>

### Python Application with Neo4j Integration 🎨
#### How it works ⚙️

The integration with Neo4j allows for advanced graph-based analysis of the data. The application can identify similarities between users and visualize these relationships using Neo4j.
#### How to use ⏩
  * Ensure Neo4j is installed and running on your system.
  * Configure the connection settings in the application to connect to the Neo4j instance.
  * Run the Python script to perform the analysis and visualize the data.

Here is an example use case of our neo4j implementation to calculate similarities between reviewers
<div align="center">
    <img src="/images/neo4j.png" alt="neo4j" width="50%" height="50%">
</div>

## Developers 🔧

We would like to thank you for taking the time to explore this project. If you have any suggestions or questions, please do not hesitate to contact us.

   * [Sergio Jimenez](https://github.com/sjrom47)
   * [Alberto Velasco](https://github.com/Alberto-cd)
