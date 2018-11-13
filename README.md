# postgres_visualizer
Tool to allow visualization of PostGresQL queries.
***
This repository contains the algorithm for postgreSQL Visualizer which is integrated onto postgreSQL Visualizer's backend at ```https://github.com/ekoeditaa/pgviz.git```.
The algorithm allows the matching of tree nodes in a postgreSQL statement's query execution plan (QEP) to an index range in the corresponding SQL statement.
The QEP JSON tree is then modified with these index range values and passed onto the postgreSQL Visualizer frontend at ```https://github.com/ekoeditaa/pgviz-frontend```.