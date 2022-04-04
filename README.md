# AutoMigrationToolSQL

SQL is a well-established and well-documented language, it is safe, versatile and widely used to run complex
queries in use cases which require rigid data storage and in cases where consistency is critical. On the other
hand, NoSQL Databases, even though being adopted relatively quickly, have some major disadvantages when
it comes to Transaction control. In many NoSQL databases, the representation of ACID Properties is vague at
best. Scalability is also a major difference between NoSQL Database Systems and SQL Database Systems.
SQL Database systems are vertically scalable as compared to horizontally scalable NoSQL Systems.
Thus, making the right choice of database systems is very important while developing the project. Making
the wrong choice might result in having to compromise on some very major aspects of the project. Typically
migrating from a database system to another will involve manually going through all the collections and
determining the structure of the data and then making the Database schema accordingly.It will also involve
going through the collections document by document basis and running insert queries, which can be a very
time consuming process.
Our project mitigates the lost time involved in fixing the mistake of choosing the wrong database system
by handling the creation of the tables in the database and addition of rows to the database.

## Installation of the Tool

Just clone the repository from git and You're done!

## Running the tool

The tool expects running instances of MongoDB and MySQL instances on the client machine. The tool also requires a working installation of python3 (>3.6 prefered) on the client's machine.
Running the tool is as simple as running
```
python3 index.py
```

Once the execution of the application completes, you can go the MySQL database and see the added tables and data
