from debugpy import connect
from Reader import Reader

in_dbname = input("Enter name of input database: ")
out_dbname = input("Enter name of output database: ")
db_username = input("Enter username: ")
db_password = input("Enter password: ")
db_host = input("Enter host's IP: ")

mongo_client=Reader(in_dbname, out_dbname, db_username, db_password, db_host)
mongo_client.iterate()