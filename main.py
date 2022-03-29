import pprint

from pymongo import mongo_client, MongoClient
import mysql.connector

cnx = mysql.connector.connect(user='root', password='',
                                  host='127.0.0.1',
                                  database = 'TEST_DB')
def connectMySql():

    cursor = cnx.cursor()
    cursor.execute('Create Database TEST_DB2')
    cnx.commit()
    cnx.close()

def createTableInMySQL(tableName, columns):
    cursor = cnx.cursor()
    command = "create table if not exists " + tableName + "("
    for col in columns:
         if(col in "desc" or col in "type"):
             col = col + "_"
         command = command[:len(command)] + col + " varchar(500),"

    command = command[:len(command) - 1] + ") CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci ;"
    print(command)

    cursor.execute(command)
    cnx.commit()

def alterTableInMySQL(tableName, columns):
    cursor = cnx.cursor()
    for col in columns:
        command = "ALTER TABLE " + tableName + " ADD COLUMN " + col + " varchar(500);"
        cursor.execute(command)
        cnx.commit()

def insertRecordIntoTable(tableName, rowData, columns):
    cursor = cnx.cursor()
    command = "INSERT INTO " + tableName + "( "
    for col in columns:
        if col in "desc" or col in "type":
            col = col + "_"
        command = command[:len(command)] + col + ","
    command = command[ :len(command) - 1] + " ) VALUES("

    for data in rowData:
        print(data)
        data=str(data).replace('"','\\"')
        command = command[:len(command)]  +'"'+ str(data) + '",'
    command = command[: len(command) - 1] + ")"
    print(command)
    cursor.execute(command)
    cnx.commit()
    #print(command)

def readMongo():
    client = MongoClient()
    dbs = client.list_database_names()
    print(dbs)
    for j in range(0, len(dbs)):
        db = client[dbs[j]]
        colls = db.list_collection_names()
        for k in range(0, len(colls)):
            tableName = colls[k]
            print(tableName)
            if(tableName == 'recipe'):
                i = 0
                oldColumnNames = []
                newColumns = []
                for doc in db[colls[k]].find():
                    columnNames = list(doc.keys())

                    if(i == 0):
                        createTableInMySQL(tableName, columnNames)
                        print('first itr')
                        oldColumnNames = columnNames
                    else:
                        newColumns = []
                        for col in columnNames:
                            if col in oldColumnNames:
                                continue
                            else:
                                newColumns.append(col)
                                oldColumnNames.append(col)
                        alterTableInMySQL(tableName, newColumns)
                    i = i + 1
                    rowData = list(doc.values())
                    insertRecordIntoTable(tableName, rowData, columnNames)

def iterateMongo():
    client = MongoClient()
    dbs = client.list_database_names()
    print(dbs)
    for j in range(0,len(dbs)):
        db = client[dbs[j]]

        colls = db.list_collection_names()
        for k in range(0, len(colls)):
            for doc in  db[colls[k]].find():
                #pprint.pprint(doc)
                keys = doc.keys()
                val = doc.values()
                print(keys)
                print(val)
            '''keys = db[colls[k]].find_one().keys()
            values = db[colls[k]].find_one().values()
            list_keys = list(keys)
            list_values = list(values)
            print("=============DB = %s, collection %s===========", db.name, colls[k])
            print(list_keys)

            for i in range(0,len(list_keys)):
                print(list_keys[i] , " : " , list_values[i])'''


if __name__ == '__main__':
    iterateMongo()
    #readMongo()



