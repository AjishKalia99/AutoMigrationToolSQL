import collections
import mysql.connector

class Writer:
    def __init__(self,database_name, db_username, db_password, db_host):
        self.connection=mysql.connector.connect(user=db_username, password=db_password,
                                  host=db_host,
                                  database = database_name)
        
    def checkTableExists(self, tablename):
        cursor = self.connection.cursor()
        cursor.execute("""
            SELECT COUNT(*)
            FROM information_schema.tables
            WHERE table_name = '{0}'
            """.format(tablename.replace('\'', '\'\'')))
        if cursor.fetchone()[0] == 1:
            cursor.close()
            return True
        cursor.close()
        return False

    def create_table(self,tablename,columns):
        cursor = self.connection.cursor()
        command = "create table if not exists " + tablename + "( "+ tablename +"_id int primary key auto_increment ,"
        for col in columns:
           command = command + tablename+"_"+col + " varchar(500),"
        command = command[:len(command)-1] + ") CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci ;"
        print("Table "+tablename+" created succesfully")
        cursor.execute(command)
        cursor.close()
        self.connection.commit()

    def get_columns(self,collection):
        cursor=self.connection.cursor()
        cursor.execute("SHOW columns FROM "+collection)
        return [column[0] for column in cursor.fetchall()]

    def alterTableInMySQL(self,tableName, columns):
        cursor = self.connection.cursor()
        for col in columns:
            command = "ALTER TABLE " + tableName + " ADD COLUMN " + tableName+"_"+col + " varchar(500);"
            cursor.execute(command)
            print("Succesfully added Column "+tableName+"_"+col)
            self.connection.commit()
        print("Table "+tableName+" Altered succesfully")

    def create_fk_constraint(self,constraint_table,constraint_from,constraint_to,constraint_collection):
        cursor = self.connection.cursor()
        print("Adding Foreign key constraints to table: "+constraint_table)
        command="ALTER TABLE "+constraint_table+" MODIFY "+constraint_from+"_id INTEGER;"
        cursor.execute(command)
        command="ALTER TABLE "+constraint_table+" ADD FOREIGN KEY ("+constraint_from+"_id) REFERENCES "+constraint_to+"("+constraint_collection+");"
        
        print("^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^")
        print(command)
        print(constraint_table)
        print(constraint_from)
        print(constraint_to)
        print(constraint_collection)
        cursor.execute(command)
        self.connection.commit()
        print("Added Foreign key constraints")

    def insertRecordIntoTable(self,tableName, rowData, columns):
        cursor = self.connection.cursor()
        command = "INSERT INTO " + tableName + "( "
        for col in columns:
            if col in "desc" or col in "type":
                col = col + "_"
            command = command[:len(command)] + col + ","
        command = command[ :len(command) - 1] + " ) VALUES("
        for data in rowData:
            data=str(data).replace('"','\\"')
            command = command[:len(command)]  +'"'+ str(data) + '",'
        command = command[: len(command) - 1] + ")"
        print("Added Data to Table "+tableName)
        cursor.execute(command)
        self.connection.commit()
        last_row=cursor.lastrowid
        return last_row