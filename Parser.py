from pickle import FALSE
from Writer import Writer

class Parser:
    def __init__(self,output_database, db_username, db_password, db_host):
        self.Writer=Writer(output_database, db_username, db_password, db_host)
        return None

    def parse_nesting(self,parent_collection,parent_id,collection_name,collection_data):
        if(any(isinstance(i, list) for i in collection_data)):
            for document in collection_data:
                if(type(collection_data[document])==list):
                    self.parse_nesting(collection_name,parent_id,document,collection_data[document])
        elif(any(isinstance(i, dict) for i in collection_data)):
            print("Key "+collection_name+" has nested Json. Creating a mapping Table and a Data Table")
            #Parse Any Changes in Structure
            if(self.Writer.checkTableExists(collection_name+"_"+parent_collection+"_mapping")==False):
                print("Table "+collection_name+"_"+parent_collection+"_mapping"+" does not exist. Will be created.")
                col_names=[]
                col_names.append(parent_collection+"_id")
                col_names.append(collection_name)
                self.Writer.create_table(collection_name+"_"+parent_collection+"_mapping",col_names)

            for document in collection_data:
                if(self.Writer.checkTableExists(collection_name) == False):
                    print("Table "+collection_name+" does not exist. Will be created according to first document!")
                    col_names=[]
                    for key in document:
                        if(type(document[key])!=list):
                            col_names.append(key)
                    col_names.append(parent_collection+"_id")  #add column to save foreign key 
                    self.Writer.create_table(collection_name,col_names)  
                    self.Writer.create_fk_constraint(collection_name,collection_name+"_"+parent_collection,parent_collection,parent_collection+"_id")
                    #self.Writer.create_fk_constraint(collection_name,collection_name+"_"+parent_collection,collection_name+"_"+parent_collection+"_mapping",collection_name+"_"+parent_collection+"_mapping_ingredients")
                    #self.Writer.create_fk_constraint(collection_name,collection_name+"_"+parent_collection,collection_name+"_"+parent_collection+"_mapping",collection_name+"_"+parent_collection+"_mapping_"+parent_collection+"_id")

                else:
                    columns=self.Writer.get_columns(collection_name)
                    col_names=[]
                    for key in document:
                        if(type(document[key])!=list and collection_name+"_"+key not in columns):
                            col_names.append(key)
                    if(len(col_names)>0):
                        print("Altering Table "+collection_name)
                        self.Writer.alterTableInMySQL(collection_name,col_names)
                    else:
                        print("No new columns found. No need to Alter the table "+collection_name)
                columns=[]
                values=[]
                for key in document:
                    if(type(document[key])!=list):
                        columns.append(collection_name+"_"+key)
                        values.append(document[key])
                row_id=self.Writer.insertRecordIntoTable(collection_name,values,columns)
                for value in collection_data:
                    self.Writer.insertRecordIntoTable(collection_name+"_"+parent_collection+"_mapping",[parent_id,row_id],
                    [collection_name+"_"+parent_collection+"_mapping"+"_"+parent_collection+"_id",collection_name+"_"+parent_collection+"_mapping"+"_"+collection_name])                
        else:
            print("Key "+collection_name+" has no nesting. Creating a mapping Table")
            if(self.Writer.checkTableExists(collection_name) == False):
                print("Table "+collection_name+" does not exist. Will be created.")
                col_names=[]
                col_names.append(parent_collection+"_id")
                col_names.append(collection_name)
                self.Writer.create_table(collection_name,col_names)
                self.Writer.create_fk_constraint(collection_name,collection_name+"_"+parent_collection,parent_collection,parent_collection+"_id")
            for value in collection_data:
                self.Writer.insertRecordIntoTable(collection_name,[parent_id,value],
                [collection_name+"_"+parent_collection+"_id",collection_name+"_"+collection_name])  
        return True  

    def parse(self,collection_name,document):
        #Parse Any Changes in Structure
        if(self.Writer.checkTableExists(collection_name) == False):
            print("Table "+collection_name+" does not exist. Will be created according to first document!")
            col_names=[]
            for key in document:
                if(type(document[key])!=list):
                    col_names.append(key)
            self.Writer.create_table(collection_name,col_names)
        else:
            columns=self.Writer.get_columns(collection_name)
            col_names=[]
            for key in document:
                if(type(document[key])!=list and collection_name+"_"+key not in columns):
                    col_names.append(key)
            if(len(col_names)>0):
                print("Altering Table "+collection_name)
                self.Writer.alterTableInMySQL(collection_name,col_names)
            else:
                print("No new columns found. No need to Alter the table "+collection_name)
        #Parse non List Values and Add to the Table
        columns=[]
        values=[]
        for key in document:
            if(type(document[key])!=list):
                columns.append(collection_name+"_"+key)
                values.append(document[key])
        row_id=self.Writer.insertRecordIntoTable(collection_name,values,columns)
        #Parse List Based Values
        for key in document:
            if(type(document[key])==list):
                self.parse_nesting(collection_name,row_id,key,document[key])