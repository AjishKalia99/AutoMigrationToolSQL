import pprint
from Parser import Parser
from pymongo import mongo_client, MongoClient


class Reader:
    def __init__(self, input_database, output_database, db_username, db_password, db_host):
        self.client = MongoClient()
        self.parser= Parser(output_database, db_username, db_password, db_host)
        self.database_name=input_database
        return None

    def iterate(self):
        dbs = self.client.list_database_names()
        database_name=self.database_name
        if(database_name not in dbs):
            print("Database "+database_name+" Not Found")
            return False
        for database in dbs:
            if(database==database_name):
                print("Database "+database_name+" Found")
                self.db=self.client[database]
                print("Iterating through Collections...")
                colls = self.db.list_collection_names()
                print(str(len(colls))+" Collection(s) found")
                for collection in colls:
                    print("Iterating through collection "+collection)
                    docs=self.db[collection].estimated_document_count()
                    if(docs==0):
                        print("Collection "+collection+" has no Documents Skipping")
                    count=1
                    for document in self.db[collection].find():
                        print("Iterating Through Document "+str(count)+" of "+str(docs))
                        count+=1
                        print("Starting Parse...")
                        self.parser.parse(collection,document)
