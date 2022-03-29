from Reader import Reader

mongo_client=Reader("recipes","test_db")
mongo_client.iterate()