from pymongo import MongoClient
client = MongoClient()

client = MongoClient('mongodb://192.168.33.11:27017')

db = client.test

name = {"name": "Mike"}

names = db.names

insertid = names.insert_one(name).inserted_id

print("id: ",insertid)

