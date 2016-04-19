from pymongo import MongoClient
from datetime import datetime

# connect to authenticated mongo database 
client = MongoClient("mongodb://46.101.103.26:27017")

def initData(model):
	# clearData()
	
	if (model == "raceone"):
		initRaceOneData()
	elif (model == "skim"):
		initSkimData()
	elif (model == "reddit"): 
		initRedditData()
	else:
		print("No data for " + model)
        

def initSkimData():
	db = client.skimdatabase

	# users is a collection of user documents 
	for x in range(50):
		user = db.users.insert_one(
			{
				"username": "user_"+str(x), 
				"email":	"user_"+str(x)+"@gmail.com",
			}
		)
	for x in range(8):
		project = db.projects.insert_one(
			{
				"name": "project_"+str(x)
			}
		)
	# print out the docuemnts in the users collection
	cursor = db.projects.find()
	for document in cursor:
		print(document)
		
if __name__ == '__main__':
    initData("skim")
	
	
# Run project on: python mongo.py 