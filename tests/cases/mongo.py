from pymongo import MongoClient
from datetime import datetime
from cases import Base

class Mongo(Base):
	# connect to authenticated mongo database 
	db = MongoClient("mongodb://46.101.103.26:27017").skimdatabase	 

	def initSkim(self):
		pass
		# users = []
		# users is a collection of user documents 
		# for x in range(50):
		# 	users[x] = db.users.insert_one(
		# 		{
		# 			"username": "user_"+str(x), 
		# 			"email":	"user_"+str(x)+"@gmail.com",
		# 		}
		# 	)
		# Projects and collaborator
		# projects = []		
		# for x in range(8):
		# 	projects[x] = db.projects.insert_one(
		# 		{
		# 			"name": "project_"+str(x),
		# 			"collaborator": [db.users.find()[x*2]] 
		# 		}
		# 	)

		# print out the documents in the users collection
		
	def clearData(self):
		cursor = self.db.projects.find()
		for document in cursor:
			print(document)	
		
	# Run project on: python mongo.py 