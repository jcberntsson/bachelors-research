from pymongo import MongoClient
from datetime import datetime
from cases import Base

class Mongo(Base):
    # connect to authenticated mongo database
    client = MongoClient("mongodb://46.101.103.26:27017")
    db = client.skimdatabase

    def initSkim(self):
        # User array to be able to print out in console
        users = []
        # Users is a collection of user documents in skimdatabase
        for x in range(50):
            users.append(self.db.users.insert_one({
                "username": "user_"+str(x),
                "email":	"user_"+str(x)+"@gmail.com",
                "password": "xpassx"
            }))
        # Projects and collaborator
        projectsArray = []
        for x in range(8):
            collaboratorsData = [ self.db.users.find({}, {})[x*2], self.db.users.find({}, {})[x*3], self.db.users.find({}, {})[x*4] ]
            project = self.db.projects.insert_one({
                "name": "project_"+str(x),
                "collaborator": collaboratorsData
            })
            for y in range(4):
                # Images
                nbr = x + 5 + y
                image = {
                    "name":"image_" + str(nbr),
                    "originalName": "original_name",
                    "extension": "jpg",
                    "encoding": "PNG/SFF",
                    "size": 1024,
                    "height": 1080,
                    "width": 720,
                    "verticalDPI":40,
                    "horizontalDPI":50,
                    "bitDepth":15,
                    "createdAt":"2016-03-03",
                    "accepted":False
                }
                self.db.projects.update({"name": "project_"+str(x)}, {"$set": {"image" :image}})
                # cursor = self.db.projects.find()
                # for document in cursor:
                #     print(document)
                # SKUS
                skus = []
                skus.append(self.db.skus.insert_one({
                    "name":"sku_" + str(nbr))
                }))
                self.db.projects.update({"name": "project_"+str(x)}, {"$set": {"sku": skus[y]}})
                
    def clearData(self):
        self.client.drop_database("skimdatabase")		
	# Run project on: python main.py         