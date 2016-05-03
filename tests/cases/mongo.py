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
                cursor = self.db.projects.find()
                # SKUS
                              
    def clearData(self):
        self.client.drop_database("skimdatabase")		
	# Run project on: python main.py         
    
    {u'collaborator': [{u'_id': ObjectId('5728b1df70f0261f09646a02')}, {u'_id': ObjectId('5728b1e070f0261f09646a09')}, {u'_id': ObjectId('5728b1e070f0261f09646a10')}], u'image': {u'horizontalDPI': 50, u'bitDepth': 15, u'name': u'image_15', u'extension': u'jpg', u'encoding': u'PNG/SFF', u'verticalDPI': 40, u'height': 1080, u'width': 720, u'originalName': u'original_name', u'accepted': False, u'createdAt': u'2016-03-03', u'size': 1024}, u'_id': ObjectId('5728b1e370f0261f09646a2d'), u'name': u'project_7'}