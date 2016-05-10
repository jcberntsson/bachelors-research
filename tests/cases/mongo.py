from pymongo import MongoClient
from datetime import datetime
from cases import Base

class Mongo(Base):
    # connect to authenticated mongo database
    client = MongoClient("mongodb://46.101.103.26:27017")
    db = client.skimdatabase

    ####################################
    ####	DATA INITIALIZATION		####
    ####################################

    def initRaceOne(self):
        # Users and Organizers
        users = []
        organizers = []
        for x in range(50):
            users.append(self.db.users.insert_one({
                "username": "user_"+str(x),
                "email": "user_"+str(x)+"@gmail.com",
                "password": "xpassx"
            }).inserted_id)
            organizers.append(self.db.organizers.insert_one({
                "username": "organizer_"+str(x),
                "fullname": "Tester",
                "password": "xpassx",
                "email": "mail_"+str(x)+"@gmail.se"
            }).inserted_id)
        # Events & Races        
        for x in range(10):
            event = self.db.events.insert_one({
                "name": "event_"+str(x),
                "logoURL": "gooogle.se/img.png",
                "organizer": organizers[x*5]
            }).inserted_id
            for y in range(5):
            race = self.db.races.insert_one({
                "name": "race_"+str(random.randint(1,500)),
                "description": "A nice race to participate in.",
                "date": "2016-06-13",
                "maxDuration": 3,
                "preview": "linktoimage.png",
                "location": "Gothenburg, Sweden",
                "logoURL":"google.se/img.png",
                "event":event,
                "prev": {"lat": 33,"lng": 44,"alt": 100}
            }).inserted_id
            # Coordinates

            
        # cursor = self.db.events.find()
        # for document in cursor:
            print (event)

        

    # def initSkim(self):
    #     # User array to be able to print out in console
    #     users = []
    #     collaboratorsData = []
    #     # Users is a collection of user documents in skimdatabase
    #     for x in range(50):  
    #         users.append(self.db.users.insert_one({
    #             "username": "user_"+str(x),
    #             "email":	"user_"+str(x)+"@gmail.com",
    #             "password": "xpassx"
    #         }).inserted_id)
    #     # Projects and collaborator
    #     for x in range(8):
    #         # collaboratorsData = [ self.db.users.find({}, {})[x*2], self.db.users.find({}, {})[x*3], self.db.users.find({}, {})[x*4] ]
    #         for y in range(10):
    #             collaboratorsData.append(self.db.users.find({}, {})[x*2+y])
    #         project = self.db.projects.insert_one({
    #             "name": "project_"+str(x),
    #             "collaborator": collaboratorsData
    #         })
    #         collaboratorsData = []
    #         for y in range(4):
    #             # Images
    #             nbr = x + 5 + y
    #             image = {
    #                 "name":"image_" + str(nbr),
    #                 "originalName": "original_name",
    #                 "extension": "jpg",
    #                 "encoding": "PNG/SFF",
    #                 "size": 1024,
    #                 "height": 1080,
    #                 "width": 720,
    #                 "verticalDPI":40,
    #                 "horizontalDPI":50,
    #                 "bitDepth":15,
    #                 "createdAt":"2016-03-03",
    #                 "accepted":False
    #             }
    #             # ROWS in project
    #             rowId = self.db.row.insert({
    #                 "name":"row_" + str(nbr),
    #                 "col":[],
    #                 "img":[]
    #             })                 
    #             self.db.projects.update({"name": "project_"+str(x)}, {"$set": {"row" :rowId, "image" :image}})
    #             # Columns in row
    #             for z in range(10):
    #                 colId = self.db.column.insert({
    #                     "header":"header_"+str(z),
    #                     "value":str(z)
    #                 })
    #                 self.db.row.update({"_id":rowId}, {"$push": {"col": colId}})
                    
    #             # Row images
    #             nbr = x + 5 + y
    #             img = {
    #                 "name":"sku_image_" + str(nbr),
    #                 "originalName": "original_name",
    #                 "extension": "jpg",
    #                 "encoding": "PNG/SFF",
    #                 "size": 1024,
    #                 "height": 1080,
    #                 "width": 720,
    #                 "verticalDPI":40,
    #                 "horizontalDPI":50,
    #                 "bitDepth":15,
    #                 "createdAt":"2016-03-03",
    #                 "accepted":False,
    #                 "comments":[]
    #             }
    #             self.db.row.update({"_id":rowId}, {"$set": {"img":img}})
    #             for z in range(2):
    #                 # Comments
    #                 user = users[x*2+z]
    #                 comment = {
    #                     "text":"Haha, cool image",
    #                     "createdAt":"2016-04-04",
    #                     "user":user
    #                 }
    #                 self.db.row.update({"_id":rowId}, {"$push": {"img.comments": comment}})
                       
    #     cursor = self.db.projects.find()
    #     for document in cursor:
    #         print (document)
            
                     


    def clearData(self):
        self.client.drop_database("skimdatabase")
        		
	# Run project on: python main.py         
    
    