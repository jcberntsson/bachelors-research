from pymongo import MongoClient
from datetime import datetime
from cases import Base

class Mongo(Base):
    # connect to authenticated mongo database
    # client = MongoClient("mongodb://46.101.103.26:27017")
    client = MongoClient("mongodb://10.135.7.215:27017")
    db = client.skimdatabase

    ####################################
    ####	DATA INITIALIZATION		####
    ####################################

    def initRaceOne(self):
        # Users and Organizers
        users = []
        organizers = []
        allCordinates = []
        for x in range(100):
            users.append(self.db.users.insert_one({
                "username": "user_"+str(x),
                "email": "user_"+str(x)+"@gmail.com",
                "password": "SuperHash"
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
                allCordinates = []
                for i in range(98):
                    allCordinates.append({
                        "lat": 10+i,
                        "lng": 11+i,
                        "alt": 12+i
                    })
                rands = []
                activities = []
                for z in range(10):
                    # Participants
                    rand = self.new_rand_int(rands, 0, 99)
                    rands.append(rand)
                    rand2 = self.new_rand_int(rands, 0, 99)
                    rands.append(rand2)
                    activities.append({
                        "participating":users[rand],
                        "joinedAt":"2016-08-08",
                        "following":users[rand2],
                        "coordinates":allCordinates,
                    })
                    
                raceId = self.db.races.insert_one({
                    "name": "race_"+str(x*y+x+y),
                    "description": "A nice race to participate in.",
                    "date": "2016-06-13",
                    "maxDuration": 3,
                    "preview": "linktoimage.png",
                    "location": "Gothenburg, Sweden",
                    "logoURL":"google.se/img.png",
                    "event":event,
                    "start": {"lat": 33,"lng": 44,"alt": 100},
                    "coordinates": allCordinates,
                    "end": {"lat": 40,"lng": 34,"alt": 320},
                    "activity": activities                     
                }).inserted_id
                    
    def initSkim(self):
        # User array to be able to print out in console
        users = []
        # Users is a collection of user documents in skimdatabase
        for x in range(50):  
            users.append(self.db.users.insert_one({
                "username": "user_"+str(x),
                "email":	"user_"+str(x)+"@gmail.com",
                "password": "xpassx"
            }).inserted_id)
        # Projects and collaborator
        for x in range(8):
            collaboratorsData = []
            for y in range(10):
                collaboratorsData.append(self.db.users.find({}, {})[x*2+y])
            images = []
            skulist = []
            skuimages = []
            for y in range(4):
                # Images
                nbr = x + 5 + y
                images.append(self.db.images.insert_one({
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
                    "accepted":False,
                    "comments":[]
                }).inserted_id)
                # SKUS
                skuValues = []
                for z in range(10):
                    # SKU Values
                    skuValues.append({
                        "header": "header_" + str(z),
                        "value": str(z)
                    })                
                # SKU images
                for z in range(2):
                    # Comments
                    comment = {
                        "text": "Haha, cool image bastard",
                        "createdAt": "2016-04-04",
                        "made_by": users[x * 2 + z]
                    }
                nbr = x + 5 + y
                imagesku = {
                    "name":"sku_image_" + str(nbr),
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
                    "accepted":False,
                    "comments":comment
                }
                skuimages.append(imagesku)
                skulist.append(self.db.skus.insert_one({
                    "name": "sku_" + str(nbr),
                    "sku_values": skuValues,
                    "images_sku": skuimages 
                }).inserted_id)
                
            project = self.db.projects.insert_one({
                "name": "project_"+str(x),
                "collaborator": collaboratorsData,
                "images": images,
                "skus": skulist
            })
            
            
            # cursor = self.db.projects.find()
            # for document in cursor:
            #     print (document)

    def clearData(self):
        self.client.drop_database("skimdatabase")
        
    ############################
    ####	TEST METHODS	####
    ############################
    # TODO: All inserting methods should first find the nodes that it is relating for
    
    # SKIM
    def fetchSKU(self):
        def setup(inner_self):
            inner_self.sku_id = self.get_random_id("skus")
    
        def run(inner_self):
            self.db.skus.find_one({"_id": inner_self.sku_id})
    
        def teardown(inner_self):
            pass
    
        return self.create_case("fetchSKU", setup, run, teardown) 
        
    def fetchUsers(self):
        def setup(inner_self):
            pass
            
        def run(inner_self):
            self.db.users.find()
            # this will print out the value print(list(self.db.users.find())) 
        
        def teardown(inner_self):
            pass
            
        return self.create_case("fetchUsers", setup, run, teardown)
    
    def commentOnImage(self):
        def setup(inner_self):
            inner_self.project_id = self.get_random_id("projects")
            out = self.db.projects.find_one({"_id":inner_self.project_id})
            
            inner_self.user_id = out["collaborator"][0]
            inner_self.image_id = out["images"][0]
            
        def run(inner_self):
            self.db.images.update(
                {"_id": inner_self.image_id}, 
                {"$push":{"comments":{"text":"Ooh, another new comment!", "createdAt":"2015-03-02@13:37"}}}
            )
        def teardown(inner_self):
            self.db.images.update(
                {"_id":inner_self.image_id},
                {"$pull":{"comments":{"text":"Ooh, another new comment!"}}}
            )
            # print(self.db.images.find_one({"_id": inner_self.image_id}))
        
        return self.create_case("commentOnImage", setup, run, teardown)

    
    def get_random_id(self, entity_name):
        from random import randint
        container = []
        for entity in self.db[entity_name].find():
            container.append(entity["_id"])
        random = randint(0, len(container))            
        return container[random]
	# Run project on: python main.py
    
   
   
#    test_cases = {
#     'skim': [
#         #'fetchSKU',
#         #'fetchUsers',
#         #'commentOnImage',
#         #'pairImageSKU',
#         #'addRowsToSKU',
#         #'fetchAllUserComments'
#         #'easy_get',
#         #'easy_get2'
#     ],
#     'raceone': [
#         #'follow',
#         #'unfollow',
#         'insertCoords',
#         #'fetchParticipants',
#         #'fetchParticipants2',
#         #'unparticipate',
#         #'fetchCoords',
#         #'removeCoords',
#         #'fetchHotRaces',
#         #'fetchRace'
#     ]
# }