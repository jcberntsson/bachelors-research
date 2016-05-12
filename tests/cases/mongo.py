from pymongo import MongoClient
from cases import Base
import random
from random import randint
import datetime


class Mongo(Base):
    # connect to authenticated mongo database
    client = MongoClient("mongodb://46.101.103.26:27017")
    #client = MongoClient("mongodb://10.135.3.156:27017")
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
                "username": "user_" + str(x),
                "email": "user_" + str(x) + "@gmail.com",
                "password": "SuperHash"
            }).inserted_id)
            organizers.append(self.db.organizers.insert_one({
                "username": "organizer_" + str(x),
                "fullname": "Tester",
                "password": "xpassx",
                "email": "mail_" + str(x) + "@gmail.se"
            }).inserted_id)
        # Events & Races        
        for x in range(10):
            event = self.db.events.insert_one({
                "name": "event_" + str(x),
                "logoURL": "gooogle.se/img.png",
                "organizer": organizers[x * 5]
            }).inserted_id
            for y in range(5):
                allCordinates = []
                for i in range(98):
                    allCordinates.append({
                        "lat": 10 + i,
                        "lng": 11 + i,
                        "alt": 12 + i,
                        "createdAt":datetime.datetime.now()
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
                        "participating": users[rand],
                        "joinedAt": "2016-08-08",
                        "following": [users[rand2]],
                        "coordinates": allCordinates,
                    })

                raceId = self.db.races.insert_one({
                    "name": "race_" + str(x * y + x + y),
                    "description": "A nice race to participate in.",
                    "date": "2016-06-13",
                    "maxDuration": 3,
                    "preview": "linktoimage.png",
                    "location": "Gothenburg, Sweden",
                    "logoURL": "google.se/img.png",
                    "event": event,
                    "start": {"lat": 33, "lng": 44, "alt": 100},
                    "coordinates": allCordinates,
                    "end": {"lat": 40, "lng": 34, "alt": 320},
                    "activities": activities
                }).inserted_id

    def initSkim(self):
        # User array to be able to print out in console
        users = []
        # Users is a collection of user documents in skimdatabase
        for x in range(50):
            users.append(self.db.users.insert_one({
                "username": "user_" + str(x),
                "email": "user_" + str(x) + "@gmail.com",
                "password": "xpassx"
            }).inserted_id)
        # Projects and collaborator
        for x in range(8):
            collaboratorsData = []
            for y in range(10):
                collaboratorsData.append(self.db.users.find({}, {})[x * 2 + y])
            images = []
            skulist = []
            skuimages = []
            for y in range(4):
                # Images
                nbr = x + 5 + y
                images.append(self.db.images.insert_one({
                    "name": "image_" + str(nbr),
                    "originalName": "original_name",
                    "extension": "jpg",
                    "encoding": "PNG/SFF",
                    "size": 1024,
                    "height": 1080,
                    "width": 720,
                    "verticalDPI": 40,
                    "horizontalDPI": 50,
                    "bitDepth": 15,
                    "createdAt": "2016-03-03",
                    "accepted": False,
                    "comments": []
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
                skuimages.append(self.db.skuimages.insert_one({
                    "name": "sku_image_" + str(nbr),
                    "originalName": "original_name",
                    "extension": "jpg",
                    "encoding": "PNG/SFF",
                    "size": 1024,
                    "height": 1080,
                    "width": 720,
                    "verticalDPI": 40,
                    "horizontalDPI": 50,
                    "bitDepth": 15,
                    "createdAt": "2016-03-03",
                    "accepted": False,
                    "comments": comment
                }).inserted_id)
                skulist.append(self.db.skus.insert_one({
                    "name": "sku_" + str(nbr),
                    "sku_values": skuValues,
                    "images_sku": skuimages
                }).inserted_id)

            project = self.db.projects.insert_one({
                "name": "project_" + str(x),
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

    def easy_get(self):
        def setup(inner_self):
            inner_self.sku_id = self.get_random_id("skus")

        def run(inner_self):
            self.db.skus.find_one({"_id": inner_self.sku_id})

        def teardown(inner_self):
            pass

        return self.create_case("easy_get", setup, run, teardown)

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
            out = self.db.users.find()
            users = list(out)
            # this will print out the value print(list(self.db.users.find())) 

        def teardown(inner_self):
            pass

        return self.create_case("fetchUsers", setup, run, teardown)

    def commentOnImage(self):
        def setup(inner_self):
            inner_self.project_id = self.get_random_id("projects")
            out = self.db.projects.find_one({"_id": inner_self.project_id})

            inner_self.user_id = out["collaborator"][0]
            inner_self.image_id = out["images"][0]

        def run(inner_self):
            self.db.images.update(
                {"_id": inner_self.image_id},
                {"$push": {"comments": {"text": "Ooh, another new comment!", "createdAt": "2015-03-02@13:37"}}}
            )

        def teardown(inner_self):
            self.db.images.update(
                {"_id": inner_self.image_id},
                {"$pull": {"comments": {"text": "Ooh, another new comment!"}}}
            )
            # print(self.db.images.find_one({"_id": inner_self.image_id}))

        return self.create_case("commentOnImage", setup, run, teardown)
    
    def pairImageSKU(self):
        def setup(inner_self):
            inner_self.project_id = self.get_random_id("projects")
            out = self.db.projects.find_one({"_id": inner_self.project_id})
            
            inner_self.sku_id = out["skus"][0]
            inner_self.image_id = out["images"][0]
            
        def run(inner_self):
            self.db.skus.update(
                {"_id": inner_self.sku_id},
                {"$push": {"images_sku": inner_self.image_id}}
            )
            # print (list(self.db.skus.find({"_id": inner_self.sku_id}))) add this before update as well to see the changes
            
        def teardown(inner_self):
            print (list(self.db.skus.find({"_id": inner_self.sku_id})))
            self.db.skus.update(
                {"_id": inner_self.sku_id},
                {"$pull": {"images_sku": inner_self.image_id}}
            )
                    
        return self.create_case("pairImageSKU", setup, run, teardown)
    
    def addRowsToSKU(self):
        def setup(inner_self):
        
        def run(inner_self):
            pass
            
        def teardown(inner_self):
            pass
            
    def get_random_id(self, entity_name):
        from random import randint
        container = []
        for entity in self.db[entity_name].find():
            container.append(entity["_id"])
        random = randint(0, len(container) - 1)
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

   
    # RACEONE
    def follow(self):
        def setup(inner_self):
            race_id = self.get_random_id("races")
            race = self.db["races"].find_one({"_id":race_id},{"coordinates":0})
            random = randint(0, len(race["activities"]) - 1)
            participant_id = race["activities"][random]["participating"]
            follower_id = self.get_random_id("users")
            while follower_id in race["activities"][random]["following"]:
                follower_id = self.get_random_id("users")
            inner_self.race_id = race_id
            inner_self.participant_id = participant_id
            inner_self.follower_id = follower_id

        def run(inner_self):
            self.db["races"].update({"_id":inner_self.race_id,"activities":{"$elemMatch":{"participating":inner_self.participant_id}}},{"$push":{"activities.$.following":inner_self.follower_id}})
        
        def teardown(inner_self):
            self.db["races"].update({"_id":inner_self.race_id,"activities":{"$elemMatch":{"participating":inner_self.participant_id}}},{"$pull":{"activities.$.following":inner_self.follower_id}})

        return self.create_case("follow", setup, run, teardown)

    def unfollow(self):
        def setup(inner_self):
            race_id = self.get_random_id("races")
            race = self.db["races"].find_one({"_id":race_id},{"coordinates":0})
            random = randint(0, len(race["activities"]) - 1)
            participant_id = race["activities"][random]["participating"]
            random2 = randint(0, len(race["activities"][random]["following"]) - 1)
            follower_id = race["activities"][random]["following"][random2]
            inner_self.race_id = race_id
            inner_self.participant_id = participant_id
            inner_self.follower_id = follower_id

        def run(inner_self):
            self.db["races"].update({"_id":inner_self.race_id,"activities":{"$elemMatch":{"participating":inner_self.participant_id}}},{"$pull":{"activities.$.following":inner_self.follower_id}})
        
        def teardown(inner_self):
            self.db["races"].update({"_id":inner_self.race_id,"activities":{"$elemMatch":{"participating":inner_self.participant_id}}},{"$push":{"activities.$.following":inner_self.follower_id}})

        return self.create_case("unfollow", setup, run, teardown)



    def insertCoords(self):
        def setup(inner_self):
            race_id = self.get_random_id("races")
            race = self.db["races"].find_one({"_id":race_id},{"coordinates":0})
            random = randint(0, len(race["activities"]) - 1)
            participant_id = race["activities"][random]["participating"]
            inner_self.race_id = race_id
            inner_self.participant_id = participant_id
            inner_self.coords = []
            for i in range(100):
                inner_self.coords.append({
                        "lat": 10 + i,
                        "lng": 11 + i,
                        "alt": 20 + i,
                        "createdAt":datetime.datetime.now()
                    })

        def run(inner_self):
            for coord in inner_self.coords:
                self.db["races"].update({"_id":inner_self.race_id,"activities":{"$elemMatch":{"participating":inner_self.participant_id}}},{"$push":{"activities.$.coordinates":coord}})

        def teardown(inner_self):
            for coord in inner_self.coords:
                self.db["races"].update({"_id":inner_self.race_id,"activities":{"$elemMatch":{"participating":inner_self.participant_id}}},{"$pull":{"activities.$.coordinates":coord}})

        return self.create_case("insertCoords", setup, run, teardown)

    def fetchParticipants(self):
        def setup(inner_self):
            pass

        def run(inner_self):
            self.db["races"].find()
            cursor = self.cnx.cursor()
            cursor.execute("SELECT participant.id, count(*) as followCount FROM participant INNER JOIN activity ON activity.participant=participant.id GROUP BY participant.id ORDER BY followCount")
            result = cursor.fetchall()
            cursor.close()

        def teardown(inner_self):
            pass

        return self.create_case("fetchParticipants", setup, run, teardown)

    def duplicateEvent(self):
        pass
        '''def setup(inner_self):
            cursor = self.cnx.cursor()
            cursor.execute("SELECT * FROM event")
            result = cursor.fetchall()
            rand = random.randint(0,len(result)-1)
            event_id = str(result[rand][0])
            inner_self.event_id = event_id
            cursor.close()
        def run(inner_self):
            cursor = self.cnx.cursor()
            cursor.execute("SELECT * FROM race WHERE event_id='"+inner_self.event_id+"'")
            result = cursor.fetchall()
            inner_self.races = result
            race_ids = "("
            for r in result:
                race_ids = race_ids +"'"+ str(r[0]) + "',"
            race_ids = race_ids[:-1]
            race_ids = race_ids + ")"
            cursor.execute("SELECT * from racemap WHERE id IN "+race_ids)
            result = cursor.fetchall()
            inner_self.racemaps = result
            cursor.execute("")
            cursor.close()

        def teardown(inner_self):
            cursor = self.cnx.cursor()
            cursor.execute("")
            cursor.close()

        return self.create_case("fetchParticipants2", setup, run, teardown)'''

    def fetchParticipants2(self):
        def setup(inner_self):
            pass
        def run(inner_self):
            cursor = self.cnx.cursor()
            cursor.execute("SELECT activity.race, participant.id, count(*) FROM participant "
                "INNER JOIN activity ON activity.participant=participant.id "
                "INNER JOIN follow WHERE activity=activity.id GROUP BY participant.id,activity.race")
            result = cursor.fetchall()
            cursor.close()

        def teardown(inner_self):
            pass

        return self.create_case("fetchParticipants2", setup, run, teardown)

    def unparticipate(self):
        def setup(inner_self):
            race_id = self.get_random_id("races")
            race = self.db["races"].find_one({"_id":race_id},{"coordinates":0})
            random = randint(0, len(race["activities"]) - 1)
            participant_id = race["activities"][random]["participating"]
            inner_self.race_id = race_id
            inner_self.participant_id = participant_id
            inner_self.activity = race["activities"][random]
        
        def run(inner_self):
            self.db["races"].update({"_id":inner_self.race_id},{"$pull":{"activities":{"participating":inner_self.participant_id}}})

        def teardown(inner_self):
            import json
            self.db["races"].update({"_id":inner_self.race_id},{"$push":{"activities":inner_self.activity}})
            
        return self.create_case("unparticipate", setup, run, teardown)



    def fetchCoords(self):
        def setup(inner_self):
            race_id = self.get_random_id("races")
            race = self.db["races"].find_one({"_id":race_id},{"coordinates":0})
            random = randint(0, len(race["activities"]) - 1)
            participant_id = race["activities"][random]["participating"]
            inner_self.race_id = race_id
            inner_self.participant_id = participant_id

        def run(inner_self):
            race = self.db["races"].find_one({"_id":inner_self.race_id},{"coordinates":0,"activities":{"$elemMatch":{"participating":inner_self.participant_id}},"activities.participating":0,"activities.following":0,"activities.joinedAt":0})
            coordinates = race["activities"][0]["coordinates"]

        def teardown(inner_self):
            pass

        return self.create_case("fetchCoords", setup, run, teardown)

    def removeCoords(self):
        def setup(inner_self):
            race_id = self.get_random_id("races")
            race = self.db["races"].find_one({"_id":race_id},{"activities":0})
            coordinates = race["coordinates"]
            random.shuffle(coordinates)
            inner_self.race_id = race_id
            inner_self.coordinates = coordinates[:(len(coordinates)//3)]
            print(len(coordinates))

        def run(inner_self):
            self.db["races"].update({"_id":inner_self.race_id},{"$pull":{"coordinates":{"$in":inner_self.coordinates}}})
            print(len(self.db["races"].find_one({"_id":inner_self.race_id},{"activities":0})["coordinates"]))

        def teardown(inner_self):
            self.db["races"].update({"_id":inner_self.race_id},{"$push":{"coordinates":{"$each":inner_self.coordinates}}})
            print(len(self.db["races"].find_one({"_id":inner_self.race_id},{"activities":0})["coordinates"]))

        return self.create_case("removeCoords", setup, run, teardown)

    def removeRace(self):
        def setup(inner_self):
            race_id = self.get_random_id("races")
            race = self.db["races"].find_one({"_id":race_id})
            inner_self.race = race
            inner_self.race_id = race_id 
               
        def run(inner_self):
            self.db["races"].remove({"_id":inner_self.race_id})

        def teardown(inner_self):
            self.db.races.insert_one(inner_self.race)

        return self.create_case("removeRace", setup, run, teardown)

    def fetchHotRaces(self):
        def setup(inner_self):
            pass

        def run(inner_self):
            cursor = self.cnx.cursor()
            cursor.execute("SELECT activity.race, count(*) as rating FROM activity "
                "INNER JOIN follow on follow.activity=activity.id GROUP BY activity.race ORDER BY rating DESC LIMIT 10")
            result = cursor.fetchall()
            cursor.close()

        def teardown(inner_self):
            pass

        return self.create_case("fetchHotRaces", setup, run, teardown)

    def fetchRace(self):
        def setup(inner_self):
            race_id = self.get_random_id("races")
            inner_self.race_id = race_id

        def run(inner_self):
            race = self.db["races"].find_one({"_id":race_id})

        def teardown(inner_self):
            pass

        return self.create_case("fetchRace", setup, run, teardown)
    

