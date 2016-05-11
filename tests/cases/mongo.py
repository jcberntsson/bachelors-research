from pymongo import MongoClient
from cases import Base
import random
from random import randint
import datetime


class Mongo(Base):
    # connect to authenticated mongo database
    #client = MongoClient("mongodb://46.101.103.26:27017")
    client = MongoClient("mongodb://10.135.3.156:27017")
    db = client.db

    ####################################
    ####	DATA INITIALIZATION		####
    ####################################

    def initRaceOne(self):
        # Users and Organizers
        users = []
        print("Creating users and organizers")
        for x in range(self.quantity_of("users")):
            users.append(self.db.users.insert_one({
                "username": "user_" + str(x),
                "email": "user_" + str(x) + "@gmail.com",
                "password": "SuperHash"
            }).inserted_id)
        organizers = []
        for x in range(self.quantity_of("organizers")):
            organizers.append(self.db.organizers.insert_one({
                "username": "organizer_" + str(x),
                "fullname": "Tester",
                "password": "xpassx",
                "email": "mail_" + str(x) + "@gmail.se"
            }).inserted_id)
        print("Users and organizers done")
        # Coordinates
        race_coordinates = []
        for i in range(self.quantity_of("race_coordinates")):
            race_coordinates.append({
                "lat": 10 + i,
                "lng": 11 + i,
                "alt": 12 + i,
                "createdAt": datetime.datetime.now()
            })
        activity_coordinates = []
        for i in range(self.quantity_of("activity_coordinates")):
            activity_coordinates.append({
                "lat": 10 + i,
                "lng": 11 + i,
                "alt": 12 + i,
                "createdAt": datetime.datetime.now()
            })
        # Events & Races
        print("Creating events")
        for x in range(self.quantity_of("events")):
            event = self.db.events.insert_one({
                "name": "event_" + str(x),
                "logoURL": "gooogle.se/img.png",
                "organizer": organizers[x]
            }).inserted_id
            for y in range(self.quantity_of("races")):
                rands = []
                activities = []
                for z in range(self.quantity_of("activities")):
                    # Participants
                    rand = self.new_rand_int(rands, 0, len(users) - 2)
                    rands.append(rand)
                    activities.append({
                        "participating": users[rand],
                        "joinedAt": "2016-08-08",
                        "following": [users[rand + 1]],
                        "coordinates": activity_coordinates,
                    })
                self.db.races.insert_one({
                    "name": "race_" + str(x * y + x + y),
                    "description": "A nice race to participate in.",
                    "date": "2016-06-13",
                    "maxDuration": 3,
                    "preview": "linktoimage.png",
                    "location": "Gothenburg, Sweden",
                    "logoURL": "google.se/img.png",
                    "event": event,
                    "start": {"lat": 33, "lng": 44, "alt": 100},
                    "coordinates": race_coordinates,
                    "end": {"lat": 40, "lng": 34, "alt": 320},
                    "activities": activities
                })
            print("Event done")

    def initSkim(self):
        # User array to be able to print out in console
        users = []
        print("Creating users")
        # Users is a collection of user documents in skimdatabase
        for x in range(self.quantity_of("users")):
            users.append(self.db.users.insert_one({
                "username": "user_" + str(x),
                "email": "user_" + str(x) + "@gmail.com",
                "password": "xpassx"
            }).inserted_id)
        print("Users done")
        # Projects and collaborator
        print("Creating projects")
        for x in range(self.quantity_of("projects")):
            collaborators = []
            for y in range(self.quantity_of("collaborators")):
                collaborators.append(users[(x + 1) * y])
            images = []
            for y in range(self.quantity_of("project_images")):
                # Images
                comments = []
                for z in range(self.quantity_of("image_comments")):
                    # Comments
                    comment = {
                        "text": "Haha, cool image bastard",
                        "createdAt": "2016-04-04",
                        "made_by": self.get_random_of(collaborators)
                    }
                    comments.append(comment)
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
                    "comments": comments
                }).inserted_id)
            sku_list = []
            for y in range(self.quantity_of("skus")):
                # SKUS
                sku_values = []
                for z in range(self.quantity_of("sku_values")):
                    # SKU Values
                    sku_values.append({
                        "header": "header_" + str(z),
                        "value": str(z)
                    })
                # SKU images
                sku_images = []
                for z in range(self.quantity_of("sku_images")):
                    comments = []
                    for a in range(self.quantity_of("image_comments")):
                        # Comments
                        comment = {
                            "text": "Haha, cool image bastard",
                            "createdAt": "2016-04-04",
                            "made_by": self.get_random_of(collaborators)
                        }
                        comments.append(comment)
                    nbr = x + 5 + y
                    image_sku = {
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
                        "comments": comments
                    }
                    sku_images.append(image_sku)
                sku_list.append(self.db.skus.insert_one({
                    "name": "sku_" + str(x * y),
                    "sku_values": sku_values,
                    "images_sku": sku_images
                }).inserted_id)

            self.db.projects.insert_one({
                "name": "project_" + str(x),
                "collaborator": collaborators,
                "images": images,
                "skus": sku_list
            })
            print("Project done")

    def initReference(self):
        for a in range(self.quantity_of("blob")):
            self.db.abc.insert_one({"name": "hello"})

    def clearData(self):
        self.client.drop_database("db")

    ############################
    ####	TEST METHODS	####
    ############################
    
    # REFERENCE
    def tinyGet(self):
        def setup(inner_self):
            pass

        def run(inner_self):
            cursor = self.db.abd.find()
            result = list(cursor)

        def teardown(inner_self):
            pass

        return self.create_case("easy_get", setup, run, teardown)

    def smallGet(self):
        def setup(inner_self):
            pass

        def run(inner_self):
            cursor = self.db.abc.find({})
            result = list(cursor)

        def teardown(inner_self):
            pass

        return self.create_case("easy_get2", setup, run, teardown)

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
            pass

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

        def run(inner_self):
            self.db["races"].update({"_id":inner_self.race_id},{"$pull":{"coordinates":{"$in":inner_self.coordinates}}})

        def teardown(inner_self):
            self.db["races"].update({"_id":inner_self.race_id},{"$push":{"coordinates":{"$each":inner_self.coordinates}}})

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
            race = self.db.races.aggregate([{"$unwind": '$activities'},{"$group": {"_id": '$_id',"totalsize": {"$sum": {"$size": '$activities.following'}}}},{"$limit":10}])
            
        def teardown(inner_self):
            pass

        return self.create_case("fetchHotRaces", setup, run, teardown)

    def fetchRace(self):
        def setup(inner_self):
            race_id = self.get_random_id("races")
            inner_self.race_id = race_id

        def run(inner_self):
            race = self.db["races"].find_one({"_id": inner_self.race_id})

        def teardown(inner_self):
            pass

        return self.create_case("fetchRace", setup, run, teardown)
    

