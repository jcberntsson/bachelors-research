from pymongo import MongoClient
from cases import Base
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
                imagesku = {
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
                }
                skuimages.append(imagesku)
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

    def get_random_id(self, entity_name):
        from random import randint
        container = []
        for entity in self.db[entity_name].find():
            container.append(entity["_id"])
        random = randint(0, len(container) - 1)
        return container[random]
   
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
            cursor = self.cnx.cursor()
            cursor.execute("SELECT id FROM activity")
            result = cursor.fetchall()
            rand = random.randint(0,len(result)-1)
            activity_id = result[rand][0]
            inner_self.activity_id = str(activity_id)
            cursor.close()

        def run(inner_self):
            cursor = self.cnx.cursor()
            cursor.execute("SELECT * FROM activityCoordinate WHERE activity="+inner_self.activity_id)
            result = cursor.fetchall()
            cursor.close()

        def teardown(inner_self):
            pass

        return self.create_case("fetchCoords", setup, run, teardown)

    def removeCoords(self):
        def setup(inner_self):
            cursor = self.cnx.cursor()
            cursor.execute("SELECT id FROM map")
            result = cursor.fetchall()
            rand = random.randint(0,len(result)-1)
            map_id = result[rand][0]
            inner_self.map_id = str(map_id)
            cursor.execute("SELECT id,lat,lng,alt,map FROM point WHERE map="+str(map_id))
            result = cursor.fetchall()
            inner_self.points = result
            cursor.execute("SELECT id FROM point WHERE map="+str(map_id))
            result = cursor.fetchall()
            random.shuffle(result)
            inner_self.point_ids = result[:(len(result)//3)]
            cursor.close()

        def run(inner_self):
            point_ids = ""
            for p in inner_self.point_ids:
                point_ids = point_ids + str(p[0]) + ","
            point_ids = point_ids[:-1]
            cursor = self.cnx.cursor()
            cursor.execute("DELETE FROM point WHERE id IN ("+point_ids+")")
            cursor.close()
            self.cnx.commit()

        def teardown(inner_self):
            cursor = self.cnx.cursor()
            cursor.execute("DELETE FROM point WHERE map="+inner_self.map_id)
            for p in inner_self.points:
                point_id = str(p[0])
                lat = str(p[1])
                lng = str(p[2])
                alt = str(p[3])
                map = str(p[4])
                cursor.execute("INSERT INTO point (id, lat,lng,alt,map) VALUES("+point_id+","+lat+","+lng+","+alt+","+map+")")
            cursor.close()
            self.cnx.commit()

        return self.create_case("removeCoords", setup, run, teardown)

    def removeRace(self):
        def setup(inner_self):
            cursor = self.cnx.cursor()
            cursor.execute("SELECT id,name,description,race_date,max_duration,preview,location,logo_url,event_id FROM race")
            result = cursor.fetchall()
            rand = random.randint(0,len(result)-1)
            inner_self.race = result[rand]
            race_id = result[rand][0]
            cursor.execute("SELECT id,map,race FROM racemap WHERE race='"+str(race_id)+"'")
            result = cursor.fetchall()
            inner_self.racemaps = result
            inner_self.race_id = str(race_id)
            cursor.close()
        def run(inner_self):
            cursor = self.cnx.cursor()
            cursor.execute("DELETE FROM race where id='"+inner_self.race_id+"'")
            cursor.close()
            self.cnx.commit()

        def teardown(inner_self):
            cursor = self.cnx.cursor()
            cursor.execute("INSERT INTO race (id,name,description,race_date,max_duration,preview,location,logo_url,event_id) VALUES('"+str(inner_self.race[0])+"','"+str(inner_self.race[1])+"','"+str(inner_self.race[2])+"','"+str(inner_self.race[3])+"','"+str(inner_self.race[4])+"','"+str(inner_self.race[5])+"','"+str(inner_self.race[6])+"','"+str(inner_self.race[7])+"','"+str(inner_self.race[8])+"')")
            for rm in inner_self.racemaps:
                cursor.execute("INSERT INTO racemap (id,map,race) VALUES('"+str(rm[0])+"','"+str(rm[1])+"','"+str(rm[2])+"')")
            cursor.close()
            self.cnx.commit()

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
            cursor = self.cnx.cursor()
            cursor.execute("SELECT id FROM race")
            result = cursor.fetchall()
            rand = random.randint(0,len(result)-1)
            race_id = result[rand][0]
            inner_self.race_id = str(race_id)

        def run(inner_self):
            cursor = self.cnx.cursor()
            cursor.execute("SELECT race.*,event.name,racemap.id,racemap.map,p1.lat,p1.lng,p1.alt,p2.lat,p2.lng,p2.alt FROM race INNER JOIN event ON race.event_id=event.id "+
                "INNER JOIN racemap ON racemap.race = race.id "+
                "LEFT JOIN point as p1 ON racemap.start_point = p1.id "+
                "LEFT JOIN point as p2 ON racemap.goal_point = p2.id "+
                "WHERE race.ID="+inner_self.race_id)
            result = cursor.fetchall()[0]
            map_id = str(result[13])
            race = result[:14]
            start_point = result[14:17]
            goal_point = result[17:20]
            cursor.execute("SELECT * FROM point WHERE map = "+map_id +" ORDER BY orderIndex")
            mapCoords = cursor.fetchall()[0]
            cursor.execute("SELECT participant.id, participant.username,participant.fullname FROM activity INNER JOIN participant ON activity.participant=participant.id WHERE race = "+inner_self.race_id)
            participants = cursor.fetchall()
            returnable = dict(race=race,start_point=start_point,goal_point=goal_point,map_coordinates=mapCoords,participants=participants)
            cursor.close()

        def teardown(inner_self):
            pass

        return self.create_case("fetchRace", setup, run, teardown)
    