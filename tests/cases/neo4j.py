from cases import Base

from neo4j.v1 import GraphDatabase, basic_auth


class Neo4j(Base):
    # driver = GraphDatabase.driver("bolt://localhost", auth=basic_auth("neo4j", "kandidat"))
    driver = GraphDatabase.driver("bolt://10.135.10.154", auth=basic_auth("neo4j", "kandidat"))
    # driver = GraphDatabase.driver("bolt://46.101.235.47", auth=basic_auth("neo4j", "kandidat"))
    session = driver.session()

    ####################################
    ####	DATA INITIALIZATION		####
    ####################################

    def initReference(self):
        session = self.session
        for x in range(self.quantity_of("blob")):
            session.run(
                'CREATE (test:TEST {name:"Hello"})'
            )

    def initRaceOne(self):
        session = self.session

        user_ids = []
        print("Creating users and organizers")
        for x in range(self.quantity_of("users")):
            cursor = session.run(
                'CREATE (user:USER { username:"user_%s", fullname:"Tester", password:"SuperHash" }) '
                'RETURN ID(user) AS user_id' % str(x)
            )
            user_id = self.evaluate(cursor, "user_id")
            user_ids.append(user_id)
        organizer_ids = []
        for x in range(self.quantity_of("organizers")):
            cursor = session.run(
                'CREATE (organizer:ORGANIZER { username:"organizer_%s", fullname:"Tester", password:"SuperHash", email:"mail@mail.se" }) '
                'RETURN ID(organizer) AS organizer_id' % str(x)
            )
            organizer_id = self.evaluate(cursor, "organizer_id")
            organizer_ids.append(organizer_id)
        print("Users and organizers done")

        print("Creating events")
        for x in range(self.quantity_of("events")):
            event_cursor = session.run(
                'START organizer=Node(%d) '
                'CREATE (event:EVENT {name:"event_name",logoURL:"google.se/img.png"})-[:MADE_BY]->(organizer) '
                'RETURN ID(event) AS event_id' % organizer_ids[x]
            )
            event_id = self.evaluate(event_cursor, "event_id")
            for y in range(self.quantity_of("races")):
                race_cursor = session.run(
                    'START event=Node(%d) '
                    'CREATE (race:RACE '
                    '   {name:"race_name",'
                    '   description:"A nice race to participate in.",'
                    '   date:"2016-06-13",'
                    '   maxDuration:3,'
                    '   preview:"linktoimage.png",'
                    '   location:"Gothenburg, Sweden",'
                    '   logoURL:"google.se/img.png"})-[:IN]->(event) '
                    'RETURN ID(race) AS race_id' % event_id
                )
                race_id = self.evaluate(race_cursor, "race_id")
                session.run(self.create_coords(self.quantity_of("race_coordinates"), race_id))

                # Participants and Followers
                rands = []
                for z in range(self.quantity_of("activities")):
                    rand = self.new_rand_int(rands, 0, len(user_ids) - 2)
                    rands.append(rand)
                    activity_cursor = session.run(
                        'START participant=Node(%d), follower=Node(%d), race=Node(%d) '
                        'CREATE (participant)-[:PARTICIPATING_IN]->(activity:ACTIVITY {joinedAt:"2016-05-05"})-[:OF]->(race) '
                        'CREATE (activity)<-[:FOLLOWING]-(follower) '
                        'RETURN ID(activity) AS activity_id' % (user_ids[rand], user_ids[rand + 1], race_id)
                    )
                    activity_id = self.evaluate(activity_cursor, "activity_id")
                    session.run(self.create_coords(self.quantity_of("activity_coordinates"), activity_id))

    def initSkim(self):
        session = self.session

        user_ids = []
        print("Creating users")
        for x in range(self.quantity_of("users")):
            cursor = session.run(
                'CREATE (user:USER { username:"user_%d", email:"mail@mail.se", password:"SuperHash" }) '
                'RETURN ID(user) AS user_id' % x
            )
            user_id = self.evaluate(cursor, "user_id")
            user_ids.append(user_id)

        print("Creating projects")
        print("Projects: %s" % self.quantity_of("projects"))
        for x in range(self.quantity_of("projects")):
            project_cursor = session.run(
                'CREATE (project:PROJECT {name:"project_%d"}) '
                'RETURN ID(project) AS project_id' % x
            )
            project_id = self.evaluate(project_cursor, "project_id")
            collaborator_ids = []
            for y in range(self.quantity_of("collaborators")):
                user_id = user_ids[(x + 1) * y]
                session.run(
                    'START project=Node(%d), collaborator=Node(%d) '
                    'CREATE (project)-[:COLLABORATOR]->(collaborator)' % (project_id, user_id)
                )
                collaborator_ids.append(user_id)
            for y in range(self.quantity_of("project_images")):
                image_cursor = session.run(
                    'START project=Node(%d) '
                    'CREATE (image:IMAGE {'
                    '   name:"image_name",'
                    '   originalName:"original_name",'
                    '   extension:"jpg",'
                    '   encoding:"PNG/SFF",'
                    '   size:1024,'
                    '   height:1080,'
                    '   width:720,'
                    '   verticalDPI:40,'
                    '   horizontalDPI:50,'
                    '   bitDepth:15,'
                    '   createdAt:"2016-03-03",'
                    '   accepted:False})-[:IN]->(project) '
                    'RETURN ID(image) AS image_id' % project_id
                )
                image_id = self.evaluate(image_cursor, "image_id")
                for z in range(self.quantity_of("image_comments")):
                    session.run(
                        'START image=Node(%d), user=Node(%d) '
                        'CREATE (user)<-[:MADE_BY]-(comment:COMMENT {text:"Ha-Ha, cool image!", createdAt:"2016-05-11"})-[:ON]->(image) ' % (
                            image_id, self.get_random_of(collaborator_ids))
                    )
            for y in range(self.quantity_of("skus")):
                sku_cursor = session.run(
                    'START project=Node(%d) '
                    'CREATE (sku:SKU {name:"sku_name"})-[:IN]->(project) '
                    'RETURN ID(sku) AS sku_id' % project_id
                )
                sku_id = self.evaluate(sku_cursor, "sku_id")
                for z in range(self.quantity_of("sku_values")):
                    session.run(
                        'START sku=Node(%d) '
                        'CREATE (value:SKU_VALUE {header:"header_%d", value:%d})-[:IN]->(sku) ' % (sku_id, z, z)
                    )
                for z in range(self.quantity_of("sku_images")):
                    image_cursor = session.run(
                        'START sku=Node(%d) '
                        'CREATE (image:IMAGE {'
                        '   name:"image_name",'
                        '   originalName:"original_name",'
                        '   extension:"jpg",'
                        '   encoding:"PNG/SFF",'
                        '   size:1024,'
                        '   height:1080,'
                        '   width:720,'
                        '   verticalDPI:40,'
                        '   horizontalDPI:50,'
                        '   bitDepth:15,'
                        '   createdAt:"2016-03-03",'
                        '   accepted:False})-[:BELONGS_TO]->(sku) '
                        'RETURN ID(image) AS image_id' % sku_id
                    )
                    image_id = self.evaluate(image_cursor, "image_id")
                    for a in range(self.quantity_of("image_comments")):
                        session.run(
                            'START image=Node(%d), user=Node(%d) '
                            'CREATE (user)<-[:MADE_BY]-(comment:COMMENT {text:"Ha-Ha, cool image!", createdAt:"2016-05-11"})-[:ON]->(image) ' % (
                                image_id, self.get_random_of(collaborator_ids))
                        )
            print("Project done")

    def clearData(self):
        # Dangerous
        self.session.run(
            'MATCH (n) '
            'DETACH DELETE n'
        )

    ########################
    ####	REFERENCE	####
    ########################

    def tinyGet(self):
        def setup(inner_self):
            pass

        def run(inner_self):
            cursor = self.session.run(
                'RETURN 1'
            )
            results = list(cursor)

        def teardown(inner_self):
            pass

        return self.create_case("tinyGet", setup, run, teardown)

    def smallGet(self):
        def setup(inner_self):
            pass

        def run(inner_self):
            cursor = self.session.run(
                'MATCH (test:TEST) '
                'RETURN test'
            )
            results = list(cursor)

        def teardown(inner_self):
            pass

        return self.create_case("smallGet", setup, run, teardown)

    ####################
    ####	SKIM	####
    ####################

    def fetchSKU(self):
        def setup(inner_self):
            inner_self.sku_id = self.get_random_id('SKU')

        def run(inner_self):
            out = self.session.run(
                'START sku=Node(%d) '
                'MATCH (value:SKU_VALUE)-[of:OF]->(sku:SKU) '
                'RETURN value' % inner_self.sku_id
            )  # .dump()
            sku = list(out)

        def teardown(inner_self):
            pass

        return self.create_case("fetchSKU", setup, run, teardown)

    def fetchUsers(self):
        def setup(inner_self):
            pass

        def run(inner_self):
            out = self.session.run(
                'MATCH (user:USER) RETURN user'
            )  # .dump()
            users = list(out)

        def teardown(inner_self):
            pass

        return self.create_case("fetchUsers", setup, run, teardown)

    def commentOnImage(self):
        def setup(inner_self):
            inner_self.project_id = self.get_random_id('PROJECT')
            cursor = self.session.run(
                'MATCH (image:IMAGE)-[:IN]->(project:PROJECT)-[:COLLABORATOR]->(user:USER) '
                'WHERE ID(project)=%d '
                'RETURN ID(image) AS image_id, ID(user) AS user_id '
                'LIMIT 1' % inner_self.project_id
            )
            info = self.first_of(cursor)
            # print(info)
            inner_self.user_id = info['user_id']
            inner_self.image_id = info['image_id']

        def run(inner_self):
            comment_cursor = self.session.run(
                'MATCH (user:USER)<-[:COLLABORATOR]-(project:PROJECT)<-[:IN]-(image:IMAGE) '
                'WHERE ID(user)=%d AND ID(project)=%d AND ID(image)=%d '
                'CREATE (image)<-[:ON]-(comment:COMMENT {text:"Ooh, another new comment!", createdAt:"2015-03-02@13:37"} )-[:MADE_BY]->(user) '
                'RETURN ID(comment) AS comment_id' % (inner_self.user_id, inner_self.project_id, inner_self.image_id)
            )
            inner_self.comment_id = self.evaluate(comment_cursor, "comment_id")

        def teardown(inner_self):
            self.session.run(
                'START comment=Node(%d) '
                'DETACH DELETE comment '
                'RETURN count(*) AS deleted_nodes' % inner_self.comment_id
            )

        return self.create_case("commentOnImage", setup, run, teardown)

    def pairImageSKU(self):
        def setup(inner_self):
            inner_self.project_id = self.get_random_id('PROJECT')
            cursor = self.session.run(
                'MATCH (image:IMAGE)-[:IN]->(project:PROJECT)<-[:IN]-(sku:SKU) '
                'WHERE ID(project)=%d '
                'RETURN ID(sku) AS sku_id, ID(image) AS image_id '
                'LIMIT 1' % inner_self.project_id
            )
            result = self.first_of(cursor)
            inner_self.sku_id = result['sku_id']
            inner_self.image_id = result['image_id']

        def run(inner_self):
            cursor = self.session.run(
                'MATCH (sku:SKU)-[:IN]->(project:PROJECT)<-[in:IN]-(image:IMAGE) '
                'WHERE ID(sku)=%d AND ID(project)=%d AND ID(image)=%d '
                'CREATE (image)-[b:BELONGS_TO]->(sku) '
                'DELETE in '
                'RETURN ID(b) AS id' % (inner_self.sku_id, inner_self.project_id, inner_self.image_id)
            )
            inner_self.belong_id = self.evaluate(cursor, "id")

        def teardown(inner_self):
            self.session.run(
                'MATCH (image:IMAGE)-[b:BELONGS_TO]->(sku:SKU)-[in:IN]->(project:PROJECT) '
                'WHERE ID(sku)=%d AND ID(project)=%d AND ID(image)=%d '
                'DELETE b '
                'CREATE (image)-[:IN]->(project) '
                'RETURN count(*) AS deleted_rows' % (inner_self.sku_id, inner_self.project_id, inner_self.image_id)
            )

        return self.create_case("pairImageSKU", setup, run, teardown)

    def addRowsToSKU(self):
        def setup(inner_self):
            inner_self.project_id = self.get_random_id('PROJECT')

        def run(inner_self):
            tx = self.session.begin_transaction()
            for i in range(10):
                tx.run(
                    'START project=Node(%d) '
                    'CREATE (sku:SKU { name: "remove_me"})-[:IN]->(project) '
                    'CREATE (:SKU_VALUE { header: "remove_me", value:"110" })-[:OF]->(sku) '
                    'CREATE (:SKU_VALUE { header: "remove_me", value:"120" })-[:OF]->(sku) '
                    'CREATE (:SKU_VALUE { header: "remove_me", value:"130" })-[:OF]->(sku) '
                    'CREATE (:SKU_VALUE { header: "remove_me", value:"140" })-[:OF]->(sku) ' % inner_self.project_id
                )
            tx.commit()

        def teardown(inner_self):
            self.session.run(
                'START project=Node(%d) '
                'MATCH (sku:SKU)-[in:IN]->(project) '
                'WHERE sku.name="remove_me" '
                'MATCH (sku)<-[of:OF]-(value:SKU_VALUE) '
                'WHERE value.header="remove_me" '
                'DELETE of, value, in, sku '
                'RETURN COUNT(*) AS deleted_rows' % inner_self.project_id
            )

        return self.create_case("addRowsToSKU", setup, run, teardown)

    def fetchAllUserComments(self):
        def setup(inner_self):
            inner_self.user_id = self.get_random_id('USER')

        def run(inner_self):
            out = self.session.run(
                'MATCH (comment:COMMENT)-[:MADE_BY]->(user:USER) '
                'WHERE ID(user)=%d '
                'RETURN comment' % inner_self.user_id
            )
            comments = list(out)

        def teardown(inner_self):
            pass

        return self.create_case("fetchAllUserComments", setup, run, teardown)

    ########################
    ####	RACEONE 	####
    ########################

    def follow(self):
        def setup(inner_self):
            inner_self.follower_id = self.get_random_id('USER')

            participant_id = self.get_random_id('USER')
            while participant_id == inner_self.follower_id:
                participant_id = self.get_random_id('USER')

            race_id = self.get_random_id('RACE')

            cursor = self.session.run(
                'MATCH (race:RACE), (user:USER) '
                'WHERE ID(race)=%s AND ID(user)=%s '
                'CREATE (user)-[:PARTICIPATING_IN]->(activity:ACTIVITY { joinedAt:"2016-05-03" })-[:OF]->(race) '
                'RETURN ID(activity) AS activity_id' % (race_id, participant_id)
            )
            inner_self.activity_id = self.evaluate(cursor, "activity_id")

        def run(inner_self):
            cursor = self.session.run(
                'MATCH (activity:ACTIVITY), (follower:USER) '
                'WHERE ID(activity)=%d AND ID(follower)=%d '
                'CREATE (follower)-[f:FOLLOWING]->(activity) '
                'RETURN follower,f,activity' % (inner_self.activity_id, inner_self.follower_id)
            )
            result = list(cursor)

        def teardown(inner_self):
            cursor = self.session.run(
                'MATCH '
                '   (follower:USER)-[following:FOLLOWING]->(activity:ACTIVITY)-[of:OF]->(:RACE),'
                '   (participant:USER)-[participating:PARTICIPATING_IN]->(activity) '
                'WHERE ID(activity)=%d AND ID(follower)=%d '
                'DELETE following, of, participating, activity '
                'RETURN COUNT(*) AS nbr_deleted' % (inner_self.activity_id, inner_self.follower_id)
            )
            result = list(cursor)

        return self.create_case("follow", setup, run, teardown)

    def unfollow(self):
        def setup(inner_self):
            inner_self.follow = self.follow()
            inner_self.follow.setup()
            inner_self.follow.run()

        def run(inner_self):
            inner_self.follow.teardown()

        def teardown(inner_self):
            pass

        return self.create_case("unfollow", setup, run, teardown)

    def insertCoords(self):
        def setup(inner_self):
            inner_self.activity_id = self.get_random_id('ACTIVITY')
            cursor = self.session.run(
                'START act=Node(%d) '
                'MATCH (act:ACTIVITY)<-[:END_FOR]-(end:COORDINATE) '
                'RETURN ID(end) AS end_id' % inner_self.activity_id
            )
            inner_self.end_id = self.evaluate(cursor, "end_id")

        def run(inner_self):
            cursor = self.session.run(
                'START activity=Node(%d) '
                'MATCH (coord:COORDINATE)-[end:END_FOR]->(activity:ACTIVITY) '
                'DELETE end '
                'RETURN ID(coord) AS coord' % inner_self.activity_id
            )
            out = self.first_of(cursor)
            prev_id = out['coord']

            query = 'START first=Node(%d), activity=Node(%d) ' \
                    'CREATE (first)-[:FOLLOWED_BY]->(coord0:COORDINATE { lat:10, lng:11, alt:20 }) ' % (
                        prev_id, inner_self.activity_id)
            for i in range(99):
                query += ' CREATE (%s)-[:FOLLOWED_BY]->(%s:COORDINATE { lat:10, lng:11, alt:20 })' % (
                    "coord" + str(i), "coord" + str(i + 1))
            query += ' CREATE (%s)-[:END_FOR]->(activity)' % "coord100"
            out = self.session.run(query)
            result = list(out)
            """
            self.graph.run(
                'START act=Node(%d) '
                'MATCH (act)-[:STARTS_WITH|FOLLOWED_BY*]-(coord:COORDINATE) '
                'RETURN COUNT(coord)' % inner_self.activity_id
            ).dump()"""

        def teardown(inner_self):
            self.session.run(
                'MATCH (act:ACTIVITY)<-[end:END_FOR]-(coord:COORDINATE) '
                'WHERE ID(act)=%d '
                'DELETE end '
                'RETURN COUNT(*) AS deleted_ends' % inner_self.activity_id
            )
            self.session.run(
                'START original_end=node(%d) '
                'MATCH (original_end:COORDINATE)-[f:FOLLOWED_BY]->(o:COORDINATE) '
                'MATCH p=(o:COORDINATE)-[:FOLLOWED_BY*0..]->(o2:COORDINATE) '
                'DELETE f, p '
                'RETURN COUNT(*) AS deleted_paths' % inner_self.end_id
            )
            self.session.run(
                'MATCH (coord:COORDINATE), (act:ACTIVITY) '
                'WHERE ID(coord)=%d AND ID(act)=%d '
                'CREATE (coord)-[:END_FOR]->(act) '
                'RETURN COUNT(*) AS created_ends' % (inner_self.end_id, inner_self.activity_id)
            )

        return self.create_case("insertCoords", setup, run, teardown)

    def fetchParticipants(self):
        def setup(inner_self):
            pass

        def run(inner_self):
            cursor = self.session.run(
                'MATCH (user:USER)-[:PARTICIPATING_IN]->(activity:ACTIVITY) '
                'RETURN user.username, count(activity) AS count '
                'ORDER BY count DESC '
                'LIMIT 10'
            )
            participants = list(cursor)

        def teardown(inner_self):
            pass

        return self.create_case("fetchParticipants", setup, run, teardown)

    def fetchParticipants2(self):
        def setup(inner_self):
            pass

        def run(inner_self):
            cursor = self.session.run(
                'MATCH (participant:USER)-[:PARTICIPATING_IN]->(act:ACTIVITY)<-[:FOLLOWING]-(follower:USER) '
                'RETURN participant.username, ID(act), count(follower) AS count '
                'ORDER BY count DESC '
                'LIMIT 10'
            )
            participants = list(cursor)

        def teardown(inner_self):
            pass

        return self.create_case("fetchParticipants2", setup, run, teardown)

    def duplicateEvent(self):
        def setup(inner_self):
            pass

        def run(inner_self):
            raise NotImplementedError

        def teardown(inner_self):
            pass

        return self.create_case("duplicateEvent", setup, run, teardown)

    def unparticipate(self):
        def setup(inner_self):
            inner_self.activity_id = self.get_random_id('ACTIVITY')
            # print(inner_self.activity_id)
            activity = self.session.run(
                'MATCH (participant:USER)-[:PARTICIPATING_IN]->(act:ACTIVITY)-[:OF]->(race:RACE) '
                'WHERE ID(act)=%s '
                'RETURN act.joinedAt AS joinedAt,ID(participant) AS participant_id,ID(race) AS race_id' % inner_self.activity_id
            )
            activity = list(activity)[0]
            inner_self.joinedAt = activity['joinedAt']
            inner_self.participant_id = activity['participant_id']
            inner_self.race_id = activity['race_id']
            followers_cursor = self.session.run(
                'MATCH (follower:USER)-[:FOLLOWING]->(act:ACTIVITY) '
                'WHERE ID(act)=%s '
                'RETURN ID(follower) AS id' % inner_self.activity_id
            )
            follower_ids = []
            for follower in followers_cursor:
                follower_ids.append(follower['id'])
            inner_self.follower_ids = follower_ids

        def run(inner_self):
            cursor = self.session.run(
                'START act=Node(%d) '
                'DETACH DELETE act '
                'RETURN COUNT(*) AS deleted' % inner_self.activity_id
            )
            deleted = self.evaluate(cursor, "deleted")

        def teardown(inner_self):
            out = self.session.run(
                'MATCH (race:RACE), (participant:USER) '
                'WHERE ID(race)=%d AND ID(participant)=%d '
                'CREATE (participant)-[:PARTICIPATING_IN]->(act:ACTIVITY { joinedAt:"%s" })-[:OF]->(race) '
                'RETURN ID(act) AS act_id' % (inner_self.race_id, inner_self.participant_id, inner_self.joinedAt)
            )
            out = list(out)[0]
            inner_self.activity_id = out['act_id']
            # print(inner_self.activity_id)
            tx = self.session.begin_transaction()
            for follower_id in inner_self.follower_ids:
                tx.run(
                    'MATCH (act:ACTIVITY), (follower:USER) '
                    'WHERE ID(act)=%d AND ID(follower)=%d '
                    'CREATE (follower)-[:FOLLOWING]->(act)' % (inner_self.activity_id, follower_id)
                )
            tx.commit()

        return self.create_case("unparticipate", setup, run, teardown)

    def fetchCoords(self):
        def setup(inner_self):
            inner_self.activity_id = self.get_random_id('ACTIVITY')

        def run(inner_self):
            cursor = self.session.run(
                'START act=Node(%d) '
                'MATCH (act:ACTIVITY)-[:STARTS_WITH|FOLLOWED_BY*]-(coord:COORDINATE) '
                'RETURN coord' % inner_self.activity_id
            )
            coordinates = list(cursor)

        def teardown(inner_self):
            pass

        return self.create_case("fetchCoords", setup, run, teardown)

    def removeCoords(self):
        def setup(inner_self):
            inner_self.race_id = self.get_random_id('RACE')
            coordinates_cursor = self.session.run(
                'START race=Node(%d) '
                'MATCH '
                '   (start:COORDINATE)<-[:STARTS_WITH]-(race)<-[:END_FOR]-(end:COORDINATE), '
                '   (start)-[:FOLLOWED_BY*]->(coord:COORDINATE)-[:FOLLOWED_BY]->(ending:COORDINATE) '
                'RETURN coord' % inner_self.race_id
            )
            coordinates = list(coordinates_cursor)
            inner_self.coords = []
            inner_self.coord_ids = []
            i = 0
            for coord in coordinates:
                if i == 1:
                    inner_self.coords.append(coord['coord'].properties)
                    inner_self.coord_ids.append(coord['coord'].id)
                i = (i + 1) % 3

        def run(inner_self):
            tx = self.session.begin_transaction()
            for coord_id in inner_self.coord_ids:
                tx.run(
                    'START middle=Node(%d) '
                    'MATCH (first:COORDINATE)-[f1:FOLLOWED_BY]->(middle:COORDINATE)-[f2:FOLLOWED_BY]->(last:COORDINATE) '
                    'DELETE f1,f2,middle '
                    'CREATE (first)-[:FOLLOWED_BY]->(last)' % coord_id
                )
            tx.commit()
            """
            coords_count = self.session.run(
                'START race=Node(%d) '
                'MATCH (race)-[:STARTS_WITH]->(start:COORDINATE)-[f:FOLLOWED_BY*]->(coord:COORDINATE) '
                'RETURN COUNT(f) AS count' % inner_self.race_id
            )
            print(self.evaluate(coords_count, "count"))
            """

        def teardown(inner_self):
            cursor = self.session.run(
                'START race=Node(%d) '
                'MATCH '
                '   (start:COORDINATE)<-[:STARTS_WITH]-(race)<-[end_for:END_FOR]-(end:COORDINATE) '
                'DELETE end_for '
                'RETURN ID(end) AS end_id' % inner_self.race_id
            )
            prev_id = self.evaluate(cursor, "end_id")
            for coord in inner_self.coords:
                cursor = self.session.run(
                    'START prev=Node(%d) '
                    'CREATE (prev)-[:FOLLOWED_BY]->(coord:COORDINATE { lat:%d, lng:%d, alt:%d }) '
                    'RETURN ID(coord) AS coord_id' % (prev_id, coord['lat'], coord['lng'], coord['alt'])
                )
                prev_id = self.evaluate(cursor, "coord_id")
            self.session.run(
                'START prev=Node(%d), race=Node(%d) '
                'CREATE (prev)-[:END_FOR]->(race)' % (prev_id, inner_self.race_id)
            )

        return self.create_case("removeCoords", setup, run, teardown)

    def removeRace(self):
        def setup(inner_self):
            pass

        def run(inner_self):
            raise NotImplementedError

        def teardown(inner_self):
            pass

        return self.create_case("removeRace", setup, run, teardown)

    def fetchHotRaces(self):
        def setup(inner_self):
            pass

        def run(inner_self):
            cursor = self.session.run(
                'MATCH '
                '   (race:RACE)<-[:OF]-(activity:ACTIVITY), '
                '   (participant:USER)-[:PARTICIPATING_IN]->(activity)<-[:FOLLOWING]-(follower:USER) '
                'WITH race, COUNT(follower)+COUNT(participant) AS popularity '
                'RETURN race, popularity '
                'ORDER BY popularity '
                'LIMIT 10'
            )
            races = list(cursor)

        def teardown(inner_self):
            pass

        return self.create_case("fetchHotRaces", setup, run, teardown)

    def fetchRace(self):
        def setup(inner_self):
            inner_self.race_id = self.get_random_id('RACE')

        def run(inner_self):
            cursor = self.session.run(
                'MATCH (race:RACE)-[:IN]->(event:EVENT) '
                'WHERE ID(race)=%d '
                'RETURN race, event' % inner_self.race_id
            )
            result = self.first_of(cursor)
            race = result['race'].properties
            event = result['event'].properties
            cursor = self.session.run(
                'MATCH (race:RACE)-[:STARTS_WITH]->(start:COORDINATE)-[:FOLLOWED_BY*]->(coord:COORDINATE) '
                'WHERE ID(race)=%d '
                'RETURN coord' % inner_self.race_id
            )
            coords = []
            for record in cursor:
                coords.append(record['coord'].properties)
            cursor = self.session.run(
                'MATCH '
                '   (race:RACE)<-[:OF]-(act:ACTIVITY)<-[:PARTICIPATING_IN]-(user:USER),'
                '   (act:ACTIVITY)<-[:FOLLOWING]-(follower:USER) '
                'WHERE ID(race)=%d '
                'RETURN user, COUNT(follower) AS nbr_of_followers' % inner_self.race_id
            )
            participants = []
            for record in cursor:
                participants.append(record["user"].properties)
            returnable = dict(information=race, event=event, coordinates=coords, participants=participants)

        def teardown(inner_self):
            pass

        return self.create_case("fetchRace", setup, run, teardown)

    ################################
    ####	HELPER METHODS		####
    ################################

    def get_random_id(self, entity_name):
        from random import randint
        cursor = self.session.run(
            'MATCH (ent:%s)'
            'RETURN ID(ent) AS ent_id' % entity_name
        )
        entities = list(cursor)
        index = randint(0, len(entities) - 1)
        return entities[index]['ent_id'] if "ent_id" in entities[index] else None

    @staticmethod
    def evaluate(cursor, name):
        return Neo4j.first_of(cursor)[name]

    @staticmethod
    def first_of(cursor):
        try:
            return list(cursor)[0]
        except IndexError as err:
            print("IndexError for evaluate(): %s" % err)
            return None

    @staticmethod
    def create_coords(nbr, for_id):
        if nbr <= 1:
            return ""
        lat = 10
        lng = 11
        alt = 20
        query = 'START race=Node(%d) ' \
                'CREATE (race)-[:STARTS_WITH]->(coord0:COORDINATE { lat:%d, lng:%d, alt:%d }) ' % (
                    for_id, lat, lng, alt)
        for i in range(nbr - 1):
            lat += 1
            lng += 1
            alt += 1
            query += 'CREATE (%s)-[:FOLLOWED_BY]->(%s:COORDINATE { lat:%d, lng:%d, alt:%d }) ' % (
                "coord" + str(i), "coord" + str(i + 1), lat, lng, alt)
        query += 'CREATE (%s)-[:END_FOR]->(race)' % ("coord" + str(nbr - 1))
        return query
