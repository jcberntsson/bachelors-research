from py2neo import Relationship, Graph, Node, Path

from cases import Base

from neo4j.v1 import GraphDatabase, basic_auth


class Neo4j(Base):
    # driver = GraphDatabase.driver("bolt://localhost", auth=basic_auth("neo4j", "kandidat"))
    driver = GraphDatabase.driver("bolt://46.101.235.47", auth=basic_auth("neo4j", "kandidat"))
    #driver = GraphDatabase.driver("bolt://10.135.10.154", auth=basic_auth("neo4j", "kandidat"))
    session = driver.session()

    # connect to authenticated graph database
    # graph = Graph("http://neo4j:kandidat@localhost:7474/db/data/")
    #graph = Graph("http://neo4j:kandidat@10.135.10.154:7474/db/data/")
    graph = Graph("http://neo4j:kandidat@46.101.235.47:7474/db/data/")

    ####################################
    ####	DATA INITIALIZATION		####
    ####################################

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
                'RETURN ID(event) AS event_id' % organizer_ids[x * 5]
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
                print("Race done")
            print("Event done")

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

    def initRaceOne2(self):
        tx = self.graph.begin()

        # Users
        users = []
        organizers = []
        print("Creating users and organizers")
        for x in range(100):
            user = Node("USER",
                        username="user_" + str(x),
                        fullname="Tester",
                        password="SuperHash")
            tx.create(user)
            users.append(user)
            organizer = Node("ORGANIZER",
                             username="organizer_" + str(x),
                             fullname="Tester",
                             password="xpassx",
                             email="mail_" + str(x) + "@mail.se")
            tx.create(organizer)
            organizers.append(organizer)
        print("Users and organizers have been created")
        # Events & Races
        print("Creating events")
        for x in range(10):
            event = Node("EVENT",
                         name="event_" + str(x),
                         logoURL="google.se/img.png")
            tx.create(event)
            tx.create(Relationship(event, "MADE_BY", organizers[x * 5]))
            print("Event info done")
            for y in range(5):
                race = Node("RACE",
                            name="race_" + str(x * y + x + y),
                            description="A nice race to participate in.",
                            date="2016-06-13",
                            maxDuration=3,
                            preview="linktoimage.png",
                            location="Gothenburg, Sweden",
                            logoURL="google.se/img.png")
                tx.create(race)
                tx.create(Relationship(race, "IN", event))
                # Coordinates
                prev = Node("COORDINATE",
                            lat=33,
                            lng=44,
                            alt=100)
                tx.create(prev)
                tx.create(Relationship(race, "STARTS_WITH", prev))
                for i in range(99):
                    coord = Node("COORDINATE",
                                 lat=10 + i,
                                 lng=11 + i,
                                 alt=20 + i)
                    tx.create(coord)
                    tx.create(Relationship(prev, "FOLLOWED_BY", coord))
                    prev = coord
                tx.create(Relationship(prev, "END_FOR", race))
                print("Map is done")

                rands = []
                for z in range(10):
                    # Participants
                    rand = self.new_rand_int(rands, 0, 99)
                    rands.append(rand)
                    rand2 = self.new_rand_int(rands, 0, 99)
                    rands.append(rand2)
                    activity = Node("ACTIVITY",
                                    joinedAt="2016-08-08")
                    tx.create(activity)
                    tx.create(Path(users[rand], "PARTICIPATING_IN", activity, "OF", race))
                    tx.create(Relationship(users[rand2], "FOLLOWING", activity))

                    # Coordinates
                    prev = Node("COORDINATE",
                                lat=33,
                                lng=44,
                                alt=100)
                    tx.create(prev)
                    tx.create(Relationship(activity, "STARTS_WITH", prev))
                    for i in range(49):
                        coord = Node("COORDINATE",
                                     lat=20 + i,
                                     lng=21 + i,
                                     alt=30 + i)
                        tx.create(coord)
                        tx.create(Relationship(prev, "FOLLOWED_BY", coord))
                        prev = coord
                    tx.create(Relationship(prev, "END_FOR", activity))
                print("A race is done")
            print("An event is done")
        tx.commit()
        # """

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
        print("Users done")

        print("Creating projects")
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
                        'CREATE (user)<-[:MADE_BY]-(comment:COMMENT {text:"Ha-Ha, cool image!", createdAt:"2016-05-11"})-[:ON]->(image) ' % (image_id, self.get_random_of(collaborator_ids))
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
                            'CREATE (user)<-[:MADE_BY]-(comment:COMMENT {text:"Ha-Ha, cool image!", createdAt:"2016-05-11"})-[:ON]->(image) ' % (image_id, self.get_random_of(collaborator_ids))
                        )
            print("Project done")

    def initSkim2(self):
        tx = self.graph.begin()

        # Users
        users = []
        for x in range(50):
            users.append(Node("USER",
                              username="user_" + str(x),
                              email="user_" + str(x) + "@mail.com",
                              password="xpassx"))
            tx.create(users[x])

        # Projects and images
        for x in range(8):
            project = Node("PROJECT",
                           name="project_" + str(x))
            tx.create(project)
            for y in range(10):
                tx.create(Relationship(project, "COLLABORATOR", users[x * 2 + y]))

            for y in range(4):
                # Images
                nbr = x + 5 + y
                image = Node("IMAGE",
                             name="image_" + str(nbr),
                             originalName="original_name",
                             extension="jpg",
                             encoding="PNG/SFF",
                             size=1024,
                             height=1080,
                             width=720,
                             verticalDPI=40,
                             horizontalDPI=50,
                             bitDepth=15,
                             createdAt="2016-03-03",
                             accepted=False)
                tx.create(image)
                tx.create(Relationship(image, "IN", project))

                # SKUS
                sku = Node("SKU",
                           name="sku_" + str(nbr))
                tx.create(sku)
                tx.create(Relationship(sku, "IN", project))
                for z in range(10):
                    # Rows
                    value = Node("SKU_VALUE",
                                 header="header_" + str(z),
                                 value=str(z))
                    tx.create(value)
                    tx.create(Relationship(value, "OF", sku))

                # SKU images
                nbr = x + 5 + y
                image = Node("IMAGE",
                             name="sku_image_" + str(nbr),
                             originalName="original_name",
                             extension="jpg",
                             encoding="PNG/SFF",
                             size=1024,
                             height=1080,
                             width=720,
                             verticalDPI=40,
                             horizontalDPI=50,
                             bitDepth=15,
                             createdAt="2016-03-03",
                             accepted=False)
                tx.create(image)
                tx.create(Relationship(image, "BELONGS_TO", sku))
                for z in range(2):
                    # Comments
                    comment = Node("COMMENT",
                                   text="Haha, cool image",
                                   createdAt="2016-04-04")
                    tx.create(comment)
                    tx.create(Relationship(comment, "ON", image))
                    tx.create(Relationship(comment, "MADE_BY", users[x * 2 + z]))

        tx.commit()

    def clearData(self):
        # Dangerous
        self.graph.delete_all()

    ############################
    ####	TEST METHODS	####
    ############################
    # TODO: All inserting methods should first find the nodes that it is relating for

    # SKIM
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
            out = self.session.run(
                'MATCH (image:IMAGE)-[:IN]->(project:PROJECT)-[:COLLABORATOR]->(user:USER) '
                'WHERE ID(project)=%d '
                'RETURN ID(image) AS image_id, ID(user) AS user_id '
                'LIMIT 1' % inner_self.project_id
            )
            # out.forward()
            # inner_self.user_id = out.current['user_id']
            # inner_self.image_id = out.current['image_id']
            info = list(out)[0]
            # print(info)
            inner_self.user_id = info['user_id']
            inner_self.image_id = info['image_id']

        def run(inner_self):
            comment_cursor = self.session.run(
                'MATCH (user:USER)<-[:COLLABORATOR]-(project:PROJECT)<-[:IN]-(image:IMAGE) '
                'WHERE ID(user)=%d AND ID(project)=%d AND ID(image)=%d '
                'CREATE (image)<-[:ON]-(comment:COMMENT {text:"Ooh, another new comment!", createdAt:"2015-03-02@13:37"} )-[:MADE_BY]->(user) '
                'RETURN ID(comment) AS comment_id' % (inner_self.user_id, inner_self.project_id, inner_self.image_id)
            )  # .dump()
            inner_self.comment_id = list(comment_cursor)[0].comment_id

        def teardown(inner_self):
            self.session.run(
                'START comment=Node(%d) '
                'DETACH DELETE comment '
                'RETURN count(*) AS deleted_nodes' % inner_self.comment_id
            )  # .dump()

        return self.create_case("commentOnImage", setup, run, teardown)

    def pairImageSKU(self):
        def setup(inner_self):
            inner_self.project_id = self.get_random_id('PROJECT')
            out = self.session.run(
                'MATCH (image:IMAGE)-[:IN]->(project:PROJECT)<-[:IN]-(sku:SKU) '
                'WHERE ID(project)=%d '
                'RETURN ID(sku) AS sku_id, ID(image) AS image_id '
                'LIMIT 1' % inner_self.project_id
            )
            info = list(out)[0]
            inner_self.sku_id = info['sku_id']
            inner_self.image_id = info['image_id']

        def run(inner_self):
            belong_cursor = self.session.run(
                'MATCH (sku:SKU)-[:IN]->(project:PROJECT)<-[in:IN]-(image:IMAGE) '
                'WHERE ID(sku)=%d AND ID(project)=%d AND ID(image)=%d '
                'CREATE (image)-[b:BELONGS_TO]->(sku) '
                'DELETE in '
                'RETURN ID(b) AS id' % (inner_self.sku_id, inner_self.project_id, inner_self.image_id)
            )  # .dump()
            inner_self.belong_id = list(belong_cursor)[0].id

        def teardown(inner_self):
            self.session.run(
                'MATCH (image:IMAGE)-[b:BELONGS_TO]->(sku:SKU)-[in:IN]->(project:PROJECT) '
                'WHERE ID(sku)=%d AND ID(project)=%d AND ID(image)=%d '
                'DELETE b '
                'CREATE (image)-[:IN]->(project) '
                'RETURN count(*) AS deleted_rows' % (inner_self.sku_id, inner_self.project_id, inner_self.image_id)
            )  # .dump()

        return self.create_case("pairImageSKU", setup, run, teardown)

    def addRowsToSKU(self):
        def setup(inner_self):
            inner_self.sku_id = self.get_random_id('SKU')

        def run(inner_self):
            tx = self.session.begin_transaction()
            for i in range(10):
                tx.run(
                    'START sku=Node(%d) '
                    'CREATE (:SKU_VALUE { header: "remove_me", value:"110" })-[:OF]->(sku) '
                    'CREATE (:SKU_VALUE { header: "remove_me", value:"120" })-[:OF]->(sku) '
                    'CREATE (:SKU_VALUE { header: "remove_me", value:"130" })-[:OF]->(sku) '
                    'CREATE (:SKU_VALUE { header: "remove_me", value:"140" })-[:OF]->(sku) ' % inner_self.sku_id
                )
            tx.commit()

        def teardown(inner_self):
            self.session.run(
                'MATCH (sku:SKU)<-[of:OF]-(value:SKU_VALUE) '
                'WHERE ID(sku)=%d AND value.header="remove_me" '
                'DELETE of, value '
                'RETURN COUNT(*) AS deleted_rows' % inner_self.sku_id
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

    # RaceOne
    def follow(self):
        def setup(inner_self):
            inner_self.follower_id = self.get_random_id('USER')

            participant_id = self.get_random_id('USER')
            while participant_id == inner_self.follower_id:
                participant_id = self.get_random_id('USER')

            race_id = self.get_random_id('RACE')

            cursor = self.graph.run(
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
            pass

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
                coord_id = coord['coord'].id
                inner_self.coords.append(coord['coord'].properties)
                if i == 1:
                    inner_self.coord_ids.append(coord_id)
                i = (i + 1) % 3
                # print(inner_self.race_id)

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
                '   (start:COORDINATE)<-[:STARTS_WITH]-(race)<-[:END_FOR]-(end:COORDINATE), '
                '   (start)-[:FOLLOWED_BY*]->(coord:COORDINATE)-[:FOLLOWED_BY]->(ending:COORDINATE) '
                'DETACH DELETE coord '
                'RETURN ID(start) AS start_id, ID(end) AS end_id' % inner_self.race_id
            )
            result = self.first_of(cursor)
            prev_id = result["start_id"]
            end_id = result["end_id"]

            for coord in inner_self.coords:
                out = self.session.run(
                    'START prev=Node(%d) '
                    'CREATE (prev)-[:FOLLOWED_BY]->(coord:COORDINATE { lat:%d, lng:%d, alt:%d }) '
                    'RETURN ID(coord) AS coord_id' % (prev_id, coord['lat'], coord['lng'], coord['alt'])
                )
                prev_id = self.evaluate(out, "coord_id")
            self.session.run(
                'START prev=Node(%d), end=Node(%d) '
                'CREATE (prev)-[:FOLLOWED_BY]->(end)' % (prev_id, end_id)
            )

        return self.create_case("removeCoords", setup, run, teardown)

    def removeRace(self):
        def setup(inner_self):
            inner_self.race_id = self.get_random_id('RACE')
            race_cursor = self.graph.run(
                'START race=Node(%d) '
                'MATCH (race)-[:IN]->(event:EVENT) '
                'RETURN race, ID(event) AS event_id' % inner_self.race_id
            )
            info = self.first_of(race_cursor)
            inner_self.race = info['race']
            inner_self.event_id = info['event_id']
            activity_cursor = self.session.run(
                'START race=Node(%d) '
                'MATCH '
                '   (race)<-[:OF]-(activity:ACTIVITY)<-[:PARTICIPATING_IN]-(participant:USER), '
                '   (activity)<-[:FOLLOWING]-(follower:USER) '
                'RETURN activity.joinedAt AS joinedAt, ID(participant) AS part_id, ID(follower) AS follower_id' % inner_self.race_id
            )
            inner_self.activities = {}
            while activity_cursor.forward():
                part_id = activity_cursor.current['part_id']
                follower_id = activity_cursor.current['follower_id']
                joined_at = activity_cursor.current['joinedAt']
                if part_id in inner_self.activities:
                    inner_self.activities[part_id]['follower_ids'].append(follower_id)
                else:
                    inner_self.activities[part_id] = dict(joinedAt=joined_at, follower_ids=[follower_id])

            """
            cursor = self.graph.run(
                'START race=Node(%d) '
                'MATCH '
                '   (race)<-[:OF]-(act:ACTIVITY)<-[:PARTICIPATING_IN]-(part:PARTICIPANT), '
                '   (act)<-[:FOLLOWING]-(follower:FOLLOWER), '
                '   (act)-[:STARTS_WITH|FOLLOWED_BY|END_FOR*]-(act_coord:COORDINATE), '
                '   (race)-[:STARTS_WITH|FOLLOWED_BY|END_FOR*]-(coord:COORDINATE), '
                '   (race)'
                'WHERE not ((race)-[:IN]-(o)) '
                'RETURN r,o,r2,o2' % inner_self.race_id
            )
            while cursor.forward():
                print(cursor.current)
            """

        def run(inner_self):
            pass

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

    def easy_get(self):
        def setup(inner_self):
            pass

        def run(inner_self):
            out = self.graph.run(
                'RETURN 1'
            )  # .dump()

        def teardown(inner_self):
            pass

        return self.create_case("easy_get", setup, run, teardown)

    def easy_get2(self):
        def setup(inner_self):
            pass

        def run(inner_self):
            result = self.session.run("MATCH (test:TEST) RETURN test")
            retained_result = list(result)

        def teardown(inner_self):
            pass

        return self.create_case("easy_get", setup, run, teardown)

    def get_random_id(self, entity_name):
        from random import randint
        entities = self.graph.run(
            'MATCH (ent:%s)'
            'RETURN ID(ent) AS ent_id' % entity_name
        )
        entity_count = self.graph.run(
            'MATCH (ent:%s) '
            'RETURN COUNT(ent)' % entity_name
        ).evaluate()
        forward_count = randint(1, entity_count)
        return entities.current['ent_id'] if entity_count > 0 and entities.forward(forward_count) else None

    @staticmethod
    def get_random_of(values):
        from random import randint
        index = randint(0, len(values) - 1)
        return values[index]

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
