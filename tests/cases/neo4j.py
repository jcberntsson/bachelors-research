import random

from py2neo import Relationship, Graph, Node, Path

from cases import Base


class Neo4j(Base):

    # connect to authenticated graph database
    #graph = Graph("http://neo4j:kandidat@10.135.10.154:7474/db/data/")
    graph = Graph("http://neo4j:kandidat@46.101.235.47:7474/db/data/")

    ####################################
    ####	DATA INITIALIZATION		####
    ####################################

    def initRaceOne(self):
        tx = self.graph.begin()

        # Users
        users = []
        organizers = []
        for x in range(50):
            users.append(Node("USER",
                              username="user_" + str(x),
                              fullname="Tester",
                              password="SuperHash"))
            tx.create(users[x])
            organizer = Node("ORGANIZER",
                             username="organizer_" + str(x),
                             fullname="Tester",
                             password="xpassx",
                             email="mail_" + str(x) + "@mail.se")
            tx.create(organizer)
            organizers.append(organizer)

        # Events & Races
        for x in range(10):
            event = Node("EVENT",
                         name="event_" + str(x),
                         logoURL="google.se/img.png")
            tx.create(event)
            tx.create(Relationship(event, "MADE_BY", organizers[x * 5]))
            for y in range(5):
                race = Node("RACE",
                            name="race_" + str(random.randint(1, 500)),
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
                tx.create(Relationship(prev, "START_FOR", race))
                for i in range(99):
                    coord = Node("COORDINATE",
                                 lat=10 + i,
                                 lng=11 + i,
                                 alt=20 + i)
                    tx.create(coord)
                    tx.create(Relationship(prev, "FOLLOWED_BY", coord))
                    prev = coord
                tx.create(Relationship(prev, "END_FOR", race))

                rands = []
                for z in range(10):
                    # Participants
                    rand = self.new_rand_int(rands, 0, 49)
                    rands.append(rand)
                    rand2 = self.new_rand_int(rands, 0, 49)
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
                    tx.create(Relationship(prev, "START_FOR", activity))
                    for i in range(49):
                        coord = Node("COORDINATE",
                                     lat=20 + i,
                                     lng=21 + i,
                                     alt=30 + i)
                        tx.create(coord)
                        tx.create(Relationship(prev, "FOLLOWED_BY", coord))
                        prev = coord
                    tx.create(Relationship(prev, "END_FOR", activity))

        tx.commit()

    def initSkim(self):
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
                    row = Node("ROW",
                               header="header_" + str(z),
                               value=str(z))
                    tx.create(row)
                    tx.create(Relationship(row, "OF", sku))

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

    def initReddit(self):
        pass

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
            self.graph.run(
                'MATCH (row:ROW)-[of:OF]->(sku:SKU) '
                'WHERE ID(sku)=%d '
                'RETURN sku,of,row' % inner_self.sku_id
            )  # .dump()

        def teardown(inner_self):
            pass

        return self.create_case("fetchSKU", setup, run, teardown)

    def fetchUsers(self):
        def setup(inner_self):
            pass

        def run(inner_self):
            self.graph.run(
                'MATCH (user:USER) RETURN user'
            )#.dump()

        def teardown(inner_self):
            pass

        return self.create_case("fetchUsers", setup, run, teardown)

    def commentOnImage(self):
        def setup(inner_self):
            inner_self.project_id = self.get_random_id('PROJECT')
            out = self.graph.run(
                'MATCH (image:IMAGE)-[:IN]->(project:PROJECT)-[:COLLABORATOR]->(user:USER) '
                'WHERE ID(project)=%d '
                'RETURN ID(image) AS image_id, ID(user) AS user_id '
                'LIMIT 1' % inner_self.project_id
            )
            out.forward()
            inner_self.user_id = out.current['user_id']
            inner_self.image_id = out.current['image_id']

        def run(inner_self):
            self.graph.run(
                'MATCH (user:USER)<-[:COLLABORATOR]-(project:PROJECT)<-[:IN]-(image:IMAGE) '
                'WHERE ID(user)=%d AND ID(project)=%d AND ID(image)=%d '
                'CREATE (image)<-[:ON]-(comment:COMMENT {text:"Ooh, another new comment!", createdAt:"2015-03-02@13:37"} )-[:MADE_BY]->(user) '
                'RETURN comment' % (inner_self.user_id, inner_self.project_id, inner_self.image_id)
            )#.dump()

        def teardown(inner_self):
            self.graph.run(
                'MATCH '
                '   (user:USER)<-[collaborator:COLLABORATOR]-(project:PROJECT)<-[in:IN]-(image:IMAGE), '
                '   (user)<-[made:MADE_BY]-(comment:COMMENT)-[on:ON]->(image) '
                'WHERE ID(user)=%d AND ID(project)=%d AND ID(image)=%d '
                'DELETE made,on,comment '
                'RETURN count(*) AS deleted_rows' % (inner_self.user_id, inner_self.project_id, inner_self.image_id)
            )#.dump()

        return self.create_case("commentOnImage", setup, run, teardown)

    def pairImageSKU(self):
        def setup(inner_self):
            inner_self.project_id = self.get_random_id('PROJECT')
            out = self.graph.run(
                'MATCH (image:IMAGE)-[:IN]->(project:PROJECT)<-[:IN]-(sku:SKU) '
                'WHERE ID(project)=%d '
                'RETURN ID(sku) AS sku_id, ID(image) AS image_id '
                'LIMIT 1' % inner_self.project_id
            )
            out.forward()
            inner_self.sku_id = out.current['sku_id']
            inner_self.image_id = out.current['image_id']

        def run(inner_self):
            self.graph.run(
                'MATCH (sku:SKU)-[:IN]->(project:PROJECT)<-[in:IN]-(image:IMAGE)'
                'WHERE ID(sku)=%d AND ID(project)=%d AND ID(image)=%d '
                'CREATE (image)-[b:BELONGS_TO]->(sku) '
                'DELETE in '
                'RETURN ID(b) AS ID' % (inner_self.sku_id, inner_self.project_id, inner_self.image_id)
            )  # .dump()

        def teardown(inner_self):
            self.graph.run(
                'MATCH (image:IMAGE)-[b:BELONGS_TO]->(sku:SKU)-[in:IN]->(project:PROJECT) '
                'WHERE ID(sku)=%d AND ID(project)=%d AND ID(image)=%d '
                'DELETE b '
                'CREATE (image)-[:IN]->(project) '
                'RETURN count(*) AS deleted_rows' % (inner_self.sku_id, inner_self.project_id, inner_self.image_id)
            )#.dump()

        return self.create_case("pairImageSKU", setup, run, teardown)

    def addRowsToSKU(self):
        def setup(inner_self):
            pass

        def run(inner_self):
            pass

        def teardown(inner_self):
            pass

        return self.create_case("addRowsToSKU", setup, run, teardown)

    def fetchAllUserComments(self):
        def setup(inner_self):
            pass

        def run(inner_self):
            pass

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

            out = self.graph.run(
                'MATCH (race:RACE), (user:USER) '
                'WHERE ID(race)=%s AND ID(user)=%s '
                'CREATE (user)-[:PARTICIPATING_IN]->(activity:ACTIVITY { joinedAt:"2016-05-03" })-[:OF]->(race) '
                'RETURN ID(activity) AS activity_id' % (race_id, participant_id)
            )
            inner_self.activity_id = out.current['activity_id'] if out.forward() else None

        def run(inner_self):
            self.graph.run(
                'MATCH (activity:ACTIVITY), (follower:USER) '
                'WHERE ID(activity)=%d AND ID(follower)=%d '
                'CREATE (follower)-[f:FOLLOWING]->(activity) '
                'RETURN follower,f,activity' % (inner_self.activity_id, inner_self.follower_id)
            )#.dump()

        def teardown(inner_self):
            self.graph.run(
                'MATCH '
                '   (follower:USER)-[following:FOLLOWING]->(activity:ACTIVITY)-[of:OF]->(:RACE),'
                '   (participant:USER)-[participating:PARTICIPATING_IN]->(activity) '
                'WHERE ID(activity)=%d AND ID(follower)=%d '
                'DELETE following, of, participating, activity '
                'RETURN COUNT(*) AS nbr_deleted' % (inner_self.activity_id, inner_self.follower_id)
            )#.dump()

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
            out = self.graph.run(
                'MATCH (race:RACE)<-[:END_FOR]-(coord:COORDINATE) '
                'RETURN ID(coord) AS coord_id, ID(race) AS race_id '
                'LIMIT 1'
            )
            if out.forward():
                inner_self.coord_id = out.current['coord_id']
                inner_self.race_id = out.current['race_id']

        def run(inner_self):
            out = self.graph.run(
                'MATCH (coord:COORDINATE)-[end:END_FOR]->(race:RACE) '
                'WHERE ID(coord)=%d AND ID(race)=%d '
                'DELETE end '
                'RETURN coord, race' % (inner_self.coord_id, inner_self.race_id)
            )
            prev = Node("COORDINATE")
            race = Node("RACE")
            if out.forward():
                prev = out.current['coord']
                race = out.current['race']

            tx = self.graph.begin()

            for i in range(1000):
                coord = Node("COORDINATE",
                             lat=10 + i,
                             lng=11 + i,
                             alt=20 + i)
                # tx.create(coord)
                tx.create(Relationship(prev, "FOLLOWED_BY", coord))
                prev = coord
            tx.create(Relationship(prev, "END_FOR", race))
            tx.commit()

        def teardown(inner_self):
            self.graph.run(
                'MATCH (race:RACE)<-[end:END_FOR]-(coord:COORDINATE) '
                'WHERE ID(race)=%d '
                'DELETE end' % inner_self.race_id
            )
            self.graph.run(
                'MATCH (end:COORDINATE)<-[followed:FOLLOWED_BY*]-(original_end:COORDINATE)'
                'WHERE ID(original_end)=%d '
                'FOREACH (f in followed | DELETE f)'
                'DELETE end '
                'RETURN COUNT(*) AS deleted' % inner_self.coord_id
            )
            self.graph.run(
                'MATCH (coord:COORDINATE), (race:RACE) '
                'WHERE ID(coord)=%d AND ID(race)=%d '
                'CREATE (coord)-[:END_FOR]->(race) '
                'RETURN COUNT(*)' % (inner_self.coord_id, inner_self.race_id)
            )

        return self.create_case("insertCoords", setup, run, teardown)

    def fetchParticipants(self):
        def setup(inner_self):
            pass

        def run(inner_self):
            self.graph.run(
                'MATCH (user:USER)-[:PARTICIPATING_IN]->(activity:ACTIVITY) '
                'RETURN user.username, count(activity) AS count '
                'ORDER BY count DESC '
                'LIMIT 10'
            ).dump()

        def teardown(inner_self):
            pass

        return self.create_case("fetchParticipants", setup, run, teardown)

    def duplicateEvent(self):
        def setup(inner_self):
            pass

        def run(inner_self):
            pass

        def teardown(inner_self):
            pass

        return self.create_case("duplicateEvent", setup, run, teardown)

    def fetchParticipants2(self):
        def setup(inner_self):
            pass

        def run(inner_self):
            self.graph.run(
                'MATCH (participant:USER)-[:PARTICIPATING_IN]->(:ACTIVITY)<-[:FOLLOWING]-(follower:USER) '
                'RETURN participant.username, count(follower) AS count '
                'ORDER BY count DESC '
                'LIMIT 10'
            ).dump()

        def teardown(inner_self):
            pass

        return self.create_case("fetchParticipants2", setup, run, teardown)

    def unparticipate(self):
        def setup(inner_self):
            inner_self.activity_id = self.get_random_id('ACTIVITY')
            activity = self.graph.run(
                'MATCH (participant:USER)-[:PARTICIPATING_IN]->(act:ACTIVITY)-[:OF]->(race:RACE) '
                'WHERE ID(act)=%s '
                'RETURN act,ID(participant) AS participant_id,ID(race) AS race_id' % inner_self.activity_id
            )
            activity.forward()
            inner_self.activity = activity.current['act']
            inner_self.participant_id = activity.current['participant_id']
            inner_self.race_id = activity.current['race_id']
            followers_cursor = self.graph.run(
                'MATCH (follower:USER)-[:FOLLOWING]->(act:ACTIVITY) '
                'WHERE ID(act)=%s '
                'RETURN ID(follower) AS follower_id' % inner_self.activity_id
            )
            follower_ids = []
            while followers_cursor.forward():
                follower_ids.append(followers_cursor.current['follower_id'])
            inner_self.follower_ids = follower_ids

        def run(inner_self):
            self.graph.run(
                'MATCH '
                '   (participant:USER)-[p:PARTICIPATING_IN]->(act:ACTIVITY)<-[f:FOLLOWING]-(follower:USER), '
                '   (act)-[of:OF]->(race:RACE) '
                'WHERE ID(act)=%d '
                'DELETE p,f,of,act '
                'RETURN COUNT(*) AS deleted' % inner_self.activity_id
            ).dump()

        def teardown(inner_self):
            self.graph.run(
                'MATCH (act:ACTIVITY), (race:RACE), (participant:USER) '
                'WHERE ID(act)=%d AND ID(race)=%d AND ID(participant)=%d '
                'CREATE (participant)-[:PARTICIPATING_IN]->(activity)-[:OF]->(race)' % (inner_self.activity_id, inner_self.race_id, inner_self.participant_id)
            )
            tx = self.graph.begin()
            for follower_id in inner_self.follower_ids:
                tx.run(
                    'MATCH (act:ACTIVITY), (follower:USER) '
                    'WHERE ID(act)=%d AND ID(follower)=%d '
                    'CREATE (follower)-[:FOLLOWING]->(act)' % (inner_self.activity_id, follower_id)
                )
            tx.commit()

        return self.create_case("unparticipate", setup, run, teardown)

    def updateCoords(self):
        def setup(inner_self):
            pass

        def run(inner_self):
            pass

        def teardown(inner_self):
            pass

        return self.create_case("updateCoords", setup, run, teardown)

    def fetchCoords(self):
        def setup(inner_self):
            pass

        def run(inner_self):
            pass

        def teardown(inner_self):
            pass

        return self.create_case("fetchCoords", setup, run, teardown)

    def removeCoords(self):
        def setup(inner_self):
            inner_self.race_id = self.get_random_id('RACE')

        def run(inner_self):
            coordinates_cursor = self.graph.run(
                'MATCH '
                '   (start:COORDINATE)-[:START_FOR]->(race:RACE)<-[:END_FOR]-(end:COORDINATE), '
                '   (start)-[:FOLLOWED_BY*]->(before:COORDINATE)-[:FOLLOWED_BY]->(coord:COORDINATE) ' # -[:FOLLOWED_BY*]->(end)
                'WHERE ID(race)=%d '
                'RETURN ID(coord) AS coord_id, coord, ID(before) AS before_id' % inner_self.race_id
            )
            i = 0
            inner_self.removed_coords = []
            tx = self.graph.begin()
            while coordinates_cursor.forward():
                if i % 3 == 0:
                    coord = coordinates_cursor.current['coord']
                    coord_id = coordinates_cursor.current['coord_id']
                    before_id = coordinates_cursor.current['before_id']
                    coordinate = {
                        "data": coord,
                        "before_id": before_id
                    }
                    inner_self.removed_coords.append(coordinate)
                    tx.run(
                        'MATCH (first:COORDINATE)-[f1:FOLLOWED_BY]->(middle:COORDINATE)-[f2:FOLLOWED_BY]->(last:COORDINATE) '
                        'WHERE ID(middle)=%d '
                        'DELETE f1,f2,middle '
                        'CREATE (first)-[:FOLLOWED_BY]->(last)' % coord_id
                    )
                i += 1
            tx.commit()

        def teardown(inner_self):
            tx = self.graph.begin()
            for coord in reversed(inner_self.removed_coords):
                tx.run(
                    'MATCH (before:COORDINATE)-[f:FOLLOWED_BY]->(after:COORDINATE) '
                    'WHERE ID(before)=%d '
                    'DELETE f '
                    'CREATE (before)-[:FOLLOWED_BY]->%s-[:FOLLOWED_BY]->(after)' % (coord['before_id'], coord['data'])
                )
            tx.commit()

        return self.create_case("removeCoords", setup, run, teardown)

    def removeRace(self):
        def setup(inner_self):
            inner_self.race_id = self.get_random_id('RACE')
            inner_self.race = self.graph.run(
                'MATCH (race:RACE) '
                'WHERE ID(race)=%d '
                'RETURN race' % inner_self.race_id
            )

        def run(inner_self):
            pass

        def teardown(inner_self):
            pass

        return self.create_case("removeRace", setup, run, teardown)

    def fetchHotRaces(self):
        def setup(inner_self):
            pass

        def run(inner_self):
            pass

        def teardown(inner_self):
            pass

        return self.create_case("fetchHotRaces", setup, run, teardown)

    def fetchRace(self):
        def setup(inner_self):
            pass

        def run(inner_self):
            pass

        def teardown(inner_self):
            pass

        return self.create_case("fetchRace", setup, run, teardown)

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
