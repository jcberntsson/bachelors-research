import random

from py2neo import Relationship, Graph, Node, Path

from cases import Base


class Neo4j(Base):
    # connect to authenticated graph database
    graph = Graph("http://neo4j:kandidat@10.135.10.154:7474/db/data/")

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
            # print("Setup")
            out = self.graph.run(
                'CREATE (sku:SKU { name: "test_sku" }) '
                'RETURN ID(sku) AS sku_id'
            )
            if out.forward():
                # print(out.current)
                inner_self.sku_id = out.current['sku_id']

        def run(inner_self):
            self.graph.run(
                'MATCH (row:ROW)-[of:OF]->(sku:SKU) '
                'WHERE ID(sku)=%d '
                'RETURN sku,of,row' % inner_self.sku_id
            )  # .dump()

        def teardown(inner_self):
            self.graph.run(
                'MATCH (sku:SKU) '
                'WHERE ID(sku)=%d '
                'DELETE sku '
                'RETURN count(*) AS deleted_rows' % inner_self.sku_id
            )  # .dump()

        return self.create_case("fetchSKU", setup, run, teardown)

    def fetchUsers(self):
        def setup(inner_self):
            pass

        def run(inner_self):
            self.graph.run(
                'MATCH (user:USER) RETURN user'
            ).dump()

        def teardown(inner_self):
            pass

        return self.create_case("fetchUsers", setup, run, teardown)

    def commentOnImage(self):
        def setup(inner_self):
            out = self.graph.run(
                'CREATE (user:USER { username: "test_user" })<-[:COLLABORATOR]-(project:PROJECT { name: "test_project" })<-[:IN]-(image:IMAGE { name: "test_image" }) '
                'RETURN ID(user) AS user_id, ID(project) AS project_id, ID(image) AS image_id'
            )
            if out.forward():
                # print(out.current)
                inner_self.user_id = out.current['user_id']
                inner_self.project_id = out.current['project_id']
                inner_self.image_id = out.current['image_id']

        def run(inner_self):
            self.graph.run(
                'MATCH (user:USER)<-[:COLLABORATOR]-(project:PROJECT)<-[:IN]-(image:IMAGE) '
                'WHERE ID(user)=%d AND ID(project)=%d AND ID(image)=%d '
                'CREATE (image)<-[:ON]-(comment:COMMENT {text:"Ooh, another new comment!", createdAt:"2015-03-02@13:37"} )-[:MADE_BY]->(user) '
                'RETURN comment' % (inner_self.user_id, inner_self.project_id, inner_self.image_id)
            )  # .dump()

        def teardown(inner_self):
            self.graph.run(
                'MATCH (user:USER)<-[collaborator:COLLABORATOR]-(project:PROJECT)<-[in:IN]-(image:IMAGE), '
                '      (user)<-[made:MADE_BY]-(comment:COMMENT)-[on:ON]->(image) '
                'WHERE ID(user)=%d AND ID(project)=%d AND ID(image)=%d '
                'DELETE collaborator,in,made,on,user,project,image,comment '
                'RETURN count(*) AS deleted_rows' % (inner_self.user_id, inner_self.project_id, inner_self.image_id)
            ).dump()

        return self.create_case("commentOnImage", setup, run, teardown)

    def pairImageSKU(self):
        def setup(inner_self):
            # print("Setup")
            # TODO Add sku rows
            out = self.graph.run(
                'CREATE (sku:SKU { name: "test_sku" })-[:IN]->(project:PROJECT { name: "test_project" })<-[in:IN]-(image:IMAGE { name:"test_image" }) '
                'RETURN ID(sku) AS sku_id, ID(project) AS project_id, ID(image) AS image_id'
            )
            if out.forward():
                # print(out.current)
                inner_self.sku_id = out.current['sku_id']
                inner_self.project_id = out.current['project_id']
                inner_self.image_id = out.current['image_id']

        def run(inner_self):
            # print("Run")
            self.graph.run(
                'MATCH (sku:SKU)-[:IN]->(project:PROJECT)<-[in:IN]-(image:IMAGE)'
                'WHERE ID(sku)=%d AND ID(project)=%d AND ID(image)=%d '
                'CREATE (image)-[b:BELONGS_TO]->(sku) '
                'DELETE in '
                'RETURN ID(b) AS ID' % (inner_self.sku_id, inner_self.project_id, inner_self.image_id)
            )  # .dump()

        def teardown(inner_self):
            # print("Teardown")
            self.graph.run(
                'MATCH (image:IMAGE)-[b:BELONGS_TO]->(sku:SKU)-[in:IN]->(project:PROJECT) '
                'WHERE ID(sku)=%d AND ID(project)=%d AND ID(image)=%d '
                'DELETE b, image, in, sku, project '
                'RETURN count(*) AS deleted_rows' % (inner_self.sku_id, inner_self.project_id, inner_self.image_id)
            )  # .dump()

        return self.create_case("pairImageSKU", setup, run, teardown)

    # RaceOne
    def follow(self):
        def setup(inner_self):
            out = self.graph.run(
                'CREATE (user:USER { username: "follower_test", fullname: "follower_name", password: "hash" })'
                'RETURN ID(user) AS follower_id'
            )
            inner_self.follower_id = str(out.current['follower_id'] if out.forward() else -1)
            print("Follower added with ID: " + inner_self.follower_id)
            out = self.graph.run(
                'CREATE (user:USER { username: "participant_test", fullname: "participant_name", password: "hash" })'
                #'-[:PARTICIPATING_IN]->(:ACTIVITY { joinedAt: "2016-05-03" })'
                'RETURN ID(user) AS participant_id'
            )
            participant_id = str(out.current['participant_id'] if out.forward() else -1)
            print("Participant added with ID: " + participant_id)
            out = self.graph.run(
                'MATCH (race:RACE)'
                'RETURN ID(race) AS race_id'
            )
            for result in out:
                print(result)
            race_id = str(-1)
            print("Race found with ID: " + race_id)
            out = self.graph.run(
                'MATCH (race:RACE), (user:USER) '
                'WHERE ID(race)=%s AND ID(user)=%s '
                'CREATE (user)-[:PARTICIPATING_IN]->(activity:ACTIVITY { joinedAt:"2016-05-03" })-[:OF]->(race) '
                'RETURN ID(activity) AS activity_id' % (race_id, participant_id)
            )
            inner_self.activity_id = str(out.current['activity_id'] if out.forward() else -1)
            print("Activity created with ID: " + inner_self.activity_id)

        def run(inner_self):
            self.graph.run(
                'MATCH (activity:ACTIVITY), (follower:USER) '
                'WHERE ID(ACTIVITY)=%s AND ID(follower)=%s '
                'CREATE (follower)-[:FOLLOWING]->(activity) '
                'RETURN follower,activity' % (inner_self.activity_id, inner_self.follower_id)
            ).dump()

        def teardown(inner_self):
            self.graph.run(
                'MATCH (follower:USER)-[following:FOLLOWING]->(activity:ACTIVITY)-[of:OF]->(:RACE),'
                '(participant:USER)-[participating:PARTICIPATING_IN]->(activity) '
                'WHERE ID(ACTIVITY)=%s AND ID(follower)=%s '
                'DELETE following, of, participating, follower, activity, participant '
                'RETURN COUNT(*) AS nbr_deleted' % (inner_self.activity_id, inner_self.follower_id)
            ).dump()

        return self.create_case("follow", setup, run, teardown)

    def unfollow(self):
        def setup(inner_self):
            out = self.graph.run(
                'CREATE (race:RACE {date:"2016-06-13",description:"A nice race to participate in.",location:"Gothenburg, Sweden",logoURL:"google.se/img.png",maxDuration:3,name:"race_235",preview:"linktoimage.png"}) '
                'RETURN ID(race) AS test_id'
            )
            inner_self.test_id = str(out.current['test_id'] if out.forward() else -1)
            #print(inner_self.test_id)

        def run(inner_self):
            out = self.graph.run(
                'MATCH (race:RACE) '
                'WHERE ID(race)=%s '
                'RETURN race' % inner_self.test_id
            )
            #print(str(out.current if out.forward() else "nothing here"))

        def teardown(inner_self):
            self.graph.run(
                'MATCH (race:RACE) '
                'WHERE ID(race)=%s '
                'DELETE race '
                'RETURN COUNT(*) AS removed' % inner_self.test_id
            )#.dump()

        return self.create_case("unfollow", setup, run, teardown)
        '''
        self.graph.run(
            'MATCH (:USER)-[:PARTICIPATING_IN]->(activity:ACTIVITY)-[:OF]->(:RACE) '
            'WHERE ID(activity) = 6168 '
            'WITH * LIMIT 1 '
            'DETACH DELETE activity '
            'RETURN count(*)'
        ).dump()
        '''

    def fetchComments(self):
        def setup(inner_self):
            pass

        def run(inner_self):
            out = self.graph.run(
                'MATCH (race:RACE) '
                'RETURN race '
                'LIMIT 1'
            )
            #print(str(out.current if out.forward() else "nothing here"))

        def teardown(inner_self):
            pass

        return self.create_case("fetchComments", setup, run, teardown)

    def fetchHotPosts(self):
        def setup(inner_self):
            from random import randint
            out = self.graph.run(
                'MATCH (race:RACE) '
                'RETURN ID(race) AS race_id'
            )
            inner_self.test_id = out.current['race_id'] if out.forward() else -1
            #out_count = self.graph.run(
            #    'MATCH (race:RACE) '
            #    'RETURN COUNT(race) AS nbr_of_races'
            #)
            #iterations = randint(0, out_count.evaluate() - 1)
            #for result in out:
            #    if iterations == 0:
            #        inner_self.test_id = result['race_id']
            #        break
            #    iterations -= 1

        def run(inner_self):
            out = self.graph.run(
                'MATCH (race:RACE) '
                'WHERE ID(race)=%d '
                'RETURN race' % inner_self.test_id
            )
            #print(str(out.current if out.forward() else "nothing here"))

        def teardown(inner_self):
            pass

        return self.create_case("fetchHotPosts", setup, run, teardown)

    def insertCoords(self):
        def setup(inner_self):
            out = self.graph.run(
                'MATCH (race:RACE)<-[:END_FOR]-(coord:COORDINATE) '
                'RETURN ID(coord) AS coord_id, ID(race) AS race_id '
                'LIMIT 1'
            )
            if out.forward():
                inner_self.coord_id = str(out.current['coord_id'])
                inner_self.race_id = str(out.current['race_id'])
                print("Race: " + inner_self.race_id + ", coord: " + inner_self.coord_id)

        def run(inner_self):
            out = self.graph.run(
                'MATCH (coord:COORDINATE)-[end:END_FOR]->(race:RACE) '
                'WHERE ID(coord)=%s AND ID(race)=%s '
                'DELETE end '
                'RETURN coord, race' % (inner_self.coord_id, inner_self.race_id)
            )
            prev = Node("COORDINATE")
            race = Node("RACE")
            if out.forward():
                prev = out.current['coord']
                race = out.current['coord']

            tx = self.graph.begin()

            for i in range(99):
                coord = Node("COORDINATE",
                             lat=10 + i,
                             lng=11 + i,
                             alt=20 + i)
                #tx.create(coord)
                tx.create(Relationship(prev, "FOLLOWED_BY", coord))
                prev = coord
            tx.create(Relationship(prev, "END_FOR", race))
            tx.commit()

        def teardown(inner_self):
            self.graph.run(
                'MATCH (:RACE)<-[:END_FOR]-(coord2:COORDINATE)<-[followed:FOLLOWED_BY*1..100]-(coord:COORDINATE) '
                'WHERE ID(coord)=%s '
                'DELETE followed, coord2 '
                'RETURN COUNT(*) AS deleted' % inner_self.coord_id
            ).dump()
            self.graph.run(
                'MATCH (coord:COORDINATE)-[end:END_FOR]->(race:RACE) '
                'WHERE ID(coord)=%s AND ID(race)=%s '
                'CREATE  end '
                'RETURN coord, race' % (inner_self.coord_id, inner_self.race_id)
            ).dump()

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

    def createComment(self):
        def setup(inner_self):
            pass

        def run(inner_self):
            pass

        def teardown(inner_self):
            pass

        return self.create_case("createComment", setup, run, teardown)

    def fetchBestFriend(self):
        def setup(inner_self):
            pass

        def run(inner_self):
            pass

        def teardown(inner_self):
            pass

        return self.create_case("fetchBestFriend", setup, run, teardown)

    def fetchUsersAndComments(self):
        def setup(inner_self):
            pass

        def run(inner_self):
            pass

        def teardown(inner_self):
            pass

        return self.create_case("fetchUsersAndComments", setup, run, teardown)

    def fetchHotPostsInSub(self):
        def setup(inner_self):
            pass

        def run(inner_self):
            pass

        def teardown(inner_self):
            pass

        return self.create_case("fetchHotPostsInSub", setup, run, teardown)

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

    def fetchMapLength(self):
        def setup(inner_self):
            pass

        def run(inner_self):
            pass

        def teardown(inner_self):
            pass

        return self.create_case("fetchMapLength", setup, run, teardown)

    def unparticipate(self):
        def setup(inner_self):
            pass

        def run(inner_self):
            pass

        def teardown(inner_self):
            pass

        return self.create_case("unparticipate", setup, run, teardown)

    def updateCoords(self):
        def setup(inner_self):
            pass

        def run(inner_self):
            pass

        def teardown(inner_self):
            pass

        return self.create_case("updateCoords", setup, run, teardown)

    def fetchPostLength(self):
        def setup(inner_self):
            pass

        def run(inner_self):
            pass

        def teardown(inner_self):
            pass

        return self.create_case("fetchPostLength", setup, run, teardown)

    def fetchCommentedPosts(self):
        def setup(inner_self):
            pass

        def run(inner_self):
            pass

        def teardown(inner_self):
            pass

        return self.create_case("fetchCommentedPosts", setup, run, teardown)

    def upvote(self):
        def setup(inner_self):
            pass

        def run(inner_self):
            pass

        def teardown(inner_self):
            pass

        return self.create_case("upvote", setup, run, teardown)

    def updateRace(self):
        def setup(inner_self):
            pass

        def run(inner_self):
            pass

        def teardown(inner_self):
            pass

        return self.create_case("updateRace", setup, run, teardown)

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
            pass

        def run(inner_self):
            pass

        def teardown(inner_self):
            pass

        return self.create_case("removeCoords", setup, run, teardown)

    def removeRace(self):
        def setup(inner_self):
            pass

        def run(inner_self):
            pass

        def teardown(inner_self):
            pass

        return self.create_case("removeRace", setup, run, teardown)

    def insertMaps(self):
        def setup(inner_self):
            pass

        def run(inner_self):
            pass

        def teardown(inner_self):
            pass

        return self.create_case("insertMaps", setup, run, teardown)
