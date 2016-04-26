import datetime
import random

from py2neo import Relationship, Graph, Node, Path

from cases.base import Base


class Neo4j(Base):
    # connect to authenticated graph database
    graph = Graph("http://neo4j:kandidat@46.101.235.47:7474/db/data/")

    ####################################
    ####	DATA INITIALIZATION		####
    ####################################

    def initRaceOne(self):
        tx = self.graph.begin()

        # Users
        users = []
        for x in range(50):
            users.append(Node("USER", username="user_" + str(random.randint(1, 50))))
            tx.create(users[x])

        # Events & Races
        events = []
        races = []
        coordinates = []
        activities = []
        for x in range(10):
            events.append(Node("EVENT", name="event_" + str(x)))
            tx.create(events[x])
            for y in range(5):
                race = Node("RACE", name="race_" + str(random.randint(1, 500)))
                tx.create(race)
                races.append(race)
                tx.create(Relationship(race, "IN", events[x]))
                # Coordinates
                coord1 = Node("COORDINATE", lat=33, lng=44)
                coordinates.append(coord1)
                coord2 = Node("COORDINATE", lat=33.1, lng=44.1)
                coordinates.append(coord2)
                coord3 = Node("COORDINATE", lat=33.2, lng=44.2)
                coordinates.append(coord3)
                tx.create(
                    Path(race, "STARTS_AT", coord1, "FOLLOWED_BY", coord2, "FOLLOWED_BY", coord3, "END_FOR", race))

                rands = []
                for z in range(random.randint(0, 5)):
                    # Participants
                    rand = self.new_rand_int(rands, 0, 49)

                    rands.append(rand)
                    activity = Node("ACTIVITY", joinedAt=str(datetime.datetime.now()))
                    tx.create(activity)
                    activities.append(activity)
                    tx.create(Path(users[rand], "PARTICIPATING_IN", activity, "OF", race))

            tx.create(Relationship(events[x], "MADE_BY", users[x * 5]))

        tx.commit()

    def initSkim(self):
        tx = self.graph.begin()

        # Users
        users = []
        for x in range(50):
            users.append(
                Node("USER", username="user_" + str(x), email="user_" + str(x) + "@mail.com"))
            tx.create(users[x])

        # Projects and images
        projects = []
        images = []
        skus = []
        comments = []
        for x in range(8):
            projects.append(Node("PROJECT", name="project_" + str(x)))
            tx.create(projects[x])
            tx.create(Relationship(projects[x], "COLLABORATOR", users[x * 2]))
            tx.create(Relationship(projects[x], "COLLABORATOR", users[x * 3]))
            tx.create(Relationship(projects[x], "COLLABORATOR", users[x * 4]))
            for y in range(4):
                # Images
                nbr = x + 5 + y
                image = Node("IMAGE", name="image_" + str(nbr), height="100", width="100", extension="png",
                             createdAt=str(datetime.datetime.now()))
                tx.create(image)
                images.append(image)
                tx.create(Relationship(image, "IN", projects[x]))
                # Inner images
                nbr = x + 5 + y
                image = Node("IMAGE", name="innerimage_" + str(nbr), height="100", width="100", extension="png",
                             createdAt=str(datetime.datetime.now()))
                tx.create(image)
                images.append(image)
                for z in range(2):
                    # Comments
                    comment = Node("COMMENT", text="Haha, cool image", createdAt=str(datetime.datetime.now()))
                    tx.create(comment)
                    comments.append(comment)
                    tx.create(Relationship(comment, "ON", image))
                    tx.create(Relationship(comment, "MADE_BY", users[x * 2]))
                # SKUS
                sku = Node("SKU", name="sku_" + str(nbr))
                tx.create(sku)
                skus.append(sku)
                tx.create(Relationship(sku, "IN", projects[x]))
                tx.create(Relationship(image, "BELONGS_TO", sku))
                for z in range(5):
                    # Rows
                    row = Node("ROW", header="header_" + str(z), value=str(z))
                    tx.create(row)
                    tx.create(Relationship(row, "OF", sku))

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
        self.graph.run(
            'MATCH (row:ROW)-[of:OF]->(sku:SKU) WHERE sku.name="sku_7" RETURN sku,of,row'
        ).dump()

    def fetchUsers(self):
        self.graph.run(
            'MATCH (user:USER) RETURN user'
        ).dump()

    def commentOnImage(self):
        # TODO: Maybe better to create all three in one create?
        self.graph.run(
            'MATCH (user:USER)<-[:COLLABORATOR]-(:PROJECT)<-[:IN]-(image:IMAGE) '
            'WITH * LIMIT 1 '
            'CREATE (image)<-[:ON]-(comment:COMMENT {text:"Ooh, another new comment!", createdAt:"2015-03-02@13:37"} )-[:MADE_BY]->(user) '
            'RETURN comment'
        ).dump()

    def pairImageSKU(self):

        def setup(inner_self):
            #print("Setup")
            out = self.graph.run(
                'CREATE (sku:SKU { name: "test_sku" })-[:IN]->(project:PROJECT { name: "test_project" })<-[in:IN]-(image:IMAGE { name:"test_image" }) '
                'RETURN ID(sku) AS sku_id, ID(project) AS project_id, ID(image) AS image_id'
            )
            if out.forward():
                #print(out.current)
                inner_self.sku_id = out.current['sku_id']
                inner_self.project_id = out.current['project_id']
                inner_self.image_id = out.current['image_id']

        def run(inner_self):
            #print("Run")
            self.graph.run(
                'MATCH (sku:SKU)-[:IN]->(project:PROJECT)<-[in:IN]-(image:IMAGE)'
                'WHERE ID(sku)=%d AND ID(project)=%d AND ID(image)=%d '
                'CREATE (image)-[b:BELONGS_TO]->(sku) '
                'DELETE in '
                'RETURN ID(b) AS ID' % (inner_self.sku_id, inner_self.project_id, inner_self.image_id)
            )#.dump()

        def teardown(inner_self):
            #print("Teardown")
            self.graph.run(
                'MATCH (image:IMAGE)-[b:BELONGS_TO]->(sku:SKU)-[in:IN]->(project:PROJECT) '
                'WHERE ID(sku)=%d AND ID(project)=%d AND ID(image)=%d '
                'DELETE b, image, in, sku, project '
                'RETURN count(*) AS deleted_rows' % (inner_self.sku_id, inner_self.project_id, inner_self.image_id)
            )#.dump()

        return self.create_case("PairImageAndSKUCase", setup, run, teardown)

        '''
        rel = self.graph.run(
            'MATCH (sku:SKU)-[:IN]->(project:PROJECT)<-[in:IN]-(image:IMAGE)'
            'WITH * SKIP 55 LIMIT 1 '
            'CREATE (image)-[b:BELONGS_TO]->(sku) '
            'DELETE in '
            'RETURN ID(b) AS ID'
        )

        relation_id = str(rel.evaluate())

        self.graph.run(
            'MATCH (image:IMAGE)-[b:BELONGS_TO]->(:SKU)-[:IN]->(project:PROJECT)'
            'WHERE ID(b) = ' + relation_id + ' '
            'CREATE (image)-[:IN]->(project) '
            'DELETE b '
            'RETURN count(*)'
        ).dump()
        '''

    # RaceOne
    def follow(self):
        self.graph.run(
            'MATCH (user:USER),(race:RACE) '
            'WITH * LIMIT 1 '
            'CREATE UNIQUE (user)-[:PARTICIPATING_IN]->'
            '(activity:ACTIVITY {joinedAt:"2015-03-02@13:37"} )-[:OF]->(race) '
            'RETURN ID(activity)'
        ).dump()

    def unfollow(self):
        self.graph.run(
            'MATCH (:USER)-[:PARTICIPATING_IN]->(activity:ACTIVITY)-[:OF]->(:RACE) '
            'WHERE ID(activity) = 6168 '
            'WITH * LIMIT 1 '
            'DETACH DELETE activity '
            'RETURN count(*)'
        ).dump()

    def fetchComments(self):
        pass

    def fetchHotPosts(self):
        pass

    def insertCoords(self):
        pass

    def fetchParticipants(self):
        pass

    def createComment(self):
        pass

    def fetchBestFriend(self):
        pass

    def fetchUsersAndComments(self):
        pass

    def fetchHotPostsInSub(self):
        pass

    def duplicateEvent(self):
        pass

    def fetchParticipants2(self):
        pass

    def fetchMapLength(self):
        pass

    def unparticipate(self):
        pass

    def updateCoords(self):
        pass

    def fetchPostLength(self):
        pass

    def fetchCommentedPosts(self):
        pass

    def upvote(self):
        pass

    def updateRace(self):
        pass

    def fetchCoords(self):
        pass

    def removeCoords(self):
        pass

    def removeRace(self):
        pass

    def insertMaps(self):
        pass

if __name__ == '__main__':
    neo = Neo4j()
    #neo.init("skim")
    # clearData()
    # initData("raceone")
    # dump(fetchUsers())
    testCase = neo.pairImageSKU()
    testCase.setup()
    testCase.run()
    testCase.teardown()
