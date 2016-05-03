import datetime
import random

from couchdb import client

from cases import Base


class Couch(Base):
    # connect to authenticated graph database
    server = client.Server(url="http://admin:mysecretpassword@46.101.118.239:5984")
    db = server['raceone'] if ('raceone' in server) else server.create('raceone')

    ####################################
    ####	DATA INITIALIZATION		####
    ####################################

    def initRaceOne(self):
        users = []
        organizers = []
        for x in range(50):
            # User
            name = "user_" + str(x)
            user = dict(
                type='USER',
                fullname=name,
                password="SuperHash")
            self.db[name] = user
            users.append(user)

            # Organizer
            name = "organizer_" + str(x)
            organizer = dict(
                type='ORGANIZER',
                username=name,
                password="SuperHash",
                email="mail@mail.se")
            self.db[name] = organizer
            organizers.append(organizer)

        for x in range(10):
            name = "event_" + str(x)
            event = dict(
                type='EVENT',
                name=name,
                logoURL='google.se/img.png',
                created_by='<<user_id>>')
            self.db[name] = event
            for y in range(5):
                nbr = x * 5 + y
                name = "race_" + str(nbr)
                race = dict(
                    type='RACE',
                    name=name,
                    event_id='',
                    description="A nice race to participate in.",
                    date="2016-06-13",
                    maxDuration=x + y if nbr < 24 else 24,
                    preview="linktoimage.png",
                    location="Gothenburg, Sweden",
                    logoURL="google.se/img.png")
                self.db[name] = race

        '''
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
                for z in range(random.randint(0, 5)):
                    # Participants
                    rand = self.new_rand_int(rands, 0, 49)

                    rands.append(rand)
                    activity = Node("ACTIVITY",
                                    joinedAt="2016-08-08")
                    tx.create(activity)
                    tx.create(Path(users[rand], "PARTICIPATING_IN", activity, "OF", race))

        tx.commit()
        '''

    def initSkim(self):
        pass
        """
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
            tx.create(Relationship(project, "COLLABORATOR", users[x * 2]))
            tx.create(Relationship(project, "COLLABORATOR", users[x * 3]))
            tx.create(Relationship(project, "COLLABORATOR", users[x * 4]))
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
        """

    def initReddit(self):
        pass

    def clearData(self):
        if 'raceone' in self.server:
            del self.server['raceone']
            self.db = self.server.create('raceone')

        # Dangerous
        #
        # if self.server['skim']:
        #    del self.server['skim']
        # if self.server['reddit']:
        #    del self.server['reddit']

    ############################
    ####	TEST METHODS	####
    ############################
    # TODO: All inserting methods should first find the nodes that it is relating for

    # SKIM
    def fetchSKU(self):
        map_fun = """
                function(doc) {
                    if (doc.type === 'RACE') {
                        emit([doc.type, doc.maxDuration], 1);
                    }
                }
                """
        reduce_fun = """
                function(keys, values) {
                    return sum(values);
                }
                """
        for row in self.db.query(map_fun, reduce_fun=reduce_fun, group=True):
            print(row)

        '''
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

        return self.create_case("fetchSKU", setup, run, teardown)'''

    def fetchUsers(self):
        def setup(inner_self):
            pass

        def run(inner_self):
            map_fun = """
                    function(doc) {
                        if (doc.type === 'USER') {
                            emit(doc.name, doc);
                        }
                    }
                    """
            for row in self.db.query(map_fun):
                print(row)

        def teardown(inner_self):
            pass

        return self.create_case("fetchUsers", setup, run, teardown)

    def commentOnImage(self):
        '''
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

        return self.create_case("commentOnImage", setup, run, teardown)'''

    def pairImageSKU(self):
        '''
        def setup(inner_self):
            # print("Setup")
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

        return self.create_case("pairImageSKU", setup, run, teardown)'''

    # RaceOne
    def follow(self):
        '''
        self.graph.run(
            'MATCH (user:USER),(race:RACE) '
            'WITH * LIMIT 1 '
            'CREATE UNIQUE (user)-[:PARTICIPATING_IN]->'
            '(activity:ACTIVITY {joinedAt:"2015-03-02@13:37"} )-[:OF]->(race) '
            'RETURN ID(activity)'
        ).dump()'''

    def unfollow(self):
        '''
        self.graph.run(
            'MATCH (:USER)-[:PARTICIPATING_IN]->(activity:ACTIVITY)-[:OF]->(:RACE) '
            'WHERE ID(activity) = 6168 '
            'WITH * LIMIT 1 '
            'DETACH DELETE activity '
            'RETURN count(*)'
        ).dump()'''

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
