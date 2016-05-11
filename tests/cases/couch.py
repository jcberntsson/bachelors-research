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
        for x in range(100):
            # User
            user = dict(
                type='USER',
                fullname="user_" + str(x),
                password="SuperHash")
            user_id, user_rev = self.db.save(user)
            users.append(user_id)

            # Organizer
            organizer = dict(
                type='ORGANIZER',
                username="organizer_" + str(x),
                password="SuperHash",
                email="mail@mail.se")
            organizer_id, organizer_rev = self.db.save(organizer)
            organizers.append(organizer_id)

        for x in range(10):
            event = dict(
                type='EVENT',
                name="event_" + str(x),
                logoURL='google.se/img.png',
                created_by=organizers[x * 5])
            event_id, event_rev = self.db.save(event)
            for y in range(5):
                nbr = x * 5 + y
                # Coordinates
                route = []
                for i in range(100):
                    coord = dict(
                        type="COORDINATE",
                        lat=10 + i,
                        lng=11 + i,
                        alt=20 + i,
                        index=i)
                    route.append(coord)
                race = dict(
                    type='RACE',
                    name="race_" + str(nbr),
                    description="A nice race to participate in.",
                    date="2016-06-13",
                    maxDuration=x + y if nbr < 24 else 24,
                    preview="linktoimage.png",
                    location="Gothenburg, Sweden",
                    logoURL="google.se/img.png",
                    event_id=event_id,
                    route=route)
                race_id, race_dev = self.db.save(race)
                coords = []
                for i in range(50):
                    coord = dict(
                        type="COORDINATE",
                        lat=10 + i,
                        lng=11 + i,
                        alt=20 + i,
                        index=i)
                    coords.append(coord)
                rands = []
                for z in range(10):
                    # Participants
                    rand = self.new_rand_int(rands, 0, 99)
                    rands.append(rand)
                    rand2 = self.new_rand_int(rands, 0, 99)
                    rands.append(rand2)
                    # Coordinates
                    activity = dict(
                        type="ACTIVITY",
                        joinedAt="2016-08-08",
                        participant_id=users[rand],
                        coordinates=coords,
                        follower_ids=[users[rand2]],
                        race_id=race_id)
                    self.db.save(activity)

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
    def fetch(self):
        map_fun = """
            function(doc) {
                if (doc.type === 'RACE') {
                    emit(doc._id, doc);
                }
            }
        """
        reduce_fun = """
            function(keys, values) {
                return sum(values);
            }
        """
        for row in self.db.query(map_fun):#, reduce_fun=reduce_fun, group=True):
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

    def fetchSKU(self):
        def setup(inner_self):
            inner_self.sku_id = self.get_random_id('SKU')

        def run(inner_self):
            pass

        def teardown(inner_self):
            pass

        return self.create_case("fetchSKU", setup, run, teardown)

    def fetchUsers(self):
        def setup(inner_self):
            pass

        def run(inner_self):
            pass

        def teardown(inner_self):
            pass

        return self.create_case("fetchUsers", setup, run, teardown)

    def commentOnImage(self):
        def setup(inner_self):
            pass

        def run(inner_self):
            pass

        def teardown(inner_self):
            pass

        return self.create_case("commentOnImage", setup, run, teardown)

    def pairImageSKU(self):
        def setup(inner_self):
            pass

        def run(inner_self):
            pass

        def teardown(inner_self):
            pass

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
            activity = self.get_random('ACTIVITY')
            follower_id = self.get_random_id('USER')
            while follower_id in activity['follower_ids']:
                follower_id = self.get_random_id('USER')
            inner_self.activity_id = activity['_id']
            inner_self.follower_id = follower_id
            #print("Act_id: %s, Follower_id: %s" % (inner_self.activity_id, inner_self.follower_id))

        def run(inner_self):
            activity = self.db.get(inner_self.activity_id)
            activity['follower_ids'].append(inner_self.follower_id)
            self.db.save(activity)
            activity['coordinates'] = []
            #print(activity)

        def teardown(inner_self):
            activity = self.db.get(inner_self.activity_id)
            activity['follower_ids'].remove(inner_self.follower_id)
            self.db.save(activity)
            activity['coordinates'] = []
            #print(activity)

        return self.create_case("follow", setup, run, teardown)

    def unfollow(self):
        def setup(inner_self):
            activity = self.get_random('ACTIVITY')
            inner_self.follower_id = self.get_random_of(activity['follower_ids'])
            inner_self.activity_id = activity['_id']
            # print("Act_id: %s, Follower_id: %s" % (inner_self.activity_id, inner_self.follower_id))

        def run(inner_self):
            activity = self.db.get(inner_self.activity_id)
            activity['follower_ids'].remove(inner_self.follower_id)
            self.db.save(activity)
            activity['coordinates'] = []
            # print(activity)

        def teardown(inner_self):
            activity = self.db.get(inner_self.activity_id)
            activity['follower_ids'].append(inner_self.follower_id)
            self.db.save(activity)
            activity['coordinates'] = []
            # print(activity)

        return self.create_case("unfollow", setup, run, teardown)

    def insertCoords(self):
        def setup(inner_self):
            activity_id = self.get_random_id('ACTIVITY')
            coords = []
            for i in range(100):
                coord = dict(
                    type="COORDINATE",
                    lat=10 + i,
                    lng=11 + i,
                    alt=20 + i,
                    index=i)
                coords.append(coord)
            inner_self.activity_id = activity_id
            inner_self.coords = coords

        def run(inner_self):
            activity = self.db.get(inner_self.activity_id)
            activity['coordinates'] = activity['coordinates'] + inner_self.coords
            self.db.save(activity)

        def teardown(inner_self):
            activity = self.db.get(inner_self.activity_id)
            for item in inner_self.coords:
                activity['coordinates'].remove(item)
            self.db.save(activity)

        return self.create_case("insertCoords", setup, run, teardown)

    def fetchParticipants(self):
        def setup(inner_self):
            pass

        def run(inner_self):
            map_func = """
                function(doc) {
                    if (doc.type === 'ACTIVITY') {
                        emit(doc.participant_id, 1);
                    }
                }
            """
            reduce_fun = "_count"
            out = self.db.query(map_func, reduce_fun=reduce_fun, group=True, limit=5, descending=True)
            for row in out:
                print(row)

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
            pass

        def teardown(inner_self):
            pass

        return self.create_case("fetchParticipants2", setup, run, teardown)

    def unparticipate(self):
        def setup(inner_self):
            activity = self.get_random('ACTIVITY')
            inner_self.participant_id = activity['participant_id']

        def run(inner_self):
            pass
            #out = self.db.view('_design/unparticipate', key=inner_self.activity_id)
            #for row in out:
                #print(row)

        def teardown(inner_self):
            pass

        return self.create_case("unparticipate", setup, run, teardown)

    def fetchCoords(self):
        def setup(inner_self):
            inner_self.activity_id = self.get_random_id('ACTIVITY')

        def run(inner_self):
            out = self.db.view('activity/coordinates', key=inner_self.activity_id)
            #for row in out:
            #    print(row)

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

    @staticmethod
    def get_random_of(items):
        from random import randint
        key = randint(0, len(items) - 1)
        return items[key]

    def get_random_id(self, entity_name):
        from random import randint
        map_fun = """
            function(doc) {
                if (doc.type === '%s') {
                    emit(doc._id, '');
                }
            }
        """ % entity_name
        result = self.db.query(map_fun)
        result_id = randint(0, result.total_rows - 1)
        return result.rows[result_id].key

    def get_random(self, entity_name):
        from random import randint
        map_fun = """
            function(doc) {
                if (doc.type === '%s') {
                    emit(doc._id, doc);
                }
            }
        """ % entity_name
        result = self.db.query(map_fun)
        if result.total_rows == 0:
            return None
        result_id = randint(0, result.total_rows - 1)
        return result.rows[result_id].value
