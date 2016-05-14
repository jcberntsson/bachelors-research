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
        tx = self.session.begin_transaction()
        print("Blobs: %s" % self.quantity_of("blob"))
        for x in range(self.quantity_of("blob")):
            tx.run(
                'CREATE (:TEST { name:{name} })',
                dict(name="hello")
            )
        tx.commit()

    def initRaceOne(self):
        session = self.session

        # Create query for race coordinates
        race_coords_query = 'START race=Node({id}) ' \
                            'CREATE (race)-[:STARTS_WITH]->(coord0:COORDINATE { lat:{lat}, lng:{lng}, alt:{alt} }) '
        for i in range(self.quantity_of("race_coordinates") - 1):
            race_coords_query += 'CREATE (%s)-[:FOLLOWED_BY]->(%s:COORDINATE { lat:{lat}, lng:{lng}, alt:{alt} }) ' % (
                "coord" + str(i), "coord" + str(i + 1))
        race_coords_query += 'CREATE (%s)-[:END_FOR]->(race)' % ("coord" + str(self.quantity_of("race_coordinates") - 1))

        # Create query for activity coordinates
        activity_coords_query = 'START race=Node({id}) ' \
                                'CREATE (race)-[:STARTS_WITH]->(coord0:COORDINATE { lat:{lat}, lng:{lng}, alt:{alt} }) '
        for i in range(self.quantity_of("activity_coordinates") - 1):
            activity_coords_query += 'CREATE (%s)-[:FOLLOWED_BY]->(%s:COORDINATE { lat:{lat}, lng:{lng}, alt:{alt} }) ' % (
                "coord" + str(i), "coord" + str(i + 1))
        activity_coords_query += 'CREATE (%s)-[:END_FOR]->(race)' % ("coord" + str(self.quantity_of("activity_coordinates") - 1))

        user_ids = []
        print("Creating users and organizers")
        for x in range(self.quantity_of("users")):
            cursor = session.run(
                'CREATE (user:USER { username:{username}, fullname:{fullname}, password:{password} }) '
                'RETURN ID(user) AS user_id',
                dict(username="user_%d" % x, fullname="Tester", password="SuperHash")
            )
            user_id = self.evaluate(cursor, "user_id")
            user_ids.append(user_id)
        organizer_ids = []
        for x in range(self.quantity_of("organizers")):
            cursor = session.run(
                'CREATE (organizer:ORGANIZER { username:{username}, fullname:{fullname}, password:{password}, email:{email} }) '
                'RETURN ID(organizer) AS organizer_id',
                dict(username="organizer_%d" % x, fullname="Tester", password="SuperHash", email="mail@mail.se")
            )
            organizer_id = self.evaluate(cursor, "organizer_id")
            organizer_ids.append(organizer_id)

        print("Creating events")
        for x in range(self.quantity_of("events")):
            event_cursor = session.run(
                'START organizer=Node({organizer_id}) '
                'CREATE (event:EVENT { name:{name},logoURL:{logoURL} })-[:MADE_BY]->(organizer) '
                'RETURN ID(event) AS event_id',
                dict(organizer_id=organizer_ids[x], name="event_name", logoURL="google.se/img.png")
            )
            event_id = self.evaluate(event_cursor, "event_id")
            for y in range(self.quantity_of("races")):
                race_cursor = session.run(
                    'START event=Node({event_id}) '
                    'CREATE (race:RACE '
                    '   {name:{name},'
                    '   description:{description},'
                    '   date:{date},'
                    '   maxDuration:{maxDuration},'
                    '   preview:{preview},'
                    '   location:{location},'
                    '   logoURL:{logoURL}})-[:IN]->(event) '
                    'RETURN ID(race) AS race_id',
                    dict(event_id=event_id, name="race_name", description="A nice race to participate in.",
                         date="2016-06-13",
                         maxDuration=3, preview="linktoimage.png", location="Gothenburg, Sweden",
                         logoURL="google.se/img.png")
                )
                race_id = self.evaluate(race_cursor, "race_id")
                session.run(race_coords_query, dict(id=race_id, lat=33, lng=44, alt=23))

                # Participants and Followers
                rands = []
                for z in range(self.quantity_of("activities")):
                    rand = self.new_rand_int(rands, 0, len(user_ids) - 2)
                    rands.append(rand)
                    activity_cursor = session.run(
                        'START participant=Node({participant_id}), follower=Node({follower_id}), race=Node({race_id}) '
                        'CREATE (participant)-[:PARTICIPATING_IN]->(activity:ACTIVITY {joinedAt:{joinedAt}})-[:OF]->(race) '
                        'CREATE (activity)<-[:FOLLOWING]-(follower) '
                        'RETURN ID(activity) AS activity_id',
                        dict(participant_id=user_ids[rand], follower_id=user_ids[rand + 1], race_id=race_id,
                             joinedAt="2016-05-05")
                    )
                    activity_id = self.evaluate(activity_cursor, "activity_id")
                    session.run(activity_coords_query, dict(id=activity_id, lat=33, lng=44, alt=23))
            print("Event done")

    def initSkim(self):
        session = self.session

        user_ids = []
        print("Creating users")
        for x in range(self.quantity_of("users")):
            cursor = session.run(
                'CREATE (user:USER { username:{username}, email:{email}, password:{password} }) '
                'RETURN ID(user) AS user_id',
                dict(username="user_%d" % x, email="mail@mail.se", password="SuperHash")
            )
            user_id = self.evaluate(cursor, "user_id")
            user_ids.append(user_id)

        print("Creating projects")
        print("Projects: %s" % self.quantity_of("projects"))
        for x in range(self.quantity_of("projects")):
            project_cursor = session.run(
                'CREATE (project:PROJECT { name:{name} }) '
                'RETURN ID(project) AS project_id',
                dict(name="project_%d" % x)
            )
            project_id = self.evaluate(project_cursor, "project_id")
            collaborator_ids = []
            tx = session.begin_transaction()
            for y in range(self.quantity_of("collaborators")):
                user_id = user_ids[(x + 1) * y]
                tx.run(
                    'START project=Node({project_id}), collaborator=Node({user_id}) '
                    'CREATE (project)-[:COLLABORATOR]->(collaborator)',
                    dict(project_id=project_id, user_id=user_id)
                )
                collaborator_ids.append(user_id)
            tx.commit()
            for y in range(self.quantity_of("project_images")):
                image_cursor = session.run(
                    'START project=Node({project_id}) '
                    'CREATE (image:IMAGE {'
                    '   name:{name},'
                    '   originalName:{originalName},'
                    '   extension:{extension},'
                    '   encoding:{encoding},'
                    '   size:{size},'
                    '   height:{height},'
                    '   width:{width},'
                    '   verticalDPI:{verticalDPI},'
                    '   horizontalDPI:{horizontalDPI},'
                    '   bitDepth:{bitDepth},'
                    '   createdAt:{createdAt},'
                    '   accepted:{accepted}})-[:IN]->(project) '
                    'RETURN ID(image) AS image_id',
                    dict(project_id=project_id, name="image_name", originalName="original_name", extension="jpg",
                         encoding="PNG/SFF", size=1024, height=1080, width=720, verticalDPI=40, horizontalDPI=50,
                         bitDepth=15, createdAt="2016-03-03", accepted=False)
                )
                image_id = self.evaluate(image_cursor, "image_id")
                tx = session.begin_transaction()
                for z in range(self.quantity_of("image_comments")):
                    tx.run(
                        'START image=Node({image_id}), user=Node({user_id}) '
                        'CREATE (user)<-[:MADE_BY]-(:COMMENT { text:{text}, createdAt:{createdAt} })-[:ON]->(image)',
                        dict(image_id=image_id, user_id=self.get_random_of(collaborator_ids), text="Ha-Ha, cool image!",
                             createdAt="2016-05-11")
                    )
                tx.commit()
            for y in range(self.quantity_of("skus")):
                sku_cursor = session.run(
                    'START project=Node({project_id}) '
                    'CREATE (sku:SKU { name:{name} })-[:IN]->(project) '
                    'RETURN ID(sku) AS sku_id',
                    dict(project_id=project_id, name="sku_name")
                )
                sku_id = self.evaluate(sku_cursor, "sku_id")
                tx = session.begin_transaction()
                for z in range(self.quantity_of("sku_values")):
                    tx.run(
                        'START sku=Node({sku_id}) '
                        'CREATE (:SKU_VALUE { header:{header}, value:{value} })-[:IN]->(sku)',
                        dict(sku_id=sku_id, header="header_%d" % z, value=z)
                    )
                tx.commit()
                for z in range(self.quantity_of("sku_images")):
                    image_cursor = session.run(
                        'START sku=Node({sku_id}) '
                        'CREATE (image:IMAGE {'
                        '   name:{name},'
                        '   originalName:{originalName},'
                        '   extension:{extension},'
                        '   encoding:{encoding},'
                        '   size:{size},'
                        '   height:{height},'
                        '   width:{width},'
                        '   verticalDPI:{verticalDPI},'
                        '   horizontalDPI:{horizontalDPI},'
                        '   bitDepth:{bitDepth},'
                        '   createdAt:{createdAt},'
                        '   accepted:{accepted}})-[:IN]->(sku) '
                        'RETURN ID(image) AS image_id',
                        dict(sku_id=sku_id, name="image_name", originalName="original_name", extension="jpg",
                             encoding="PNG/SFF", size=1024, height=1080, width=720, verticalDPI=40, horizontalDPI=50,
                             bitDepth=15, createdAt="2016-03-03", accepted=False)
                    )
                    image_id = self.evaluate(image_cursor, "image_id")
                    tx = session.begin_transaction()
                    for a in range(self.quantity_of("image_comments")):
                        tx.run(
                            'START image=Node({image_id}), user=Node({user_id}) '
                            'CREATE (user)<-[:MADE_BY]-(:COMMENT { text:{text}, createdAt:{createdAt} })-[:ON]->(image)',
                            dict(image_id=image_id, user_id=self.get_random_of(collaborator_ids),
                                 text="Ha-Ha, cool image!", createdAt="2016-05-11")
                        )
                    tx.commit()
            print("Project done")

    def clearData(self):
        # Dangerous
        cursor = self.session.run(
            'MATCH (n) '
            'DETACH DELETE n '
            'RETURN COUNT(*) AS deleted'
        )
        deleted = self.evaluate(cursor, "deleted")
        print("Cleared %s nodes and relationships" % deleted)

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
                'START sku=Node({sku_id}) '
                'MATCH (value:SKU_VALUE)-[of:OF]->(sku:SKU) '
                'RETURN value',
                dict(sku_id=inner_self.sku_id)
            )
            sku = list(out)

        def teardown(inner_self):
            pass

        return self.create_case("fetchSKU", setup, run, teardown)

    def fetchUsers(self):
        def setup(inner_self):
            pass

        def run(inner_self):
            out = self.session.run(
                'MATCH (user:USER) '
                'RETURN user'
            )
            users = list(out)

        def teardown(inner_self):
            pass

        return self.create_case("fetchUsers", setup, run, teardown)

    def commentOnImage(self):
        def setup(inner_self):
            inner_self.project_id = self.get_random_id('PROJECT')
            cursor = self.session.run(
                'START project=Node({project_id}) '
                'MATCH (image:IMAGE)-[:IN]->(project:PROJECT)-[:COLLABORATOR]->(user:USER) '
                'RETURN ID(image) AS image_id, ID(user) AS user_id '
                'LIMIT 1',
                dict(project_id=inner_self.project_id)
            )
            info = self.first_of(cursor)
            inner_self.user_id = info['user_id']
            inner_self.image_id = info['image_id']

        def run(inner_self):
            comment_cursor = self.session.run(
                'START user=Node({user_id}), project=Node({project_id}), image=Node({image_id}) '
                'MATCH (user:USER)<-[:COLLABORATOR]-(project:PROJECT)<-[:IN]-(image:IMAGE) '
                'CREATE (image)<-[:ON]-(comment:COMMENT {text:{text}, createdAt:{createdAt}} )-[:MADE_BY]->(user) '
                'RETURN ID(comment) AS comment_id',
                dict(user_id=inner_self.user_id, project_id=inner_self.project_id, image_id=inner_self.image_id,
                     text="Ooh, another new comment!", createdAt="2015-03-02@13:37")
            )
            inner_self.comment_id = self.evaluate(comment_cursor, "comment_id")

        def teardown(inner_self):
            self.session.run(
                'START comment=Node({comment_id}) '
                'DETACH DELETE comment '
                'RETURN count(*) AS deleted_nodes',
                dict(comment_id=inner_self.comment_id)
            )

        return self.create_case("commentOnImage", setup, run, teardown)

    def pairImageSKU(self):
        def setup(inner_self):
            inner_self.project_id = self.get_random_id('PROJECT')
            cursor = self.session.run(
                'START project=Node({project_id}) '
                'MATCH (image:IMAGE)-[:IN]->(project:PROJECT)<-[:IN]-(sku:SKU) '
                'RETURN ID(sku) AS sku_id, ID(image) AS image_id '
                'LIMIT 1',
                dict(project_id=inner_self.project_id)
            )
            result = self.first_of(cursor)
            inner_self.sku_id = result['sku_id']
            inner_self.image_id = result['image_id']

        def run(inner_self):
            cursor = self.session.run(
                'START sku=Node({sku_id}), project=Node({project_id}), image=Node({image_id}) '
                'MATCH (sku:SKU)-[:IN]->(project:PROJECT)<-[in:IN]-(image:IMAGE) '
                'CREATE (image)-[b:BELONGS_TO]->(sku) '
                'DELETE in '
                'RETURN ID(b) AS id',
                dict(sku_id=inner_self.sku_id, project_id=inner_self.project_id, image_id=inner_self.image_id)
            )
            inner_self.belong_id = self.evaluate(cursor, "id")

        def teardown(inner_self):
            self.session.run(
                'START sku=Node({sku_id}), project=Node({project_id}), image=Node({image_id}) '
                'MATCH (image:IMAGE)-[b:BELONGS_TO]->(sku:SKU)-[in:IN]->(project:PROJECT) '
                'DELETE b '
                'CREATE (image)-[:IN]->(project) '
                'RETURN count(*) AS deleted_rows',
                dict(sku_id=inner_self.sku_id, project_id=inner_self.project_id, image_id=inner_self.image_id)
            )

        return self.create_case("pairImageSKU", setup, run, teardown)

    def addRowsToSKU(self):
        def setup(inner_self):
            inner_self.project_id = self.get_random_id('PROJECT')

        def run(inner_self):
            tx = self.session.begin_transaction()
            for i in range(10):
                tx.run(
                    'START project=Node({project_id}) '
                    'CREATE (sku:SKU { name: {header}})-[:IN]->(project) '
                    'CREATE (:SKU_VALUE { header: {header}, value:{value} })-[:OF]->(sku) '
                    'CREATE (:SKU_VALUE { header: {header}, value:{value} })-[:OF]->(sku) '
                    'CREATE (:SKU_VALUE { header: {header}, value:{value} })-[:OF]->(sku) '
                    'CREATE (:SKU_VALUE { header: {header}, value:{value} })-[:OF]->(sku)',
                    dict(project_id=inner_self.project_id, header="remove_me", value="120")
                )
            tx.commit()

        def teardown(inner_self):
            self.session.run(
                'START project=Node({project_id}) '
                'MATCH (sku:SKU)-[in:IN]->(project) '
                'WHERE sku.name={header} '
                'MATCH (sku)<-[of:OF]-(value:SKU_VALUE) '
                'WHERE value.header={header} '
                'DELETE of, value, in, sku '
                'RETURN COUNT(*) AS deleted_rows',
                dict(project_id=inner_self.project_id, header="remove_me")
            )

        return self.create_case("addRowsToSKU", setup, run, teardown)

    def fetchAllUserComments(self):
        def setup(inner_self):
            inner_self.user_id = self.get_random_id('USER')

        def run(inner_self):
            out = self.session.run(
                'START user=Node({user_id}) '
                'MATCH (comment:COMMENT)-[:MADE_BY]->(user:USER) '
                'RETURN comment',
                dict(user_id=inner_self.user_id)
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
                'START race=Node({race_id}), user=Node({user_id}) '
                'CREATE (user)-[:PARTICIPATING_IN]->(activity:ACTIVITY { joinedAt:{joinedAt} })-[:OF]->(race) '
                'RETURN ID(activity) AS activity_id',
                dict(race_id=race_id, user_id=participant_id, joinedAt="2016-05-03")
            )
            inner_self.activity_id = self.evaluate(cursor, "activity_id")

        def run(inner_self):
            cursor = self.session.run(
                'START activity=Node({activity_id}), follower=Node({user_id}) '
                'CREATE (follower)-[f:FOLLOWING]->(activity) '
                'RETURN follower,f,activity',
                dict(activity_id=inner_self.activity_id, user_id=inner_self.follower_id)
            )
            result = list(cursor)

        def teardown(inner_self):
            cursor = self.session.run(
                'START activity=Node({activity_id}), follower=Node({user_id}) '
                'MATCH '
                '   (follower:USER)-[following:FOLLOWING]->(activity:ACTIVITY)-[of:OF]->(:RACE),'
                '   (participant:USER)-[participating:PARTICIPATING_IN]->(activity) '
                'DELETE following, of, participating, activity '
                'RETURN COUNT(*) AS nbr_deleted',
                dict(activity_id=inner_self.activity_id, user_id=inner_self.follower_id)
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
                'START act=Node({activity_id}) '
                'MATCH (act:ACTIVITY)<-[:END_FOR]-(end:COORDINATE) '
                'RETURN ID(end) AS end_id',
                dict(activity_id=inner_self.activity_id)
            )
            inner_self.end_id = self.evaluate(cursor, "end_id")

        def run(inner_self):
            cursor = self.session.run(
                'START activity=Node({activity_id}) '
                'MATCH (coord:COORDINATE)-[end:END_FOR]->(activity:ACTIVITY) '
                'DELETE end '
                'RETURN ID(coord) AS coord',
                dict(activity_id=inner_self.activity_id)
            )
            out = self.first_of(cursor)
            prev_id = out['coord']

            for i in range(99):
                cursor = self.session.run(
                    'START prev=Node({prev_id}) '
                    'CREATE (coord:COORDINATE { lat:{lat}, lng:{lng}, alt:{alt} }) '
                    'RETURN ID(coord) AS coord_id',
                    dict(prev_id=prev_id, lat=11, lng=12, alt=33)
                )
                prev_id = self.evaluate(cursor, "coord_id")
            cursor = self.session.run(
                'START activity=Node({activity_id}), prev=Node({coord_id}) '
                'CREATE(prev)-[:END_FOR]->(activity)',
                dict(activity_id=inner_self.activity_id, coord_id=prev_id)
            )
            result = list(cursor)

            """
            query = 'START first=Node({coord_id}), activity=Node({activity_id}) ' \
                    'CREATE (first)-[:FOLLOWED_BY]->(coord0:COORDINATE { lat:10, lng:11, alt:20 }) '
            for i in range(99):
                query += ' CREATE (%s)-[:FOLLOWED_BY]->(%s:COORDINATE { lat:10, lng:11, alt:20 })' % ("coord" + str(i), "coord" + str(i + 1))
            query += ' CREATE (%s)-[:END_FOR]->(activity)' % "coord99"
            out = self.session.run(query, dict(coord_id=prev_id, activity_id=inner_self.activity_id))
            result = list(out)
            """
            """
            self.graph.run(
                'START act=Node(%d) '
                'MATCH (act)-[:STARTS_WITH|FOLLOWED_BY*]-(coord:COORDINATE) '
                'RETURN COUNT(coord)' % inner_self.activity_id
            ).dump()"""

        def teardown(inner_self):
            self.session.run(
                'START act=Node({activity_id}) '
                'MATCH (act:ACTIVITY)<-[end:END_FOR]-(coord:COORDINATE) '
                'DELETE end',
                dict(activity_id=inner_self.activity_id)
            )
            self.session.run(
                'START original_end=node({coord_id}) '
                'MATCH (original_end:COORDINATE)-[f:FOLLOWED_BY]->(o:COORDINATE) '
                'MATCH p=(o:COORDINATE)-[:FOLLOWED_BY*0..]->(o2:COORDINATE) '
                'DELETE f, p '
                'RETURN COUNT(*) AS deleted_paths',
                dict(coord_id=inner_self.end_id)
            )
            self.session.run(
                'START coord=Node({coord_id}), activity=Node({activity_id}) '
                'CREATE (coord)-[:END_FOR]->(activity)',
                dict(coord_id=inner_self.end_id, activity_id=inner_self.activity_id)
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
            cursor = self.session.run(
                'START act=Node({activity_id}) '
                'MATCH (participant:USER)-[:PARTICIPATING_IN]->(act:ACTIVITY)-[:OF]->(race:RACE) '
                'RETURN act.joinedAt AS joinedAt,ID(participant) AS participant_id,ID(race) AS race_id',
                dict(activity_id=inner_self.activity_id)
            )
            activity = self.first_of(cursor)
            inner_self.joinedAt = activity['joinedAt']
            inner_self.participant_id = activity['participant_id']
            inner_self.race_id = activity['race_id']
            followers_cursor = self.session.run(
                'START act=Node({activity_id}) '
                'MATCH (follower:USER)-[:FOLLOWING]->(act:ACTIVITY) '
                'RETURN ID(follower) AS id',
                dict(activity_id=inner_self.activity_id)
            )
            follower_ids = []
            for follower in followers_cursor:
                follower_ids.append(follower['id'])
            inner_self.follower_ids = follower_ids

        def run(inner_self):
            cursor = self.session.run(
                'START act=Node({activity_id}) '
                'DETACH DELETE act '
                'RETURN COUNT(*) AS deleted',
                dict(activity_id=inner_self.activity_id)
            )
            deleted = self.evaluate(cursor, "deleted")

        def teardown(inner_self):
            cursor = self.session.run(
                'START race=Node({race_id}), participant=Node({user_id}) '
                'CREATE (participant)-[:PARTICIPATING_IN]->(act:ACTIVITY { joinedAt:{joinedAt} })-[:OF]->(race) '
                'RETURN ID(act) AS act_id',
                dict(race_id=inner_self.race_id, user_id=inner_self.participant_id, joinedAt=inner_self.joinedAt)
            )
            out = self.first_of(cursor)
            inner_self.activity_id = out['act_id']
            tx = self.session.begin_transaction()
            for follower_id in inner_self.follower_ids:
                tx.run(
                    'STARt act=Node({activity_id}), follower=Node({follower_id}) '
                    'CREATE (follower)-[:FOLLOWING]->(act)',
                    dict(activity_id=inner_self.activity_id, follower_id=follower_id)
                )
            tx.commit()

        return self.create_case("unparticipate", setup, run, teardown)

    def fetchCoords(self):
        def setup(inner_self):
            inner_self.activity_id = self.get_random_id('ACTIVITY')

        def run(inner_self):
            cursor = self.session.run(
                'START act=Node({activity_id}) '
                'MATCH (act:ACTIVITY)-[:STARTS_WITH|FOLLOWED_BY*]-(coord:COORDINATE) '
                'RETURN coord',
                dict(activity_id=inner_self.activity_id)
            )
            coordinates = list(cursor)

        def teardown(inner_self):
            pass

        return self.create_case("fetchCoords", setup, run, teardown)

    def removeCoords(self):
        def setup(inner_self):
            inner_self.race_id = self.get_random_id('RACE')
            coordinates_cursor = self.session.run(
                'START race=Node({race_id}) '
                'MATCH '
                '   (start:COORDINATE)<-[:STARTS_WITH]-(race)<-[:END_FOR]-(end:COORDINATE), '
                '   (start)-[:FOLLOWED_BY*]->(coord:COORDINATE)-[:FOLLOWED_BY]->(ending:COORDINATE) '
                'RETURN coord',
                dict(race_id=inner_self.race_id)
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
                    'START middle=Node({coord_id}) '
                    'MATCH (first:COORDINATE)-[f1:FOLLOWED_BY]->(middle:COORDINATE)-[f2:FOLLOWED_BY]->(last:COORDINATE) '
                    'DELETE f1,f2,middle '
                    'CREATE (first)-[:FOLLOWED_BY]->(last)',
                    dict(coord_id=coord_id)
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
                'START race=Node({race_id}) '
                'MATCH '
                '   (start:COORDINATE)<-[:STARTS_WITH]-(race)<-[end_for:END_FOR]-(end:COORDINATE) '
                'DELETE end_for '
                'RETURN ID(end) AS end_id',
                dict(race_id=inner_self.race_id)
            )
            prev_id = self.evaluate(cursor, "end_id")
            for coord in inner_self.coords:
                cursor = self.session.run(
                    'START prev=Node({coord_id}) '
                    'CREATE (prev)-[:FOLLOWED_BY]->(coord:COORDINATE { lat:{lat}, lng:{lng}, alt:{alt} }) '
                    'RETURN ID(coord) AS coord_id',
                    dict(coord_id=prev_id, lat=coord['lat'], lng=coord['lng'], alt=coord['alt'])
                )
                prev_id = self.evaluate(cursor, "coord_id")
            self.session.run(
                'START prev=Node({coord_id}), race=Node({race_id}) '
                'CREATE (prev)-[:END_FOR]->(race)',
                dict(coord_id=prev_id, race_id=inner_self.race_id)
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
                'START race=Node({race_id}) '
                'MATCH (race:RACE)-[:IN]->(event:EVENT) '
                'RETURN race, event',
                dict(race_id=inner_self.race_id)
            )
            result = self.first_of(cursor)
            race = result['race'].properties
            event = result['event'].properties
            cursor = self.session.run(
                'START race=Node({race_id}) '
                'MATCH (race:RACE)-[:STARTS_WITH]->(start:COORDINATE)-[:FOLLOWED_BY*]->(coord:COORDINATE) '
                'RETURN coord',
                dict(race_id=inner_self.race_id)
            )
            coords = []
            for record in cursor:
                coords.append(record['coord'].properties)
            cursor = self.session.run(
                'START race=Node({race_id}) '
                'MATCH '
                '   (race:RACE)<-[:OF]-(act:ACTIVITY)<-[:PARTICIPATING_IN]-(user:USER),'
                '   (act:ACTIVITY)<-[:FOLLOWING]-(follower:USER) '
                'RETURN user, COUNT(follower) AS nbr_of_followers',
                dict(race_id=inner_self.race_id)
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
    def create_coords(nbr):
        if nbr <= 1:
            return ""
        query = 'START race=Node({id}) ' \
                'CREATE (race)-[:STARTS_WITH]->(coord0:COORDINATE { lat:{lat}, lng:{lng}, alt:{alt} }) '
        for i in range(nbr - 1):
            query += 'CREATE (%s)-[:FOLLOWED_BY]->(%s:COORDINATE { lat:{lat}, lng:{lng}, alt:{alt} }) ' % (
                "coord" + str(i), "coord" + str(i + 1))
        query += 'CREATE (%s)-[:END_FOR]->(race)' % ("coord" + str(nbr - 1))
        return query
        """
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
        """
