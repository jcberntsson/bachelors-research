import datetime
import random

import mysql.connector

from cases.base import Base


class MySQL(Base):
    # connect to authenticated graph database
    cnx = mysql.connector.connect(user='vagrant', password='vagrant', host='46.101.234.110', database='research')
    
    ####################################
    ####	DATA INITIALIZATION		####
    ####################################

    def initRaceOne(self):
        ##Drop old tables
        cursor = self.cnx.cursor()
        cursor.execute("DROP TABLE activity;DROP TABLE participant;DROP TABLE tag;DROP TABLE racegroup;DROP TABLE racemap;DROP TABLE race;DROP TABLE eventmap;DROP TABLE event;DROP TABLE racemap;DROP TABLE raceprofile;DROP TABLE point;DROP TABLE map;DROP TABLE category;DROP TABLE organizer;",multi=True)
        self.cnx.commit()
        ##Create tables
        print("creating tables")
        cursor = self.cnx.cursor()
        TABLES = {}
        TABLES['organizer'] = (
            "CREATE TABLE organizer ("
            "  id bigint NOT NULL AUTO_INCREMENT,"
            "  username varchar(20),"
            "  fullname varchar(50),"
            "  password varchar(20),"
            "  email varchar(50),"
            "  organization varchar(50),"
            "  phone varchar(20),"
            "  url varchar(50),"
            "  PRIMARY KEY (id)"
            ") ENGINE=InnoDB")
        TABLES['event'] = (
            "CREATE TABLE event ("
            "  id bigint NOT NULL AUTO_INCREMENT,"
            "  name varchar(50),"
            "  logoUrl varchar(50),"
            "  organizer_id bigint,"
            "  successor_id bigint,"
            "  predecessor_id bigint,"
            "  preview varchar(50),"
            "  PRIMARY KEY (id),"
            "  CONSTRAINT event_organizer_fk FOREIGN KEY (organizer_id) "
            "     REFERENCES organizer (id) ON DELETE CASCADE,"
            "  CONSTRAINT event_successor_fk FOREIGN KEY (successor_id) "
            "     REFERENCES event (id) ON DELETE CASCADE,"
            "  CONSTRAINT event_predecessor_fk FOREIGN KEY (predecessor_id) "
            "     REFERENCES event (id) ON DELETE CASCADE"
            ") ENGINE=InnoDB")
        TABLES['category'] = (
            "CREATE TABLE category ("
            "  id int NOT NULL AUTO_INCREMENT,"
            "  name varchar(50),"
            "  PRIMARY KEY (id)"
            ") ENGINE=InnoDB")
        TABLES['raceprofile'] = (
            "CREATE TABLE raceprofile ("
            "  id int NOT NULL AUTO_INCREMENT,"
            "  name varchar(50),"
            "  PRIMARY KEY (id)"
            ") ENGINE=InnoDB")
        TABLES['map'] = (
            "CREATE TABLE map ("
            "  id bigint NOT NULL AUTO_INCREMENT,"
            "  name varchar(50),"
            "  PRIMARY KEY (id)"
            ") ENGINE=InnoDB")
        TABLES['point'] = (
            "CREATE TABLE point ("
            "  id bigint NOT NULL AUTO_INCREMENT,"
            "  lat DECIMAL(11, 8),"
            "  lng DECIMAL(11, 8),"
            "  alt DECIMAL(11, 8),"
            "  map bigint,"
            "  PRIMARY KEY (id),"
            "  CONSTRAINT point_map_fk FOREIGN KEY (map) "
            "     REFERENCES map (id)"
            ") ENGINE=InnoDB")
        TABLES['racemap'] = (
            "CREATE TABLE racemap ("
            "  id bigint NOT NULL AUTO_INCREMENT,"
            "  map bigint,"
            "  start_point bigint,"
            "  goal_point bigint,"
            "  PRIMARY KEY (id),"
            "  CONSTRAINT racemap_map_fk FOREIGN KEY (map) "
            "     REFERENCES map (id),"
            "  CONSTRAINT racemap_startpoint_fk FOREIGN KEY (start_point) "
            "     REFERENCES point (id),"
            "  CONSTRAINT racemap_goalpoint_fk FOREIGN KEY (goal_point) "
            "     REFERENCES point (id)"
            ") ENGINE=InnoDB")
        TABLES['eventmap'] = (
            "CREATE TABLE eventmap ("
            "  id bigint NOT NULL AUTO_INCREMENT,"
            "  map bigint,"
            "  event bigint,"
            "  PRIMARY KEY (id),"
            "  CONSTRAINT eventmap_map_fk FOREIGN KEY (map) "
            "     REFERENCES map (id),"
            "  CONSTRAINT eventmap_event_fk FOREIGN KEY (event) "
            "     REFERENCES event (id)"
            ") ENGINE=InnoDB")
        TABLES['participant'] = (
            "CREATE TABLE participant ("
            "  id bigint NOT NULL AUTO_INCREMENT,"
            "  username varchar(50),"
            "  PRIMARY KEY (id)"
            ") ENGINE=InnoDB")
        TABLES['activity'] = (
            "CREATE TABLE activity ("
            "  id int NOT NULL AUTO_INCREMENT,"
            "  participant bigint,"
            "  race bigint,"
            "  joinedAt datetime,"
            "  PRIMARY KEY (id),"
            "  CONSTRAINT activity_participant_fk FOREIGN KEY (participant) "
            "     REFERENCES participant (id),"
            "  CONSTRAINT activity_race_fk FOREIGN KEY (race) "
            "     REFERENCES race (id)"
            ") ENGINE=InnoDB")
        TABLES['race'] = (
            "CREATE TABLE race ("
            "  id bigint NOT NULL AUTO_INCREMENT,"
            "  name varchar(50),"
            "  description varchar(200),"
            "  race_date datetime,"
            "  max_duration int,"
            "  preview varchar(50),"
            "  location varchar(50),"
            "  logo_url varchar(50),"
            "  event_id bigint,"
            "  map_id bigint,"
            "  raceprofile int,"
            "  category int,"
            "  PRIMARY KEY (id),"
            "  CONSTRAINT race_category_fk FOREIGN KEY (category) "
            "     REFERENCES category (id),"
            "  CONSTRAINT race_event_fk FOREIGN KEY (event_id) "
            "     REFERENCES event (id),"
            "  CONSTRAINT race_map_fk FOREIGN KEY (map_id) "
            "     REFERENCES racemap (id),"
            "  CONSTRAINT race_raceprofile_fk FOREIGN KEY (raceprofile) "
            "     REFERENCES raceprofile (id)"
            ") ENGINE=InnoDB")
        TABLES['tag'] = (
            "CREATE TABLE tag ("
            "  id int NOT NULL AUTO_INCREMENT,"
            "  name varchar(50),"
            "  race bigint,"
            "  PRIMARY KEY (id),"
            "  CONSTRAINT tag_race_fk FOREIGN KEY (race) "
            "     REFERENCES race (id)"
            ") ENGINE=InnoDB")
        TABLES['racegroup'] = (
            "CREATE TABLE racegroup ("
            "  id int NOT NULL AUTO_INCREMENT,"
            "  name varchar(50),"
            "  race bigint,"
            "  PRIMARY KEY (id),"
            "  CONSTRAINT racegroup_race_fk FOREIGN KEY (race) "
            "     REFERENCES race (id)"
            ") ENGINE=InnoDB")
        table_creation_ddl = ""
        for name, ddl in TABLES.items():
            table_creation_ddl = table_creation_ddl+"; "+ddl
        try:
            cursor.execute(table_creation_ddl,multi=True)
            self.cnx.commit()
        except mysql.connector.Error as err:
            '''if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
                print("already exists.")'''
            '''else:'''
            print(err.msg)
        else:
            print("OK")
        cursor.close()
        
        # Users
        cursor = self.cnx.cursor()
        print("Inserting organizers and participants.")
        organizers = []
        participants = []
        for x in range(50):
            username = "user_" + str(random.randint(1, 50))
            participant_name = "participant_" + str(random.randint(1, 50))
            cursor.execute("INSERT INTO organizer (username) VALUES('"+ username +"')")
            organizers.append(cursor.lastrowid)
            cursor.execute("INSERT INTO participant (username) VALUES('"+ participant_name +"')")
            participants.append(cursor.lastrowid)
        cursor.close()
        self.cnx.commit()

        # Events & Races
        print("Inserting events and races.")
        cursor = self.cnx.cursor()
        events = []
        races = []
        coordinates = []
        activities = []
        maps = []
        racemaps = []
        for x in range(10):
            eventname = "event_" + str(x)
            cursor.execute("INSERT INTO event (name,organizer_id) VALUES('"+eventname+"','"+str(organizers[x])+"')")
            event_id = cursor.lastrowid
            events.append(event_id)
            for y in range(5):
                racename = "race_" + str(random.randint(1, 500))
                mapname = "map_" + str(random.randint(1, 500))
                cursor.execute("INSERT INTO map (name) VALUES('"+mapname+"')")
                map_id = cursor.lastrowid
                maps.append(map_id)
                cursor.execute("INSERT INTO racemap (map) VALUES('"+str(map_id)+"')")
                racemap_id = cursor.lastrowid
                racemaps.append(racemap_id)
                cursor.execute("INSERT INTO point (lat,lng,map) VALUES(33,44,'"+str(map_id)+"')")
                coordinates.append(cursor.lastrowid)
                cursor.execute("INSERT INTO point (lat,lng,map) VALUES(33.1,44.1,'"+str(map_id)+"')")
                coordinates.append(cursor.lastrowid)
                cursor.execute("INSERT INTO point (lat,lng,map) VALUES(33.2,44.2,'"+str(map_id)+"')")
                coordinates.append(cursor.lastrowid)
                cursor.execute("INSERT INTO race (name,map_id,event_id) VALUES('"+racename+"','"+str(racemap_id)+"','"+str(events[x])+"')")  
                race_id=cursor.lastrowid
                races.append(race_id)            
                rands = []
                for z in range(random.randint(0, 5)):
                    # Participants
                    rand = self.new_rand_int(rands, 0, 49)
                    
                    cursor.execute("INSERT INTO activity (participant,race,joinedAt) VALUES('"+str(participants[rand])+"','"+str(race_id)+"','"+str(datetime.datetime.now())+"')")
                    activities.append(cursor.lastrowid)

            #tx.create(Relationship(events[x], "MADE_BY", users[x * 5]))
        cursor.close()
        self.cnx.commit()

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
        pass
        #self.graph.delete_all()

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

