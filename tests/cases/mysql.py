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
        try:
            cursor = self.cnx.cursor()
            cursor.execute("DROP TABLE activity")
            cursor.execute("DROP TABLE participant")
            cursor.execute("DROP TABLE tag")
            cursor.execute("DROP TABLE racegroup")
            cursor.execute("DROP TABLE race")
            cursor.execute("DROP TABLE racemap")
            cursor.execute("DROP TABLE eventmap")
            cursor.execute("DROP TABLE event")
            cursor.execute("DROP TABLE raceprofile")
            cursor.execute("DROP TABLE point")
            cursor.execute("DROP TABLE map")
            cursor.execute("DROP TABLE category")
            cursor.execute("DROP TABLE organizer")
            self.cnx.commit();
        except mysql.connector.Error as err:
            print(err.msg)
        else:
            print("Dropping OK")
        cursor.close()
        ##Create tables
        print("creating tables")
        TABLES = []
        TABLES.append(
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
        TABLES.append(
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
            "     REFERENCES event (id),"
            "  CONSTRAINT event_predecessor_fk FOREIGN KEY (predecessor_id) "
            "     REFERENCES event (id)"
            ") ENGINE=InnoDB")
        TABLES.append(
            "CREATE TABLE category ("
            "  id int NOT NULL AUTO_INCREMENT,"
            "  name varchar(50),"
            "  PRIMARY KEY (id)"
            ") ENGINE=InnoDB")
        TABLES.append(
            "CREATE TABLE raceprofile ("
            "  id int NOT NULL AUTO_INCREMENT,"
            "  name varchar(50),"
            "  PRIMARY KEY (id)"
            ") ENGINE=InnoDB")
        TABLES.append(
            "CREATE TABLE map ("
            "  id bigint NOT NULL AUTO_INCREMENT,"
            "  name varchar(50),"
            "  PRIMARY KEY (id)"
            ") ENGINE=InnoDB")
        TABLES.append(
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
        TABLES.append(
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
        TABLES.append(
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
        TABLES.append(
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
        TABLES.append(
            "CREATE TABLE tag ("
            "  id int NOT NULL AUTO_INCREMENT,"
            "  name varchar(50),"
            "  race bigint,"
            "  PRIMARY KEY (id),"
            "  CONSTRAINT tag_race_fk FOREIGN KEY (race) "
            "     REFERENCES race (id)"
            ") ENGINE=InnoDB")
        TABLES.append(
            "CREATE TABLE racegroup ("
            "  id int NOT NULL AUTO_INCREMENT,"
            "  name varchar(50),"
            "  race bigint,"
            "  PRIMARY KEY (id),"
            "  CONSTRAINT racegroup_race_fk FOREIGN KEY (race) "
            "     REFERENCES race (id)"
            ") ENGINE=InnoDB")
        TABLES.append(
            "CREATE TABLE participant ("
            "  id bigint NOT NULL AUTO_INCREMENT,"
            "  username varchar(50),"
            "  fullname varchar(50),"
            "  password varchar(20),"
            "  PRIMARY KEY (id)"
            ") ENGINE=InnoDB")
        TABLES.append(
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
        for ddl in TABLES:
            try:
                cursor = self.cnx.cursor()
                cursor.execute(ddl)
                self.cnx.commit()
                cursor.close()
            except mysql.connector.Error as err:
                '''if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
                    print("already exists.")'''
                '''else:'''
                print(err)
        
        # Users
        cursor = self.cnx.cursor()
        print("Inserting organizers and participants.")
        organizers = []
        participants = []
        for x in range(50):
            username = "user_" + str(random.randint(1, 50))
            participant_name = "participant_" + str(random.randint(1, 50))
            cursor.execute("INSERT INTO organizer (username,fullname,password,email) VALUES('"+ username +"','Tester','SuperHash','test@mail.com')")
            organizers.append(cursor.lastrowid)
            cursor.execute("INSERT INTO participant (username,fullname,password) VALUES('"+ participant_name +"','Tester','SuperHash')")
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
            cursor.execute("INSERT INTO event (name,organizer_id,logoUrl) VALUES('"+eventname+"','"+str(organizers[x])+"','google.se/img.png')")
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
                for p in range(100):
                    cursor.execute("INSERT INTO point (lat,lng,alt,map) VALUES("+str(10+p)+","+str(11+p)+","+str(20+p)+",'"+str(map_id)+"')")
                    coordinates.append(cursor.lastrowid)
                cursor.execute("INSERT INTO race (name,description,race_date,max_duration,preview,location,logo_url,map_id,event_id) VALUES('"+racename+"','A nice race to participate in','2016-06-13',3,'linktoimage.png','Gothenburg, Sweden','google.se/logo.png','"+str(racemap_id)+"','"+str(events[x])+"')")  
                race_id=cursor.lastrowid
                races.append(race_id)            
                rands = []
                for z in range(random.randint(0, 5)):
                    # Participants
                    rand = self.new_rand_int(rands, 0, 49)
                    
                    cursor.execute("INSERT INTO activity (participant,race,joinedAt) VALUES('"+str(participants[rand])+"','"+str(race_id)+"','"+str(datetime.datetime.now())+"')")
                    activities.append(cursor.lastrowid)

        cursor.close()
        self.cnx.commit()

    def initSkim(self):
        ##Drop old tables
        try:
            cursor = self.cnx.cursor()
            cursor.execute("DROP TABLE comment")
            cursor.execute("DROP TABLE image")
            cursor.execute("DROP TABLE skuValue")
            cursor.execute("DROP TABLE header")
            cursor.execute("DROP TABLE sku")
            cursor.execute("DROP TABLE contribution")
            cursor.execute("DROP TABLE contributor")
            cursor.execute("DROP TABLE project")
            self.cnx.commit();
        except mysql.connector.Error as err:
            print(err.msg)
        else:
            print("Dropping OK")
        cursor.close()
        
        ##Create tables
        print("Creating tables")
        TABLES = []
        TABLES.append(
            "CREATE TABLE project ("
            "  id bigint NOT NULL AUTO_INCREMENT,"
            "  name varchar(50),"
            "  PRIMARY KEY (id)"
            ") ENGINE=InnoDB")
        TABLES.append(
            "CREATE TABLE contributor ("
            "  id bigint NOT NULL AUTO_INCREMENT,"
            "  username varchar(20),"
            "  email varchar(50),"
            "  password varchar(20),"
            "  PRIMARY KEY (id)"
            ") ENGINE=InnoDB")
        TABLES.append(
            "CREATE TABLE contribution ("
            "  id bigint NOT NULL AUTO_INCREMENT,"
            "  contributor bigint,"
            "  project bigint,"
            "  PRIMARY KEY (id),"
            "  CONSTRAINT contribution_contributor_fk FOREIGN KEY (contributor) "
            "     REFERENCES contributor (id),"
            "  CONSTRAINT contribution_project_fk FOREIGN KEY (project) "
            "     REFERENCES project (id)"
            ") ENGINE=InnoDB")
        TABLES.append(
            "CREATE TABLE sku ("
            "  id bigint NOT NULL AUTO_INCREMENT,"
            "  project bigint,"
            "  PRIMARY KEY (id),"
            "  CONSTRAINT sku_project_fk FOREIGN KEY (project) "
            "     REFERENCES project (id)"
            ") ENGINE=InnoDB")
        TABLES.append(
            "CREATE TABLE header ("
            "  sku_id bigint,"
            "  name varchar(50),"
            "  PRIMARY KEY (sku_id,name),"
            "  CONSTRAINT header_sku_fk FOREIGN KEY (sku_id) "
            "     REFERENCES sku (id)"
            ") ENGINE=InnoDB")
        TABLES.append(
            "CREATE TABLE skuValue ("
            "  id bigint NOT NULL AUTO_INCREMENT,"
            "  sku_id bigint,"
            "  value varchar(100),"
            "  header_name varchar(50),"
            "  PRIMARY KEY (id),"
            "  CONSTRAINT skuValue_header_fk FOREIGN KEY (sku_id,header_name) "
            "     REFERENCES header (sku_id,name)"
            ") ENGINE=InnoDB")
        TABLES.append(
            "CREATE TABLE image ("
            "  id bigint NOT NULL AUTO_INCREMENT,"
            "  sku bigint,"
            "  project bigint,"
            "  name varchar(40),"
            "  original_name varchar(40),"
            "  extension varchar(10),"
            "  encoding varchar(10),"
            "  size int,"
            "  height int,"
            "  width int,"
            "  verticalDPI int,"
            "  horizontalDPI int,"
            "  bitDepth int,"
            "  createdAt datetime,"
            "  accepted Boolean,"
            "  PRIMARY KEY (id),"
            "  CONSTRAINT image_sku_fk FOREIGN KEY (sku) "
            "     REFERENCES sku (id),"
            "  CONSTRAINT image_project_fk FOREIGN KEY (project) "
            "     REFERENCES project (id)"
            ") ENGINE=InnoDB")
        TABLES.append(
            "CREATE TABLE comment ("
            "  id bigint NOT NULL AUTO_INCREMENT,"
            "  creator bigint,"
            "  image bigint,"
            "  text varchar(300),"
            "  createdAt datetime,"
            "  PRIMARY KEY (id),"
            "  CONSTRAINT comment_contributor_fk FOREIGN KEY (creator) "
            "     REFERENCES contributor (id),"
            "  CONSTRAINT comment_image_fk FOREIGN KEY (image) "
            "     REFERENCES image (id)"
            ") ENGINE=InnoDB")     
        for ddl in TABLES:
            try:
                cursor = self.cnx.cursor()
                cursor.execute(ddl)
                self.cnx.commit()
                cursor.close()
            except mysql.connector.Error as err:
                '''if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
                    print("already exists.")'''
                '''else:'''
                print(err)

        # Users
        users = []
        cursor = self.cnx.cursor()
        for x in range(50):
            cursor.execute("INSERT INTO contributor (username,email,password) VALUES ('user_"+str(x)+"','user_"+str(x)+"@mail.com','xpassx')")
            users.append(cursor.lastrowid)
        self.cnx.commit()
        cursor.close()

        # Projects and images
        cursor = self.cnx.cursor()
        for x in range(8):
            cursor.execute("INSERT INTO project (name) VALUES('project_"+str(x)+"')")
            project_id = cursor.lastrowid
            
            for c in range(10):
                cursor.execute("INSERT INTO contribution (contributor,project) VALUES('"+str(users[x*2+c])+"','"+str(project_id)+"')")
            for y in range(4):
                # Images
                nbr = x + 5 + y
                cursor.execute("INSERT INTO image (name,original_name,extension,encoding,size,height,width,verticalDPI,horizontalDPI,bitDepth,createdAt,accepted,project) "
                    "VALUES('image_"+str(nbr)+"','original_name','jpg','PNG/SFF',1024,1080,720,40,50,15,'2016-03-03',0,'"+str(project_id)+"')")

                # SKUS
                cursor.execute("INSERT INTO sku (project) VALUES('"+str(project_id)+"')")
                sku_id = cursor.lastrowid
                for z in range(10):
                    # Rows
                    cursor.execute("INSERT INTO header (sku_id,name) VALUES('"+str(sku_id)+"','header_"+str(z)+"')")
                    cursor.execute("INSERT INTO skuValue (sku_id,header_name,value) VALUES('"+str(sku_id)+"','header_"+str(z)+"','"+str(z)+"')")

                # SKU images
                nbr = x + 5 + y
                cursor.execute("INSERT INTO image (name,original_name,extension,encoding,size,height,width,verticalDPI,horizontalDPI,bitDepth,createdAt,accepted,sku) "
                    "VALUES('image_"+str(nbr)+"','original_name','jpg','PNG/SFF',1024,1080,720,40,50,15,'2016-03-03',0,'"+str(sku_id)+"')")
                image_id = cursor.lastrowid
                for z in range(2):
                    # Comments
                    cursor.execute("INSERT INTO comment (text,createdAt,creator,image) "
                        "VALUES('Haha, cool image','2016-04-04','"+str(users[x*2+z])+"','"+str(image_id)+"')")
        self.cnx.commit()
        cursor.close()

    def initReddit(self):
        pass

    def clearData(self):
        # Dangerous
        pass
        #self.graph.delete_all()

    ############################
    ####	TEST METHODS	####
    ############################

    # SKIM
    def fetchSKU(self):
        def setup(inner_self):
            # print("Setup")
            cursor = self.cnx.cursor()
            cursor.execute("INSERT INTO project (name) VALUE('test_project')")
            project_id = cursor.lastrowid
            cursor.execute("INSERT INTO sku (project) VALUES('"+str(project_id)+"')")
            sku_id = cursor.lastrowid
            for z in range(10):
                # Rows
                cursor.execute("INSERT INTO header (sku_id,name) VALUES('"+str(sku_id)+"','header_"+str(z)+"')")
                cursor.execute("INSERT INTO skuValue (sku_id,header_name,value) VALUES('"+str(sku_id)+"','header_"+str(z)+"','"+str(z)+"')")
            inner_self.sku_id = sku_id
            cursor.close()

        def run(inner_self):
            cursor = self.cnx.cursor()
            cursor.execute("SELECT s.ID,s.project,header.name,skuValue.value FROM sku as s "
                "INNER JOIN header ON s.id=header.sku_id "
                "INNER JOIN skuValue ON skuValue.sku_id = s.id AND skuValue.header_name=header.name "
                "GROUP BY s.ID "
                "HAVING s.ID = '"+str(inner_self.sku_id)+"'")
            result = cursor.fetchall()
            print(result)
            cursor.close()

        def teardown(inner_self):
            cursor = self.cnx.cursor()
            cursor.execute ("DELETE FROM skuValue WHERE sku_id='"+str(inner_self.sku_id)+"'")
            cursor.execute ("DELETE FROM header WHERE sku_id='"+str(inner_self.sku_id)+"'")
            cursor.execute ("DELETE FROM sku WHERE id='"+str(inner_self.sku_id)+"'")
            rc = cursor.rowcount
            cursor.close()
            self.cnx.commit()
            return rc

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
