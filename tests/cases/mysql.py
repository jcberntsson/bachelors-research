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
            cursor.execute("DROP TABLE activityCoordinate")
            cursor.execute("DROP TABLE follow")
            cursor.execute("DROP TABLE activity")
            cursor.execute("DROP TABLE participant")
            cursor.execute("DROP TABLE tag")
            cursor.execute("DROP TABLE racegroup")
            cursor.execute("DROP TABLE racemap")
            cursor.execute("DROP TABLE race")
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
            "  orderIndex bigint,"
            "  lat DECIMAL(11, 8),"
            "  lng DECIMAL(11, 8),"
            "  alt DECIMAL(11, 8),"
            "  map bigint,"
            "  PRIMARY KEY (id),"
            "  CONSTRAINT point_map_fk FOREIGN KEY (map) "
            "     REFERENCES map (id)"
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
            "  raceprofile int,"
            "  category int,"
            "  PRIMARY KEY (id),"
            "  CONSTRAINT race_category_fk FOREIGN KEY (category) "
            "     REFERENCES category (id),"
            "  CONSTRAINT race_event_fk FOREIGN KEY (event_id) "
            "     REFERENCES event (id),"
            "  CONSTRAINT race_raceprofile_fk FOREIGN KEY (raceprofile) "
            "     REFERENCES raceprofile (id)"
            ") ENGINE=InnoDB")
        TABLES.append(
            "CREATE TABLE racemap ("
            "  id bigint NOT NULL AUTO_INCREMENT,"
            "  map bigint,"
            "  race bigint,"
            "  start_point bigint,"
            "  goal_point bigint,"
            "  PRIMARY KEY (id),"
            "  CONSTRAINT racemap_map_fk FOREIGN KEY (map) "
            "     REFERENCES map (id),"
            "  CONSTRAINT racemap_race_fk FOREIGN KEY (race) "
            "     REFERENCES race (id) ON DELETE CASCADE,"
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
            "  id bigint NOT NULL AUTO_INCREMENT,"
            "  participant bigint,"
            "  race bigint,"
            "  joinedAt datetime,"
            "  PRIMARY KEY (id),"
            "  CONSTRAINT activity_participant_fk FOREIGN KEY (participant) "
            "     REFERENCES participant (id) ON DELETE CASCADE,"
            "  CONSTRAINT activity_race_fk FOREIGN KEY (race) "
            "     REFERENCES race (id) ON DELETE CASCADE"
            ") ENGINE=InnoDB")
        TABLES.append(
            "CREATE TABLE follow ("
            "  follower bigint,"
            "  activity bigint,"
            "  followedAt datetime,"
            "  PRIMARY KEY (follower,activity),"
            "  CONSTRAINT follow_follower_fk FOREIGN KEY (follower) "
            "     REFERENCES participant (id) ON DELETE CASCADE,"
            "  CONSTRAINT follow_activity_fk FOREIGN KEY (activity) "
            "     REFERENCES activity (id) ON DELETE CASCADE"
            ") ENGINE=InnoDB")
        TABLES.append(
            "CREATE TABLE activityCoordinate ("
            "  id bigint NOT NULL AUTO_INCREMENT,"
            "  activity bigint,"
            "  createdAt datetime(6),"
            "  lat DECIMAL(11, 8),"
            "  lng DECIMAL(11, 8),"
            "  alt DECIMAL(11, 8),"
            "  PRIMARY KEY (id),"
            "  CONSTRAINT activityCoordinate_activity_fk FOREIGN KEY (activity) "
            "     REFERENCES activity (id) ON DELETE CASCADE"
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
                for p in range(100):
                    cursor.execute("INSERT INTO point (lat,lng,alt,map,orderIndex) VALUES("+str(10+p)+","+str(11+p)+","+str(20+p)+",'"+str(map_id)+"',"+str(p)+")")
                    coordinates.append(cursor.lastrowid)
                cursor.execute("INSERT INTO race (name,description,race_date,max_duration,preview,location,logo_url,event_id) VALUES('"+racename+"','A nice race to participate in','2016-06-13',3,'linktoimage.png','Gothenburg, Sweden','google.se/logo.png','"+str(events[x])+"')")  
                race_id=cursor.lastrowid
                races.append(race_id)   
                cursor.execute("INSERT INTO racemap (map,race) VALUES('"+str(map_id)+"','"+str(race_id)+"')")
                racemap_id = cursor.lastrowid
                racemaps.append(racemap_id)         
                rands = []
                for z in range(random.randint(0, 5)):
                    # Participants
                    rand = self.new_rand_int(rands, 0, 48)
                    
                    cursor.execute("INSERT INTO activity (participant,race,joinedAt) VALUES('"+str(participants[rand])+"','"+str(race_id)+"','"+str(datetime.datetime.now())+"')")
                    activities.append(cursor.lastrowid)
                    activity_id = cursor.lastrowid
                    cursor.execute("INSERT INTO follow (follower,activity,followedAt) VALUES('"+str(participants[rand+1])+"','"+str(activity_id)+"','"+str(datetime.datetime.now())+"')") 
                    for p in range(50):
                        cursor.execute("INSERT INTO activityCoordinate (activity,createdAt,lat,lng,alt) VALUES('"+str(activity_id)+"','2016-03-03',"+str(10+p)+","+str(11+p)+","+str(20+p)+")")

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
            inner_self.sku_id = str(sku_id)
            cursor.close()

        def run(inner_self):
            cursor = self.cnx.cursor()
            cursor.execute("SELECT s.ID,s.project,header.name,skuValue.value FROM sku as s "
                "INNER JOIN header ON s.id=header.sku_id "
                "INNER JOIN skuValue ON skuValue.sku_id = s.id AND skuValue.header_name=header.name "
                "WHERE s.ID = '"+inner_self.sku_id+"'")
            result = cursor.fetchall()
            cursor.close()

        def teardown(inner_self):
            cursor = self.cnx.cursor()
            cursor.execute ("DELETE FROM skuValue WHERE sku_id='"+inner_self.sku_id+"'")
            cursor.execute ("DELETE FROM header WHERE sku_id='"+inner_self.sku_id+"'")
            cursor.execute ("DELETE FROM sku WHERE id='"+inner_self.sku_id+"'")
            rc = cursor.rowcount
            cursor.close()
            self.cnx.commit()
            return rc

        return self.create_case("fetchSKU", setup, run, teardown)

    def fetchUsers(self):
        def setup(inner_self):
            pass

        def run(inner_self):
            cursor = self.cnx.cursor()
            cursor.execute("SELECT * FROM contributor")
            result = cursor.fetchall()
            cursor.close()

        def teardown(inner_self):
            pass

        return self.create_case("fetchUsers", setup, run, teardown)

    def commentOnImage(self):
        def setup(inner_self):
            cursor = self.cnx.cursor()
            #Contributor
            cursor.execute("INSERT INTO contributor (username) VALUES ('test_user')")
            user_id = cursor.lastrowid
            #Project
            cursor.execute("INSERT INTO project (name) VALUES ('test_project')")
            project_id = cursor.lastrowid
            #Contribution
            cursor.execute("INSERT INTO contribution (contributor,project) VALUES ('"+str(user_id)+"','"+str(project_id)+"')")
            contribution_id = cursor.lastrowid
            #Image
            cursor.execute("INSERT INTO image (name,original_name,extension,encoding,size,height,width,verticalDPI,horizontalDPI,bitDepth,createdAt,accepted,project) "
                "VALUES('test_image','original_name','jpg','PNG/SFF',1024,1080,720,40,50,15,'2016-03-03',0,'"+str(project_id)+"')")
            image_id = cursor.lastrowid        
            #Output
            inner_self.user_id = str(user_id)
            inner_self.project_id = str(project_id)
            inner_self.image_id = str(image_id)
            inner_self.contribution_id = str(contribution_id)
            cursor.close()
            self.cnx.commit()

        def run(inner_self):
            cursor = self.cnx.cursor()
            cursor.execute("INSERT INTO comment (text,createdAt,creator,image) "
                "VALUES('Haha, cool image','2016-04-04','"+inner_self.user_id+"','"+inner_self.image_id+"')")
            inner_self.comment_id = cursor.lastrowid 
            cursor.close()
            self.cnx.commit()       

        def teardown(inner_self):
            cursor = self.cnx.cursor()
            cursor.execute("DELETE FROM comment WHERE id='"+inner_self.comment_id+"'")
            cursor.execute("DELETE FROM image WHERE id='"+inner_self.image_id+"'")
            cursor.execute("DELETE FROM contribution WHERE id='"+inner_self.contribution_id+"'")
            cursor.execute("DELETE FROM contributor WHERE id='"+inner_self.user_id+"'")
            cursor.execute("DELETE FROM project WHERE id='"+inner_self.project_id+"'")
            cursor.close()
            self.cnx.commit()

        return self.create_case("commentOnImage", setup, run, teardown)

    def pairImageSKU(self):
        def setup(inner_self):
            # print("Setup")
            cursor = self.cnx.cursor()
            #Project
            cursor.execute("INSERT INTO project (name) VALUES ('test_project')")
            project_id = cursor.lastrowid
            #Image
            cursor.execute("INSERT INTO image (name,original_name,extension,encoding,size,height,width,verticalDPI,horizontalDPI,bitDepth,createdAt,accepted,project) "
                "VALUES('test_image','original_name','jpg','PNG/SFF',1024,1080,720,40,50,15,'2016-03-03',0,'"+str(project_id)+"')")
            image_id = cursor.lastrowid
            #SKU
            cursor.execute("INSERT INTO sku (project) VALUES('"+str(project_id)+"')")
            sku_id = cursor.lastrowid
            for z in range(10):
                # Rows
                cursor.execute("INSERT INTO header (sku_id,name) VALUES('"+str(sku_id)+"','header_"+str(z)+"')")
                cursor.execute("INSERT INTO skuValue (sku_id,header_name,value) VALUES('"+str(sku_id)+"','header_"+str(z)+"','"+str(z)+"')")
            
            cursor.close()
            self.cnx.commit()
            #OUTPUT
            inner_self.sku_id = str(sku_id)
            inner_self.project_id = str(project_id)
            inner_self.image_id = str(image_id)

        def run(inner_self):
            # print("Run")
            cursor = self.cnx.cursor()
            cursor.execute("UPDATE image SET sku='"+inner_self.sku_id+"' WHERE project='"+inner_self.project_id+"' AND id='"+inner_self.image_id+"'")
            cursor.close()
            self.cnx.commit()

        def teardown(inner_self):
            # print("Teardown")
            cursor = self.cnx.cursor()
            cursor.execute ("DELETE FROM skuValue WHERE sku_id='"+inner_self.sku_id+"'")
            cursor.execute ("DELETE FROM header WHERE sku_id='"+inner_self.sku_id+"'")
            cursor.execute("DELETE FROM image WHERE id='"+inner_self.image_id+"'")
            cursor.execute ("DELETE FROM sku WHERE id='"+inner_self.sku_id+"'")
            cursor.execute("DELETE FROM project WHERE id='"+inner_self.project_id+"'")
            
            cursor.close()
            self.cnx.commit()

        return self.create_case("pairImageSKU", setup, run, teardown)

    def addRowsToSKU(self):
        def setup(inner_self):
            cursor = self.cnx.cursor()
            cursor.execute("SELECT id FROM sku")
            result = cursor.fetchall()
            rand = random.randint(0,len(result)-1)
            sku_id = result[rand][0]
            inner_self.sku_id = str(sku_id)
            cursor.execute("INSERT INTO header(sku_id,name) VALUES ("+inner_self.sku_id+",'remove_me1'),("+inner_self.sku_id+",'remove_me2'),("+inner_self.sku_id+",'remove_me3'),("+inner_self.sku_id+",'remove_me4')")
            cursor.close()

        def run(inner_self):
            cursor = self.cnx.cursor()
            for i in range(10):
                cursor.execute("INSERT INTO skuValue(sku_id,value,header_name) VALUES ("+inner_self.sku_id+",'110','remove_me1')")
                cursor.execute("INSERT INTO skuValue(sku_id,value,header_name) VALUES ("+inner_self.sku_id+",'120','remove_me2')")
                cursor.execute("INSERT INTO skuValue(sku_id,value,header_name) VALUES ("+inner_self.sku_id+",'130','remove_me3')")
                cursor.execute("INSERT INTO skuValue(sku_id,value,header_name) VALUES ("+inner_self.sku_id+",'140','remove_me4')")
            self.cnx.commit()
            cursor.close()

        def teardown(inner_self):
            cursor = self.cnx.cursor()
            cursor.execute("DELETE FROM skuValue WHERE header_name='remove_me1'")
            cursor.execute("DELETE FROM skuValue WHERE header_name='remove_me2'")
            cursor.execute("DELETE FROM skuValue WHERE header_name='remove_me3'")
            cursor.execute("DELETE FROM skuValue WHERE header_name='remove_me4'")
            cursor.execute("DELETE FROM header WHERE name='remove_me1'")
            cursor.execute("DELETE FROM header WHERE name='remove_me2'")
            cursor.execute("DELETE FROM header WHERE name='remove_me3'")
            cursor.execute("DELETE FROM header WHERE name='remove_me4'")
            self.cnx.commit()
            cursor.close()

        return self.create_case("addRowsToSKU", setup, run, teardown)

    def fetchAllUserComments(self):
        def setup(inner_self):
            cursor = self.cnx.cursor()
            cursor.execute("SELECT id FROM contributor")
            result = cursor.fetchall()
            rand = random.randint(0,len(result))
            contributor_id = result[rand][0]
            inner_self.contributor_id = str(contributor_id)
            cursor.close()

        def run(inner_self):
            cursor = self.cnx.cursor()
            cursor.execute("SELECT * FROM comment WHERE creator ="+inner_self.contributor_id)
            result = cursor.fetchall()
            cursor.close()

        def teardown(inner_self):
            pass

        return self.create_case("fetchAllUserComments", setup, run, teardown)

    # RaceOne
    def follow(self):
        def setup(inner_self):
            cursor = self.cnx.cursor()
            cursor.execute("INSERT INTO participant (username,fullname,password) VALUES('test_follower','Tester','SuperHash')")
            follower_id = cursor.lastrowid
            cursor.execute("INSERT INTO participant (username,fullname,password) VALUES('test_participant','Tester','SuperHash')")
            participant_id = cursor.lastrowid
            cursor.execute("SELECT id FROM race")
            result = cursor.fetchall()
            rand = random.randint(0,len(result))
            race_id = result[rand][0]
            cursor.execute("INSERT INTO activity (participant,race,joinedAt) VALUES('"+str(participant_id)+"','"+str(race_id)+"','"+str(datetime.datetime.now())+"'")
            activity_id = cursor.lastrowid
            cursor.close()
            self.cnx.commit()
            inner_self.follower_id = str(follower_id)
            inner_self.participant_id = str(participant_id)
            inner_self.race_id = str(race_id)
            inner_self.activity_id = str(activity_id)

        def run(inner_self):
            cursor = self.cnx.cursor()
            cursor.execute("INSERT INTO follow (follower,activity,followedAt) VALUES ('"+inner_self.follower_id+"','"
                +inner_self.activity_id+"','"+str(datetime.datetime.now())+"')")
            cursor.close()
            self.cnx.commit()
                        
        def teardown(inner_self):
            cursor = self.cnx.cursor()
            cursor.execute("DELETE FROM follow WHERE follower='"+inner_self.follower_id
                +"' AND participant='"+inner_self.participant_id+"' AND race='"+inner_self.race_id+"'")
            cursor.execute("DELETE FROM activity WHERE id='"+inner_self.activity_id+"'")
            cursor.execute("DELETE FROM participant WHERE id='"+inner_self.participant_id+"'")
            cursor.execute("DELETE FROM participant WHERE id='"+inner_self.follower_id+"'")    
            cursor.close()
            self.cnx.commit()

        return self.create_case("follow", setup, run, teardown)

    def unfollow(self):
        def setup(inner_self):
            cursor = self.cnx.cursor()
            cursor.execute("INSERT INTO participant (username,fullname,password) VALUES('test_follower','Tester','SuperHash')")
            follower_id = cursor.lastrowid
            cursor.execute("INSERT INTO participant (username,fullname,password) VALUES('test_participant','Tester','SuperHash')")
            participant_id = cursor.lastrowid
            cursor.execute("SELECT id FROM race")
            result = cursor.fetchall()
            rand = random.randint(0,len(result))
            race_id = result[rand][0]
            cursor.execute("INSERT INTO activity (participant,race,joinedAt) VALUES('"+str(participant_id)+"','"+str(race_id)+"','"+str(datetime.datetime.now())+"')")
            activity_id = cursor.lastrowid
            cursor.execute("INSERT INTO follow (follower,activity,followedAt) VALUES ('"+str(follower_id)+"','"
                +str(activity_id)+"','"+str(datetime.datetime.now())+"')")
            cursor.close()
            self.cnx.commit()
            inner_self.follower_id = str(follower_id)
            inner_self.participant_id = str(participant_id)
            inner_self.race_id = str(race_id)
            inner_self.activity_id = str(activity_id)

        def run(inner_self):
            cursor = self.cnx.cursor()
            cursor.execute("DELETE FROM follow WHERE follower='"+inner_self.follower_id
                +"' AND activity='"+inner_self.activity_id+"'")
            cursor.close()
            self.cnx.commit()
                        
        def teardown(inner_self):
            cursor = self.cnx.cursor()
            cursor.execute("DELETE FROM activity WHERE id='"+inner_self.activity_id+"'")
            cursor.execute("DELETE FROM participant WHERE id='"+inner_self.participant_id+"'")
            cursor.execute("DELETE FROM participant WHERE id='"+inner_self.follower_id+"'")    
            cursor.close()
            self.cnx.commit()

        return self.create_case("unfollow", setup, run, teardown)



    def insertCoords(self):
        def setup(inner_self):
            cursor = self.cnx.cursor()
            cursor.execute("SELECT id FROM activity")
            result = cursor.fetchall()
            rand = random.randint(0,len(result)-1)
            activity_id = result[rand][0]
            inner_self.activity_id = str(activity_id)
            inner_self.start_time = str(datetime.datetime.now())
            cursor.close()

        def run(inner_self):
            cursor = self.cnx.cursor()
            for i in range(100):
                cursor.execute("INSERT INTO activityCoordinate (activity,createdAt,lat,lng,alt) VALUES("+
                    inner_self.activity_id+",'"+str(datetime.datetime.now())+"',"+str(10+i)+","+str(11+i)+","+str(20+i)+")")
            cursor.close()
            self.cnx.commit()   

        def teardown(inner_self):
            cursor = self.cnx.cursor()
            cursor.execute("DELETE FROM activityCoordinate WHERE activity="+inner_self.activity_id+" AND createdAt > '"+inner_self.start_time+"'")
            cursor.close()
            self.cnx.commit()

        return self.create_case("insertCoords", setup, run, teardown)

    def fetchParticipants(self):
        def setup(inner_self):
            pass

        def run(inner_self):
            cursor = self.cnx.cursor()
            cursor.execute("SELECT participant.id, count(*) as followCount FROM participant INNER JOIN activity ON activity.participant=participant.id GROUP BY participant.id ORDER BY followCount")
            result = cursor.fetchall()
            cursor.close()

        def teardown(inner_self):
            pass

        return self.create_case("fetchParticipants", setup, run, teardown)

    def duplicateEvent(self):
        pass
        '''def setup(inner_self):
            cursor = self.cnx.cursor()
            cursor.execute("SELECT * FROM event")
            result = cursor.fetchall()
            rand = random.randint(0,len(result)-1)
            event_id = str(result[rand][0])
            inner_self.event_id = event_id
            cursor.close()
        def run(inner_self):
            cursor = self.cnx.cursor()
            cursor.execute("SELECT * FROM race WHERE event_id='"+inner_self.event_id+"'")
            result = cursor.fetchall()
            inner_self.races = result
            race_ids = "("
            for r in result:
                race_ids = race_ids +"'"+ str(r[0]) + "',"
            race_ids = race_ids[:-1]
            race_ids = race_ids + ")"
            cursor.execute("SELECT * from racemap WHERE id IN "+race_ids)
            result = cursor.fetchall()
            inner_self.racemaps = result
            cursor.execute("")
            cursor.close()

        def teardown(inner_self):
            cursor = self.cnx.cursor()
            cursor.execute("")
            cursor.close()

        return self.create_case("fetchParticipants2", setup, run, teardown)'''

    def fetchParticipants2(self):
        def setup(inner_self):
            pass
        def run(inner_self):
            cursor = self.cnx.cursor()
            cursor.execute("SELECT activity.race, participant.id, count(*) FROM participant "
                "INNER JOIN activity ON activity.participant=participant.id "
                "INNER JOIN follow WHERE activity=activity.id GROUP BY participant.id,activity.race")
            result = cursor.fetchall()
            cursor.close()

        def teardown(inner_self):
            pass

        return self.create_case("fetchParticipants2", setup, run, teardown)

    def unparticipate(self):
        def setup(inner_self):
            cursor = self.cnx.cursor()
            cursor.execute("SELECT activity.id, activity.participant,activity.race,activity.joinedAt FROM activity INNER JOIN follow WHERE activity.id=follow.activity")
            result = cursor.fetchall()
            rand = random.randint(0,len(result)-1)
            inner_self.activity = result[rand]
            activity_id = result[rand][0]
            cursor.execute("SELECT follower,followedAt FROM follow WHERE activity='"+str(activity_id)+"'")
            result = cursor.fetchall()
            inner_self.activity_id = str(activity_id)
            inner_self.follows = result
            cursor.close()
        def run(inner_self):
            cursor = self.cnx.cursor()
            cursor.execute("DELETE FROM activity WHERE activity.id = '"+inner_self.activity_id+"'") 
            cursor.close()
            self.cnx.commit()

        def teardown(inner_self):
            cursor = self.cnx.cursor()
            cursor.execute("INSERT INTO activity (id,participant,race,joinedAt) VALUES('"+
                str(inner_self.activity[0])+"','"+str(inner_self.activity[1])+"','"+str(inner_self.activity[2])+"','"+str(inner_self.activity[3])+"')")
            for f in inner_self.follows:
                cursor.execute("INSERT INTO follow (follower,activity,followedAt) VALUES('"+
                    str(f[0])+"','"+inner_self.activity_id+"','"+str(f[1])+"')")
            cursor.close()
            self.cnx.commit()
        return self.create_case("unparticipate", setup, run, teardown)



    def fetchCoords(self):
        def setup(inner_self):
            cursor = self.cnx.cursor()
            cursor.execute("SELECT id FROM activity")
            result = cursor.fetchall()
            rand = random.randint(0,len(result)-1)
            activity_id = result[rand][0]
            inner_self.activity_id = str(activity_id)
            cursor.close()

        def run(inner_self):
            cursor = self.cnx.cursor()
            cursor.execute("SELECT * FROM activityCoordinate WHERE activity="+inner_self.activity_id)
            result = cursor.fetchall()
            cursor.close()

        def teardown(inner_self):
            pass

        return self.create_case("fetchCoords", setup, run, teardown)

    def removeCoords(self):
        def setup(inner_self):
            cursor = self.cnx.cursor()
            cursor.execute("SELECT id FROM map")
            result = cursor.fetchall()
            rand = random.randint(0,len(result)-1)
            map_id = result[rand][0]
            inner_self.map_id = str(map_id)
            cursor.execute("SELECT id,lat,lng,alt,map FROM point WHERE map="+str(map_id))
            result = cursor.fetchall()
            inner_self.points = result
            cursor.execute("SELECT id FROM point WHERE map="+str(map_id))
            result = cursor.fetchall()
            random.shuffle(result)
            inner_self.point_ids = result[:(len(result)//3)]
            cursor.close()

        def run(inner_self):
            point_ids = ""
            for p in inner_self.point_ids:
                point_ids = point_ids + str(p[0]) + ","
            point_ids = point_ids[:-1]
            cursor = self.cnx.cursor()
            cursor.execute("DELETE FROM point WHERE id IN ("+point_ids+")")
            cursor.close()
            self.cnx.commit()

        def teardown(inner_self):
            cursor = self.cnx.cursor()
            cursor.execute("DELETE FROM point WHERE map="+inner_self.map_id)
            for p in inner_self.points:
                point_id = str(p[0])
                lat = str(p[1])
                lng = str(p[2])
                alt = str(p[3])
                map = str(p[4])
                cursor.execute("INSERT INTO point (id, lat,lng,alt,map) VALUES("+point_id+","+lat+","+lng+","+alt+","+map+")")
            cursor.close()
            self.cnx.commit()

        return self.create_case("removeCoords", setup, run, teardown)

    def removeRace(self):
        def setup(inner_self):
            cursor = self.cnx.cursor()
            cursor.execute("SELECT id,name,description,race_date,max_duration,preview,location,logo_url,event_id FROM race")
            result = cursor.fetchall()
            rand = random.randint(0,len(result)-1)
            inner_self.race = result[rand]
            race_id = result[rand][0]
            cursor.execute("SELECT id,map,race FROM racemap WHERE race='"+str(race_id)+"'")
            result = cursor.fetchall()
            inner_self.racemaps = result
            inner_self.race_id = str(race_id)
            cursor.close()
        def run(inner_self):
            cursor = self.cnx.cursor()
            cursor.execute("DELETE FROM race where id='"+inner_self.race_id+"'")
            cursor.close()
            self.cnx.commit()

        def teardown(inner_self):
            cursor = self.cnx.cursor()
            cursor.execute("INSERT INTO race (id,name,description,race_date,max_duration,preview,location,logo_url,event_id) VALUES('"+str(inner_self.race[0])+"','"+str(inner_self.race[1])+"','"+str(inner_self.race[2])+"','"+str(inner_self.race[3])+"','"+str(inner_self.race[4])+"','"+str(inner_self.race[5])+"','"+str(inner_self.race[6])+"','"+str(inner_self.race[7])+"','"+str(inner_self.race[8])+"')")
            for rm in inner_self.racemaps:
                cursor.execute("INSERT INTO racemap (id,map,race) VALUES('"+str(rm[0])+"','"+str(rm[1])+"','"+str(rm[2])+"')")
            cursor.close()
            self.cnx.commit()

        return self.create_case("removeRace", setup, run, teardown)

    def fetchHotRaces(self):
        def setup(inner_self):
            pass

        def run(inner_self):
            cursor = self.cnx.cursor()
            cursor.execute("SELECT activity.race, count(*) as rating FROM activity "
                "INNER JOIN follow on follow.activity=activity.id GROUP BY activity.race ORDER BY rating DESC LIMIT 10")
            result = cursor.fetchall()
            print(result)
            cursor.close()

        def teardown(inner_self):
            pass

        return self.create_case("fetchHotRaces", setup, run, teardown)

    def fetchRace(self):
        def setup(inner_self):
            cursor = self.cnx.cursor()
            cursor.execute("SELECT id FROM race")
            result = cursor.fetchall()
            rand = random.randint(0,len(result)-1)
            race_id = result[rand][0]
            inner_self.race_id = str(race_id)

        def run(inner_self):
            cursor = self.cnx.cursor()
            print("SELECT race.*,event.name,racemap.id,racemap.map,p1.lat,p1.lng,p1.alt,p2.lat,p2.lng,p2.alt FROM race INNER JOIN event ON race.event_id=event.id "+
                "INNER JOIN racemap ON racemap.race = race.id "+
                "INNER JOIN point as p1 ON racemap.start_point = p1.id "+
                "INNER JOIN point as p2 ON racemap.goal_point = p2.id WHERE race.ID="+inner_self.race_id)
            cursor.execute("SELECT race.*,event.name,racemap.id,racemap.map,p1.lat,p1.lng,p1.alt,p2.lat,p2.lng,p2.alt FROM race INNER JOIN event ON race.event_id=event.id "+
                "INNER JOIN racemap ON racemap.race = race.id "+
                "LEFT JOIN point as p1 ON racemap.start_point = p1.id "+
                "LEFT JOIN point as p2 ON racemap.goal_point = p2.id "+
                "WHERE race.ID="+inner_self.race_id)
            result = cursor.fetchall()[0]
            print(result)
            map_id = str(result[13])
            race = result[:14]
            start_point = result[14:17]
            goal_point = result[17:20]
            print(map_id)
            cursor.execute("SELECT * FROM point WHERE map = "+map_id +" ORDER BY orderIndex")
            mapCoords = cursor.fetchall()[0]
            cursor.execute("SELECT participant.id, participant.username,participant.fullname FROM activity INNER JOIN participant WHERE activity.participant=participant.id WHERE race = "+inner_self.race_id)
            participants = cursor.fetchall()[0]
            print(participants)
            cursor.close()

        def teardown(inner_self):
            pass

        return self.create_case("fetchRace", setup, run, teardown)
