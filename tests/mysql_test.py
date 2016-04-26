import mysql.connector
from datetime import date, datetime, timedelta

def reddit_createTables(cnx):
    print("creating tables")
    cursor = cnx.cursor()
    TABLES = {}
    TABLES['user'] = (
        "CREATE TABLE 'user' ("
        "  'id' int(11) NOT NULL AUTO_INCREMENT,"
        "  'username' varchar(20) NOT NULL,"
        "  PRIMARY KEY ('id')"
        ") ENGINE=InnoDB")
    TABLES['post'] = (
        "CREATE TABLE 'post' ("
        "  'id' int(11) NOT NULL AUTO_INCREMENT,"
        "  'creator_id' int(11) NOT NULL AUTO_INCREMENT,"
        "  'title' varchar(100) NOT NULL,"
        "  'created_at' date NOT NULL,"
        "  PRIMARY KEY ('id')"
        "  CONSTRAINT 'user_fk' FOREIGN KEY ('creator_id') "
        "     REFERENCES 'user' ('id') ON DELETE CASCADE"
        ") ENGINE=InnoDB")
    TABLES['comment'] = (
        "CREATE TABLE 'comment' ("
        "  'id' int(11) NOT NULL AUTO_INCREMENT,"
        "  'creator_id' int(11) NOT NULL,"
        "  'post_id' int(11) NOT NULL,"
        "  'content' text NOT NULL,"
        "  'created_at' date NOT NULL,"
        "  PRIMARY KEY ('id')"
        "  CONSTRAINT 'user_fk' FOREIGN KEY ('creator_id') "
        "     REFERENCES 'user' ('id') ON DELETE CASCADE"
        "  CONSTRAINT 'post_fk' FOREIGN KEY (`post_id`) "
        "     REFERENCES 'post' ('id') ON DELETE CASCADE"
        ") ENGINE=InnoDB")
    TABLES['upvote'] = (
        "CREATE TABLE 'upvote' ("
        "  'creator_id' int(11) NOT NULL,"
        "  'post_id' int(11) NOT NULL,"
        "  'created_at' date NOT NULL,"
        "  PRIMARY KEY ('creator_id,post_id')"
        "  CONSTRAINT 'user_fk' FOREIGN KEY ('creator_id') "
        "     REFERENCES 'user' ('id') ON DELETE CASCADE"
        "  CONSTRAINT 'post_fk' FOREIGN KEY (`post_id`) "
        "     REFERENCES 'post' ('id') ON DELETE CASCADE"
        ") ENGINE=InnoDB")
    TABLES['downvote'] = (
        "CREATE TABLE 'downvote' ("
        "  'creator_id' int(11) NOT NULL,"
        "  'post_id' int(11) NOT NULL,"
        "  'created_at' date NOT NULL,"
        "  PRIMARY KEY ('creator_id,post_id')"
        "  CONSTRAINT 'user_fk' FOREIGN KEY ('creator_id') "
        "     REFERENCES 'user' ('id') ON DELETE CASCADE"
        "  CONSTRAINT 'post_fk' FOREIGN KEY (`post_id`) "
        "     REFERENCES 'post' ('id') ON DELETE CASCADE"
        ") ENGINE=InnoDB")
    for name, ddl in TABLES.items():
        try:
            print("Creating table {}: ".format(name), end='')
            cursor.execute(ddl)
        except mysql.connector.Error as err:
            '''if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
                print("already exists.")'''
            '''else:'''
            print(err.msg)
        else:
            print("OK")
        cursor.close()
        cnx.close()
        
def reddit_insertData(cnx):
    cursor = cnx.cursor()
    add_user = ("INSERT INTO user "
               "(username) "
               "VALUES (")
    usernames = [("simon"),("takman"),("nordmark"),("jcb-it"),("pedro"),("virre")]
    userIDs = []
    for username in usernames:
        cursor.execute(add_user + username + ")")
        userIDs.append(cursor.lastrowid)
    cursor.close()
    
    
def reddit_testLog(cnx):
    cursor = cnx.cursor()
    userQuery = ("SELECT * FROM users")
    cursor.execute(userQuery)
    cursor.close()
    
def main():
    cnx = mysql.connector.connect(user='vagrant', password='vagrant', host='46.101.234.110', database='research')
    reddit_createTables(cnx)
    reddit_insertData(cnx)
    reddit_testLog(cnx)
    cnx.close()

main()