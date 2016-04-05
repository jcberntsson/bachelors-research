import mysql.connector

cnx = mysql.connector.connect(user='vagrant', password='vagrant',
                              host='192.168.33.1',
                              database='research')
cnx.close()
