import couchdb

# If your CouchDB server is running elsewhere, set it up like this:
couch = couchdb.Server('http://192.168.33.15:5984')

# create database
db = couch.create('testa') # newly created

#create a document and insert it into the db:
doc = {'foo': 'bar'}
db.save(doc)