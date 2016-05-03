#!/usr/bin/python

<<<<<<< HEAD
from cases import Mongo

# Run
if __name__ == '__main__':

    m = Mongo()

    m.init("skim")
=======
from cases import Neo4j

# Run
if __name__ == '__main__':
    neo = Neo4j()
    #neo.init("raceone")
    case = neo.fetchUsers()
    case.run()
>>>>>>> 7e28094ee25e5ec49aa7b279298031bed756ac24
    #testCase = neo.commentOnImage()
