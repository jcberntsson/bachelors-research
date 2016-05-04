#!/usr/bin/python
from cases import MySQL

'''# Run
if __name__ == '__main__':

    m = Mongo()

    m.init("skim")
=======
from cases import Neo4j'''

# Run
if __name__ == '__main__':
    m = MySQL()
    m.init("raceone")
    case = m.duplicateEvent()
    case.setup()
    case.run()
    case.teardown()
