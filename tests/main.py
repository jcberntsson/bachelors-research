#!/usr/bin/python
from cases import Neo4j

# Run
if __name__ == '__main__':
    m = Mongo()
    m.init("raceone")
    case = m.unparticipate()
    case.setup()
    case.run()
    case.teardown()

