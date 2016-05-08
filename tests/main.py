#!/usr/bin/python
from cases import Neo4j

# Run
if __name__ == '__main__':
    m = MySQL()
    m.init("raceone")
    case = m.fetchHotRaces()
    case.setup()
    case.run()
    case.teardown()
