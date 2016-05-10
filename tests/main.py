#!/usr/bin/python
from cases import Neo4j

# Run
if __name__ == '__main__':
    neo = Neo4j()
    #neo.init("raceone")
    case = neo.fetchCoords()
    case.setup()
    case.run()
    case.teardown()
