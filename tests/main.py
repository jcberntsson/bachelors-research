#!/usr/bin/python
from cases import Couch

# Run
if __name__ == '__main__':
    neo = Couch()
    #neo.init("raceone")
    case = neo.fetchCoords()
    case.setup()
    case.run()
    case.teardown()
