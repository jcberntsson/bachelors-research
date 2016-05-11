#!/usr/bin/python
from cases import Mongo

# Run
if __name__ == '__main__':
    m = Mongo()
    m.init("raceone")
    case = m.fetchCoords()
    case.setup()
    case.run()
    case.teardown()

