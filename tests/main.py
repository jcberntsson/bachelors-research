#!/usr/bin/python
from cases import Mongo

# Run
if __name__ == '__main__':
    m = Mongo()
    m.clearData()
    m.init("raceone")
    #print(m.get_random_id("RACE"))
    case = m.removeRace()
    case.setup()
    case.run()
    case.teardown()
