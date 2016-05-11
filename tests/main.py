#!/usr/bin/python
from cases import Mongo

# Run
if __name__ == '__main__':
    m = Mongo()
    m.init("raceone")
    case = m.fetchParticipants2()
    case.setup()
    case.run()
    case.teardown()

