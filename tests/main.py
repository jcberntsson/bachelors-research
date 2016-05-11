#!/usr/bin/python
from cases import Couch

# Run
if __name__ == '__main__':
    c = Couch()
    c.init("raceone")
    case = c.follow()
    case.setup()
    case.run()
    case.teardown()
