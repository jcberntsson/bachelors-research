#!/usr/bin/python
from cases import MySQL

# Run
if __name__ == '__main__':
    m = MySQL()
    m.init("raceone")
    case = m.removeCoords()
    case.setup()
    #case.run()
    #case.teardown()
