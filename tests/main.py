#!/usr/bin/python
from cases import MySQL

# Run
if __name__ == '__main__':
    m = MySQL()
    #neo.init("raceone")
    case = m.fetchAllUserComments()
    case.setup()
    case.run()
    case.teardown()
