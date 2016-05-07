#!/usr/bin/python
from cases import MySQL

# Run
if __name__ == '__main__':
    neo = MySQL()
    neo.init("raceone")
    case = neo.fetchHotRaces()
    case.setup()
    case.run()
    case.teardown()
