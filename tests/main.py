#!/usr/bin/python
from cases import MySQL

# Run
if __name__ == '__main__':
    m = MySQL()
    m.init("skim")
    case = m.addRowsToSKU()
    case.setup()
    case.run()
    case.teardown()
