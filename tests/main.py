#!/usr/bin/python
from cases import Mongo

# Run
if __name__ == '__main__':
    m = Mongo()
    m.init("skim")
    case = m.fetchAllUserComments()
    m.clearData()
    case.setup()
    case.run()
    case.teardown()
