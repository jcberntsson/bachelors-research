#!/usr/bin/python
from cases import Mongo

# Run
if __name__ == '__main__':
    m = Mongo()
    m.init("reference")
    case = m.tinyGet()
    case.setup()
    case.run()
    case.teardown()

