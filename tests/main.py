#!/usr/bin/python
from cases import Mongo

# Run
if __name__ == '__main__':
    mongo = Mongo()
    mongo.init("skim")
    case = mongo.commentOnImage()
    case.setup()
    case.run()
    case.teardown()


