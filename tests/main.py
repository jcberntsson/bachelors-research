#!/usr/bin/python

from cases import MySQL

# Run
if __name__ == '__main__':
    m = MySQL()
    m.init("skim")
    testCase = m.pairImageSKU()
    #testCase = neo.commentOnImage()
    testCase.setup()
    testCase.run()
    testCase.teardown()
