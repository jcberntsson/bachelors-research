#!/usr/bin/python

# Imports
from pycallgraph import PyCallGraph
from pycallgraph import Config
from pycallgraph import GlobbingFilter
from pycallgraph.output import GraphvizOutput
from sys import argv
import os
import datetime

# Check if argument is provided
if (len(argv) < 2):
    print("Too few arguments. A argument for test file is needed.")
    print("Syntax: 'python benchmarking.py <testFileName> <testMethodName>'")
    print("Example: 'python benchmarking.py mysql unfollow'")
    print("Exiting...")
    exit()

# Definition and explanation of the column system in the Google Spreadsheet
"""
Total Time		Peak Time / op		AVG Time / op		Load Peak		Load AVG
    2				3					4					5				6
"""
cols = { "total_time": 2, "peak_time": 3, "avg_time": 4, "load_peak": 5, "load_avg": 6 }

# Declare variables
now = str(datetime.datetime.now()).replace(" ", "_").replace(":", ".")
testFile = argv[1]
directory = 'graphs/' + testFile
outputfile = directory + '/' + now
testMethodName = ""
if (len(argv) > 2):
    testMethodName = argv[2]
    outputfile += '-' + testMethodName
outputfile += '.png'
times = 1
if len(argv) > 3:
    times = int(argv[3])

# Create directory if needed
os.makedirs(directory, exist_ok=True)

# Import test method
if testFile == 'mysql':
    import mysql
    testMethod = getattr(mysql, testMethodName)
# ... More cases
else:
    import test
    testMethod = getattr(test, "main")

# Configuration for graphs
config = Config()
config.trace_filter = GlobbingFilter(exclude=[
    'pycallgraph.*',
    '*.secret_function',
])
output = GraphvizOutput()
output.output_file = outputfile

import googlesheet
sheet = googlesheet.Sheet(testFile)

# Profile the method
def profiling():
    #with PyCallGraph(output = output, config = config):
        #timer = timeit("testMethod()", setup="from __main__ import profiling", number=times)
        #print(timer)
        #for x in range(times):
    testMethod()

# Run
if __name__ == '__main__':
    from timeit import Timer
    #import cProfile
    #import re
    #cp = cProfile.run('re.compile("profiling")')
    #print(cp)
    #import cProfile, pstats#, StringIO
    #pr = cProfile.Profile()
    ##pr.enable()
    #profiling()
    #pr.disable()
    #s = StringIO.StringIO()
    #sortby = 'cumulative'
    #ps = pstats.Stats(pr)#, stream=s).sort_stats(sortby)
    #ps.print_stats()
    #print(s.getvalue())
    
    timeArray = Timer("profiling()", setup="from __main__ import profiling").repeat(times, 1)
    print(timeArray)
    #sheet.updateValue("#" + testMethodName, cols["total_time"], str(timer).replace(".", ","))
    #profiling()
    