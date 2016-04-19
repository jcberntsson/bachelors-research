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
if (len(argv) < 4):
    print("Too few arguments. A argument for test file is needed.")
    print("Syntax: 'python benchmarking.py <database> <company> <test case> <times>'")
    print("Example: 'python benchmarking.py mysql unfollow'")
    print("Exiting...")
    exit()

# Definition and explanation of the column system in the Google Spreadsheet
"""
Total Time		Peak Time / op		AVG Time / op		Load Peak		Load AVG
    2				3					4					5				6
"""
cols = { "total_time": 2, "peak_time": 3, "avg_time": 4, "load_peak": 5, "load_avg": 6 }

# Extract arguments
database = argv[1]
company = argv[2]
test_case = argv[3]
times = int(argv[4])

# Test arguments
validCompany = (company == "raceone" or company == "skim" or company == "reddit")
if not validCompany:
    print("Invalid company " + company + ", no logging to google spreadsheet will be possible")
    exit()

# Declare variables
now = str(datetime.datetime.now()).replace(" ", "_").replace(":", ".")
directory = 'graphs/' + database + "-" + company
outputfile = directory + '/' + now + '-' + test_case + '.png'

# Create directory if needed
os.makedirs(directory, exist_ok=True)

# Import test method
if database == 'mysql':
    import mysql
    testMethod = getattr(mysql, test_case)
elif database == 'neo4j':
    import neo4j
    #init(company)
    testMethod = getattr(neo4j, test_case)
elif database == 'mongo':
    import mongo
    testMethod = getattr(mongo, test_case)
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

if validCompany:
    import googlesheet
    sheet = googlesheet.Sheet(database + "-" + company)

# Profile the method
def profiling():
    #with PyCallGraph(output = output, config = config):
        #timer = timeit("testMethod()", setup="from __main__ import profiling", number=times)
        #print(timer)
        #for x in range(times):
    testMethod()

def formatValue(value):
    return str(value).replace(".", ",")

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
    totalTime = 0
    peakTime = 0
    for time in timeArray:
        totalTime += time
        if (time > peakTime):
            peakTime = time
    avgTime = totalTime / len(timeArray)
    sheet.updateValue("#" + test_case, cols["total_time"], formatValue(totalTime))
    sheet.updateValue("#" + test_case, cols["peak_time"], formatValue(peakTime))
    sheet.updateValue("#" + test_case, cols["avg_time"], formatValue(avgTime))
    #profiling()
    