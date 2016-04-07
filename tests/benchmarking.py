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
	print("Syntax: 'python benchmarking.py <testFile> <testMethod>'")
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
testMethod = ""
if (len(argv) > 2):
	testMethod = argv[2]
	outputfile += '-' + testMethod
outputfile += '.png'

# Create directory if needed
os.makedirs(directory, exist_ok=True)

# Import test method
if testFile == 'mysql':
	import mysql
	methodToCall = getattr(mysql, testMethod)
# ... More cases
else:
	import test
	methodToCall = getattr(test, "main")

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
    with PyCallGraph(output=output, config=config):
        #for x in range(500):
        methodToCall()

# Run
if __name__ == '__main__':
	#addRow()
	#testGoogleDocs()
	from timeit import timeit
	timer = timeit("profiling()", setup="from __main__ import profiling", number=1)
	print(timer)
	sheet.updateValue("#" + testMethod, cols["total_time"], str(timer).replace(".", ","))
	#profiling()
	