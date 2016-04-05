#!/usr/bin/python
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
	print("Syntax: 'python benchmarking.py <testFile> <comment>'")
	print("Example: 'python benchmarking.py mysql onlyTesting'")
	print("Exiting...")
	exit()

# Declare variables
now = str(datetime.datetime.now()).replace(" ", "_").replace(":", ".")
testFile = argv[1]
directory = 'graphs/' + testFile
outputfile = directory + '/' + now
if (len(argv) > 2):
	outputfile += '-' + argv[2]
outputfile += '.png'

# Create directory if needed
os.makedirs(directory, exist_ok=True)

# Import test method
if testFile == 'mysql':
	from test import main
# ... More cases
else:
	from test import main	

# Configuration for graphs
config = Config()
config.trace_filter = GlobbingFilter(exclude=[
    'pycallgraph.*',
    '*.secret_function',
])
output = GraphvizOutput()
output.output_file = outputfile

# Profile the method
def profiling():
    with PyCallGraph(output=output, config=config):
        #for x in range(500):
        main()

# Run
if __name__ == '__main__':
	from timeit import timeit
	print(timeit("profiling()", setup="from __main__ import profiling", number=1))