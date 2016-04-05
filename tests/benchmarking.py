#!/usr/bin/python
from pycallgraph import PyCallGraph
from pycallgraph import Config
from pycallgraph import GlobbingFilter
from pycallgraph.output import GraphvizOutput
from sys import argv

if len(argv) == 5:
    script_name, a, b, c, d = argv
else:
    print("Error inte arguments: ", argv)

from 'test.py

from argv[1] import main

config = Config()
config.trace_filter = GlobbingFilter(exclude=[
    'pycallgraph.*',
    '*.secret_function',
])

output = GraphvizOutput()
output.output_file = 'basic.png'

def profiling():
    with PyCallGraph(output=output, config=config):
        for x in range(500):
            main()
    
def test():
    y = 1
    for x in range(500):
        y += x
    print("Done: ", y)
    
if __name__ == '__main__':
    profiling()