#!/usr/bin/python
from cases import Neo4j

# Run
if __name__ == '__main__':
    m = Neo4j()
    #m.init("skim")
    print(m.get_random_id("RACE"))
    #case = m.fetchParticipants2()
    #case.setup()
    #case.run()
    #case.teardown()

