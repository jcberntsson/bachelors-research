from py2neo import Relationship, Graph, Node, Path

# connect to authenticated graph database
graph = Graph("http://neo4j:kandidat@46.101.235.47:7474/db/data/")


def initData():
	clearData()
	
	tx = graph.begin()
	
	# Users
	users = []
	for x in range(50):
		users.append(Node("User", username="user_"+str(x)))
		tx.create(users[x])
	
	# Events & Races
	events = []
	for x in range(10):
		events.append(Node("Event", name="event_"+str(x)))
		tx.create(events[x])
		for y in range(5):
			nbr = x + 5 + y
			race = Node("Race", name="race_"+str(nbr))
			tx.create(race)
			tx.create(Relationship(race, "IN", events[x]))
			# Coordinates
			coord1 = Node("Coordinate", lat=33, lng=44)
			coord2 = Node("Coordinate", lat=33.1, lng=44.1)
			coord3 = Node("Coordinate", lat=33.2, lng=44.2)
			tx.create(Path(race, "STARTS_AT", coord1, "FOLLOWED_BY", coord2, "FOLLOWED_BY", coord3, "END_FOR", race))
		
		tx.create(Relationship(events[x], "MADE_BY", users[x*5]))
	
	tx.commit()

def clearData():
	# Dangerous
	graph.delete_all();
	
if __name__ == '__main__':
    initData()