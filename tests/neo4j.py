from py2neo import Relationship, Graph, Node, Path
import datetime

# connect to authenticated graph database
graph = Graph("http://neo4j:kandidat@46.101.235.47:7474/db/data/")


def initData(model):
	clearData()
	
	if (model == "raceone"):
		initRaceOneData()
	elif (model == "skim"):
		initSkimData()
	elif (model == "reddit"): 
		initRedditData()
	else:
		print("No data for " + model)
	
def initRaceOneData():
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

def initSkimData():
	tx = graph.begin()
	
	# Users
	users = []
	for x in range(50):
		users.append(Node("User", username="user_"+str(x), email="user_"+str(x)+"@mail.com"))
		tx.create(users[x])
	
	# Projects and images
	projects = []
	for x in range(8):
		projects.append(Node("Project", name="project_"+str(x)))
		tx.create(projects[x])
		tx.create(Relationship(projects[x], "COLLABORATOR", users[x*2]))
		tx.create(Relationship(projects[x], "COLLABORATOR", users[x*3]))
		tx.create(Relationship(projects[x], "COLLABORATOR", users[x*4]))
		for y in range(4):
			nbr = x + 5 + y
			image = Node("Image", name="image_"+str(nbr), height="100", width="100", extension="png", createdAt=str(datetime.datetime.now()))
			tx.create(image)
			tx.create(Relationship(image, "IN", projects[x]))
			# Coordinates
			nbr = x + 5 + y
			image = Node("Image", name="innerimage_"+str(nbr), height="100", width="100", extension="png", createdAt=str(datetime.datetime.now()))
			tx.create(image)
			for z in range(2):
				comment = Node("Comment", text="Haha, cool image", createdAt=str(datetime.datetime.now()))
				tx.create(comment)
				tx.create(Relationship(comment, "ON", image))
				tx.create(Relationship(comment, "MADE_BY", users[x*2]))
			sku = Node("SKU", name="sku_"+str(nbr))
			tx.create(sku)
			tx.create(Relationship(sku, "IN", projects[x]))
			tx.create(Relationship(image, "BELONGS_TO", sku))
			for z in range(5):
				row = Node("Row", header="header_"+str(z), value=str(z))
				tx.create(row)
				tx.create(Relationship(row, "OF", sku))
		
	tx.commit()
	
def initRedditData():
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
    initData("skim")