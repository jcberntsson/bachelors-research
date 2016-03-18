from cassandra.cluster import Cluster
cluster = Cluster(contact_points=['192.168.33.12',])

session = cluster.connect()

print("done")