from cassandra.cluster import Cluster
from hdfs import InsecureClient

client= InsecureClient('http://192.168.1.49:50070')
cluster = Cluster(['192.168.1.51', '192.168.1.50','192.168.1.48'])
session = cluster.connect('cobates')
sql = ""

with client.read('/hasil_dummy_rata/part-00000') as reader:
	rata2 = reader.read()
with client.read('/hasil_dummy/part-00000') as reader:
	pencarian = reader.read()
	a=session.execute("select count(id) as id from spark")
	#print(a[0].id)
	session.execute("INSERT INTO spark (id,jasb,rah) values (%s,%s,%s);",(a[0].id+1,pencarian,rata2))
