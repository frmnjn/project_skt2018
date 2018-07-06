#!/usr/bin/python
from cassandra.cluster import Cluster 
import json
import csv

#cluster = Cluster()

cluster = Cluster(['192.168.43.95', '192.168.43.250'])
session = cluster.connect('cobates') 

data={}
 

rows = session.execute('SELECT * FROM namatabel') 

for row in rows:
    data[str(row.id)]={"status":str(row.status),"waktu":str(row.waktu)} 

print(json.dumps(data))

