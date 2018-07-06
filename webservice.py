#!/usr/bin/python
from cassandra.cluster import Cluster 
from flask import Flask, request
import json

#cluster = Cluster()

cluster = Cluster(['192.168.43.250', '192.168.43.96'])
session = cluster.connect('cobates') 



#data.append('sensor':[])
 
app = Flask("App")



@app.route('/data', methods=['GET'])
def handle_get():
    #Konversi dari list/dictionary ke string format JSON
    rows = session.execute('SELECT * FROM namatabel') 
    data={'sensor':[]}
    for row in rows:
   	    if row.status=='bahaya':
 	           data['sensor'].append({'status':row.status,'waktu':row.waktu}) 

    return json.dumps(data)


@app.route('/spark', methods=['GET'])
def get_spark():
     #Konversi dari list/dictionary ke string format JSON
     rows = session.execute('SELECT * FROM spark') 
     data_spark={'spark':[]}

     for row in rows:
	 
         data_spark['spark'].append({'rAh':row.rah,'jAsB':row.jasb}) 

     return json.dumps(data_spark)

app.run('192.168.43.171',port=7777)

