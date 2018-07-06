#!/usr/bin/python
# Import library paho mqtt
import paho.mqtt.client as mqtt_client
import json
from cassandra.cluster import Cluster


# Inisiasi client mqtt sebagai subscriber
sub = mqtt_client.Client()

# Koneksikan ke broker
sub.connect("192.168.43.236", 1883)

cluster = Cluster(['192.168.43.171'])
session = cluster.connect('dev')
i=0

# 2 Fungsi untuk handle message yang masuk
def handle_message(mqttc, obj, msg):
    # Dapatkan topik dan payloadnya
    topic = msg.topic
    print(topic)
    payload = json.loads(msg.payload)  # you can use json.loads to convert string to json
    # print(json.dumps(payload))
    waktu = payload['date']+" "+payload['time']
    status = ""
    if(payload['peer'] == "0"):
        status = "aman"
    else:
        status = "bahaya"

    print(waktu)
    print(status)


    global session
    global i
    
    session.execute('INSERT INTO data (id,status,waktu) VALUES ('+str(i)+',\''+str(status)+'\',\''+str(waktu)+'\');')
    #session.execute(
        #'INSERT INTO namatabel (id,status,waktu) VALUES (8888,"aman","tanggal 30");')
    i=i+1

# Daftarkan fungsinya untuk event on_message
sub.on_message = handle_message

# Subscribe ke sebuah topik
sub.subscribe("/sensor/#")

# Loop forever
sub.loop_forever()
