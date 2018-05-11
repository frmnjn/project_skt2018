# Import library paho mqtt
import paho.mqtt.client as mqtt_client

# Inisiasi client mqtt sebagai subscriber
sub = mqtt_client.Client()

# Koneksikan ke broker
sub.connect("192.168.43.236", 1883)

# Fungsi untuk handle message yang masuk
def handle_message(mqttc, obj, msg):
    # Dapatkan topik dan payloadnya
    topic = msg.topic
    print(topic)
    payload = json.loads(msg.payload)  # you can use json.loads to convert string to json
    print(json.dumps(payload))

# Daftarkan fungsinya untuk event on_message
sub.on_message = handle_message

sub.publish("/test", "30")

# Subscribe ke sebuah topik
sub.subscribe("/sensor/#")

# Loop forever
sub.loop_forever()