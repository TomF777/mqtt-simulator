from paho.mqtt import client as mqtt_client
import time
import json
import random
from datetime import datetime, timezone
from threading import Thread


class MqttClient():
    
    def __init__(self, client_id, broker, port):
        """
        
        """
        
        self.client = mqtt_client.Client(mqtt_client.CallbackAPIVersion.VERSION2, client_id, protocol=mqtt_client.MQTTv5)
        self.client.username_pw_set("mqttbroker", "IoT@2023")
        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message
        self.client.connect(broker, port)
        self.read_topic = None
        self.read_payload = None
        
    def on_message(self, client, userdata, message):
        print(message.topic, "  ", message.payload)
        
    def on_connect(self, mqttc, userdata, flags, rc, props):
        if rc == 0:
            print(" Connected to MQTT Broker")
        else:
            print(f" Failed to connect to MQTT Broker, error code: {rc}")
            if rc==1:
                    print("1: Connection refused - incorrect protocol version")
            elif rc==2:
                    print("2: Connection refused - invalid client identifier")
            elif rc==3: 
                    print("3: Connection refused - server unavailable")
            elif rc==4: 
                    print("4: Connection refused - bad username or password")
            elif rc==5: 
                    print("5: Connection refused - not authorised")
        time.sleep(2)
            
    def start(self):
        self.client.loop_start
        
    def publish_message(self, topic, json):
        self.client.publish(topic, json)
        
    def read_message(self):
        return self.read_topic, self.read_payload
    
    def close(self):
        self.client.loop_stop()
        self.client.disconnect()
        
   
def vib_sim(avg_vib_level, interval, mqtt_client, topic):
    
    while True:
        number = random.randint(1, 5)
        vib_level = avg_vib_level - 2.5 + number
        timestamp = datetime.now(timezone.utc).timestamp()
        json_send = { "vib": vib_level, "ts":timestamp}
        mqtt_client.publish_message(topic, json.dumps(json_send))
        time.sleep(interval)
        
        
def temp_humid_sim_1(avg_temp_level, avg_humid_level, mqtt_client, topic):
    
    while True:
        num1 = random.randint(1, 20)
        num2 = random.randint(1, 20)
        
        temp_level = avg_temp_level - 2 + num1 * 0.1
        humid_level = avg_humid_level - 10 + num2
        timestamp = datetime.now(timezone.utc).timestamp()
        json_send = { "temp": temp_level, "humid": humid_level, "ts":timestamp}
        mqtt_client.publish_message(topic, json.dumps(json_send))
        time.sleep(1)
        
    
        
json_send = {
                "vib1": 23,
                "vib2": 45,
                "temp1": 23.5,
                "humid2": 77
            }

mqtt_client = MqttClient("IoT_Industrial", "192.168.1.165", 1883)
mqtt_client.start()

  
vib_sim_1 = Thread(target=vib_sim, name="vib_sim_1", args = (20, 1, mqtt_client, "IoT_Industrial/Vib1" ))
vib_sim_2 = Thread(target=vib_sim, name="vib_sim_2", args = (30, 0.2, mqtt_client, "IoT_Industrial/Vib2" ))
vib_sim_3 = Thread(target=vib_sim, name="vib_sim_3", args = (30, 2, mqtt_client, "IoT_Industrial/Vib3" ))

temp_humid_sim_1 = Thread(target=temp_humid_sim_1, name="temp_humid_sim_1", args = (24, 50, mqtt_client, "IoT_Industrial/TempHumid1" ))

vib_sim_1.start()
vib_sim_2.start()
vib_sim_3.start()
temp_humid_sim_1.start()

