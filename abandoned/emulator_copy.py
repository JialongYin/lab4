# Import SDK packages
from AWSIoTPythonSDK.MQTTLib import AWSIoTMQTTClient
import time
import json
import pandas as pd
import numpy as np


#TODO 1: modify the following parameters
#Starting and end index, modify this
device_st = 0
device_end = 1

#Path to the dataset, modify this
data_path = "./data2/vehicle{}.csv"

#Path to your certificates, modify this
certificate_formatter = "./certificates/device_{}.certificate.pem"
key_formatter = "./certificates/device_{}.private.pem"


class MQTTClient:
    def __init__(self, device_id, cert, key, data_i):
        # For certificate based connection
        self.device_id = str(device_id)
        self.state = 0
        self.data = data_i.iloc[:50]
        self.index = 0
        self.cardinality = len(self.data)
        self.client = AWSIoTMQTTClient(self.device_id)
        #TODO 2: modify your broker address
        self.client.configureEndpoint("ayafsxbunp2us-ats.iot.us-east-1.amazonaws.com", 8883)
        self.client.configureCredentials("./AmazonRootCA1.pem", key, cert)
        self.client.configureOfflinePublishQueueing(-1)  # Infinite offline Publish queueing
        self.client.configureDrainingFrequency(2)  # Draining: 2 Hz
        self.client.configureConnectDisconnectTimeout(10)  # 10 sec
        self.client.configureMQTTOperationTimeout(5)  # 5 sec
        self.client.onMessage = self.customOnMessage



    def customOnMessage(self,message):
        #TODO3: fill in the function to show your received message
        print("client {} received payload {} from topic {}".format(self.device_id, message.payload, message.topic))

    def customCallback(self, client, userdata, message):
        self.state = json.loads(message.payload)["max_emission"]
        print("client {} max CO2 emission so far {} from topic {}".format(self.device_id, self.state, message.topic))



    # Suback callback
    def customSubackCallback(self,mid, data):
        #You don't need to write anything here
        pass


    # Puback callback
    def customPubackCallback(self,mid):
        #You don't need to write anything here
        pass


    def publish(self):
        #TODO4: fill in this function for your publish
        if self.index < self.cardinality:
            Payload = self.data.iloc[self.index].to_json()
            self.index += 1
            self.client.publish("myTopic", Payload, 0)
            self.client.subscribe("myTopic{}".format(self.device_id), 0, callback=self.customCallback)
            return False
        else:
            return True

print("Loading vehicle data...")
data = []
for i in range(device_end):
    a = pd.read_csv(data_path.format(i))
    data.append(a)

print("Initializing MQTTClients...")
clients = []
for device_id in range(device_st, device_end):
    print(device_id)
    client = MQTTClient(device_id,certificate_formatter.format(device_id,device_id), key_formatter.format(device_id,device_id), data[device_id])
    client.client.connect()
    clients.append(client)


while True:
    Done = True
    for i,c in enumerate(clients):
        res = c.publish()
        time.sleep(3)
        if not res:
            Done = False
    if Done:
        break

for c in clients:
    c.client.disconnect()
print("All devices disconnected")
exit()
