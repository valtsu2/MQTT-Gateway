import paho.mqtt.client as mqttClient
from loguru import logger
import json
from datetime import datetime
import time
import sys
import wirepas_mesh_messaging as wmm

content = ""
send_content = False

class Broker:

    def __init__(self, address, port, topic, client_id, username, password, tls_enabled):
        self.address = address
        self.port = port
        self.topic = topic 
        self.client_id = client_id
        self.username = username
        self.password = password
        self.tls_enabled = tls_enabled
        self.client = mqttClient.Client
        self.connected = False

    def __del__(self):
        self.client.disconnect()
        self.client.loop_stop()        

    def connect(self):

        def on_connect(client, userdata, flags, rc):
            while(self.connected == False):
                if rc == 0:
                    logger.info(f"{self.address}:{self.port} found...")
                    self.connected = True
                else:
                    time.sleep(1.5)
                    logger.info("Trying to reach the broker...")
                time.sleep(1)

 
        self.client = mqttClient.Client(self.client_id)

        if self.tls_enabled:
            self.client.tls_set_context(context=None)                       #Enables SSL/TLS support
            self.client.tls_insecure_set(True)                      #insecure tls connection when set to True (certificate not working currently at IoT-ticket side)

        self.client.username_pw_set(self.username,self.password)   #comment if testing on local broker without credentials
        self.client.on_connect = on_connect
        self.client.connect(self.address, self.port)
        self.client.loop_start() 

    def subscribe(self):

        def on_message(client, userdata, msg):
            global content, send_content
            content = msg.payload
            send_content = True

            uplink_message = wmm.ReceivedDataEvent.from_payload(content)

            text = str(uplink_message.payload)
            payload = text.split(" ",1 )[1]
            logger.info(payload[1:4])
            if payload[1:4] == "x01":
                logger.info("received data")
                logger.info(payload)

            elif payload[1:4] == "xf7":
                logger.info("received diagnostic")
                logger.info(payload)


                #diagnostic_data = {
                #    "src_addr" : 
                #}

            else:
                logger.info("received unknown data")
                logger.info(payload)

            

            #logger.info("MQTT topic: " + msg.topic + "\n")
            
        self.client.subscribe(self.topic)
        self.client.on_message = on_message
        
    def publish(self):
        global content, send_content
        if send_content:
            send_content = False
            with open('data.json','r') as json_file:
                msg = str(json.load(json_file))
                msg = msg.replace("\'","\"") #Json load converts the double quatation marks into singular, this reverts it
                self.client.publish(self.topic,msg)
                logger.success("message published to cloud broker")

    def disconnect(self):
        self.client.disconnect()
        self.client.loop_stop()
        logger.success(f"{self.address}:{self.port} disconnected")


def prepare_json(msg):

    timestamp = str(datetime.utcnow())
    timestamp = timestamp[:10] + "T" + timestamp[11:]
    timestamp += "Z"
    data =  {
        "t": [
            {
                "n": "test_data",
                "dt": "double",
                "unit": " ",
                "data": [
                        {
                            timestamp: msg
                        }
                        ]
                    }
                ]
            }
            
    with open("data.json","w") as json_file:
        json.dump(data,json_file)

def initialize_logger():
    logger.remove()
    fmt="{time:DD.MM.YYYY - HH:mm:ss:SS}|| {level} || {message}"
    logger.add(sys.stderr, format=fmt)
    file_name = "{time:DD.MM.YYYY_HH:mm:ss}.log"
    logger.add(file_name, rotation="50 MB", format=fmt)
    
def main():

    initialize_logger()

    try:
        
        broker_address = "localhost"  #for testing locally
        port = 1883
        topic ="gw-event/received_data/#"
        client_id = "subscriber"
        username = ""
        password = ""

        local_broker = Broker(broker_address,port,topic,client_id,username,password, False)
        local_broker.connect()

        while local_broker.connected != True:    #Wait for connection
            time.sleep(1)
            logger.info("Waiting for local broker to ack connection...")
        logger.success("Local broker operational")
        local_broker.subscribe()

        while(True):
            time.sleep(1)
    except KeyboardInterrupt:
        local_broker.disconnect()


if __name__ == '__main__':
    main()