# from confluent_kafka import Producer, KafkaError
import json
import time
import random
import numpy as np
from google.cloud import pubsub_v1
import os
import base64
from google.cloud import aiplatform
from google.cloud.aiplatform.gapic.schema import predict

# Read arguments and configurations and initialize
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/home/cloudcomputinggroups1/modern-kiln-382923-a71e500b3025.json"
project_id = "modern-kiln-382923"
topic_id = "image_input"
filename = "/home/cloudcomputinggroups1/car.txt"
client = pubsub_v1.PublisherClient()
topic_path = client.topic_path(project_id, topic_id)


while (True):
    try:
        # profile_name = profileNames[random.randint(0, 2)];
        # profile = DEVICE_PROFILES[profile_name]
        # # get random values within a normal distribution of the value
        # temp = max(0, np.random.normal(profile['temp'][0], profile['temp'][1]))
        # humd = max(0, min(np.random.normal(profile['humd'][0], profile['humd'][1]), 100))
        # pres = max(0, np.random.normal(profile['pres'][0], profile['pres'][1]))

        # # create dictionary
        # msg={"time": time.time(), "profile_name": profile_name, "temperature": temp,"humidity": humd, "pressure":pres};

        # #randomly eliminate some measurements
        # for i in range(3):
        #     if(random.randrange(0,10)<1):
        #         choice=random.randrange(0,3)
        #         if(choice==0):
        #             msg['temperature']=None;
        #         elif (choice==1):
        #             msg['humidity']=None;
        #         else:
        #             msg['pressure']=None;
        # record_key=record_key+1;
        # record_value=json.dumps(msg, indent=2).encode('utf-8');
        # print(record_value)

        with open(filename, "rb") as f:
            file_content = f.read()
        msg = {'car': str(file_content)}
        # The format of each instance should conform to the deployed model's prediction input schema.
        
        instance = json.dumps(msg,indent=2).encode('utf-8')
        #print(instance)
        api_future = client.publish(topic_path, instance)
        message_id = api_future.result()
        print(f"Published to {topic_path} : {message_id}")
        time.sleep(10)
    except KeyboardInterrupt:
        break
