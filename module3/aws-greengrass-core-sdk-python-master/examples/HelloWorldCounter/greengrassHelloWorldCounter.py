#
# Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
#

# greengrassHelloWorldCounter.py
# Demonstrates a simple publish to a topic using Greengrass core sdk
# This lambda function will retrieve underlying platform information and send a hello world message along with the
# platform information to the topic 'hello/world/counter' along with a counter to keep track of invocations.
#
# This Lambda function requires the AWS Greengrass SDK to run on Greengrass devices.
# This can be found on the AWS IoT Console.

import json
import logging
import platform
import sys
import time
import base64
import gzip

import greengrasssdk

# Setup logging to stdout
logger = logging.getLogger(__name__)
logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)

# Creating a greengrass core sdk client
client = greengrasssdk.client("iot-data")

veh_max = dict()
def function_handler(event, context):
    global veh_max
    try:
        CO2 = float(event["vehicle_CO2"])
        veh_i = event["vehicle_id"]
        if veh_i not in veh_max:
            veh_max[veh_i] = CO2
        else:
            veh_max[veh_i] = max(CO2, veh_max[veh_i])

        topicBack = "myTopic{}".format(int(veh_i[3]))
        a = gzip.compress(json.dumps({"max_emission": "{}".format(veh_max[veh_i])}).encode('utf-8'))
        pay_load = base64.b64encode(a)
        client.publish(
            topic=topicBack,
            queueFullPolicy="AllOrException",
            payload=pay_load
        )
    except Exception as e:
        logger.error("Failed to publish message: " + repr(e))
        en = str(type(event)) + "Failed to publish message: " + repr(e)
        client.publish(
                topic="myTopic4",
                queueFullPolicy="AllOrException",
                payload=en
            )
    # time.sleep(20)
    return
