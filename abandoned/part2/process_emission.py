import json
import logging
import sys

import greengrasssdk

# Logging
logger = logging.getLogger(__name__)
logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)

# SDK Client
client = greengrasssdk.client("iot-data")

# Counter
# my_counter = 0
veh_max = dict()
def lambda_handler(event, context):
    global veh_max
    try:
        #TODO1: Get your data
        CO2 = float(event["vehicle_CO2"])
        veh_i = event["vehicle_id"]

        #TODO2: Calculate max CO2 emission
        if veh_i not in veh_max:
            veh_max[veh_i] = CO2
        else:
            veh_max[veh_i] = max(CO2, veh_max[veh_i])

        #TODO3: Return the result
        client.publish(
            topic="myTopic{}".format(veh_i[3:]),
            payload=json.dumps(
                {"max_emission": "{}".format(veh_max[veh_i])}
            ),
        )
    except Exception as e:
        logger.error("Failed to publish message: " + repr(e))
        en = str(type(event)) + "Failed to publish message: " + repr(e)
        client.publish(
                topic="myTopic".format(veh_i[3:]),
                queueFullPolicy="AllOrException",
                payload=en
            )
    time.sleep(20)
    return
