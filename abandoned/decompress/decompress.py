import base64
import gzip
import json
import logging
import sys
import greengrasssdk

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)
streamHandler = logging.StreamHandler(stream=sys.stdout)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
streamHandler.setFormatter(formatter)
logger.addHandler(streamHandler)

def decode_to_bytes(e):
    return base64.b64decode(e)

def decompress_to_string(binary_data):
    return gzip.decompress(binary_data).decode('utf-8')

def lambda_handler(event, context):
    try:
        # Creating a greengrass core sdk client
        client = greengrasssdk.client("iot-data")
        
        logger.info("event before processing: {}".format(event))

        decompressed_data = []

        for e in event:
            binary_data = decode_to_bytes(e)
            decompressed_string = decompress_to_string(binary_data)

            decompressed_data.append(json.loads(decompressed_string))

        logger.info("event after processing: {}".format(decompressed_data))

    except Exception as e:
        logger.error("Failed to publish message: " + repr(e))
        en = str(type(event)) + "Failed to publish message: " + repr(e)
        client.publish(
                topic="myTopic4",
                queueFullPolicy="AllOrException",
                payload=en
            )

    return decompressed_data
