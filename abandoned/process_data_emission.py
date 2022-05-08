# Import SDK packages
from AWSIoTPythonSDK.MQTTLib import AWSIoTMQTTClient
import time
import json
import pandas as pd
import numpy as np

import base64
import gzip
import logging
import sys


#TODO 1: modify the following parameters
#Starting and end index, modify this
device_st = 0
device_end = 5

#Path to the dataset, modify this
data_path = "./data2/vehicle{}.csv"
data_save_path = "./data2/vehicle_maxEmission{}.csv"


print("Loading vehicle data...")
for i in range(device_end):
    df = pd.read_csv(data_path.format(i))
    data = [""]
    max_emission = 0
    for index, row in df.iterrows():
        emission_cur = row["vehicle_CO2"]
        if emission_cur > max_emission:
            max_emission = emission_cur
        data.append(max_emission)
    df_save = pd.DataFrame(data, columns=["max_emission"])
    df_save.to_csv(data_save_path.format(i), index=False)
