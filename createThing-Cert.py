import boto3

import json
################################################### Create random name for things
import random
import string

################################################### Parameters for Thing
thingArn = ''
thingId = ''
defaultPolicyName = 'My_Iot_Policy'
###################################################
thingGroupArn = 'arn:aws:iot:us-east-1:152442585149:thinggroup/myThingGroup'

def createThing(i):
    global thingClient
    thingName = "demo{}_".format(i)+''.join([random.choice(string.ascii_letters + string.digits) for n in range(15)]) # demo
    thingResponse = thingClient.create_thing(
                      thingName = thingName
                        )
    # print(thingResponse)
    data = json.loads(json.dumps(thingResponse, sort_keys=False, indent=4))
    for element in data:
        if element == 'thingArn':
            thingArn = data['thingArn']
        elif element == 'thingId':
            thingId = data['thingId']
            createCertificate(thingName, i)
    response = thingClient.add_thing_to_thing_group(
                thingGroupName='myThingGroup',
                thingGroupArn=thingGroupArn,
                thingName=thingName,
                thingArn=thingArn,
                overrideDynamicGroups=True
    )

def createCertificate(thingName, i):
    global thingClient
    certResponse = thingClient.create_keys_and_certificate(
                setAsActive = True
    )
    data = json.loads(json.dumps(certResponse, sort_keys=False, indent=4))
    for element in data:
        if element == 'certificateArn':
            certificateArn = data['certificateArn']
        elif element == 'keyPair':
            PublicKey = data['keyPair']['PublicKey']
            PrivateKey = data['keyPair']['PrivateKey']
        elif element == 'certificatePem':
            certificatePem = data['certificatePem']
        elif element == 'certificateId':
            certificateId = data['certificateId']

    with open('./certificates_create/device_{}.public.pem'.format(i), 'w') as outfile: # certificates_create    certificates
        outfile.write(PublicKey)
    with open('./certificates_create/device_{}.private.pem'.format(i), 'w') as outfile:
        outfile.write(PrivateKey)
    with open('./certificates_create/device_{}.certificate.pem'.format(i), 'w') as outfile:
        outfile.write(certificatePem)

    response = thingClient.attach_policy(
            policyName = defaultPolicyName,
            target = certificateArn
    )
    response = thingClient.attach_thing_principal(
            thingName = thingName,
            principal = certificateArn
     )

thingClient = boto3.client('iot')
for i in range(50): # 50
    createThing(i)
    print("Thing {} created".format(i))
