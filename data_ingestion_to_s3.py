import requests
import boto3
import uuid
import json
import datetime
import os
#import random
from botocore.exceptions import ClientError
import logging

def upload_file(file_name, bucket, object_name=None):
    if object_name is None:
        object_name = file_name
    s3_client = boto3.client('s3')
    response = s3_client.upload_file(file_name, bucket, object_name)
#    try:
#        response = s3_client.upload_file(file_name, bucket, object_name)
#    except ClientError as e:
#        logging.error(e)
#        return False
#    return True

bucket_name ="sudeshrandom"

#partition_key = str(uuid.uuid4())
while True:
    count=0
    file_name=datetime.datetime.now().strftime("%Y%m%d_%H%M%S")+'_random_stream.json'
    fname = open(file_name,'w')
    fname.write('[')
    while count<=500:
        r = requests.get('https://randomuser.me/api/?exc=login')
        data = json.dumps(r.json())
        count=count+1
        if count <= 500:
            fname.write("[%s],\n" %data)
        else:
            fname.write("[%s]\n" %data)
    fname.write(']')
    fname.close()
    key="new/"+file_name
    response = upload_file(file_name,bucket_name ,key)
    os.remove(file_name)



