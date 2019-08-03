# -*- coding: utf-8 -*-
"""
Created on Sat Aug  3 12:48:55 2019

@author: sudesh.amarnath
"""

import boto3
import os
import glob
import findspark
findspark.init('/home/ubuntu/spark-2.1.1-bin-hadoop2.7')
import pyspark
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('test').getOrCreate()
from pyspark.sql.functions import concat_ws,concat,lit,Column,regexp_replace

def upload_file(file_name, bucket, object_name=None):
    if object_name is None:
        object_name = file_name
    s3_client = boto3.client('s3')
    response = s3_client.upload_file(file_name, bucket, object_name)
s3 = boto3.resource('s3')
my_bucket = s3.Bucket('sudeshrandom')

for file in my_bucket.objects.all():
    if ".json" in file.key:
        file_name=file.key
        fname=file_name.replace('new/','')
        with open(fname,'w') as f:
            wfile_path='/home/ubuntu/processed/'+fname.replace('.json','')
            obj = my_bucket.Object(file.key)
            f.write(obj.get()['Body'].read().decode('utf-8'))
            df= spark.read.option("multiLine", "true").json(fname)
            df.select(concat_ws("",df['info.seed']).alias("id"),\
                        concat(concat_ws("",df['results.name.first']),lit(' '),concat_ws("",df['results.name.last'])).alias('Full_Name'),\
                        concat_ws("",df['results.gender']).alias("Gender"),\
                        concat_ws("",df['results.dob.date']).astype('date').alias("DoB"),\
                        concat_ws("",df['results.email']).alias("Email"),\
                        concat_ws("",df['results.phone']).alias("home_phone"),\
                        concat_ws("",df['results.cell']).alias("cell_phone"),\
                        concat_ws("",df['results.location.street']).alias("Street"),\
                        concat_ws("",df['results.location.city']).alias("City"),\
                        concat_ws("",df['results.location.state']).alias("State"),\
                        concat_ws("",df['results.nat']).alias("Country"),\
                        concat_ws("",df['results.location.postcode']).astype('int').alias("Postcode"),\
                        concat_ws("",df['results.location.coordinates.latitude']).alias("Latitude"),\
                        concat_ws("",df['results.location.coordinates.longitude']).alias("Longitude")         
                        ).coalesce(1).write.option("mapreduce.fileoutputcommitter.marksuccessfuljobs","false").option("header","true").csv(wfile_path)
            os.remove(fname)
            
            for s3_file in glob.glob(wfile_path+'/*.csv'):
                cname='processed/'+fname.replace('.json','.csv')
                upload_file(s3_file,'sudeshrandom',cname)
           
