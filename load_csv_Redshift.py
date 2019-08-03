import boto3
import psycopg2
conn=psycopg2.connect(dbname= 'dev',host='examplecluster.cay7bypxb9oq.us-east-1.redshift.amazonaws.com',port= '5439', user= 'awsuser', password= 'Nihalsp2240')
cur = conn.cursor()
s3 = boto3.resource('s3')

## Bucket to use
bucket = s3.Bucket('sudeshrandom')

## List objects within a given prefix
for obj in bucket.objects.filter(Delimiter='/', Prefix='processed/'):
  if ".csv" in obj.key:
    sql = """copy people from 's3://sudeshrandom/{}'
                iam_role 'arn:aws:iam::671650857215:role/myRedshiftRole'
                ignoreheader 2
                null as 'NA'
                delimiter ','""".format(obj.key)
    cur.execute(sql)
    conn.commit()
