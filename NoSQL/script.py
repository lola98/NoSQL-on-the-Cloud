#!/usr/bin/env python
# coding: utf-8

# In[40]:


import boto3
import os

# create an s3 instance object
s3 = boto3.resource('s3',
                    aws_access_key_id=os.getenv('AWS_ACCESS_KEY'),
                    aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY') 
                   )


# In[42]:


# create a busket
try:
    s3.create_bucket(Bucket='14848-hw3-bucket', CreateBucketConfiguration={
        'LocationConstraint': 'us-west-1'}) 
except Exception as e:
    print (e)
    
bucket = s3.Bucket("14848-hw3-bucket")
bucket.Acl().put(ACL='public-read')


# In[43]:


# upload experiments.csv into busket
body = open('datasets/experiments.csv', 'rb')
o = s3.Object('14848-hw3-bucket', 'experiments.csv').put(Body=body)
s3.Object('14848-hw3-bucket', 'experiments.csv').Acl().put(ACL='public-read')


# In[44]:


# create dynamo db resource
dyndb = boto3.resource('dynamodb',
                       region_name='us-west-1',
                       aws_access_key_id=os.getenv('AWS_ACCESS_KEY'),
                       aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY') 
)


# In[45]:


# create table in dynamo db
try:
    table = dyndb.create_table(
        TableName='DataTable',
        KeySchema=[
            {
                'AttributeName': 'PartitionKey',
                'KeyType': 'HASH'
            }, {
                'AttributeName': 'RowKey',
                'KeyType': 'RANGE'
            }
        ],
        AttributeDefinitions=[
            {
                'AttributeName': 'PartitionKey',
                'AttributeType': 'S'
            }, {
                'AttributeName': 'RowKey',
                'AttributeType': 'S'
            },
        ],
        ProvisionedThroughput={
            'ReadCapacityUnits': 5,
            'WriteCapacityUnits': 5
        }
)
except Exception as e:
    print (e)
    table = dyndb.Table("DataTable")
    
table.meta.client.get_waiter('table_exists').wait(TableName='DataTable')


# In[46]:


import csv

# columns in experiments.csv: Id, Temp, Conductivity, Concentration, URL
with open('datasets/experiments.csv', 'rt') as csvfile: 
    csvf = csv.reader(csvfile, delimiter=',', quotechar='|')
    
    # skips the first row of the CSV file.
    next(csvf)
    
    for item in csvf:
        print (item)
        
        # upload exp1, 2, 3 data to s3 bucket
        body = open('datasets/'+item[4], 'rb') 
        s3.Object('14848-hw3-bucket', item[4]).put(Body=body )
        md = s3.Object('14848-hw3-bucket', item[4]).Acl().put(ACL='public-read')

        url = "https://s3-us-west-1.amazonaws.com/14848-hw3-bucket/" + item[4] 

        
        # put items in the table
        partition_key = item[4].split('.csv')[0]
        metadata_item = {'PartitionKey': partition_key, 'RowKey': item[0], 
                     'temp' : item[1], 'conductivity' : item[2], 'concentration' : item[3], 'url':url}
        
        try: 
            table.put_item(Item=metadata_item)
        except:
            print ("item may already be there or another failure")


# In[47]:


print("Fetch PartitionKey exp1")
response = table.get_item(
    Key={
        'PartitionKey': 'exp1',
        'RowKey': '1'
    }
)
item = response['Item'] 

print(item)
print()
print(response)
print()


# In[48]:


print("Fetch PartitionKey exp2 \n")
response = table.get_item(
    Key={
        'PartitionKey': 'exp2',
        'RowKey': '2'
    }
)
item = response['Item'] 

print(item)
print()
print(response)
print()


# In[49]:


print("Fetch PartitionKey exp3 \n")
response = table.get_item(
    Key={
        'PartitionKey': 'exp3',
        'RowKey': '3'
    }
)
item = response['Item'] 

print(item)
print()
print(response)
print()

