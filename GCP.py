#!/usr/bin/env python
# coding: utf-8

# In[38]:


from gcloud import storage
#from json2table import convert
import pandas as pd
import json
import psycopg2
import sys


# In[39]:


# Imports the Google Cloud client library
#from google.cloud import storage

# Instantiates a client
storage_client = storage.Client()

# The name for the new bucket
bucket_name = "johnsbucket2021"

# Creates the new bucket
#bucket = storage_client.create_bucket(bucket_name)

#print("Bucket {} created.".format(bucket.name))


# In[40]:


def upload_blob(bucket_name, source_file_name, destination_blob_name):
    """Uploads a file to the bucket."""

    #storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    blob.upload_from_filename(source_file_name)

    print(
        "File {} uploaded to {}.".format(
            source_file_name, destination_blob_name
        )
    )


# In[41]:


source_file_name='/Users/johnsippy/Documents/johnproject/emp.json'
destination_blob_name='emp.json'
upload_blob(bucket_name, source_file_name, destination_blob_name)


# In[42]:


# get bucket data as blob
bucket = storage_client.bucket(bucket_name)
blob = bucket.get_blob('emp.json')
# convert to string
json_data = json.loads(blob.download_as_string(client=None))


# In[43]:


print(json_data)


# In[44]:


#json_object = {"key" : "value"}
#build_direction = "LEFT_TO_RIGHT"
#table_attributes = {"style" : "width:100%"}
#html = convert(json_object, build_direction=build_direction, table_attributes=table_attributes)
#print(html)


# In[111]:


#df = pd.DataFrame.from_dict(json_data, orient='columns')
#pd.json_normalize(json_data)
dataset=pd.DataFrame.from_dict(pd.json_normalize(json_data), orient='columns').rename(columns={"address.streetAddress": "Street", "address.city": "City","address.state":"State","address.postalCode":"postalCode","phoneNumbers.type":"phonetype","phoneNumbers.number":"phonenumber"})


# In[112]:


print(dataset)


# In[ ]:


print(dataset.firstName[0])
print(dataset.lastName[0]) 
print(dataset.gender[0])
print(dataset.age[0])
print(dataset.Street[0])
print(dataset.City[0])
print(dataset.State[0])
print(dataset.postalCode[0])
print(dataset.phonetype[0])
print(dataset.phonenumber[0])


# In[140]:


#Enter the values for you database connection
connstr = pd.read_csv('/Users/johnsippy/Documents/johnproject/conn.csv', sep=",", header=None)


# In[141]:


df=pd.DataFrame(connstr)


# In[142]:


dsn_database=(df[0][0])
dsn_hostname=(df[1][0])
dsn_port=str(df[2][0])
dsn_uid=(df[3][0])
dsn_pwd=(df[4][0])


# In[154]:


try:
    conn_string = "host="+dsn_hostname+" port="+dsn_port+" dbname="+dsn_database+" user="+dsn_uid+" password="+dsn_pwd
    # print("Connecting to database\n	->%s" % (conn_string))
    conn=psycopg2.connect(conn_string)
    print("Connected!\n")
except:
    print("Unable to connect to the database.")


# In[155]:


cur=conn.cursor()


# In[156]:


cur.execute("insert into master.employees values('"+dataset.firstName[0]+"','"
      +dataset.lastName[0]+"','"+dataset.gender[0]+"','"+str(dataset.age[0])+"','"+dataset.phonetype[0]+"','"
      +str(dataset.phonenumber[0])+"','"
      +dataset.Street[0]+"','"+dataset.City[0]+"','"+dataset.State[0]+"','"
      +str(dataset.postalCode[0])+"')")


# In[157]:


cur.execute("commit")


# In[ ]:




