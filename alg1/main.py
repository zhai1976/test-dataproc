from google.cloud import bigquery
import datetime

from pyspark.sql import SparkSession # <<< ADD THIS IMPORT

# ADD THIS BLOCK to define the 'spark' variable
# It either gets the existing SparkSession created by Dataproc 
# or creates a new one if it doesn't exist.
spark = SparkSession.builder.appName("DataProcessingJob").getOrCreate()

client=bigquery.Client(location='us-central1')
query_sql="""
SELECT distinct city, state_name, current_timestamp() as query_time 
FROM `ping-project-471703.test.arizona_cities` 
where county_name='Coconino' and city like 'M%'
"""

print("Starting query.....")
start_read_time=datetime.datetime.now()
query_job=client.query(query=query_sql)
data=query_job.to_dataframe()
print("------------type of data")
print(type(data))                     #get <class 'pandas.core.frame.DataFrame'>  bcz GCP use Pandas connector
df=spark.createDataFrame(data)        # Now you have to convert to pySpark DF. spark SC can be directly used.
df.show(100)
print(type(df))

print("--------done with main.py job")
