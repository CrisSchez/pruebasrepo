import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.types import *



spark = SparkSession\
    .builder\
    .appName("PythonSQL")\
    .master("local[*]")\
    .getOrCreate()


import cml.data_v1 as cmldata

CONNECTION_NAME = "se-aw-dl"
conn = cmldata.get_connection(CONNECTION_NAME)
spark = conn.get_spark_session()

# Sample usage to run query through spark
EXAMPLE_SQL_QUERY = "show databases"
spark.sql(EXAMPLE_SQL_QUERY).show()

# Show the data in the hive table
spark.sql("select * from default.telco_churn").show()
!pip3 install pandas
# To get more detailed information about the hive table you can run this:
df = spark.sql("SELECT * FROM default.telco_churn").toPandas()

dfFemale=df[df['gender']=='Female']
