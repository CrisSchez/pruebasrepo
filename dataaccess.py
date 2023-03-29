!pip3 install -r requirements.txt


# 1.- USING SPARK - DATALAKE

import os
import sys


from pyspark.sql import SparkSession
from pyspark.sql.types import *

import cml.data_v1 as cmldata

CONNECTION_NAME = "csrxdatalake"
conn = cmldata.get_connection(CONNECTION_NAME)
spark = conn.get_spark_session()

# Sample usage to run query through spark
EXAMPLE_SQL_QUERY = "show databases"
spark.sql(EXAMPLE_SQL_QUERY).show()
telco_churn_spark=spark.sql("select * from default.telco_churn")

telco_churn_spark.printSchema()

telcoiceberg=spark.read.format("iceberg").load("spark_catalog.default.telco_iceberg")

# 1.1.- SPARK TO PANDAS 
telco_churn_df=telco_churn_spark.toPandas()

# Now we can read in the data from Cloud Storage into Spark...

storage = 's3a://csrenvbucketue1'

schema = StructType(
    [
        StructField("customerid", StringType(), True),
        StructField("gender", StringType(), True),
        StructField("seniorcitizen", StringType(), True),
        StructField("partner", StringType(), True),
        StructField("dependents", StringType(), True),
        StructField("tenure", DoubleType(), True),
        StructField("phoneservice", StringType(), True),
        StructField("multiplelines", StringType(), True),
        StructField("internetservice", StringType(), True),
        StructField("onlinesecurity", StringType(), True),
        StructField("onlinebackup", StringType(), True),
        StructField("deviceprotection", StringType(), True),
        StructField("techsupport", StringType(), True),
        StructField("streamingtv", StringType(), True),
        StructField("streamingmovies", StringType(), True),
        StructField("contract", StringType(), True),
        StructField("paperlessbilling", StringType(), True),
        StructField("paymentmethod", StringType(), True),
        StructField("monthlycharges", DoubleType(), True),
        StructField("totalcharges", DoubleType(), True),
        StructField("churn", StringType(), True)
    ]
)

telco_churn_s3 = spark.read.csv(
    "{}/datalake/data/churn/WA_Fn-UseC_-Telco-Customer-Churn-.csv".format(
        storage),
    header=True,
    schema=schema,
    sep=',',
    nullValue='NA'
)

# ...and inspect the data.
spark.sql("drop table if exists telco_churn_s3")
telco_churn_s3.show()
if ('telco_churn_s3' not in list(spark.sql("show tables in default").toPandas()['tableName'])):
    print("creating the telco_churn_s3 database")
    telco_churn_s3\
        .write.format("parquet")\
        .mode("overwrite")\
        .saveAsTable(
            'default.telco_churn_s3'
        )
telco_churn_s3.printSchema()





# 2.- USING IMPYLA - DATA WAREHOUSE
    
    
    
from impala.dbapi import connect
from impala.util import as_pandas


from cmlbootstrap import CMLBootstrap
# Set the setup variables needed by CMLBootstrap
HOST = os.getenv("CDSW_API_URL").split(
    ":")[0] + "://" + os.getenv("CDSW_DOMAIN")
USERNAME = os.getenv("CDSW_PROJECT_URL").split(
    "/")[6]  # args.username  # "vdibia"
API_KEY = os.getenv("CDSW_API_KEY") 
PROJECT_NAME = os.getenv("CDSW_PROJECT")  

# Instantiate API Wrapper
cml = CMLBootstrap(HOST, USERNAME, API_KEY, PROJECT_NAME)

variables=cml.get_environment_variables()
USERNAME=variables['PROJECT_OWNER']
uservariables=cml.get_user()
USERPASS=uservariables['environment']['WORKLOAD_PASSWORD']
variables
# Connect to Impala using Impyla
# Secure clusters will require additional parameters to connect to Impala.
# Recommended: Specify IMPALA_HOST as an environment variable in your project settings
IMPALA_HOST=IMPALA_HOST='coordinator-csr-sandbox-impala-cdw.dw-csrenv.a465-9q4k.cloudera.site'
IMPALA_PORT='443'
#jdbc:impala://coordinator-ClouderaEssencesHue.dw-demo-cloudera-forum-cdp-env.djki-j7ns.cloudera.site:443/default;AuthMech=3;transportMode=http;httpPath=cliservice;ssl=1;UID=cristina.sanchez;PWD=PASSWORD

conn = connect(host=IMPALA_HOST,
               port=IMPALA_PORT,
               auth_mechanism='LDAP',
               user=USERNAME,
               password=USERPASS,
               use_http_transport=True,
               http_path='/cliservice',
               use_ssl=True)
cursor = conn.cursor()
cursor.execute("select * from default.uic_data")

# Execute using SQL


conn.close()
