import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame
import re

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node airport_dim
airport_dim_node1744845612978 = glueContext.create_dynamic_frame.from_catalog(database="airline-datamart", table_name="dev_airlines_airports_dim", redshift_tmp_dir="s3://temp-bucket-gluejob/airport_dim/", transformation_ctx="airport_dim_node1744845612978")

# Script generated for node daily_flight_data
daily_flight_data_node1744844932063 = glueContext.create_dynamic_frame.from_catalog(database="airline-datamart", table_name="daily_flights", transformation_ctx="daily_flight_data_node1744844932063")

# Script generated for node Filter
Filter_node1744845044772 = Filter.apply(frame=daily_flight_data_node1744844932063, f=lambda row: (row["depdelay"] >= 60), transformation_ctx="Filter_node1744845044772")

# Script generated for node Join
Join_node1744854514276 = Join.apply(frame1=Filter_node1744845044772, frame2=airport_dim_node1744845612978, keys1=["originairportid"], keys2=["airport_id"], transformation_ctx="Join_node1744854514276")

# Script generated for node modify_depart_airport_columns
modify_depart_airport_columns_node1744854678758 = ApplyMapping.apply(frame=Join_node1744854514276, mappings=[("depdelay", "long", "dep_delay", "bigint"), ("arrdelay", "long", "arr_delay", "bigint"), ("destairportid", "long", "destairportid", "long"), ("carrier", "string", "carrier", "string"), ("city", "string", "dep_city", "string"), ("name", "string", "dep_airport", "string"), ("state", "string", "dep_state", "string")], transformation_ctx="modify_depart_airport_columns_node1744854678758")

# Script generated for node Join_for_Arrival_airport
Join_for_Arrival_airport_node1744855011405 = Join.apply(frame1=modify_depart_airport_columns_node1744854678758, frame2=airport_dim_node1744845612978, keys1=["destairportid"], keys2=["airport_id"], transformation_ctx="Join_for_Arrival_airport_node1744855011405")

# Script generated for node modify_arrival_airport_columns
modify_arrival_airport_columns_node1744855062401 = ApplyMapping.apply(frame=Join_for_Arrival_airport_node1744855011405, mappings=[("dep_delay", "bigint", "dep_delay", "bigint"), ("arr_delay", "bigint", "arr_delay", "bigint"), ("carrier", "string", "carrier", "string"), ("dep_city", "string", "dep_city", "string"), ("dep_airport", "string", "dep_airport", "string"), ("dep_state", "string", "dep_state", "string"), ("city", "string", "arr_city", "string"), ("name", "string", "arr_airport", "string"), ("state", "string", "arr_state", "string")], transformation_ctx="modify_arrival_airport_columns_node1744855062401")

# Script generated for node Amazon Redshift
AmazonRedshift_node1744874601780 = glueContext.write_dynamic_frame.from_options(frame=modify_arrival_airport_columns_node1744855062401, connection_type="redshift", connection_options={"redshiftTmpDir": "s3://aws-glue-assets-805358685644-us-east-2/temporary/", "useConnectionProperties": "true", "dbtable": "airlines.daily_flights_fact", "connectionName": "Redshift_airline_datawarehouse", "preactions": "CREATE TABLE IF NOT EXISTS airlines.daily_flights_fact (dep_delay VARCHAR, arr_delay VARCHAR, carrier VARCHAR, dep_city VARCHAR, dep_airport VARCHAR, dep_state VARCHAR, arr_city VARCHAR, arr_airport VARCHAR, arr_state VARCHAR);"}, transformation_ctx="AmazonRedshift_node1744874601780")

job.commit()