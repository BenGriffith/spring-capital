import utils
import pyspark
import os
import shutil
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from azure.storage.blob import BlobClient
from pathlib import Path
from cloudpathlib import CloudPath, AzureBlobClient

# Initialize Spark
spark = SparkSession.builder.getOrCreate()

# Initialize CloudPath
client = AzureBlobClient(connection_string=utils.azure_conn)

# Create custom schema
common_event = StructType([StructField('trade_dt', DateType(), True),
                           StructField('rec_type', StringType(), True),
                           StructField('symbol', StringType(), True),
                           StructField('exchange', StringType(), True),
                           StructField('event_tm', TimestampType(), True),
                           StructField('event_seq_nb', IntegerType(), True),
                           StructField('arrival_tm', TimestampType(), True),
                           StructField('trade_pr', FloatType(), True),
                           StructField('bid_pr', FloatType(), True),
                           StructField('bid_size', IntegerType(), True),
                           StructField('ask_pr', FloatType(), True),
                           StructField('ask_size', IntegerType(), True),
                           StructField('partition', StringType(), True)])

# Load from Azure storage
azure_path, azure_file1 = utils.get_azure_directory_file(client, f"az://{utils.azure_container}/data/csv/{utils.trade_dt_05}/NYSE", "txt")

blob = BlobClient.from_connection_string(conn_str=utils.azure_conn, container_name=utils.azure_container, blob_name=azure_path)

with open(azure_file1, "wb") as my_blob:
    blob_data = blob.download_blob()
    blob_data.readinto(my_blob)

# Load from Azure storage
raw_csv_05 = spark.sparkContext.textFile(azure_file1)

# Parse csv data
parsed_csv_05 = raw_csv_05.map(lambda line: utils.parse_csv(line))

# Create dataframe
csv_data_05 = spark.createDataFrame(parsed_csv_05, schema=common_event)

# Load from Azure storage
azure_path, azure_file2 = utils.get_azure_directory_file(client, f"az://{utils.azure_container}/data/csv/{utils.trade_dt_06}/NYSE", "txt")

blob = BlobClient.from_connection_string(conn_str=utils.azure_conn, container_name=utils.azure_container, blob_name=azure_path)

with open(azure_file2, "wb") as my_blob:
    blob_data = blob.download_blob()
    blob_data.readinto(my_blob)

raw_csv_06 = spark.sparkContext.textFile(azure_file2)

# Parse csv data
parsed_csv_06 = raw_csv_06.map(lambda line: utils.parse_csv(line))

# Create dataframe
csv_data_06 = spark.createDataFrame(parsed_csv_06, schema=common_event)

# Join csv dataframes
csv_data = csv_data_05.union(csv_data_06)

# Load from Azure storage
azure_path, azure_file3 = utils.get_azure_directory_file(client, f"az://{utils.azure_container}/data/json/{utils.trade_dt_05}/NASDAQ", "txt")

blob = BlobClient.from_connection_string(conn_str=utils.azure_conn, container_name=utils.azure_container, blob_name=azure_path)

with open(azure_file3, "wb") as my_blob:
    blob_data = blob.download_blob()
    blob_data.readinto(my_blob)

raw_json_05 = spark.sparkContext.textFile(azure_file3)

# Parse json data
parsed_json_05 = raw_json_05.map(lambda line: utils.parse_json(line))

# Create dataframe
json_data_05 = spark.createDataFrame(parsed_json_05, schema=common_event)

# Load Azure storage
azure_path, azure_file4 = utils.get_azure_directory_file(client, f"az://{utils.azure_container}/data/json/{utils.trade_dt_06}/NASDAQ", "txt")

blob = BlobClient.from_connection_string(conn_str=utils.azure_conn, container_name=utils.azure_container, blob_name=azure_path)

with open(azure_file4, "wb") as my_blob:
    blob_data = blob.download_blob()
    blob_data.readinto(my_blob)

raw_json_06 = spark.sparkContext.textFile(azure_file4)

# Parse json data
parsed_json_06 = raw_json_06.map(lambda line: utils.parse_json(line))

# Create dataframe
json_data_06 = spark.createDataFrame(parsed_json_06, schema=common_event)

# Join json dataframes
json_data = json_data_05.union(json_data_06)

# Join csv and json dataframes
data = csv_data.union(json_data)

# Write to Azure storage
data.write.partitionBy("partition").parquet("temp")

temp_path = Path("temp")

for temp_file in temp_path.glob("*/*.parquet"):

    blob = BlobClient.from_connection_string(conn_str=utils.azure_conn, container_name=utils.azure_container, blob_name=f"output_dir/{temp_file.parent.stem}/{temp_file.stem}{temp_file.suffix}") 

    with open(temp_file, "rb") as file:
        blob.upload_blob(file)  

# Clean up
os.remove(azure_file1)
os.remove(azure_file2)
os.remove(azure_file3)
os.remove(azure_file4)
shutil.rmtree(temp_path)