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

os.mkdir("partition=T")
os.mkdir("partition=Q")

# Initialize Spark
spark = SparkSession.builder.getOrCreate()

# Initialize CloudPath
client = AzureBlobClient(connection_string=utils.azure_conn)
root = client.CloudPath(f"az://{utils.azure_container}/output_dir/partition=T")

for path in root.glob("**/*.parquet"):

    azure_path = str(path)[28:]
    azure_file = f"{str(path.stem)}{str(path.suffix)}"

    blob = BlobClient.from_connection_string(conn_str=utils.azure_conn, container_name=utils.azure_container, blob_name=azure_path) 

    with open(f"partition=T/{azure_file}", "wb") as my_blob:
        blob_data = blob.download_blob()
        blob_data.readinto(my_blob)

# Read parquet and create dataframe
trade_common = spark.read.format("parquet").load("partition=T")

# Retrieve relevant fields
trade = trade_common.select("trade_dt", "rec_type", "symbol", "exchange", "event_tm", "event_seq_nb", "arrival_tm", "trade_pr")

trade_corrected = utils.apply_latest(trade)

# Separate dataframes by trade date
trade_corrected_05 = trade_corrected.where(trade_corrected.trade_dt == utils.trade_dt_05)
trade_corrected_06 = trade_corrected.where(trade_corrected.trade_dt == utils.trade_dt_06)

# Write to Azure blob in parquet format
trade_corrected_05.write.parquet(f"trade/trade_dt={utils.trade_dt_05}")

temp_path = Path(f"trade/trade_dt={utils.trade_dt_05}")

for temp_file in temp_path.glob("*.parquet"):

    blob = BlobClient.from_connection_string(conn_str=utils.azure_conn, container_name=utils.azure_container, blob_name=f"trade/trade_dt/{utils.trade_dt_05}/{temp_file.stem}{temp_file.suffix}") 

    with open(temp_file, "rb") as file:
        blob.upload_blob(file) 

# Write to Azure blob in parquet format
trade_corrected_06.write.parquet(f"trade/trade_dt={utils.trade_dt_06}")

temp_path = Path(f"trade/trade_dt={utils.trade_dt_06}")

for temp_file in temp_path.glob("*.parquet"):

    blob = BlobClient.from_connection_string(conn_str=utils.azure_conn, container_name=utils.azure_container, blob_name=f"trade/trade_dt/{utils.trade_dt_06}/{temp_file.stem}{temp_file.suffix}") 

    with open(temp_file, "rb") as file:
        blob.upload_blob(file)

root = client.CloudPath(f"az://{utils.azure_container}/output_dir/partition=Q")

for path in root.glob("**/*.parquet"):

    azure_path = str(path)[28:]
    azure_file = f"{str(path.stem)}{str(path.suffix)}"

    blob = BlobClient.from_connection_string(conn_str=utils.azure_conn, container_name=utils.azure_container, blob_name=azure_path) 

    with open(f"partition=Q/{azure_file}", "wb") as my_blob:
        blob_data = blob.download_blob()
        blob_data.readinto(my_blob)

# Read parquet and create dataframe
quote_common = spark.read.format('parquet').load("partition=Q")

# Retrieve relevant fields
quote = quote_common.select("trade_dt", "rec_type", "symbol", "exchange", "event_tm", "event_seq_nb", "arrival_tm", "bid_pr", "bid_size", "ask_pr", "ask_size")

quote_corrected = utils.apply_latest(quote)

# Separate dataframes by trade date
quote_corrected_05 = quote_corrected.where(quote_corrected.trade_dt == utils.trade_dt_05)
quote_corrected_06 = quote_corrected.where(quote_corrected.trade_dt == utils.trade_dt_06)

# Write to Azure blob in parquet format
quote_corrected_05.write.parquet(f"quote/trade_dt={utils.trade_dt_05}")

temp_path = Path(f"quote/trade_dt={utils.trade_dt_05}")

for temp_file in temp_path.glob("*.parquet"):

    blob = BlobClient.from_connection_string(conn_str=utils.azure_conn, container_name=utils.azure_container, blob_name=f"quote/trade_dt/{utils.trade_dt_05}/{temp_file.stem}{temp_file.suffix}") 

    with open(temp_file, "rb") as file:
        blob.upload_blob(file)

# Write to Azure blob in parquet format
quote_corrected_06.write.parquet(f"quote/trade_dt={utils.trade_dt_06}")

temp_path = Path(f"quote/trade_dt={utils.trade_dt_06}")

for temp_file in temp_path.glob("*.parquet"):

    blob = BlobClient.from_connection_string(conn_str=utils.azure_conn, container_name=utils.azure_container, blob_name=f"quote/trade_dt/{utils.trade_dt_06}/{temp_file.stem}{temp_file.suffix}") 

    with open(temp_file, "rb") as file:
        blob.upload_blob(file)

# Clean up
shutil.rmtree("partition=Q")
shutil.rmtree("partition=T")
shutil.rmtree("trade")
shutil.rmtree("quote")