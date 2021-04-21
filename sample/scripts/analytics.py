import datetime
import utils
import os
import shutil
import pyspark
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from azure.storage.blob import BlobClient
from pathlib import Path
from cloudpathlib import CloudPath, AzureBlobClient

os.makedirs(f"quote/trade_dt/{utils.trade_dt_05}")
os.makedirs(f"quote/trade_dt/{utils.trade_dt_06}")
os.makedirs(f"trade/trade_dt/{utils.trade_dt_05}")
os.makedirs(f"trade/trade_dt/{utils.trade_dt_06}")

# Initialize Spark
spark = SparkSession.builder.getOrCreate()

# Initialize CloudPath
client = AzureBlobClient(connection_string=utils.azure_conn)

# Read parquet and create dataframe
root = client.CloudPath(f"az://{utils.azure_container}/quote/trade_dt/{utils.trade_dt_05}")

for path in root.glob("**/*.parquet"):

    azure_path = str(path)[28:]
    azure_file = f"{str(path.stem)}{str(path.suffix)}"

    blob = BlobClient.from_connection_string(conn_str=utils.azure_conn, container_name=utils.azure_container, blob_name=azure_path) 

    with open(f"quote/trade_dt/{utils.trade_dt_05}/{azure_file}", "wb") as my_blob:
        blob_data = blob.download_blob()
        blob_data.readinto(my_blob)

quote_05 = spark.read.format("parquet").load(f"quote/trade_dt/{utils.trade_dt_05}")

# Read parquet and create dataframe
root = client.CloudPath(f"az://{utils.azure_container}/quote/trade_dt/{utils.trade_dt_06}")

for path in root.glob("**/*.parquet"):

    azure_path = str(path)[28:]
    azure_file = f"{str(path.stem)}{str(path.suffix)}"

    blob = BlobClient.from_connection_string(conn_str=utils.azure_conn, container_name=utils.azure_container, blob_name=azure_path) 

    with open(f"quote/trade_dt/{utils.trade_dt_06}/{azure_file}", "wb") as my_blob:
        blob_data = blob.download_blob()
        blob_data.readinto(my_blob)

quote_06 = spark.read.format("parquet").load(f"quote/trade_dt/{utils.trade_dt_06}")

# Join dataframes to create one dataframe
quote_05.union(quote_06).createOrReplaceTempView("quotes")

# Retrieve individual fields
quote = spark.sql("select trade_dt, rec_type, stock_symbol, stock_exchange, latest_quote, event_seq_nb, bid_pr, bid_size, ask_pr, ask_size from quotes")

# Create temp table
quote.createOrReplaceTempView("tmp_quote")

# Create dataframe including last 30 minute avg quote price
mov_avg_df = spark.sql("""select trade_dt, rec_type, stock_symbol, stock_exchange, latest_quote, event_seq_nb, 
    bid_pr, 
    avg(bid_pr) over(partition by stock_symbol order by latest_quote range between interval '30' minutes preceding and current row) as mov_avg_bid_pr,
    bid_size,
    ask_pr,
    avg(ask_pr) over(partition by stock_symbol order by latest_quote range between interval '30' minutes preceding and current row) as mov_avg_ask_pr,
    ask_size
    from tmp_quote
""")

# Create temp table
mov_avg_df.createOrReplaceTempView("temp_quote_moving_avg")

# Retrieve yesterday's date
current_date = datetime.datetime.strptime(utils.trade_dt_06, "%Y-%m-%d")
previous_date = current_date - datetime.timedelta(1)

# Retrieve individual fields based off yesterday's date
previous_date_quote = spark.sql("select stock_symbol, stock_exchange, latest_quote, event_seq_nb, bid_pr, ask_pr from quotes where trade_dt = '{}'".format(previous_date))

# Create temp table
previous_date_quote.createOrReplaceTempView("previous_date_quote")

# Create dataframe
temp_last_quote = spark.sql("""select stock_symbol as symbol, last_bid_pr, last_ask_pr from
(select stock_symbol,
 last_value(bid_pr) over (partition by stock_symbol order by latest_quote rows between unbounded preceding and unbounded following) as last_bid_pr,
 last_value(ask_pr) over (partition by stock_symbol order by latest_quote rows between unbounded preceding and unbounded following) as last_ask_pr
 from temp_quote_moving_avg)
 """).distinct()

# Read parquet and create dataframe
root = client.CloudPath(f"az://{utils.azure_container}/trade/trade_dt/{utils.trade_dt_05}")

for path in root.glob("**/*.parquet"):

    azure_path = str(path)[28:]
    azure_file = f"{str(path.stem)}{str(path.suffix)}"

    blob = BlobClient.from_connection_string(conn_str=utils.azure_conn, container_name=utils.azure_container, blob_name=azure_path) 

    with open(f"trade/trade_dt/{utils.trade_dt_05}/{azure_file}", "wb") as my_blob:
        blob_data = blob.download_blob()
        blob_data.readinto(my_blob)

trade_05 = spark.read.format("parquet").load(f"trade/trade_dt/{utils.trade_dt_05}")

# Read parquet and create dataframe
root = client.CloudPath(f"az://{utils.azure_container}/trade/trade_dt/{utils.trade_dt_06}")

for path in root.glob("**/*.parquet"):

    azure_path = str(path)[28:]
    azure_file = f"{str(path.stem)}{str(path.suffix)}"

    blob = BlobClient.from_connection_string(conn_str=utils.azure_conn, container_name=utils.azure_container, blob_name=azure_path) 

    with open(f"trade/trade_dt/{utils.trade_dt_06}/{azure_file}", "wb") as my_blob:
        blob_data = blob.download_blob()
        blob_data.readinto(my_blob)

trade_06 = spark.read.format("parquet").load(f"trade/trade_dt/{utils.trade_dt_06}")

# Create temp table
trade_05.union(trade_06).createOrReplaceTempView("trades")

# Create custom schema
common_schema = StructType([StructField("trade_dt", DateType(), True),
                            StructField("rec_type", StringType(), True),
                            StructField("stock_symbol", StringType(), True),
                            StructField("event_tm", TimestampType(), True),
                            StructField("event_seq_nb", IntegerType(), True),
                            StructField("stock_exchange", StringType(), True),
                            StructField("bid_pr", FloatType(), True),
                            StructField("bid_size", IntegerType(), True),
                            StructField("ask_pr", FloatType(), True),
                            StructField("ask_size", IntegerType(), True),
                            StructField("trade_pr", FloatType(), True), 
                            StructField("mov_avg_bid_pr", StringType(), True),
                            StructField("mov_avg_ask_pr", StringType(), True)])

# Create dataframe
quote_trade_union = spark.sql("""
select trade_dt,
       rec_type,
       stock_symbol, 
       latest_quote as event_tm,
       event_seq_nb,
       stock_exchange,
       bid_pr,
       bid_size,
       ask_pr,
       ask_size,
       null as trade_pr,
       mov_avg_bid_pr,
       mov_avg_ask_pr
from temp_quote_moving_avg
union
select trade_dt,
       rec_type,
       stock_symbol,
       latest_trade as event_tm,
       event_seq_nb,
       stock_exchange,
       null as bid_pr,
       null as bid_size,
       null as ask_pr,
       null as ask_size,
       trade_pr,
       null as mov_avg_bid_pr,
       null as mov_avg_ask_pr
from trades""")

# Apply custom schema to dataframe
quote_trade_union = spark.createDataFrame(quote_trade_union.rdd, common_schema)

# Create temp table
quote_trade_union.createOrReplaceTempView("quote_trade_union")

quote_trade_union_update = spark.sql("""
select *,
last_value(trade_pr) over(partition by stock_symbol) as last_trade_pr
from quote_trade_union
""")

# Create temp table
quote_trade_union_update.createOrReplaceTempView("quote_trade_union_update")

# Filter out records != 'Q'
quote_trade_update = spark.sql("""

select *
from quote_trade_union_update
where rec_type = 'Q'

""")

# Join tables using stock symbol
temp_table = quote_trade_update.join(broadcast(temp_last_quote), quote_trade_update.stock_symbol == temp_last_quote.symbol, how="left")

# Create temp table
temp_table.createOrReplaceTempView("temp_table")

# Create final dataframe
quote_final = spark.sql("""
select *
from temp_table
""")

quote_final.show(20)

# Clean up
shutil.rmtree("trade")
shutil.rmtree("quote")