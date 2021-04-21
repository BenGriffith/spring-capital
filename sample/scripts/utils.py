import json
import pyspark
from pyspark.sql.functions import *
from datetime import datetime
from cloudpathlib import CloudPath, AzureBlobClient

# Azure
azure_storage = ""
azure_container = ""
azure_key = ""
azure_conn = ""

# Supporting variables
trade_dt_05 = "2020-08-05"
trade_dt_06 = "2020-08-06"

# Supporting functions
def get_azure_directory_file(azure_client, path, file_type):
    """
    Retrieve Azure file path and file name
    """
    
    root = azure_client.CloudPath(path)

    for path in root.glob(f"**/*.{file_type}"):

        return str(path)[28:], f"{str(path.stem)}{str(path.suffix)}"


def parse_csv(line):
    """
    Parse CSV file according to define schema
    """

    record_type_pos = 2
    record = line.split(",")

    try:
        if record[record_type_pos] == 'T':
              return (datetime.strptime(record[0], '%Y-%m-%d').date(), 
                      record[2], 
                      record[3], 
                      record[6], 
                      datetime.strptime(record[4], '%Y-%m-%d %H:%M:%S.%f'), 
                      int(record[5]), 
                      datetime.strptime(record[1], '%Y-%m-%d %H:%M:%S.%f'), 
                      float(record[7]), 
                      None, 
                      None, 
                      None, 
                      None, 
                      record[2])
        elif record[record_type_pos] == 'Q':
              return (datetime.strptime(record[0], '%Y-%m-%d').date(), 
                      record[2], 
                      record[3], 
                      record[6], 
                      datetime.strptime(record[4], '%Y-%m-%d %H:%M:%S.%f'), 
                      int(record[5]), 
                      datetime.strptime(record[1], '%Y-%m-%d %H:%M:%S.%f'), 
                      None, 
                      float(record[7]), 
                      int(record[8]), 
                      float(record[9]), 
                      int(record[10]), 
                      record[2])
    except:
        return (None, None, None, None, None, None, None, None, None, None, None, None, 'B')


def parse_json(line):
    """
    Parse JSON file according to define schema
    """

    line = json.loads(line)
    record_type = line['event_type']

    try:
        if record_type == 'T':
              return (datetime.strptime(line['trade_dt'], '%Y-%m-%d').date(), 
                      record_type, 
                      line['symbol'], 
                      line['exchange'], 
                      datetime.strptime(line['event_tm'], '%Y-%m-%d %H:%M:%S.%f'), 
                      line['event_seq_nb'], 
                      datetime.strptime(line['file_tm'], '%Y-%m-%d %H:%M:%S.%f'), 
                      line['price'], 
                      None, 
                      None, 
                      None, 
                      None, 
                      record_type)
        elif record_type == 'Q':
              return (datetime.strptime(line['trade_dt'], '%Y-%m-%d').date(), 
                      record_type, 
                      line['symbol'], 
                      line['exchange'], 
                      datetime.strptime(line['event_tm'], '%Y-%m-%d %H:%M:%S.%f'), 
                      line['event_seq_nb'], 
                      datetime.strptime(line['file_tm'], '%Y-%m-%d %H:%M:%S.%f'), 
                      None, 
                      line['bid_pr'], 
                      line['bid_size'], 
                      line['ask_pr'], 
                      line['ask_size'], 
                      record_type)
    except:
        return (None, None, None, None, None, None, None, None, None, None, None, None, 'B')


def apply_latest(df):
  
    if df.first()["rec_type"] == "T":
    
        # Group records based on latest trade date
        df_grouped = df.groupBy("trade_dt", "rec_type", "symbol", "arrival_tm", "event_seq_nb").agg(max("event_tm").alias("latest_trade"))
    
        # Join with original dataframe to retrieve exchange and trade_pr for latest trade date
        df_joined = df_grouped.join(df.select("event_tm", "exchange", "trade_pr"), df.event_tm == df_grouped.latest_trade, "inner")

        # Retrieve relevant fields
        df_final = df_joined.select("trade_dt", "rec_type", col("symbol").alias("stock_symbol"), col("exchange").alias("stock_exchange"), "latest_trade", "event_seq_nb", "arrival_tm", "trade_pr").orderBy("trade_dt", "symbol", "event_seq_nb")

        return df_final
  
    elif df.first()["rec_type"] == "Q":
  
        # Group records based on latest trade date
        df_grouped = df.groupBy("trade_dt", "rec_type", "symbol", "arrival_tm", "event_seq_nb").agg(max("event_tm").alias("latest_quote"))
    
        # Join with original dataframe to retrieve exchange, bid_pr, bid_size, ask_pr and ask_size
        df_joined = df_grouped.join(df.select("event_tm", "exchange", "bid_pr", "bid_size", "ask_pr", "ask_size"), df.event_tm == df_grouped.latest_quote, "inner")
    
        # Retrieve relevant fields
        df_final = df_joined.select("trade_dt", "rec_type", col("symbol").alias("stock_symbol"), col("exchange").alias("stock_exchange"), "latest_quote", "event_seq_nb", "arrival_tm", "bid_pr", "bid_size", "ask_pr", "ask_size").orderBy("trade_dt", "symbol", "event_seq_nb")
    
        return df_final