from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DoubleType, BooleanType
from pyspark.sql.functions import col
from graphframes import GraphFrame

spark = SparkSession.builder \
    .appName("Transactions") \
    .getOrCreate()

spark.getActiveSession().sparkContext.setCheckpointDir("Runs/")

# Converted the original header to:
# Timestamp,From_Bank,From_Account,To_Bank,To_Account,Amount_Received,Receiving_Currency,Amount_Paid,Payment_Currency,Payment_Format,Is_Laundering
schema = StructType([
    StructField("Timestamp", TimestampType(), True),
    StructField("From_Bank", StringType(), True),
    StructField("From_Account", StringType(), True),
    StructField("To_Bank", StringType(), True),
    StructField("To_Account", StringType(), True),
    StructField("Amount_Received", DoubleType(), True),
    StructField("Receiving_Currency", StringType(), True),
    StructField("Amount_Paid", DoubleType(), True),
    StructField("Payment_Currency", StringType(), True),
    StructField("Payment_Format", StringType(), True),
    StructField("Is_Laundering", BooleanType(), True)
])

transactions_df = spark.read.csv("/home/mrrabbit/Downloads/ibm/HI-Large_trans_new.csv",
                                 header=True,
                                 schema=schema)

transactions_df = transactions_df.sample(False, 0.1, seed=42) # For testing locally

from_accounts = transactions_df.select(col("From_Account").alias("id")).distinct()
to_accounts = transactions_df.select(col("To_Account").alias("id")).distinct()
vertices = from_accounts.union(to_accounts).distinct()

edges = transactions_df.select(col("From_Account").alias("src"),
                               col("To_Account").alias("dst"),
                               col("Amount_Paid").alias("amount"),
                               col("Is_Laundering").alias("loundering"))

graph = GraphFrame(vertices, edges)

page_rank_results = graph.pageRank(resetProbability=0.15, maxIter=2)

page_rank_results.withColumnRenamed("id", ":ID(Account)").write.csv("page_ranks_with_data", header=True)
edges.withColumnRenamed(
    "src", ":START_ID(Account)").withColumnRenamed(
    "dst", ":END_ID(Account)").write.csv("edges",
                                         header=True)
vertices.withColumnRenamed("id", ":ID(Account)").write.csv("vertices",
                                                           header=True)

spark.stop()