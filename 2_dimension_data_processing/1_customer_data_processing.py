# Databricks notebook source
from pyspark.sql import functions as f
from delta.tables import DeltaTable

# COMMAND ----------

# MAGIC %run /Workspace/consolidated_pipeline/1_setup_folder/utilities

# COMMAND ----------

print(bronze_schema, silver_schema, gold_schema)

# COMMAND ----------

dbutils.widgets.text("catalog", "fmcg", "Catalog")
dbutils.widgets.text("data_source", "customers", "Data Source")

# COMMAND ----------

catalog = dbutils.widgets.get("catalog")
data_source = dbutils.widgets.get("data_source")

print(catalog, data_source)
# DBTITLE 1

# COMMAND ----------

base_path = f's3://sportsbar-dp-kaushal/{data_source}/*csv'
print(base_path)

# COMMAND ----------

df = (
    spark.read.format("csv")
    .option("header", True)
    .option("inferSchema", True)
    .load(base_path)
    .withColumn("read_timestamp", f.current_timestamp())
    .select("*", "_metadata.file_name", "_metadata.file_size")
)

display(df.limit(10))

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df.write\
    .format("delta")\
    .option("delta.enableChangeDataFeed","true")\
    .mode("overwrite")\
    .saveAsTable(f"{catalog}.{bronze_schema}.{data_source}")

# COMMAND ----------

# MAGIC %md
# MAGIC ###  SILVER PROCESSING
# MAGIC

# COMMAND ----------

df_bronze = spark.sql(f"SELECT * FROM {catalog}.{bronze_schema}.{data_source}")
display(df_bronze.limit(10))

# COMMAND ----------

df_duplicates = df_bronze.groupBy("customer_id").count().filter(f.col("count")>1)
df_duplicates.show()

# COMMAND ----------

df_silver= df_bronze.dropDuplicates(['customer_id'])
df_duplicates.show()

# COMMAND ----------


print("rows defore dropduplicates: ", df_bronze.count())
print("rows after dropduplicates: ", df_silver.count())

# COMMAND ----------

df_silver.filter(f.col("customer_name") != f.trim(f.col("customer_name"))).show()

# COMMAND ----------

df_silver = df_silver.withColumn("customer_name", f.trim(f.col("customer_name")))
df_silver.filter(f.col("customer_name") != f.trim(f.col("customer_name"))).show()

# COMMAND ----------

df_silver.select(f.col("city")).distinct().show()

# COMMAND ----------

#typos --> correct names

city_mapping = {
    'Bengalore':'Bengaluru',
    'Bengaluruu':'Bengaluru',

    'Hyderabadd':'Hyderabad',
    'Hyderbad':'Hyderabad',

    'NewDelhi':'New Delhi',
    'NewDheli':'New Delhi',
    'NewDelhee':'New Delhi'
}

allowed = ['Bengaluru','Hyderabad','New Delhi']




# COMMAND ----------

# DBTITLE 1,Apply city mapping and validation
df_silver = (
    df_silver
    .replace(city_mapping, subset=["city"])
    .withColumn(
        "city",
        f.when(f.col("city").isNull(), None)
        .when(f.col("city").isin(allowed), f.col("city"))
        .otherwise(None)
    )
)

# COMMAND ----------

# DBTITLE 1,Verify cleaned city values
df_silver.select(f.col("city")).distinct().show()

# COMMAND ----------

df_silver.select("customer_name").distinct().show()


# COMMAND ----------

df_silver = df_silver.withColumn(
    "customer_name",
    f.when(f.col("customer_name").isNull(),None)
    .otherwise(f.initcap("customer_name"))
)

# COMMAND ----------

df_silver.select("customer_name").distinct().show()


# COMMAND ----------

 df_silver.filter(f.col("city").isNull()).show()

# COMMAND ----------

null_customer_names = [ "Sprintx Nutrition", "Zenathlete Foods", "Primefuel Nutrition", "Recovery Lane" ]
df_silver.filter(f.col("customer_name").isin(null_customer_names)).show()

# COMMAND ----------

23
# Business Confirmation Note: City corrections confirmed by business team
customer_city_fix = {
# Sprintx Nutrition
789403: "New Delhi",
# Xenathlete Foods
789420: "Bengaluru",
# Primefuel Nutrition
789521: "Hyderabad",
# Recovery Lane
789603: "Hyderabad"
}

df_fix = spark.createDataFrame(
[(k, v) for k, v in customer_city_fix.items()],
["customer_id", "fixed_city"]
)

display(df_fix)

# COMMAND ----------

df_silver = (
df_silver
.join(df_fix, "customer_id", "left")
.withColumn(
"city",
f.coalesce("city", "fixed_city") # Replace null with fixed city
)
.drop("fixed_city")
)

# COMMAND ----------

df_silver = df_silver.withColumn("customer_id", f.col("customer_id").cast("string"))

# COMMAND ----------

df_silver.printSchema()

# COMMAND ----------

df_silver = (
df_silver
# Build final customer column: "CustomerName-City" or "CustomerName-Unknown"
.withColumn(
"customer",
f.concat_ws("-", "customer_name", f.coalesce(f.col("city"), f.lit("Unknown")))
)
# Static attributes aligned with parent data model
.withColumn("market", f.lit("India"))
.withColumn("platform", f.lit("Sports Bar"))
.withColumn("channel", f.lit("Acquisition"))
)

# COMMAND ----------

df_silver.show(truncate = False)

# COMMAND ----------

df_silver.drop("customer name").write\
.format("delta") \
.option("delta.enableChangeDataFeed", "true") \
.option("mergeSchema", "true") \
.mode("overwrite") \
.saveAsTable(f"{catalog}.{silver_schema}.{data_source}")

# COMMAND ----------

# MAGIC %md 
# MAGIC ### GOLD PROCESSING

# COMMAND ----------

df_silver = spark.sql(f"SELECT * FROM {catalog}.{silver_schema}.{data_source};")


#take req cols only
df_gold = df_silver.select("customer_id","customer_name", "city","customer","market","platform","channel")

# COMMAND ----------

display(df_gold)

# COMMAND ----------

# saving in gold layer

df_gold.write\
    .format("delta")\
    .option("delta.enableChangeDataFeed", "true")\
    .mode("overwrite")\
    .saveAsTable(f"{catalog}.{gold_schema}.sb_dim_{data_source}")

# COMMAND ----------

delta_table = DeltaTable.forName(spark, "fmcg.gold.dim_customers")
df_child_customers = spark.table("fmcg.gold.sb_dim_customers").select(
f.col("customer_id").alias("customer_code"),
"customer",
"market",
"platform",
"channel"
)

# COMMAND ----------

delta_table.alias("target").merge(
source=df_child_customers.alias("source"),
condition="target.customer_code = source.customer_code"
).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

# COMMAND ----------

