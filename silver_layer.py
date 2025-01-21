# Databricks notebook source
# MAGIC %md
# MAGIC # SILVER LAYER SCRIPT 

# COMMAND ----------

# MAGIC %md
# MAGIC ### DATA ACCESS USING APP.

# COMMAND ----------

# App Id: 63dbc039-d570-4e32-b28d-7d3755ba80d3
# Dir Id: f6cb831a-7ff7-4563-a35b-6d6ca3b3f036
# SECRET: Tyj8Q~sifl6HD320uRgGfcCtIhfglozHt8Jhmbw_ 

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.awpdeltalake.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.awpdeltalake.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.awpdeltalake.dfs.core.windows.net", "63dbc039-d570-4e32-b28d-7d3755ba80d3")
spark.conf.set("fs.azure.account.oauth2.client.secret.awpdeltalake.dfs.core.windows.net", "Tyj8Q~sifl6HD320uRgGfcCtIhfglozHt8Jhmbw_")
spark.conf.set("fs.azure.account.oauth2.client.endpoint.awpdeltalake.dfs.core.windows.net", "https://login.microsoftonline.com/f6cb831a-7ff7-4563-a35b-6d6ca3b3f036/oauth2/token")

# COMMAND ----------

# MAGIC %md
# MAGIC ### DATA LOADING 

# COMMAND ----------

# MAGIC %md
# MAGIC ###Read Calender Data

# COMMAND ----------

from pyspark.sql.functions import * 

# COMMAND ----------

df_calender = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("abfss://bronz@awpdeltalake.dfs.core.windows.net/AdventureWorks_Calendar")

# COMMAND ----------

display(df_calender)

# COMMAND ----------

# MAGIC %md
# MAGIC ##TRANSPORTATION 

# COMMAND ----------

### Calendar Data Transformation
df_cal = df_calender.withColumn('Month',month(col('Date')))\
                    .withColumn('Year',year(col('Date')))

df_cal.display()

# COMMAND ----------

df_cal.write.format("parquet")\
            .mode("overwrite")\
            .option("path", "abfss://silver@awpdeltalake.dfs.core.windows.net/AdventureWorks_Calendar")\
            .save()


# COMMAND ----------

### Customer Data Transformation

df_cus = spark.read.format("csv")\
    .option("header", "true")\
    .option("inferSchema", "true")\
    .load("abfss://bronz@awpdeltalake.dfs.core.windows.net/AdventureWorks_Customers")

df_cus.display()

# COMMAND ----------

df_cus.printSchema()

# COMMAND ----------

###df_cus.withColumn('FullName',concat(col('Prefix'),lit(' '),col('FirstName'),lit(' '),col('LastName'))).display()

df_cus = df_cus.withColumn('FullName',concat_ws(' ',col('Prefix'),col('FirstName'),col('LastName')))
df_cus.display()

# COMMAND ----------

df_cus.write.format("parquet").mode("overwrite").option("path", "abfss://silver@awpdeltalake.dfs.core.windows.net/AdventureWorks_Customers").save()

# COMMAND ----------

###  Load Product Categories Data to silver layer 

df_prod_cat = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("abfss://bronz@awpdeltalake.dfs.core.windows.net/AdventureWorks_Product_Categories")


df_prod_cat.write.format("parquet").mode("overwrite").option("path", "abfss://silver@awpdeltalake.dfs.core.windows.net/AdventureWorks_Product_Categories").save()

# COMMAND ----------

### Product Data Transformation

df_pro = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("abfss://bronz@awpdeltalake.dfs.core.windows.net/AdventureWorks_Products")

df_pro.display()

# COMMAND ----------

df_pro = df_pro.withColumn("ProductSKU",split(col("ProductSKU"),"-")[0])\
               .withColumn("ProductName",split(col("ProductName"),"-")[0])

df_pro.display()

# COMMAND ----------

df_pro.write.format("parquet").mode("overwrite").option("path", "abfss://silver@awpdeltalake.dfs.core.windows.net/AdventureWorks_Products").save()

# COMMAND ----------

### Load the Return Data to Silver Layer

df_ret = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("abfss://bronz@awpdeltalake.dfs.core.windows.net/AdventureWorks_Returns")

# write data into silver layer
df_ret.write.format("parquet").mode("overwrite").option("path", "abfss://silver@awpdeltalake.dfs.core.windows.net/AdventureWorks_Returns").save()

# COMMAND ----------

### Load the Territory Data to Silver Layer
df_ter = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("abfss://bronz@awpdeltalake.dfs.core.windows.net/AdventureWorks_Territories")

# write data into silver layer
df_ter.write.format("parquet").mode("overwrite").option("path", "abfss://silver@awpdeltalake.dfs.core.windows.net/AdventureWorks_Territories").save()

# COMMAND ----------

### Load the SubCategories Data to Silver Layer
df_subcat = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("abfss://bronz@awpdeltalake.dfs.core.windows.net/Product_Subcategories")

# write data into silver layer
df_subcat.write.format("parquet").mode("overwrite").option("path", "abfss://silver@awpdeltalake.dfs.core.windows.net/Product_Subcategories").save()

# COMMAND ----------

### Tranformation of the Sales Data

# Specify the base path to the container
base_path = "abfss://bronz@awpdeltalake.dfs.core.windows.net/"

# Specify the folder pattern to match all sales files
sales_path = f"{base_path}AdventureWorks_Sales_201*/"

# Read the CSV files into a single DataFrame
df_sales = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(sales_path)

# Show the loaded data
df_sales.display()

# COMMAND ----------

df_sales = df_sales.withColumn("StockDate",to_timestamp(col("StockDate")))\
                   .withColumn("OrderNumber",regexp_replace(col("OrderNumber"),"S","T"))\
                   .withColumn("Multiplication",col("OrderQuantity")*col("OrderLineItem"))

df_sales.display()

# COMMAND ----------

df_sales.write.format("parquet").mode("overwrite").option("path", "abfss://silver@awpdeltalake.dfs.core.windows.net/AdventureWorks_Sales").save()

# COMMAND ----------

df_sales.groupBy('OrderDate').agg(count('OrderNumber')).display()

# COMMAND ----------

df_prod_cat.display()

# COMMAND ----------



# COMMAND ----------

df_ter.display()

# COMMAND ----------

