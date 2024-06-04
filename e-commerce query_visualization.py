# Databricks notebook source
# DBTITLE 1,Start Spark Session
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
print('Spark session created.')

# COMMAND ----------

# DBTITLE 1,Load the customer and invoice data
invoices = spark.sql('SELECT * FROM hive_metastore.default.invoices')
display(invoices)

# COMMAND ----------

# DBTITLE 1,Check number of the countries
result_df = spark.sql("SELECT COUNT(DISTINCT Country) AS Number_countries FROM hive_metastore.default.invoices")
display(result_df)

# COMMAND ----------

# DBTITLE 1,Top 10 countries with most customers (Using SQL & Plotting by display function of Databricks)
resultDF = spark.sql("SELECT Country,\
                    COUNT(DISTINCT CustomerID) as TotalClientNumber\
                    FROM hive_metastore.default.invoices\
                    GROUP BY Country\
                    ORDER BY TotalClientNumber DESC\
                    LIMIT 10")
display(resultDF)

# COMMAND ----------

# DBTITLE 1,Top 10 countries with the most costumers (Using pyspark & Displaying with Databricks display function)
from pyspark.sql.functions import countDistinct

data = invoices.groupBy("Country").agg(countDistinct("CustomerID").alias("UniqueCustomerNumber"))

# Convert the Spark DataFrame to Pandas DataFrame
data_pd = data.toPandas()
# Rename column and sort values
data_pd = data_pd.rename(columns={'UniqueCustomerNumber': "UniqueCustomerNumber"}).sort_values(by="UniqueCustomerNumber", ascending=False)

data_pd = data.head(20)
display(data_pd)

# COMMAND ----------

# DBTITLE 1,Top 10 countries with the most costumers (Displaying by Matplotlib)
import matplotlib.pyplot as plt
import pandas as pd
from matplotlib.pyplot import figure
import pyspark
data = pd.DataFrame(data_pd,columns=["Country","UniqueCustomerNumber"])
plt.rcParams["figure.figsize"] = (20,3)
data.plot(kind='bar',x='Country',y='UniqueCustomerNumber')
plt.show()


# COMMAND ----------

# DBTITLE 1,Customers Ordering The Most
# CHALLENGE FOR THE STUDENT: REMOVE THE NULL CustomerID with sparkSQL
result_df = spark.sql("SELECT CustomerID,COUNT(DISTINCT InvoiceNo) as TotalOrderNumber\
                      FROM hive_metastore.default.invoices\
                      GROUP BY Country, CustomerID\
                      ORDER BY TotalOrderNumber DESC\
                      LIMIT 10")
display(result_df)

# COMMAND ----------

items = spark.sql('SELECT * FROM hive_metastore.default.items')
display(items)

# COMMAND ----------

# DBTITLE 1,Distrubution of Items Per Order
result_df = spark.sql("SELECT StockCode,COUNT(DISTINCT InvoiceNo)\
                      FROM hive_metastore.default.items\
                      GROUP BY StockCode")
display(result_df)

# COMMAND ----------

# DBTITLE 1,Most Ordered Items
most_ordered_items_df = spark.sql("SELECT StockCode,Description,SUM(Quantity) AS TotalQuantity\
                                  FROM hive_metastore.default.items\
                                  GROUP BY StockCode,Description\
                                  ORDER BY TotalQuantity DESC\
                                  LIMIT 10")
display(most_ordered_items_df)

# COMMAND ----------

# DBTITLE 1,Price Distrubution Per Item
# CHALLENGE FOR THE STUDENTS: WHY ARE THERE NEGATIVE PRICES?
price_df = spark.sql("SELECT UnitPrice\
                      FROM hive_metastore.default.items\
                      GROUP BY StockCode,Description,UnitPrice")
price_df.describe().show()


# COMMAND ----------

display(price_df.select('UnitPrice'))

# COMMAND ----------

# DBTITLE 1,Price Distribution Per Item Plotting By Matplotlib
import matplotlib.pyplot as plt
from matplotlib.pyplot import figure
price_df_pd = price_df.toPandas()
plt.rcParams["figure.figsize"] = (10,8)
plt.hist(price_df_pd['UnitPrice'],bins=100);

# COMMAND ----------

# MAGIC %md ### Which customers bought a WHILE METAL LANTERN?

# COMMAND ----------

price_df = spark.sql("""
SELECT DISTINCT invoices.CustomerID
FROM hive_metastore.default.items
JOIN hive_metastore.default.invoices ON hive_metastore.default.items.InvoiceNo=hive_metastore.default.invoices.InvoiceNo
WHERE  hive_metastore.default.items.Description = 'WHITE METAL LANTERN' 
AND  hive_metastore.default.invoices.CustomerID IS NOT NULL
""")
price_df.show()


# COMMAND ----------

# MAGIC %md ### Which ITEMS are the most revenue generating per country outside of UK?

# COMMAND ----------


result = spark.sql("""
SELECT items.Description, avg(items.UnitPrice) * sum(items.Quantity) as total_revenue, hive_metastore.default.invoices.Country
FROM  hive_metastore.default.items
JOIN  hive_metastore.default.invoices ON  hive_metastore.default.items.InvoiceNo= hive_metastore.default.invoices.InvoiceNo
WHERE  hive_metastore.default.invoices.Country != "United Kingdom"
GROUP BY  hive_metastore.default.items.Description,  hive_metastore.default.invoices.Country
ORDER BY total_revenue desc,  hive_metastore.default.invoices.Country,  hive_metastore.default.items.Description
""")
display(result)


# COMMAND ----------

## And in UK
# CHALLENGE. Actually the GROUP BY is not needed this time, how would you simplify the query?
result = spark.sql("""
SELECT  hive_metastore.default.items.Description, avg( hive_metastore.default.items.UnitPrice) * sum( hive_metastore.default.items.Quantity) as total_revenue,  hive_metastore.default.invoices.Country
FROM  hive_metastore.default.items
JOIN  hive_metastore.default.invoices ON  hive_metastore.default.items.InvoiceNo= hive_metastore.default.invoices.InvoiceNo
WHERE  hive_metastore.default.invoices.Country = "United Kingdom"
GROUP BY  hive_metastore.default.items.Description,  hive_metastore.default.invoices.Country
ORDER BY total_revenue desc,  hive_metastore.default.invoices.Country,  hive_metastore.default.items.Description
""")
display(result)

# COMMAND ----------


