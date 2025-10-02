# Databricks notebook source
afile_path = "/Volumes/workspace/default/netflix_titles/netflix_titles.csv"

# COMMAND ----------

import pandas as pd
df = pd.read_csv('/Volumes/workspace/default/netflix_titles/netflix_titles.csv')
df.display()

# COMMAND ----------

print(df.isnull().sum())
print(df.isnull().mean()*100)

# COMMAND ----------

df = df.dropna(how = "all")
df.isnull().sum()

# COMMAND ----------

print(df.duplicated())

# COMMAND ----------

df = df.drop_duplicates()
df.shape

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC **NORMALIZATION**

# COMMAND ----------

import pandas as pd
from sklearn.preprocessing import MinMaxScaler

# Define df by reading the CSV file
df = pd.read_csv('/Volumes/workspace/default/netflix_titles/netflix_titles.csv')
df = df.dropna(how="all")
df = df.drop_duplicates()

# Select numeric columns for normalization
numeric_cols = df.select_dtypes(include=['float64', 'int64']).columns

scaler = MinMaxScaler()
df[numeric_cols] = scaler.fit_transform(df[numeric_cols])

display(df)

# COMMAND ----------

df = df.dropna()
display(df)

# COMMAND ----------

display(df)

# COMMAND ----------



# COMMAND ----------

# Load data as Spark DataFrame
df_spark = spark.read.csv('/Volumes/workspace/default/netflix_titles/netflix_titles.csv', header=True, inferSchema=True)
df_spark = df_spark.dropna(how="all").dropDuplicates()

# COMMAND ----------

# Select categorical columns (example: 'type', 'rating')
categorical_cols = [field for field, dtype in df_spark.dtypes if dtype == 'string']

# COMMAND ----------

# Select numeric columns for normalization
numeric_cols = [field for field, dtype in df_spark.dtypes if dtype in ['int', 'double', 'float', 'bigint', 'smallint', 'tinyint', 'decimal']]

from pyspark.ml.feature import MinMaxScaler, VectorAssembler

if numeric_cols:
    assembler = VectorAssembler(inputCols=numeric_cols, outputCol="features")
    df_vector = assembler.transform(df_spark)
    scaler = MinMaxScaler(inputCol="features", outputCol="scaledFeatures")
    scaler_model = scaler.fit(df_vector)
    df_scaled = scaler_model.transform(df_vector)
    from pyspark.sql.functions import udf
    from pyspark.sql.types import ArrayType, DoubleType

    def extract_array(arr):
        return arr if arr is not None else [None]*len(numeric_cols)

    extract_udf = udf(extract_array, ArrayType(DoubleType()))
    df_scaled = df_scaled.withColumn("scaled_array", extract_udf("scaledFeatures"))

    for idx, col in enumerate(numeric_cols):
        df_scaled = df_scaled.withColumn(col, df_scaled["scaled_array"][idx])

    df_scaled = df_scaled.drop("features", "scaledFeatures", "scaled_array")
    display(df_scaled)
else:
    display(df_spark)

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

from pyspark.sql.funtions import when

for cat in categories:
    col_name = cat.strip().replace(" ","_").replace("&","and").replace(".","").replace("...", "etc").replace("/","_").
    replace("'","").replace(",", "").replace(" "," ")
    df = df.withColumn(col_name, when(col(col_name) == True, 1).otherwise(0))

    display(df)

# COMMAND ----------

remove nulls in director column