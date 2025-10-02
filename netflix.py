# Databricks notebook source
# DBTITLE 1,Import Required Libraries
from pyspark import *
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import *
from pyspark.sql.functions import *
from pyspark.sql.functions import to_date, col

# COMMAND ----------

# DBTITLE 1,Load Netflix Dataset
df = spark.read.csv("/Volumes/workspace/default/netflix", header=True, inferSchema=True)
display(df)
df.printSchema()

# COMMAND ----------

# DBTITLE 1,Replace Missing Values and Show DataFrame
df = df.fillna('Not found')
display(df)


# COMMAND ----------

# DBTITLE 1,Count Rows in DataFrame
df.count()

# COMMAND ----------

# DBTITLE 1,Add Movie and TV Show Indicator Columns
df = df.withColumn('movie', when(col('type')=='Movie', 1).otherwise(0))
df = df.withColumn('tv_show', when(col('type')=='TV Show', 1).otherwise(0))
df = df.drop('type')
display(df)


# COMMAND ----------

# DBTITLE 1,Split Listed In Column into Array and Display DataFrame
df = df.withColumn('listed_in', split(col('listed_in'), ","))
display(df)


# COMMAND ----------

# DBTITLE 1,Create Category Flags for Movie and TV Show Genres
categories = [
    "Independent Movies",
    "Romantic TV Shows",
    "Thrillers",
    "Dramas",
    "Docuseries",
    "Sports Movies",
    "Horror Movies",
    "Cult Movies",
    "TV Mysteries",
    "TV Horror",
    "Classic Movies",
    "Anime Features",
    "Stand-Up Comedy &...",
    "Crime TV Shows",
    "TV Sci-Fi & Fantasy",
    "Faith & Spiritua..."
]

for cat in categories:
    col_name = cat.strip().replace(" ", "_").replace("&", "and").replace(".", "").replace("-", "_").replace("...", "etc").replace("/", "_").replace("'", "").replace(",", "").replace("  ", " ")
    df = df.withColumn(col_name, array_contains(col('listed_in'), cat))

display(df)

# COMMAND ----------

# DBTITLE 1,Convert Rating Column to Array Type in DataFrame
df = df.withColumn('rating', array(col('rating')))


# COMMAND ----------

# DBTITLE 1,Create Boolean Columns for Each Unique Rating Value
distinct_ratings = [row['rating'] for row in df.selectExpr("explode(rating) as rating").distinct().collect()]

for rating in distinct_ratings:
    col_name = rating.strip().replace(" ", "_").replace("-", "_").replace(".", "").replace("/", "_").replace("&", "and").replace("'", "")
    df = df.withColumn(col_name, array_contains(col('rating'), rating))

# COMMAND ----------

# DBTITLE 1,Preview DataFrame Contents
display(df)

# COMMAND ----------

# DBTITLE 1,Split Country Column into Array Format in DataFrame
df = df.withColumn(
    'country',
    split(col('country'), ' ')
)

# COMMAND ----------

# DBTITLE 1,Extract First Country Value from Country Array Column
df = df.withColumn('country', expr("country[0]"))

# COMMAND ----------

# DBTITLE 1,Standardize Country Names in DataFrame and Display
df = df.replace(
    {
        'United': 'USA',
        'Not': 'Not Found',
        'South': 'South Africa',
        'Hong':'Hong Kong',
        'New': 'New Jersey',
        'West': 'West Indies',
        'North': 'North Korea',
        'Central': 'Central African Republic',
        'East': 'East Timor'

    },
    subset=['country']
)
display(df)

# COMMAND ----------

# DBTITLE 1,Convert Country Column to Single Value Array Format
df = df.withColumn('country', array(col('country')))

# COMMAND ----------

# DBTITLE 1,Create Boolean Columns for Each Unique Country Value
distinct_countries = [row['country'] for row in df.selectExpr("explode(country) as country").distinct().collect()]

for country in distinct_countries:
    col_name = country.strip().replace(" ", "_").replace("-", "_").replace(".", "").replace("/", "_").replace("&", "and").replace("'", "")
    df = df.withColumn(col_name, when(array_contains(col('country'), country), 1).otherwise(0))

display(df)

# COMMAND ----------

# DBTITLE 1,e ...
df = df.replace({'true':'1', 'false':'0'})
display(df)

# COMMAND ----------

# DBTITLE 1,Remove Unnecessary Columns from DataFrame and Display
df = df.drop("rating", "84_min", "74_min", "66_min", 'country', 'listed_in', 'type')
display(df)

# COMMAND ----------

# DBTITLE 1,Display Complete DataFrame Contents
display(df)

# COMMAND ----------

# DBTITLE 1,Write Cleaned DataFrame to CSV and Display File List
df.coalesce(1).write.mode("overwrite").option("header", "true").csv("/Volumes/workspace/default/netflix/cleaned_netflix_csv_single")
display(dbutils.fs.ls("/Volumes/workspace/default/netflix/cleaned_netflix_csv_single"))


# COMMAND ----------

# DBTITLE 1,List Files in Cleaned Netflix Data Directory
import os
os.listdir("/Volumes/workspace/default/netflix/cleaned_netflix_csv_single")


# COMMAND ----------

# To download this notebook with output, click "File" > "Export" > "Download as IPython (.ipynb)" in the Databricks notebook UI.
# Alternatively, you can use the Databricks REST API to export the notebook with output.
# Example using Databricks CLI (must be installed and configured):
# databricks workspace export /Workspace/path/to/your_notebook.ipynb ./your_notebook_with_output.ipynb --format SOURCE