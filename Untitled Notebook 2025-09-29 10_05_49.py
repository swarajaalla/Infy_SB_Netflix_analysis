# Databricks notebook source
# MAGIC %restart_python
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %pip install kaggle

# COMMAND ----------

download_path ="/Volumes/workspace/default/netflix/kaggle"
dbutils.fs.mkdirs(download_path)

# COMMAND ----------

import os
os.environ['KAGGLE_USERNAME'] = "ankitahub"
os.environ['KAGGLE_KEY'] ="10ab0b2b8f256946825ceb12f5c30199"

# COMMAND ----------

!kaggle datasets download -d shivamb/netflix-shows -p {download_path} --unzip


# COMMAND ----------

import pandas as pd
data = pd.read_csv(download_path+"/netflix_titles.csv")


# COMMAND ----------

data

# COMMAND ----------

data.head(5)

# COMMAND ----------

data.info()

# COMMAND ----------

"""Deleting redundant columns."""

# COMMAND ----------

data.columns

# COMMAND ----------

data.drop(columns = "rating", inplace = True)


# COMMAND ----------

data.columns

# COMMAND ----------

"""Renaming columns."""


# COMMAND ----------

data.columns

# COMMAND ----------

new_column_names = []
for i in data.columns:
  new_column_names.append(i.capitalize())

# COMMAND ----------

new_column_names

# COMMAND ----------

data.columns = new_column_names



# COMMAND ----------

data.head()

# COMMAND ----------

"""Dropping Duplicates."""

# COMMAND ----------

data.duplicated().sum()

# COMMAND ----------

"""Removing NaN values"""

# COMMAND ----------

data.isna()

# COMMAND ----------

data.isna().sum()

# COMMAND ----------

data.dropna(inplace=True)


# COMMAND ----------

data.isna().sum()

# COMMAND ----------

data

# COMMAND ----------

data.to_csv("Cleaned_DataCSV.csv",index = False)

# COMMAND ----------

(
  spark.createDataFrame(df)
  .write
  .mode("overwrite")
  .option("header", True)
  .csv("dbfs:/Volumes/workspace/default/netflix/cleaned_netflix")
)