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