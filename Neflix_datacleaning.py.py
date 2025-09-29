# Databricks notebook source
import pandas as pd
import numpy as np

# Load dataset
df = pd.read_csv("/Volumes/workspace/default/netflix_analysis/netflix_titles.csv")   # change path if needed


# COMMAND ----------

display(df)

# COMMAND ----------

print(df.shape)        # rows, columns
print(df.info())       # data types & nulls
print(df.head())       # first 5 rows
print(df.describe())   # summary stats


# COMMAND ----------

print(df.isnull().sum())


# COMMAND ----------

import pandas as pd
df = pd.read_csv("/Volumes/workspace/default/netflix_analysis/netflix_titles.csv")
df.dropna()
df.fillna(0)
display(df)

# COMMAND ----------

df = df.drop_duplicates()

# COMMAND ----------

before = df.shape[0]
df = df.drop_duplicates()
after = df.shape[0]
print(f"\nRemoved {before - after} duplicate rows.") 

# COMMAND ----------

df['type'] = df['type'].str.strip()
df['rating'] = df['rating'].str.strip()
display(df)

# COMMAND ----------

df['country'] = df['country'].str.strip()
df['directory']=df['director'].str.lower()
df['country'] = df['country'].str.replace(r'[^a-z  A-Z]','', regex=True)
display(df)

# COMMAND ----------

df.to_csv(
    "/Volumes/workspace/default/netflix_analysis/netflix_titles/cleaned_netflix_titles.csv",
    index=False
)
