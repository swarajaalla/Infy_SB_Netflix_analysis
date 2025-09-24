# Databricks notebook source
"/Volumes/workspace/default/netflix/netflix_analysis.csv"

# COMMAND ----------

import pandas as pd
df = pd.read_csv("/Volumes/workspace/default/netflix/netflix_analysis.csv")
df.shape
df.head()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleaning the dataset 

# COMMAND ----------

# MAGIC %md
# MAGIC - ### Remove Duplicates

# COMMAND ----------

df.info()
df = df.drop_duplicates()
df = df.drop_duplicates(subset=['title','release_year'])

# COMMAND ----------

# MAGIC %md
# MAGIC - ### Handle Missing Values

# COMMAND ----------

df.isnull().sum()

# COMMAND ----------

for col in ['director', 'cast', 'country', 'rating']:
    df[col] = df[col].fillna("Unknown")

# COMMAND ----------

#fixing todatetime format 
df['date_added'] = pd.to_datetime(df['date_added'], errors='coerce')

# COMMAND ----------

#binary column
df['date_missing'] = df['date_added'].isna().astype(int)

# COMMAND ----------

# Breaks columns with multiple value into multiple rows.
df_exploded = df.assign(genre=df['listed_in'].str.split(',')).explode('genre')
df_exploded['genre'] = df_exploded['genre'].str.strip()

# COMMAND ----------

# Handling Outliers
df['duration'] = df['duration'].str.replace(' min', '').str.replace(' Season[s]?', '', regex=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Normalize Categorical Features**

# COMMAND ----------

#no extra spaces
df['type'] = df['type'].str.strip() 

# COMMAND ----------

# Rating based on Age Group
rating_map = {
    'G': 'Kids', 'TV-Y': 'Kids', 'TV-G': 'Kids',
    'PG': 'Family', 'TV-PG': 'Family', 'TV-Y7': 'Family', 'TV-Y7-FV': 'Family',
    'PG-13': 'Teens', 'TV-14': 'Teens',
    'R': 'Adults', 'NC-17': 'Adults', 'TV-MA': 'Adults'
}

df['rating_group'] = df['rating'].map(rating_map).fillna('Unknown')
df.display()

# COMMAND ----------

# standard naming
df['country'] = df['country'].replace({'USA': 'United States'})


# COMMAND ----------

# Converts categories into binary
pd.get_dummies(df['type'], prefix='type')

# COMMAND ----------

# Grouping Rare Categories to avoid noise
rare_countries = df['country'].value_counts()[df['country'].value_counts()<20].index
df['country'] = df['country'].replace(rare_countries, 'Other')

# COMMAND ----------

cleaned_file_path = "/Volumes/workspace/default/netflix/cleaned_netflix.csv"
df.to_csv(cleaned_file_path, index=False)