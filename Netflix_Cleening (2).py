# Databricks notebook source



# COMMAND ----------

/Volumes/workspace/default/netflix_dataset/netflix_titles.csv

# COMMAND ----------

#Load & initial inspection
import pandas as pd
df=pd.read_csv("/Volumes/workspace/default/netflix_dataset/netflix_titles.csv")
print("Dataset shape:", df.shape)     #DataFrame size check panna.
display(df.head())  # First 5 rows preview panna(default 5 rows)
display(df.info())  # Full Data summary
display(df.describe(include=[float, int])) # Full Descriptive statistics



# COMMAND ----------

#Data types and missing-value summary
df.dtypes # Column list and type summary

#missing values count
missing=df.isna().sum().sort_values(ascending=False)
display(missing)

#Percentage missing
missing_percentage=(df.isna().mean()*100).round(2).sort_values(ascending=False)
print(missing_percentage)



# COMMAND ----------

obj_cols=df.select_dtypes(include="object").columns
print(obj_cols)
df[obj_cols]=df[obj_cols].apply(lambda s: s.str.strip())#Removing trailing spaces


# COMMAND ----------

df['release_year'] = pd.to_numeric(df['release_year'], errors='coerce').astype('Int64')
if 'date_added' in df.columns:
    df['date_added'] = pd.to_datetime(df['date_added'], errors='coerce')
display(df[['release_year', 'date_added']])
# release_year → text → safe integer (NaN handle)
# date_added → text → datetime (NaT handle)

# COMMAND ----------

# Mark rows missing keys
missing_key_rows = df[df['title'].isna() | df['release_year'].isna()]
# Option A: drop rows missing title
df = df.dropna(subset=['title'])
# Title & release year = key info.
# Missing title → useless row → drop pannrom.
# Missing other stuff (director, country) → replace with “Unknown” → safe-a analysis panna.
df['country'] = df['country'].fillna('Unknown')
df['listed_in'] = df['listed_in'].fillna('Unknown')



# COMMAND ----------

#Handling missing values
df['director'] = df['director'].fillna("Unknown")
df['cast'] = df['cast'].fillna("Unknown")
df['country'] = df['country'].fillna("Unknown")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 

# COMMAND ----------

# Example inspect
unique_genres_raw = sorted(set([g.strip() for s in df['listed_in'].dropna() for g in s.split(',')]))
print(unique_genres_raw[:50])

# Create normalization dict manually for common name fixes:
genre_map = {'Dramas': 'Drama', 'International Movies':'International', 'Sci-Fi':'Sci-Fi'}  # example
# apply mapping after splitting into list
df['genres_list'] = df['listed_in'].fillna("").apply(lambda s: [genre_map.get(x.strip(), x.strip()) for x in s.split(",") if x.strip()])


# COMMAND ----------

#Parse Date and time(Converting date_added to datetime)
df['date_added'] = pd.to_datetime(df['date_added'], errors='coerce')
df['release_year'] = pd.to_numeric(df['release_year'], errors='coerce')
df['release_year'] = df['release_year'].fillna(-1).astype(int)#if any null fill it with (-1)

# COMMAND ----------

#Filling missing rating with not rated
df['rating'].fillna("Not Rated", inplace=True)
display(df)

# COMMAND ----------

#Replace empty strings and whitespace with NaN
import numpy as np
df = df.replace(r'^\s*$', np.nan, regex=True)
# Fill all missing values for object columns with "Unknown"
for col in df.select_dtypes(include="object").columns:
    df[col] = df[col].fillna("Unknown")
# Fill missing numeric columns with a placeholder (e.g., -1 or 0)
for col in df.select_dtypes(include=["float", "int", "Int64"]).columns:
    df[col] = df[col].fillna(-1)
# Fill missing datetime columns with a placeholder or drop if not needed
for col in df.select_dtypes(include=["datetime64[ns]"]).columns:
    df[col] = df[col].fillna(pd.Timestamp("1900-01-01"))
#Final check for missing values
missing = df.isna().sum().sort_values(ascending=False)
display(missing)


# COMMAND ----------

#checks for null values
print(df.isnull().sum())

# COMMAND ----------

# Save Cleaned Dataset
df.to_csv("/Volumes/workspace/default/netflix_dataset/netflix_cleaned.csv", index=False)
print(" Normalized dataset saved as 'netflix_cleaned.csv'")
