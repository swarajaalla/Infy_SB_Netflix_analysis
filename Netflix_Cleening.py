# Databricks notebook source



# COMMAND ----------

/Volumes/workspace/default/netflix_dataset/netflix_titles.csv

# COMMAND ----------

import pandas as pd
df=pd.read_csv("/Volumes/workspace/default/netflix_dataset/netflix_titles.csv")
print("Dataset shape:", df.shape)     #DataFrame size check panna.
display(df.head())  # First 5 rows preview panna(default 5 rows)
display(df.info())  # Full Data summary
display(df.describe(include=[float, int])) # Full Descriptive statistics


# COMMAND ----------

df.dtypes # Column list and type summary

#missing values count
missing=df.isna().sum().sort_values(ascending=False)
display(missing)

#Percentage missing
missing_percentage=(df.isna().mean()*100).round(2).sort_values(ascending=False)
print(missing_percentage)



# COMMAND ----------

obj_cols=df.select_dtypes(include="object").columns
df[obj_cols]=df[obj_cols].apply(lambda s: s.str.strip())
# df['listed_in'] = df['listed_in'].str.title()
# print(df['listed_in'])


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

# Example inspect
unique_genres_raw = sorted(set([g.strip() for s in df['listed_in'].dropna() for g in s.split(',')]))
print(unique_genres_raw[:50])

# Create normalization dict manually for common name fixes:
genre_map = {'Dramas': 'Drama', 'International Movies':'International', 'Sci-Fi':'Sci-Fi'}  # example
# apply mapping after splitting into list
df['genres_list'] = df['listed_in'].fillna("").apply(lambda s: [genre_map.get(x.strip(), x.strip()) for x in s.split(",") if x.strip()])


# COMMAND ----------

#Parse Date and time
df['date_added'] = pd.to_datetime(df['date_added'], errors='coerce')

# COMMAND ----------

#Filling missing rating with not rated
df['rating'].fillna("Not Rated", inplace=True)
display(df)

# COMMAND ----------

#checks for null values
print(df.isnull().sum())

# COMMAND ----------

# Save normalized dataset 
df.to_csv("/Volumes/workspace/default/netflix_dataset/netflix_cleaned_normalized.csv", index=False)
print("Normalized dataset saved as 'netflix_cleaned_normalized.csv'")
