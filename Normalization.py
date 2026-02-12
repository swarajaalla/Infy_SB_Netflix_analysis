# Databricks notebook source


# COMMAND ----------

import pandas as pd
df=pd.read_csv("/Volumes/workspace/default/netflix_dataset/netflix_titles.csv")
print("Cleaned dataset loaded for normalization")

# COMMAND ----------

#country Normalization
df['country'] = df['country'].replace({
    "United States": "USA",
    "U.S.": "USA",
    "United States of America": "USA",
    "UK": "United Kingdom"
})


# COMMAND ----------

#Rating Normalization
rating_map = {
    "UR": "Unrated", "NR": "Unrated", "": "Unknown",
    "TV-G": "G", "TV-Y": "G", "TV-Y7": "PG",
    "TV-PG": "PG", "PG-13": "PG-13", "R": "R", "NC-17": "NC-17"
}
df['rating'] = df['rating'].replace(rating_map)


# COMMAND ----------

#Genre Normalization
df['listed_in'] = df['listed_in'].apply(
    lambda x: [i.strip() for i in x.split(',')] if isinstance(x, str) else []
)


# COMMAND ----------

#Duration Normalization
df['duration'] = df['duration'].str.replace(" Seasons", " Season")
df[['duration_value','duration_unit']] = df['duration'].str.extract(r'(\d+)\s*(\w+)')


# COMMAND ----------

#Save cleaned dataset 
df.to_csv("/Volumes/workspace/default/netflix_dataset/netflix_cleaned.csv", index=False)
print("Cleaned dataset saved as 'netflix_cleaned.csv'")