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
df = pd.read_csv(download_path+"/netflix_titles.csv")


# COMMAND ----------

df

# COMMAND ----------

df = df.drop(columns=["title", "director", "cast" , "show_id", "description"])

# COMMAND ----------

df

# COMMAND ----------

df.info()

# COMMAND ----------

df = df.drop_duplicates()

# COMMAND ----------

df = df.rename(columns={
    "type": "content_type",
    "listed_in": "genres"
})

# COMMAND ----------

print("Missing values before cleaning:\n", df.isnull().sum())

# COMMAND ----------

df = df.fillna({
    
    "country": "Unknown",
    "rating": "Not Rated",
    "duration": "Unknown"
})


# COMMAND ----------

df['duration'] = df['duration'].str.replace("Seasons", "Season")  # unify plural
df['duration'] = df['duration'].str.strip()

# COMMAND ----------

df['duration_int'] = df['duration'].str.extract(r'(\d+)').astype(float)
df['duration_type'] = df['duration'].str.extract(r'([a-zA-Z]+)')

# COMMAND ----------

df['duration_int'] = df['duration_int'].astype('Int64')

# COMMAND ----------

df.drop_duplicates(inplace=True)

# COMMAND ----------

for col in ['content_type', 'country', 'rating', 'genres']:
    df[col] = df[col].astype(str).str.strip()

# COMMAND ----------

df.reset_index(drop=True, inplace=True)

# COMMAND ----------

print(df.head(10))
print(df[['duration', 'duration_int', 'duration_type']].sample(10))

# COMMAND ----------

df

# COMMAND ----------



# COMMAND ----------

# download_path = "/Volumes/workspace/default/netflix/cleaned_data"

df.to_csv(f"{download_path}/netflix_newcleaned.csv", index=False)



# COMMAND ----------

# NORMALISATION

# COMMAND ----------


from sklearn.preprocessing import LabelEncoder, MinMaxScaler

# COMMAND ----------

df = pd.read_csv(f"{download_path}/netflix_newcleaned.csv")


# COMMAND ----------

# Categorical Normalization
# content_type → Label Encoded (Movie=0, TV Show=1)
# country → Frequency Encoded (# of shows/movies from that country)
# rating → Ordinal Encoded (G=0 … TV-MA=10, unknown = -1)
# genres → Frequency Encoded (based on main/first genre)
# duration_type → Label Encoded (min=0, Season=1, etc.)

# 🔹 Numeric Normalization
# release_year → Frequency Encoded (# of titles released that year)
# duration_int → Min-Max Normalized (scaled between 0–1)
# date_added → Converted to numeric (# of days since earliest date, normalized)

# COMMAND ----------

df_norm = df.copy()

# COMMAND ----------

le_content = LabelEncoder()
df_norm["content_type"] = le_content.fit_transform(df_norm["content_type"])

# COMMAND ----------

country_freq = df_norm["country"].value_counts().to_dict()
df_norm["country"] = df_norm["country"].map(country_freq)

# COMMAND ----------

rating_order = [
    "G", "PG", "PG-13", "R", "NC-17",   # Movies
    "TV-Y", "TV-Y7", "TV-G", "TV-PG", "TV-14", "TV-MA" # TV Shows
]
rating_mapping = {rating: idx for idx, rating in enumerate(rating_order)}
df_norm["rating"] = df_norm["rating"].map(rating_mapping).fillna(-1).astype(int)

# COMMAND ----------

df_norm["main_genre"] = df_norm["genres"].apply(lambda x: x.split(",")[0].strip())
genre_freq = df_norm["main_genre"].value_counts().to_dict()
df_norm["genres"] = df_norm["main_genre"].map(genre_freq)
df_norm.drop(columns=["main_genre"], inplace=True)

# COMMAND ----------

le_duration = LabelEncoder()
df_norm["duration_type"] = le_duration.fit_transform(df_norm["duration_type"])

# COMMAND ----------

year_freq = df_norm["release_year"].value_counts().to_dict()
df_norm["release_year"] = df_norm["release_year"].map(year_freq)

# COMMAND ----------

scaler = MinMaxScaler()
df_norm["duration_int"] = scaler.fit_transform(df_norm[["duration_int"]])

# COMMAND ----------

df_norm["date_added"] = pd.to_datetime(df_norm["date_added"], errors="coerce")
min_date = df_norm["date_added"].min()
df_norm["date_added"] = (df_norm["date_added"] - min_date).dt.days
df_norm["date_added"] = df_norm["date_added"].fillna(df_norm["date_added"].median())

# COMMAND ----------

df_norm.drop(columns=["duration"], inplace=True)

# COMMAND ----------

df_norm

# COMMAND ----------

df_norm.to_csv(f"{download_path}/netflix_normalized_full.csv", index=False)

# COMMAND ----------



# COMMAND ----------

