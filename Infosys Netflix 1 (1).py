# Databricks notebook source
import pandas as pd

df = pd.read_csv(
    "/Volumes/workspace/default/netflix/NETFLIX_TITLES/netflix_titles.csv"
)
display(df)

# COMMAND ----------

df = df.drop_duplicates()
display(df)

# COMMAND ----------

df = df.dropna()

# COMMAND ----------

print(df.shape)
df.head()

# COMMAND ----------

df.info()
df.describe(include="all")


# COMMAND ----------

print("/nMissing values per column:")
print(df.isnull().sum())

# COMMAND ----------


df['country'] = df['country'].fillna("unknown")
df['directory']=df['director'].fillna("Not Available")
df['cast']=df['cast'].fillna("Not Available")
df['rating']=df['rating'].fillna("Not Rated")
df['duration']=df['duration'].fillna("0")
print(df)

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


df_cleaned = df.copy()  

df_cleaned.to_csv(
    "/Volumes/workspace/default/netflix/NETFLIX_TITLES/cleaned_netflix_titles.csv",
    index=False
)
print("\nCleaned dataset saved as 'netflix_cleaned.csv'")

# COMMAND ----------

from sklearn.preprocessing import LabelEncoder, OneHotEncoder
import pandas as pd
df_cleaned = pd.read_csv(
    "/Volumes/workspace/default/netflix/NETFLIX_TITLES/cleaned_netflix_titles.csv"
)
label_encoder = LabelEncoder()
df_cleaned['rating_encoded'] = label_encoder.fit_transform(df_cleaned['rating'])
display(df_cleaned)



# COMMAND ----------

country_freq = df_cleaned['country'].value_counts().to_dict()
df_cleaned['country_encoded'] = df_cleaned['country'].map(country_freq)
display(df_cleaned)


# COMMAND ----------

 # Normalize 'listed_in' (genres) using Multi-label One-Hot Encoding ----------
# Split multi-genre strings into lists
df_cleaned['genres_list'] = df_cleaned['listed_in'].apply(lambda x: [g.strip() for g in x.split(',')])
display(df_cleaned)



# COMMAND ----------

from sklearn.preprocessing import MultiLabelBinarizer

mlb = MultiLabelBinarizer()
df_cleaned = pd.concat(
    [
        df_cleaned,
        pd.DataFrame(
            mlb.fit_transform(df_cleaned['genres_list']),
            columns=mlb.classes_
        )
    ],
    axis=1
)
display(df_cleaned)

# COMMAND ----------

# Create a MultiLabelBinarizer object
mlb = MultiLabelBinarizer()
# Fit and transform the 'genres_list' column
df_cleaned = pd.concat([df_cleaned, pd.DataFrame(mlb.fit_transform(df_cleaned['genres_list']), columns=mlb.classes_)], axis=1)
display(df_cleaned)


# COMMAND ----------

mlb = pd.get_dummies(df_cleaned['genres_list'].apply(pd.Series).stack()).groupby(level=0).sum()
df_normalized = pd.concat([df_cleaned, mlb], axis=1)

# COMMAND ----------


# ---------- Frequency Encoding for Rating ----------
rating_freq = df_cleaned['rating'].value_counts().to_dict()
df_cleaned['rating_encoded'] = df_cleaned['rating'].map(rating_freq)
display(df_cleaned)


# COMMAND ----------

# ---------- Frequency Encoding for Country ----------
country_freq = df_cleaned['country'].value_counts().to_dict()
df_cleaned['country_encoded'] = df_cleaned['country'].map(country_freq)
display(df_cleaned)

# COMMAND ----------

#---------- Frequency Encoding for Genres ----------
# First, take only the first genre from 'listed_in' (to avoid multi-genre issue)
df_cleaned['primary_genre'] = df_cleaned['listed_in'].apply(lambda x: x.split(",")[0].strip())

genre_freq = df_cleaned['primary_genre'].value_counts().to_dict()
df_cleaned['genre_encoded'] = df_cleaned['primary_genre'].map(genre_freq)

display(df_cleaned)

# COMMAND ----------


df_freq = df_normalized.copy()  # or your frequency encoding logic

df_freq.to_csv(
    "/Volumes/workspace/default/netflix/NETFLIX_TITLES/freq_encoded_netflix_titles.csv",
    index=False
)
print("Frequency encoded dataset saved as 'freq_encoded_netflix_titles.csv'")