# Databricks notebook source


# COMMAND ----------

    import pandas as pd
    data=pd.read_csv("/Volumes/workspace/default/netflix_dataset/netflix_cleaned.csv")
    df=pd.DataFrame(data)
    df.head(10)

# COMMAND ----------


df.isnull().sum()

# COMMAND ----------

# DBTITLE 1,Check data types
print(df.dtypes)

# COMMAND ----------

# DBTITLE 1,Categorical value counts
# Value counts for categorical columns
categorical_cols = df.select_dtypes(include=['object', 'category']).columns
for col in categorical_cols:
    print(f"\nValue counts for {col}:")
    print(df[col].value_counts().head(5))

# COMMAND ----------

# DBTITLE 1,Visualize type distribution
import pandas as pd
df = pd.read_csv("/Volumes/workspace/default/netflix_dataset/netflix_cleaned.csv")
import matplotlib.pyplot as plt
plt.figure(figsize=(10,6))
df['release_year'].value_counts().sort_index().plot(kind='bar')
plt.xlabel('Release Year')
plt.ylabel('Count')
plt.title('Distribution of Release Years')
plt.tight_layout()
plt.show()

# COMMAND ----------

# DBTITLE 1,Visualize release year distribution
# Visualize the top 10 countries with the most titles
import matplotlib.pyplot as plt

top_countries = df['country'].value_counts().head(10)
plt.figure(figsize=(10,6))
top_countries.plot(kind='bar')
plt.xlabel('Country')
plt.ylabel('Number of Titles')
plt.title('Top 10 Countries by Number of Netflix Titles')
plt.tight_layout()
plt.show()

# COMMAND ----------

# DBTITLE 1,Visualize top countries
# Visualize the top 10 genres with the most titles
import matplotlib.pyplot as plt

# Split genres if multiple are present in a single cell, then count
genre_counts = df['listed_in'].str.split(',').explode().str.strip().value_counts().head(10)
plt.figure(figsize=(10,6))
genre_counts.plot(kind='bar')
plt.xlabel('Genre')
plt.ylabel('Number of Titles')
plt.title('Top 10 Genres by Number of Netflix Titles')
plt.tight_layout()
plt.show()

# COMMAND ----------

# DBTITLE 1,Visualize top genres
import matplotlib.pyplot as plt

# Split genres if multiple are present in a single cell, then count
genre_counts = df['listed_in'].str.split(',').explode().str.strip().value_counts().head(10)
plt.figure(figsize=(10,6))
genre_counts.plot(kind='bar')
plt.xlabel('Genre')
plt.ylabel('Number of Titles')
plt.title('Top 10 Genres by Number of Netflix Titles')
plt.tight_layout()
plt.show()

# COMMAND ----------

# DBTITLE 1,Univariate: Type distribution
# Visualize the distribution of ratings
import matplotlib.pyplot as plt

plt.figure(figsize=(10,6))
df['rating'].value_counts().plot(kind='bar')
plt.xlabel('Rating')
plt.ylabel('Count')
plt.title('Distribution of Ratings')
plt.tight_layout()
plt.show()

# COMMAND ----------

# DBTITLE 1,Univariate: Rating distribution
# Visualize the distribution of release years as a histogram
import matplotlib.pyplot as plt

plt.figure(figsize=(10,6))
df['release_year'].hist(bins=20)
plt.xlabel('Release Year')
plt.ylabel('Count')
plt.title('Histogram of Release Years')
plt.tight_layout()
plt.show()

# COMMAND ----------

# DBTITLE 1,Univariate: Release year histogram
# Visualize the distribution of types (e.g., Movie, TV Show)
import matplotlib.pyplot as plt

plt.figure(figsize=(8,5))
df['type'].value_counts().plot(kind='bar')
plt.xlabel('Type')
plt.ylabel('Count')
plt.title('Distribution of Content Types')
plt.tight_layout()
plt.show()

# COMMAND ----------

# DBTITLE 1,Bivariate: Type vs Rating
# Bivariate analysis: Visualize the count of each type (Movie, TV Show) by release year
import matplotlib.pyplot as plt

type_year_counts = df.groupby(['release_year', 'type']).size().unstack(fill_value=0)
type_year_counts.plot(kind='bar', stacked=True, figsize=(14,7))
plt.xlabel('Release Year')
plt.ylabel('Count')
plt.title('Count of Content Types by Release Year')
plt.tight_layout()
plt.show()

# COMMAND ----------

# DBTITLE 1,Bivariate: Type vs Release Year
# Bivariate analysis: Visualize the count of each type (Movie, TV Show) by release year
import matplotlib.pyplot as plt

type_year_counts = df.groupby(['release_year', 'type']).size().unstack(fill_value=0)
type_year_counts.plot(kind='bar', stacked=True, figsize=(14,7))
plt.xlabel('Release Year')
plt.ylabel('Count')
plt.title('Count of Content Types by Release Year')
plt.tight_layout()
plt.show()

# COMMAND ----------

# DBTITLE 1,Clean missing values and duplicates
#FEATURE ENGINEERING

# COMMAND ----------

# Step 1: Data Cleaning
import pandas as pd
import numpy as np

# Handle missing values
# For columns with many missing values, drop them if not useful
to_drop = [col for col in df.columns if df[col].isnull().mean() > 0.5]
df = df.drop(columns=to_drop)

# For remaining missing values, fill with sensible defaults
for col in ['country', 'director', 'cast']:
    if col in df.columns:
        df[col] = df[col].fillna('Unknown')

if 'date_added' in df.columns:
    df['date_added'] = df['date_added'].fillna(df['date_added'].mode()[0])

if 'rating' in df.columns:
    df['rating'] = df['rating'].fillna(df['rating'].mode()[0])

if 'duration' in df.columns:
    df['duration'] = df['duration'].fillna(df['duration'].median() if df['duration'].dtype != 'O' else df['duration'].mode()[0])

unnecessary_cols = ['show_id'] if 'show_id' in df.columns else []
df = df.drop(columns=unnecessary_cols)

# Check results
df.info()
df.isnull().sum()

# COMMAND ----------

# DBTITLE 1,Feature Extraction and Engineering


# COMMAND ----------

# DBTITLE 1,Date Features Extraction
# Date Features Extraction
import datetime
if 'date_added' in df.columns:
    df['date_added'] = pd.to_datetime(df['date_added'], errors='coerce')
    df['year_added'] = df['date_added'].dt.year
    df['month_added'] = df['date_added'].dt.month
    df['day_added'] = df['date_added'].dt.day
    df['weekday_added'] = df['date_added'].dt.weekday
    df['years_since_added'] = datetime.datetime.now().year - df['year_added']
print(df[['date_added','year_added','month_added','day_added','weekday_added','years_since_added']].head())

# COMMAND ----------

# DBTITLE 1,Content Age Feature
# Content Age Feature
if 'release_year' in df.columns:
    df['content_age'] = datetime.datetime.now().year - df['release_year']
print(df[['release_year','content_age']].head())

# COMMAND ----------

# DBTITLE 1,Duration Features Extraction
# Duration Features Extraction
def extract_duration(row):
    if 'min' in row:
        return int(row.split(' ')[0])
    return np.nan

def extract_seasons(row):
    if 'Season' in row:
        return int(row.split(' ')[0])
    return np.nan

if 'duration' in df.columns:
    df['duration_minutes'] = df['duration'].apply(extract_duration)
    df['num_seasons'] = df['duration'].apply(extract_seasons)
print(df[['duration','duration_minutes','num_seasons']].head())

# COMMAND ----------

# DBTITLE 1,Genre Features Extraction
# Genre Features Extraction
from collections import Counter
if 'listed_in' in df.columns:
    genre_list = df['listed_in'].str.split(',').apply(lambda x: [g.strip() for g in x])
    all_genres = Counter([g for sublist in genre_list for g in sublist])
    top_genres = [g for g, _ in all_genres.most_common(10)]
    for genre in top_genres:
        df[f'genre_{genre.replace(" ", "_")}'] = genre_list.apply(lambda x: int(genre in x))
    df['num_genres'] = genre_list.apply(len)
print(df[['listed_in','num_genres'] + [f'genre_{g.replace(" ", "_")}' for g in top_genres]].head())

# COMMAND ----------

# DBTITLE 1,Cast and Director Features Extraction
# Cast and Director Features Extraction
from collections import Counter
if 'cast' in df.columns:
    df['num_cast'] = df['cast'].apply(lambda x: len(x.split(',')) if x != 'Unknown' else 0)
    
    all_cast = Counter([actor.strip() for sublist in df['cast'].str.split(',') for actor in sublist if actor != 'Unknown'])
    top_cast = set([actor for actor, _ in all_cast.most_common(10)])
    df['top_cast_flag'] = df['cast'].apply(lambda x: int(any(actor.strip() in top_cast for actor in x.split(','))))
if 'director' in df.columns:
    all_directors = Counter([d.strip() for d in df['director'] if d != 'Unknown'])
    top_directors = set([d for d, _ in all_directors.most_common(10)])
    df['has_famous_director'] = df['director'].apply(lambda x: int(x in top_directors))
print(df[['cast','num_cast','top_cast_flag','director','has_famous_director']].head())

# COMMAND ----------

# DBTITLE 1,Country Features Extraction
# Country Features Extraction
from collections import Counter
if 'country' in df.columns:
    country_list = df['country'].str.split(',').apply(lambda x: [c.strip() for c in x])
    all_countries = Counter([c for sublist in country_list for c in sublist if c != 'Unknown'])
    top_countries = [c for c, _ in all_countries.most_common(10)]
    for country in top_countries:
        df[f'country_{country.replace(" ", "_")}'] = country_list.apply(lambda x: int(country in x))
    df['country_other'] = country_list.apply(lambda x: int(not any(c in top_countries for c in x)))
print(df[['country'] + [f'country_{c.replace(" ", "_")}' for c in top_countries] + ['country_other']].head())

# COMMAND ----------

# DBTITLE 1,Country Features Extraction
# Country Features Extraction
from collections import Counter
if 'country' in df.columns:
    country_list = df['country'].str.split(',').apply(lambda x: [c.strip() for c in x])
    all_countries = Counter([c for sublist in country_list for c in sublist if c != 'Unknown'])
    top_countries = [c for c, _ in all_countries.most_common(10)]
    for country in top_countries:
        df[f'country_{country.replace(" ", "_")}'] = country_list.apply(lambda x: int(country in x))
    df['country_other'] = country_list.apply(lambda x: int(not any(c in top_countries for c in x)))
print(df[['country'] + [f'country_{c.replace(" ", "_")}' for c in top_countries] + ['country_other']].head())