# Databricks notebook source
# MAGIC %restart_python

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
df = pd.read_csv(download_path+"/netflix_newcleaned.csv")

# COMMAND ----------

df

# COMMAND ----------

print("Missing values before cleaning:\n", df.isnull().sum())

# COMMAND ----------

df.duplicated().sum()


# COMMAND ----------

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# COMMAND ----------

# Check column names to find release or date-added column
print(df.columns)

# Convert 'release_year' or 'date_added' to datetime (if exists)
df['date_added'] = pd.to_datetime(df['date_added'], errors='coerce')

# Extract the year
df['year_added'] = df['date_added'].dt.year

# Count number of titles per year
content_per_year = df['year_added'].value_counts().sort_index()

# Plot content growth
plt.figure(figsize=(10,5))
sns.lineplot(x=content_per_year.index, y=content_per_year.values)
plt.title("Netflix Content Growth Over Time")
plt.xlabel("Year Added to Netflix")
plt.ylabel("Number of Titles")
plt.grid(True)
plt.show()


# COMMAND ----------

plt.figure(figsize=(6,4))
sns.countplot(x='content_type', data=df, palette='pastel')
plt.title("Distribution of Content Type (Movies vs TV Shows)")
plt.xlabel("Type")
plt.ylabel("Count")
plt.show()


# COMMAND ----------

plt.figure(figsize=(10,5))
sns.countplot(y='rating', data=df, order=df['rating'].value_counts().index, palette='cool')
plt.title("Distribution of Ratings")
plt.xlabel("Count")
plt.ylabel("Rating")
plt.show()


# COMMAND ----------

# Some rows may have multiple genres separated by commas
df['genres'] = df['genres'].astype(str)

# Split and count
from collections import Counter
genre_list = df['genres'].str.split(', ')
genres = [g for sublist in genre_list for g in sublist]
top_genres = pd.DataFrame(Counter(genres).most_common(10), columns=['Genre', 'Count'])

# Plot
plt.figure(figsize=(10,5))
sns.barplot(x='Count', y='Genre', data=top_genres, palette='magma')
plt.title("Top 10 Most Common Genres on Netflix")
plt.show()


# COMMAND ----------

df['country'] = df['country'].astype(str)
country_list = df['country'].str.split(', ')
countries = [c for sublist in country_list for c in sublist]
top_countries = pd.DataFrame(Counter(countries).most_common(10), columns=['Country', 'Count'])

# Plot
plt.figure(figsize=(10,5))
sns.barplot(x='Count', y='Country', data=top_countries, palette='viridis')
plt.title("Top 10 Countries Contributing to Netflix Content")
plt.show()


# COMMAND ----------

plt.figure(figsize=(6,4))
sns.heatmap(df.corr(numeric_only=True), annot=True, cmap='coolwarm')
plt.title("Correlation Heatmap of Numeric Features")
plt.show()


# COMMAND ----------

# MAGIC %md
# MAGIC Red / Dark Orange	Strong Positive Correlation	e.g. release_year ↑ → year_added ↑
# MAGIC
# MAGIC 🔵 Blue / Dark Purple	Strong Negative Correlation	e.g. duration_int ↓ → year_added ↑
# MAGIC
# MAGIC ⚪ White / Light Colors	Weak or No Correlation

# COMMAND ----------

