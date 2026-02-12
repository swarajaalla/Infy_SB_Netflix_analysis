# Databricks notebook source
import pandas as pd
df_cleaned = pd.read_csv("/Volumes/workspace/default/netflix/cleaned_netflix.csv")
df_cleaned.head()

import matplotlib.pyplot as plt
import seaborn as sns
# style for better visuals
sns.set(style="whitegrid")

# genre count
genre_counts = df_cleaned['listed_in'].str.split(', ', expand=True).stack().value_counts()
display(genre_counts)

# Top 10 Most Common Genres
top_genres = genre_counts.head(10)
plt.title("Top 10 Most Common Genres")
top_genres.plot(kind='bar', color='pink')
plt.xlabel("Genre")
plt.ylabel("Count")
plt.show()

# content type
content_type_counts = df_cleaned['type'].value_counts()
display(content_type_counts)

# Movies vs TV Shows comparision
df_cleaned['type'].value_counts().plot(kind='pie')
plt.title("Distribution of Content Type (Movies vs TV Shows)")
plt.xlabel("Type")
plt.ylabel("Count")
plt.show()

# Country counts
country_counts = df_cleaned['country'].str.split(', ', expand=True).stack().value_counts()
display(country_counts)

# Top 10 Countries with Most Content
top_countries = df_cleaned['country'].value_counts().head(10)
top_countries.plot(kind = 'bar', color = 'orange')
plt.title("Top 10 Countries Contributing Content")
plt.xlabel("Number of Titles")
plt.ylabel("Country")
plt.show()

# Trend of Netflix content released per year
content_per_year = df_cleaned['release_year'].value_counts().sort_index()
content_per_year.plot(kind='line', marker='o')
plt.title("Content Growth Over Time")
plt.xlabel("Year")
plt.ylabel("Number of Titles Released")
plt.show()

# Duration Analysis (Movies)
movie_df = df_cleaned[df_cleaned['type'] == 'Movie']
movie_df['duration'] = pd.to_numeric(movie_df['duration'], errors='coerce')

plt.figure(figsize=(8,5))
sns.histplot(movie_df['duration'], bins=30, kde=True, color="#2ecc71")
plt.title("Distribution of Movie Duration")
plt.xlabel("Duration (minutes)")
plt.ylabel("Frequency")
plt.show()


# Filter dataset for recent years (2010 and above), Group by year and type
recent_data = df_cleaned[df_cleaned['release_year'] >= 2010]
content_by_year_type = recent_data.groupby(['release_year', 'type']).size().unstack(fill_value=0)
content_by_year_type.plot(kind='bar', stacked=True, figsize=(10,5), colormap='cool')

plt.title("Netflix Content Growth (2010-Present)")
plt.xlabel("Release Year")
plt.ylabel("Number of Titles")
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()

# Ratings Distribution
plt.figure(figsize=(10,5))
sns.countplot(data=df_cleaned, x='rating', order=df_cleaned['rating'].value_counts().index, palette='coolwarm')
plt.title("Distribution of Ratings (Audience Categories)")
plt.xlabel("Rating")
plt.ylabel("Count")
plt.xticks(rotation=45)
plt.show()
