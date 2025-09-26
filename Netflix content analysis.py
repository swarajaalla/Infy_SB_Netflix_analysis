# Databricks notebook source
"/Volumes/workspace/default/netflix/netflix_analysis.csv"

import pandas as pd
df = pd.read_csv("/Volumes/workspace/default/netflix/netflix_analysis.csv")
df.shape
df.head()
# ============================================================================
#CLEANING THE DATASET 
# ============================================================================

# 1. Remove Duplicates
df.info()
df = df.drop_duplicates()
df = df.drop_duplicates(subset=['title','release_year'])

# 2. Handle Missing Values
df.isnull().sum()

# Fill missing categorical values with "Unknown"
for col in ['director', 'cast', 'country', 'rating']:
    df[col] = df[col].fillna("Unknown")
    
# 3. fixing to datetime format 
df['date_added'] = pd.to_datetime(df['date_added'], errors='coerce')

# Create a binary flag for missing 'date_added'
df['date_missing'] = df['date_added'].isna().astype(int)

# 4. Normalize Multi-Value Columns
# Breaks columns with multiple value into multiple rows.
df_exploded = df.assign(genre=df['listed_in'].str.split(',')).explode('genre')
df_exploded['genre'] = df_exploded['genre'].str.strip()

# 5. Handling Outliers
df['duration'] = df['duration'].str.replace(' min', '').str.replace(' Season[s]?', '', regex=True)

# =================================================================================
# NORMALIZE CATEGORICAL FEATURES 
# =================================================================================

# 6. no extra spaces
df['type'] = df['type'].str.strip() 

# 7. Rating based on Age Group
rating_map = {
    'G': 'Kids', 'TV-Y': 'Kids', 'TV-G': 'Kids',
    'PG': 'Family', 'TV-PG': 'Family', 'TV-Y7': 'Family', 'TV-Y7-FV': 'Family',
    'PG-13': 'Teens', 'TV-14': 'Teens',
    'R': 'Adults', 'NC-17': 'Adults', 'TV-MA': 'Adults'
}

df['rating_group'] = df['rating'].map(rating_map).fillna('Unknown')
df.display()

# 8. standard naming
df['country'] = df['country'].replace({'USA': 'United States'})

# 9. Converts categories into binary
pd.get_dummies(df['type'], prefix='type')

# 10. Grouping Rare Categories to avoid noise
rare_countries = df['country'].value_counts()[df['country'].value_counts()<20].index
df['country'] = df['country'].replace(rare_countries, 'Other')

cleaned_file_path = "/Volumes/workspace/default/netflix/cleaned_netflix.csv"
df.to_csv(cleaned_file_path, index=False)

# ====================================================================================
# Basic EDA (Exploratory Data Analysis)
# ====================================================================================

import matplotlib.pyplot as plt
import seaborn as sns
# style for better visuals
sns.set(style="whitegrid")

# Movies vs TV Shows comparision
df['type'].value_counts().plot(kind='bar', color=['#ff9999','#66b3ff'])
plt.title("Distribution of Content Type (Movies vs TV Shows)")
plt.xlabel("Type")
plt.ylabel("Count")
plt.show()

# Trend of Netflix content released per year
content_per_year = df['release_year'].value_counts().sort_index()
content_per_year.plot(kind='line', marker='o')
plt.title("Content Growth Over Time")
plt.xlabel("Year")
plt.ylabel("Number of Titles Released")
plt.show()

# Top 10 Countries with Most Content
top_countries = df['country'].value_counts().head(10)
top_countries.plot(kind='barh', color='skyblue')
plt.title("Top 10 Countries Contributing Content")
plt.xlabel("Number of Titles")
plt.ylabel("Country")
plt.show()

# Distribution of Ratings (Kids, Teens, Adults, etc)
df['rating_group'].value_counts().plot(kind='bar', color='lightgreen')
plt.title("Distribution of Content by Rating Group")
plt.xlabel("Rating Group")
plt.ylabel("Count")
plt.show()

# First and Latest Show Release
print("Earliest Release Year:", df['release_year'].min())
print("Most Recent Release Year:", df['release_year'].max())

# Top 10 Most Common Genres
top_genres = df_exploded['genre'].value_counts().head(10)
top_genres.plot(kind='bar', color='orange')
plt.title("Top 10 Most Common Genres")
plt.xlabel("Genre")
plt.ylabel("Count")
plt.show()
