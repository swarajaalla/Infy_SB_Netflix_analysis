# Databricks notebook source
import pandas as pd
# Load into pandas
df_read=pd.read_csv("/Volumes/workspace/default/netflix/netflix_titles.csv")
df_read.display()

# COMMAND ----------

# Shape and info
print("Shape of dataset:", df_read.shape)
print("\nData Types:\n", df_read.dtypes)

# COMMAND ----------

# Check duplicates
print("Duplicates before:", df_read.duplicated().sum())

# Drop duplicates
df_read = df_read.drop_duplicates()

print("Shape after dropping duplicates:", df_read.shape)

# COMMAND ----------

df_read.isnull().sum().sort_values(ascending=False)


# COMMAND ----------

# Fill missing values with defaults
df_read['director'] = df_read['director'].fillna("Unknown")
df_read['cast'] = df_read['cast'].fillna("Not Available")
df_read['country'] = df_read['country'].fillna("Unknown")
df_read['date_added'] = df_read['date_added'].fillna("Not Available")
df_read['rating'] = df_read['rating'].fillna("Not Rated")
df_read['duration'] = df_read['duration'].fillna("Unknown")

# Drop rows missing critical fields
df_read = df_read.dropna(subset=['title', 'type'])

print("After null handling:", df_read.shape)


# COMMAND ----------

text_cols = df.select_dtypes(include=['object', 'category']).columns
print("Text columns:", text_cols.tolist())

# COMMAND ----------

# Standardize text fields for consistency
df_read['duration'] = df_read['duration'].str.strip().str.title()
df_read['cast'] = df_read['cast'].str.strip()
display(df_read.head())

# COMMAND ----------

df_clean=df_read


# COMMAND ----------

# 1. Top 10 directors by number of titles
top_directors = df_clean['director'].value_counts().head(10)

# 2. Distribution of Movies vs. TV Shows
type_distribution = df_clean['type'].value_counts()

# 3. Top 10 countries with most content
top_countries = df_clean['country'].value_counts().head(10)

# 4. Most common ratings
top_ratings = df_clean['rating'].value_counts().head(10)

# Display results
print("Top Directors:\n", top_directors, "\n")
print("Type Distribution:\n", type_distribution, "\n")
print("Top Countries:\n", top_countries, "\n")
print("Top Ratings:\n", top_ratings, "\n")

# COMMAND ----------

# Save cleaned dataset to CSV
df_clean.to_csv("/Volumes/workspace/default/netflix/netflix_cleaned.csv", index=False)
print("Cleaned dataset saved as 'netflix_cleaned.csv'")