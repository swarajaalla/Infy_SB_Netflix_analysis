# Databricks notebook source
# Netflix EDA 

# Step 1: Import libraries
import pandas as pd
import matplotlib.pyplot as plt

# COMMAND ----------

# Step 2: Load the dataset
df = pd.read_csv("/Workspace/NETFLIX_INF.csv")

# COMMAND ----------

# Step 3: Initial Inspection
print("Shape of dataset:", df.shape)
print("\nColumns:\n", df.columns)
print("\nFirst 5 rows:\n", df.head())
print("\nMissing Values:\n", df.isnull().sum())


# COMMAND ----------

# Step 4: Handle Missing Values (Data Cleaning)
# ----------------------------------------------
# Fill missing categorical values and extract year
df['country'] = df['country'].fillna('Unknown')
df['rating'] = df['rating'].fillna('Not Rated')
df['type'] = df['type'].fillna('Unknown')


# COMMAND ----------

# Extract year from 'date_added' if available
def extract_year(date):
    try:
        return pd.to_datetime(date).year
    except:
        return None

df['year_added'] = df['date_added'].apply(extract_year)
df['year'] = df['year_added'].fillna(df['release_year'])
df['year'] = pd.to_numeric(df['year'], errors='coerce').astype('Int64')


# COMMAND ----------

# Save cleaned dataset
df.to_csv("netflix_cleaned.csv", index=False)
print("\n✅ Data cleaned and saved as 'netflix_cleaned.csv'")

# COMMAND ----------

# Step 5: Exploratory Data Analysis (EDA)

# (A) Content Growth Over Time
growth = df.dropna(subset=['year']).groupby('year').size().reset_index(name='Count')
plt.figure(figsize=(10,5))
plt.plot(growth['year'], growth['Count'], marker='o')
plt.title("📈 Netflix Content Growth Over Years")
plt.xlabel("Year")
plt.ylabel("Number of Titles")
plt.grid(True)
plt.show()

# COMMAND ----------

# (B) Genre Distribution
genres = df['listed_in'].dropna().str.split(',').explode().str.strip()
top_genres = genres.value_counts().head(20)
plt.figure(figsize=(10,6))
top_genres.plot(kind='bar')
plt.title("🎭 Top 20 Genres on Netflix")
plt.xlabel("Genre")
plt.ylabel("Count")
plt.show()

# COMMAND ----------

# (C) Rating Distribution
ratings = df['rating'].value_counts()
plt.figure(figsize=(10,5))
ratings.plot(kind='bar')
plt.title("🔤 Rating Distribution")
plt.xlabel("Rating")
plt.ylabel("Count")
plt.show()

# COMMAND ----------

# (D) Content Type (Movies vs TV Shows)
types = df['type'].value_counts()
plt.figure(figsize=(6,4))
types.plot(kind='bar')
plt.title("🎬 Content Type Distribution")
plt.xlabel("Type")
plt.ylabel("Count")
plt.show()


# COMMAND ----------

# (E) Country-Level Analysis
countries = df['country'].dropna().str.split(',').explode().str.strip()
top_countries = countries.value_counts().head(15)
plt.figure(figsize=(10,6))
top_countries.plot(kind='bar')
plt.title("🌍 Top 15 Countries Producing Netflix Content")
plt.xlabel("Country")
plt.ylabel("Number of Titles")
plt.show()

print("\n✅ EDA Completed Successfully!")
