# Databricks notebook source
import pandas as pd


# Load into pandas
df = pd.read_csv("/Volumes/workspace/default/netflix/netflix_titles.csv")

df.display()




# COMMAND ----------

# Drop duplicate rows
df = df.drop_duplicates()

print("After removing duplicates:", df.shape)


# COMMAND ----------

# Check null counts
print("Null values before cleaning:\n", df.isnull().sum())

# Fill missing values with defaults
df['director'] = df['director'].fillna("Unknown")
df['cast'] = df['cast'].fillna("Not Available")
df['country'] = df['country'].fillna("Unknown")
df['date_added'] = df['date_added'].fillna("Not Available")
df['rating'] = df['rating'].fillna("Not Rated")
df['duration'] = df['duration'].fillna("Unknown")

# Drop rows missing critical fields
df = df.dropna(subset=['title', 'type'])

print("After null handling:", df.shape)


# COMMAND ----------

# Standardize text fields for consistency
df['type'] = df['type'].str.strip().str.title()
df['rating'] = df['rating'].str.strip()
df['country'] = df['country'].str.strip().str.title()

display(df.head())


# COMMAND ----------

# Clean up text formatting
df['type'] = df['type'].str.strip().str.title()
df['rating'] = df['rating'].str.strip()
df['country'] = df['country'].str.strip().str.title()

display(df.head())


# COMMAND ----------

# Confirm no nulls left
df.isnull().sum()

# Preview cleaned dataset
display(df.head())


# COMMAND ----------

#  Save cleaned file
output_path = "/Volumes/workspace/default/netflix/netflix_cleaned.csv"
df.to_csv(output_path, index=False)

print("\n Cleaned dataset saved at:", output_path)