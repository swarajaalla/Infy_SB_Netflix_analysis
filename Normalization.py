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

text_cols = df_read.select_dtypes(include=['object', 'category']).columns
print("Text columns:", text_cols.tolist())

# COMMAND ----------

# Standardize text fields for consistency
df_read['duration'] = df_read['duration'].str.strip().str.title()
df_read['cast'] = df_read['cast'].str.strip()
display(df_read.head())

# COMMAND ----------

df_read['date_added'] = pd.to_datetime(df_read['date_added'], errors='coerce')
df_read.head(10)

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

# COMMAND ----------

import warnings
warnings.filterwarnings("ignore", category=UserWarning)
from sklearn.preprocessing import LabelEncoder, OneHotEncoder, OrdinalEncoder, MinMaxScaler
df_encoded = df_clean.copy()

# COMMAND ----------

df_normalised = df_encoded.copy()

# -------------------------------
# 1. Label Encoding for 'rating'
label_encoder = LabelEncoder()
df_normalised['rating_label'] = label_encoder.fit_transform(df_normalised['rating'].astype(str))

display(df_normalised[['rating', 'rating_label']].head(20))

# COMMAND ----------

# 2. One-Hot Encoding for 'type'
# -------------------------------
onehot_encoder = OneHotEncoder(sparse_output=False, drop=None)
type_encoded = onehot_encoder.fit_transform(df_normalised[['type']])
df_type_onehot = pd.DataFrame(type_encoded, columns=onehot_encoder.get_feature_names_out(['type']))
df_type_onehot.index = df_normalised.index


# COMMAND ----------


# Merge one-hot encoded columns
df_normalised = pd.concat([df_normalised, df_type_onehot], axis=1)
display(df_normalised.head(20))

# COMMAND ----------

# 3. Ordinal Encoding for 'country'
# -------------------------------
country_order = df_normalised['country'].value_counts().index.tolist()
ordinal_encoder = OrdinalEncoder(categories=[country_order])
df_normalised['country_ordinal'] = ordinal_encoder.fit_transform(df_normalised[['country']])


display(df_normalised[['country', 'country_ordinal']].head())


# COMMAND ----------

# 4. Normalization (MinMaxScaler)
# -------------------------------
scaler = MinMaxScaler()

# Normalize numerical + encoded columns (not one-hot, since they’re already 0/1)
cols_to_normalize = ['rating_label', 'country_ordinal']
df_normalised[cols_to_normalize] = scaler.fit_transform(df_normalised[cols_to_normalize])



# COMMAND ----------

# -------------------------------
# Display final combined dataframe
display(df_normalised.head(10))

# COMMAND ----------

# Save normalised dataset to CSV
df_normalised.to_csv("/Volumes/workspace/default/netflix/netflix_normalised.csv", index=False)
print("Normalised dataset saved as 'netflix_normalised.csv'")