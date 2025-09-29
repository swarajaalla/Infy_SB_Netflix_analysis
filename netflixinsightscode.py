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

print("\nMissing values:\n", df.isnull().sum())



# COMMAND ----------

# Fill missing rating and duration with 0
df['rating'] = df['rating'].fillna(0)
df['duration'] = df['duration'].fillna(0)

# Verify
print("Nulls after handling:\n", df[['rating', 'duration']].isnull().sum())
display(df[['title', 'rating', 'duration']].head(10))


# COMMAND ----------

print("\nMissing values per column:")
print(df.isnull().sum())

# COMMAND ----------

print(df.shape[0])
df_cleaned = df.drop_duplicates()
print(df_cleaned.shape[0])
print(f"\nRemoved {df.shape[0] - df_cleaned.shape[0]} duplicate rows.")

# COMMAND ----------

#  Save cleaned file
output_path = "/Volumes/workspace/default/netflix/netflix_cleaned.csv"
df.to_csv(output_path, index=False)

print("\n Cleaned dataset saved at:", output_path)

# COMMAND ----------

# DBTITLE 1,normalization


from sklearn.preprocessing import LabelEncoder
import pandas as pd

# --- Label Encoding for 'rating' ---
label_encoder = LabelEncoder()
df['rating_encoded'] = label_encoder.fit_transform(df['rating'])

# --- One-Hot Encoding for 'type' ---
if 'type' in df.columns:
    df = pd.get_dummies(df, columns=['type'], prefix='type')

# --- Handle 'listed_in' (Genres) ---
df['listed_in'] = df['listed_in'].str.replace(", ", ",")  # normalize commas
df_exploded = df.assign(
    listed_in=df['listed_in'].str.split(',')
).explode('listed_in')

# One-hot encode genres
df_genres = pd.get_dummies(df_exploded['listed_in'], prefix='genre')

# Drop existing genre columns before joining to avoid overlap
genre_cols = [col for col in df_exploded.columns if col.startswith('genre_')]
df_exploded = df_exploded.drop(columns=genre_cols, errors='ignore')

# Merge back (one row per title, keep all genres)
df = df_exploded.join(df_genres).groupby(df_exploded.index).max()

# --- Handle 'country' ---
df['country'] = df['country'].str.replace(", ", ",")  # normalize commas
df_country_exploded = df.assign(
    country=df['country'].str.split(',')
).explode('country')

# One-hot encode countries
df_countries = pd.get_dummies(df_country_exploded['country'], prefix='country')

# Drop existing country columns before joining to avoid overlap
country_cols = [col for col in df_country_exploded.columns if col.startswith('country_')]
df_country_exploded = df_country_exploded.drop(columns=country_cols, errors='ignore')

# Merge back (one row per title, keep all countries)
df = df_country_exploded.join(df_countries).groupby(df_country_exploded.index).max()
# --- Final reset ---
df.reset_index(drop=True, inplace=True)

# Cast all integer columns to int64 to avoid Arrow type issues
for col in df.select_dtypes(include=['int', 'int8', 'int16', 'int32', 'uint8', 'uint16', 'uint32']).columns:
    df[col] = df[col].astype('int64')

display(df.head())


print("✅ Final columns after encoding:\n", df.columns.tolist())



# Save fully normalized dataset
output_path = "/Volumes/workspace/default/netflix/netflix_normalized.csv"
df.to_csv(output_path, index=False)

print("\n✅ Fully normalized dataset saved at:", output_path)
