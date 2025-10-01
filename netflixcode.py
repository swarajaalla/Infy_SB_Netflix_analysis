# Databricks notebook source
import pandas as pd


# Load into pandas
df_read = pd.read_csv("/Volumes/workspace/default/netflix/netflix_titles.csv")

display(df_read)




# COMMAND ----------

print(f'rows: {df_read.shape[0]},col:{df_read.shape[1]}')
print(df_read.columns)
print(df_read.info())

# COMMAND ----------

df_read.describe(include='all')


# COMMAND ----------

print("\nMissing values per column:")
print(df_read.isnull().sum())

# COMMAND ----------

df_head=df_read.dropna(subset=['date_added'])
display(df_head)
print("\nMissing values per column:")
print(df_head.isnull().sum())
print(df_head.shape[0])
df_read['country']=df_read['country'].fillna("Unknown")


# COMMAND ----------

df_read['director'] = df_read['director'].fillna("Not Available")
df_read['cast'] = df_read['cast'].fillna("Not Available")
df_read['country'] = df_read['country'].fillna("Unknown")
df_read['duration'] = df_read['duration'].fillna("0")

# COMMAND ----------

mode_rating=df_read['rating'].mode()[0]
df_read['rating']=df_read['rating'].fillna("mode_rating")

# COMMAND ----------

print("\nMissing values per column:")
print(df_read.isnull().sum())


# COMMAND ----------

# DBTITLE 1,duplicate handling
print(df_read.shape[0])
df_cleaned = df_read.drop_duplicates()
print(df_cleaned.shape[0])
print(f"\nRemoved {df_read.shape[0] - df_cleaned.shape[0]} duplicate rows.")

# COMMAND ----------

# DBTITLE 1,white spaces cleaning
df_cleaned['type']=df_cleaned['type'].str.strip()
df_cleaned['rating']=df_cleaned['rating'].str.strip()


# COMMAND ----------

df_read['duration_num']=df_read['duration'].str.extract(r'(\df)').astype(float)
df_cleaned['duration_type']=df_cleaned['duration'].str.extract(r'(min|Season|Seasons)')


# COMMAND ----------

display(df_read)

# COMMAND ----------

df_read.type.value_counts()

# COMMAND ----------

df_read['date_added'] = pd.to_datetime(df_read['date_added'], errors='coerce')
display(df_read['date_added'].min())
display(df_read['date_added'].max())

# COMMAND ----------

df_read.listed_in.value_counts()

# COMMAND ----------

df_cleaned.to_csv("/Volumes/workspace/default/netflix/netflix_cleaned.csv", index=False)
print("\n Cleaned dataset saved as 'netflix_cleaned.csv'")

# COMMAND ----------

#  Copy cleaned dataset and set display options
import pandas as pd
from sklearn.preprocessing import LabelEncoder, OrdinalEncoder, MinMaxScaler

pd.set_option('display.max_columns', None)  # show all columns
pd.set_option('display.max_rows', None)     # show all rows

# Copy cleaned dataset
df_norm = df_cleaned.copy()


# COMMAND ----------

#  Min-Max Scaling for 'duration_num' and display all rows
# Extract numeric duration
df_norm['duration_num'] = pd.to_numeric(
    df_norm['duration'].str.extract(r'(\d+)')[0], errors='coerce'
).fillna(0)

# Min-Max Scaling
scaler = MinMaxScaler()
df_norm['duration_norm'] = scaler.fit_transform(df_norm[['duration_num']])

# Display duration columns for all rows
display(df_norm[['duration', 'duration_num', 'duration_norm']])


# COMMAND ----------

# One-Hot Encoding for 'type' and display all rows
df_type_onehot = pd.get_dummies(df_norm['type'], prefix='type')

# Cast all columns to int32 to avoid Arrow type issues
df_type_onehot = df_type_onehot.astype('int32')

df_norm = pd.concat([df_norm, df_type_onehot], axis=1)

pd.set_option('display.max_rows', None)
display(df_type_onehot)

# COMMAND ----------

#Frequency Encoding for 'country' and display all rows
country_freq = df_norm['country'].value_counts().to_dict()
df_norm['country_freq'] = df_norm['country'].map(country_freq)

display(df_norm[['country', 'country_freq']])


# COMMAND ----------

# Remove rows with invalid ratings (not in the allowed list)
valid_ratings = [
    'G', 'PG', 'PG-13', 'R', 'NC-17',
    'TV-Y', 'TV-Y7', 'TV-G', 'TV-PG', 'TV-14', 'TV-MA', 'Not Rated',
    'NR', 'UR', 'Not Rated', 'TV-Y7-FV'
]
df_norm = df_norm[df_norm['rating'].isin(valid_ratings)]

# Fix uncommon ratings
df_norm['rating'] = df_norm['rating'].replace(
    ['NR', 'UR', 'Not Rated', 'TV-Y7-FV'],
    'Not Rated'
)

# Ordinal encoding
rating_order = [[
    'G', 'PG', 'PG-13', 'R', 'NC-17',
    'TV-Y', 'TV-Y7', 'TV-G', 'TV-PG', 'TV-14', 'TV-MA', 'Not Rated'
]]
encoder = OrdinalEncoder(categories=rating_order)
df_norm['rating_ord'] = encoder.fit_transform(df_norm[['rating']])

display(df_norm[['rating', 'rating_ord']])

# COMMAND ----------

#  Label Encoding for 'rating', 'country', 'director', 'cast'
label_cols = ['rating', 'country', 'director', 'cast']
for col in label_cols:
    le = LabelEncoder()
    df_norm[col + '_label'] = le.fit_transform(df_norm[col].astype(str))

display(df_norm[['rating_label','country_label','director_label','cast_label']])


# COMMAND ----------

# Genres encoding
# Primary genre label encoding
df_norm['primary_genre'] = df_norm['listed_in'].str.split(',').str[0]
le_genre = LabelEncoder()
df_norm['primary_genre_label'] = le_genre.fit_transform(
    df_norm['primary_genre'].astype(str)
)

# Multi-hot encoding for first 2 genres only to reduce dimensionality
df_norm['genre_1'] = df_norm['listed_in'].str.split(',').str[0]
df_norm['genre_2'] = df_norm['listed_in'].str.split(',').str[1]

df_genre_onehot = pd.get_dummies(
    df_norm[['genre_1', 'genre_2']].fillna(''),
    prefix=['genre1', 'genre2']
)

df_genre_onehot = df_genre_onehot.astype('int32')

df_norm = pd.concat([df_norm, df_genre_onehot], axis=1)

display(df_genre_onehot)

# COMMAND ----------

#  Extract date features and display all rows
df_norm['date_added'] = pd.to_datetime(df_norm['date_added'], errors='coerce')
df_norm['year_added'] = df_norm['date_added'].dt.year
df_norm['month_added'] = df_norm['date_added'].dt.month
df_norm['day_added'] = df_norm['date_added'].dt.day
df_norm['dayofweek_added'] = df_norm['date_added'].dt.dayofweek

display(df_norm[['date_added','year_added','month_added','day_added','dayofweek_added']])


# COMMAND ----------

#  Save final normalized dataset
df_norm.to_csv("/Volumes/workspace/default/netflix/netflix_normalized.csv", index=False)
print("\n✅ Normalized dataset saved as 'netflix_normalized.csv'")
print(f"Shape of dataset: {df_norm.shape}")
print(f"Columns: {df_norm.columns[:20]}")
