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

#  Save cleaned file
df_cleaned.to_csv("/Volumes/workspace/default/netflix/netflix_cleaned.csv", index=False)
print("\n Cleaned dataset saved as 'netflix_cleaned.csv'")

# COMMAND ----------

# DBTITLE 1,normalization
from sklearn.preprocessing import LabelEncoder
import pandas as pd

pd.set_option('display.max_rows', None)  

print("\n---  Label Encoding for 'rating' ---")

df_normalized = df_cleaned.copy()

label_encoder = LabelEncoder()
df_normalized['rating_label'] = label_encoder.fit_transform(df_cleaned['rating'].astype(str))

display(df_normalized[['rating', 'rating_label']])


# COMMAND ----------

from sklearn.preprocessing import OneHotEncoder

print("\n---  One-Hot Encoding for 'type' and 'listed_in' ---")

onehot_encoder = OneHotEncoder(sparse_output=False, drop=None)

# Encode 'type'
type_encoded_array = onehot_encoder.fit_transform(df_cleaned[['type']])
df_type_onehot = pd.DataFrame(
    type_encoded_array, 
    columns=onehot_encoder.get_feature_names_out(['type'])
)
df_type_onehot.index = df_cleaned.index

# Encode 'listed_in' (each unique genre separately — multi-hot encoding)
listed_encoded_array = onehot_encoder.fit_transform(df_cleaned[['listed_in']])
df_listed_onehot = pd.DataFrame(
    listed_encoded_array, 
    columns=onehot_encoder.get_feature_names_out(['listed_in'])
)
df_listed_onehot.index = df_cleaned.index

# Combine into df_normalized
df_normalized = pd.concat([df_normalized, df_type_onehot, df_listed_onehot], axis=1)

display(df_normalized.head())

# COMMAND ----------

from sklearn.preprocessing import OrdinalEncoder

print("\n---  Ordinal Encoding for 'country' ---")

country_order = df_cleaned['country'].value_counts().index.tolist()

ordinal_encoder_country = OrdinalEncoder(categories=[country_order])
df_normalized['country_ordinal'] = ordinal_encoder_country.fit_transform(df_cleaned[['country']])

display(df_normalized[['country', 'country_ordinal']])


# COMMAND ----------

df_normalized.to_csv("/Volumes/workspace/default/netflix/netflix_normalized.csv", index=False)
print("\nAll normalized columns saved in 'netflix_normalized.csv'")
