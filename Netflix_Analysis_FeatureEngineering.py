# Databricks notebook source
# Step 3: Feature Engineering
# Import libraries
import pandas as pd
import numpy as np
from sklearn.preprocessing import LabelEncoder, MinMaxScaler, MultiLabelBinarizer

# COMMAND ----------

# Load cleaned dataset
df = pd.read_csv("/Workspace/Users/hemavarshinis.23aid@kongu.edu/NETFLIX_INF_CLEANED.csv")

# COMMAND ----------

# Preview data
print("Dataset shape:", df.shape)
print(df.head())

# COMMAND ----------

# 1. Encode categorical variables
# a) Encode Content Type (Movie / TV Show)
df['type_encoded'] = df['type'].map({'Movie': 0, 'TV Show': 1})


# COMMAND ----------

# b) Simplify Ratings
rating_map = {
    'TV-Y': 'Kids', 'TV-Y7': 'Kids', 'G': 'Kids',
    'PG': 'Teens', 'TV-PG': 'Teens',
    'R': 'Adults', 'TV-MA': 'Adults'
}
df['rating_group'] = df['rating'].map(rating_map)
df['rating_group'].fillna('Unknown', inplace=True)

le = LabelEncoder()
df['rating_encoded'] = le.fit_transform(df['rating_group'])

# COMMAND ----------

# c) Encode Genres (listed_in)
df['genre_list'] = df['listed_in'].fillna('').apply(lambda x: x.split(', '))
mlb = MultiLabelBinarizer()
genre_encoded = pd.DataFrame(mlb.fit_transform(df['genre_list']), columns=mlb.classes_)
df = pd.concat([df, genre_encoded], axis=1)


# COMMAND ----------

# d) Encode Country (top 10)
df['country'] = df['country'].fillna('Unknown')
top_countries = df['country'].value_counts().nlargest(10).index
df['country'] = df['country'].apply(lambda x: x if x in top_countries else 'Other')
df = pd.get_dummies(df, columns=['country'], drop_first=True)

# COMMAND ----------

# 2. Feature Creation
# a) Content Age
current_year = 2025
df['content_age'] = current_year - df['release_year']

# COMMAND ----------

# b) Duration Category
def categorize_duration(x):
    if pd.isna(x):
        return 'Unknown'
    x = str(x)
    if 'min' in x:
        mins = int(x.split()[0])
        if mins < 60:
            return 'Short'
        elif mins <= 120:
            return 'Medium'
        else:
            return 'Long'
    elif 'Season' in x:
        return 'Series'
    else:
        return 'Unknown'

df['duration_category'] = df['duration'].apply(categorize_duration)

# Encode duration category
df['duration_encoded'] = LabelEncoder().fit_transform(df['duration_category'])

# COMMAND ----------

# c) Original vs Licensed (NaN in Director)
df['is_original'] = df['director'].isna().astype(int)


# COMMAND ----------

# 3. Scaling numerical features
scaler = MinMaxScaler()
df['content_age_scaled'] = scaler.fit_transform(df[['content_age']])


# COMMAND ----------

# 4. Save Feature-Engineered Dataset

df.to_csv("netflix_feature_engineered.csv", index=False)

print("\n✅ Feature Engineering Completed Successfully!")
print("Saved as 'netflix_feature_engineered.csv'")
print("Final dataset shape:", df.shape)
