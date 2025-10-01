# Databricks notebook source
# MAGIC %restart_python
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %pip install kaggle

# COMMAND ----------

download_path ="/Volumes/workspace/default/netflix/kaggle"
dbutils.fs.mkdirs(download_path)

# COMMAND ----------

import os
os.environ['KAGGLE_USERNAME'] = "ankitahub"
os.environ['KAGGLE_KEY'] ="10ab0b2b8f256946825ceb12f5c30199"

# COMMAND ----------

!kaggle datasets download -d shivamb/netflix-shows -p {download_path} --unzip


# COMMAND ----------

import pandas as pd
df = pd.read_csv(download_path+"/netflix_titles.csv")


# COMMAND ----------

df

# COMMAND ----------

df.head(5)

# COMMAND ----------

df.info()

# COMMAND ----------

df = df.drop_duplicates(subset=["title", "type"])

# COMMAND ----------

df.rename(columns={
    "show_id": "id",
    "type": "content_type",
    "listed_in": "genres"
}, inplace=True)

# COMMAND ----------

print("Missing values before cleaning:\n", df.isnull().sum())

# COMMAND ----------

fill_values = {
    "director": "Unknown",
    "cast": "Unknown",
    "country": "Unknown",
    "rating": "Not Rated",
    "duration": "Unknown"
}
df.fillna(value=fill_values, inplace=True)

# COMMAND ----------

df["date_added"] = pd.to_datetime(df["date_added"], errors="coerce").dt.strftime("%d-%m-%y")

# COMMAND ----------

df[["duration_value", "duration_unit"]] = df["duration"].str.extract(r'(\d+)\s*(\w+)')
df["duration_value"] = pd.to_numeric(df["duration_value"], errors="coerce")

# COMMAND ----------

df.dropna(subset=["title", "content_type"], inplace=True)

# COMMAND ----------

df.reset_index(drop=True, inplace=True)

# COMMAND ----------

print("Missing values after cleaning:\n", df.isnull().sum())
print(df.info())
print("\nSample cleaned data:\n", df.head())

# COMMAND ----------

df

# COMMAND ----------

# download_path = "/Volumes/workspace/default/netflix/cleaned_data"

df.to_csv(f"{download_path}/netflix_newcleaned.csv", index=False)



# COMMAND ----------

NORMALISATION

# COMMAND ----------

from sklearn.preprocessing import MinMaxScaler, OneHotEncoder
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.impute import SimpleImputer

# COMMAND ----------

df = pd.read_csv(f"{download_path}/netflix_newcleaned.csv")


# COMMAND ----------

numeric_cols = ["release_year", "duration_value"]
categorical_cols = ["content_type", "rating", "country", "duration_unit"]

# COMMAND ----------

numeric_transformer = Pipeline(steps=[
    ("imputer", SimpleImputer(strategy="median")),   # fill missing values
    ("scaler", MinMaxScaler())                       # scale 0–1
])

categorical_transformer = Pipeline(steps=[
    ("imputer", SimpleImputer(strategy="most_frequent")), # fill missing with most common
    ("encoder", OneHotEncoder(handle_unknown="ignore"))   # one-hot encode
])

# COMMAND ----------

preprocessor = ColumnTransformer(
    transformers=[
        ("num", numeric_transformer, numeric_cols),
        ("cat", categorical_transformer, categorical_cols)
    ],
    remainder="drop"   # drop unused columns (like id, title, description, cast, etc.)
)

# COMMAND ----------

processed = preprocessor.fit_transform(df)

# COMMAND ----------

new_cat_cols = preprocessor.named_transformers_["cat"]["encoder"].get_feature_names_out(categorical_cols)
all_new_cols = numeric_cols + list(new_cat_cols)

df_normalized = pd.DataFrame(processed.toarray() if hasattr(processed, "toarray") else processed, 
                             columns=all_new_cols)

# COMMAND ----------

df_normalized

# COMMAND ----------

df_normalized.to_csv(f"{download_path}/netflix_normalized_full.csv", index=False)


# COMMAND ----------

