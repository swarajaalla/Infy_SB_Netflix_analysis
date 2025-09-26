# Databricks notebook source
import pandas as pd
df=pd.read_csv("/Volumes/workspace/default/infosys_data/netflix_titles.csv")
print(df)
df.display()

# COMMAND ----------

df.head()

# COMMAND ----------

df.tail()


# COMMAND ----------

df.describe()

# COMMAND ----------

df.info()

# COMMAND ----------

df.isnull().sum()

# COMMAND ----------

import pandas as pd
df=pd.read_csv("/Volumes/workspace/default/infosys_data/netflix_titles.csv")
df.drop_duplicates(subset='director', inplace=True)
df.display()


# COMMAND ----------

df.drop_duplicates(subset=['director'], inplace=True)
df.drop_duplicates(subset=['cast'], inplace=True)
df.drop_duplicates(subset=['country'], inplace=True)
df.drop_duplicates(subset=['date_added'], inplace=True)
df.drop_duplicates(subset=['rating'], inplace=True)




# COMMAND ----------

df.isnull().sum()

# COMMAND ----------

df['director'].fillna('Unknown', inplace=True)
df['cast'].fillna('Unknown', inplace=True)
df['rating'].fillna('Unknown', inplace=True)
df['duration'].fillna('Unknown', inplace=True)
df['date_added'].fillna('Unknown', inplace=True)




# COMMAND ----------

df.isnull().sum()

# COMMAND ----------

import pandas as pd
df=pd.read_csv("/Volumes/workspace/default/infosys_data/netflix_titles.csv")

# 1️⃣ Handle 'duration' column
# Separate minutes and seasons
df['duration_min'] = df['duration'].str.replace(' min','', regex=False)
df['duration_seasons'] = df['duration'].str.replace(' Seasons','', regex=False)
df['duration_season_single'] = df['duration'].str.replace(' Season','', regex=False)

# Convert numeric parts to numbers
df['duration_min'] = pd.to_numeric(df['duration_min'], errors='coerce')
df['duration_seasons'] = pd.to_numeric(df['duration_seasons'], errors='coerce')
df['duration_seasons'] = df['duration_seasons'].fillna(pd.to_numeric(df['duration_season_single'], errors='coerce'))

# Drop helper column
df.drop(columns=['duration','duration_season_single'], inplace=True)

# 2️⃣ Convert 'date_added' to datetime
df['date_added'] = pd.to_datetime(df['date_added'], errors='coerce')

# 3️⃣ Convert text/categorical columns to numeric codes
categorical_cols = ['show_id','type','title','director','cast','country','rating','listed_in','description']
for col in categorical_cols:
    df[col] = df[col].astype('category').cat.codes
print(df.head())


# COMMAND ----------

import pandas as pd
df=pd.read_csv("/Volumes/workspace/default/infosys_data/netflix_titles.csv")
print(df.shape[0])
df_cleaned=df.drop_duplicates()
print(df_cleaned.shape[0])
print(f"\n removed{df.shape[0]-df_cleaned.shape[0]}.duplicate rows.")

# COMMAND ----------

df_cleaned['duration_num']=df_cleaned['duration'].str.extract(r'(\d+)', expand=False).astype(float)
df_cleaned['duration_type']=df_cleaned['duration'].str.extract(r'(min|season|seasons)')

# COMMAND ----------

print(df_cleaned)

# COMMAND ----------

df.country.value_counts()

# COMMAND ----------

df.type.value_counts()

# COMMAND ----------

df.listed_in.value_counts()

# COMMAND ----------

df['date_added'] = pd.to_datetime(df['date_added'])
df['month'] = df['date_added'].dt.month
display(df)

# COMMAND ----------

df.date_added.max()