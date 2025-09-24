# Databricks notebook source


# COMMAND ----------

/Volumes/workspace/default/netflix_dataset/netflix_titles.csv

# COMMAND ----------

import pandas as pd
df=pd.read_csv("/Volumes/workspace/default/netflix_dataset/netflix_titles.csv")
df.head()

# COMMAND ----------

#check dataset structure         
print(df.info()) #column type and null values     
print(df.head(3))#sample records with 3 values

# COMMAND ----------

#To remove duplicates
df.drop_duplicates(inplace=True)
display(df)

# COMMAND ----------

#Handling missing values
df['director'].fillna("Unknown", inplace=True)
df['cast'].fillna("Unknown", inplace=True)
df['country'].fillna("Unknown", inplace=True)

# COMMAND ----------

#Filling missing rating with not rated
df['rating'].fillna("Not Rated", inplace=True)
display(df)

# COMMAND ----------

#checks for null values
print(df.isnull().sum())

# COMMAND ----------

#Save cleaned dataset 
df.to_csv("/Volumes/workspace/default/netflix_dataset/netflix_cleaned.csv", index=False)
print("Cleaned dataset saved as 'netflix_cleaned.csv'")