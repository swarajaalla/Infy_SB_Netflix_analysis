# Databricks notebook source
afile_path = "/Volumes/workspace/default/netflix_titles/netflix_titles.csv"

# COMMAND ----------

import pandas as pd
df = pd.read_csv('/Volumes/workspace/default/netflix_titles/netflix_titles.csv')
df.display()

# COMMAND ----------

print(df.isnull().sum())
print(df.isnull().mean()*100)

# COMMAND ----------

df = df.dropna(how = "all")
df.isnull().sum()

# COMMAND ----------

print(df.duplicated())

# COMMAND ----------

df = df.drop_duplicates()
df.shape

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ****FEATURE** **ENGENEERING****

# COMMAND ----------



# COMMAND ----------

# Duration and content length
def categorize_content_length(duration):
    if pd.isnull(duration):
        return 'Unknown'
    if 'Season' in duration:
        return 'Series'
    try:
        minutes = int(duration.split(' ')[0])
        if minutes < 40:
            return 'Short'
        elif minutes < 90:
            return 'Medium'
        else:
            return 'Long'
    except:
        return 'Unknown'

df['content_length_category'] = df['duration'].apply(categorize_content_length)
df[['duration', 'content_length_category']].head()

# COMMAND ----------

#Create a new feature 'Content_length_category' based on the duration of content
def get_contente_lenght_category(row):
    duration = str(row['duration'])
    if pd.isnull(duration) or duration.lower() == 'nan':
        return 'unknown'
    if 'Season' in duration:
        try:
            seasons = int(duration.split()[0])
            if seasons == 1:
                return 'Single Seasons'
            elif seasons <= 3:
                return 'Few Seasons'
            else:
                return 'Many Seasons'
        except:
            return 'Unkonwn'
    elif 'min' in duration:
        try:
            minutes = int(duration.split()[0])
            if minutes < 60:
                return 'Short'
            elif minutes < 120:
                return 'medium'
            else:
                return 'long'
        except:
            return 'unknown'
    else:
        return 'unkonwn'

df['contente_lenght_category'] = df.apply(get_contente_lenght_category, axis=1)
df = df[['type', 'duration', 'contente_lenght_category']]
display(df)

# COMMAND ----------

#Create a new feature 'origin_type' to classify 'original' or 'licensed' content
df = pd.read_csv('/Volumes/workspace/default/netflix_titles/netflix_titles.csv')

def classify_origin(row):
    title_lower = str(row['title']).lower()
    if 'original' in title_lower:
        return 'Original'
    else:
        return 'Licensed'

df['origin_type'] = df.apply(classify_origin, axis=1)
display(df[['title', 'origin_type']])

# COMMAND ----------

# How many originals were released in each year
df['date_added'] = pd.to_datetime(df['date_added'], errors='coerce')
df['year_added'] = df['date_added'].dt.year
originals_per_year = df[df['origin_type'] == 'Original'].groupby('year_added').size().reset_index(name='originals_count')
display(originals_per_year)

# COMMAND ----------



# COMMAND ----------

#How many licenced were released in each year 
licensed_per_year = df[df['origin_type'] == 'Licensed'].groupby('year_added').size().reset_index(name='licensed_count')
display(licensed_per_year)

# COMMAND ----------

# Original VS Licensed 
import matplotlib.pyplot as plt

origin_counts = df['origin_type'].value_counts()
origin_counts.plot(kind='bar', color=['#1f77b4', '#ff7f0e'])
plt.xlabel('Content Type')
plt.ylabel('Count')
plt.title('Original vs Licensed Content')
plt.xticks(rotation=0)
plt.show()

# COMMAND ----------

# Original VS Licensed
import matplotlib.pyplot as plt

origin_counts = df['origin_type'].value_counts()
plt.figure(figsize=(6,6))
plt.pie(origin_counts, labels=origin_counts.index, autopct='%1.1f%%', colors=['#1f77b4', '#ff7f0e'], startangle=90)
plt.title('Original vs Licensed Content')
plt.axis('equal')
plt.show()