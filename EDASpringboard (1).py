# Databricks notebook source


# COMMAND ----------

    import pandas as pd
    data=pd.read_csv("/Volumes/workspace/default/netflix_dataset/netflix_cleaned.csv")
    df=pd.DataFrame(data)
    df.head(10)

# COMMAND ----------


df.isnull().sum()

# COMMAND ----------

# DBTITLE 1,Check data types
print(df.dtypes)

# COMMAND ----------

# DBTITLE 1,Categorical value counts
# Value counts for categorical columns
categorical_cols = df.select_dtypes(include=['object', 'category']).columns
for col in categorical_cols:
    print(f"\nValue counts for {col}:")
    print(df[col].value_counts().head(5))

# COMMAND ----------

# DBTITLE 1,Visualize type distribution
import pandas as pd
df = pd.read_csv("/Volumes/workspace/default/netflix_dataset/netflix_cleaned.csv")
import matplotlib.pyplot as plt
plt.figure(figsize=(10,6))
df['release_year'].value_counts().sort_index().plot(kind='bar')
plt.xlabel('Release Year')
plt.ylabel('Count')
plt.title('Distribution of Release Years')
plt.tight_layout()
plt.show()

# COMMAND ----------

# DBTITLE 1,Visualize release year distribution
# Visualize the top 10 countries with the most titles
import matplotlib.pyplot as plt

top_countries = df['country'].value_counts().head(10)
plt.figure(figsize=(10,6))
top_countries.plot(kind='bar')
plt.xlabel('Country')
plt.ylabel('Number of Titles')
plt.title('Top 10 Countries by Number of Netflix Titles')
plt.tight_layout()
plt.show()

# COMMAND ----------

# DBTITLE 1,Visualize top countries
# Visualize the top 10 genres with the most titles
import matplotlib.pyplot as plt

# Split genres if multiple are present in a single cell, then count
genre_counts = df['listed_in'].str.split(',').explode().str.strip().value_counts().head(10)
plt.figure(figsize=(10,6))
genre_counts.plot(kind='bar')
plt.xlabel('Genre')
plt.ylabel('Number of Titles')
plt.title('Top 10 Genres by Number of Netflix Titles')
plt.tight_layout()
plt.show()

# COMMAND ----------

# DBTITLE 1,Visualize top genres
import matplotlib.pyplot as plt

# Split genres if multiple are present in a single cell, then count
genre_counts = df['listed_in'].str.split(',').explode().str.strip().value_counts().head(10)
plt.figure(figsize=(10,6))
genre_counts.plot(kind='bar')
plt.xlabel('Genre')
plt.ylabel('Number of Titles')
plt.title('Top 10 Genres by Number of Netflix Titles')
plt.tight_layout()
plt.show()

# COMMAND ----------

# DBTITLE 1,Univariate: Type distribution
# Visualize the distribution of ratings
import matplotlib.pyplot as plt

plt.figure(figsize=(10,6))
df['rating'].value_counts().plot(kind='bar')
plt.xlabel('Rating')
plt.ylabel('Count')
plt.title('Distribution of Ratings')
plt.tight_layout()
plt.show()

# COMMAND ----------

# DBTITLE 1,Univariate: Rating distribution
# Visualize the distribution of release years as a histogram
import matplotlib.pyplot as plt

plt.figure(figsize=(10,6))
df['release_year'].hist(bins=20)
plt.xlabel('Release Year')
plt.ylabel('Count')
plt.title('Histogram of Release Years')
plt.tight_layout()
plt.show()

# COMMAND ----------

# DBTITLE 1,Univariate: Release year histogram
# Visualize the distribution of types (e.g., Movie, TV Show)
import matplotlib.pyplot as plt

plt.figure(figsize=(8,5))
df['type'].value_counts().plot(kind='bar')
plt.xlabel('Type')
plt.ylabel('Count')
plt.title('Distribution of Content Types')
plt.tight_layout()
plt.show()

# COMMAND ----------

# DBTITLE 1,Bivariate: Type vs Rating


# COMMAND ----------

# DBTITLE 1,Bivariate: Type vs Release Year
