# Databricks notebook source
# --- Import Libraries ---
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# --- Load cleaned dataset ---
df = pd.read_csv("netflix_cleaned_data_final2.csv")

print("✅ Dataset Loaded Successfully!")
print("Shape of dataset:", df.shape)
display(df.head())


# COMMAND ----------

print(" Dataset Info")
df.info()


# COMMAND ----------

df.describe()

# COMMAND ----------

plt.figure(figsize=(6,4))

# assign hue to match new seaborn behavior
sns.countplot(data=df, x='is_tv_show', hue='is_tv_show', palette='coolwarm', legend=False)

plt.title("Distribution: Movies vs TV Shows", fontsize=14)
plt.xlabel("Type (0 = Movie, 1 = TV Show)", fontsize=12)
plt.ylabel("Count", fontsize=12)
plt.xticks([0, 1], ['Movie', 'TV Show'])
plt.show()

# percentage of each type
tv_counts = df['is_tv_show'].value_counts(normalize=True) * 100
print("\nPercentage distribution:")
print(tv_counts)


# COMMAND ----------

top_countries = df['country'].value_counts().head(10)

plt.figure(figsize=(8,4))

# Assign hue to match Seaborn’s latest requirements
sns.barplot(
    x=top_countries.values,
    y=top_countries.index,
    hue=top_countries.index,     # added hue
    palette='viridis',
    legend=False
)

plt.title("Top 10 Countries Producing Netflix Content", fontsize=14)
plt.xlabel("Number of Titles", fontsize=12)
plt.ylabel("Country", fontsize=12)
plt.tight_layout()
plt.show()


# COMMAND ----------

import seaborn as sns

# Convert to numeric, coerce errors
df['added_year'] = pd.to_numeric(df['added_year'], errors='coerce')

# Remove invalid years (0 or NaN)
df = df[df['added_year'] > 0]

# COMMAND ----------

yearly = df.groupby('added_year').size().sort_index()


# COMMAND ----------

plt.figure(figsize=(10,5))
sns.lineplot(x=yearly.index, y=yearly.values, marker='o', color='teal')
plt.title("Trend of Netflix Content Added per Year")
plt.xlabel("Year Added to Netflix")
plt.ylabel("Number of Titles")
plt.show()


# COMMAND ----------

# Fill missing values
df['rating_simplified'] = df['rating_simplified'].fillna('Unknown').astype(str)

plt.figure(figsize=(8,4))
sns.countplot(
    data=df,
    x='rating_simplified',
    hue='rating_simplified',  # add hue to use palette
    palette='cool',
    dodge=False,              # avoid splitting bars
    legend=False              # hide redundant legend
)
plt.title("Content Distribution by Rating")
plt.xlabel("Rating Category")
plt.ylabel("Count")
plt.show()

# COMMAND ----------

plt.figure(figsize=(7,4))
sns.histplot(df['duration_num'], bins=30, kde=True, color='salmon')
plt.title("Distribution of Content Duration")
plt.xlabel("Duration (Minutes or Seasons)")
plt.ylabel("Count")
plt.show()


# COMMAND ----------

!pip install wordcloud


# COMMAND ----------

from wordcloud import WordCloud

text = " ".join(df['listed_in'].dropna())
wordcloud = WordCloud(width=800, height=400, background_color='white', colormap='plasma').generate(text)

plt.figure(figsize=(10,5))
plt.imshow(wordcloud, interpolation='bilinear')
plt.axis('off')
plt.title("Popular Genres on Netflix")
plt.show()


# COMMAND ----------

plt.figure(figsize=(8,6))
sns.heatmap(df.corr(numeric_only=True), annot=True, cmap='coolwarm', fmt='.2f')
plt.title("Correlation Heatmap of Numeric Features")
plt.show()


# COMMAND ----------

print("✅ EDA Completed Successfully!")
