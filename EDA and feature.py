# Databricks notebook source
# Databricks notebook source
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from collections import Counter

# Load cleaned dataset
df_read = pd.read_csv("/Volumes/workspace/default/netflix/netflix_cleaned.csv")

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)

display(df_read)



# COMMAND ----------


# Netflix Content Growth Over Time

# Convert date_added to datetime
df_read['date_added'] = pd.to_datetime(df_read['date_added'], errors='coerce')

# Extract year from date_added
df_read['year_added'] = df_read['date_added'].dt.year

# Count titles added per year
content_growth = df_read['year_added'].value_counts().sort_index()

# Plot
plt.figure(figsize=(10,5))
plt.plot(content_growth.index, content_growth.values, marker='o', color='purple')
plt.title("Netflix Content Growth Over Time", fontsize=14)
plt.xlabel("Year Added")
plt.ylabel("Number of Titles Added")
plt.grid(True)
plt.show()


# COMMAND ----------

#content type distribution
plt.figure(figsize=(6,4))
sns.countplot(x='type', data=df_read, hue='type', palette='Set2', legend=False)
plt.title('Distribution of Content Type (Movies vs TV Shows)', fontsize=14)
plt.xlabel('Content Type')
plt.ylabel('Count')
plt.show()


# COMMAND ----------

#  top 10 genres using pandas
from collections import Counter

# Split and flatten the 'listed_in' column to get all genres
all_genres = df_read['listed_in'].str.split(',').explode().str.strip()

# Count the occurrences of each genre
top_genres = all_genres.value_counts().head(10).reset_index()
top_genres.columns = ['Genre', 'Count']

# Plot
plt.figure(figsize=(10,6))
sns.barplot(
    y='Genre',
    x='Count',
    data=top_genres,
    hue='Genre',
    palette='mako',
    legend=False
)
plt.title('Top 10 Most Frequent Genres on Netflix', fontsize=14)
plt.xlabel('Count')
plt.ylabel('Genre')
plt.show()

# COMMAND ----------

#  top 10 most frequent ratings
top_ratings = df_read['rating'].value_counts().head(10).index
filtered_df = df_read[df_read['rating'].isin(top_ratings)]

plt.figure(figsize=(8,6))
sns.countplot(
    y='rating',
    data=filtered_df,
    order=filtered_df['rating'].value_counts().index,
    palette='rocket',
    hue='rating',
    legend=False
)
plt.title('Distribution of Top 10 Content Ratings', fontsize=14)
plt.xlabel('Count')
plt.ylabel('Rating')
plt.show()

# COMMAND ----------

# Calculate top 10 genres using pandas
all_genres = df_read['listed_in'].str.split(',').explode().str.strip()
top_genres = all_genres.value_counts().head(10).reset_index()
top_genres.columns = ['Genre', 'Count']

# Plot
plt.figure(figsize=(10, 6))
sns.barplot(
    y="Genre",
    x="Count",
    data=top_genres,
    hue="Genre",
    palette="mako",
    legend=False
)
plt.title("Top 10 Most Frequent Genres on Netflix", fontsize=14)
plt.xlabel("Count")
plt.ylabel("Genre")
plt.show()

# COMMAND ----------

# Calculate top 10 countries contributing Netflix content
all_countries = df_read['country'].dropna().str.split(',').explode().str.strip()
top_countries = all_countries.value_counts().head(10)

# Plot
plt.figure(figsize=(10,6))
sns.barplot(
    x=top_countries.values,
    y=top_countries.index,
    palette='viridis',
    hue=top_countries.index,
    legend=False
)
plt.title("Top 10 Countries Contributing to Netflix Content", fontsize=14)
plt.xlabel("Number of Titles")
plt.ylabel("Country")
plt.show()

# COMMAND ----------


#Content Length
# Extract numeric duration
df_read['duration_num'] = pd.to_numeric(
    df_read['duration'].str.extract(r'(\d+)')[0], errors='coerce'
)

# Function for content length category
def content_length_category(duration):
    if pd.isnull(duration):
        return "Unknown"
    if duration <= 60:
        return "Short (<1hr)"
    elif 60 < duration <= 120:
        return "Medium (1–2hrs)"
    elif 120 < duration <= 200:
        return "Long (2–3hrs)"
    elif duration > 200:
        return "Very Long (>3hrs)"
    else:
        return "Unknown"

df_read['Content_Length_Category'] = df_read['duration_num'].apply(content_length_category)

# Show all rows
pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)

display(df_read[['title', 'duration', 'duration_num', 'Content_Length_Category']])

# Plot distribution without FutureWarning
plt.figure(figsize=(8,6))
sns.countplot(
    x='Content_Length_Category',
    data=df_read,
    color='skyblue'  # Single color avoids hue warning
)
plt.title("Distribution of Content Length Categories", fontsize=14)
plt.xlabel("Content Length Category")
plt.ylabel("Count")
plt.show()



# COMMAND ----------

#Feature Engineering - Original vs Licensed
def original_vs_licensed(title):
    if "Netflix" in str(title):
        return "Original"
    else:
        return "Licensed"

df_read['Original_vs_Licensed'] = df_read['title'].apply(original_vs_licensed)

# Show all rows
pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
display(df_read[['title', 'Original_vs_Licensed']])

# Pie chart for Original vs Licensed
counts = df_read['Original_vs_Licensed'].value_counts()

plt.figure(figsize=(7,7))
plt.pie(counts, labels=counts.index, autopct='%1.1f%%', startangle=140, colors=['#ff9999','#66b3ff'])
plt.title('Distribution of Original vs Licensed Content', fontsize=14)
plt.show()

# COMMAND ----------

#content growth by month
# Ensure 'date_added' is datetime
df_read['date_added'] = pd.to_datetime(df_read['date_added'], errors='coerce')

# Group by month and plot
monthly_counts = df_read.groupby(df_read['date_added'].dt.month).size()
monthly_counts.plot(
    kind='bar',
    figsize=(8, 5),
    color='skyblue'
)
plt.title("Netflix Content Added by Month")
plt.xlabel("Month")
plt.ylabel("Number of Titles")
plt.show()

# COMMAND ----------

#genre vs content type
plt.figure(figsize=(12,6))
sns.countplot(y='listed_in', hue='type', data=df_read, order=df_read['listed_in'].value_counts().iloc[:10].index)
plt.title("Top Genres by Content Type")
plt.show()


# COMMAND ----------

#rating distrubution by content type
plt.figure(figsize=(10,6))
sns.countplot(x='rating', hue='type', data=df_read, palette='Set1')
plt.title("Rating Distribution by Content Type")
plt.xticks(rotation=45)
plt.show()



# COMMAND ----------

numeric_df = df_read.select_dtypes(include=['float64','int64'])
plt.figure(figsize=(12,8))
sns.heatmap(numeric_df.corr(), annot=True, cmap="coolwarm")
plt.title("Correlation Heatmap")
plt.show()


# COMMAND ----------

df_read.to_csv("/Volumes/workspace/default/netflix/netflix_feature_eda.csv", index=False)
print("Feature engineered dataset saved successfully.")
