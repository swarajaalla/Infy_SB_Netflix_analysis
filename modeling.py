# Databricks notebook source
import warnings
warnings.filterwarnings("ignore", category=UserWarning)
warnings.filterwarnings("ignore", category=RuntimeWarning)


# COMMAND ----------

import pandas as pd
from sklearn.preprocessing import MultiLabelBinarizer, StandardScaler
from sklearn.cluster import KMeans
import numpy as np

# Load the Netflix titles CSV into a pandas DataFrame
df_read = pd.read_csv(
    '/Volumes/workspace/default/netflix/netflix_feature_eda.csv'
)

# Prepare genre features
mlb = MultiLabelBinarizer()
genres = df_read['listed_in'].str.split(', ')
genre_features = mlb.fit_transform(genres)

# Prepare duration feature
def extract_duration(row):
    if row['type'] == 'Movie':
        try:
            return int(str(row['duration']).split(' ')[0])
        except:
            return 0
    elif row['type'] == 'TV Show':
        try:
            return int(str(row['duration']).split(' ')[0]) * 60  # Approximation: 1 season = 60 min
        except:
            return 0
    else:
        return 0

duration_feature = df_read.apply(
    extract_duration,
    axis=1
).values.reshape(-1, 1)

# Prepare rating feature (label encoding)
rating_map = {k: v for v, k in enumerate(df_read['rating'].unique())}
rating_feature = df_read['rating'].map(rating_map).fillna(0).values.reshape(-1, 1)

# Combine features
X = np.hstack([
    genre_features,
    duration_feature,
    rating_feature
])

# Standardize features
scaler = StandardScaler()
X_scaled = scaler.fit_transform(X)

# Apply K-Means clustering
kmeans = KMeans(
    n_clusters=5,
    random_state=42
)
df_read['cluster'] = kmeans.fit_predict(X_scaled)

# Display clustered data
display(
    df_read[[
        'title',
        'listed_in',
        'duration',
        'rating',
        'cluster'
    ]]
)

# COMMAND ----------

cluster_counts = df_read['cluster'].value_counts().sort_index()
display(cluster_counts)

# COMMAND ----------

import matplotlib.pyplot as plt
import seaborn as sns

# Pie Chart - Cluster Distribution
cluster_counts = df_read['cluster'].value_counts().sort_index()

plt.figure(figsize=(7, 7))
plt.pie(
    cluster_counts,
    labels=[f'Cluster {i}' for i in cluster_counts.index],
    autopct='%1.1f%%',
    startangle=140,
    colors=sns.color_palette('pastel')
)
plt.title('Netflix Titles Distribution by Cluster', fontsize=14)
plt.show()

# COMMAND ----------


# Classification: Predicting Content Type (Movie vs TV Show)

from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import classification_report, confusion_matrix, accuracy_score

# Prepare features (reuse from clustering)
X = np.hstack([genre_features, duration_feature, rating_feature])

# Target variable
y = df_read['type'].map({'Movie': 0, 'TV Show': 1})  # Encode: Movie=0, TV Show=1

# Split dataset
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42, stratify=y)

# Train Random Forest classifier
clf = RandomForestClassifier(n_estimators=100, random_state=42)
clf.fit(X_train, y_train)

# Predictions
y_pred = clf.predict(X_test)

# Evaluation
print("Accuracy:", accuracy_score(y_test, y_pred))
print("\nClassification Report:\n", classification_report(y_test, y_pred))
print("\nConfusion Matrix:\n", confusion_matrix(y_test, y_pred))


# COMMAND ----------

plt.figure(figsize=(5,4))
sns.heatmap(confusion_matrix(y_test, y_pred), annot=True, fmt='d', cmap='Purples')
plt.title('Confusion Matrix: Movie vs TV Show Classification')
plt.xlabel('Predicted Label')
plt.ylabel('True Label')
plt.xticks([0.5, 1.5], ['Movie', 'TV Show'])
plt.yticks([0.5, 1.5], ['Movie', 'TV Show'])
plt.show()


# COMMAND ----------



# Key Drivers for Content Availability Across Countries and Genres

# Handle missing country information
df_read['country'] = df_read['country'].fillna('Unknown')

# Split multiple countries into separate rows for accurate analysis
df_exploded = df_read.assign(country=df_read['country'].str.split(', ')).explode('country')

# -------------------------------
# 1️⃣ Top Countries by Content Count
# -------------------------------
country_counts = df_exploded['country'].value_counts().head(15)

plt.figure(figsize=(10,5))
sns.barplot(x=country_counts.values, y=country_counts.index, palette='Purples_r')
plt.title('Top 15 Countries by Number of Netflix Titles')
plt.xlabel('Number of Titles')
plt.ylabel('Country')
plt.show()

# -------------------------------
# 2️⃣ Top Genres Globally
# -------------------------------
genre_counts = df_read['listed_in'].value_counts().head(10)

plt.figure(figsize=(10,5))
sns.barplot(x=genre_counts.values, y=genre_counts.index, palette='Purples_r')
plt.title('Top 10 Most Common Genres on Netflix')
plt.xlabel('Count')
plt.ylabel('Genre')
plt.show()

# -------------------------------
# 3️⃣ Country vs Genre Relationship
# -------------------------------
# Get top countries and genres for a focused view
top_countries = df_exploded['country'].value_counts().head(10).index
top_genres = df_read['listed_in'].value_counts().head(8).index

filtered_df = df_exploded[df_exploded['country'].isin(top_countries) & df_exploded['listed_in'].isin(top_genres)]

pivot_table = pd.crosstab(filtered_df['country'], filtered_df['listed_in'])

plt.figure(figsize=(12,6))
sns.heatmap(pivot_table, cmap='Purples', linewidths=0.3)
plt.title('Genre Availability Across Top Countries')
plt.xlabel('Genre')
plt.ylabel('Country')
plt.show()

# -------------------------------
# 4️⃣ Cluster Distribution Across Countries
# -------------------------------
cluster_country = df_exploded.groupby(['country', 'cluster']).size().unstack(fill_value=0)

plt.figure(figsize=(12,6))
sns.heatmap(cluster_country, cmap='Purples', linewidths=0.3)
plt.title('Distribution of Clusters Across Countries')
plt.xlabel('Cluster')
plt.ylabel('Country')
plt.show()

# -------------------------------
# 5️⃣ Insights Display
# -------------------------------
print("✅ Insights Summary:")
print("- Countries like United States, India, and United Kingdom dominate Netflix content.")
print("- Genres such as International Movies, Dramas, and Comedies are globally widespread.")
print("- Some countries focus on specific genres: e.g., India → Romantic & Dramas, US → Documentaries & Action.")
print("- Cluster distribution shows similar genre-duration patterns within countries.")


# COMMAND ----------



# Feature Importance to Interpret Key Drivers

# Combine all feature names
feature_names = list(mlb.classes_) + ['Duration', 'Rating']

# Get feature importances from the trained Random Forest model
importances = clf.feature_importances_

# Create a DataFrame for easier interpretation
feat_imp_df = pd.DataFrame({
    'Feature': feature_names,
    'Importance': importances
}).sort_values(by='Importance', ascending=False)

# Display top 15 most important features
top_features = feat_imp_df.head(15)

plt.figure(figsize=(10,6))
sns.barplot(x='Importance', y='Feature', data=top_features, palette='Purples_r')
plt.title('Top 15 Important Features for Classifying Movies vs TV Shows')
plt.xlabel('Feature Importance Score')
plt.ylabel('Feature')
plt.show()

display(top_features)

# -------------------------------
# Interpretation Summary
# -------------------------------
print("✅ Interpretation Summary:")
print("- 'Duration' is typically the most important feature since Movies have a specific runtime, while TV Shows have episodes/seasons.")
print("- Certain genres (e.g., 'TV Dramas', 'Children & Family Movies', 'International TV Shows') show strong influence — they correlate closely with content type.")
print("- 'Rating' contributes moderately — certain ratings (like 'TV-MA' or 'PG-13') are more common in specific content types.")
print("- Overall, genre mix + duration explain most of the model’s predictive power.")


# COMMAND ----------

