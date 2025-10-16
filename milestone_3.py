# Databricks notebook source

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import classification_report
from sklearn.ensemble import RandomForestClassifier
df = pd.read_csv("netflix_cleaned_data_final2.csv")
# Feature Engineering - Create Genre Dummies
# This is crucial for analyzing genres in clustering and classification.
genre_dummies = df['listed_in'].str.get_dummies(sep=', ')
model_df = pd.concat([df, genre_dummies], axis=1)

print("✅ Data and Genre Dummies Ready for Modeling!")

# COMMAND ----------


#  Select Features and Scale Data
# Select numerical/ordinal features and all the new genre columns
clustering_features = ['duration_num', 'rating_ordinal'] + list(genre_dummies.columns)

# Drop rows with NaN (missing values) in the selected features for simplicity
X_clust = model_df[clustering_features].dropna()
model_df = model_df.loc[X_clust.index] # Keep model_df aligned

# Initialize and apply StandardScaler
scaler = StandardScaler()
X_clust_scaled = scaler.fit_transform(X_clust)

print(f"Total Features for Clustering: {len(clustering_features)}")
print("✅ Data Scaled!")

# COMMAND ----------


# The Elbow Method to find K
wcss = [] # Within-Cluster Sum of Squares
for i in range(1, 11):
    # n_init=10 is the number of times K-Means will run with different initial centroids
    kmeans = KMeans(n_clusters=i, init='k-means++', max_iter=300, n_init=10, random_state=42)
    kmeans.fit(X_clust_scaled)
    wcss.append(kmeans.inertia_) # inertia is the WCSS

# Plot the result
plt.figure(figsize=(8, 4))
plt.plot(range(1, 11), wcss, marker='o')
plt.title('Elbow Method for Optimal K')
plt.xlabel('Number of Clusters (K)')
plt.ylabel('WCSS')
plt.show()

# Choosing K=4 or K=5 is usually a good starting point based on typical results
K_OPTIMAL = 4

# COMMAND ----------


#  Apply K-Means and Analyze
kmeans = KMeans(n_clusters=K_OPTIMAL, init='k-means++', max_iter=300, n_init=10, random_state=42)
model_df['Cluster'] = kmeans.fit_predict(X_clust_scaled)

# Analyze the characteristics of each cluster
cluster_analysis = model_df.groupby('Cluster')[['duration_num', 'rating_ordinal', 'is_tv_show']].mean()

print("\n--- Cluster Average Characteristics ---")
print(cluster_analysis)
# Interpretation example: A cluster with high is_tv_show average is a TV Show dominant cluster.

# Analyze top genres per cluster
genre_cols = list(genre_dummies.columns)
cluster_genre_counts = model_df.groupby('Cluster')[genre_cols].sum()

print("\n--- Cluster Top Genres (Top 3 per Cluster) ---")
for i in range(K_OPTIMAL):
    top_3_genres = cluster_genre_counts.loc[i].nlargest(3)
    print(f"Cluster {i}: {', '.join(top_3_genres.index)} (Count: {top_3_genres.values})")

print(f"\n✅ Clustering complete! Titles grouped into {K_OPTIMAL} clusters.")

# COMMAND ----------


#  Define Features (X) and Target (Y)

# Target: is_tv_show (0=Movie, 1=TV Show)
y = model_df['is_tv_show']

# Features: duration, rating, country code, and genre dummies
# We use the pre-encoded 'country_code' and 'director_code' from Milestone 1 for simplicity
X_features = ['duration_num', 'rating_ordinal', 'country_code'] + list(genre_dummies.columns)
X = model_df[X_features]

# Split data into Training (70%) and Testing (30%)
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3, random_state=42, stratify=y)

# Scale only the numerical features in X (duration)
scaler = StandardScaler()
X_train['duration_num'] = scaler.fit_transform(X_train[['duration_num']])
X_test['duration_num'] = scaler.transform(X_test[['duration_num']])

print("✅ Data split and scaled for Classification!")

# COMMAND ----------


#  Train and Evaluate Logistic Regression
# Initialize a simple Logistic Regression model
# class_weight='balanced' helps the model deal with unequal numbers of Movies and TV Shows
log_model = LogisticRegression(random_state=42, class_weight='balanced', max_iter=1000)

# Train the model
log_model.fit(X_train, y_train)

# Make predictions
y_pred = log_model.predict(X_test)

# Print the Classification Report
print("\n--- Classification Report (Movie vs. TV Show) ---")
print(classification_report(y_test, y_pred))
# Key metrics: Precision (accuracy of positive predictions) and Recall (coverage of positive predictions)

print("\n✅ Logistic Regression Trained and Evaluated.")

# COMMAND ----------


# Feature Importance (Coefficients)

# Create a DataFrame of feature names and their coefficients
coefficients = pd.DataFrame({
    'Feature': X_features,
    # The coefficient's absolute value shows importance. Positive sign means it increases TV Show probability.
    'Coefficient': log_model.coef_[0]
})

# Sort by the absolute value of the coefficient
coefficients['Abs_Coefficient'] = coefficients['Coefficient'].abs()
feature_importance = coefficients.sort_values(by='Abs_Coefficient', ascending=False).drop('Abs_Coefficient', axis=1)

print("\n--- Top 10 Features Driving TV Show Classification ---")
print(feature_importance.head(10))

# Visualize the top 10 (most influential features, positive or negative)
plt.figure(figsize=(10, 5))
sns.barplot(x='Coefficient', y='Feature', data=feature_importance.head(10), palette='coolwarm')
plt.title('Feature Influence on Predicting TV Show (Positive = TV Show, Negative = Movie)')
plt.show()

# Interpretation: A large positive coefficient (e.g., 'TV Show Genre') strongly suggests a TV Show (1).
# A large negative coefficient (e.g., 'duration_num') strongly suggests a Movie (0).

# COMMAND ----------


#  Analyze Country-Genre Drivers

# Select the top 5 countries (excluding Unknown)
top_countries = df['country'].value_counts().head(6).index.tolist()
top_countries = [c for c in top_countries if c != 'Unknown']

# Filter the genre dummy table for these top countries
country_genre_df = model_df[model_df['country'].isin(top_countries)]
country_genre_counts = country_genre_df.groupby('country')[list(genre_dummies.columns)].sum()

# Display the top 3 genres for the US and India as examples
print("\n--- Top 3 Genres in Key Content Hubs ---")
if 'United States' in top_countries:
    print(f"\nUnited States Top Genres:")
    print(country_genre_counts.loc['United States'].nlargest(3))

if 'India' in top_countries:
    print(f"\nIndia Top Genres:")
    print(country_genre_counts.loc['India'].nlargest(3))

print("\n✅ Key Drivers Analysis Complete!")