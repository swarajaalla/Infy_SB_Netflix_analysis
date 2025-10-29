# Databricks notebook source
# K-MEANS CLUSTERING ON NETFLIX DATASET
# Step 1: Import required libraries
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.preprocessing import LabelEncoder, StandardScaler
from sklearn.cluster import KMeans


# COMMAND ----------

# Step 2: Load the dataset
df = pd.read_csv("/Workspace/Users/hemavarshinis.23aid@kongu.edu/netflix_feature_engineered.csv")

print("Dataset Loaded Successfully!")
print("Shape of Dataset:", df.shape)
print(df.head())


# COMMAND ----------

# Step 3: Select relevant features for clustering
# (Modify based on your actual column names)
possible_features = ['type', 'release_year', 'duration', 'rating', 'listed_in', 'country']
features = [col for col in possible_features if col in df.columns]

df_selected = df[features].copy()
print("\nSelected Features for Clustering:", features)


# COMMAND ----------

# Step 4: Handle categorical columns using Label Encoding
le = LabelEncoder()
for col in df_selected.columns:
    if df_selected[col].dtype == 'object':
        df_selected[col] = le.fit_transform(df_selected[col].astype(str))

print("\nCategorical Columns Encoded")

# COMMAND ----------

# Step 5: Standardize the data (Scaling)
scaler = StandardScaler()
X_scaled = scaler.fit_transform(df_selected)

print("\nData Scaled Successfully")

# COMMAND ----------

# Step 6: Determine the optimal number of clusters using Elbow Method
inertia = []
K = range(1, 11)

for k in K:
    kmeans = KMeans(n_clusters=k, random_state=42)
    kmeans.fit(X_scaled)
    inertia.append(kmeans.inertia_)

plt.figure(figsize=(8,5))
plt.plot(K, inertia, 'bo-')
plt.xlabel('Number of Clusters (k)')
plt.ylabel('Inertia')
plt.title('Elbow Method to Find Optimal k')
plt.show()

print("\nChoose the value of k from the 'elbow' point on the graph.")

# COMMAND ----------

# Step 7: Apply K-Means Clustering
kmeans = KMeans(n_clusters=3, random_state=42)
df['Cluster'] = kmeans.fit_predict(X_scaled)

print("\nK-Means Clustering Applied Successfully!")
print(df[['title', 'type', 'release_year', 'rating', 'Cluster']].head())


# COMMAND ----------

# Step 8: Visualize the clusters
if 'release_year' in df_selected.columns and 'duration' in df_selected.columns:
    plt.figure(figsize=(8,5))
    sns.scatterplot(
        x=df_selected['release_year'],
        y=df_selected['duration'],
        hue=df['Cluster'],
        palette='tab10'
    )
    plt.title('K-Means Clustering of Netflix Shows')
    plt.xlabel('Release Year')
    plt.ylabel('Duration')
    plt.show()

# COMMAND ----------

# Step 9: Analyze Cluster Insights
cluster_summary = df.groupby('Cluster').mean(numeric_only=True)
print("\nCluster Summary:\n", cluster_summary)

print("\nNumber of items in each cluster:")
print(df['Cluster'].value_counts())

# COMMAND ----------

# Step 10: Save clustered dataset
df.to_csv("Netflix_clustered.csv", index=False)
print("\nClustered dataset saved as 'Netflix_clustered.csv'")


# COMMAND ----------

# CLASSIFICATION ON NETFLIX DATASET
# Step 1: Import libraries
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import LabelEncoder, StandardScaler
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score, classification_report, confusion_matrix
import seaborn as sns
import matplotlib.pyplot as plt

# Step 2: Select features and target
target = 'type'
features = ['release_year', 'duration', 'rating', 'listed_in', 'country']

# Keep only columns that exist in the dataset
features = [col for col in features if col in df.columns]
print("\nSelected Features for Classification:", features)

df_class = df[features + [target]].dropna().copy()

# Step 3: Encode categorical columns
le = LabelEncoder()
for col in df_class.columns:
    if df_class[col].dtype == 'object':
        df_class[col] = le.fit_transform(df_class[col].astype(str))

print("\nLabel Encoding completed for categorical columns.")

# Step 4: Split features and target
X = df_class[features]
y = df_class[target]

# Step 5: Train-test split
X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, random_state=42, stratify=y
)
print("\nTrain-Test Split Done.")
print("Training Set:", X_train.shape, " | Testing Set:", X_test.shape)

# Step 6: Feature scaling
scaler = StandardScaler()
X_train_scaled = scaler.fit_transform(X_train)
X_test_scaled = scaler.transform(X_test)

print("\nFeature Scaling Completed.")

# Step 7: Train Random Forest Classifier
rf = RandomForestClassifier(n_estimators=100, random_state=42)
rf.fit(X_train_scaled, y_train)

# Step 8: Model Evaluation
y_pred = rf.predict(X_test_scaled)

accuracy = accuracy_score(y_test, y_pred)
print("\nModel Accuracy:", round(accuracy * 100, 2), "%")

print("\nClassification Report:\n", classification_report(y_test, y_pred))

# Confusion Matrix
cm = confusion_matrix(y_test, y_pred)
plt.figure(figsize=(5,4))
sns.heatmap(cm, annot=True, fmt='d', cmap='Blues', xticklabels=['Movie','TV Show'], yticklabels=['Movie','TV Show'])
plt.title('Confusion Matrix - Random Forest Classifier')
plt.xlabel('Predicted')
plt.ylabel('Actual')
plt.show()

# Step 9: Feature Importance
feature_importance = pd.Series(rf.feature_importances_, index=features).sort_values(ascending=False)
print("\nFeature Importance:\n", feature_importance)

plt.figure(figsize=(8,4))
sns.barplot(x=feature_importance.values, y=feature_importance.index, palette='viridis')
plt.title('Feature Importance - Random Forest Classifier')
plt.xlabel('Importance Score')
plt.ylabel('Feature')
plt.show()
