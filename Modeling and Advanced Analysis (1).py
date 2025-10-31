# Databricks notebook source
 

# COMMAND ----------

    import pandas as pd
    data=pd.read_csv("/Volumes/workspace/default/netflix_dataset/eda_feature_engineering.csv")
    df_read=pd.DataFrame(data)
    df_read.head(10)
    display(df_read)

# COMMAND ----------

# Step 1: Import libraries
import pandas as pd
import numpy as np
from sklearn.preprocessing import MultiLabelBinarizer, StandardScaler, LabelEncoder
from sklearn.cluster import KMeans
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import confusion_matrix, precision_score, recall_score, f1_score, accuracy_score
import matplotlib.pyplot as plt
import seaborn as sns

# Copy your dataframe
df = df_read.copy()


# COMMAND ----------

# Convert genres to numbers
mlb = MultiLabelBinarizer()
genre_features = mlb.fit_transform(df['listed_in'])

# Extract duration number
def get_minutes(x):
    if 'Season' in x:
        return 60  # assume 1 season = 60 min
    else:
        try:
            return int(x.split(' ')[0])
        except:
            return 0

df['duration_mins'] = df['duration'].apply(get_minutes)

# Encode ratings as numbers
rating_map = {k: v for v, k in enumerate(df['rating'].unique())}
df['rating_num'] = df['rating'].map(rating_map)

# Combine features
X = np.hstack([genre_features, 
               df['duration_mins'].values.reshape(-1,1),
               df['rating_num'].values.reshape(-1,1)])

# Scale data
scaler = StandardScaler()
X_scaled = scaler.fit_transform(X)

# K-Means Clustering
kmeans = KMeans(n_clusters=5, random_state=42)
df['cluster'] = kmeans.fit_predict(X_scaled)

# Show sample output
print(df[['title', 'listed_in', 'duration', 'rating', 'cluster']].head(10))


# COMMAND ----------

# Check how many shows in each cluster
print("\nCluster counts:")
print(df['cluster'].value_counts())

# Average duration per cluster
print("\nAverage duration per cluster:")
print(df.groupby('cluster')['duration_mins'].mean())


# COMMAND ----------

# DBTITLE 1,Classification(Movie& Tv shows)
# Prepare features
X = df[['duration_mins', 'rating_num']].copy()

# Add simple genre features (sum over each genre column)
X = pd.concat([X, pd.DataFrame(genre_features, columns=mlb.classes_)], axis=1)

# Encode target (Movie=1, TV Show=0)
df['type'] = df['type'].fillna('Movie')
df['type_num'] = df['type'].map({'Movie':1, 'TV Show':0})

y = df['type_num']

# Train / test split
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3, random_state=42)


# COMMAND ----------

# DBTITLE 1,Random fores algorithm
# Train RandomForest
model = RandomForestClassifier(random_state=42)
model.fit(X_train, y_train)

# Predict
y_pred = model.predict(X_test)

# Confusion matrix
cm = confusion_matrix(y_test, y_pred)
print("\nConfusion Matrix:\n", cm)

# Extract tp, tn, fp, fn
tn, fp, fn, tp = cm.ravel()
print(f"\nTP={tp}, TN={tn}, FP={fp}, FN={fn}")

# Metrics
precision = precision_score(y_test, y_pred)
recall = recall_score(y_test, y_pred)
f1 = f1_score(y_test, y_pred)
accuracy = accuracy_score(y_test, y_pred)

print(f"\nAccuracy = {accuracy:.2f}")
print(f"Precision = {precision:.2f}")
print(f"Recall = {recall:.2f}")
print(f"F1 Score = {f1:.2f}")


# COMMAND ----------

# DBTITLE 1,Confusion matrix Plot
sns.heatmap(cm, annot=True, fmt='d', cmap='Blues')
plt.xlabel('Predicted')
plt.ylabel('Actual')
plt.title('Confusion Matrix')
plt.show()


# COMMAND ----------

# DBTITLE 1,Feature Importance
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
# Get feature importances from Random Forest
feature_importance = model.feature_importances_

# Create a DataFrame for better visualization
features = X.columns
importance_df = pd.DataFrame({'Feature': features, 'Importance': feature_importance})
importance_df = importance_df.sort_values('Importance', ascending=False)

# Show top 10 important features
print("\nTop 10 Important Features:\n", importance_df.head(10))

# Plot feature importance
plt.figure(figsize=(10, 6))
sns.barplot(x='Importance', y='Feature', data=importance_df.head(10), palette='viridis')
plt.title('Top 10 Important Features (RF)', fontsize=14)
plt.xlabel('Importance Score')
plt.ylabel('Feature Name')
plt.show()

# COMMAND ----------

# --- Country and Genre Analysis ---
# Top 10 countries by count in each cluster
country_counts = df.groupby('cluster')['country'].value_counts().groupby(level=0).nlargest(3)
print("\nTop 3 countries per cluster:\n", country_counts)

# Plot country distribution
plt.figure(figsize=(10,6))
sns.countplot(data=df, x='cluster', hue='country')
plt.title("Country Distribution Across Clusters", fontsize=14)
plt.xlabel("Cluster Number")
plt.ylabel("Count of Titles")
plt.legend(bbox_to_anchor=(1.05, 1), loc='upper left')
plt.show()

# Genre Distribution
plt.figure(figsize=(12,6))
genre_cluster = df.groupby('cluster')['listed_in'].apply(lambda x: ','.join(x)).reset_index()

# Convert each genre into separate counts
all_genres = []
for cluster, row in genre_cluster.iterrows():
    genres = row['listed_in'].split(',')
    for g in genres:
        all_genres.append((row['cluster'], g.strip()))

genre_df = pd.DataFrame(all_genres, columns=['cluster', 'genre'])
plt.figure(figsize=(12,6))
sns.countplot(data=genre_df, x='cluster', hue='genre')
plt.title("Genre Distribution Across Clusters", fontsize=14)
plt.xlabel("Cluster Number")
plt.ylabel("Genre Count")
plt.legend(bbox_to_anchor=(1.05, 1), loc='upper left')
plt.show()


# COMMAND ----------

from sklearn.decomposition import PCA

# Reduce to 2 dimensions for visualization
pca = PCA(n_components=2)
pca_result = pca.fit_transform(X_scaled)

df['pca1'] = pca_result[:,0]
df['pca2'] = pca_result[:,1]

# Plot clusters
plt.figure(figsize=(10,6))
sns.scatterplot(data=df, x='pca1', y='pca2', hue='cluster', palette='tab10')
plt.title("K-Means Clusters Visualization (PCA Reduced)", fontsize=14)
plt.xlabel("PCA 1")
plt.ylabel("PCA 2")
plt.legend(title="Cluster")
plt.show()


# COMMAND ----------

print(f"Accuracy = {accuracy:.2f}")
print(f"Precision = {precision:.2f}")
print(f"Recall = {recall:.2f}")
print(f"F1 Score = {f1:.2f}")


# COMMAND ----------

print("Train Accuracy:", model.score(X_train, y_train))
print("Test Accuracy:", model.score(X_test, y_test))


# COMMAND ----------

# Convert duration into numerical minutes safely
def get_minutes(x):
    if pd.isna(x):
        return 0
    x = str(x).strip()
    if "Season" in x:         # TV Show
        try:
            n = int(x.split()[0])
            return n * 60      # assume 1 season ≈ 60 minutes
        except:
            return 60
    elif "min" in x:           # Movie
        try:
            return int(x.split()[0])
        except:
            return 0
    else:
        return 0

# Apply to duration column
df['duration_mins'] = df['duration'].apply(get_minutes)

# Drop the original text column
df = df.drop(columns=['duration'])


# COMMAND ----------

# DBTITLE 1,Encode genres and ratings
from sklearn.preprocessing import MultiLabelBinarizer

# Encode genres
mlb = MultiLabelBinarizer()
genre_features = mlb.fit_transform(df['listed_in'])

# Encode rating as number
rating_map = {k: v for v, k in enumerate(df['rating'].unique())}
df['rating_num'] = df['rating'].map(rating_map)


# COMMAND ----------

# DBTITLE 1,Prepare features and target
import numpy as np

# Combine all features
X = np.hstack([
    genre_features,
    df['duration_mins'].values.reshape(-1, 1),
    df['rating_num'].values.reshape(-1, 1)
])

# Target: 1 = Movie, 0 = TV Show
df['type'] = df['type'].fillna('Movie')
df['type_num'] = df['type'].map({'Movie': 1, 'TV Show': 0})
y = df['type_num']


# COMMAND ----------

# DBTITLE 1,Split and scale data
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler

# Split data
X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.3, random_state=42
)

# Scale
scaler = StandardScaler()
X_train = scaler.fit_transform(X_train)
X_test = scaler.transform(X_test)


# COMMAND ----------

# DBTITLE 1,Train and evaluate Random Forest
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import confusion_matrix, accuracy_score, precision_score, recall_score, f1_score

# Train model
model = RandomForestClassifier(random_state=42)
model.fit(X_train, y_train)

# Predict
y_pred = model.predict(X_test)

# Metrics
cm = confusion_matrix(y_test, y_pred)
accuracy = accuracy_score(y_test, y_pred)
precision = precision_score(y_test, y_pred)
recall = recall_score(y_test, y_pred)
f1 = f1_score(y_test, y_pred)

print("Confusion Matrix:\n", cm)
print(f"Accuracy = {accuracy:.2f}")
print(f"Precision = {precision:.2f}")
print(f"Recall = {recall:.2f}")
print(f"F1 Score = {f1:.2f}")


# COMMAND ----------

# DBTITLE 1,Plot feature importance
import matplotlib.pyplot as plt
import numpy as np

# Get feature importances
importances = model.feature_importances_

# Get top 10 features
indices = np.argsort(importances)[-10:]
plt.figure(figsize=(10,5))
plt.barh(range(len(indices)), importances[indices], align='center')
plt.yticks(range(len(indices)), np.array(list(mlb.classes_) + ['duration_mins', 'rating_num'])[indices])
plt.xlabel('Importance Score')
plt.ylabel('Feature Name')
plt.title('Top 10 Important Features (Random Forest)')
plt.show()

# COMMAND ----------

print(f"Accuracy = {accuracy:.2f}")
print(f"Precision = {precision:.2f}")
print(f"Recall = {recall:.2f}")
print(f"F1 Score = {f1:.2f}")