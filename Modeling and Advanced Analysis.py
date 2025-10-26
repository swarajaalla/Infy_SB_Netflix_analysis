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
