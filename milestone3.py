import pandas as pd
df = pd.read_csv("/Volumes/workspace/default/netflix/netflix_titles.csv")
from sklearn.preprocessing import MultiLabelBinarizer, StandardScaler
from sklearn.cluster import KMeans
import numpy as np

# Prepare genre features
mlb = MultiLabelBinarizer()
genres = df['listed_in'].astype(str).str.split(', ')
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
            return int(str(row['duration']).split(' ')[0]) * 60  # Approximate: 1 season = 60 min
        except:
            return 0
    else:
        return 0

duration_feature = df.apply(extract_duration, axis=1).values.reshape(-1, 1)

# Prepare rating feature (label encoding)
rating_map = {k: v for v, k in enumerate(df['rating'].unique())}
rating_feature = df['rating'].map(rating_map).fillna(0).values.reshape(-1, 1)

# Combine features
X = np.hstack([genre_features, duration_feature, rating_feature])

# Standardize features
scaler = StandardScaler()
X_scaled = scaler.fit_transform(X)

# KMeans clustering
kmeans = KMeans(n_clusters=5, random_state=42)
df['cluster'] = kmeans.fit_predict(X_scaled)

display(df[['title', 'listed_in', 'duration', 'rating', 'cluster']])



import pandas as pd
df = pd.read_csv("/Volumes/workspace/default/netflix/netflix_titles.csv")
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import LabelEncoder, StandardScaler
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import classification_report, confusion_matrix

# Feature engineering
df['duration_num'] = pd.to_numeric(df['duration'].str.extract('(\d+)')[0], errors='coerce')
df['title_length'] = df['title'].astype(str).apply(len)
df['num_countries'] = df['country'].apply(lambda x: len(str(x).split(',')) if pd.notnull(x) else 0)
df['rating_encoded'] = LabelEncoder().fit_transform(df['rating'].astype(str))
df['genre_encoded'] = LabelEncoder().fit_transform(df['listed_in'].astype(str))

# Prepare features and target
features = ['duration_num', 'title_length', 'num_countries', 'rating_encoded', 'genre_encoded']
df = df.dropna(subset=features + ['type'])
X = df[features]
y = df['type'].map({'Movie': 0, 'TV Show': 1})

# Train/test split
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

# Standardize features
scaler = StandardScaler()
X_train_scaled = scaler.fit_transform(X_train)
X_test_scaled = scaler.transform(X_test)

# Train classifier
clf = RandomForestClassifier(random_state=42)
clf.fit(X_train_scaled, y_train)

# Predict and evaluate
y_pred = clf.predict(X_test_scaled)
print(classification_report(y_test, y_pred, target_names=['Movie', 'TV Show']))
print(confusion_matrix(y_test, y_pred))



import pandas as pd
df = pd.read_csv("/Volumes/workspace/default/netflix/netflix_titles.csv")

# Explode 'country' and 'listed_in' (genre) columns for analysis
df_exploded = df.copy()
df_exploded['country'] = df_exploded['country'].astype(str).str.split(',')
df_exploded['genre'] = df_exploded['listed_in'].astype(str).str.split(', ')
df_exploded = df_exploded.explode('country').explode('genre')
df_exploded['country'] = df_exploded['country'].str.strip()
df_exploded['genre'] = df_exploded['genre'].str.strip()

# Remove missing/unknown values
df_exploded = df_exploded[(df_exploded['country'].notna()) & (df_exploded['country'] != 'nan') & (df_exploded['country'] != 'Unknown')]
df_exploded = df_exploded[(df_exploded['genre'].notna()) & (df_exploded['genre'] != 'nan') & (df_exploded['genre'] != 'Unknown')]

# Aggregate: Count of titles per country and genre
country_genre_counts = df_exploded.groupby(['country', 'genre']).size().reset_index(name='title_count')

# Pivot for heatmap visualization
country_genre_pivot = country_genre_counts.pivot(index='country', columns='genre', values='title_count').fillna(0)

import matplotlib.pyplot as plt
import seaborn as sns

# Top 10 countries and genres for focused analysis
top_countries = df_exploded['country'].value_counts().head(10).index
top_genres = df_exploded['genre'].value_counts().head(10).index
heatmap_data = country_genre_pivot.loc[top_countries, top_genres]

plt.figure(figsize=(12,8))
sns.heatmap(heatmap_data, annot=True, fmt='.0f', cmap='YlOrRd')
plt.title('Content Availability: Top Countries vs Top Genres')
plt.xlabel('Genre')
plt.ylabel('Country')
plt.tight_layout()
plt.show()

# Analyze key drivers using feature importance (Random Forest)
from sklearn.ensemble import RandomForestRegressor

# Prepare features: For each country, aggregate features
country_features = df_exploded.groupby('country').agg({
    'show_id': 'count',
    'type': lambda x: x.value_counts().idxmax(),
    'release_year': 'median',
    'rating': lambda x: x.value_counts().idxmax(),
    'genre': pd.Series.nunique
}).rename(columns={
    'show_id': 'num_titles',
    'type': 'dominant_type',
    'release_year': 'median_release_year',
    'rating': 'dominant_rating',
    'genre': 'num_genres'
}).reset_index()

# Encode categorical features
country_features['dominant_type'] = country_features['dominant_type'].astype('category').cat.codes
country_features['dominant_rating'] = country_features['dominant_rating'].astype('category').cat.codes

# Target: Number of titles (content availability)
X = country_features[['dominant_type', 'median_release_year', 'dominant_rating', 'num_genres']]
y = country_features['num_titles']

rf = RandomForestRegressor(random_state=42)
rf.fit(X, y)

# Feature importance
feature_importance = pd.Series(rf.feature_importances_, index=X.columns).sort_values(ascending=False)
display(feature_importance)

plt.figure(figsize=(7,4))
feature_importance.plot(kind='bar')
plt.title('Key Drivers for Content Availability Across Countries')
plt.ylabel('Feature Importance')
plt.tight_layout()
plt.show()



import pandas as pd
df = pd.read_csv("/Volumes/workspace/default/netflix/netflix_titles.csv")
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import LabelEncoder, StandardScaler
from sklearn.ensemble import RandomForestClassifier
import matplotlib.pyplot as plt
import seaborn as sns

# Feature engineering
df['duration_num'] = pd.to_numeric(df['duration'].str.extract('(\d+)')[0], errors='coerce')
df['title_length'] = df['title'].astype(str).apply(len)
df['num_countries'] = df['country'].apply(lambda x: len(str(x).split(',')) if pd.notnull(x) else 0)
df['rating_encoded'] = LabelEncoder().fit_transform(df['rating'].astype(str))
df['genre_encoded'] = LabelEncoder().fit_transform(df['listed_in'].astype(str))

# Prepare features and target
features = ['duration_num', 'title_length', 'num_countries', 'rating_encoded', 'genre_encoded']
df = df.dropna(subset=features + ['type'])
X = df[features]
y = df['type'].map({'Movie': 0, 'TV Show': 1})

# Train/test split
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

# Standardize features
scaler = StandardScaler()
X_train_scaled = scaler.fit_transform(X_train)
X_test_scaled = scaler.transform(X_test)

# Train classifier
clf = RandomForestClassifier(random_state=42)
clf.fit(X_train_scaled, y_train)

# Feature importance
importances = clf.feature_importances_
feature_names = features
importance_df = pd.DataFrame({'feature': feature_names, 'importance': importances}).sort_values(by='importance', ascending=False)
display(importance_df)

plt.figure(figsize=(8,4))
sns.barplot(x='importance', y='feature', data=importance_df)
plt.title('Feature Importance (Random Forest)')
plt.xlabel('Importance')
plt.ylabel('Feature')
plt.tight_layout()
plt.show()