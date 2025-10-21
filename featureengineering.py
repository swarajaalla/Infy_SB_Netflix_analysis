import pandas as pd
df = pd.read_csv("/Volumes/workspace/default/netflix/netflix_titles.csv")

# Feature engineering: Licensed or Original content
def licensed_or_original(row):
    title_lower = str(row['title']).lower()
    if 'original' in title_lower or 'netflix original' in title_lower:
        return 'Original'
    else:
        return 'Licensed'

df['content_origin'] = df.apply(licensed_or_original, axis=1)
display(df[['title', 'content_origin']].head())

import matplotlib.pyplot as plt
import seaborn as sns

plt.figure(figsize=(6,4))
sns.countplot(x='content_origin', data=df)
plt.title('Distribution of Content Origin')
plt.xlabel('Content Origin')
plt.ylabel('Count')
plt.tight_layout()
plt.show()


# Feature engineering: Title length column
import pandas as pd
df = pd.read_csv("/Volumes/workspace/default/netflix/netflix_titles.csv")
df['title_length'] = df['title'].astype(str).apply(len)
display(df[['title', 'title_length']].head())

import matplotlib.pyplot as plt
import seaborn as sns

plt.figure(figsize=(10,5))
sns.histplot(df['title_length'], bins=30, kde=True)
plt.title('Distribution of Title Lengths')
plt.xlabel('Title Length (Number of Characters)')
plt.ylabel('Count')
plt.tight_layout()
plt.show()

# Feature engineering on 'country' column

# 1. Number of countries per title
df['num_countries'] = df['country'].apply(lambda x: len(str(x).split(',')) if pd.notnull(x) else 0)

# 2. Is USA included
df['is_usa'] = df['country'].apply(lambda x: int('United States' in str(x)))

# 3. First country listed
df['first_country'] = df['country'].apply(lambda x: str(x).split(',')[0].strip() if pd.notnull(x) else 'Unknown')

# 4. Top 10 countries one-hot encoding
top_countries = df['country'].str.split(',').explode().str.strip().value_counts().head(10).index
for country in top_countries:
    df[f'country_{country.replace(" ", "_")}'] = df['country'].apply(lambda x: int(country in str(x)))

display(df[['country', 'num_countries', 'is_usa', 'first_country'] + [f'country_{c.replace(" ", "_")}' for c in top_countries]].head())

import matplotlib.pyplot as plt
import seaborn as sns

# Distribution of number of countries per title
plt.figure(figsize=(8,4))
sns.histplot(df['num_countries'], bins=range(1, df['num_countries'].max()+2), kde=False)
plt.title('Distribution of Number of Countries per Title')
plt.xlabel('Number of Countries')
plt.ylabel('Count')
plt.tight_layout()
plt.show()

# Proportion of titles including USA
plt.figure(figsize=(5,4))
sns.countplot(x='is_usa', data=df)
plt.title('Titles Including USA')
plt.xlabel('Is USA Included (1=Yes, 0=No)')
plt.ylabel('Count')
plt.tight_layout()
plt.show()

# Top 10 first countries listed
top_first_countries = df['first_country'].value_counts().head(10)
plt.figure(figsize=(10,5))
top_first_countries.plot(kind='bar')
plt.title('Top 10 First Countries Listed')
plt.xlabel('First Country')
plt.ylabel('Count')
plt.tight_layout()
plt.show()

# Top 10 countries one-hot encoded counts
one_hot_cols = [f'country_{c.replace(" ", "_")}' for c in top_countries]
df[one_hot_cols].sum().plot(kind='bar', figsize=(10,5))
plt.title('Top 10 Countries (One-Hot Encoded Counts)')
plt.xlabel('Country')
plt.ylabel('Count')
plt.tight_layout()
plt.show()


# Feature engineering: Create age category based on 'rating'
import pandas as pd
df = pd.read_csv("/Volumes/workspace/default/netflix/netflix_titles.csv")

def age_category(rating):
    if rating in ['G', 'TV-G', 'TV-Y', 'TV-Y7', 'TV-Y7-FV']:
        return 'Kids'
    elif rating in ['PG', 'TV-PG']:
        return 'Family'
    elif rating in ['PG-13', 'TV-14']:
        return 'Teens'
    elif rating in ['R', 'NC-17', 'TV-MA']:
        return 'Adults'
    else:
        return 'Unknown'

df['age_category'] = df['rating'].apply(age_category)
display(df[['title', 'rating', 'age_category']].head())

import matplotlib.pyplot as plt
import seaborn as sns

plt.figure(figsize=(7,4))
sns.countplot(x='age_category', data=df, order=['Kids','Family','Teens','Adults','Unknown'])
plt.title('Distribution of Age Category')
plt.xlabel('Age Category')
plt.ylabel('Count')
plt.tight_layout()
plt.show()

# Feature engineering on 'rating' column

# 1. Create age category based on rating
def age_category(rating):
    if rating in ['G', 'TV-G', 'TV-Y', 'TV-Y7', 'TV-Y7-FV']:
        return 'Kids'
    elif rating in ['PG', 'TV-PG']:
        return 'Family'
    elif rating in ['PG-13', 'TV-14']:
        return 'Teens'
    elif rating in ['R', 'NC-17', 'TV-MA']:
        return 'Adults'
    else:
        return 'Unknown'

df['age_category'] = df['rating'].apply(age_category)

# 2. Is the rating for mature audiences
mature_ratings = ['R', 'NC-17', 'TV-MA']
df['is_mature'] = df['rating'].apply(lambda x: int(x in mature_ratings))

# 3. One-hot encode top 5 most common ratings
top_ratings = df['rating'].value_counts().head(5).index
for rating in top_ratings:
    df[f'rating_{rating.replace("-", "_")}'] = df['rating'].apply(lambda x: int(x == rating))

display(df[['rating', 'age_category', 'is_mature'] + [f'rating_{r.replace("-", "_")}' for r in top_ratings]].head())

import matplotlib.pyplot as plt
import seaborn as sns

# Visualize age category distribution
plt.figure(figsize=(7,4))
sns.countplot(x='age_category', data=df, order=['Kids','Family','Teens','Adults','Unknown'])
plt.title('Distribution of Age Category')
plt.xlabel('Age Category')
plt.ylabel('Count')
plt.tight_layout()
plt.show()

# Visualize is_mature distribution
plt.figure(figsize=(5,4))
sns.countplot(x='is_mature', data=df)
plt.title('Mature Audience Content')
plt.xlabel('Is Mature (1=Yes, 0=No)')
plt.ylabel('Count')
plt.tight_layout()
plt.show()

# Visualize one-hot encoded top 5 ratings
one_hot_cols = [f'rating_{r.replace("-", "_")}' for r in top_ratings]
df[one_hot_cols].sum().plot(kind='bar', figsize=(8,4))
plt.title('Top 5 Ratings (One-Hot Encoded Counts)')
plt.xlabel('Rating')
plt.ylabel('Count')
plt.tight_layout()
plt.show()

# Feature engineering: Create content length category
import pandas as pd
df = pd.read_csv("/Volumes/workspace/default/netflix/netflix_titles.csv")
df['duration_num'] = pd.to_numeric(df['duration'], errors='coerce')

def length_category(row):
    if row['type'] == 'Movie':
        if pd.isna(row['duration_num']):
            return 'Unknown'
        elif row['duration_num'] < 60:
            return 'Short'
        elif row['duration_num'] < 120:
            return 'Feature'
        else:
            return 'Long'
    elif row['type'] == 'TV Show':
        if pd.isna(row['duration_num']):
            return 'Unknown'
        elif row['duration_num'] == 1:
            return 'Mini-Series'
        elif row['duration_num'] < 4:
            return 'Short Series'
        elif row['duration_num'] < 10:
            return 'Standard Series'
        else:
            return 'Long Series'
    else:
        return 'Unknown'

df['length_category'] = df.apply(length_category, axis=1)
display(df[['type', 'duration', 'length_category']].head())

import matplotlib.pyplot as plt
import seaborn as sns

plt.figure(figsize=(10,5))
sns.countplot(x='length_category', data=df, order=df['length_category'].value_counts().index)
plt.title('Distribution of Content Length Categories')
plt.xlabel('Length Category')
plt.ylabel('Count')
plt.tight_layout()
plt.show()

plt.figure(figsize=(10,5))
sns.countplot(x='type', hue='length_category', data=df, order=df['type'].value_counts().index)
plt.title('Content Length Category by Type')
plt.xlabel('Type')
plt.ylabel('Count')
plt.legend(title='Length Category', bbox_to_anchor=(1.05, 1), loc='upper left')
plt.tight_layout()
plt.show()