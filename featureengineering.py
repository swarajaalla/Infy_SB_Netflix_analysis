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
# Feature engineering: Licensed or Original content
def licensed_or_original(row):
    title_lower = str(row['title']).lower()
    if 'original' in title_lower or 'netflix original' in title_lower:
        return 'Original'
    else:
        return 'Licensed'

df['content_origin'] = df.apply(licensed_or_original, axis=1)
display(df[['title', 'content_origin']].head())
df = pd.read_csv("/Volumes/workspace/default/netflix/netflix_titles.csv")
df['title_length'] = df['title'].astype(str).apply(len)
display(df[['title', 'title_length']].head())
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
display(df[['title', 'rating', 'age_category']].head())# Feature engineering on 'rating' column

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