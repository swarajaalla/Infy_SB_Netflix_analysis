import pandas as pd
file_path = "/Volumes/workspace/default/netflix/netflix_titles.csv"
df = pd.read_csv(file_path)
display(df)

df.info()
df.isnull().sum()
df.duplicated().sum()
df.describe(include='all')
display(df.head())

Handle missing values
df['director'] = df['director'].fillna("Unknown")
df['cast'] = df['cast'].fillna("Not Available")
df['country'] = df['country'].fillna("Unknown")
df['date_added'] = pd.to_datetime(df['date_added'], errors='coerce')
df['rating'] = df['rating'].fillna("Unknown")
df['duration'] = df['duration'].fillna("Unknown")
display(df.head())

Standardize text
df['title'] = df['title'].str.strip()
df['type'] = df['type'].str.title()
df['country'] = df['country'].str.strip()
print(df.shape)
display(df.head())

Drop rows with missing description
df = df[df['description'].notna()]
display(df)

Reset index
df = df.reset_index(drop=True)
display(df)

cleaned_file_path = "/Volumes/workspace/default/netflix/netflix_cleaned.csv"
df.to_csv(cleaned_file_path, index=False)
display(df.head())
