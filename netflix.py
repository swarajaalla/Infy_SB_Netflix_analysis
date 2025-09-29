import pandas as pd
file_path = "/Volumes/workspace/default/netflix/netflix_titles.csv"
df = pd.read_csv(file_path)
display(df)

df.info()
df.isnull().sum()
df.duplicated().sum()
df.describe(include='all')

# Fill missing values
df['director'].fillna("Unknown", inplace=True)
df['cast'].fillna("Not Available", inplace=True)
df['country'].fillna("Unknown", inplace=True)
df['date_added'].fillna("Not Available", inplace=True)
df['rating'].fillna("Not Rated", inplace=True)
df['duration'].fillna("Unknown", inplace=True)

print("\nMissing values after handling:")
print(df)

df=df.drop_duplicates()
display(df)
