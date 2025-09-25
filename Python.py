import pandas as pd
df=pd.read_csv("/Volumes/workspace/default/netflix/netflix_titles.csv")
display(df)
df.info()
df.describe()
df.isnull.sum()
df.duplicated().sum()
df=df.dropna()
df=df.drop_duplicates()
before=df.shape[0]
df=df.drop_duplicates()
after=df.shape[0]
print(f"\nRemoved {before-after} duplicate rows.”)
df['type']=df['type'].str.strip()
df['rating']=df['rating'].str.strip()
display(df.head())
df.info()




