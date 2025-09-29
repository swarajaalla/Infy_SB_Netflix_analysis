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
import pandas as pd
from sklearn.preprocessing import OneHotEncoder
from sklearn.impute import SimpleImputer
df = pd.read_csv(        "/Volumes/workspace/default/netflix/netflix_titles.csv"
)

df.drop_duplicates(inplace=True)
num_cols = df.select_dtypes(include=["int64", "float64"]).columns
cat_cols = df.select_dtypes(include=["object", "category"]).columns
num_imputer = SimpleImputer(strategy="mean")
df[num_cols] = num_imputer.fit_transform(df[num_cols])
cat_imputer = SimpleImputer(strategy="most_frequent")
df[cat_cols] = cat_imputer.fit_transform(df[cat_cols])

# === 2. Normalize Categorical Data ===
encoder = OneHotEncoder(sparse_output=False,handle_unknown='ignore')
encoded_cat = encoder.fit_transform(df[cat_cols])
encoded_cat_df = pd.DataFrame(encoded_cat, columns=encoder.get_feature_names_out(cat_cols))

df = df.drop(columns=cat_cols)
df = pd.concat([df.reset_index(drop=True), encoded_cat_df.reset_index(drop=True)], axis=1)


print(df.head())

        





