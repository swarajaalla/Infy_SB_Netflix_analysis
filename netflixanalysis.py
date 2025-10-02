
import pandas as pd
df = pd.read_csv("/Volumes/workspace/default/netflix/netflix_titles.csv"
)
df.shape
df.head()
df.info()
df = df.drop_duplicates()
df = df.drop_duplicates(subset=['title','release_year'])

df.isnull().sum()


for col in ['director', 'cast', 'country', 'rating']:
    df[col] = df[col].fillna("Unknown")
    

df['date_added'] = pd.to_datetime(df['date_added'], errors='coerce')

df['date_missing'] = df['date_added'].isna().astype(int)


df_exploded = df.assign(genre=df['listed_in'].str.split(',')).explode('genre')
df_exploded['genre'] = df_exploded['genre'].str.strip()
df['type']=df['type'].str.strip()
df['rating']=df['rating'].str.strip()

before=df.shape[0]
df=df.drop_duplicates()
after=df.shape[0]
print(f"\nRemoved {before - after} duplicate rows.")


df['duration'] = df['duration'].str.replace(' min', '').str.replace(' Season[s]?', '', regex=True)
df.to_csv("/Volumes/workspace/default/netflix/cleaned_netflix_titles.csv", index=False)


cleaned_file = "/Volumes/workspace/default/netflix/cleaned_netflix.csv"
df.to_csv(cleaned_file, index=False)
display(df)
df.info()
df.describe()
from sklearn.preprocessing import OneHotEncoder

# Select categorical columns to encode
categorical_cols = ['type', 'rating', 'country', 'genre']

# One-hot encode the selected columns
encoder = OneHotEncoder(sparse_output=False, handle_unknown='ignore')
encoded = encoder.fit_transform(df_exploded[categorical_cols])

# Create a DataFrame with the encoded columns
encoded_df = pd.DataFrame(encoded, columns=encoder.get_feature_names_out(categorical_cols), index=df_exploded.index)

# Concatenate the encoded columns back to the original DataFrame
df_encoded = pd.concat([df_exploded.drop(columns=categorical_cols), encoded_df], axis=1)

display(df_encoded)
from sklearn.preprocessing import LabelEncoder

# Select categorical columns to label encode
categorical_cols = ['type', 'rating', 'country', 'genre']

# Apply label encoding to each categorical column
df_label_encoded = df_exploded.copy()
for col in categorical_cols:
    le = LabelEncoder()
    df_label_encoded[col] = le.fit_transform(df_label_encoded[col])

display(df_label_encoded)
from sklearn.preprocessing import OrdinalEncoder

# Frequency Encoding
df_freq_encoded = df_exploded.copy()
for col in ['type', 'rating', 'country', 'genre']:
    freq = df_freq_encoded[col].value_counts(normalize=True)
    df_freq_encoded[col + '_freq'] = df_freq_encoded[col].map(freq)

display(df_freq_encoded)

# Ordinal Encoding
ordinal_cols = ['type', 'rating', 'country', 'genre']
ordinal_encoder = OrdinalEncoder()
df_ordinal_encoded = df_exploded.copy()
df_ordinal_encoded[ordinal_cols] = ordinal_encoder.fit_transform(df_ordinal_encoded[ordinal_cols])

display(df_ordinal_encoded)
from sklearn.preprocessing import Normalizer

# Select numeric columns to normalize
numeric_cols = df_encoded.select_dtypes(include=['float64', 'int64']).columns

# Apply normalization
normalizer = Normalizer()
normalized = normalizer.fit_transform(df_encoded[numeric_cols])

# Create a DataFrame with normalized columns
normalized_df = pd.DataFrame(normalized, columns=numeric_cols, index=df_encoded.index)

# Concatenate normalized columns back to the original DataFrame
df_normalized = pd.concat([df_encoded.drop(columns=numeric_cols), normalized_df], axis=1)

display(df_normalized)
