# Databricks notebook source
import pandas as pd
df=pd.read_csv("/Volumes/workspace/default/infosys_data/netflix_titles.csv")
print(df)
df.display()

# COMMAND ----------

df.shape



# COMMAND ----------

df.columns

# COMMAND ----------

df.head(5)

# COMMAND ----------

df.tail(5)

# COMMAND ----------

df.describe()

# COMMAND ----------

df.info()

# COMMAND ----------

df.isnull().sum()

# COMMAND ----------

df_cleaned = df.copy() 
df_cleaned = df_cleaned.fillna({
    'director': 'Unknown', 
    'cast': 'Unknown', 
    'rating': 'Unknown', 
    'duration': 'Unknown', 
    'date_added': 'Unknown', 
    'country': 'Unknown'
})
df_cleaned = df_cleaned.drop_duplicates(subset=['director', 'cast'])
df_cleaned.isnull().sum()

# COMMAND ----------

# --- Step 1: Simplify the Rating Categories ---
# Consolidate many ratings into fewer, manageable groups.
df_cleaned['rating_simplified'] = df_cleaned['rating'].replace({
    # Combine younger audiences
    'TV-Y7-FV': 'TV-Y7', 'TV-G': 'TV-Y7', 'G': 'TV-Y7', 'TV-Y': 'TV-Y7',
    # Combine teen audiences
    'PG': 'TV-14', 'PG-13': 'TV-14',
    # Combine mature audiences
    'R': 'TV-MA', 'NC-17': 'TV-MA',
    # Group Not Rated/Unrated
    'UR': 'NR' 
})
# --- Step 2: Apply Ordinal Encoding ---
rating_order = {
    'Unknown': 0,   # Not Rated/Unknown (lowest/default)
    'NR': 0,        
    'TV-Y7': 1,     # Young Children (least restrictive)
    'TV-14': 2,     # Teens/Older Teens
    'TV-MA': 3      # Mature Adults (most restrictive)
}

df_cleaned['rating_ordinal'] = df_cleaned['rating_simplified'].map(rating_order)
# Display the resulting mapping
print("Rating Ordinal Scale Check:")
print(df_cleaned[['rating_simplified', 'rating_ordinal']].drop_duplicates().sort_values(by='rating_ordinal'))

# COMMAND ----------

# Define the chronological order for release eras (oldest to newest)
era_order = {
    'Pre-90s': 1,
    '1990s': 2,
    '2000s': 3,
    '2010-2014': 4,
    '2015-2019': 5,
    '2020+': 6
}

# Apply the mapping (must convert the 'Categorical' type to string first)
df_cleaned['release_era_ordinal'] = df_cleaned['release_era'].astype(str).map(era_order)

print("\nRelease Era Ordinal Check:")
print(df_cleaned[['release_era', 'release_era_ordinal']].drop_duplicates().sort_values(by='release_era_ordinal'))

# COMMAND ----------

# Convert director and cast strings to numerical codes
df_cleaned['director_code'] = df_cleaned['director'].astype('category').cat.codes
df_cleaned['cast_code'] = df_cleaned['cast'].astype('category').cat.codes

print("\nDirector/Cast Encoding Status:")
print(f"Unique Directors: {df_cleaned['director_code'].nunique()}")

# COMMAND ----------

# Map 'Movie' to 0 and 'TV Show' to 1
type_map = {'Movie': 0, 'TV Show': 1}
df_cleaned['is_tv_show'] = df_cleaned['type'].map(type_map)

print("\nType Binary Check:")
print(df_cleaned['is_tv_show'].value_counts())

# COMMAND ----------


print(df_cleaned.isnull().sum())

print("\n--- First 5 Rows of Cleaned Data ---")
df_cleaned.head()

# COMMAND ----------

# Run this to see all columns you've created
df_cleaned.head().T

# COMMAND ----------

# Assuming 'df' is your original loaded dataset. 

# --- 1. Master Cleaning: Fill Nulls and Drop Duplicates ---
df_cleaned = df.fillna({
    'director': 'Unknown',
    'cast': 'Unknown',
    'country': 'Unknown',
    'rating': 'Unknown',
    'duration': 'Unknown',
    'date_added': 'Unknown'
}).drop_duplicates().copy()


# --- 2. Master Feature Engineering ---

# Date Components
df_cleaned['date_added'] = pd.to_datetime(df_cleaned['date_added'], errors='coerce')
df_cleaned['added_year'] = df_cleaned['date_added'].dt.year
df_cleaned['added_month'] = df_cleaned['date_added'].dt.month

# Duration Split (CRITICAL STEP THAT CREATES DURATION_TYPE)
df_cleaned['duration_num'] = df_cleaned['duration'].str.extract(r'(\d+)', expand=False).astype(float)
df_cleaned['duration_type'] = df_cleaned['duration'].str.extract(r'(min|season|seasons)', expand=False).replace({'seasons': 'season'})
df_cleaned = df_cleaned.drop(columns=['duration'])

# Rating Simplification
df_cleaned['rating_simplified'] = df_cleaned['rating'].replace({
    'TV-Y7-FV': 'TV-Y7', 'TV-G': 'TV-Y7', 'G': 'TV-Y7', 'TV-Y': 'TV-Y7',
    'PG': 'TV-14', 'PG-13': 'TV-14', 'R': 'TV-MA', 'NC-17': 'TV-MA', 'UR': 'NR'
})
df_cleaned = df_cleaned.drop(columns=['rating'])


# --- 3. Master Encoding ---

# Ordinal Encoding (Ratings & Era)
df_cleaned['rating_ordinal'] = df_cleaned['rating_simplified'].map({'Unknown': 0, 'NR': 0, 'TV-Y7': 1, 'TV-14': 2, 'TV-MA': 3})
df_cleaned['is_tv_show'] = df_cleaned['type'].map({'Movie': 0, 'TV Show': 1})

# Nominal Encoding (All other text columns)
nominal_cols_to_encode = ['director', 'cast', 'country', 'listed_in', 'description', 'duration_type']

for col in nominal_cols_to_encode:
    # Use .loc to avoid the SettingWithCopyWarning
    df_cleaned.loc[:, col] = df_cleaned[col].astype(str)
    df_cleaned.loc[:, col + '_code'] = df_cleaned[col].astype('category').cat.codes

print("--- Data Preparation Complete. Final Cleaned Data Head ---")
print(df_cleaned.head())

# COMMAND ----------

# The final check for missing values
print(df_cleaned.isnull().sum())

# COMMAND ----------

# See a summary of the data types for every column
df_cleaned.info()

# COMMAND ----------

# The final check for duplicates
print(f"Total number of rows in your final clean data: {len(df_cleaned)}")
print(f"Number of unique rows (using all columns): {len(df_cleaned.drop_duplicates())}")

# COMMAND ----------

# Create a new column with the date formatted as DD-MM-YYYY string
df_cleaned['date_added_formatted'] = df_cleaned['date_added'].dt.strftime('%d-%m-%Y')

# Display the original and the new formatted column to check the result
print(df_cleaned[['date_added', 'date_added_formatted']].head())

# COMMAND ----------

cleaned_file_name = 'netflix_cleaned_data.csv'
df_cleaned.to_csv(cleaned_file_name, index=False)
print(f"Data successfully saved to {cleaned_file_name}")