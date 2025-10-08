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

df.head()

# COMMAND ----------

df.tail()

# COMMAND ----------

df.describe()

# COMMAND ----------

df.info()

# COMMAND ----------

df.isnull().sum()

# COMMAND ----------

# --- Step 3: Clean Missing Values and Remove Duplicates ---
df_cleaned = df.copy()
df_cleaned.fillna({
    'director': 'Unknown',
    'cast': 'Unknown',
    'country': 'Unknown',
    'rating': 'Unknown',
    'duration': 'Unknown',
    'date_added': 'Unknown'
}, inplace=True)

# Remove duplicates (optional: only on key attributes)
df_cleaned.drop_duplicates(subset=['show_id'], inplace=True)

print("\n✅ Missing values handled and duplicates removed.")


# COMMAND ----------

# --- Step 4: Convert Date Columns ---
df_cleaned['date_added'] = pd.to_datetime(df_cleaned['date_added'], errors='coerce')
df_cleaned['added_year'] = df_cleaned['date_added'].dt.year
df_cleaned['added_month'] = df_cleaned['date_added'].dt.month

# COMMAND ----------

# --- Step 5: Process Duration ---
df_cleaned['duration_num'] = df_cleaned['duration'].str.extract(r'(\d+)', expand=False).astype(float)
df_cleaned['duration_type'] = df_cleaned['duration'].str.extract(r'(min|season|seasons)', expand=False)
df_cleaned['duration_type'] = df_cleaned['duration_type'].replace({'seasons': 'season'})

# COMMAND ----------

# --- Step 6: Simplify Ratings ---
df_cleaned['rating_simplified'] = df_cleaned['rating'].replace({
    'TV-Y7-FV': 'TV-Y7', 'TV-G': 'TV-Y7', 'G': 'TV-Y7', 'TV-Y': 'TV-Y7',
    'PG': 'TV-14', 'PG-13': 'TV-14',
    'R': 'TV-MA', 'NC-17': 'TV-MA',
    'UR': 'NR'
})


# COMMAND ----------

# Create ordinal encoding
rating_order = {'Unknown': 0, 'NR': 0, 'TV-Y7': 1, 'TV-14': 2, 'TV-MA': 3}
df_cleaned['rating_ordinal'] = df_cleaned['rating_simplified'].map(rating_order)

# COMMAND ----------

# --- Step 7: Type Encoding (Movie vs TV Show) ---
df_cleaned['is_tv_show'] = df_cleaned['type'].map({'Movie': 0, 'TV Show': 1})

# COMMAND ----------

# --- Step 8: Encode Text Columns ---
text_columns = ['director', 'cast', 'country', 'listed_in', 'description', 'duration_type']
for col in text_columns:
    df_cleaned[col] = df_cleaned[col].astype(str)
    df_cleaned[col + '_code'] = df_cleaned[col].astype('category').cat.codes

# COMMAND ----------

# --- Step 9: Add Readable Date Format ---
df_cleaned['date_added_formatted'] = df_cleaned['date_added'].dt.strftime('%d-%m-%Y')

# COMMAND ----------

# --- Step 10: Save Cleaned File ---
cleaned_file_name = "netflix_cleaned_data1.csv"
df_cleaned.to_csv(cleaned_file_name, index=False)
print(f"\n✅ Cleaned data saved to '{cleaned_file_name}'")
print(f"Final dataset shape: {df_cleaned.shape}")

# COMMAND ----------

# --- Optional: Final Check ---
print("\n--- Missing Values After Cleaning ---")
print(df_cleaned.isnull().sum())

# COMMAND ----------

# --- Final Missing Value Fix ---
df_cleaned['date_added'].fillna(pd.NaT, inplace=True)
df_cleaned['added_year'].fillna(0, inplace=True)
df_cleaned['added_month'].fillna(0, inplace=True)
df_cleaned['duration_num'].fillna(df_cleaned['duration_num'].median(), inplace=True)
df_cleaned['rating_ordinal'].fillna(0, inplace=True)

# Verify again
print("\n✅ After Final Fix - Missing Values Check:")
print(df_cleaned.isnull().sum())

# Save final cleaned dataset again
df_cleaned.to_csv('netflix_cleaned_data_final2.csv', index=False)
print("\n💾 Saved final cleaned dataset as 'netflix_cleaned_data_final2.csv'")
