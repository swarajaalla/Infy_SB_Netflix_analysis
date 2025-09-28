# Infy_SB_Netflix_analysis
Netflix Content Strategy Analyzer – Progress Update
***What I Have Learned Till Now***
1. Dataset Handling
*Learned how to download datasets from Kaggle.
*Worked with the Netflix Movies and TV Shows dataset (8,000+ titles).
*Loaded the dataset into Databricks for analysis using Pandas.

2. Databricks Basics

*Created an account in Databricks.
*Learned how to create and connect to a cluster.
*Understood how to run Python code in the Databricks workspace notebook environment.

3. Data Cleaning with Pandas

Checked dataset structure using:
df.shape
df.info()
df.head(3)

Removed duplicates to ensure unique records:

df.drop_duplicates(inplace=True)


Handled missing values:

Filled missing director, cast, and country with "Unknown".
Filled missing rating with "Not Rated".
Checked null values after cleaning:

df.isnull().sum()


Saved cleaned dataset back for further use:
df.to_csv("netflix_cleaned.csv", index=False)

4. Key Concepts Learned

How to explore dataset structure (rows, columns, types, nulls).
How to remove duplicates from a dataset.
How to fill missing values in categorical columns.
How to convert data into a cleaned version that is ready for analysis.
How to run and display results inside Databricks notebooks.

Next Steps in the intern:

*Perform Exploratory Data Analysis (EDA) on Netflix dataset.
*Generate visualizations of trends (content growth, genres, ratings, countries).
*Build an interactive dashboard using Streamlit/Tableau.
