# Infy_SB_Netflix_analysis

## Project Overview

This project analyzes the Netflix dataset to uncover insights about content trends, genres, ratings, country-level contributions, and more.
The goal is to clean, explore, and visualize the data to build meaningful insights for further analysis and modeling.

## Dataset

Source: Netflix content dataset (netflix_analysis.csv)

Size: ~8,000+ records

Key Columns: title, type, director, cast, country, date_added, release_year, rating, duration, listed_in (genres), description

## Data Cleaning Steps Performed
### 1. Removed Duplicates

- Dropped duplicate rows

- Dropped duplicate titles with the same release year

### 2. Handled Missing Values

- Filled missing values in director, cast, country, and rating with "Unknown"

- Converted date_added to datetime format

- Created a binary column date_missing to track missing dates

### 3. Exploded Genres

- Split multiple genres in listed_in into separate rows for better analysis

### 4. Standardized Duration

- Cleaned duration column (removed "min", standardized season labels)

### 5. Normalized Categorical Features

- Removed extra spaces from type

- Created rating_group (Kids, Family, Teens, Adults, Unknown) from ratings

- Standardized country names (e.g., "USA" → "United States")

- One-hot encoded type

- Grouped rare countries into "Other"

### 6. Saved Cleaned Dataset

Exported as cleaned_netflix.csv for further analysis

## Insights Generated (So Far)

- Top genres identified across Netflix content

- Most rated shows and movies extracted

- Most watched shows and movies analyzed based on frequency

- Ratings distribution grouped into Kids, Family, Teens, and Adults

- Netflix content growth over time analyzed year by year

- Country-level content contributions identified

## Tools and Technologies

- Language: Python

- Libraries: Pandas, Matplotlib, Seaborn

- Platform: Databricks

- Version Control: GitHub


