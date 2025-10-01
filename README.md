# Netflix Dataset Analysis

**Name:** Gagan Dhanapune  
**Email:** gagandhanapune@gmail.com  

---

## Project Overview

This project focuses on analyzing the Netflix content dataset available on Kaggle. The dataset contains information about movies and TV shows available on Netflix, including details such as title, director, cast, country, release year, rating, duration, and genre.  

The analysis is divided into two main phases:  

- **Phase 1:** Data Cleaning  
- **Phase 2:** Data Normalization  

These phases lay the foundation for exploratory data analysis (EDA), feature engineering, and predictive modeling in later milestones.

---

## Phase 1: Data Cleaning

### Steps Performed
1. **Loading the Dataset**  
   - Imported Netflix dataset using Pandas.
   
2. **Handling Missing Values**  
   - Filled missing values for `director`, `cast`, `country`, `date_added`, and `rating` with placeholders like `'Not Available'`.
   - Replaced missing `duration` with `'0 min'`.
   
3. **Removing Duplicates**  
   - Ensured each row is unique based on all columns.
   
4. **Text Standardization**  
   - Stripped extra spaces in string columns.
   - Standardized text formatting (e.g., title casing for `title` and `director`).
   
5. **Duration Extraction**  
   - Extracted numeric values from the `duration` column.
   - Created a separate column for duration type (`min` or `Season`).

6. **Date Formatting**  
   - Converted `date_added` to datetime format.
   - Replaced invalid or missing dates with a placeholder.

7. **Handling Multi-Values**  
   - For columns like `director` and `country`, only the first value was retained if multiple values were listed.

### Insights from Cleaning
- The dataset contains **8807 rows and 12 columns** initially.  
- Missing data was significant in `director`, `cast`, and `country` columns.  
- Duration and release year provide valuable numeric metrics for analysis.  
- Cleaning ensures the dataset is consistent, complete, and ready for normalization and further analysis.  

---

## Phase 2: Data Normalization

### Steps Performed
1. **Encoding Categorical Features**  
   - **Genres (`listed_in`)**: One-hot encoded using `MultiLabelBinarizer`, creating separate binary columns for each genre.  
   - **Rating**: Ordinal encoding based on content maturity level (`G`, `PG`, `PG-13`, `R`, `TV-MA`, etc.).  
   - **Country**: Label encoded to convert string country names into numeric codes.  

2. **Optional Feature Engineering**  
   - Encoded `type` column into binary columns: `is_movie` and `is_tv_show`.  

3. **Saving Normalized Dataset**  
   - Saved as `netflix_normalized.csv`, which is ready for EDA and modeling.  

### Insights from Normalization
- Each genre now has its own column, allowing for **multi-genre analysis**.  
- Rating encoding allows for **comparative analysis** across content maturity levels.  
- Country encoding enables **geographical insights** into Netflix content distribution.  
- Normalized data supports machine learning models and clustering algorithms.

---

## Potential Analysis & Metrics

After Phase 1 & 2, the dataset is fully prepared for:

1. **Content Distribution Analysis**  
   - Top genres, ratings, and countries contributing to Netflix content.  
   - Content type distribution (Movies vs TV Shows).  

2. **Growth Over Time**  
   - Analyze trends in content addition by year.  
   - Duration trends for movies and TV shows.  

3. **Audience & Rating Insights**  
   - Distribution of content by ratings (G, PG, TV-MA, etc.).  
   - Compare rating distribution across countries or genres.  

4. **Genre-Based Insights**  
   - Count of multi-genre content.  
   - Popular genre combinations.  

5. **Machine Learning Potential**  
   - **Clustering** of content based on genre, duration, and ratings.  
   - **Classification** of content type (Movie vs TV Show).  
   - Feature importance analysis to determine key drivers of content availability.  

---

---

## Conclusion

By completing **Phase 1 & 2**, we now have a **cleaned and normalized Netflix dataset**, suitable for deep exploratory analysis and predictive modeling.  
This structured dataset allows insights into content type distribution, genre popularity, ratings, and country-level contributions, forming the basis for interactive dashboards and advanced analysis in later milestones.
