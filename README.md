# Netflix Dataset Analysis

**Name:** Gagan Dhanapune  
**Email:** gagandhanapune@gmail.com  

---

## Project Overview

This project focuses on analyzing the Netflix content dataset available on Kaggle. The dataset contains information about movies and TV shows available on Netflix, including details such as title, director, cast, country, release year, rating, duration, and genre.  

The analysis is divided into three main phases:  

- **Phase 1:** Data Cleaning  
- **Phase 2:** Data Normalization  
- **Phase 3:** Exploratory Data Analysis (EDA)

These phases lay the foundation for feature engineering and predictive modeling in later milestones.

---

## Phase 1: Data Cleaning

### Steps Performed
1. **Loading the Dataset**  
   - Imported the raw Netflix dataset using Pandas.
   
2. **Handling Missing Values**  
   - Filled missing values for `director`, `cast`, `country`, `date_added`, and `rating` with placeholders like `'NA'`.
   
3. **Removing Duplicates**  
   - Ensured each row is unique based on all columns to maintain data integrity.
   
4. **Text Standardization**  
   - Stripped extra spaces in string columns like `title`.
   
5. **Duration Extraction**  
   - Extracted numeric values from the `duration` column.
   - Created a separate column for duration type (`min` or `Season`) for easier analysis.

6. **Date Formatting**  
   - Converted `date_added` to datetime format to enable time-series analysis.

### Insights from Cleaning
- The dataset contains **8,807 rows and 12 columns** initially.  
- Missing data was significant in the `director`, `cast`, and `country` columns, which was handled by filling them with a placeholder.  
- Cleaning ensures the dataset is consistent, complete, and ready for normalization and further analysis.  

---

## Phase 2: Data Normalization

### Steps Performed
1. **Encoding Categorical Features**  
   - **Genres (`listed_in`)**: One-hot encoded using `MultiLabelBinarizer`, creating separate binary columns for each genre.  
   - **Rating**: Ordinal encoding based on content maturity level (`G`, `PG`, `PG-13`, `R`, `TV-MA`, etc.).  
   - **Country**: Label encoded to convert string country names into numeric codes.  

2. **Feature Engineering**  
   - Encoded the `type` column into binary columns: `is_movie` and `is_tv_show`.  

3. **Saving Normalized Dataset**  
   - Saved as `netflix_normalized.csv`, which is ready for EDA and modeling.  

### Insights from Normalization
- Each genre now has its own column, allowing for **multi-genre analysis**.  
- Rating encoding allows for **comparative analysis** across content maturity levels.  
- Country encoding enables **geographical insights** into Netflix content distribution.  
- Normalized data supports machine learning models and clustering algorithms.

---

## Phase 3: Exploratory Data Analysis (EDA)

This phase focused on uncovering patterns, trends, and key characteristics of the Netflix content library through visualizations and statistical summaries.

### Key Insights from EDA

1. **Content Type Distribution**
   - The Netflix library is heavily dominated by **Movies**, which make up **69.4%** of the content.
   - **TV Shows** constitute the remaining **30.6%**.

2. **Content Growth and Release Trends**
   - There has been a significant and rapid increase in content added to the platform, especially after **2015**.
   - The peak years for adding new content were **2018 and 2019**, reflecting a period of aggressive library expansion.
   - Most of the content available on Netflix is relatively new, with the majority of titles being released after **2010**.

3. **Rating Analysis**
   - The most common rating for both Movies and TV Shows is **TV-MA** (Mature Audiences), followed closely by **TV-14**.
   - This indicates that Netflix's content catalog is strongly geared towards **adult and young adult audiences**.
   - Content rated for children (e.g., TV-Y, G) is significantly less common.

4. **Geographical Distribution**
   - The **United States** is the leading contributor of content on Netflix by a substantial margin.
   - **India** is the second-largest content producer, driven by its large film industry.
   - The **United Kingdom** ranks third, with other notable contributions from countries like Canada and South Korea.

5. **Duration Analysis**
   - For **Movies**, the duration is fairly consistent across release years, with most films clustering between 75 and 150 minutes.
   - For **TV Shows**, the data shows that most series have a limited number of seasons, typically between 1 to 3. There is a slight trend indicating that newer TV shows often have fewer seasons compared to older ones.

---

## Potential Future Analysis & Metrics

After completing the EDA, the dataset is fully prepared for more advanced analysis:

1. **Deeper Content Analysis**  
   - Investigate popular genre combinations (e.g., which genres are most frequently paired together).
   - Analyze the relationship between ratings and content duration (e.g., do R-rated movies tend to be longer?).

2. **Time-Series Insights**  
   - Explore the seasonal trends in adding new content.
   - Analyze the average age of content in the library over time.

3. **Audience & Rating Insights**  
   - Compare rating distributions across different top-contributing countries.  
   - Identify which genres are most common for each rating category.

4. **Machine Learning Potential**  
   - **Clustering** of content based on genre, duration, and ratings to identify "content clusters."
   - **Classification** models to predict the content type (Movie vs. TV Show) based on other features.  
   - Feature importance analysis to determine which attributes are most influential.

---

## Conclusion

By completing **Phases 1, 2, and 3**, we now have a **cleaned, normalized, and thoroughly explored Netflix dataset**. The EDA has revealed clear trends in content type, genre popularity, rating distribution, and geographical contributions. These insights form a strong foundation for building interactive dashboards, generating business recommendations, and developing advanced machine learning models in subsequent project milestones.
