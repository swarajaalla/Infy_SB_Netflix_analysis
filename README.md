
# Netflix Dataset Analysis

**Name:** Gagan Dhanapune  
**Email:** gagandhanapune@gmail.com  

---

## Project Overview

This project focuses on analyzing the Netflix content dataset available on Kaggle. The dataset contains information about movies and TV shows available on Netflix, including details such as title, director, cast, country, release year, rating, duration, and genre.  

The analysis is divided into four main phases:  

- **Phase 1:** Data Cleaning  
- **Phase 2:** Data Normalization  
- **Phase 3:** Exploratory Data Analysis (EDA)
- **Phase 4:** Feature Engineering

These phases prepare the dataset for in-depth analysis, visualization, and potential machine learning applications.

---

## Phase 1: Data Cleaning

### Steps Performed
1. **Loading the Dataset**  
   - Imported the raw Netflix dataset using Pandas.
   
2. **Handling Missing Values**  
   - Filled missing values for `director`, `cast`, `country`, `date_added`, and `rating` with the placeholder `'NA'`.
   
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

## Phase 4: Feature Engineering

This phase involved creating new, meaningful features from the existing cleaned data to make analysis more powerful and insightful.

### Steps Performed & Insights

**1. Creating Content Length Category**
- **Process:** A custom function was created to categorize each title's duration into meaningful groups. The logic was applied differently for movies (based on minutes) and TV shows (based on seasons).
  - **Movies:** Short (≤ 60 min), Medium (61–120 min), Long (> 120 min).
  - **TV Shows:** Limited (1 Season), Moderate (2–3 Seasons), Long-running (≥ 4 Seasons).
- **Output:** A new column, `Content_Length_Category`, was created.
- **Insights:**
  - Most movies on Netflix fall into the **Medium** length category.
  - The majority of TV shows are categorized as **Limited** or **Moderate**, suggesting a focus on shorter-format series.

**2. Identifying Netflix Originals**
- **Process:** A new binary feature, `is_original`, was created to distinguish between Netflix Originals and licensed content. This was done by checking if the word "Netflix" appeared in the title.
- **Output:** A new column, `is_original`, with values 'Original' or 'Licensed'.
- **Insights:**
  - The analysis reveals that the vast majority of titles in the dataset are **licensed content**, not Netflix Originals. This indicates a strong reliance on acquiring popular shows and movies to build its library.

**3. Mapping Countries to Regions**
- **Process:** Each title's `country` was mapped to a broader geographical region (e.g., North America, Asia, Europe). A custom function was applied to handle multiple countries and assign them to the most relevant region.
- **Output:** A new column, `Region`, was created.
- **Insights:**
  - **North America** (primarily the U.S.) is the dominant content provider.
  - **Asia** follows as a significant contributor, with **India** and **South Korea** leading the way, which aligns with Netflix's global content strategy.

**4. Creating Content Age Groups**
- **Process:** The various rating labels (e.g., TV-MA, PG-13, R) were simplified into four distinct audience groups: **Kids**, **Teens**, **Adults**, and **Unknown**.
- **Output:** A new column, `Content_Age_Group`, was created to simplify audience segmentation.
- **Insights:**
  - The content library is heavily skewed towards **Teens and Adults**, which together account for nearly 90% of the content.
  - This shows a clear focus on mature storytelling and content for older audiences over children's programming.

### Visualizations Created in Phase 4
- **Bar Chart:** Distribution of Content Length Categories.
- **Bar Chart:** Comparison of Original vs. Licensed Titles.
- **Line Chart:** Yearly Growth of Netflix Content (Movies vs. TV Shows).
- **Pie & Bar Charts:** Distribution of Audience Categories (Content\_Age\_Group).
- **Bar Chart:** Number of Titles by Region.
- **Stacked Bar Chart:** Distribution of Audience Categories within each Region.

---

## Key Insights - Final Summary

- **Licensed vs. Original Content:** Most titles on Netflix are licensed, indicating a strategy of streaming popular shows and movies produced by other companies.
- **Content Length:** Most movies are of **Medium length** (61-120 mins), while most TV shows are **Limited or Moderate series** (1-3 seasons).
- **Audience Target:** The majority of Netflix's content is aimed at **Teens and Adults**, with a smaller portion dedicated to Kids.
- **Regional Contribution:** The primary source of content is **North America**, with **Asia** (led by India and South Korea) being a strong secondary contributor.
- **Content Growth:** The volume of content added to Netflix each year has been increasing, especially in recent years, which points to the platform's rapid global expansion.

---

## Conclusion

By completing **Phases 1 through 4**, we now have a **cleaned, normalized, and feature-rich Netflix dataset**. The EDA and feature engineering have revealed clear trends in content type, genre popularity, rating distribution, and geographical contributions. These insights provide a strong foundation for building interactive dashboards, generating business recommendations, and developing advanced machine learning models in subsequent project milestones.
