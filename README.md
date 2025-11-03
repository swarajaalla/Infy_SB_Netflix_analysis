# Netflix Dataset Analysis

**Author:** Gagan Dhanapune  
**Email:** gagandhanapune@gmail.com

---

## 🚀 Project Overview

This project provides an in-depth analysis of the Netflix shows and movies dataset from Kaggle. The goal was to take a raw dataset and transform it into a clean, feature-rich format to uncover insights and build machine learning models. The analysis covers content trends, geographical distribution, rating patterns, and the application of both unsupervised and supervised learning techniques.

The project is structured into the following key phases:
1.  **Data Cleaning & Normalization:** Preparing the raw data for analysis.
2.  **Exploratory Data Analysis (EDA):** Visualizing and understanding the dataset's primary characteristics.
3.  **Feature Engineering:** Creating new, insightful features to enhance analysis.
4.  **Modeling & Advanced Analytics:** Applying Clustering and Classification models to uncover patterns and make predictions.

---

## 📋 Project Phases

#### **Phase 1: Data Cleaning**
- **Handled Missing Values:** Filled nulls in `director`, `cast`, and `country` with "NA".
- **Standardized Data:** Cleaned the `duration` column by separating it into a numeric value (`duration_num`) and a type (`duration_type` like "min" or "Season").
- **Formatted Dates:** Converted `date_added` to a datetime format for time-series analysis.

#### **Phase 2: Data Normalization**
- **Encoded Categorical Data:** Converted columns like `listed_in` (genres), `rating`, and `country` into numerical formats using One-Hot and Label Encoding to prepare them for machine learning models.

---

## 📊 Key Insights from Exploratory Data Analysis (EDA)

1.  **Content Type Distribution:**
    *   Netflix's library is heavily dominated by **Movies**, which account for **69.4%** of the content, compared to **30.6%** for **TV Shows**.

2.  **Content Growth and Release Trends:**
    *   There was a dramatic increase in content added to Netflix starting around **2015**.
    *   The peak years for adding new titles were **2018 and 2019**, reflecting a period of aggressive content acquisition.
    *   Most content is recent, with the majority of titles released after **2010**.

3.  **Rating Analysis:**
    *   The most common ratings are **TV-MA** (Mature Audiences) and **TV-14**, indicating a strong focus on content for adults and young adults.
    *   Content for young children (e.g., TV-Y, G) is significantly less common.

4.  **Geographical Distribution:**
    *   The **United States** is the largest producer of content on the platform.
    *   **India** is the second-largest contributor, followed by the **United Kingdom**.

5.  **Duration Analysis:**
    *   Most movies fall between **75 and 150 minutes**.
    *   Most TV shows are short-form, with **1 to 3 seasons**.

---

## 🛠️ Feature Engineering

New features were created to provide deeper insights for the modeling phase:

-   **`Content_Length_Category`**: Grouped titles into "Short," "Medium," or "Long" based on their duration.
-   **`Content_Type`**: Differentiated between "Original" and "Licensed" content.
-   **`Content_Age_Group`**: Simplified ratings into "Kids," "Teens," and "Adults."
-   **`Region`**: Mapped countries to broader regions like "North America" and "Asia."
-   **Time-Based Features:** Extracted `year_added` and `month_added` to analyze trends over time.

---

## 🤖 Machine Learning Modeling

### 1. Clustering with K-Means (Unsupervised Learning)

-   **Goal:** To group similar Netflix titles based on their genre, duration, and rating.
-   **Process:**
    1.  Selected features (`listed_in`, `duration`, `rating`) were preprocessed and scaled.
    2.  TV Show seasons were converted to an equivalent minute duration for consistency.
    3.  A **K-Means** model was trained to group the data into **5 distinct clusters**.
-   **Results & Insights:**
    *   The model successfully segmented the content. **Cluster 2** was the largest, containing nearly **70%** of all titles.
    *   The clusters primarily differentiated content based on **type (Movie vs. TV Show)**, with `duration` being a key factor. For example, Clusters 1 and 2 were dominated by movies, while Clusters 0, 3, and 4 were mostly TV shows.

### 2. Classification with Random Forest (Supervised Learning)

-   **Goal:** To predict whether a title is a **Movie** or a **TV Show** using its other features.
-   **Process:**
    1.  Features used for prediction were `release_year`, `duration_num`, and `rating`.
    2.  A **Random Forest Classifier** was trained on an 80/20 train-test split of the data.
-   **Results & Insights:**
    *   **High Accuracy:** The model achieved an impressive **99.9% accuracy** on the test set, indicating it is highly effective at distinguishing between movies and TV shows.
    *   **Feature Importance:** The most critical feature for this prediction was **`duration_num`**. The numeric difference between movie runtimes (e.g., 90-150 min) and TV show season counts (e.g., 1-10) was the strongest predictor.
    *   **Confusion Matrix:** The model made only **one incorrect prediction** out of 1,762 test samples, confirming its high precision and recall.


## 💡 Phase 6: Advanced Analysis - Content Drivers

A **Logistic Regression** model was used to analyze the coefficients and interpret the factors driving a title to be produced by a **Top 10 Content Country**.

**Hypothesis:** Are top-contributing countries specialized in the most popular genres?
**Features Analyzed:** `duration`, `rating_code`, `content_age_years`, and `is_top_genre`.

### Logistic Regression Coefficient Analysis

| Feature | Coefficient | Impact Interpretation |
| :--- | :--- | :--- |
| **is\_top\_genre** | **-0.6141** | **Strongest Negative Impact.** Being in one of the Top 10 most common genres significantly *decreases* the likelihood of a title coming from a Top Country. |
| **duration** | **+0.0006** | Marginally positive correlation. |
| **rating\_code** | **-0.0785** | Small negative correlation. |

---

## 💻 Technologies & Libraries Used

-   **Python:** Core programming language.
-   **Pandas & NumPy:** For data manipulation and numerical operations.
-   **Matplotlib & Seaborn:** For data visualization.
-   **Scikit-learn:** For implementing K-Means Clustering, Random Forest Classification, and data preprocessing (StandardScaler, LabelEncoder).

---

## 🏁 Conclusion

This project successfully demonstrates a complete data analysis workflow on the Netflix dataset. Through EDA and feature engineering, we identified key trends in Netflix's content strategy, focusing on mature, licensed movies and shorter-run TV series primarily from the US and India. The machine learning models further validated these findings, with K-Means effectively segmenting content types and the Random Forest classifier accurately predicting them based on duration.

