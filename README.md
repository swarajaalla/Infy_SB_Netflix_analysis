
# 🎬 **Netflix Dataset Analysis**

**Author:** Gagan Dhanapune
**Email:** [gagandhanapune@gmail.com](mailto:gagandhanapune@gmail.com)

---

## 🚀 **Project Overview**

This project provides a comprehensive analysis of the **Netflix Movies and TV Shows Dataset** sourced from Kaggle.
The objective is to transform raw data into actionable insights through **data cleaning**, **exploration**, **feature engineering**, and **machine learning modeling**.

The analysis focuses on:

* Understanding content trends and release patterns
* Identifying geographical and rating distributions
* Building machine learning models to classify and cluster Netflix titles

---

## 🧭 **Project Workflow**

The project follows a systematic six-phase pipeline:

1. **Data Cleaning & Preparation**
2. **Data Normalization**
3. **Exploratory Data Analysis (EDA)**
4. **Feature Engineering**
5. **Machine Learning Modeling**
6. **Advanced Analytical Insights**

---

## 🧹 **Phase 1: Data Cleaning**

* **Missing Values:** Replaced nulls in `director`, `cast`, and `country` columns with `"NA"`.
* **Duration Standardization:** Split the `duration` column into:

  * `duration_num` → numeric part
  * `duration_type` → “min” or “Season”
* **Date Formatting:** Converted `date_added` to `datetime` for time-based analysis.

---

## ⚙️ **Phase 2: Data Normalization**

* **Categorical Encoding:**

  * Applied **One-Hot Encoding** for multi-category features such as `listed_in` (genres).
  * Applied **Label Encoding** for single-category columns like `rating` and `country`.
* Ensured consistent data types for all features to prepare the dataset for machine learning.

---

## 📊 **Phase 3: Exploratory Data Analysis (EDA)**

### 🔸 Content Type Distribution

* **Movies** dominate Netflix’s catalog (**69.4%**) compared to **TV Shows (30.6%)**.

### 🔸 Content Growth Trends

* Sharp content growth post-**2015**.
* **2018–2019** marked Netflix’s peak in new title additions.
* Majority of content released **after 2010**.

### 🔸 Rating Analysis

* **TV-MA** and **TV-14** are the most frequent ratings → content aimed at adults and young adults.
* Limited content for children (`TV-Y`, `G`).

### 🔸 Geographical Insights

* **United States** is the largest content producer.
* **India** and **United Kingdom** follow as major contributors.

### 🔸 Duration Analysis

* Movies typically range between **75–150 minutes**.
* TV Shows usually consist of **1–3 seasons**.

---

## 🧩 **Phase 4: Feature Engineering**

Created new variables to enhance analysis and model interpretability:

| Feature                     | Description                                                          |
| :-------------------------- | :------------------------------------------------------------------- |
| `Content_Length_Category`   | Groups content as **Short**, **Medium**, or **Long**                 |
| `Content_Type`              | Classifies as **Original** or **Licensed**                           |
| `Content_Age_Group`         | Simplifies ratings into **Kids**, **Teens**, or **Adults**           |
| `Region`                    | Groups countries into larger regions (e.g., *Asia*, *North America*) |
| `year_added`, `month_added` | Extracted for temporal trend analysis                                |

---

## 🤖 **Phase 5: Machine Learning Modeling**

### 🧭 **1. Clustering (Unsupervised Learning – K-Means)**

**Objective:**
Group Netflix titles based on similarities in genre, duration, and rating.

**Process:**

1. Scaled and preprocessed selected features (`listed_in`, `duration`, `rating`).
2. Converted TV seasons to equivalent minute-based durations for uniformity.
3. Applied **K-Means** with **5 clusters**.

**Insights:**

* The largest cluster (**Cluster 2**) contained ~70% of titles.
* Clusters primarily separated by **content type** (Movies vs. TV Shows).
* **Duration** was the strongest differentiator between clusters.

---

### 🌲 **2. Classification (Supervised Learning – Random Forest)**

**Objective:**
Predict whether a title is a **Movie** or **TV Show** using its features.

**Features Used:**
`release_year`, `duration_num`, and `rating`

**Process:**

* Split data (80% training, 20% testing).
* Trained a **Random Forest Classifier** for binary prediction.

**Results:**

| Metric                 | Result                      |
| :--------------------- | :-------------------------- |
| **Accuracy**           | **99.9%**                   |
| **Misclassifications** | 1 out of 1,762 test samples |

**Key Insights:**

* **`duration_num`** was the most influential feature.
* Movies (in minutes) and TV Shows (in seasons) were distinctly separable.
* Excellent **precision** and **recall** confirmed model robustness.

---

## 🧠 **Phase 6: Advanced Analysis – Content Drivers**

A **Logistic Regression** model was implemented to interpret what factors drive a title to originate from a **Top 10 Content-Producing Country**.

**Hypothesis:**
Do top-producing countries focus more on specific genres or content characteristics?

**Features Analyzed:**
`duration`, `rating_code`, `content_age_years`, `is_top_genre`

**Results:**

| Feature          | Coefficient | Interpretation                                                                                                   |
| :--------------- | :---------- | :--------------------------------------------------------------------------------------------------------------- |
| **is_top_genre** | **-0.6141** | Strongest **negative** correlation — popular genres are *less likely* to originate from top-producing countries. |
| **duration**     | **+0.0006** | Slight **positive** relationship — longer duration modestly increases likelihood.                                |
| **rating_code**  | **-0.0785** | Weak **negative** relationship.                                                                                  |

**Conclusion:**
Top content-producing countries are diversifying into less common genres rather than repeating global top categories.

---

## 💻 **Technologies & Libraries Used**

| Category                 | Tools                                                      |
| :----------------------- | :--------------------------------------------------------- |
| **Programming Language** | Python                                                     |
| **Data Handling**        | Pandas, NumPy                                              |
| **Visualization**        | Matplotlib, Seaborn                                        |
| **Machine Learning**     | Scikit-learn (K-Means, Random Forest, Logistic Regression) |

---

**Key Outcomes:**

* Netflix’s library leans heavily toward **movies** and **adult-oriented content**.
* **US** and **India** dominate production.
* **Duration** is the most powerful predictor of content type.
* **Machine Learning models** (K-Means & Random Forest) achieved excellent clustering and classification results.
* **Logistic Regression** revealed nuanced insights into content origin and genre diversity.

---

