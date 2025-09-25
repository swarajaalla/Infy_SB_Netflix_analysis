
# Netflix Data Analysis

## Project Domain Knowledge

Analyzing Netflix dataset to understand content patterns, audience focus, and regional trends.

## Project Objective & Plan

* Clean and prepare raw Netflix data.
* Generate insights on content type, genres, ratings, and trends.
* Use cleaned data for future visualization and analysis.

## Data Observation

* Dataset has movies and TV shows with fields like title, type, director, cast, country, rating, release year, and duration.
* Found missing values, duplicates, and mixed formats in duration.

## Tools Used

* Python (Pandas, NumPy)
* Databricks
* Git

## Data Flow

Raw Data → Cleaning → Transformation → Insights → Save Clean Data

## Databricks Overview

Platform used for collaborative data cleaning, analysis, and future scalability.

## Data Cleaning

* Filled missing values with "NA".
* Removed duplicates.
* Split `duration` into `duration_num` and `duration_type`.
* Cleaned text and standardized data.
