# Databricks notebook source
# DBTITLE 1,Load_Netflix_Data
from pyspark.sql import SparkSession

# Load cleaned Netflix dataset
path = "/Volumes/workspace/default/netflix/cleaned_netflix_csv_single/"
df = spark.read.csv(path, header=True, inferSchema=True)


display(df)
df.printSchema()


# COMMAND ----------

# DBTITLE 1,Columns
# Cell: Show_Columns_and_Sample
print("Columns in the dataset:")
print(df.columns)

#display(df)


# COMMAND ----------

# DBTITLE 1,Detect_Boolean_Columns
from pyspark.sql.functions import col, lower, trim

boolean_like_columns = []

for c, dtype in df.dtypes:
    if dtype == "boolean":
        # actual boolean column
        boolean_like_columns.append(c)
    elif dtype == "string":
        # check if any value is "true" or "false" in the column (fast using filter)
        exists_true = df.filter(lower(trim(col(c))) == "true").limit(1).count() > 0
        exists_false = df.filter(lower(trim(col(c))) == "false").limit(1).count() > 0
        if exists_true or exists_false:
            boolean_like_columns.append(c)

print("Columns containing True/False values:\n")
for col_name in boolean_like_columns:
    print(col_name)


# COMMAND ----------

# DBTITLE 1,count
# Count total rows
total_rows = df.count()

# Count distinct titles + release_year
distinct_rows = df.select("title", "release_year").distinct().count()

print(f"Total rows: {total_rows}")
print(f"Distinct rows by title + release_year: {distinct_rows}")


# COMMAND ----------

# DBTITLE 1,Remove_Duplicates
df = df.dropDuplicates(["title", "release_year"])
print("After dropping duplicates, total rows:", df.count())


# COMMAND ----------

# DBTITLE 1,Null Counts
from pyspark.sql.functions import col, sum as _sum

null_counts = df.select([_sum(col(c).isNull().cast("int")).alias(c) for c in df.columns])
null_counts.display()


# COMMAND ----------

# DBTITLE 1,Boolean Conversion
from pyspark.sql.functions import col, when, lower, trim, lit

# Example list of columns you want to convert
boolean_columns = ["Independent_Movies", "Romantic_TV_Shows", "Thrillers"]

for c in boolean_columns:
    df = df.withColumn(
        c,
        when(col(c) == True, 1)  # actual boolean True
        .when(lower(trim(col(c).cast("string"))) == "true", 1)  # string "true"
        .when(lower(trim(col(c).cast("string"))) == "false", 0)  # string "false"
        .otherwise(0)  # anything else → 0
    )
display(df)


# COMMAND ----------

# MAGIC %md
# MAGIC Analyze Netflix content growth over time

# COMMAND ----------

# DBTITLE 1,Release years
df.select("release_year").distinct().orderBy("release_year").show(20)


# COMMAND ----------

# DBTITLE 1,content per year
from pyspark.sql.functions import col, count

content_per_year = (
    df.groupBy("release_year")
      .agg(count("*").alias("content_count"))
      .orderBy(col("release_year"))
)

content_per_year.show()


# COMMAND ----------

# DBTITLE 1,Netflix Content Growth Over Time
import plotly.express as px

spark.conf.set("spark.sql.ansi.enabled", "false")

from pyspark.sql.functions import year, count, col, to_date

if 'date_added' in df.columns:
    df_growth = df.withColumn(
        "year_added",
        year(
            to_date(
                col("date_added"),
                "MMMM d, yyyy"
            )
        )
    )
    growth_by_year = df_growth.groupBy("year_added").agg(
        count("*").alias("content_count")
    ).orderBy("year_added")
    display(growth_by_year)
    growth_pd = growth_by_year.toPandas()
    fig = px.line(
        growth_pd,
        x="year_added",
        y="content_count",
        title="Netflix Content Growth Over Time"
    )
    fig.show()

# COMMAND ----------

# MAGIC %md
# MAGIC Inspect Genre Column

# COMMAND ----------

# DBTITLE 1,Genre Columns
genre_cols = [col for col in df.columns if col in [
    "Independent_Movies", "Romantic_TV_Shows", "Thrillers", "Dramas",
    "Docuseries", "Sports_Movies", "Horror_Movies", "Cult_Movies",
    "TV_Mysteries", "TV_Horror", "Classic_Movies", "Anime_Features",
    "Stand_Up_Comedy_and", "Crime_TV_Shows", "TV_Sci_Fi_and_Fantasy",
    "Faith_and_Spiritua"
]]
print(genre_cols)


# COMMAND ----------

# DBTITLE 1,Genre_Distribution_Calculation
from pyspark.sql.functions import col, sum as _sum

# list of genre columns
genre_cols = [
    "Independent_Movies", "Romantic_TV_Shows", "Thrillers", "Dramas",
    "Docuseries", "Sports_Movies", "Horror_Movies", "Cult_Movies",
    "TV_Mysteries", "TV_Horror", "Classic_Movies", "Anime_Features",
    "Stand_Up_Comedy_and", "Crime_TV_Shows", "TV_Sci_Fi_and_Fantasy",
    "Faith_and_Spiritua"
]

# cast boolean to integer
df_casted = df.select([col(g).cast("int").alias(g) for g in genre_cols])

# now sum them
genre_counts = df_casted.select([_sum(col(g)).alias(g) for g in genre_cols])
genre_counts.show()


# COMMAND ----------

# DBTITLE 1,Distribution of Genres in Dataset
import pandas as pd
import matplotlib.pyplot as plt

# Convert the single-row PySpark DataFrame to Pandas
genre_counts_pd = genre_counts.toPandas().T.reset_index()
genre_counts_pd.columns = ['Genre', 'Count']

# Plot
plt.figure(figsize=(10, 6))
plt.barh(genre_counts_pd['Genre'], genre_counts_pd['Count'])
plt.xlabel("Number of Titles")
plt.ylabel("Genre")
plt.title("Distribution of Genres in Dataset")
plt.show()


# COMMAND ----------

# DBTITLE 1,Find the Dominant Genre for Each Title
from pyspark.sql.functions import array, greatest

# Combine all genre columns into one array to see the max genre per title
df_genre_flag = df.withColumn(
    "total_genres",
    sum([col(g).cast("int") for g in genre_cols])
)
df_genre_flag.select("title", "total_genres").show(5)


# COMMAND ----------

# DBTITLE 1,Visualize Top 5 Genres
top5 = genre_counts_pd.head(5)

plt.figure(figsize=(8, 5))
plt.barh(top5['Genre'], top5['Count'], color='lightgreen')
plt.xlabel("Number of Titles")
plt.ylabel("Genre")
plt.title("Top 5 Most Common Genres")
plt.gca().invert_yaxis()
plt.show()


# COMMAND ----------

# MAGIC %md
# MAGIC Ratings distribution & Movies vs TV Shows

# COMMAND ----------

# DBTITLE 1,Rating Columns
# List of rating candidates
rating_candidates = [
    "G","PG","PG_13","R","NC_17","UR","NR",
    "TV_G","TV_PG","TV_Y","TV_Y7","TV_Y7_FV","TV_14","TV_MA"
]

# Keep only columns present in the dataset
rating_cols = [c for c in df.columns if c in rating_candidates]
print("Rating columns found:", rating_cols)


# COMMAND ----------

# DBTITLE 1,Rating Count
from pyspark.sql.functions import col, sum as _sum

# Convert each rating column to int and sum
ratings_counts = df.select([col(c).cast("int").alias(c) for c in rating_cols])
ratings = ratings_counts.select([_sum(col(c)).alias(c) for c in rating_cols])

# Convert to Pandas for easier viewing
ratings_pd = ratings.toPandas().T.reset_index()
ratings_pd.columns = ["Rating", "Count"]

# Sort descending by count
ratings_pd = ratings_pd.sort_values("Count", ascending=False)
ratings_pd


# COMMAND ----------

# DBTITLE 1,Distribution Of Ratings
import matplotlib.pyplot as plt
import seaborn as sns

sns.set_style("whitegrid")
plt.figure(figsize=(10,6))

# Bar chart for all ratings
plt.barh(ratings_pd['Rating'], ratings_pd['Count'], color='lightgreen')
plt.xlabel("Number of Titles")
plt.ylabel("Rating")
plt.title("Distribution of Ratings")
plt.gca().invert_yaxis()  # highest count on top
plt.show()


# COMMAND ----------

top5_ratings = ratings_pd.head(5)
print(top5_ratings)


# COMMAND ----------

# MAGIC %md
# MAGIC Visualize the distribution of content type
# MAGIC

# COMMAND ----------

# DBTITLE 1,Content Type Columns
# Content type columns in your dataset
content_cols = ["movie", "tv_show"]

print("✅ Content types available:")
print(content_cols)


# COMMAND ----------

# DBTITLE 1,Count of those Columns
from pyspark.sql.functions import col, sum as _sum

# Sum each content type column (cast boolean to int if needed)
content_counts = df.select(
    _sum(col("movie")).alias("Movies"),
    _sum(col("tv_show")).alias("TV_Shows")
)

# Show the counts
content_counts.show()


# COMMAND ----------

# DBTITLE 1,Content Type Distribution
import matplotlib.pyplot as plt

# Convert to pandas for plotting
content_pd = content_counts.toPandas().T.reset_index()
content_pd.columns = ["Content_Type", "Count"]

# Plot horizontal bar chart
plt.figure(figsize=(6, 4))
plt.barh(content_pd['Content_Type'], content_pd['Count'], color=['teal', 'brown'])
plt.xlabel("Number of Titles")
plt.ylabel("Content Type")
plt.title("Distribution of Content Type")
plt.show()


# COMMAND ----------

# MAGIC %md
# MAGIC Identify country-level content contributions

# COMMAND ----------

# DBTITLE 1,Country columns
# List of some example country columns (adjust if needed)
country_candidates = [
    "USA","India","Canada","UK","France","Australia","Japan","Brazil","Germany","Mexico"
]

# Keep only columns that actually exist in your dataset
country_cols = [c for c in df.columns if c in country_candidates]

print("✅ Country columns found:")
print(country_cols)


# COMMAND ----------

# DBTITLE 1,Count Titles per Country
from pyspark.sql.functions import col, sum as _sum

# Convert boolean/int columns to int and sum each
df_country_cast = df.select([col(c).cast("int").alias(c) for c in country_cols])
country_counts = df_country_cast.select([_sum(col(c)).alias(c) for c in country_cols])

# Show the counts
country_counts.show()


# COMMAND ----------

# DBTITLE 1,Plot Top Countries
import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns

# Convert to pandas and reshape
country_pd = country_counts.toPandas().T.reset_index()
country_pd.columns = ["Country", "Count"]

# Sort descending by count
country_pd = country_pd.sort_values("Count", ascending=False)

# Take top 5 countries for clarity
top_countries = country_pd.head(5)

# Plot horizontal bar chart
sns.set_style("whitegrid")
plt.figure(figsize=(8,5))
plt.barh(top_countries['Country'], top_countries['Count'], color=sns.color_palette("cubehelix", 5))
plt.xlabel("Number of Titles")
plt.ylabel("Country")
plt.title("Top 5 Countries by Number of Titles")
plt.gca().invert_yaxis()  # Highest on top
plt.show()


# COMMAND ----------

# MAGIC %md
# MAGIC BIVARAINT ANALYSIS
# MAGIC

# COMMAND ----------

# DBTITLE 1,Clean Ratings Column
from pyspark.sql.functions import when, lit, col

rating_columns = [
    "TV_14", "TV_Y", "TV_PG", "E_40", "TV_Y7", "G",
    "TV_MA", "R", "PG", "TV_G", "PG_13", "NC_17",
    "TV_Y7_FV", "UR", "NR"
]

df = df.withColumn(
    "rating",
    when(col("TV_14") == True, lit("TV-14"))
    .when(col("TV_Y") == True, lit("TV-Y"))
    .when(col("TV_PG") == True, lit("TV-PG"))
    .when(col("E_40") == True, lit("E-40"))
    .when(col("TV_Y7") == True, lit("TV-Y7"))
    .when(col("G") == True, lit("G"))
    .when(col("TV_MA") == True, lit("TV-MA"))
    .when(col("R") == True, lit("R"))
    .when(col("PG") == True, lit("PG"))
    .when(col("TV_G") == True, lit("TV-G"))
    .when(col("PG_13") == True, lit("PG-13"))
    .when(col("NC_17") == True, lit("NC-17"))
    .when(col("TV_Y7_FV") == True, lit("TV-Y7-FV"))
    .when(col("UR") == True, lit("UR"))
    .when(col("NR") == True, lit("NR"))
    .otherwise(lit("Not_Found"))
)

# Show top 20 rows
df.select("title", "rating").show(20, truncate=False)


# COMMAND ----------

# DBTITLE 1,Ratings Distribution (Bar Graph)
from pyspark.sql.functions import count
import matplotlib.pyplot as plt

# Count titles per rating
rating_counts = df.groupBy("rating").agg(count("*").alias("count")).orderBy(col("count").desc())
rating_counts_pd = rating_counts.toPandas()

plt.figure(figsize=(12,6))
bars = plt.bar(rating_counts_pd['rating'], rating_counts_pd['count'], color='skyblue', edgecolor='black')
for bar in bars:
    plt.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 5, f'{int(bar.get_height())}', ha='center', va='bottom')
plt.title("Netflix Ratings Distribution", fontsize=16, fontweight="bold")
plt.xlabel("Rating")
plt.ylabel("Number of Titles")
plt.xticks(rotation=45)
plt.grid(axis='y', linestyle='--', alpha=0.7)
plt.tight_layout()
plt.show()


# COMMAND ----------

from pyspark.sql.functions import sum as _sum
import seaborn as sns

# Sum movie and tv_show columns per rating
content_counts = df.groupBy("rating").agg(
    _sum("movie").alias("Movie_Count"),
    _sum("tv_show").alias("TVShow_Count")
).orderBy(col("Movie_Count").desc())

# Convert to Pandas
content_counts_pd = content_counts.toPandas()

# Melt for grouped bar plot
content_melted = content_counts_pd.melt(
    id_vars=["rating"],
    value_vars=["Movie_Count", "TVShow_Count"],
    var_name="Content_Type",
    value_name="Count"
)

plt.figure(figsize=(12,6))
bars = sns.barplot(data=content_melted, x="rating", y="Count", hue="Content_Type", palette="pastel")
for p in bars.patches:
    bars.annotate(format(int(p.get_height()), ','),
                  (p.get_x() + p.get_width()/2., p.get_height()),
                  ha='center', va='bottom', fontsize=9)
plt.title("Number of Movies vs TV Shows per Rating", fontsize=16, fontweight="bold")
plt.xlabel("Rating")
plt.ylabel("Number of Titles")
plt.xticks(rotation=45)
plt.grid(axis="y", linestyle="--", alpha=0.7)
plt.tight_layout()
plt.show()
