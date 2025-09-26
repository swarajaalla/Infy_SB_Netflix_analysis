# Databricks notebook source
df = spark.table("workspace.default.netflix_titles")
display(df)


# COMMAND ----------


df = df.dropna(how="all")
display(df)


# COMMAND ----------

# Drop rows where title is missing (title is required)
df = df.na.drop(subset=["title","director","cast","country","date_added","release_year","rating","duration"])
display(df)



# COMMAND ----------


# Remove exact duplicates using key fields
df = df.dropDuplicates([
    "show_id", "title", "director", "cast", "country", "date_added",
    "release_year", "rating", "duration", "listed_in", "description"
])

display(df)

# COMMAND ----------

from pyspark.sql.functions import avg, when

# Define a mapping from string ratings to numeric scores
df_numeric = df.withColumn(
    "rating_numeric",
    when(df.rating == "TV-MA", 1)
    .when(df.rating == "R", 2)
    .when(df.rating == "PG-13", 3)
    .when(df.rating == "PG", 4)
    .when(df.rating == "TV-14", 5)
    .when(df.rating == "TV-PG", 6)
    .when(df.rating == "TV-G", 7)
    .when(df.rating == "G", 8)
    .otherwise(None)
)

df_grouped = df_numeric.groupBy("genre").agg(
    avg("rating_numeric").alias("avg_rating")
)

display(df_grouped)