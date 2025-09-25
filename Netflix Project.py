# Databricks notebook source
df = spark.table("workspace.default.netflix_titles")
display(df)


# COMMAND ----------

df = spark.read.table("netflix_titles")
df = df.dropna(how="all")
display(df)

# COMMAND ----------

# Drop rows where title is missing (title is required)
df = df.na.drop(subset=["title"])
display(df)

# COMMAND ----------

# Remove exact duplicates using key fields
df = df.dropDuplicates([
    "show_id", "title", "director", "cast", "country", "date_added",
    "release_year", "rating", "duration", "listed_in", "description"
])
display(df)