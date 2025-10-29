# Databricks notebook source
afile_path = "/Volumes/workspace/default/netflix_titles/netflix_titles.csv"

# COMMAND ----------

# MAGIC %md
# MAGIC **CLUSTERING**

# COMMAND ----------

import pandas as pd
df = pd.read_csv('/Volumes/workspace/default/netflix_titles/netflix_titles.csv')
df.display()

# COMMAND ----------



# COMMAND ----------

#prepare gener feature
from pyspark.sql.functions import split, explode, trim, col

# Load CSV as Spark DataFrame
df_spark = spark.read.option("header", True).csv(afile_path)

# Split 'listed_in' column (genres) into array, explode to get one genre per row, and trim whitespace
df_genre = df_spark.withColumn("genre", explode(split(col("listed_in"), ","))) \
                   .withColumn("genre", trim(col("genre")))

display(df_genre.select("show_id", "title", "genre"))

# COMMAND ----------

# Prepare rating feature

df_rating = df_spark.withColumn("rating", trim(col("rating")))
display(df_rating.select("show_id", "title", "rating"))

# COMMAND ----------

# prepare 'combine_feature'

from pyspark.sql.functions import concat_ws

df_combine = df_spark.withColumn(
    "combine_feature",
    concat_ws(" ", 
        col("title"), 
        col("director"), 
        col("cast"), 
        col("listed_in"), 
        col("description")
    )
)

display(df_combine.select("show_id", "combine_feature"))

# COMMAND ----------

afile_path = "/Volumes/workspace/default/netflix_titles/netflix_titles.csv"

df_spark = spark.read.option("header", True).csv(afile_path)

from pyspark.sql.functions import concat_ws, col

df_combine = df_spark.withColumn(
    "combine_feature",
    concat_ws(" ", 
        col("title"), 
        col("director"), 
        col("cast"), 
        col("listed_in"), 
        col("description")
    )
)

# COMMAND ----------

from pyspark.ml.feature import Tokenizer, HashingTF, IDF, VectorAssembler
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator

# Tokenize the combine_feature column
tokenizer = Tokenizer(inputCol="combine_feature", outputCol="words")
words_data = tokenizer.transform(df_combine)

# Compute term frequencies
hashing_tf = HashingTF(inputCol="words", outputCol="rawFeatures", numFeatures=1000)
featurized_data = hashing_tf.transform(words_data)

# Compute the IDF (inverse document frequency)
idf = IDF(inputCol="rawFeatures", outputCol="features")
idf_model = idf.fit(featurized_data)
rescaled_data = idf_model.transform(featurized_data)

# Apply KMeans clustering
kmeans = KMeans(featuresCol="features", predictionCol="cluster", k=5, seed=1)
model = kmeans.fit(rescaled_data)
predictions = model.transform(rescaled_data)

# Evaluate clustering by computing Silhouette score
evaluator = ClusteringEvaluator(featuresCol="features", predictionCol="cluster")
silhouette = evaluator.evaluate(predictions)

display(predictions.select("show_id", "title", "cluster"))

# COMMAND ----------

# MAGIC %md
# MAGIC **CLASSIFICATION**

# COMMAND ----------

from pyspark.sql.functions import split, explode, trim, col

df_genre = df_spark.withColumn("genre", explode(split(col("listed_in"), ","))) \
                   .withColumn("genre", trim(col("genre")))

# COMMAND ----------

# Classification of content type (Movie vs TV Show) using features

from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.classification import LogisticRegression
from pyspark.ml import Pipeline

# Select relevant features and label
feature_cols = ["rating", "genre"]
df_features = df_genre.withColumn("type_label", col("type"))

# StringIndexers for categorical features
indexers = [
    StringIndexer(inputCol="rating", outputCol="rating_idx", handleInvalid="keep"),
    StringIndexer(inputCol="genre", outputCol="genre_idx", handleInvalid="keep"),
    StringIndexer(inputCol="type_label", outputCol="label", handleInvalid="keep")
]

# Assemble features
assembler = VectorAssembler(
    inputCols=["rating_idx", "genre_idx"],
    outputCol="features"
)

# Logistic Regression classifier
lr = LogisticRegression(featuresCol="features", labelCol="label")

# Pipeline
pipeline = Pipeline(stages=indexers + [assembler, lr])

# Fit model
model = pipeline.fit(df_features)

# Predict content type
predictions = model.transform(df_features)

display(predictions.select("show_id", "title", "genre", "rating", "type", "prediction"))

# COMMAND ----------

from pyspark.sql.functions import split, explode, trim, col, countDistinct, count, desc

# Explode 'country' and 'listed_in' (genre) columns to get one country and one genre per row
df_country_genre = df_spark \
    .withColumn("country", explode(split(col("country"), ","))) \
    .withColumn("country", trim(col("country"))) \
    .withColumn("genre", explode(split(col("listed_in"), ","))) \
    .withColumn("genre", trim(col("genre")))

# Count number of unique titles per country and genre
country_genre_counts = df_country_genre.groupBy("country", "genre") \
    .agg(countDistinct("show_id").alias("title_count")) \
    .orderBy(desc("title_count"))

display(country_genre_counts)

# Analyze which genres are most available in the most countries
genre_country_coverage = df_country_genre.groupBy("genre") \
    .agg(countDistinct("country").alias("country_count"),
         countDistinct("show_id").alias("title_count")) \
    .orderBy(desc("country_count"))

display(genre_country_coverage)

# Analyze which countries have the most diverse genre availability
country_genre_diversity = df_country_genre.groupBy("country") \
    .agg(countDistinct("genre").alias("genre_count"),
         countDistinct("show_id").alias("title_count")) \
    .orderBy(desc("genre_count"))

display(country_genre_diversity)

# COMMAND ----------

