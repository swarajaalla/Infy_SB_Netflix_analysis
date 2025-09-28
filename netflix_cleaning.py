#importing all the modules which can be needed in future 

from pyspark import *
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import *
from pyspark.sql.functions import *
from pyspark.sql.functions import to_date, col

# Reading the DataSet

df = spark.table("SpringBoard.netflix_titles")

df.printSchema()

df = df.withColumn('duration', split('duration', " "))

df = df.withColumn('duration_time' , col('duration')[0])
df = df.withColumn('duration_unit' , col('duration')[1])

df = df.withColumn('duration_time' , col('duration_time').cast('int'))
df = df.withColumn('release_year' , col('release_year').cast('int'))

df = df.withColumn('date_added', regexp_replace('date_added', ",", " "))

df_distinct = df.select("type").distinct()

df_fil = df.filter(col('type') == 'Movie')

df = df.drop(col('duration'))

df = df.select('show_id', "type", "duration_time",
               "duration_unit", "title", "director",
               "cast", "country", "date_added", "release_year",
               "rating","listed_in", "description")

df = df.fillna({'country': 'UNKNOWN', 'director': 'NOT KNOWN'})

df = df.withColumn('country', regexp_replace('country', ",", ""))

df = df.dropna()

df = df.withColumnRenamed('listed_in', 'genre')
df = df.withColumn(
    'genre',
    split(col('genre'), ",")
)

display(df)























