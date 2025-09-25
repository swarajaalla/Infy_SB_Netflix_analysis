#importing all the modules which can be needed in future 

from pyspark import *
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import *

# Reading the DataSet

df = spark.table("netflix_titles.csv")

# removing Duplicates

df_rd = df.DropDuplicates()

# Handeling Nulls

df_hn = df_rd.dropna('any')

# Some changing the view of tha data in the column eg country names was in single line and now it is in element of the list same goes with listed_in column

df_drop.withColumn('country', split(col('country'), ","))\
  .withColumn('listed_in', split(col('listed_in'), ",") ).display()

# Changing the Date formate

df_drop = df_drop.withColumn(
    "date_added",
    to_date(col("date_added"), "MMMM d, yyyy")
)

# importing the file in the csv file

df_drop.to_csv("netflix_titles_cleaned.csv")

