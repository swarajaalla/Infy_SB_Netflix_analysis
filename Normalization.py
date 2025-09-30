df = df.withColumn(
    'listed_in',
    split(col('listed_in'), ",")
)

df = df.withColumn("type", array(col("type")))

df = df.withColumn('movie', array_contains(col('type'), 'Movie'))
df = df.withColumn('tv_show', array_contains(col('type'), 'TV Show'))

df = df.withColumn("movie", when(col("movie") == True, 1).otherwise(0)) \
       .withColumn("tv_show", when(col("tv_show") == True, 1).otherwise(0))

df = df.drop("type")

df = df.fillna('Not found')

ratings_dict = {
    "G": 1,
    "TV-Y": 2,
    "TV-Y7": 3,
    "TV-G": 4,
    "PG": 5,
    "TV-PG": 6,
    "PG-13": 7,
    "TV-14": 8,
    "R": 9,
    "TV-MA": 10
}

mapping_expr = create_map([lit(x) for pair in ratings_dict.items() for x in pair])
df = df.withColumn(
    "rating",
    mapping_expr.getItem(col("rating")).cast(IntegerType())
)

df = df.select('show_id', 'title', 'movie', 'tv_show', 'title', 'duration_time', "duration_unit","rating","listed_in","release_year","date_added","country","director","cast","description")







































