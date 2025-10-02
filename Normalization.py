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

df = df.select('show_id', 'title', 'movie', 'tv_show', 'title', 'duration_time',
               "duration_unit","rating","listed_in","release_year","date_added",
               "country","director","cast","description")

categories = [
    "Independent Movies",
    "Romantic TV Shows",
    "Thrillers",
    "Dramas",
    "Docuseries",
    "Sports Movies",
    "Horror Movies",
    "Cult Movies",
    "TV Mysteries",
    "TV Horror",
    "Classic Movies",
    "Anime Features",
    "Stand-Up Comedy &...",
    "Crime TV Shows",
    "TV Sci-Fi & Fantasy",
    "Faith & Spiritua..."
]

for cat in categories:
    col_name = cat.strip().replace(" ", "_").replace("&", "and").replace(".", "").replace("-", "_").replace("...", "etc").replace("/", "_").replace("'", "").replace(",", "").replace("  ", " ")
    df = df.withColumn(col_name, array_contains(col('listed_in'), cat))


for cat in categories:
    col_name = cat.strip().replace(" ", "_").replace("&", "and").replace(".", "").replace("-", "_").replace("...", "etc").replace("/", "_").replace("'", "").replace(",", "").replace("  ", " ")
    df = df.withColumn(col_name, when(col(col_name) == True, 1).otherwise(0))


df = df.drop(col('listed_in'))

df = df.withColumn('rating', array(col('rating')))

distinct_ratings = [row['rating'] for row in df.selectExpr("explode(rating) as rating").distinct().collect()]

for rating in distinct_ratings:
    col_name = rating.strip().replace(" ", "_").replace("-", "_").replace(".", "").replace("/", "_").replace("&", "and").replace("'", "")
    df = df.withColumn(col_name, array_contains(col('rating'), rating))

for rating in distinct_ratings:
    col_name = rating.strip().replace(" ", "_").replace("-", "_").replace(".", "").replace("/", "_").replace("&", "and").replace("'", "")
    df = df.withColumn(col_name, when(col(col_name) == True, 1).otherwise(0))


df = df.drop("rating", "84_min", "74_min", "66_min")

df = df.dropDuplicates(subset=['title'])

df = df.withColumn(
    'country',
    split(col('country'), ' ')
)


df = df.replace(
    {
        'United': 'USA',
        'Not': 'Not Found',
        'South': 'South Africa',
        'Hong':'Hong Kong',
        'New': 'New Jersey',
        'West': 'West Indies',
        'North': 'North Korea',
        'Central': 'Central African Republic',
        'East': 'East Timor'

    },
    subset=['country']
)

df = df.withColumn('country', array(col('country')))

distinct_countries = [row['country'] for row in df.selectExpr("explode(country) as country").distinct().collect()]

for country in distinct_countries:
    col_name = country.strip().replace(" ", "_").replace("-", "_").replace(".", "").replace("/", "_").replace("&", "and").replace("'", "")
    df = df.withColumn(col_name, when(array_contains(col('country'), country), 1).otherwise(0))

display(df)







































