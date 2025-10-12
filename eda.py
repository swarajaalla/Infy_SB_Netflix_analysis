import matplotlib.pyplot as plt

# Count number of titles added per year
content_growth = df['release_year'].value_counts().sort_index()

# Plot content growth over time
plt.figure(figsize=(12,6))
plt.plot(content_growth.index, content_growth.values, marker='o')
plt.title('Netflix Content Growth Over Time')
plt.xlabel('Release Year')
plt.ylabel('Number of Titles')
plt.grid(True)
plt.tight_layout()
plt.show()

# Bar plot for the same
plt.figure(figsize=(12,6))
plt.bar(content_growth.index, content_growth.values)
plt.title('Netflix Content Growth Over Time')
plt.xlabel('Release Year')
plt.ylabel('Number of Titles')
plt.tight_layout()
plt.show()
import matplotlib.pyplot as plt

# Genre Distribution (Top 10)
#top_genres = df_exploded['genre'].value_counts().head(10)
plt.figure(figsize=(10,6))
#top_genres.plot(kind='bar')
plt.title('Top 10 Most Common Genres')
plt.xlabel('Genre')
plt.ylabel('Count')
plt.tight_layout()
plt.show()

# Ratings Distribution (Top 10)
top_ratings = df['rating'].value_counts().head(10)
plt.figure(figsize=(10,6))
top_ratings.plot(kind='bar')
plt.title('Most Common Ratings')
plt.xlabel('Rating')
plt.ylabel('Count')
plt.tight_layout()
plt.show()

# Content Type Distribution
type_counts = df['type'].value_counts()
plt.figure(figsize=(6,4))
type_counts.plot(kind='pie', autopct='%1.1f%%', startangle=90, colors=['#66b3ff','#ff9999'])
plt.title('Content Type Distribution')
plt.ylabel('')
plt.tight_layout()
plt.axis('equal')
plt.show()
import matplotlib.pyplot as plt
import plotly.express as px

# Count number of titles per country (handling multiple countries per title)
country_exploded = df.copy()
country_exploded['country'] = country_exploded['country'].str.split(',')
country_exploded = country_exploded.explode('country')
country_exploded['country'] = country_exploded['country'].str.strip()
country_counts = country_exploded['country'].value_counts().drop('Unknown', errors='ignore').head(20)

# Bar chart: Top 20 countries by content count
plt.figure(figsize=(12,6))
country_counts.plot(kind='bar')
plt.title('Top 20 Countries by Number of Netflix Titles')
plt.xlabel('Country')
plt.ylabel('Number of Titles')
plt.tight_layout()
plt.show()

# World map: Content count by country (top 50 for clarity)
country_map_counts = country_exploded['country'].value_counts().drop('Unknown', errors='ignore').head(50)
country_map_df = country_map_counts.reset_index()
country_map_df.columns = ['country', 'count']

fig = px.choropleth(
    country_map_df,
    locations='country',
    locationmode='country names',
    color='count',
    color_continuous_scale='Reds',
    title='Netflix Content Contribution by Country'
)
fig.show()