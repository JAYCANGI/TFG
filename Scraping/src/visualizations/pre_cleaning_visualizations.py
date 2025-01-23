import pandas as pd
import matplotlib.pyplot as plt
from wordcloud import WordCloud
import sqlite3
import seaborn as sns
import numpy as np

# Load the database data into DataFrames
conn = sqlite3.connect('data/processed/articles.db')
links_df = pd.read_sql_query('SELECT * FROM links', conn)
content_df = pd.read_sql_query('SELECT * FROM content', conn)



# Parse the publish_date with time zone handling
content_df['publish_date'] = pd.to_datetime(content_df['publish_date'], utc=True)

# (Optional) Remove the time zone if not needed
content_df['publish_date'] = content_df['publish_date'].dt.tz_localize(None)

# Merge content_df with links_df to get the newspaper name
content_df = pd.merge(content_df, links_df[['id', 'newspaper']], on='id')

# 1. Newspaper Contribution
plt.figure(figsize=(10, 6))
links_df['newspaper'].value_counts().plot(kind='bar', color='skyblue')
plt.title('Number of Articles by Newspaper')
plt.xlabel('Newspaper')
plt.ylabel('Count')
plt.xticks(rotation=45)
plt.tight_layout()
plt.savefig('Scraping/src/visualizations/Pre_Cleaning/newspaper_contribution_before.png')
plt.show()

# 2. Publication Dates
content_df['publish_date'] = pd.to_datetime(content_df['publish_date'])
plt.figure(figsize=(10, 6))
content_df['publish_date'].dt.date.value_counts().sort_index().plot(kind='line', color='green')
plt.title('Number of Articles Published Over Time')
plt.xlabel('Date')
plt.ylabel('Count')
plt.tight_layout()
plt.savefig('Scraping/src/visualizations/Pre_Cleaning/publication_dates_before.png')
plt.show()

# 3. Article Lengths by Newspaper
# Calculate article lengths
content_df['text_length'] = content_df['text'].str.split().apply(len)

# Calculate outlier boundaries using IQR method
def get_outlier_bounds(df, column):
    Q1 = df[column].quantile(0.25)
    Q3 = df[column].quantile(0.75)
    IQR = Q3 - Q1
    lower_bound = Q1 - 1.5 * IQR
    upper_bound = Q3 + 1.5 * IQR
    return lower_bound, upper_bound

# Get outlier boundaries for each newspaper
outliers_summary = {}
for newspaper in content_df['newspaper'].unique():
    subset = content_df[content_df['newspaper'] == newspaper]
    lower, upper = get_outlier_bounds(subset, 'text_length')
    outliers = subset[
        (subset['text_length'] < lower) | 
        (subset['text_length'] > upper)
    ]
    outliers_summary[newspaper] = {
        'total_articles': len(subset),
        'outliers': len(outliers),
        'outlier_percentage': (len(outliers)/len(subset))*100,
        'lower_bound': lower,
        'upper_bound': upper
    }

# Create visualization
plt.figure(figsize=(15, 10))

# Box plot to show outliers
sns.boxplot(data=content_df, x='newspaper', y='text_length', 
            showfliers=True, palette='Set3')

# Customize plot
plt.title('Article Lengths Distribution with Outliers by Newspaper', 
          fontsize=16, pad=20)
plt.xlabel('Newspaper', fontsize=14)
plt.ylabel('Length (Words)', fontsize=14)
plt.xticks(rotation=45, ha='right')
plt.grid(axis='y', linestyle='--', alpha=0.7)

# Add outlier summary
summary_text = "Outlier Summary:\n"
for newspaper, stats in outliers_summary.items():
    summary_text += f"\n{newspaper}:\n"
    summary_text += f"Total articles: {stats['total_articles']}\n"
    summary_text += f"Outliers: {stats['outliers']} ({stats['outlier_percentage']:.1f}%)\n"
    summary_text += f"Bounds: {int(stats['lower_bound'])} - {int(stats['upper_bound'])}\n"

plt.figtext(1.02, 0.5, summary_text, fontsize=10, ha='left')

# Adjust layout and save
plt.tight_layout()
plt.savefig('Scraping/src/visualizations/Pre_Cleaning/article_lengths_outliers.png', 
            bbox_inches='tight', dpi=300)
plt.show()

# 4. Word Cloud
all_text = ' '.join(content_df['text'])
wordcloud = WordCloud(width=800, height=400, background_color='white').generate(all_text)
plt.figure(figsize=(10, 6))
plt.imshow(wordcloud, interpolation='bilinear')
plt.axis('off')
plt.title('Word Cloud of Article Texts')
plt.savefig('Scraping/src/visualizations/Pre_Cleaning/wordcloud_raw_before.png')
plt.show()

# Close the database connection
conn.close()
