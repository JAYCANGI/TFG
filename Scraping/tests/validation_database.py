import sqlite3


conn = sqlite3.connect('data/processed/articles.db')
cursor = conn.cursor()

# Check the first 5 rows in the `links` table
cursor.execute('SELECT * FROM links LIMIT 5')
for row in cursor.fetchall():
    print(row)

conn.close()
conn = sqlite3.connect('data/processed/articles.db')
cursor = conn.cursor()

# Check the first 5 rows in the `content` table
cursor.execute('SELECT * FROM content LIMIT 5')
for row in cursor.fetchall():
    print(row)

conn.close()
