import sqlite3
import pandas as pd
import os

# Create the database and tables
def initialize_database(db_path='data/processed/articles.db'):
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Create `links` table
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS links (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        newspaper TEXT NOT NULL,
        url TEXT UNIQUE NOT NULL
    )
    ''')


    # Create `content` table
    cursor.execute('''
    DROP TABLE IF EXISTS content;
    
    ''')
    cursor.execute('''
    CREATE TABLE content (
        id INTEGER PRIMARY KEY, -- Same as links.id
        title TEXT,
        publish_date TEXT,
        text TEXT,
        FOREIGN KEY (id) REFERENCES links (id) ON DELETE CASCADE
    );
    ''')
    

    conn.commit()
    conn.close()
    print("Database initialized.")

# Insert links into the database
def insert_links(csv_file, db_path='data/processed/articles.db'):
    df = pd.read_csv(csv_file)
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    for _, row in df.iterrows():
        try:
            cursor.execute('''
            INSERT OR IGNORE INTO links (newspaper, url)
            VALUES (?, ?)
            ''', (row['Newspaper'], row['Link']))
        except sqlite3.Error as e:
            print(f"Error inserting link: {row['Link']}, {e}")

    conn.commit()
    conn.close()
    print("Inserted links into the database.")

def insert_content(csv_file, db_path='data/processed/articles.db'):
    """
    Inserts article content from the CSV file into the `content` table.
    """
    df = pd.read_csv(csv_file, sep=';')
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    for _, row in df.iterrows():
        try:
            # Get the `id` from the `links` table for the current URL
            cursor.execute('SELECT id FROM links WHERE url = ?', (row['URL'],))
            url_id = cursor.fetchone()
            
            if url_id is None:
                print(f"URL not found in links table: {row['URL']}")
                continue

            # Insert into the content table using the same `id`
            cursor.execute('''
                INSERT OR IGNORE INTO content (id, title, publish_date, text)
                VALUES (?, ?, ?, ?)
            ''', (url_id[0], row['Title'], row['Date'], row['Text']))
        except sqlite3.Error as e:
            print(f"Error inserting content for URL: {row['URL']}, {e}")

    conn.commit()
    conn.close()
    print("Inserted content into the database.")

# Populate the database
def populate_database(links_csv, content_csv, db_path='data/processed/articles.db'):
    initialize_database(db_path)
    insert_links(links_csv, db_path)
    insert_content(content_csv, db_path)

# Main execution
if __name__ == "__main__":
    links_csv_path = "data/all_links.csv"
    content_csv_path = "data/processed/all_content.csv"
    database_path = "data/processed/articles.db"

    populate_database(links_csv_path, content_csv_path, database_path)
