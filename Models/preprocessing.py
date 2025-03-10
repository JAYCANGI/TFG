import sqlite3 as sql
import pandas as pd
import spacy
import nltk
nltk.download('stopwords')
from nltk.corpus import stopwords
import re
import tqdm

# Connection with the database
def connect_to_db(db_path):
    try:
        conn = sql.connect(db_path)
        return conn
    except Exception as e:
        print(f"Error connecting to database: {e}")
        return None
 
# Fetch the data from the database
def fetch_text_data(conn):
    if conn is None:
        return pd.DataFrame()  # Return an empty DataFrame if connection failed
    query = 'SELECT id, title, text FROM content'
    df = pd.read_sql_query(query, conn)
    return df


#Clean the text
def clean_text(text):
    text = text.lower()
    text = re.sub(r'http\S+|www.\S+', '', text)  # Remove URLs
    text = re.sub(r'\d+', '', text)  # Remove numbers
    text = re.sub(r'[^\w\s]', '', text)  # Remove punctuation
    text = re.sub(r'\s+', ' ', text).strip()  # Remove extra spaces
    return text

#Remove Stopwords
def remove_stopwords(text):
    stop_words = set(stopwords.words('spanish'))
    return ' '.join([word for word in text.split() if word not in stop_words])

 #Lemmatization and Tokenization
#nlp = spacy.load("es_core_news_sm")
#def lemmatize_text(text):
    doc = nlp(text)
    return " ".join([token.lemma_ for token in doc if token.is_alpha and token.pos_ in ["ADJ", "VERB"]])

# Add clean columns to the database
def add_clean_columns(conn):
    cursor = conn.cursor()
    cursor.execute('ALTER TABLE content ADD COLUMN clean_text TEXT')
    cursor.execute('ALTER TABLE content ADD COLUMN clean_title TEXT')
    conn.commit()

# Update the database with clean text and title columns
def update_clean_columns(conn, df):
    try:
        try:
            add_clean_columns(conn)
        except sql.OperationalError:
            print('Columns already exist')

        # Process each text and update database
        for index, row in tqdm.tqdm(df.iterrows(), total=len(df), desc="Updating clean columns"):
            clean_text_value = clean_text(row['text'])
            clean_title_value = clean_text(row['title'])
            clean_text_value = remove_stopwords(clean_text_value)
            clean_title_value = remove_stopwords(clean_title_value)

            # Update database
            cursor = conn.cursor()
            cursor.execute('''
            UPDATE content 
            SET clean_text = ?, clean_title = ? 
            WHERE id = ?
            ''', (clean_text_value, clean_title_value, row['id']))

        conn.commit()
        print("Successfully updated clean columns in database")

    except Exception as e:
        print(f"Error updating clean columns: {e}")


def main(db_path):
    conn = connect_to_db(db_path)
    df = fetch_text_data(conn)
    if not df.empty:
        print('Data fetched from the database.')
        pd.set_option('display.max_colwidth', None)
        update_clean_columns(conn, df)
        
    else:
        print("No data fetched from the database.")

if __name__ == "__main__":
    db_path = 'Code/data/processed/articles.db'
    main(db_path)