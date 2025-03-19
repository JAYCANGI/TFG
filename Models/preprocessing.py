import sqlite3 as sql
import pandas as pd
import spacy
import nltk
import re
import tqdm
from nltk.corpus import stopwords

# Download and load stopwords
nltk.download('stopwords')
stop_words = set(stopwords.words('spanish'))

# Load spaCy model for lemmatization
nlp = spacy.load("es_core_news_sm")

#Function to connect to the database
def connect_to_db(db_path):
    try:
        conn = sql.connect(db_path)
        return conn
    except Exception as e:
        print(f"Error connecting to database: {e}")
        return None
 
#Function to fetch the data from the database
def fetch_text_data(conn):
    if conn is None:
        return pd.DataFrame()  # Return an empty DataFrame if connection failed
    query = 'SELECT id, title, text FROM content'
    df = pd.read_sql_query(query, conn)
    return df

#Function to clean text
def clean_text(text):
    text = text.lower()  # Convert to lowercase
    text = re.sub(r'http\S+|www.\S+', '', text)  # Remove URLs
    text = re.sub(r'\d+', '', text)  # Remove numbers
    text = re.sub(r'[^\w\sáéíóúüñ]', '', text)  # Remove punctuation but keep accents
    text = re.sub(r'\s+', ' ', text).strip()  # Remove extra spaces
    return text

#Function to remove stopwords (optimized: stopwords loaded once)
def remove_stopwords(text):
    return ' '.join([word for word in text.split() if word not in stop_words])

#Function for lemmatization
#def lemmatize_text(text):
#   doc = nlp(text)
#  return " ".join([token.lemma_ for token in doc if token.is_alpha and token.pos_ in ["NOUN", "ADJ", "VERB"]])

#Function to add clean columns if they don’t exist
def add_clean_columns(conn):
    cursor = conn.cursor()
    try:
        cursor.execute('ALTER TABLE content ADD COLUMN clean_text TEXT')
        cursor.execute('ALTER TABLE content ADD COLUMN clean_title TEXT')
        conn.commit()
        print("Columns added successfully.")
    except sql.OperationalError:
        print("Columns already exist.")

#Function to update database efficiently
def update_clean_columns(conn, df):
    try:
        add_clean_columns(conn)

        cleaned_data = []
        for _, row in tqdm.tqdm(df.iterrows(), total=len(df), desc="Processing articles"):
            clean_text_value = clean_text(row['text'])
            clean_title_value = clean_text(row['title'])
            clean_text_value = remove_stopwords(clean_text_value)
            clean_title_value = remove_stopwords(clean_title_value)
            #clean_text_value = lemmatize_text(clean_text_value)
            #clean_title_value = lemmatize_text(clean_title_value)
            
            cleaned_data.append((clean_text_value, clean_title_value, row['id']))

        # Bulk update for efficiency
        cursor = conn.cursor()
        cursor.executemany('''
        UPDATE content 
        SET clean_text = ?, clean_title = ? 
        WHERE id = ?
        ''', cleaned_data)

        conn.commit()
        print("Successfully updated clean columns in the database.")

    except Exception as e:
        print(f"Error updating clean columns: {e}")

#Main function
def main(db_path):
    conn = connect_to_db(db_path)
    if conn:
        df = fetch_text_data(conn)
        if not df.empty:
            print('Data fetched from the database.')
            pd.set_option('display.max_colwidth', None)
            update_clean_columns(conn, df)
        else:
            print("No data fetched from the database.")
    else:
        print("Failed to connect to the database.")

if __name__ == "__main__":
    db_path = 'Code/data/processed/articles.db'
    main(db_path)
