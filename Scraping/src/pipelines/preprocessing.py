import sqlite3 as sql
import pandas as pd
from transformers import BertModel,BertTokenizer
import torch
import matplotlib.pyplot as plt
from wordcloud import WordCloud
import numpy as np
import tqdm as tqdm

# Initialize the BERT tokenizer
tokenizer = BertTokenizer.from_pretrained('bert-base-multilingual-cased')

# Connection with the database
def connect_to_db(db_path):
    conn = sql.connect(db_path)
    return conn

# Fetch the data from the database
def fetch_text_data(conn):
    query = 'SELECT id,text FROM content'
    df = pd.read_sql_query(query, conn)
    return df

#Update the databse with tokens column
def add_tokens_column(conn):
    cursor=conn.cursor()
    cursor.execute('ALTER TABLE content ADD COLUMN tokens TEXT')
    conn.commit()

def update_tokens_column(conn,df):
    try:
        try:
            add_tokens_column(conn)
        except sql.OperationalError:
            print('Column already exists')

        # Process each text and update database
        for index, row in tqdm(df.iterrows(), total=len(df), desc="Tokenizing texts"):
            # Truncate and tokenize text
            truncated_text = truncate_text(row['text'])
            tokens = tokenizer.tokenize(truncated_text)
            tokens_str = ','.join(tokens)  # Convert tokens list to string for storage
            
            # Update database
            cursor = conn.cursor()
            cursor.execute('''
                UPDATE content 
                SET tokens = ? 
                WHERE id = ?
            ''', (tokens_str, row['id']))
            
        conn.commit()
        print("Successfully stored tokens in database")
        
    except Exception as e:
        print(f"Error storing tokens: {e}")
        

# Function to truncate texts to a maximum token length
def truncate_text(text, max_length=510):  # 510 to leave space for [CLS] and [SEP]
    # Tokenize the text
    tokens = tokenizer.tokenize(text)
    
    # Truncate to max_length tokens if needed
    truncated_tokens = tokens[:max_length]
    
    # Calculate padding length
    padding_length = max_length - len(truncated_tokens)
    
    # Add padding tokens if needed
    if padding_length > 0:
        padding_tokens = ['[PAD]'] * padding_length
        truncated_tokens = truncated_tokens + padding_tokens
    
    # Add [CLS] and [SEP] tokens
    truncated_tokens = ['[CLS]'] + truncated_tokens + ['[SEP]']
    
    # Convert back to string
    truncated_text = tokenizer.convert_tokens_to_string(truncated_tokens)
    return truncated_text

# Process the text data for BERT and get token lengths with truncation
def process_text_with_truncation(df):
    texts = df['text'].tolist()
    tokenized_texts = []
    token_lengths = []

    for text in texts:
        # Truncate the text
        truncated_text = truncate_text(text)
        
        # Tokenize the truncated text using BERT's tokenizer
        encoded_input = tokenizer.encode_plus(
            truncated_text,
            add_special_tokens=True,  # Add [CLS] and [SEP]
            max_length=512,           # Maximum length allowed by BERT
            truncation=True,          # Truncate if longer than max_length
            padding='max_length',     # Pad to max_length
            return_tensors='pt'       # Return PyTorch tensors
        )
        
        tokenized_texts.append(encoded_input)
        token_lengths.append(len(tokenizer.tokenize(truncated_text)))
        print(f'For text: {text[:50]} token length is: {len(tokenizer.tokenize(truncated_text))}')
    print(tokenized_texts[0]['input_ids'])
    return token_lengths

# Visualize token lengths distribution with min, max, avg lines
def plot_token_length_distribution(token_lengths):
    plt.figure(figsize=(20, 10))
    
    # Plotting the actual token lengths
    plt.plot(token_lengths, label='Token Lengths', color='skyblue')
    
    # Calculate min, max, avg lengths
    min_length = min(token_lengths) if token_lengths else 0
    max_length = max(token_lengths) if token_lengths else 0
    avg_length = np.mean(token_lengths) if token_lengths else 0

    # Count how many texts exceed the 512-token limit
    num_exceeding_512 = sum(1 for length in token_lengths if length > 512)

    # Plotting the min, max, avg lines
    plt.axhline(y=min_length, color='red', linestyle='--', label=f'Min Length ({min_length})')
    plt.axhline(y=max_length, color='green', linestyle='--', label=f'Max Length ({max_length})')
    plt.axhline(y=avg_length, color='orange', linestyle='--', label=f'Avg Length ({avg_length:.2f})')
    plt.axhline(y=512, color='purple', linestyle='--', label=f'Limit (512 tokens)')
    
    # Label in lines
    plt.text(0, min_length + 1, f'Min: {min_length}', fontsize=10, color='red')
    plt.text(0, max_length + 1, f'Max: {max_length}', fontsize=10, color='green')
    plt.text(0, avg_length + 1, f'Avg: {avg_length:.2f}', fontsize=10, color='orange')
    plt.text(0, 512 + 1, f'Limit: 512', fontsize=10, color='purple')

    # Graph settings
    plt.title(f'Distribution of Token Lengths (Num Exceeding 512: {num_exceeding_512})')
    plt.xlabel('Text Index')
    plt.ylabel('Token Length')
    plt.legend()
    plt.grid(True)
    plt.show()

# Generate a word cloud from the text data
def generate_word_cloud(texts):
    all_tokens = []
    for text in texts:
        # Truncate text before tokenization to ensure it doesn't exceed 512 tokens
        truncated_text = truncate_text(text)
        tokens = tokenizer.tokenize(truncated_text)
        all_tokens.extend(tokens)

    # Join tokens into a single string
    text_string = ' '.join(all_tokens)

    # Create and display the word cloud
    wordcloud = WordCloud(width=800, height=400, background_color='white').generate(text_string)
    plt.figure(figsize=(10, 6))
    plt.imshow(wordcloud, interpolation='bilinear')
    plt.axis('off')
    plt.title('Word Cloud of Tokens')
    plt.show()

# Main function  
def main(db_path):
    conn = connect_to_db(db_path)
    df = fetch_text_data(conn)
    token_lengths = process_text_with_truncation(df)
    # Store tokens in database
    update_tokens_column(df, conn)
    
    # Generate visualizations
    token_lengths = process_text_with_truncation(df.head(10))
    plot_token_length_distribution(token_lengths)
    generate_word_cloud(df['text'])
    
    conn.close()

if __name__ == '__main__':
    db_path = 'data/processed/articles.db'
    main(db_path)