import os
import pandas as pd
from newspaper import Article
from tqdm import tqdm

def scrape_content(csv_file, output_dir="data/processed"):
    """
    Scrapes article content (title, text, etc.) from the URLs in the given CSV and saves it to a CSV.
    """
    # Load the CSV with all links
    df = pd.read_csv(csv_file)


    # Prepare the output directory
    os.makedirs(output_dir, exist_ok=True)

    all_content = []
    errors = []

    # Iterate through each row with a progress bar
    for _, row in tqdm(df.iterrows(), total=len(df), desc="Scraping content"):
        newspaper = row['Newspaper']
        url = row['Link']

        try:
            # Use newspaper3k to scrape the article
            article = Article(url,language="es")
            article.download()
            article.parse()

            # Store extracted details
            content = {
                "ID": row['ID'],
                "Newspaper": newspaper,
                "URL": url,
                "Title": article.title,
                "Date": article.publish_date.isoformat() if article.publish_date else None,  # Convert to ISO format
                "Text": article.text
            }
            all_content.append(content)

        except Exception as e:
            # Log errors for failed URLs
            errors.append({"URL": url, "Error": str(e)})
            print(f"Failed to scrape {url}: {e}")

    # Save the extracted content to a CSV file
    output_file = os.path.join(output_dir, "all_content.csv")
    content_df = pd.DataFrame(all_content)
    content_df.to_csv(output_file, index=False, encoding="utf-8-sig", sep=";")
    print(f"Saved content to {output_file}")

    # Save errors to a separate CSV file
    if errors:
        error_file = os.path.join(output_dir, "errors.csv")
        error_df = pd.DataFrame(errors)
        error_df.to_csv(error_file, index=False, encoding="utf-8-sig")
        print(f"Logged {len(errors)} errors to {error_file}")

# Example usage
if __name__ == "__main__":
    scrape_content("data/all_links.csv")
