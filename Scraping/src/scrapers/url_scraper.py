import json
import requests
from bs4 import BeautifulSoup
import os
import csv
from urllib.parse import quote
import time
from tqdm import tqdm

# Load site configuration from JSON file
with open('Scraping\src\scrapers\config\sites_config.json', 'r') as f:
    sites_config = json.load(f)

# Function to scrape article links using requests and BeautifulSoup
def scrape_article_links(site_name, site_config, max_pages=30):
    """
    Scrapes article links for a site using requests and BeautifulSoup.
    """

    # Handle URL-based pagination
    base_url = site_config['base_url']
    pagination_pattern = site_config.get('pagination_pattern', None)
    article_selector = site_config.get('article_link_selector', 'a')  # Default selector
    start_page = site_config.get('start_page', 1)
    end_page = site_config.get('end_page', max_pages)

    all_links = []
    for page in tqdm(range(start_page, end_page + 1)):
        try:
            # Build the URL for each page
            if pagination_pattern:
                url = f"{base_url}{pagination_pattern.format(page_number=page)}"
            else:
                url = base_url

            # Encode the URL to handle special characters
            url = quote(url, safe=':/?&=')
            
            # Send HTTP request
            headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
            }
            response = requests.get(url, headers=headers)
            if response.status_code != 200:
                print(f"Failed to fetch {url}")
                break

            # Parse the HTML
            soup = BeautifulSoup(response.content, 'html.parser')
            articles = soup.select(article_selector)

            # Extract and store links
            for article in articles:
                link = article.get('href')
                if link and link not in all_links:
                    # Ensure the link is absolute
                    if not link.startswith('http'):
                        link = base_url + link
                    all_links.append(link)

        except Exception as e:
            print(f"Error while scraping {url}: {e}")

    return all_links


# Function to save all scraped links to a single CSV
def save_links_to_csv(all_links, output_file="data/all_links.csv"):
    """
    Save all scraped links to a single CSV file.
    Each row contains an ID, newspaper name, and the link.
    The ID is sequential across all newspapers.
    """
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    with open(output_file, "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(["ID", "Newspaper", "Link"])  # Write header
        
        # Sequential ID across all newspapers
        unique_id = 1
        for newspaper, links in all_links.items():
            for link in links:
                writer.writerow([unique_id, newspaper, link])
                unique_id += 1  # Increment the global ID counter
    
    print(f"Saved all links to {output_file}")


# Function to scrape all sites
def scrape_all_sites(config_file, output_dir="data/raw"):
    """
    Scrapes all sites listed in the config file.
    """
    # Load the unified configuration
    with open(config_file, 'r') as file:
        config = json.load(file)

    all_links = {}

    for site_name, site_config in config.items():
        tqdm.write(f"Starting scrape for: {site_name}")
        
        try:
            # Scrape the site
            links = scrape_article_links(site_name, site_config)
            all_links[site_name] = links

            # Save individual site results (optional)
            os.makedirs(output_dir, exist_ok=True)
            output_file = os.path.join(output_dir, f"{site_name}_links.json")
            with open(output_file, 'w') as file:
                json.dump(links, file, indent=4)

            print(f"Extracted {len(links)} links for {site_name}. Saved to {output_file}")

        except Exception as e:
            print(f"Error while scraping {site_name}: {e}")

    # Save all links to a single CSV file
    save_links_to_csv(all_links, output_file="data/all_links.csv")


# Main execution
if __name__ == "__main__":
    # Dynamically determine the path to the configuration file
    config_path ="Scraping\src\scrapers\config\sites_config.json"
    scrape_all_sites(config_path)

