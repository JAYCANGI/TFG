import pandas as pd

# Load the CSV
csv_file = "data/all_links.csv"
df = pd.read_csv(csv_file)

# Check for missing data
missing = df[df.isnull().any(axis=1)]
if not missing.empty:
    print(f"Missing data found:\n{missing}")
else:
    print("No missing data.")

# Check for duplicate links
duplicates = df[df.duplicated(subset=["Link"])]
if not duplicates.empty:
    print(f"Duplicate links found:\n{duplicates}")
else:
    print("No duplicate links.")

# Print basic stats
print(f"Total links: {len(df)}")
print(f"Unique links: {df['Link'].nunique()}")



df_content = pd.read_csv("data/processed/all_content.csv", sep=";")
print(df_content.info())
print(df_content.head())
# Check for duplicate links
duplicates_content = df_content[df_content.duplicated(subset=["URL"])]
if not duplicates_content.empty:
    print(f"Duplicate links found:\n{duplicates_content}")
else:
    print("No duplicate links.")

# Print basic stats
print(f"Total links: {len(df_content)}")
print(f"Unique links: {df_content['URL'].nunique()}")
