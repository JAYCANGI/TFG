import sqlite3
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches

class DatabaseOutlierHandler:
    def __init__(self, db_path, content_table, links_table):
        """
        Initialize the DatabaseOutlierHandler with database connection details.
        
        Args:
            db_path (str): Path to the SQLite database.
            content_table (str): Name of the content table.
            links_table (str): Name of the links table.
        """
        self.db_path = db_path
        self.content_table = content_table
        self.links_table = links_table

    def _execute_query(self, query, params=None, fetch=False):
        try:
            with sqlite3.connect(self.db_path) as conn:
                if fetch:
                    return pd.read_sql_query(query, conn)
                
                cursor = conn.cursor()
                if params:
                    cursor.execute(query, params)
                else:
                    cursor.execute(query)
                conn.commit()
        except sqlite3.Error as e:
            print(f"Database error: {e}")
            return None

    def fetch_data(self):
        """
        Fetch data from content and links tables.
        
        Returns:
            pd.DataFrame: DataFrame with joined data.
        """
        query = f"""
            SELECT content.id, content.text,content.publish_date, links.newspaper
            FROM {self.content_table}
            JOIN {self.links_table} ON {self.content_table}.id = {self.links_table}.id
        """
        return self._execute_query(query, fetch=True)

    def format_time(self, df, column="publish_date"):
        df[column] = pd.to_datetime(df[column], errors="coerce",format='ISO8601')
        print('DATE ADJUSTED')
        print(df)
        return df
    
    def detect_outliers(self, df, column='word_count', threshold=1.5):
        """
        Detect outliers using Interquartile Range (IQR) method.
        
        Args:
            df (pd.DataFrame): Input DataFrame.
            column (str): Column to check for outliers.
            threshold (float): IQR multiplier for outlier detection.
        
        Returns:
            pd.DataFrame: DataFrame with outlier information.
        """
        # Print initial article counts
        print('NUMBER OF ARTICLES PER NEWSPAPER:')
        print(df.groupby('newspaper')[column].count())
        print("\n******\n")

        # Calculate IQR and bounds
        q1 = df[column].quantile(0.25)
        q3 = df[column].quantile(0.75)
        iqr = q3 - q1
        lower_bound = q1 - threshold * iqr
        upper_bound = q3 + threshold * iqr

        # Mark outliers
        df["outlier"] = ~df[column].between(lower_bound, upper_bound)

        # Print article counts after outlier removal
        print('NUMBER OF ARTICLES PER NEWSPAPER AFTER ELIMINATING OUTLIERS:')
        print(df[~df["outlier"]].groupby('newspaper')[column].count())
        
        return df

    def remove_outliers(self, df):
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            
            # Check if the column already exists
            cursor.execute(f"PRAGMA table_info({self.content_table})")
            columns = [info[1] for info in cursor.fetchall()]
            
            if "outlier" not in columns:
                # Add the outlier column if it doesn't exist
                cursor.execute(f"ALTER TABLE {self.content_table} ADD COLUMN outlier INTEGER")
            
            # Update the outlier column based on the DataFrame
            update_query = f"UPDATE {self.content_table} SET outlier = ? WHERE id = ?"
            update_data = df[["outlier", "id"]].values.tolist()
            cursor.executemany(update_query, update_data)
            
            # Delete rows where outlier is 1
            delete_content_query = f"DELETE FROM {self.content_table} WHERE outlier = 1"
            delete_links_query = f"DELETE FROM {self.links_table} WHERE id IN (SELECT id FROM {self.content_table} WHERE outlier = 1)"
            
            # Execute delete queries
            cursor.execute(delete_content_query)
            cursor.execute(delete_links_query)
            
            # Get number of rows deleted
            rows_deleted = cursor.rowcount
            
            conn.commit()
        
            print(f"Outliers removed from the '{self.content_table}' and '{self.links_table}' tables.")
            return rows_deleted
        

    def visualize_outliers(self, df, column='word_count'):
    
        
        plt.figure(figsize=(14, 10))
        
        # Create boxplot with hue to avoid deprecation warning
        sns.boxplot(
            data=df,
            x=column,
            y="newspaper",
            hue="newspaper",  # Add this line
            palette="pastel",
            legend=False,  # Add this line
            showmeans=True,
            meanprops={
                "marker": "o", 
                "markerfacecolor": "red", 
                "markeredgecolor": "red", 
                "markersize": 10,
            },
            flierprops={
                "marker": "o", 
                "markerfacecolor": "grey", 
                "markeredgecolor": "black", 
                "markersize": 6,
            }
        )

        # Highlight individual outliers
        for newspaper in df["newspaper"].unique():
            subset = df[df["newspaper"] == newspaper]
            
            # Get outliers based on the 'outlier' column
            outliers = subset[subset["outlier"]]
            
            for _, outlier in outliers.iterrows():
                plt.scatter(
                    outlier[column],
                    newspaper,
                    color="red",
                    zorder=5,
                    s=100,  # Slightly larger marker
                    alpha=0.7,
                    edgecolors="black"
                )

        # Create custom legend
        mean_patch = mpatches.Patch(
            color='red', 
            label='Mean Value', 
            alpha=0.7
        )
        outlier_patch = mpatches.Patch(
            facecolor='red',  # Use facecolor instead of color
            label='Outliers', 
            alpha=0.7, 
            edgecolor='black'
        )
        plt.legend(
            handles=[mean_patch, outlier_patch], 
            title="Legend", 
            loc="best"
        )
        
        plt.title(f"Boxplot con Outliers para {column} por Newspaper")
        plt.xlabel(column.replace('_', ' ').title())
        plt.ylabel("Newspaper")
        plt.grid(axis="x", linestyle="--", alpha=0.7)
        plt.tight_layout()
        
        # Save the plot instead of showing it
        plt.savefig('Scraping/src/visualizations/After_Cleaning/outliers_boxplot.png')
        
    #visualize boxplot after delete
    def boxplot_after(self, df, column='word_count'):
            """
            Create a boxplot visualization without outliers.
            
            Args:
                df (pd.DataFrame): Input DataFrame.
                column (str): Column to visualize.
            """
            plt.figure(figsize=(14, 10))
            
            sns.boxplot(
                data=df,
                x=column,
                y="newspaper",
                palette="pastel",
                showmeans=True,
                meanprops={
                    "marker": "o", 
                    "markerfacecolor": "red", 
                    "markeredgecolor": "red", 
                    "markersize": 10,
                },
                flierprops={
                    "marker": "o", 
                    "markerfacecolor": "grey", 
                    "markeredgecolor": "black", 
                    "markersize": 6,
                },
                showfliers=False  # Exclude outliers
            )
            
            plt.title(f"Boxplot sin Outliers para {column} por Newspaper")
            plt.xlabel(column.replace('_', ' ').title())
            plt.ylabel("Newspaper")
            plt.grid(axis="x", linestyle="--", alpha=0.7)
            plt.tight_layout()
            plt.savefig('Scraping/src/visualizations/After_Cleaning/outliers_boxplot_after_delete.png')

def main(db_path, content_table, links_table, remove_outliers=True):
    # Initialize handler
    handler = DatabaseOutlierHandler(db_path, content_table, links_table)

    # Fetch data
    df = handler.fetch_data()
    
    # Format time column
    df = handler.format_time(df, column="publish_date")

    # Add derived metrics
    df["word_count"] = df["text"].apply(lambda x: len(x.split()))
    df["char_count"] = df["text"].apply(len)

    # Detect outliers
    df_with_outliers = handler.detect_outliers(df, column="word_count")

    # Optional: Print outliers
    print("\nOutliers detected:")
    outliers = df_with_outliers[df_with_outliers["outlier"]]
    print(outliers[["id", "text", "word_count", "char_count"]])
    print(f"Total outliers: {len(outliers)}")
    
    # Visualize outliers
    handler.boxplot_after(df_with_outliers)
    
    # Remove outliers from database if specified
    if remove_outliers:
        removed_count = handler.remove_outliers(df_with_outliers)
        print(f"\nRemoved {removed_count} outliers from the database.")
    
    #Visualize outliers after delete
    handler.visualize_outliers(df_with_outliers,column='word_count')

if __name__ == "__main__":
    db_path = "data/processed/articles.db"
    
    # Example usage with and without outlier removal
    main(db_path, content_table="content", links_table="links", remove_outliers=True)
   