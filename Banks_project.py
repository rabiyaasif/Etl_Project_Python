import logging
import requests
from bs4 import BeautifulSoup
import pandas as pd
import sqlite3

# Set up logging
def setup_logging(log_file='etl_process.log'):
    logging.basicConfig(filename=log_file, 
                        level=logging.INFO, 
                        format='%(asctime)s:%(levelname)s:%(message)s')

def log_message(message):
    logging.info(message)
    print(message)  # Optional: print the message to the console

# Extract data
def extract_data(url):
    log_message("Starting the extraction process")
    
    try:
        response = requests.get(url)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Example: Extracting data from a table
        table = soup.find('table')
        headers = [header.text.strip() for header in table.find_all('th')]
        rows = []
        for row in table.find_all('tr')[1:]:
            cols = [col.text.strip() for col in row.find_all('td')]
            rows.append(cols)
        
        df = pd.DataFrame(rows, columns=headers)
        
        log_message("Data extraction completed successfully")
        return df

    except requests.exceptions.RequestException as e:
        log_message(f"Error during requests to {url}: {str(e)}")
    except Exception as e:
        log_message(f"Error during the extraction process: {str(e)}")

# Transform data
def transform_data(df, csv_path):
    log_message("Starting data transformation")
    
    # Load exchange rates
    exchange_rates = pd.read_csv(csv_path)
    
    # Example transformations
    # Convert numerical columns to appropriate data types
    df['Market cap(US$ billion)'] = df['Market cap(US$ billion)'].str.replace(',', '').astype(float)
    
    # Add transformed columns
    df['Market cap(EUR billion)'] = df['Market cap(US$ billion)'] * exchange_rates['EUR'][0]
    df['Market cap(GBP billion)'] = df['Market cap(US$ billion)'] * exchange_rates['GBP'][0]
    df['Market cap(INR billion)'] = df['Market cap(US$ billion)'] * exchange_rates['INR'][0]
    
    log_message("Data transformation completed")
    return df

# Load data to CSV
def load_to_csv(df, filename):
    log_message(f"Saving data to {filename}")
    
    df.to_csv(filename, index=False)
    log_message(f"Data saved to {filename}")

# Example usage
setup_logging()
# Replace with your actual URL
url = 'https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks'
df = extract_data(url)

# Replace with your actual CSV file path for exchange rates
#csv_path = 'C:\Users\CSC\Desktop\exchange_rates.csv'
csv_path = 'C:\\Users\\CSC\\Desktop\\exchange_rates.csv'

if df is not None:
    df_transformed = transform_data(df, csv_path)
    
# Replace with your actual output CSV file path
    csv_filename = 'C:\\Users\\CSC\\Desktop\\large_banks.csv'
    load_to_csv(df_transformed, csv_filename)

def load_to_db(df, db_name, table_name):
    log_message(f"Loading data to database: {db_name}, table: {table_name}")
    
    conn = sqlite3.connect(db_name)
    df.to_sql(table_name, conn, if_exists='replace', index=False)
    conn.close()
    
    log_message("Data loaded to database")

db_name = 'largest_banks.db'
table_name = 'banks'
load_to_db(df, db_name, table_name)

def run_query(db_name, query):
    log_message(f"Running query: {query}")
    
    conn = sqlite3.connect(db_name)
    result = pd.read_sql_query(query, conn)
    conn.close()
    
    log_message("Query executed successfully")
    return result


def list_tables(db_name):
    conn = sqlite3.connect(db_name)
    query = "SELECT name FROM sqlite_master WHERE type='table';"
    tables = pd.read_sql_query(query, conn)
    conn.close()
    return tables

# Example usage
if __name__ == "__main__":
    setup_logging()
    
    db_name = 'C:/Users/CSC/Desktop/largest_banks.db'
    
    # List all tables in the database to verify the table name
    tables = list_tables(db_name)
    print("Tables in database:")
    print(tables)
    
    # Assuming the correct table name is 'banks'
    table_name = 'Rank'
    
    # Query 1: Select all data from the correct table
    query1 = f"SELECT * FROM {table_name}"
    result1 = run_query(db_name, query1)
    print(result1)
    
