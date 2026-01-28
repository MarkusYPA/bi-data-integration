import psycopg2
import pandas as pd
import os

# Database connection parameters
DB_HOST = "localhost"
DB_NAME = "gold_db"
DB_USER = "gold_user"
DB_PASS = "gold_password"
DB_PORT = "5432"

def execute_query(query_file_path):
    """
    Executes an SQL query from a file and returns the results as a pandas DataFrame.
    """
    conn = None
    df = pd.DataFrame()
    try:
        conn = psycopg2.connect(host=DB_HOST, database=DB_NAME, user=DB_USER, password=DB_PASS, port=DB_PORT)
        print(f"Executing query from {query_file_path}...")
        with open(query_file_path, 'r') as file:
            query = file.read()
        df = pd.read_sql(query, conn)
        print("Query executed successfully.")
    except Exception as e:
        print(f"Error executing query from {query_file_path}: {e}")
    finally:
        if conn:
            conn.close()
    return df

def analyze_sales_per_capita():
    """
    Analyzes sales per capita over time.
    """
    df_sales_per_capita = execute_query('sales_per_capita.sql')
    if not df_sales_per_capita.empty:
        print("\n--- Sales Per Capita Over Time ---")
        print(df_sales_per_capita.head())
        print(f"\nTotal rows: {len(df_sales_per_capita)}")
        print("\nSales per capita analysis complete. You can further analyze the 'df_sales_per_capita' DataFrame.")
    else:
        print("\nNo data for sales per capita analysis.")

def analyze_sales_and_tourism_correlation():
    """
    Analyzes the correlation between sales and tourism statistics.
    """
    df_sales_tourism = execute_query('sales_and_tourism.sql')
    if not df_sales_tourism.empty:
        print("\n--- Sales and Tourism Data ---")
        print(df_sales_tourism.head())
        print(f"\nTotal rows: {len(df_sales_tourism)}")

        # Calculate correlation
        if 'total_sales' in df_sales_tourism.columns and 'total_visitors' in df_sales_tourism.columns:
            correlation = df_sales_tourism['total_sales'].corr(df_sales_tourism['total_visitors'])
            print(f"\nCorrelation between Total Sales and Total Visitors: {correlation:.4f}")
            print("\nSales and tourism correlation analysis complete. You can further analyze the 'df_sales_tourism' DataFrame.")
        else:
            print("\nCannot calculate correlation: 'total_sales' or 'total_visitors' column not found.")
    else:
        print("\nNo data for sales and tourism correlation analysis.")

if __name__ == "__main__":
    analyze_sales_per_capita()
    analyze_sales_and_tourism_correlation()
