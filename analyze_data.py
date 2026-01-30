import pandas as pd
import os
from sqlalchemy import create_engine
from scipy.stats import pearsonr

# Database connection parameters
DB_HOST = "localhost"
DB_NAME = "gold_db"
DB_USER = "gold_user"
DB_PASS = "gold_password"
DB_PORT = "5432"

# Create a SQLAlchemy engine
DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(DATABASE_URL)

head_rows = 25

def execute_query(query_file_path):
    """
    Executes an SQL query from a file and returns the results as a pandas DataFrame.
    """
    df = pd.DataFrame()
    try:
        print(f"Executing query from {query_file_path}...")
        with open(query_file_path, 'r') as file:
            query = file.read()
        df = pd.read_sql(query, engine)
        print("Query executed successfully.")
    except Exception as e:
        print(f"Error executing query from {query_file_path}: {e}")
    return df

def analyze_sales_per_capita():
    """
    Q1: Sales per capita over time.
    """
    df_sales_per_capita = execute_query('analysis_queries/q1_sales_per_capita.sql')
    if not df_sales_per_capita.empty:
        print("\n--- Q1: Sales Per Capita Over Time ---")
        print(df_sales_per_capita.head(head_rows))
        print(f"\nTotal rows: {len(df_sales_per_capita)}")
        print("\nSales per capita analysis complete. You can further analyze the 'df_sales_per_capita' DataFrame.")
    else:
        print("\nNo data for sales per capita analysis.")

def analyze_sales_and_tourism_correlation():
    """
    Q2: Correlation between sales and tourism statistics.
    """
    df_sales_tourism = execute_query('analysis_queries/q2_sales_and_tourism.sql')
    if not df_sales_tourism.empty:
        print("\n--- Q2: Sales and Tourism Data ---")
        print(df_sales_tourism.head(head_rows))
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

def analyze_municipality_sales_tourism():
    """
    Q3: Municipalities with highest sales per capita and tourism relation.
    """
    df = execute_query('analysis_queries/q3_municipality_sales_tourism.sql')
    if not df.empty:
        print("\n--- Q3: Municipalities - Sales Per Capita & Tourism ---")
        print(df.head(head_rows))
        
        # Simple correlation check
        if 'sales_per_capita' in df.columns and 'total_visitors' in df.columns:
            corr_visitors = df['sales_per_capita'].corr(df['total_visitors'])
            corr_revenue = df['sales_per_capita'].corr(df['total_tourism_revenue'])
            print(f"\nCorrelation (Sales Per Capita vs. Total Visitors): {corr_visitors:.4f}")
            print(f"Correlation (Sales Per Capita vs. Total Tourism Revenue): {corr_revenue:.4f}")
    else:
        print("\nNo data for Q3 analysis.")

def analyze_seasonality():
    """
    Q4: Seasonal patterns in tourism and grocery sales.
    """
    df = execute_query('analysis_queries/q4_seasonality.sql')
    if not df.empty:
        print("\n--- Q4: Seasonal Patterns (Sales & Tourism) ---")
        print(df)
    else:
        print("\nNo data for Q4 analysis.")

def analyze_product_category_location():
    """
    Q5: Product category performance across municipalities.
    """
    df = execute_query('analysis_queries/q5_product_category_location.sql')
    if not df.empty:
        print("\n--- Q5: Product Category Performance by Municipality ---")
        print(df.head(head_rows))
    else:
        print("\nNo data for Q5 analysis.")

def analyze_store_performance():
    """
    Q6: Top performing stores.
    """
    df = execute_query('analysis_queries/q6_store_performance.sql')
    if not df.empty:
        print("\n--- Q6: Top Performing Stores ---")
        print(df.head(head_rows))
    else:
        print("\nNo data for Q6 analysis.")

def analyze_tourism_trends():
    """
    Q7: Tourism revenue change over time by municipality.
    """
    df = execute_query('analysis_queries/q7_tourism_trends.sql')
    if not df.empty:
        print("\n--- Q7: Tourism Revenue Trends by Municipality ---")
        print(df.head(head_rows))
    else:
        print("\nNo data for Q7 analysis.")

def analyze_population_sales():
    """
    Q8: Relationship between population size and total grocery sales.
    """
    df = execute_query('analysis_queries/q8_population_vs_sales.sql')
    if not df.empty:
        print("\n--- Q8: Population vs. Total Grocery Sales ---")
        print(df)
        if 'avg_population' in df.columns and 'total_sales' in df.columns:
            correlation = df['avg_population'].corr(df['total_sales'])
            print(f"\nCorrelation (Population vs. Total Sales): {correlation:.4f}")
    else:
        print("\nNo data for Q8 analysis.")

def analyze_weekday_weekend():
    """
    Q9: Sales patterns between weekdays and weekends.
    """
    df = execute_query('analysis_queries/q9_weekday_weekend_sales.sql')
    if not df.empty:
        print("\n--- Q9: Weekday vs. Weekend Sales ---")
        print(df)
    else:
        print("\nNo data for Q9 analysis.")

def analyze_category_seasonal_tourism():
    """
    Q10: Product category sales correlate with tourism seasons.
    """
    df = execute_query('analysis_queries/q10_category_seasonal_tourism.sql')
    if not df.empty:
        print("\n--- Q10: Product Category Sales vs. Tourism Seasonality ---")
        # To see correlation per category, we can group by category
        categories = df['category'].unique()
        print("\nCorrelation between Monthly Category Sales and Total Tourism Visitors:")
        for cat in categories:
            cat_df = df[df['category'] == cat]
            if len(cat_df) > 1: # Need at least 2 points for correlation
                corr, p_value = pearsonr(cat_df['category_sales'], cat_df['total_visitors'])
                print(f"  {cat}: Correlation={corr:.4f}, p-value={p_value:.4f}")
                if p_value < 0.05:
                    print(f"    -> Significant at p < 0.05")
            else:
                print(f"  {cat}: Not enough data")
    else:
        print("\nNo data for Q10 analysis.")

if __name__ == "__main__":
    analyze_sales_per_capita()
    analyze_sales_and_tourism_correlation()
    analyze_municipality_sales_tourism()
    analyze_seasonality()
    analyze_product_category_location()
    analyze_store_performance()
    analyze_tourism_trends()
    analyze_population_sales()
    analyze_weekday_weekend()
    analyze_category_seasonal_tourism()
