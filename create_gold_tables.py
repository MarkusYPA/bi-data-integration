import os
import psycopg2
from psycopg2 import sql

def get_db_connection():
    """Establishes a connection to the PostgreSQL database."""
    try:
        conn = psycopg2.connect(
            host=os.environ.get("POSTGRES_HOST", "localhost"),
            dbname=os.environ.get("POSTGRES_DB", "gold_db"),
            user=os.environ.get("POSTGRES_USER", "gold_user"),
            password=os.environ.get("POSTGRES_PASSWORD", "gold_password"),
            port=os.environ.get("POSTGRES_PORT", "5432")
        )
        print("Database connection established successfully.")
        return conn
    except psycopg2.OperationalError as e:
        print(f"Could not connect to the database: {e}")
        return None

def execute_sql_from_file(cursor, file_path):
    """Reads and executes a SQL script from a file."""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            sql_query = f.read()
            cursor.execute(sql_query)
            # Extract table name from file path for the success message
            table_name = os.path.basename(file_path).replace('.sql', '')
            print(f"Successfully created table: {table_name}")
    except (IOError, psycopg2.Error) as e:
        print(f"Error executing script {file_path}: {e}")
        raise

def main():
    """
    Main function to create all tables in the gold star schema.
    Tables are created in an order that respects foreign key constraints.
    """
    conn = get_db_connection()
    if not conn:
        return

    # Order is important: dimensions first, then facts.
    sql_files = [
        # Dimensions
        "dim_municipality.sql",
        "dim_date.sql",
        "dim_product.sql",
        "dim_store.sql",
        # Facts
        "fact_sales.sql",
        "fact_tourism.sql",
        "fact_demographics.sql"
    ]

    try:
        with conn.cursor() as cursor:
            print("Starting to create gold tables...")
            for file_name in sql_files:
                file_path = os.path.join("gold", file_name)
                execute_sql_from_file(cursor, file_path)
            
            conn.commit()
            print("\nAll tables created successfully and transaction committed.")

    except Exception as e:
        print(f"\nAn error occurred: {e}. Rolling back transaction.")
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()
            print("Database connection closed.")

if __name__ == '__main__':
    main()
