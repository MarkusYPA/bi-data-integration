
import os
import pandas as pd
from sqlalchemy import create_engine, text
from datetime import date

# --- 1. CONFIGURATION & DATABASE CONNECTION ---

# Database connection details from environment variables or defaults
DB_USER = os.environ.get("POSTGRES_USER", "gold_user")
DB_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "gold_password")
DB_HOST = os.environ.get("POSTGRES_HOST", "localhost")
DB_PORT = os.environ.get("POSTGRES_PORT", "5432")
DB_NAME = os.environ.get("POSTGRES_DB", "gold_db")

SILVER_PATH = 'silver'


def get_db_engine():
    """Creates and returns a SQLAlchemy engine."""
    url = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    try:
        engine = create_engine(url)
        # Test connection
        with engine.connect() as conn:
            print("Database connection established successfully.")
        return engine
    except Exception as e:
        print(f"Failed to connect to the database: {e}")
        return None


def clear_tables(engine):
    """Clears all gold tables in the correct order before loading."""
    table_names = [
        "fact_sales", "fact_tourism", "fact_demographics",
        "dim_store", "dim_product", "dim_municipality", "dim_date"
    ]
    with engine.connect() as conn:
        with conn.begin():  # Start a transaction
            for table_name in table_names:
                print(f"Clearing table: {table_name}")
                conn.execute(
                    text(f'TRUNCATE TABLE {table_name} RESTART IDENTITY CASCADE;'))
        print("All gold tables cleared.")

# --- 2. DIMENSION POPULATION ---


def populate_dim_date(engine, start_date_str='2000-01-01', end_date_str='2030-12-31'):
    """
    Populates the date dimension with a complete range of dates.
    """
    print("Populating dimension: dim_date")
    df = pd.DataFrame({"date": pd.date_range(start_date_str, end_date_str)})
    df["date_key"] = df.date.dt.strftime("%Y%m%d").astype(int)
    df["day_of_week"] = df.date.dt.dayofweek + 1  # Monday=1, Sunday=7
    df["day_of_month"] = df.date.dt.day
    df["day_of_year"] = df.date.dt.dayofyear
    df["month_of_year"] = df.date.dt.month
    df["quarter_of_year"] = df.date.dt.quarter
    df["year"] = df.date.dt.year
    df["month_name"] = df.date.dt.month_name()
    df["day_name"] = df.date.dt.day_name()
    df["is_weekend"] = df.date.dt.dayofweek >= 5

    df.to_sql('dim_date', engine, if_exists='append', index=False)
    print("dim_date populated.")


def populate_dim_municipality(engine):
    """
    Unifies and populates municipality data from multiple silver sources.
    """
    print("Populating dimension: dim_municipality")

    # 1. Read from demographics.csv
    demo_df = pd.read_csv(os.path.join(
        SILVER_PATH, 'demographics.csv'), encoding='utf-8')

    # Extract municipality names from column headers (e.g., "Brändö Kvinnor")
    muni_cols = [col for col in demo_df.columns if 'Kvinnor' in col]
    muni_names = {col.split(' ')[0] for col in muni_cols}
    demo_munis_df = pd.DataFrame({'name': list(muni_names)})

    # 2. Read from stores.csv
    stores_df = pd.read_csv(os.path.join(SILVER_PATH, 'stores.csv'), encoding='utf-8')[
        ["municipality_name", "municipality_code"]].rename(columns={"municipality_name": "name"})

    # 3. Read from tourism.csv
    tourism_df = pd.read_csv(os.path.join(SILVER_PATH, 'tourism.csv'), encoding='utf-8')[
        ["municipality_name", "municipality_code"]].rename(columns={"municipality_name": "name"})

    # Combine all discovered municipalities
    all_munis = pd.concat(
        [demo_munis_df, stores_df, tourism_df],
        ignore_index=True
    ).drop_duplicates(subset=["name"])

    # Exclude the aggregate 'Åland' row
    all_munis = all_munis[all_munis['name'].str.lower() != 'åland']

    # Prepare for final load
    dim_df = all_munis[['name', 'municipality_code']].reset_index(drop=True)

    dim_df.to_sql('dim_municipality', engine, if_exists='append', index=False)
    print("dim_municipality populated.")


def populate_dim_product(engine):
    """Populates the product dimension from products.csv."""
    print("Populating dimension: dim_product")
    df = pd.read_csv(os.path.join(
        SILVER_PATH, 'products.csv'), encoding='utf-8')

    # Rename CSV columns to match database schema
    df = df.rename(columns={
        'product_name': 'name',
        'product_category': 'category'
    })

    dim_df = df[['product_id', 'name', 'category',
                 'unit_price', 'unit_type', 'supplier']]
    dim_df.to_sql('dim_product', engine, if_exists='append', index=False)
    print("dim_product populated.")


def populate_dim_store(engine, municipality_map):
    """Populates the store dimension, mapping municipality names to keys."""
    print("Populating dimension: dim_store")
    df = pd.read_csv(os.path.join(SILVER_PATH, 'stores.csv'), encoding='utf-8')

    # Rename CSV columns to match database schema
    df = df.rename(columns={
        'store_location': 'address',
        'store_name': 'name'
    })

    # Map municipality name to municipality_key
    df['municipality_key'] = df['municipality_name'].map(municipality_map)

    dim_df = df[['store_id', 'name', 'address', 'municipality_key']]
    dim_df.to_sql('dim_store', engine, if_exists='append', index=False)
    print("dim_store populated.")

# --- 3. FACT TABLE POPULATION ---


def populate_fact_sales(engine, date_map, product_map, store_map):
    """Populates the sales fact table, processing in chunks."""
    print("Populating fact table: fact_sales")

    chunk_size = 100000
    sales_file = os.path.join(SILVER_PATH, 'grocery_sales.csv')

    for i, chunk in enumerate(pd.read_csv(sales_file, chunksize=chunk_size, encoding='utf-8')):
        print(f"  Processing chunk {i+1}...")

        # Map business keys to surrogate keys
        chunk['date_key'] = pd.to_datetime(chunk['date']).dt.strftime(
            '%Y%m%d').astype(int).map(date_map)
        chunk['product_key'] = chunk['product_id'].map(product_map)
        chunk['store_key'] = chunk['store_id'].map(store_map)

        # Select and rename columns for the fact table
        fact_df = chunk[['date_key', 'product_key',
                         'store_key', 'sales_amount', 'units_sold']]

        fact_df.to_sql('fact_sales', engine, if_exists='append', index=False)

    print("fact_sales populated.")


def populate_fact_tourism(engine, date_map, municipality_map):
    """Populates the tourism fact table."""
    print("Populating fact table: fact_tourism")
    df = pd.read_csv(os.path.join(
        SILVER_PATH, 'tourism.csv'), encoding='utf-8')

    # Map business keys to surrogate keys
    df['date_key'] = pd.to_datetime(df['date']).dt.strftime(
        '%Y%m%d').astype(int).map(date_map)
    df['municipality_key'] = df['municipality_name'].map(municipality_map)

    # Select columns for the fact table
    fact_df = df[['date_key', 'municipality_key', 'accommodation_type',
                  'origin_country', 'visitor_count', 'revenue']]

    fact_df.to_sql('fact_tourism', engine, if_exists='append', index=False)
    print("fact_tourism populated.")


def populate_fact_demographics(engine, date_map, municipality_map):
    """Populates the demographics fact table by unpivoting the source data."""
    print("Populating fact table: fact_demographics")
    df = pd.read_csv(os.path.join(
        SILVER_PATH, 'demographics.csv'), encoding='utf-8')

    # Unpivot the dataframe from wide to long format
    id_vars = ['år']
    value_vars = [
        col for col in df.columns if 'Kvinnor' in col or 'Män' in col]
    melted_df = df.melt(id_vars=id_vars, value_vars=value_vars,
                        var_name='municipality_gender', value_name='population_count')

    # Split 'municipality_gender' into two separate columns
    split_data = melted_df['municipality_gender'].str.split(' ', expand=True)
    melted_df['municipality_name'] = split_data[0]
    melted_df['gender'] = split_data[1]

    # For yearly data, we map to the first day of the year
    melted_df['date_key'] = pd.to_datetime(
        melted_df['år'], format='%Y').dt.strftime('%Y0101').astype(int).map(date_map)

    # Map municipality name to surrogate key
    melted_df['municipality_key'] = melted_df['municipality_name'].map(
        municipality_map)

    # Clean up and select final columns
    # The source data does not contain an age_group, so it's omitted.
    fact_df = melted_df[['date_key', 'municipality_key',
                         'gender', 'population_count']].copy()

    # Remove rows that couldn't be mapped (e.g., the 'Åland' total row)
    fact_df.dropna(subset=['municipality_key'], inplace=True)
    fact_df['municipality_key'] = fact_df['municipality_key'].astype(int)

    fact_df.to_sql('fact_demographics', engine,
                   if_exists='append', index=False)
    print("fact_demographics populated.")

# --- 4. HELPER FUNCTIONS & MAIN ORCHESTRATION ---


def get_dimension_map(engine, table_name, key_col, value_col):
    """Fetches a dimension table to create a business key to surrogate key map."""
    df = pd.read_sql_table(table_name, engine)
    if key_col == value_col:
        # Handles the case for a 1:1 map, e.g., for dim_date.
        # Creates a dictionary mapping a column's values to themselves.
        return pd.Series(df[key_col].values, index=df[key_col]).to_dict()
    else:
        return df.set_index(key_col)[value_col].to_dict()


def main():
    """Main ETL orchestration function."""
    engine = get_db_engine()
    if not engine:
        return

    # It's good practice to clear tables to ensure a fresh load
    clear_tables(engine)

    # --- Populate Dimensions ---
    populate_dim_date(engine)
    populate_dim_municipality(engine)
    populate_dim_product(engine)

    # --- Create mapping dictionaries for FKs ---
    print("\nCreating dimension maps for fact processing...")
    municipality_map = get_dimension_map(
        engine, 'dim_municipality', 'name', 'municipality_key')
    date_map = get_dimension_map(
        engine, 'dim_date', 'date_key', 'date_key')  # Simple 1:1 map
    product_map = get_dimension_map(
        engine, 'dim_product', 'product_id', 'product_key')

    # --- Populate remaining dimensions that have dependencies ---
    populate_dim_store(engine, municipality_map)
    # We need to re-fetch the store map after populating it
    store_map = get_dimension_map(engine, 'dim_store', 'store_id', 'store_key')
    print("Dimension maps created.")

    # --- Populate Fact Tables ---
    print("\nPopulating Fact Tables...")
    populate_fact_sales(engine, date_map, product_map, store_map)
    populate_fact_tourism(engine, date_map, municipality_map)
    populate_fact_demographics(engine, date_map, municipality_map)

    print("\nETL process completed successfully!")


if __name__ == '__main__':
    main()
