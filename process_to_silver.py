
import pandas as pd
import json
import os
import glob
from concurrent.futures import ProcessPoolExecutor

# Define paths
bronze_path = 'bronze'
silver_path = 'silver'

def process_demographics():
    """
    Processes demographics data:
    - Converts 'år' to integer.
    - Calculates total population for each municipality.
    """
    print("Processing demographics data...")
    file_path = os.path.join(bronze_path, 'demographics', 'api_data_gender.csv')
    df = pd.read_csv(file_path, dtype={'år': str}, encoding='utf-8')

    # Clean the 'år' column by removing quotes and converting to integer
    df['år'] = df['år'].str.replace('"', '').astype(int)

    # Identify municipality columns (those with "Kvinnor" or "Män")
    municipality_cols = [col for col in df.columns if ' Kvinnor' in col or ' Män' in col]

    # Convert population data to numeric, coercing errors
    for col in municipality_cols:
        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)

    # Calculate totals for each municipality
    # Get unique municipality names from column headers
    municipalities = sorted(list(set([col.replace(' Kvinnor', '').replace(' Män', '') for col in municipality_cols])))

    for muni in municipalities:
        kvinnor_col = f"{muni} Kvinnor"
        man_col = f"{muni} Män"
        total_col = f"{muni} Total"
        if kvinnor_col in df.columns and man_col in df.columns:
            df[total_col] = df[kvinnor_col] + df[man_col]

    # Save to silver
    output_path = os.path.join(silver_path, 'demographics.csv')
    df.to_csv(output_path, index=False, encoding='utf-8')
    print(f"Demographics data saved to {output_path}")

def process_tourism():
    """
    Processes tourism data:
    - Verifies date-related columns.
    - Verifies 'accommodation_type'.
    """
    print("Processing tourism data...")
    file_path = os.path.join(bronze_path, 'tourism', 'tourism_data.csv')
    df = pd.read_csv(file_path, encoding='utf-8')

    # Verify date columns
    df['date'] = pd.to_datetime(df['date'])
    df['year'] = pd.to_numeric(df['year'])
    df['month'] = pd.to_numeric(df['month'])

    # Verify that the year from 'date' matches the 'year' column
    assert (df['date'].dt.year == df['year']).all()
    # Verify that the month from 'date' matches the 'month' column
    assert (df['date'].dt.month == df['month']).all()

    # Verify 'accommodation_type'
    valid_accommodation_types = ["guesthouse", "camping", "hotel"]
    # Find any types not in the valid list
    invalid_types = df[~df['accommodation_type'].isin(valid_accommodation_types)]
    if not invalid_types.empty:
        print(f"Warning: Found unexpected accommodation types: {invalid_types['accommodation_type'].unique()}")
        # For this exercise, we'll allow them but in a real scenario, this would need a decision.

    # Save to silver
    output_path = os.path.join(silver_path, 'tourism.csv')
    df.to_csv(output_path, index=False, encoding='utf-8')
    print(f"Tourism data saved to {output_path}")

def process_grocery_sales():
    """
    Processes grocery sales data from multiple JSON files:
    - Converts 'store_id' and 'product_id' to numeric.
    - Extracts 'day', 'month', 'year' from 'date'.
    - Combines all yearly data into one CSV.
    """
    print("Processing grocery sales data...")
    json_files = glob.glob(os.path.join(bronze_path, 'grocery', 'grocery_sales_*.json'))
    all_sales_data = []

    for file in json_files:
        with open(file, 'r', encoding='utf-8') as f:
            data = json.load(f)
            for entry in data:
                # Transform IDs
                entry['store_id'] = int(entry['store_id'].replace('STORE_', ''))
                entry['product_id'] = int(entry['product_id'].replace('PROD_', ''))

                # Handle date
                sale_date = pd.to_datetime(entry['date'])
                entry['day'] = sale_date.day
                entry['month'] = sale_date.month
                entry['year'] = sale_date.year
                
                all_sales_data.append(entry)
            print(f"processed {file}")

    df = pd.DataFrame(all_sales_data)
    
    # Reorder columns to have date info together
    if not df.empty:
        cols = ['store_id', 'product_id', 'date', 'year', 'month', 'day', 'sales_amount', 'units_sold']
        # Ensure all columns exist before reordering
        df = df[[c for c in cols if c in df.columns]]


    output_path = os.path.join(silver_path, 'grocery_sales.csv')
    df.to_csv(output_path, index=False, encoding='utf-8')
    print(f"Grocery sales data saved to {output_path}")
    

def process_single_file(file):
    df = pd.read_json(file, encoding='utf-8')

    df['store_id'] = (
        df['store_id']
        .str.replace('STORE_', '', regex=False)
        .astype('int32')
    )
    df['product_id'] = (
        df['product_id']
        .str.replace('PROD_', '', regex=False)
        .astype('int32')
    )

    df['date'] = pd.to_datetime(df['date'], errors='coerce')
    df['year'] = df['date'].dt.year.astype('int16')
    df['month'] = df['date'].dt.month.astype('int8')
    df['day'] = df['date'].dt.day.astype('int8')

    print(f"processed {file}")
    return df


def process_grocery_sales_parallel():
    print("Processing grocery sales data...")

    json_files = glob.glob(os.path.join(bronze_path, 'grocery', 'grocery_sales_*.json'))

    # Use slightly fewer workers than cores (best practice)
    max_workers = min(8, os.cpu_count() - 1)

    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        dfs = list(executor.map(process_single_file, json_files))

    df = pd.concat(dfs, ignore_index=True)

    cols = ['store_id', 'product_id', 'date', 'year', 'month', 'day',
            'sales_amount', 'units_sold']
    df = df[[c for c in cols if c in df.columns]]

    output_path = os.path.join(silver_path, 'grocery_sales.csv')
    df.to_csv(output_path, index=False, encoding='utf-8')


def process_products():
    """
    Processes product data:
    - Converts 'product_id' to numeric.
    """
    print("Processing products data...")
    file_path = os.path.join(bronze_path, 'grocery', 'products.json')
    with open(file_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    for item in data:
        item['product_id'] = int(item['product_id'].replace('PROD_', ''))

    df = pd.DataFrame(data)
    output_path = os.path.join(silver_path, 'products.csv')
    df.to_csv(output_path, index=False, encoding='utf-8')
    print(f"Products data saved to {output_path}")

def process_stores():
    """
    Processes store data:
    - Converts 'store_id' to numeric.
    """
    print("Processing stores data...")
    file_path = os.path.join(bronze_path, 'grocery', 'stores.json')
    with open(file_path, 'r', encoding='utf-8') as f:
        data = json.load(f)

    for item in data:
        item['store_id'] = int(item['store_id'].replace('STORE_', ''))
        
    df = pd.DataFrame(data)
    output_path = os.path.join(silver_path, 'stores.csv')
    df.to_csv(output_path, index=False, encoding='utf-8')
    print(f"Stores data saved to {output_path}")


if __name__ == '__main__':
    # Create silver directory if it doesn't exist
    if not os.path.exists(silver_path):
        os.makedirs(silver_path)

    process_demographics()
    process_tourism()
    #process_grocery_sales()
    process_grocery_sales_parallel()
    process_products()
    process_stores()
    
    print("\nSilver data processing complete.")
