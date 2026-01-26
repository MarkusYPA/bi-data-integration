import json
from collections import defaultdict

# Load stores
with open('../data/grocery/stores.json', 'r') as f:
    stores = {s['store_id']: s for s in json.load(f)}

# Load sales for a year
with open('../data/grocery/grocery_sales_2023.json', 'r') as f:
    sales = json.load(f)

# Aggregate by municipality and month
monthly_sales = defaultdict(float)

for sale in sales:
    store = stores[sale['store_id']]
    municipality = store['municipality_code']
    year, month = sale['date'][:7].split('-')
    # Extract YYYY-MM
    key = (municipality, year, month)
    monthly_sales[key] += sale['sales_amount']

# Print results
for (municipality, year, month), total in sorted(monthly_sales.items()):
    print(f"{municipality} {year}-{month}: {total:.2f} EUR")