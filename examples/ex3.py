import csv
import json
from collections import defaultdict

# Load tourism data
tourism = {}
with open('../data/tourism/tourism_data.csv', 'r') as f:
    reader = csv.DictReader(f)
    for row in reader:
        key = (row['municipality_code'], row['year'], row['month'])
        tourism[key] = {
            'visitor_count': int(row['visitor_count']),
            'revenue': float(row['revenue'])
        }

# Load stores
with open('../data/grocery/stores.json', 'r') as f:
    stores = {s['store_id']: s for s in json.load(f)}

# Load and aggregate grocery sales
with open('../data/grocery/grocery_sales_2023.json', 'r') as f:
    sales = json.load(f)

monthly_grocery = defaultdict(float)
for sale in sales:
    store = stores[sale['store_id']]
    municipality = store['municipality_code']
    year, month = sale['date'][:7].split('-')
    key = (municipality, year, month)
    monthly_grocery[key] += sale['sales_amount']

# Correlate
for key in sorted(tourism.keys()):
    if key in monthly_grocery:
        print(f"{key[0]} {key[1]}-{key[2]}:")
        print(f" Tourism: {tourism[key]['visitor_count']} visitors, {tourism[key]['revenue']:.2f} EUR revenue")
        print(f" Grocery: {monthly_grocery[key]:.2f} EUR")
        print()