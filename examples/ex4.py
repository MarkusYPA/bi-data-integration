import json
from collections import defaultdict

# Load products
with open('../data/grocery/products.json', 'r') as f:
    products = {p['product_id']: p for p in json.load(f)}

# Load sales
with open('../data/grocery/grocery_sales_2023.json', 'r') as f:
    sales = json.load(f)

# Aggregate by category
category_sales = defaultdict(float)
for sale in sales:
    product = products[sale['product_id']]
    category = product['product_category']
    category_sales[category] += sale['sales_amount']

# Print results
for category, total in sorted(category_sales.items(), key=lambda x: x[1], reverse=True):
    print(f"{category}: {total:.2f} EUR")