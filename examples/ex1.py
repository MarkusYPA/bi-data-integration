import json

# Load stores
with open('../bronze/grocery/stores.json', 'r') as f:
    stores = {s['store_id']: s for s in json.load(f)}
    
# Load sales
with open('../bronze/grocery/grocery_sales_2023.json', 'r') as f:
    sales = json.load(f)
    
# Join
for sale in sales:
    store = stores[sale['store_id']]    
    print(f"Store: {store['store_name']} in {store['municipality_name']}")
    print(f"Sales: {sale['sales_amount']} EUR on {sale['date']}")
