import requests
import json
import os

# The API endpoint
url = "https://pxweb.asub.ax:443/PXWeb/api/v1/en/Statistik/KO/KO007.px"

# Read query file
with open("bronze/costofliving-pxapi-api_table_KO007.px.json", "r", encoding="utf-8") as f:
    full_query_data = json.load(f)

# The API expects the queryObj part containing 'query' and 'response'
query = full_query_data.get("queryObj", full_query_data)

def fetch_data():
    try:
        # We use json=query to automatically set Content-Type to application/json
        response = requests.post(url, json=query)

        # Check if the request was successful
        response.raise_for_status()

        # Ensure directory exists
        os.makedirs("bronze/costofliving", exist_ok=True)

        # Save response to file. newline="" avoids extra blank lines on Windows
        with open("bronze/costofliving/costofliving.csv", "w", encoding="utf-8", newline="") as f:
            f.write(response.text)
            
        print("Data successfully saved to bronze/costofliving/costofliving.csv")

    except requests.exceptions.HTTPError as err:
        print(f"HTTP error occurred: {err}")
    except Exception as err:
        print(f"An error occurred: {err}")

if __name__ == "__main__":
    fetch_data()
