import requests
import json

# The API endpoint
url = "https://pxweb.asub.ax:443/PXWeb/api/v1/sv/Statistik/BE/Befolkningens%20storlek%20och%20struktur/BE001.px"

# Read query file
with open("demo_api_query.json", "r", encoding="utf-8") as f:
    query = json.load(f)

def fetch_data():
    try:
        # We use json=query to automatically set Content-Type to application/json
        response = requests.post(url, json=query)
        
        # Check if the request was successful
        response.raise_for_status()
        
        # Parse the result
        data = response.json()
        
        # Save response to file
        with open("data/demographics/api_data.json", "w", encoding="utf-8") as f:
            json.dump(data, f, indent=4, ensure_ascii=False)
        
    except requests.exceptions.HTTPError as err:
        print(f"HTTP error occurred: {err}")
    except Exception as err:
        print(f"An error occurred: {err}")

if __name__ == "__main__":
    fetch_data()