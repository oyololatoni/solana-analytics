import requests
import json

API_URL = "http://localhost:8000/screener/filter"

def test_external_screener():
    print("Testing External Screener...")
    
    # 1. Test Generic Boosted (No query)
    payload = {
        "source": "external",
        "query": "",
        "filters": [
            {"metric": "volume_24h", "condition": "gt", "value": 0}
        ]
    }
    
    try:
        res = requests.post(API_URL, json=payload, timeout=10)
        print(f"Status: {res.status_code}")
        if res.ok:
            data = res.json()
            print(f"Got {len(data)} results.")
            if data:
                print(f"Top 1: {data[0]}")
        else:
            print(f"Error: {res.text}")
            
    except Exception as e:
        print(f"Failed: {e}")

    # 2. Test Search Query (e.g. 'SOL')
    print("\nTesting Search Query 'SOL'...")
    payload["query"] = "SOL"
    try:
        res = requests.post(API_URL, json=payload, timeout=10)
        print(f"Status: {res.status_code}")
        if res.ok:
            data = res.json()
            print(f"Got {len(data)} results.")
            if data:
                print(f"Top 1: {data[0]}")
        else:
            print(f"Error: {res.text}")

    except Exception as e:
        print(f"Failed: {e}")

if __name__ == "__main__":
    test_external_screener()
