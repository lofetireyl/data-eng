import os
import sys
import requests

BASE_URL = "https://api.getsong.co/"
API_KEY = "8dbafd87de8bde42760f94b6104452db"

def main():
    if len(sys.argv) < 3:
        print("Usage: python search.py <type: song|artist|both> <lookup> [limit]")
        sys.exit(1)

    search_type = sys.argv[1]
    lookup = sys.argv[2]
    limit = sys.argv[3] if len(sys.argv) > 3 else None

    params = {
        "type": search_type,
        "lookup": lookup,
        "api_key": API_KEY,
    }
    if limit:
        params["limit"] = limit

    headers = {"X-API-KEY": API_KEY, "Accept": "application/json"}

    url = BASE_URL.rstrip("/") + "/search/"
    try:
        r = requests.get(url, params=params, headers=headers, timeout=20)
        r.raise_for_status()
        print(r.text) 
    except requests.HTTPError as e:
        print(f"HTTP-Fehler: {e.response.status_code} {e.response.text}")
        sys.exit(1)
    except Exception as e:
        print(f"Fehler: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()