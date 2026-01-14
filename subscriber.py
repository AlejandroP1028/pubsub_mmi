import json
import time
import urllib.request

BROKER_URL = "http://localhost:8000/messages"

def main():
    while True:
        with urllib.request.urlopen(BROKER_URL) as resp:
            data = json.loads(resp.read())

        for msg in data["messages"]:
            print("Received:", msg)

        time.sleep(1)

if __name__ == "__main__":
    main()
