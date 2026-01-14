import csv
import json
import urllib.request
import urllib.error

BROKER_URL = "http://localhost:8000/publish"

def publish(message):
    data = json.dumps(message).encode()
    req = urllib.request.Request(
        BROKER_URL,
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST"
    )

    try:
        with urllib.request.urlopen(req) as resp:
            resp.read()
        return True
    except urllib.error.URLError as e:
        print(f"Error publishing {message['name']}: {e}")
        return False

def main():
    total_published = 0

    # Read CSV and sort by priority ascending
    with open("people.csv", newline="") as f:
        reader = csv.DictReader(f)
        rows = sorted(reader, key=lambda r: int(r["priority"]))

        for row in rows:
            message = {
                "name": row["name"],
                "age": int(row["age"]),
                "country": row["country"],
                "company": row["company"],
                "priority": int(row["priority"])
            }

            if publish(message):
                print(f"Published (Priority {message['priority']}): {message['name']}")
                total_published += 1

    print(f"\nDone. Total messages published: {total_published}")

if __name__ == "__main__":
    main()
