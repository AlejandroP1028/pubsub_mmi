import json
import threading
import queue
import time
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

class Broker:
    def __init__(self):
        self.messages = []
        self.subscribers = {}
        self.lock = threading.Lock()
        self.next_id = 0

    def publish(self, payload):
        with self.lock:
            msg = {
                "id": self.next_id,
                "payload": payload,
                "delivered_to": set()
            }
            self.next_id += 1
            self.messages.append(msg)
            return msg

    def add_subscriber(self, sub_type):
        sub_id = str(uuid.uuid4())
        with self.lock:
            self.subscribers[sub_id] = {
                "id": sub_id,
                "type": sub_type,
                "queue": queue.Queue()
            }
        # broadcast stored messages to new subscriber
        self.broadcast()
        return sub_id

    def remove_subscriber(self, sub_id):
        with self.lock:
            self.subscribers.pop(sub_id, None)

    def broadcast(self):
        with self.lock:
            for msg in self.messages:
                for sid, sub in self.subscribers.items():
                    if sid not in msg["delivered_to"]:
                        sub["queue"].put(msg["payload"])
                        msg["delivered_to"].add(sid)

            if self.subscribers:
                active = set(self.subscribers.keys())
                self.messages = [
                    m for m in self.messages
                    if m["delivered_to"] != active
                ]

    def status(self):
        with self.lock:
            return {
                "stored_messages": len(self.messages),
                "active_subscribers": len(self.subscribers)
            }


broker = Broker()


class BrokerHandler(BaseHTTPRequestHandler):

    def send_json(self, code, payload):
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(json.dumps(payload).encode())

    def do_POST(self):
        if self.path != "/publish":
            self.send_json(404, {"error": "not found"})
            return

        length = int(self.headers.get("Content-Length", 0))
        body = json.loads(self.rfile.read(length))

        broker.publish(body)
        broker.broadcast()

        self.send_json(200, {"status": "published"})

    def do_GET(self):
        if self.path == "/status":
            self.send_json(200, broker.status())
            return

        if self.path.startswith("/messages"):
            sub_id = broker.add_subscriber("poll")

            messages = []
            start = time.time()

            try:
                while True:
                    try:
                        msg = broker.subscribers[sub_id]["queue"].get(timeout=1)
                        messages.append(msg)
                    except queue.Empty:
                        if messages or time.time() - start > 15:
                            break
            finally:
                broker.remove_subscriber(sub_id)

            self.send_json(200, {"messages": messages})
            return

        if self.path == "/subscribe":
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream")
            self.send_header("Cache-Control", "no-cache")
            self.end_headers()

            sub_id = broker.add_subscriber("sse")

            try:
                while True:
                    msg = broker.subscribers[sub_id]["queue"].get()
                    self.wfile.write(f"data: {json.dumps(msg)}\n\n".encode())
                    self.wfile.flush()
            except Exception:
                pass
            finally:
                broker.remove_subscriber(sub_id)


def run():
    server = ThreadingHTTPServer(("localhost", 8000), BrokerHandler)
    print("Broker running on http://localhost:8000")
    server.serve_forever()

if __name__ == "__main__":
    run()
