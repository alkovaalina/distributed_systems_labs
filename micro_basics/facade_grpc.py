from flask import Flask, request, jsonify
import requests
import uuid
import time
import logging
import grpc
import tracker_pb2
import tracker_pb2_grpc

server = Flask(__name__)

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("FacadeServer")

# gRPC канал до logging
channel = grpc.insecure_channel('localhost:50051')
tracker_stub = tracker_pb2_grpc.TrackerStub(channel)

placeholder_url = "http://localhost:8882/placeholder"

# Функція з retry для gRPC LogEntry
def push_to_tracker_grpc(key, data, max_attempts=3, wait_time=1):
    payload = tracker_pb2.LogRequest(key=key, data=data)
    for try_num in range(max_attempts):
        try:
            response = tracker_stub.LogEntry(payload, timeout=4)
            log.info(f"Data {key} transmitted to tracker via gRPC: {response.status}")
            return response
        except grpc.RpcError as error:
            log.warning(f"Try {try_num + 1}/{max_attempts} failed for {key}: {error.details()}")
            if try_num < max_attempts - 1:
                time.sleep(wait_time)
            continue
    log.error(f"Transmission failed for {key} after {max_attempts} attempts")
    return None

# Функція для gRPC GetEntries
def fetch_tracker_entries():
    try:
        response = tracker_stub.GetEntries(tracker_pb2.Empty(), timeout=4)
        return response.entries
    except grpc.RpcError as error:
        log.error(f"Failed to fetch tracker entries: {error.details()}")
        return "Tracker unavailable"

@server.route("/entry", methods=["POST", "GET"])
def process():
    if request.method == "POST":
        content = request.form.get("data")
        if not content:
            return jsonify({"error": "Empty data"}), 400

        unique_key = str(uuid.uuid4())

        outcome = push_to_tracker_grpc(unique_key, content)
        if outcome is None:
            return jsonify({"error": "Tracker unreachable"}), 503

        return jsonify({"key": unique_key, "data": content}), 200

    elif request.method == "GET":
        tracker_data = fetch_tracker_entries()  # gRPC-запит до TrackerService
        placeholder_data = requests.get(placeholder_url)  # HTTP до PlaceholderService
        combined = f"Tracker says: {tracker_data} | Placeholder says: {placeholder_data.text}"
        return combined, 200

    else:
        return jsonify({"error": "Invalid method"}), 405

if __name__ == "__main__":
    server.run(port=8880)
