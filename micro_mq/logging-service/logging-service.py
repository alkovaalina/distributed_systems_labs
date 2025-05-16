from flask import Flask, request, jsonify
import hazelcast
import os
import logging

tracker = Flask(__name__)
INSTANCE_ID = os.getenv("INSTANCE_ID", "default")
PORT = int(os.getenv("PORT", "8881"))

tracker.logger.setLevel(logging.DEBUG)

hz = hazelcast.HazelcastClient(
    cluster_members=["hazelcast1:5701", "hazelcast2:5701", "hazelcast3:5701"],
    cluster_name="dev"
)
records = hz.get_map("logs").blocking()

@tracker.route("/tracker", methods=["POST", "GET"])
def handle():
    if request.method == "POST":
        if request.is_json:
            payload = request.get_json()
            identifier = payload.get("msg")
            info = payload.get("msg")
        else:
            identifier = request.form.get("key")
            info = request.form.get("data")

        if not (identifier and info):
            return jsonify({"error": "Відсутній ключ або дані"}), 400

        if records.contains_key(identifier):
            tracker.logger.info(f"[{INSTANCE_ID}] Skipped: {identifier}")
            return jsonify({"status": "Дублікат"}), 201

        records.put(identifier, info)
        tracker.logger.info(f"[{INSTANCE_ID}] Stored: {info} [Key: {identifier}]")
        return jsonify({"status": "OK"}), 201

    elif request.method == "GET":
        values = records.values()
        return jsonify({"messages": list(values)})

if __name__ == "__main__":
    tracker.logger.info(f"[{INSTANCE_ID}] Starting logging-service on port {PORT}")
    tracker.run(host='0.0.0.0', port=PORT)
