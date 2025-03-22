from flask import Flask, request, jsonify
import requests
import uuid
import time
import logging

server = Flask(__name__)

# Налаштування логів
logging.basicConfig(level=logging.INFO)
log = logging.getLogger("FacadeServer")

tracker_url = "http://localhost:8881/tracker"
placeholder_url = "http://localhost:8882/placeholder"

# Функція для повторних спроб
def push_to_tracker(payload, max_attempts=3, wait_time=1):
    for try_num in range(max_attempts):
        try:
            result = requests.post(tracker_url, data=payload, timeout=4)
            if result.status_code == 201:
                log.info(f"Data {payload['key']} transmitted to tracker")
                return result
        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as error:
            log.warning(f"Try {try_num + 1}/{max_attempts} failed for {payload['key']}: {error}")
            if try_num < max_attempts - 1:
                time.sleep(wait_time)
            continue
    log.error(f"Transmission failed for {payload['key']} after {max_attempts} attempts")
    return None

@server.route("/entry", methods=["POST", "GET"])
def process():
    if request.method == "POST":
        content = request.form.get("data")
        if not content:
            return jsonify({"error": "Empty data"}), 400

        unique_key = str(uuid.uuid4()) #зміна на фіксоване значення для тесту дедуплікації
        packet = {"key": unique_key, "data": content}

        # Використовуємо retry
        outcome = push_to_tracker(packet)
        if outcome is None:
            return jsonify({"error": "Tracker unreachable"}), 503

        return jsonify({"key": unique_key, "data": content}), 200

    elif request.method == "GET":
        tracker_data = requests.get(tracker_url)
        placeholder_data = requests.get(placeholder_url)
        combined = f"Tracker says: {tracker_data.text} | Placeholder says: {placeholder_data.text}"
        return combined, 200

    else:
        return jsonify({"error": "Invalid method"}), 405

if __name__ == "__main__":
    server.run(port=8880)
