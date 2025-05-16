import random
import requests
from flask import Flask, request, jsonify
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
import json
import time

app = Flask(__name__)
CONFIG_SERVER_URL = "http://config-server:8888"
KAFKA_TOPIC = "messages"
KAFKA_BROKER = ["kafka1:9092", "kafka2:9093", "kafka3:9094"]

# Kafka connection
for i in range(10):
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BROKER,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        print("[Facade] Підключено до Kafka", flush=True)
        break
    except NoBrokersAvailable:
        print(f"[Facade] Kafka ще не доступна (attempt {i+1}/10), повторна спроба через 5 с...", flush=True)
        time.sleep(5)
else:
    raise Exception("[Facade] Kafka недоступна після кількох спроб")

def get_service_urls(service_name):
    try:
        response = requests.get(f"{CONFIG_SERVER_URL}/services/{service_name}", timeout=2)
        return response.json() if response.status_code == 200 else []
    except Exception as e:
        print(f"[Facade] Помилка Config: {e}")
        return []

def send_to_random_instance(service_name, path, method="GET", data=None):
    urls = get_service_urls(service_name)
    random.shuffle(urls)
    for base_url in urls:
        try:
            url = f"{base_url}{path}"
            if method == "POST":
                r = requests.post(url, json=data, timeout=3)
            else:
                r = requests.get(url, timeout=3)
            if r.status_code in (200, 201):
                return r.json()
        except Exception as e:
            print(f"[Facade] Warning: fail {url}: {e}")
    return {"error": "Помилка всіх екземплярів"}, 500

@app.route("/entry", methods=["POST"])
def post_entry():
    msg = request.json.get("msg")
    if msg:
        # Відправка до Kafka
        producer.send(KAFKA_TOPIC, {"msg": msg})
        print(f"[Facade] Відправлено до Kafka: {msg}")

        # Відправка до випадкового logging-service
        response = send_to_random_instance("logging-service", "/tracker", method="POST", data={"msg": msg})
        if "error" in response:
            print(f"[Facade] Не вдалося надіслати до logging-service: {response['error']}")
            return jsonify({"error": "Не вдалося зберегти в logging-service"}), 500

        return jsonify({"status": "Відправлено до Kafka та logging-service"}), 201
    return jsonify({"error": "Відсутній msg"}), 400

@app.route("/entry", methods=["GET"])
def get_logs():
    return send_to_random_instance("logging-service", "/tracker", method="GET")

@app.route("/notify", methods=["GET"])
def notify():
    return send_to_random_instance("messages-service", "/messages", method="GET")

@app.route("/combined", methods=["GET"])
def get_combined_messages():
    # Отримання повідомлень з випадкового logging-service
    logging_response = send_to_random_instance("logging-service", "/tracker", method="GET")
    logging_messages = logging_response.get("messages", []) if "error" not in logging_response else []

    # Отримання повідомлень з випадкового messages-service
    messages_response = send_to_random_instance("messages-service", "/messages", method="GET")
    messages_service_messages = messages_response.get("messages", []) if "error" not in messages_response else []

    # Об’єднання і видалення дублікатів
    combined_messages = list(set(logging_messages + messages_service_messages))
    return jsonify({"messages": combined_messages})

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=8880)
