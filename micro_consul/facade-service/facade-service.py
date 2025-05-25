import random
import requests
from flask import Flask, request, jsonify
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
import json
import time
import consul
import os
import atexit

app = Flask(__name__)
CONSUL_HOST = os.getenv("CONSUL_HOST", "consul")
CONSUL_PORT = int(os.getenv("CONSUL_PORT", "8500"))
INSTANCE_ID = os.getenv("INSTANCE_ID", "facade1")
PORT = 8880


consul_client = consul.Consul(host=CONSUL_HOST, port=CONSUL_PORT)


def register_service():
    consul_client.agent.service.register(
        service_id=INSTANCE_ID,
        name="facade-service",
        address="facade-service",
        port=PORT,
        check=consul.Check.http(f"http://facade-service:{PORT}/health", interval="10s", timeout="5s")
    )
    print(f"[Facade] Зареєстровано в Consul як {INSTANCE_ID}")


def deregister_service():
    consul_client.agent.service.deregister(INSTANCE_ID)
    print(f"[Facade] Видалено з Consul")


@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "healthy"}), 200


def get_kafka_config():
    for _ in range(10):
        try:
            _, brokers_data = consul_client.kv.get("config/kafka/brokers")
            if brokers_data is None:
                raise ValueError("Ключ config/kafka/brokers відсутній у Consul")
            brokers = json.loads(brokers_data["Value"].decode("utf-8"))
            _, topic_data = consul_client.kv.get("config/kafka/topic")
            if topic_data is None:
                raise ValueError("Ключ config/kafka/topic відсутній у Consul")
            topic = json.loads(topic_data["Value"].decode("utf-8"))
            return brokers, topic
        except Exception as e:
            print(f"[Facade] Помилка отримання конфігурації Kafka з Consul: {e}")
            time.sleep(5)
    raise Exception("[Facade] Не вдалося отримати конфігурацію Kafka після кількох спроб")


KAFKA_BROKERS, KAFKA_TOPIC = get_kafka_config()
for i in range(10):
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BROKERS,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        print("[Facade] Підключено до Kafka", flush=True)
        break
    except NoBrokersAvailable:
        print(f"[Facade] Kafka недоступна (спроба {i+1}/10), повтор через 5с...", flush=True)
        time.sleep(5)
else:
    raise Exception("[Facade] Kafka недоступна після кількох спроб")

def get_service_urls(service_name):
    try:
        _, services = consul_client.catalog.service(service_name)
        return [f"http://{s['ServiceAddress']}:{s['ServicePort']}" for s in services if s["ServiceID"]]
    except Exception as e:
        print(f"[Facade] Помилка виявлення сервісу {service_name}: {e}")
        return []

def send_to_random_instance(service_name, path, method="GET", data=None):
    urls = get_service_urls(service_name)
    random.shuffle(urls)
    for url in urls:
        try:
            full_url = f"{url}{path}"
            if method == "POST":
                r = requests.post(full_url, json=data, timeout=3)
            else:
                r = requests.get(full_url, timeout=3)
            if r.status_code in (200, 201):
                return r.json()
        except Exception as e:
            print(f"[Facade] Попередження: не вдалося звернутися до {url}: {e}")
    return {"error": "Помилка всіх екземплярів"}, 500

@app.route("/entry", methods=["POST"])
def post_entry():
    msg = request.json.get("msg")
    if msg:
        producer.send(KAFKA_TOPIC, {"msg": msg})
        print(f"[Facade] Відправлено до Kafka: {msg}")
        response = send_to_random_instance("logging-service", "/tracker", method="POST", data={"msg": msg})
        if "error" in response:
            print(f"[Facade] Не вдалося надіслати до logging-service: {response['error']}")
            return jsonify({"error": "Не вдалося зберегти в logging-service"}), 500
        print(f"[Facade] Успішно надіслано до logging-service: {response}")
        return jsonify({"status": "Відправлено до Kafka та logging-service"}), 201
    print("[Facade] Помилка: Відсутній msg у запиті")
    return jsonify({"error": "Відсутній msg"}), 400

@app.route("/entry", methods=["GET"])
def get_logs():
    return send_to_random_instance("logging-service", "/tracker", method="GET")

@app.route("/notify", methods=["GET"])
def notify():
    return send_to_random_instance("messages-service", "/messages", method="GET")

@app.route("/combined", methods=["GET"])
def get_combined_messages():
    logging_response = send_to_random_instance("logging-service", "/tracker", method="GET")
    logging_messages = logging_response.get("messages", []) if "error" not in logging_response else []
    messages_response = send_to_random_instance("messages-service", "/messages", method="GET")
    messages_service_messages = messages_response.get("messages", []) if "error" not in messages_response else []
    combined_messages = list(set(logging_messages + messages_service_messages))
    return jsonify({"messages": combined_messages})

if __name__ == "__main__":
    register_service()
    atexit.register(deregister_service)
    app.run(host='0.0.0.0', port=PORT)
