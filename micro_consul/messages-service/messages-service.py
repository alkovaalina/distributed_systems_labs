from flask import Flask, jsonify
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable
import json
import threading
import os
import time
import consul
import atexit

app = Flask(__name__)
INSTANCE_ID = os.getenv("INSTANCE_ID", "default")
PORT = int(os.getenv("PORT", "8890"))
CONSUL_HOST = os.getenv("CONSUL_HOST", "consul")
CONSUL_PORT = int(os.getenv("CONSUL_PORT", "8500"))

messages = []

consul_client = consul.Consul(host=CONSUL_HOST, port=CONSUL_PORT)

def register_service():
    consul_client.agent.service.register(
        service_id=INSTANCE_ID,
        name="messages-service",
        address="messages-service" + INSTANCE_ID[-1],
        port=PORT,
        check=consul.Check.http(f"http://messages-service{INSTANCE_ID[-1]}:{PORT}/health", interval="10s", timeout="5s")
    )
    print(f"[{INSTANCE_ID}] Зареєстровано в Consul як {INSTANCE_ID}")


def deregister_service():
    consul_client.agent.service.deregister(INSTANCE_ID)
    print(f"[{INSTANCE_ID}] Видалено з Consul")


@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "healthy"}), 200

def get_kafka_config():
    for attempt in range(10):
        try:
            _, brokers_data = consul_client.kv.get("config/kafka/brokers")
            print(f"[{INSTANCE_ID}] Attempt {attempt + 1}: brokers_data = {brokers_data}")
            if brokers_data is None:
                raise ValueError("Ключ config/kafka/brokers відсутній у Consul")
            brokers = json.loads(brokers_data["Value"].decode("utf-8"))
            _, topic_data = consul_client.kv.get("config/kafka/topic")
            if topic_data is None:
                raise ValueError("Ключ config/kafka/topic відсутній у Consul")
            topic = json.loads(topic_data["Value"].decode("utf-8"))
            print(f"[{INSTANCE_ID}] Successfully retrieved Kafka config: brokers={brokers}, topic={topic}")
            return brokers, topic
        except Exception as e:
            print(f"[{INSTANCE_ID}] Attempt {attempt + 1} failed: {e}")
            time.sleep(5)
    raise Exception(f"[{INSTANCE_ID}] Не вдалося отримати конфігурацію Kafka після 10 спроб")

def consume():
    KAFKA_BROKERS, KAFKA_TOPIC = get_kafka_config()
    for i in range(10):
        try:
            consumer = KafkaConsumer(
                KAFKA_TOPIC,
                bootstrap_servers=KAFKA_BROKERS,
                auto_offset_reset="earliest",
                group_id="group-messages",
                value_deserializer=lambda m: json.loads(m.decode('utf-8'))
            )
            print(f"[{INSTANCE_ID}] Підключено до Kafka", flush=True)
            break
        except NoBrokersAvailable:
            print(f"[{INSTANCE_ID}] Kafka недоступна (спроба {i+1}/10), повтор через 5с...", flush=True)
            time.sleep(5)
    else:
        raise Exception(f"[{INSTANCE_ID}] Kafka недоступна після кількох спроб")

    print(f"[{INSTANCE_ID}] Запуск циклу споживача Kafka", flush=True)
    for msg in consumer:
        content = msg.value.get("msg")
        if content:
            messages.append(content)
            print(f"[{INSTANCE_ID}] Отримано з Kafka: {content}", flush=True)

@app.route("/messages", methods=["GET"])
def get_messages():
    return jsonify({"messages": messages})

if __name__ == "__main__":
    time.sleep(20)
    register_service()
    atexit.register(deregister_service)
    print(f"[{INSTANCE_ID}] Запуск на порті {PORT}")
    threading.Thread(target=consume, daemon=True).start()
    app.run(host='0.0.0.0', port=PORT)
