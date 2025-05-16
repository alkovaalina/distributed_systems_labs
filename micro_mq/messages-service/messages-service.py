from flask import Flask, jsonify
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable
import json
import threading
import os
import time

app = Flask(__name__)
INSTANCE_ID = os.getenv("INSTANCE_ID", "default")
PORT = int(os.getenv("PORT", "8890"))

messages = []

def consume():
    for i in range(10):
        try:
            consumer = KafkaConsumer(
                "messages",
                bootstrap_servers=["kafka1:9092", "kafka2:9093", "kafka3:9094"],
                auto_offset_reset="earliest",
                #group_id=f"group-{INSTANCE_ID}",
                group_id="group-messages",
                value_deserializer=lambda m: json.loads(m.decode('utf-8'))
            )
            print(f"[{INSTANCE_ID}] Підключено до Kafka", flush=True)
            break
        except NoBrokersAvailable:
            print(f"[{INSTANCE_ID}] Kafka недоступний (спроба {i+1}/10), повтор через 5с...", flush=True)
            time.sleep(5)
    else:
        raise Exception(f"[{INSTANCE_ID}] Kafka недоступний після кількох спроб")

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
    print(f"[{INSTANCE_ID}] Starting on port {PORT}")
    threading.Thread(target=consume, daemon=True).start()
    app.run(host='0.0.0.0', port=PORT)
